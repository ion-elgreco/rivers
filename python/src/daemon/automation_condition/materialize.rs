//! Materialization fan-out for fired conditions.
use std::collections::{HashMap, HashSet};

use rivers_core::condition::MaterializationPlan;
use rivers_core::storage::{LaunchedBy, PartitionKey as CorePartitionKey};

use super::engine::ConditionTickEngine;
use super::persist::ConditionTickHandle;
use crate::daemon::types::{BackfillRequestData, MaterializationRequestData};
use crate::partitions::PyPartitionKey;
use crate::repository::tag_keys;

impl ConditionTickEngine {
    /// Mint run ids and build the run-shaped requests for a plan, registering
    /// the pre-dispatch marks. Split from the dispatch so the ids can be
    /// persisted as crash-recovery intent BEFORE any run goes out.
    pub(super) fn prepare_run_requests(
        &mut self,
        plan: &MaterializationPlan,
        handle: &mut ConditionTickHandle,
    ) -> Vec<MaterializationRequestData> {
        let mut run_requests: Vec<MaterializationRequestData> = Vec::new();
        if plan.is_empty() {
            return run_requests;
        }

        handle.seed_assets(
            plan.unpartitioned
                .iter()
                .chain(plan.single_partition_groups.values().flatten())
                .chain(plan.multi_partition_backfills.iter().map(|(a, _)| a)),
        );

        let now = handle.timestamp();

        if !plan.unpartitioned.is_empty() {
            let run_id = uuid::Uuid::new_v4().to_string();
            for asset in &plan.unpartitioned {
                self.pass
                    .cache
                    .register_dispatched_run(asset.clone(), run_id.clone(), now, None);
            }
            handle.note_run(&plan.unpartitioned, &run_id);
            run_requests.push(MaterializationRequestData {
                run_id,
                asset_selection: plan.unpartitioned.clone(),
                partition_key: None,
                tags: vec![],
                launched_by: LaunchedBy::Condition,
            });
        }

        for (pk, assets) in &plan.single_partition_groups {
            let run_id = uuid::Uuid::new_v4().to_string();
            for asset in assets {
                self.pass.cache.register_dispatched_run(
                    asset.clone(),
                    run_id.clone(),
                    now,
                    Some(pk.clone()),
                );
            }
            handle.note_run(assets, &run_id);
            run_requests.push(MaterializationRequestData {
                run_id,
                asset_selection: assets.clone(),
                partition_key: Some(pk.clone()),
                tags: vec![],
                launched_by: LaunchedBy::Condition,
            });
        }
        run_requests
    }

    /// Dispatch the prepared run requests plus the plan's backfills.
    pub(super) async fn dispatch_materializations(
        &mut self,
        plan: MaterializationPlan,
        run_requests: Vec<MaterializationRequestData>,
        backfill_ids_by_asset: &HashMap<String, String>,
        handle: &mut ConditionTickHandle,
    ) {
        if !run_requests.is_empty() {
            match self
                .run_dispatcher
                .dispatch_materialization(&run_requests)
                .await
            {
                Ok(outcome) => {
                    for err in outcome.errors {
                        tracing::error!(
                            target: "rivers::daemon",
                            error = %err,
                            "condition run dispatch error"
                        );
                    }
                    let created: HashSet<&str> = outcome.ids.iter().map(String::as_str).collect();
                    for req in &run_requests {
                        if !created.contains(req.run_id.as_str()) {
                            for asset in &req.asset_selection {
                                self.pass.cache.clear_dispatched_run(asset, &req.run_id);
                            }
                            handle.unnote_run(
                                &req.asset_selection,
                                &req.run_id,
                                "run was not created by the dispatcher",
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        target: "rivers::daemon",
                        error = %e,
                        "condition run dispatch failed"
                    );
                    let error = format!("run dispatch failed: {e}");
                    for req in &run_requests {
                        for asset in &req.asset_selection {
                            self.pass.cache.clear_dispatched_run(asset, &req.run_id);
                        }
                        handle.unnote_run(&req.asset_selection, &req.run_id, &error);
                    }
                }
            }
        }

        if !plan.multi_partition_backfills.is_empty() {
            self.dispatch_multi_partition_backfills(
                plan.multi_partition_backfills,
                backfill_ids_by_asset,
                handle,
            )
            .await;
        }
    }

    /// Multi-partition selections become a backfill per asset.
    async fn dispatch_multi_partition_backfills(
        &mut self,
        mats: Vec<(String, Vec<CorePartitionKey>)>,
        backfill_ids_by_asset: &HashMap<String, String>,
        handle: &mut ConditionTickHandle,
    ) {
        let backfill_asset_keys: Vec<String> = mats.iter().map(|(k, _)| k.clone()).collect();
        let mut requests: Vec<BackfillRequestData> = Vec::with_capacity(mats.len());
        for (asset_key, partition_keys) in mats {
            let backfill_id = backfill_ids_by_asset.get(&asset_key).cloned();
            let strategy = self
                .pass
                .conditions_by_key
                .get(asset_key.as_str())
                .map(|&idx| &self.pass.conditions[idx])
                .and_then(|c| c.backfill_strategy.clone())
                .map(|s| crate::partitions::backfill_strategy::PyBackfillStrategy::from_core(&s));
            let py_keys: Vec<PyPartitionKey> =
                partition_keys.iter().map(PyPartitionKey::from).collect();
            let mut tags = HashMap::new();
            tags.insert(
                tag_keys::PRIORITY.to_string(),
                crate::repository::DEFAULT_BACKFILL_PRIORITY.to_string(),
            );
            requests.push(BackfillRequestData {
                target: crate::daemon::RunType::Materialization(vec![asset_key]),
                partition_keys: Some(py_keys),
                partition_range: None,
                strategy,
                failure_policy: Some("continue".to_string()),
                max_concurrency: 4,
                tags: Some(tags),
                dry_run: false,
                backfill_id,
            });
        }

        match self.backfill_dispatcher.dispatch(&requests).await {
            Ok(outcome) => {
                for err in &outcome.errors {
                    tracing::error!(
                        target: "rivers::daemon",
                        error = %err,
                        "condition backfill dispatch error"
                    );
                }
                let failed: HashSet<&str> = outcome
                    .failed_targets
                    .iter()
                    .flatten()
                    .map(String::as_str)
                    .collect();
                // `results` holds the successes in request order (one asset
                // per condition backfill request).
                let mut results = outcome.results.iter();
                for asset_key in &backfill_asset_keys {
                    if failed.contains(asset_key.as_str()) {
                        self.pass.cache.clear_predispatch_mark(asset_key);
                        handle.note_backfill_error(asset_key, "backfill dispatch failed");
                    } else if let Some(result) = results.next()
                        && !result.backfill_id.is_empty()
                    {
                        handle.note_backfill(asset_key, &result.backfill_id);
                    }
                }
            }
            Err(e) => {
                tracing::error!(
                    target: "rivers::daemon",
                    error = %e,
                    "condition backfill dispatch failed"
                );
                let error = format!("backfill dispatch failed: {e}");
                for asset_key in &backfill_asset_keys {
                    self.pass.cache.clear_predispatch_mark(asset_key);
                    handle.note_backfill_error(asset_key, &error);
                }
            }
        }
    }
}
