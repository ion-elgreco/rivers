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
    /// Dispatch materializations for the assets whose conditions fired this tick.
    pub(super) async fn dispatch_materializations(
        &mut self,
        plan: MaterializationPlan,
        handle: &mut ConditionTickHandle,
    ) {
        if plan.is_empty() {
            return;
        }

        let now = handle.timestamp();
        let mut run_requests: Vec<MaterializationRequestData> = Vec::new();

        if !plan.unpartitioned.is_empty() {
            let run_id = uuid::Uuid::new_v4().to_string();
            for asset in &plan.unpartitioned {
                self.pass
                    .cache
                    .register_dispatched_run(asset.clone(), run_id.clone(), now, None);
            }
            handle.register_run(run_id.clone());
            run_requests.push(MaterializationRequestData {
                run_id,
                asset_selection: plan.unpartitioned,
                partition_key: None,
                tags: vec![],
                launched_by: LaunchedBy::Condition,
            });
        }

        for (pk, assets) in plan.single_partition_groups {
            let run_id = uuid::Uuid::new_v4().to_string();
            for asset in &assets {
                self.pass.cache.register_dispatched_run(
                    asset.clone(),
                    run_id.clone(),
                    now,
                    Some(pk.clone()),
                );
            }
            handle.register_run(run_id.clone());
            run_requests.push(MaterializationRequestData {
                run_id,
                asset_selection: assets,
                partition_key: Some(pk),
                tags: vec![],
                launched_by: LaunchedBy::Condition,
            });
        }

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
                            handle.unregister_run(&req.run_id);
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        target: "rivers::daemon",
                        error = %e,
                        "condition run dispatch failed"
                    );
                    for req in &run_requests {
                        for asset in &req.asset_selection {
                            self.pass.cache.clear_dispatched_run(asset, &req.run_id);
                        }
                        handle.unregister_run(&req.run_id);
                    }
                }
            }
        }

        if !plan.multi_partition_backfills.is_empty() {
            self.dispatch_multi_partition_backfills(plan.multi_partition_backfills, handle)
                .await;
        }
    }

    /// Multi-partition selections become a backfill per asset.
    async fn dispatch_multi_partition_backfills(
        &mut self,
        mats: Vec<(String, Vec<CorePartitionKey>)>,
        handle: &mut ConditionTickHandle,
    ) {
        let backfill_asset_keys: Vec<String> = mats.iter().map(|(k, _)| k.clone()).collect();
        let mut requests: Vec<BackfillRequestData> = Vec::with_capacity(mats.len());
        for (asset_key, partition_keys) in mats {
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
            });
        }

        match self.backfill_dispatcher.dispatch(&requests).await {
            Ok(outcome) => {
                for result in outcome.results {
                    if !result.backfill_id.is_empty() {
                        handle.register_backfill_id(result.backfill_id);
                    }
                }
                for err in outcome.errors {
                    tracing::error!(
                        target: "rivers::daemon",
                        error = %err,
                        "condition backfill dispatch error"
                    );
                }
                for target in &outcome.failed_targets {
                    for asset_key in target {
                        self.pass.cache.clear_predispatch_mark(asset_key);
                    }
                }
            }
            Err(e) => {
                tracing::error!(
                    target: "rivers::daemon",
                    error = %e,
                    "condition backfill dispatch failed"
                );
                for asset_key in &backfill_asset_keys {
                    self.pass.cache.clear_predispatch_mark(asset_key);
                }
            }
        }
    }
}
