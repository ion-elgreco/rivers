//! Materialization fan-out for fired conditions.
//!
//! `dispatch_materializations` consumes a `MaterializationPlan` (already
//! classified into the three shapes by `ConditionPass::classify`) and
//! dispatches each shape through a shared dispatcher seam:
//!
//! * Run shapes (unpartitioned bulk + single-partition group) become
//!   `MaterializationRequestData` and go through `RunDispatcherKind`.
//!   The engine mints the run_id up front so it can register with
//!   `cache.register_dispatched_run` for phantom tracking before dispatch.
//! * Multi-partition backfills become `BackfillRequestData` and go through
//!   `BackfillDispatcherKind`.
//!
//! Both dispatcher seams are shared with the schedule/sensor loop —
//! schedule/sensor uses `RunDispatcher::dispatch` (job-resolved requests),
//! condition uses `RunDispatcher::dispatch_materialization` (asset
//! selection with caller-minted run_id), but the underlying Direct/Queued
//! mode logic is centralized in `dispatchers.rs`.
use std::collections::HashMap;

use rivers_core::condition::MaterializationPlan;
use rivers_core::storage::{LaunchedBy, PartitionKey as CorePartitionKey};

use super::engine::ConditionTickEngine;
use super::persist::ConditionTickHandle;
use crate::daemon::types::{BackfillRequestData, MaterializationRequestData};
use crate::partitions::PyPartitionKey;
use crate::repository::tag_keys;

impl ConditionTickEngine {
    /// Dispatch materializations for the assets whose conditions fired this
    /// tick. Run shapes are batched into a single
    /// `RunDispatcher::dispatch_materialization` call; multi-partition
    /// backfills are batched into a `BackfillDispatcher::dispatch` call.
    /// Each minted run_id is registered with the cache (for phantom
    /// tracking) and the handle (for the global tick record).
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
                    .register_dispatched_run(asset.clone(), run_id.clone(), now);
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
                self.pass
                    .cache
                    .register_dispatched_run(asset.clone(), run_id.clone(), now);
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
                }
                Err(e) => {
                    tracing::error!(
                        target: "rivers::daemon",
                        error = %e,
                        "condition run dispatch failed"
                    );
                }
            }
        }

        if !plan.multi_partition_backfills.is_empty() {
            self.dispatch_multi_partition_backfills(plan.multi_partition_backfills, handle)
                .await;
        }
    }

    /// Multi-partition selections become a backfill per asset. Builds a
    /// `BackfillRequestData` per `(asset, partition_keys)` pair and dispatches
    /// the batch through the shared [`BackfillDispatcherKind`] — same path
    /// the schedule/sensor loop uses. The dispatcher spawns one OS thread
    /// per request and awaits all resulting oneshots before returning the
    /// per-request `backfill_id`s.
    ///
    /// Tick.run_ids is deliberately NOT updated with the backfill's sub-runs
    /// (those are an impl detail of the backfill); only the backfill_id is
    /// registered on the global tick handle.
    async fn dispatch_multi_partition_backfills(
        &mut self,
        mats: Vec<(String, Vec<CorePartitionKey>)>,
        handle: &mut ConditionTickHandle,
    ) {
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
            }
            Err(e) => {
                tracing::error!(
                    target: "rivers::daemon",
                    error = %e,
                    "condition backfill dispatch failed"
                );
            }
        }
    }
}
