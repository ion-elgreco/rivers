//! Per-tick orchestration of automation conditions on the Python side.
use std::sync::Arc;

use rivers_core::condition::{ConditionPass, EvalResultRow};
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{ConditionEvalRecord, ScopedStorageHandle, TickRecord};

use crate::daemon::dispatchers::{BackfillDispatcherKind, RunDispatcherKind};
use crate::daemon::types::{ConditionEvalWriteMsg, TickWriteMsg};

pub(super) struct ConditionTickEngine {
    pub(super) pass: ConditionPass,

    pub(super) code_location_id: String,
    pub(super) storage: ScopedStorageHandle<SurrealStorage>,
    /// Shared with the schedule/sensor path.
    pub(super) run_dispatcher: Arc<RunDispatcherKind>,
    pub(super) backfill_dispatcher: Arc<BackfillDispatcherKind>,
    pub(super) tick_tx: tokio::sync::mpsc::UnboundedSender<TickWriteMsg>,
    pub(super) eval_tx: tokio::sync::mpsc::UnboundedSender<ConditionEvalWriteMsg>,
    pub(super) max_ticks_retained: Option<usize>,
    pub(super) max_evals_retained: Option<usize>,
}

impl ConditionTickEngine {
    /// Run one tick: refresh cache, evaluate, dispatch materializations, persist state.
    pub(super) async fn tick(&mut self, now: i64) {
        let has_changes = match self
            .pass
            .refresh_cache(self.storage.backend().as_ref(), now)
            .await
        {
            Ok(changed) => changed,
            Err(e) => {
                tracing::error!(target: "rivers::daemon", error = %e, "condition cache refresh failed");
                return;
            }
        };

        let mut dynamic_keys = std::collections::HashMap::new();
        for ns in self.pass.dynamic_universe_namespaces() {
            match self.storage.scoped().get_dynamic_partitions(&ns).await {
                Ok(keys) => {
                    dynamic_keys.insert(ns, keys.into_iter().collect());
                }
                Err(e) => {
                    tracing::warn!(
                        target: "rivers::daemon",
                        namespace = %ns,
                        error = %e,
                        "dynamic partition universe refresh failed"
                    );
                }
            }
        }
        let universe_changed = self
            .pass
            .refresh_partition_universes(chrono::Local::now().naive_local(), &dynamic_keys);
        let has_changes = has_changes || universe_changed;

        tracing::trace!(
            target: "rivers::dbg::cond",
            has_changes,
            is_initial = self.pass.eval_state.is_initial,
            "tick: post-refresh"
        );
        self.pass.ensure_time_based_eval_set();
        if self.pass.should_skip(has_changes) {
            tracing::trace!(
                target: "rivers::dbg::cond",
                has_changes,
                "tick: SKIPPED"
            );
            return;
        }

        let selective =
            !has_changes && self.pass.has_time_based && self.pass.time_based_eval_set.is_some();
        let output = self.pass.run(now, selective);

        if tracing::enabled!(target: "rivers::dbg::cond", tracing::Level::TRACE) {
            let mut fired: Vec<&str> = Vec::new();
            let mut not_fired: Vec<&str> = Vec::new();
            for r in &output.results {
                let key = self.pass.conditions[r.info_idx].asset_key.as_str();
                if r.result.fired {
                    fired.push(key);
                } else {
                    not_fired.push(key);
                }
            }
            tracing::trace!(
                target: "rivers::dbg::cond",
                has_changes,
                ?fired,
                ?not_fired,
                plan_unpartitioned = ?output.plan.unpartitioned,
                "tick: RAN"
            );
        }

        let placeholder_keys = output
            .plan
            .unpartitioned
            .iter()
            .cloned()
            .chain(
                output
                    .plan
                    .single_partition_groups
                    .values()
                    .flat_map(|assets| assets.iter().cloned()),
            )
            .chain(
                output
                    .plan
                    .multi_partition_backfills
                    .iter()
                    .map(|(asset_key, _)| asset_key.clone()),
            );
        for asset_key in placeholder_keys {
            let _ = self.tick_tx.send(TickWriteMsg {
                record: TickRecord {
                    code_location_id: self.code_location_id.clone(),
                    automation_name: asset_key,
                    automation_type: "AutomationCondition".into(),
                    status: "Requested".into(),
                    timestamp: now,
                    run_ids: vec![],
                    backfill_ids: vec![],
                    skip_reason: None,
                    error: None,
                    cursor: None,
                },
                max_ticks_retained: self.max_ticks_retained,
            });
        }

        if !output.results.is_empty() {
            let mut handle = super::persist::ConditionTickHandle::new(
                self.code_location_id.clone(),
                now,
                &output.results,
            );
            self.dispatch_materializations(output.plan, &mut handle)
                .await;
            let tick_id = handle.finalize(&self.storage).await;
            self.send_eval_records(&output.results, now, &tick_id);
        }

        if let Err(e) = self
            .storage
            .scoped()
            .set_condition_eval_state(&self.pass.eval_state)
            .await
        {
            tracing::warn!(
                target: "rivers::daemon",
                error = %e,
                "failed to persist condition eval state; latches reset on restart"
            );
        }
    }

    /// Send per-asset `ConditionEvalRecord`s referencing the already-persisted global `tick_id`.
    pub(super) fn send_eval_records(&self, results: &[EvalResultRow], now: i64, tick_id: &str) {
        let mut eval_records = Vec::with_capacity(results.len());
        for row in results {
            let info = &self.pass.conditions[row.info_idx];
            match serde_json::to_vec(&row.tree) {
                Ok(tree_json) => {
                    let selection_json = row
                        .result
                        .selection
                        .as_ref()
                        .and_then(|sel| serde_json::to_vec(sel).ok());
                    eval_records.push(ConditionEvalRecord {
                        code_location_id: self.code_location_id.clone(),
                        asset_key: info.asset_key.clone(),
                        tick_id: tick_id.to_string(),
                        timestamp: now,
                        fired: row.result.fired,
                        eval_duration_us: row.duration_us,
                        run_ids: vec![],
                        tree_json,
                        selection_json,
                    });
                }
                Err(e) => tracing::warn!(
                    target: "rivers::daemon",
                    asset = %info.asset_key,
                    error = %e,
                    "failed to serialize condition eval tree; skipping eval record"
                ),
            }
        }
        if !eval_records.is_empty() {
            let _ = self.eval_tx.send(ConditionEvalWriteMsg {
                evals: eval_records,
                max_evals_retained: self.max_evals_retained,
            });
        }
    }
}
