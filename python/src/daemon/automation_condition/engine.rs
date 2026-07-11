//! Per-tick orchestration of automation conditions on the Python side.
use std::sync::Arc;

use rivers_core::condition::{ConditionPass, EvalResultRow, PendingDispatch, PendingDispatchEntry};
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

        let selective = !has_changes
            && !self.pass.needs_retry
            && self.pass.has_time_based
            && self.pass.time_based_eval_set.is_some();
        let output = self.pass.plan_tick(now, selective);

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

        let mut dispatch_failed: std::collections::HashSet<String> = Default::default();
        let mut intent_written = false;
        if !output.results.is_empty() {
            let mut handle = super::persist::ConditionTickHandle::new(
                self.code_location_id.clone(),
                now,
                &output.results,
            );
            let run_requests = self.prepare_run_requests(&output.plan, &mut handle);
            // Persist the dispatch intent BEFORE anything goes out: a crash
            // between dispatch and the eval-state persist below would
            // otherwise replay the tick's consumed latches on restart and
            // double-materialize.
            let scalar_states = self.pass.pending_dispatch_states(&output, now);
            if !scalar_states.is_empty() {
                let mut run_ids_by_asset: std::collections::HashMap<String, Vec<String>> =
                    Default::default();
                for req in &run_requests {
                    for asset in &req.asset_selection {
                        run_ids_by_asset
                            .entry(asset.clone())
                            .or_default()
                            .push(req.run_id.clone());
                    }
                }
                let pending = PendingDispatch {
                    tick_timestamp: now,
                    entries: scalar_states
                        .into_iter()
                        .map(|(asset_key, committed)| PendingDispatchEntry {
                            run_ids: run_ids_by_asset.remove(&asset_key).unwrap_or_default(),
                            asset_key,
                            committed,
                        })
                        .collect(),
                };
                match self
                    .storage
                    .scoped()
                    .set_condition_pending_dispatch(&pending)
                    .await
                {
                    Ok(()) => intent_written = true,
                    Err(e) => tracing::warn!(
                        target: "rivers::daemon",
                        error = %e,
                        "failed to persist dispatch intent; a crash before the eval-state persist may double-fire"
                    ),
                }
            }
            self.dispatch_materializations(output.plan.clone(), run_requests, &mut handle)
                .await;
            // Per-asset tick history derives from the dispatch OUTCOME — a
            // pre-written "Requested" row can't show a failed or dropped
            // dispatch.
            for (asset_key, outcome) in handle.outcomes() {
                if outcome.error.is_some() {
                    dispatch_failed.insert(asset_key.clone());
                }
                let _ = self.tick_tx.send(TickWriteMsg {
                    record: TickRecord {
                        code_location_id: self.code_location_id.clone(),
                        automation_name: asset_key.clone(),
                        automation_type: "AutomationCondition".into(),
                        status: if outcome.error.is_some() {
                            "Failed".into()
                        } else {
                            "Requested".into()
                        },
                        timestamp: now,
                        run_ids: outcome.run_ids.clone(),
                        backfill_ids: outcome.backfill_ids.clone(),
                        skip_reason: None,
                        error: outcome.error.clone(),
                        cursor: None,
                    },
                    max_ticks_retained: self.max_ticks_retained,
                });
            }
            let tick_id = handle.finalize(&self.storage).await;
            self.send_eval_records(&output.results, now, &tick_id);
        }
        // Latches advance only for assets whose dispatch went out; failed
        // ones stay armed and force a retry evaluation next tick.
        self.pass.commit_tick(&output, &dispatch_failed, now);

        match self
            .storage
            .scoped()
            .set_condition_eval_state(&self.pass.eval_state)
            .await
        {
            Ok(()) => {
                if intent_written {
                    // Cleared only after the state landed — the intent is the
                    // crash guard for the window in between.
                    if let Err(e) = self
                        .storage
                        .scoped()
                        .set_condition_pending_dispatch(&PendingDispatch::default())
                        .await
                    {
                        tracing::warn!(
                            target: "rivers::daemon",
                            error = %e,
                            "failed to clear dispatch intent; restart re-runs an idempotent recovery"
                        );
                    }
                }
            }
            Err(e) => tracing::error!(
                target: "rivers::daemon",
                error = %e,
                "failed to persist condition eval state; a restart replays this tick's latches"
            ),
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
