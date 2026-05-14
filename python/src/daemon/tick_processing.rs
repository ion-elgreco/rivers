//! Tick result processing — given a completed `TickResult` from the
//! schedule/sensor loop, dispatches any run/backfill requests via the
//! configured dispatchers, advances the corresponding `AutomationEntry`'s
//! state, and forwards a `TickRecord` to the background tick writer.
use std::sync::Arc;

use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{ScopedStorageHandle, TickRecord};

use super::automation_entry::AutomationEntry;
use super::dispatchers::{
    BackfillDispatchOutcome, BackfillDispatcherKind, DispatchOutcome, RunDispatcherKind,
};
use super::types::{EvalOutcome, TickResult, TickWriteMsg};
use crate::executor::ops::now_ts;

pub(crate) async fn process_tick_result(
    automations: &mut [AutomationEntry],
    tick_tx: &tokio::sync::mpsc::UnboundedSender<TickWriteMsg>,
    handle: &ScopedStorageHandle<SurrealStorage>,
    run_dispatcher: &Arc<RunDispatcherKind>,
    backfill_dispatcher: &Arc<BackfillDispatcherKind>,
    tick_result: TickResult,
    max_ticks_retained: Option<usize>,
) {
    let TickResult {
        index,
        result,
        prev_cursor,
        dispatched_at: _,
    } = tick_result;

    let entry = &mut automations[index];
    let auto_name = entry.name().to_string();
    let auto_type = entry.automation_type_str();
    let launched_by = entry.launched_by();
    let timestamp = now_ts();

    match result {
        Ok(
            ref outcome @ EvalOutcome::RunRequests {
                ref run_requests,
                ref materialization_requests,
                ref backfill_requests,
                ..
            },
        ) => {
            let final_cursor = outcome.cursor_or(prev_cursor);

            let tick_outcome = run_dispatcher
                .dispatch_tick(run_requests, materialization_requests, launched_by.clone())
                .await;
            let run_ids = log_dispatch_outcome(
                tick_outcome,
                &auto_name,
                auto_type,
                run_dispatcher.mode_label(),
                "run",
            );

            let backfill_ids = if backfill_requests.is_empty() {
                vec![]
            } else {
                let bf_outcome = backfill_dispatcher.dispatch(backfill_requests).await;
                log_backfill_dispatch_outcome(
                    bf_outcome,
                    &auto_name,
                    auto_type,
                    backfill_dispatcher.mode_label(),
                )
            };

            let _ = tick_tx.send(TickWriteMsg {
                record: TickRecord {
                    code_location_id: handle.code_location_id().to_string(),
                    automation_name: auto_name.clone(),
                    automation_type: auto_type.into(),
                    status: "Success".into(),
                    timestamp,
                    run_ids,
                    backfill_ids,
                    skip_reason: None,
                    error: None,
                    cursor: final_cursor,
                },
                max_ticks_retained,
            });
            entry.complete_eval(outcome);
            tracing::info!(
                target: "rivers::daemon",
                automation_type = auto_type,
                name = %auto_name,
                mode = run_dispatcher.mode_label(),
                "tick succeeded"
            );
        }
        Ok(ref outcome @ EvalOutcome::Skipped { ref reason, .. }) => {
            let final_cursor = outcome.cursor_or(prev_cursor);
            let _ = tick_tx.send(TickWriteMsg {
                record: TickRecord {
                    code_location_id: handle.code_location_id().to_string(),
                    automation_name: auto_name.clone(),
                    automation_type: auto_type.into(),
                    status: "Skipped".into(),
                    timestamp,
                    run_ids: vec![],
                    backfill_ids: vec![],
                    skip_reason: Some(reason.clone()),
                    error: None,
                    cursor: final_cursor,
                },
                max_ticks_retained,
            });
            entry.complete_eval(outcome);
            tracing::debug!(
                target: "rivers::daemon",
                automation_type = auto_type,
                name = %auto_name,
                reason = %reason,
                "tick skipped"
            );
        }
        Err(e) => {
            let _ = tick_tx.send(TickWriteMsg {
                record: TickRecord {
                    code_location_id: handle.code_location_id().to_string(),
                    automation_name: auto_name.clone(),
                    automation_type: auto_type.into(),
                    status: "Failed".into(),
                    timestamp,
                    run_ids: vec![],
                    backfill_ids: vec![],
                    skip_reason: None,
                    error: Some(e.clone()),
                    cursor: prev_cursor,
                },
                max_ticks_retained,
            });
            entry.complete_eval_on_error();
            tracing::error!(
                target: "rivers::daemon",
                automation_type = auto_type,
                name = %auto_name,
                error = %e,
                "tick failed"
            );
        }
    }
}

/// Unwrap a `DispatchOutcome`, logging any per-request errors and outer failures
/// uniformly. Returns the successful ids; preserves today's behavior of "tick
/// status stays Success even if some requests failed."
fn log_dispatch_outcome(
    outcome: anyhow::Result<DispatchOutcome>,
    auto_name: &str,
    auto_type: &str,
    mode_label: &'static str,
    kind: &'static str,
) -> Vec<String> {
    match outcome {
        Ok(DispatchOutcome { ids, errors }) => {
            for err in &errors {
                tracing::error!(
                    target: "rivers::executor",
                    automation_type = auto_type,
                    name = %auto_name,
                    mode = mode_label,
                    kind = kind,
                    error = %err,
                    "{} request failed",
                    kind
                );
            }
            ids
        }
        Err(e) => {
            tracing::error!(
                target: "rivers::executor",
                automation_type = auto_type,
                name = %auto_name,
                mode = mode_label,
                kind = kind,
                error = %e,
                "{} dispatch failed",
                kind
            );
            vec![]
        }
    }
}

/// Backfill counterpart of [`log_dispatch_outcome`]. The dispatcher
/// returns rich `PyBackfillResult`s; daemon ticks only need the ids
/// (and a successful dispatch with `is_dry_run=true` is filtered out
/// since it produces no record).
fn log_backfill_dispatch_outcome(
    outcome: anyhow::Result<BackfillDispatchOutcome>,
    auto_name: &str,
    auto_type: &str,
    mode_label: &'static str,
) -> Vec<String> {
    match outcome {
        Ok(BackfillDispatchOutcome { results, errors }) => {
            for err in &errors {
                tracing::error!(
                    target: "rivers::executor",
                    automation_type = auto_type,
                    name = %auto_name,
                    mode = mode_label,
                    kind = "backfill",
                    error = %err,
                    "backfill request failed",
                );
            }
            results
                .into_iter()
                .filter(|r| !r.is_dry_run && !r.backfill_id.is_empty())
                .map(|r| r.backfill_id)
                .collect()
        }
        Err(e) => {
            tracing::error!(
                target: "rivers::executor",
                automation_type = auto_type,
                name = %auto_name,
                mode = mode_label,
                kind = "backfill",
                error = %e,
                "backfill dispatch failed",
            );
            vec![]
        }
    }
}
