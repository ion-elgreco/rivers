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
            let (run_ids, mut dispatch_errors) = log_dispatch_outcome(
                tick_outcome,
                &auto_name,
                auto_type,
                run_dispatcher.mode_label(),
                "run",
            );

            let backfill_ids = if backfill_requests.is_empty() {
                vec![]
            } else {
                // Same outcome shape as runs: successful non-dry-run ids +
                // per-request errors (a dry run produces no record).
                let bf_outcome = backfill_dispatcher.dispatch(backfill_requests).await.map(
                    |BackfillDispatchOutcome {
                         results,
                         errors,
                         failed_targets: _,
                     }| DispatchOutcome {
                        ids: results
                            .into_iter()
                            .filter(|r| !r.is_dry_run && !r.backfill_id.is_empty())
                            .map(|r| r.backfill_id)
                            .collect(),
                        errors,
                    },
                );
                let (ids, errors) = log_dispatch_outcome(
                    bf_outcome,
                    &auto_name,
                    auto_type,
                    backfill_dispatcher.mode_label(),
                    "backfill",
                );
                dispatch_errors.extend(errors);
                ids
            };

            // A dropped request is a failed tick — silence here means a run
            // the automation intended simply never exists.
            let (status, error) = if dispatch_errors.is_empty() {
                ("Success".to_string(), None)
            } else {
                ("Failed".to_string(), Some(dispatch_errors.join("; ")))
            };
            let failed = error.is_some();
            let _ = tick_tx.send(TickWriteMsg {
                record: TickRecord {
                    code_location_id: handle.code_location_id().to_string(),
                    automation_name: auto_name.clone(),
                    automation_type: auto_type.into(),
                    status,
                    timestamp,
                    run_ids,
                    backfill_ids,
                    skip_reason: None,
                    error,
                    cursor: final_cursor,
                },
                max_ticks_retained,
            });
            entry.complete_eval(outcome);
            if !failed {
                tracing::info!(
                    target: "rivers::daemon",
                    automation_type = auto_type,
                    name = %auto_name,
                    mode = run_dispatcher.mode_label(),
                    "tick succeeded"
                );
            }
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

/// Unwrap a `DispatchOutcome`, logging any per-request errors and outer
/// failures uniformly. Returns the successful ids and the error messages —
/// a dropped request must surface on the tick record, not just in the logs.
fn log_dispatch_outcome(
    outcome: anyhow::Result<DispatchOutcome>,
    auto_name: &str,
    auto_type: &str,
    mode_label: &'static str,
    kind: &'static str,
) -> (Vec<String>, Vec<String>) {
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
            let messages = errors.iter().map(|e| e.to_string()).collect();
            (ids, messages)
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
            (vec![], vec![e.to_string()])
        }
    }
}
