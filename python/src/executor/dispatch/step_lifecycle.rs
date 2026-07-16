//! Step lifecycle helper — wraps the variable phase 4 ("run the work") with
//! the invariant prep/finalize phases shared across in-process, async, and
//! parallel-pool backends.
//!
//! Phase shape:
//!   1. (parallel/k8s only — caller's responsibility) IO compatibility check
//!   2. (optional) Pool-claim / semaphore acquisition
//!   3. Emit `StepStart` events
//!   4. Run the work — supplied by the backend via [`SyncWorker`] / [`AsyncWorker`]
//!   5. Release pool guard (after the work completes)
//!   6. (sync flavor only) Route outcome through `process_outcome`. The async
//!      flavor returns the [`WorkOutcome`] so the orchestrator can route it
//!      after the JoinSet collection — `process_outcome` requires `&mut
//!      BatchContext` which isn't available inside spawned tasks.
//!
//! K8s does not use the lifecycle helper: its step pods write events directly
//! and the orchestrator-side path is just "build Job, poll, mark failed."

use std::sync::Arc;

use pyo3::prelude::*;
use rivers_core::execution::plan::ExecutionStep;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{EventRecord, ScopedStorageHandle, StorageBackend};
use tokio::sync::{Semaphore, mpsc};

use crate::errors::ExecutionError;

use rivers_core::execution::retry::{compute_delay, should_retry};

use super::super::ops::{self, now_ts};
use super::context::BatchContext;
use super::failure::{self, classify_pyerr, rng01};
use super::pool_claim::PoolGuard;
use super::results::{emit_captured_logs, process_outcome};
use super::types::WorkOutcome;

/// Phase-4 work supplied by an in-process backend. Runs synchronously with
/// the GIL held; the lifecycle hands `&BatchContext` so the worker can read
/// resolved repo state during invocation. Takes `&self` so the retry loop can
/// re-run the same work for a later attempt.
pub(crate) trait SyncWorker {
    fn run_work(&self, py: Python, ctx: &BatchContext) -> WorkOutcome;
}

/// Phase-4 work supplied by a backend that schedules onto a tokio task
/// (async or parallel-pool). The worker runs inside `spawn_blocking` so it
/// can't borrow from the orchestrator stack — it owns its data. Takes `&self`
/// (shared via `Arc` across attempts) so the retry loop can re-run it.
pub(crate) trait AsyncWorker: Send + Sync + 'static {
    fn run_work(&self) -> WorkOutcome;
}

/// Run the synchronous lifecycle around `worker`. Pool-claim failures use
/// `record_failure_no_hooks` (no failure hooks fire — the step never started).
/// All other phase-4 outcomes route through `process_outcome`.
pub(crate) fn run_step_sync_lifecycle<W: SyncWorker>(
    py: Python,
    ctx: &mut BatchContext,
    step: &ExecutionStep,
    step_name: &str,
    event_names: &[String],
    pools: Vec<(String, u32)>,
    worker: W,
    failures: &mut Vec<(String, PyErr)>,
) {
    let mut guard = if pools.is_empty() {
        None
    } else {
        match PoolGuard::acquire_blocking(
            py,
            ctx.sink.storage,
            &pools,
            ctx.scope.run_id,
            step_name,
            ctx.event_sender(),
        ) {
            Ok(g) => Some(g),
            Err(e) => {
                ctx.record_failure_no_hooks(
                    step_name,
                    ExecutionError::new_err(format!("Failed to claim pool slots: {e}")),
                    failures,
                );
                return;
            }
        }
    };

    for name in event_names {
        ctx.emit_start(name, now_ts());
    }

    let policy = ctx.retry_policy_for(step);
    let mut attempt: u32 = 1;
    if ctx.scope.resume
        && policy.is_some()
        && let Some(first) = event_names.first()
    {
        attempt += failure::prior_step_retries_blocking(
            py,
            ctx.sink.storage.backend(),
            ctx.scope.run_id,
            first,
        );
    }
    let outcome = loop {
        let outcome = worker.run_work(py, ctx);
        let WorkOutcome::Error {
            error,
            captured_logs,
            failure_config,
        } = outcome
        else {
            break outcome;
        };
        let Some(policy) = &policy else {
            break WorkOutcome::Error {
                error,
                captured_logs,
                failure_config,
            };
        };
        let (reason, exc_types) = classify_pyerr(py, &error);
        if !should_retry(policy, reason, &exc_types, attempt) {
            break WorkOutcome::Error {
                error,
                captured_logs,
                failure_config,
            };
        }
        emit_captured_logs(ctx, step_name, captured_logs);
        let delay = compute_delay(policy, attempt, rng01());
        for name in event_names {
            ctx.emit_step_retry(name, attempt, reason, delay);
        }
        tracing::info!(
            step = step_name,
            attempt,
            reason = reason.as_str(),
            delay_ms = delay.as_millis() as u64,
            "step failed, retrying"
        );
        if !delay.is_zero() {
            // A sleeping step must not hold pool slots (it would starve
            // siblings and other runs), and cancellation cuts the wait short.
            if let Some(g) = guard.take() {
                g.release_blocking(py);
            }
            if failure::backoff_sleep_cancellable_blocking(
                py,
                ctx.sink.storage.backend(),
                ctx.scope.run_id,
                delay,
            ) {
                break WorkOutcome::Error {
                    error,
                    captured_logs: None,
                    failure_config,
                };
            }
            if !pools.is_empty() {
                match PoolGuard::acquire_blocking(
                    py,
                    ctx.sink.storage,
                    &pools,
                    ctx.scope.run_id,
                    step_name,
                    ctx.event_sender(),
                ) {
                    Ok(g) => guard = Some(g),
                    Err(e) => {
                        break WorkOutcome::Error {
                            error: ExecutionError::new_err(format!(
                                "Failed to re-claim pool slots for retry: {e}"
                            )),
                            captured_logs: None,
                            failure_config,
                        };
                    }
                }
            }
        }
        // Zero-backoff ladders have no sleep to interrupt — probe once per
        // attempt so a cancelled run stops instead of burning the budget.
        if failure::run_cancelled_blocking(py, ctx.sink.storage.backend(), ctx.scope.run_id) {
            break WorkOutcome::Error {
                error,
                captured_logs: None,
                failure_config,
            };
        }
        attempt += 1;
        let ts = now_ts();
        for name in event_names {
            ctx.emit_start(name, ts);
        }
    };

    if let Some(g) = guard {
        g.release_blocking(py);
    }

    process_outcome(py, ctx, step, step_name, event_names, outcome, failures);
}

/// Run the asynchronous lifecycle around `worker`. The lifecycle returns a
/// [`WorkOutcome`]; the orchestrator collects outcomes from its JoinSet and
/// routes each through `process_outcome` against the live `&mut BatchContext`.
///
/// Pool-claim failures and `spawn_blocking` panics are returned as
/// `WorkOutcome::Error` (no captured logs, no resolved config) — when routed
/// through `process_outcome` they go through `handle_failure` and run failure
/// hooks. (Sync lifecycle's pool-claim path is no-hooks; this difference
/// preserves long-standing per-backend behavior.)
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_step_async_lifecycle<W: AsyncWorker>(
    storage: ScopedStorageHandle<SurrealStorage>,
    pools: Vec<(String, u32)>,
    run_id: String,
    pool_step_name: String,
    start_event_names: Vec<String>,
    events_tx: mpsc::UnboundedSender<EventRecord>,
    semaphore: Option<Arc<Semaphore>>,
    retry_policy: Option<rivers_core::execution::retry::RetryPolicy>,
    resume: bool,
    worker: W,
) -> WorkOutcome {
    let mut permit = match &semaphore {
        Some(sem) => Some(
            sem.clone()
                .acquire_owned()
                .await
                .expect("semaphore not closed"),
        ),
        None => None,
    };

    let mut guard = if pools.is_empty() {
        None
    } else {
        match PoolGuard::acquire(
            &storage,
            &pools,
            &run_id,
            &pool_step_name,
            events_tx.clone(),
        )
        .await
        {
            Ok(g) => Some(g),
            Err(e) => {
                return prep_error(format!("Failed to claim pool slots: {e}"));
            }
        }
    };

    let start_ts = now_ts();
    let code_location_id = storage.code_location_id().to_string();
    for name in &start_event_names {
        ops::emit_step_start_via_tx(&events_tx, &code_location_id, &run_id, name, start_ts);
    }

    let worker = Arc::new(worker);
    let mut attempt: u32 = 1;
    if resume
        && retry_policy.is_some()
        && let Some(first) = start_event_names.first()
    {
        attempt += failure::prior_step_retries(storage.backend(), &run_id, first).await;
    }
    let outcome = loop {
        let w = Arc::clone(&worker);
        // Classification needs the GIL; attach on the blocking thread, not here.
        let joined = tokio::task::spawn_blocking(move || {
            let outcome = w.run_work();
            let classified = match &outcome {
                WorkOutcome::Error { error, .. } => {
                    Python::try_attach(|py| classify_pyerr(py, error))
                }
                _ => None,
            };
            (outcome, classified)
        })
        .await;
        let (mut outcome, classified) = match joined {
            Ok(pair) => pair,
            Err(e) => break prep_error(format!("spawn_blocking join error: {e}")),
        };
        if !matches!(outcome, WorkOutcome::Error { .. }) {
            break outcome;
        }
        let (Some(policy), Some((reason, exc_types))) = (&retry_policy, classified.as_ref()) else {
            break outcome;
        };
        if !should_retry(policy, *reason, exc_types, attempt) {
            break outcome;
        }
        // Flush the failed attempt's logs now (taken out so a cancelled
        // backoff below can't re-emit them with the returned outcome).
        if let WorkOutcome::Error { captured_logs, .. } = &mut outcome
            && let Some((stdout, stderr, logs)) = captured_logs.take()
        {
            ops::emit_log_output_via_tx(
                &events_tx,
                &code_location_id,
                &run_id,
                &pool_step_name,
                &stdout,
                &stderr,
                &logs,
                now_ts(),
            );
        }
        let delay = compute_delay(policy, attempt, rng01());
        for name in &start_event_names {
            ops::emit_step_retry_via_tx(
                &events_tx,
                &code_location_id,
                &run_id,
                name,
                attempt,
                *reason,
                delay,
                now_ts(),
            );
        }
        tracing::info!(
            step = %pool_step_name,
            attempt,
            reason = reason.as_str(),
            delay_ms = delay.as_millis() as u64,
            "step failed, retrying"
        );
        if !delay.is_zero() {
            // A sleeping step must not hold its concurrency permit or pool
            // slots, and cancellation cuts the wait short.
            if let Some(g) = guard.take() {
                g.release().await;
            }
            drop(permit.take());
            if failure::backoff_sleep_cancellable(storage.backend(), &run_id, delay).await {
                break outcome;
            }
            if let Some(sem) = &semaphore {
                permit = Some(
                    sem.clone()
                        .acquire_owned()
                        .await
                        .expect("semaphore not closed"),
                );
            }
            if !pools.is_empty() {
                match PoolGuard::acquire(
                    &storage,
                    &pools,
                    &run_id,
                    &pool_step_name,
                    events_tx.clone(),
                )
                .await
                {
                    Ok(g) => guard = Some(g),
                    Err(e) => {
                        break prep_error(format!("Failed to re-claim pool slots for retry: {e}"));
                    }
                }
            }
        }
        // Zero-backoff ladders have no sleep to interrupt — probe once per
        // attempt so a cancelled run stops instead of burning the budget.
        if storage
            .backend()
            .is_cancelled(&run_id)
            .await
            .unwrap_or(false)
        {
            break outcome;
        }
        attempt += 1;
        let ts = now_ts();
        for name in &start_event_names {
            ops::emit_step_start_via_tx(&events_tx, &code_location_id, &run_id, name, ts);
        }
    };

    drop(permit);
    if let Some(g) = guard {
        g.release().await;
    }

    outcome
}

fn prep_error(msg: String) -> WorkOutcome {
    WorkOutcome::Error {
        error: ExecutionError::new_err(msg),
        captured_logs: None,
        failure_config: None,
    }
}
