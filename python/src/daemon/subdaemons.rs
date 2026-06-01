use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use pyo3::prelude::*;
use rivers_core::run_backend::RunHealthStatus;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{RunStatus, ScopedStorageHandle, StorageBackend};
use tokio_util::sync::CancellationToken;

use super::RunBackendKind;
use super::automation_entry::AutomationEntry;
use super::dispatchers::{BackfillDispatcherKind, RunDispatcherKind};
use super::eval_dispatcher::{DueEval, EvalDispatcher};
use super::tick_processing::process_tick_result;
use super::types::{TickResult, TickWriteMsg};
use crate::executor::ops::now_ts;
use crate::gil_threads::GilThreads;
use crate::repository::PyCodeRepository;

/// Backfill pickup loop — polls for Requested backfills owned by this CL every
/// 5s and executes them. Scoped per CL so two daemons sharing a SurrealDB
/// don't race on each other's backfills.
pub(crate) fn spawn_backfill_pickup_loop(
    repo: Arc<Py<PyCodeRepository>>,
    handle: ScopedStorageHandle<SurrealStorage>,
    cancel: CancellationToken,
    run_queue_enabled: bool,
    gil_threads: GilThreads,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let interval = std::time::Duration::from_secs(5);
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(interval) => {}
            }
            let pending = match handle
                .scoped()
                .get_backfills(
                    Some(10),
                    Some(rivers_core::storage::BackfillStatus::Requested),
                )
                .await
            {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!(
                        target: "rivers::daemon",
                        error = %e,
                        "failed to query pending backfills"
                    );
                    continue;
                }
            };
            for record in pending {
                let repo = repo.clone();
                let backfill_id = record.backfill_id.clone();
                gil_threads.spawn(move || {
                    let repo_ref = repo.get();
                    if run_queue_enabled {
                        if let Err(e) = repo_ref.execute_backfill_queued_inner(&backfill_id) {
                            tracing::error!(
                                target: "rivers::daemon",
                                backfill_id = %backfill_id,
                                error = %e,
                                "backfill queued execution failed"
                            );
                        }
                    } else if let Err(e) = repo_ref.execute_backfill_inner(&backfill_id, None) {
                        tracing::error!(
                            target: "rivers::daemon",
                            backfill_id = %backfill_id,
                            error = %e,
                            "backfill execution failed"
                        );
                    }
                });
            }
        }
    })
}

/// Run queue coordinator — dequeues runs and launches them, with periodic health checks.
pub(crate) fn spawn_run_queue_coordinator(
    rq_config: rivers_core::concurrency::RunQueueConfig,
    handle: ScopedStorageHandle<SurrealStorage>,
    run_backend: Arc<RunBackendKind>,
    repo: Arc<Py<PyCodeRepository>>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let dequeue_interval = rq_config.dequeue_interval;
    let storage = Arc::clone(handle.backend());
    let coordinator = rivers_core::concurrency::RunQueueCoordinator::new(rq_config, handle);
    let is_k8s = matches!(*run_backend, RunBackendKind::Kubernetes(_));

    tracing::info!(
        target: "rivers::daemon",
        interval = ?dequeue_interval,
        max_concurrent_runs = coordinator.config().max_concurrent_runs,
        backend = if is_k8s { "kubernetes" } else { "local" },
        "starting run queue coordinator"
    );
    tokio::spawn(async move {
        let interval = dequeue_interval;
        let health_interval = Duration::from_secs(5);
        let mut active_runs: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut last_health_check = tokio::time::Instant::now();

        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(interval) => {}
            }

            if last_health_check.elapsed() >= health_interval && !active_runs.is_empty() {
                let mut checks = tokio::task::JoinSet::new();
                for run_id in active_runs.iter().cloned() {
                    let backend = Arc::clone(&run_backend);
                    checks.spawn(async move {
                        let health = backend.check_run_health(&run_id).await;
                        (run_id, health)
                    });
                }
                while let Some(joined) = checks.join_next().await {
                    let Ok((run_id, health)) = joined else {
                        continue;
                    };
                    match health {
                        Ok(RunHealthStatus::Exited) => {
                            tracing::info!(
                                target: "rivers::coordinator",
                                run_id = %run_id,
                                "run exited (detected via health check)"
                            );
                            active_runs.remove(&run_id);
                        }
                        Ok(RunHealthStatus::Missing) => {
                            if let Ok(Some(_)) = storage.get_run_outcome(&run_id).await {
                                tracing::info!(
                                    target: "rivers::coordinator",
                                    run_id = %run_id,
                                    "run CR missing but outcome already recorded — skipping"
                                );
                            } else {
                                tracing::warn!(
                                    target: "rivers::coordinator",
                                    run_id = %run_id,
                                    "run missing — marking as canceled"
                                );
                                let _ = storage
                                    .update_run_status(&run_id, RunStatus::Canceled, Some(now_ts()))
                                    .await;
                            }
                            active_runs.remove(&run_id);
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::warn!(
                                target: "rivers::coordinator",
                                run_id = %run_id,
                                error = %e,
                                "health check failed"
                            );
                        }
                    }
                }
                last_health_check = tokio::time::Instant::now();
            }

            match coordinator.tick().await {
                Ok(launched) => {
                    for run in &launched {
                        if let Err(e) = run_backend.launch(run, &repo).await {
                            tracing::error!(
                                target: "rivers::coordinator",
                                run_id = %run.run_id,
                                error = %e,
                                "failed to launch dequeued run"
                            );
                        } else {
                            active_runs.insert(run.run_id.clone());
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        target: "rivers::coordinator",
                        error = %e,
                        "run queue coordinator tick failed"
                    );
                }
            }
        }
    })
}

/// Backfill monitor — polls for InProgress backfills owned by this CL every
/// 5s and finalizes them when all their runs reach a terminal state.
pub(crate) fn spawn_backfill_monitor(
    handle: ScopedStorageHandle<SurrealStorage>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let interval = Duration::from_secs(5);
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(interval) => {}
            }

            let backfills = match handle
                .scoped()
                .get_backfills(None, Some(rivers_core::storage::BackfillStatus::InProgress))
                .await
            {
                Ok(bfs) => bfs,
                Err(e) => {
                    tracing::warn!(
                        target: "rivers::backfill_monitor",
                        error = %e,
                        "failed to query in-progress backfills"
                    );
                    continue;
                }
            };

            let mut completions = tokio::task::JoinSet::new();
            for bf in backfills {
                let backend = Arc::clone(handle.backend());
                completions.spawn(async move {
                    let result = backend.try_complete_backfill(&bf.backfill_id).await;
                    (bf.backfill_id, result)
                });
            }
            while let Some(joined) = completions.join_next().await {
                let Ok((backfill_id, result)) = joined else {
                    continue;
                };
                match result {
                    Ok(Some(status)) => {
                        tracing::info!(
                            target: "rivers::backfill_monitor",
                            backfill_id = %backfill_id,
                            status = ?status,
                            "backfill completed"
                        );
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(
                            target: "rivers::backfill_monitor",
                            backfill_id = %backfill_id,
                            error = %e,
                            "failed to check backfill completion"
                        );
                    }
                }
            }
        }
    })
}

/// Schedule & sensor dispatch loop — evaluates due automations, dispatches evals,
/// and processes results on a continuous tick cycle.
pub(crate) fn spawn_schedule_sensor_loop(
    mut automations: Vec<AutomationEntry>,
    tick_tx: tokio::sync::mpsc::UnboundedSender<TickWriteMsg>,
    handle: ScopedStorageHandle<SurrealStorage>,
    run_dispatcher: Arc<RunDispatcherKind>,
    backfill_dispatcher: Arc<BackfillDispatcherKind>,
    eval_dispatcher: Arc<EvalDispatcher>,
    cancel: CancellationToken,
    max_ticks_retained: Option<usize>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut join_set = tokio::task::JoinSet::<TickResult>::new();

        'outer: loop {
            if cancel.is_cancelled() {
                break 'outer;
            }

            while let Some(Ok(tick_result)) = join_set.try_join_next() {
                process_tick_result(
                    &mut automations,
                    &tick_tx,
                    &handle,
                    &run_dispatcher,
                    &backfill_dispatcher,
                    tick_result,
                    max_ticks_retained,
                )
                .await;
            }

            let now = Utc::now();
            let due: Vec<DueEval> = collect_and_mark_due(&mut automations, now);
            eval_dispatcher.spawn_due(due, &mut join_set, now).await;

            let next_wait = automations
                .iter()
                .filter_map(|e| e.next_due_in(now))
                .min()
                .unwrap_or(Duration::from_secs(5));

            let sleep_fut = tokio::time::sleep(next_wait);
            tokio::pin!(sleep_fut);

            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break 'outer,
                    Some(Ok(tick_result)) = join_set.join_next(), if !join_set.is_empty() => {
                        let completed_idx = tick_result.index;
                        process_tick_result(&mut automations, &tick_tx, &handle, &run_dispatcher, &backfill_dispatcher, tick_result, max_ticks_retained).await;
                        let now_inner = Utc::now();
                        if automations[completed_idx].is_due(now_inner) {
                            break;
                        }
                        if let Some(due_in) = automations[completed_idx].next_due_in(now_inner) {
                            let new_deadline = tokio::time::Instant::now() + due_in;
                            if new_deadline < sleep_fut.deadline() {
                                sleep_fut.as_mut().reset(new_deadline);
                            }
                        }
                    }
                    _ = &mut sleep_fut => {
                        break;
                    }
                }
            }
        }

        // Drain in-flight evals so their `spawn_blocking` GIL workers aren't
        // abandoned on the shared runtime (see `gil_threads`); results dropped.
        while join_set.join_next().await.is_some() {}
    })
}

/// Snapshot the indices of automations that are due, build their `DueEval`
/// records, and mark each as dispatched (transitions cron's next_occurrence /
/// sensor's last_eval + in_flight). Mutating `automations` here keeps the
/// per-entry state machine driven by a single side-effect site.
fn collect_and_mark_due(
    automations: &mut [AutomationEntry],
    now: chrono::DateTime<Utc>,
) -> Vec<DueEval> {
    let due_indices: Vec<usize> = automations
        .iter()
        .enumerate()
        .filter(|(_, e)| e.is_due(now))
        .map(|(i, _)| i)
        .collect();

    let mut due: Vec<DueEval> = Vec::with_capacity(due_indices.len());
    for i in due_indices {
        tracing::info!(
            target: "rivers::daemon",
            automation_type = automations[i].automation_type_str(),
            name = automations[i].name(),
            eval_mode = ?automations[i].eval_mode(),
            "evaluating tick"
        );

        let params = automations[i].to_eval_params();
        let prev_cursor = automations[i].cursor().map(str::to_string);
        automations[i].mark_dispatched(now);

        due.push(DueEval {
            index: i,
            params,
            prev_cursor,
        });
    }
    due
}
