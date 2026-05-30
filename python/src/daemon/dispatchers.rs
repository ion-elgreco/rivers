//! Run and backfill dispatch — the seam between an `EvalOutcome`'s
//! `RunRequest`/`BackfillRequest` lists and the actual creation of `RunRecord`s
//! / launch of backfills. Each strategy is a variant of the corresponding
//! `*DispatcherKind` enum (matches the `RunBackendKind` precedent in `mod.rs`).
//!
//! - Direct vs Queued is the run-side strategy: direct creates + spawns an OS
//!   thread to call `execute_run`; queued submits via `repo.submit_run` so the
//!   `RunQueueCoordinator` picks them up.
//! - Backfills currently only have a local strategy (`repo.backfill` on an OS
//!   thread, results collected via oneshot). The trait-shaped enum exists so a
//!   future remote/distributed adapter drops in cleanly.
use std::sync::Arc;

use anyhow::anyhow;
use pyo3::prelude::*;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{LaunchedBy, RunRecord, RunStatus};

use super::types::{BackfillRequestData, MaterializationRequestData, RunRequestData};
use crate::partitions::PyPartitionKey;
use crate::repository::{PyBackfillResult, PyCodeRepository, RepoHandle, priority_from_tags};

/// Launch a `Started` run on a fresh OS thread. Both the daemon's
/// Direct dispatcher and the gRPC `ExecuteJob` handler obtain the
/// `run_id` via [`RepoHandle::create_started_run`] and hand off here
/// for execution. Fire-and-forget: errors are logged via tracing —
/// the run's own status-update path reports failure to storage.
///
/// `std::thread::spawn` over `tokio::task::spawn_blocking` here — see
/// [`crate::runtime`] for the rationale.
pub(crate) fn launch_started_run(
    handle: RepoHandle,
    job_name: String,
    partition_key: Option<PyPartitionKey>,
    run_id: String,
) {
    let handle = std::thread::spawn(move || {
        Python::try_attach(|py| {
            let job = match handle.get_job(py, &job_name) {
                Ok(j) => j,
                Err(e) => {
                    tracing::error!(
                        target: "rivers::executor",
                        job = %job_name,
                        run_id = %run_id,
                        error = %e,
                        "job not found at launch"
                    );
                    return;
                }
            };
            if let Err(e) =
                job.borrow(py)
                    .execute_run(py, partition_key, &run_id, None, false, true)
            {
                tracing::error!(
                    target: "rivers::executor",
                    job = %job_name,
                    run_id = %run_id,
                    error = %e,
                    "job run failed"
                );
            }
        });
    });
    // Joined before shutdown / finalize — the thread holds the GIL.
    crate::shutdown::register_run_handle(handle);
}

/// Result of dispatching a batch of run or backfill requests.
///
/// `ids` holds the successfully created run / backfill ids (one per
/// successful request). `errors` holds per-request failures with rich context
/// already baked in via `anyhow::Error::context` — the caller only needs to
/// log them. Length of `ids` + length of `errors` may be less than the input
/// length when an outer failure (e.g. GIL acquisition) aborts the batch.
#[derive(Default)]
pub(crate) struct DispatchOutcome {
    pub(crate) ids: Vec<String>,
    pub(crate) errors: Vec<anyhow::Error>,
}

pub(crate) struct DirectRunDispatcher {
    /// Used by `dispatch_materialization` to call
    /// `materialize_with_launcher`, which fundamentally needs the GIL
    /// to run user asset code.
    repo: Arc<Py<PyCodeRepository>>,
    handle: RepoHandle,
}

pub(crate) struct QueuedRunDispatcher {
    handle: RepoHandle,
    /// Used by `dispatch_materialization` to write Queued `RunRecord`s
    /// directly via `SurrealStorage::enqueue_run`. The condition path has
    /// the asset selection pre-resolved and doesn't need the repo's
    /// graph/job lookup that `submit_run` performs.
    storage: Arc<SurrealStorage>,
    code_location_id: String,
}

pub(crate) enum RunDispatcherKind {
    Direct(DirectRunDispatcher),
    Queued(QueuedRunDispatcher),
}

impl RunDispatcherKind {
    pub(crate) fn new(
        repo: Arc<Py<PyCodeRepository>>,
        handle: RepoHandle,
        storage: Arc<SurrealStorage>,
        code_location_id: String,
        run_queue_enabled: bool,
    ) -> Self {
        if run_queue_enabled {
            Self::Queued(QueuedRunDispatcher {
                handle,
                storage,
                code_location_id,
            })
        } else {
            Self::Direct(DirectRunDispatcher { repo, handle })
        }
    }

    pub(crate) fn mode_label(&self) -> &'static str {
        match self {
            Self::Direct(_) => "direct",
            Self::Queued(_) => "queued",
        }
    }

    /// Schedule/sensor tick entry: dispatch one tick's worth of work — both
    /// job-targeted runs and ad-hoc materializations — and return a single
    /// merged outcome. The two halves run concurrently via `tokio::join!`;
    /// they touch disjoint critical sections (both paths now write
    /// records via async storage and only spawn launch threads as a
    /// fire-and-forget side effect) so parallelism is free.
    ///
    /// `DispatchOutcome.ids` is deterministic: job ids first, materialize
    /// ids appended.
    pub(crate) async fn dispatch_tick(
        &self,
        job_reqs: &[RunRequestData],
        mat_reqs: &[MaterializationRequestData],
        launched_by: LaunchedBy,
    ) -> anyhow::Result<DispatchOutcome> {
        let (jobs, mats) = tokio::join!(
            self.dispatch_jobs(job_reqs, launched_by),
            self.dispatch_materialization(mat_reqs),
        );
        let mut merged = jobs?;
        let mut mat_outcome = mats?;
        merged.ids.append(&mut mat_outcome.ids);
        merged.errors.append(&mut mat_outcome.errors);
        Ok(merged)
    }

    /// Job-targeted half of a tick. Every request must carry `job_name`
    /// (set on the `RunRequest` itself or applied as a default by
    /// `parse_*_result`). Asset-selection runs go through
    /// [`Self::dispatch_materialization`].
    pub(crate) async fn dispatch_jobs(
        &self,
        requests: &[RunRequestData],
        launched_by: LaunchedBy,
    ) -> anyhow::Result<DispatchOutcome> {
        match self {
            Self::Direct(d) => d.dispatch_jobs(requests, launched_by).await,
            Self::Queued(d) => d.dispatch_jobs(requests, launched_by).await,
        }
    }

    /// Condition daemon entry: pre-resolved asset selection with
    /// caller-minted run_ids. Also called as the materialize half of
    /// [`Self::dispatch_tick`].
    ///
    /// Direct mode dispatches via `repo.materialize_with_launcher`; Queued
    /// mode writes a Queued `RunRecord` + `RunQueued` event directly to
    /// storage. The returned `DispatchOutcome.ids` echoes the input run_ids
    /// in success order; callers usually already track them.
    ///
    /// Trusts the caller to have validated `asset_selection` and
    /// `partition_key` already — system-boundary callers (gRPC) check
    /// at the handler; internal callers (sensor/schedule eval,
    /// condition daemon) are pre-validated upstream or accept retry
    /// semantics.
    pub(crate) async fn dispatch_materialization(
        &self,
        requests: &[MaterializationRequestData],
    ) -> anyhow::Result<DispatchOutcome> {
        match self {
            Self::Direct(d) => d.dispatch_materialization(requests).await,
            Self::Queued(d) => d.dispatch_materialization(requests).await,
        }
    }
}

impl DirectRunDispatcher {
    /// Materialization variant: write the `Started` `RunRecord`
    /// up-front via [`RepoHandle::create_materialization_run`] (GIL-free,
    /// in this async fn), then spawn one OS thread per request to drive
    /// the GIL-bound execution via `materialize_with_launcher`. The
    /// thread re-uses the existing record (`RunInit::Existing`) so no
    /// duplicate write happens.
    ///
    /// Pre-writing the record means storage failures land in
    /// `DispatchOutcome.errors` synchronously (before the launch
    /// thread is spawned), instead of being swallowed inside the
    /// fire-and-forget thread.
    async fn dispatch_materialization(
        &self,
        requests: &[MaterializationRequestData],
    ) -> anyhow::Result<DispatchOutcome> {
        if requests.is_empty() {
            return Ok(DispatchOutcome::default());
        }
        let mut ids: Vec<String> = Vec::with_capacity(requests.len());
        let mut errors: Vec<anyhow::Error> = Vec::new();
        for req in requests {
            if let Err(e) = self
                .handle
                .create_materialization_run(
                    req.asset_selection.clone(),
                    req.partition_key.clone(),
                    req.tags.clone(),
                    req.launched_by.clone(),
                    req.run_id.clone(),
                )
                .await
            {
                errors.push(anyhow!("materialization {}: {}", req.run_id, e));
                continue;
            }
            ids.push(req.run_id.clone());

            let repo = Arc::clone(&self.repo);
            let assets = req.asset_selection.clone();
            let run_id = req.run_id.clone();
            let py_pk = req.partition_key.as_ref().map(PyPartitionKey::from);
            let launched_by = req.launched_by.clone();
            let handle = std::thread::spawn(move || {
                if let Err(e) = repo.get().materialize_with_launcher(
                    Some(assets),
                    py_pk,
                    None,
                    false,
                    None,
                    Some(run_id),
                    false,
                    false,
                    launched_by,
                ) {
                    tracing::error!(
                        target: "rivers::daemon",
                        error = %e,
                        "materialize_with_launcher failed",
                    );
                }
            });
            // Tracked so SIGTERM waits for the run before Python finalize.
            crate::shutdown::register_materialization_handle(handle);
        }
        Ok(DispatchOutcome { ids, errors })
    }

    /// Phase 1 (GIL-free): create one `Started` `RunRecord` per request via
    /// `RepoHandle::create_started_run`. Partition validation happens
    /// against the job's resolved asset selection, so a misconfigured
    /// schedule/sensor partition fails synchronously.
    ///
    /// Phase 2 (detached OS thread): launch each successfully created run
    /// via [`launch_started_run`]. Fire-and-forget — the daemon does not
    /// await launch completion. Per-request failures from Phase 1 land in
    /// `DispatchOutcome.errors`; partial progress is preserved.
    async fn dispatch_jobs(
        &self,
        requests: &[RunRequestData],
        launched_by: LaunchedBy,
    ) -> anyhow::Result<DispatchOutcome> {
        if requests.is_empty() {
            return Ok(DispatchOutcome::default());
        }

        let mut ids: Vec<String> = Vec::with_capacity(requests.len());
        let mut errors: Vec<anyhow::Error> = Vec::new();

        for r in requests {
            let job_name = match r.job_name.clone() {
                Some(j) => j,
                None => {
                    errors.push(anyhow!(
                        "internal: RunRequest reached `dispatch` without job_name (partition {:?})",
                        r.partition_key
                    ));
                    continue;
                }
            };

            let py_pk = r.partition_key.clone();

            match self
                .handle
                .create_started_run(&job_name, py_pk.as_ref(), launched_by.clone())
                .await
            {
                Ok(run_id) => {
                    launch_started_run(self.handle.clone(), job_name, py_pk, run_id.clone());
                    ids.push(run_id);
                }
                Err(e) => errors.push(anyhow!(
                    "create run for job '{}' (partition {:?}): {}",
                    job_name,
                    r.partition_key,
                    e
                )),
            }
        }

        Ok(DispatchOutcome { ids, errors })
    }
}

impl QueuedRunDispatcher {
    /// Materialization variant: write a Queued `RunRecord` for each
    /// request via `SurrealStorage::enqueue_run`, using the caller-minted
    /// run_id and the pre-resolved asset selection. The Python boundary is
    /// not crossed (no GIL, no job lookup) — the storage primitive owns
    /// the "Queued + RunQueued event" invariant.
    async fn dispatch_materialization(
        &self,
        requests: &[MaterializationRequestData],
    ) -> anyhow::Result<DispatchOutcome> {
        if requests.is_empty() {
            return Ok(DispatchOutcome::default());
        }
        let mut ids: Vec<String> = Vec::with_capacity(requests.len());
        let mut errors: Vec<anyhow::Error> = Vec::new();
        let now = rivers_core::util::now_ts();
        for req in requests {
            let priority = priority_from_tags(&req.tags);
            let run_record = RunRecord {
                run_id: req.run_id.clone(),
                code_location_id: self.code_location_id.clone(),
                job_name: None,
                status: RunStatus::Queued,
                start_time: now,
                end_time: None,
                tags: req.tags.clone(),
                node_names: req.asset_selection.clone(),
                priority,
                partition_key: req.partition_key.clone(),
                block_reason: None,
                launched_by: req.launched_by.clone(),
            };
            if let Err(e) = self.storage.enqueue_run(&run_record).await {
                errors.push(anyhow!(
                    "enqueue_run for materialization {}: {}",
                    req.run_id,
                    e
                ));
                continue;
            }
            ids.push(req.run_id.clone());
        }
        Ok(DispatchOutcome { ids, errors })
    }

    /// Submit each request via `repo.submit_run`, producing `Queued`
    /// `RunRecord`s for the `RunQueueCoordinator` to dequeue. Every request
    /// must carry `job_name` (asset-selection runs go through
    /// `dispatch_materialization`).
    async fn dispatch_jobs(
        &self,
        requests: &[RunRequestData],
        launched_by: LaunchedBy,
    ) -> anyhow::Result<DispatchOutcome> {
        if requests.is_empty() {
            return Ok(DispatchOutcome::default());
        }

        let mut ids: Vec<String> = Vec::with_capacity(requests.len());
        let mut errors: Vec<anyhow::Error> = Vec::new();

        for r in requests {
            let job_name = match r.job_name.clone() {
                Some(j) => j,
                None => {
                    errors.push(anyhow!(
                        "internal: RunRequest reached `dispatch` without job_name (partition {:?})",
                        r.partition_key
                    ));
                    continue;
                }
            };

            let selection = self.handle.job_asset_names(&job_name);
            match self
                .handle
                .submit_run(
                    selection,
                    r.partition_key.as_ref(),
                    None,
                    launched_by.clone(),
                    Some(job_name.clone()),
                )
                .await
            {
                Ok(h) => ids.push(h.run_id),
                Err(e) => errors.push(anyhow!(
                    "submit run (job '{}', partition {:?}): {}",
                    job_name,
                    r.partition_key,
                    e
                )),
            }
        }
        Ok(DispatchOutcome { ids, errors })
    }
}

pub(crate) struct LocalBackfillDispatcher {
    repo: Arc<Py<PyCodeRepository>>,
}

pub(crate) enum BackfillDispatcherKind {
    Local(LocalBackfillDispatcher),
}

/// Per-request result of a backfill dispatch.
///
/// Distinct from [`DispatchOutcome`] (run-shaped, ids only) — backfills
/// produce richer per-request data (`num_partitions`, `run_ids`,
/// `is_dry_run`, …) that gRPC's `LaunchBackfill` surfaces verbatim.
/// The daemon's tick logger only consumes `backfill_id` and ignores the
/// rest; that asymmetry is fine — both callers go through the same
/// dispatch but read different slices of the result.
#[derive(Default)]
pub(crate) struct BackfillDispatchOutcome {
    pub(crate) results: Vec<PyBackfillResult>,
    pub(crate) errors: Vec<anyhow::Error>,
}

impl BackfillDispatcherKind {
    pub(crate) fn new_local(repo: Arc<Py<PyCodeRepository>>) -> Self {
        Self::Local(LocalBackfillDispatcher { repo })
    }

    pub(crate) fn mode_label(&self) -> &'static str {
        match self {
            Self::Local(_) => "local",
        }
    }

    pub(crate) async fn dispatch(
        &self,
        requests: &[BackfillRequestData],
    ) -> anyhow::Result<BackfillDispatchOutcome> {
        match self {
            Self::Local(d) => d.dispatch(requests).await,
        }
    }
}

impl LocalBackfillDispatcher {
    /// Spawn one OS thread per backfill request; collect [`PyBackfillResult`]s
    /// via oneshot. `std::thread::spawn` over `spawn_blocking` — see
    /// [`crate::runtime`].
    ///
    /// `dry_run` requests are valid: the resulting `backfill_id` is empty
    /// and `is_dry_run` is `true`, but the result is otherwise complete
    /// (partition counts, status). Callers that need a real backfill_id
    /// (the daemon tick logger) inspect `is_dry_run` themselves.
    async fn dispatch(
        &self,
        requests: &[BackfillRequestData],
    ) -> anyhow::Result<BackfillDispatchOutcome> {
        if requests.is_empty() {
            return Ok(BackfillDispatchOutcome::default());
        }

        let mut pending: Vec<(
            Vec<String>,
            tokio::sync::oneshot::Receiver<Result<PyBackfillResult, String>>,
        )> = Vec::with_capacity(requests.len());

        for bf in requests {
            let repo = Arc::clone(&self.repo);
            let bf = bf.clone();
            let selection = bf.selection.clone();
            let (tx, rx) = tokio::sync::oneshot::channel::<Result<PyBackfillResult, String>>();
            let h = std::thread::spawn(move || {
                let tags = bf.tags.as_ref().map(|t| {
                    t.iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect::<Vec<(String, String)>>()
                });
                let result = match repo.get().backfill_inner(
                    Some(bf.selection.clone()),
                    bf.partition_keys.clone(),
                    bf.partition_range.clone(),
                    bf.strategy.clone(),
                    bf.failure_policy.as_deref().unwrap_or("continue"),
                    bf.max_concurrency,
                    tags,
                    None,  // config
                    false, // block=false
                    bf.dry_run,
                ) {
                    Ok(result) => {
                        tracing::info!(
                            target: "rivers::daemon",
                            backfill_id = %result.backfill_id,
                            num_partitions = result.num_partitions,
                            dry_run = result.is_dry_run,
                            "launched backfill"
                        );
                        Ok(result)
                    }
                    Err(e) => Err(e.to_string()),
                };
                let _ = tx.send(result);
            });
            crate::shutdown::register_backfill_handle(h);
            pending.push((selection, rx));
        }

        let mut results: Vec<PyBackfillResult> = Vec::with_capacity(pending.len());
        let mut errors: Vec<anyhow::Error> = Vec::new();
        for (selection, rx) in pending {
            match rx.await {
                Ok(Ok(result)) => results.push(result),
                Ok(Err(e)) => errors.push(anyhow!("backfill {:?}: {}", selection, e)),
                Err(e) => errors.push(anyhow!(
                    "backfill {:?}: oneshot recv failed: {}",
                    selection,
                    e
                )),
            }
        }

        Ok(BackfillDispatchOutcome { results, errors })
    }
}
