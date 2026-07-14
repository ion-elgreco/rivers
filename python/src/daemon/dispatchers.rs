//! Run and backfill dispatch — the seam between eval outcomes and run/backfill creation.
use std::sync::Arc;

use anyhow::anyhow;
use pyo3::prelude::*;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{LaunchedBy, RunRecord, RunStatus};

use super::types::{BackfillRequestData, MaterializationRequestData, RunRequestData};
use crate::gil_threads::GilThreads;
use crate::partitions::PyPartitionKey;
use crate::repository::{PyBackfillResult, PyCodeRepository, RepoHandle, priority_from_tags};

/// Launch a `Started` run on a fresh OS thread.
pub(crate) fn launch_started_run(
    handle: RepoHandle,
    job_name: String,
    partition_key: Option<PyPartitionKey>,
    run_id: String,
    gil_threads: &GilThreads,
) {
    gil_threads.spawn(move || {
        let attached = Python::try_attach(|py| {
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
        if attached.is_none() {
            // The Started record is already durable; without this the phantom
            // run reads as in-flight forever after the next daemon start.
            crate::runtime::rt().block_on(
                handle.mark_run_launch_failed(&run_id, "interpreter unavailable at launch"),
            );
        }
    });
}

/// Result of dispatching a batch of run or backfill requests.
#[derive(Default)]
pub(crate) struct DispatchOutcome {
    pub(crate) ids: Vec<String>,
    pub(crate) errors: Vec<anyhow::Error>,
}

pub(crate) struct DirectRunDispatcher {
    /// Used by `dispatch_materialization` to call `materialize_with_launcher`.
    repo: Arc<Py<PyCodeRepository>>,
    handle: RepoHandle,
    gil_threads: GilThreads,
}

pub(crate) struct QueuedRunDispatcher {
    handle: RepoHandle,
    /// Used by `dispatch_materialization` to write Queued `RunRecord`s directly via `SurrealStorage::enqueue_run`.
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
        gil_threads: GilThreads,
    ) -> Self {
        if run_queue_enabled {
            Self::Queued(QueuedRunDispatcher {
                handle,
                storage,
                code_location_id,
            })
        } else {
            Self::Direct(DirectRunDispatcher {
                repo,
                handle,
                gil_threads,
            })
        }
    }

    pub(crate) fn mode_label(&self) -> &'static str {
        match self {
            Self::Direct(_) => "direct",
            Self::Queued(_) => "queued",
        }
    }

    /// Schedule/sensor tick entry: dispatch one tick's worth of work and return a merged outcome.
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

    /// Job-targeted half of a tick.
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

    /// Condition daemon entry: pre-resolved asset selection with caller-minted run_ids.
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
    /// Materialization variant: write the `Started` `RunRecord` up-front, then spawn one OS thread per request.
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
            let handle = self.handle.clone();
            let assets = req.asset_selection.clone();
            let run_id = req.run_id.clone();
            let py_pk = req.partition_key.as_ref().map(PyPartitionKey::from);
            let launched_by = req.launched_by.clone();
            self.gil_threads.spawn(move || {
                if let Err(e) = repo.get().materialize_with_launcher(
                    Some(assets),
                    py_pk,
                    None,
                    false,
                    None,
                    Some(run_id.clone()),
                    false,
                    false,
                    launched_by,
                ) {
                    tracing::error!(
                        target: "rivers::daemon",
                        error = %e,
                        "materialize_with_launcher failed",
                    );
                    // Mark the pre-created Started record Failed, else the asset reads
                    // in-flight forever (an early return leaves it Started).
                    crate::runtime::rt().block_on(
                        handle.mark_run_launch_failed(&run_id, "materialization launch failed"),
                    );
                }
            });
        }
        Ok(DispatchOutcome { ids, errors })
    }

    /// Create one `Started` `RunRecord` per request, then launch each on a detached OS thread.
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
                    launch_started_run(
                        self.handle.clone(),
                        job_name,
                        py_pk,
                        run_id.clone(),
                        &self.gil_threads,
                    );
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
    /// Materialization variant: write a Queued `RunRecord` for each request via `SurrealStorage::enqueue_run`.
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
            let priority = priority_from_tags(&req.tags);
            let run_record = RunRecord {
                run_id: req.run_id.clone(),
                code_location_id: self.code_location_id.clone(),
                job_name: None,
                status: RunStatus::Queued,
                start_time: rivers_core::util::now_ts(),
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

    /// Submit each request via `repo.submit_run`, producing `Queued` `RunRecord`s.
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
    gil_threads: GilThreads,
}

pub(crate) enum BackfillDispatcherKind {
    Local(LocalBackfillDispatcher),
}

/// Per-request result of a backfill dispatch.
#[derive(Default)]
pub(crate) struct BackfillDispatchOutcome {
    pub(crate) results: Vec<PyBackfillResult>,
    pub(crate) errors: Vec<anyhow::Error>,
    /// The request selections (asset lists) that failed to launch.
    pub(crate) failed_targets: Vec<Vec<String>>,
}

impl BackfillDispatcherKind {
    pub(crate) fn new_local(repo: Arc<Py<PyCodeRepository>>, gil_threads: GilThreads) -> Self {
        Self::Local(LocalBackfillDispatcher { repo, gil_threads })
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
    /// Spawn one OS thread per backfill request; collect [`PyBackfillResult`]s via oneshot.
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
            let target = bf.target.clone();
            let label: Vec<String> = match &target {
                crate::daemon::RunType::Materialization(sel) => sel.clone(),
                crate::daemon::RunType::Job(name) => vec![format!("job:{name}")],
            };
            let (tx, rx) = tokio::sync::oneshot::channel::<Result<PyBackfillResult, String>>();
            self.gil_threads.spawn(move || {
                let tags = bf.tags.as_ref().map(|t| {
                    t.iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect::<Vec<(String, String)>>()
                });
                let result = match repo.get().backfill_inner(
                    target,
                    bf.partition_keys.clone(),
                    bf.partition_range.clone(),
                    bf.strategy.clone(),
                    bf.failure_policy.as_deref().unwrap_or("continue"),
                    bf.max_concurrency,
                    tags,
                    None,  // config
                    false, // block=false
                    bf.dry_run,
                    bf.backfill_id.clone(),
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
            pending.push((label, rx));
        }

        let mut results: Vec<PyBackfillResult> = Vec::with_capacity(pending.len());
        let mut errors: Vec<anyhow::Error> = Vec::new();
        let mut failed_targets: Vec<Vec<String>> = Vec::new();
        for (selection, rx) in pending {
            match rx.await {
                Ok(Ok(result)) => results.push(result),
                Ok(Err(e)) => {
                    errors.push(anyhow!("backfill {:?}: {}", selection, e));
                    failed_targets.push(selection);
                }
                Err(e) => {
                    errors.push(anyhow!(
                        "backfill {:?}: oneshot recv failed: {}",
                        selection,
                        e
                    ));
                    failed_targets.push(selection);
                }
            }
        }

        Ok(BackfillDispatchOutcome {
            results,
            errors,
            failed_targets,
        })
    }
}
