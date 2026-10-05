use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};
use std::thread::ThreadId;

use pyo3::prelude::*;
use pyo3::sync::{MutexExt, RwLockExt};

use crate::config::ResourceVariant;
use crate::errors::ExecutionError;
use crate::executor::Executor;
use crate::executor::ops::now_ts;
use crate::job::PyJob;
use crate::partitions::{PartitionsDefinition, PyBackfillStrategy, PyPartitionKey};
use crate::storage::{DetachOnClose, PyStorageType};
use rivers_core::repo::CodeRepository;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{
    BackfillFailurePolicy, BackfillStatus, LaunchedBy, PartitionKey, RunRecord, RunStatus,
    StorageBackend, tag_keys,
};

use super::resolved_node::ResolvedNode;
use super::results::{PyBackfillStatusResult, PyRunHandle};
use super::validation::{
    DynamicKeyCheck, assets_supporting_action, dynamic_partition_checks,
    ensure_assets_support_action, ensure_job_verb, ensure_whole_asset_chosen, resolve_selection,
    validate_partition_for_verb, verify_dynamic_partition_keys,
};

pub(crate) fn priority_from_tags(tags: &[(String, String)]) -> i32 {
    tags.iter()
        .find(|(k, _)| k == tag_keys::PRIORITY)
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0)
}

/// The verb a job's runs execute, for stamping on their `RunRecord`.
/// `None` for materialize jobs (and unknown/unresolved names). Takes the
/// borrowed map so callers that already hold the state can use it.
pub(super) fn job_action(
    jobs_info: &HashMap<String, JobSummary>,
    job_name: Option<&str>,
) -> Option<String> {
    job_name.and_then(|j| jobs_info.get(j).and_then(|s| s.action.clone()))
}

#[derive(Clone)]
pub(crate) struct JobSummary {
    pub name: String,
    pub node_names: Vec<String>,
    pub asset_names: Vec<String>,
    pub executor: Option<Executor>,
    /// The verb this job's runs execute; `None` means materialize.
    pub action: Option<String>,
}

/// Snapshot of a registered sensor — populated at resolve time so
/// observability paths (gRPC `GetSensors`) don't need to borrow `Py<...>`.
#[derive(Clone)]
pub(crate) struct SensorSummary {
    pub name: String,
    pub job_name: Option<String>,
    pub default_status: crate::automation::PySensorStatus,
    pub minimum_interval: Option<String>,
    pub description: Option<String>,
    pub asset_selection: Option<Vec<String>>,
    pub tags: Option<HashMap<String, String>>,
}

#[derive(Clone)]
pub(crate) struct ScheduleSummary {
    pub name: String,
    pub cron_schedule: String,
    pub job_name: String,
    pub default_status: crate::automation::PyScheduleStatus,
    pub timezone: Option<String>,
    pub description: Option<String>,
    pub tags: Option<HashMap<String, String>>,
}

/// Populated by resolve(); `None` before that.
pub(crate) struct ResolvedState {
    pub(crate) inner_repo: CodeRepository,
    pub(crate) node_map: HashMap<String, ResolvedNode>,
    pub(crate) jobs: HashMap<String, Py<PyJob>>,
    pub(crate) jobs_info: HashMap<String, JobSummary>,
    pub(crate) sensors_info: HashMap<String, SensorSummary>,
    pub(crate) schedules_info: HashMap<String, ScheduleSummary>,
    pub(crate) storage: DetachOnClose<Arc<SurrealStorage>>,
    pub(crate) storage_type: PyStorageType,
    pub(crate) resources: HashMap<String, ResourceVariant>,
    pub(crate) io_handler_registry: crate::assets::io_handler_registry::IOHandlerRegistry,
    /// Plan-build inputs computed once at resolve time and shared across every
    /// `validate_and_build_plan` call (per-job during resolve, plus the synthetic
    /// job materialize constructs on each call).
    pub(crate) step_kinds: HashMap<String, rivers_core::execution::plan::StepKind>,
    pub(crate) multi_asset_groups: HashMap<String, String>,
    pub(crate) composition_order: HashMap<String, usize>,
    pub(crate) run_backend: Arc<crate::daemon::RunBackendKind>,
    pub(crate) code_location_id: String,
}

/// The [`ResolvedState`] a repository shares with its [`RepoHandle`]s. A
/// caller takes its own `Arc` and the lock is released at once, so a run
/// keeps the state it started with when the repository releases it.
#[derive(Clone, Default)]
pub(crate) struct SharedState(Arc<RwLock<Option<Arc<ResolvedState>>>>);

impl SharedState {
    pub(crate) fn get(&self) -> Option<Arc<ResolvedState>> {
        self.0.read().unwrap().clone()
    }

    pub(crate) fn get_attached(&self, py: Python<'_>) -> Option<Arc<ResolvedState>> {
        self.0.read_py_attached(py).unwrap().clone()
    }

    /// `None` also while a writer holds the lock.
    pub(super) fn try_get(&self) -> Option<Arc<ResolvedState>> {
        self.0.try_read().ok()?.clone()
    }

    /// Returns the previous state, so it is dropped after the lock is released.
    pub(super) fn replace(
        &self,
        py: Python<'_>,
        state: Option<Arc<ResolvedState>>,
    ) -> Option<Arc<ResolvedState>> {
        std::mem::replace(&mut *self.0.write_py_attached(py).unwrap(), state)
    }
}

/// Non-py twin of [`PyCodeRepository`] for the dispatch surface.
///
/// Shares the pyclass's [`SharedState`], so the dispatcher (and any other
/// Rust caller) can submit runs without going through `repo.borrow(py)`.
///
/// Construct via [`PyCodeRepository::handle`] under the GIL once at
/// dispatcher startup; clone freely thereafter.
#[derive(Clone)]
pub(crate) struct RepoHandle {
    pub(super) state: SharedState,
    pub(super) backfill_cancel_flags:
        Arc<std::sync::Mutex<HashMap<String, Arc<std::sync::atomic::AtomicBool>>>>,
}

/// One run to enqueue via [`submit_runs`].
pub(crate) struct RunSubmission {
    /// `None` = all assets.
    pub(crate) selection: Option<Vec<String>>,
    pub(crate) partition_key: Option<PyPartitionKey>,
    pub(crate) tags: Option<Vec<(String, String)>>,
    /// `Some(job)` enqueues the run as a job execution (resolved with the job's
    /// plan + executor on dequeue); `None` for an ad-hoc materialization.
    pub(crate) job_name: Option<String>,
    /// The verb the dequeued run executes; `None` means materialize. For a
    /// job, the verb its backfill recorded, checked against the job's own.
    pub(crate) action: Option<String>,
    /// See [`rivers_core::storage::RunRecord::config`].
    pub(crate) config: Option<String>,
}

impl RepoHandle {
    fn resolved(&self) -> PyResult<Arc<ResolvedState>> {
        self.state.get().ok_or_else(|| {
            ExecutionError::new_err("Repository not resolved — call resolve() first")
        })
    }

    /// Best-effort: fail a `Started` run whose launch never began (e.g. the
    /// interpreter was finalizing), so it can't wedge in-flight gating forever.
    /// `reason` is persisted on a `RunLaunchFailed` event — without it the run
    /// detail has an empty timeline and the error only reaches the terminal.
    pub(crate) async fn mark_run_launch_failed(&self, run_id: &str, reason: &str) {
        let Some(state) = self.state.get() else {
            return;
        };
        crate::daemon::fail_unlaunched_run(&state.storage, &state.code_location_id, run_id, reason)
            .await;
    }

    /// Look up a user-defined job's asset selection. `None` if the repo
    /// isn't resolved or the job doesn't exist. GIL-free — reads the
    /// pre-computed map populated at resolve time.
    pub(crate) fn job_asset_names(&self, name: &str) -> Option<Vec<String>> {
        self.state
            .get()
            .and_then(|s| s.jobs_info.get(name).map(|j| j.asset_names.clone()))
    }

    /// The verb a user-defined job runs; `None` means materialize.
    pub(crate) fn job_verb(&self, name: &str) -> Option<String> {
        self.state
            .get()
            .and_then(|s| job_action(&s.jobs_info, Some(name)))
    }

    /// Assets defining `action`, sorted.
    pub(crate) fn assets_supporting_action(&self, action: &str) -> PyResult<Vec<String>> {
        let state = self.state.get().ok_or_else(|| {
            ExecutionError::new_err("CodeRepository not resolved — call resolve() first")
        })?;
        Ok(assets_supporting_action(&state.node_map, action))
    }

    /// Reject any selected asset that doesn't define `action`.
    pub(crate) fn validate_assets_support_action(
        &self,
        selection: &[String],
        action: &str,
    ) -> PyResult<()> {
        let state = self.state.get().ok_or_else(|| {
            ExecutionError::new_err("CodeRepository not resolved — call resolve() first")
        })?;
        ensure_assets_support_action(&state.node_map, selection, action)
    }

    /// A keyless gRPC `RunAction` that did not choose the whole asset: see
    /// [`ensure_whole_asset_chosen`].
    pub(crate) fn validate_keyless_action(
        &self,
        asset_names: &[String],
        action: &str,
    ) -> PyResult<()> {
        let state = self.state.get().ok_or_else(|| {
            ExecutionError::new_err("CodeRepository not resolved — call resolve() first")
        })?;
        ensure_whole_asset_chosen(
            &state.node_map,
            asset_names.iter().map(String::as_str),
            action,
        )
    }

    /// [`Self::validate_keyless_action`] for a keyless gRPC `ExecuteJob`: the
    /// job's verb over its assets. A materialize job has no choice to make.
    pub(crate) fn validate_keyless_job(&self, job_name: &str) -> PyResult<()> {
        let state = self.state.get().ok_or_else(|| {
            ExecutionError::new_err("CodeRepository not resolved — call resolve() first")
        })?;
        let Some(job) = state.jobs_info.get(job_name) else {
            return Ok(());
        };
        let Some(verb) = job.action.as_deref() else {
            return Ok(());
        };
        ensure_whole_asset_chosen(
            &state.node_map,
            job.asset_names.iter().map(String::as_str),
            verb,
        )
    }

    /// See [`ensure_job_verb`].
    pub(crate) fn validate_job_verb(&self, job_name: &str, expected: Option<&str>) -> PyResult<()> {
        let state = self.state.get().ok_or_else(|| {
            ExecutionError::new_err("CodeRepository not resolved — call resolve() first")
        })?;
        ensure_job_verb(&state.jobs_info, job_name, expected)
    }

    pub(crate) fn list_jobs(&self) -> Vec<JobSummary> {
        self.state
            .get()
            .map(|s| s.jobs_info.values().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn job_names(&self) -> Vec<String> {
        self.state
            .get()
            .map(|s| s.jobs_info.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn code_location_id(&self) -> Option<String> {
        self.state.get().map(|s| s.code_location_id.clone())
    }

    pub(crate) fn list_sensors(&self) -> Vec<SensorSummary> {
        self.state
            .get()
            .map(|s| s.sensors_info.values().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn list_schedules(&self) -> Vec<ScheduleSummary> {
        self.state
            .get()
            .map(|s| s.schedules_info.values().cloned().collect())
            .unwrap_or_default()
    }

    /// See [`PyCodeRepository::submit_run`]. `config` is stored on the
    /// record for the dequeuing backend to apply.
    pub(crate) async fn submit_run(
        &self,
        selection: Option<Vec<String>>,
        partition_key: Option<&PyPartitionKey>,
        tags: Option<Vec<(String, String)>>,
        launched_by: LaunchedBy,
        job_name: Option<String>,
        config: Option<String>,
    ) -> PyResult<PyRunHandle> {
        let (record, storage, dyn_checks) = {
            let state = self.resolved()?;
            let graph = state
                .inner_repo
                .graph
                .as_ref()
                .ok_or_else(|| ExecutionError::new_err("Graph not resolved"))?;

            let asset_names: Vec<String> = if let Some(ref sel) = selection {
                sel.clone()
            } else {
                graph
                    .node_indices()
                    .map(|idx| graph[idx].name.clone())
                    .collect()
            };

            // The record carries the job's verb, so the key must satisfy it.
            let action = job_action(&state.jobs_info, job_name.as_deref());
            validate_partition_for_verb(
                &state,
                asset_names.iter().map(String::as_str),
                partition_key,
                action.as_deref(),
            )?;
            let dyn_checks = dynamic_partition_checks(
                &state,
                asset_names.iter().map(String::as_str),
                partition_key,
            );

            let run_id = uuid::Uuid::new_v4().to_string();
            let now = now_ts();
            let run_tags = tags.unwrap_or_default();
            let core_pk = partition_key.map(|pk| pk.into());
            let priority = priority_from_tags(&run_tags);

            let record = RunRecord {
                run_id,
                code_location_id: state.code_location_id.clone(),
                job_name,
                status: RunStatus::Queued,
                start_time: now,
                end_time: None,
                tags: run_tags,
                node_names: asset_names,
                priority,
                partition_key: core_pk,
                block_reason: None,
                launched_by,
                action,
                config,
            };
            (record, state.storage.clone(), dyn_checks)
        };

        verify_dynamic_partition_keys(&storage, &record.code_location_id, &dyn_checks).await?;

        storage
            .enqueue_run(&record)
            .await
            .map_err(|e| ExecutionError::new_err(format!("Failed to enqueue run: {e}")))?;

        tracing::info!(
            target: "rivers::repo",
            run_id = %record.run_id,
            priority = record.priority,
            "run enqueued"
        );

        Ok(PyRunHandle {
            run_id: record.run_id,
            storage: DetachOnClose::new(storage),
        })
    }

    /// Direct counterpart to [`Self::submit_run`]: write a `RunRecord`
    /// already in `Started` state for an explicit job. The caller is
    /// expected to launch the job's `execute_run` on its own thread
    /// (see [`crate::daemon::dispatchers::launch_started_run`]).
    ///
    /// Validates `partition_key` against the job's asset selection —
    /// the same check `submit_run` performs, so a misconfigured
    /// schedule/sensor partition fails synchronously instead of after
    /// the record is written. Returns the freshly minted `run_id`. No
    /// event is emitted (Started runs don't carry a queue transition).
    pub(crate) async fn create_started_run(
        &self,
        job_name: &str,
        partition_key: Option<&PyPartitionKey>,
        launched_by: LaunchedBy,
        run_id_override: Option<String>,
        config: Option<String>,
    ) -> PyResult<String> {
        let state = self.resolved()?;
        create_started_run(
            &state,
            job_name,
            partition_key,
            launched_by,
            run_id_override,
            config,
        )
        .await
    }

    /// Materialization counterpart to [`Self::create_started_run`]:
    /// writes a `RunRecord{Started}` for an asset selection (ad-hoc,
    /// no `job_name`). Caller-minted `run_id`. No validation — the
    /// caller is responsible (gRPC validates at the boundary;
    /// internal callers like sensors/schedules/conditions trust their
    /// upstream).
    ///
    /// Used by:
    ///   * `DirectRunDispatcher::dispatch_materialization` (async fn,
    ///     before spawning the launch thread — record-write failures
    ///     land in `DispatchOutcome.errors` synchronously)
    ///   * `PyCodeRepository::materialize_with_launcher` (the Python
    ///     pymethod path used by `repo.materialize()` and per-partition
    ///     `repo.backfill()` fan-out — bridged via `rt().block_on`)
    pub(crate) async fn create_materialization_run(
        &self,
        asset_selection: Vec<String>,
        partition_key: Option<rivers_core::storage::PartitionKey>,
        tags: Vec<(String, String)>,
        launched_by: LaunchedBy,
        run_id: String,
        action: Option<String>,
        config: Option<String>,
    ) -> PyResult<()> {
        let state = self.resolved()?;
        create_materialization_run(
            &state,
            asset_selection,
            partition_key,
            tags,
            launched_by,
            run_id,
            action,
            config,
        )
        .await
    }

    /// Validate `partition_key` against the given `asset_names` —
    /// rejects missing keys for partitioned assets and keys that don't
    /// match an asset's partition definition. Same check
    /// [`Self::submit_run`] / [`Self::create_started_run`] perform
    /// inline; exposed separately so callers that bypass those
    /// (e.g. gRPC `materialize` going through
    /// `dispatch_materialization`) can fail synchronously instead of
    /// after a fire-and-forget thread swallows the error.
    pub(crate) async fn validate_partition_for_selection(
        &self,
        asset_names: &[String],
        partition_key: Option<&PyPartitionKey>,
    ) -> PyResult<()> {
        self.validate_partition_inner(asset_names, partition_key, None)
            .await
    }

    /// Verb-aware twin of [`Self::validate_partition_for_selection`], for the
    /// gRPC action boundary. An unkeyed observe is a whole-asset observation
    /// (the observe fn takes no partition), so a partitioned observable must
    /// not demand a key the verb has nowhere to put.
    pub(crate) async fn validate_partition_for_action(
        &self,
        asset_names: &[String],
        partition_key: Option<&PyPartitionKey>,
        action: &str,
    ) -> PyResult<()> {
        self.validate_partition_inner(asset_names, partition_key, Some(action))
            .await
    }

    async fn validate_partition_inner(
        &self,
        asset_names: &[String],
        partition_key: Option<&PyPartitionKey>,
        verb: Option<&str>,
    ) -> PyResult<()> {
        let (dyn_checks, storage, code_location_id) = {
            let state = self.resolved()?;
            validate_partition_for_verb(
                &state,
                asset_names.iter().map(String::as_str),
                partition_key,
                verb,
            )?;
            (
                dynamic_partition_checks(
                    &state,
                    asset_names.iter().map(String::as_str),
                    partition_key,
                ),
                state.storage.clone(),
                state.code_location_id.clone(),
            )
        };
        verify_dynamic_partition_keys(&storage, &code_location_id, &dyn_checks).await
    }

    /// Reject any name in `asset_names` that isn't a resolved node in
    /// the repo. Companion to [`Self::validate_partition_for_selection`]
    /// — same motivation: dispatch paths that fan out asynchronously
    /// (Direct `dispatch_materialization`'s fire-and-forget thread,
    /// Queued's record-write) would otherwise swallow the error.
    pub(crate) fn validate_assets_exist(&self, asset_names: &[String]) -> PyResult<()> {
        let state = self.resolved()?;
        resolve_selection(&state.node_map, asset_names)?;
        Ok(())
    }

    /// GIL-free. Used by gRPC `materialize` to expand empty selection
    /// ("materialize everything") into an explicit list before dispatch.
    pub(crate) fn asset_names(&self) -> PyResult<Vec<String>> {
        Ok(self
            .resolved()?
            .node_map
            .iter()
            .filter(|&(_k, n)| matches!(n, ResolvedNode::Asset(_)))
            .map(|(k, _n)| k.clone())
            .collect())
    }

    /// Returns a `Py<PyJob>` ready for GIL-bound use (e.g. `execute_run` on
    /// a launch thread).
    pub(crate) fn get_job(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyJob>> {
        let state = self.state.get_attached(py).ok_or_else(|| {
            ExecutionError::new_err("Repository not resolved — call resolve() first")
        })?;
        state
            .jobs
            .get(name)
            .map(|j| j.clone_ref(py))
            .ok_or_else(|| ExecutionError::new_err(format!("Job '{name}' not found")))
    }

    pub(crate) async fn get_backfill(
        &self,
        backfill_id: &str,
    ) -> PyResult<Option<PyBackfillStatusResult>> {
        let storage = self.resolved()?.storage.clone();
        let record = storage
            .get_backfill(backfill_id)
            .await
            .map_err(|e| ExecutionError::new_err(format!("Failed to get backfill: {e}")))?;
        Ok(record.map(|r| PyBackfillStatusResult {
            backfill_id: r.backfill_id,
            status: format!("{:?}", r.status),
            total_partitions: r.partition_keys.len(),
            completed_partitions: r.completed_partitions.len(),
            failed_partitions: r.failed_partitions.len(),
            canceled_partitions: r.canceled_partitions.len(),
            run_ids: r.run_ids,
            error: r.error,
            tags: r.tags,
            launched_by: r.launched_by.into(),
            action: r.action,
        }))
    }

    /// Load the original `BackfillRecord` and convert it to a
    /// `BackfillRequestData` for resubmission. Appends `tag_keys::RERUN_OF`
    /// pointing at the original backfill id.
    pub(crate) async fn build_rerun_request(
        &self,
        backfill_id: &str,
        dry_run: bool,
        launched_by: rivers_core::storage::LaunchedBy,
    ) -> PyResult<crate::daemon::BackfillRequestData> {
        let storage = self.resolved()?.storage.clone();
        let record = storage
            .get_backfill(backfill_id)
            .await
            .map_err(|e| ExecutionError::new_err(format!("Failed to load backfill: {e}")))?
            .ok_or_else(|| {
                ExecutionError::new_err(format!("backfill '{backfill_id}' not found"))
            })?;

        let partition_keys: Vec<PyPartitionKey> = record
            .partition_keys
            .iter()
            .map(PyPartitionKey::from)
            .collect();
        let strategy = PyBackfillStrategy::from_core(&record.strategy);
        let failure_policy = match record.failure_policy {
            BackfillFailurePolicy::Continue => "continue".to_string(),
            BackfillFailurePolicy::StopOnFailure => "stop_on_failure".to_string(),
        };
        let max_concurrency = record.max_concurrency.clamp(0, u32::MAX as i64) as u32;

        let mut tag_map: HashMap<String, String> = record.tags.into_iter().collect();
        tag_map.insert(tag_keys::RERUN_OF.to_string(), backfill_id.to_string());

        let target = match record.job_name {
            Some(name) => crate::daemon::RunType::Job(name),
            None => crate::daemon::RunType::Materialization(record.asset_selection),
        };

        Ok(crate::daemon::BackfillRequestData {
            target,
            partition_keys: Some(partition_keys),
            partition_range: None,
            strategy: Some(strategy),
            failure_policy: Some(failure_policy),
            max_concurrency,
            tags: Some(tag_map),
            launched_by,
            dry_run,
            backfill_id: None,
            // A Job target must still run it: see `backfill_inner`.
            action: record.action,
            config: None,
        })
    }

    /// Build a re-execution request from the stored `RunRecord`, reusing its
    /// partition key + tags (job runs replay as jobs, ad-hoc as materializations).
    /// Tags the new run with `RERUN_OF` = the original id.
    pub(crate) async fn build_run_rerun_request(
        &self,
        run_id: &str,
        launched_by: rivers_core::storage::LaunchedBy,
    ) -> PyResult<crate::daemon::RunRerunRequest> {
        let (storage, own_cl) = {
            let state = self.resolved()?;
            (state.storage.clone(), state.code_location_id.clone())
        };
        let record = storage
            .get_run(run_id)
            .await
            .map_err(|e| ExecutionError::new_err(format!("Failed to load run: {e}")))?
            .ok_or_else(|| ExecutionError::new_err(format!("run '{run_id}' not found")))?;
        // Storage is shared across locations: replaying a foreign run against
        // this repo's definitions would dispatch a mangled copy under our id.
        if record.code_location_id != own_cl {
            return Err(ExecutionError::new_err(format!(
                "run '{run_id}' belongs to code location '{}' — retry it from that location",
                record.code_location_id
            )));
        }

        let mut tags = record.tags;
        tags.retain(|(k, _)| k != tag_keys::RERUN_OF);
        tags.push((tag_keys::RERUN_OF.to_string(), run_id.to_string()));

        match record.job_name {
            Some(job_name) => {
                // The job is dispatched by name and runs its own verb.
                self.validate_job_verb(&job_name, record.action.as_deref())?;
                Ok(crate::daemon::RunRerunRequest::Job(
                    crate::daemon::RunRequestData {
                        run_key: None,
                        tags: Some(tags.into_iter().collect()),
                        partition_key: record.partition_key.as_ref().map(PyPartitionKey::from),
                        job_name: Some(job_name),
                        launched_by,
                        config: record.config,
                    },
                ))
            }
            None => Ok(crate::daemon::RunRerunRequest::Materialization(
                crate::daemon::MaterializationRequestData {
                    run_id: uuid::Uuid::new_v4().to_string(),
                    asset_selection: record.node_names,
                    partition_key: record.partition_key,
                    tags,
                    launched_by,
                    action: record.action,
                    config: record.config,
                },
            )),
        }
    }

    /// Build a backfill over an asset's not-yet-materialized partitions (full
    /// universe − materialized). Errors if the asset is unpartitioned or nothing
    /// is missing.
    pub(crate) async fn build_missing_backfill_request(
        &self,
        asset_key: &str,
        max_concurrency: u32,
        launched_by: rivers_core::storage::LaunchedBy,
    ) -> PyResult<crate::daemon::BackfillRequestData> {
        // Dynamic keys live in storage, not the def — capture the namespace and
        // fetch it from storage below; other kinds enumerate from the def here.
        enum Universe {
            Keys(Vec<PyPartitionKey>),
            Dynamic(String),
        }
        let (universe, storage, code_location_id) = {
            let state = self.resolved()?;
            let def = state
                .node_map
                .get(asset_key)
                .and_then(|n| n.partitions_def())
                .ok_or_else(|| {
                    ExecutionError::new_err(format!("asset '{asset_key}' is not partitioned"))
                })?;
            let universe = match def {
                PartitionsDefinition::Dynamic { name } => Universe::Dynamic(name.clone()),
                _ => Universe::Keys(def.get_partition_keys_window(0, def.partition_count())?),
            };
            (
                universe,
                state.storage.clone(),
                state.code_location_id.clone(),
            )
        };

        let ctx = rivers_core::storage::CodeLocationContext::new(code_location_id);
        let scoped = storage.for_code_location(&ctx);

        let all: Vec<PyPartitionKey> = match universe {
            Universe::Keys(keys) => keys,
            Universe::Dynamic(name) => scoped
                .get_dynamic_partitions(&name)
                .await
                .map_err(|e| {
                    ExecutionError::new_err(format!("Failed to load dynamic partitions: {e}"))
                })?
                .into_iter()
                .map(|k| PyPartitionKey::Single { key: vec![k] })
                .collect(),
        };

        let materialized: std::collections::HashSet<PartitionKey> = scoped
            .get_materialized_partitions(asset_key)
            .await
            .map_err(|e| {
                ExecutionError::new_err(format!("Failed to load materialized partitions: {e}"))
            })?
            .into_iter()
            .collect();

        let missing: Vec<PyPartitionKey> = all
            .into_iter()
            .filter(|pk| !materialized.contains(&rivers_core::storage::PartitionKey::from(pk)))
            .collect();

        if missing.is_empty() {
            return Err(ExecutionError::new_err(format!(
                "asset '{asset_key}' has no missing partitions to materialize"
            )));
        }

        Ok(crate::daemon::BackfillRequestData {
            target: crate::daemon::RunType::Materialization(vec![asset_key.to_string()]),
            partition_keys: Some(missing),
            partition_range: None,
            strategy: None,
            failure_policy: None,
            max_concurrency,
            tags: None,
            dry_run: false,
            backfill_id: None,
            launched_by,
            action: None,
            config: None,
        })
    }

    /// Cancel an in-progress backfill: signal the in-process coordinator
    /// (if any), settle or CAS the record via storage, and cancel any
    /// still-live child runs. A backfill whose runs all finished keeps its
    /// derived status — a late cancel prevented nothing. Returns whether a
    /// live coordinator was signalled.
    pub(crate) async fn cancel_backfill(&self, backfill_id: String) -> PyResult<bool> {
        let signaled = {
            let flags = self.backfill_cancel_flags.lock().unwrap();
            if let Some(flag) = flags.get(&backfill_id) {
                flag.store(true, std::sync::atomic::Ordering::Relaxed);
                true
            } else {
                false
            }
        };

        let storage = self.resolved()?.storage.clone();
        let status = storage
            .cancel_backfill(&backfill_id)
            .await
            .map_err(|e| ExecutionError::new_err(format!("Failed to cancel backfill: {e}")))?;

        // Cascade to live children — queued-mode runs would otherwise be
        // dequeued and executed regardless of the parent's status. A linked
        // id with no row yet is a run mid-creation: canceling it now plants
        // the flag the executor honors at startup.
        if status == BackfillStatus::Canceled {
            let record = storage
                .get_backfill(&backfill_id)
                .await
                .map_err(|e| ExecutionError::new_err(format!("{e}")))?;
            if let Some(record) = record {
                let runs = storage
                    .get_runs_by_ids(&record.run_ids, None)
                    .await
                    .map_err(|e| ExecutionError::new_err(format!("{e}")))?;
                let terminal: std::collections::HashSet<&str> = runs
                    .iter()
                    .filter(|r| {
                        matches!(
                            r.status,
                            RunStatus::Success | RunStatus::Failure | RunStatus::Canceled
                        )
                    })
                    .map(|r| r.run_id.as_str())
                    .collect();
                for run_id in &record.run_ids {
                    if terminal.contains(run_id.as_str()) {
                        continue;
                    }
                    if let Err(e) = self.cancel_run(run_id).await {
                        tracing::warn!(
                            target: "rivers::repo",
                            backfill_id = %backfill_id,
                            run_id = %run_id,
                            error = %e,
                            "backfill child run cancel failed"
                        );
                    }
                }
            }
        }

        Ok(signaled)
    }

    /// Request run cancellation: persist the cancel flag in storage and
    /// signal the run backend (Local in-process / K8s pod kill).
    pub(crate) async fn cancel_run(&self, run_id: &str) -> PyResult<bool> {
        let (storage, run_backend) = {
            let state = self.resolved()?;
            (state.storage.clone(), state.run_backend.clone())
        };

        // Flag first so a run dispatched concurrently still sees the cancel at
        // executor startup.
        storage
            .request_cancellation(run_id)
            .await
            .map_err(|e| ExecutionError::new_err(format!("failed to request cancellation: {e}")))?;

        // A still-queued run has no backend to signal — flip it to Canceled so
        // it leaves the queue instead of lingering flagged-but-Queued.
        if storage
            .cancel_queued_run(run_id)
            .await
            .map_err(|e| ExecutionError::new_err(format!("failed to cancel queued run: {e}")))?
        {
            tracing::info!(target: "rivers::repo", run_id = %run_id, "queued run canceled");
            return Ok(true);
        }

        match run_backend.terminate_run(run_id).await {
            Ok(true) => {
                tracing::info!(target: "rivers::repo", run_id = %run_id, "run terminated via backend")
            }
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(target: "rivers::repo", run_id = %run_id, error = %e, "backend terminate_run failed")
            }
        }

        tracing::info!(target: "rivers::repo", run_id = %run_id, "run cancellation requested");
        Ok(true)
    }

    /// Delete a terminal run and its history from storage. Errors if the
    /// run is still active; `Ok(false)` if no such run exists.
    pub(crate) async fn delete_run(&self, run_id: &str) -> PyResult<bool> {
        let storage = self.resolved()?.storage.clone();

        let deleted = storage
            .delete_run(run_id)
            .await
            .map_err(|e| ExecutionError::new_err(format!("failed to delete run: {e}")))?;
        if deleted {
            tracing::info!(target: "rivers::repo", run_id = %run_id, "run deleted");
        }
        Ok(deleted)
    }
}

/// Batched storage write.
pub(super) async fn submit_runs(
    state: &ResolvedState,
    runs: Vec<RunSubmission>,
    launched_by: LaunchedBy,
) -> PyResult<Vec<String>> {
    let (records, storage, dyn_checks) = {
        let graph = state
            .inner_repo
            .graph
            .as_ref()
            .ok_or_else(|| ExecutionError::new_err("Graph not resolved"))?;

        let now = now_ts();
        let all_names: Vec<String> = graph
            .node_indices()
            .map(|idx| graph[idx].name.clone())
            .collect();

        let mut records: Vec<RunRecord> = Vec::with_capacity(runs.len());
        let mut dyn_checks: Vec<DynamicKeyCheck> = Vec::new();

        for sub in &runs {
            let asset_names = sub.selection.clone().unwrap_or_else(|| all_names.clone());
            let action = sub.action.clone();
            validate_partition_for_verb(
                state,
                asset_names.iter().map(String::as_str),
                sub.partition_key.as_ref(),
                action.as_deref(),
            )?;
            dyn_checks.extend(dynamic_partition_checks(
                state,
                asset_names.iter().map(String::as_str),
                sub.partition_key.as_ref(),
            ));
            let run_id = uuid::Uuid::new_v4().to_string();
            let run_tags = sub.tags.clone().unwrap_or_default();
            let priority = priority_from_tags(&run_tags);
            let core_pk = sub.partition_key.as_ref().map(|pk| pk.into());

            records.push(RunRecord {
                run_id,
                code_location_id: state.code_location_id.clone(),
                job_name: sub.job_name.clone(),
                status: RunStatus::Queued,
                start_time: now,
                end_time: None,
                tags: run_tags,
                node_names: asset_names,
                priority,
                partition_key: core_pk,
                block_reason: None,
                launched_by: launched_by.clone(),
                action,
                config: sub.config.clone(),
            });
        }
        (records, state.storage.clone(), dyn_checks)
    };

    if let Some(first) = records.first() {
        verify_dynamic_partition_keys(&storage, &first.code_location_id, &dyn_checks).await?;
    }

    // Backfill runs carry the run_ids link on the backfill record in the
    // same transaction, so the link can never lie about what exists.
    match &launched_by {
        LaunchedBy::Backfill { backfill_id } => {
            let live = storage
                .enqueue_backfill_runs(&records, backfill_id)
                .await
                .map_err(|e| ExecutionError::new_err(format!("Failed to enqueue runs: {e}")))?;
            if !live {
                let canceled: Vec<rivers_core::storage::PartitionKey> = records
                    .iter()
                    .filter_map(|r| r.partition_key.as_ref())
                    .flat_map(|pk| pk.members())
                    .collect();
                let _ = storage
                    .update_backfill_progress(backfill_id, &[], &[], &[], &canceled)
                    .await;
                tracing::info!(
                    target: "rivers::repo",
                    backfill_id = %backfill_id,
                    count = records.len(),
                    "backfill canceled during submission — swept enqueued batch"
                );
            }
        }
        _ => storage
            .enqueue_runs(&records)
            .await
            .map_err(|e| ExecutionError::new_err(format!("Failed to enqueue runs: {e}")))?,
    }

    let run_ids: Vec<String> = records.into_iter().map(|r| r.run_id).collect();
    tracing::info!(
        target: "rivers::repo",
        count = run_ids.len(),
        "runs enqueued (batch)"
    );

    Ok(run_ids)
}

/// See [`RepoHandle::create_started_run`].
pub(super) async fn create_started_run(
    state: &ResolvedState,
    job_name: &str,
    partition_key: Option<&PyPartitionKey>,
    launched_by: LaunchedBy,
    run_id_override: Option<String>,
    config: Option<String>,
) -> PyResult<String> {
    let (record, storage, dyn_checks) = {
        let summary = state
            .jobs_info
            .get(job_name)
            .ok_or_else(|| ExecutionError::new_err(format!("Job '{job_name}' not found")))?;
        let asset_names = summary.asset_names.clone();
        let action = summary.action.clone();

        validate_partition_for_verb(
            state,
            asset_names.iter().map(String::as_str),
            partition_key,
            action.as_deref(),
        )?;
        let dyn_checks =
            dynamic_partition_checks(state, asset_names.iter().map(String::as_str), partition_key);

        let run_id = run_id_override.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let now = now_ts();
        let core_pk = partition_key.map(|pk| pk.into());

        let record = RunRecord {
            run_id,
            code_location_id: state.code_location_id.clone(),
            job_name: Some(job_name.to_string()),
            status: RunStatus::Started,
            start_time: now,
            end_time: None,
            tags: Vec::new(),
            node_names: asset_names,
            priority: 0,
            partition_key: core_pk,
            block_reason: None,
            launched_by,
            action,
            config,
        };
        (record, state.storage.clone(), dyn_checks)
    };

    verify_dynamic_partition_keys(&storage, &record.code_location_id, &dyn_checks).await?;

    storage
        .create_run(&record)
        .await
        .map_err(|e| ExecutionError::new_err(format!("Failed to create run: {e}")))?;

    tracing::info!(
        target: "rivers::repo",
        run_id = %record.run_id,
        job_name = %job_name,
        "started run created"
    );

    Ok(record.run_id)
}

/// See [`RepoHandle::create_materialization_run`].
pub(super) async fn create_materialization_run(
    state: &ResolvedState,
    asset_selection: Vec<String>,
    partition_key: Option<rivers_core::storage::PartitionKey>,
    tags: Vec<(String, String)>,
    launched_by: LaunchedBy,
    run_id: String,
    action: Option<String>,
    config: Option<String>,
) -> PyResult<()> {
    let priority = priority_from_tags(&tags);
    let record = RunRecord {
        run_id,
        code_location_id: state.code_location_id.clone(),
        job_name: None,
        status: RunStatus::Started,
        start_time: now_ts(),
        end_time: None,
        tags,
        node_names: asset_selection,
        priority,
        partition_key,
        block_reason: None,
        launched_by,
        action,
        config,
    };

    state
        .storage
        .create_run(&record)
        .await
        .map_err(|e| ExecutionError::new_err(format!("Failed to create run: {e}")))?;

    tracing::info!(
        target: "rivers::repo",
        run_id = %record.run_id,
        "materialization run created"
    );

    Ok(())
}

/// Serializes the first resolve, so concurrent first callers resolve once.
/// A call from the resolving thread itself, made by code the resolve runs
/// such as a resource's `setup()`, gets an error: it would wait for itself.
#[derive(Default)]
pub(super) struct ResolveLock {
    mutex: Mutex<()>,
    owner: Mutex<Option<ThreadId>>,
}

impl ResolveLock {
    pub(super) fn lock(&self, py: Python<'_>) -> PyResult<ResolveGuard<'_>> {
        let current = std::thread::current().id();
        if *self.owner() == Some(current) {
            return Err(ExecutionError::new_err(
                "CodeRepository is still resolving — do not use it from code that runs \
                 during resolve, such as a resource's setup()",
            ));
        }
        let guard = self
            .mutex
            .lock_py_attached(py)
            .unwrap_or_else(PoisonError::into_inner);
        *self.owner() = Some(current);
        Ok(ResolveGuard {
            lock: self,
            _guard: guard,
        })
    }

    fn owner(&self) -> MutexGuard<'_, Option<ThreadId>> {
        self.owner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Clears the owner before it releases the lock, also when the resolve
/// fails or panics.
pub(super) struct ResolveGuard<'a> {
    lock: &'a ResolveLock,
    _guard: MutexGuard<'a, ()>,
}

impl Drop for ResolveGuard<'_> {
    fn drop(&mut self) {
        *self.lock.owner() = None;
    }
}
