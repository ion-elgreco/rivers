//! Background daemon — runs schedules, sensors, and automation conditions on a tick loop.
//!
//! `DaemonHandle` spawns schedule, sensor, and condition evaluation loops on a Tokio
//! runtime in a background thread. Communicates results back via channels and uses
//! `CancellationToken` for graceful shutdown from `PyCodeRepository.stop_daemon()`.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

use chrono::Utc;
use pyo3::PyTypeInfo;
use pyo3::prelude::*;
use rivers_core::run_backend::{RunBackend, RunHealthStatus};
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use tokio_util::sync::CancellationToken;

use crate::automation::PyEvalMode;
use crate::repository::PyCodeRepository;
use crate::runtime::rt;
use crate::storage::{PyStorage, PyStorageType};

mod automation_condition;
mod automation_entry;
mod batch_writer;
mod dispatchers;
mod eval_dispatcher;
mod parse;
mod schedule;
mod sensors;
mod subdaemons;
mod subprocess_eval;
mod tick_processing;
mod types;

pub use subprocess_eval::{eval_schedule_in_subprocess, eval_sensor_in_subprocess};

use automation_condition::{ConditionEvalLoopConfig, condition_eval_loop};
use automation_entry::AutomationEntry;
use batch_writer::{spawn_condition_eval_writer, spawn_tick_writer};
pub(crate) use dispatchers::{BackfillDispatcherKind, RunDispatcherKind};
use eval_dispatcher::EvalDispatcher;
use schedule::ScheduleInfo;
use sensors::SensorInfo;
use subdaemons::{
    spawn_backfill_monitor, spawn_backfill_pickup_loop, spawn_run_queue_coordinator,
    spawn_schedule_sensor_loop,
};

pub(crate) use parse::{
    assemble_call_args, extract_sensor_outcome_from_parts, extract_tick_outcome_from_parts,
};
pub(crate) use types::{
    BackfillRequestData, BoxedPyFuture, ConditionEvalWriteMsg, GIL_SEMAPHORE,
    MaterializationRequestData, PrecomputedArgs, ResolvedEvalMode, RunRequestData, SensorOutcome,
    TickOutcome, TickWriteMsg,
};

// RunBackend uses RPITIT so isn't object-safe; enum dispatch instead.
// Kubernetes carries a much larger payload (~384 bytes); box it so the enum
// stays small for the common Local variant.
pub(crate) enum RunBackendKind {
    Local(crate::backends::local::LocalRunBackend),
    Kubernetes(Box<rivers_k8s::run_backend::K8sRunBackend>),
}

impl RunBackendKind {
    async fn launch(
        &self,
        run_info: &rivers_core::storage::CoordinatorRunInfo,
        ctx: &(dyn std::any::Any + Send + Sync),
    ) -> anyhow::Result<()> {
        match self {
            Self::Local(b) => b.launch(run_info, ctx).await,
            Self::Kubernetes(b) => b.launch(run_info, ctx).await,
        }
    }

    pub(crate) async fn terminate_run(&self, run_id: &str) -> anyhow::Result<bool> {
        match self {
            Self::Local(b) => b.terminate_run(run_id).await,
            Self::Kubernetes(b) => b.terminate_run(run_id).await,
        }
    }

    async fn check_run_health(&self, run_id: &str) -> anyhow::Result<RunHealthStatus> {
        match self {
            Self::Local(b) => b.check_run_health(run_id).await,
            Self::Kubernetes(b) => b.check_run_health(run_id).await,
        }
    }
}

struct AutomationDaemon {
    repo: Py<PyCodeRepository>,
    storage: Arc<SurrealStorage>,
    cancel: CancellationToken,
    /// Cancelled when the spawned `daemon_main_loop` task fully exits
    /// (after every subdaemon has joined and in-flight runs are drained).
    /// `stop()` / `Drop` await this so we don't return until the runtime is
    /// genuinely free of our work.
    done: CancellationToken,
    /// Set once `start()` has spawned `daemon_main_loop`. `Drop` only waits on
    /// `done` when started — otherwise the token never fires and the wait hangs.
    started: AtomicBool,
    /// Max ticks retained per automation. `None` disables pruning.
    max_ticks_retained: Option<usize>,
    is_memory_storage: bool,
    /// Interval between condition evaluation ticks. Default: 30s.
    condition_eval_interval: std::time::Duration,
}

impl AutomationDaemon {
    fn new(
        repo: Py<PyCodeRepository>,
        storage: Arc<SurrealStorage>,
        is_memory_storage: bool,
    ) -> Self {
        Self {
            repo,
            storage,
            cancel: crate::shutdown::drain_token().child_token(),
            done: CancellationToken::new(),
            started: AtomicBool::new(false),
            max_ticks_retained: Some(100),
            is_memory_storage,
            condition_eval_interval: std::time::Duration::from_secs(30),
        }
    }

    fn start(&mut self, py: Python<'_>) -> PyResult<()> {
        tracing::trace!(target: "rivers::dbg::daemon", "AutomationDaemon.start: ENTER");
        // Wrapped in `Arc` because the eval dispatcher reuses them per subprocess
        // submit (rebuilt as `(name, class, json_data)` triples in the worker).
        let resources: Arc<HashMap<String, Py<PyAny>>> = Arc::new({
            let repo_ref = self.repo.get();
            let guard = repo_ref.state.read().unwrap();
            guard
                .as_ref()
                .map(|s| {
                    s.resources
                        .iter()
                        .map(|(k, v)| (k.clone(), v.inner().clone_ref(py)))
                        .collect()
                })
                .unwrap_or_default()
        });

        let (schedules, sensors) = self.extract_automation_info(py, resources.as_ref());

        let (asset_conditions, upstream_partition_keys) = {
            let repo_ref = self.repo.get();
            let conditions = repo_ref.extract_asset_conditions();
            let upstream_pks = repo_ref.extract_upstream_partition_keys(&conditions);
            (conditions, upstream_pks)
        };

        if !asset_conditions.is_empty() {
            tracing::info!(
                target: "rivers::daemon",
                count = asset_conditions.len(),
                "tracking assets with automation conditions"
            );
        }

        let needs_loky = schedules
            .iter()
            .any(|s| matches!(s.eval_mode, ResolvedEvalMode::Subprocess))
            || sensors
                .iter()
                .any(|s| matches!(s.eval_mode, ResolvedEvalMode::Subprocess));

        let loky_executor: Option<Arc<Py<PyAny>>> = if needs_loky {
            let loky = py.import("loky").map_err(|_| {
                pyo3::exceptions::PyImportError::new_err(
                    "loky is required for subprocess eval mode. Install it: uv pip install rivers[loky]",
                )
            })?;
            let executor = loky.call_method0("get_reusable_executor")?;
            tracing::info!(target: "rivers::daemon", "initialized loky ProcessPoolExecutor for subprocess eval");
            Some(Arc::new(executor.unbind()))
        } else {
            None
        };

        for info in &schedules {
            tracing::info!(
                target: "rivers::daemon",
                name = %info.name,
                cron = %info.cron_schedule,
                eval_mode = ?info.eval_mode,
                "tracking schedule"
            );
        }
        for info in &sensors {
            tracing::info!(
                target: "rivers::daemon",
                name = %info.name,
                interval = ?info.minimum_interval,
                eval_mode = ?info.eval_mode,
                "tracking sensor"
            );
        }

        let run_queue_config = {
            let repo_ref = self.repo.get();
            repo_ref
                .run_queue_config
                .as_ref()
                .map(|c| c.borrow(py).to_core(py))
        };

        let repo = self.repo.clone_ref(py).into();
        let storage = self.storage.clone();
        let cancel = self.cancel.clone();

        let max_ticks_retained = self.max_ticks_retained;
        let is_memory_storage = self.is_memory_storage;
        let condition_eval_interval = self.condition_eval_interval;

        let done = self.done.clone();
        tracing::trace!(target: "rivers::dbg::daemon", "AutomationDaemon.start: spawning daemon_main_loop on rt()");
        let handle = rt().spawn(async move {
            tracing::trace!(target: "rivers::dbg::daemon", "daemon_main_loop task: STARTED");
            daemon_main_loop(DaemonLoopConfig {
                schedule_infos: schedules,
                sensor_infos: sensors,
                asset_conditions,
                upstream_partition_keys,
                repo,
                storage,
                cancel,
                loky_executor,
                resources,
                max_ticks_retained,
                is_memory_storage,
                condition_eval_interval,
                run_queue_config,
            })
            .await;
            tracing::trace!(target: "rivers::dbg::daemon", "daemon_main_loop task: RETURNED, signaling done");
            // Signal completion *after* every subdaemon has joined inside
            // `daemon_main_loop`. `stop()` awaits this token.
            done.cancel();
        });
        crate::shutdown::register_daemon_handle(handle, self.cancel.clone());
        self.started.store(true, Relaxed);
        tracing::trace!(target: "rivers::dbg::daemon", "AutomationDaemon.start: EXIT");

        Ok(())
    }

    fn stop(&self, py: Python<'_>) {
        tracing::trace!(target: "rivers::dbg::daemon", "AutomationDaemon.stop: ENTER, cancelling");
        self.cancel.cancel();
        // Block until `daemon_main_loop` has joined every subdaemon.
        // Released GIL because the wait runs on the shared tokio runtime —
        // some subdaemons may need to acquire GIL during their drain.
        let done = self.done.clone();
        tracing::trace!(target: "rivers::dbg::daemon", "AutomationDaemon.stop: detaching GIL, blocking on done.cancelled()");
        py.detach(|| {
            rt().block_on(done.cancelled());
        });
        tracing::trace!(target: "rivers::dbg::daemon", "AutomationDaemon.stop: EXIT");
    }

    fn extract_automation_info(
        &self,
        py: Python<'_>,
        resources: &HashMap<String, Py<PyAny>>,
    ) -> (Vec<ScheduleInfo>, Vec<SensorInfo>) {
        let repo_ref = self.repo.get();
        let schedules = repo_ref.extract_schedules(py, resources);
        let sensors = repo_ref.extract_sensors(py, resources);
        (schedules, sensors)
    }
}

pub(crate) fn resolve_eval_mode(
    py: Python,
    eval_fn: Option<&Py<PyAny>>,
    user_mode: &PyEvalMode,
) -> ResolvedEvalMode {
    let Some(f) = eval_fn else {
        return ResolvedEvalMode::SyncInProcess;
    };

    match user_mode {
        PyEvalMode::Subprocess => ResolvedEvalMode::Subprocess,
        PyEvalMode::Auto | PyEvalMode::InProcess => {
            let owned = Some(f.clone_ref(py));
            if crate::assets::decorator::is_coroutine_function(py, &owned) {
                ResolvedEvalMode::AsyncInProcess
            } else {
                ResolvedEvalMode::SyncInProcess
            }
        }
    }
}

pub(crate) fn precompute_args(
    py: Python,
    eval_fn: &Py<PyAny>,
    resources: &HashMap<String, Py<PyAny>>,
) -> Result<PrecomputedArgs, String> {
    let annotations =
        crate::executor::ops::get_annotations(py, eval_fn).map_err(|e| e.to_string())?;

    let sensor_ctx = crate::context::sensor::PySensorEvaluationContext::type_object(py);
    let schedule_ctx = crate::context::schedule::PyScheduleEvaluationContext::type_object(py);
    let ctx_types = [sensor_ctx.as_any(), schedule_ctx.as_any()];

    let mut config_instance = None;
    let mut resource_args: Vec<Py<PyAny>> = Vec::new();
    for (k, v) in annotations.iter() {
        let param_name: String = k.extract().map_err(|e: pyo3::PyErr| e.to_string())?;
        if param_name == "return" {
            continue;
        }
        if ctx_types
            .iter()
            .any(|ct| crate::executor::ops::annotation_is(&v, ct))
        {
            config_instance = crate::executor::ops::extract_config_from_annotation(py, &v, None)
                .map_err(|e| e.to_string())?;
        } else if let Some(resource) = resources.get(&param_name) {
            resource_args.push(resource.clone_ref(py));
        }
    }

    Ok(PrecomputedArgs {
        config_instance,
        resource_args,
    })
}

struct DaemonLoopConfig {
    schedule_infos: Vec<ScheduleInfo>,
    sensor_infos: Vec<SensorInfo>,
    asset_conditions: Vec<rivers_core::condition::AssetConditionInfo>,
    upstream_partition_keys: HashMap<String, HashSet<rivers_core::storage::PartitionKey>>,
    repo: Arc<Py<PyCodeRepository>>,
    storage: Arc<SurrealStorage>,
    cancel: CancellationToken,
    loky_executor: Option<Arc<Py<PyAny>>>,
    /// Registered resources, threaded to the eval dispatcher for subprocess
    /// transport. Same handles `precompute_args` saw at extraction time.
    resources: Arc<HashMap<String, Py<PyAny>>>,
    max_ticks_retained: Option<usize>,
    is_memory_storage: bool,
    condition_eval_interval: std::time::Duration,
    run_queue_config: Option<rivers_core::concurrency::RunQueueConfig>,
}

#[tracing::instrument(skip_all, target = "rivers::daemon", fields(
    schedules = config.schedule_infos.len(),
    sensors = config.sensor_infos.len(),
    conditions = config.asset_conditions.len(),
))]
async fn daemon_main_loop(config: DaemonLoopConfig) {
    let DaemonLoopConfig {
        schedule_infos,
        sensor_infos,
        asset_conditions,
        upstream_partition_keys,
        repo,
        storage,
        cancel,
        loky_executor,
        resources,
        max_ticks_retained,
        is_memory_storage,
        condition_eval_interval,
        run_queue_config,
    } = config;

    let (run_backend, code_location_id, repo_handle, gil_threads) = {
        let repo_ref = repo.get();
        let handle = repo_ref.handle();
        let gil_threads = repo_ref.gil_threads.clone();
        let state = repo_ref.state.read().unwrap();
        let s = state
            .as_ref()
            .expect("daemon spawned only after CodeRepository::resolve");
        (
            s.run_backend.clone(),
            s.code_location_id.clone(),
            handle,
            gil_threads,
        )
    };

    let mut automations: Vec<AutomationEntry> = Vec::new();

    for info in schedule_infos {
        match croner::parser::CronParser::builder()
            .seconds(croner::parser::Seconds::Optional)
            .build()
            .parse(&info.cron_schedule)
        {
            Ok(cron) => {
                let cron = Box::new(cron);
                let next = cron.find_next_occurrence(&Utc::now(), false).ok();
                automations.push(AutomationEntry::Schedule {
                    info,
                    cron,
                    next_occurrence: next,
                });
            }
            Err(e) => {
                tracing::error!(
                    target: "rivers::daemon",
                    name = %info.name,
                    cron = %info.cron_schedule,
                    error = %e,
                    "failed to parse cron expression"
                );
            }
        }
    }

    let handle = rivers_core::storage::ScopedStorageHandle::new(
        Arc::clone(&storage),
        rivers_core::storage::CodeLocationContext::new(code_location_id),
    );

    for info in sensor_infos {
        let ticks = handle
            .scoped()
            .get_ticks(&info.name, 1)
            .await
            .unwrap_or_default();
        automations.push(AutomationEntry::Sensor {
            cursor: ticks.first().and_then(|t| t.cursor.clone()),
            last_tick_time: ticks.first().map(|t| t.timestamp as f64),
            last_eval: None,
            in_flight: false,
            info,
        });
    }

    // Spawn background sub-daemons. Collect every JoinHandle so we can
    // synchronously drain them after the cancel token fires — without this
    // wait, `daemon.stop()` returns while the subdaemons are still ticking
    // on the shared `rt()` runtime, and their leftover work contaminates
    // the next test (storage ops on dropped DBs, GIL contention, etc.).
    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    let (tick_tx, tick_writer_handle) =
        spawn_tick_writer(handle.clone(), cancel.clone(), is_memory_storage);
    handles.push(tick_writer_handle);

    let run_queue_enabled = run_queue_config.is_some();
    let run_dispatcher = Arc::new(RunDispatcherKind::new(
        Arc::clone(&repo),
        repo_handle,
        Arc::clone(&storage),
        handle.code_location_id().to_string(),
        run_queue_enabled,
        gil_threads.clone(),
    ));
    let backfill_dispatcher = Arc::new(BackfillDispatcherKind::new_local(
        Arc::clone(&repo),
        gil_threads.clone(),
    ));
    let eval_dispatcher = Arc::new(EvalDispatcher::new(loky_executor, resources));

    if !asset_conditions.is_empty() {
        let max_evals_retained: Option<usize> = std::env::var("RIVERS_MAX_CONDITION_EVALS")
            .ok()
            .and_then(|v| v.parse().ok())
            .or(Some(100));
        let (eval_tx, eval_writer_handle) =
            spawn_condition_eval_writer(handle.clone(), cancel.clone(), max_evals_retained);
        handles.push(eval_writer_handle);
        handles.push(tokio::spawn(condition_eval_loop(ConditionEvalLoopConfig {
            conditions: asset_conditions,
            storage: handle.clone(),
            run_dispatcher: Arc::clone(&run_dispatcher),
            backfill_dispatcher: Arc::clone(&backfill_dispatcher),
            cancel: cancel.clone(),
            interval: condition_eval_interval,
            tick_tx: tick_tx.clone(),
            max_ticks_retained,
            eval_tx,
            max_evals_retained,
            upstream_partition_keys,
        })));
    }

    handles.push(spawn_backfill_pickup_loop(
        Arc::clone(&repo),
        handle.clone(),
        cancel.clone(),
        run_queue_enabled,
        gil_threads.clone(),
    ));

    if let Some(rq_config) = run_queue_config {
        handles.push(spawn_run_queue_coordinator(
            rq_config,
            handle.clone(),
            Arc::clone(&run_backend),
            Arc::clone(&repo),
            cancel.clone(),
        ));
    }

    handles.push(spawn_backfill_monitor(handle.clone(), cancel.clone()));

    handles.push(spawn_schedule_sensor_loop(
        automations,
        tick_tx,
        handle,
        run_dispatcher,
        backfill_dispatcher,
        eval_dispatcher,
        cancel.clone(),
        max_ticks_retained,
    ));

    tracing::trace!(target: "rivers::dbg::daemon", n_handles = handles.len(), "daemon_main_loop: all subdaemons spawned, awaiting cancel");
    cancel.cancelled().await;
    tracing::trace!(target: "rivers::dbg::daemon", n_handles = handles.len(), "daemon_main_loop: cancel observed, draining subdaemons");

    // Drain every subdaemon before returning. Each spawned task exits at
    // the top of its `tokio::select!` once `cancel` is observed — joining
    // here ensures `daemon.stop()` is true to its name.
    for (i, h) in handles.into_iter().enumerate() {
        tracing::trace!(target: "rivers::dbg::daemon", subdaemon_idx = i, "daemon_main_loop: awaiting subdaemon");
        let _ = h.await;
        tracing::trace!(target: "rivers::dbg::daemon", subdaemon_idx = i, "daemon_main_loop: subdaemon joined");
    }

    // Subdaemon loops are the only spawners and have all exited, so draining now
    // is race-free. On `spawn_blocking` because `drain()` joins (blocks).
    tracing::trace!(target: "rivers::dbg::daemon", "daemon_main_loop: draining in-flight runs");
    let drained = tokio::task::spawn_blocking(move || gil_threads.drain())
        .await
        .unwrap_or(0);
    if drained > 0 {
        tracing::info!(target: "rivers::shutdown", count = drained, kind = "daemon", "in-flight threads drained");
    }
    tracing::trace!(target: "rivers::dbg::daemon", "daemon_main_loop: ALL subdaemons drained, returning");
}

#[pyclass(name = "AutomationDaemon", module = "rivers._core")]
pub struct PyAutomationDaemon {
    inner: AutomationDaemon,
}

#[pymethods]
impl PyAutomationDaemon {
    #[new]
    #[pyo3(signature = (repo, storage, *, max_ticks_retained = Some(100), condition_eval_interval = "30s"))]
    fn new(
        repo: Py<PyCodeRepository>,
        storage: &PyStorage,
        max_ticks_retained: Option<usize>,
        condition_eval_interval: &str,
    ) -> PyResult<Self> {
        let interval =
            crate::utils::parse_duration("condition_eval_interval", condition_eval_interval)?;
        let is_memory = storage.storage_type == PyStorageType::Memory;
        let mut daemon = AutomationDaemon::new(repo, Arc::clone(storage.backend()), is_memory);
        daemon.max_ticks_retained = max_ticks_retained;
        daemon.condition_eval_interval = interval;
        Ok(Self { inner: daemon })
    }

    fn start(&mut self, py: Python<'_>) -> PyResult<()> {
        self.inner.start(py)
    }

    fn stop(&self, py: Python<'_>) -> PyResult<()> {
        self.inner.stop(py);
        Ok(())
    }
}

impl Drop for PyAutomationDaemon {
    fn drop(&mut self) {
        self.inner.cancel.cancel();
        // Safety net if dropped without `stop()`: wait for `daemon_main_loop`'s
        // drain. Skipped if never started (`done` never fires) or finalizing
        // (`try_attach` → None) — draining mid-finalize is the race we're closing.
        if !self.inner.started.load(Relaxed) {
            return;
        }
        let done = self.inner.done.clone();
        Python::try_attach(|py| {
            py.detach(|| rt().block_on(done.cancelled()));
        });
    }
}
