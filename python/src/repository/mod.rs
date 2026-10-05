//! CodeRepository — the top-level container that holds assets, jobs, schedules, and sensors.
//!
//! `PyCodeRepository` is the main pyclass. `resolve()` builds the `AssetGraph`, validates jobs,
//! and populates `ResolvedState` (graph + node_map + jobs). Provides `materialize()`, `observe()`,
//! `_start_grpc_server()` for the UI backend, and daemon start/stop for schedules/sensors/conditions.
pub mod resolved_node;

mod backfill;
mod graph;
mod handle;
mod launch;
mod resolve;
mod results;
mod validation;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use pyo3::prelude::*;

use crate::errors::{AssetDefinitionError, ExecutionError, NodeNotFoundError};
use rivers_core::assets::graph::{GraphTopology, TopologyNode};
use rivers_core::repo::CodeRepository;
use rivers_core::storage::{BackfillFailurePolicy, LaunchedBy, StorageBackend};

use crate::runtime::{io_rt, rt};

pub(crate) use rivers_core::storage::tag_keys;

pub(crate) use backfill::*;
pub(crate) use graph::*;
pub(crate) use handle::*;
pub use results::*;
pub(crate) use validation::*;

use crate::assets::decorator::PyAsset;
use crate::automation::schedule::{self, PyScheduleDefinition, PyScheduleTickResult};
use crate::automation::sensor::{self, PySensorDefinition, PySensorTickResult};
use crate::config::ResourceVariant;
use crate::config::run_config::{checked_run_config, run_config_to_json};
use crate::executor::Executor;
use crate::job::PyJob;
use crate::partitions::{
    PartitionsDefinition, PyBackfillStrategy, PyPartitionKey, PyPartitionKeyRange,
};
use crate::storage::{DetachOnClose, PyStorage};

use self::resolved_node::ResolvedNode;

#[pyclass(name = "CodeRepository", frozen, module = "rivers._core")]
pub struct PyCodeRepository {
    raw_assets: Vec<Py<PyAsset>>,
    raw_tasks: Vec<Py<PyAny>>,
    raw_jobs: Option<Vec<Py<PyJob>>>,
    pub(crate) raw_schedules: HashMap<String, Py<PyScheduleDefinition>>,
    pub(crate) raw_sensors: HashMap<String, Py<PySensorDefinition>>,
    default_executor: Option<Executor>,
    raw_resources: HashMap<String, ResourceVariant>,
    /// Named partition definitions referenced by `partitions_def="name"` on
    /// assets/tasks.
    raw_partition_defs: HashMap<String, Py<PartitionsDefinition>>,
    /// Named retry policies referenced by `retry="name"` on assets/jobs.
    raw_retries: HashMap<String, rivers_core::execution::retry::RetryPolicy>,
    /// Repo-wide retry default; the lowest-precedence rung (asset > job > this).
    raw_default_retry: Option<rivers_core::execution::retry::RetryRef>,
    pub(crate) run_queue_config: Option<Py<crate::concurrency::PyRunQueueConfig>>,
    pub(crate) run_backend_config: Option<Py<crate::concurrency::PyRunBackendConfig>>,
    pool_limits: Option<HashMap<String, i32>>,
    pub(crate) state: SharedState,
    resolve_lock: ResolveLock,
    backfill_cancel_flags:
        Arc<std::sync::Mutex<HashMap<String, Arc<std::sync::atomic::AtomicBool>>>>,
    /// Worker threads for this location's runs, shared into the daemon and gRPC
    /// dispatchers and drained at their shutdown. See [`crate::gil_threads`].
    pub(crate) gil_threads: crate::gil_threads::GilThreads,
}

/// Job validation is *not* included here — see
/// [`PyCodeRepository::validate_and_build_job_plans`].
struct BuiltGraph {
    inner_repo: CodeRepository,
    node_map: HashMap<String, ResolvedNode>,
    step_kinds: HashMap<String, rivers_core::execution::plan::StepKind>,
    graph_task_names: HashMap<String, Vec<String>>,
    multi_asset_groups: HashMap<String, String>,
    composition_order: HashMap<String, usize>,
}

#[pymethods]
impl PyCodeRepository {
    #[new]
    #[pyo3(signature = (assets, tasks=None, jobs=None, schedules=None, sensors=None, default_executor=None, resources=None, partition_defs=None, retries=None, default_retry_policy=None, run_queue=None, run_backend=None, pool_limits=None))]
    #[allow(clippy::too_many_arguments)]
    fn new<'py>(
        py: Python<'py>,
        assets: Vec<Bound<'py, PyAny>>,
        tasks: Option<Vec<Py<PyAny>>>,
        jobs: Option<Vec<Py<PyJob>>>,
        schedules: Option<Vec<Py<PyScheduleDefinition>>>,
        sensors: Option<Vec<Py<PySensorDefinition>>>,
        default_executor: Option<Executor>,
        resources: Option<HashMap<String, ResourceVariant>>,
        partition_defs: Option<HashMap<String, Py<PartitionsDefinition>>>,
        retries: Option<HashMap<String, crate::retry::PyRetryPolicy>>,
        default_retry_policy: Option<Bound<'py, PyAny>>,
        run_queue: Option<Py<crate::concurrency::PyRunQueueConfig>>,
        run_backend: Option<Py<crate::concurrency::PyRunBackendConfig>>,
        pool_limits: Option<HashMap<String, i32>>,
    ) -> PyResult<Self> {
        // Class-form assets (types subclassing Asset) desugar here — at
        // registration, not at class creation.
        let mut raw_assets: Vec<Py<PyAsset>> = Vec::with_capacity(assets.len());
        // Composed lists (`[*common, *team_a]`) may name one asset twice; that
        // is one definition, registered once. Distinct definitions sharing a
        // name still fail at resolve.
        let mut seen: HashSet<usize> = HashSet::new();
        for item in &assets {
            if !seen.insert(item.as_ptr() as usize) {
                continue;
            }
            if let Ok(instance) = item.extract::<Py<PyAsset>>() {
                raw_assets.push(instance);
            } else if let Ok(t) = item.cast::<pyo3::types::PyType>() {
                if !t.is_subclass_of::<PyAsset>()? {
                    return Err(AssetDefinitionError::new_err(format!(
                        "assets entry {item} is a class that does not subclass rivers.Asset \
                         (or MultiAsset / GraphAsset / ExternalAsset)"
                    )));
                }
                let desugared = crate::assets::class_form::desugar(item)?;
                raw_assets.push(desugared.extract(py)?);
            } else {
                return Err(AssetDefinitionError::new_err(format!(
                    "assets entries must be Asset instances or Asset subclasses, got {}",
                    item.get_type()
                )));
            }
        }
        Ok(Self {
            raw_assets,
            raw_tasks: tasks.unwrap_or_default(),
            raw_jobs: jobs,
            raw_schedules: schedules
                .unwrap_or_default()
                .into_iter()
                .map(|s| {
                    let name = s.get().name.clone();
                    (name, s)
                })
                .collect(),
            raw_sensors: sensors
                .unwrap_or_default()
                .into_iter()
                .map(|s| {
                    let name = s.get().name.clone();
                    (name, s)
                })
                .collect(),
            default_executor,
            raw_resources: resources.unwrap_or_default(),
            raw_partition_defs: partition_defs.unwrap_or_default(),
            raw_retries: retries
                .unwrap_or_default()
                .into_iter()
                .map(|(k, v)| (k, v.inner))
                .collect(),
            raw_default_retry: crate::retry::extract_retry_ref(default_retry_policy)?,
            run_queue_config: run_queue,
            run_backend_config: run_backend,
            pool_limits,
            state: SharedState::default(),
            resolve_lock: ResolveLock::default(),
            backfill_cancel_flags: Arc::new(std::sync::Mutex::new(HashMap::new())),
            gil_threads: crate::gil_threads::GilThreads::new(),
        })
    }

    /// Resolve the asset graph, initialize storage, and register asset catalog.
    /// If not called explicitly, auto-resolves with in-memory storage on first use.
    #[pyo3(signature = (storage=None))]
    fn resolve(&self, py: Python, storage: Option<&PyStorage>) -> PyResult<()> {
        self.resolve_inner(py, storage)?;
        Ok(())
    }

    /// Run the storage-independent validation pipeline: graph composition,
    /// partition / external / resource-reference validation, and per-job plan
    /// building. Does not initialize storage, run resource `setup()`, resolve
    /// IO handler `ResourceRef`s, register assets/pools, or persist topology.
    ///
    /// Intended for CLI / IDE / UI tools that want fast feedback without the
    /// side effects of a full :py:meth:`resolve`. Always re-runs (no idempotency
    /// guard) so it can be called repeatedly while the user edits code.
    fn validate(&self, py: Python) -> PyResult<()> {
        let bg = self.build_and_validate(py)?;
        let resolved_graph = bg
            .inner_repo
            .graph
            .as_ref()
            .expect("graph resolved by build_and_validate");
        self.validate_and_build_job_plans(
            resolved_graph,
            &bg.node_map,
            &bg.step_kinds,
            &bg.multi_asset_groups,
            &bg.composition_order,
        )?;
        Ok(())
    }

    #[getter]
    fn assets(&self, py: Python) -> PyResult<HashMap<String, Py<PyAsset>>> {
        let state = self.ensure_resolved()?;
        Ok(state
            .node_map
            .iter()
            .filter_map(|(k, node)| {
                if let ResolvedNode::Asset(asset_node) = node {
                    Some((k.clone(), asset_node.inner.clone_ref(py)))
                } else {
                    None
                }
            })
            .collect())
    }

    #[getter]
    fn storage(&self) -> PyResult<PyStorage> {
        let state = self.ensure_resolved()?;
        Ok(PyStorage {
            handle: DetachOnClose::new(rivers_core::storage::ScopedStorageHandle::new(
                Arc::clone(&state.storage),
                rivers_core::storage::CodeLocationContext::new(state.code_location_id.clone()),
            )),
            storage_type: state.storage_type,
        })
    }

    #[getter]
    fn schedules(&self) -> Vec<&Py<PyScheduleDefinition>> {
        self.raw_schedules.values().collect()
    }

    fn get_schedule(&self, name: &str) -> PyResult<&Py<PyScheduleDefinition>> {
        self.raw_schedules
            .get(name)
            .ok_or_else(|| NodeNotFoundError::new_err(format!("Schedule '{}' not found", name)))
    }

    #[getter]
    fn sensors(&self) -> Vec<&Py<PySensorDefinition>> {
        self.raw_sensors.values().collect()
    }

    fn get_sensor(&self, name: &str) -> PyResult<&Py<PySensorDefinition>> {
        self.raw_sensors
            .get(name)
            .ok_or_else(|| NodeNotFoundError::new_err(format!("Sensor '{}' not found", name)))
    }

    #[pyo3(signature = (name, cursor=None, last_tick_time=None))]
    pub(crate) fn evaluate_sensor(
        &self,
        py: Python,
        name: &str,
        cursor: Option<&str>,
        last_tick_time: Option<f64>,
    ) -> PyResult<PySensorTickResult> {
        let sens = self
            .raw_sensors
            .get(name)
            .ok_or_else(|| NodeNotFoundError::new_err(format!("Sensor '{}' not found", name)))?;
        let sens_ref = sens.borrow(py);
        let empty = HashMap::new();
        let state = self.state.get_attached(py);
        let resources = state.as_ref().map(|s| &s.resources).unwrap_or(&empty);
        sensor::evaluate_sensor(py, &sens_ref, cursor, last_tick_time, resources)
    }

    #[pyo3(signature = (name, execution_time=None))]
    pub(crate) fn evaluate_schedule(
        &self,
        py: Python,
        name: &str,
        execution_time: Option<&str>,
    ) -> PyResult<PyScheduleTickResult> {
        let sched = self
            .raw_schedules
            .get(name)
            .ok_or_else(|| NodeNotFoundError::new_err(format!("Schedule '{}' not found", name)))?;
        let sched_ref = sched.borrow(py);
        // RFC 3339 UTC, the form the daemon passes for a cron tick.
        let exec_time = execution_time.map(|s| s.to_string()).unwrap_or_else(|| {
            jiff::Timestamp::now()
                .strftime("%Y-%m-%dT%H:%M:%SZ")
                .to_string()
        });
        let empty = HashMap::new();
        let state = self.state.get_attached(py);
        let resources = state.as_ref().map(|s| &s.resources).unwrap_or(&empty);
        schedule::evaluate_schedule(py, &sched_ref, &exec_time, resources)
    }

    pub(crate) fn get_job(&self, py: Python, name: &str) -> PyResult<Py<PyJob>> {
        let state = self.ensure_resolved()?;
        state
            .jobs
            .get(name)
            .map(|j| j.clone_ref(py))
            .ok_or_else(|| NodeNotFoundError::new_err(format!("Job '{}' not found", name)))
    }

    /// Observe external assets through the run spine ( retrofit):
    /// `observe` is the built-in action, so this is `run_action("observe")`
    /// over the observable externals — with run records, cancellation, log
    /// capture, and the UI run page the old direct loop never had.
    #[pyo3(signature = (asset_names=None))]
    #[tracing::instrument(skip_all, target = "rivers::repo", name = "observe")]
    pub(crate) fn observe(
        &self,
        py: Python,
        asset_names: Option<Vec<String>>,
    ) -> PyResult<PyRunResult> {
        // Resolve the targets here rather than letting the action spine do it:
        // `observe` filters to what it can serve (names that aren't observable
        // externals are skipped, not fatal) and observing nothing is a
        // successful no-op — repos without externals call this too.
        let targets: Vec<String> = {
            let state = self.ensure_resolved()?;
            let mut names: Vec<String> = state
                .node_map
                .iter()
                .filter(|(name, node)| {
                    node.supports_action("observe")
                        && asset_names.as_ref().is_none_or(|n| n.contains(name))
                })
                .map(|(name, _)| name.clone())
                .collect();
            // node_map is a HashMap — sort so step order is reproducible.
            names.sort();
            names
        };
        if targets.is_empty() {
            return Ok(PyRunResult {
                success: true,
                run_id: String::new(),
                materialized_assets: vec![],
                failed_assets: vec![],
            });
        }
        self.run_action(
            py,
            "observe".to_string(),
            Some(targets),
            None,
            None,
            true,
            None,
            None,
            false,
        )
    }

    #[pyo3(signature = (selection=None, partition_key=None, tags=None, raise_on_error=true, config=None, run_id_override=None, include_upstream=false, resume=false, retry=None))]
    #[tracing::instrument(skip_all, target = "rivers::repo", name = "materialize")]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn materialize(
        &self,
        py: Python<'_>,
        selection: Option<Vec<String>>,
        partition_key: Option<PyPartitionKey>,
        tags: Option<Vec<(String, String)>>,
        raise_on_error: bool,
        config: Option<Py<PyAny>>,
        run_id_override: Option<String>,
        include_upstream: bool,
        resume: bool,
        retry: Option<Bound<'_, PyAny>>,
    ) -> PyResult<PyRunResult> {
        let retry = crate::retry::extract_retry_ref(retry)?;
        let config = run_config_to_json(py, config.as_ref())?;
        py.detach(|| {
            self.materialize_with_launcher(
                selection,
                partition_key,
                tags,
                raise_on_error,
                config,
                run_id_override,
                include_upstream,
                resume,
                retry,
                LaunchedBy::Manual { user: None },
            )
        })
    }

    /// Run a named action over an asset selection. Mirrors
    /// `materialize`, but the plan has one step per target with no upstream
    /// pull-in, and each step invokes the action's function with an
    /// `ActionContext`.
    #[pyo3(signature = (action, selection=None, partition_key=None, tags=None, raise_on_error=true, config=None, run_id_override=None, resume=false))]
    #[tracing::instrument(skip_all, target = "rivers::repo", name = "run_action")]
    pub(crate) fn run_action(
        &self,
        py: Python<'_>,
        action: String,
        selection: Option<Vec<String>>,
        partition_key: Option<PyPartitionKey>,
        tags: Option<Vec<(String, String)>>,
        raise_on_error: bool,
        config: Option<Py<PyAny>>,
        run_id_override: Option<String>,
        resume: bool,
    ) -> PyResult<PyRunResult> {
        let config = run_config_to_json(py, config.as_ref())?;
        py.detach(|| {
            self.run_action_with_launcher(
                action,
                selection,
                partition_key,
                tags,
                raise_on_error,
                config,
                run_id_override,
                resume,
                LaunchedBy::Manual { user: None },
            )
        })
    }

    /// Walks the registry chain `node.io_handler() → default`. Useful for
    /// debugging "which handler does asset X actually use?" without
    /// running execution.
    fn io_handler_for_output(&self, py: Python, name: String) -> PyResult<Py<PyAny>> {
        let state = self.ensure_resolved()?;
        let node = state.node_map.get(&name).ok_or_else(|| {
            NodeNotFoundError::new_err(format!("Node '{}' not found in repository", name))
        })?;
        Ok(state.io_handler_registry.for_output(py, node))
    }

    #[pyo3(signature = (name, partition_key=None, type_hint=None))]
    fn load_node(
        &self,
        py: Python,
        name: String,
        partition_key: Option<PyPartitionKey>,
        type_hint: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let state = self.ensure_resolved()?;
        let node = state.node_map.get(&name).ok_or_else(|| {
            NodeNotFoundError::new_err(format!("Node '{}' not found in repository", name))
        })?;
        let handler = state.io_handler_registry.for_output(py, node);
        let partition = crate::executor::ops::build_partition_context(node, &partition_key)?;
        let ctx = crate::context::io::PyInputContext {
            asset_name: name.clone(),
            downstream_asset: "__load_node__".to_string(),
            asset_metadata: node.metadata(),
            partition,
            type_hint,
        };
        handler.call_method1(py, "load_input", (ctx,))
    }

    #[pyo3(signature = (host, port, grpc_url, synthetic=None))]
    fn _start_ui_server(
        &self,
        py: Python,
        host: String,
        port: u16,
        grpc_url: String,
        synthetic: Option<String>,
    ) -> PyResult<()> {
        let storage_arc = Arc::clone(&self.ensure_resolved()?.storage);

        py.detach(|| {
            let graph = if let Some(ref scale) = synthetic {
                let n = rivers_ui::synthetic::parse_node_count(scale);
                let g = rivers_ui::synthetic::generate_synthetic_graph(n);
                Some(GraphTopology {
                    nodes: g
                        .nodes
                        .into_iter()
                        .map(|n| TopologyNode {
                            name: n.name,
                            kind: n
                                .kind
                                .parse()
                                .expect("synthetic graph produced invalid NodeKind"),
                            group: n.group,
                            parent_graph: n.parent_graph,
                        })
                        .collect(),
                    edges: g.edges,
                })
            } else {
                None
            };
            let graph = graph.map(Arc::new);

            // Dev mode: synthesize a one-entry registry pointing at the
            // in-process gRPC backend. In a real cluster this list comes from
            // the operator's `CodeLocationRegistry`; here we have no
            // operator, so the UI sees a single location named "default" in
            // namespace "dev".
            let module = std::env::var("RIVERS_MODULE").unwrap_or_default();
            let registry =
                rivers_ui::code_location_registry::Registry::dev_single(grpc_url, module);

            // Post-drain shutdown token: UI stays alive during drain so /readyz is reachable.
            let shutdown = crate::shutdown::shutdown_token().child_token();
            let handle = rt().spawn(async move {
                let auth = match rivers_ui::auth::AuthRuntime::from_env().await {
                    Ok(auth) => auth,
                    Err(e) => {
                        tracing::error!(target: "rivers::auth", error = %e, "invalid RIVERS_AUTH_* configuration; UI not started");
                        return;
                    }
                };
                if let Err(e) =
                    rivers_ui::start_server(storage_arc, graph, host, port, registry, auth, shutdown)
                        .await
                {
                    tracing::error!(target: "rivers::ui", error = %e, "UI server error");
                }
            });
            crate::shutdown::register_ui_handle(handle);
        });

        Ok(())
    }

    // ── Backfill API ──

    #[pyo3(signature = (
        selection = None,
        partition_keys = None,
        partition_range = None,
        strategy = None,
        failure_policy = "continue",
        max_concurrency = 4,
        tags = None,
        config = None,
        block = true,
        dry_run = false,
        action = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn backfill(
        &self,
        py: Python<'_>,
        selection: Option<Vec<String>>,
        partition_keys: Option<Vec<PyPartitionKey>>,
        partition_range: Option<PyPartitionKeyRange>,
        strategy: Option<PyBackfillStrategy>,
        failure_policy: &str,
        max_concurrency: u32,
        tags: Option<Vec<(String, String)>>,
        config: Option<Py<PyAny>>,
        block: bool,
        dry_run: bool,
        action: Option<String>,
    ) -> PyResult<PyBackfillResult> {
        // `None` is the explicit "every asset that defines the verb"; an empty
        // list is a caller's computed selection that came out empty.
        if let Some(verb) = &action
            && selection.as_ref().is_some_and(Vec::is_empty)
        {
            return Err(ExecutionError::new_err(format!(
                "backfill(action='{verb}') got an empty selection: pass selection=None \
                 to target every asset that defines it"
            )));
        }
        let config = run_config_to_json(py, config.as_ref())?;
        py.detach(|| {
            self.backfill_inner(
                crate::daemon::RunType::Materialization(selection.unwrap_or_default()),
                partition_keys,
                partition_range,
                strategy,
                failure_policy,
                max_concurrency,
                tags,
                config,
                block,
                dry_run,
                None,
                rivers_core::storage::LaunchedBy::default(),
                action,
            )
        })
    }

    /// Dispatches one materialize() call per partition key, tracking progress
    /// in storage. Called by `backfill(block=true)` or by the daemon loop
    /// for Requested backfills.
    pub(crate) fn execute_backfill(&self, py: Python<'_>, backfill_id: &str) -> PyResult<()> {
        py.detach(|| self.execute_backfill_inner(backfill_id))
    }

    /// The coordinator dequeues and executes each partition run respecting
    /// concurrency limits.
    pub(crate) fn execute_backfill_queued(
        &self,
        py: Python<'_>,
        backfill_id: &str,
    ) -> PyResult<()> {
        py.detach(|| self.execute_backfill_queued_inner(backfill_id))
    }

    pub(crate) fn cancel_backfill(&self, py: Python<'_>, backfill_id: String) -> PyResult<bool> {
        py.detach(|| {
            self.ensure_resolved()?;
            io_rt().block_on(self.handle().cancel_backfill(backfill_id))
        })
    }

    pub(crate) fn get_backfill(
        &self,
        py: Python<'_>,
        backfill_id: String,
    ) -> PyResult<Option<PyBackfillStatusResult>> {
        py.detach(|| {
            self.ensure_resolved()?;
            io_rt().block_on(self.handle().get_backfill(&backfill_id))
        })
    }

    /// Loads the original `BackfillRecord` from storage and resubmits it via
    /// `backfill()`, preserving asset selection, partition keys, strategy,
    /// failure policy, concurrency, tags and verb. Appends a `rivers/rerun_of`
    /// tag pointing at the original backfill id. A job backfill whose job now
    /// runs another verb is refused.
    #[pyo3(signature = (backfill_id, block = true, dry_run = false))]
    pub(crate) fn rerun_backfill(
        &self,
        py: Python<'_>,
        backfill_id: String,
        block: bool,
        dry_run: bool,
    ) -> PyResult<PyBackfillResult> {
        py.detach(|| {
            let state = self.ensure_resolved()?;
            let record = io_rt()
                .block_on(state.storage.get_backfill(&backfill_id))
                .map_err(|e| ExecutionError::new_err(format!("Failed to load backfill: {e}")))?
                .ok_or_else(|| {
                    ExecutionError::new_err(format!("backfill '{backfill_id}' not found"))
                })?;

            let partition_keys: Vec<PyPartitionKey> = record
                .partition_keys
                .iter()
                .map(PyPartitionKey::from)
                .collect();

            // Definitions may have evolved since the original backfill —
            // replay the partitions that still exist instead of failing the
            // whole rerun on the first retired key.
            let total = partition_keys.len();
            let (candidates, storage, code_location_id) = {
                let selection_names: Vec<String> = match &record.job_name {
                    Some(name) => state
                        .jobs_info
                        .get(name)
                        .map(|j| j.asset_names.clone())
                        .ok_or_else(|| {
                            ExecutionError::new_err(format!("Job '{name}' not found"))
                        })?,
                    None => record.asset_selection.clone(),
                };
                let candidates: Vec<(PyPartitionKey, Vec<DynamicKeyCheck>)> = partition_keys
                    .into_iter()
                    .filter(|key| {
                        validate_partition_for_verb(
                            &state,
                            selection_names.iter().map(String::as_str),
                            Some(key),
                            record.action.as_deref(),
                        )
                        .is_ok()
                    })
                    .map(|key| {
                        let checks = dynamic_partition_checks(
                            &state,
                            selection_names.iter().map(String::as_str),
                            Some(&key),
                        );
                        (key, checks)
                    })
                    .collect();
                (
                    candidates,
                    state.storage.clone(),
                    state.code_location_id.clone(),
                )
            };
            let mut partition_keys: Vec<PyPartitionKey> = Vec::with_capacity(candidates.len());
            for (key, checks) in candidates {
                // Only a genuinely unregistered key is retired — a storage
                // failure says nothing about the key and must abort the rerun.
                let retired = !checks.is_empty()
                    && !rt()
                        .block_on(unregistered_dynamic_keys(
                            &storage,
                            &code_location_id,
                            &checks,
                        ))?
                        .is_empty();
                if !retired {
                    partition_keys.push(key);
                }
            }
            if partition_keys.len() < total {
                tracing::warn!(
                    target: "rivers::repo",
                    backfill_id = %backfill_id,
                    dropped = total - partition_keys.len(),
                    kept = partition_keys.len(),
                    "rerun skipping partitions no longer valid for the current definitions"
                );
            }
            if partition_keys.is_empty() {
                return Err(ExecutionError::new_err(format!(
                    "Cannot rerun backfill '{backfill_id}': none of its {total} partitions \
                     are valid for the current definitions"
                )));
            }

            let strategy = PyBackfillStrategy::from_core(&record.strategy);
            let failure_policy = match record.failure_policy {
                BackfillFailurePolicy::Continue => "continue",
                BackfillFailurePolicy::StopOnFailure => "stop_on_failure",
            };
            let max_concurrency = record.max_concurrency.clamp(0, u32::MAX as i64) as u32;

            let mut tags = record.tags.clone();
            tags.push((tag_keys::RERUN_OF.to_string(), backfill_id));

            let target = match &record.job_name {
                Some(name) => crate::daemon::RunType::Job(name.clone()),
                None => crate::daemon::RunType::Materialization(record.asset_selection.clone()),
            };

            self.backfill_inner(
                target,
                Some(partition_keys),
                None,
                Some(strategy),
                failure_policy,
                max_concurrency,
                Some(tags),
                None,
                block,
                dry_run,
                None,
                rivers_core::storage::LaunchedBy::default(),
                record.action.clone(),
            )
        })
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        py: Python,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.teardown_resources(py);
        Ok(false) // don't suppress exceptions
    }

    fn shutdown(&self, py: Python) {
        self.teardown_resources(py);
    }

    /// Drop the resolved state and the storage it holds; the next call
    /// resolves again. The CLI removes its scratch store after this.
    fn _release_storage(&self, py: Python) {
        drop(self.state.replace(py, None));
    }

    /// Test helper. Only works when run_queue is configured. `job_name`
    /// submits the way the queued dispatcher does: the job's assets and verb;
    /// `config` is stored on the record for the dequeuing backend.
    #[pyo3(signature = (selection=None, partition_key=None, job_name=None, config=None))]
    fn _submit_run(
        &self,
        py: Python,
        selection: Option<Vec<String>>,
        partition_key: Option<PyPartitionKey>,
        job_name: Option<String>,
        config: Option<Py<PyAny>>,
    ) -> PyResult<PyRunHandle> {
        if !self.has_run_queue() {
            return Err(ExecutionError::new_err(
                "Cannot submit run: no RunQueueConfig set",
            ));
        }
        // Test helper — auto-resolve so Python tests don't have to call resolve()
        // explicitly first. Production callers (gRPC, daemon) always run after
        // resolve and bypass this helper.
        let config = {
            let state = self.ensure_resolved()?;
            checked_run_config(
                py,
                run_config_to_json(py, config.as_ref())?.as_deref(),
                &state.node_map,
                &state.resources,
            )?
        };
        py.detach(|| {
            let selection = match &job_name {
                Some(job) if selection.is_none() => self.handle().job_asset_names(job),
                _ => selection,
            };
            io_rt().block_on(self.submit_run(
                selection,
                partition_key.as_ref(),
                None,
                LaunchedBy::Manual { user: None },
                job_name,
                config,
            ))
        })
    }

    /// Returns the actual port bound (may differ from requested if it was in use).
    fn _start_grpc_server(slf: &Bound<'_, Self>, host: String, port: u16) -> PyResult<u16> {
        /// Max wait between cancellation and forced abort of both the
        /// in-flight serve future and the runtime's background tasks.
        /// Sized for production: long enough for in-flight RPCs to drain
        /// cleanly under SIGTERM, capped so a stuck client connection
        /// can't block shutdown indefinitely.
        const GRPC_SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

        tracing::trace!(target: "rivers::dbg::grpc", %host, port, "_start_grpc_server: ENTER");
        let py = slf.py();
        let binding = slf.borrow();
        let (storage, code_location_id) = {
            let state = binding.ensure_resolved()?;
            (Arc::clone(&state.storage), state.code_location_id.clone())
        };
        let repo_handle = binding.handle();
        let has_run_queue = binding.has_run_queue();
        let repo: Py<PyCodeRepository> = slf.clone().unbind();
        let gil_threads = binding.gil_threads.clone();
        let repo_arc = Arc::new(repo.clone_ref(py));
        let run_dispatcher = Arc::new(crate::daemon::RunDispatcherKind::new(
            Arc::clone(&repo_arc),
            repo_handle.clone(),
            storage,
            code_location_id,
            has_run_queue,
            gil_threads.clone(),
        ));
        let backfill_dispatcher = Arc::new(crate::daemon::BackfillDispatcherKind::new_local(
            repo_arc,
            gil_threads.clone(),
        ));

        let (port_tx, port_rx) = std::sync::mpsc::channel::<u16>();

        // Child of the process shutdown token: fires on graceful SIGTERM
        // AND when a new `_start_grpc_server` call supersedes us via
        // `register_grpc_handle` (which calls `cancel()` on the prior token).
        let server_cancel = crate::shutdown::shutdown_token().child_token();
        let cancel_for_callee = server_cancel.clone();
        let cancel_for_grace = server_cancel.clone();

        tracing::trace!(target: "rivers::dbg::grpc", "_start_grpc_server: spawning std::thread for new tokio Runtime");
        let gil_threads_for_server = gil_threads.clone();
        let handle = std::thread::spawn(move || {
            tracing::trace!(target: "rivers::dbg::grpc", "_start_grpc_server thread: creating Runtime");
            let rt = tokio::runtime::Runtime::new().expect("Failed to create gRPC runtime");
            tracing::trace!(target: "rivers::dbg::grpc", "_start_grpc_server thread: block_on(start_grpc_server)");
            rt.block_on(async move {
                let server_fut = crate::grpc_server::start_grpc_server(
                    repo,
                    repo_handle,
                    run_dispatcher,
                    backfill_dispatcher,
                    gil_threads_for_server,
                    host,
                    port,
                    port_tx,
                    cancel_for_callee,
                );
                tokio::pin!(server_fut);
                // Drop the server future after the grace window if cancel
                // fires — tonic's graceful shutdown waits indefinitely for
                // stale client connections, which would block tests by
                // tens of seconds without this cap.
                tokio::select! {
                    biased;
                    _ = async {
                        cancel_for_grace.cancelled().await;
                        tokio::time::sleep(GRPC_SHUTDOWN_GRACE).await;
                    } => {
                        tracing::trace!(target: "rivers::dbg::grpc", "gRPC server force-aborted after grace");
                    }
                    result = &mut server_fut => {
                        if let Err(e) = result {
                            tracing::error!(target: "rivers::grpc", error = %e, "gRPC server error");
                        }
                    }
                }
            });
            // Serving has stopped, so no handler can spawn more work — drain this
            // server's in-flight runs before tearing down the runtime.
            let drained = gil_threads.drain();
            if drained > 0 {
                tracing::info!(target: "rivers::shutdown", count = drained, kind = "grpc", "in-flight threads drained");
            }
            rt.shutdown_timeout(GRPC_SHUTDOWN_GRACE);
            tracing::trace!(target: "rivers::dbg::grpc", "_start_grpc_server thread: EXIT");
        });
        crate::shutdown::register_grpc_handle(handle, server_cancel);

        tracing::trace!(target: "rivers::dbg::grpc", "_start_grpc_server: waiting for port via mpsc");
        let actual_port = py
            .detach(move || port_rx.recv_timeout(std::time::Duration::from_secs(5)))
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to get gRPC port: {e}"))
            })?;

        tracing::trace!(target: "rivers::dbg::grpc", actual_port, "_start_grpc_server: EXIT (returning port)");
        Ok(actual_port)
    }

    /// Stop the gRPC server started by [`Self::_start_grpc_server`]: cancel and
    /// join its serve thread, which drains its in-flight runs. No-op if none
    /// running; idempotent.
    fn _stop_grpc_server(&self, py: Python<'_>) {
        py.detach(crate::shutdown::stop_grpc);
    }
}

pub fn register_repository_module(parent_module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_submodule!(parent_module, "repo", [
        PyCodeRepository as "CodeRepository",
        PyRunResult as "RunResult",
        PyRunHandle as "RunHandle",
        PyBackfillResult as "BackfillResult",
        PyBackfillStatusResult as "BackfillStatus",
    ])
}
