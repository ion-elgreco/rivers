//! CodeRepository — the top-level container that holds assets, jobs, schedules, and sensors.
//!
//! `PyCodeRepository` is the main pyclass. `resolve()` builds the `AssetGraph`, validates jobs,
//! and populates `ResolvedState` (graph + node_map + jobs). Provides `materialize()`, `observe()`,
//! `_start_grpc_server()` for the UI backend, and daemon start/stop for schedules/sensors/conditions.
pub mod resolved_node;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use pyo3::PyTypeInfo;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::errors::{
    AssetDefinitionError, AssetNotFoundError, ConfigurationError, ExecutionError,
    GraphValidationError, NodeNotFoundError, PartitionValidationError,
};
use rivers_core::assets::graph::{GraphTopology, NodeRef, TopologyNode, to_topology};
use rivers_core::repo::CodeRepository;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{
    BackfillFailurePolicy, BackfillRecord, BackfillStatus, EventRecord, EventType, LaunchedBy,
    PartitionKey, RunRecord, RunStatus, StorageBackend,
};

use crate::runtime::{io_rt, rt};

pub(crate) use rivers_core::storage::tag_keys;

/// Default run priority for backfill-spawned runs (-10 = lower than regular runs at 0).
pub(crate) const DEFAULT_BACKFILL_PRIORITY: i32 = -10;

pub(crate) fn priority_from_tags(tags: &[(String, String)]) -> i32 {
    tags.iter()
        .find(|(k, _)| k == tag_keys::PRIORITY)
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0)
}

/// Walk a selection of asset names and yield `(name, partitions_def)`
/// for each asset that has one. Single source of truth for the iteration
/// pattern shared by `validate_partition_for_selection` and
/// `validate_job_partition_compatibility`. GIL-free — reads the cached
/// `PartitionsDefinition` value off the resolved node.
fn iter_partitioned_assets<'a, I>(
    node_map: &'a HashMap<String, ResolvedNode>,
    asset_names: I,
) -> Vec<(&'a str, &'a PartitionsDefinition)>
where
    I: IntoIterator<Item = &'a str>,
{
    asset_names
        .into_iter()
        .filter_map(|name| {
            node_map
                .get(name)
                .and_then(|n| n.partitions_def())
                .map(|pd| (name, pd))
        })
        .collect()
}

/// Reject submissions that either (a) omit a partition key when the
/// selection contains partitioned assets, or (b) supply a key that doesn't
/// match the asset's partition definition (wrong shape, out-of-range time
/// window, unknown static key, etc.).
///
/// Single source of truth, called from `submit_run`, `submit_runs`, and
/// `materialize_with_launcher` so the run-queue path fails synchronously
/// (otherwise a queued run goes nowhere when dequeued) and the direct path
/// gets the same message before any storage write.
fn validate_partition_for_selection<'a>(
    state: &'a ResolvedState,
    asset_names: impl IntoIterator<Item = &'a str>,
    partition_key: Option<&PyPartitionKey>,
) -> PyResult<()> {
    let partitioned = iter_partitioned_assets(&state.node_map, asset_names);

    let Some(pk) = partition_key else {
        if partitioned.is_empty() {
            return Ok(());
        }
        let names: Vec<&str> = partitioned.iter().map(|(n, _)| *n).collect();
        return Err(ExecutionError::new_err(format!(
            "Cannot materialize without partition_key: assets {:?} have partition \
             definitions. Provide a partition_key or exclude them from selection.",
            names
        )));
    };

    for (name, pd) in &partitioned {
        if !pd.validate_partition_key(pk)? {
            return Err(ExecutionError::new_err(format!(
                "Invalid partition_key {:?} for asset '{}': not a member of its \
                 partition definition.",
                pk, name
            )));
        }
    }
    Ok(())
}

/// Reject a user-defined job whose partitioned assets can never share a
/// single partition_key. Folds `PartitionsDefinition::intersect` across
/// every partitioned asset; on failure the assets and the per-kind reason
/// (cadence mismatch, disjoint keys, etc.) get surfaced at `repo.resolve()`
/// instead of at every execute click.
///
/// Only enforced on user-defined `Job`s. `repo.materialize(selection=...)`
/// builds an ephemeral plan per call and isn't subject to this check —
/// callers can pick any compatible asset subset.
fn validate_job_partition_compatibility(
    job_name: &str,
    asset_names: &[String],
    node_map: &HashMap<String, ResolvedNode>,
) -> PyResult<()> {
    let partitioned = iter_partitioned_assets(node_map, asset_names.iter().map(String::as_str));

    if partitioned.len() < 2 {
        return Ok(());
    }

    let (first_name, first_pd) = &partitioned[0];
    let mut acc: PartitionsDefinition = (*first_pd).clone();
    let mut acc_assets: Vec<&str> = vec![*first_name];

    for (name, pd) in &partitioned[1..] {
        match acc.intersect(pd) {
            Ok(intersected) => {
                acc = intersected;
                acc_assets.push(*name);
            }
            Err(reason) => {
                return Err(PartitionValidationError::new_err(format!(
                    "Job '{}' has incompatible partition definitions: assets \
                     {:?} intersect, but adding '{}' fails: {}.",
                    job_name, acc_assets, name, reason
                )));
            }
        }
    }
    Ok(())
}

use crate::assets::decorator::{Asset, PyAsset};
use crate::assets::io_handler::IOHandler;
use crate::automation::schedule::{self, PyScheduleDefinition, PyScheduleTickResult};
use crate::automation::sensor::{self, PySensorDefinition, PySensorTickResult};
use crate::config::ResourceVariant;
use crate::context::asset::PyAssetExecutionContext;
use crate::executor::Executor;
use crate::executor::ops::{
    enumerate_params, get_annotations, is_context_annotation, merge_metadata, now_ts,
    register_assets_from_nodes,
};
use crate::job::PyJob;
use crate::partitions::mapping::PartitionMapping;
use crate::partitions::{
    PartitionsDefinition, PyBackfillStrategy, PyPartitionKey, PyPartitionKeyRange,
};
use crate::result_types;
use crate::storage::{PyStorage, PyStorageType};
use crate::task::{PyBashTask, PyTask};

use self::resolved_node::{ResolvedAsset, ResolvedBashTask, ResolvedNode, ResolvedTask};

#[pyclass(name = "RunResult", frozen, get_all, module = "rivers._core")]
pub struct PyRunResult {
    pub success: bool,
    pub run_id: String,
    pub materialized_assets: Vec<String>,
    pub failed_assets: Vec<(String, String)>,
}

#[pymethods]
impl PyRunResult {
    fn __repr__(&self) -> String {
        format!(
            "RunResult(success={}, run_id='{}', materialized={}, failed={})",
            self.success,
            self.run_id,
            self.materialized_assets.len(),
            self.failed_assets.len(),
        )
    }
}

#[pyclass(name = "RunHandle", frozen, module = "rivers._core")]
pub struct PyRunHandle {
    #[pyo3(get)]
    pub(crate) run_id: String,
    storage: Arc<SurrealStorage>,
}

#[pymethods]
impl PyRunHandle {
    #[getter]
    fn status(&self, py: Python) -> PyResult<String> {
        py.detach(|| {
            let run = rt()
                .block_on(self.storage.get_run(&self.run_id))
                .map_err(|e| ExecutionError::new_err(format!("Failed to get run status: {e}")))?
                .ok_or_else(|| {
                    ExecutionError::new_err(format!("Run '{}' not found", self.run_id))
                })?;
            Ok(format!("{:?}", run.status))
        })
    }

    /// Block until the run reaches a terminal state. Raises TimeoutError on timeout.
    #[pyo3(signature = (timeout=None))]
    fn wait(&self, py: Python, timeout: Option<f64>) -> PyResult<PyRunResult> {
        py.detach(|| {
            let start = std::time::Instant::now();
            let poll_interval = std::time::Duration::from_millis(100);

            loop {
                let run = rt()
                    .block_on(self.storage.get_run(&self.run_id))
                    .map_err(|e| ExecutionError::new_err(format!("Failed to poll run: {e}")))?
                    .ok_or_else(|| {
                        ExecutionError::new_err(format!("Run '{}' not found", self.run_id))
                    })?;

                match run.status {
                    RunStatus::Success | RunStatus::Failure | RunStatus::Canceled => {
                        return Ok(PyRunResult {
                            success: run.status == RunStatus::Success,
                            run_id: run.run_id,
                            materialized_assets: run.node_names,
                            failed_assets: vec![],
                        });
                    }
                    _ => {}
                }

                if let Some(t) = timeout
                    && start.elapsed().as_secs_f64() >= t
                {
                    return Err(pyo3::exceptions::PyTimeoutError::new_err(format!(
                        "Timed out waiting for run '{}' after {t}s",
                        self.run_id
                    )));
                }

                std::thread::sleep(poll_interval);
            }
        })
    }

    fn cancel(&self, py: Python) -> PyResult<()> {
        py.detach(|| {
            let run = rt()
                .block_on(self.storage.get_run(&self.run_id))
                .map_err(|e| ExecutionError::new_err(format!("Failed to get run: {e}")))?
                .ok_or_else(|| {
                    ExecutionError::new_err(format!("Run '{}' not found", self.run_id))
                })?;

            match run.status {
                RunStatus::Success | RunStatus::Failure | RunStatus::Canceled => return Ok(()),
                _ => {}
            }

            io_rt()
                .block_on(self.storage.update_run_status(
                    &self.run_id,
                    RunStatus::Canceled,
                    Some(now_ts()),
                ))
                .map_err(|e| ExecutionError::new_err(format!("Failed to cancel run: {e}")))?;
            Ok(())
        })
    }

    fn __repr__(&self) -> String {
        format!("RunHandle(run_id='{}')", self.run_id)
    }
}

#[pyclass(name = "BackfillResult", frozen, get_all, module = "rivers._core")]
pub struct PyBackfillResult {
    pub backfill_id: String,
    pub num_partitions: usize,
    pub num_runs: usize,
    pub status: String,
    pub completed: usize,
    pub failed: usize,
    pub canceled: usize,
    pub run_ids: Vec<String>,
    pub is_dry_run: bool,
    pub partition_keys: Vec<PyPartitionKey>,
}

#[pymethods]
impl PyBackfillResult {
    fn __repr__(&self) -> String {
        if self.is_dry_run {
            format!(
                "BackfillResult(dry_run, partitions={}, runs={})",
                self.num_partitions, self.num_runs,
            )
        } else {
            format!(
                "BackfillResult(id='{}', status='{}', completed={}, failed={}, canceled={})",
                self.backfill_id, self.status, self.completed, self.failed, self.canceled,
            )
        }
    }
}

#[pyclass(name = "BackfillStatus", frozen, get_all, module = "rivers._core")]
pub struct PyBackfillStatusResult {
    pub backfill_id: String,
    pub status: String,
    pub total_partitions: usize,
    pub completed_partitions: usize,
    pub failed_partitions: usize,
    pub canceled_partitions: usize,
    pub run_ids: Vec<String>,
    pub error: Option<String>,
    pub tags: Vec<(String, String)>,
}

#[pymethods]
impl PyBackfillStatusResult {
    fn __repr__(&self) -> String {
        format!(
            "BackfillStatus(id='{}', status='{}', completed={}/{}, failed={})",
            self.backfill_id,
            self.status,
            self.completed_partitions,
            self.total_partitions,
            self.failed_partitions,
        )
    }
}

fn append_deps(
    py: Python,
    deps: &mut Vec<NodeRef>,
    func: &Py<PyAny>,
    resource_keys: &HashSet<&String>,
) -> PyResult<()> {
    for (name, annotation) in enumerate_params(py, func)? {
        if name == "return" || name == "self" {
            continue;
        }
        let is_ctx = annotation
            .as_ref()
            .is_some_and(|a| is_context_annotation(py, a));
        if is_ctx || name == "context" {
            continue;
        }
        if resource_keys.contains(&name) {
            continue;
        }
        deps.push(NodeRef::ByName(name));
    }
    Ok(())
}

/// Reject any non-context, non-upstream parameter on a node that doesn't
/// reference a known resource key.
fn validate_resource_references(
    py: Python,
    node_map: &HashMap<String, ResolvedNode>,
    resource_keys: &HashSet<&String>,
    composition_task_names: &HashSet<String>,
) -> PyResult<()> {
    let node_names: HashSet<&str> = node_map.keys().map(|s| s.as_str()).collect();

    for (node_name, node) in node_map {
        // Tasks whose deps come from composition bindings have positional
        // params that don't match asset/resource names — skip them.
        if composition_task_names.contains(node_name) {
            continue;
        }

        // BashTask / ExternalAsset without observe_fn have no callable to
        // introspect — nothing to validate.
        let func = match node {
            ResolvedNode::BashTask(_) => continue,
            _ => match node.annotations(py)? {
                Some(_) => node.callable(py)?,
                None => continue,
            },
        };

        let mut is_first_param = true;
        for (param_name, annotation) in enumerate_params(py, &func)? {
            if param_name == "return" || param_name == "self" {
                continue;
            }

            let is_ctx = annotation
                .as_ref()
                .is_some_and(|a| is_context_annotation(py, a));

            if is_first_param {
                is_first_param = false;
                if is_ctx || (param_name == "context" && !node_names.contains(param_name.as_str()))
                {
                    continue;
                }
            } else if is_ctx {
                continue; // context in wrong position — execute_step will catch this
            }

            if node_names.contains(param_name.as_str()) {
                continue;
            }

            if resource_keys.contains(&param_name) {
                continue;
            }

            let available: Vec<&str> = resource_keys.iter().map(|s| s.as_str()).collect();
            return Err(ConfigurationError::new_err(format!(
                "Node '{}': parameter '{}' does not match any upstream asset or resource. \
                 Available resources: {:?}",
                node_name, param_name, available
            )));
        }
    }

    Ok(())
}

fn validate_schedule_sensor_resource_references(
    py: Python,
    schedules: &HashMap<String, Py<PyScheduleDefinition>>,
    sensors: &HashMap<String, Py<PySensorDefinition>>,
    resource_keys: &HashSet<&String>,
) -> PyResult<()> {
    for (name, schedule) in schedules {
        let schedule_ref = schedule.borrow(py);
        if let Some(ref eval_fn) = schedule_ref.evaluation_fn {
            validate_eval_fn_resources(py, eval_fn, name, "Schedule", resource_keys)?;
        }
    }

    for (name, sensor) in sensors {
        let sensor_ref = sensor.borrow(py);
        if let Some(ref eval_fn) = sensor_ref.evaluation_fn {
            validate_eval_fn_resources(py, eval_fn, name, "Sensor", resource_keys)?;
        }
    }

    Ok(())
}

/// Sensors must declare *some* run target — either `job_name` or
/// `asset_selection`. Without one, a tick has nothing to dispatch against
/// and the daemon would silently fail every fired RunRequest.
///
/// Also rejects targets that don't exist in the resolved repo: a typo'd
/// `job_name` or `asset_selection` entry would otherwise resolve fine
/// and produce stuck queued runs (Queued mode) or logged-not-surfaced
/// errors (Direct mode) at every tick. The check fires once at
/// resolve time so the failure mode is "your repo doesn't define X"
/// instead of silent dispatch failure forever.
fn validate_sensor_run_targets(
    py: Python,
    sensors: &HashMap<String, Py<PySensorDefinition>>,
    asset_names: &HashSet<&str>,
    job_names: &HashSet<&str>,
) -> PyResult<()> {
    for (name, sensor) in sensors {
        let s = sensor.borrow(py);
        let has_job = s.job_name.as_ref().is_some_and(|j| !j.is_empty());
        let has_selection = s.asset_selection.as_ref().is_some_and(|a| !a.is_empty());
        if !has_job && !has_selection {
            return Err(crate::errors::SensorDefinitionError::new_err(format!(
                "Sensor '{}' must declare either `job_name` or `asset_selection` — \
                 the daemon has no target to dispatch against.",
                name
            )));
        }

        if let Some(job_name) = s.job_name.as_ref().filter(|j| !j.is_empty())
            && !job_names.contains(job_name.as_str())
        {
            return Err(crate::errors::SensorDefinitionError::new_err(format!(
                "Sensor '{}' references unknown job '{}'. Define the job and \
                 add it to `CodeRepository(jobs=...)`.",
                name, job_name
            )));
        }

        if let Some(selection) = s.asset_selection.as_ref() {
            for asset in selection {
                if !asset_names.contains(asset.as_str()) {
                    return Err(crate::errors::SensorDefinitionError::new_err(format!(
                        "Sensor '{}' references unknown asset '{}' in \
                         `asset_selection`.",
                        name, asset
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Schedules carry a mandatory `job_name`; reject schedules whose
/// target isn't a user-defined job in this repo. Same motivation as
/// [`validate_sensor_run_targets`]'s existence check — a typo'd
/// `job_name` would otherwise produce stuck/failed runs at every
/// cron tick.
fn validate_schedule_run_targets(
    py: Python,
    schedules: &HashMap<String, Py<PyScheduleDefinition>>,
    job_names: &HashSet<&str>,
) -> PyResult<()> {
    for (name, schedule) in schedules {
        let s = schedule.borrow(py);
        if !job_names.contains(s.job_name.as_str()) {
            return Err(crate::errors::ScheduleDefinitionError::new_err(format!(
                "Schedule '{}' references unknown job '{}'. Define the job \
                 and add it to `CodeRepository(jobs=...)`.",
                name, s.job_name
            )));
        }
    }
    Ok(())
}

fn validate_eval_fn_resources(
    py: Python,
    eval_fn: &Py<PyAny>,
    name: &str,
    kind: &str,
    resource_keys: &HashSet<&String>,
) -> PyResult<()> {
    let annotations = get_annotations(py, eval_fn)?;

    let mut is_first = true;
    for (k, v) in annotations.iter() {
        let param_name: String = k.extract()?;
        if param_name == "return" {
            continue;
        }

        if is_first {
            is_first = false;
            if is_context_annotation(py, &v) || param_name == "context" {
                continue;
            }
        }

        if resource_keys.contains(&param_name) {
            continue;
        }

        let available: Vec<&str> = resource_keys.iter().map(|s| s.as_str()).collect();
        return Err(ConfigurationError::new_err(format!(
            "{} '{}': parameter '{}' does not match any known resource. \
             Available resources: {:?}",
            kind, name, param_name, available
        )));
    }

    Ok(())
}

pub(crate) struct UnresolvedGraph {
    pub graph: BTreeMap<String, Vec<NodeRef>>,
    pub node_map: HashMap<String, ResolvedNode>,
    /// Task names whose deps come from graph asset composition bindings.
    pub composition_task_names: HashSet<String>,
    /// graph_name → namespaced task names (for io_handler_override)
    pub graph_task_names: HashMap<String, Vec<String>>,
    /// Step kinds for fan-out/collect steps
    pub step_kinds: HashMap<String, rivers_core::execution::plan::StepKind>,
}

pub(crate) fn build_unresolved_graph(
    py: Python,
    assets: &[Py<PyAsset>],
    tasks: &[Py<PyAny>],
    resource_keys: &HashSet<&String>,
) -> PyResult<UnresolvedGraph> {
    let mut unresolved_graph: BTreeMap<String, Vec<NodeRef>> = BTreeMap::new();
    let mut node_map: HashMap<String, ResolvedNode> = HashMap::new();

    for decorator_py in assets {
        let inner_asset = &decorator_py.borrow(py).inner;
        let mut deps = Vec::new();

        match inner_asset {
            Asset::Single(single_asset) => {
                let func = single_asset.wraps.as_ref().unwrap();
                append_deps(py, &mut deps, func, resource_keys)?;

                // Add lineage-only deps from explicit deps list.
                for dep_name in &single_asset.dep_only_names {
                    deps.push(NodeRef::ByName(dep_name.clone()));
                }

                let asset_name = single_asset.name.clone().unwrap();
                unresolved_graph.insert(asset_name.clone(), deps);
                node_map.insert(
                    asset_name,
                    ResolvedNode::Asset(Box::new(ResolvedAsset::new(
                        py,
                        decorator_py.clone_ref(py),
                        None,
                    )?)),
                );
            }
            Asset::Multi(multi_asset) => {
                let func = multi_asset.wraps.as_ref().unwrap();
                append_deps(py, &mut deps, func, resource_keys)?;

                // Top-level lineage-only deps apply to every output.
                for dep_name in &multi_asset.dep_only_names {
                    deps.push(NodeRef::ByName(dep_name.clone()));
                }

                for inner in &multi_asset.assets {
                    let asset_name = inner.name.clone().unwrap();
                    let output_name = asset_name.clone();
                    // Per-output lineage-only deps add edges only to this output.
                    let mut per_output_deps = deps.clone();
                    for dep_name in &inner.dep_only_names {
                        let already_present = per_output_deps.iter().any(
                            |n| matches!(n, NodeRef::ByName(existing) if existing == dep_name),
                        );
                        if !already_present {
                            per_output_deps.push(NodeRef::ByName(dep_name.clone()));
                        }
                    }
                    unresolved_graph.insert(asset_name.clone(), per_output_deps);
                    node_map.insert(
                        asset_name,
                        ResolvedNode::Asset(Box::new(ResolvedAsset::new(
                            py,
                            decorator_py.clone_ref(py),
                            Some(output_name),
                        )?)),
                    );
                }
            }
            Asset::Graph(graph_asset) => {
                let func = graph_asset.wraps.as_ref().unwrap();
                append_deps(py, &mut deps, func, resource_keys)?;

                // Add lineage-only deps from explicit deps list.
                for dep_name in &graph_asset.dep_only_names {
                    deps.push(NodeRef::ByName(dep_name.clone()));
                }

                // Graph asset depends on all its internal tasks so it
                // executes after them in the plan.
                for invocation in &graph_asset.invocations {
                    deps.push(NodeRef::ByName(invocation.name.clone()));
                }

                let asset_name = graph_asset.name.clone().unwrap();
                unresolved_graph.insert(asset_name.clone(), deps);
                node_map.insert(
                    asset_name,
                    ResolvedNode::Asset(Box::new(ResolvedAsset::new(
                        py,
                        decorator_py.clone_ref(py),
                        None,
                    )?)),
                );
            }
            Asset::External(ext) => {
                let asset_name = ext.name.clone().unwrap();
                unresolved_graph.insert(asset_name.clone(), Vec::new());
                node_map.insert(
                    asset_name,
                    ResolvedNode::Asset(Box::new(ResolvedAsset::new(
                        py,
                        decorator_py.clone_ref(py),
                        None,
                    )?)),
                );
            }
        };
    }

    // Build composition-derived dependency overrides from graph assets.
    // Invocation names are namespaced as "{graph_name}/{task_name}" for tasks.
    use rivers_core::composition::{InputBinding, InvocationKind};
    use rivers_core::execution::plan::StepKind;
    let mut composition_bindings: HashMap<String, Vec<InputBinding>> = HashMap::new();
    // bare task name → list of namespaced names
    let mut task_namespaced_entries: HashMap<String, Vec<String>> = HashMap::new();
    // graph_name → list of namespaced task names (for post-resolve io_handler_override)
    let mut graph_task_names: HashMap<String, Vec<String>> = HashMap::new();
    let mut step_kinds: HashMap<String, StepKind> = HashMap::new();
    // Virtual nodes that need graph entries but no node_map entries.
    let mut collect_steps: HashMap<String, Vec<NodeRef>> = HashMap::new();
    // Per-graph inheritance maps so internal tasks pick up the parent's
    // partition / io / metadata configuration.
    let mut graph_partitions_def: HashMap<String, Py<PartitionsDefinition>> = HashMap::new();
    let mut graph_partition_mappings: HashMap<String, HashMap<String, PartitionMapping>> =
        HashMap::new();
    let mut graph_input_io_handlers: HashMap<String, HashMap<String, IOHandler>> = HashMap::new();
    let mut graph_input_metadata: HashMap<String, HashMap<String, HashMap<String, String>>> =
        HashMap::new();
    for decorator_py in assets {
        let inner_asset = &decorator_py.borrow(py).inner;
        if let Asset::Graph(graph_asset) = inner_asset {
            let graph_name = graph_asset.name.clone().unwrap_or_default();
            if let Some(ref pd) = graph_asset.partitions_def {
                graph_partitions_def.insert(graph_name.clone(), pd.clone_ref(py));
            }
            if let Some(ref pm) = graph_asset.partition_mappings {
                graph_partition_mappings.insert(graph_name.clone(), pm.0.clone());
            }
            if !graph_asset.input_io_handlers.is_empty() {
                graph_input_io_handlers.insert(
                    graph_name.clone(),
                    graph_asset
                        .input_io_handlers
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone_ref(py)))
                        .collect(),
                );
            }
            if !graph_asset.input_metadata.is_empty() {
                graph_input_metadata.insert(graph_name.clone(), graph_asset.input_metadata.clone());
            }
            for invocation in &graph_asset.invocations {
                let kind = StepKind::from(&invocation.invocation_kind);
                if kind != StepKind::Normal {
                    step_kinds.insert(invocation.name.clone(), kind);
                }
                // Register collect/collect_stream as virtual nodes with dep on mapped step
                match &invocation.invocation_kind {
                    InvocationKind::Collect { mapped_node }
                    | InvocationKind::CollectStream { mapped_node, .. } => {
                        collect_steps.insert(
                            invocation.name.clone(),
                            vec![NodeRef::ByName(mapped_node.clone())],
                        );
                    }
                    _ => {}
                }

                // Only tasks are namespaced; asset invocations keep bare names.
                // Skip collect steps — they're virtual and handled above.
                if matches!(
                    invocation.node_type,
                    rivers_core::composition::InvokedNodeType::Task
                ) && !matches!(
                    invocation.invocation_kind,
                    InvocationKind::Collect { .. } | InvocationKind::CollectStream { .. }
                ) {
                    let bare_name = invocation.name.rsplit('/').next().unwrap().to_string();
                    task_namespaced_entries
                        .entry(bare_name)
                        .or_default()
                        .push(invocation.name.clone());
                    graph_task_names
                        .entry(graph_name.clone())
                        .or_default()
                        .push(invocation.name.clone());
                }
                composition_bindings
                    .insert(invocation.name.clone(), invocation.input_bindings.clone());
            }
        }
    }
    for (name, deps) in collect_steps {
        unresolved_graph.insert(name, deps);
    }
    let composition_task_names: HashSet<String> = composition_bindings.keys().cloned().collect();

    fn build_remap_from_bindings(
        py: Python,
        bindings: Vec<InputBinding>,
        wraps: &Option<Py<PyAny>>,
    ) -> PyResult<(Vec<NodeRef>, HashMap<String, String>)> {
        let mut remap = HashMap::new();
        let mut all_deps: Vec<NodeRef> = Vec::new();

        let named: Vec<_> = bindings.iter().filter(|b| b.param_name.is_some()).collect();
        let positional: Vec<_> = bindings.iter().filter(|b| b.param_name.is_none()).collect();

        let named_params: HashSet<String> = named
            .iter()
            .map(|b| b.param_name.clone().unwrap())
            .collect();
        for b in &named {
            let pname = b.param_name.clone().unwrap();
            remap.insert(pname, b.upstream_node_name.clone());
            all_deps.push(NodeRef::ByName(b.upstream_node_name.clone()));
        }

        if let Some(wraps) = wraps {
            let annotations = get_annotations(py, wraps)?;
            let mut pos_idx = 0;
            for (k, v) in annotations.iter() {
                let pname: String = k.extract()?;
                if pname == "return" || pname == "self" {
                    continue;
                }
                // Skip context parameters — they are injected by the executor,
                // not resolved as upstream dependencies.
                if is_context_annotation(py, &v) || pname == "context" {
                    continue;
                }
                if named_params.contains(&pname) {
                    continue;
                }
                if pos_idx < positional.len() {
                    remap.insert(pname, positional[pos_idx].upstream_node_name.clone());
                    all_deps.push(NodeRef::ByName(
                        positional[pos_idx].upstream_node_name.clone(),
                    ));
                    pos_idx += 1;
                } else {
                    // Unbound param: resolve by name (annotation-based)
                    all_deps.push(NodeRef::ByName(pname));
                }
            }
        }
        Ok((all_deps, remap))
    }

    for task_py in tasks {
        if let Ok(py_task) = task_py.cast_bound::<PyTask>(py) {
            let task = py_task.borrow();
            let task_name = task
                .inner
                .name
                .clone()
                .ok_or_else(|| AssetDefinitionError::new_err("Task has no name"))?;

            let task_ref: Py<PyTask> = py_task.clone().unbind();

            // Create namespaced entries for each graph that uses this task.
            let has_namespaced =
                if let Some(namespaced_names) = task_namespaced_entries.remove(&task_name) {
                    for ns_name in &namespaced_names {
                        let bindings = composition_bindings.remove(ns_name).unwrap_or_default();
                        let (comp_deps, remap) =
                            build_remap_from_bindings(py, bindings, &task.inner.wraps)?;
                        // Inherit partitions_def and partition mappings from parent graph asset.
                        // Only propagate mapping entries for this task's actual deps.
                        let graph_name = ns_name.split('/').next().unwrap_or("");
                        let pd_override = graph_partitions_def
                            .get(graph_name)
                            .map(|p| p.clone_ref(py));
                        let dep_names: HashSet<&str> = comp_deps
                            .iter()
                            .filter_map(|d| match d {
                                NodeRef::ByName(n) => Some(n.as_str()),
                                _ => None,
                            })
                            .collect();
                        fn filter_by_deps<V: Clone>(
                            source: Option<&HashMap<String, V>>,
                            dep_names: &HashSet<&str>,
                        ) -> Option<HashMap<String, V>> {
                            source
                                .map(|full| {
                                    full.iter()
                                        .filter(|(k, _)| dep_names.contains(k.as_str()))
                                        .map(|(k, v)| (k.clone(), v.clone()))
                                        .collect::<HashMap<_, _>>()
                                })
                                .filter(|m| !m.is_empty())
                        }

                        let pm_override =
                            filter_by_deps(graph_partition_mappings.get(graph_name), &dep_names);
                        let ioh_override = graph_input_io_handlers
                            .get(graph_name)
                            .map(|full| {
                                full.iter()
                                    .filter(|(k, _)| dep_names.contains(k.as_str()))
                                    .map(|(k, v)| (k.clone(), v.clone_ref(py)))
                                    .collect::<HashMap<_, _>>()
                            })
                            .filter(|m| !m.is_empty());
                        let meta_override =
                            filter_by_deps(graph_input_metadata.get(graph_name), &dep_names);
                        unresolved_graph.insert(ns_name.clone(), comp_deps);
                        node_map.insert(
                            ns_name.clone(),
                            ResolvedNode::Task(ResolvedTask::new(
                                py,
                                task_ref.clone_ref(py),
                                Some(remap),
                                Some(graph_name.to_string()),
                                pd_override,
                                pm_override,
                                ioh_override,
                                meta_override,
                            )?),
                        );
                    }
                    true
                } else {
                    false
                };

            drop(task);

            // Bare entries use annotation-based deps which may reference
            // params that don't exist as graph nodes — skip when this task
            // is used exclusively inside graph compositions.
            if !has_namespaced {
                let mut deps = Vec::new();
                let task = py_task.borrow();
                if let Some(ref wraps) = task.inner.wraps {
                    append_deps(py, &mut deps, wraps, resource_keys)?;
                }
                drop(task);
                unresolved_graph.insert(task_name.clone(), deps);
                node_map.insert(
                    task_name,
                    ResolvedNode::Task(ResolvedTask::new(
                        py, task_ref, None, None, None, None, None, None,
                    )?),
                );
            }
        } else if let Ok(py_bash) = task_py.cast_bound::<PyBashTask>(py) {
            let bash = py_bash.borrow();
            let task_name = bash.name.clone();
            drop(bash);

            let bash_ref: Py<PyBashTask> = py_bash.clone().unbind();

            // Create namespaced entries for each graph that uses this BashTask.
            let has_namespaced =
                if let Some(namespaced_names) = task_namespaced_entries.remove(&task_name) {
                    for ns_name in &namespaced_names {
                        let bindings = composition_bindings.remove(ns_name).unwrap_or_default();
                        let deps: Vec<NodeRef> = bindings
                            .iter()
                            .map(|b| NodeRef::ByName(b.upstream_node_name.clone()))
                            .collect();
                        // Inherit partitions_def and partition mappings from parent graph asset.
                        let graph_name = ns_name.split('/').next().unwrap_or("");
                        let pd_override = graph_partitions_def
                            .get(graph_name)
                            .map(|p| p.clone_ref(py));
                        let pm_override = graph_partition_mappings
                            .get(graph_name)
                            .map(|full_pm| {
                                let dep_names: HashSet<&str> = deps
                                    .iter()
                                    .filter_map(|d| match d {
                                        NodeRef::ByName(n) => Some(n.as_str()),
                                        _ => None,
                                    })
                                    .collect();
                                full_pm
                                    .iter()
                                    .filter(|(k, _)| dep_names.contains(k.as_str()))
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect::<HashMap<_, _>>()
                            })
                            .filter(|m| !m.is_empty());
                        unresolved_graph.insert(ns_name.clone(), deps);
                        node_map.insert(
                            ns_name.clone(),
                            ResolvedNode::BashTask(ResolvedBashTask::new(
                                py,
                                bash_ref.clone_ref(py),
                                pd_override,
                                pm_override,
                            )),
                        );
                    }
                    true
                } else {
                    false
                };

            if !has_namespaced {
                unresolved_graph.insert(task_name.clone(), Vec::new());
                node_map.insert(
                    task_name,
                    ResolvedNode::BashTask(ResolvedBashTask::new(py, bash_ref, None, None)),
                );
            }
        } else {
            return Err(AssetDefinitionError::new_err(
                "tasks must contain Task or BashTask instances",
            ));
        }
    }

    Ok(UnresolvedGraph {
        graph: unresolved_graph,
        node_map,
        composition_task_names,
        graph_task_names,
        step_kinds,
    })
}

/// Validate partition mappings on all graph edges.
///
/// Rules:
/// - Identity mapping: both upstream and downstream must have partitions of the same type
/// - TimeWindow mapping: both must have TimeWindow partitions
/// - Static mapping: all keys in the mapping must be valid partition keys
/// - AllPartitions: always valid (fan-out / fan-in)
/// - If downstream is partitioned and upstream is partitioned, default mapping is Identity
/// - If downstream is partitioned and upstream is NOT partitioned, no mapping needed (unpartitioned dep is shared)
/// - If downstream is NOT partitioned and upstream IS partitioned, error (need AllPartitions mapping)
fn validate_partition_mappings(
    node_map: &HashMap<String, ResolvedNode>,
    unresolved_graph: &BTreeMap<String, Vec<NodeRef>>,
) -> PyResult<()> {
    for (node_name, deps) in unresolved_graph {
        let downstream_node = match node_map.get(node_name) {
            Some(n) => n,
            None => continue,
        };
        let downstream_partitions = downstream_node.partitions_def();
        let explicit_mappings = downstream_node.partition_mapping();

        for dep in deps {
            let dep_name = match dep {
                NodeRef::ByName(name) => name.as_str(),
                _ => continue,
            };

            let upstream_node = match node_map.get(dep_name) {
                Some(n) => n,
                None => continue,
            };
            let upstream_partitions = upstream_node.partitions_def();

            let mapping = explicit_mappings.as_ref().and_then(|m| m.get(dep_name));

            PartitionMapping::validate_edge(mapping, downstream_partitions, upstream_partitions)
                .map_err(|e| {
                    PartitionValidationError::new_err(format!(
                        "Asset '{}' depends on '{}': {}",
                        node_name, dep_name, e
                    ))
                })?;
        }

        // Check that all explicit mapping keys reference actual dependencies
        if let Some(ref mappings) = explicit_mappings {
            let dep_names: HashSet<&str> = deps
                .iter()
                .filter_map(|d| match d {
                    NodeRef::ByName(name) => Some(name.as_str()),
                    _ => None,
                })
                .collect();
            for mapping_key in mappings.keys() {
                if !dep_names.contains(mapping_key.as_str()) {
                    return Err(PartitionValidationError::new_err(format!(
                        "Asset '{}': partition_mapping references '{}' which is not a dependency",
                        node_name, mapping_key
                    )));
                }
            }
        }
    }
    Ok(())
}

#[derive(Clone)]
pub(crate) struct JobSummary {
    pub name: String,
    pub node_names: Vec<String>,
    pub asset_names: Vec<String>,
    pub executor: Option<Executor>,
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
    pub(crate) storage: Arc<SurrealStorage>,
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

/// Non-py twin of [`PyCodeRepository`] for the dispatch surface.
///
/// Holds an `Arc` of the same shared resolved-state lock that the pyclass
/// holds, so the dispatcher (and any other Rust caller) can submit runs
/// without going through `repo.borrow(py)`. The hot path is fully GIL-free:
/// reads acquire the read lock, drop it after extracting what's needed,
/// then `await` the storage write.
///
/// Construct via [`PyCodeRepository::handle`] under the GIL once at
/// dispatcher startup; clone freely thereafter.
#[derive(Clone)]
pub(crate) struct RepoHandle {
    state: Arc<std::sync::RwLock<Option<ResolvedState>>>,
    backfill_cancel_flags:
        Arc<std::sync::Mutex<HashMap<String, Arc<std::sync::atomic::AtomicBool>>>>,
}

impl RepoHandle {
    /// Look up a user-defined job's asset selection. `None` if the repo
    /// isn't resolved or the job doesn't exist. GIL-free — reads the
    /// pre-computed map populated at resolve time.
    pub(crate) fn job_asset_names(&self, name: &str) -> Option<Vec<String>> {
        self.state
            .read()
            .unwrap()
            .as_ref()
            .and_then(|s| s.jobs_info.get(name).map(|j| j.asset_names.clone()))
    }

    pub(crate) fn list_jobs(&self) -> Vec<JobSummary> {
        self.state
            .read()
            .unwrap()
            .as_ref()
            .map(|s| s.jobs_info.values().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn job_names(&self) -> Vec<String> {
        self.state
            .read()
            .unwrap()
            .as_ref()
            .map(|s| s.jobs_info.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn code_location_id(&self) -> Option<String> {
        self.state
            .read()
            .unwrap()
            .as_ref()
            .map(|s| s.code_location_id.clone())
    }

    pub(crate) fn list_sensors(&self) -> Vec<SensorSummary> {
        self.state
            .read()
            .unwrap()
            .as_ref()
            .map(|s| s.sensors_info.values().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn list_schedules(&self) -> Vec<ScheduleSummary> {
        self.state
            .read()
            .unwrap()
            .as_ref()
            .map(|s| s.schedules_info.values().cloned().collect())
            .unwrap_or_default()
    }

    /// See [`PyCodeRepository::submit_run`].
    pub(crate) async fn submit_run(
        &self,
        selection: Option<Vec<String>>,
        partition_key: Option<&PyPartitionKey>,
        tags: Option<Vec<(String, String)>>,
        launched_by: LaunchedBy,
        job_name: Option<String>,
    ) -> PyResult<PyRunHandle> {
        // Sync prep under the read lock — drop the guard before the await.
        let (record, storage) = {
            let guard = self.state.read().unwrap();
            let state = guard.as_ref().ok_or_else(|| {
                ExecutionError::new_err("Repository not resolved — call resolve() first")
            })?;
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

            validate_partition_for_selection(
                state,
                asset_names.iter().map(String::as_str),
                partition_key,
            )?;

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
            };
            (record, state.storage.clone())
        };

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
            storage,
        })
    }

    pub(crate) async fn submit_runs(
        &self,
        runs: Vec<(
            Option<Vec<String>>,
            Option<&PyPartitionKey>,
            Option<Vec<(String, String)>>,
        )>,
        launched_by: LaunchedBy,
    ) -> PyResult<Vec<String>> {
        let (records, storage) = {
            let guard = self.state.read().unwrap();
            let state = guard.as_ref().ok_or_else(|| {
                ExecutionError::new_err("Repository not resolved — call resolve() first")
            })?;
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

            for (selection, partition_key, tags) in &runs {
                let asset_names = selection.clone().unwrap_or_else(|| all_names.clone());
                validate_partition_for_selection(
                    state,
                    asset_names.iter().map(String::as_str),
                    *partition_key,
                )?;
                let run_id = uuid::Uuid::new_v4().to_string();
                let run_tags = tags.clone().unwrap_or_default();
                let priority = priority_from_tags(&run_tags);
                let core_pk = partition_key.map(|pk| pk.into());

                records.push(RunRecord {
                    run_id,
                    code_location_id: state.code_location_id.clone(),
                    job_name: None,
                    status: RunStatus::Queued,
                    start_time: now,
                    end_time: None,
                    tags: run_tags,
                    node_names: asset_names,
                    priority,
                    partition_key: core_pk,
                    block_reason: None,
                    launched_by: launched_by.clone(),
                });
            }
            (records, state.storage.clone())
        };

        storage
            .enqueue_runs(&records)
            .await
            .map_err(|e| ExecutionError::new_err(format!("Failed to enqueue runs: {e}")))?;

        let run_ids: Vec<String> = records.into_iter().map(|r| r.run_id).collect();
        tracing::info!(
            target: "rivers::repo",
            count = run_ids.len(),
            "runs enqueued (batch)"
        );

        Ok(run_ids)
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
    ) -> PyResult<String> {
        let (record, storage) = {
            let guard = self.state.read().unwrap();
            let state = guard.as_ref().ok_or_else(|| {
                ExecutionError::new_err("Repository not resolved — call resolve() first")
            })?;

            let asset_names = state
                .jobs_info
                .get(job_name)
                .map(|j| j.asset_names.clone())
                .ok_or_else(|| ExecutionError::new_err(format!("Job '{job_name}' not found")))?;

            validate_partition_for_selection(
                state,
                asset_names.iter().map(String::as_str),
                partition_key,
            )?;

            let run_id = uuid::Uuid::new_v4().to_string();
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
            };
            (record, state.storage.clone())
        };

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
    ) -> PyResult<()> {
        let (record, storage) = {
            let guard = self.state.read().unwrap();
            let state = guard.as_ref().ok_or_else(|| {
                ExecutionError::new_err("Repository not resolved — call resolve() first")
            })?;

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
            };
            (record, state.storage.clone())
        };

        storage
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

    /// Validate `partition_key` against the given `asset_names` —
    /// rejects missing keys for partitioned assets and keys that don't
    /// match an asset's partition definition. Same check
    /// [`Self::submit_run`] / [`Self::create_started_run`] perform
    /// inline; exposed separately so callers that bypass those
    /// (e.g. gRPC `materialize` going through
    /// `dispatch_materialization`) can fail synchronously instead of
    /// after a fire-and-forget thread swallows the error.
    pub(crate) fn validate_partition_for_selection(
        &self,
        asset_names: &[String],
        partition_key: Option<&PyPartitionKey>,
    ) -> PyResult<()> {
        let guard = self.state.read().unwrap();
        let state = guard.as_ref().ok_or_else(|| {
            ExecutionError::new_err("Repository not resolved — call resolve() first")
        })?;
        validate_partition_for_selection(
            state,
            asset_names.iter().map(String::as_str),
            partition_key,
        )
    }

    /// Reject any name in `asset_names` that isn't a resolved node in
    /// the repo. Companion to [`Self::validate_partition_for_selection`]
    /// — same motivation: dispatch paths that fan out asynchronously
    /// (Direct `dispatch_materialization`'s fire-and-forget thread,
    /// Queued's record-write) would otherwise swallow the error.
    pub(crate) fn validate_assets_exist(&self, asset_names: &[String]) -> PyResult<()> {
        let guard = self.state.read().unwrap();
        let state = guard.as_ref().ok_or_else(|| {
            ExecutionError::new_err("Repository not resolved — call resolve() first")
        })?;
        for name in asset_names {
            if !state.node_map.contains_key(name) {
                return Err(AssetNotFoundError::new_err(format!(
                    "Selection contains unknown asset: '{name}'"
                )));
            }
        }
        Ok(())
    }

    /// GIL-free. Used by gRPC `materialize` to expand empty selection
    /// ("materialize everything") into an explicit list before dispatch.
    pub(crate) fn asset_names(&self) -> PyResult<Vec<String>> {
        let guard = self.state.read().unwrap();
        Ok(guard
            .as_ref()
            .ok_or_else(|| {
                ExecutionError::new_err("Repository not resolved — call resolve() first")
            })?
            .node_map
            .iter()
            .filter(|&(_k, n)| matches!(n, ResolvedNode::Asset(_)))
            .map(|(k, _n)| k.clone())
            .collect())
    }

    /// Returns a `Py<PyJob>` ready for GIL-bound use (e.g. `execute_run` on
    /// a launch thread). Drops the read lock before returning so it can't
    /// serialize against `resolve()` for the caller's GIL-attached work.
    pub(crate) fn get_job(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyJob>> {
        let guard = self.state.read().unwrap();
        let state = guard.as_ref().ok_or_else(|| {
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
        let storage = {
            let guard = self.state.read().unwrap();
            let state = guard.as_ref().ok_or_else(|| {
                ExecutionError::new_err("Repository not resolved — call resolve() first")
            })?;
            state.storage.clone()
        };
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
        }))
    }

    /// Load the original `BackfillRecord` and convert it to a
    /// `BackfillRequestData` for resubmission. Appends `tag_keys::RERUN_OF`
    /// pointing at the original backfill id.
    pub(crate) async fn build_rerun_request(
        &self,
        backfill_id: &str,
        dry_run: bool,
    ) -> PyResult<crate::daemon::BackfillRequestData> {
        let storage = {
            let guard = self.state.read().unwrap();
            let state = guard.as_ref().ok_or_else(|| {
                ExecutionError::new_err("Repository not resolved — call resolve() first")
            })?;
            state.storage.clone()
        };
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

        Ok(crate::daemon::BackfillRequestData {
            selection: record.asset_selection,
            partition_keys: Some(partition_keys),
            partition_range: None,
            strategy: Some(strategy),
            failure_policy: Some(failure_policy),
            max_concurrency,
            tags: Some(tag_map),
            dry_run,
        })
    }

    /// Cancel an in-progress backfill: signal the in-process coordinator
    /// (if any) and mark the record `Canceled`. Returns whether a live
    /// coordinator was signalled.
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

        let storage = {
            let guard = self.state.read().unwrap();
            let state = guard.as_ref().ok_or_else(|| {
                ExecutionError::new_err("Repository not resolved — call resolve() first")
            })?;
            state.storage.clone()
        };
        storage
            .update_backfill_status(&backfill_id, BackfillStatus::Canceled, Some(now_ts()))
            .await
            .map_err(|e| ExecutionError::new_err(format!("Failed to cancel backfill: {e}")))?;

        Ok(signaled)
    }

    /// Request run cancellation: persist the cancel flag in storage and
    /// signal the run backend (Local in-process / K8s pod kill).
    pub(crate) async fn cancel_run(&self, run_id: &str) -> PyResult<bool> {
        let (storage, run_backend) = {
            let guard = self.state.read().unwrap();
            let state = guard.as_ref().ok_or_else(|| {
                ExecutionError::new_err("Repository not resolved — call resolve() first")
            })?;
            (state.storage.clone(), state.run_backend.clone())
        };

        storage
            .request_cancellation(run_id)
            .await
            .map_err(|e| ExecutionError::new_err(format!("failed to request cancellation: {e}")))?;

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
}

#[pyclass(name = "CodeRepository", frozen, module = "rivers._core")]
pub struct PyCodeRepository {
    raw_assets: Vec<Py<PyAsset>>,
    raw_tasks: Vec<Py<PyAny>>,
    raw_jobs: Option<Vec<Py<PyJob>>>,
    pub(crate) raw_schedules: HashMap<String, Py<PyScheduleDefinition>>,
    pub(crate) raw_sensors: HashMap<String, Py<PySensorDefinition>>,
    default_executor: Option<Executor>,
    raw_resources: HashMap<String, ResourceVariant>,
    pub(crate) run_queue_config: Option<Py<crate::concurrency::PyRunQueueConfig>>,
    pub(crate) run_backend_config: Option<Py<crate::concurrency::PyRunBackendConfig>>,
    pool_limits: Option<HashMap<String, i32>>,
    /// Wrapped in `Arc` so a non-py [`RepoHandle`] can hold a shared reference
    /// and dispatch runs without going through `repo.borrow(py)`.
    pub(crate) state: Arc<std::sync::RwLock<Option<ResolvedState>>>,
    backfill_cancel_flags:
        Arc<std::sync::Mutex<HashMap<String, Arc<std::sync::atomic::AtomicBool>>>>,
}

impl PyCodeRepository {
    fn effective_executor(&self) -> Executor {
        if std::env::var("RIVERS_STEP_POD").is_ok_and(|v| v == "1") {
            return Executor::InProcess {};
        }
        self.default_executor.clone().unwrap_or(Executor::Parallel {
            max_workers: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
            max_async_concurrent: None,
        })
    }

    /// `job_name=None` records the run as ad-hoc (asset-selection only — no
    /// user-defined `Job`).
    pub(crate) async fn submit_run(
        &self,
        selection: Option<Vec<String>>,
        partition_key: Option<&PyPartitionKey>,
        tags: Option<Vec<(String, String)>>,
        launched_by: LaunchedBy,
        job_name: Option<String>,
    ) -> PyResult<PyRunHandle> {
        self.handle()
            .submit_run(selection, partition_key, tags, launched_by, job_name)
            .await
    }

    /// Batched storage write.
    pub(crate) async fn submit_runs(
        &self,
        runs: Vec<(
            Option<Vec<String>>,
            Option<&PyPartitionKey>,
            Option<Vec<(String, String)>>,
        )>,
        launched_by: LaunchedBy,
    ) -> PyResult<Vec<String>> {
        self.handle().submit_runs(runs, launched_by).await
    }

    /// Build a non-py [`RepoHandle`] sharing this repo's resolved state.
    /// Cheap (one Arc clone + bool copy); call once at dispatcher startup
    /// while holding the GIL, then use the handle from any thread.
    pub(crate) fn handle(&self) -> RepoHandle {
        RepoHandle {
            state: Arc::clone(&self.state),
            backfill_cancel_flags: Arc::clone(&self.backfill_cancel_flags),
        }
    }

    pub(crate) fn has_run_queue(&self) -> bool {
        self.run_queue_config.is_some()
    }

    /// Internal counterpart of the pymethod `materialize`, accepting an
    /// explicit `LaunchedBy` so internal callers (backfill executor, condition
    /// daemon) can stamp the run origin. The pymethod version delegates here
    /// with `LaunchedBy::Manual`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn materialize_with_launcher(
        &self,
        selection: Option<Vec<String>>,
        partition_key: Option<PyPartitionKey>,
        tags: Option<Vec<(String, String)>>,
        raise_on_error: bool,
        config: Option<HashMap<String, Py<PyAny>>>,
        run_id_override: Option<String>,
        include_upstream: bool,
        resume: bool,
        launched_by: LaunchedBy,
    ) -> PyResult<PyRunResult> {
        let guard = self.ensure_resolved()?;
        let state = guard.as_ref().unwrap();
        let graph = state
            .inner_repo
            .graph
            .as_ref()
            .ok_or_else(|| ExecutionError::new_err("Graph not resolved"))?;

        // Job::validate_and_build_plan auto-includes graph task names and
        // collect-step virtual nodes, so we only need to compute the
        // externally-visible selection here.
        let mut selected_names: HashSet<String> = if let Some(ref sel) = selection {
            for name in sel {
                match state.node_map.get(name) {
                    None => {
                        return Err(AssetNotFoundError::new_err(format!(
                            "Selection contains unknown asset: '{name}'"
                        )));
                    }
                    Some(node) if node.is_external() && !node.is_observable_external() => {
                        return Err(ExecutionError::new_err(format!(
                            "Cannot materialize external asset without observe function: '{name}'"
                        )));
                    }
                    _ => {}
                }
            }
            sel.iter().cloned().collect()
        } else {
            state
                .node_map
                .iter()
                .filter(|(_, node)| !node.is_external() || node.is_observable_external())
                .map(|(name, _)| name.clone())
                .collect()
        };

        if include_upstream && selection.is_some() {
            let expanded = rivers_core::assets::graph::upstream_closure(graph, &selected_names);
            selected_names = expanded
                .into_iter()
                .filter(|name| {
                    state
                        .node_map
                        .get(name)
                        .map(|n| !n.is_external() || n.is_observable_external())
                        .unwrap_or(false)
                })
                .collect();
        };

        validate_partition_for_selection(
            state,
            selected_names.iter().map(String::as_str),
            partition_key.as_ref(),
        )?;

        // allow_incomplete_deps keeps the permissive "load missing upstream
        // from io_handler" semantics materialize has always offered (Job's
        // strict completeness check is too strict for ad-hoc selections).
        let mut synthetic_job = PyJob::new_synthetic(
            selected_names.into_iter().collect(),
            self.effective_executor(),
            true,
        );
        Python::attach(|py| {
            synthetic_job.configure_for_repo(
                py,
                &state.storage,
                &state.code_location_id,
                &state.resources,
                &state.io_handler_registry,
            );
        });
        synthetic_job.validate_and_build_plan(
            graph,
            &state.node_map,
            &state.step_kinds,
            &state.multi_asset_groups,
            &state.composition_order,
        )?;

        // If `run_id_override` points at an existing record (queue-launched
        // run, or a Direct dispatcher that pre-wrote the record before
        // spawning the launch thread), reuse it. Otherwise mint a fresh id
        // and route the record-write through
        // [`RepoHandle::create_materialization_run`] so the Direct dispatcher
        // and this path share the same seam. Either way, `run_inner` always
        // runs against an existing record (`RunInit::Existing`).
        let run_id = match run_id_override {
            Some(rid) => rid,
            None => uuid::Uuid::new_v4().to_string(),
        };
        let existing = rt()
            .block_on(state.storage.get_run(&run_id))
            .unwrap_or(None);
        let tags_vec = tags.unwrap_or_default();
        if existing.is_none() {
            let core_pk = partition_key.as_ref().map(|pk| pk.into());
            let asset_selection: Vec<String> = synthetic_job.asset_names();
            io_rt().block_on(self.handle().create_materialization_run(
                asset_selection,
                core_pk,
                tags_vec.clone(),
                launched_by,
                run_id.clone(),
            ))?;
        }

        Python::attach(|py| {
            synthetic_job.run_inner(
                py,
                run_id,
                crate::executor::run_lifecycle::RunInit::Existing,
                partition_key,
                tags_vec,
                config,
                resume,
                raise_on_error,
            )
        })
    }

    /// Logs errors instead of propagating them.
    fn teardown_resources(&self, py: Python) {
        let guard = self.state.read().unwrap();
        if let Some(state) = guard.as_ref() {
            for (key, resource) in &state.resources {
                if let ResourceVariant::Resource(inner) = resource {
                    let obj = inner.bind(py);
                    if obj.hasattr("teardown").unwrap_or(false)
                        && let Err(e) = inner.call_method0(py, "teardown")
                    {
                        tracing::warn!(target: "rivers::resources", resource = %key, error = %e, "resource teardown failed");
                    }
                }
            }
        }
    }

    pub fn asset_names(&self) -> PyResult<Vec<String>> {
        let guard = self.state.read().unwrap();
        Ok(guard
            .as_ref()
            .ok_or_else(|| {
                ExecutionError::new_err("CodeRepository not resolved — call resolve() first")
            })?
            .node_map
            .iter()
            .filter(|&(_k, n)| matches!(n, ResolvedNode::Asset(_)))
            .map(|(k, _n)| k.clone())
            .collect())
    }

    fn ensure_resolved(&self) -> PyResult<std::sync::RwLockReadGuard<'_, Option<ResolvedState>>> {
        {
            let guard = self.state.read().unwrap();
            if guard.is_none() {
                drop(guard);
                Python::attach(|py| self.resolve_inner(py, None))?;
            }
        }
        Ok(self.state.read().unwrap())
    }

    /// Storage-independent graph-only validation: builds the graph, runs
    /// static checks, resolves topology, validates resource references, and
    /// computes plan-build inputs (`multi_asset_groups`, `composition_order`).
    ///
    /// Does **not** mutate user `PyJob`s — that happens in
    /// [`Self::validate_and_build_job_plans`], which must run after
    /// [`Self::resolve_resources_and_handlers`] so the per-job cloned
    /// `node_map` subset captures the resolved IO handler overrides.
    fn build_and_validate<'py>(&self, py: Python<'py>) -> PyResult<BuiltGraph> {
        let resource_keys: &HashSet<&String> = &self.raw_resources.keys().collect();
        let UnresolvedGraph {
            graph: unresolved_graph,
            node_map,
            composition_task_names,
            graph_task_names,
            step_kinds,
        } = build_unresolved_graph(py, &self.raw_assets, &self.raw_tasks, resource_keys)?;

        validate_partition_mappings(&node_map, &unresolved_graph)?;

        // External assets with automation_condition must have an observe_fn.
        for node in node_map.values() {
            if let ResolvedNode::Asset(asset_node) = node
                && asset_node.kind == resolved_node::AssetKind::External
                && asset_node.automation_condition.is_some()
                && asset_node.observe_fn.is_none()
            {
                return Err(AssetDefinitionError::new_err(format!(
                    "External asset '{}' has an automation_condition but no observe function. \
                         Use @Asset.external(...) as a decorator on an observe function.",
                    asset_node.name
                )));
            }
        }

        let mut inner_repo = CodeRepository::new(unresolved_graph);
        inner_repo
            .resolve_asset_graph()
            .map_err(GraphValidationError::new_err)?;

        validate_resource_references(py, &node_map, resource_keys, &composition_task_names)?;
        validate_schedule_sensor_resource_references(
            py,
            &self.raw_schedules,
            &self.raw_sensors,
            resource_keys,
        )?;

        // Sensors / schedules can only dispatch against assets and jobs
        // that actually exist — fail at resolve time rather than at
        // every tick.
        let job_names_owned: Vec<String> = self
            .raw_jobs
            .as_ref()
            .map(|jobs| {
                jobs.iter()
                    .map(|j| j.borrow(py).name().to_string())
                    .collect()
            })
            .unwrap_or_default();
        let asset_name_set: HashSet<&str> = node_map.keys().map(String::as_str).collect();
        let job_name_set: HashSet<&str> = job_names_owned.iter().map(String::as_str).collect();
        validate_sensor_run_targets(py, &self.raw_sensors, &asset_name_set, &job_name_set)?;
        validate_schedule_run_targets(py, &self.raw_schedules, &job_name_set)?;

        // Plan-build inputs are graph-static; compute once and share across every
        // `validate_and_build_plan` invocation (per-job here, plus the synthetic
        // job materialize constructs on each call).
        let multi_asset_groups = crate::executor::ops::build_multi_asset_groups(&node_map);
        let mut composition_order: HashMap<String, usize> = HashMap::new();
        for node in node_map.values() {
            if let ResolvedNode::Asset(asset_node) = node
                && asset_node.kind == resolved_node::AssetKind::Graph
            {
                for (i, name) in asset_node.graph_invocation_order.iter().enumerate() {
                    composition_order.insert(name.clone(), i);
                }
            }
        }

        Ok(BuiltGraph {
            inner_repo,
            node_map,
            step_kinds,
            graph_task_names,
            multi_asset_groups,
            composition_order,
        })
    }

    /// Mutates each `PyJob` (extends `node_names` with namespaced internal
    /// tasks + collect steps; sets `plan` and the cloned `node_map` subset).
    ///
    /// The cloned `node_map` subset on each `PyJob` snapshots the *current*
    /// state of `node_map` — including any `io_handler_override` populated by
    /// [`Self::resolve_resources_and_handlers`]. Callers that want overrides
    /// reflected in execution must run `resolve_resources_and_handlers` first.
    fn validate_and_build_job_plans(
        &self,
        py: Python,
        resolved_graph: &rivers_core::assets::graph::AssetGraph,
        node_map: &HashMap<String, ResolvedNode>,
        step_kinds: &HashMap<String, rivers_core::execution::plan::StepKind>,
        multi_asset_groups: &HashMap<String, String>,
        composition_order: &HashMap<String, usize>,
    ) -> PyResult<()> {
        let default_executor = self.effective_executor();

        if let Some(ref job_list) = self.raw_jobs {
            let mut seen_names: HashSet<String> = HashSet::new();
            for job_py in job_list {
                let mut job = job_py.borrow_mut(py);
                let name = job.name().to_string();
                if !seen_names.insert(name.clone()) {
                    return Err(GraphValidationError::new_err(format!(
                        "Duplicate job name: '{}'",
                        name
                    )));
                }
                job.maybe_set_executor(default_executor.clone());
                job.validate_and_build_plan(
                    resolved_graph,
                    node_map,
                    step_kinds,
                    multi_asset_groups,
                    composition_order,
                )?;
                validate_job_partition_compatibility(&name, &job.node_names, node_map)?;
            }
        }

        Ok(())
    }

    /// Resolve `IOHandler::ResourceRef` → `Instance` on every asset and refresh
    /// the per-input + node-level overrides on namespaced composition tasks.
    /// Mutates `node_map` in place (overrides on `ResolvedTask` are post-set).
    /// Also calls `resource.setup()` on every Resource that defines it.
    /// Returns the shared default `InMemoryIOHandler` instance.
    fn resolve_resources_and_handlers(
        &self,
        py: Python,
        node_map: &mut HashMap<String, ResolvedNode>,
        graph_task_names: &HashMap<String, Vec<String>>,
    ) -> PyResult<Py<PyAny>> {
        let resource_keys: HashSet<&String> = self.raw_resources.keys().collect();
        let mut handlers: HashMap<String, &Py<PyAny>> = HashMap::new();

        for (key, variant) in &self.raw_resources {
            if node_map.contains_key(key) {
                let warnings = py.import("warnings")?;
                warnings.call_method1(
                    "warn",
                    (format!(
                        "Resource key '{}' shadows an asset with the same name. \
                         The asset will take precedence during parameter injection.",
                        key
                    ),),
                )?;
            }
            match variant {
                ResourceVariant::Resource(r) if r.bind(py).hasattr("setup")? => {
                    r.call_method0(py, "setup")?;
                }
                ResourceVariant::IOHandler(handler) => {
                    handlers.insert(key.clone(), handler);
                }
                _ => (),
            }
        }

        let io_handler_keys = &handlers.keys().collect::<HashSet<_>>();
        let resource_keys_excluding_io_handlers = resource_keys
            .difference(io_handler_keys)
            .copied()
            .collect::<HashSet<_>>();
        for asset_py in &self.raw_assets {
            {
                let mut asset = asset_py.borrow_mut(py);
                asset.inner_mut().resolve_io_handler_refs(
                    py,
                    &handlers,
                    &resource_keys_excluding_io_handlers,
                )?;
            }
            let asset = asset_py.borrow(py);
            if let Asset::Graph(graph_asset) = asset.inner() {
                let graph_name = graph_asset.name.as_deref().unwrap_or_default();
                let resolved = graph_asset.node_io_handler.as_ref().and_then(|h| match h {
                    crate::assets::io_handler::IOHandler::Instance(handler) => Some(
                        crate::assets::io_handler::IOHandler::Instance(handler.clone_ref(py)),
                    ),
                    crate::assets::io_handler::IOHandler::ResourceRef(_) => None,
                });
                if let Some(handler) = resolved
                    && let Some(task_names) = graph_task_names.get(graph_name)
                {
                    for ns_name in task_names {
                        if let Some(ResolvedNode::Task(task)) = node_map.get_mut(ns_name) {
                            task.io_handler_override = Some(handler.clone_ref(py));
                        }
                    }
                }
                // Refresh per-input handler overrides on namespaced composition tasks
                // with the now-resolved values. ResolvedTask was constructed inside
                // build_unresolved_graph (before resolve_io_handler_refs ran), so any
                // ResourceRef entries in input_io_handler_override are still unresolved.
                if let Some(task_names) = graph_task_names.get(graph_name) {
                    for ns_name in task_names {
                        if let Some(ResolvedNode::Task(task)) = node_map.get_mut(ns_name)
                            && let Some(ref mut override_map) = task.input_io_handler_override
                        {
                            for (dep, handler) in override_map.iter_mut() {
                                if let Some(resolved) = graph_asset.input_io_handlers.get(dep) {
                                    *handler = resolved.clone_ref(py);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Shared default for nodes without an explicit handler. The in-process executor
        // uses this as a fallback; the parallel executor rejects nodes without handlers
        // (since in-memory can't cross process boundaries).
        Ok(py
            .import("rivers.io_handlers.memory")?
            .getattr("InMemoryIOHandler")?
            .call0()?
            .unbind())
    }

    fn persist_topology(
        &self,
        py: Python,
        node_map: &HashMap<String, ResolvedNode>,
        resolved_graph: &rivers_core::assets::graph::AssetGraph,
        storage_handle: &rivers_core::storage::ScopedStorageHandle<SurrealStorage>,
    ) -> PyResult<()> {
        py.detach(|| {
            let mut topology = to_topology(resolved_graph);

            let mut task_to_graph: HashMap<String, String> = HashMap::new();
            for (name, gn) in node_map {
                if gn.is_graph_asset() {
                    for task_name in gn.graph_task_names() {
                        task_to_graph.insert(task_name, name.clone());
                    }
                }
            }

            for topo_node in &mut topology.nodes {
                if let Some(gn) = node_map.get(&topo_node.name) {
                    topo_node.group = gn.group();
                    topo_node.kind = match gn.asset_type() {
                        "single" | "multi" | "external" => {
                            rivers_core::assets::graph::NodeKind::Asset
                        }
                        "graph" => rivers_core::assets::graph::NodeKind::GraphAsset,
                        "task" => rivers_core::assets::graph::NodeKind::Task,
                        other => {
                            return Err(GraphValidationError::new_err(format!(
                                "unknown asset_type from graph node: '{other}'"
                            )));
                        }
                    };
                }
                if let Some(parent) = task_to_graph.get(&topo_node.name) {
                    topo_node.parent_graph = Some(parent.clone());
                }
            }

            let _ = io_rt().block_on(storage_handle.scoped().set_graph_topology(&topology));
            Ok(())
        })
    }

    /// Register concurrency pools: explicit limits from `pool_limits`, then
    /// auto-register any asset-declared pools not already configured (unlimited).
    fn register_pools(
        &self,
        py: Python,
        node_map: &HashMap<String, ResolvedNode>,
        storage_handle: &rivers_core::storage::ScopedStorageHandle<SurrealStorage>,
    ) {
        const DEFAULT_LEASE_DURATION_SECS: u32 = 300;

        py.detach(|| {
            if let Some(ref limits) = self.pool_limits {
                for (pool_key, limit) in limits {
                    let _ = io_rt().block_on(storage_handle.scoped().set_pool_limit(
                        pool_key,
                        *limit,
                        DEFAULT_LEASE_DURATION_SECS,
                    ));
                }
            }

            let mut seen_pools: HashSet<String> = HashSet::new();
            for node in node_map.values() {
                for (pool_key, _) in node.pool() {
                    seen_pools.insert(pool_key);
                }
            }
            let explicit_keys: HashSet<&String> = self
                .pool_limits
                .as_ref()
                .map(|m| m.keys().collect())
                .unwrap_or_default();
            for pool_key in &seen_pools {
                if !explicit_keys.contains(pool_key) {
                    let _ = io_rt().block_on(storage_handle.scoped().set_pool_limit(
                        pool_key,
                        -1,
                        DEFAULT_LEASE_DURATION_SECS,
                    ));
                }
            }
        });
    }

    fn init_run_backend(
        &self,
        py: Python,
        code_location_id: &str,
    ) -> PyResult<Arc<crate::daemon::RunBackendKind>> {
        let k8s_cfg = self
            .run_backend_config
            .as_ref()
            .map(|cfg| {
                cfg.borrow(py)
                    .build_k8s_config(code_location_id.to_string())
            })
            .transpose()?
            .flatten();
        if let Some(k8s_cfg) = k8s_cfg {
            let client = py
                .detach(|| rt().block_on(kube_client::Client::try_default()))
                .map_err(|e| {
                    crate::errors::ConfigurationError::new_err(format!(
                        "failed to create K8s client: {e}"
                    ))
                })?;
            Ok(Arc::new(crate::daemon::RunBackendKind::Kubernetes(
                Box::new(rivers_k8s::run_backend::K8sRunBackend::new(client, k8s_cfg)),
            )))
        } else {
            Ok(Arc::new(crate::daemon::RunBackendKind::Local(
                crate::backends::local::LocalRunBackend::new(),
            )))
        }
    }

    #[tracing::instrument(skip_all, target = "rivers::repo", name = "resolve")]
    fn resolve_inner(&self, py: Python, storage: Option<&PyStorage>) -> PyResult<()> {
        {
            let guard = self.state.read().unwrap();
            if guard.is_some() {
                return Ok(());
            }
        }

        let BuiltGraph {
            inner_repo,
            mut node_map,
            step_kinds,
            graph_task_names,
            multi_asset_groups,
            composition_order,
        } = self.build_and_validate(py)?;

        let (storage_arc, storage_type) = if let Some(s) = storage {
            (Arc::clone(s.backend()), s.storage_type)
        } else {
            let storage = py.detach(|| {
                io_rt()
                    .block_on(SurrealStorage::new_memory())
                    .map(Arc::new)
                    .map_err(|e| {
                        ConfigurationError::new_err(format!("Failed to init storage: {e}"))
                    })
            })?;
            tracing::info!(target: "rivers::storage", backend = "memory", "storage ready (auto-fallback)");
            (storage, PyStorageType::Memory)
        };

        // Resolve the code-location identity once: stamped on every RunRecord
        // this repo creates so the daemon's coordinator only dequeues runs
        // belonging to this CL. Falls back from `RIVERS_CODE_LOCATION_ID` to
        // `RIVERS_CODE_LOCATION_NAME` to a process-wide default.
        let code_location_id = rivers_k8s::env::current_code_location_id();

        // Must run *before* validate_and_build_job_plans: the per-job cloned
        // node_map subset built inside validate_and_build_plan snapshots
        // io_handler_override at call time.
        let default_io_handler =
            self.resolve_resources_and_handlers(py, &mut node_map, &graph_task_names)?;
        let io_handler_registry =
            crate::assets::io_handler_registry::IOHandlerRegistry::new(default_io_handler);

        let storage_handle = rivers_core::storage::ScopedStorageHandle::new(
            Arc::clone(&storage_arc),
            rivers_core::storage::CodeLocationContext::new(code_location_id.clone()),
        );

        let resolved_graph = inner_repo
            .graph
            .as_ref()
            .expect("graph resolved by build_and_validate");
        // Daemon pod will never set this env var
        let register_catalog = std::env::var("RIVERS_RUN_ID").is_err();
        if register_catalog {
            self.persist_topology(py, &node_map, resolved_graph, &storage_handle)?;
        }

        self.validate_and_build_job_plans(
            py,
            resolved_graph,
            &node_map,
            &step_kinds,
            &multi_asset_groups,
            &composition_order,
        )?;

        // Plans were built in validate_and_build_job_plans; this pass only wires
        // the storage-dependent state needed at run time.
        let mut job_map: HashMap<String, Py<PyJob>> = HashMap::new();
        if let Some(ref job_list) = self.raw_jobs {
            for job_py in job_list {
                let mut job = job_py.borrow_mut(py);
                let name = job.name().to_string();
                job.configure_for_repo(
                    py,
                    &storage_arc,
                    &code_location_id,
                    &self.raw_resources,
                    &io_handler_registry,
                );
                drop(job);
                job_map.insert(name, job_py.clone_ref(py));
            }
        }

        if register_catalog {
            register_assets_from_nodes(&storage_handle, &node_map, py);
            self.register_pools(py, &node_map, &storage_handle);
        } else {
            tracing::debug!(
                target: "rivers::repo",
                "skipping catalog registration (non-daemon pod)"
            );
        }

        let run_backend = self.init_run_backend(py, &code_location_id)?;

        tracing::info!(
            target: "rivers::repo",
            nodes = node_map.len(),
            jobs = job_map.len(),
            code_location = %code_location_id,
            "repository resolved"
        );

        let jobs_info: HashMap<String, JobSummary> = job_map
            .iter()
            .map(|(name, job_py)| {
                let job = job_py.borrow(py);
                (
                    name.clone(),
                    JobSummary {
                        name: job.name.clone(),
                        node_names: job.node_names.clone(),
                        asset_names: job.asset_names(),
                        executor: job.executor.clone(),
                    },
                )
            })
            .collect();

        let sensors_info: HashMap<String, SensorSummary> = self
            .raw_sensors
            .iter()
            .map(|(name, sens_py)| {
                let s = sens_py.borrow(py);
                (
                    name.clone(),
                    SensorSummary {
                        name: s.name.clone(),
                        job_name: s.job_name.clone(),
                        default_status: s.default_status.clone(),
                        minimum_interval: s.minimum_interval.clone(),
                        description: s.description.clone(),
                        asset_selection: s.asset_selection.clone(),
                        tags: s.tags.clone(),
                    },
                )
            })
            .collect();

        let schedules_info: HashMap<String, ScheduleSummary> = self
            .raw_schedules
            .iter()
            .map(|(name, sched_py)| {
                let s = sched_py.borrow(py);
                (
                    name.clone(),
                    ScheduleSummary {
                        name: s.name.clone(),
                        cron_schedule: s.cron_schedule.clone(),
                        job_name: s.job_name.clone(),
                        default_status: s.default_status.clone(),
                        timezone: s.timezone.clone(),
                        description: s.description.clone(),
                        tags: s.tags.clone(),
                    },
                )
            })
            .collect();

        *self.state.write().unwrap() = Some(ResolvedState {
            inner_repo,
            node_map,
            jobs: job_map,
            jobs_info,
            sensors_info,
            schedules_info,
            storage: storage_arc,
            storage_type,
            resources: self
                .raw_resources
                .iter()
                .map(|(k, v)| (k.clone(), v.clone_ref(py)))
                .collect(),
            io_handler_registry,
            step_kinds,
            multi_asset_groups,
            composition_order,
            run_backend,
            code_location_id,
        });

        Ok(())
    }
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
    #[pyo3(signature = (assets, tasks=None, jobs=None, schedules=None, sensors=None, default_executor=None, resources=None, run_queue=None, run_backend=None, pool_limits=None))]
    fn new(
        assets: Vec<Py<PyAsset>>,
        tasks: Option<Vec<Py<PyAny>>>,
        jobs: Option<Vec<Py<PyJob>>>,
        schedules: Option<Vec<Py<PyScheduleDefinition>>>,
        sensors: Option<Vec<Py<PySensorDefinition>>>,
        default_executor: Option<Executor>,
        resources: Option<HashMap<String, ResourceVariant>>,
        run_queue: Option<Py<crate::concurrency::PyRunQueueConfig>>,
        run_backend: Option<Py<crate::concurrency::PyRunBackendConfig>>,
        pool_limits: Option<HashMap<String, i32>>,
    ) -> PyResult<Self> {
        Ok(Self {
            raw_assets: assets,
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
            run_queue_config: run_queue,
            run_backend_config: run_backend,
            pool_limits,
            state: Arc::new(std::sync::RwLock::new(None)),
            backfill_cancel_flags: Arc::new(std::sync::Mutex::new(HashMap::new())),
        })
    }

    /// Resolve the asset graph, initialize storage, and register asset catalog.
    /// If not called explicitly, auto-resolves with in-memory storage on first use.
    #[pyo3(signature = (storage=None))]
    fn resolve(&self, py: Python, storage: Option<&PyStorage>) -> PyResult<()> {
        self.resolve_inner(py, storage)
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
            py,
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
        let guard = self.ensure_resolved()?;
        let state = guard.as_ref().unwrap();
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
        let guard = self.ensure_resolved()?;
        let state = guard.as_ref().unwrap();
        Ok(PyStorage {
            handle: rivers_core::storage::ScopedStorageHandle::new(
                Arc::clone(&state.storage),
                rivers_core::storage::CodeLocationContext::new(state.code_location_id.clone()),
            ),
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
        let guard = self.state.read().unwrap();
        let resources = guard.as_ref().map(|s| &s.resources).unwrap_or(&empty);
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
        let exec_time = execution_time.map(|s| s.to_string()).unwrap_or_else(|| {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            format!("{}", now)
        });
        let empty = HashMap::new();
        let guard = self.state.read().unwrap();
        let resources = guard.as_ref().map(|s| &s.resources).unwrap_or(&empty);
        schedule::evaluate_schedule(py, &sched_ref, &exec_time, resources)
    }

    pub(crate) fn get_job(&self, py: Python, name: &str) -> PyResult<Py<PyJob>> {
        let guard = self.ensure_resolved()?;
        let state = guard.as_ref().unwrap();
        state
            .jobs
            .get(name)
            .map(|j| j.clone_ref(py))
            .ok_or_else(|| NodeNotFoundError::new_err(format!("Job '{}' not found", name)))
    }

    #[pyo3(signature = (asset_names=None))]
    #[tracing::instrument(skip_all, target = "rivers::repo", name = "observe")]
    pub(crate) fn observe(
        &self,
        py: Python,
        asset_names: Option<Vec<String>>,
    ) -> PyResult<Py<PyAny>> {
        let guard = self.ensure_resolved()?;
        let state = guard.as_ref().unwrap();
        let result_dict = PyDict::new(py);

        // Shared bridge avoids creating/destroying one per asset observation.
        let has_async_observe = state.node_map.values().any(|node| {
            matches!(
                node,
                ResolvedNode::Asset(asset_node)
                    if asset_node.kind == resolved_node::AssetKind::External
                        && asset_node.is_async
            )
        });
        let observe_bridge = if has_async_observe {
            crate::executor::async_exec::AsyncBridge::new(py).ok()
        } else {
            None
        };

        for (name, node) in &state.node_map {
            if let ResolvedNode::Asset(asset_node) = node {
                if asset_node.kind != resolved_node::AssetKind::External {
                    continue;
                }

                // Filter by asset_names if provided
                if let Some(ref names) = asset_names
                    && !names.contains(name)
                {
                    continue;
                }

                if let Some(ref observe_fn) = asset_node.observe_fn {
                    let annotations = get_annotations(py, observe_fn)?;
                    let ctx_type = PyAssetExecutionContext::type_object(py);
                    let has_context_param = annotations.iter().any(|(k, v)| {
                        k.extract::<String>().ok().as_deref() != Some("return")
                            && crate::executor::ops::is_context_annotation(py, &v)
                    });

                    let (raw_result, ctx_py) = if has_context_param {
                        let config_instance = annotations
                            .iter()
                            .find(|(k, _)| k.extract::<String>().ok().as_deref() != Some("return"))
                            .filter(|(_, v)| {
                                crate::executor::ops::annotation_is(v, ctx_type.as_any())
                            })
                            .map(|(_, v)| {
                                crate::executor::ops::extract_config_from_annotation(py, &v, None)
                            })
                            .transpose()?
                            .flatten();
                        let ctx = PyAssetExecutionContext::new(
                            name.clone(),
                            asset_node.tags.clone(),
                            asset_node.kinds.clone(),
                            asset_node.group.clone(),
                            None,
                            asset_node.metadata.clone(),
                            None,
                            false,
                            vec![],
                        )
                        .with_config(config_instance);
                        let ctx_py = Py::new(py, ctx)?;
                        let result =
                            observe_fn.call1(py, (ctx_py.clone_ref(py),)).map_err(|e| {
                                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                                    "observe failed for asset '{}': {}",
                                    name, e
                                ))
                            })?;
                        (result, Some(ctx_py))
                    } else {
                        let result = observe_fn.call0(py).map_err(|e| {
                            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                                "observe failed for asset '{}': {}",
                                name, e
                            ))
                        })?;
                        (result, None)
                    };

                    let is_async_observe = asset_node.is_async;
                    let observe_result = if is_async_observe {
                        let bridge = observe_bridge.as_ref().ok_or_else(|| {
                            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                                "AsyncBridge unavailable for async observe_fn",
                            )
                        })?;
                        bridge.run_coroutine(py, raw_result.into_bound(py))?
                    } else {
                        raw_result
                    };

                    let extracted = result_types::try_extract_result_type(py, &observe_result)?;

                    let (mut merged_metadata, ctx_data_version) = if let Some(ref ctx) = ctx_py {
                        let ctx_ref = ctx.borrow(py);
                        let meta = ctx_ref.drain_output_metadata();
                        let dv = ctx_ref.drain_data_version();
                        (meta, dv)
                    } else {
                        (Vec::new(), None)
                    };

                    let data_version = if let Some(ext) = extracted {
                        merge_metadata(&mut merged_metadata, &ext.metadata);
                        ext.data_version.or(ctx_data_version)
                    } else {
                        ctx_data_version
                    };

                    if merged_metadata.is_empty() {
                        result_dict.set_item(name, py.None())?;
                    } else {
                        let dict = PyDict::new(py);
                        for (k, v) in &merged_metadata {
                            dict.set_item(k, v.clone().into_pyobject(py)?)?;
                        }
                        result_dict.set_item(name, dict)?;
                    }

                    let ts = now_ts();
                    let metadata_pairs: Vec<(String, String)> = merged_metadata
                        .into_iter()
                        .map(|(k, v)| {
                            (
                                k,
                                serde_json::to_string(&v).unwrap_or_else(|_| format!("{:?}", v)),
                            )
                        })
                        .collect();
                    let event = EventRecord {
                        code_location_id: state.code_location_id.clone(),
                        event_type: EventType::Observation { data_version },
                        asset_key: Some(name.clone()),
                        run_id: String::new(),
                        partition_key: None,
                        timestamp: ts,
                        metadata: metadata_pairs,
                        input_data_versions: vec![],
                    };
                    py.detach(|| {
                        let _ = io_rt().block_on(state.storage.store_event(&event));
                    });
                }
            }
        }

        Ok(result_dict.into_any().unbind())
    }

    #[pyo3(signature = (selection=None, partition_key=None, tags=None, raise_on_error=true, config=None, run_id_override=None, include_upstream=false, resume=false))]
    #[tracing::instrument(skip_all, target = "rivers::repo", name = "materialize")]
    pub(crate) fn materialize(
        &self,
        py: Python<'_>,
        selection: Option<Vec<String>>,
        partition_key: Option<PyPartitionKey>,
        tags: Option<Vec<(String, String)>>,
        raise_on_error: bool,
        config: Option<HashMap<String, Py<PyAny>>>,
        run_id_override: Option<String>,
        include_upstream: bool,
        resume: bool,
    ) -> PyResult<PyRunResult> {
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
                LaunchedBy::Manual,
            )
        })
    }

    /// Walks the registry chain `node.io_handler() → default`. Useful for
    /// debugging "which handler does asset X actually use?" without
    /// running execution.
    fn io_handler_for_output(&self, py: Python, name: String) -> PyResult<Py<PyAny>> {
        let guard = self.ensure_resolved()?;
        let state = guard.as_ref().unwrap();
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
        let guard = self.ensure_resolved()?;
        let state = guard.as_ref().unwrap();
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
        let guard = self.ensure_resolved()?;
        let state = guard.as_ref().unwrap();
        let storage_arc = Arc::clone(&state.storage);
        drop(guard);

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
                if let Err(e) =
                    rivers_ui::start_server(storage_arc, graph, host, port, registry, shutdown)
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
    ))]
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
        config: Option<HashMap<String, Py<PyAny>>>,
        block: bool,
        dry_run: bool,
    ) -> PyResult<PyBackfillResult> {
        py.detach(|| {
            self.backfill_inner(
                selection,
                partition_keys,
                partition_range,
                strategy,
                failure_policy,
                max_concurrency,
                tags,
                config,
                block,
                dry_run,
            )
        })
    }

    /// Dispatches one materialize() call per partition key, tracking progress
    /// in storage. Called by `backfill(block=true)` or by the daemon loop
    /// for Requested backfills.
    #[pyo3(signature = (backfill_id, config=None))]
    pub(crate) fn execute_backfill(
        &self,
        py: Python<'_>,
        backfill_id: &str,
        config: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<()> {
        py.detach(|| self.execute_backfill_inner(backfill_id, config))
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
            let _guard = self.ensure_resolved()?;
            io_rt().block_on(self.handle().cancel_backfill(backfill_id))
        })
    }

    pub(crate) fn get_backfill(
        &self,
        py: Python<'_>,
        backfill_id: String,
    ) -> PyResult<Option<PyBackfillStatusResult>> {
        py.detach(|| {
            let _guard = self.ensure_resolved()?;
            io_rt().block_on(self.handle().get_backfill(&backfill_id))
        })
    }

    /// Loads the original `BackfillRecord` from storage and resubmits it via
    /// `backfill()`, preserving asset selection, partition keys, strategy,
    /// failure policy, concurrency, and tags. Appends a `rivers/rerun_of`
    /// tag pointing at the original backfill id.
    #[pyo3(signature = (backfill_id, block = true, dry_run = false))]
    pub(crate) fn rerun_backfill(
        &self,
        py: Python<'_>,
        backfill_id: String,
        block: bool,
        dry_run: bool,
    ) -> PyResult<PyBackfillResult> {
        py.detach(|| {
            let record = {
                let guard = self.ensure_resolved()?;
                let state = guard.as_ref().unwrap();
                io_rt()
                    .block_on(state.storage.get_backfill(&backfill_id))
                    .map_err(|e| ExecutionError::new_err(format!("Failed to load backfill: {e}")))?
                    .ok_or_else(|| {
                        ExecutionError::new_err(format!("backfill '{backfill_id}' not found"))
                    })?
            };

            let partition_keys: Vec<PyPartitionKey> = record
                .partition_keys
                .iter()
                .map(PyPartitionKey::from)
                .collect();
            let strategy = PyBackfillStrategy::from_core(&record.strategy);
            let failure_policy = match record.failure_policy {
                BackfillFailurePolicy::Continue => "continue",
                BackfillFailurePolicy::StopOnFailure => "stop_on_failure",
            };
            let max_concurrency = record.max_concurrency.clamp(0, u32::MAX as i64) as u32;

            let mut tags = record.tags.clone();
            tags.push((tag_keys::RERUN_OF.to_string(), backfill_id));

            self.backfill_inner(
                Some(record.asset_selection),
                Some(partition_keys),
                None,
                Some(strategy),
                failure_policy,
                max_concurrency,
                Some(tags),
                None,
                block,
                dry_run,
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

    /// Test helper. Only works when run_queue is configured.
    #[pyo3(signature = (selection=None, partition_key=None))]
    fn _submit_run(
        &self,
        py: Python,
        selection: Option<Vec<String>>,
        partition_key: Option<PyPartitionKey>,
    ) -> PyResult<PyRunHandle> {
        if !self.has_run_queue() {
            return Err(ExecutionError::new_err(
                "Cannot submit run: no RunQueueConfig set",
            ));
        }
        // Test helper — auto-resolve so Python tests don't have to call resolve()
        // explicitly first. Production callers (gRPC, daemon) always run after
        // resolve and bypass this helper.
        let _guard = self.ensure_resolved()?;
        drop(_guard);
        py.detach(|| {
            io_rt().block_on(self.submit_run(
                selection,
                partition_key.as_ref(),
                None,
                LaunchedBy::Manual,
                None,
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
        let _guard = binding.ensure_resolved()?;
        let repo_handle = binding.handle();
        let has_run_queue = binding.has_run_queue();
        let repo: Py<PyCodeRepository> = slf.clone().unbind();
        let (storage, code_location_id) = {
            let state_guard = binding.state.read().unwrap();
            let state = state_guard
                .as_ref()
                .expect("ensure_resolved succeeded above");
            (Arc::clone(&state.storage), state.code_location_id.clone())
        };
        let repo_arc = Arc::new(repo.clone_ref(py));
        let run_dispatcher = Arc::new(crate::daemon::RunDispatcherKind::new(
            Arc::clone(&repo_arc),
            repo_handle.clone(),
            storage,
            code_location_id,
            has_run_queue,
        ));
        let backfill_dispatcher =
            Arc::new(crate::daemon::BackfillDispatcherKind::new_local(repo_arc));

        let (port_tx, port_rx) = std::sync::mpsc::channel::<u16>();

        // Child of the process shutdown token: fires on graceful SIGTERM
        // AND when a new `_start_grpc_server` call supersedes us via
        // `register_grpc_handle` (which calls `cancel()` on the prior token).
        let server_cancel = crate::shutdown::shutdown_token().child_token();
        let cancel_for_callee = server_cancel.clone();
        let cancel_for_grace = server_cancel.clone();

        tracing::trace!(target: "rivers::dbg::grpc", "_start_grpc_server: spawning std::thread for new tokio Runtime");
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
            rt.shutdown_timeout(GRPC_SHUTDOWN_GRACE);
            tracing::trace!(target: "rivers::dbg::grpc", "_start_grpc_server thread: EXIT");
        });
        crate::shutdown::register_grpc_handle(handle, server_cancel);

        tracing::trace!(target: "rivers::dbg::grpc", "_start_grpc_server: waiting for port via mpsc");
        let actual_port = port_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to get gRPC port: {e}"))
            })?;

        tracing::trace!(target: "rivers::dbg::grpc", actual_port, "_start_grpc_server: EXIT (returning port)");
        Ok(actual_port)
    }
}

/// Kept out of `#[pymethods]` so they aren't exposed to Python. Daemon
/// dispatchers, subdaemons, and other pymethod bodies that have already
/// detached the GIL call these directly.
impl PyCodeRepository {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn backfill_inner(
        &self,
        selection: Option<Vec<String>>,
        partition_keys: Option<Vec<PyPartitionKey>>,
        partition_range: Option<PyPartitionKeyRange>,
        strategy: Option<PyBackfillStrategy>,
        failure_policy: &str,
        max_concurrency: u32,
        tags: Option<Vec<(String, String)>>,
        config: Option<HashMap<String, Py<PyAny>>>,
        block: bool,
        dry_run: bool,
    ) -> PyResult<PyBackfillResult> {
        let guard = self.ensure_resolved()?;
        let state = guard.as_ref().unwrap();

        let resolved_keys: Vec<PyPartitionKey> = if let Some(keys) = partition_keys {
            if keys.is_empty() {
                return Err(ExecutionError::new_err("partition_keys must not be empty"));
            }
            keys
        } else if let Some(range) = partition_range {
            let selected = selection.as_deref().unwrap_or(&[]);
            let parts_def = selected
                .iter()
                .filter_map(|name| state.node_map.get(name).and_then(|n| n.partitions_def()))
                .next()
                .or_else(|| {
                    state
                        .node_map
                        .values()
                        .filter_map(|n| n.partitions_def())
                        .next()
                })
                .ok_or_else(|| {
                    ExecutionError::new_err(
                        "partition_range specified but no partitioned assets found in selection",
                    )
                })?;
            range.resolve(parts_def)?
        } else {
            return Err(ExecutionError::new_err(
                "Either partition_keys or partition_range must be provided",
            ));
        };

        let num_partitions = resolved_keys.len();

        // Resolution: explicit > asset default > MultiRun.
        let resolved_strategy = if let Some(s) = strategy {
            s
        } else {
            let selected_names = selection.as_deref().unwrap_or(&[]);
            let asset_strategies: Vec<PyBackfillStrategy> = selected_names
                .iter()
                .filter_map(|name| state.node_map.get(name)?.backfill_strategy())
                .collect();
            if !asset_strategies.is_empty()
                && asset_strategies.iter().all(|s| s == &asset_strategies[0])
            {
                asset_strategies.into_iter().next().unwrap()
            } else {
                PyBackfillStrategy::MultiRun {}
            }
        };

        let core_strategy = resolved_strategy.to_core();
        let run_groups = rivers_core::execution::backfill::group_into_runs(
            &core_strategy,
            &resolved_keys
                .iter()
                .map(PartitionKey::from)
                .collect::<Vec<_>>(),
        );
        let num_runs = run_groups.len();

        if dry_run {
            return Ok(PyBackfillResult {
                backfill_id: String::new(),
                num_partitions,
                num_runs,
                status: "dry_run".to_string(),
                completed: 0,
                failed: 0,
                canceled: 0,
                run_ids: Vec::new(),
                is_dry_run: true,
                partition_keys: resolved_keys,
            });
        }

        let backfill_id = uuid::Uuid::new_v4().to_string();
        let fp = match failure_policy {
            "stop_on_failure" => BackfillFailurePolicy::StopOnFailure,
            _ => BackfillFailurePolicy::Continue,
        };
        let core_keys: Vec<PartitionKey> = resolved_keys.iter().map(PartitionKey::from).collect();
        let backfill_tags = tags.clone().unwrap_or_default();

        let record = BackfillRecord {
            backfill_id: backfill_id.clone(),
            code_location_id: state.code_location_id.clone(),
            status: BackfillStatus::Requested,
            strategy: core_strategy,
            failure_policy: fp.clone(),
            asset_selection: selection.clone().unwrap_or_default(),
            partition_keys: core_keys,
            run_ids: Vec::new(),
            completed_partitions: Vec::new(),
            failed_partitions: Vec::new(),
            canceled_partitions: Vec::new(),
            max_concurrency: max_concurrency as i64,
            tags: backfill_tags.clone(),
            create_time: now_ts(),
            end_time: None,
            error: None,
        };

        io_rt()
            .block_on(state.storage.create_backfill(&record))
            .map_err(|e| ExecutionError::new_err(format!("Failed to create backfill: {e}")))?;

        drop(guard); // release lock before executing

        if block {
            self.execute_backfill_inner(&backfill_id, config)?;

            let guard = self.ensure_resolved()?;
            let state = guard.as_ref().unwrap();
            let final_record = rt()
                .block_on(state.storage.get_backfill(&backfill_id))
                .map_err(|e| ExecutionError::new_err(format!("{e}")))?;

            let status = final_record
                .as_ref()
                .map(|r| format!("{:?}", r.status))
                .unwrap_or_else(|| "Unknown".to_string());
            let completed = final_record
                .as_ref()
                .map(|r| r.completed_partitions.len())
                .unwrap_or(0);
            let failed = final_record
                .as_ref()
                .map(|r| r.failed_partitions.len())
                .unwrap_or(0);
            let canceled = final_record
                .as_ref()
                .map(|r| r.canceled_partitions.len())
                .unwrap_or(0);
            let run_ids = final_record
                .as_ref()
                .map(|r| r.run_ids.clone())
                .unwrap_or_default();

            Ok(PyBackfillResult {
                backfill_id,
                num_partitions,
                num_runs: run_ids.len(),
                status,
                completed,
                failed,
                canceled,
                run_ids,
                is_dry_run: false,
                partition_keys: resolved_keys,
            })
        } else {
            // Non-blocking: return immediately. A daemon picks up
            // Requested backfills via execute_backfill().
            Ok(PyBackfillResult {
                backfill_id,
                num_partitions,
                num_runs,
                status: "Requested".to_string(),
                completed: 0,
                failed: 0,
                canceled: 0,
                run_ids: Vec::new(),
                is_dry_run: false,
                partition_keys: resolved_keys,
            })
        }
    }

    pub(crate) fn execute_backfill_inner(
        &self,
        backfill_id: &str,
        config: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<()> {
        let guard = self.ensure_resolved()?;
        let state = guard.as_ref().unwrap();

        let record = rt()
            .block_on(state.storage.get_backfill(backfill_id))
            .map_err(|e| ExecutionError::new_err(format!("{e}")))?
            .ok_or_else(|| {
                ExecutionError::new_err(format!("Backfill '{backfill_id}' not found"))
            })?;

        if record.status != BackfillStatus::Requested {
            return Err(ExecutionError::new_err(format!(
                "Backfill '{backfill_id}' is {:?}, expected Requested",
                record.status
            )));
        }

        let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let mut flags = self.backfill_cancel_flags.lock().unwrap();
            flags.insert(backfill_id.to_string(), cancel.clone());
        }

        let partition_keys: Vec<PyPartitionKey> = record
            .partition_keys
            .iter()
            .map(PyPartitionKey::from)
            .collect();

        io_rt()
            .block_on(state.storage.update_backfill_status(
                backfill_id,
                BackfillStatus::InProgress,
                None,
            ))
            .map_err(|e| ExecutionError::new_err(format!("{e}")))?;

        let core_keys: Vec<PartitionKey> = partition_keys.iter().map(PartitionKey::from).collect();
        let run_groups =
            rivers_core::execution::backfill::group_into_runs(&record.strategy, &core_keys);

        let mut stop = false;
        let mut canceled_keys: Vec<PartitionKey> = Vec::new();

        for group in &run_groups {
            if cancel.load(std::sync::atomic::Ordering::Relaxed) || stop {
                canceled_keys.extend(group.iter().cloned());
                continue;
            }

            // Inherit backfill tags and default priority to -10 (lower than
            // scheduled runs) unless user explicitly set it. The backfill
            // origin is tracked via `LaunchedBy::Backfill`, not a tag.
            let mut run_tags: Vec<(String, String)> = record.tags.clone();
            if !run_tags.iter().any(|(k, _)| k == tag_keys::PRIORITY) {
                run_tags.push((
                    tag_keys::PRIORITY.to_string(),
                    DEFAULT_BACKFILL_PRIORITY.to_string(),
                ));
            }

            let mut group_run_ids = Vec::new();
            let mut group_completed = Vec::new();
            let mut group_failed = Vec::new();

            for core_pk in group {
                let py_pk = PyPartitionKey::from(core_pk);
                let run_config = config.as_ref().map(|c| {
                    Python::attach(|py| {
                        c.iter()
                            .map(|(k, v)| (k.clone(), v.clone_ref(py)))
                            .collect::<HashMap<String, Py<PyAny>>>()
                    })
                });
                let result = self.materialize_with_launcher(
                    Some(record.asset_selection.clone()),
                    Some(py_pk),
                    Some(run_tags.clone()),
                    false,
                    run_config,
                    None,
                    false,
                    false,
                    LaunchedBy::Backfill {
                        backfill_id: backfill_id.to_string(),
                    },
                );

                match result {
                    Ok(run_result) => {
                        group_run_ids.push(run_result.run_id);
                        if run_result.success {
                            group_completed.push(core_pk.clone());
                        } else {
                            group_failed.push(core_pk.clone());
                        }
                    }
                    Err(_) => {
                        group_failed.push(core_pk.clone());
                    }
                }
            }

            if !group_failed.is_empty()
                && matches!(record.failure_policy, BackfillFailurePolicy::StopOnFailure)
            {
                stop = true;
            }

            let _ = io_rt().block_on(state.storage.update_backfill_progress(
                backfill_id,
                &group_run_ids,
                &group_completed,
                &group_failed,
                &[],
            ));
        }

        if !canceled_keys.is_empty() {
            let _ = io_rt().block_on(state.storage.update_backfill_progress(
                backfill_id,
                &[],
                &[],
                &[],
                &canceled_keys,
            ));
        }

        if cancel.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = io_rt().block_on(state.storage.update_backfill_status(
                backfill_id,
                BackfillStatus::Canceled,
                Some(now_ts()),
            ));
        } else if !canceled_keys.is_empty() {
            // Stop-on-failure caused early termination
            let _ = io_rt().block_on(state.storage.update_backfill_status(
                backfill_id,
                BackfillStatus::CompletedFailed,
                Some(now_ts()),
            ));
        } else {
            let _ = io_rt().block_on(state.storage.try_complete_backfill(backfill_id));
        }

        {
            let mut flags = self.backfill_cancel_flags.lock().unwrap();
            flags.remove(backfill_id);
        }

        Ok(())
    }

    pub(crate) fn execute_backfill_queued_inner(&self, backfill_id: &str) -> PyResult<()> {
        let guard = self.ensure_resolved()?;
        let state = guard.as_ref().unwrap();

        let record = rt()
            .block_on(state.storage.get_backfill(backfill_id))
            .map_err(|e| ExecutionError::new_err(format!("{e}")))?
            .ok_or_else(|| {
                ExecutionError::new_err(format!("Backfill '{backfill_id}' not found"))
            })?;

        if record.status != BackfillStatus::Requested {
            return Err(ExecutionError::new_err(format!(
                "Backfill '{backfill_id}' is {:?}, expected Requested",
                record.status
            )));
        }

        io_rt()
            .block_on(state.storage.update_backfill_status(
                backfill_id,
                BackfillStatus::InProgress,
                None,
            ))
            .map_err(|e| ExecutionError::new_err(format!("{e}")))?;

        let core_keys: Vec<PartitionKey> = record.partition_keys.clone();
        let run_groups =
            rivers_core::execution::backfill::group_into_runs(&record.strategy, &core_keys);

        let mut run_tags: Vec<(String, String)> = record.tags.clone();
        if !run_tags.iter().any(|(k, _)| k == tag_keys::PRIORITY) {
            run_tags.push((
                tag_keys::PRIORITY.to_string(),
                DEFAULT_BACKFILL_PRIORITY.to_string(),
            ));
        }

        let partition_keys: Vec<PyPartitionKey> = run_groups
            .iter()
            .flatten()
            .map(PyPartitionKey::from)
            .collect();

        let runs: Vec<_> = partition_keys
            .iter()
            .map(|pk| {
                (
                    Some(record.asset_selection.clone()),
                    Some(pk),
                    Some(run_tags.clone()),
                )
            })
            .collect();

        let run_ids = io_rt().block_on(self.submit_runs(
            runs,
            LaunchedBy::Backfill {
                backfill_id: backfill_id.to_string(),
            },
        ))?;

        // Link run_ids to the backfill so try_complete_backfill can finalize
        // when all runs are terminal.
        let _ = io_rt().block_on(state.storage.update_backfill_progress(
            backfill_id,
            &run_ids,
            &[],
            &[],
            &[],
        ));

        Ok(())
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
