//! Job definition — a named subset of assets with an executor and execution plan.
//!
//! `PyJob` pyclass wraps a named selection of asset nodes. During validation it extracts the
//! subgraph and builds an `ExecutionPlan` with topological ordering. Supports `allow_incomplete_deps`
//! for partial graph execution. `execute()` delegates to the configured `Executor` backend.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use petgraph::Direction;
use pyo3::prelude::*;
use rivers_core::assets::graph::AssetGraph;
use rivers_core::execution::plan::{ExecutionPlan, StepKind};
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{LaunchedBy, ScopedStorageHandle};

use rivers_core::execution::retry::{RetryPolicy, RetryRef};

use crate::assets::io_handler_registry::IOHandlerRegistry;
use crate::config::ResourceVariant;
use crate::errors::{ConfigurationError, ExecutionError, GraphValidationError, NodeNotFoundError};
use crate::executor::run_lifecycle::{RunInit, RunPlanArgs, run_plan};

use crate::assets::decorator::PyAsset;
use crate::executor::Executor;
use crate::partitions::PyPartitionKey;
use crate::repository::PyRunResult;
use crate::repository::resolved_node::ResolvedNode;
use crate::task::{PyBashTask, PyTask};

#[pyclass(name = "Job", module = "rivers._core")]
pub struct PyJob {
    pub(crate) name: String,
    pub(crate) node_names: Vec<String>,
    pub(crate) executor: Option<Executor>,
    /// Job-level retry default; assets with their own policy keep it.
    retry: Option<RetryRef>,
    /// The verb this job executes. `None` means materialize; when
    /// set, the plan is built with `ExecutionPlan::for_action`.
    pub(crate) action: Option<String>,
    allow_incomplete_deps: bool,
    /// `true` for the synthetic job built by `materialize_with_launcher`. The
    /// `name` field still holds an internal label for error messages, but the
    /// run record's `job_name` is written as `None` (ad-hoc run).
    synthetic: bool,
    // Set after validation by CodeRepository:
    plan: Option<ExecutionPlan>,
    node_map: Option<HashMap<String, ResolvedNode>>,
    /// Storage scoped to the owning code location, set by `CodeRepository`
    /// during resolve. `None` for unresolved jobs. Used by `run_record` (which
    /// short-circuits when this is None) and `execute_run` for materialization.
    storage: Option<ScopedStorageHandle<SurrealStorage>>,
    pub(crate) resources: HashMap<String, ResourceVariant>,
    /// Repository `retries` registry, copied in by `configure_for_repo` so
    /// named refs on actions resolve at execution time.
    pub(crate) retries: HashMap<String, RetryPolicy>,
    io_handler_registry: Option<IOHandlerRegistry>,
}

impl PyJob {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The value to record on `RunRecord.job_name`. `None` for synthetic
    /// (materialize) jobs that don't correspond to a user-defined `Job`.
    fn record_name(&self) -> Option<String> {
        if self.synthetic {
            None
        } else {
            Some(self.name.clone())
        }
    }

    pub(crate) fn maybe_set_executor(&mut self, executor: Executor) {
        if self.executor.is_none() {
            self.executor = Some(executor);
        }
    }

    pub(crate) fn set_storage(&mut self, storage: ScopedStorageHandle<SurrealStorage>) {
        self.storage = Some(storage);
    }

    pub(crate) fn set_resources(
        &mut self,
        py: Python,
        resources: &HashMap<String, ResourceVariant>,
    ) {
        self.resources = resources
            .iter()
            .map(|(k, v)| (k.clone(), v.clone_ref(py)))
            .collect();
    }

    pub(crate) fn set_io_handler_registry(&mut self, py: Python, registry: &IOHandlerRegistry) {
        self.io_handler_registry = Some(registry.clone_ref(py));
    }

    /// Wire the job to its owning code location: scoped storage handle, resources,
    /// and IO handler registry. Used both during resolve (per-job + auto-default
    /// job) and at materialize time (synthetic job).
    pub(crate) fn configure_for_repo(
        &mut self,
        py: Python,
        storage: &Arc<SurrealStorage>,
        code_location_id: &str,
        resources: &HashMap<String, ResourceVariant>,
        io_handler_registry: &IOHandlerRegistry,
        retries: &HashMap<String, RetryPolicy>,
    ) {
        self.set_storage(ScopedStorageHandle::new(
            Arc::clone(storage),
            rivers_core::storage::CodeLocationContext::new(code_location_id.to_string()),
        ));
        self.set_resources(py, resources);
        self.set_io_handler_registry(py, io_handler_registry);
        self.retries = retries.clone();
    }

    fn base(
        name: String,
        node_names: Vec<String>,
        executor: Option<Executor>,
        allow_incomplete_deps: bool,
    ) -> Self {
        Self {
            name,
            node_names,
            executor,
            retry: None,
            action: None,
            allow_incomplete_deps,
            synthetic: false,
            plan: None,
            node_map: None,
            storage: None,
            resources: HashMap::new(),
            retries: HashMap::new(),
            io_handler_registry: None,
        }
    }

    /// Create the synthetic job used by `repo.materialize()` over an ad-hoc
    /// asset selection. The resulting `RunRecord.job_name` is `None`.
    pub(crate) fn new_synthetic(
        node_names: Vec<String>,
        executor: Executor,
        allow_incomplete_deps: bool,
        retry: Option<RetryRef>,
    ) -> Self {
        let mut job = Self::base(
            "<materialize>".to_string(),
            node_names,
            Some(executor),
            allow_incomplete_deps,
        );
        job.synthetic = true;
        job.retry = retry;
        job
    }

    /// Resolve a `retry="name"` job-level reference against the repository
    /// `retries` registry; errors on an unknown name.
    pub(crate) fn resolve_retry_ref(
        &mut self,
        retries: &HashMap<String, RetryPolicy>,
    ) -> PyResult<()> {
        if let Some(RetryRef::Named(key)) = &self.retry {
            let policy = retries.get(key).ok_or_else(|| {
                ConfigurationError::new_err(format!(
                    "unknown retry policy '{key}' referenced by job '{}'; registered: {:?}",
                    self.name,
                    retries.keys().collect::<Vec<_>>()
                ))
            })?;
            self.retry = Some(RetryRef::Inline(policy.clone()));
        }
        Ok(())
    }

    /// Nearest-wins retry fill over this job's node subset: a node keeps its
    /// own policy; otherwise the job's; otherwise `fallback` (the repo default).
    pub(crate) fn fill_retry_defaults(&mut self, fallback: Option<&RetryPolicy>) {
        let default = self
            .retry
            .as_ref()
            .and_then(|r| r.as_inline())
            .cloned()
            .or_else(|| fallback.cloned());
        let Some(policy) = default else { return };
        if let Some(map) = &mut self.node_map {
            for node in map.values_mut() {
                let slot = match node {
                    ResolvedNode::Asset(a) => &mut a.retry,
                    ResolvedNode::Task(t) => &mut t.retry,
                    ResolvedNode::BashTask(b) => &mut b.retry,
                };
                if slot.is_none() {
                    *slot = Some(policy.clone());
                }
            }
        }
    }

    /// Validate the job's nodes against the repository graph and build the execution plan.
    ///
    /// `multi_asset_groups` and `composition_order` are graph-static inputs computed
    /// once at resolve time and shared across every job (and the materialize synthetic
    /// job) so we don't rebuild them per call.
    pub(crate) fn validate_and_build_plan(
        &mut self,
        graph: &AssetGraph,
        repo_node_map: &HashMap<String, ResolvedNode>,
        step_kinds: &HashMap<String, StepKind>,
        multi_asset_groups: &HashMap<String, String>,
        composition_order: &HashMap<String, usize>,
        retries: &HashMap<String, RetryPolicy>,
    ) -> PyResult<()> {
        if self.action.is_some() {
            return self.validate_and_build_action_plan(graph, repo_node_map, retries);
        }

        // Auto-include namespaced internal tasks for graph assets and collect steps.
        let mut seen: HashSet<String> = self.node_names.iter().cloned().collect();
        let mut extra_nodes: Vec<String> = Vec::new();
        for name in &self.node_names {
            if let Some(node) = repo_node_map.get(name) {
                for task_name in node.graph_task_names() {
                    if seen.insert(task_name.clone()) {
                        extra_nodes.push(task_name);
                    }
                }
            }
        }
        for (step_name, kind) in step_kinds {
            let mapped = match kind {
                StepKind::Collect { mapped_step } => mapped_step,
                StepKind::CollectStream { mapped_step, .. } => mapped_step,
                _ => continue,
            };
            if seen.contains(mapped) && seen.insert(step_name.clone()) {
                extra_nodes.push(step_name.clone());
            }
        }
        self.node_names.extend(extra_nodes);

        for name in &self.node_names {
            if !repo_node_map.contains_key(name) && !step_kinds.contains_key(name) {
                return Err(NodeNotFoundError::new_err(format!(
                    "Job '{}': node '{}' not found in repository",
                    self.name, name
                )));
            }
        }

        let node_name_set_owned: HashSet<String> = self.node_names.iter().cloned().collect();

        let name_to_idx = rivers_core::assets::graph::name_to_index(graph);

        // Validate subgraph completeness, then apply exception rules
        // (external assets, allow_incomplete_deps + io_handler).
        if let Err(missing) =
            rivers_core::assets::graph::validate_subgraph_completeness(graph, &node_name_set_owned)
        {
            for (node, dep_name) in missing {
                let dep_node = repo_node_map.get(dep_name.as_str()).ok_or_else(|| {
                    NodeNotFoundError::new_err(format!(
                        "Job '{}': dependency '{}' not found in repository",
                        self.name, dep_name
                    ))
                })?;

                // External assets are always valid deps (io_handler guaranteed).
                if dep_node.is_external() {
                    continue;
                }

                if self.allow_incomplete_deps {
                    let has_handler = Python::attach(|py| dep_node.has_io_handler(py));
                    if !has_handler {
                        return Err(GraphValidationError::new_err(format!(
                            "Job '{}': node '{}' depends on '{}' which is not in the job \
                             and has no io_handler",
                            self.name, node, dep_name
                        )));
                    }
                } else {
                    return Err(GraphValidationError::new_err(format!(
                        "Job '{}': node '{}' depends on '{}' which is not in the job. \
                         Add it to the job or set allow_incomplete_deps=True",
                        self.name, node, dep_name
                    )));
                }
            }
        }

        // Multi-asset outputs are grouped into single steps.
        let mut plan = ExecutionPlan::from_subgraph(
            graph,
            &node_name_set_owned,
            multi_asset_groups,
            composition_order,
        );
        plan.apply_fan_out_kinds(step_kinds);
        // TODO: validate plan (e.g. all fan-out sources exist in node_map) before execution
        self.plan = Some(plan);

        // Build node_map subset (include external/incomplete deps for load_input).
        // Skip virtual collect steps — they don't have ResolvedNode entries.
        // The two `clone_ref` loops are the only GIL-bound work left in this
        // function — acquire once and do them together.
        let subset = Python::attach(|py| {
            let mut subset: HashMap<String, ResolvedNode> = HashMap::new();
            for name in &self.node_names {
                if let Some(node) = repo_node_map.get(name) {
                    subset.insert(name.clone(), node.clone_ref(py));
                }
            }
            // Include deps not in the job that have io_handlers (external
            // assets or allow_incomplete_deps).
            for name in &self.node_names {
                let node_idx = match name_to_idx.get(name.as_str()) {
                    Some(&idx) => idx,
                    None => continue, // virtual collect step — not in the graph
                };
                for dep_idx in graph.neighbors_directed(node_idx, Direction::Outgoing) {
                    let dep_name = &graph[dep_idx].name;
                    if !subset.contains_key(dep_name.as_str())
                        && let Some(dep_node) = repo_node_map.get(dep_name.as_str())
                        && (dep_node.is_external() || self.allow_incomplete_deps)
                    {
                        subset.insert(dep_name.clone(), dep_node.clone_ref(py));
                    }
                }
            }
            subset
        });
        self.node_map = Some(subset);

        Ok(())
    }

    /// Plan an action run: one step per named target, no upstream pull-in, no
    /// graph-task or collect expansion. Validates that every target defines
    /// the verb and that declared orderings agree.
    fn validate_and_build_action_plan(
        &mut self,
        graph: &AssetGraph,
        repo_node_map: &HashMap<String, ResolvedNode>,
        retries: &HashMap<String, RetryPolicy>,
    ) -> PyResult<()> {
        let verb = self.action.clone().expect("checked by caller");
        let mut ordering: Option<rivers_core::execution::plan::ActionOrdering> = None;

        for name in &self.node_names {
            let node = repo_node_map.get(name).ok_or_else(|| {
                NodeNotFoundError::new_err(format!(
                    "Job '{}': node '{}' not found in repository",
                    self.name, name
                ))
            })?;
            // Externals define exactly the built-in `observe`; any other verb
            // fails the find_action check below.
            let found = Python::attach(|py| {
                node.find_action(py, &verb).map(|a| {
                    let named_retry = match &a.retry {
                        Some(RetryRef::Named(key)) => Some(key.clone()),
                        _ => None,
                    };
                    (a.ordering, named_retry)
                })
            });
            let Some((ord, named_retry)) = found else {
                return Err(GraphValidationError::new_err(format!(
                    "Job '{}': asset '{}' does not define action '{}'",
                    self.name, name, verb
                )));
            };
            if let Some(key) = named_retry
                && !retries.contains_key(&key)
            {
                return Err(ConfigurationError::new_err(format!(
                    "unknown retry policy '{key}' referenced by action '{verb}' on \
                     asset '{name}'; registered: {:?}",
                    retries.keys().collect::<Vec<_>>()
                )));
            }
            match ordering {
                None => ordering = Some(ord),
                Some(prev) if prev != ord => {
                    return Err(GraphValidationError::new_err(format!(
                        "Job '{}': action '{}' declares conflicting orderings across targets",
                        self.name, verb
                    )));
                }
                _ => {}
            }
        }

        self.plan = Some(ExecutionPlan::for_action(
            graph,
            &verb,
            &self.node_names,
            ordering.unwrap_or_default(),
        ));

        let subset = Python::attach(|py| {
            self.node_names
                .iter()
                .filter_map(|n| {
                    repo_node_map
                        .get(n)
                        .map(|node| (n.clone(), node.clone_ref(py)))
                })
                .collect()
        });
        self.node_map = Some(subset);
        Ok(())
    }
}

#[pymethods]
impl PyJob {
    /// The verb this job's runs execute; `None` means materialize.
    #[getter]
    fn action(&self) -> Option<&str> {
        self.action.as_deref()
    }

    /// The job's name as registered in the repository.
    #[getter(name)]
    fn name_py(&self) -> &str {
        &self.name
    }

    #[new]
    #[pyo3(signature = (name, assets, executor=None, allow_incomplete_deps=false, retry=None, action=None))]
    fn new<'py>(
        py: Python<'py>,
        name: String,
        assets: Vec<Py<PyAny>>,
        executor: Option<Executor>,
        allow_incomplete_deps: bool,
        retry: Option<Bound<'py, PyAny>>,
        action: Option<String>,
    ) -> PyResult<Self> {
        let mut node_names = Vec::new();
        for obj in &assets {
            if let Ok(asset) = obj.cast_bound::<PyAsset>(py) {
                let inner = &asset.borrow().inner;
                // Multi-assets are registered in the graph under individual output
                // names, not the parent function name.
                if let Some(output_names) = inner.multi_asset_names() {
                    for oname in output_names {
                        let n = oname.clone().ok_or_else(|| {
                            GraphValidationError::new_err("Multi-asset output has no name")
                        })?;
                        node_names.push(n);
                    }
                } else {
                    let asset_name = inner
                        .name()
                        .clone()
                        .ok_or_else(|| GraphValidationError::new_err("Asset has no name"))?;
                    node_names.push(asset_name);
                }
            } else if let Ok(task) = obj.cast_bound::<PyTask>(py) {
                let task_name = task
                    .borrow()
                    .inner
                    .name
                    .clone()
                    .ok_or_else(|| GraphValidationError::new_err("Task has no name"))?;
                node_names.push(task_name);
            } else if let Ok(bash) = obj.cast_bound::<PyBashTask>(py) {
                node_names.push(bash.borrow().name.clone());
            } else if let Ok(t) = obj.bind(py).cast::<pyo3::types::PyType>()
                && t.is_subclass_of::<PyAsset>()?
            {
                // Class-form asset: a job is a selection by name, so only the
                // node names are derived here — desugaring stays in the repo.
                node_names.extend(crate::assets::class_form::node_names(t.as_any())?);
            } else {
                return Err(GraphValidationError::new_err(
                    "Job assets must be Asset, Task, or BashTask instances, or Asset subclasses",
                ));
            }
        }

        let mut job = Self::base(name, node_names, executor, allow_incomplete_deps);
        job.retry = crate::retry::extract_retry_ref(retry)?;
        if job.retry.is_some() && action.is_some() {
            return Err(ConfigurationError::new_err(format!(
                "Job '{}': job-level retry does not apply to action runs — \
                 declare the policy on the action itself",
                job.name
            )));
        }
        job.action = action;
        Ok(job)
    }

    #[pyo3(signature = (
        partition_key=None,
        tags=None,
        config=None,
        raise_on_error=true,
    ))]
    pub(crate) fn execute(
        &self,
        py: Python,
        partition_key: Option<PyPartitionKey>,
        tags: Option<Vec<(String, String)>>,
        config: Option<HashMap<String, Py<PyAny>>>,
        raise_on_error: bool,
    ) -> PyResult<PyRunResult> {
        self.run_inner(
            py,
            uuid::Uuid::new_v4().to_string(),
            RunInit::Create {
                launched_by: LaunchedBy::Manual { user: None },
            },
            partition_key,
            tags.unwrap_or_default(),
            config,
            false,
            raise_on_error,
        )
    }

    /// Execute a previously created run. Updates run status in storage on
    /// completion. Daemon dispatch and the K8s run pod (`rivers execute
    /// --job`) use this after the run record has been written via
    /// `RepoHandle::create_started_run`.
    #[pyo3(name = "_execute_run", signature = (
        run_id,
        partition_key=None,
        config=None,
        resume=false,
        raise_on_error=true,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn execute_run(
        &self,
        py: Python,
        run_id: &str,
        partition_key: Option<PyPartitionKey>,
        config: Option<HashMap<String, Py<PyAny>>>,
        resume: bool,
        raise_on_error: bool,
    ) -> PyResult<PyRunResult> {
        self.run_inner(
            py,
            run_id.to_string(),
            RunInit::Existing,
            partition_key,
            Vec::new(),
            config,
            resume,
            raise_on_error,
        )
    }
}

impl PyJob {
    /// Single entry point used by `execute`, `execute_run`, and
    /// `materialize_with_launcher` (via a synthetic job).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn run_inner(
        &self,
        py: Python,
        run_id: String,
        init: RunInit,
        partition_key: Option<PyPartitionKey>,
        tags: Vec<(String, String)>,
        config: Option<HashMap<String, Py<PyAny>>>,
        resume: bool,
        raise_on_error: bool,
    ) -> PyResult<PyRunResult> {
        let (plan, node_map, executor) = self.validated_parts()?;
        // A verb reaching a partitioned asset without a key is not a no-op: an
        // `Unmaterialize` action emits a whole-asset Deletion and clears every
        // partition. `run_action` gates this; the job path has to as well.
        if let Some(action) = self.action.as_deref()
            && !crate::repository::is_keyless_observe(Some(action), partition_key.as_ref())
        {
            crate::repository::validate_partition_in_map(
                node_map,
                plan.all_asset_names().iter().map(String::as_str),
                partition_key.as_ref(),
                Some(action),
            )?;
        }
        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| ExecutionError::new_err("Job has no storage configured."))?;
        let registry = self.resolve_io_handler_registry(py)?;

        run_plan(
            py,
            RunPlanArgs {
                plan,
                node_map,
                executor,
                storage,
                resources: &self.resources,
                retries: &self.retries,
                io_handler_registry: &registry,
                job_name: self.record_name(),
                run_id,
                init,
                partition_key,
                tags,
                config,
                resume,
                raise_on_error,
            },
        )
    }

    pub(crate) fn asset_names(&self) -> Vec<String> {
        self.plan
            .as_ref()
            .map(|p| p.all_asset_names())
            .unwrap_or_default()
    }

    fn validated_parts(
        &self,
    ) -> PyResult<(&ExecutionPlan, &HashMap<String, ResolvedNode>, &Executor)> {
        let (plan, node_map) = self
            .plan
            .as_ref()
            .zip(self.node_map.as_ref())
            .ok_or_else(|| ExecutionError::new_err("Job has not been validated."))?;
        let executor = self
            .executor
            .as_ref()
            .ok_or_else(|| ExecutionError::new_err("Job has no executor."))?;
        Ok((plan, node_map, executor))
    }

    /// Resolve the IO handler registry, creating an in-memory-default registry
    /// when none is configured (e.g. tests that construct a Job without going
    /// through CodeRepository).
    fn resolve_io_handler_registry(&self, py: Python) -> PyResult<IOHandlerRegistry> {
        match &self.io_handler_registry {
            Some(r) => Ok(r.clone_ref(py)),
            None => IOHandlerRegistry::new_in_memory(py),
        }
    }
}

pub fn register_job_module(parent_module: &Bound<'_, PyModule>) -> PyResult<()> {
    parent_module.add_class::<PyJob>()?;
    Ok(())
}
