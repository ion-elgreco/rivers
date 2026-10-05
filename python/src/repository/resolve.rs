use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use pyo3::prelude::*;

use crate::assets::decorator::Asset;
use crate::config::ResourceVariant;
use crate::errors::{
    AssetDefinitionError, ConfigurationError, ExecutionError, GraphValidationError,
};
use crate::executor::ops::register_assets_from_nodes;
use crate::job::PyJob;
use crate::partitions::PartitionsDefinition;
use crate::runtime::{io_rt, rt};
use crate::storage::{DetachOnClose, PyStorage, PyStorageType};
use rivers_core::assets::graph::to_topology;
use rivers_core::repo::CodeRepository;
use rivers_core::storage::surrealdb_backend::SurrealStorage;

use super::*;

impl PyCodeRepository {
    /// The resolved state; resolves with in-memory storage on first use.
    pub(super) fn ensure_resolved(&self) -> PyResult<Arc<ResolvedState>> {
        match self.state.try_get() {
            Some(state) => Ok(state),
            None => Python::attach(|py| self.resolve_inner(py, None)),
        }
    }

    /// Storage-independent graph-only validation: builds the graph, runs
    /// static checks, resolves topology, validates resource references, and
    /// computes plan-build inputs (`multi_asset_groups`, `composition_order`).
    ///
    /// Job plans are built in [`Self::validate_and_build_job_plans`], which
    /// must run after [`Self::resolve_resources_and_handlers`] so the per-job
    /// cloned `node_map` subset captures the resolved IO handler overrides.
    pub(super) fn build_and_validate<'py>(&self, py: Python<'py>) -> PyResult<BuiltGraph> {
        let resource_keys: &HashSet<&String> = &self.raw_resources.keys().collect();
        let UnresolvedGraph {
            graph: unresolved_graph,
            node_map,
            composition_task_names,
            graph_task_names,
            step_kinds,
        } = build_unresolved_graph(
            py,
            &self.raw_assets,
            &self.raw_tasks,
            resource_keys,
            &self.raw_partition_defs,
        )?;

        validate_partition_mappings(py, &node_map, &unresolved_graph)?;

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

        // in_latest_time_window() at root scope filters the asset's own
        // partitions, so it requires single-dimension time-window partitioning.
        for node in node_map.values() {
            if let ResolvedNode::Asset(asset_node) = node
                && let Some(cond) = &asset_node.automation_condition
                && cond.node.has_root_scope_latest_time_window()
            {
                let time_partitioned = matches!(
                    node.partitions_def(),
                    Some(PartitionsDefinition::TimeWindow { .. })
                );
                if !time_partitioned {
                    return Err(AssetDefinitionError::new_err(format!(
                        "Asset '{}' uses in_latest_time_window() but is not time-window \
                         partitioned, so the filter would have nothing to select. Use it \
                         on single-dimension time-window partitioned assets, or inside \
                         any_deps_match/all_deps_match to filter a dep's partitions.",
                        asset_node.name
                    )));
                }
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
            .map(|jobs| jobs.iter().map(|j| j.get().name().to_string()).collect())
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

    /// Builds this repository's own copy of each user `Job` (`node_names`
    /// extended with namespaced internal tasks + collect steps, `plan`, the
    /// cloned `node_map` subset, executor and retry). The user's `Job` keeps
    /// only its declaration, so several repositories can share it.
    ///
    /// The cloned `node_map` subset on each copy snapshots the *current*
    /// state of `node_map` — including any `io_handler_override` populated by
    /// [`Self::resolve_resources_and_handlers`]. Callers that want overrides
    /// reflected in execution must run `resolve_resources_and_handlers` first.
    pub(super) fn validate_and_build_job_plans(
        &self,
        resolved_graph: &rivers_core::assets::graph::AssetGraph,
        node_map: &HashMap<String, ResolvedNode>,
        step_kinds: &HashMap<String, rivers_core::execution::plan::StepKind>,
        multi_asset_groups: &HashMap<String, String>,
        composition_order: &HashMap<String, usize>,
    ) -> PyResult<Vec<PyJob>> {
        let default_executor = self.effective_executor();
        let default_retry = self.resolved_default_retry()?;

        let mut jobs = Vec::new();
        if let Some(ref job_list) = self.raw_jobs {
            let mut seen_names: HashSet<String> = HashSet::new();
            for job_py in job_list {
                let mut job = job_py.get().declaration();
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
                    &self.raw_retries,
                )?;
                job.resolve_retry_ref(&self.raw_retries)?;
                job.fill_retry_defaults(default_retry.as_ref());
                validate_job_partition_compatibility(
                    &name,
                    &job.node_names,
                    node_map,
                    job.action.as_deref(),
                )?;
                jobs.push(job);
            }
        }

        Ok(jobs)
    }

    /// Resolve a node's `retry` ref against the repository `retries`
    /// registry; errors on an unknown name.
    fn resolve_retry_ref(
        r: Option<rivers_core::execution::retry::RetryRef>,
        retries: &HashMap<String, rivers_core::execution::retry::RetryPolicy>,
        kind: &str,
        owner: &str,
    ) -> PyResult<Option<rivers_core::execution::retry::RetryPolicy>> {
        use rivers_core::execution::retry::RetryRef;
        match r {
            None => Ok(None),
            Some(RetryRef::Inline(p)) => Ok(Some(p)),
            Some(RetryRef::Named(key)) => match retries.get(&key) {
                Some(p) => Ok(Some(p.clone())),
                None => Err(ConfigurationError::new_err(format!(
                    "unknown retry policy '{key}' referenced by {kind} '{owner}'; registered: {:?}",
                    retries.keys().collect::<Vec<_>>()
                ))),
            },
        }
    }

    /// Resolve the resource keys in every node's IO handlers against this
    /// repository's resources, and set each graph's `node_io_handler` as the
    /// override on its namespaced composition tasks. Only `node_map` changes:
    /// the definitions keep their keys for other repositories built from them.
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
        let others = &resource_keys
            .difference(io_handler_keys)
            .copied()
            .collect::<HashSet<_>>();
        for asset_py in &self.raw_assets {
            let asset = asset_py.get();
            if let Asset::Graph(graph_asset) = asset.inner()
                && let Some(node_io_handler) = &graph_asset.node_io_handler
            {
                let graph_name = graph_asset.name.as_deref().unwrap_or_default();
                let mut handler = node_io_handler.clone_ref(py);
                handler.resolve_in_place(py, &handlers, others, "Asset", graph_name)?;
                for ns_name in graph_task_names.get(graph_name).into_iter().flatten() {
                    match node_map.get_mut(ns_name) {
                        Some(ResolvedNode::Task(task)) => {
                            task.io_handler_override = Some(handler.clone_ref(py));
                        }
                        Some(ResolvedNode::BashTask(task)) => {
                            task.io_handler_override = Some(handler.clone_ref(py));
                        }
                        _ => {}
                    }
                }
            }
        }
        for node in node_map.values_mut() {
            node.resolve_io_handlers(py, &handlers, others)?;
        }

        for node in node_map.values_mut() {
            match node {
                ResolvedNode::Asset(a) => {
                    let r = a
                        .inner
                        .get()
                        .inner()
                        .retry_for_output(a.output_name.as_deref())
                        .cloned();
                    a.retry = Self::resolve_retry_ref(r, &self.raw_retries, "asset", &a.name)?;
                }
                ResolvedNode::Task(t) => {
                    let r = t.inner.get().inner.retry.clone();
                    t.retry = Self::resolve_retry_ref(r, &self.raw_retries, "task", &t.name)?;
                }
                ResolvedNode::BashTask(b) => {
                    let r = b.inner.get().retry.clone();
                    b.retry = Self::resolve_retry_ref(r, &self.raw_retries, "task", &b.name)?;
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
    /// auto-register any asset-declared pools not already configured
    /// (unlimited), plus the implicit per-asset pool for every asset
    /// declaring an `Exclusive` action.
    fn register_pools(
        &self,
        py: Python,
        node_map: &HashMap<String, ResolvedNode>,
        storage_handle: &rivers_core::storage::ScopedStorageHandle<SurrealStorage>,
    ) -> PyResult<()> {
        const DEFAULT_LEASE_DURATION_SECS: u32 = 300;

        let exclusive_pools: Vec<String> = node_map
            .iter()
            .filter(|(_, node)| node.has_exclusive_action())
            .map(|(name, _)| crate::executor::dispatch::implicit_asset_pool(name))
            .collect();

        py.detach(|| {
            // One write per pool, all in flight together — resolve() otherwise
            // pays a storage round-trip per pool, sequentially. Later inserts
            // win, preserving the old write order (exclusive over explicit).
            let mut writes: HashMap<String, i32> = HashMap::new();
            if let Some(ref limits) = self.pool_limits {
                for (pool_key, limit) in limits {
                    writes.insert(pool_key.clone(), *limit);
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
                    writes.insert(pool_key.clone(), -1);
                }
            }
            for pool_key in &exclusive_pools {
                writes.insert(
                    pool_key.clone(),
                    crate::executor::dispatch::EXCLUSIVE_POOL_CAPACITY,
                );
            }
            // A lost row here makes every later claim on that pool hard-fail
            // "pool not configured" — fail resolve() loudly instead.
            io_rt()
                .block_on(async {
                    let scoped = storage_handle.scoped();
                    futures_util::future::join_all(writes.iter().map(|(pool_key, limit)| {
                        scoped.set_pool_limit(pool_key, *limit, DEFAULT_LEASE_DURATION_SECS)
                    }))
                    .await
                    .into_iter()
                    .collect::<anyhow::Result<Vec<()>>>()
                })
                .map(|_| ())
                .map_err(|e| {
                    ExecutionError::new_err(format!("failed to register concurrency pools: {e}"))
                })
        })
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
                crate::backends::local::LocalRunBackend::new(self.gil_threads.clone()),
            )))
        }
    }

    #[tracing::instrument(skip_all, target = "rivers::repo", name = "resolve")]
    pub(super) fn resolve_inner(
        &self,
        py: Python,
        storage: Option<&PyStorage>,
    ) -> PyResult<Arc<ResolvedState>> {
        if let Some(state) = self.state.get_attached(py) {
            return Ok(state);
        }
        let _resolving = self.resolve_lock.lock(py)?;
        if let Some(state) = self.state.get_attached(py) {
            return Ok(state);
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

        let jobs = self.validate_and_build_job_plans(
            resolved_graph,
            &node_map,
            &step_kinds,
            &multi_asset_groups,
            &composition_order,
        )?;

        // Plans were built in validate_and_build_job_plans; this pass only wires
        // the storage-dependent state needed at run time.
        let mut job_map: HashMap<String, Py<PyJob>> = HashMap::new();
        for mut job in jobs {
            job.configure_for_repo(
                py,
                &storage_arc,
                &code_location_id,
                &self.raw_resources,
                &io_handler_registry,
                &self.raw_retries,
            );
            job_map.insert(job.name().to_string(), Py::new(py, job)?);
        }

        if register_catalog {
            register_assets_from_nodes(&storage_handle, &node_map, py);
            self.register_pools(py, &node_map, &storage_handle)?;
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
                let job = job_py.get();
                (
                    name.clone(),
                    JobSummary {
                        name: job.name.clone(),
                        node_names: job.node_names.clone(),
                        asset_names: job.asset_names(),
                        executor: job.executor.clone(),
                        action: job.action.clone(),
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

        let state = Arc::new(ResolvedState {
            inner_repo,
            node_map,
            jobs: job_map,
            jobs_info,
            sensors_info,
            schedules_info,
            storage: DetachOnClose::new(storage_arc),
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
        self.state.replace(py, Some(Arc::clone(&state)));

        Ok(state)
    }
}
