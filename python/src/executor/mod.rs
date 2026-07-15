//! Executor types and plan execution — in-process, parallel, and async backends.
//!
//! Defines the `Executor` enum (InProcess / Parallel / Kubernetes) and the
//! `execute_plan()` entry point. Delegates step dispatch to the `dispatch`
//! module and fan-out/IO/finalize to `ops`. Parallel mode uses loky process
//! pools; in-process runs sync steps on the calling thread and async steps on
//! the shared Tokio runtime with bounded concurrency.
pub mod async_exec;
pub mod dispatch;
pub mod event_writer;
pub mod in_process;
pub mod kubernetes;
pub mod ops;
pub mod parallel;
pub(crate) mod run_lifecycle;

use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;
use rivers_core::execution::plan::ExecutionPlan;
use rivers_core::storage::StorageBackend;

use crate::assets::io_handler_registry::IOHandlerRegistry;
use crate::config::ResourceVariant;
use crate::errors::ConfigurationError;

use crate::partitions::PyPartitionKey;
use crate::repository::resolved_node::ResolvedNode;
use crate::runtime::{io_rt, rt};

use self::event_writer::EventWriter;
use self::ops::{StorageHandle, now_ts};

/// Maps a graph asset's final task name to the graph asset name.
/// Used for dual IO write: when the final task completes, its output is also
/// written through the graph asset's IO handler.
pub(crate) struct GraphNodeMap {
    pub final_nodes: HashMap<String, String>,
}

const DEFAULT_EXECUTOR_KEY: &str = "__default__";

/// Reserved metadata keys in the `rivers/` namespace.
pub(crate) mod metadata_keys {
    /// Per-asset executor override (e.g. `"in_process"`, `"parallel"`).
    pub const EXECUTOR: &str = "rivers/executor";
    /// Node-level executor override for graph asset internal tasks.
    /// Takes precedence over [`EXECUTOR`] for namespaced (graph/task) steps.
    pub const NODE_EXECUTOR: &str = "rivers/node/executor";
}

fn default_max_workers() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// True inside a K8s step-worker pod (`RIVERS_STEP_POD=1`, stamped by the
/// step Job builder).
pub(crate) fn in_step_pod() -> bool {
    static IN_STEP_POD: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *IN_STEP_POD.get_or_init(|| std::env::var("RIVERS_STEP_POD").is_ok_and(|v| v == "1"))
}

#[pyclass(name = "Executor", frozen, from_py_object, module = "rivers._core")]
#[derive(Clone, Debug)]
pub enum Executor {
    InProcess {},
    Parallel {
        max_workers: usize,
        max_async_concurrent: Option<usize>,
    },
    Kubernetes {
        worker_image: Option<String>,
        max_concurrent_steps: Option<usize>,
        namespace: Option<String>,
        service_account: String,
        worker_cpu: String,
        worker_memory: String,
    },
}

#[pymethods]
impl Executor {
    #[staticmethod]
    fn in_process() -> Self {
        Self::InProcess {}
    }

    #[staticmethod]
    #[pyo3(signature = (max_workers=None, max_async_concurrent=None))]
    fn parallel(max_workers: Option<usize>, max_async_concurrent: Option<usize>) -> Self {
        Self::Parallel {
            max_workers: max_workers.unwrap_or_else(default_max_workers),
            max_async_concurrent,
        }
    }

    #[staticmethod]
    #[pyo3(signature = (
        worker_image=None,
        *,
        max_concurrent_steps=None,
        namespace=None,
        service_account=rivers_k8s::defaults::SERVICE_ACCOUNT,
        worker_cpu=rivers_k8s::defaults::RUN_CPU,
        worker_memory=rivers_k8s::defaults::RUN_MEMORY,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn kubernetes(
        worker_image: Option<String>,
        max_concurrent_steps: Option<usize>,
        namespace: Option<String>,
        service_account: &str,
        worker_cpu: &str,
        worker_memory: &str,
    ) -> Self {
        Self::Kubernetes {
            worker_image,
            max_concurrent_steps,
            namespace,
            service_account: service_account.to_string(),
            worker_cpu: worker_cpu.to_string(),
            worker_memory: worker_memory.to_string(),
        }
    }

    fn __repr__(&self) -> String {
        match self {
            Self::InProcess {} => "Executor.InProcess()".to_string(),
            Self::Parallel {
                max_workers,
                max_async_concurrent,
            } => match max_async_concurrent {
                Some(n) => format!(
                    "Executor.Parallel(max_workers={max_workers}, max_async_concurrent={n})"
                ),
                None => format!("Executor.Parallel(max_workers={max_workers})"),
            },
            Self::Kubernetes {
                worker_image,
                namespace,
                ..
            } => format!(
                "Executor.Kubernetes(worker_image={}, namespace={})",
                worker_image
                    .as_deref()
                    .map_or("auto".to_string(), |v| format!("'{v}'")),
                namespace
                    .as_deref()
                    .map_or("auto".to_string(), |v| format!("'{v}'")),
            ),
        }
    }
}

impl Executor {
    pub(crate) fn from_metadata(value: &str) -> PyResult<Self> {
        match value {
            "in_process" => Ok(Executor::InProcess {}),
            "parallel" => Ok(Executor::Parallel {
                max_workers: default_max_workers(),
                max_async_concurrent: None,
            }),
            other => Err(ConfigurationError::new_err(format!(
                "Unknown executor in {} metadata: '{other}'. \
                 Valid values: 'in_process', 'parallel'",
                metadata_keys::EXECUTOR,
            ))),
        }
    }

    fn run_batch(
        &self,
        py: Python,
        ctx: &mut dispatch::BatchContext,
        step_indices: &[usize],
    ) -> Vec<(String, PyErr)> {
        match self {
            Executor::InProcess {} => {
                dispatch::execute_level_batch(&in_process::InProcessBackend, py, ctx, step_indices)
            }
            Executor::Parallel {
                max_workers,
                max_async_concurrent,
            } => dispatch::execute_level_batch(
                &parallel::ParallelBackend {
                    max_workers: *max_workers,
                    max_async_concurrent: *max_async_concurrent,
                },
                py,
                ctx,
                step_indices,
            ),
            Executor::Kubernetes {
                worker_image,
                max_concurrent_steps,
                namespace,
                service_account,
                worker_cpu,
                worker_memory,
            } => dispatch::execute_level_batch(
                &kubernetes::KubernetesBackend::new(
                    worker_image
                        .clone()
                        .or_else(rivers_k8s::env::detect_code_location_image),
                    *max_concurrent_steps,
                    namespace
                        .clone()
                        .unwrap_or_else(rivers_k8s::env::detect_namespace),
                    service_account.clone(),
                    worker_cpu.clone(),
                    worker_memory.clone(),
                ),
                py,
                ctx,
                step_indices,
            ),
        }
    }

    /// Execute the full plan with collecting semantics.
    /// Iterates levels, respects per-asset executor overrides, and continues past failures.
    /// Returns a list of `(step_name, error)` for all failures.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn execute_plan(
        &self,
        py: Python,
        plan: &ExecutionPlan,
        node_map: &HashMap<String, ResolvedNode>,
        partition_key: &Option<PyPartitionKey>,
        storage: StorageHandle,
        run_id: &str,
        resources: &HashMap<String, ResourceVariant>,
        config_overrides: &Option<HashMap<String, Py<PyAny>>>,
        io_handler_registry: &IOHandlerRegistry,
        resume: bool,
    ) -> Vec<(String, PyErr)> {
        if !matches!(self, Executor::Kubernetes { .. }) {
            let ignored: Vec<&str> = node_map
                .iter()
                .filter(|(_, n)| n.compute().is_some())
                .map(|(name, _)| name.as_str())
                .collect();
            if !ignored.is_empty() {
                tracing::warn!(
                    assets = ?ignored,
                    "compute= is ignored by the in_process/parallel executors; \
                     it applies on Kubernetes"
                );
            }
        }

        let needs_bridge = plan
            .steps
            .iter()
            .any(|s| node_map.get(&s.name).map(|n| n.is_async()).unwrap_or(false));

        let bridge = if needs_bridge {
            match async_exec::AsyncBridge::new(py) {
                Ok(b) => Some(b),
                Err(e) => {
                    tracing::error!("Failed to create AsyncBridge for async steps: {e}");
                    None
                }
            }
        } else {
            None
        };

        py.detach(|| {
            let writer = EventWriter::new(storage.clone());
            let mut data_versions: HashMap<String, String> = HashMap::new();
            let mut failed_names: HashSet<String> = HashSet::new();

            let completed_steps: HashSet<String> = if resume {
                match rt().block_on(rivers_k8s::resume::build_resume_state(
                    storage.backend().as_ref(),
                    run_id,
                )) {
                    Ok(state) => {
                        data_versions.extend(state.data_versions);
                        state.completed_steps
                    }
                    Err(e) => {
                        tracing::error!(run_id, error = %e, "Failed to load resume state — running from scratch");
                        HashSet::new()
                    }
                }
            } else {
                HashSet::new()
            };
            let mut graph_started: HashSet<String> = HashSet::new();
            let mut failures: Vec<(String, PyErr)> = Vec::new();
            // Persistent across levels: fan-out mapping keys and collect results
            let mut mapped_instance_keys: HashMap<String, Vec<String>> = HashMap::new();
            // Per-step dynamic_keys captured when the source asset runs locally in
            // this orchestrator process. Used by the fan-out path to skip stale
            // on-disk `__keys` files when the source has actually been re-run with
            // plain values in this batch.
            let mut step_dynamic_keys: HashMap<String, Vec<String>> = HashMap::new();
            let mut step_failed_partitions: HashMap<String, Vec<(PyPartitionKey, String)>> =
                HashMap::new();

            prefill_external_dep_versions(plan, storage, &mut data_versions);

            let mut graph_nodes = GraphNodeMap {
                final_nodes: HashMap::new(),
            };
            for (name, node) in node_map {
                if let Some(final_n) = node.graph_final_node() {
                    graph_nodes.final_nodes.insert(final_n, name.clone());
                }
            }

            let levels = plan.group_steps_by_level();

            for (level_idx, level) in levels.iter().enumerate() {
                if matches!(
                    io_rt().block_on(storage.backend().is_cancelled(run_id)),
                    Ok(true)
                ) {
                    let remaining: Vec<&str> = levels[level_idx..]
                        .iter()
                        .flat_map(|l| l.iter())
                        .map(|&idx| plan.steps[idx].name.as_str())
                        .filter(|n| !completed_steps.contains(*n) && !failed_names.contains(*n))
                        .collect();
                    tracing::info!(
                        run_id,
                        deferred = ?remaining,
                        "Execution cancelled, remaining steps deferred"
                    );
                    break;
                }

                // For namespaced tasks (graph/task), check the parent graph asset's
                // NODE_EXECUTOR first, then EXECUTOR, then default.
                let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
                let mut metadata_cache: HashMap<String, Option<HashMap<String, String>>> =
                    HashMap::new();
                for &step_idx in level {
                    let step = &plan.steps[step_idx];
                    let lookup_name = if step.name.contains('/') {
                        step.name.split('/').next().unwrap()
                    } else {
                        step.name.as_str()
                    };
                    if !metadata_cache.contains_key(lookup_name) {
                        let meta = node_map.get(lookup_name).and_then(|n| n.metadata());
                        metadata_cache.insert(lookup_name.to_string(), meta);
                    }
                    let executor_key = metadata_cache
                        .get(lookup_name)
                        .and_then(|m| m.as_ref())
                        .and_then(|m| {
                            if step.name.contains('/') {
                                m.get(metadata_keys::NODE_EXECUTOR)
                                    .or_else(|| m.get(metadata_keys::EXECUTOR))
                            } else {
                                m.get(metadata_keys::EXECUTOR)
                            }
                        })
                        .cloned()
                        .unwrap_or_else(|| DEFAULT_EXECUTOR_KEY.to_string());
                    groups.entry(executor_key).or_default().push(step_idx);
                }

                for (executor_key, step_indices) in &groups {
                    let executor = if executor_key == DEFAULT_EXECUTOR_KEY {
                        self.clone()
                    } else {
                        match Executor::from_metadata(executor_key) {
                            Ok(e) => e,
                            Err(e) => {
                                // NOTE: maybe hard stop here because this is a big configuration issue
                                // Mark all steps in this group as failed
                                let msg = e.to_string();
                                for &step_idx in step_indices {
                                    let step = &plan.steps[step_idx];
                                    for name in step.event_names() {
                                        ops::emit_step_failure(
                                            &writer, run_id, name, &msg, now_ts(),
                                        );
                                        failures.push((
                                            name.clone(),
                                            ConfigurationError::new_err(msg.clone()),
                                        ));
                                        failed_names.insert(name.clone());
                                    }
                                }
                                continue;
                            }
                        }
                    };

                    let mut ctx = dispatch::BatchContext {
                        scope: dispatch::RunScope {
                            run_id,
                            partition_key,
                            plan,
                            completed_steps: &completed_steps,
                        },
                        state: dispatch::RunState {
                            data_versions: &mut data_versions,
                            failed_names: &mut failed_names,
                            graph_started: &mut graph_started,
                            mapped_instance_keys: &mut mapped_instance_keys,
                            step_dynamic_keys: &mut step_dynamic_keys,
                            failed_partitions: &mut step_failed_partitions,
                        },
                        sink: dispatch::EventSink {
                            writer: &writer,
                            storage,
                        },
                        repo: dispatch::Repo {
                            node_map,
                            graph_nodes: &graph_nodes,
                            io_handler_registry,
                            resources,
                            config_overrides,
                            bridge: bridge.as_ref(),
                        },
                    };
                    let batch_failures =
                        Python::attach(|py| executor.run_batch(py, &mut ctx, step_indices));
                    failures.extend(batch_failures);
                }
            }

            if let Some(ref b) = bridge {
                Python::attach(|py| b.shutdown(py));
            }

            // Defense-in-depth: catch leaked slots from crashed steps.
            if let Err(e) =
                io_rt().block_on(storage.backend().free_concurrency_slots_for_run(run_id))
            {
                tracing::warn!(run_id = %run_id, error = %e, "failed to free run-level concurrency slots");
            }

            writer.flush();

            failures
        })
    }
}

/// Pre-populate `data_versions` with `last_data_version` from storage for graph
/// dependencies that are NOT steps in the plan (i.e. loaded via IO handler).
/// Single batch query instead of N individual lookups.
fn prefill_external_dep_versions(
    plan: &ExecutionPlan,
    storage: StorageHandle,
    data_versions: &mut HashMap<String, String>,
) {
    let step_names: HashSet<&str> = plan
        .steps
        .iter()
        .flat_map(|s| std::iter::once(s.name.as_str()).chain(s.outputs.iter().map(|o| o.as_str())))
        .collect();
    let external_deps: Vec<String> = plan
        .steps
        .iter()
        .flat_map(|s| s.graph_dependencies.iter())
        .filter(|dep| !step_names.contains(dep.as_str()))
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if !external_deps.is_empty()
        && let Ok(records) =
            io_rt().block_on(storage.scoped().get_asset_records_by_keys(&external_deps))
    {
        for rec in records {
            if let Some(dv) = rec.last_data_version {
                data_versions.insert(rec.asset_key, dv);
            }
        }
    }
}

pub fn register_executor_module(parent_module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_submodule!(parent_module, "executor", [
        Executor as "Executor",
    ])
}
