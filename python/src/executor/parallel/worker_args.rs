//! Worker argument serialization for cross-process step execution via loky.
use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::assets::io_handler_registry::{IOHandlerRegistry, IOHandlerSource};
use crate::config::ResourceVariant;
use crate::errors::ExecutionError;
use crate::partitions::PyPartitionKey;
use crate::repository::resolved_node::ResolvedNode;

use super::super::ops::{self, is_context_annotation};

pub(super) struct WorkerArgs {
    /// Function arguments (direct values loaded in parent, or placeholders for resources).
    pub args: Vec<Py<PyAny>>,
    /// Context kwargs dict for AssetExecutionContext, or None.
    pub context_kwargs: Option<Py<PyAny>>,
    /// (arg_index, resource_py_object) for resource serialization.
    pub resource_positions: Vec<(usize, ResourceVariant)>,
    /// Index of the fan-out source placeholder in `args` (for mapped steps).
    pub fan_out_arg_index: Option<usize>,
    /// Resolved Pydantic config instance (parent-side copy). Threaded through
    /// to `WorkOutcome::Error::failure_config` so failure hooks see the same
    /// instance the worker's `context.config` produced.
    pub config_instance: Option<Py<PyAny>>,
}

/// Resolve function inputs for the parallel worker path.
/// Context is now supported — extracted as kwargs for subprocess construction.
/// All upstream nodes have IO handlers after resolve, so inputs are loaded
/// via _IOLoadSpec in the subprocess.
/// `fan_out_source`: if set, the named upstream node gets a placeholder instead of
/// _IOLoadSpec — the caller injects the actual item before submission.
/// `config_overrides_for_step`: per-step Pydantic-model kwargs from
/// `Job(config=...)`. The resolved config instance is added to `context_kwargs`
/// so the worker reconstructs `AssetExecutionContext.config` correctly.
#[allow(clippy::too_many_arguments)]
pub(super) fn resolve_worker_args(
    py: Python,
    step_name: &str,
    func: &Py<PyAny>,
    node_map: &HashMap<String, ResolvedNode>,
    partition_key: &Option<PyPartitionKey>,
    resources: &HashMap<String, ResourceVariant>,
    fan_out_source: Option<&str>,
    input_overrides: &HashMap<String, Py<PyAny>>,
    output_selection: &[String],
    registry: &IOHandlerRegistry,
    config_overrides_for_step: Option<&Bound<'_, PyDict>>,
) -> PyResult<WorkerArgs> {
    let node = node_map.get(step_name).expect("step must be in node_map");

    if node.is_bash_task() {
        return Ok(WorkerArgs {
            args: Vec::new(),
            context_kwargs: None,
            resource_positions: Vec::new(),
            fan_out_arg_index: None,
            config_instance: None,
        });
    }

    let param_remap = node.param_remap();
    let mut args = Vec::new();
    let mut resource_positions = Vec::new();
    let mut context_kwargs = None;
    let mut fan_out_arg_index = None;
    let mut config_instance: Option<Py<PyAny>> = None;
    let mut is_first_param = true;

    for (param_name, annotation) in ops::enumerate_params(py, func)? {
        if param_name == "return" {
            continue;
        }

        let is_ctx = annotation
            .as_ref()
            .is_some_and(|a| is_context_annotation(py, a));

        if is_first_param {
            is_first_param = false;
            if is_ctx || (param_name == "context" && !node_map.contains_key(&param_name)) {
                let kwargs = PyDict::new(py);
                let is_multi = !output_selection.is_empty();
                let context_name = if is_multi {
                    node.name().unwrap_or_else(|_| step_name.to_string())
                } else {
                    step_name.to_string()
                };
                kwargs.set_item("asset_name", context_name)?;
                kwargs.set_item("tags", node.tags())?;
                kwargs.set_item("kinds", node.kinds())?;
                kwargs.set_item("group", node.group())?;
                kwargs.set_item("code_version", node.code_version())?;
                kwargs.set_item("asset_metadata", node.metadata())?;
                let partition = ops::build_partition_context(node, partition_key)?;
                kwargs.set_item("partition", partition)?;
                kwargs.set_item("is_multi_asset", is_multi)?;
                kwargs.set_item("output_selection", output_selection.to_vec())?;

                // Resolve config from the parameterized context annotation
                // (e.g. AssetExecutionContext[MyConfig]) so the worker sees the
                // same `context.config` instance the in-process path would
                // produce. Pydantic models pickle via cloudpickle on submit.
                // Keep a parent-side clone for `WorkOutcome::Error::failure_config`
                // so failure hooks see the same instance.
                if let Some(ref a) = annotation
                    && is_ctx
                    && let Some(cfg) =
                        ops::extract_config_from_annotation(py, a, config_overrides_for_step)?
                {
                    kwargs.set_item("config", cfg.clone_ref(py))?;
                    config_instance = Some(cfg);
                }

                context_kwargs = Some(kwargs.unbind().into_any());
                continue;
            }
        } else if is_ctx {
            return Err(ExecutionError::new_err(format!(
                "Context must be the first parameter of '{}'",
                step_name
            )));
        }

        // Self-dependency: load in parent, pickle to worker. No fallback —
        // parallel needs a real handler since cross-subprocess self-deps
        // can't rely on in-process state.
        if param_name == "self" {
            let annotation = annotation.ok_or_else(|| {
                ExecutionError::new_err(format!(
                    "Asset '{}': `self` parameter requires a `SelfDependency[T]` annotation",
                    step_name
                ))
            })?;
            let self_dep = ops::load_self_dependency(
                py,
                step_name,
                node,
                partition_key,
                &annotation,
                registry,
                None,
            )?;
            args.push(self_dep);
            continue;
        }

        let resolved_name = param_remap
            .and_then(|m| m.get(&param_name))
            .cloned()
            .unwrap_or_else(|| param_name.clone());

        if fan_out_source == Some(resolved_name.as_str()) {
            fan_out_arg_index = Some(args.len());
            args.push(py.None());
            continue;
        }

        if let Some(val) = input_overrides.get(&resolved_name) {
            args.push(val.clone_ref(py));
            continue;
        }

        // Upstream: load via _IOLoadSpec — worker loads directly from IO handler.
        // ForKeys/Subset may produce Skip, in which case push py.None() directly.
        if let Some(upstream_node) = node_map.get(&resolved_name) {
            let resolution = ops::map_partition_key_for_upstream(
                &resolved_name,
                node,
                upstream_node,
                partition_key,
            )?;
            match resolution {
                ops::UpstreamKeyResolution::Skip => {
                    args.push(py.None());
                }
                ops::UpstreamKeyResolution::Load(_) => {
                    // Type hint is `None` for unannotated params.
                    let type_hint = annotation
                        .as_ref()
                        .map(|a| a.clone().unbind())
                        .unwrap_or_else(|| py.None());
                    let spec = build_io_load_spec(
                        py,
                        &resolved_name,
                        step_name,
                        node,
                        upstream_node,
                        partition_key,
                        type_hint,
                        registry,
                    )?;
                    args.push(spec);
                }
            }
            continue;
        }

        if let Some(resource) = resources.get(&param_name) {
            let idx = args.len();
            resource_positions.push((idx, resource.clone_ref(py)));
            // Placeholder — replaced by worker wrapper.
            args.push(py.None());
        }
    }

    Ok(WorkerArgs {
        args,
        context_kwargs,
        resource_positions,
        fan_out_arg_index,
        config_instance,
    })
}

/// Build an _IOLoadSpec Python object for loading an upstream input in the subprocess.
/// This avoids pickling large data across process boundaries — the worker loads
/// directly from the IO handler.
fn build_io_load_spec(
    py: Python,
    param_name: &str,
    downstream_name: &str,
    downstream_node: &ResolvedNode,
    upstream_node: &ResolvedNode,
    partition_key: &Option<PyPartitionKey>,
    type_hint: Py<PyAny>,
    registry: &IOHandlerRegistry,
) -> PyResult<Py<PyAny>> {
    let (handler, source) =
        registry.for_upstream_input_with_source(py, downstream_node, upstream_node, param_name);

    // Pickle-safe transport. Only wrap with `IOHandlerRef` (which reconstructs
    // by re-importing `module.qualname.io_handler` in the subprocess) when the
    // handler came from upstream's own definition or a graph override — those
    // ARE re-discoverable via the asset's import path. An `InputOverride` and
    // the registry `Default` are arbitrary instances with no stable lookup
    // path; ship them raw and let cloudpickle handle them.
    let pickled_handler = match source {
        IOHandlerSource::Definition | IOHandlerSource::GraphOverride => {
            let upstream_func = upstream_node.callable(py)?;
            make_io_handler_ref(py, &upstream_func).unwrap_or(handler)
        }
        IOHandlerSource::InputOverride | IOHandlerSource::Default => handler,
    };

    let metadata = downstream_node
        .input_metadata(py, param_name)
        .or_else(|| upstream_node.metadata());

    // Skip is already handled by the caller — build_io_load_spec is only called
    // for Load resolutions.
    let has_mapping = downstream_node
        .partition_mapping()
        .and_then(|m| m.get(param_name).cloned())
        .is_some();
    let upstream_key = match ops::map_partition_key_for_upstream(
        param_name,
        downstream_node,
        upstream_node,
        partition_key,
    )? {
        ops::UpstreamKeyResolution::Load(key) => key,
        ops::UpstreamKeyResolution::Skip => {
            unreachable!("build_io_load_spec called for a Skip resolution")
        }
    };
    let partition = if has_mapping {
        ops::build_mapped_partition_context(upstream_node, &upstream_key)?
    } else {
        ops::build_partition_context(upstream_node, &upstream_key)?
    };

    let kwargs = PyDict::new(py);
    kwargs.set_item("asset_name", param_name)?;
    kwargs.set_item("downstream_asset", downstream_name)?;
    kwargs.set_item("asset_metadata", metadata)?;
    kwargs.set_item("partition", partition)?;
    kwargs.set_item("type_hint", type_hint)?;

    let spec = super::worker::PyIOLoadSpec {
        handler: pickled_handler,
        input_context_kwargs: kwargs.unbind().into_any(),
    };
    Ok(Py::new(py, spec)?.into_any())
}

/// Extract (module, qualname) from a callable for pickle-safe ref construction.
/// Returns Err for closures/local functions (qualname contains `<locals>`).
pub(crate) fn extract_module_qualname(py: Python, func: &Py<PyAny>) -> PyResult<(String, String)> {
    let module: String = func.getattr(py, "__module__")?.extract(py)?;
    let qualname: String = func.getattr(py, "__qualname__")?.extract(py)?;
    if qualname.contains("<") {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "qualname contains <locals>",
        ));
    }
    Ok((module, qualname))
}

/// Wrap a callable in a `FuncRef` for pickle-safe transport to loky workers.
/// Falls back to direct pickling for closures/local functions.
pub(crate) fn make_func_ref(py: Python, func: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    let (module, qualname) = extract_module_qualname(py, func)?;
    Ok(Py::new(py, super::worker::PyFuncRef::new(module, qualname))?.into_any())
}

/// Wrap an IO handler in an `IOHandlerRef` for pickle-safe transport.
/// Falls back to raw handler for closures/local functions.
pub(super) fn make_io_handler_ref(py: Python, func: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    let (module, qualname) = extract_module_qualname(py, func)?;
    Ok(Py::new(py, super::worker::PyIOHandlerRef::new(module, qualname))?.into_any())
}

/// Resolve a node's *output* IO handler for pickle-safe transport to a loky
/// worker. Walks the registry's `for_output` chain and applies `IOHandlerRef`
/// wrapping when the source has a stable import-path lookup.
///
/// - `Definition` → wrap via `task_func` (worker re-imports `module.qualname.io_handler`).
/// - `GraphOverride` → wrap via the parent graph asset's callable
///   (worker re-imports `parent_graph.qualname.node_io_handler`).
/// - `Default` → ship the registry default raw (it's a process-shared instance).
///
/// Each wrap step falls back to the raw handler when `make_io_handler_ref`
/// fails (callables defined inside `<locals>`, e.g. test closures).
pub(super) fn resolve_io_handler_ref(
    py: Python,
    step_name: &str,
    task_func: &Py<PyAny>,
    node_map: &HashMap<String, ResolvedNode>,
    registry: &IOHandlerRegistry,
) -> Py<PyAny> {
    let node = match node_map.get(step_name) {
        Some(n) => n,
        None => return registry.default_handler(py),
    };
    let (handler, source) = registry.for_output_with_source(py, node);
    match source {
        IOHandlerSource::GraphOverride => {
            // Reconstructed from the parent graph asset's `node_io_handler`.
            // Look up by typed `parent_graph_name` rather than splitting the
            // namespaced step name on `/`.
            let parent_func = node
                .parent_graph_name()
                .and_then(|name| node_map.get(name))
                .and_then(|gn| gn.callable(py).ok());
            parent_func
                .and_then(|gf| make_io_handler_ref(py, &gf).ok())
                .unwrap_or(handler)
        }
        IOHandlerSource::Definition => make_io_handler_ref(py, task_func).unwrap_or(handler),
        IOHandlerSource::Default | IOHandlerSource::InputOverride => handler,
    }
}

/// Build submit args for loky using _worker_execute_step.
/// Always wraps with the worker function for consistent resource/context/output handling.
/// `instance_name`: overrides `step_name` for IO output context (e.g. "double__0" for map instances).
/// `fan_out_item`: if set, replaces the placeholder at `worker_args.fan_out_arg_index`.
pub(super) fn build_worker_submit_args(
    py: Python,
    func: Py<PyAny>,
    step_name: &str,
    instance_name: Option<&str>,
    node: &ResolvedNode,
    partition_key: &Option<PyPartitionKey>,
    worker_args: &WorkerArgs,
    fan_out_item: Option<&Py<PyAny>>,
    node_map: &HashMap<String, ResolvedNode>,
    registry: &IOHandlerRegistry,
    multi_outputs: &[String],
) -> PyResult<Vec<Py<PyAny>>> {
    let output_name = instance_name.unwrap_or(step_name);
    let is_multi = !multi_outputs.is_empty();

    let worker_fn = py.import("rivers._core")?.getattr("worker_execute_step")?;

    let py_args = PyList::new(py, &worker_args.args)?;
    if let (Some(idx), Some(item)) = (worker_args.fan_out_arg_index, fan_out_item) {
        py_args.set_item(idx, item.bind(py))?;
    }

    // (param_index, class, json_data) per resource.
    let py_resource_specs = PyList::empty(py);
    for (idx, resource) in &worker_args.resource_positions {
        let resource = resource.inner();
        let cls = resource.getattr(py, "__class__")?;
        let json_data: Py<PyAny> = resource.call_method0(py, "model_dump_json")?;
        let tuple = PyTuple::new(
            py,
            &[idx.into_pyobject(py)?.into_any().unbind(), cls, json_data],
        )?;
        py_resource_specs.append(tuple)?;
    }

    // Wrap func in _FuncRef for pickle-safe transport. Falls back to raw
    // func if __module__/__qualname__ are unavailable (e.g. lambdas in tests).
    let pickled_func = make_func_ref(py, &func).unwrap_or_else(|_| func.clone_ref(py));

    let io_spec = if is_multi {
        build_multi_output_specs(py, multi_outputs, &func, node_map, partition_key, registry)?
    } else {
        build_single_output_spec(
            py,
            output_name,
            step_name,
            node,
            &func,
            partition_key,
            node_map,
            registry,
        )?
    };

    let submit_args = vec![
        worker_fn.unbind(),
        pickled_func,
        py_args.unbind().into_any(),
        worker_args
            .context_kwargs
            .as_ref()
            .map(|c| c.clone_ref(py))
            .unwrap_or_else(|| py.None()),
        py_resource_specs.unbind().into_any(),
        io_spec.io_handler(py),
        io_spec.output_context_kwargs(py),
        io_spec.multi_output_specs(py),
    ];

    Ok(submit_args)
}

enum WorkerIOSpec {
    Single {
        io_handler: Py<PyAny>,
        output_context_kwargs: Py<PyAny>,
    },
    Multi(Py<PyAny>),
}

impl WorkerIOSpec {
    fn io_handler(&self, py: Python) -> Py<PyAny> {
        match self {
            Self::Single { io_handler, .. } => io_handler.clone_ref(py),
            Self::Multi(_) => py.None(),
        }
    }

    fn output_context_kwargs(&self, py: Python) -> Py<PyAny> {
        match self {
            Self::Single {
                output_context_kwargs,
                ..
            } => output_context_kwargs.clone_ref(py),
            Self::Multi(_) => py.None(),
        }
    }

    fn multi_output_specs(&self, py: Python) -> Py<PyAny> {
        match self {
            Self::Single { .. } => py.None(),
            Self::Multi(specs) => specs.clone_ref(py),
        }
    }
}

fn build_single_output_spec(
    py: Python,
    output_name: &str,
    step_name: &str,
    node: &ResolvedNode,
    func: &Py<PyAny>,
    partition_key: &Option<PyPartitionKey>,
    node_map: &HashMap<String, ResolvedNode>,
    registry: &IOHandlerRegistry,
) -> PyResult<WorkerIOSpec> {
    let io_handler = node.io_handler(py);
    let has_io = io_handler.is_some();
    let output_context_kwargs: Py<PyAny> = if has_io {
        let kwargs = PyDict::new(py);
        kwargs.set_item("asset_name", output_name)?;
        kwargs.set_item("asset_metadata", node.metadata())?;
        let partition = ops::build_partition_context(node, partition_key)?;
        kwargs.set_item("partition", partition)?;
        let return_hint = ops::extract_return_hint(py, func)?;
        kwargs.set_item("type_hint", return_hint)?;
        kwargs.unbind().into_any()
    } else {
        py.None()
    };
    let handler = if has_io {
        resolve_io_handler_ref(py, step_name, func, node_map, registry)
    } else {
        py.None()
    };
    Ok(WorkerIOSpec::Single {
        io_handler: handler,
        output_context_kwargs,
    })
}

fn build_multi_output_specs(
    py: Python,
    outputs: &[String],
    func: &Py<PyAny>,
    node_map: &HashMap<String, ResolvedNode>,
    partition_key: &Option<PyPartitionKey>,
    registry: &IOHandlerRegistry,
) -> PyResult<WorkerIOSpec> {
    let return_hint = ops::extract_return_hint(py, func)?;
    let specs = PyList::empty(py);
    for out_name in outputs {
        let out_node = node_map.get(out_name).ok_or_else(|| {
            pyo3::exceptions::PyKeyError::new_err(format!(
                "Multi-asset output '{}' not in node_map",
                out_name
            ))
        })?;
        let handler_ref = resolve_io_handler_ref(py, out_name, func, node_map, registry);
        let kwargs = PyDict::new(py);
        kwargs.set_item("asset_name", out_name)?;
        kwargs.set_item("asset_metadata", out_node.metadata())?;
        let partition = ops::build_partition_context(out_node, partition_key)?;
        kwargs.set_item("partition", partition)?;
        kwargs.set_item("type_hint", &return_hint)?;
        let tuple = PyTuple::new(
            py,
            &[
                out_name.into_pyobject(py)?.into_any().unbind(),
                handler_ref,
                kwargs.unbind().into_any(),
            ],
        )?;
        specs.append(tuple)?;
    }
    Ok(WorkerIOSpec::Multi(specs.unbind().into_any()))
}
