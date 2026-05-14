//! Step invocation — calls Python asset/task functions with argument injection.
//!
//! Resolves upstream values via IO handlers, injects `AssetExecutionContext` and resources
//! based on type-hint annotations, performs output type validation, and unwraps `Output` /
//! `Observation` / `DynamicOutput` wrappers. Detects composition context for graph assets.
use std::collections::HashMap;

use pyo3::PyTypeInfo;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyNone, PyTuple};
use rivers_core::execution::plan::ExecutionStep;

use crate::assets::io_handler_registry::IOHandlerRegistry;
use crate::config::ResourceVariant;
use crate::context::asset::PyAssetExecutionContext;
use crate::context::task::PyTaskExecutionContext;
use crate::errors::{AssetOutputValidationError, ConfigurationError, ExecutionError};
use crate::partitions::PyPartitionKey;
use crate::repository::resolved_node::ResolvedNode;
use crate::result_types;

use super::StepResult;
use super::io::{build_partition_context, load_self_dependency, load_upstream_input};

pub(crate) fn annotation_is(annotation: &Bound<PyAny>, type_obj: &Bound<PyAny>) -> bool {
    if annotation.is(type_obj) {
        return true;
    }
    if let Ok(origin) = annotation.getattr("__origin__") {
        return origin.is(type_obj);
    }
    false
}

pub(crate) fn is_context_annotation(py: Python, annotation: &Bound<PyAny>) -> bool {
    let asset_ctx = PyAssetExecutionContext::type_object(py);
    let task_ctx = PyTaskExecutionContext::type_object(py);
    annotation_is(annotation, asset_ctx.as_any()) || annotation_is(annotation, task_ctx.as_any())
}

pub(crate) fn is_task_context_annotation(py: Python, annotation: &Bound<PyAny>) -> bool {
    annotation_is(annotation, PyTaskExecutionContext::type_object(py).as_any())
}

pub(crate) fn get_annotations<'py>(
    py: Python<'py>,
    func: &Py<PyAny>,
) -> PyResult<Bound<'py, PyDict>> {
    let annotations = func.getattr(py, "__annotations__")?;
    Ok(annotations.cast_bound::<PyDict>(py)?.clone())
}

/// Enumerate `(name, optional annotation)` for every *injectable* parameter on
/// `func` via `inspect.signature` in declaration order. Includes unannotated
/// params (`def downstream(upstream)`) — `__annotations__` alone would silently
/// drop them, breaking dep inference and arg injection.
///
/// **Skipped:**
/// - Params with a default value (`def leaf(root, _i=i)`) — Python supplies
///   the default; we'd misread the param as an unresolved dep otherwise.
/// - Variadic `*args` / `**kwargs` — never injectable, only collect leftovers.
///
/// Callers fall back to the param NAME when annotation is `None` (e.g.
/// matching against asset / resource names) and skip annotation-typed checks
/// like `is_context_annotation`.
pub(crate) fn enumerate_params<'py>(
    py: Python<'py>,
    func: &Py<PyAny>,
) -> PyResult<Vec<(String, Option<Bound<'py, PyAny>>)>> {
    let inspect = py.import("inspect")?;
    let signature = inspect.call_method1("signature", (func.bind(py),))?;
    let parameters = signature.getattr("parameters")?;
    let parameter_cls = inspect.getattr("Parameter")?;
    let empty_sentinel = parameter_cls.getattr("empty")?;
    let var_positional = parameter_cls.getattr("VAR_POSITIONAL")?;
    let var_keyword = parameter_cls.getattr("VAR_KEYWORD")?;

    let mut out = Vec::new();
    for item in parameters.call_method0("values")?.try_iter()? {
        let param = item?;
        let kind = param.getattr("kind")?;
        if kind.eq(&var_positional)? || kind.eq(&var_keyword)? {
            continue;
        }
        if !param.getattr("default")?.is(&empty_sentinel) {
            continue;
        }
        let name: String = param.getattr("name")?.extract()?;
        let annotation = param.getattr("annotation")?;
        let annotation = if annotation.is(&empty_sentinel) {
            None
        } else {
            Some(annotation)
        };
        out.push((name, annotation));
    }
    Ok(out)
}

pub(crate) fn extract_return_hint(py: Python, func: &Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
    let annotations = get_annotations(py, func)?;
    Ok(annotations.get_item("return")?.map(|v| v.unbind()))
}

/// Validate that a return value matches the declared return type hint.
/// Handles common cases: basic types, None, Any, Optional, Union, generic containers.
/// Raises AssetOutputValidationError on mismatch.
pub(crate) fn validate_return_type(
    py: Python,
    result: &Py<PyAny>,
    return_hint: Option<&Py<PyAny>>,
    step_name: &str,
) -> PyResult<()> {
    let hint = match return_hint {
        Some(h) => h,
        None => return Ok(()),
    };

    let hint_bound = hint.bind(py);

    if hint_bound.is_none() {
        return Ok(());
    }

    // Output/Observation/Materialization hints: value was already unwrapped before validation.
    let output_type = result_types::PyOutput::type_object(py);
    let observation_type = result_types::PyObservation::type_object(py);
    let materialization_type = result_types::PyMaterialization::type_object(py);
    if hint_bound.is(&output_type)
        || hint_bound.is(&observation_type)
        || hint_bound.is(&materialization_type)
    {
        return Ok(());
    }

    let typing = py.import("typing")?;
    let isinstance = py.import("builtins")?.getattr("isinstance")?;

    let any_type = typing.getattr("Any")?;
    if hint_bound.is(&any_type) {
        return Ok(());
    }

    let result_bound = result.bind(py);
    let get_origin = typing.getattr("get_origin")?;
    let origin = get_origin.call1((hint_bound,))?;

    if !origin.is_none() {
        let types_union = py.import("types")?.getattr("UnionType")?;
        let union_origin = typing.getattr("Union")?;
        let is_union = origin.is(&union_origin) || origin.eq(&types_union).unwrap_or(false);

        if is_union {
            let get_args = typing.getattr("get_args")?;
            let args = get_args.call1((hint_bound,))?;
            for arg in args.try_iter()? {
                let arg = arg?;
                if result_bound.is_instance_of::<PyNone>() {
                    let none_type = PyNone::get(py).get_type();
                    if arg.eq(&none_type).unwrap_or(false) {
                        return Ok(());
                    }
                }
                if isinstance
                    .call1((result_bound, &arg))
                    .ok()
                    .and_then(|r| r.is_truthy().ok())
                    .unwrap_or(false)
                {
                    return Ok(());
                }
            }
            return Err(AssetOutputValidationError::new_err(format!(
                "Asset '{}' returned value of type '{}' but expected '{}'",
                step_name,
                result_bound.get_type().qualname()?,
                hint_bound,
            )));
        }

        if isinstance
            .call1((result_bound, &origin))
            .ok()
            .and_then(|r| r.is_truthy().ok())
            .unwrap_or(false)
        {
            return Ok(());
        }

        return Err(AssetOutputValidationError::new_err(format!(
            "Asset '{}' returned value of type '{}' but expected '{}'",
            step_name,
            result_bound.get_type().qualname()?,
            hint_bound,
        )));
    }

    if result_bound.is_instance_of::<PyNone>() {
        let none_type = PyNone::get(py).get_type();
        if hint_bound.eq(&none_type).unwrap_or(false) {
            return Ok(());
        }
    }

    match isinstance.call1((result_bound, hint_bound)) {
        Ok(r) if r.is_truthy().unwrap_or(false) => Ok(()),
        _ => Err(AssetOutputValidationError::new_err(format!(
            "Asset '{}' returned value of type '{}' but expected '{}'",
            step_name,
            result_bound.get_type().qualname()?,
            hint_bound,
        ))),
    }
}

/// e.g. given the annotation for `AssetExecutionContext[MyConfig]`, extracts and instantiates `MyConfig`.
pub(crate) fn extract_config_from_annotation(
    py: Python,
    annotation: &Bound<PyAny>,
    overrides: Option<&Bound<PyDict>>,
) -> PyResult<Option<Py<PyAny>>> {
    use crate::config::ResourceVariant;
    if let Ok(args) = annotation.getattr("__args__")
        && let Ok(first_arg) = args.get_item(0)
    {
        let config_variant: ResourceVariant = first_arg.as_borrowed().extract()?;
        return Ok(Some(config_variant.instantiate_config(py, overrides)?));
    }
    Ok(None)
}

pub(crate) struct BuiltStepArgs {
    pub args: Vec<Py<PyAny>>,
    pub return_hint: Option<Py<PyAny>>,
    pub config_instance: Option<Py<PyAny>>,
    pub context_injected: bool,
    /// The context object (first arg) if context was injected.
    pub ctx_ref: Option<Py<PyAny>>,
}

/// Build the argument list for a step by resolving annotations, context injection,
/// upstream inputs, resources, and config. `config_out` receives a clone of the
/// resolved config instance as soon as it's resolved, so callers can pass it to
/// failure hooks even if a later step (input loading, function call) fails.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_step_args(
    py: Python,
    step: &ExecutionStep,
    node: &ResolvedNode,
    node_map: &HashMap<String, ResolvedNode>,
    partition_key: &Option<PyPartitionKey>,
    resources: &HashMap<String, ResourceVariant>,
    config_overrides: &Option<HashMap<String, Py<PyAny>>>,
    registry: &IOHandlerRegistry,
    input_overrides: &HashMap<String, Py<PyAny>>,
    config_out: &mut Option<Py<PyAny>>,
) -> PyResult<BuiltStepArgs> {
    let partition = build_partition_context(node, partition_key)?;
    let func = node.callable(py)?;
    let return_hint = extract_return_hint(py, &func)?;

    let overrides_dict = config_overrides
        .as_ref()
        .and_then(|m| m.get(&step.name))
        .map(|obj| obj.bind(py).cast::<PyDict>())
        .transpose()?;
    let mut is_first_param = true;
    let mut context_injected = false;
    let mut config_instance: Option<Py<PyAny>> = None;
    let mut ctx_ref: Option<Py<PyAny>> = None;
    let mut args: Vec<Py<PyAny>> = Vec::new();

    for (param_name, annotation) in enumerate_params(py, &func)? {
        if param_name == "return" {
            continue;
        }

        let is_ctx = annotation
            .as_ref()
            .is_some_and(|a| is_context_annotation(py, a));
        if is_ctx
            && config_instance.is_none()
            && let Some(ref a) = annotation
        {
            config_instance = extract_config_from_annotation(py, a, overrides_dict)?;
            if let Some(ref c) = config_instance {
                *config_out = Some(c.clone_ref(py));
            }
        }

        if is_first_param {
            is_first_param = false;
            if is_ctx || (param_name == "context" && !node_map.contains_key(&param_name)) {
                let tags = node.tags();
                let is_task_ctx = annotation
                    .as_ref()
                    .is_some_and(|a| is_task_context_annotation(py, a));
                let ctx_obj = if is_task_ctx {
                    let ctx =
                        PyTaskExecutionContext::new(step.name.clone(), tags, partition.clone())
                            .with_config(config_instance.as_ref().map(|c| c.clone_ref(py)));
                    Py::new(py, ctx)?.into_any()
                } else {
                    let is_multi = !step.outputs.is_empty();
                    let context_name = if is_multi {
                        node.name().unwrap_or_else(|_| step.name.clone())
                    } else {
                        step.name.clone()
                    };
                    let ctx = PyAssetExecutionContext::new(
                        context_name,
                        tags,
                        node.kinds(),
                        node.group(),
                        node.code_version(),
                        node.metadata(),
                        partition.clone(),
                        is_multi,
                        if is_multi {
                            step.outputs.clone()
                        } else {
                            vec![]
                        },
                    )
                    .with_config(config_instance.as_ref().map(|c| c.clone_ref(py)));
                    Py::new(py, ctx)?.into_any()
                };
                ctx_ref = Some(ctx_obj.clone_ref(py));
                args.push(ctx_obj);
                context_injected = true;
                continue;
            }
        } else if is_ctx {
            return Err(ExecutionError::new_err(format!(
                "Context must be the first parameter of '{}'",
                step.name
            )));
        }

        if param_name == "self" {
            // SelfDependency requires `SelfDependency[T]`; the annotation is
            // load-bearing (extracts `T` for the InputContext type hint).
            let annotation = annotation.ok_or_else(|| {
                ExecutionError::new_err(format!(
                    "Asset '{}': `self` parameter requires a `SelfDependency[T]` annotation",
                    step.name
                ))
            })?;
            // Fall back to the default handler so first runs (no stored value)
            // get `inner: None` rather than ConfigurationError. The parallel
            // worker path (`worker_args.rs`) passes `None` instead — there,
            // missing handler IS an error since cross-subprocess self-deps
            // need real persistence.
            let default = registry.default_handler(py);
            let self_dep = load_self_dependency(
                py,
                &step.name,
                node,
                partition_key,
                &annotation,
                registry,
                Some(&default),
            )?;
            args.push(self_dep);
            continue;
        }

        let resolved_name = node
            .param_remap()
            .and_then(|m| m.get(&param_name))
            .cloned()
            .unwrap_or_else(|| param_name.clone());

        if let Some(override_val) = input_overrides.get(&resolved_name) {
            args.push(override_val.clone_ref(py));
        } else if let Some(upstream_node) = node_map.get(&resolved_name) {
            // Type hint is `None` for unannotated params — IOHandlers either
            // ignore type_hint or branch on `is None`.
            let type_hint = annotation.map(|a| a.unbind()).unwrap_or_else(|| py.None());
            let loaded = load_upstream_input(
                py,
                &resolved_name,
                &step.name,
                node,
                upstream_node,
                partition_key,
                type_hint,
                registry,
            )?;
            args.push(loaded);
        } else if let Some(resource) = resources.get(&param_name) {
            if overrides_dict.is_some() {
                args.push(resource.instantiate_config(py, overrides_dict)?);
            } else {
                args.push(resource.inner().clone_ref(py));
            }
        } else {
            return Err(ConfigurationError::new_err(format!(
                "Asset '{}': parameter '{}' does not match any upstream asset or resource",
                step.name, param_name
            )));
        }
    }

    Ok(BuiltStepArgs {
        args,
        return_hint,
        config_instance,
        context_injected,
        ctx_ref,
    })
}

use crate::metadata::MetadataValue;
use crate::result_types::ResultKind;

/// Output of `process_raw_result` — the per-step state derived from the user
/// function's return value, with wrappers unwrapped and metadata merged.
pub(crate) struct ProcessedResult {
    pub result: Py<PyAny>,
    pub output_metadata: Vec<(String, MetadataValue)>,
    pub data_version: Option<String>,
    pub tags: Option<Vec<String>>,
    pub dynamic_keys: Option<Vec<String>>,
    pub kind: ResultKind,
}

/// Process a raw Python result by extracting Output/Observation/Materialization
/// wrappers, validating the return type, draining context metadata, and unwrapping
/// DynamicOutput. Shared between `execute_step` and `finish_async_step`.
pub(crate) fn process_raw_result(
    py: Python,
    raw_result: &Py<PyAny>,
    return_hint: Option<&Py<PyAny>>,
    context_injected: bool,
    ctx_ref: Option<&Py<PyAny>>,
    step_name: &str,
) -> PyResult<ProcessedResult> {
    let extracted = result_types::try_extract_result_type(py, raw_result)?;

    let (actual_result, result_metadata, result_data_version, result_tags, result_kind) =
        if let Some(ext) = extracted {
            let val = ext.value.unwrap_or_else(|| py.None());
            (val, ext.metadata, ext.data_version, ext.tags, ext.kind)
        } else {
            (
                raw_result.clone_ref(py),
                Vec::new(),
                None,
                None,
                ResultKind::Output,
            )
        };

    validate_return_type(py, &actual_result, return_hint, step_name)?;

    let (mut output_metadata, context_data_version) = if context_injected {
        if let Some(first) = ctx_ref {
            if let Ok(ctx) = first.bind(py).cast::<PyAssetExecutionContext>() {
                let ctx = ctx.borrow();
                (ctx.drain_output_metadata(), ctx.drain_data_version())
            } else {
                (Vec::new(), None)
            }
        } else {
            (Vec::new(), None)
        }
    } else {
        (Vec::new(), None)
    };

    super::merge_metadata(&mut output_metadata, &result_metadata);
    let data_version = result_data_version.or(context_data_version);

    let (final_result, dynamic_keys) =
        match crate::result_types::try_unwrap_dynamic_outputs(py, &actual_result)? {
            Some((values, keys)) => (values, Some(keys)),
            None => (actual_result, None),
        };

    Ok(ProcessedResult {
        result: final_result,
        output_metadata,
        data_version,
        tags: result_tags,
        dynamic_keys,
        kind: result_kind,
    })
}

/// Execute a single step in the current process with context injection.
///
/// `config_out` is populated with a clone of the resolved config instance as
/// soon as `build_step_args` resolves it (i.e. before the user function runs).
/// Callers handling a later `Err` can therefore still pass a real config to
/// failure hooks instead of `None`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_step(
    py: Python,
    step: &ExecutionStep,
    node_map: &HashMap<String, ResolvedNode>,
    partition_key: &Option<PyPartitionKey>,
    resources: &HashMap<String, ResourceVariant>,
    config_overrides: &Option<HashMap<String, Py<PyAny>>>,
    registry: &IOHandlerRegistry,
    input_overrides: &HashMap<String, Py<PyAny>>,
    task_locals: Option<&pyo3_async_runtimes::TaskLocals>,
    config_out: &mut Option<Py<PyAny>>,
) -> PyResult<StepResult> {
    let node = node_map.get(&step.name).ok_or_else(|| {
        ExecutionError::new_err(format!("Node '{}' not found in execution plan", step.name))
    })?;

    let func = node.callable(py)?;

    if node.annotations(py)?.is_some() {
        let built = build_step_args(
            py,
            step,
            node,
            node_map,
            partition_key,
            resources,
            config_overrides,
            registry,
            input_overrides,
            config_out,
        )?;

        let args_tuple = PyTuple::new(py, &built.args)?;

        let is_multi = !step.outputs.is_empty();
        let is_async_node = node.is_async();
        if is_multi {
            let inspect = py.import("inspect")?;
            let is_gen: bool = inspect
                .call_method1("isgeneratorfunction", (&func,))?
                .is_truthy()?;
            let is_async_gen: bool = inspect
                .call_method1("isasyncgenfunction", (&func,))?
                .is_truthy()?;
            // Gen path: moves built.ctx_ref into the GeneratorType variant
            // and early-returns. Non-gen path below is only reached when this
            // outer `if` is false, so ctx_ref is still owned there.
            if is_gen || is_async_gen {
                let generator = func.call1(py, args_tuple)?;
                let kind = if is_async_gen {
                    super::GeneratorType::Async {
                        context: built.ctx_ref,
                    }
                } else {
                    super::GeneratorType::Sync {
                        context: built.ctx_ref,
                    }
                };
                return Ok(StepResult {
                    result: generator,
                    return_hint: built.return_hint,
                    output_metadata: Vec::new(),
                    data_version: None,
                    config_instance: built.config_instance,
                    tags: None,
                    dynamic_keys: None,
                    result_kind: ResultKind::Output,
                    generator: Some(kind),
                });
            }
        }

        let raw_result = if is_async_node {
            let coroutine = func.call1(py, args_tuple)?;
            let locals = task_locals.ok_or_else(|| {
                ExecutionError::new_err(format!(
                    "Async node '{}' requires TaskLocals — this is an internal error",
                    step.name
                ))
            })?;
            let future =
                pyo3_async_runtimes::into_future_with_locals(locals, coroutine.into_bound(py))?;
            py.detach(|| crate::runtime::rt().block_on(future))?
        } else {
            func.call1(py, args_tuple)?
        };

        let processed = process_raw_result(
            py,
            &raw_result,
            built.return_hint.as_ref(),
            built.context_injected,
            built.ctx_ref.as_ref(),
            &step.name,
        )?;

        Ok(StepResult {
            result: processed.result,
            return_hint: built.return_hint,
            output_metadata: processed.output_metadata,
            data_version: processed.data_version,
            config_instance: built.config_instance,
            tags: processed.tags,
            dynamic_keys: processed.dynamic_keys,
            result_kind: processed.kind,
            generator: None,
        })
    } else if node.is_bash_task() {
        let result = func.call0(py)?;
        Ok(StepResult {
            result,
            return_hint: None,
            output_metadata: Vec::new(),
            data_version: None,
            config_instance: None,
            tags: None,
            dynamic_keys: None,
            result_kind: ResultKind::Output,
            generator: None,
        })
    } else {
        Err(ExecutionError::new_err(format!(
            "Node '{}' has no annotations and is not a BashTask — cannot execute",
            step.name
        )))
    }
}
