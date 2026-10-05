//! Step invocation — calls Python asset/task functions with argument injection.
//!
//! Resolves upstream values via IO handlers, injects `AssetExecutionContext` and resources
//! based on type-hint annotations, performs output type validation, and unwraps `Output` /
//! `Observation` / `DynamicOutput` wrappers. Detects composition context for graph assets.
use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use rivers_core::execution::plan::ExecutionStep;

use crate::assets::io_handler_registry::IOHandlerRegistry;
use crate::config::ResourceVariant;
use crate::context::asset::PyAssetExecutionContext;
use crate::context::task::PyTaskExecutionContext;
use crate::errors::{ConfigurationError, ExecutionError};
use crate::partitions::PyPartitionKey;
use crate::repository::resolved_node::ResolvedNode;
use crate::result_types;

use super::StepResult;
use super::annotations::{
    enumerate_params, extract_config_from_annotation, extract_return_hint, get_annotations,
    is_action_context_annotation, is_context_annotation, is_task_context_annotation,
    resolve_annotation, validate_return_type,
};
use super::io::{build_partition_context, load_self_dependency, load_upstream_input};

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
    run_id: &str,
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
                        Some(run_id.to_string()),
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
            args.push(resource.instantiate_config(py, None)?);
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
                (ctx.drain_output_metadata(py), ctx.drain_data_version())
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
    run_id: &str,
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
            run_id,
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
                    failed_partitions: Vec::new(),
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

        let failed_partitions = drain_failed_partitions(py, built.ctx_ref.as_ref());

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
            failed_partitions,
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
            failed_partitions: Vec::new(),
        })
    } else {
        Err(ExecutionError::new_err(format!(
            "Node '{}' has no annotations and is not a BashTask — cannot execute",
            step.name
        )))
    }
}

/// Execute one action step: bind an `ActionContext` — no dependency
/// inputs, upstream is never executed for actions — and call the action's
/// function. Everything around the call (capture, retry ladder, events) is
/// the same lifecycle materialize steps use.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_action_step(
    py: Python,
    verb: &str,
    step: &ExecutionStep,
    node_map: &HashMap<String, ResolvedNode>,
    partition_key: &Option<PyPartitionKey>,
    resources: &HashMap<String, ResourceVariant>,
    config_overrides: &Option<HashMap<String, Py<PyAny>>>,
    registry: &IOHandlerRegistry,
    run_id: &str,
    task_locals: Option<&pyo3_async_runtimes::TaskLocals>,
) -> PyResult<StepResult> {
    let node = node_map.get(&step.name).ok_or_else(|| {
        ExecutionError::new_err(format!("Node '{}' not found in execution plan", step.name))
    })?;
    let action = node.find_action(verb).ok_or_else(|| {
        ExecutionError::new_err(format!(
            "Asset '{}' does not define action '{}'",
            step.name, verb
        ))
    })?;
    let func = action
        .func
        .as_ref()
        .map(|f| f.clone_ref(py))
        .ok_or_else(|| {
            ExecutionError::new_err(format!(
                "Action '{}' on asset '{}' has no function",
                verb, step.name
            ))
        })?;

    let handler = registry.for_output(py, node);
    let handler = (!handler.is_none(py)).then_some(handler);

    let overrides_dict = config_overrides
        .as_ref()
        .and_then(|m| m.get(&step.name))
        .map(|obj| obj.bind(py).cast::<PyDict>())
        .transpose()?;

    let is_observe = action.outcome == crate::assets::action::ActionOutcome::Observe;

    // The built-in observe keeps its historical call convention: no-arg, or
    // one `AssetExecutionContext` parameter (with the config generic). User
    // actions receive an `ActionContext`.
    let mut observe_ctx: Option<Py<PyAny>> = None;
    let mut action_ctx: Option<Py<PyAny>> = None;
    let call_result = (|| -> PyResult<Py<PyAny>> {
        let (target, call_args): (&Py<PyAny>, Vec<Py<PyAny>>) = if is_observe {
            let annotations = get_annotations(py, &func)?;
            let has_context_param = annotations.iter().any(|(k, v)| {
                k.extract::<String>().ok().as_deref() != Some("return")
                    && is_context_annotation(py, &v)
            });
            if has_context_param {
                let config_instance = annotations
                    .iter()
                    .find(|(k, _)| k.extract::<String>().ok().as_deref() != Some("return"))
                    .map(|(_, v)| extract_config_from_annotation(py, &v, overrides_dict))
                    .transpose()?
                    .flatten();
                // A keyed observe records its data version against that
                // partition, so the body has to be able to read the key.
                let ctx = PyAssetExecutionContext::new(
                    step.name.clone(),
                    Some(run_id.to_string()),
                    node.tags(),
                    node.kinds(),
                    node.group(),
                    None,
                    node.metadata(),
                    super::build_partition_context(node, partition_key)?,
                    false,
                    vec![],
                )
                .with_config(config_instance);
                let ctx_py = Py::new(py, ctx)?.into_any();
                observe_ctx = Some(ctx_py.clone_ref(py));
                (&func, vec![ctx_py])
            } else {
                (&func, Vec::new())
            }
        } else {
            let partition = super::build_partition_context(node, partition_key)?;
            // First parameter is the context; the rest are resources injected
            // by name — the same rule materialize functions use.
            let params = enumerate_params(py, &func)?;
            let config_instance = match params.first() {
                Some((param, Some(annotation))) => {
                    match resolve_annotation(py, &func, annotation) {
                        Ok(a) if is_action_context_annotation(py, &a) => {
                            extract_config_from_annotation(py, &a, overrides_dict)?
                        }
                        Ok(_) => None,
                        // The annotation may carry no config type at all, so
                        // fail only when the caller's config would be dropped.
                        Err(e) if overrides_dict.is_some() => {
                            return Err(ConfigurationError::new_err(format!(
                                "Action '{}' on asset '{}': config was given, but the \
                                 annotation of '{}' cannot be resolved: {}",
                                verb, step.name, param, e
                            )));
                        }
                        Err(_) => None,
                    }
                }
                _ => None,
            };
            let ctx = crate::context::action::PyActionContext::new(
                step.name.clone(),
                verb.to_string(),
                run_id.to_string(),
                node.metadata(),
                partition,
                handler,
                config_instance,
            );
            let mut args: Vec<Py<PyAny>> = Vec::new();
            if let Some((first_name, _)) = params.first()
                && resources.contains_key(first_name)
            {
                return Err(ConfigurationError::new_err(format!(
                    "Action '{}' on asset '{}': the first parameter is always the \
                     ActionContext, but '{}' names a resource — move it after the \
                     context",
                    verb, step.name, first_name
                )));
            }
            if !params.is_empty() {
                let ctx_py = Py::new(py, ctx)?.into_any();
                action_ctx = Some(ctx_py.clone_ref(py));
                args.push(ctx_py);
                for (param_name, annotation) in params.iter().skip(1) {
                    if annotation
                        .as_ref()
                        .is_some_and(|a| is_action_context_annotation(py, a))
                    {
                        return Err(ExecutionError::new_err(format!(
                            "Context must be the first parameter of action '{}' on asset '{}'",
                            verb, step.name
                        )));
                    }
                    if let Some(resource) = resources.get(param_name) {
                        args.push(resource.instantiate_config(py, None)?);
                    } else {
                        return Err(ConfigurationError::new_err(format!(
                            "Action '{}' on asset '{}': parameter '{}' does not \
                             match any resource",
                            verb, step.name, param_name
                        )));
                    }
                }
            }
            (&func, args)
        };

        let raw = if call_args.is_empty() {
            target.call0(py)?
        } else {
            target.call1(py, PyTuple::new(py, &call_args)?)?
        };
        if action.is_async {
            let locals = task_locals.ok_or_else(|| {
                ExecutionError::new_err(format!(
                    "Async action '{}' requires TaskLocals — this is an internal error",
                    verb
                ))
            })?;
            let future = pyo3_async_runtimes::into_future_with_locals(locals, raw.into_bound(py))?;
            py.detach(|| crate::runtime::rt().block_on(future))
        } else {
            Ok(raw)
        }
    })();

    // The built-in observe returns an Observation — merge its metadata with
    // anything set on the context, result-type data version winning, exactly
    // like the retired `repo.observe` loop did.
    let mut result = call_result?;
    let mut output_metadata = Vec::new();
    let mut data_version = None;
    if is_observe {
        if let Some(ctx) = &observe_ctx
            && let Ok(bound) = ctx.bind(py).cast::<PyAssetExecutionContext>()
        {
            let ctx_ref = bound.borrow();
            output_metadata = ctx_ref.drain_output_metadata(py);
            data_version = ctx_ref.drain_data_version();
        }
        if let Some(ext) = crate::result_types::try_extract_result_type(py, &result)? {
            super::merge_metadata(&mut output_metadata, &ext.metadata);
            data_version = ext.data_version.or(data_version);
            result = ext.value.unwrap_or_else(|| py.None());
        }
    }

    // An observe body marks keys on its `AssetExecutionContext`; a user action
    // marks them on its `ActionContext`. Exactly one of the two exists per call.
    let mut failed_partitions: Vec<(crate::partitions::PyPartitionKey, String)> = action_ctx
        .as_ref()
        .and_then(|c| {
            c.bind(py)
                .cast::<crate::context::action::PyActionContext>()
                .ok()
                .map(|b| b.get().drain_failed_partitions().into_iter().collect())
        })
        .unwrap_or_default();
    failed_partitions.extend(drain_failed_partitions(py, observe_ctx.as_ref()));

    Ok(StepResult {
        result,
        return_hint: None,
        output_metadata,
        data_version,
        config_instance: None,
        tags: None,
        dynamic_keys: None,
        result_kind: ResultKind::Output,
        generator: None,
        failed_partitions,
    })
}

pub(crate) fn drain_failed_partitions(
    py: Python,
    ctx_ref: Option<&Py<PyAny>>,
) -> Vec<(crate::partitions::PyPartitionKey, String)> {
    ctx_ref
        .and_then(|c| {
            c.bind(py).cast::<PyAssetExecutionContext>().ok().map(|b| {
                b.get()
                    .drain_failed_backfill_partitions()
                    .into_iter()
                    .collect()
            })
        })
        .unwrap_or_default()
}
