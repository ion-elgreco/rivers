use pyo3::PyTypeInfo;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyNone, PyString};

use crate::context::asset::PyAssetExecutionContext;
use crate::context::task::PyTaskExecutionContext;
use crate::errors::AssetOutputValidationError;
use crate::result_types;

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

pub(crate) fn is_action_context_annotation(py: Python, annotation: &Bound<PyAny>) -> bool {
    let action_ctx = crate::context::action::PyActionContext::type_object(py);
    annotation_is(annotation, action_ctx.as_any())
}

pub(crate) fn get_annotations<'py>(
    py: Python<'py>,
    func: &Py<PyAny>,
) -> PyResult<Bound<'py, PyDict>> {
    let annotations = func.getattr(py, "__annotations__")?;
    Ok(annotations.cast_bound::<PyDict>(py)?.clone())
}

/// A parameter annotation as an object. Under PEP 563 it is a string,
/// evaluated in the function's module globals. Only this one annotation is
/// evaluated: `typing.get_type_hints` fails as a whole when any other name in
/// the signature exists only for type checkers.
pub(crate) fn resolve_annotation<'py>(
    py: Python<'py>,
    func: &Py<PyAny>,
    annotation: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    if !annotation.is_instance_of::<PyString>() {
        return Ok(annotation.clone());
    }
    let globals = py
        .import("inspect")?
        .call_method1("unwrap", (func,))?
        .getattr("__globals__")?;
    py.import("builtins")?
        .getattr("eval")?
        .call1((annotation, globals))
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
    if func.bind(py).is_instance_of::<crate::task::PyBashTask>() {
        return Ok(None);
    }
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
