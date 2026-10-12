use pyo3::PyTypeInfo;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};

use crate::context::asset::PyAssetExecutionContext;
use crate::context::task::PyTaskExecutionContext;
use crate::errors::AssetOutputValidationError;
use crate::metadata::MetadataValue;
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

/// The function's return annotation, evaluated when it is a string (PEP 563).
pub(crate) fn extract_return_hint(py: Python, func: &Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
    if func.bind(py).is_instance_of::<crate::task::PyBashTask>() {
        return Ok(None);
    }
    let annotations = get_annotations(py, func)?;
    let Some(hint) = annotations.get_item("return")? else {
        return Ok(None);
    };
    match resolve_annotation(py, func, &hint) {
        Ok(hint) => Ok(Some(hint.unbind())),
        Err(cause) => {
            let err = AssetOutputValidationError::new_err(format!(
                "Cannot evaluate the return annotation '{hint}': {cause}"
            ));
            err.set_cause(py, Some(cause));
            Err(err)
        }
    }
}

/// A return value that matches its hint, as rivers stores it.
pub(crate) struct CheckedOutput {
    pub value: Py<PyAny>,
    /// Metadata an integration's validator adds to the materialization.
    pub metadata: Vec<(String, MetadataValue)>,
}

impl CheckedOutput {
    fn unchanged(py: Python, value: &Py<PyAny>) -> Self {
        Self {
            value: value.clone_ref(py),
            metadata: Vec::new(),
        }
    }
}

enum HintMatch {
    Yes(CheckedOutput),
    /// `Some` holds the error an integration's validator raised.
    No(Option<PyErr>),
}

/// Validate a return value against the declared return type hint: classes,
/// `None`, `Any`, unions, `Annotated`, generic containers (by their origin),
/// and hints an integration validates (`rivers.integrations.output_validator`).
/// The checked value can differ from `result`, e.g. a frame that pandera
/// coerced. Raises AssetOutputValidationError on mismatch.
pub(crate) fn validate_return_type(
    py: Python,
    result: &Py<PyAny>,
    return_hint: Option<&Py<PyAny>>,
    step_name: &str,
) -> PyResult<CheckedOutput> {
    let Some(hint) = return_hint.map(|h| h.bind(py)) else {
        return Ok(CheckedOutput::unchanged(py, result));
    };

    match check_hint(py, result, hint)? {
        HintMatch::Yes(checked) => Ok(checked),
        HintMatch::No(None) => Err(AssetOutputValidationError::new_err(format!(
            "Asset '{}' returned value of type '{}' but expected '{}'",
            step_name,
            result.bind(py).get_type().qualname()?,
            hint,
        ))),
        HintMatch::No(Some(cause)) => {
            let err = AssetOutputValidationError::new_err(format!(
                "Asset '{}' returned a value that does not match '{}': {}",
                step_name,
                hint,
                cause.value(py),
            ));
            err.set_cause(py, Some(cause));
            Err(err)
        }
    }
}

fn check_hint(py: Python, value: &Py<PyAny>, hint: &Bound<PyAny>) -> PyResult<HintMatch> {
    let typing = py.import("typing")?;

    // Output/Observation/Materialization hints: value was already unwrapped before validation.
    if hint.is_none()
        || hint.is(typing.getattr("Any")?)
        || hint.is(result_types::PyOutput::type_object(py))
        || hint.is(result_types::PyObservation::type_object(py))
        || hint.is(result_types::PyMaterialization::type_object(py))
    {
        return Ok(HintMatch::Yes(CheckedOutput::unchanged(py, value)));
    }

    let origin = typing.call_method1("get_origin", (hint,))?;
    if origin.is(typing.getattr("Annotated")?) {
        return check_annotated(py, value, hint);
    }

    let is_union = origin.is(typing.getattr("Union")?)
        || origin
            .eq(py.import("types")?.getattr("UnionType")?)
            .unwrap_or(false);
    if is_union {
        let mut cause = None;
        for arm in typing.call_method1("get_args", (hint,))?.try_iter()? {
            match check_hint(py, value, &arm?)? {
                HintMatch::Yes(checked) => return Ok(HintMatch::Yes(checked)),
                HintMatch::No(err) => cause = cause.or(err),
            }
        }
        return Ok(HintMatch::No(cause));
    }

    if let Some(checked) = run_validator(py, value, hint)? {
        return Ok(checked);
    }

    let class = if origin.is_none() { hint } else { &origin };
    let matches = py
        .import("builtins")?
        .getattr("isinstance")?
        .call1((value, class))
        .and_then(|r| r.is_truthy())
        .unwrap_or(false);
    Ok(if matches {
        HintMatch::Yes(CheckedOutput::unchanged(py, value))
    } else {
        HintMatch::No(None)
    })
}

/// `Annotated[T, *items]`: the value must match `T`, then pass each item an
/// integration validates, e.g. a pandera `DataFrameSchema`.
fn check_annotated(py: Python, value: &Py<PyAny>, hint: &Bound<PyAny>) -> PyResult<HintMatch> {
    let mut checked = match check_hint(py, value, &hint.getattr("__origin__")?)? {
        HintMatch::Yes(checked) => checked,
        no => return Ok(no),
    };

    for item in hint.getattr("__metadata__")?.try_iter()? {
        match run_validator(py, &checked.value, &item?)? {
            Some(HintMatch::Yes(next)) => {
                super::merge_metadata(&mut checked.metadata, &next.metadata);
                checked.value = next.value;
            }
            Some(no) => return Ok(no),
            None => {}
        }
    }
    Ok(HintMatch::Yes(checked))
}

/// `None` when no integration validates `hint`, or its validator returns
/// `NotImplemented`.
fn run_validator(
    py: Python,
    value: &Py<PyAny>,
    hint: &Bound<PyAny>,
) -> PyResult<Option<HintMatch>> {
    let validator = py
        .import("rivers.integrations")?
        .call_method1("output_validator", (hint,))?;
    if validator.is_none() {
        return Ok(None);
    }

    let returned = match validator.call1((value, hint)) {
        Ok(returned) => returned.unbind(),
        Err(err) => return Ok(Some(HintMatch::No(Some(err)))),
    };
    if returned.is(py.NotImplemented()) {
        return Ok(None);
    }

    let checked = match result_types::try_extract_result_type(py, &returned)? {
        Some(output) => CheckedOutput {
            value: output.value.unwrap_or_else(|| py.None()),
            metadata: output.metadata,
        },
        None => CheckedOutput::unchanged(py, &returned),
    };
    Ok(Some(HintMatch::Yes(checked)))
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
