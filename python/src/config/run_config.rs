//! Per-run config overrides across the storage boundary: the JSON object a
//! run record stores (`{"asset": {"field": value}}`) and the per-asset
//! mapping the executor applies.
use std::collections::HashMap;
use std::fmt;

use anyhow::{anyhow, bail};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};

use crate::errors::ExecutionError;
use crate::executor::ops::{
    enumerate_params, is_action_context_annotation, is_context_annotation, resolve_annotation,
};

/// Check the config a launch request carries: a JSON object whose keys name
/// assets in `selection` and whose values are objects. Returns the text to
/// store; an empty object means no overrides.
pub(crate) fn validate_run_config(
    json: &str,
    selection: &[String],
) -> anyhow::Result<Option<String>> {
    let value: serde_json::Value =
        serde_json::from_str(json).map_err(|e| anyhow!("config is not valid JSON: {e}"))?;
    let map = value
        .as_object()
        .ok_or_else(|| anyhow!("config must be a JSON object keyed by asset name"))?;
    for (asset, overrides) in map {
        if !selection.iter().any(|s| s == asset) {
            bail!("config names '{asset}', which is not in the selection");
        }
        if !overrides.is_object() {
            bail!("config for '{asset}' must be a JSON object of field values");
        }
    }
    if map.is_empty() {
        return Ok(None);
    }
    Ok(Some(value.to_string()))
}

/// The stored JSON as the executor's per-asset override mapping.
pub(crate) fn parse_run_config(py: Python<'_>, json: &str) -> PyResult<HashMap<String, Py<PyAny>>> {
    let loaded = py.import("json")?.call_method1("loads", (json,))?;
    let dict = loaded
        .cast_into::<PyDict>()
        .map_err(|_| ExecutionError::new_err("run config is not a JSON object"))?;
    let mut out = HashMap::with_capacity(dict.len());
    for (key, overrides) in dict.iter() {
        let key: String = key.extract()?;
        if !overrides.is_instance_of::<PyDict>() {
            return Err(ExecutionError::new_err(format!(
                "run config for '{key}' is not a JSON object"
            )));
        }
        out.insert(key, overrides.unbind());
    }
    Ok(out)
}

/// The override mapping as the JSON a run record stores. `None` when a value
/// is not JSON-serializable even through pydantic's encoder: the run still
/// applies the overrides in-process, it just does not record them.
pub(crate) fn run_config_to_json(
    py: Python<'_>,
    config: Option<&HashMap<String, Py<PyAny>>>,
) -> Option<String> {
    let config = config.filter(|c| !c.is_empty())?;
    let dumped = (|| -> PyResult<String> {
        let dict = PyDict::new(py);
        for (key, overrides) in config {
            dict.set_item(key, overrides)?;
        }
        let kwargs = PyDict::new(py);
        kwargs.set_item(
            "default",
            py.import("pydantic_core")?.getattr("to_jsonable_python")?,
        )?;
        py.import("json")?
            .call_method("dumps", (dict,), Some(&kwargs))?
            .extract()
    })();
    match dumped {
        Ok(json) => Some(json),
        Err(e) => {
            tracing::debug!(
                target: "rivers::repo",
                error = %e,
                "run config is not JSON-serializable; not recorded on the run"
            );
            None
        }
    }
}

/// The pydantic config class named by `func`'s context annotation
/// (`AssetExecutionContext[Config]`, `ActionContext[Config]`), or `None`
/// when it takes no config.
pub(crate) fn config_class<'py>(
    py: Python<'py>,
    func: &Py<PyAny>,
) -> PyResult<Option<Bound<'py, PyType>>> {
    let base_model = py.import("pydantic")?.getattr("BaseModel")?;
    let base_model = base_model.cast::<PyType>()?;
    for (_, annotation) in enumerate_params(py, func)? {
        let Some(annotation) = annotation else {
            continue;
        };
        // A name that exists only for type checkers cannot be evaluated;
        // that parameter is then not a context.
        let Ok(annotation) = resolve_annotation(py, func, &annotation) else {
            continue;
        };
        if !is_context_annotation(py, &annotation) && !is_action_context_annotation(py, &annotation)
        {
            continue;
        }
        let Ok(args) = annotation.getattr("__args__") else {
            return Ok(None);
        };
        let Ok(first) = args.get_item(0) else {
            return Ok(None);
        };
        let cls = if first.is_instance_of::<PyType>() {
            first
        } else {
            first.get_type().into_any()
        };
        let cls = cls.cast_into::<PyType>()?;
        if !cls.is_subclass(base_model)? {
            return Ok(None);
        }
        return Ok(Some(cls));
    }
    Ok(None)
}

/// The JSON schema of `func`'s config class, or `None` when it takes no config.
pub(crate) fn config_schema_json(py: Python<'_>, func: &Py<PyAny>) -> PyResult<Option<String>> {
    let Some(cls) = config_class(py, func)? else {
        return Ok(None);
    };
    let schema = cls.call_method0("model_json_schema")?;
    let text: String = py
        .import("json")?
        .call_method1("dumps", (schema,))?
        .extract()?;
    Ok(Some(text))
}

/// One step of pydantic's `loc`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LocPart {
    Key(String),
    Index(u32),
}

/// One error from building a config class from overrides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConfigIssue {
    pub asset: String,
    pub loc: Vec<LocPart>,
    pub message: String,
    /// pydantic's error type (`missing`, `int_parsing`, `value_error`, ...),
    /// or `exception` for anything else the constructor raised.
    pub kind: String,
}

impl fmt::Display for ConfigIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.asset)?;
        for part in &self.loc {
            match part {
                LocPart::Key(key) => write!(f, ".{key}")?,
                LocPart::Index(index) => write!(f, "[{index}]")?,
            }
        }
        write!(f, ": {}", self.message)
    }
}

/// The errors `cls(**overrides)` raises: what a run hits when it builds
/// the config. Pydantic's own list, or one `exception` entry for anything
/// else the constructor raised.
pub(crate) fn config_errors(
    py: Python<'_>,
    asset: &str,
    cls: &Bound<'_, PyType>,
    overrides: &Bound<'_, PyDict>,
) -> PyResult<Vec<ConfigIssue>> {
    let Err(err) = cls.call((), Some(overrides)) else {
        return Ok(Vec::new());
    };
    let validation_error = py.import("pydantic")?.getattr("ValidationError")?;
    if !err.is_instance(py, &validation_error) {
        return Ok(vec![ConfigIssue {
            asset: asset.to_string(),
            loc: Vec::new(),
            message: err.value(py).str()?.to_string(),
            kind: "exception".to_string(),
        }]);
    }
    let mut issues = Vec::new();
    for item in err.value(py).call_method0("errors")?.try_iter()? {
        let item = item?;
        let mut loc = Vec::new();
        for part in item.get_item("loc")?.try_iter()? {
            let part = part?;
            loc.push(match part.extract::<u32>() {
                Ok(index) => LocPart::Index(index),
                Err(_) => LocPart::Key(part.str()?.to_string()),
            });
        }
        issues.push(ConfigIssue {
            asset: asset.to_string(),
            loc,
            message: item.get_item("msg")?.extract()?,
            kind: item.get_item("type")?.extract()?,
        });
    }
    Ok(issues)
}

/// [`parse_run_config`] for a stored record's field, from a thread that is
/// not attached to the interpreter.
pub(crate) fn parse_run_config_json(
    json: Option<&str>,
) -> PyResult<Option<HashMap<String, Py<PyAny>>>> {
    json.map(|s| Python::attach(|py| parse_run_config(py, s)))
        .transpose()
}
