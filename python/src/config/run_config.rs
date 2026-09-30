//! The launch document across the storage boundary: the JSON text a run
//! record stores and the per-run settings the executor applies.
//!
//! ```json
//! {"assets": {"raw_users": {"config": {"batch_size": 100},
//!                           "metadata": {"delta/mode": "overwrite"}}}}
//! ```
//!
//! Every section is optional; an absent one means "as defined".
use std::collections::HashMap;
use std::fmt;

use anyhow::{anyhow, bail};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};
use serde_json::{Map, Value};

use crate::errors::{ConfigurationError, ExecutionError};
use crate::executor::ops::{
    enumerate_params, is_action_context_annotation, is_context_annotation, resolve_annotation,
};
use crate::repository::resolved_node::ResolvedNode;

/// The sections a document may have.
const SECTIONS: &[&str] = &["assets"];

/// Check the document a launch carries and return it canonical: compact,
/// with empty parts dropped, or `None` when it sets nothing. `known` says
/// whether a name under `assets` is one this launch can run.
pub(crate) fn validate_run_config(
    json: &str,
    known: impl Fn(&str) -> bool,
) -> anyhow::Result<Option<String>> {
    let value: Value =
        serde_json::from_str(json).map_err(|e| anyhow!("config is not valid JSON: {e}"))?;
    let Value::Object(mut root) = value else {
        bail!("config must be a JSON object with an 'assets' section");
    };
    for key in root.keys() {
        if !SECTIONS.contains(&key.as_str()) {
            bail!(
                "config has an unknown section '{key}'; expected {}",
                SECTIONS.join(", ")
            );
        }
    }
    let mut out = Map::new();
    if let Some(assets) = root.remove("assets") {
        let Value::Object(assets) = assets else {
            bail!("config.assets must be a JSON object keyed by asset name");
        };
        let mut kept = Map::new();
        for (name, overrides) in assets {
            if !known(&name) {
                bail!("config names '{name}', which this launch does not run");
            }
            let Value::Object(overrides) = overrides else {
                bail!("config.assets.{name} must be a JSON object with 'config' and 'metadata'");
            };
            let mut entry = Map::new();
            for (key, value) in overrides {
                match key.as_str() {
                    "config" => {
                        let Value::Object(fields) = value else {
                            bail!(
                                "config.assets.{name}.config must be a JSON object of field values"
                            );
                        };
                        if !fields.is_empty() {
                            entry.insert(key, Value::Object(fields));
                        }
                    }
                    "metadata" => {
                        let Value::Object(metadata) = value else {
                            bail!("config.assets.{name}.metadata must be a JSON object of strings");
                        };
                        if let Some((k, _)) = metadata.iter().find(|(_, v)| !v.is_string()) {
                            bail!("config.assets.{name}.metadata.{k} must be a string");
                        }
                        if !metadata.is_empty() {
                            entry.insert(key, Value::Object(metadata));
                        }
                    }
                    other => bail!(
                        "config.assets.{name} has an unknown key '{other}'; expected config, metadata"
                    ),
                }
            }
            if !entry.is_empty() {
                kept.insert(name, Value::Object(entry));
            }
        }
        if !kept.is_empty() {
            out.insert("assets".to_string(), Value::Object(kept));
        }
    }
    if out.is_empty() {
        return Ok(None);
    }
    Ok(Some(Value::Object(out).to_string()))
}

/// [`validate_run_config`] for the Python API: the text a pymethod built
/// from its `config=` argument, checked before any record is written.
pub(crate) fn check_run_config(
    config: Option<&str>,
    known: impl Fn(&str) -> bool,
) -> PyResult<Option<String>> {
    match config {
        Some(json) => {
            validate_run_config(json, known).map_err(|e| ConfigurationError::new_err(e.to_string()))
        }
        None => Ok(None),
    }
}

/// The overrides a document holds for one asset.
pub(crate) struct AssetOverrides {
    /// Field values for the config class, a dict.
    pub config: Option<Py<PyAny>>,
    /// Metadata keys added to or replacing the asset's own for this run.
    pub metadata: HashMap<String, String>,
}

/// A stored document, parsed for a run.
pub(crate) struct RunDocument {
    pub assets: HashMap<String, AssetOverrides>,
}

impl RunDocument {
    /// The per-asset config dicts the executor hands to the config classes.
    pub(crate) fn config_overrides(&self, py: Python<'_>) -> Option<HashMap<String, Py<PyAny>>> {
        let map: HashMap<String, Py<PyAny>> = self
            .assets
            .iter()
            .filter_map(|(name, o)| Some((name.clone(), o.config.as_ref()?.clone_ref(py))))
            .collect();
        (!map.is_empty()).then_some(map)
    }

    /// `node_map` with the metadata overrides merged into the assets they
    /// name, or `None` when the document has none for a node in the map.
    /// Only an asset carries metadata; an override for a task is an error.
    pub(crate) fn overlay_node_map(
        &self,
        py: Python<'_>,
        node_map: &HashMap<String, ResolvedNode>,
    ) -> PyResult<Option<HashMap<String, ResolvedNode>>> {
        let overrides: Vec<(&String, &HashMap<String, String>)> = self
            .assets
            .iter()
            .filter(|(name, o)| !o.metadata.is_empty() && node_map.contains_key(*name))
            .map(|(name, o)| (name, &o.metadata))
            .collect();
        if overrides.is_empty() {
            return Ok(None);
        }
        let mut overlay: HashMap<String, ResolvedNode> = node_map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone_ref(py)))
            .collect();
        for (name, metadata) in overrides {
            match overlay.get_mut(name) {
                Some(ResolvedNode::Asset(node)) => node
                    .metadata
                    .get_or_insert_with(HashMap::new)
                    .extend(metadata.iter().map(|(k, v)| (k.clone(), v.clone()))),
                _ => {
                    return Err(ConfigurationError::new_err(format!(
                        "config: metadata overrides apply to assets; '{name}' is a task"
                    )));
                }
            }
        }
        Ok(Some(overlay))
    }
}

fn object<'py>(value: Bound<'py, PyAny>, what: &str) -> PyResult<Bound<'py, PyDict>> {
    value
        .cast_into::<PyDict>()
        .map_err(|_| ExecutionError::new_err(format!("run config: {what} is not a JSON object")))
}

/// The stored text as a [`RunDocument`].
pub(crate) fn parse_run_config(py: Python<'_>, json: &str) -> PyResult<RunDocument> {
    let loaded = py.import("json")?.call_method1("loads", (json,))?;
    let root = object(loaded, "the document")?;
    let mut assets = HashMap::new();
    if let Some(section) = root.get_item("assets")? {
        for (name, overrides) in object(section, "'assets'")?.iter() {
            let name: String = name.extract()?;
            let overrides = object(overrides, &format!("'{name}'"))?;
            let config = overrides
                .get_item("config")?
                .map(|c| {
                    object(c, &format!("the config of '{name}'")).map(|d| d.into_any().unbind())
                })
                .transpose()?;
            let metadata = match overrides.get_item("metadata")? {
                Some(m) => m.extract::<HashMap<String, String>>().map_err(|_| {
                    ExecutionError::new_err(format!(
                        "run config: the metadata of '{name}' is not an object of strings"
                    ))
                })?,
                None => HashMap::new(),
            };
            assets.insert(name, AssetOverrides { config, metadata });
        }
    }
    Ok(RunDocument { assets })
}

/// The document a Python caller passed as the text the launchers carry.
/// Values go through pydantic's encoder, so dates, paths and enums are
/// fine; anything it cannot encode is an error.
pub(crate) fn run_config_to_json(
    py: Python<'_>,
    config: Option<&Py<PyAny>>,
) -> PyResult<Option<String>> {
    let Some(config) = config else {
        return Ok(None);
    };
    let kwargs = PyDict::new(py);
    kwargs.set_item(
        "default",
        py.import("pydantic_core")?.getattr("to_jsonable_python")?,
    )?;
    let text: String = py
        .import("json")?
        .call_method("dumps", (config,), Some(&kwargs))
        .map_err(|e| ConfigurationError::new_err(format!("config is not JSON-serializable: {e}")))?
        .extract()?;
    Ok(Some(text))
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

/// One error found in a document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConfigIssue {
    /// Where in the document the checked object sits, e.g.
    /// `["assets", "raw_users", "config"]`.
    pub path: Vec<String>,
    /// pydantic's `loc` inside that object; empty for the object itself.
    pub loc: Vec<LocPart>,
    pub message: String,
    /// pydantic's error type (`missing`, `int_parsing`, `value_error`, ...),
    /// `exception` for anything else a constructor raised, or `invalid`
    /// for a document part the definitions refuse.
    pub kind: String,
}

impl fmt::Display for ConfigIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path.join("."))?;
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
/// else the constructor raised. `path` locates `overrides` in the document.
pub(crate) fn config_errors(
    py: Python<'_>,
    path: &[String],
    cls: &Bound<'_, PyType>,
    overrides: &Bound<'_, PyDict>,
) -> PyResult<Vec<ConfigIssue>> {
    let Err(err) = cls.call((), Some(overrides)) else {
        return Ok(Vec::new());
    };
    let validation_error = py.import("pydantic")?.getattr("ValidationError")?;
    if !err.is_instance(py, &validation_error) {
        return Ok(vec![ConfigIssue {
            path: path.to_vec(),
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
            path: path.to_vec(),
            loc,
            message: item.get_item("msg")?.extract()?,
            kind: item.get_item("type")?.extract()?,
        });
    }
    Ok(issues)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(json: &str) -> Result<Option<String>, String> {
        validate_run_config(json, |name| name == "a" || name == "b").map_err(|e| e.to_string())
    }

    #[test]
    fn canonical_form_drops_empty_parts() {
        assert_eq!(check("{}"), Ok(None));
        assert_eq!(check(r#"{"assets": {}}"#), Ok(None));
        assert_eq!(check(r#"{"assets": {"a": {}}}"#), Ok(None));
        assert_eq!(
            check(r#"{"assets": {"a": {"config": {}, "metadata": {}}}}"#),
            Ok(None)
        );
        assert_eq!(
            check(
                r#"{"assets": {"b": {"metadata": {"k": "v"}}, "a": {"config": {"x": 1}, "metadata": {}}}}"#
            ),
            Ok(Some(
                r#"{"assets":{"a":{"config":{"x":1}},"b":{"metadata":{"k":"v"}}}}"#.to_string()
            ))
        );
    }

    #[test]
    fn shape_errors_name_their_place() {
        let cases = [
            ("nope", "config is not valid JSON"),
            ("[1]", "config must be a JSON object"),
            (
                r#"{"other": {}}"#,
                "unknown section 'other'; expected assets",
            ),
            (r#"{"assets": 1}"#, "config.assets must be a JSON object"),
            (
                r#"{"assets": {"c": {}}}"#,
                "config names 'c', which this launch does not run",
            ),
            (
                r#"{"assets": {"a": 1}}"#,
                "config.assets.a must be a JSON object",
            ),
            (
                r#"{"assets": {"a": {"other": {}}}}"#,
                "config.assets.a has an unknown key 'other'",
            ),
            (
                r#"{"assets": {"a": {"config": 1}}}"#,
                "config.assets.a.config must be a JSON object",
            ),
            (
                r#"{"assets": {"a": {"metadata": []}}}"#,
                "config.assets.a.metadata must be a JSON object of strings",
            ),
            (
                r#"{"assets": {"a": {"metadata": {"k": 1}}}}"#,
                "config.assets.a.metadata.k must be a string",
            ),
        ];
        for (json, expected) in cases {
            let err = check(json).unwrap_err();
            assert!(err.contains(expected), "{json}: {err}");
        }
    }
}
