//! MetadataValue enum — typed metadata entries for asset materialization events.
//!
//! 21 variants covering text, numeric, temporal, structured (JSON/table), URL, path,
//! notebook, and binary (Arrow schema) metadata. `coerce_to_metadata_value()` auto-converts
//! Python primitives (str, int, float, bool) so users rarely need explicit constructors.
use chrono::{Datelike, NaiveDateTime, Timelike};
use pyo3::exceptions::PyTypeError;

use crate::errors::InvalidMetadataError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyList, PyTuple};
use url::Url;

use crate::schema::{PySchemaWrapper, schema_from_ipc_bytes, schema_to_ipc_bytes};

#[pyclass(
    name = "MetadataValue",
    frozen,
    from_py_object,
    module = "rivers._core"
)]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum MetadataValue {
    /// Free-form text.
    Text { value: String },
    /// Integer value.
    Int { value: i64 },
    /// Floating-point value.
    Float { value: f64 },
    /// Boolean value.
    Bool { value: bool },
    /// Validated URL (stored as string, validated on construction).
    Url { value: String },
    /// Filesystem or object-store path.
    Path { value: String },
    /// Serialized JSON string.
    Json { value: String },
    /// Markdown-formatted content.
    Markdown { value: String },
    /// Unix epoch timestamp in seconds.
    Timestamp { value: f64 },
    /// Explicit null.
    Null {},
    /// Binary data size in bytes.
    Bytes { value: u64 },
    /// Time duration in seconds.
    Duration { value: f64 },
    /// SQL query with optional dialect hint.
    Sql {
        query: String,
        dialect: Option<String>,
    },
    /// Code snippet with optional language hint.
    CodeBlock {
        code: String,
        language: Option<String>,
    },
    /// Image data (base64, URL, or path).
    Image { value: String },
    /// Percentage value (rendered as %).
    Percentage { value: f64 },
    /// List of metadata values.
    List { values: Vec<MetadataValue> },
    /// Date range with start and end datetimes.
    DateRange {
        start: NaiveDateTime,
        end: NaiveDateTime,
    },
    /// Arrow schema (stored as IPC bytes for serialization).
    Schema { ipc_bytes: Vec<u8> },
    /// Data version identifier for an asset materialization or observation.
    DataVersion { value: String },
}

#[pymethods]
impl MetadataValue {
    #[staticmethod]
    fn text(value: String) -> Self {
        Self::Text { value }
    }

    #[staticmethod]
    fn int(value: i64) -> Self {
        Self::Int { value }
    }

    #[staticmethod]
    fn float_(value: f64) -> Self {
        Self::Float { value }
    }

    #[staticmethod]
    fn bool_(value: bool) -> Self {
        Self::Bool { value }
    }

    /// Parse and validate a URL. Raises ValueError if invalid.
    /// The stored value is the canonical form produced by parsing, so equivalent
    /// inputs (e.g. `https://example.com` and `https://example.com/`) compare equal.
    #[staticmethod]
    fn url(value: String) -> PyResult<Self> {
        let parsed = Url::parse(&value)
            .map_err(|e| InvalidMetadataError::new_err(format!("Invalid URL: {e}")))?;
        Ok(Self::Url {
            value: parsed.into(),
        })
    }

    #[staticmethod]
    fn path(value: String) -> Self {
        Self::Path { value }
    }

    #[staticmethod]
    fn json(value: String) -> Self {
        Self::Json { value }
    }

    #[staticmethod]
    fn md(value: String) -> Self {
        Self::Markdown { value }
    }

    #[staticmethod]
    fn timestamp(value: f64) -> Self {
        Self::Timestamp { value }
    }

    #[staticmethod]
    fn null() -> Self {
        Self::Null {}
    }

    #[staticmethod]
    fn bytes(value: u64) -> Self {
        Self::Bytes { value }
    }

    #[staticmethod]
    fn duration(value: f64) -> Self {
        Self::Duration { value }
    }

    #[staticmethod]
    #[pyo3(signature = (query, dialect=None))]
    fn sql(query: String, dialect: Option<String>) -> Self {
        Self::Sql { query, dialect }
    }

    #[staticmethod]
    #[pyo3(signature = (code, language=None))]
    fn code_block(code: String, language: Option<String>) -> Self {
        Self::CodeBlock { code, language }
    }

    #[staticmethod]
    fn image(value: String) -> Self {
        Self::Image { value }
    }

    #[staticmethod]
    fn percentage(value: f64) -> Self {
        Self::Percentage { value }
    }

    #[staticmethod]
    fn list_(values: Vec<MetadataValue>) -> Self {
        Self::List { values }
    }

    #[staticmethod]
    fn date_range(start: NaiveDateTime, end: NaiveDateTime) -> Self {
        Self::DateRange { start, end }
    }

    /// Create from an Arrow schema (any object implementing __arrow_c_schema__).
    #[staticmethod]
    fn schema(value: pyo3_arrow::PySchema) -> Self {
        let ipc_bytes = schema_to_ipc_bytes(value.as_ref());
        Self::Schema { ipc_bytes }
    }

    /// Data version identifier (e.g. content hash, ETag, or custom version string).
    #[staticmethod]
    fn data_version(value: String) -> Self {
        Self::DataVersion { value }
    }

    /// Unwrap to a raw Python value.
    fn raw_value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::Text { value }
            | Self::Url { value }
            | Self::Path { value }
            | Self::Json { value }
            | Self::Markdown { value }
            | Self::Image { value } => Ok(value.into_pyobject(py)?.into_any().unbind()),
            Self::Sql { query, .. } => Ok(query.into_pyobject(py)?.into_any().unbind()),
            Self::CodeBlock { code, .. } => Ok(code.into_pyobject(py)?.into_any().unbind()),
            Self::Int { value } => Ok(value.into_pyobject(py)?.into_any().unbind()),
            Self::Float { value }
            | Self::Timestamp { value }
            | Self::Duration { value }
            | Self::Percentage { value } => Ok(value.into_pyobject(py)?.into_any().unbind()),
            Self::Bool { value } => Ok(PyBool::new(py, *value).to_owned().into_any().unbind()),
            Self::Bytes { value } => Ok(value.into_pyobject(py)?.into_any().unbind()),
            Self::Null {} => Ok(py.None()),
            Self::List { values } => {
                let items: Vec<Py<PyAny>> = values
                    .iter()
                    .map(|v| v.clone().into_pyobject(py).map(|o| o.into_any().unbind()))
                    .collect::<Result<_, _>>()?;
                Ok(PyList::new(py, &items)?.into_any().unbind())
            }
            Self::DateRange { start, end } => {
                let start_py = start.into_pyobject(py)?.into_any().unbind();
                let end_py = end.into_pyobject(py)?.into_any().unbind();
                Ok(PyTuple::new(py, &[start_py, end_py])?.into_any().unbind())
            }
            Self::Schema { ipc_bytes } => {
                let schema = schema_from_ipc_bytes(ipc_bytes)?;
                let wrapper = PySchemaWrapper::from_arc(schema);
                Ok(wrapper.into_pyobject(py)?.into_any().unbind())
            }
            Self::DataVersion { value } => Ok(value.into_pyobject(py)?.into_any().unbind()),
        }
    }

    fn __repr__(&self) -> String {
        match self {
            Self::Text { value } => format!("MetadataValue.text({value:?})"),
            Self::Int { value } => format!("MetadataValue.int({value})"),
            Self::Float { value } => format!("MetadataValue.float_({value:?})"),
            Self::Bool { value } => format!("MetadataValue.bool_({value})"),
            Self::Url { value } => format!("MetadataValue.url({value:?})"),
            Self::Path { value } => format!("MetadataValue.path({value:?})"),
            Self::Json { value } => format!("MetadataValue.json({value:?})"),
            Self::Markdown { value } => format!("MetadataValue.md({value:?})"),
            Self::Timestamp { value } => format!("MetadataValue.timestamp({value:?})"),
            Self::Null {} => "MetadataValue.null()".to_string(),
            Self::Bytes { value } => format!("MetadataValue.bytes({value})"),
            Self::Duration { value } => format!("MetadataValue.duration({value:?})"),
            Self::Sql { query, dialect } => match dialect {
                Some(d) => format!("MetadataValue.sql({query:?}, dialect={d:?})"),
                None => format!("MetadataValue.sql({query:?})"),
            },
            Self::CodeBlock { code, language } => match language {
                Some(l) => format!("MetadataValue.code_block({code:?}, language={l:?})"),
                None => format!("MetadataValue.code_block({code:?})"),
            },
            Self::Image { value } => format!("MetadataValue.image({value:?})"),
            Self::Percentage { value } => format!("MetadataValue.percentage({value:?})"),
            Self::List { values } => {
                let items: Vec<String> = values.iter().map(|v| v.__repr__()).collect();
                format!("MetadataValue.list_([{}])", items.join(", "))
            }
            Self::DateRange { start, end } => {
                format!(
                    "MetadataValue.date_range({}, {})",
                    fmt_naive_datetime(start),
                    fmt_naive_datetime(end)
                )
            }
            Self::Schema { ipc_bytes } => {
                if let Ok(schema) = schema_from_ipc_bytes(ipc_bytes) {
                    let fields: Vec<String> = schema
                        .fields()
                        .iter()
                        .map(|f| format!("{}: {}", f.name(), f.data_type()))
                        .collect();
                    format!("MetadataValue.schema({{{}}})", fields.join(", "))
                } else {
                    format!("MetadataValue.schema(<{} bytes>)", ipc_bytes.len())
                }
            }
            Self::DataVersion { value } => format!("MetadataValue.data_version({value:?})"),
        }
    }

    fn __eq__(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Text { value: a }, Self::Text { value: b }) => a == b,
            (Self::Int { value: a }, Self::Int { value: b }) => a == b,
            (Self::Float { value: a }, Self::Float { value: b }) => a == b,
            (Self::Bool { value: a }, Self::Bool { value: b }) => a == b,
            (Self::Url { value: a }, Self::Url { value: b }) => a == b,
            (Self::Path { value: a }, Self::Path { value: b }) => a == b,
            (Self::Json { value: a }, Self::Json { value: b }) => a == b,
            (Self::Markdown { value: a }, Self::Markdown { value: b }) => a == b,
            (Self::Timestamp { value: a }, Self::Timestamp { value: b }) => a == b,
            (Self::Null {}, Self::Null {}) => true,
            (Self::Bytes { value: a }, Self::Bytes { value: b }) => a == b,
            (Self::Duration { value: a }, Self::Duration { value: b }) => a == b,
            (
                Self::Sql {
                    query: q1,
                    dialect: d1,
                },
                Self::Sql {
                    query: q2,
                    dialect: d2,
                },
            ) => q1 == q2 && d1 == d2,
            (
                Self::CodeBlock {
                    code: c1,
                    language: l1,
                },
                Self::CodeBlock {
                    code: c2,
                    language: l2,
                },
            ) => c1 == c2 && l1 == l2,
            (Self::Image { value: a }, Self::Image { value: b }) => a == b,
            (Self::Percentage { value: a }, Self::Percentage { value: b }) => a == b,
            (Self::List { values: a }, Self::List { values: b }) => {
                a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x.__eq__(y))
            }
            (Self::DateRange { start: s1, end: e1 }, Self::DateRange { start: s2, end: e2 }) => {
                s1 == s2 && e1 == e2
            }
            (Self::Schema { ipc_bytes: a }, Self::Schema { ipc_bytes: b }) => a == b,
            (Self::DataVersion { value: a }, Self::DataVersion { value: b }) => a == b,
            _ => false,
        }
    }
}

fn fmt_naive_datetime(dt: &NaiveDateTime) -> String {
    let micro = dt.nanosecond() / 1000;
    if micro == 0 {
        format!(
            "datetime.datetime({}, {}, {}, {}, {}, {})",
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour(),
            dt.minute(),
            dt.second(),
        )
    } else {
        format!(
            "datetime.datetime({}, {}, {}, {}, {}, {}, {})",
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour(),
            dt.minute(),
            dt.second(),
            micro,
        )
    }
}

/// Convert a Python value to MetadataValue.
/// If already a MetadataValue, returns as-is. Otherwise coerces primitives.
pub fn coerce_to_metadata_value(
    _py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<MetadataValue> {
    if let Ok(mv) = value.extract::<MetadataValue>() {
        return Ok(mv);
    }
    // bool MUST come before int — Python bool subclasses int
    if value.is_instance_of::<PyBool>() {
        return Ok(MetadataValue::Bool {
            value: value.is_truthy()?,
        });
    }
    if let Ok(i) = value.extract::<i64>() {
        return Ok(MetadataValue::Int { value: i });
    }
    if let Ok(f) = value.extract::<f64>() {
        return Ok(MetadataValue::Float { value: f });
    }
    if let Ok(s) = value.extract::<String>() {
        return Ok(MetadataValue::Text { value: s });
    }
    if value.is_none() {
        return Ok(MetadataValue::Null {});
    }
    if value.hasattr("__arrow_c_schema__")? {
        let py_schema: pyo3_arrow::PySchema = value.extract()?;
        let ipc_bytes = schema_to_ipc_bytes(py_schema.as_ref());
        return Ok(MetadataValue::Schema { ipc_bytes });
    }
    Err(PyTypeError::new_err(format!(
        "Cannot coerce {} to MetadataValue. Use MetadataValue.text(), .int(), etc.",
        value.get_type().name()?
    )))
}
