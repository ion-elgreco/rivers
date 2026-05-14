//! Result types — Output, Observation, Materialization, and DynamicOutput returned from asset functions.
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::metadata::{MetadataValue, coerce_to_metadata_value};

/// Per-asset materialization result.
///
/// Returned from an `@Asset` function to carry output metadata, data version,
/// and tags alongside the materialized value.
#[pyclass(name = "Output", frozen, module = "rivers._core")]
pub struct PyOutput {
    value: Option<Py<PyAny>>,
    #[pyo3(get)]
    output_name: Option<String>,
    metadata: Option<Py<PyDict>>,
    #[pyo3(get)]
    data_version: Option<String>,
    #[pyo3(get)]
    tags: Option<Vec<String>>,
}

#[pymethods]
impl PyOutput {
    #[new]
    #[pyo3(signature = (value=None, *, output_name=None, metadata=None, data_version=None, tags=None))]
    fn new(
        value: Option<Py<PyAny>>,
        output_name: Option<String>,
        metadata: Option<Py<PyDict>>,
        data_version: Option<String>,
        tags: Option<Vec<String>>,
    ) -> Self {
        Self {
            value,
            output_name,
            metadata,
            data_version,
            tags,
        }
    }

    #[getter]
    fn value(&self, py: Python) -> Py<PyAny> {
        match &self.value {
            Some(v) => v.clone_ref(py),
            None => py.None(),
        }
    }

    #[getter]
    fn metadata(&self, py: Python) -> Py<PyAny> {
        match &self.metadata {
            Some(d) => d.clone_ref(py).into_any(),
            None => py.None(),
        }
    }

    fn __repr__(&self, py: Python) -> String {
        let has_value = self.value.is_some();
        let has_meta = self
            .metadata
            .as_ref()
            .map(|d| d.bind(py).len() > 0)
            .unwrap_or(false);
        match (&self.output_name, &self.data_version) {
            (Some(name), Some(dv)) => format!(
                "Output(output_name='{}', has_value={}, has_metadata={}, data_version='{}')",
                name, has_value, has_meta, dv
            ),
            (Some(name), None) => format!(
                "Output(output_name='{}', has_value={}, has_metadata={})",
                name, has_value, has_meta
            ),
            (None, Some(dv)) => format!(
                "Output(has_value={}, has_metadata={}, data_version='{}')",
                has_value, has_meta, dv
            ),
            (None, None) => format!("Output(has_value={}, has_metadata={})", has_value, has_meta),
        }
    }
}

/// Per-asset materialization-only result.
///
/// Returned from an `@Asset` function when the asset has already persisted its
/// own output (terminal side-effecting nodes, or assets that manage their own
/// IO). The framework emits a Materialization event with the supplied metadata
/// and `data_version` but never calls `handle_output`.
///
/// Use this **instead of** `Output(value)` when:
///   - The asset writes directly to its destination (API push, message emit,
///     external table write) and there's nothing to round-trip through an IO
///     handler.
///   - You want the run to record provenance (data_version, metadata) without
///     persisting a value rivers can later `load_input`.
///
/// Downstream consumers cannot `load_input` an asset that returned
/// `Materialization` (no IO write happened) — that's by design. Treat such
/// assets as terminal in the graph.
#[pyclass(name = "Materialization", frozen, module = "rivers._core")]
pub struct PyMaterialization {
    #[pyo3(get)]
    output_name: Option<String>,
    metadata: Option<Py<PyDict>>,
    #[pyo3(get)]
    data_version: Option<String>,
    #[pyo3(get)]
    tags: Option<Vec<String>>,
}

#[pymethods]
impl PyMaterialization {
    #[new]
    #[pyo3(signature = (*, output_name=None, metadata=None, data_version=None, tags=None))]
    fn new(
        output_name: Option<String>,
        metadata: Option<Py<PyDict>>,
        data_version: Option<String>,
        tags: Option<Vec<String>>,
    ) -> Self {
        Self {
            output_name,
            metadata,
            data_version,
            tags,
        }
    }

    #[getter]
    fn metadata(&self, py: Python) -> Py<PyAny> {
        match &self.metadata {
            Some(d) => d.clone_ref(py).into_any(),
            None => py.None(),
        }
    }

    fn __repr__(&self, py: Python) -> String {
        let has_meta = self
            .metadata
            .as_ref()
            .map(|d| d.bind(py).len() > 0)
            .unwrap_or(false);
        match (&self.output_name, &self.data_version) {
            (Some(name), Some(dv)) => format!(
                "Materialization(output_name='{}', has_metadata={}, data_version='{}')",
                name, has_meta, dv
            ),
            (Some(name), None) => format!(
                "Materialization(output_name='{}', has_metadata={})",
                name, has_meta
            ),
            (None, Some(dv)) => format!(
                "Materialization(has_metadata={}, data_version='{}')",
                has_meta, dv
            ),
            (None, None) => format!("Materialization(has_metadata={})", has_meta),
        }
    }
}

/// Per-asset observation result.
///
/// Returned from an external asset's observe function to carry observation
/// metadata and data version.
#[pyclass(name = "Observation", frozen, module = "rivers._core")]
pub struct PyObservation {
    #[pyo3(get)]
    output_name: Option<String>,
    metadata: Option<Py<PyDict>>,
    #[pyo3(get)]
    data_version: Option<String>,
}

#[pymethods]
impl PyObservation {
    #[new]
    #[pyo3(signature = (*, output_name=None, metadata=None, data_version=None))]
    fn new(
        output_name: Option<String>,
        metadata: Option<Py<PyDict>>,
        data_version: Option<String>,
    ) -> Self {
        Self {
            output_name,
            metadata,
            data_version,
        }
    }

    #[getter]
    fn metadata(&self, py: Python) -> Py<PyAny> {
        match &self.metadata {
            Some(d) => d.clone_ref(py).into_any(),
            None => py.None(),
        }
    }

    fn __repr__(&self) -> String {
        match (&self.output_name, &self.data_version) {
            (Some(name), Some(dv)) => {
                format!("Observation(output_name='{}', data_version='{}')", name, dv)
            }
            (Some(name), None) => format!("Observation(output_name='{}')", name),
            (None, Some(dv)) => format!("Observation(data_version='{}')", dv),
            (None, None) => "Observation()".to_string(),
        }
    }
}

/// A value with an explicit mapping key for dynamic fan-out.
///
/// When a producer asset returns a list of `DynamicOutput`, the executor
/// uses `.key` as the mapping key (instance name) instead of a numeric index.
#[pyclass(name = "DynamicOutput", frozen, module = "rivers._core")]
pub struct PyDynamicOutput {
    #[pyo3(get)]
    pub key: String,
    pub value: Py<PyAny>,
}

#[pymethods]
impl PyDynamicOutput {
    #[new]
    fn new(key: String, value: Py<PyAny>) -> Self {
        Self { key, value }
    }

    #[getter]
    fn value(&self, py: Python) -> Py<PyAny> {
        self.value.clone_ref(py)
    }

    fn __repr__(&self) -> String {
        format!("DynamicOutput(key='{}')", self.key)
    }
}

/// If the result is a list containing DynamicOutput items, unwrap the values
/// into a plain list and return the keys separately. Returns None if the result
/// is not a list of DynamicOutput.
pub(crate) fn try_unwrap_dynamic_outputs(
    py: Python,
    result: &Py<PyAny>,
) -> PyResult<Option<(Py<PyAny>, Vec<String>)>> {
    match result.bind(py).try_iter() {
        Ok(_) => {}
        Err(_) => return Ok(None),
    };

    let bound = result.bind(py);
    // Must be a list (not a string or other iterable)
    if !bound.is_instance_of::<pyo3::types::PyList>() {
        return Ok(None);
    }

    let py_list: &Bound<pyo3::types::PyList> = bound.cast()?;
    if py_list.is_empty() {
        return Ok(None);
    }

    // If first element is a DynamicOutput, unwrap all
    let first = py_list.get_item(0)?;
    if first.extract::<PyRef<'_, PyDynamicOutput>>().is_err() {
        return Ok(None);
    }

    let mut values = Vec::with_capacity(py_list.len());
    let mut keys = Vec::with_capacity(py_list.len());
    for item in py_list.iter() {
        if let Ok(dynamic) = item.extract::<PyRef<'_, PyDynamicOutput>>() {
            keys.push(dynamic.key.clone());
            values.push(dynamic.value.clone_ref(py));
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "Cannot mix DynamicOutput and plain values in a fan-out source list. \
                 Either all items must be DynamicOutput or none.",
            ));
        }
    }

    let py_values = pyo3::types::PyList::new(py, values.iter().map(|v| v.bind(py)))?;
    Ok(Some((py_values.unbind().into_any(), keys)))
}

/// Discriminator for the three asset return-type wrappers. Drives the
/// per-output recipe in `ops::for_each_output` consumers (write IO + emit
/// Materialization, just emit Materialization, or just emit Observation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResultKind {
    /// `Output(value, ...)` — framework writes value via IO handler, then emits Materialization.
    Output,
    /// `Observation(...)` — external asset state; emit Observation event, no IO.
    Observation,
    /// `Materialization(...)` — user persisted it themselves; emit Materialization event, no IO.
    Materialization,
}

impl ResultKind {
    /// Compact byte tag for IPC across the worker pickle boundary.
    /// Stable wire values: 0 = Output, 1 = Observation, 2 = Materialization.
    pub(crate) fn as_u8(self) -> u8 {
        match self {
            ResultKind::Output => 0,
            ResultKind::Observation => 1,
            ResultKind::Materialization => 2,
        }
    }

    /// Inverse of `as_u8`. Returns `Err` for an unknown tag (forward-compat
    /// guard for older orchestrator + newer worker mismatches).
    pub(crate) fn from_u8(tag: u8) -> PyResult<Self> {
        match tag {
            0 => Ok(ResultKind::Output),
            1 => Ok(ResultKind::Observation),
            2 => Ok(ResultKind::Materialization),
            other => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Unknown ResultKind tag: {other}"
            ))),
        }
    }
}

/// Extracted fields from a result type (Output, Observation, or Materialization).
pub(crate) struct ExtractedResult {
    /// The unwrapped value (None for Observation and Materialization — neither carries a value).
    pub value: Option<Py<PyAny>>,
    /// Metadata entries extracted from the result dict.
    pub metadata: Vec<(String, MetadataValue)>,
    /// Explicit data version.
    pub data_version: Option<String>,
    /// Tags (Output and Materialization only — Observation has no tags).
    pub tags: Option<Vec<String>>,
    /// Which result-type variant this came from.
    pub kind: ResultKind,
    /// For multi-asset generator yields: which output this belongs to.
    pub output_name: Option<String>,
}

/// Try to extract result type fields from a Python object.
/// Returns None if the object is none of Output, Observation, or Materialization.
pub(crate) fn try_extract_result_type(
    py: Python,
    obj: &Py<PyAny>,
) -> PyResult<Option<ExtractedResult>> {
    let bound = obj.bind(py);

    if let Ok(output) = bound.cast::<PyOutput>() {
        let o = output.borrow();
        let metadata = extract_metadata_dict(py, &o.metadata)?;
        return Ok(Some(ExtractedResult {
            value: o.value.as_ref().map(|v| v.clone_ref(py)),
            metadata,
            data_version: o.data_version.clone(),
            tags: o.tags.clone(),
            kind: ResultKind::Output,
            output_name: o.output_name.clone(),
        }));
    }

    if let Ok(observation) = bound.cast::<PyObservation>() {
        let o = observation.borrow();
        let metadata = extract_metadata_dict(py, &o.metadata)?;
        return Ok(Some(ExtractedResult {
            value: None,
            metadata,
            data_version: o.data_version.clone(),
            tags: None,
            kind: ResultKind::Observation,
            output_name: o.output_name.clone(),
        }));
    }

    if let Ok(materialization) = bound.cast::<PyMaterialization>() {
        let m = materialization.borrow();
        let metadata = extract_metadata_dict(py, &m.metadata)?;
        return Ok(Some(ExtractedResult {
            value: None,
            metadata,
            data_version: m.data_version.clone(),
            tags: m.tags.clone(),
            kind: ResultKind::Materialization,
            output_name: m.output_name.clone(),
        }));
    }

    Ok(None)
}

/// Convert an optional PyDict of metadata to Vec<(String, MetadataValue)>.
fn extract_metadata_dict(
    py: Python,
    dict: &Option<Py<PyDict>>,
) -> PyResult<Vec<(String, MetadataValue)>> {
    let dict = match dict {
        Some(d) => d,
        None => return Ok(Vec::new()),
    };
    let bound = dict.bind(py);
    let mut entries = Vec::with_capacity(bound.len());
    for (k, v) in bound.iter() {
        let key: String = k.extract()?;
        let mv = coerce_to_metadata_value(py, &v)?;
        entries.push((key, mv));
    }
    Ok(entries)
}
