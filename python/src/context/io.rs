//! InputContext and OutputContext — passed to IO handler load_input/handle_output methods.
//!
//! `PyOutputContext` also collects output metadata and data_version written by the handler
//! for propagation back to the finalization step.
use std::collections::HashMap;
use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::metadata::{MetadataValue, coerce_to_metadata_value};
use crate::partitions::PartitionContext;

#[pyclass(name = "OutputContext", frozen, module = "rivers._core")]
pub struct PyOutputContext {
    #[pyo3(get)]
    pub asset_name: String,
    #[pyo3(get)]
    pub asset_metadata: Option<HashMap<String, String>>,
    #[pyo3(get)]
    pub partition: Option<PartitionContext>,
    #[pyo3(get)]
    pub type_hint: Option<Py<PyAny>>,
    _output_metadata: Mutex<HashMap<String, MetadataValue>>,
    _data_version: Mutex<Option<String>>,
}

impl PyOutputContext {
    pub fn new(
        asset_name: String,
        asset_metadata: Option<HashMap<String, String>>,
        partition: Option<PartitionContext>,
        type_hint: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            asset_name,
            asset_metadata,
            partition,
            type_hint,
            _output_metadata: Mutex::new(HashMap::new()),
            _data_version: Mutex::new(None),
        }
    }

    pub fn new_with_metadata(
        asset_name: String,
        asset_metadata: Option<HashMap<String, String>>,
        partition: Option<PartitionContext>,
        type_hint: Option<Py<PyAny>>,
        initial: Vec<(String, MetadataValue)>,
    ) -> Self {
        Self {
            asset_name,
            asset_metadata,
            partition,
            type_hint,
            _output_metadata: Mutex::new(initial.into_iter().collect()),
            _data_version: Mutex::new(None),
        }
    }
}

#[pymethods]
impl PyOutputContext {
    #[new]
    #[pyo3(signature = (asset_name, asset_metadata=None, partition=None, type_hint=None))]
    fn py_new(
        asset_name: String,
        asset_metadata: Option<HashMap<String, String>>,
        partition: Option<PartitionContext>,
        type_hint: Option<Py<PyAny>>,
    ) -> Self {
        Self::new(asset_name, asset_metadata, partition, type_hint)
    }

    /// Add output metadata from a dict of key-value pairs.
    /// Values can be MetadataValue or raw str/int/float/bool/None (auto-coerced).
    fn add_output_metadata(&self, metadata: Bound<'_, PyDict>) -> PyResult<()> {
        let mut entries = self._output_metadata.lock().unwrap();
        for (k, v) in metadata.iter() {
            let key: String = k.extract()?;
            let mv = coerce_to_metadata_value(v.py(), &v)?;
            entries.insert(key, mv);
        }
        Ok(())
    }

    /// Register a custom data version for this materialization.
    /// Overrides the auto-generated UUID version.
    fn register_data_version(&self, version: String) {
        *self._data_version.lock().unwrap() = Some(version);
    }

    /// The output metadata collected by IO handlers, or None if empty.
    #[getter]
    fn output_metadata(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let entries = self._output_metadata.lock().unwrap();
        if entries.is_empty() {
            return Ok(py.None());
        }
        let dict = PyDict::new(py);
        for (k, v) in &*entries {
            dict.set_item(k, v.clone())?;
        }
        Ok(dict.into())
    }

    pub fn drain_data_version(&self) -> Option<String> {
        self._data_version.lock().unwrap().take()
    }

    /// Construct from a Python kwargs dict (used by worker subprocess).
    #[staticmethod]
    pub fn from_kwargs(_py: Python, kwargs: &Bound<'_, PyDict>) -> PyResult<Self> {
        let asset_name: String = kwargs.get_item("asset_name")?.unwrap().extract()?;
        let asset_metadata: Option<HashMap<String, String>> = kwargs
            .get_item("asset_metadata")?
            .and_then(|v| v.extract().ok());
        let partition: Option<PartitionContext> =
            kwargs.get_item("partition")?.and_then(|v| v.extract().ok());
        let type_hint: Option<Py<PyAny>> = kwargs
            .get_item("type_hint")?
            .and_then(|v| if v.is_none() { None } else { Some(v.unbind()) });
        Ok(Self::new(asset_name, asset_metadata, partition, type_hint))
    }

    fn __repr__(&self) -> String {
        format!("OutputContext(asset_name='{}')", self.asset_name)
    }
}

#[pyclass(name = "InputContext", frozen, get_all, module = "rivers._core")]
pub struct PyInputContext {
    pub asset_name: String,
    pub downstream_asset: String,
    pub asset_metadata: Option<HashMap<String, String>>,
    pub partition: Option<PartitionContext>,
    pub type_hint: Option<Py<PyAny>>,
}

#[pymethods]
impl PyInputContext {
    #[new]
    #[pyo3(signature = (asset_name, downstream_asset, asset_metadata=None, partition=None, type_hint=None))]
    pub fn new(
        asset_name: String,
        downstream_asset: String,
        asset_metadata: Option<HashMap<String, String>>,
        partition: Option<PartitionContext>,
        type_hint: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            asset_name,
            downstream_asset,
            asset_metadata,
            partition,
            type_hint,
        }
    }

    /// Construct from a Python kwargs dict (used by worker subprocess).
    #[staticmethod]
    pub fn from_kwargs(_py: Python, kwargs: &Bound<'_, PyDict>) -> PyResult<Self> {
        let asset_name: String = kwargs.get_item("asset_name")?.unwrap().extract()?;
        let downstream_asset: String = kwargs.get_item("downstream_asset")?.unwrap().extract()?;
        let asset_metadata: Option<HashMap<String, String>> = kwargs
            .get_item("asset_metadata")?
            .and_then(|v| v.extract().ok());
        let partition: Option<PartitionContext> =
            kwargs.get_item("partition")?.and_then(|v| v.extract().ok());
        let type_hint: Option<Py<PyAny>> = kwargs
            .get_item("type_hint")?
            .and_then(|v| if v.is_none() { None } else { Some(v.unbind()) });
        Ok(Self::new(
            asset_name,
            downstream_asset,
            asset_metadata,
            partition,
            type_hint,
        ))
    }

    fn __repr__(&self) -> String {
        format!(
            "InputContext(asset_name='{}', downstream_asset='{}')",
            self.asset_name, self.downstream_asset
        )
    }
}
