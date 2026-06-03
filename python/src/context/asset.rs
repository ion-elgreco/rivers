//! AssetExecutionContext — passed to @Asset functions during materialization.
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::errors::PartitionValidationError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple, PyType};

use crate::errors::ExecutionError;
use crate::metadata::{MetadataValue, coerce_to_metadata_value};
use crate::partitions::{PartitionContext, PyPartitionKey};

/// Context injected into asset functions as the first parameter.
#[pyclass(name = "AssetExecutionContext", frozen, module = "rivers._core")]
pub struct PyAssetExecutionContext {
    #[pyo3(get)]
    pub asset_name: String,
    #[pyo3(get)]
    pub tags: Option<Vec<String>>,
    #[pyo3(get)]
    pub kinds: Vec<String>,
    #[pyo3(get)]
    pub group: Option<String>,
    #[pyo3(get)]
    pub code_version: Option<String>,
    #[pyo3(get)]
    pub asset_metadata: Option<HashMap<String, String>>,
    #[pyo3(get)]
    pub is_multi_asset: bool,
    /// For multi-assets: the output names being materialized in this execution.
    /// Empty for single-asset execution.
    #[pyo3(get)]
    pub output_selection: Vec<String>,
    #[pyo3(get)]
    pub partition: Option<PartitionContext>,
    /// Pydantic config instance (set via `AssetExecutionContext[Config]` generic).
    pub config_instance: Option<Py<PyAny>>,
    /// Accumulated output metadata (last-wins on duplicate keys).
    _output_metadata: Mutex<HashMap<String, MetadataValue>>,
    _data_version: Mutex<Option<String>>,
    /// Partition keys marked as failed during a batched backfill run.
    _failed_backfill_partitions: Mutex<HashMap<PyPartitionKey, String>>,
    _logger: OnceLock<Py<PyAny>>,
}

impl PyAssetExecutionContext {
    pub fn new(
        asset_name: String,
        tags: Option<Vec<String>>,
        kinds: Vec<String>,
        group: Option<String>,
        code_version: Option<String>,
        asset_metadata: Option<HashMap<String, String>>,
        partition: Option<PartitionContext>,
        is_multi_asset: bool,
        output_selection: Vec<String>,
    ) -> Self {
        Self {
            asset_name,
            tags,
            kinds,
            group,
            code_version,
            is_multi_asset,
            output_selection,
            asset_metadata,
            partition,
            config_instance: None,
            _output_metadata: Mutex::new(HashMap::new()),
            _data_version: Mutex::new(None),
            _failed_backfill_partitions: Mutex::new(HashMap::new()),
            _logger: OnceLock::new(),
        }
    }

    pub fn drain_failed_backfill_partitions(&self) -> HashMap<PyPartitionKey, String> {
        std::mem::take(&mut *self._failed_backfill_partitions.lock().unwrap())
    }

    pub fn with_config(mut self, config: Option<Py<PyAny>>) -> Self {
        self.config_instance = config;
        self
    }

    pub fn drain_output_metadata(&self) -> Vec<(String, MetadataValue)> {
        let mut guard = self._output_metadata.lock().unwrap();
        std::mem::take(&mut *guard).into_iter().collect()
    }

    /// Read accumulated output metadata without draining.
    /// Used by generator multi-assets to apply context metadata as shared metadata per-yield.
    pub fn peek_output_metadata(&self) -> Vec<(String, MetadataValue)> {
        let guard = self._output_metadata.lock().unwrap();
        guard.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}

#[pymethods]
impl PyAssetExecutionContext {
    #[new]
    #[pyo3(signature = (asset_name, tags=None, kinds=vec![], group=None, code_version=None, asset_metadata=None, partition=None, is_multi_asset=false, output_selection=vec![], config=None))]
    fn py_new(
        asset_name: String,
        tags: Option<Vec<String>>,
        kinds: Vec<String>,
        group: Option<String>,
        code_version: Option<String>,
        asset_metadata: Option<HashMap<String, String>>,
        partition: Option<PartitionContext>,
        is_multi_asset: bool,
        output_selection: Vec<String>,
        config: Option<Py<PyAny>>,
    ) -> Self {
        Self::new(
            asset_name,
            tags,
            kinds,
            group,
            code_version,
            asset_metadata,
            partition,
            is_multi_asset,
            output_selection,
        )
        .with_config(config)
    }

    /// Mark a partition key as failed during a batched backfill run.
    /// Only valid when `backfill_partition_keys` is set (SingleRun/PerDimension).
    /// Partitions not marked as failed are considered succeeded.
    fn mark_partition_failed(&self, partition_key: PyPartitionKey, error: String) -> PyResult<()> {
        let valid = self
            .partition
            .as_ref()
            .map(|p| p.keys.contains(&partition_key))
            .unwrap_or(false);
        if !valid {
            return Err(ExecutionError::new_err(format!(
                "partition key {} is not in this context's partition keys",
                partition_key.__repr__()
            )));
        }
        self._failed_backfill_partitions
            .lock()
            .unwrap()
            .insert(partition_key, error);
        Ok(())
    }

    /// The Pydantic config instance, or None if no config type was specified.
    #[getter]
    fn config(&self) -> Option<&Py<PyAny>> {
        self.config_instance.as_ref()
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

    /// The output metadata collected so far, or None if empty.
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

    /// True if this asset is being executed with a partition key.
    #[getter]
    fn has_partition_key(&self) -> bool {
        self.partition.is_some()
    }

    /// The single partition key string. Raises for non-partitioned or multi-key assets.
    #[getter]
    fn partition_key(&self) -> PyResult<String> {
        match &self.partition {
            Some(ctx) => {
                if ctx.key_count() > 1 {
                    return Err(PartitionValidationError::new_err(
                        "partition_key is ambiguous for a batched (SingleRun/PerDimension) run; \
                         use context.partition.keys instead",
                    ));
                }
                match ctx.first_key() {
                    crate::partitions::PyPartitionKey::Single { key } if key.len() == 1 => {
                        Ok(key[0].clone())
                    }
                    _ => Err(PartitionValidationError::new_err(
                        "partition_key is only available for single-key partitions",
                    )),
                }
            }
            None => Err(PartitionValidationError::new_err(
                "No partition key available — asset is not partitioned",
            )),
        }
    }

    /// The `(start, end)` datetime tuple for time-window partitions, or None.
    #[getter]
    fn partition_time_window(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        match &self.partition {
            Some(ctx) => ctx.time_window(py),
            None => Ok(None),
        }
    }

    /// Python logger named `code-repo.assets.<asset_name>`, lazily initialized.
    #[getter]
    fn log<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let logger = self._logger.get_or_init(|| {
            let logging = py.import("logging").expect("failed to import logging");
            let name = format!("code-repo.assets.{}", self.asset_name);
            logging
                .call_method1("getLogger", (name,))
                .expect("failed to get logger")
                .unbind()
        });
        Ok(logger.bind(py).clone())
    }

    #[classmethod]
    fn __class_getitem__(cls: &Bound<'_, PyType>, item: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let py = item.py();
        let types = py.import("types")?;
        let generic_alias = types.getattr("GenericAlias")?;
        let args = PyTuple::new(py, std::slice::from_ref(item))?;
        generic_alias.call1((cls, args)).map(|v| v.unbind())
    }

    pub fn drain_data_version(&self) -> Option<String> {
        self._data_version.lock().unwrap().take()
    }

    fn __repr__(&self) -> String {
        format!("AssetExecutionContext(asset_name='{}')", self.asset_name)
    }
}
