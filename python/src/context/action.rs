//! ActionContext — passed to asset actions.
//!
//! Actions receive no upstream inputs (upstream is never executed); they get
//! the asset's identity, its IO handler as a config bag, and the per-asset
//! metadata that IO resolution honors.
use std::collections::HashMap;
use std::sync::OnceLock;

use pyo3::prelude::*;
use pyo3::types::{PyTuple, PyType};

use crate::errors::PartitionValidationError;
use crate::partitions::PartitionContext;

/// Context injected into action functions as their only parameter.
#[pyclass(name = "ActionContext", frozen, module = "rivers._core")]
pub struct PyActionContext {
    /// The asset the action operates on.
    #[pyo3(get)]
    pub asset_key: String,
    /// The verb being executed.
    #[pyo3(get)]
    pub action: String,
    #[pyo3(get)]
    pub run_id: String,
    #[pyo3(get)]
    pub asset_metadata: Option<HashMap<String, String>>,
    #[pyo3(get)]
    pub partition: Option<PartitionContext>,
    /// The asset's resolved IO handler — the action's config bag (same
    /// storage options / URIs the write path uses). None when the asset has
    /// no handler and no repository default exists.
    #[pyo3(get)]
    pub io_handler: Option<Py<PyAny>>,
    /// Config instance built from the action's `ActionContext[Config]`
    /// annotation; override values come from `run_action(config=...)`.
    #[pyo3(get)]
    pub config: Option<Py<PyAny>>,
    _logger: OnceLock<Py<PyAny>>,
}

impl PyActionContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        asset_key: String,
        action: String,
        run_id: String,
        asset_metadata: Option<HashMap<String, String>>,
        partition: Option<PartitionContext>,
        io_handler: Option<Py<PyAny>>,
        config: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            asset_key,
            action,
            run_id,
            asset_metadata,
            partition,
            io_handler,
            config,
            _logger: OnceLock::new(),
        }
    }
}

#[pymethods]
impl PyActionContext {
    /// True if this action is running against a partition.
    #[getter]
    fn has_partition_key(&self) -> bool {
        self.partition.is_some()
    }

    /// The single partition key string. Raises for non-partitioned assets and
    /// multi-key batches, matching `AssetExecutionContext.partition_key`.
    #[getter]
    fn partition_key(&self) -> PyResult<String> {
        match &self.partition {
            Some(ctx) => {
                if ctx.key_count() > 1 {
                    return Err(PartitionValidationError::new_err(
                        "partition_key is ambiguous for a batched run; \
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
                "No partition key available — action run is not partitioned",
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

    /// Python logger named `code-repo.actions.<asset_key>`, lazily initialized.
    #[getter]
    fn log<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let logger = self._logger.get_or_init(|| {
            let logging = py.import("logging").expect("failed to import logging");
            let name = format!("code-repo.actions.{}", self.asset_key);
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

    fn __repr__(&self) -> String {
        format!(
            "ActionContext(asset_key='{}', action='{}', run_id='{}')",
            self.asset_key, self.action, self.run_id
        )
    }
}
