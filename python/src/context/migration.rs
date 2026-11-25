//! MigrationContext — passed to the convert of an asset migration.
use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyTuple, PyType};

/// Context injected into a convert function as its first parameter.
#[pyclass(name = "MigrationContext", frozen, module = "rivers._core")]
pub struct PyMigrationContext {
    /// The asset whose stored data the convert changes.
    #[pyo3(get)]
    pub asset_name: String,
    #[pyo3(get)]
    pub run_id: String,
    /// The version of the stored data after the convert.
    #[pyo3(get)]
    pub to_version: u32,
    #[pyo3(get)]
    pub asset_metadata: Option<HashMap<String, String>>,
    /// The asset's resolved IO handler: where its stored data is.
    #[pyo3(get)]
    pub io_handler: Option<Py<PyAny>>,
    /// Config instance built from the `MigrationContext[Config]` annotation.
    #[pyo3(get)]
    pub config: Option<Py<PyAny>>,
    _logger: PyOnceLock<Py<PyAny>>,
}

impl PyMigrationContext {
    pub fn new(
        asset_name: String,
        run_id: String,
        to_version: u32,
        asset_metadata: Option<HashMap<String, String>>,
        io_handler: Option<Py<PyAny>>,
        config: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            asset_name,
            run_id,
            to_version,
            asset_metadata,
            io_handler,
            config,
            _logger: PyOnceLock::new(),
        }
    }
}

#[pymethods]
impl PyMigrationContext {
    /// Build a context by hand — for calling a convert function directly in a
    /// unit test. Runs build their own.
    #[new]
    #[pyo3(signature = (asset_name, to_version, run_id=String::new(), asset_metadata=None, io_handler=None, config=None))]
    fn py_new(
        asset_name: String,
        to_version: u32,
        run_id: String,
        asset_metadata: Option<HashMap<String, String>>,
        io_handler: Option<Py<PyAny>>,
        config: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        if to_version < 2 {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "to_version must be 2 or more, got {to_version}"
            )));
        }
        Ok(Self::new(
            asset_name,
            run_id,
            to_version,
            asset_metadata,
            io_handler,
            config,
        ))
    }

    /// The version of the stored data before the convert.
    #[getter]
    fn from_version(&self) -> u32 {
        self.to_version - 1
    }

    /// Python logger named `code-repo.migrations.<asset_name>`, lazily
    /// initialized.
    #[getter]
    fn log<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let logger = self._logger.get_or_try_init(py, || {
            let name = format!("code-repo.migrations.{}", self.asset_name);
            py.import("logging")?
                .call_method1("getLogger", (name,))
                .map(Bound::unbind)
        })?;
        Ok(logger.bind(py).clone())
    }

    #[classmethod]
    fn __class_getitem__(cls: &Bound<'_, PyType>, item: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let py = item.py();
        let args = PyTuple::new(py, std::slice::from_ref(item))?;
        py.import("types")?
            .getattr("GenericAlias")?
            .call1((cls, args))
            .map(Bound::unbind)
    }

    fn __repr__(&self) -> String {
        format!(
            "MigrationContext(asset_name='{}', from_version={}, to_version={}, run_id='{}')",
            self.asset_name,
            self.to_version - 1,
            self.to_version,
            self.run_id
        )
    }
}
