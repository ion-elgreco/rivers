//! HookContext — passed to success/failure hook functions after asset execution.
use std::collections::HashMap;

use pyo3::prelude::*;

/// Context passed to hook functions on invocation.
#[pyclass(name = "HookContext", frozen, module = "rivers._core")]
pub struct PyHookContext {
    #[pyo3(get)]
    pub asset_name: String,
    #[pyo3(get)]
    pub run_id: String,
    /// "success" or "failure".
    #[pyo3(get)]
    pub hook_type: String,
    /// The asset's return value (only present for success hooks).
    #[pyo3(get)]
    pub output: Option<Py<PyAny>>,
    /// Error message (only present for failure hooks).
    #[pyo3(get)]
    pub error: Option<String>,
    #[pyo3(get)]
    pub metadata: Option<HashMap<String, String>>,
    /// Pydantic config instance (set via `HookContext[Config]` generic).
    pub config_instance: Option<Py<PyAny>>,
}

impl PyHookContext {
    pub fn new(
        asset_name: String,
        run_id: String,
        hook_type: String,
        output: Option<Py<PyAny>>,
        error: Option<String>,
        metadata: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            asset_name,
            run_id,
            hook_type,
            output,
            error,
            metadata,
            config_instance: None,
        }
    }

    pub fn with_config(mut self, config: Option<Py<PyAny>>) -> Self {
        self.config_instance = config;
        self
    }
}

#[pymethods]
impl PyHookContext {
    /// The Pydantic config instance, or None if no config type was specified.
    #[getter]
    fn config(&self, py: Python) -> Option<Py<PyAny>> {
        self.config_instance.as_ref().map(|c| c.clone_ref(py))
    }

    #[classmethod]
    fn __class_getitem__(
        cls: &Bound<'_, pyo3::types::PyType>,
        item: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let py = item.py();
        let types = py.import("types")?;
        let generic_alias = types.getattr("GenericAlias")?;
        let args = pyo3::types::PyTuple::new(py, std::slice::from_ref(item))?;
        generic_alias.call1((cls, args)).map(|v| v.unbind())
    }

    fn __repr__(&self) -> String {
        format!(
            "HookContext(asset_name='{}', hook_type='{}')",
            self.asset_name, self.hook_type
        )
    }
}
