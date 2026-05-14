use pyo3::prelude::*;
use pyo3::types::{PyTuple, PyType};

/// Wrapper type for declaring that an asset depends on its own previous materialization.
///
/// Used in type annotations as `SelfDependency[T]` to indicate the asset receives
/// its own prior output as an input parameter.
#[pyclass(name = "SelfDependency", module = "rivers._core")]
pub struct PySelfDependency {
    pub(crate) inner: Option<Py<PyAny>>,
}

#[pymethods]
impl PySelfDependency {
    fn get_inner(&self) -> Option<&Py<PyAny>> {
        self.inner.as_ref()
    }

    /// Enable `SelfDependency[T]` syntax in Python type annotations.
    #[classmethod]
    fn __class_getitem__(cls: &Bound<'_, PyType>, item: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let py = item.py();
        let types = py.import("types")?;
        let generic_alias = types.getattr("GenericAlias")?;
        let args = PyTuple::new(py, std::slice::from_ref(item))?;
        generic_alias.call1((cls, args)).map(|v| v.unbind())
    }

    /// Pickling support for parallel executor transport.
    fn __reduce__(&self, py: Python) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let cls = py
            .import("rivers._core")?
            .getattr("SelfDependency")?
            .unbind();
        let inner = self
            .inner
            .as_ref()
            .map(|v| v.clone_ref(py))
            .unwrap_or_else(|| py.None());
        let args = PyTuple::new(py, &[inner])?;
        Ok((cls, args.unbind().into_any()))
    }

    #[new]
    #[pyo3(signature = (inner=None))]
    fn new(py: Python, inner: Option<Py<PyAny>>) -> Self {
        let inner = inner.and_then(|v| if v.is_none(py) { None } else { Some(v) });
        Self { inner }
    }
}
