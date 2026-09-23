//! PyO3 wrapper for the executor-neutral per-asset `Compute`.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use rivers_core::execution::compute::Compute;

/// Per-asset compute resources (`@Asset(compute=…)`). Values are Kubernetes-style
/// quantity strings; `None` on an axis inherits the executor default.
#[pyclass(name = "Compute", frozen, from_py_object, module = "rivers._core")]
#[derive(Clone, Debug)]
pub struct PyCompute {
    pub(crate) inner: Compute,
}

#[pymethods]
impl PyCompute {
    #[new]
    #[pyo3(signature = (*, cpu=None, memory=None, gpu=None))]
    fn new(cpu: Option<String>, memory: Option<String>, gpu: Option<String>) -> Self {
        Self {
            inner: Compute { cpu, memory, gpu },
        }
    }

    #[getter]
    fn cpu(&self) -> Option<String> {
        self.inner.cpu.clone()
    }

    #[getter]
    fn memory(&self) -> Option<String> {
        self.inner.memory.clone()
    }

    #[getter]
    fn gpu(&self) -> Option<String> {
        self.inner.gpu.clone()
    }

    /// A class-form asset defined in a script ships to loky workers by value,
    /// `compute` included.
    fn __getnewargs_ex__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyTuple>, Bound<'py, PyDict>)> {
        let Self {
            inner: Compute { cpu, memory, gpu },
        } = self;
        let kwargs = PyDict::new(py);
        kwargs.set_item("cpu", cpu)?;
        kwargs.set_item("memory", memory)?;
        kwargs.set_item("gpu", gpu)?;
        Ok((PyTuple::empty(py), kwargs))
    }

    fn __repr__(&self) -> String {
        let mut parts = Vec::new();
        if let Some(cpu) = &self.inner.cpu {
            parts.push(format!("cpu='{cpu}'"));
        }
        if let Some(mem) = &self.inner.memory {
            parts.push(format!("memory='{mem}'"));
        }
        if let Some(gpu) = &self.inner.gpu {
            parts.push(format!("gpu='{gpu}'"));
        }
        format!("Compute({})", parts.join(", "))
    }
}
