//! PartitionContext — provides partition keys and time window info to asset functions.
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use super::definition::PartitionsDefinition;
use super::key::PyPartitionKey;

#[pyclass(
    name = "PartitionContext",
    frozen,
    from_py_object,
    module = "rivers._core"
)]
#[derive(Clone, Debug)]
pub struct PartitionContext {
    /// The partition key(s) being processed. Single-element for normal
    /// materialization and MultiRun backfills; multiple for SingleRun/PerDimension.
    #[pyo3(get)]
    pub keys: Vec<PyPartitionKey>,
    #[pyo3(get)]
    pub definition: PartitionsDefinition,
}

impl PartitionContext {
    pub fn new(key: PyPartitionKey, definition: PartitionsDefinition) -> Self {
        Self {
            keys: vec![key],
            definition,
        }
    }

    pub fn new_multi(keys: Vec<PyPartitionKey>, definition: PartitionsDefinition) -> Self {
        Self { keys, definition }
    }

    /// Convenience: get the first partition key.
    /// Use `keys` directly when handling backfills with multiple keys.
    pub fn first_key(&self) -> &PyPartitionKey {
        &self.keys[0]
    }

    /// For TimeWindow partitions, return the (start, end) datetimes of this window.
    /// Only works when there's a single key.
    pub fn time_window(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        if self.keys.len() != 1 {
            return Ok(None);
        }
        match &self.keys[0] {
            PyPartitionKey::Single { key } if key.len() == 1 => {
                match self.definition.compute_time_window(&key[0])? {
                    Some((ws, we)) => {
                        let start_py = ws.into_pyobject(py)?.into_any().unbind();
                        let end_py = we.into_pyobject(py)?.into_any().unbind();
                        let tuple = PyTuple::new(py, &[start_py, end_py])?;
                        Ok(Some(tuple.into_any().unbind()))
                    }
                    None => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }
}

#[pymethods]
impl PartitionContext {
    #[new]
    #[pyo3(signature = (keys, definition))]
    fn py_new(keys: Vec<PyPartitionKey>, definition: PartitionsDefinition) -> Self {
        Self { keys, definition }
    }

    /// The single partition key. Convenience for `keys[0]` when processing
    /// one partition at a time (normal materialization or MultiRun backfill).
    // TODO: remove this getter — callers should use `keys` directly.
    // Keeping for backward compat during the transition; remove once all
    // downstream code (IO handlers, tests, user assets) is migrated to `keys`.
    #[getter]
    fn key(&self) -> PyPartitionKey {
        self.keys[0].clone()
    }

    #[pyo3(name = "time_window")]
    fn py_time_window(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.time_window(py)
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let reconstruct = py
            .import("rivers._core")?
            .getattr("_reconstruct_partition_context")?
            .unbind();
        let data = PyDict::new(py);
        let keys_py: Vec<Py<PyPartitionKey>> = self
            .keys
            .iter()
            .map(|k| Py::new(py, k.clone()))
            .collect::<PyResult<_>>()?;
        data.set_item("keys", keys_py)?;
        let def_py = Py::new(py, self.definition.clone())?;
        data.set_item("definition", def_py)?;
        let args = PyTuple::new(py, [data.into_any()])?;
        Ok((reconstruct, args.unbind().into_any()))
    }

    fn __repr__(&self) -> String {
        let keys_repr: Vec<String> = self.keys.iter().map(|k| k.__repr__()).collect();
        format!(
            "PartitionContext(keys=[{}], definition={})",
            keys_repr.join(", "),
            self.definition.__repr__()
        )
    }
}
