//! TaskContext — passed to @Task functions during graph asset execution.
use std::sync::OnceLock;

use crate::errors::PartitionValidationError;
use pyo3::prelude::*;
use pyo3::types::{PyTuple, PyType};

use crate::partitions::PartitionContext;

/// Context injected into task functions as the first parameter.
#[pyclass(name = "TaskExecutionContext", frozen, module = "rivers._core")]
pub struct PyTaskExecutionContext {
    #[pyo3(get)]
    pub task_name: String,
    #[pyo3(get)]
    pub tags: Option<Vec<String>>,
    #[pyo3(get)]
    pub partition: Option<PartitionContext>,
    /// Pydantic config instance (set via `TaskExecutionContext[Config]` generic).
    pub config_instance: Option<Py<PyAny>>,
    _logger: OnceLock<Py<PyAny>>,
}

impl PyTaskExecutionContext {
    pub fn new(
        task_name: String,
        tags: Option<Vec<String>>,
        partition: Option<PartitionContext>,
    ) -> Self {
        Self {
            task_name,
            tags,
            partition,
            config_instance: None,
            _logger: OnceLock::new(),
        }
    }

    pub fn with_config(mut self, config: Option<Py<PyAny>>) -> Self {
        self.config_instance = config;
        self
    }
}

#[pymethods]
impl PyTaskExecutionContext {
    #[new]
    #[pyo3(signature = (task_name, tags=None, partition=None))]
    fn py_new(
        task_name: String,
        tags: Option<Vec<String>>,
        partition: Option<PartitionContext>,
    ) -> Self {
        Self::new(task_name, tags, partition)
    }

    #[getter]
    fn config(&self, py: Python) -> Option<Py<PyAny>> {
        self.config_instance.as_ref().map(|c| c.clone_ref(py))
    }

    #[getter]
    fn has_partition_key(&self) -> bool {
        self.partition.is_some()
    }

    #[getter]
    fn partition_key(&self) -> PyResult<String> {
        match &self.partition {
            Some(ctx) => match ctx.first_key() {
                crate::partitions::PyPartitionKey::Single { key } if !key.is_empty() => {
                    Ok(key[0].clone())
                }
                _ => Err(PartitionValidationError::new_err(
                    "partition_key is only available for single-key partitions",
                )),
            },
            None => Err(PartitionValidationError::new_err(
                "No partition key available — task is not partitioned",
            )),
        }
    }

    #[getter]
    fn partition_time_window(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        match &self.partition {
            Some(ctx) => ctx.time_window(py),
            None => Ok(None),
        }
    }

    #[getter]
    fn log<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let logger = self._logger.get_or_init(|| {
            let logging = py.import("logging").expect("failed to import logging");
            let name = format!("code-repo.tasks.{}", self.task_name);
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
        format!("TaskExecutionContext(task_name='{}')", self.task_name)
    }
}
