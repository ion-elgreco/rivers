//! PyTask — a Python-callable task used inside graph asset composition.
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use crate::assets::decorator::{is_coroutine_function, name_or_fn_name};
use crate::assets::io_handler::IOHandler;
use crate::composition::{
    InvokedNodeType, PyInvokedNodeOutput, extract_input_bindings, is_in_composition,
    observe_invocation,
};
use crate::errors::TaskDefinitionError;
use crate::partitions::PartitionsDefRef;
use crate::partitions::mapping::PartitionMappingDict;

pub struct Task {
    pub wraps: Option<Py<PyAny>>,
    pub is_async: bool,
    pub name: Option<String>,
    pub tags: Option<Vec<String>>,
    pub partitions_def: Option<PartitionsDefRef>,
    pub partition_mapping: Option<PartitionMappingDict>,
    /// IO handler for the task. Set to the shared `InMemoryIOHandler` by default during resolve.
    pub io_handler: Option<IOHandler>,
    pub retry: Option<rivers_core::execution::retry::RetryRef>,
}

impl Task {
    /// A copy of this config that wraps `func`, named after `func` unless the
    /// config has a name.
    fn wrapping(&self, py: Python, func: Py<PyAny>) -> PyResult<Self> {
        let name = match &self.name {
            Some(name) => name.clone(),
            None => func.getattr(py, "__name__")?.to_string(),
        };
        let wraps = Some(func);
        Ok(Self {
            is_async: is_coroutine_function(py, &wraps),
            wraps,
            name: Some(name),
            tags: self.tags.clone(),
            partitions_def: self.partitions_def.as_ref().map(|p| p.clone_ref(py)),
            partition_mapping: self.partition_mapping.clone(),
            io_handler: self.io_handler.as_ref().map(|h| h.clone_ref(py)),
            retry: self.retry.clone(),
        })
    }
}

/// A composable task, exposed to Python as `Task`.
///
/// `Task` acts as both a decorator (`@Task`, `@Task(name=...)`) and a
/// composable node in the execution DAG. A `Task` without a function is a
/// decorator: each call returns a new `Task` around the given function. A
/// `Task` with a function records an invocation when called inside a
/// composition context, and calls the function otherwise.
#[pyclass(name = "Task", module = "rivers._core", frozen)]
pub struct PyTask {
    pub inner: Task,
}

#[pymethods]
impl PyTask {
    #[new]
    #[pyo3(signature = (wraps=None, name=None, tags=None, partitions_def=None, partition_mapping=None, io_handler=None, retry=None))]
    fn new(
        py: Python,
        wraps: Option<Py<PyAny>>,
        name: Option<String>,
        tags: Option<Vec<String>>,
        partitions_def: Option<PartitionsDefRef>,
        partition_mapping: Option<PartitionMappingDict>,
        io_handler: Option<IOHandler>,
        retry: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let task_name = name_or_fn_name(py, name, &wraps);

        let is_async = is_coroutine_function(py, &wraps);
        Ok(Self {
            inner: Task {
                wraps,
                is_async,
                name: task_name,
                tags,
                partitions_def,
                partition_mapping,
                io_handler,
                retry: crate::retry::extract_retry_ref(retry)?,
            },
        })
    }

    #[pyo3(signature = (*args, **kwargs))]
    fn __call__(
        slf: &Bound<'_, Self>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let task = &slf.get().inner;
        let Some(func) = &task.wraps else {
            let inner = task.wrapping(py, args.get_item(0)?.unbind())?;
            return Ok(Py::new(py, Self { inner })?.into_any());
        };
        if is_in_composition() {
            let name = task
                .name
                .as_deref()
                .ok_or_else(|| TaskDefinitionError::new_err("Task must have a name"))?;
            let input_bindings = extract_input_bindings(args, kwargs)?;
            let registered_name = observe_invocation(name, InvokedNodeType::Task, input_bindings);
            let output = PyInvokedNodeOutput::with_default_output(registered_name);
            Ok(output.into_pyobject(py)?.into_any().unbind())
        } else {
            func.call(py, args, kwargs)
        }
    }

    #[getter]
    fn is_async(&self) -> bool {
        self.inner.is_async
    }

    #[getter]
    fn name(&self) -> Option<&str> {
        self.inner.name.as_deref()
    }

    #[getter]
    fn tags(&self) -> Option<&Vec<String>> {
        self.inner.tags.as_ref()
    }

    #[getter]
    fn io_handler(&self, py: Python) -> Option<Py<PyAny>> {
        self.inner
            .io_handler
            .as_ref()
            .and_then(|h| h.handler())
            .map(|obj| obj.clone_ref(py))
    }

    #[getter]
    fn _task_fn(&self, py: Python) -> PyResult<Py<PyAny>> {
        self.inner
            .wraps
            .as_ref()
            .map(|f| f.clone_ref(py))
            .ok_or_else(|| TaskDefinitionError::new_err("Task has no function"))
    }
}
