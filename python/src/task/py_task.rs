//! PyTask — a Python-callable task used inside graph asset composition.
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::assets::decorator::{is_coroutine_function, name_or_fn_name};
use crate::assets::io_handler::IOHandler;
use crate::composition::{
    InvokedNodeType, PyInvokedNodeOutput, extract_input_bindings, is_in_composition,
    observe_invocation,
};
use crate::errors::TaskDefinitionError;
use crate::partitions::PartitionsDefinition;
use crate::partitions::mapping::PartitionMappingDict;

pub struct Task {
    pub wraps: Option<Py<PyAny>>,
    pub is_async: bool,
    pub name: Option<String>,
    pub tags: Option<Vec<String>>,
    pub partitions_def: Option<Py<PartitionsDefinition>>,
    pub partition_mapping: Option<PartitionMappingDict>,
    /// IO handler for the task. Set to the shared `InMemoryIOHandler` by default during resolve.
    pub io_handler: Option<IOHandler>,
    pub retry: Option<rivers_core::execution::retry::RetryRef>,
}

/// A composable task, exposed to Python as `Task`.
///
/// `Task` acts as both a decorator (`@Task`, `@Task(name=...)`) and a
/// composable node in the execution DAG. When called inside a composition
/// context it records an invocation; otherwise it executes the wrapped
/// function directly.
#[pyclass(name = "Task", module = "rivers._core")]
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
        partitions_def: Option<Py<PartitionsDefinition>>,
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
        py: Python,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let mut this = slf.borrow_mut();

        if this.inner.wraps.is_none() {
            let func = args.get_item(0)?;
            let func: Py<PyAny> = func.into();
            let function_name = func.getattr(py, "__name__")?.to_string();
            this.inner.is_async = is_coroutine_function(py, &Some(func.clone_ref(py)));
            this.inner.wraps = Some(func);
            this.inner.name.get_or_insert(function_name);
            drop(this);
            Ok(slf.clone().unbind().into_any())
        } else if is_in_composition() {
            let name = this
                .inner
                .name
                .as_ref()
                .ok_or_else(|| TaskDefinitionError::new_err("Task must have a name"))?
                .clone();
            drop(this);
            let input_bindings = extract_input_bindings(args, kwargs)?;
            let registered_name = observe_invocation(&name, InvokedNodeType::Task, input_bindings);
            let output = PyInvokedNodeOutput::with_default_output(registered_name);
            Ok(output.into_pyobject(py)?.into_any().unbind())
        } else {
            let func = this
                .inner
                .wraps
                .as_ref()
                .ok_or_else(|| TaskDefinitionError::new_err("Task has no function"))?
                .clone_ref(py);
            drop(this);
            let result = func.call(py, args, kwargs)?;
            Ok(result)
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
        self.inner.io_handler.as_ref().and_then(|h| match h {
            crate::assets::io_handler::IOHandler::Instance(obj) => Some(obj.clone_ref(py)),
            crate::assets::io_handler::IOHandler::ResourceRef(_) => None,
        })
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
