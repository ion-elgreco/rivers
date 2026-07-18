//! BashTask — a task that executes a shell command string.
use std::collections::HashMap;

use pyo3::exceptions::PyOSError;
use pyo3::prelude::*;

use crate::errors::TaskDefinitionError;
use pyo3::types::PyTuple;
use rivers_core::task::BashCommand;

use crate::assets::io_handler::IOHandler;
use crate::composition::{
    InvokedNodeType, PyInvokedNodeOutput, extract_input_bindings, is_in_composition,
    observe_invocation,
};
use crate::partitions::mapping::PartitionMappingDict;

#[derive(Clone, Debug)]
pub struct PyBashCommand(pub BashCommand);

impl<'py> FromPyObject<'py, '_> for PyBashCommand {
    type Error = PyErr;

    fn extract(ob: pyo3::Borrowed<'py, '_, pyo3::PyAny>) -> PyResult<Self> {
        if let Ok(s) = ob.extract::<String>() {
            Ok(PyBashCommand(BashCommand::Shell(s)))
        } else if let Ok(v) = ob.extract::<Vec<String>>() {
            if v.is_empty() {
                return Err(TaskDefinitionError::new_err(
                    "command list must not be empty",
                ));
            }
            Ok(PyBashCommand(BashCommand::Exec(v)))
        } else {
            Err(pyo3::exceptions::PyTypeError::new_err(
                "command must be a str or list[str]",
            ))
        }
    }
}

impl<'py> IntoPyObject<'py> for PyBashCommand {
    type Target = pyo3::PyAny;
    type Output = Bound<'py, pyo3::PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        (&self).into_pyobject(py)
    }
}

impl<'py> IntoPyObject<'py> for &PyBashCommand {
    type Target = pyo3::PyAny;
    type Output = Bound<'py, pyo3::PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match &self.0 {
            BashCommand::Shell(s) => Ok(s.into_pyobject(py)?.into_any()),
            BashCommand::Exec(v) => Ok(v.into_pyobject(py)?.into_any()),
        }
    }
}

/// A task that executes a shell command, exposed to Python as `BashTask`.
#[pyclass(name = "BashTask", module = "rivers._core")]
pub struct PyBashTask {
    pub name: String,
    pub command: PyBashCommand,
    pub env: Option<HashMap<String, String>>,
    pub cwd: Option<String>,
    pub tags: Option<Vec<String>>,
    pub partition_mapping: Option<PartitionMappingDict>,
    /// IO handler for the task. Set to the shared `InMemoryIOHandler` by default during resolve.
    pub io_handler: Option<IOHandler>,
    pub retry: Option<rivers_core::execution::retry::RetryRef>,
}

#[pymethods]
impl PyBashTask {
    #[new]
    #[pyo3(signature = (name, command, env=None, cwd=None, tags=None, partition_mapping=None, io_handler=None, retry=None))]
    fn new(
        name: String,
        command: PyBashCommand,
        env: Option<HashMap<String, String>>,
        cwd: Option<String>,
        tags: Option<Vec<String>>,
        partition_mapping: Option<PartitionMappingDict>,
        io_handler: Option<IOHandler>,
        retry: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        Ok(Self {
            name,
            command,
            env,
            cwd,
            tags,
            partition_mapping,
            io_handler,
            retry: crate::retry::extract_retry_ref(retry)?,
        })
    }

    #[pyo3(signature = (*args, **kwargs))]
    fn __call__(
        &self,
        py: Python,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        if is_in_composition() {
            let input_bindings = extract_input_bindings(args, kwargs)?;
            let registered_name =
                observe_invocation(&self.name, InvokedNodeType::Task, input_bindings);
            let output = PyInvokedNodeOutput::with_default_output(registered_name);
            Ok(output.into_pyobject(py)?.into_any().unbind())
        } else {
            let result = rivers_core::task::execute_bash_command(
                &self.command.0,
                self.env.as_ref(),
                self.cwd.as_deref(),
            )
            .map_err(PyOSError::new_err)?;
            Ok(result.into_pyobject(py)?.into_any().unbind())
        }
    }

    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn tags(&self) -> Option<&Vec<String>> {
        self.tags.as_ref()
    }

    #[getter]
    fn command(&self) -> PyBashCommand {
        self.command.clone()
    }

    #[getter]
    fn env(&self) -> Option<&HashMap<String, String>> {
        self.env.as_ref()
    }

    #[getter]
    fn cwd(&self) -> Option<&str> {
        self.cwd.as_deref()
    }

    /// Support pickling for parallel executor transport.
    fn __reduce__(&self, py: Python) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let cls = py.import("rivers._core")?.getattr("BashTask")?.unbind();
        let args = PyTuple::new(
            py,
            &[
                (&self.name).into_pyobject(py)?.into_any().unbind(),
                (&self.command).into_pyobject(py)?.unbind(),
                self.env.as_ref().into_pyobject(py)?.into_any().unbind(),
                self.cwd.as_ref().into_pyobject(py)?.into_any().unbind(),
                self.tags.as_ref().into_pyobject(py)?.into_any().unbind(),
            ],
        )?;
        Ok((cls, args.unbind().into_any()))
    }
}
