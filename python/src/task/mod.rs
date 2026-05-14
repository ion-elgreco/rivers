//! Task types: Python tasks (@Task) and shell tasks (BashTask).
pub mod bash_task;
pub mod py_task;

pub use bash_task::PyBashTask;
pub use py_task::PyTask;

use crate::context::task::PyTaskExecutionContext;

use pyo3::prelude::*;

pub fn register_task_module(parent_module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_submodule!(parent_module, "tasks", [
        PyTask as "Task",
        PyBashTask as "BashTask",
        PyTaskExecutionContext as "TaskExecutionContext",
    ])
}
