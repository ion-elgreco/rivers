//! Parallel executor — runs independent steps concurrently via loky process pool.
mod collect_specs;
mod execute;
mod loky;
pub(crate) mod worker;
pub(crate) mod worker_args;

use pyo3::prelude::*;

use crate::errors::ExecutionError;
use crate::repository::resolved_node::ResolvedNode;

const IN_MEMORY_IO_ERROR: &str = "uses InMemoryIOHandler which cannot work with parallel \
     execution. InMemoryIOHandler stores data in process-local memory that is not \
     accessible from subprocesses. Use PickleIOHandler or DeltaIOHandler instead, or switch \
     to Executor.in_process().";

/// Validate that a step's IO handler is not InMemoryIOHandler (which can't cross process boundaries).
/// Catches both: no handler (default fallback) and explicit InMemoryIOHandler instances.
/// Only called for steps that will actually run in subprocesses (parallel path).
fn validate_not_in_memory_io(py: Python, step_name: &str, node: &ResolvedNode) -> PyResult<()> {
    match node.io_handler(py) {
        None => Err(ExecutionError::new_err(format!(
            "Asset '{step_name}' {IN_MEMORY_IO_ERROR}"
        ))),
        Some(handler) => {
            let memory_cls = py
                .import("rivers.io_handlers.memory")?
                .getattr("InMemoryIOHandler")?;
            if handler.bind(py).is_instance(&memory_cls)? {
                return Err(ExecutionError::new_err(format!(
                    "Asset '{step_name}' {IN_MEMORY_IO_ERROR}"
                )));
            }
            Ok(())
        }
    }
}

pub(crate) use execute::ParallelBackend;
