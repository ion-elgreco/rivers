//! Loky process pool interface — manages the Python loky executor for parallel steps.
//!
//! Acquires a reusable `loky.get_reusable_executor()` process pool. Step
//! submission and result collection happen inline in `parallel/execute.rs`.
use pyo3::prelude::*;

use super::execute::ParallelBackend;

/// The one loky acquisition site: returns the live pool when healthy and
/// respawns it when broken, so per-attempt callers self-heal after a worker
/// death.
pub(super) fn acquire_reusable_executor<'py>(
    py: Python<'py>,
    max_workers: usize,
) -> PyResult<Bound<'py, PyAny>> {
    let loky = py.import("loky")?;
    loky.call_method1("get_reusable_executor", (max_workers,))
}

impl ParallelBackend {
    pub(super) fn get_loky_executor<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        acquire_reusable_executor(py, self.max_workers)
    }
}
