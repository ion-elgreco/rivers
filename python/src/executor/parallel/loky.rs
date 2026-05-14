//! Loky process pool interface — manages the Python loky executor for parallel steps.
//!
//! Acquires a reusable `loky.get_reusable_executor()` process pool. Step
//! submission and result collection happen inline in `parallel/execute.rs`.
use pyo3::prelude::*;

use super::execute::ParallelBackend;

impl ParallelBackend {
    pub(super) fn get_loky_executor<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let loky = py.import("loky")?;
        loky.call_method1("get_reusable_executor", (self.max_workers,))
    }
}
