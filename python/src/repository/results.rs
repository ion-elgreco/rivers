use std::sync::Arc;

use pyo3::prelude::*;

use crate::errors::ExecutionError;
use crate::executor::ops::now_ts;
use crate::partitions::PyPartitionKey;
use crate::runtime::{io_rt, rt};
use crate::storage::{DetachOnClose, PyLaunchedBy};
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{RunStatus, StorageBackend};

#[pyclass(name = "RunResult", frozen, get_all, module = "rivers._core")]
pub struct PyRunResult {
    pub success: bool,
    pub run_id: String,
    pub materialized_assets: Vec<String>,
    pub failed_assets: Vec<(String, String)>,
}

#[pymethods]
impl PyRunResult {
    fn __repr__(&self) -> String {
        format!(
            "RunResult(success={}, run_id='{}', materialized={}, failed={})",
            self.success,
            self.run_id,
            self.materialized_assets.len(),
            self.failed_assets.len(),
        )
    }
}

#[pyclass(name = "RunHandle", frozen, module = "rivers._core")]
pub struct PyRunHandle {
    #[pyo3(get)]
    pub(crate) run_id: String,
    pub(super) storage: DetachOnClose<Arc<SurrealStorage>>,
}

#[pymethods]
impl PyRunHandle {
    #[getter]
    fn status(&self, py: Python) -> PyResult<String> {
        py.detach(|| {
            let run = rt()
                .block_on(self.storage.get_run(&self.run_id))
                .map_err(|e| ExecutionError::new_err(format!("Failed to get run status: {e}")))?
                .ok_or_else(|| {
                    ExecutionError::new_err(format!("Run '{}' not found", self.run_id))
                })?;
            Ok(format!("{:?}", run.status))
        })
    }

    /// Block until the run reaches a terminal state. Raises TimeoutError on timeout.
    #[pyo3(signature = (timeout=None))]
    fn wait(&self, py: Python, timeout: Option<f64>) -> PyResult<PyRunResult> {
        py.detach(|| {
            let start = std::time::Instant::now();
            let poll_interval = std::time::Duration::from_millis(100);

            loop {
                let run = rt()
                    .block_on(self.storage.get_run(&self.run_id))
                    .map_err(|e| ExecutionError::new_err(format!("Failed to poll run: {e}")))?
                    .ok_or_else(|| {
                        ExecutionError::new_err(format!("Run '{}' not found", self.run_id))
                    })?;

                match run.status {
                    RunStatus::Success | RunStatus::Failure | RunStatus::Canceled => {
                        return Ok(PyRunResult {
                            success: run.status == RunStatus::Success,
                            run_id: run.run_id,
                            materialized_assets: run.node_names,
                            failed_assets: vec![],
                        });
                    }
                    _ => {}
                }

                if let Some(t) = timeout
                    && start.elapsed().as_secs_f64() >= t
                {
                    return Err(pyo3::exceptions::PyTimeoutError::new_err(format!(
                        "Timed out waiting for run '{}' after {t}s",
                        self.run_id
                    )));
                }

                std::thread::sleep(poll_interval);
            }
        })
    }

    fn cancel(&self, py: Python) -> PyResult<()> {
        py.detach(|| {
            let run = rt()
                .block_on(self.storage.get_run(&self.run_id))
                .map_err(|e| ExecutionError::new_err(format!("Failed to get run: {e}")))?
                .ok_or_else(|| {
                    ExecutionError::new_err(format!("Run '{}' not found", self.run_id))
                })?;

            match run.status {
                RunStatus::Success | RunStatus::Failure | RunStatus::Canceled => return Ok(()),
                _ => {}
            }

            io_rt()
                .block_on(self.storage.update_run_status(
                    &self.run_id,
                    RunStatus::Canceled,
                    Some(now_ts()),
                ))
                .map_err(|e| ExecutionError::new_err(format!("Failed to cancel run: {e}")))?;
            Ok(())
        })
    }

    fn __repr__(&self) -> String {
        format!("RunHandle(run_id='{}')", self.run_id)
    }
}

#[pyclass(name = "BackfillResult", frozen, get_all, module = "rivers._core")]
pub struct PyBackfillResult {
    pub backfill_id: String,
    pub num_partitions: usize,
    pub num_runs: usize,
    pub status: String,
    pub completed: usize,
    pub failed: usize,
    pub canceled: usize,
    pub run_ids: Vec<String>,
    pub is_dry_run: bool,
    pub partition_keys: Vec<PyPartitionKey>,
}

#[pymethods]
impl PyBackfillResult {
    fn __repr__(&self) -> String {
        if self.is_dry_run {
            format!(
                "BackfillResult(dry_run, partitions={}, runs={})",
                self.num_partitions, self.num_runs,
            )
        } else {
            format!(
                "BackfillResult(id='{}', status='{}', completed={}, failed={}, canceled={})",
                self.backfill_id, self.status, self.completed, self.failed, self.canceled,
            )
        }
    }
}

#[pyclass(name = "BackfillStatus", frozen, get_all, module = "rivers._core")]
pub struct PyBackfillStatusResult {
    pub backfill_id: String,
    pub status: String,
    pub total_partitions: usize,
    pub completed_partitions: usize,
    pub failed_partitions: usize,
    pub canceled_partitions: usize,
    pub run_ids: Vec<String>,
    pub error: Option<String>,
    pub tags: Vec<(String, String)>,
    pub launched_by: PyLaunchedBy,
    /// The verb child runs execute. `None` means materialize.
    pub action: Option<String>,
}

#[pymethods]
impl PyBackfillStatusResult {
    fn __repr__(&self) -> String {
        format!(
            "BackfillStatus(id='{}', status='{}', completed={}/{}, failed={})",
            self.backfill_id,
            self.status,
            self.completed_partitions,
            self.total_partitions,
            self.failed_partitions,
        )
    }
}
