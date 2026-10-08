//! RunStatusSensorContext — passed to run-status sensor callbacks, once per run.
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use rivers_core::execution::retry::meta;
use rivers_core::storage::{EventType, PartitionKey, RunRecord, StoredEvent};

use crate::partitions::PyPartitionKey;
use crate::storage::PyRunRecord;

/// One failed step of a run.
#[pyclass(
    name = "StepFailure",
    frozen,
    skip_from_py_object,
    module = "rivers._core"
)]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PyStepFailure {
    /// Asset (or task) whose step failed.
    #[pyo3(get)]
    pub asset_name: String,
    pub partition_key: Option<PartitionKey>,
    /// Error message recorded for the failure.
    #[pyo3(get)]
    pub error: String,
}

#[pymethods]
impl PyStepFailure {
    /// Partition the step ran for, if any.
    #[getter]
    fn partition_key(&self) -> Option<PyPartitionKey> {
        self.partition_key.as_ref().map(PyPartitionKey::from)
    }

    fn __repr__(&self) -> String {
        format!(
            "StepFailure(asset_name='{}', error={:?})",
            self.asset_name, self.error
        )
    }
}

/// A run that reached the watched status, with its failure detail. Built by
/// the daemon and sent as JSON to subprocess evals.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct RunStatusEvent {
    pub(crate) run: RunRecord,
    pub(crate) step_failures: Vec<PyStepFailure>,
    pub(crate) launch_error: Option<String>,
}

impl RunStatusEvent {
    /// `events` are the run's `StepFailure` / `RunLaunchFailed` events, oldest first.
    pub(crate) fn new(run: RunRecord, events: Vec<StoredEvent>) -> Self {
        let error_of = |e: &StoredEvent| {
            meta::value(&e.metadata, "error")
                .unwrap_or_default()
                .to_string()
        };
        // A failed step writes one StepFailure without a key and, when it ran
        // for partitions, one with the key. A keyed event alone is a partition
        // marked failed inside a step that did not fail.
        let twin = |of: &StoredEvent, keyed: bool| {
            events.iter().find(|e| {
                matches!(e.event_type, EventType::StepFailure)
                    && e.asset_key == of.asset_key
                    && e.partition_key.is_some() == keyed
            })
        };
        let mut step_failures = Vec::new();
        let mut launch_error = None;
        for event in &events {
            match (&event.event_type, &event.asset_key) {
                (EventType::RunLaunchFailed, _) => launch_error = Some(error_of(event)),
                (EventType::StepFailure, Some(asset)) => {
                    let partition_key = match &event.partition_key {
                        None => twin(event, true).and_then(|k| k.partition_key.clone()),
                        Some(_) if twin(event, false).is_some() => continue,
                        Some(key) => Some(key.clone()),
                    };
                    step_failures.push(PyStepFailure {
                        asset_name: asset.clone(),
                        partition_key,
                        error: error_of(event),
                    });
                }
                _ => {}
            }
        }
        Self {
            run,
            step_failures,
            launch_error,
        }
    }
}

/// Context passed to a run-status sensor callback for one run.
#[pyclass(name = "RunStatusSensorContext", frozen, module = "rivers._core")]
pub struct PyRunStatusSensorContext {
    #[pyo3(get)]
    pub sensor_name: String,
    /// The run that reached the watched status.
    #[pyo3(get)]
    pub run: Py<PyRunRecord>,
    /// Failed steps, oldest first. Empty unless the run failed.
    #[pyo3(get)]
    pub step_failures: Vec<PyStepFailure>,
    /// Why the run could not start, if it failed before any step ran.
    #[pyo3(get)]
    pub launch_error: Option<String>,
    config_instance: Option<Py<PyAny>>,
    _logger: PyOnceLock<Py<PyAny>>,
}

impl PyRunStatusSensorContext {
    pub(crate) fn new(
        py: Python,
        sensor_name: String,
        event: RunStatusEvent,
        config: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        Ok(Self {
            sensor_name,
            run: Py::new(py, PyRunRecord::from(event.run))?,
            step_failures: event.step_failures,
            launch_error: event.launch_error,
            config_instance: config,
            _logger: PyOnceLock::new(),
        })
    }
}

#[pymethods]
impl PyRunStatusSensorContext {
    #[getter]
    fn config(&self, py: Python) -> Option<Py<PyAny>> {
        self.config_instance.as_ref().map(|c| c.clone_ref(py))
    }

    #[getter]
    fn log<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let logger = self._logger.get_or_try_init(py, || {
            let logging = py.import("logging")?;
            let name = format!("code-repo.sensors.{}", self.sensor_name);
            logging
                .call_method1("getLogger", (name,))
                .map(Bound::unbind)
        })?;
        Ok(logger.bind(py).clone())
    }

    #[classmethod]
    fn __class_getitem__(
        cls: &Bound<'_, pyo3::types::PyType>,
        item: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let py = item.py();
        let types = py.import("types")?;
        let generic_alias = types.getattr("GenericAlias")?;
        let args = pyo3::types::PyTuple::new(py, std::slice::from_ref(item))?;
        generic_alias.call1((cls, args)).map(|v| v.unbind())
    }

    fn __repr__(&self, py: Python) -> String {
        let run = self.run.borrow(py);
        format!(
            "RunStatusSensorContext(sensor_name='{}', run_id='{}', status='{}')",
            self.sensor_name, run.run_id, run.status
        )
    }
}
