//! ScheduleEvaluationContext — passed to schedule evaluation functions.
use std::sync::OnceLock;

use pyo3::prelude::*;
use pyo3::types::PyType;

/// Context passed to schedule evaluation functions.
#[pyclass(name = "ScheduleEvaluationContext", frozen, module = "rivers._core")]
pub struct PyScheduleEvaluationContext {
    /// When this tick was scheduled to fire (ISO 8601 string).
    #[pyo3(get)]
    pub scheduled_execution_time: String,
    #[pyo3(get)]
    pub schedule_name: String,
    /// Pydantic config instance (set via `ScheduleEvaluationContext[Config]` generic).
    pub config_instance: Option<Py<PyAny>>,
    _logger: OnceLock<Py<PyAny>>,
}

impl PyScheduleEvaluationContext {
    pub fn new(scheduled_execution_time: String, schedule_name: String) -> Self {
        Self {
            scheduled_execution_time,
            schedule_name,
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
impl PyScheduleEvaluationContext {
    #[new]
    #[pyo3(signature = (scheduled_execution_time, schedule_name, config=None))]
    fn py_new(
        scheduled_execution_time: String,
        schedule_name: String,
        config: Option<Py<PyAny>>,
    ) -> Self {
        Self::new(scheduled_execution_time, schedule_name).with_config(config)
    }

    #[getter]
    fn config(&self) -> Option<&Py<PyAny>> {
        self.config_instance.as_ref()
    }

    #[getter]
    fn log<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let logger = self._logger.get_or_init(|| {
            let logging = py.import("logging").expect("failed to import logging");
            let name = format!("code-repo.schedules.{}", self.schedule_name);
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
        let args = pyo3::types::PyTuple::new(py, std::slice::from_ref(item))?;
        generic_alias.call1((cls, args)).map(|v| v.unbind())
    }

    fn __reduce__(
        slf: &Bound<'_, Self>,
    ) -> PyResult<(Py<PyAny>, (String, String, Option<Py<PyAny>>))> {
        let py = slf.py();
        let s = slf.borrow();
        Ok((
            slf.get_type().unbind().into_any(),
            (
                s.scheduled_execution_time.clone(),
                s.schedule_name.clone(),
                s.config_instance.as_ref().map(|c| c.clone_ref(py)),
            ),
        ))
    }

    fn __repr__(&self) -> String {
        format!(
            "ScheduleEvaluationContext(schedule_name='{}', scheduled_execution_time='{}')",
            self.schedule_name, self.scheduled_execution_time
        )
    }
}
