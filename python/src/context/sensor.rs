//! SensorEvaluationContext — passed to sensor evaluation functions.
use std::sync::OnceLock;

use pyo3::prelude::*;

/// Context passed to sensor evaluation functions.
#[pyclass(name = "SensorEvaluationContext", frozen, module = "rivers._core")]
pub struct PySensorEvaluationContext {
    #[pyo3(get)]
    pub sensor_name: String,
    /// Persistent cursor from the last evaluation (user-managed state).
    #[pyo3(get)]
    pub cursor: Option<String>,
    /// Unix timestamp of the last tick completion, if any.
    #[pyo3(get)]
    pub last_tick_time: Option<f64>,
    /// Pydantic config instance (set via `SensorEvaluationContext[Config]` generic).
    pub config_instance: Option<Py<PyAny>>,
    _logger: OnceLock<Py<PyAny>>,
}

impl PySensorEvaluationContext {
    pub fn new(sensor_name: String, cursor: Option<String>, last_tick_time: Option<f64>) -> Self {
        Self {
            sensor_name,
            cursor,
            last_tick_time,
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
impl PySensorEvaluationContext {
    #[new]
    #[pyo3(signature = (sensor_name, cursor=None, last_tick_time=None, config=None))]
    fn py_new(
        sensor_name: String,
        cursor: Option<String>,
        last_tick_time: Option<f64>,
        config: Option<Py<PyAny>>,
    ) -> Self {
        Self::new(sensor_name, cursor, last_tick_time).with_config(config)
    }

    #[getter]
    fn config(&self, py: Python) -> Option<Py<PyAny>> {
        self.config_instance.as_ref().map(|c| c.clone_ref(py))
    }

    #[getter]
    fn log<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let logger = self._logger.get_or_init(|| {
            let logging = py.import("logging").expect("failed to import logging");
            let name = format!("code-repo.sensors.{}", self.sensor_name);
            logging
                .call_method1("getLogger", (name,))
                .expect("failed to get logger")
                .unbind()
        });
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

    fn __reduce__(
        slf: &Bound<'_, Self>,
    ) -> PyResult<(
        Py<PyAny>,
        (String, Option<String>, Option<f64>, Option<Py<PyAny>>),
    )> {
        let py = slf.py();
        let s = slf.borrow();
        Ok((
            slf.get_type().unbind().into_any(),
            (
                s.sensor_name.clone(),
                s.cursor.clone(),
                s.last_tick_time,
                s.config_instance.as_ref().map(|c| c.clone_ref(py)),
            ),
        ))
    }

    fn __repr__(&self) -> String {
        format!(
            "SensorEvaluationContext(sensor_name='{}', cursor={:?})",
            self.sensor_name, self.cursor
        )
    }
}
