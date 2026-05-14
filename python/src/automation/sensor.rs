//! Sensor definitions — polling-based triggers with cursor state.
use std::collections::HashMap;

use pyo3::PyTypeInfo;
use pyo3::prelude::*;

use super::PyEvalMode;
use super::schedule::{PyBackfillRequest, PyRunRequest, PySkipReason, TickRequest};
use crate::config::ResourceVariant;
use crate::context::sensor::PySensorEvaluationContext;
use crate::errors::{
    ConfigurationError, ExecutionError, ResultDefinitionError, SensorDefinitionError,
};

/// Default status for a sensor.
#[pyclass(
    name = "SensorStatus",
    frozen,
    eq,
    eq_int,
    from_py_object,
    module = "rivers._core"
)]
#[derive(Clone, Default, PartialEq)]
pub enum PySensorStatus {
    /// Sensor is active and will be evaluated by the daemon.
    #[default]
    Running,
    /// Sensor is paused and will not be evaluated.
    Stopped,
}

/// Accepts either a `str` or a `SkipReason` instance from Python.
#[derive(FromPyObject)]
enum SkipReasonInput {
    Message(String),
    Instance(Py<PySkipReason>),
}

impl SkipReasonInput {
    fn into_skip_reason(self, py: Python) -> PyResult<Py<PySkipReason>> {
        match self {
            Self::Message(s) => Py::new(py, PySkipReason { message: s }),
            Self::Instance(sr) => Ok(sr),
        }
    }
}

/// A comprehensive result object for sensor evaluations.
///
/// Allows returning run requests, skip reason, and cursor update atomically.
#[pyclass(name = "SensorResult", frozen, get_all, module = "rivers._core")]
pub struct PySensorResult {
    /// Requests to trigger — can contain RunRequest and/or BackfillRequest objects.
    /// Mutually exclusive with `skip_reason`.
    pub run_requests: Option<Vec<TickRequest>>,
    /// Reason the tick was skipped; mutually exclusive with `run_requests`.
    pub skip_reason: Option<Py<PySkipReason>>,
    /// Cursor value to persist for the next evaluation tick.
    pub cursor: Option<String>,
}

#[pymethods]
impl PySensorResult {
    /// Create a new sensor result with optional run requests, skip reason, and cursor.
    ///
    /// `run_requests` accepts a list of `RunRequest` and/or `BackfillRequest` objects.
    #[new]
    #[pyo3(signature = (run_requests=None, skip_reason=None, cursor=None))]
    fn new(
        py: Python,
        run_requests: Option<Vec<TickRequest>>,
        skip_reason: Option<SkipReasonInput>,
        cursor: Option<String>,
    ) -> PyResult<Self> {
        let skip_reason = skip_reason.map(|sr| sr.into_skip_reason(py)).transpose()?;

        if skip_reason.is_some()
            && run_requests
                .as_ref()
                .map(|r| !r.is_empty())
                .unwrap_or(false)
        {
            return Err(ResultDefinitionError::new_err(
                "SensorResult cannot have both run_requests and skip_reason",
            ));
        }

        Ok(Self {
            run_requests,
            skip_reason,
            cursor,
        })
    }

    fn __reduce__(
        slf: &Bound<'_, Self>,
    ) -> PyResult<(
        Py<PyAny>,
        (
            Option<Vec<Py<PyAny>>>,
            Option<Py<PySkipReason>>,
            Option<String>,
        ),
    )> {
        let py = slf.py();
        let s = slf.borrow();
        let skip = s.skip_reason.as_ref().map(|sr| sr.clone_ref(py));
        let reqs: Option<Vec<Py<PyAny>>> = s.run_requests.as_ref().map(|r| {
            r.iter()
                .map(|tr| match tr {
                    TickRequest::Run(r) => r.clone_ref(py).into_any(),
                    TickRequest::Backfill(b) => b.clone_ref(py).into_any(),
                })
                .collect()
        });
        Ok((
            slf.get_type().unbind().into_any(),
            (reqs, skip, s.cursor.clone()),
        ))
    }

    fn __repr__(&self) -> String {
        if self.skip_reason.is_some() {
            format!("SensorResult(skipped, cursor={:?})", self.cursor)
        } else {
            let count = self.run_requests.as_ref().map(|r| r.len()).unwrap_or(0);
            format!(
                "SensorResult(run_requests={}, cursor={:?})",
                count, self.cursor
            )
        }
    }
}

/// A sensor definition that evaluates external conditions and produces run requests.
///
/// Can be used as a decorator:
/// ```python
/// @rs.Sensor(job_name="my_job")
/// def my_sensor(context: rs.SensorEvaluationContext):
///     if something_changed():
///         return rs.RunRequest()
///     return rs.SkipReason("nothing new")
/// ```
#[pyclass(name = "Sensor", frozen, get_all, module = "rivers._core")]
pub struct PySensorDefinition {
    /// Sensor name (defaults to function name).
    pub(crate) name: String,
    /// Name of the job to trigger.
    pub(crate) job_name: Option<String>,
    /// Evaluation function called on each tick.
    pub(crate) evaluation_fn: Option<Py<PyAny>>,
    /// Minimum interval between evaluation ticks as a human-readable duration (e.g. `"30s"`).
    pub(crate) minimum_interval: Option<String>,
    /// Whether the sensor starts as running or stopped.
    pub(crate) default_status: PySensorStatus,
    /// Human-readable description.
    pub(crate) description: Option<String>,
    /// Tags for categorization.
    pub(crate) tags: Option<HashMap<String, String>>,
    /// Asset keys this sensor monitors.
    pub(crate) asset_selection: Option<Vec<String>>,
    /// Execution mode for the evaluation function.
    pub(crate) eval_mode: PyEvalMode,
    /// Timeout for evaluation as a human-readable duration (e.g. `"5m"`); `None` means no limit.
    pub(crate) eval_timeout: Option<String>,
}

#[pymethods]
impl PySensorDefinition {
    /// Create a new sensor definition. Can also be used as a decorator.
    #[new]
    #[pyo3(signature = (
        func = None,
        *,
        name = None,
        job_name = None,
        minimum_interval = None,
        default_status = PySensorStatus::default(),
        description = None,
        tags = None,
        asset_selection = None,
        eval_mode = PyEvalMode::Auto,
        eval_timeout = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python,
        func: Option<Py<PyAny>>,
        name: Option<String>,
        job_name: Option<String>,
        minimum_interval: Option<String>,
        default_status: PySensorStatus,
        description: Option<String>,
        tags: Option<HashMap<String, String>>,
        asset_selection: Option<Vec<String>>,
        eval_mode: PyEvalMode,
        eval_timeout: Option<String>,
    ) -> PyResult<Self> {
        let sensor_name = if let Some(n) = name {
            if n.is_empty() {
                return Err(SensorDefinitionError::new_err(
                    "sensor name cannot be empty",
                ));
            }
            n
        } else if let Some(ref f) = func {
            f.getattr(py, "__name__")?.to_string()
        } else {
            // Empty — will be set when used as decorator via __call__
            String::new()
        };
        Ok(Self {
            name: sensor_name,
            job_name,
            evaluation_fn: func,
            minimum_interval,
            default_status,
            description,
            tags,
            asset_selection,
            eval_mode,
            eval_timeout,
        })
    }

    /// When used as `@Sensor(job_name=...)` (without func),
    /// calling the returned object applies the decorated function.
    fn __call__(&self, py: Python, func: Py<PyAny>) -> PyResult<Self> {
        let sensor_name = if self.name.is_empty() {
            func.getattr(py, "__name__")?.to_string()
        } else {
            self.name.clone()
        };
        Ok(Self {
            name: sensor_name,
            job_name: self.job_name.clone(),
            evaluation_fn: Some(func),
            minimum_interval: self.minimum_interval.clone(),
            default_status: self.default_status.clone(),
            description: self.description.clone(),
            tags: self.tags.clone(),
            asset_selection: self.asset_selection.clone(),
            eval_mode: self.eval_mode.clone(),
            eval_timeout: self.eval_timeout.clone(),
        })
    }

    fn __repr__(&self) -> String {
        format!("Sensor(name='{}', job_name={:?})", self.name, self.job_name)
    }
}

/// Result of evaluating a sensor tick.
#[pyclass(name = "SensorTickResult", frozen, get_all, module = "rivers._core")]
pub struct PySensorTickResult {
    /// The sensor name.
    pub sensor_name: String,
    /// Requests generated by this tick (RunRequest and/or BackfillRequest).
    pub run_requests: Vec<TickRequest>,
    /// Skip reason if the tick was skipped.
    pub skip_reason: Option<Py<PySkipReason>>,
    /// Updated cursor value, if any.
    pub cursor: Option<String>,
}

#[pymethods]
impl PySensorTickResult {
    fn __repr__(&self) -> String {
        if self.skip_reason.is_some() {
            format!(
                "SensorTickResult(sensor='{}', skipped, cursor={:?})",
                self.sensor_name, self.cursor
            )
        } else {
            format!(
                "SensorTickResult(sensor='{}', run_requests={}, cursor={:?})",
                self.sensor_name,
                self.run_requests.len(),
                self.cursor
            )
        }
    }
}

/// Evaluate a sensor definition, calling its evaluation function.
pub(crate) fn evaluate_sensor(
    py: Python,
    sensor_def: &PySensorDefinition,
    cursor: Option<&str>,
    last_tick_time: Option<f64>,
    resources: &HashMap<String, ResourceVariant>,
) -> PyResult<PySensorTickResult> {
    let eval_fn = sensor_def
        .evaluation_fn
        .as_ref()
        .ok_or_else(|| ConfigurationError::new_err("Sensor has no evaluation function"))?;

    let annotations = crate::executor::ops::get_annotations(py, eval_fn)?;

    let ctx_type = PySensorEvaluationContext::type_object(py);
    let mut call_args: Vec<Py<PyAny>> = Vec::new();
    for (k, v) in annotations.iter() {
        let param_name: String = k.extract()?;
        if param_name == "return" {
            continue;
        }
        if crate::executor::ops::annotation_is(&v, ctx_type.as_any()) {
            let config_instance =
                crate::executor::ops::extract_config_from_annotation(py, &v, None)?;
            let ctx = Py::new(
                py,
                PySensorEvaluationContext::new(
                    sensor_def.name.clone(),
                    cursor.map(|s| s.to_string()),
                    last_tick_time,
                )
                .with_config(config_instance),
            )?;
            call_args.push(ctx.into_any());
        } else if let Some(resource) = resources.get(&param_name) {
            call_args.push(resource.inner().clone_ref(py));
        } else {
            return Err(SensorDefinitionError::new_err(format!(
                "Unknown parameter '{}' in sensor '{}': not a context or registered resource",
                param_name, sensor_def.name
            )));
        }
    }
    let args_tuple = pyo3::types::PyTuple::new(py, &call_args)?;
    let result = eval_fn.call1(py, args_tuple)?;
    let parsed = parse_sensor_result(py, &result, sensor_def.job_name.as_deref())?;
    let mut all_requests: Vec<TickRequest> = parsed
        .run_requests
        .into_iter()
        .map(TickRequest::Run)
        .collect();
    all_requests.extend(
        parsed
            .backfill_requests
            .into_iter()
            .map(TickRequest::Backfill),
    );
    Ok(PySensorTickResult {
        sensor_name: sensor_def.name.clone(),
        run_requests: all_requests,
        skip_reason: parsed.skip_reason,
        cursor: parsed.cursor,
    })
}

/// Parsed result from a sensor evaluation function.
pub(crate) struct SensorParseResult {
    pub run_requests: Vec<Py<PyRunRequest>>,
    pub backfill_requests: Vec<Py<PyBackfillRequest>>,
    pub skip_reason: Option<Py<PySkipReason>>,
    pub cursor: Option<String>,
}

/// Parse the result of a sensor evaluation function.
pub(crate) fn parse_sensor_result(
    py: Python,
    result: &Py<PyAny>,
    default_job_name: Option<&str>,
) -> PyResult<SensorParseResult> {
    let bound = result.bind(py);

    if bound.is_none() {
        let skip = Py::new(
            py,
            PySkipReason {
                message: "Eval fn didn't return a RunRequest, no reason provided".to_string(),
            },
        )?;
        return Ok(SensorParseResult {
            run_requests: vec![],
            backfill_requests: vec![],
            skip_reason: Some(skip),
            cursor: None,
        });
    }

    let apply_defaults = |req: &Bound<PyRunRequest>| -> PyResult<Py<PyRunRequest>> {
        let mut request = req.borrow().clone();
        if request.job_name.is_none() {
            request.job_name = default_job_name.map(|s| s.to_string());
        }
        Py::new(py, request)
    };

    if let Ok(sr) = bound.cast::<PySensorResult>() {
        let sr_ref = sr.borrow();
        let mut run_requests = Vec::new();
        let mut backfill_requests = Vec::new();
        if let Some(ref reqs) = sr_ref.run_requests {
            for tr in reqs {
                match tr {
                    TickRequest::Run(r) => {
                        let bound_req = r.bind(py);
                        run_requests.push(apply_defaults(bound_req)?);
                    }
                    TickRequest::Backfill(b) => {
                        backfill_requests.push(b.clone_ref(py));
                    }
                }
            }
        }
        let skip = sr_ref.skip_reason.as_ref().map(|s| s.clone_ref(py));
        let cursor = sr_ref.cursor.clone();
        return Ok(SensorParseResult {
            run_requests,
            backfill_requests,
            skip_reason: skip,
            cursor,
        });
    }

    if let Ok(skip) = bound.cast::<PySkipReason>() {
        return Ok(SensorParseResult {
            run_requests: vec![],
            backfill_requests: vec![],
            skip_reason: Some(skip.clone().unbind()),
            cursor: None,
        });
    }

    if let Ok(req) = bound.cast::<PyRunRequest>() {
        return Ok(SensorParseResult {
            run_requests: vec![apply_defaults(req)?],
            backfill_requests: vec![],
            skip_reason: None,
            cursor: None,
        });
    }

    if let Ok(req) = bound.cast::<PyBackfillRequest>() {
        return Ok(SensorParseResult {
            run_requests: vec![],
            backfill_requests: vec![req.clone().unbind()],
            skip_reason: None,
            cursor: None,
        });
    }

    if let Ok(iter) = bound.try_iter() {
        let mut run_requests = Vec::new();
        let mut backfill_requests = Vec::new();
        for item in iter {
            let item = item?;
            if let Ok(req) = item.cast::<PyRunRequest>() {
                run_requests.push(apply_defaults(req)?);
            } else if let Ok(req) = item.cast::<PyBackfillRequest>() {
                backfill_requests.push(req.clone().unbind());
            } else {
                return Err(ExecutionError::new_err(format!(
                    "Sensor evaluation function returned a list containing unsupported item: {}",
                    item.get_type().qualname()?
                )));
            }
        }
        return Ok(SensorParseResult {
            run_requests,
            backfill_requests,
            skip_reason: None,
            cursor: None,
        });
    }

    Err(ExecutionError::new_err(format!(
        "Sensor evaluation function must return RunRequest, BackfillRequest, SkipReason, \
         SensorResult, list, or None; got {}",
        bound.get_type().qualname()?
    )))
}

/// Register the `rivers._core.sensor` submodule (Sensor, SensorResult, SensorStatus, etc.).
pub fn register_sensor_module(parent_module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_submodule!(parent_module, "sensor", [
        PySensorEvaluationContext as "SensorEvaluationContext",
        PySensorStatus as "SensorStatus",
        PySensorResult as "SensorResult",
        PySensorDefinition as "Sensor",
        PySensorTickResult as "SensorTickResult",
    ])
}
