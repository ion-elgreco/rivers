//! Schedule definitions — cron-based triggers that produce run requests and backfill requests.
use std::collections::HashMap;

use pyo3::PyTypeInfo;
use pyo3::prelude::*;

use super::PyEvalMode;
use crate::config::ResourceVariant;
use crate::context::schedule::PyScheduleEvaluationContext;
use crate::errors::{ExecutionError, ScheduleDefinitionError};
use crate::partitions::PyPartitionKey;
use crate::partitions::backfill_strategy::PyBackfillStrategy;
use crate::partitions::key_range::PyPartitionKeyRange;

/// A request emitted by a schedule or sensor — either a run or a backfill.
pub enum TickRequest {
    Run(Py<PyRunRequest>),
    Backfill(Py<PyBackfillRequest>),
}

impl Clone for TickRequest {
    fn clone(&self) -> Self {
        Python::try_attach(|py| match self {
            Self::Run(r) => Self::Run(r.clone_ref(py)),
            Self::Backfill(b) => Self::Backfill(b.clone_ref(py)),
        })
        .expect("Python interpreter not available for TickRequest::clone")
    }
}

impl TickRequest {
    pub fn as_run(&self) -> Option<&Py<PyRunRequest>> {
        match self {
            Self::Run(r) => Some(r),
            _ => None,
        }
    }

    pub fn as_backfill(&self) -> Option<&Py<PyBackfillRequest>> {
        match self {
            Self::Backfill(b) => Some(b),
            _ => None,
        }
    }
}

impl<'py> pyo3::IntoPyObject<'py> for TickRequest {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Self::Run(r) => Ok(r.into_bound(py).into_any()),
            Self::Backfill(b) => Ok(b.into_bound(py).into_any()),
        }
    }
}

#[allow(deprecated)] // downcast on Borrowed — cast() doesn't support clone().unbind()
impl<'py> pyo3::FromPyObject<'py, '_> for TickRequest {
    type Error = PyErr;

    fn extract(ob: pyo3::Borrowed<'py, '_, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(r) = ob.downcast::<PyRunRequest>() {
            Ok(Self::Run(r.clone().unbind()))
        } else if let Ok(b) = ob.downcast::<PyBackfillRequest>() {
            Ok(Self::Backfill(b.clone().unbind()))
        } else {
            Err(pyo3::exceptions::PyTypeError::new_err(format!(
                "expected RunRequest or BackfillRequest, got {}",
                ob.get_type().qualname()?
            )))
        }
    }
}

/// A request to launch a run, returned from schedule/sensor evaluation functions.
#[pyclass(
    name = "RunRequest",
    frozen,
    get_all,
    from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PyRunRequest {
    /// Optional dedup key. If None, a new run is always created.
    pub run_key: Option<String>,
    /// Tags to attach to the run.
    pub tags: Option<HashMap<String, String>>,
    /// Partition key to execute.
    pub partition_key: Option<String>,
    /// Job name to target (for multi-job schedules/sensors).
    pub job_name: Option<String>,
}

#[pymethods]
impl PyRunRequest {
    /// Create a new run request.
    #[new]
    #[pyo3(signature = (run_key=None, tags=None, partition_key=None, job_name=None))]
    fn new(
        run_key: Option<String>,
        tags: Option<HashMap<String, String>>,
        partition_key: Option<String>,
        job_name: Option<String>,
    ) -> Self {
        Self {
            run_key,
            tags,
            partition_key,
            job_name,
        }
    }

    fn __reduce__(
        slf: &Bound<'_, Self>,
    ) -> (
        Py<PyAny>,
        (
            Option<String>,
            Option<HashMap<String, String>>,
            Option<String>,
            Option<String>,
        ),
    ) {
        let s = slf.borrow();
        (
            slf.get_type().unbind().into_any(),
            (
                s.run_key.clone(),
                s.tags.clone(),
                s.partition_key.clone(),
                s.job_name.clone(),
            ),
        )
    }

    fn __repr__(&self) -> String {
        format!(
            "RunRequest(run_key={:?}, tags={:?}, partition_key={:?}, job_name={:?})",
            self.run_key, self.tags, self.partition_key, self.job_name
        )
    }
}

/// A request to launch a backfill, returned from schedule/sensor evaluation functions.
#[pyclass(
    name = "BackfillRequest",
    frozen,
    get_all,
    from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PyBackfillRequest {
    /// Asset selection to backfill.
    pub selection: Vec<String>,
    /// Explicit partition keys to backfill.
    pub partition_keys: Option<Vec<PyPartitionKey>>,
    /// Partition range — resolved at execution time.
    pub partition_range: Option<PyPartitionKeyRange>,
    /// Backfill strategy override. None = use asset defaults.
    pub strategy: Option<PyBackfillStrategy>,
    /// Failure policy. None = "continue".
    pub failure_policy: Option<String>,
    /// Max concurrent partition runs.
    pub max_concurrency: u32,
    /// Tags to attach to the backfill and all its runs.
    pub tags: Option<HashMap<String, String>>,
}

#[pymethods]
impl PyBackfillRequest {
    #[new]
    #[pyo3(signature = (
        selection,
        partition_keys = None,
        partition_range = None,
        strategy = None,
        failure_policy = None,
        max_concurrency = 4,
        tags = None,
    ))]
    fn new(
        selection: Vec<String>,
        partition_keys: Option<Vec<PyPartitionKey>>,
        partition_range: Option<PyPartitionKeyRange>,
        strategy: Option<PyBackfillStrategy>,
        failure_policy: Option<String>,
        max_concurrency: u32,
        tags: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            selection,
            partition_keys,
            partition_range,
            strategy,
            failure_policy,
            max_concurrency,
            tags,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "BackfillRequest(selection={:?}, partitions={}, max_concurrency={})",
            self.selection,
            self.partition_keys
                .as_ref()
                .map(|k| k.len().to_string())
                .unwrap_or_else(|| "range".to_string()),
            self.max_concurrency,
        )
    }

    fn __reduce__(
        slf: &Bound<'_, Self>,
    ) -> (
        Py<PyAny>,
        (
            Vec<String>,
            Option<Vec<PyPartitionKey>>,
            Option<PyPartitionKeyRange>,
            Option<PyBackfillStrategy>,
            Option<String>,
            u32,
            Option<HashMap<String, String>>,
        ),
    ) {
        let s = slf.borrow();
        (
            slf.get_type().unbind().into_any(),
            (
                s.selection.clone(),
                s.partition_keys.clone(),
                s.partition_range.clone(),
                s.strategy.clone(),
                s.failure_policy.clone(),
                s.max_concurrency,
                s.tags.clone(),
            ),
        )
    }
}

/// Reason to skip a schedule tick.
#[pyclass(
    name = "SkipReason",
    frozen,
    get_all,
    from_py_object,
    module = "rivers._core"
)]
#[derive(Clone)]
pub struct PySkipReason {
    pub message: String,
}

#[pymethods]
impl PySkipReason {
    /// Create a new skip reason with an optional message.
    #[new]
    #[pyo3(signature = (message=""))]
    fn new(message: &str) -> Self {
        Self {
            message: message.to_string(),
        }
    }

    fn __reduce__(slf: &Bound<'_, Self>) -> (Py<PyAny>, (String,)) {
        let s = slf.borrow();
        (slf.get_type().unbind().into_any(), (s.message.clone(),))
    }

    fn __repr__(&self) -> String {
        format!("SkipReason('{}')", self.message)
    }
}

/// Default status for a schedule.
#[pyclass(
    name = "ScheduleStatus",
    frozen,
    eq,
    eq_int,
    from_py_object,
    module = "rivers._core"
)]
#[derive(Clone, Default, PartialEq)]
pub enum PyScheduleStatus {
    /// Schedule is active and will be evaluated by the daemon.
    #[default]
    Running,
    /// Schedule is paused and will not be evaluated.
    Stopped,
}

/// A schedule definition that triggers job execution on a cron schedule.
///
/// Can be used as a decorator:
/// ```python
/// @rs.Schedule(cron_schedule="0 0 * * *", job_name="my_job")
/// def my_schedule(context: rs.ScheduleEvaluationContext):
///     return rs.RunRequest()
/// ```
#[pyclass(name = "Schedule", frozen, get_all, module = "rivers._core")]
pub struct PyScheduleDefinition {
    /// Schedule name (defaults to function name or `{job_name}_schedule`).
    pub(crate) name: String,
    /// Cron expression controlling when the schedule fires.
    pub(crate) cron_schedule: String,
    /// Name of the job to trigger.
    pub(crate) job_name: String,
    /// Optional evaluation function called on each tick.
    pub(crate) evaluation_fn: Option<Py<PyAny>>,
    /// Whether the schedule starts as running or stopped.
    pub(crate) default_status: PyScheduleStatus,
    /// Timezone for cron evaluation (e.g. `"US/Eastern"`).
    pub(crate) timezone: Option<String>,
    /// Tags applied to triggered runs.
    pub(crate) tags: Option<HashMap<String, String>>,
    /// Human-readable description.
    pub(crate) description: Option<String>,
    /// Execution mode for the evaluation function.
    pub(crate) eval_mode: PyEvalMode,
    /// Timeout for evaluation as a human-readable duration (e.g. `"5m"`); `None` means no limit.
    pub(crate) eval_timeout: Option<String>,
}

#[pymethods]
impl PyScheduleDefinition {
    /// Create a new schedule definition. Can also be used as a decorator.
    #[new]
    #[pyo3(signature = (
        func = None,
        *,
        cron_schedule,
        job_name,
        name = None,
        default_status = PyScheduleStatus::default(),
        timezone = None,
        tags = None,
        description = None,
        eval_mode = PyEvalMode::Auto,
        eval_timeout = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python,
        func: Option<Py<PyAny>>,
        cron_schedule: String,
        job_name: String,
        name: Option<String>,
        default_status: PyScheduleStatus,
        timezone: Option<String>,
        tags: Option<HashMap<String, String>>,
        description: Option<String>,
        eval_mode: PyEvalMode,
        eval_timeout: Option<String>,
    ) -> PyResult<Self> {
        if cron_schedule.is_empty() {
            return Err(ScheduleDefinitionError::new_err(
                "cron_schedule cannot be empty",
            ));
        }
        if job_name.is_empty() {
            return Err(ScheduleDefinitionError::new_err("job_name cannot be empty"));
        }
        let schedule_name = if let Some(n) = name {
            n
        } else if let Some(ref f) = func {
            f.getattr(py, "__name__")?.to_string()
        } else {
            format!("{}_schedule", job_name)
        };
        Ok(Self {
            name: schedule_name,
            cron_schedule,
            job_name,
            evaluation_fn: func,
            default_status,
            timezone,
            tags,
            description,
            eval_mode,
            eval_timeout,
        })
    }

    /// When used as `@Schedule(cron_schedule=..., job_name=...)` (without func),
    /// calling the returned object applies the decorated function.
    fn __call__(&self, py: Python, func: Py<PyAny>) -> PyResult<Self> {
        let schedule_name = if self.name == format!("{}_schedule", self.job_name) {
            func.getattr(py, "__name__")?.to_string()
        } else {
            self.name.clone()
        };
        Ok(Self {
            name: schedule_name,
            cron_schedule: self.cron_schedule.clone(),
            job_name: self.job_name.clone(),
            evaluation_fn: Some(func),
            default_status: self.default_status.clone(),
            timezone: self.timezone.clone(),
            tags: self.tags.clone(),
            description: self.description.clone(),
            eval_mode: self.eval_mode.clone(),
            eval_timeout: self.eval_timeout.clone(),
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "Schedule(name='{}', cron='{}', job='{}')",
            self.name, self.cron_schedule, self.job_name
        )
    }
}

/// Result of evaluating a schedule tick.
#[pyclass(name = "ScheduleTickResult", frozen, get_all, module = "rivers._core")]
pub struct PyScheduleTickResult {
    /// The schedule name.
    pub schedule_name: String,
    /// Requests generated by this tick (RunRequest and/or BackfillRequest).
    pub run_requests: Vec<TickRequest>,
    /// Skip reason if the tick was skipped.
    pub skip_reason: Option<Py<PySkipReason>>,
}

#[pymethods]
impl PyScheduleTickResult {
    fn __repr__(&self) -> String {
        if self.skip_reason.is_some() {
            format!(
                "ScheduleTickResult(schedule='{}', skipped)",
                self.schedule_name
            )
        } else {
            format!(
                "ScheduleTickResult(schedule='{}', run_requests={})",
                self.schedule_name,
                self.run_requests.len()
            )
        }
    }
}

/// Evaluate a schedule definition, calling its evaluation function or creating a default RunRequest.
pub(crate) fn evaluate_schedule(
    py: Python,
    schedule: &PyScheduleDefinition,
    execution_time: &str,
    resources: &HashMap<String, ResourceVariant>,
) -> PyResult<PyScheduleTickResult> {
    let parsed = if let Some(ref eval_fn) = schedule.evaluation_fn {
        let annotations = crate::executor::ops::get_annotations(py, eval_fn)?;

        let ctx_type = PyScheduleEvaluationContext::type_object(py);
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
                    PyScheduleEvaluationContext::new(
                        execution_time.to_string(),
                        schedule.name.clone(),
                    )
                    .with_config(config_instance),
                )?;
                call_args.push(ctx.into_any());
            } else if let Some(resource) = resources.get(&param_name) {
                call_args.push(resource.inner().clone_ref(py));
            } else {
                return Err(ScheduleDefinitionError::new_err(format!(
                    "Unknown parameter '{}' in schedule '{}': not a context or registered resource",
                    param_name, schedule.name
                )));
            }
        }
        let args_tuple = pyo3::types::PyTuple::new(py, &call_args)?;
        let result = eval_fn.call1(py, args_tuple)?;
        parse_schedule_result(py, &result, &schedule.job_name, &schedule.tags)?
    } else {
        let req = PyRunRequest {
            run_key: None,
            tags: schedule.tags.clone(),
            partition_key: None,
            job_name: Some(schedule.job_name.clone()),
        };
        ScheduleParseResult {
            run_requests: vec![Py::new(py, req)?],
            backfill_requests: vec![],
            skip_reason: None,
        }
    };

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
    Ok(PyScheduleTickResult {
        schedule_name: schedule.name.clone(),
        run_requests: all_requests,
        skip_reason: parsed.skip_reason,
    })
}

/// Parsed result from a schedule evaluation function.
pub(crate) struct ScheduleParseResult {
    pub run_requests: Vec<Py<PyRunRequest>>,
    pub backfill_requests: Vec<Py<PyBackfillRequest>>,
    pub skip_reason: Option<Py<PySkipReason>>,
}

/// Parse the result of a schedule evaluation function.
pub(crate) fn parse_schedule_result(
    py: Python,
    result: &Py<PyAny>,
    default_job_name: &str,
    default_tags: &Option<HashMap<String, String>>,
) -> PyResult<ScheduleParseResult> {
    let bound = result.bind(py);

    if bound.is_none() {
        let skip = Py::new(
            py,
            PySkipReason::new("Eval fn didn't return a RunRequest, no reason provided"),
        )?;
        return Ok(ScheduleParseResult {
            run_requests: vec![],
            backfill_requests: vec![],
            skip_reason: Some(skip),
        });
    }

    if let Ok(skip) = bound.cast::<PySkipReason>() {
        return Ok(ScheduleParseResult {
            run_requests: vec![],
            backfill_requests: vec![],
            skip_reason: Some(skip.clone().unbind()),
        });
    }

    let apply_defaults = |req: &Bound<PyRunRequest>| -> PyResult<Py<PyRunRequest>> {
        let mut request = req.borrow().clone();
        if request.job_name.is_none() {
            request.job_name = Some(default_job_name.to_string());
        }
        if request.tags.is_none() {
            request.tags = default_tags.clone();
        }
        Py::new(py, request)
    };

    if let Ok(req) = bound.cast::<PyRunRequest>() {
        return Ok(ScheduleParseResult {
            run_requests: vec![apply_defaults(req)?],
            backfill_requests: vec![],
            skip_reason: None,
        });
    }

    if let Ok(req) = bound.cast::<PyBackfillRequest>() {
        return Ok(ScheduleParseResult {
            run_requests: vec![],
            backfill_requests: vec![req.clone().unbind()],
            skip_reason: None,
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
                    "Schedule evaluation function returned a list containing unsupported item: {}",
                    item.get_type().qualname()?
                )));
            }
        }
        return Ok(ScheduleParseResult {
            run_requests,
            backfill_requests,
            skip_reason: None,
        });
    }

    Err(ExecutionError::new_err(format!(
        "Schedule evaluation function must return RunRequest, BackfillRequest, SkipReason, list, or None; got {}",
        bound.get_type().qualname()?
    )))
}

/// Register the `rivers._core.schedule` submodule (RunRequest, SkipReason, Schedule, etc.).
pub fn register_schedule_module(parent_module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_submodule!(parent_module, "schedule", [
        PyRunRequest as "RunRequest",
        PyBackfillRequest as "BackfillRequest",
        PySkipReason as "SkipReason",
        PyScheduleEvaluationContext as "ScheduleEvaluationContext",
        PyScheduleDefinition as "Schedule",
        PyScheduleStatus as "ScheduleStatus",
        PyScheduleTickResult as "ScheduleTickResult",
    ], [
        PyEvalMode,
    ])
}
