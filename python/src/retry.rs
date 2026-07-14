//! PyO3 wrappers for the declarative retry policy.

use std::time::Duration;

use pyo3::exceptions::{PyBaseException, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyType;
use rivers_core::execution::retry::{
    Backoff, ComputeEscalation, FailureReason, RetryOn, RetryPolicy, RetryRef,
};

use crate::errors::ConfigurationError;

// ── FailureReason ───────────────────────────────────────────────────────────

/// Why a step failed. Drives retry eligibility and is recorded on failure events.
#[pyclass(
    name = "FailureReason",
    eq,
    eq_int,
    from_py_object,
    module = "rivers._core"
)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PyFailureReason {
    #[pyo3(name = "ERROR")]
    Error,
    #[pyo3(name = "OUT_OF_MEMORY")]
    OutOfMemory,
    #[pyo3(name = "TIMEOUT")]
    Timeout,
    #[pyo3(name = "INFRASTRUCTURE")]
    Infrastructure,
    #[pyo3(name = "CANCELLED")]
    Cancelled,
}

impl From<PyFailureReason> for FailureReason {
    fn from(r: PyFailureReason) -> Self {
        match r {
            PyFailureReason::Error => FailureReason::Error,
            PyFailureReason::OutOfMemory => FailureReason::OutOfMemory,
            PyFailureReason::Timeout => FailureReason::Timeout,
            PyFailureReason::Infrastructure => FailureReason::Infrastructure,
            PyFailureReason::Cancelled => FailureReason::Cancelled,
        }
    }
}

impl From<FailureReason> for PyFailureReason {
    fn from(r: FailureReason) -> Self {
        match r {
            FailureReason::Error => PyFailureReason::Error,
            FailureReason::OutOfMemory => PyFailureReason::OutOfMemory,
            FailureReason::Timeout => PyFailureReason::Timeout,
            FailureReason::Infrastructure => PyFailureReason::Infrastructure,
            FailureReason::Cancelled => PyFailureReason::Cancelled,
        }
    }
}

// ── Backoff ─────────────────────────────────────────────────────────────────

fn to_duration(secs: f64) -> Duration {
    Duration::from_secs_f64(secs.max(0.0))
}

fn check_jitter(jitter: f64) -> PyResult<()> {
    if !(0.0..=1.0).contains(&jitter) {
        return Err(ConfigurationError::new_err(format!(
            "jitter must be between 0.0 and 1.0, got {jitter}"
        )));
    }
    Ok(())
}

/// A retry wait schedule. Built via the named constructors (`constant`,
/// `linear`, `exponential`, `fixed`); each takes a relative `jitter` (0..1) and
/// an optional `max_delay` ceiling (seconds).
#[pyclass(name = "Backoff", frozen, from_py_object, module = "rivers._core")]
#[derive(Clone, Debug)]
pub struct PyBackoff {
    pub(crate) inner: Backoff,
}

#[pymethods]
impl PyBackoff {
    #[staticmethod]
    #[pyo3(signature = (delay, *, jitter=0.0, max_delay=None))]
    fn constant(delay: f64, jitter: f64, max_delay: Option<f64>) -> PyResult<Self> {
        check_jitter(jitter)?;
        Ok(Self {
            inner: Backoff::constant(to_duration(delay), jitter, max_delay.map(to_duration)),
        })
    }

    #[staticmethod]
    #[pyo3(signature = (step, *, initial=0.0, jitter=0.0, max_delay=None))]
    fn linear(step: f64, initial: f64, jitter: f64, max_delay: Option<f64>) -> PyResult<Self> {
        check_jitter(jitter)?;
        Ok(Self {
            inner: Backoff::linear(
                to_duration(step),
                to_duration(initial),
                jitter,
                max_delay.map(to_duration),
            ),
        })
    }

    #[staticmethod]
    #[pyo3(signature = (initial, *, factor=2.0, jitter=0.0, max_delay=None))]
    fn exponential(
        initial: f64,
        factor: f64,
        jitter: f64,
        max_delay: Option<f64>,
    ) -> PyResult<Self> {
        check_jitter(jitter)?;
        if factor <= 0.0 {
            return Err(ConfigurationError::new_err(format!(
                "Backoff.exponential factor must be > 0, got {factor}"
            )));
        }
        Ok(Self {
            inner: Backoff::exponential(
                to_duration(initial),
                factor,
                jitter,
                max_delay.map(to_duration),
            ),
        })
    }

    #[staticmethod]
    #[pyo3(signature = (schedule, *, jitter=0.0))]
    fn fixed(schedule: Vec<f64>, jitter: f64) -> PyResult<Self> {
        check_jitter(jitter)?;
        if schedule.is_empty() {
            return Err(ConfigurationError::new_err(
                "Backoff.fixed requires a non-empty schedule",
            ));
        }
        Ok(Self {
            inner: Backoff::fixed(schedule.into_iter().map(to_duration).collect(), jitter),
        })
    }

    fn __repr__(&self) -> String {
        format!("Backoff({:?})", self.inner.shape)
    }
}

// ── RetryOn preset ──────────────────────────────────────────────────────────

/// Preset failure sets eligible for retry. `retry_on` also accepts an explicit
/// list of exception types / [`PyFailureReason`] (normalized in `RetryPolicy`).
#[pyclass(name = "RetryOn", eq, eq_int, from_py_object, module = "rivers._core")]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PyRetryOn {
    #[pyo3(name = "ALL")]
    All,
    #[pyo3(name = "TRANSIENT")]
    Transient,
}

/// Normalize the `retry_on` argument (a preset, or a sequence mixing exception
/// types and `FailureReason` members) into the core [`RetryOn`].
fn normalize_retry_on(val: Option<Bound<'_, PyAny>>) -> PyResult<RetryOn> {
    let Some(val) = val else {
        return Ok(RetryOn::All);
    };
    if let Ok(preset) = val.extract::<PyRetryOn>() {
        return Ok(match preset {
            PyRetryOn::All => RetryOn::All,
            PyRetryOn::Transient => RetryOn::Transient,
        });
    }
    let iter = val.try_iter().map_err(|_| {
        ConfigurationError::new_err(
            "retry_on must be RetryOn.ALL, RetryOn.TRANSIENT, or a list of \
             exception types / FailureReason values",
        )
    })?;
    let mut exceptions = Vec::new();
    let mut reasons = Vec::new();
    for item in iter {
        let item = item?;
        if let Ok(reason) = item.extract::<PyFailureReason>() {
            reasons.push(reason.into());
        } else if let Ok(ty) = item.cast::<PyType>() {
            if ty.is_subclass_of::<PyBaseException>()? {
                let module: String = ty.getattr("__module__")?.extract()?;
                let qualname: String = ty.getattr("__qualname__")?.extract()?;
                exceptions.push(format!("{module}.{qualname}"));
            } else {
                return Err(PyTypeError::new_err(format!(
                    "retry_on exception type must subclass BaseException: {}",
                    ty.name()?
                )));
            }
        } else {
            return Err(PyTypeError::new_err(
                "retry_on elements must be exception types or FailureReason values",
            ));
        }
    }
    Ok(RetryOn::Match {
        exceptions,
        reasons,
    })
}

// ── ComputeEscalation ───────────────────────────────────────────────────────

/// Grow a step's compute on OOM retries, up to a required ceiling. Effective
/// only on executors that provision compute per step (Kubernetes).
#[pyclass(
    name = "ComputeEscalation",
    frozen,
    from_py_object,
    module = "rivers._core"
)]
#[derive(Clone, Debug)]
pub struct PyComputeEscalation {
    pub(crate) inner: ComputeEscalation,
}

#[pymethods]
impl PyComputeEscalation {
    #[new]
    #[pyo3(signature = (*, max_memory, factor=2.0, cpu_factor=None, max_cpu=None, on=None))]
    fn new(
        max_memory: String,
        factor: f64,
        cpu_factor: Option<f64>,
        max_cpu: Option<String>,
        on: Option<Vec<PyFailureReason>>,
    ) -> PyResult<Self> {
        if factor <= 0.0 {
            return Err(ConfigurationError::new_err(format!(
                "ComputeEscalation factor must be > 0, got {factor}"
            )));
        }
        if let Some(cf) = cpu_factor {
            if cf <= 0.0 {
                return Err(ConfigurationError::new_err(format!(
                    "ComputeEscalation cpu_factor must be > 0, got {cf}"
                )));
            }
        }
        let on = on
            .map(|v| {
                v.into_iter()
                    .map(Into::into)
                    .collect::<Vec<FailureReason>>()
            })
            .unwrap_or_else(|| vec![FailureReason::OutOfMemory]);
        Ok(Self {
            inner: ComputeEscalation {
                factor,
                max_memory,
                cpu_factor,
                max_cpu,
                on,
            },
        })
    }

    #[getter]
    fn factor(&self) -> f64 {
        self.inner.factor
    }

    #[getter]
    fn max_memory(&self) -> String {
        self.inner.max_memory.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "ComputeEscalation(factor={}, max_memory='{}')",
            self.inner.factor, self.inner.max_memory
        )
    }
}

// ── RetryPolicy ─────────────────────────────────────────────────────────────

/// A declarative retry policy attached to an asset or job.
#[pyclass(name = "RetryPolicy", frozen, from_py_object, module = "rivers._core")]
#[derive(Clone, Debug)]
pub struct PyRetryPolicy {
    pub(crate) inner: RetryPolicy,
}

#[pymethods]
impl PyRetryPolicy {
    #[new]
    #[pyo3(signature = (max_retries=1, *, backoff=None, retry_on=None, escalate=None))]
    fn new(
        max_retries: u32,
        backoff: Option<PyBackoff>,
        retry_on: Option<Bound<'_, PyAny>>,
        escalate: Option<PyComputeEscalation>,
    ) -> PyResult<Self> {
        let retry_on = normalize_retry_on(retry_on)?;
        Ok(Self {
            inner: RetryPolicy {
                max_retries,
                backoff: backoff.map(|b| b.inner),
                retry_on,
                escalate: escalate.map(|e| e.inner),
            },
        })
    }

    #[getter]
    fn max_retries(&self) -> u32 {
        self.inner.max_retries
    }

    fn __repr__(&self) -> String {
        format!("RetryPolicy(max_retries={})", self.inner.max_retries)
    }
}

/// Extract a `retry=RetryPolicy | str | None` argument into a [`RetryRef`]. A
/// string is a name resolved against the repository `retries` registry at
/// `resolve()`; a `RetryPolicy` is inlined.
pub(crate) fn extract_retry_ref(val: Option<Bound<'_, PyAny>>) -> PyResult<Option<RetryRef>> {
    let Some(val) = val else {
        return Ok(None);
    };
    if let Ok(policy) = val.extract::<PyRetryPolicy>() {
        return Ok(Some(RetryRef::Inline(policy.inner)));
    }
    if let Ok(name) = val.extract::<String>() {
        return Ok(Some(RetryRef::Named(name)));
    }
    Err(PyValueError::new_err(
        "retry must be a RetryPolicy or a str naming a registered policy",
    ))
}
