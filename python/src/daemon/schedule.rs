//! Schedule daemon loop — evaluates cron schedules and submits run requests.
use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;

use crate::automation::parse_schedule_result;
use crate::automation::schedule::PyScheduleStatus;
use crate::repository::PyCodeRepository;

use super::{
    BoxedPyFuture, GIL_SEMAPHORE, PrecomputedArgs, ResolvedEvalMode, RunRequestData, TickOutcome,
    assemble_call_args, extract_tick_outcome_from_parts, precompute_args, resolve_eval_mode,
};

pub(super) struct ScheduleInfo {
    pub(super) name: String,
    pub(super) cron_schedule: String,
    pub(super) job_name: String,
    pub(super) timezone: Option<String>,
    pub(super) tags: Option<HashMap<String, String>>,
    pub(super) eval_mode: ResolvedEvalMode,
    pub(super) eval_timeout: std::time::Duration,
    pub(super) eval_fn: Option<Arc<Py<PyAny>>>,
    pub(super) precomputed: Option<Arc<PrecomputedArgs>>,
}

impl PyCodeRepository {
    pub(in crate::daemon) fn extract_schedules(
        &self,
        py: Python,
        resources: &HashMap<String, Py<PyAny>>,
    ) -> Vec<ScheduleInfo> {
        self.raw_schedules
            .iter()
            .filter_map(|(_, sched_py)| {
                let sched = sched_py.borrow(py);
                if matches!(sched.default_status, PyScheduleStatus::Running) {
                    let eval_mode =
                        resolve_eval_mode(py, sched.evaluation_fn.as_ref(), &sched.eval_mode);
                    let eval_fn = sched
                        .evaluation_fn
                        .as_ref()
                        .map(|f| Arc::new(f.clone_ref(py)));
                    let precomputed = eval_fn.as_ref().and_then(|f| {
                        precompute_args(py, f, resources)
                            .map_err(|e| {
                                tracing::warn!(
                                    target: "rivers::daemon",
                                    name = %sched.name,
                                    error = %e,
                                    "failed to precompute args for schedule"
                                );
                            })
                            .ok()
                            .map(Arc::new)
                    });
                    Some(ScheduleInfo {
                        name: sched.name.clone(),
                        cron_schedule: sched.cron_schedule.clone(),
                        job_name: sched.job_name.clone(),
                        timezone: sched.timezone.clone(),
                        tags: sched.tags.clone(),
                        eval_mode,
                        eval_timeout: sched
                            .eval_timeout
                            .as_deref()
                            .map(|v| crate::utils::parse_duration("eval_timeout", v))
                            .transpose()
                            .ok()
                            .flatten()
                            .unwrap_or(std::time::Duration::from_secs(300)),
                        eval_fn,
                        precomputed,
                    })
                } else {
                    None
                }
            })
            .collect()
    }
}

pub(super) async fn evaluate_schedule_sync(
    eval_fn: Option<&Arc<Py<PyAny>>>,
    precomputed: &Arc<PrecomputedArgs>,
    name: &str,
    execution_time: &str,
    job_name: &str,
    tags: &Option<HashMap<String, String>>,
) -> Result<TickOutcome, String> {
    let Some(eval_fn) = eval_fn.cloned() else {
        // No eval_fn — pure cron schedule, no Python needed
        return Ok(TickOutcome::RunRequests(
            vec![RunRequestData {
                run_key: None,
                tags: tags.clone(),
                partition_key: None,
                job_name: Some(job_name.to_string()),
            }],
            vec![], // no materialization requests for schedules
            vec![],
        ));
    };

    let precomputed = precomputed.clone();
    let name = name.to_string();
    let exec_time = execution_time.to_string();
    let job_name = job_name.to_string();
    let tags = tags.clone();

    let _permit = GIL_SEMAPHORE.acquire().await.map_err(|e| e.to_string())?;
    tokio::task::spawn_blocking(move || {
        Python::try_attach(|py| -> Result<TickOutcome, String> {
            let ctx = Py::new(
                py,
                crate::context::schedule::PyScheduleEvaluationContext::new(exec_time, name)
                    .with_config(
                        precomputed
                            .config_instance
                            .as_ref()
                            .map(|c| c.clone_ref(py)),
                    ),
            )
            .map_err(|e| e.to_string())?;

            let call_args = assemble_call_args(py, ctx.into_any(), &precomputed);
            let args_tuple =
                pyo3::types::PyTuple::new(py, &call_args).map_err(|e| e.to_string())?;
            let result = eval_fn.call1(py, args_tuple).map_err(|e| e.to_string())?;
            let parsed =
                parse_schedule_result(py, &result, &job_name, &tags).map_err(|e| e.to_string())?;
            Ok(extract_tick_outcome_from_parts(
                py,
                &parsed.run_requests,
                &parsed.backfill_requests,
                parsed.skip_reason.as_ref(),
            ))
        })
        .unwrap_or(Err("Python not attached".into()))
    })
    .await
    .map_err(|e| e.to_string())?
}

// Async in-process eval (pyo3-async-runtimes):
// Phase 1: GIL — build args, call async fn → coroutine → into_future
// Phase 2: No GIL — await the Rust future (Python I/O overlaps)
// Phase 3: GIL — parse result
pub(super) async fn evaluate_schedule_async(
    eval_fn: &Arc<Py<PyAny>>,
    precomputed: &Arc<PrecomputedArgs>,
    name: &str,
    execution_time: &str,
    job_name: &str,
) -> Result<TickOutcome, String> {
    let eval_fn = eval_fn.clone();
    let precomputed = precomputed.clone();
    let name = name.to_string();
    let exec_time = execution_time.to_string();
    let job_name = job_name.to_string();

    let _permit = GIL_SEMAPHORE.acquire().await.map_err(|e| e.to_string())?;
    let rust_future = tokio::task::spawn_blocking(move || {
        Python::try_attach(|py| -> Result<BoxedPyFuture, String> {
            let ctx = Py::new(
                py,
                crate::context::schedule::PyScheduleEvaluationContext::new(exec_time, name.clone())
                    .with_config(
                        precomputed
                            .config_instance
                            .as_ref()
                            .map(|c| c.clone_ref(py)),
                    ),
            )
            .map_err(|e| e.to_string())?;

            let call_args = assemble_call_args(py, ctx.into_any(), &precomputed);
            let args_tuple =
                pyo3::types::PyTuple::new(py, &call_args).map_err(|e| e.to_string())?;
            let coroutine = eval_fn.call1(py, args_tuple).map_err(|e| e.to_string())?;
            let future = pyo3_async_runtimes::tokio::into_future(coroutine.into_bound(py))
                .map_err(|e| e.to_string())?;
            Ok(Box::pin(future) as BoxedPyFuture)
        })
        .unwrap_or(Err("Python not attached".into()))
    })
    .await
    .map_err(|e| e.to_string())??;
    drop(_permit);

    let py_result = rust_future.await.map_err(|e| e.to_string())?;

    let _permit = GIL_SEMAPHORE.acquire().await.map_err(|e| e.to_string())?;
    tokio::task::spawn_blocking(move || {
        Python::try_attach(|py| -> Result<TickOutcome, String> {
            let parsed = parse_schedule_result(py, &py_result, &job_name, &None)
                .map_err(|e| e.to_string())?;
            Ok(extract_tick_outcome_from_parts(
                py,
                &parsed.run_requests,
                &parsed.backfill_requests,
                parsed.skip_reason.as_ref(),
            ))
        })
        .unwrap_or(Err("Python not attached".into()))
    })
    .await
    .map_err(|e| e.to_string())?
}
