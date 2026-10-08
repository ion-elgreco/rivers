//! Run-status sensors — call the sensor function once for each run of this
//! code location that reaches the watched status. The cursor logic lives in
//! [`rivers_core::sensor::RunStatusCursor`].
use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;
use rivers_core::sensor::RunStatusCursor;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{RunStatus, ScopedStorageHandle, StorageBackend};

use super::eval_dispatcher::{build_resource_specs, wait_loky_result};
use super::parse::assemble_call_args;
use super::subprocess_eval::invoke_eval_fn;
use super::types::{EvalOutcome, EvalParams, PY_EVAL_PERMITS, PrecomputedArgs, ResolvedEvalMode};
use crate::context::run_status::{PyRunStatusSensorContext, RunStatusEvent};
use crate::errors::ExecutionError;
use crate::executor::ops::now_ts;
use crate::executor::parallel::worker_args::make_func_ref;

#[derive(Clone)]
pub(crate) struct RunStatusSpec {
    pub(crate) status: RunStatus,
    pub(crate) monitored_jobs: Option<Vec<String>>,
}

/// The runs not handled yet, with their failure detail, and the cursor past them.
async fn fetch(
    handle: &ScopedStorageHandle<SurrealStorage>,
    spec: &RunStatusSpec,
    cursor: RunStatusCursor,
) -> anyhow::Result<(RunStatusCursor, Vec<RunStatusEvent>)> {
    let rows = handle
        .scoped()
        .get_runs_ended_since(
            cursor.since(),
            spec.status.clone(),
            spec.monitored_jobs.as_deref(),
            cursor.read_limit(),
        )
        .await?;
    let new = cursor.select_new(rows);
    if new.is_empty() {
        return Ok((cursor.idle(now_ts()), Vec::new()));
    }
    let cursor = cursor.advance(&new);
    let mut events = Vec::with_capacity(new.len());
    for run in new {
        let failures = if run.status == RunStatus::Failure {
            handle.backend().get_run_failure_events(&run.run_id).await?
        } else {
            Vec::new()
        };
        events.push(RunStatusEvent::new(run, failures));
    }
    Ok((cursor, events))
}

/// Run one tick of a run-status sensor. A function error fails the tick but
/// still moves the cursor past that run, as Dagster does.
pub(super) async fn evaluate(
    params: &EvalParams,
    spec: &RunStatusSpec,
    cursor: Option<&str>,
    handle: &ScopedStorageHandle<SurrealStorage>,
    loky: Option<&Arc<Py<PyAny>>>,
    resources: &Arc<HashMap<String, Py<PyAny>>>,
) -> Result<EvalOutcome, String> {
    let status = format!("{:?}", spec.status);
    let Some(cursor) = RunStatusCursor::parse(cursor) else {
        return Ok(EvalOutcome::Skipped {
            reason: format!("Watching for {status} runs from now on"),
            cursor: Some(RunStatusCursor::init(now_ts()).to_json()),
        });
    };
    let tick = async {
        let (cursor, events) = fetch(handle, spec, cursor)
            .await
            .map_err(|e| e.to_string())?;
        if events.is_empty() {
            return Ok(EvalOutcome::Skipped {
                reason: format!("No new {status} runs"),
                cursor: Some(cursor.to_json()),
            });
        }
        let eval_fn = params
            .eval_fn
            .as_ref()
            .ok_or_else(|| format!("Sensor '{}' has no evaluation function", params.name))?;
        let run_ids: Vec<String> = events.iter().map(|e| e.run.run_id.clone()).collect();
        let outcomes = match params.eval_mode {
            ResolvedEvalMode::SyncInProcess | ResolvedEvalMode::AsyncInProcess => {
                let precomputed = params
                    .precomputed
                    .as_ref()
                    .ok_or_else(|| format!("'{}' has no precomputed args", params.name))?;
                call_in_process(eval_fn, precomputed, &params.name, events).await?
            }
            ResolvedEvalMode::Subprocess => {
                let loky = loky.ok_or("loky executor must be initialized for subprocess eval")?;
                call_subprocess(
                    loky,
                    resources,
                    eval_fn,
                    &params.name,
                    events,
                    params.timeout,
                )
                .await?
            }
        };
        let errors: Vec<String> = run_ids
            .iter()
            .zip(outcomes)
            .filter_map(|(id, error)| error.map(|e| format!("run {id}: {e}")))
            .collect();
        Ok(EvalOutcome::Handled {
            cursor: Some(cursor.to_json()),
            error: (!errors.is_empty()).then(|| errors.join("; ")),
        })
    };
    tokio::time::timeout(params.timeout, tick)
        .await
        .unwrap_or(Err(format!(
            "Sensor '{}' eval timed out after {}s",
            params.name,
            params.timeout.as_secs()
        )))
}

/// Call `eval_fn` once per run, in order. One entry per run: the error, if
/// the call raised or returned something other than `None`. An async function
/// runs to completion with `asyncio.run`.
pub(super) fn call_each(
    py: Python,
    eval_fn: &Py<PyAny>,
    sensor_name: &str,
    events: Vec<RunStatusEvent>,
    pre: &PrecomputedArgs,
) -> Vec<Option<String>> {
    events
        .into_iter()
        .map(|event| {
            let called = (|| -> PyResult<()> {
                let config = pre.config_instance.as_ref().map(|c| c.clone_ref(py));
                let ctx =
                    PyRunStatusSensorContext::new(py, sensor_name.to_string(), event, config)?;
                let ctx = Py::new(py, ctx)?.into_any();
                let out = invoke_eval_fn(py, eval_fn, assemble_call_args(py, ctx, pre))?;
                if out.is_none(py) {
                    return Ok(());
                }
                Err(ExecutionError::new_err(format!(
                    "a run-status sensor function must return None; got {}",
                    out.bind(py).get_type().qualname()?
                )))
            })();
            called.err().map(|e| e.to_string())
        })
        .collect()
}

async fn call_in_process(
    eval_fn: &Arc<Py<PyAny>>,
    precomputed: &Arc<PrecomputedArgs>,
    name: &str,
    events: Vec<RunStatusEvent>,
) -> Result<Vec<Option<String>>, String> {
    let eval_fn = eval_fn.clone();
    let precomputed = precomputed.clone();
    let name = name.to_string();
    let _permit = PY_EVAL_PERMITS.acquire().await.map_err(|e| e.to_string())?;
    tokio::task::spawn_blocking(move || {
        Python::try_attach(|py| call_each(py, &eval_fn, &name, events, &precomputed))
            .ok_or_else(|| "Python not attached".to_string())
    })
    .await
    .map_err(|e| e.to_string())?
}

async fn call_subprocess(
    loky: &Arc<Py<PyAny>>,
    resources: &Arc<HashMap<String, Py<PyAny>>>,
    eval_fn: &Arc<Py<PyAny>>,
    name: &str,
    events: Vec<RunStatusEvent>,
    timeout: std::time::Duration,
) -> Result<Vec<Option<String>>, String> {
    let payloads = events
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    let loky = loky.clone();
    let resources = resources.clone();
    let eval_fn = eval_fn.clone();
    let sensor_name = name.to_string();

    let permit = PY_EVAL_PERMITS.acquire().await.map_err(|e| e.to_string())?;
    let py_future = tokio::task::spawn_blocking(move || {
        Python::try_attach(|py| -> PyResult<Py<PyAny>> {
            let wrapper = py
                .import("rivers._core")?
                .getattr("eval_run_status_sensor_in_subprocess")?;
            let specs = build_resource_specs(py, &resources)?;
            let func_ref = make_func_ref(py, &eval_fn).unwrap_or_else(|_| eval_fn.clone_ref(py));
            Ok(loky
                .bind(py)
                .call_method1("submit", (wrapper, func_ref, sensor_name, payloads, specs))?
                .unbind())
        })
        .map(|r| r.map_err(|e| e.to_string()))
        .unwrap_or_else(|| Err("Python not attached".into()))
    })
    .await
    .map_err(|e| e.to_string())??;
    drop(permit);

    let raw = wait_loky_result(py_future, timeout, format!("Sensor '{name}'")).await?;

    let _permit = PY_EVAL_PERMITS.acquire().await.map_err(|e| e.to_string())?;
    tokio::task::spawn_blocking(move || {
        Python::try_attach(|py| {
            raw.extract::<Vec<Option<String>>>(py)
                .map_err(|e| e.to_string())
        })
        .unwrap_or_else(|| Err("Python not attached".into()))
    })
    .await
    .map_err(|e| e.to_string())?
}
