//! Sensor daemon loop — polls sensors and submits run requests with cursor tracking.
use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;

use crate::automation::parse_sensor_result;
use crate::automation::sensor::PySensorStatus;
use crate::repository::PyCodeRepository;

use super::{
    BoxedPyFuture, GIL_SEMAPHORE, PrecomputedArgs, ResolvedEvalMode, SensorOutcome,
    assemble_call_args, extract_sensor_outcome_from_parts, precompute_args, resolve_eval_mode,
};

pub(super) struct SensorInfo {
    pub(super) name: String,
    pub(super) job_name: Option<String>,
    pub(super) minimum_interval: std::time::Duration,
    #[allow(dead_code)]
    pub(super) tags: Option<HashMap<String, String>>,
    pub(super) asset_selection: Option<Vec<String>>,
    pub(super) eval_mode: ResolvedEvalMode,
    pub(super) eval_timeout: std::time::Duration,
    pub(super) eval_fn: Option<Arc<Py<PyAny>>>,
    pub(super) precomputed: Option<Arc<PrecomputedArgs>>,
}

impl PyCodeRepository {
    pub(in crate::daemon) fn extract_sensors(
        &self,
        py: Python,
        resources: &HashMap<String, Py<PyAny>>,
    ) -> Vec<SensorInfo> {
        self.raw_sensors
            .iter()
            .filter_map(|(_, sens_py)| {
                let sens = sens_py.borrow(py);
                if matches!(sens.default_status, PySensorStatus::Running) {
                    let eval_mode =
                        resolve_eval_mode(py, sens.evaluation_fn.as_ref(), &sens.eval_mode);
                    let eval_fn = sens
                        .evaluation_fn
                        .as_ref()
                        .map(|f| Arc::new(f.clone_ref(py)));
                    let precomputed = eval_fn.as_ref().and_then(|f| {
                        precompute_args(py, f, resources)
                            .map_err(|e| {
                                tracing::warn!(
                                    target: "rivers::daemon",
                                    name = %sens.name,
                                    error = %e,
                                    "failed to precompute args for sensor"
                                );
                            })
                            .ok()
                            .map(Arc::new)
                    });
                    Some(SensorInfo {
                        name: sens.name.clone(),
                        job_name: sens.job_name.clone(),
                        minimum_interval: sens
                            .minimum_interval
                            .as_deref()
                            .map(|v| crate::utils::parse_duration("minimum_interval", v))
                            .transpose()
                            .ok()
                            .flatten()
                            .unwrap_or(std::time::Duration::from_secs(30)),
                        tags: sens.tags.clone(),
                        asset_selection: sens.asset_selection.clone(),
                        eval_mode,
                        eval_timeout: sens
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

pub(super) async fn evaluate_sensor_sync(
    eval_fn: &Arc<Py<PyAny>>,
    precomputed: &Arc<PrecomputedArgs>,
    name: &str,
    cursor: Option<&str>,
    last_tick_time: Option<f64>,
    default_job_name: Option<&str>,
    default_asset_selection: Option<&[String]>,
    launched_by: &rivers_core::storage::LaunchedBy,
) -> Result<SensorOutcome, String> {
    let eval_fn = eval_fn.clone();
    let precomputed = precomputed.clone();
    let name = name.to_string();
    let cursor = cursor.map(|s| s.to_string());
    let default_job: Option<String> = default_job_name.map(|s| s.to_string());
    let asset_sel: Option<Vec<String>> = default_asset_selection.map(|s| s.to_vec());
    let launched_by = launched_by.clone();

    let _permit = GIL_SEMAPHORE.acquire().await.map_err(|e| e.to_string())?;
    tokio::task::spawn_blocking(move || {
        Python::try_attach(|py| -> Result<SensorOutcome, String> {
            let ctx = Py::new(
                py,
                crate::context::sensor::PySensorEvaluationContext::new(
                    name,
                    cursor,
                    last_tick_time,
                )
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
            let parsed = parse_sensor_result(py, &result, default_job.as_deref())
                .map_err(|e| e.to_string())?;
            Ok(extract_sensor_outcome_from_parts(
                py,
                &parsed.run_requests,
                &parsed.backfill_requests,
                parsed.skip_reason.as_ref(),
                parsed.cursor,
                asset_sel.as_deref(),
                &launched_by,
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
pub(super) async fn evaluate_sensor_async(
    eval_fn: &Arc<Py<PyAny>>,
    precomputed: &Arc<PrecomputedArgs>,
    name: &str,
    cursor: Option<&str>,
    last_tick_time: Option<f64>,
    default_job_name: Option<&str>,
    default_asset_selection: Option<&[String]>,
    launched_by: &rivers_core::storage::LaunchedBy,
) -> Result<SensorOutcome, String> {
    let eval_fn = eval_fn.clone();
    let precomputed = precomputed.clone();
    let name = name.to_string();
    let cursor = cursor.map(|s| s.to_string());
    let default_job: Option<String> = default_job_name.map(|s| s.to_string());
    let asset_sel: Option<Vec<String>> = default_asset_selection.map(|s| s.to_vec());
    let launched_by = launched_by.clone();

    let _permit = GIL_SEMAPHORE.acquire().await.map_err(|e| e.to_string())?;
    let rust_future = tokio::task::spawn_blocking(move || {
        Python::try_attach(|py| -> Result<BoxedPyFuture, String> {
            let ctx = Py::new(
                py,
                crate::context::sensor::PySensorEvaluationContext::new(
                    name.clone(),
                    cursor,
                    last_tick_time,
                )
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
        Python::try_attach(|py| -> Result<SensorOutcome, String> {
            let parsed = parse_sensor_result(py, &py_result, default_job.as_deref())
                .map_err(|e| e.to_string())?;
            Ok(extract_sensor_outcome_from_parts(
                py,
                &parsed.run_requests,
                &parsed.backfill_requests,
                parsed.skip_reason.as_ref(),
                parsed.cursor,
                asset_sel.as_deref(),
                &launched_by,
            ))
        })
        .unwrap_or(Err("Python not attached".into()))
    })
    .await
    .map_err(|e| e.to_string())?
}
