//! Eval dispatch — routes a batch of due automations to the right execution
//! path based on each item's `ResolvedEvalMode`. In-process evals (sync /
//! async) are spawned individually; subprocess evals are batch-submitted in a
//! single GIL acquisition (a real cost optimization — `loky.submit` per item
//! would acquire the GIL N times) and their wait tasks are spawned afterwards.
//!
//! The choice of mode is data-driven (each automation carries its own
//! `eval_mode`) rather than configuration-driven, so a single facade with
//! internal routing — not a strategy enum — is the right shape here.
//!
//! Subprocess transport mirrors the multiprocess executor: the user's eval
//! function crosses as a `FuncRef(module, qualname)` (imported in the worker,
//! not pickled), and registered resources cross as `(name, class, json_data)`
//! triples (re-instantiated via `cls.model_validate_json` in the worker).
//! `_FuncRef.__reduce__` returns the resolved callable directly, so the
//! Rust wrapper in `subprocess_eval` receives the eval function unwrapped.
use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};
use tracing::Instrument;

use super::parse::{extract_sensor_outcome_from_parts, extract_tick_outcome_from_parts};
use super::schedule::{evaluate_schedule_async, evaluate_schedule_sync};
use super::sensors::{evaluate_sensor_async, evaluate_sensor_sync};
use super::types::{
    AutomationKind, EvalOutcome, EvalParams, GIL_SEMAPHORE, ResolvedEvalMode, TickResult,
};
use crate::automation::{parse_schedule_result, parse_sensor_result};
use crate::executor::parallel::worker_args::make_func_ref;

/// One automation that's due to evaluate this iteration. Carries the per-item
/// state the loop needs to thread through to the eventual `TickResult`.
pub(crate) struct DueEval {
    pub(crate) index: usize,
    pub(crate) params: EvalParams,
    pub(crate) prev_cursor: Option<String>,
}

pub(crate) struct EvalDispatcher {
    loky_executor: Option<Arc<Py<PyAny>>>,
    /// Registered resources (name → Pydantic instance). Serialized at submit
    /// time into `(name, class, json_data)` triples so the subprocess wrapper
    /// can rebuild + `setup()` them before invoking the eval function.
    resources: Arc<HashMap<String, Py<PyAny>>>,
}

impl EvalDispatcher {
    pub(crate) fn new(
        loky_executor: Option<Arc<Py<PyAny>>>,
        resources: Arc<HashMap<String, Py<PyAny>>>,
    ) -> Self {
        Self {
            loky_executor,
            resources,
        }
    }

    /// Spawn each due eval as a task on the caller's `join_set`. Subprocess
    /// evals are batch-submitted in one GIL acquisition before their wait
    /// tasks are spawned; in-process evals are spawned individually.
    pub(crate) async fn spawn_due(
        &self,
        due: Vec<DueEval>,
        join_set: &mut tokio::task::JoinSet<TickResult>,
        dispatched_at: DateTime<Utc>,
    ) {
        let mut subprocess_items: Vec<DueEval> = Vec::new();

        for d in due {
            if matches!(d.params.eval_mode, ResolvedEvalMode::Subprocess) {
                subprocess_items.push(d);
            } else {
                self.spawn_in_process(d, join_set, dispatched_at);
            }
        }

        if !subprocess_items.is_empty() {
            self.spawn_subprocess_batch(subprocess_items, join_set, dispatched_at)
                .await;
        }
    }

    fn spawn_in_process(
        &self,
        due: DueEval,
        join_set: &mut tokio::task::JoinSet<TickResult>,
        dispatched_at: DateTime<Utc>,
    ) {
        let DueEval {
            index,
            params,
            prev_cursor,
        } = due;
        let span = tracing::info_span!(
            target: "rivers::daemon",
            "eval",
            automation_type = automation_type_str(&params.kind),
            name = %params.name,
        );
        join_set.spawn(
            async move {
                let result = run_in_process_eval(&params).await;
                TickResult {
                    index,
                    result,
                    prev_cursor,
                    dispatched_at,
                }
            }
            .instrument(span),
        );
    }

    async fn spawn_subprocess_batch(
        &self,
        items: Vec<DueEval>,
        join_set: &mut tokio::task::JoinSet<TickResult>,
        dispatched_at: DateTime<Utc>,
    ) {
        let loky = self
            .loky_executor
            .as_ref()
            .expect("loky executor must be initialized when subprocess items exist");

        let submitted = batch_submit_subprocess(&items, loky, &self.resources).await;

        for (due, submit_result) in items.into_iter().zip(submitted.into_iter()) {
            let DueEval {
                index,
                params,
                prev_cursor,
            } = due;
            let span = tracing::info_span!(
                target: "rivers::daemon",
                "eval",
                automation_type = automation_type_str(&params.kind),
                name = %params.name,
            );

            match submit_result {
                Ok(py_future) => {
                    let timeout = params.timeout;
                    let timeout_secs = timeout.as_secs();
                    let name = params.name.clone();
                    let kind = params.kind.clone();
                    let default_job_name = params.default_job_name.clone();
                    let default_asset_selection = params.default_asset_selection.clone();
                    let launched_by = params.launched_by.clone();
                    let tags = params.tags.clone();
                    join_set.spawn(
                        async move {
                            let result = tokio::time::timeout(
                                timeout,
                                wait_subprocess_result(
                                    py_future,
                                    &kind,
                                    default_job_name.as_deref(),
                                    default_asset_selection.as_deref(),
                                    &launched_by,
                                    &tags,
                                ),
                            )
                            .await
                            .unwrap_or(Err(format!(
                                "{} '{}' subprocess eval timed out after {}s",
                                automation_type_str(&kind),
                                name,
                                timeout_secs
                            )));
                            TickResult {
                                index,
                                result,
                                prev_cursor,
                                dispatched_at,
                            }
                        }
                        .instrument(span),
                    );
                }
                Err(e) => {
                    join_set.spawn(
                        async move {
                            TickResult {
                                index,
                                result: Err(e),
                                prev_cursor,
                                dispatched_at,
                            }
                        }
                        .instrument(span),
                    );
                }
            }
        }
    }
}

fn automation_type_str(kind: &AutomationKind) -> &'static str {
    match kind {
        AutomationKind::Schedule { .. } => "Schedule",
        AutomationKind::Sensor { .. } => "Sensor",
    }
}

/// Run a single in-process eval (sync or async). Subprocess evals are NOT
/// routed through here — the dispatcher batches them via
/// `spawn_subprocess_batch` for single-GIL-acquisition efficiency.
async fn run_in_process_eval(params: &EvalParams) -> Result<EvalOutcome, String> {
    let name = &params.name;
    let timeout = params.timeout;
    let timeout_secs = timeout.as_secs();
    let precomputed = params
        .precomputed
        .as_ref()
        .ok_or_else(|| format!("'{}' has no precomputed args", name))?;

    match &params.eval_mode {
        ResolvedEvalMode::SyncInProcess => match &params.kind {
            AutomationKind::Schedule { exec_time } => tokio::time::timeout(
                timeout,
                evaluate_schedule_sync(
                    params.eval_fn.as_ref(),
                    precomputed,
                    name,
                    exec_time,
                    params.default_job_name.as_deref().unwrap_or_default(),
                    &params.tags,
                ),
            )
            .await
            .unwrap_or(Err(format!(
                "Schedule '{}' eval timed out after {}s",
                name, timeout_secs
            )))
            .map(EvalOutcome::from),
            AutomationKind::Sensor {
                cursor,
                last_tick_time,
            } => {
                let eval_fn = params
                    .eval_fn
                    .as_ref()
                    .ok_or_else(|| format!("Sensor '{}' has no evaluation function", name))?;
                tokio::time::timeout(
                    timeout,
                    evaluate_sensor_sync(
                        eval_fn,
                        precomputed,
                        name,
                        cursor.as_deref(),
                        *last_tick_time,
                        params.default_job_name.as_deref(),
                        params.default_asset_selection.as_deref(),
                        &params.launched_by,
                    ),
                )
                .await
                .unwrap_or(Err(format!(
                    "Sensor '{}' eval timed out after {}s",
                    name, timeout_secs
                )))
                .map(EvalOutcome::from)
            }
        },
        ResolvedEvalMode::AsyncInProcess => {
            let eval_fn = params
                .eval_fn
                .as_ref()
                .ok_or_else(|| format!("'{}' has no evaluation function", name))?;
            match &params.kind {
                AutomationKind::Schedule { exec_time } => tokio::time::timeout(
                    timeout,
                    evaluate_schedule_async(
                        eval_fn,
                        precomputed,
                        name,
                        exec_time,
                        params.default_job_name.as_deref().unwrap_or_default(),
                    ),
                )
                .await
                .unwrap_or(Err(format!(
                    "Schedule '{}' async eval timed out after {}s",
                    name, timeout_secs
                )))
                .map(EvalOutcome::from),
                AutomationKind::Sensor {
                    cursor,
                    last_tick_time,
                } => tokio::time::timeout(
                    timeout,
                    evaluate_sensor_async(
                        eval_fn,
                        precomputed,
                        name,
                        cursor.as_deref(),
                        *last_tick_time,
                        params.default_job_name.as_deref(),
                        params.default_asset_selection.as_deref(),
                        &params.launched_by,
                    ),
                )
                .await
                .unwrap_or(Err(format!(
                    "Sensor '{}' async eval timed out after {}s",
                    name, timeout_secs
                )))
                .map(EvalOutcome::from),
            }
        }
        ResolvedEvalMode::Subprocess => {
            unreachable!("subprocess evals are routed via spawn_subprocess_batch")
        }
    }
}

/// Build `[(name, class, json_data), ...]` for every registered resource —
/// subprocess workers rebuild instances via `cls.model_validate_json` before
/// `precompute_args` looks them up by parameter name.
fn build_resource_specs<'py>(
    py: Python<'py>,
    resources: &HashMap<String, Py<PyAny>>,
) -> PyResult<Bound<'py, PyList>> {
    let specs = PyList::empty(py);
    for (name, resource) in resources {
        let bound = resource.bind(py);
        let cls = bound.getattr("__class__")?;
        let json_data = bound.call_method0("model_dump_json")?;
        specs.append(PyTuple::new(
            py,
            &[
                name.into_pyobject(py)?.into_any().unbind(),
                cls.unbind(),
                json_data.unbind(),
            ],
        )?)?;
    }
    Ok(specs)
}

/// Phase 1: brief GIL — submit one eval per due item to the loky
/// `ProcessPoolExecutor`. Returns the loky future handles (one per input);
/// per-item submission failures get folded into `Err` slots rather than
/// aborting the batch.
async fn batch_submit_subprocess(
    items: &[DueEval],
    loky_executor: &Arc<Py<PyAny>>,
    resources: &Arc<HashMap<String, Py<PyAny>>>,
) -> Vec<Result<Py<PyAny>, String>> {
    let executor = loky_executor.clone();
    let resources = resources.clone();

    let submit_data: Vec<(Arc<Py<PyAny>>, String, AutomationKind)> = items
        .iter()
        .filter_map(|d| {
            d.params
                .eval_fn
                .as_ref()
                .map(|f| (f.clone(), d.params.name.clone(), d.params.kind.clone()))
        })
        .collect();

    let _permit = GIL_SEMAPHORE.acquire().await.unwrap();
    let results = tokio::task::spawn_blocking(move || {
        Python::try_attach(|py| -> Vec<Result<Py<PyAny>, String>> {
            let core = match py.import("rivers._core") {
                Ok(m) => m,
                Err(e) => {
                    return submit_data.iter().map(|_| Err(e.to_string())).collect();
                }
            };

            let schedule_wrapper = core.getattr("eval_schedule_in_subprocess").ok();
            let sensor_wrapper = core.getattr("eval_sensor_in_subprocess").ok();
            let resource_specs = match build_resource_specs(py, &resources) {
                Ok(specs) => specs.unbind(),
                Err(e) => {
                    return submit_data.iter().map(|_| Err(e.to_string())).collect();
                }
            };

            submit_data
                .iter()
                .map(|(eval_fn, name, kind)| {
                    // Same FuncRef wrap-or-fallback shape as the multiprocess
                    // executor in `worker_args::build_worker_submit_args` —
                    // cloudpickle handles closures / locals directly.
                    let func_ref =
                        make_func_ref(py, eval_fn).unwrap_or_else(|_| eval_fn.clone_ref(py));
                    match kind {
                        AutomationKind::Schedule { exec_time } => {
                            let wrapper = schedule_wrapper.as_ref().ok_or_else(|| {
                                "eval_schedule_in_subprocess not found".to_string()
                            })?;
                            executor
                                .bind(py)
                                .call_method1(
                                    "submit",
                                    (
                                        wrapper,
                                        func_ref,
                                        name.as_str(),
                                        exec_time.as_str(),
                                        resource_specs.bind(py),
                                    ),
                                )
                                .map(|f| f.unbind())
                                .map_err(|e| e.to_string())
                        }
                        AutomationKind::Sensor {
                            cursor,
                            last_tick_time,
                        } => {
                            let wrapper = sensor_wrapper
                                .as_ref()
                                .ok_or_else(|| "eval_sensor_in_subprocess not found".to_string())?;
                            executor
                                .bind(py)
                                .call_method1(
                                    "submit",
                                    (
                                        wrapper,
                                        func_ref,
                                        name.as_str(),
                                        cursor.as_deref(),
                                        *last_tick_time,
                                        resource_specs.bind(py),
                                    ),
                                )
                                .map(|f| f.unbind())
                                .map_err(|e| e.to_string())
                        }
                    }
                })
                .collect()
        })
        .unwrap_or_else(|| {
            submit_data
                .iter()
                .map(|_| Err("Python not attached".into()))
                .collect()
        })
    })
    .await
    .unwrap_or_else(|e| items.iter().map(|_| Err(e.to_string())).collect());
    drop(_permit);

    results
}

/// Phase 2 (no GIL): wait for the loky future. Phase 3 (brief GIL): parse the
/// returned PyO3 result (RunRequest / SkipReason / SensorResult / list / None)
/// using the same `parse_schedule_result` / `parse_sensor_result` path as
/// in-process evals — possible because those types pickle natively via their
/// `__reduce__` methods.
async fn wait_subprocess_result(
    py_future: Py<PyAny>,
    kind: &AutomationKind,
    default_job_name: Option<&str>,
    default_asset_selection: Option<&[String]>,
    launched_by: &rivers_core::storage::LaunchedBy,
    tags: &Option<HashMap<String, String>>,
) -> Result<EvalOutcome, String> {
    let job_name: Option<String> = default_job_name.map(|s| s.to_string());
    let asset_sel: Option<Vec<String>> = default_asset_selection.map(|s| s.to_vec());
    let launched_by = launched_by.clone();
    let tags = tags.clone();
    let kind = kind.clone();

    let raw_result = tokio::task::spawn_blocking(move || {
        Python::try_attach(|py| -> Result<Py<PyAny>, String> {
            py_future
                .bind(py)
                .call_method1("result", (300.0,))
                .map(|r| r.unbind())
                .map_err(|e| e.to_string())
        })
        .unwrap_or(Err("Python not attached".into()))
    })
    .await
    .map_err(|e| e.to_string())??;

    let _permit = GIL_SEMAPHORE.acquire().await.map_err(|e| e.to_string())?;
    tokio::task::spawn_blocking(move || {
        Python::try_attach(|py| -> Result<EvalOutcome, String> {
            match kind {
                AutomationKind::Schedule { .. } => {
                    let parsed = parse_schedule_result(
                        py,
                        &raw_result,
                        job_name.as_deref().unwrap_or_default(),
                        &tags,
                    )
                    .map_err(|e| e.to_string())?;
                    Ok(extract_tick_outcome_from_parts(
                        py,
                        &parsed.run_requests,
                        &parsed.backfill_requests,
                        parsed.skip_reason.as_ref(),
                    )
                    .into())
                }
                AutomationKind::Sensor { .. } => {
                    let parsed = parse_sensor_result(py, &raw_result, job_name.as_deref())
                        .map_err(|e| e.to_string())?;
                    Ok(extract_sensor_outcome_from_parts(
                        py,
                        &parsed.run_requests,
                        &parsed.backfill_requests,
                        parsed.skip_reason.as_ref(),
                        parsed.cursor,
                        asset_sel.as_deref(),
                        &launched_by,
                    )
                    .into())
                }
            }
        })
        .unwrap_or(Err("Python not attached".into()))
    })
    .await
    .map_err(|e| e.to_string())?
}
