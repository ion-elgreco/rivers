//! Rust-side subprocess wrappers for schedule/sensor evaluation.
//!
//! These functions are submitted to loky as the wrapper callable. Loky
//! cloudpickles the wrapper's arguments — `eval_fn` is shipped as a `FuncRef`
//! that auto-reconstructs to the imported callable in the worker (see
//! `executor::parallel::refs::PyFuncRef`), so the user's function is never
//! pickled. Resources cross via `(name, class, json_data)` tuples and are
//! re-instantiated in the worker. Same pattern as the multiprocess executor.
//!
//! The flow inside the worker mirrors the in-process eval path in
//! `daemon::schedule` / `daemon::sensors`: rebuild resources, run
//! `precompute_args` to extract the typed config + resource args, build the
//! evaluation context with the resolved config, assemble call args, invoke
//! the eval function (driving any returned coroutine via `asyncio.run`), and
//! return the raw Python result. Parsing into `RunRequest` / `SkipReason` /
//! `SensorResult` is the orchestrator's job — those types pickle natively.
use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

use super::parse::assemble_call_args;
use super::run_status::call_each;
use super::{precompute_args, types::PrecomputedArgs};
use crate::context::run_status::RunStatusEvent;
use crate::context::schedule::PyScheduleEvaluationContext;
use crate::context::sensor::PySensorEvaluationContext;

/// Reconstruct registered resources from `(name, class, json_data)` triples.
/// Each resource is rebuilt via `cls.model_validate_json(json_data)` and gets
/// `setup()` called (matching the in-process daemon's resource lifecycle).
fn rebuild_resources<'py>(
    py: Python<'py>,
    resource_specs: &Bound<'py, PyList>,
) -> PyResult<(HashMap<String, Py<PyAny>>, Vec<Py<PyAny>>)> {
    let mut resources = HashMap::new();
    let mut order = Vec::with_capacity(resource_specs.len());
    for spec in resource_specs.iter() {
        // Tear down already-setup resources if a later one fails to build/setup.
        let built = (|| -> PyResult<(String, Py<PyAny>)> {
            let tuple: &Bound<PyTuple> = spec.cast()?;
            let name: String = tuple.get_item(0)?.extract()?;
            let cls = tuple.get_item(1)?;
            let json_data = tuple.get_item(2)?;
            let resource = cls.call_method1("model_validate_json", (&json_data,))?;
            resource.call_method0("setup")?;
            Ok((name, resource.unbind()))
        })();
        match built {
            Ok((name, resource)) => {
                order.push(resource.clone_ref(py));
                resources.insert(name, resource);
            }
            Err(e) => {
                teardown_resources(py, &order);
                return Err(e);
            }
        }
    }
    Ok((resources, order))
}

/// Invoke `eval_fn`; if the return value is a coroutine, drive it to
/// completion via `asyncio.run`. Returns the raw Python result.
pub(super) fn invoke_eval_fn(
    py: Python,
    eval_fn: &Py<PyAny>,
    call_args: Vec<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    let args_tuple = PyTuple::new(py, &call_args)?;
    let result = eval_fn.call1(py, args_tuple)?;
    let inspect = py.import("inspect")?;
    let is_coro = inspect
        .call_method1("iscoroutine", (result.bind(py),))?
        .is_truthy()?;
    if is_coro {
        let asyncio = py.import("asyncio")?;
        Ok(asyncio.call_method1("run", (result.bind(py),))?.unbind())
    } else {
        Ok(result)
    }
}

/// Tear down resources, swallowing per-resource errors so a misbehaving
/// `teardown()` can't shadow the eval result.
fn teardown_resources(py: Python, resources: &[Py<PyAny>]) {
    for resource in resources {
        let _ = resource.call_method0(py, "teardown");
    }
}

/// Rebuild the resources, resolve config + resource args via the shared
/// precompute path, run `f`, and tear down on ANY exit — a failure would
/// otherwise leak setup resources in the reused loky worker.
fn with_resources<T>(
    py: Python,
    eval_fn: &Py<PyAny>,
    resource_specs: &Bound<'_, PyList>,
    f: impl FnOnce(PrecomputedArgs) -> PyResult<T>,
) -> PyResult<T> {
    let (resources, ordered) = rebuild_resources(py, resource_specs)?;
    let result = precompute_args(py, eval_fn, &resources)
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)
        .and_then(f);
    teardown_resources(py, &ordered);
    result
}

/// Call `eval_fn` with `[ctx, *resource_args]`. Returns the raw Python result
/// (RunRequest / SkipReason / SensorResult / list / None).
fn run_eval(
    py: Python,
    eval_fn: Py<PyAny>,
    resource_specs: &Bound<'_, PyList>,
    build_ctx: impl FnOnce(Python, Option<Py<PyAny>>) -> PyResult<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    with_resources(py, &eval_fn, resource_specs, |pre| {
        let config = pre.config_instance.as_ref().map(|c| c.clone_ref(py));
        let ctx = build_ctx(py, config)?;
        invoke_eval_fn(py, &eval_fn, assemble_call_args(py, ctx, &pre))
    })
}

/// Subprocess wrapper for schedule evaluation. Submitted to loky with a
/// `FuncRef`-wrapped `eval_fn` so the user's function is imported in the worker
/// rather than cloudpickled.
#[pyfunction]
pub fn eval_schedule_in_subprocess(
    py: Python,
    eval_fn: Py<PyAny>,
    schedule_name: String,
    scheduled_execution_time: String,
    resource_specs: Bound<'_, PyList>,
) -> PyResult<Py<PyAny>> {
    run_eval(py, eval_fn, &resource_specs, |py, config| {
        Ok(Py::new(
            py,
            PyScheduleEvaluationContext::new(scheduled_execution_time, schedule_name)
                .with_config(config),
        )?
        .into_any())
    })
}

/// Subprocess wrapper for a run-status sensor tick: calls `eval_fn` once per
/// JSON-encoded run. Returns one entry per run: `None` on success, else the
/// error message.
#[pyfunction]
pub fn eval_run_status_sensor_in_subprocess(
    py: Python,
    eval_fn: Py<PyAny>,
    sensor_name: String,
    events: Vec<String>,
    resource_specs: Bound<'_, PyList>,
) -> PyResult<Vec<Option<String>>> {
    let events = events
        .iter()
        .map(|raw| serde_json::from_str::<RunStatusEvent>(raw))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    with_resources(py, &eval_fn, &resource_specs, |pre| {
        Ok(call_each(py, &eval_fn, &sensor_name, events, &pre))
    })
}

/// Subprocess wrapper for sensor evaluation. Same shape as the schedule
/// wrapper but threads `cursor` / `last_tick_time` into the context.
#[pyfunction]
#[pyo3(signature = (eval_fn, sensor_name, cursor, last_tick_time, resource_specs))]
pub fn eval_sensor_in_subprocess(
    py: Python,
    eval_fn: Py<PyAny>,
    sensor_name: String,
    cursor: Option<String>,
    last_tick_time: Option<f64>,
    resource_specs: Bound<'_, PyList>,
) -> PyResult<Py<PyAny>> {
    run_eval(py, eval_fn, &resource_specs, |py, config| {
        Ok(Py::new(
            py,
            PySensorEvaluationContext::new(sensor_name, cursor, last_tick_time).with_config(config),
        )?
        .into_any())
    })
}
