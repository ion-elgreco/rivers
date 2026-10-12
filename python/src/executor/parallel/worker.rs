//! Rust implementation of the parallel worker step execution.
//!
//! Replaces the Python `_worker_execute_step` in `rivers/worker.py`.
//! Reuses existing Rust functions for result extraction, IO handling, etc.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::config::rebuild_resource;
use crate::context::io::{PyInputContext, PyOutputContext};
use crate::result_types::ResultKind;

/// Describes how to load an upstream input from an IO handler in the worker.
#[pyclass(name = "IOLoadSpec", frozen, module = "rivers._core")]
pub struct PyIOLoadSpec {
    #[pyo3(get)]
    pub handler: Py<PyAny>,
    #[pyo3(get)]
    pub input_context_kwargs: Py<PyAny>,
}

#[pymethods]
impl PyIOLoadSpec {
    #[new]
    fn new(handler: Py<PyAny>, input_context_kwargs: Py<PyAny>) -> Self {
        Self {
            handler,
            input_context_kwargs,
        }
    }

    fn __reduce__(&self, py: Python) -> PyResult<(Py<PyAny>, (Py<PyAny>, Py<PyAny>))> {
        let cls = py.import("rivers._core")?.getattr("IOLoadSpec")?;
        Ok((
            cls.unbind(),
            (
                self.handler.clone_ref(py),
                self.input_context_kwargs.clone_ref(py),
            ),
        ))
    }
}

/// Describes how to load collected map instance outputs as a list in the worker.
#[pyclass(name = "CollectLoadSpec", frozen, module = "rivers._core")]
pub struct PyCollectLoadSpec {
    #[pyo3(get)]
    pub specs: Py<PyAny>,
}

#[pymethods]
impl PyCollectLoadSpec {
    #[new]
    fn new(specs: Py<PyAny>) -> Self {
        Self { specs }
    }

    fn __reduce__(&self, py: Python) -> PyResult<(Py<PyAny>, (Py<PyAny>,))> {
        let cls = py.import("rivers._core")?.getattr("CollectLoadSpec")?;
        Ok((cls.unbind(), (self.specs.clone_ref(py),)))
    }
}

/// Describes how to load collected map instance outputs as a lazy iterator in the worker.
#[pyclass(name = "CollectStreamLoadSpec", frozen, module = "rivers._core")]
pub struct PyCollectStreamLoadSpec {
    #[pyo3(get)]
    pub specs: Py<PyAny>,
}

#[pymethods]
impl PyCollectStreamLoadSpec {
    #[new]
    fn new(specs: Py<PyAny>) -> Self {
        Self { specs }
    }

    fn __reduce__(&self, py: Python) -> PyResult<(Py<PyAny>, (Py<PyAny>,))> {
        let cls = py
            .import("rivers._core")?
            .getattr("CollectStreamLoadSpec")?;
        Ok((cls.unbind(), (self.specs.clone_ref(py),)))
    }
}

/// Result from worker subprocess. Carries one item per output (per-output
/// metadata + data_version are preserved; values stay in IO).
///
/// `outputs` is a Python list of 4-tuples
/// `(name: str, kind: int, metadata: list[tuple[str, MetadataValue]], data_version: str | None)`.
/// For multi-asset, one tuple per output the step completed. For
/// single-output, one tuple named after the step (the instance for mapped
/// fan-out).
///
/// `captured_logs` carries `(stdout, stderr, rust_logs)` from the worker's
/// per-step capture. None when the user code didn't run (pre-spawn failures
/// surface through the loky exception path, not via `WorkerResult`).
///
/// `dynamic_keys` is the fan-out mapping keys produced by a single-output
/// asset that yielded `DynamicOutput`s. Only set on the single-output path
/// (multi-asset can't carry DynamicOutputs). The orchestrator persists these
/// to KV after collecting the result, keyed by the materialization's
/// `data_version` (loky workers don't write KV themselves — embedded RocksDB
/// is single-process; the orchestrator owns the lock).
#[pyclass(name = "WorkerResult", frozen, module = "rivers._core")]
pub struct PyWorkerResult {
    #[pyo3(get)]
    pub outputs: Py<PyAny>,
    #[pyo3(get)]
    pub captured_logs: Option<(String, String, String)>,
    #[pyo3(get)]
    pub dynamic_keys: Option<Vec<String>>,
    /// `(partition_key_json, error)` for each `mark_partition_failed` mark,
    /// drained from the worker context and carried back to the orchestrator.
    #[pyo3(get)]
    pub failed_partitions: Vec<(String, String)>,
    /// The final task of a graph asset also wrote the graph's output.
    #[pyo3(get)]
    pub graph_written: bool,
    /// The exception the graph's io_handler raised on that write.
    #[pyo3(get)]
    pub graph_error: Option<Py<PyAny>>,
    /// A multi-asset's failure after it completed the `outputs` above: the
    /// orchestrator fails only its other outputs. The [`WorkerStepError`] a
    /// failed step raises, without the logs (`captured_logs` has them).
    #[pyo3(get)]
    pub error: Option<Py<PyAny>>,
}

#[pymethods]
impl PyWorkerResult {
    #[new]
    #[pyo3(signature = (outputs, captured_logs=None, dynamic_keys=None, failed_partitions=vec![], graph_written=false, graph_error=None, error=None))]
    fn new(
        outputs: Py<PyAny>,
        captured_logs: Option<(String, String, String)>,
        dynamic_keys: Option<Vec<String>>,
        failed_partitions: Vec<(String, String)>,
        graph_written: bool,
        graph_error: Option<Py<PyAny>>,
        error: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            outputs,
            captured_logs,
            dynamic_keys,
            failed_partitions,
            graph_written,
            graph_error,
            error,
        }
    }

    fn __reduce__(
        &self,
        py: Python,
    ) -> PyResult<(
        Py<PyAny>,
        (
            Py<PyAny>,
            Option<(String, String, String)>,
            Option<Vec<String>>,
            Vec<(String, String)>,
            bool,
            Option<Py<PyAny>>,
            Option<Py<PyAny>>,
        ),
    )> {
        let cls = py.import("rivers._core")?.getattr("WorkerResult")?;
        Ok((
            cls.unbind(),
            (
                self.outputs.clone_ref(py),
                self.captured_logs.clone(),
                self.dynamic_keys.clone(),
                self.failed_partitions.clone(),
                self.graph_written,
                self.graph_error.as_ref().map(|e| e.clone_ref(py)),
                self.error.as_ref().map(|e| e.clone_ref(py)),
            ),
        ))
    }
}

/// Lazy iterator for _CollectStreamLoadSpec resolution.
/// Each __next__ loads one map instance result from IO; threads that share it
/// each get different results.
#[pyclass(frozen, module = "rivers._core")]
pub struct WorkerCollectStreamIter {
    specs: Vec<Py<PyAny>>, // list of _IOLoadSpec Python objects
    index: AtomicUsize,
}

#[pymethods]
impl WorkerCollectStreamIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let Some(spec) = self.specs.get(self.index.fetch_add(1, Ordering::Relaxed)) else {
            return Err(PyStopIteration::new_err(()));
        };
        load_from_spec(py, spec)
    }
}

fn load_from_spec(py: Python, spec: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    let handler = spec.getattr(py, "handler")?;
    let kwargs = spec.getattr(py, "input_context_kwargs")?;
    let dict: &Bound<PyDict> = kwargs.bind(py).cast()?;
    let ctx = Py::new(py, PyInputContext::from_kwargs(py, dict)?)?;
    handler.call_method1(py, "load_input", (ctx,))
}

use crate::context::asset::PyAssetExecutionContext;
use crate::errors::WorkerStepError;
use crate::executor::dispatch::CapturedLogs;
use crate::executor::ops::{self, OutOfProcess, write_output};
use crate::executor::traceback;
use crate::metadata::MetadataValue;

/// Self-contained step execution for loky subprocess.
///
/// Replaces Python `_worker_execute_step`. Reuses Rust functions for
/// result type extraction, return type validation, DynamicOutput unwrapping,
/// and IO handling. `return_hint` is resolved by the orchestrator: a function
/// shipped by value lacks the module globals a string annotation names.
#[pyfunction]
#[pyo3(signature = (func, step_name, return_hint, args, context_kwargs, resource_specs, io_handler, output_context_kwargs, multi_output_specs=None, graph_output=None))]
pub fn worker_execute_step(
    py: Python,
    func: Py<PyAny>,
    step_name: String,
    return_hint: Option<Py<PyAny>>,
    args: Bound<'_, PyList>,
    context_kwargs: Option<Py<PyAny>>,
    resource_specs: Bound<'_, PyList>,
    io_handler: Option<Py<PyAny>>,
    output_context_kwargs: Option<Py<PyAny>>,
    multi_output_specs: Option<Bound<'_, PyList>>,
    graph_output: Option<(Py<PyAny>, Py<PyAny>)>,
) -> PyResult<Py<PyAny>> {
    // Per-step capture: install the proxy writers (idempotent, child-process
    // local) and start a fresh StepCapture / Rust log capture pair around the
    // user code so prints + tracing logs land in the orchestrator's
    // `run_logs` stream.
    let _ = py
        .import("rivers._capture")
        .and_then(|m| m.call_method0("install"));
    let step_capture = py
        .import("rivers._capture")
        .and_then(|m| m.getattr("StepCapture"))
        .and_then(|cls| cls.call0())
        .ok();
    if let Some(ref c) = step_capture {
        let _ = c.call_method0("start");
        crate::log_capture::start();
    }

    let resolved_args: Vec<Py<PyAny>> = args
        .iter()
        .map(|arg| {
            if arg.is_instance_of::<PyIOLoadSpec>() {
                load_from_spec(py, &arg.unbind())
            } else if arg.is_instance_of::<PyCollectLoadSpec>() {
                // Barrier collect: load all specs into a list
                let specs: Vec<Py<PyAny>> = arg.getattr("specs")?.extract()?;
                let items: Vec<Py<PyAny>> = specs
                    .iter()
                    .map(|spec| load_from_spec(py, spec))
                    .collect::<PyResult<_>>()?;
                let list = PyList::new(py, items.iter().map(|v| v.bind(py)))?;
                Ok(list.unbind().into_any())
            } else if arg.is_instance_of::<PyCollectStreamLoadSpec>() {
                // Streaming collect: return lazy iterator
                let specs: Vec<Py<PyAny>> = arg.getattr("specs")?.extract()?;
                let iter = WorkerCollectStreamIter {
                    specs,
                    index: AtomicUsize::new(0),
                };
                Ok(Py::new(py, iter)?.into_any())
            } else {
                Ok(arg.unbind())
            }
        })
        .collect::<PyResult<_>>()?;

    let mut resolved: Vec<Py<PyAny>> = resolved_args;

    let mut deserialized_resources: Vec<Py<PyAny>> = Vec::new();
    for item in resource_specs.iter() {
        let (param_idx, resource) = rebuild_resource::<usize>(&item)?;
        deserialized_resources.push(resource.clone().unbind());
        resolved[param_idx] = resource.unbind();
    }

    let ctx_obj: Option<Py<PyAny>> = if let Some(ref kwargs) = context_kwargs {
        let ctx_cls = py
            .import("rivers._core")?
            .getattr("AssetExecutionContext")?;
        let dict: &Bound<PyDict> = kwargs.bind(py).cast()?;
        let ctx = ctx_cls.call((), Some(dict))?;
        resolved.insert(0, ctx.clone().unbind());
        Some(ctx.unbind())
    } else {
        None
    };

    // A graph's final task writes its value as the graph's output too; a
    // failed write fails only the graph, so it does not fail this step.
    let mut graph_written = false;
    let mut graph_error: Option<Py<PyAny>> = None;

    // Output items shipped back to the orchestrator, one tuple per output the
    // step completed (see `PyWorkerResult`). Kept outside the closure: a
    // multi-asset that fails partway still reports the outputs before it.
    let outputs_list = PyList::empty(py);

    // Execute with resource teardown guarantee. Closure returns any
    // single-output `dynamic_keys` (multi-asset can't carry DynamicOutputs).
    // The wrapping `PyWorkerResult` is built outside so it can carry the
    // captured logs alongside.
    let outputs_or_err = (|| -> PyResult<Option<Vec<String>>> {
        let args_tuple = PyTuple::new(py, &resolved)?;
        let raw_result = func.call1(py, args_tuple)?;

        // A multi-asset generator: for_each_output_generator handles per-yield
        // peek/drain itself, so shared state stays empty (the in_process gen
        // path skips the return type check and ctx drain too). Worker only
        // handles sync code (async funcs are delegated to AsyncBackend in the
        // main process), so an async generator is impossible here. Any other
        // result is processed like the in-process path.
        let is_gen = multi_output_specs.is_some()
            && py
                .import("inspect")?
                .call_method1("isgenerator", (&raw_result,))?
                .is_truthy()?;
        let synth = if is_gen {
            ops::StepResult {
                result: raw_result,
                return_hint: None,
                output_metadata: Vec::new(),
                data_version: None,
                config_instance: None,
                tags: None,
                dynamic_keys: None,
                result_kind: ResultKind::Output,
                failed_partitions: Vec::new(),
                generator: Some(ops::GeneratorType::Sync {
                    context: ctx_obj.as_ref().map(|c| c.clone_ref(py)),
                }),
            }
        } else {
            ops::process_raw_result(
                py,
                &raw_result,
                return_hint.as_ref(),
                ctx_obj.as_ref(),
                &step_name,
            )?
        };

        let mut single_dynamic_keys: Option<Vec<String>> = None;

        if let Some(ref specs) = multi_output_specs {
            // Multi-asset: drive the shared iterator over the selected outputs.
            // A `None` handler marks an output kept in memory.
            let mut spec_map: HashMap<String, (Option<Py<PyAny>>, Py<PyAny>)> = HashMap::new();
            let mut selected_outputs: Vec<String> = Vec::new();
            for spec in specs.iter() {
                let spec_tuple: &Bound<PyTuple> = spec.cast()?;
                let name: String = spec_tuple.get_item(0)?.extract()?;
                let handler: Option<Py<PyAny>> = spec_tuple.get_item(1)?.extract()?;
                let kwargs = spec_tuple.get_item(2)?.unbind();
                spec_map.insert(name.clone(), (handler, kwargs));
                selected_outputs.push(name);
            }

            ops::for_each_output(py, &synth, &selected_outputs, "", None, |py, item| {
                match item {
                    ops::OutputItem::Materialization {
                        name,
                        value,
                        mut metadata,
                        data_version,
                    } => {
                        // value.is_some() => Output: write IO from the worker
                        // and report tag=Output. value.is_none() => Materialization:
                        // user-managed, report tag=Materialization.
                        let (final_dv, kind) = if let Some(value) = value {
                            let (handler, out_kwargs) = match spec_map.get(&name) {
                                Some(s) => s,
                                None => return Ok(()),
                            };
                            let Some(handler) = handler else {
                                return Err(OutOfProcess::Parallel.in_memory_error(&name));
                            };
                            let out_dict: &Bound<PyDict> = out_kwargs.bind(py).cast()?;
                            let out_ctx = Py::new(py, PyOutputContext::from_kwargs(py, out_dict)?)?;
                            let written = write_output(py, handler, &out_ctx, &value)?;
                            ops::merge_metadata(&mut metadata, &written.metadata);
                            (written.data_version.or(data_version), ResultKind::Output)
                        } else {
                            (data_version, ResultKind::Materialization)
                        };
                        append_output_tuple(&outputs_list, &name, kind, metadata, final_dv)?;
                    }
                    ops::OutputItem::Observation {
                        name,
                        metadata,
                        data_version,
                    } => {
                        append_output_tuple(
                            &outputs_list,
                            &name,
                            ResultKind::Observation,
                            metadata,
                            data_version,
                        )?;
                    }
                }
                Ok(())
            })?;
        } else {
            // Single-output: drive the shared iterator with an empty
            // selected_outputs (single shape). Surface the dynamic keys to the
            // outer scope so PyWorkerResult can ship them back; the
            // orchestrator persists keys to KV after the worker returns.
            single_dynamic_keys = synth.dynamic_keys.clone();

            // Asset name from the OutputContext kwargs — the orchestrator
            // populated this with `step_name` (instance_name for mapped).
            let asset_name = if let Some(ref kwargs) = output_context_kwargs {
                let dict: &Bound<PyDict> = kwargs.bind(py).cast()?;
                dict.get_item("asset_name")?
                    .map(|v| v.extract::<String>())
                    .transpose()?
                    .unwrap_or_default()
            } else {
                String::new()
            };

            ops::for_each_output(py, &synth, &[], &asset_name, None, |py, item| {
                match item {
                    ops::OutputItem::Materialization {
                        name,
                        value,
                        mut metadata,
                        data_version,
                    } => {
                        let (final_dv, kind) = if let Some(value) = value {
                            let (Some(handler), Some(out_kwargs)) =
                                (&io_handler, &output_context_kwargs)
                            else {
                                return Err(OutOfProcess::Parallel.in_memory_error(&name));
                            };
                            let out_dict: &Bound<PyDict> = out_kwargs.bind(py).cast()?;
                            let out_ctx = Py::new(py, PyOutputContext::from_kwargs(py, out_dict)?)?;
                            let written = write_output(py, handler, &out_ctx, &value)?;
                            ops::merge_metadata(&mut metadata, &written.metadata);
                            let final_dv = written.data_version.or(data_version);
                            if let Some((graph_handler, graph_kwargs)) = &graph_output {
                                let graph_dict: &Bound<PyDict> = graph_kwargs.bind(py).cast()?;
                                let graph_ctx =
                                    Py::new(py, PyOutputContext::from_kwargs(py, graph_dict)?)?;
                                match write_output(py, graph_handler, &graph_ctx, &value) {
                                    Ok(_) => graph_written = true,
                                    Err(e) => graph_error = Some(e.into_value(py).into_any()),
                                }
                            }
                            (final_dv, ResultKind::Output)
                        } else {
                            (data_version, ResultKind::Materialization)
                        };
                        append_output_tuple(&outputs_list, &name, kind, metadata, final_dv)?;
                    }
                    ops::OutputItem::Observation { .. } => {
                        // External-asset observations route through a separate
                        // orchestrator path before the worker is called.
                        unreachable!("single-output worker path does not produce Observation");
                    }
                }
                Ok(())
            })?;
        }

        Ok(single_dynamic_keys)
    })();

    // Teardown unconditionally, including on failure.
    for resource in &deserialized_resources {
        let _ = resource.call_method0(py, "teardown");
    }

    // Always finish capture — same payload on success and failure paths.
    let captured_logs: Option<(String, String, String)> = step_capture.and_then(|c| {
        let stdout_stderr = c
            .call_method0("finish")
            .and_then(|t| t.extract::<(String, String)>())
            .unwrap_or_default();
        let rust_logs = crate::log_capture::take();
        if stdout_stderr.0.is_empty() && stdout_stderr.1.is_empty() && rust_logs.is_empty() {
            None
        } else {
            Some((stdout_stderr.0, stdout_stderr.1, rust_logs))
        }
    });

    let (dynamic_keys, error) = match outputs_or_err {
        Ok(dynamic_keys) => (dynamic_keys, None),
        Err(err) => {
            let package = traceback::app_package(func.bind(py));
            let tb = traceback::capture_json(py, &err, package.as_deref());
            // Only a multi-asset completes outputs before it fails; it keeps
            // them, as in-process.
            if outputs_list.is_empty() {
                return Err(wrap_step_error(py, err, captured_logs, tb));
            }
            let error = wrap_step_error(py, err, None, tb);
            (None, Some(error.into_value(py).into_any()))
        }
    };
    let wr = PyWorkerResult {
        outputs: outputs_list.unbind().into_any(),
        captured_logs,
        dynamic_keys,
        failed_partitions: drain_ctx_failed_partitions(py, ctx_obj.as_ref()),
        graph_written,
        graph_error,
        error,
    };
    Ok(Py::new(py, wr)?.into_any())
}

/// Wrap a failed step's exception for the trip back to the parent (see
/// [`WorkerStepError`]). The exception becomes the wrapper's cause, so loky's
/// remote traceback text still shows the step's own frames.
fn wrap_step_error(
    py: Python,
    err: PyErr,
    captured_logs: Option<(String, String, String)>,
    traceback: Option<String>,
) -> PyErr {
    let wrapper = WorkerStepError::new_err(err.to_string());
    let value = wrapper.value(py);
    let attached = value
        .setattr("step_error", err.value(py))
        .and_then(|()| value.setattr("captured_logs", captured_logs))
        .and_then(|()| value.setattr("traceback", traceback))
        .and_then(|()| value.setattr("__cause__", err.value(py)));
    match attached {
        Ok(()) => wrapper,
        Err(_) => err,
    }
}

/// The step's own exception and captured logs from a failed worker step (see
/// [`WorkerStepError`]). The step's traceback goes back onto the exception for
/// [`traceback::capture_json`], and loky's remote traceback becomes its cause,
/// as loky itself sets it on a plain exception. Other errors (pre-spawn, a
/// dead worker) pass through without logs.
pub(crate) fn unwrap_worker_error(py: Python, error: PyErr) -> (PyErr, CapturedLogs) {
    if !error.is_instance_of::<WorkerStepError>(py) {
        return (error, None);
    }
    let wrapper = error.value(py);
    let Ok(step_error) = wrapper.getattr("step_error") else {
        return (error, None);
    };
    let captured_logs = wrapper
        .getattr("captured_logs")
        .ok()
        .and_then(|logs| logs.extract().ok())
        .flatten();
    if let Ok(tb) = wrapper.getattr("traceback")
        && !tb.is_none()
    {
        let _ = step_error.setattr(traceback::STASH_ATTR, tb);
    }
    if let Ok(cause) = wrapper.getattr("__cause__")
        && !cause.is_none()
    {
        let _ = step_error.setattr("__cause__", cause);
    }
    (PyErr::from_value(step_error), captured_logs)
}

/// Drain `mark_partition_failed` marks from a worker-side context, serialized as
/// `(partition_key_json, error)` for transport back to the orchestrator.
fn drain_ctx_failed_partitions(py: Python, ctx_obj: Option<&Py<PyAny>>) -> Vec<(String, String)> {
    if let Some(ctx) = ctx_obj
        && let Ok(bound) = ctx.bind(py).cast::<PyAssetExecutionContext>()
    {
        return bound
            .borrow()
            .drain_failed_backfill_partitions()
            .into_iter()
            .map(|(pk, err)| (rivers_core::storage::PartitionKey::from(&pk).to_json(), err))
            .collect();
    }
    Vec::new()
}

/// Append `(name, kind_tag, metadata, dv_or_none)` to the outputs list
/// shipped back over IPC. `kind_tag` is the `ResultKind::as_u8`
/// discriminator: 0 = Output (IO write happened in worker, just emit
/// Materialization on orchestrator), 1 = Observation, 2 = Materialization
/// (no IO write happened, emit Materialization). The metadata stays
/// `MetadataValue`s, which pickle with their variant.
fn append_output_tuple(
    outputs_list: &Bound<'_, PyList>,
    name: &str,
    kind: ResultKind,
    metadata: Vec<(String, MetadataValue)>,
    data_version: Option<String>,
) -> PyResult<()> {
    outputs_list.append((name, kind.as_u8(), metadata, data_version))
}
