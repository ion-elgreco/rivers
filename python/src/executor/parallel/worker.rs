//! Rust implementation of the parallel worker step execution.
//!
//! Replaces the Python `_worker_execute_step` in `rivers/worker.py`.
//! Reuses existing Rust functions for result extraction, IO handling, etc.

use std::collections::HashMap;

use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::context::io::{PyInputContext, PyOutputContext};
use crate::result_types::{self, ResultKind};

fn resolve_module_attr(py: Python, module: &str, qualname: &str) -> PyResult<Py<PyAny>> {
    let importlib = py.import("importlib")?;
    let mod_obj = importlib.call_method1("import_module", (module,))?;
    let mut obj = mod_obj.unbind();
    for attr in qualname.split('.') {
        obj = obj.getattr(py, attr)?;
    }
    Ok(obj)
}

fn unwrap_callable(py: Python, obj: &Py<PyAny>) -> Py<PyAny> {
    if let Ok(f) = obj.getattr(py, "_asset_fn") {
        return f;
    }
    if let Ok(f) = obj.getattr(py, "_task_fn") {
        return f;
    }
    obj.clone_ref(py)
}

/// Lightweight function reference that pickles as (module, qualname) strings.
#[pyclass(name = "FuncRef", module = "rivers._core")]
pub struct PyFuncRef {
    module: String,
    qualname: String,
}

#[pymethods]
impl PyFuncRef {
    #[new]
    pub fn new(module: String, qualname: String) -> Self {
        Self { module, qualname }
    }

    fn __reduce__(&self, py: Python) -> PyResult<(Py<PyAny>, (String, String))> {
        let reconstruct = py
            .import("rivers._core")?
            .getattr("_reconstruct_func_ref")?;
        Ok((
            reconstruct.unbind(),
            (self.module.clone(), self.qualname.clone()),
        ))
    }

    fn __repr__(&self) -> String {
        format!("FuncRef({}:{})", self.module, self.qualname)
    }

    fn __call__(&self, py: Python) -> PyResult<Py<PyAny>> {
        let obj = resolve_module_attr(py, &self.module, &self.qualname)?;
        Ok(unwrap_callable(py, &obj))
    }
}

#[pyfunction]
pub fn _reconstruct_func_ref(py: Python, module: String, qualname: String) -> PyResult<Py<PyAny>> {
    let obj = resolve_module_attr(py, &module, &qualname)?;
    Ok(unwrap_callable(py, &obj))
}

/// Lightweight IO handler reference that reconstructs from the asset definition.
#[pyclass(name = "IOHandlerRef", module = "rivers._core")]
pub struct PyIOHandlerRef {
    module: String,
    qualname: String,
}

#[pymethods]
impl PyIOHandlerRef {
    #[new]
    pub fn new(module: String, qualname: String) -> Self {
        Self { module, qualname }
    }

    fn __reduce__(&self, py: Python) -> PyResult<(Py<PyAny>, (String, String))> {
        let reconstruct = py
            .import("rivers._core")?
            .getattr("_reconstruct_io_handler_ref")?;
        Ok((
            reconstruct.unbind(),
            (self.module.clone(), self.qualname.clone()),
        ))
    }
}

/// Follows node_io_handler → io_handler → None.
#[pyfunction]
pub fn _reconstruct_io_handler_ref(
    py: Python,
    module: String,
    qualname: String,
) -> PyResult<Py<PyAny>> {
    let obj = resolve_module_attr(py, &module, &qualname)?;
    if let Ok(nio) = obj.getattr(py, "node_io_handler")
        && !nio.is_none(py)
    {
        return Ok(nio);
    }
    if let Ok(io) = obj.getattr(py, "io_handler")
        && !io.is_none(py)
    {
        return Ok(io);
    }
    Ok(py.None())
}

/// Describes how to load an upstream input from an IO handler in the worker.
#[pyclass(name = "IOLoadSpec", module = "rivers._core")]
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
#[pyclass(name = "CollectLoadSpec", module = "rivers._core")]
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
#[pyclass(name = "CollectStreamLoadSpec", module = "rivers._core")]
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
/// `(name: str, is_observation: bool, metadata: dict | None, data_version: str | None)`.
/// For multi-asset, one tuple per selected output. For single-output, one
/// tuple with `name = ""` (the consumer substitutes the dispatch-side
/// `step_name` for non-mapped runs and the instance name for mapped fan-out).
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
#[pyclass(name = "WorkerResult", module = "rivers._core")]
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
}

#[pymethods]
impl PyWorkerResult {
    #[new]
    #[pyo3(signature = (outputs, captured_logs=None, dynamic_keys=None, failed_partitions=vec![]))]
    fn new(
        outputs: Py<PyAny>,
        captured_logs: Option<(String, String, String)>,
        dynamic_keys: Option<Vec<String>>,
        failed_partitions: Vec<(String, String)>,
    ) -> Self {
        Self {
            outputs,
            captured_logs,
            dynamic_keys,
            failed_partitions,
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
            ),
        ))
    }
}

// ---------------------------------------------------------------------------
// Partition pickle reconstruction helpers
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn _reconstruct_partition_key(py: Python, data: Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
    let core = py.import("rivers._core")?;
    let variant: String = data.get_item("variant")?.unwrap().extract()?;
    let cls = core.getattr("PartitionKey")?.getattr(variant.as_str())?;
    match variant.as_str() {
        "Single" => {
            let key = data.get_item("key")?.unwrap();
            Ok(cls.call1((key,))?.unbind())
        }
        "Multi" => {
            let keys = data.get_item("keys")?.unwrap();
            Ok(cls.call1((keys,))?.unbind())
        }
        "Set" => {
            let keys = data.get_item("keys")?.unwrap();
            Ok(cls.call1((keys,))?.unbind())
        }
        _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Unknown PartitionKey variant: {variant}"
        ))),
    }
}

#[pyfunction]
pub fn _reconstruct_partitions_definition(
    py: Python,
    data: Bound<'_, PyDict>,
) -> PyResult<Py<PyAny>> {
    let core = py.import("rivers._core")?;
    let variant: String = data.get_item("variant")?.unwrap().extract()?;
    let cls = core
        .getattr("PartitionsDefinition")?
        .getattr(variant.as_str())?;
    let kwargs = PyDict::new(py);
    match variant.as_str() {
        "Static" => {
            kwargs.set_item("keys", data.get_item("keys")?.unwrap())?;
        }
        "TimeWindow" => {
            for key in ["cron_schedule", "interval_seconds", "start", "end", "fmt"] {
                kwargs.set_item(key, data.get_item(key)?.unwrap())?;
            }
        }
        "Multi" => {
            kwargs.set_item("dimensions", data.get_item("dimensions")?.unwrap())?;
        }
        "Dynamic" => {
            kwargs.set_item("name", data.get_item("name")?.unwrap())?;
        }
        _ => {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Unknown PartitionsDefinition variant: {variant}"
            )));
        }
    }
    Ok(cls.call((), Some(&kwargs))?.unbind())
}

#[pyfunction]
pub fn _reconstruct_partition_mapping(py: Python, data: Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
    let core = py.import("rivers._core")?;
    let variant: String = data.get_item("variant")?.unwrap().extract()?;
    let cls = core.getattr("PartitionMapping")?;
    match variant.as_str() {
        "Identity" => Ok(cls.call_method0("identity")?.unbind()),
        "AllPartitions" => Ok(cls.call_method0("all_partitions")?.unbind()),
        "Static" => {
            let mapping = data.get_item("mapping")?.unwrap();
            Ok(cls.call_method1("static_", (mapping,))?.unbind())
        }
        "TimeWindow" => {
            let offset = data.get_item("offset")?.unwrap();
            Ok(cls.call_method1("time_window", (offset,))?.unbind())
        }
        "Multi" => {
            let dims = data.get_item("dimension_mappings")?.unwrap();
            Ok(cls.call_method1("multi", (dims,))?.unbind())
        }
        "MultiToSingle" => {
            let dim_name = data.get_item("dimension_name")?.unwrap();
            let mapping = data.get_item("partition_mapping")?.unwrap();
            Ok(cls
                .call_method1("multi_to_single", (dim_name, mapping))?
                .unbind())
        }
        "SpecificPartitions" => {
            let keys = data.get_item("partition_keys")?.unwrap();
            Ok(cls.call_method1("specific_partitions", (keys,))?.unbind())
        }
        "ForKeys" => {
            let selectors = data.get_item("selectors")?.unwrap();
            Ok(cls.call_method1("for_keys", (selectors,))?.unbind())
        }
        "Subset" => Ok(cls.call_method0("subset")?.unbind()),
        _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Unknown PartitionMapping variant: {variant}"
        ))),
    }
}

#[pyfunction]
pub fn _reconstruct_partition_context(py: Python, data: Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
    let ctx_cls = py.import("rivers._core")?.getattr("PartitionContext")?;
    let keys = data.get_item("keys")?.unwrap();
    let definition = data.get_item("definition")?.unwrap();
    Ok(ctx_cls.call1((keys, definition))?.unbind())
}

/// Lazy iterator for _CollectStreamLoadSpec resolution.
/// Each __next__ loads one map instance result from IO.
#[pyclass(module = "rivers._core")]
pub struct WorkerCollectStreamIter {
    specs: Vec<Py<PyAny>>, // list of _IOLoadSpec Python objects
    index: usize,
}

#[pymethods]
impl WorkerCollectStreamIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.index >= self.specs.len() {
            return Err(PyStopIteration::new_err(()));
        }
        let spec = &self.specs[self.index];
        self.index += 1;
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
use crate::executor::ops::{self, metadata_to_pickle_safe_dict, write_output};
use crate::metadata::MetadataValue;

/// Returns None if metadata is empty.
fn metadata_vec_to_pickle_safe(
    py: Python,
    metadata: &[(String, crate::metadata::MetadataValue)],
) -> PyResult<Option<Py<PyAny>>> {
    if metadata.is_empty() {
        return Ok(None);
    }
    let d = PyDict::new(py);
    for (k, v) in metadata {
        d.set_item(k, v.clone().into_pyobject(py)?)?;
    }
    Ok(Some(metadata_to_pickle_safe_dict(py, &d)?))
}

/// Self-contained step execution for loky subprocess.
///
/// Replaces Python `_worker_execute_step`. Reuses Rust functions for
/// result type extraction, DynamicOutput unwrapping, and IO handling.
#[pyfunction]
#[pyo3(signature = (func, args, context_kwargs, resource_specs, io_handler, output_context_kwargs, multi_output_specs=None))]
pub fn worker_execute_step(
    py: Python,
    func: Py<PyAny>,
    args: Bound<'_, PyList>,
    context_kwargs: Option<Py<PyAny>>,
    resource_specs: Bound<'_, PyList>,
    io_handler: Option<Py<PyAny>>,
    output_context_kwargs: Option<Py<PyAny>>,
    multi_output_specs: Option<Bound<'_, PyList>>,
) -> PyResult<Py<PyAny>> {
    // Per-step capture: install the proxy writers (idempotent, child-process
    // local) and start a fresh StepCapture / Rust log capture pair around the
    // user code so prints + tracing logs land in the orchestrator's
    // `LogOutput` event stream.
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
            if arg.extract::<PyRef<'_, PyIOLoadSpec>>().is_ok() {
                load_from_spec(py, &arg.unbind())
            } else if arg.extract::<PyRef<'_, PyCollectLoadSpec>>().is_ok() {
                // Barrier collect: load all specs into a list
                let specs: Vec<Py<PyAny>> = arg.getattr("specs")?.extract()?;
                let items: Vec<Py<PyAny>> = specs
                    .iter()
                    .map(|spec| load_from_spec(py, spec))
                    .collect::<PyResult<_>>()?;
                let list = PyList::new(py, items.iter().map(|v| v.bind(py)))?;
                Ok(list.unbind().into_any())
            } else if arg.extract::<PyRef<'_, PyCollectStreamLoadSpec>>().is_ok() {
                // Streaming collect: return lazy iterator
                let specs: Vec<Py<PyAny>> = arg.getattr("specs")?.extract()?;
                let iter = WorkerCollectStreamIter { specs, index: 0 };
                Ok(Py::new(py, iter)?.into_any())
            } else {
                Ok(arg.unbind())
            }
        })
        .collect::<PyResult<_>>()?;

    let mut resolved: Vec<Py<PyAny>> = resolved_args;

    let mut deserialized_resources: Vec<Py<PyAny>> = Vec::new();
    for item in resource_specs.iter() {
        let tuple: &Bound<PyTuple> = item.cast()?;
        let param_idx: usize = tuple.get_item(0)?.extract()?;
        let cls = tuple.get_item(1)?;
        let json_data = tuple.get_item(2)?;
        let resource = cls.call_method1("model_validate_json", (&json_data,))?;
        resource.call_method0("setup")?;
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

    // Execute with resource teardown guarantee. Closure returns the outputs
    // list plus any single-output `dynamic_keys` (multi-asset can't carry
    // DynamicOutputs). The wrapping `PyWorkerResult` is built outside so it
    // can carry the captured logs alongside.
    let outputs_or_err = (|| -> PyResult<(Py<PyAny>, Option<Vec<String>>)> {
        let args_tuple = PyTuple::new(py, &resolved)?;
        let raw_result = func.call1(py, args_tuple)?;

        // Output items list shipped back to the orchestrator. One tuple per
        // selected output: (name, is_observation, metadata_dict_or_none, dv).
        let outputs_list = PyList::empty(py);
        let mut single_dynamic_keys: Option<Vec<String>> = None;

        if let Some(ref specs) = multi_output_specs {
            // Multi-asset: build a synthetic StepResult and drive the shared
            // iterator. Per-yield ctx peek/drain happens inside
            // `for_each_output_generator`; for dict we drain once up-front.
            let mut spec_map: HashMap<String, (Py<PyAny>, Py<PyAny>)> = HashMap::new();
            let mut selected_outputs: Vec<String> = Vec::new();
            for spec in specs.iter() {
                let spec_tuple: &Bound<PyTuple> = spec.cast()?;
                let name: String = spec_tuple.get_item(0)?.extract()?;
                let handler = spec_tuple.get_item(1)?.unbind();
                let kwargs = spec_tuple.get_item(2)?.unbind();
                spec_map.insert(name.clone(), (handler, kwargs));
                selected_outputs.push(name);
            }

            let is_gen: bool = py
                .import("inspect")?
                .call_method1("isgenerator", (&raw_result,))?
                .is_truthy()?;

            // Dict path: drain ctx once for shared metadata + dv. Gen path:
            // for_each_output_generator handles per-yield peek/drain itself,
            // so we leave shared empty (matches the in_process gen path which
            // also ignores step_result.output_metadata for generators).
            let (shared_metadata, shared_dv) = if is_gen {
                (Vec::new(), None)
            } else {
                drain_ctx_state(py, ctx_obj.as_ref())?
            };

            // Worker only handles sync code (async funcs are delegated to
            // AsyncBackend in the main process), so an async generator is
            // impossible here.
            let synth = ops::StepResult {
                result: raw_result,
                return_hint: None,
                output_metadata: shared_metadata,
                data_version: shared_dv,
                config_instance: None,
                tags: None,
                dynamic_keys: None,
                result_kind: ResultKind::Output,
                failed_partitions: Vec::new(),
                generator: is_gen.then(|| ops::GeneratorType::Sync {
                    context: ctx_obj.as_ref().map(|c| c.clone_ref(py)),
                }),
            };

            ops::for_each_output(py, &synth, &selected_outputs, "", None, |py, item| {
                match item {
                    ops::OutputItem::Materialization {
                        name,
                        value,
                        metadata,
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
                            let out_dict: &Bound<PyDict> = out_kwargs.bind(py).cast()?;
                            let out_ctx = Py::new(py, PyOutputContext::from_kwargs(py, out_dict)?)?;
                            let io_dv = write_output(py, handler, &out_ctx, &value)?;
                            (io_dv.or(data_version), ResultKind::Output)
                        } else {
                            (data_version, ResultKind::Materialization)
                        };
                        let meta_py = metadata_vec_to_pickle_safe(py, &metadata)?;
                        append_output_tuple(py, &outputs_list, &name, kind, meta_py, final_dv)?;
                    }
                    ops::OutputItem::Observation {
                        name,
                        metadata,
                        data_version,
                    } => {
                        let meta_py = metadata_vec_to_pickle_safe(py, &metadata)?;
                        append_output_tuple(
                            py,
                            &outputs_list,
                            &name,
                            ResultKind::Observation,
                            meta_py,
                            data_version,
                        )?;
                    }
                }
                Ok(())
            })?;
        } else {
            // Single-output: process the raw result the same way the in-
            // process path does (Output / Materialization wrapper extraction,
            // ctx drain, DynamicOutput unwrap), then drive the shared
            // iterator with an empty selected_outputs (single shape).
            let extracted = result_types::try_extract_result_type(py, &raw_result)?;
            let (mut actual_result, result_metadata, result_dv, result_kind) =
                if let Some(ext) = extracted {
                    let val = ext.value.unwrap_or_else(|| py.None());
                    (val, ext.metadata, ext.data_version, ext.kind)
                } else {
                    (raw_result, Vec::new(), None, ResultKind::Output)
                };

            let (mut shared_metadata, ctx_dv) = drain_ctx_state(py, ctx_obj.as_ref())?;
            ops::merge_metadata(&mut shared_metadata, &result_metadata);
            // result-type dv wins over ctx-set dv (matches process_raw_result
            // in invoke.rs).
            let shared_dv = result_dv.or(ctx_dv);

            let dynamic_keys = match result_types::try_unwrap_dynamic_outputs(py, &actual_result)? {
                Some((values, keys)) => {
                    actual_result = values;
                    Some(keys)
                }
                None => None,
            };
            // Surface to outer scope so PyWorkerResult can ship it back; the
            // orchestrator persists keys to KV after the worker returns.
            single_dynamic_keys = dynamic_keys.clone();

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

            let synth = ops::StepResult {
                result: actual_result,
                return_hint: None,
                output_metadata: shared_metadata,
                data_version: shared_dv,
                config_instance: None,
                tags: None,
                dynamic_keys: dynamic_keys.clone(),
                result_kind,
                generator: None,
                failed_partitions: Vec::new(),
            };

            ops::for_each_output(py, &synth, &[], &asset_name, None, |py, item| {
                match item {
                    ops::OutputItem::Materialization {
                        name,
                        value,
                        metadata,
                        data_version,
                    } => {
                        let (final_dv, kind) = if let Some(value) = value {
                            let final_dv = if let (Some(handler), Some(out_kwargs)) =
                                (&io_handler, &output_context_kwargs)
                            {
                                let out_dict: &Bound<PyDict> = out_kwargs.bind(py).cast()?;
                                let out_ctx =
                                    Py::new(py, PyOutputContext::from_kwargs(py, out_dict)?)?;
                                let io_dv = write_output(py, handler, &out_ctx, &value)?;
                                io_dv.or(data_version)
                            } else {
                                data_version
                            };
                            (final_dv, ResultKind::Output)
                        } else {
                            (data_version, ResultKind::Materialization)
                        };
                        let meta_py = metadata_vec_to_pickle_safe(py, &metadata)?;
                        append_output_tuple(py, &outputs_list, &name, kind, meta_py, final_dv)?;
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

        Ok((outputs_list.unbind().into_any(), single_dynamic_keys))
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

    match outputs_or_err {
        Ok((outputs, dynamic_keys)) => {
            let failed_partitions = drain_ctx_failed_partitions(py, ctx_obj.as_ref());
            let wr = PyWorkerResult {
                outputs,
                captured_logs,
                dynamic_keys,
                failed_partitions,
            };
            Ok(Py::new(py, wr)?.into_any())
        }
        Err(err) => {
            // Stash captured logs on the exception itself so the parent can
            // recover them after `future.result()` re-raises. cloudpickle
            // preserves arbitrary attributes on most exception types.
            if let Some(logs) = captured_logs {
                let _ = err.value(py).setattr("_rivers_captured_logs", logs);
            }
            Err(err)
        }
    }
}

/// Drain `output_metadata` and `data_version` from a worker-side
/// AssetExecutionContext (if present and the right type).
fn drain_ctx_state(
    py: Python,
    ctx_obj: Option<&Py<PyAny>>,
) -> PyResult<(Vec<(String, MetadataValue)>, Option<String>)> {
    if let Some(ctx) = ctx_obj
        && let Ok(bound) = ctx.bind(py).cast::<PyAssetExecutionContext>()
    {
        let ctx_borrow = bound.borrow();
        let metadata = ctx_borrow.drain_output_metadata();
        let dv = ctx_borrow.drain_data_version();
        return Ok((metadata, dv));
    }
    Ok((Vec::new(), None))
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

/// Append `(name, kind_tag, metadata_or_none, dv_or_none)` to the
/// outputs list shipped back over IPC. `kind_tag` is the `ResultKind::as_u8`
/// discriminator: 0 = Output (IO write happened in worker, just emit
/// Materialization on orchestrator), 1 = Observation, 2 = Materialization
/// (no IO write happened, emit Materialization).
fn append_output_tuple(
    py: Python,
    outputs_list: &Bound<'_, PyList>,
    name: &str,
    kind: ResultKind,
    metadata_py: Option<Py<PyAny>>,
    data_version: Option<String>,
) -> PyResult<()> {
    let dv_py: Py<PyAny> = match data_version {
        Some(s) => s.into_pyobject(py)?.into_any().unbind(),
        None => py.None(),
    };
    let tuple = PyTuple::new(
        py,
        [
            name.into_pyobject(py)?.into_any().unbind(),
            kind.as_u8().into_pyobject(py)?.into_any().unbind(),
            metadata_py.unwrap_or_else(|| py.None()),
            dv_py,
        ],
    )?;
    outputs_list.append(tuple)?;
    Ok(())
}
