use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::assets::decorator::PyAsset;
use crate::assets::io_handler::IOHandler;
use crate::context::io::{PyInputContext, PyOutputContext};
use crate::task::PyTask;

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
#[pyclass(name = "FuncRef", frozen, module = "rivers._core")]
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

/// A bound method that pickles as its function and owner, and unpickles as
/// the same bound method.
#[pyclass(name = "BoundMethod", frozen, module = "rivers._core")]
pub struct PyBoundMethod {
    func: Py<PyAny>,
    owner: Py<PyAny>,
}

impl PyBoundMethod {
    pub fn new(func: Py<PyAny>, owner: Py<PyAny>) -> Self {
        Self { func, owner }
    }
}

#[pymethods]
impl PyBoundMethod {
    fn __reduce__(&self, py: Python) -> PyResult<(Py<PyAny>, (Py<PyAny>, Py<PyAny>))> {
        let method_type = py.import("types")?.getattr("MethodType")?;
        Ok((
            method_type.unbind(),
            (self.func.clone_ref(py), self.owner.clone_ref(py)),
        ))
    }
}

/// Lightweight IO handler reference that reconstructs from the asset definition.
#[pyclass(name = "IOHandlerRef", frozen, module = "rivers._core")]
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

/// node_io_handler → io_handler → None, the one probe order for both child
/// reconstruction and the parent-side shippability check. Requires the found
/// object to expose `load_input`: on a class-form asset the attribute lookup
/// reaches the `Asset` base's getset descriptors, which are not handlers.
/// On an asset or a task only a handler set at definition counts: a fresh
/// import holds the key of a resource handler, not the handler.
pub(super) fn handler_attr(py: Python, obj: &Py<PyAny>) -> Option<Py<PyAny>> {
    let definition_handler = |h: &IOHandler| match h {
        IOHandler::Instance(handler) => Some(handler.clone_ref(py)),
        IOHandler::ResourceRef(_) | IOHandler::Resource(_) => None,
    };
    if let Ok(asset) = obj.bind(py).cast::<PyAsset>() {
        let asset = asset.get();
        return [asset.inner.node_io_handler(), asset.inner.io_handler()]
            .into_iter()
            .flatten()
            .find_map(definition_handler);
    }
    if let Ok(task) = obj.bind(py).cast::<PyTask>() {
        return task
            .get()
            .inner
            .io_handler
            .as_ref()
            .and_then(definition_handler);
    }
    for attr in ["node_io_handler", "io_handler"] {
        if let Ok(h) = obj.getattr(py, attr)
            && !h.is_none(py)
            && h.bind(py).hasattr("load_input").unwrap_or(false)
        {
            return Some(h);
        }
    }
    None
}

/// Resolve `module.qualname` and find its handler, walking up one qualname
/// segment if the leaf has none — a class-form asset's callable is
/// `Class.method`, and the handler lives on the class.
pub(super) fn resolve_handler_from_path(
    py: Python,
    module: &str,
    qualname: &str,
) -> Option<Py<PyAny>> {
    let obj = resolve_module_attr(py, module, qualname).ok()?;
    if let Some(h) = handler_attr(py, &obj) {
        return Some(h);
    }
    let (parent, _) = qualname.rsplit_once('.')?;
    let parent_obj = resolve_module_attr(py, module, parent).ok()?;
    handler_attr(py, &parent_obj)
}

/// Follows node_io_handler → io_handler on the leaf, then its parent. A path
/// that reaches no handler rebuilds to a `MissingIOHandler`: raising here
/// would fail every pending step of the pool, not only this one.
#[pyfunction]
pub fn _reconstruct_io_handler_ref(
    py: Python,
    module: String,
    qualname: String,
) -> PyResult<Py<PyAny>> {
    match resolve_handler_from_path(py, &module, &qualname) {
        Some(handler) => Ok(handler),
        None => {
            let reference = format!("{module}.{qualname}");
            Ok(Py::new(py, PyMissingIOHandler { reference })?.into_any())
        }
    }
}

/// Stands in for an `IOHandlerRef` that found no handler in the worker, so
/// the step fails instead of skipping the write.
#[pyclass(name = "MissingIOHandler", frozen, module = "rivers._core")]
struct PyMissingIOHandler {
    reference: String,
}

impl PyMissingIOHandler {
    fn error(&self, asset_name: &str) -> PyErr {
        pyo3::exceptions::PyRuntimeError::new_err(format!(
            "asset '{asset_name}': io_handler reference {} found no handler in the worker",
            self.reference
        ))
    }
}

#[pymethods]
impl PyMissingIOHandler {
    fn handle_output(&self, context: PyRef<'_, PyOutputContext>, _obj: Py<PyAny>) -> PyResult<()> {
        Err(self.error(&context.asset_name))
    }

    fn load_input(&self, context: PyRef<'_, PyInputContext>) -> PyResult<Py<PyAny>> {
        Err(self.error(&context.asset_name))
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
