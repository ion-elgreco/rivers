//! Fan-out (map/collect) execution logic for dynamic task parallelism.
//!
//! Implements map, collect, and collect_stream execution modes. Extracts `DynamicOutput`
//! mapping keys from iterable results, bridges Python generators for streaming collect,
//! and loads fan-out source data via IO handlers for downstream map instances.
use std::collections::HashMap;

use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use rivers_core::storage::PartitionKey as CorePartitionKey;
use rivers_core::storage::ScopedStorageHandle;
use rivers_core::storage::surrealdb_backend::SurrealStorage;

use crate::assets::io_handler_registry::IOHandlerRegistry;
use crate::context::io::PyInputContext;
use crate::partitions::PartitionContext;
use crate::partitions::PyPartitionKey;
use crate::repository::resolved_node::ResolvedNode;
use crate::result_types::PyDynamicOutput;
use crate::runtime::io_rt;

/// Extract (mapping_key, value) from a fan-out item.
/// If the item is a DynamicOutput, uses its key and unwraps the value.
/// Otherwise, uses the numeric index as the key and the item as-is.
pub(crate) fn extract_mapping_key(
    py: Python,
    index: usize,
    item: &Py<PyAny>,
) -> (String, Py<PyAny>) {
    if let Ok(dynamic) = item.extract::<PyRef<'_, PyDynamicOutput>>(py) {
        (dynamic.key.clone(), dynamic.value.clone_ref(py))
    } else {
        (index.to_string(), item.clone_ref(py))
    }
}

/// Best-effort persist of a fan-out source's mapping keys via the storage
/// layer. Logs and swallows failures rather than failing the materialization;
/// the downstream fan-out resolver falls back to synthetic indices when KV
/// returns absent.
pub(crate) fn persist_dynamic_keys(
    storage: &ScopedStorageHandle<SurrealStorage>,
    asset_name: &str,
    partition_key: &Option<PyPartitionKey>,
    data_version: &str,
    keys: &[String],
) {
    let partition_core = partition_key.as_ref().map(CorePartitionKey::from);
    let result = io_rt().block_on(storage.scoped().set_dynamic_keys(
        asset_name,
        partition_core.as_ref(),
        data_version,
        keys,
    ));
    if let Err(e) = result {
        tracing::warn!(
            target: "rivers::executor",
            asset = %asset_name,
            error = %e,
            "failed to persist dynamic_keys to KV"
        );
    }
}

/// Resolve the predefined mapping keys for a fan-out source.
///
/// Lookup order:
///
/// 1. `step_dynamic_keys` — per-step `dynamic_keys` produced when the source
///    ran in *this* orchestrator process (free; no IO).
///    - non-empty `Vec` → source ran here with `DynamicOutput`s; use these keys
///      directly.
///    - empty `Vec`     → source ran here with plain values; return `None` so
///      callers fall back to synthetic `extract_mapping_key` indices.
/// 2. KV (rivers-internal storage), keyed by the source's current
///    `data_version` (read from `data_versions` — populated for cross-run
///    resume by `build_resume_state` and for graph deps by
///    `prefill_external_dep_versions`).
///    Each successful materialization writes to its own `data_version`-scoped
///    slot, so concurrent runs of the same asset+partition can't collide and
///    plain-values runs leave no entry to confuse a future read.
///    - kv_get returns the keys → use them.
///    - kv_get returns absent → source ran with plain values (no entry was
///      written) → `None` → synthetic indices.
///    - storage error → log and return `None`.
///    - missing dv (source unknown to this run) → `None` → synthetic indices.
pub(crate) fn resolve_predefined_keys(
    fan_out_source: &str,
    partition_key: &Option<PyPartitionKey>,
    step_dynamic_keys: &HashMap<String, Vec<String>>,
    data_versions: &HashMap<String, String>,
    storage: &ScopedStorageHandle<SurrealStorage>,
) -> Option<Vec<String>> {
    if let Some(keys) = step_dynamic_keys.get(fan_out_source) {
        return if keys.is_empty() {
            None
        } else {
            Some(keys.clone())
        };
    }

    let dv = data_versions.get(fan_out_source)?;
    let partition_core = partition_key.as_ref().map(CorePartitionKey::from);
    let result = io_rt().block_on(storage.scoped().get_dynamic_keys(
        fan_out_source,
        partition_core.as_ref(),
        dv,
    ));
    match result {
        Ok(Some(keys)) if !keys.is_empty() => Some(keys),
        Ok(_) => None,
        Err(e) => {
            tracing::warn!(
                target: "rivers::executor",
                asset = %fan_out_source,
                error = %e,
                "dynamic_keys KV read failed; falling back to synthetic indices"
            );
            None
        }
    }
}

pub(crate) fn load_fan_out_source(
    py: Python,
    source_name: &str,
    source_node: &ResolvedNode,
    partition_key: &Option<PyPartitionKey>,
    registry: &IOHandlerRegistry,
) -> PyResult<Py<PyAny>> {
    let io_handler = registry.for_output(py, source_node);

    let partition = super::build_partition_context(source_node, partition_key)?;

    let ctx = PyInputContext::new(
        source_name.to_string(),
        "fan_out".to_string(),
        source_node.metadata(),
        partition,
        None,
    );
    let ctx_py = Py::new(py, ctx)?;

    io_handler.call_method1(py, "load_input", (ctx_py,))
}

pub(crate) fn collect_mapped_outputs(
    py: Python,
    mapped_step: &str,
    node_map: &HashMap<String, ResolvedNode>,
    partition_key: &Option<PyPartitionKey>,
    registry: &IOHandlerRegistry,
    mapped_instance_keys: &HashMap<String, Vec<String>>,
) -> PyResult<Py<PyAny>> {
    let mapped_node = node_map
        .get(mapped_step)
        .expect("mapped step must be in node_map — invalid plan");
    let keys = mapped_instance_keys
        .get(mapped_step)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let mut items: Vec<Py<PyAny>> = Vec::with_capacity(keys.len());
    for key in keys {
        let instance_name = format!("{}__{}", mapped_step, key);
        items.push(load_fan_out_source(
            py,
            &instance_name,
            mapped_node,
            partition_key,
            registry,
        )?);
    }
    let list = pyo3::types::PyList::new(py, items.iter().map(|v| v.bind(py)))?;
    Ok(list.unbind().into_any())
}

pub(crate) fn collect_mapped_stream(
    py: Python,
    mapped_step: &str,
    _ordered: bool,
    node_map: &HashMap<String, ResolvedNode>,
    partition_key: &Option<PyPartitionKey>,
    registry: &IOHandlerRegistry,
    mapped_instance_keys: &HashMap<String, Vec<String>>,
) -> PyResult<Py<PyAny>> {
    let mapped_node = node_map
        .get(mapped_step)
        .expect("mapped step must be in node_map — invalid plan");
    let keys = mapped_instance_keys
        .get(mapped_step)
        .cloned()
        .unwrap_or_default();
    let iter = MappedResultsIter::new(py, mapped_step, keys, mapped_node, partition_key, registry)?;
    Ok(Py::new(py, iter)?.into_any())
}

/// Lazy iterator that loads each mapped result from IO on __next__.
#[pyclass(module = "rivers._core")]
pub(crate) struct MappedResultsIter {
    mapped_step: String,
    keys: Vec<String>,
    index: usize,
    io_handler: Py<PyAny>,
    metadata: Option<HashMap<String, String>>,
    partition: Option<PartitionContext>,
}

impl MappedResultsIter {
    pub fn new(
        py: Python,
        mapped_step: &str,
        keys: Vec<String>,
        mapped_node: &ResolvedNode,
        partition_key: &Option<PyPartitionKey>,
        registry: &IOHandlerRegistry,
    ) -> PyResult<Self> {
        let io_handler = registry.for_output(py, mapped_node);
        let partition = super::build_partition_context(mapped_node, partition_key)?;
        Ok(Self {
            mapped_step: mapped_step.to_string(),
            keys,
            index: 0,
            io_handler,
            metadata: mapped_node.metadata(),
            partition,
        })
    }
}

#[pymethods]
impl MappedResultsIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.index >= self.keys.len() {
            return Err(PyStopIteration::new_err(()));
        }
        let key = &self.keys[self.index];
        self.index += 1;
        let instance_name = format!("{}__{}", self.mapped_step, key);
        let ctx = Py::new(
            py,
            PyInputContext::new(
                instance_name,
                "collect_stream".to_string(),
                self.metadata.clone(),
                self.partition.clone(),
                None,
            ),
        )?;
        self.io_handler.call_method1(py, "load_input", (ctx,))
    }
}
