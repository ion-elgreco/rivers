//! IO operations — load_input and handle_output via IO handler dispatch.
//!
//! Defers handler selection to [`IOHandlerRegistry`] (one seam for the chain
//! `node.io_handler() → upstream.io_handler() → default`); this module just
//! builds the `OutputContext` / `InputContext` and delegates to the handler's
//! Python methods. Handles partition key mapping for upstream dependencies
//! via `PartitionMapping`.
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::assets::io_handler_registry::IOHandlerRegistry;
use crate::assets::self_dependency::PySelfDependency;
use crate::context::io::{PyInputContext, PyOutputContext};
use crate::errors::PartitionValidationError;
use crate::metadata::MetadataValue;
use crate::partitions::mapping::UpstreamKeyResolution;
use crate::partitions::{PartitionContext, PartitionMapping, PyPartitionKey};
use crate::repository::resolved_node::ResolvedNode;

pub(crate) fn build_partition_context(
    node: &ResolvedNode,
    partition_key: &Option<PyPartitionKey>,
) -> PyResult<Option<PartitionContext>> {
    let partition_key = match partition_key.as_ref() {
        Some(k) => k,
        None => return Ok(None),
    };
    let def = match node.partitions_def() {
        Some(d) => d,
        None => {
            return Err(PartitionValidationError::new_err(format!(
                "Node '{}' is not partitioned, but a partition_key was provided",
                node.name()?
            )));
        }
    };
    if !def.validate_partition_key(partition_key)? {
        return Err(PartitionValidationError::new_err(format!(
            "Partition key {:?} is not valid for node '{}'",
            partition_key,
            node.name()?
        )));
    }
    Ok(Some(PartitionContext::new(
        partition_key.clone(),
        def.clone(),
    )))
}

/// Build a PartitionContext for a mapped upstream key.
/// Unlike `build_partition_context`, this skips validation since the mapped key
/// may not match the upstream's full partition definition (e.g. a Single key
/// for a Multi-partitioned upstream via multi_to_single).
pub(crate) fn build_mapped_partition_context(
    node: &ResolvedNode,
    partition_key: &Option<PyPartitionKey>,
) -> PyResult<Option<PartitionContext>> {
    let key = match partition_key {
        Some(k) => k,
        None => return Ok(None),
    };
    let def = match node.partitions_def() {
        Some(d) => d,
        None => {
            return Err(PartitionValidationError::new_err(format!(
                "Node '{}' is not partitioned, but a partition_key was provided",
                node.name()?
            )));
        }
    };
    Ok(Some(PartitionContext::new(key.clone(), def.clone())))
}

/// Apply the downstream node's partition_mapping to decide how to load the upstream.
///
/// Thin wrapper that flattens ResolvedNode-derived state into values and delegates
/// to `PartitionMapping::resolve_upstream_key`. No mapping defaults to Identity.
pub(crate) fn map_partition_key_for_upstream(
    upstream_name: &str,
    downstream_node: &ResolvedNode,
    upstream_node: &ResolvedNode,
    partition_key: &Option<PyPartitionKey>,
) -> PyResult<UpstreamKeyResolution> {
    let mapping = downstream_node
        .partition_mapping()
        .and_then(|m| m.get(upstream_name).cloned());
    let down_def = downstream_node.partitions_def();
    let up_def = upstream_node.partitions_def();

    let identity = PartitionMapping::Identity {};
    let m = mapping.as_ref().unwrap_or(&identity);
    m.resolve_upstream_key(partition_key.as_ref(), down_def, up_def)
        .map_err(|e| PartitionValidationError::new_err(e.to_string()))
}

/// Load the asset's own previous output for a self-dependency parameter.
/// Returns a `SelfDependency` instance with the loaded value (or None on first run).
pub(crate) fn load_self_dependency(
    py: Python,
    step_name: &str,
    node: &ResolvedNode,
    partition_key: &Option<PyPartitionKey>,
    annotation: &Bound<PyAny>,
    registry: &IOHandlerRegistry,
    fallback_handler: Option<&Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    let handler = registry.for_self_dependency(py, node, step_name, fallback_handler)?;

    let type_hint = annotation.getattr("__args__")?.get_item(0)?.unbind();

    let metadata = node.metadata();
    let partition = build_partition_context(node, partition_key)?;
    let ctx = PyInputContext {
        asset_name: step_name.to_string(),
        downstream_asset: step_name.to_string(),
        asset_metadata: metadata,
        partition,
        type_hint: Some(type_hint.clone_ref(py)),
    };

    // Try loading; if fails (first run / table doesn't exist), inner = None
    let inner = handler.call_method1(py, "load_input", (ctx,)).ok();

    Ok(Py::new(py, PySelfDependency { inner })?.into_any())
}

/// Walks the registry chain `downstream.input → upstream → default`.
pub(crate) fn load_upstream_input(
    py: Python,
    param_name: &str,
    downstream_name: &str,
    downstream_node: &ResolvedNode,
    upstream_node: &ResolvedNode,
    partition_key: &Option<PyPartitionKey>,
    type_hint: Py<PyAny>,
    registry: &IOHandlerRegistry,
) -> PyResult<Py<PyAny>> {
    // Resolution may produce Skip for ForKeys/Subset mappings.
    let resolution =
        map_partition_key_for_upstream(param_name, downstream_node, upstream_node, partition_key)?;

    let upstream_key = match resolution {
        UpstreamKeyResolution::Skip => return Ok(py.None()),
        UpstreamKeyResolution::Load(key) => key,
    };

    let handler = registry.for_upstream_input(py, downstream_node, upstream_node, param_name);
    let metadata = downstream_node
        .input_metadata(py, param_name)
        .or_else(|| upstream_node.metadata());
    let has_mapping = downstream_node
        .partition_mapping()
        .and_then(|m| m.get(param_name).cloned())
        .is_some();
    // When a mapping was applied, the mapped key may not match the upstream's
    // full partition definition (e.g. multi_to_single produces a Single key for
    // a Multi-partitioned upstream). Build context without validation in that case.
    let partition = if has_mapping {
        build_mapped_partition_context(upstream_node, &upstream_key)?
    } else {
        build_partition_context(upstream_node, &upstream_key)?
    };
    let ctx = PyInputContext {
        asset_name: param_name.to_string(),
        downstream_asset: downstream_name.to_string(),
        asset_metadata: metadata,
        partition,
        type_hint: Some(type_hint),
    };
    handler.call_method1(py, "load_input", (ctx,))
}

/// Write a result to IO via a handler. Returns the data_version registered by
/// the IO handler. This is the shared core used by both the in-process
/// executor and the parallel worker.
///
/// Fan-out `dynamic_keys` indices live in rivers-internal KV, not in user IO —
/// the orchestrator writes them via `kv_set` after this function returns,
/// keyed by the resulting `data_version`.
pub(crate) fn write_output(
    py: Python,
    handler: &Py<PyAny>,
    out_ctx: &Py<PyOutputContext>,
    result: &Py<PyAny>,
) -> PyResult<Option<String>> {
    handler.call_method1(py, "handle_output", (out_ctx.clone_ref(py), result))?;
    let data_version = out_ctx.borrow(py).drain_data_version();
    Ok(data_version)
}

/// Returns the data_version registered by the IO handler via
/// `output_context.register_data_version()`. Walks the registry chain
/// `node.io_handler() → default`.
pub(crate) fn handle_step_output(
    py: Python,
    step_name: &str,
    node: &ResolvedNode,
    result: &Py<PyAny>,
    partition_key: &Option<PyPartitionKey>,
    return_hint: Option<&Py<PyAny>>,
    output_metadata: Vec<(String, MetadataValue)>,
    registry: &IOHandlerRegistry,
) -> PyResult<Option<String>> {
    let handler = registry.for_output(py, node);
    let metadata = node.metadata();
    let partition = build_partition_context(node, partition_key)?;
    let ctx = Py::new(
        py,
        PyOutputContext::new_with_metadata(
            step_name.to_string(),
            metadata,
            partition,
            return_hint.map(|h| h.clone_ref(py)),
            output_metadata,
        ),
    )?;
    write_output(py, &handler, &ctx, result)
}

/// Convert metadata values to pickle-safe raw Python values.
/// MetadataValue objects get unwrapped via `raw_value()`; unpicklable values
/// fall back to `repr()`. Used when metadata must cross process boundaries.
pub(crate) fn metadata_to_pickle_safe_dict(
    py: Python,
    meta: &Bound<'_, PyDict>,
) -> PyResult<Py<PyAny>> {
    let pickle = py.import("pickle")?;
    let result = PyDict::new(py);
    for (k, v) in meta.iter() {
        if let Ok(raw_value_method) = v.getattr("raw_value") {
            let raw = raw_value_method.call0()?;
            match pickle.call_method1("dumps", (&raw,)) {
                Ok(_) => result.set_item(&k, &raw)?,
                Err(_) => result.set_item(&k, raw.repr()?)?,
            }
        } else {
            result.set_item(&k, &v)?;
        }
    }
    Ok(result.unbind().into_any())
}
