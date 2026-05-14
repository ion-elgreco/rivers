//! Shared executor operations: invocation, IO, fan-out, and finalization.
mod fan_out;
mod finalize;
pub(crate) mod invoke;
pub(crate) mod io;
mod outputs;

use std::collections::HashMap;

use pyo3::prelude::*;
use rivers_core::storage::ScopedStorageHandle;
use rivers_core::storage::surrealdb_backend::SurrealStorage;

use crate::metadata::MetadataValue;
use crate::repository::resolved_node::ResolvedNode;
use crate::result_types::ResultKind;

/// Shorthand type for the per-CL storage handle passed through executor
/// chains. Bundles `Arc<SurrealStorage>` with the owning code-location
/// identity so per-CL methods can be reached without re-passing the id.
pub(crate) type StorageHandle<'a> = &'a ScopedStorageHandle<SurrealStorage>;

/// Generator multi-asset state attached to a `StepResult`. `Sync` uses
/// `__next__`; `Async` uses `__anext__` driven through the async bridge.
/// `context` is the `PyAssetExecutionContext` the generator was given (when
/// requested) — used for per-yield `peek_output_metadata` and
/// `drain_data_version`.
pub(crate) enum GeneratorType {
    Sync { context: Option<Py<PyAny>> },
    Async { context: Option<Py<PyAny>> },
}

pub(crate) struct StepResult {
    pub result: Py<PyAny>,
    pub return_hint: Option<Py<PyAny>>,
    pub output_metadata: Vec<(String, MetadataValue)>,
    /// Data version registered by the asset function via context.register_data_version().
    pub data_version: Option<String>,
    /// Config instance used during execution (passed through to hooks).
    pub config_instance: Option<Py<PyAny>>,
    /// Tags from an Output result type.
    #[allow(dead_code)]
    pub tags: Option<Vec<String>>,
    /// Mapping keys extracted from DynamicOutput items (if the result was a list of DynamicOutput).
    /// When present, `result` contains the unwrapped values list.
    pub dynamic_keys: Option<Vec<String>>,
    /// Discriminator for the single-output path only — distinguishes
    /// `Output(value)` (write IO + emit Materialization), `Materialization(...)`
    /// (emit only), and `Observation(...)` (Observation event). Multi-asset
    /// paths (dict, generator) re-extract the kind per output / per yield and
    /// ignore this field.
    pub result_kind: ResultKind,
    /// `Some(...)` if `result` is an unevaluated multi-asset generator
    /// (sync or async). `None` for single-output and dict multi-asset.
    /// Carries the per-yield context inside the variant.
    pub generator: Option<GeneratorType>,
}

/// Upsert: for each overlay entry, remove any existing entry with the same key, then append.
pub(crate) fn merge_metadata(
    target: &mut Vec<(String, MetadataValue)>,
    overlay: &[(String, MetadataValue)],
) {
    for (k, v) in overlay {
        target.retain(|(ek, _)| ek != k);
        target.push((k.clone(), v.clone()));
    }
}

/// Maps each output node name to its multi-asset's own name (the group key).
/// Used by `ExecutionPlan::from_subgraph` to group outputs into single steps.
pub(crate) fn build_multi_asset_groups(
    node_map: &HashMap<String, ResolvedNode>,
) -> HashMap<String, String> {
    let mut groups = HashMap::new();
    for (name, node) in node_map {
        if node.multi_asset_output_name().is_some()
            && let Ok(multi_name) = node.name()
        {
            groups.insert(name.clone(), multi_name);
        }
    }
    groups
}

pub(crate) use crate::partitions::mapping::UpstreamKeyResolution;
pub(crate) use fan_out::{
    MappedResultsIter, collect_mapped_outputs, collect_mapped_stream, extract_mapping_key,
    load_fan_out_source, persist_dynamic_keys, resolve_predefined_keys,
};
pub(crate) use finalize::{
    collect_input_data_versions, emit_log_output, emit_materialization, emit_observation,
    emit_step_failure, emit_step_start, emit_step_start_via_tx, emit_step_success,
    extract_data_version, now_ts, register_assets_from_nodes, run_failure_hooks, run_success_hooks,
};
pub(crate) use invoke::{
    annotation_is, enumerate_params, execute_step, extract_config_from_annotation,
    extract_return_hint, get_annotations, is_context_annotation,
};
pub(crate) use io::{
    build_mapped_partition_context, build_partition_context, handle_step_output,
    load_self_dependency, map_partition_key_for_upstream, metadata_to_pickle_safe_dict,
    write_output,
};
pub(crate) use outputs::{OutputItem, for_each_output};
