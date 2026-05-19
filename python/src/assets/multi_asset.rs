//! Multi-asset — a single function that produces multiple named outputs.
use pyo3::prelude::*;

use std::collections::HashMap;

use super::decorator::{Asset, AssetDef, PyAsset};
use super::io_handler::IOHandler;
use super::single_asset::SingleAsset;
use crate::automation::PyAutomationCondition;
use crate::hooks::PyHook;
use crate::partitions::backfill_strategy::PyBackfillStrategy;
use crate::partitions::definition::PartitionsDefinition;
use crate::partitions::mapping::PartitionMappingDict;

pub struct MultiAsset {
    pub name: Option<String>,
    pub wraps: Option<Py<PyAny>>,
    pub is_async: bool,
    pub code_version: Option<String>,
    pub assets: Vec<SingleAsset>,
    pub partitions_def: Option<Py<PartitionsDefinition>>,
    /// Input dep names (from `AssetDef.input()` — must match fn params).
    pub input_dep_names: Vec<String>,
    /// Lineage-only dep names (non-input deps from `deps` parameter).
    pub dep_only_names: Vec<String>,
    /// Precomputed partition mappings from deps (keyed by dep name).
    pub partition_mappings: Option<PartitionMappingDict>,
    /// IO handler overrides from input deps (keyed by dep/param name).
    pub input_io_handlers: HashMap<String, IOHandler>,
    /// Metadata overrides from input deps (keyed by dep/param name).
    pub input_metadata: HashMap<String, HashMap<String, String>>,
    pub hooks: Option<Vec<Py<PyHook>>>,
    pub automation_condition: Option<PyAutomationCondition>,
    pub backfill_strategy: Option<PyBackfillStrategy>,
}

/// Python-exposed marker subclass created via `Asset.from_multi(...)`.
#[pyclass(name = "MultiAsset", extends=PyAsset, module = "rivers._core")]
pub struct PyMultiAsset;

#[pymethods]
impl PyMultiAsset {
    /// The `AssetDef` for each output defined by this multi-asset.
    #[getter]
    fn output_defs(self_: PyRef<'_, Self>) -> Vec<AssetDef> {
        let py = self_.py();
        let super_ = self_.as_super();
        match super_.inner() {
            Asset::Multi(multi) => multi
                .assets
                .iter()
                .map(|a| AssetDef {
                    name: a.name.clone().unwrap_or_default(),
                    tags: a.tags.clone(),
                    kinds: a.kinds.clone(),
                    group: a.group.clone(),
                    code_version: a.code_version.clone(),
                    io_handler: a.io_handler.as_ref().map(|h| h.clone_ref(py)),
                    metadata: a.metadata.clone(),
                    partitions_def: a.partitions_def.as_ref().map(|p| p.clone_ref(py)),
                    partition_mapping: a.partition_mapping.clone(),
                    pool: a.pool.clone(),
                    // from_multi consumed and merged the original DepDef list;
                    // the reconstructed view does not preserve it.
                    deps: Vec::new(),
                })
                .collect(),
            _ => Vec::new(),
        }
    }
}
