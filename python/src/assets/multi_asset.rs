//! Multi-asset — a single function that produces multiple named outputs.
use pyo3::prelude::*;

use std::collections::HashMap;

use super::decorator::{Asset, AssetDef, PyAsset};
use super::io_handler::IOHandler;
use super::single_asset::SingleAsset;
use crate::automation::PyAutomationCondition;
use crate::hooks::PyHook;
use crate::partitions::backfill_strategy::PyBackfillStrategy;
use crate::partitions::definition::PartitionsDefRef;
use crate::partitions::mapping::PartitionMappingDict;

pub struct MultiAsset {
    pub name: Option<String>,
    pub wraps: Option<Py<PyAny>>,
    pub is_async: bool,
    pub code_version: Option<String>,
    pub assets: Vec<SingleAsset>,
    pub partitions_def: Option<PartitionsDefRef>,
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
    /// Compute for the step — one multi-asset is one step (one pod), so this
    /// lives on the multi-asset, not per output.
    pub compute: Option<rivers_core::execution::compute::Compute>,
    /// Retry policy for the step — a multi-asset retries as one unit, so this
    /// lives on the multi-asset, not per output.
    pub retry: Option<rivers_core::execution::retry::RetryRef>,
}

impl MultiAsset {
    pub fn clone_ref(&self, py: Python) -> Self {
        Self {
            name: self.name.clone(),
            wraps: self.wraps.as_ref().map(|f| f.clone_ref(py)),
            is_async: self.is_async,
            code_version: self.code_version.clone(),
            assets: self.assets.iter().map(|a| a.clone_ref(py)).collect(),
            partitions_def: self.partitions_def.as_ref().map(|p| p.clone_ref(py)),
            input_dep_names: self.input_dep_names.clone(),
            dep_only_names: self.dep_only_names.clone(),
            partition_mappings: self.partition_mappings.clone(),
            input_io_handlers: self
                .input_io_handlers
                .iter()
                .map(|(k, h)| (k.clone(), h.clone_ref(py)))
                .collect(),
            input_metadata: self.input_metadata.clone(),
            hooks: self
                .hooks
                .as_ref()
                .map(|hooks| hooks.iter().map(|h| h.clone_ref(py)).collect()),
            automation_condition: self.automation_condition.clone(),
            backfill_strategy: self.backfill_strategy.clone(),
            compute: self.compute.clone(),
            retry: self.retry.clone(),
        }
    }
}

/// Python-exposed marker subclass created via `Asset.from_multi(...)`.
#[pyclass(name = "MultiAsset", extends=PyAsset, subclass, frozen, module = "rivers._core")]
pub struct PyMultiAsset;

#[pymethods]
impl PyMultiAsset {
    /// The `AssetDef` for each output defined by this multi-asset.
    #[getter]
    fn output_defs(slf: &Bound<'_, Self>) -> Vec<AssetDef> {
        let py = slf.py();
        match slf.as_super().get().inner() {
            Asset::Multi(multi) => multi
                .assets
                .iter()
                .map(|a| AssetDef {
                    name: a.name.clone(),
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
                    actions: a.actions.iter().map(|x| x.clone_ref(py)).collect(),
                })
                .collect(),
            _ => Vec::new(),
        }
    }
}
