use std::collections::HashMap;

use pyo3::prelude::*;

use super::decorator::{Kinds, PyAsset};
use super::io_handler::IOHandler;
use crate::automation::PyAutomationCondition;
use crate::hooks::PyHook;
use crate::partitions::backfill_strategy::PyBackfillStrategy;
use crate::partitions::mapping::PartitionMappingDict;

pub struct SingleAsset {
    pub wraps: Option<Py<PyAny>>,
    pub is_async: bool,
    pub name: Option<String>,
    pub tags: Option<Vec<String>>,
    pub kinds: Kinds,
    pub group: Option<String>,
    pub code_version: Option<String>,
    pub io_handler: Option<IOHandler>,
    pub metadata: Option<HashMap<String, String>>,
    /// Partitions definition: inline or a name into the repository
    /// `partition_defs` registry.
    pub partitions_def: Option<crate::partitions::PartitionsDefRef>,
    pub partition_mapping: Option<PartitionMappingDict>,
    /// Input dep names (from `AssetDef.input()` — must match fn params).
    pub input_dep_names: Vec<String>,
    /// Lineage-only dep names (non-input deps from `deps` parameter).
    pub dep_only_names: Vec<String>,
    /// IO handler overrides from input deps (keyed by dep/param name).
    pub input_io_handlers: HashMap<String, IOHandler>,
    /// Metadata overrides from input deps (keyed by dep/param name).
    pub input_metadata: HashMap<String, HashMap<String, String>>,
    pub hooks: Option<Vec<Py<PyHook>>>,
    pub automation_condition: Option<PyAutomationCondition>,
    pub backfill_strategy: Option<PyBackfillStrategy>,
    /// Pool membership: normalized (pool_key, slots_consumed) pairs.
    pub pool: Vec<(String, u32)>,
    /// Retry policy: inline or a name into the repository `retries` registry.
    pub retry: Option<rivers_core::execution::retry::RetryRef>,
    /// Per-asset compute request; axes left unset inherit the executor default.
    pub compute: Option<rivers_core::execution::compute::Compute>,
    /// Named actions this asset supports beyond materialize.
    pub actions: Vec<Py<super::action::PyAssetAction>>,
}

impl SingleAsset {
    pub fn clone_ref(&self, py: Python) -> Self {
        Self {
            wraps: self.wraps.as_ref().map(|f| f.clone_ref(py)),
            is_async: self.is_async,
            name: self.name.clone(),
            tags: self.tags.clone(),
            kinds: self.kinds.clone(),
            group: self.group.clone(),
            code_version: self.code_version.clone(),
            io_handler: self.io_handler.as_ref().map(|h| h.clone_ref(py)),
            metadata: self.metadata.clone(),
            partitions_def: self.partitions_def.as_ref().map(|p| p.clone_ref(py)),
            partition_mapping: self.partition_mapping.clone(),
            input_dep_names: self.input_dep_names.clone(),
            dep_only_names: self.dep_only_names.clone(),
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
            pool: self.pool.clone(),
            retry: self.retry.clone(),
            compute: self.compute.clone(),
            actions: self.actions.iter().map(|a| a.clone_ref(py)).collect(),
        }
    }
}

/// Python-exposed marker subclass created by the `Asset(...)` decorator.
#[pyclass(name = "SingleAsset", extends=PyAsset, frozen, module = "rivers._core")]
pub struct PySingleAsset;
