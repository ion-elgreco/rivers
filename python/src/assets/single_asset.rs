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
}

/// Python-exposed marker subclass created by the `Asset(...)` decorator.
#[pyclass(name = "SingleAsset", extends=PyAsset, module = "rivers._core")]
pub struct PySingleAsset;
