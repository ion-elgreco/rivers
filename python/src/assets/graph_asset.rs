//! Graph asset — a composite asset whose body invokes tasks via composition context.
//!
//! `GraphAsset` stores `invocation_order` and `invocations` captured from the composition
//! context during the decorator body. `node_io_handler` provides persistence for internal
//! tasks, falling back to the graph-level IO handler or the repository default.
use super::decorator::{Kinds, PyAsset};
use super::io_handler::IOHandler;
use crate::automation::PyAutomationCondition;
use crate::composition::InvokedNode;
use crate::hooks::PyHook;
use crate::partitions::PartitionsDefRef;
use crate::partitions::backfill_strategy::PyBackfillStrategy;
use crate::partitions::mapping::PartitionMappingDict;
use pyo3::prelude::*;
use std::collections::HashMap;

pub struct GraphAsset {
    pub name: Option<String>,
    pub wraps: Option<Py<PyAny>>,
    pub kinds: Kinds,
    pub group: Option<String>,
    pub code_version: Option<String>,
    pub tags: Option<Vec<String>>,
    pub io_handler: Option<IOHandler>,
    /// IO handler for internal tasks. Falls back to io_handler, then default.
    pub node_io_handler: Option<IOHandler>,
    pub metadata: Option<HashMap<String, String>>,
    pub partitions_def: Option<PartitionsDefRef>,
    /// Partition mappings for external asset dependencies (derived from deps).
    /// Maps asset name → mapping that transforms the graph's partition key
    /// to the upstream asset's partition space.
    pub partition_mappings: Option<PartitionMappingDict>,
    /// Lineage-only dep names (non-input deps from `deps` parameter).
    pub dep_only_names: Vec<String>,
    /// IO handler overrides from input deps (keyed by dep/param name).
    pub input_io_handlers: HashMap<String, IOHandler>,
    /// Metadata overrides from input deps (keyed by dep/param name).
    pub input_metadata: HashMap<String, HashMap<String, String>>,
    pub hooks: Option<Vec<Py<PyHook>>>,
    pub automation_condition: Option<PyAutomationCondition>,
    pub backfill_strategy: Option<PyBackfillStrategy>,
    /// Retry policy for the graph asset's own step; internal tasks are
    /// independent steps carrying their own policies.
    pub retry: Option<rivers_core::execution::retry::RetryRef>,
    pub invocations: Vec<InvokedNode>,
    /// Namespaced task names in composition order (the order they were called in the graph body).
    pub invocation_order: Vec<String>,
    /// The final task whose output becomes the graph asset's output.
    /// Derived from the graph function's return value during composition.
    pub final_node: Option<String>,
    /// Named actions on the graph's persisted output. Actions never
    /// re-run the composition; internal task intermediates are not targets.
    pub actions: Vec<Py<super::action::PyAssetAction>>,
}

impl GraphAsset {
    pub fn clone_ref(&self, py: Python) -> Self {
        Self {
            name: self.name.clone(),
            wraps: self.wraps.as_ref().map(|f| f.clone_ref(py)),
            kinds: self.kinds.clone(),
            group: self.group.clone(),
            code_version: self.code_version.clone(),
            tags: self.tags.clone(),
            io_handler: self.io_handler.as_ref().map(|h| h.clone_ref(py)),
            node_io_handler: self.node_io_handler.as_ref().map(|h| h.clone_ref(py)),
            metadata: self.metadata.clone(),
            partitions_def: self.partitions_def.as_ref().map(|p| p.clone_ref(py)),
            partition_mappings: self.partition_mappings.clone(),
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
            retry: self.retry.clone(),
            invocations: self.invocations.clone(),
            invocation_order: self.invocation_order.clone(),
            final_node: self.final_node.clone(),
            actions: self.actions.iter().map(|a| a.clone_ref(py)).collect(),
        }
    }
}

/// Python-exposed marker subclass created via `Asset.from_graph(...)`.
#[pyclass(name = "GraphAsset", extends=PyAsset, subclass, frozen, module = "rivers._core")]
pub struct PyGraphAsset;
