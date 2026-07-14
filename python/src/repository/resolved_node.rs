//! ResolvedNode — fully-resolved snapshot of a node in the dependency graph.
//!
//! Each variant wraps a dedicated struct (`ResolvedAsset`, `ResolvedTask`,
//! `ResolvedBashTask`) so per-variant data and impls live together. Each struct
//! holds an `inner: Py<...>` back-reference to the originating Python object —
//! refcount-cheap, accessible for fields not pre-flattened on the struct.
//!
//! Invariant fields are pre-flattened at construction; mutable / late-resolved
//! values (io_handler, input_io_handler, input_metadata, callable) are still
//! read through `inner` so they reflect any post-construction mutation in the
//! underlying Python object.
use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use rivers_core::execution::retry::{RetryPolicy, RetryRef};

use crate::assets::decorator::{Asset, PyAsset};
use crate::assets::io_handler::IOHandler;
use crate::automation::PyAutomationCondition;
use crate::hooks::PyHook;
use crate::partitions::PartitionsDefinition;
use crate::partitions::backfill_strategy::PyBackfillStrategy;
use crate::partitions::mapping::PartitionMapping;
use crate::task::{PyBashTask, PyTask};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AssetKind {
    Single,
    Multi,
    Graph,
    External,
}

pub(crate) struct ResolvedAsset {
    pub inner: Py<PyAsset>,
    /// For multi-asset outputs, the name of this specific output.
    /// `None` for single/graph/external assets.
    pub output_name: Option<String>,
    pub name: String,
    pub kind: AssetKind,
    pub is_async: bool,
    pub is_observable_external: bool,
    pub tags: Option<Vec<String>>,
    pub kinds: Vec<String>,
    pub group: Option<String>,
    pub code_version: Option<String>,
    pub pool: Vec<(String, u32)>,
    /// Resolved retry policy (registry names collapsed to concrete at resolve()).
    pub retry: Option<RetryPolicy>,
    pub metadata: Option<HashMap<String, String>>,
    pub backfill_strategy: Option<PyBackfillStrategy>,
    /// Flattened pure-Rust partition definition. The originating
    /// `Py<PartitionsDefinition>` (a frozen pyclass) is unwrapped at
    /// construction so internal validation never needs the GIL.
    pub partitions_def: Option<PartitionsDefinition>,
    pub partition_mapping: Option<HashMap<String, PartitionMapping>>,
    pub success_hooks: Vec<Py<PyHook>>,
    pub failure_hooks: Vec<Py<PyHook>>,
    /// For Graph assets: names of the namespaced internal tasks.
    /// Empty for other asset kinds.
    pub graph_task_names: Vec<String>,
    /// For Graph assets: name of the final task whose output becomes the
    /// graph asset's output.
    pub graph_final_node: Option<String>,
    /// For Multi assets: names of every output.
    /// Empty for other asset kinds.
    pub all_multi_outputs: Vec<String>,
    /// For Graph assets: order in which composition tasks were invoked.
    /// Empty for other asset kinds.
    pub graph_invocation_order: Vec<String>,
    /// For External assets: the observe function, if set.
    /// `None` for non-External assets and Externals without observe_fn.
    pub observe_fn: Option<Py<PyAny>>,
    /// Automation condition attached to the asset, if any.
    pub automation_condition: Option<PyAutomationCondition>,
}

/// Composition tasks carry override fields propagated from their parent
/// graph asset; standalone tasks have all overrides as `None`.
pub(crate) struct ResolvedTask {
    pub inner: Py<PyTask>,
    pub name: String,
    pub is_async: bool,
    pub tags: Option<Vec<String>>,
    /// Pre-merged partitions_def: the override (from parent graph asset) takes
    /// precedence over the task's own partitions_def. Stored as the unwrapped
    /// pure-Rust value (frozen pyclass) so reads don't take the GIL.
    pub partitions_def: Option<PartitionsDefinition>,
    /// Pre-merged partition_mapping: override OR task's own.
    pub partition_mapping: Option<HashMap<String, PartitionMapping>>,
    /// For tasks inside a graph asset composition: maps parameter names
    /// to upstream node names. E.g., `{"value": "a"}` means the `value`
    /// parameter receives the output of node `a`.
    /// `None` for standalone tasks (deps resolved from `__annotations__`).
    pub param_remap: Option<HashMap<String, String>>,
    /// Name of the parent graph asset for namespaced composition tasks
    /// (i.e. `ns_name = "{parent_graph_name}/{task_name}"`). `None` for
    /// standalone tasks. Set at construction time alongside `param_remap`
    /// so callers don't need to recover the relationship via string-splitting
    /// the namespaced name.
    pub parent_graph_name: Option<String>,
    /// IO handler override from graph asset's `node_io_handler`.
    /// When set, takes precedence over the task's own IO handler.
    /// Set after construction by the io_handler resolution pass in resolve().
    pub io_handler_override: Option<IOHandler>,
    /// IO handler overrides for specific input deps (from parent graph asset's deps),
    /// keyed by upstream node name. Resolved through `param_remap` at access time.
    pub input_io_handler_override: Option<HashMap<String, IOHandler>>,
    /// Metadata overrides for specific input deps (from parent graph asset's deps),
    /// keyed by upstream node name. Resolved through `param_remap` at access time.
    pub input_metadata_override: Option<HashMap<String, HashMap<String, String>>>,
}

pub(crate) struct ResolvedBashTask {
    pub inner: Py<PyBashTask>,
    pub name: String,
    pub tags: Option<Vec<String>>,
    /// Partitions def from the parent graph asset (PyBashTask itself has none).
    /// Stored as the unwrapped pure-Rust value (frozen pyclass) so reads
    /// don't take the GIL.
    pub partitions_def: Option<PartitionsDefinition>,
    /// Pre-merged partition_mapping: override OR bash task's own.
    pub partition_mapping: Option<HashMap<String, PartitionMapping>>,
}

/// A node in the dependency graph — Asset, Task, or BashTask.
///
/// `ResolvedAsset` is much larger than the other variants (~648 vs ~392 bytes);
/// boxing it keeps the enum's stack footprint at the size of the next-largest
/// variant.
pub(crate) enum ResolvedNode {
    Asset(Box<ResolvedAsset>),
    Task(ResolvedTask),
    BashTask(ResolvedBashTask),
}

impl ResolvedAsset {
    /// `output_name` disambiguates multi-asset outputs.
    pub fn new(py: Python, inner: Py<PyAsset>, output_name: Option<String>) -> PyResult<Self> {
        let asset_ref = inner.borrow(py);
        let asset = asset_ref.inner();

        let kind = match asset {
            Asset::Single(_) => AssetKind::Single,
            Asset::Multi(_) => AssetKind::Multi,
            Asset::Graph(_) => AssetKind::Graph,
            Asset::External(_) => AssetKind::External,
        };

        let name = asset
            .name()
            .clone()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("Asset has no name"))?;

        let is_async = match asset {
            Asset::Single(s) => s.is_async,
            Asset::Multi(m) => m.is_async,
            Asset::External(e) => e.is_async_observe,
            Asset::Graph(_) => false,
        };

        let is_observable_external = matches!(
            asset,
            Asset::External(ext) if ext.observe_fn.is_some()
        );

        let tags = asset.tags().cloned();
        let kinds = asset.kinds().0.clone();
        let group = asset.group().cloned();
        let code_version = asset.code_version().cloned();
        let pool = asset.pool().clone();
        // Registry names are collapsed to Inline by resolve_retry_refs before this.
        let retry = asset.retry().and_then(|r| match r {
            RetryRef::Inline(p) => Some(p.clone()),
            RetryRef::Named(_) => None,
        });
        let metadata = asset.metadata().cloned();
        let backfill_strategy = asset.backfill_strategy().cloned();

        // For Multi outputs, prefer the per-output partitions_def, falling back
        // to the multi-level value. For other shapes, use the asset-level value.
        let partitions_def = if let (Asset::Multi(multi), Some(out)) = (asset, &output_name) {
            multi
                .assets
                .iter()
                .find(|sa| sa.name.as_deref() == Some(out.as_str()))
                .and_then(|sa| sa.partitions_def.as_ref().map(|p| p.borrow(py).clone()))
                .or_else(|| multi.partitions_def.as_ref().map(|p| p.borrow(py).clone()))
        } else {
            asset.partitions_def().map(|p| p.borrow(py).clone())
        };

        let partition_mapping = asset.partition_mapping().map(|m| m.0.clone());

        let (success_hooks, failure_hooks) = match asset.hooks() {
            Some(hooks) => {
                let mut s = Vec::new();
                let mut f = Vec::new();
                for h in hooks {
                    let h_ref = h.borrow(py);
                    if h_ref.is_success() {
                        s.push(h.clone_ref(py));
                    }
                    if h_ref.is_failure() {
                        f.push(h.clone_ref(py));
                    }
                }
                (s, f)
            }
            None => (Vec::new(), Vec::new()),
        };

        let graph_task_names = match asset {
            Asset::Graph(g) => g
                .invocations
                .iter()
                .filter(|inv| {
                    matches!(
                        inv.node_type,
                        rivers_core::composition::InvokedNodeType::Task
                    )
                })
                .map(|inv| inv.name.clone())
                .collect(),
            _ => Vec::new(),
        };

        let graph_final_node = match asset {
            Asset::Graph(g) => g.final_node.clone(),
            _ => None,
        };

        let all_multi_outputs = match asset {
            Asset::Multi(multi) => multi
                .assets
                .iter()
                .filter_map(|sa| sa.name.clone())
                .collect(),
            _ => Vec::new(),
        };

        let graph_invocation_order = match asset {
            Asset::Graph(g) => g.invocation_order.clone(),
            _ => Vec::new(),
        };

        let observe_fn = match asset {
            Asset::External(ext) => ext.observe_fn.as_ref().map(|f| f.clone_ref(py)),
            _ => None,
        };

        let automation_condition = asset.automation_condition().cloned();

        drop(asset_ref);

        Ok(Self {
            inner,
            output_name,
            name,
            kind,
            is_async,
            is_observable_external,
            tags,
            kinds,
            group,
            code_version,
            pool,
            retry,
            metadata,
            backfill_strategy,
            partitions_def,
            partition_mapping,
            success_hooks,
            failure_hooks,
            graph_task_names,
            graph_final_node,
            all_multi_outputs,
            graph_invocation_order,
            observe_fn,
            automation_condition,
        })
    }

    pub fn clone_ref(&self, py: Python) -> Self {
        Self {
            inner: self.inner.clone_ref(py),
            output_name: self.output_name.clone(),
            name: self.name.clone(),
            kind: self.kind,
            is_async: self.is_async,
            is_observable_external: self.is_observable_external,
            tags: self.tags.clone(),
            kinds: self.kinds.clone(),
            group: self.group.clone(),
            code_version: self.code_version.clone(),
            pool: self.pool.clone(),
            retry: self.retry.clone(),
            metadata: self.metadata.clone(),
            backfill_strategy: self.backfill_strategy.clone(),
            partitions_def: self.partitions_def.clone(),
            partition_mapping: self.partition_mapping.clone(),
            success_hooks: self.success_hooks.iter().map(|h| h.clone_ref(py)).collect(),
            failure_hooks: self.failure_hooks.iter().map(|h| h.clone_ref(py)).collect(),
            graph_task_names: self.graph_task_names.clone(),
            graph_final_node: self.graph_final_node.clone(),
            all_multi_outputs: self.all_multi_outputs.clone(),
            graph_invocation_order: self.graph_invocation_order.clone(),
            observe_fn: self.observe_fn.as_ref().map(|f| f.clone_ref(py)),
            automation_condition: self.automation_condition.clone(),
        }
    }
}

impl ResolvedTask {
    /// `*_override` arguments are merged with the task's own definition and
    /// stored as flat fields where they're invariant (partitions_def,
    /// partition_mapping).
    pub fn new(
        py: Python,
        inner: Py<PyTask>,
        param_remap: Option<HashMap<String, String>>,
        parent_graph_name: Option<String>,
        partitions_def_override: Option<Py<PartitionsDefinition>>,
        partition_mapping_override: Option<HashMap<String, PartitionMapping>>,
        input_io_handler_override: Option<HashMap<String, IOHandler>>,
        input_metadata_override: Option<HashMap<String, HashMap<String, String>>>,
    ) -> PyResult<Self> {
        let task_ref = inner.borrow(py);
        let task = &task_ref.inner;

        let name = task
            .name
            .clone()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("Task has no name"))?;
        let is_async = task.is_async;
        let tags = task.tags.clone();

        let partitions_def = partitions_def_override
            .map(|p| p.borrow(py).clone())
            .or_else(|| task.partitions_def.as_ref().map(|p| p.borrow(py).clone()));

        let partition_mapping = partition_mapping_override
            .or_else(|| task.partition_mapping.as_ref().map(|m| m.0.clone()));

        drop(task_ref);

        Ok(Self {
            inner,
            name,
            is_async,
            tags,
            partitions_def,
            partition_mapping,
            param_remap,
            parent_graph_name,
            io_handler_override: None,
            input_io_handler_override,
            input_metadata_override,
        })
    }

    pub fn clone_ref(&self, py: Python) -> Self {
        Self {
            inner: self.inner.clone_ref(py),
            name: self.name.clone(),
            is_async: self.is_async,
            tags: self.tags.clone(),
            partitions_def: self.partitions_def.clone(),
            partition_mapping: self.partition_mapping.clone(),
            param_remap: self.param_remap.clone(),
            parent_graph_name: self.parent_graph_name.clone(),
            io_handler_override: self.io_handler_override.as_ref().map(|h| h.clone_ref(py)),
            input_io_handler_override: self.input_io_handler_override.as_ref().map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.clone_ref(py)))
                    .collect()
            }),
            input_metadata_override: self.input_metadata_override.clone(),
        }
    }
}

impl ResolvedBashTask {
    pub fn new(
        py: Python,
        inner: Py<PyBashTask>,
        partitions_def_override: Option<Py<PartitionsDefinition>>,
        partition_mapping_override: Option<HashMap<String, PartitionMapping>>,
    ) -> Self {
        let bash_ref = inner.borrow(py);
        let name = bash_ref.name.clone();
        let tags = bash_ref.tags.clone();
        let partition_mapping = partition_mapping_override
            .or_else(|| bash_ref.partition_mapping.as_ref().map(|m| m.0.clone()));
        drop(bash_ref);

        let partitions_def = partitions_def_override
            .as_ref()
            .map(|p| p.borrow(py).clone());

        Self {
            inner,
            name,
            tags,
            partitions_def,
            partition_mapping,
        }
    }

    pub fn clone_ref(&self, py: Python) -> Self {
        Self {
            inner: self.inner.clone_ref(py),
            name: self.name.clone(),
            tags: self.tags.clone(),
            partitions_def: self.partitions_def.clone(),
            partition_mapping: self.partition_mapping.clone(),
        }
    }
}

impl ResolvedNode {
    pub fn name(&self) -> PyResult<String> {
        match self {
            ResolvedNode::Asset(node) => Ok(node.name.clone()),
            ResolvedNode::Task(node) => Ok(node.name.clone()),
            ResolvedNode::BashTask(node) => Ok(node.name.clone()),
        }
    }

    pub fn is_external(&self) -> bool {
        matches!(
            self,
            ResolvedNode::Asset(node) if node.kind == AssetKind::External
        )
    }

    pub fn is_observable_external(&self) -> bool {
        matches!(self, ResolvedNode::Asset(node) if node.is_observable_external)
    }

    /// For multi-asset outputs, return the output name for this node.
    /// Returns `None` for single/graph/external assets and tasks.
    pub fn multi_asset_output_name(&self) -> Option<&str> {
        match self {
            ResolvedNode::Asset(node) => node.output_name.as_deref(),
            _ => None,
        }
    }

    /// For multi-asset output nodes, return all output names of the parent multi-asset.
    /// Returns an empty vec for non-multi-asset nodes.
    #[allow(dead_code)]
    pub fn all_multi_asset_output_names(&self) -> Vec<String> {
        match self {
            ResolvedNode::Asset(node) => node.all_multi_outputs.clone(),
            _ => Vec::new(),
        }
    }

    pub fn is_async(&self) -> bool {
        match self {
            ResolvedNode::Asset(node) => node.is_async,
            ResolvedNode::Task(node) => node.is_async,
            ResolvedNode::BashTask(_) => false,
        }
    }

    pub fn is_bash_task(&self) -> bool {
        matches!(self, ResolvedNode::BashTask(_))
    }

    pub fn is_graph_asset(&self) -> bool {
        matches!(self, ResolvedNode::Asset(node) if node.kind == AssetKind::Graph)
    }

    /// For graph assets, return the namespaced task names that are part of this graph.
    /// E.g. for graph asset "pipeline" with tasks a, b → ["pipeline/a", "pipeline/b"].
    pub fn graph_task_names(&self) -> Vec<String> {
        match self {
            ResolvedNode::Asset(node) => node.graph_task_names.clone(),
            _ => Vec::new(),
        }
    }

    /// For graph assets, return the final node name (the task whose output
    /// becomes the graph asset's output). Derived from the graph function's
    /// return value during composition.
    pub fn graph_final_node(&self) -> Option<String> {
        match self {
            ResolvedNode::Asset(node) => node.graph_final_node.clone(),
            _ => None,
        }
    }

    pub fn asset_type(&self) -> &'static str {
        match self {
            ResolvedNode::Asset(node) => match node.kind {
                AssetKind::Single => "single",
                AssetKind::Multi => "multi",
                AssetKind::Graph => "graph",
                AssetKind::External => "external",
            },
            ResolvedNode::Task(_) | ResolvedNode::BashTask(_) => "task",
        }
    }

    /// For Assets/Tasks: the wrapped Python function.
    /// For ExternalAsset: the observe function.
    /// For BashTask: the BashTask object itself (implements __call__).
    pub fn callable(&self, py: Python) -> PyResult<Py<PyAny>> {
        match self {
            ResolvedNode::Asset(node) => {
                let asset = node.inner.borrow(py);
                match asset.inner() {
                    Asset::External(ext) => ext
                        .observe_fn
                        .as_ref()
                        .ok_or_else(|| {
                            pyo3::exceptions::PyValueError::new_err(
                                "External asset has no observe function",
                            )
                        })
                        .map(|f| f.clone_ref(py)),
                    _ => Ok(asset.inner()._asset_fn()?.clone_ref(py)),
                }
            }
            ResolvedNode::Task(node) => {
                let task = node.inner.borrow(py);
                let func = task.inner.wraps.as_ref().ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err("Task has no wrapped function")
                })?;
                Ok(func.clone_ref(py))
            }
            ResolvedNode::BashTask(node) => Ok(node.inner.clone_ref(py).into_any()),
        }
    }

    /// Returns None for BashTask and ExternalAsset without observe_fn.
    pub fn annotations<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        match self {
            ResolvedNode::Asset(node) => {
                let asset = node.inner.borrow(py);
                let func = match asset.inner() {
                    Asset::External(ext) => match ext.observe_fn {
                        Some(ref f) => f.clone_ref(py),
                        None => return Ok(None),
                    },
                    _ => asset.inner()._asset_fn()?.clone_ref(py),
                };
                let ann = func.getattr(py, "__annotations__")?;
                Ok(Some(ann.into_bound(py).cast_into::<PyDict>()?))
            }
            ResolvedNode::Task(node) => {
                let task = node.inner.borrow(py);
                let func = task.inner.wraps.as_ref().ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err("Task has no wrapped function")
                })?;
                let ann = func.getattr(py, "__annotations__")?;
                Ok(Some(ann.into_bound(py).cast_into::<PyDict>()?))
            }
            ResolvedNode::BashTask(_) => Ok(None),
        }
    }

    pub fn tags(&self) -> Option<Vec<String>> {
        match self {
            ResolvedNode::Asset(node) => node.tags.clone(),
            ResolvedNode::Task(node) => node.tags.clone(),
            ResolvedNode::BashTask(node) => node.tags.clone(),
        }
    }

    /// All ResourceRef variants must be resolved to Instance by resolve time.
    /// Panics if an unresolved ResourceRef is encountered — this indicates a bug
    /// in the resolution pipeline.
    pub fn io_handler(&self, py: Python) -> Option<Py<PyAny>> {
        match self {
            ResolvedNode::Asset(node) => {
                let asset = node.inner.borrow(py);
                asset_output_io_handler(asset.inner(), &node.output_name)
                    .map(|h| expect_resolved_handler(h, py))
            }
            ResolvedNode::Task(node) => {
                if let Some(ref override_h) = node.io_handler_override {
                    return Some(expect_resolved_handler(override_h, py));
                }
                node.inner
                    .borrow(py)
                    .inner
                    .io_handler
                    .as_ref()
                    .map(|h| expect_resolved_handler(h, py))
            }
            ResolvedNode::BashTask(node) => node
                .inner
                .borrow(py)
                .io_handler
                .as_ref()
                .map(|h| expect_resolved_handler(h, py)),
        }
    }

    /// Name of the parent graph asset for namespaced composition tasks.
    /// Returns `Some` only for `Task` nodes constructed inside a graph
    /// composition (set at construction time in `build_unresolved_graph`).
    /// Lets callers find the parent graph asset without splitting the
    /// namespaced step name on `/`.
    pub fn parent_graph_name(&self) -> Option<&str> {
        match self {
            ResolvedNode::Task(node) => node.parent_graph_name.as_deref(),
            ResolvedNode::Asset(_) | ResolvedNode::BashTask(_) => None,
        }
    }

    pub fn input_io_handler(&self, py: Python, param_name: &str) -> Option<Py<PyAny>> {
        match self {
            ResolvedNode::Asset(node) => {
                let asset = node.inner.borrow(py);
                asset
                    .inner()
                    .input_io_handler(param_name)
                    .map(|h| expect_resolved_handler(h, py))
            }
            ResolvedNode::Task(node) => {
                // For composition tasks, resolve the param name through param_remap
                // to find the upstream name, then check overrides.
                let resolved = node
                    .param_remap
                    .as_ref()
                    .and_then(|m| m.get(param_name))
                    .map(|s| s.as_str())
                    .unwrap_or(param_name);
                node.input_io_handler_override
                    .as_ref()
                    .and_then(|m| m.get(resolved))
                    .map(|h| expect_resolved_handler(h, py))
            }
            _ => None,
        }
    }

    pub fn input_metadata(&self, py: Python, param_name: &str) -> Option<HashMap<String, String>> {
        match self {
            ResolvedNode::Asset(node) => node
                .inner
                .borrow(py)
                .inner()
                .input_metadata(param_name)
                .cloned(),
            ResolvedNode::Task(node) => {
                let resolved = node
                    .param_remap
                    .as_ref()
                    .and_then(|m| m.get(param_name))
                    .map(|s| s.as_str())
                    .unwrap_or(param_name);
                node.input_metadata_override
                    .as_ref()
                    .and_then(|m| m.get(resolved))
                    .cloned()
            }
            _ => None,
        }
    }

    pub fn has_io_handler(&self, py: Python) -> bool {
        match self {
            ResolvedNode::Asset(node) => {
                let asset = node.inner.borrow(py);
                asset_output_io_handler(asset.inner(), &node.output_name).is_some()
            }
            ResolvedNode::Task(node) => {
                node.io_handler_override.is_some()
                    || node.inner.borrow(py).inner.io_handler.is_some()
            }
            ResolvedNode::BashTask(node) => node.inner.borrow(py).io_handler.is_some(),
        }
    }

    /// Determines whether IOHandlerRef can reconstruct the handler after re-import
    /// in spawn-based worker subprocesses.
    pub fn has_definition_io_handler(&self, py: Python) -> bool {
        match self {
            ResolvedNode::Asset(_) => self.has_io_handler(py),
            ResolvedNode::Task(node) => node.inner.borrow(py).inner.io_handler.is_some(),
            ResolvedNode::BashTask(node) => node.inner.borrow(py).io_handler.is_some(),
        }
    }

    pub fn metadata(&self) -> Option<HashMap<String, String>> {
        match self {
            ResolvedNode::Asset(node) => node.metadata.clone(),
            ResolvedNode::Task(_) | ResolvedNode::BashTask(_) => None,
        }
    }

    pub fn kinds(&self) -> Vec<String> {
        match self {
            ResolvedNode::Asset(node) => node.kinds.clone(),
            ResolvedNode::Task(_) | ResolvedNode::BashTask(_) => Vec::new(),
        }
    }

    pub fn group(&self) -> Option<String> {
        match self {
            ResolvedNode::Asset(node) => node.group.clone(),
            ResolvedNode::Task(_) | ResolvedNode::BashTask(_) => None,
        }
    }

    pub fn code_version(&self) -> Option<String> {
        match self {
            ResolvedNode::Asset(node) => node.code_version.clone(),
            ResolvedNode::Task(_) | ResolvedNode::BashTask(_) => None,
        }
    }

    pub fn pool(&self) -> Vec<(String, u32)> {
        match self {
            ResolvedNode::Asset(node) => node.pool.clone(),
            ResolvedNode::Task(_) | ResolvedNode::BashTask(_) => Vec::new(),
        }
    }

    // Consumed by the executor retry loop (landing in a later increment).
    #[allow(dead_code)]
    pub fn retry(&self) -> Option<&RetryPolicy> {
        match self {
            ResolvedNode::Asset(node) => node.retry.as_ref(),
            _ => None,
        }
    }

    pub fn backfill_strategy(&self) -> Option<PyBackfillStrategy> {
        match self {
            ResolvedNode::Asset(node) => node.backfill_strategy.clone(),
            _ => None,
        }
    }

    pub fn partitions_def(&self) -> Option<&PartitionsDefinition> {
        match self {
            ResolvedNode::Asset(node) => node.partitions_def.as_ref(),
            ResolvedNode::Task(node) => node.partitions_def.as_ref(),
            ResolvedNode::BashTask(node) => node.partitions_def.as_ref(),
        }
    }

    pub fn partition_mapping(&self) -> Option<HashMap<String, PartitionMapping>> {
        match self {
            ResolvedNode::Asset(node) => node.partition_mapping.clone(),
            ResolvedNode::Task(node) => node.partition_mapping.clone(),
            ResolvedNode::BashTask(node) => node.partition_mapping.clone(),
        }
    }

    pub fn success_hooks(&self) -> &[Py<PyHook>] {
        match self {
            ResolvedNode::Asset(node) => &node.success_hooks,
            _ => &[],
        }
    }

    pub fn failure_hooks(&self) -> &[Py<PyHook>] {
        match self {
            ResolvedNode::Asset(node) => &node.failure_hooks,
            _ => &[],
        }
    }

    pub fn has_success_hooks(&self) -> bool {
        match self {
            ResolvedNode::Asset(node) => !node.success_hooks.is_empty(),
            _ => false,
        }
    }

    pub fn has_failure_hooks(&self) -> bool {
        match self {
            ResolvedNode::Asset(node) => !node.failure_hooks.is_empty(),
            _ => false,
        }
    }

    pub fn param_remap(&self) -> Option<&HashMap<String, String>> {
        match self {
            ResolvedNode::Task(node) => node.param_remap.as_ref(),
            _ => None,
        }
    }

    pub fn clone_ref(&self, py: Python) -> Self {
        match self {
            ResolvedNode::Asset(node) => ResolvedNode::Asset(Box::new(node.clone_ref(py))),
            ResolvedNode::Task(node) => ResolvedNode::Task(node.clone_ref(py)),
            ResolvedNode::BashTask(node) => ResolvedNode::BashTask(node.clone_ref(py)),
        }
    }
}

/// Panics if the handler is still a `ResourceRef` — by the time ResolvedNode
/// methods are called, `CodeRepository.resolve()` should have replaced every
/// `ResourceRef` with its `Instance`.
fn expect_resolved_handler(h: &IOHandler, py: Python) -> Py<PyAny> {
    match h {
        IOHandler::Instance(handler) => handler.clone_ref(py),
        IOHandler::ResourceRef(key) => unreachable!(
            "BUG: unresolved ResourceRef('{}') after CodeRepository.resolve()",
            key
        ),
    }
}

fn asset_output_io_handler<'a>(
    asset: &'a Asset,
    output_name: &Option<String>,
) -> Option<&'a IOHandler> {
    if let (Asset::Multi(multi), Some(name)) = (asset, output_name) {
        multi
            .assets
            .iter()
            .find(|ad| ad.name.as_deref() == Some(name.as_str()))
            .and_then(|ad| ad.io_handler.as_ref())
    } else {
        asset.io_handler()
    }
}
