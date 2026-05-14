//! Single seam for runtime IO handler resolution.
//!
//! The chain `node.io_handler() ?? upstream.io_handler() ?? default` was
//! historically rewritten in four sites (`ops/io.rs::handle_step_output`,
//! `load_upstream_input`, `load_self_dependency`, plus the parallel mirror in
//! `worker_args.rs`). Each rewrite ran a slightly different chain and the
//! threading of `default_io_handler` as a `&Py<PyAny>` parameter through every
//! executor backend made adding or reordering steps a cross-cutting edit.
//!
//! `IOHandlerRegistry` owns the default and exposes intent-bearing methods —
//! one per resolution kind. The private chain walkers also report
//! [`IOHandlerSource`], which the parallel pickle path consumes to decide
//! whether `IOHandlerRef` wrapping is applicable (only `Definition` and
//! `GraphOverride` can be reconstructed via the asset's import path; an
//! `InputOverride` carries an arbitrary handler instance set by the
//! downstream's `AssetDef.input(io_handler=...)`).
use pyo3::prelude::*;

use crate::errors::ConfigurationError;
use crate::repository::resolved_node::ResolvedNode;

/// Where a resolved IO handler originated. Load-bearing for the parallel
/// pickle path: only `Definition` / `GraphOverride` have a stable
/// `module.qualname` lookup back to the handler instance — `InputOverride`
/// and `Default` are arbitrary instances and must be shipped as-is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IOHandlerSource {
    /// The node's own definition (`@Asset(io_handler=...)`, `@Task(io_handler=...)`).
    Definition,
    /// Inherited from a parent graph asset's `node_io_handler` (set as
    /// `task.io_handler_override` during `resolve_resources_and_handlers`).
    GraphOverride,
    /// Per-input override on the downstream's `AssetDef.input(io_handler=...)`.
    InputOverride,
    /// Fell through to the registry's shared default (typically `InMemoryIOHandler`).
    Default,
}

/// Owns the shared default handler and resolves the runtime chain for every
/// IO operation (output write, upstream input load, self-dependency load).
pub struct IOHandlerRegistry {
    default: Py<PyAny>,
}

impl IOHandlerRegistry {
    /// Construct from an explicit default handler (typically the shared
    /// `InMemoryIOHandler` produced by `resolve_resources_and_handlers`).
    pub fn new(default: Py<PyAny>) -> Self {
        Self { default }
    }

    /// Clone this registry, reusing the default handler via `Py::clone_ref`.
    /// Used by callers that need to ship the registry across owned-context
    /// boundaries (e.g. the async backend's `SharedStepContext`).
    pub fn clone_ref(&self, py: Python) -> Self {
        Self {
            default: self.default.clone_ref(py),
        }
    }

    /// Construct with a fresh `rivers.io_handlers.memory.InMemoryIOHandler` as
    /// the default. Used by the test-`Job` path that bypasses
    /// `CodeRepository::resolve()`.
    pub fn new_in_memory(py: Python) -> PyResult<Self> {
        let default = py
            .import("rivers.io_handlers.memory")?
            .getattr("InMemoryIOHandler")?
            .call0()?
            .unbind();
        Ok(Self { default })
    }

    /// Return the shared default handler (used by parallel pre-flight
    /// validation that needs to identify "assets relying on the default").
    pub fn default_handler(&self, py: Python) -> Py<PyAny> {
        self.default.clone_ref(py)
    }

    /// Resolve the handler for writing `node`'s output.
    ///
    /// Chain: `node.io_handler() → default`. (Graph asset propagation has
    /// already collapsed `node_io_handler` into `task.io_handler_override` by
    /// the time the runtime sees a `ResolvedNode`.)
    pub fn for_output(&self, py: Python, node: &ResolvedNode) -> Py<PyAny> {
        self.resolve_for_output(py, node).0
    }

    /// Source-aware variant of [`for_output`]. The pickle path uses the
    /// returned [`IOHandlerSource`] to choose between `IOHandlerRef` wrapping
    /// (`Definition`, `GraphOverride`) and shipping the raw instance
    /// (`Default`).
    pub fn for_output_with_source(
        &self,
        py: Python,
        node: &ResolvedNode,
    ) -> (Py<PyAny>, IOHandlerSource) {
        self.resolve_for_output(py, node)
    }

    /// Resolve the handler for loading `param_name` from `upstream` into
    /// `downstream`.
    ///
    /// Chain: `downstream.input_io_handler(param) → upstream.io_handler() → default`.
    pub fn for_upstream_input(
        &self,
        py: Python,
        downstream: &ResolvedNode,
        upstream: &ResolvedNode,
        param_name: &str,
    ) -> Py<PyAny> {
        self.resolve_for_upstream_input(py, downstream, upstream, param_name)
            .0
    }

    /// Source-aware variant of [`for_upstream_input`]. The pickle path uses
    /// the returned [`IOHandlerSource`] to decide whether to wrap as
    /// `IOHandlerRef`. An `InputOverride` MUST be shipped as the raw handler
    /// instance — wrapping via the upstream's callable would silently
    /// reconstruct upstream's own handler and discard the override.
    pub fn for_upstream_input_with_source(
        &self,
        py: Python,
        downstream: &ResolvedNode,
        upstream: &ResolvedNode,
        param_name: &str,
    ) -> (Py<PyAny>, IOHandlerSource) {
        self.resolve_for_upstream_input(py, downstream, upstream, param_name)
    }

    /// Resolve the handler for loading a self-dependency.
    ///
    /// Chain: `node.io_handler() → fallback → ConfigurationError`. Unlike the
    /// other resolvers, self-deps refuse to fall back to the registry default
    /// (`InMemoryIOHandler` would always observe `None` for "previous run's
    /// value", silently breaking the contract).
    pub fn for_self_dependency(
        &self,
        py: Python,
        node: &ResolvedNode,
        step_name: &str,
        fallback: Option<&Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        if let Some(handler) = node.io_handler(py) {
            return Ok(handler);
        }
        if let Some(handler) = fallback {
            return Ok(handler.clone_ref(py));
        }
        Err(ConfigurationError::new_err(format!(
            "Asset '{}' uses SelfDependency but has no io_handler",
            step_name
        )))
    }

    // -- private chain walkers ------------------------------------------------

    fn resolve_for_output(&self, py: Python, node: &ResolvedNode) -> (Py<PyAny>, IOHandlerSource) {
        if let Some(handler) = node.io_handler(py) {
            return (handler, output_source_for(py, node));
        }
        (self.default.clone_ref(py), IOHandlerSource::Default)
    }

    fn resolve_for_upstream_input(
        &self,
        py: Python,
        downstream: &ResolvedNode,
        upstream: &ResolvedNode,
        param_name: &str,
    ) -> (Py<PyAny>, IOHandlerSource) {
        if let Some(handler) = downstream.input_io_handler(py, param_name) {
            return (handler, IOHandlerSource::InputOverride);
        }
        if let Some(handler) = upstream.io_handler(py) {
            return (handler, output_source_for(py, upstream));
        }
        (self.default.clone_ref(py), IOHandlerSource::Default)
    }
}

/// Distinguish whether `node.io_handler()` came from the node's own
/// definition or from a graph asset's `node_io_handler` propagation.
/// Both are eligible for `IOHandlerRef` wrapping in the pickle path, but
/// reconstruction looks up different attributes (`io_handler` vs
/// `node_io_handler`) — see `_reconstruct_io_handler_ref`.
fn output_source_for(py: Python, node: &ResolvedNode) -> IOHandlerSource {
    if node.has_definition_io_handler(py) {
        IOHandlerSource::Definition
    } else {
        IOHandlerSource::GraphOverride
    }
}
