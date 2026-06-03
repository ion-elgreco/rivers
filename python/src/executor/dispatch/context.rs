//! Dispatch batch context — tracks step states and dependencies during execution.
//!
//! `BatchContext` is the parameter bundle threaded through every executor
//! backend. Its 17 fields are clustered into four typed sub-structs so callers
//! can express their dependency narrowly (e.g. an IO helper takes `&Repo`,
//! a state-mutating helper takes `&mut RunState`).
use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;
use rivers_core::execution::plan::ExecutionPlan;
use rivers_core::storage::surrealdb_backend::SurrealStorage;
use rivers_core::storage::{EventRecord, ScopedStorageHandle};
use tokio::sync::mpsc;

use crate::assets::io_handler_registry::IOHandlerRegistry;
use crate::config::ResourceVariant;
use crate::metadata::MetadataValue;
use crate::partitions::PyPartitionKey;
use crate::repository::resolved_node::ResolvedNode;

use super::super::GraphNodeMap;
use super::super::async_exec::AsyncBridge;
use super::super::event_writer::EventWriter;
use super::super::ops::{self, now_ts};

/// Immutable run-scope identity: who/what/when this batch is executing.
pub(crate) struct RunScope<'a> {
    pub run_id: &'a str,
    pub partition_key: &'a Option<PyPartitionKey>,
    pub plan: &'a ExecutionPlan,
    /// Steps already completed before this batch (resume case). Read-only.
    pub completed_steps: &'a HashSet<String>,
}

/// Mutable per-batch progress tracking.
pub(crate) struct RunState<'a> {
    pub data_versions: &'a mut HashMap<String, String>,
    pub failed_names: &'a mut HashSet<String>,
    pub graph_started: &'a mut HashSet<String>,
    pub mapped_instance_keys: &'a mut HashMap<String, Vec<String>>,
    /// Per-step record of `dynamic_keys` produced when an asset was executed
    /// in this orchestrator process. Presence is the signal "we saw this
    /// source step run in this batch": empty `Vec` means it ran with plain
    /// values; non-empty means it ran with `DynamicOutput`s. Absent means the
    /// source was not executed locally (cross-run resume, or worker-side
    /// execution under Parallel/Kubernetes), in which case the fan-out path
    /// falls back to the on-disk `__keys` file.
    pub step_dynamic_keys: &'a mut HashMap<String, Vec<String>>,
}

impl<'a> RunState<'a> {
    pub fn record_data_version(&mut self, name: String, version: String) {
        self.data_versions.insert(name, version);
    }

    pub fn mark_failed(&mut self, name: String) {
        self.failed_names.insert(name);
    }

    pub fn was_failed(&self, name: &str) -> bool {
        self.failed_names.contains(name)
    }

    /// Mark a graph asset as started; returns true if this is the first time.
    pub fn mark_graph_started(&mut self, name: String) -> bool {
        self.graph_started.insert(name)
    }

    pub fn record_mapped_keys(&mut self, name: String, keys: Vec<String>) {
        self.mapped_instance_keys.insert(name, keys);
    }

    pub fn record_dynamic_keys(&mut self, name: String, keys: Vec<String>) {
        self.step_dynamic_keys.insert(name, keys);
    }
}

/// Event/storage I/O sink — where step lifecycle events go.
pub(crate) struct EventSink<'a> {
    pub writer: &'a EventWriter,
    pub storage: &'a ScopedStorageHandle<SurrealStorage>,
}

/// Resolved repository data + execution scope (read-only deps).
pub(crate) struct Repo<'a> {
    pub node_map: &'a HashMap<String, ResolvedNode>,
    pub graph_nodes: &'a GraphNodeMap,
    pub io_handler_registry: &'a IOHandlerRegistry,
    pub resources: &'a HashMap<String, ResourceVariant>,
    pub config_overrides: &'a Option<HashMap<String, Py<PyAny>>>,
    pub bridge: Option<&'a AsyncBridge>,
}

/// All shared state for a batch execution.
///
/// Fields are clustered by lifecycle: immutable run identity (`scope`),
/// mutable progress (`state`), event/storage sinks (`sink`), and resolved
/// repository deps (`repo`). The coordinator methods on this struct
/// (`record_step_success`, `record_failure_no_hooks`, `fail_all_steps`) span
/// multiple bags, so they stay on `BatchContext` rather than living inside
/// any one sub-struct. Per-event-name step-failure emission lives in
/// `dispatch::handle_failure` (see `dispatch/results.rs`).
pub(crate) struct BatchContext<'a> {
    pub scope: RunScope<'a>,
    pub state: RunState<'a>,
    pub sink: EventSink<'a>,
    pub repo: Repo<'a>,
}

impl<'a> BatchContext<'a> {
    pub(crate) fn step_pools(&self, step_name: &str) -> Vec<(String, u32)> {
        self.repo
            .node_map
            .get(step_name)
            .map(|n| n.pool())
            .unwrap_or_default()
    }

    pub(crate) fn event_sender(&self) -> mpsc::UnboundedSender<EventRecord> {
        self.sink.writer.sender()
    }

    pub(crate) fn emit_start(&self, step_name: &str, ts: i64) {
        ops::emit_step_start(self.sink.writer, self.scope.run_id, step_name, ts);
    }

    pub(crate) fn emit_success(&self, step_name: &str) {
        ops::emit_step_success(self.sink.writer, self.scope.run_id, step_name, now_ts());
    }

    /// Emit only the event — no hooks, no recording.
    pub(crate) fn emit_step_failure(&self, step_name: &str, msg: &str) {
        ops::emit_step_failure(
            self.sink.writer,
            self.scope.run_id,
            step_name,
            msg,
            now_ts(),
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn emit_materialization(
        &self,
        step_name: &str,
        metadata: &[(String, MetadataValue)],
        data_version: Option<String>,
        input_versions: Vec<(String, String)>,
        ts: i64,
    ) {
        match self.scope.partition_key {
            Some(pk) => {
                for member in pk.members() {
                    ops::emit_materialization(
                        self.sink.writer,
                        self.scope.run_id,
                        step_name,
                        &Some(member),
                        metadata,
                        data_version.clone(),
                        input_versions.clone(),
                        ts,
                    );
                }
            }
            None => ops::emit_materialization(
                self.sink.writer,
                self.scope.run_id,
                step_name,
                &None,
                metadata,
                data_version,
                input_versions,
                ts,
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn emit_observation(
        &self,
        step_name: &str,
        metadata: &[(String, MetadataValue)],
        data_version: Option<String>,
        ts: i64,
    ) {
        match self.scope.partition_key {
            Some(pk) => {
                for member in pk.members() {
                    ops::emit_observation(
                        self.sink.writer,
                        self.scope.run_id,
                        step_name,
                        &Some(member),
                        metadata,
                        data_version.clone(),
                        ts,
                    );
                }
            }
            None => ops::emit_observation(
                self.sink.writer,
                self.scope.run_id,
                step_name,
                &None,
                metadata,
                data_version,
                ts,
            ),
        }
    }

    pub(crate) fn emit_log_output(
        &self,
        step_name: &str,
        stdout: &str,
        stderr: &str,
        logs: &str,
        ts: i64,
    ) {
        ops::emit_log_output(
            self.sink.writer,
            self.scope.run_id,
            step_name,
            stdout,
            stderr,
            logs,
            ts,
        );
    }

    /// Record a step failure without hooks. Emit event, mark failed, push error.
    pub(crate) fn record_failure_no_hooks(
        &mut self,
        step_name: &str,
        error: PyErr,
        failures: &mut Vec<(String, PyErr)>,
    ) {
        ops::emit_step_failure(
            self.sink.writer,
            self.scope.run_id,
            step_name,
            &error.to_string(),
            now_ts(),
        );
        self.state.mark_failed(step_name.to_string());
        failures.push((step_name.to_string(), error));
    }

    /// Fail all instances in a batch with the same error message.
    /// Emits failure events, fires per-instance failure hooks (config=None
    /// since pre-spawn batches haven't resolved one), and marks each failed.
    pub(crate) fn fail_all_instances(
        &mut self,
        instances: &[super::types::StepInstance],
        msg: &str,
        failures: &mut Vec<(String, PyErr)>,
    ) {
        let ts = ops::now_ts();
        for inst in instances {
            ops::emit_step_failure(
                self.sink.writer,
                self.scope.run_id,
                &inst.instance_name,
                msg,
                ts,
            );
            let step = &self.scope.plan.steps[inst.idx];
            if let Some(node) = self.repo.node_map.get(&step.name)
                && node.has_failure_hooks()
            {
                Python::attach(|py| {
                    ops::run_failure_hooks(
                        py,
                        node,
                        &inst.instance_name,
                        self.scope.run_id,
                        msg,
                        node.metadata(),
                        None,
                    );
                });
            }
            failures.push((
                inst.instance_name.clone(),
                crate::errors::ExecutionError::new_err(msg.to_string()),
            ));
            self.state.mark_failed(inst.instance_name.clone());
        }
    }
}
