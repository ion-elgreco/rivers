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
use rivers_core::storage::{AssetScope, ScopedStorageHandle};
use tokio::sync::mpsc;

use crate::assets::io_handler_registry::IOHandlerRegistry;
use crate::config::ResourceVariant;
use crate::metadata::MetadataValue;
use crate::partitions::PyPartitionKey;
use crate::repository::resolved_node::ResolvedNode;

use super::super::async_exec::AsyncBridge;
use super::super::event_writer::{EventWriter, WriterMsg};
use super::super::ops::{self, now_ts};
use super::super::{GraphNodeMap, in_step_pod};

/// Immutable run-scope identity: who/what/when this batch is executing.
pub(crate) struct RunScope<'a> {
    pub run_id: &'a str,
    pub partition_key: &'a Option<PyPartitionKey>,
    pub plan: &'a ExecutionPlan,
    /// Steps already completed before this batch (resume case). Read-only.
    pub completed_steps: &'a HashSet<String>,
    /// Resuming a prior run: retry ladders seed their attempt number from
    /// recorded StepRetry events instead of restarting the budget.
    pub resume: bool,
}

/// Mutable per-batch progress tracking.
pub(crate) struct RunState<'a> {
    pub data_versions: &'a mut HashMap<String, String>,
    pub failed_names: &'a mut HashSet<String>,
    pub graph_started: &'a mut HashSet<String>,
    pub mapped_instance_keys: &'a mut HashMap<String, Vec<String>>,
    pub failed_partitions: &'a mut HashMap<String, Vec<(PyPartitionKey, String)>>,
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
    pub retries: &'a HashMap<String, rivers_core::execution::retry::RetryPolicy>,
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

/// The implicit pool an exclusive action shares with materialize.
pub(crate) fn implicit_asset_pool(asset_key: &str) -> String {
    format!("__asset__:{asset_key}")
}

/// Capacity of that pool. Exclusion is decided by partition overlap
/// ([`AssetScope`]), not by counting, so every step takes a single slot and
/// this only has to be high enough never to bind.
pub(crate) const EXCLUSIVE_POOL_CAPACITY: u32 = 1_000_000;

impl<'a> BatchContext<'a> {
    /// What this step touches on the implicit asset pool. `exclusive` marks an
    /// action; `partitions` is `None` for an unpartitioned run, which conflicts
    /// with everything on that asset.
    pub(crate) fn asset_scope(&self, exclusive: bool) -> AssetScope {
        let partitions = self
            .scope
            .partition_key
            .as_ref()
            .map(|pk| {
                pk.members()
                    .into_iter()
                    .map(|m| rivers_core::storage::PartitionKey::from(&m).to_display())
                    .collect::<Vec<_>>()
            })
            // An empty set intersects nothing, so it would neither block nor be
            // blocked. Fall back to whole-asset, the conservative reading.
            .filter(|members| !members.is_empty());
        AssetScope {
            partitions,
            exclusive,
        }
    }

    /// Pools this step claims, and the scope for any implicit asset pool among
    /// them (`None` when it claims none).
    pub(crate) fn step_pools(
        &self,
        step: &rivers_core::execution::plan::ExecutionStep,
    ) -> (Vec<(String, u32)>, Option<AssetScope>) {
        let mut pools = self
            .repo
            .node_map
            .get(&step.name)
            .map(|n| n.pool())
            .unwrap_or_default();
        let user_pools = pools.len();
        let mut exclusive = false;
        // Exclusive actions: both sides take one slot of the asset's implicit
        // pool and the claim carries an `AssetScope`, so admission is decided by
        // which partitions each side touches rather than by slot count. A multi
        // materializes all outputs in one step, so it claims each output's pool.
        // Assets without exclusive actions never touch this. Reads only the
        // exclusivity cached at resolve — no GIL inside the detached region.
        match self.scope.plan.verb() {
            Some(verb) => {
                exclusive = self
                    .repo
                    .node_map
                    .get(&step.name)
                    .map(|n| n.action_is_exclusive(verb))
                    .unwrap_or(false);
                if exclusive {
                    pools.push((implicit_asset_pool(&step.name), 1));
                }
            }
            None => {
                let has_exclusive = |name: &str| {
                    self.repo
                        .node_map
                        .get(name)
                        .map(|n| n.has_exclusive_action())
                        .unwrap_or(false)
                };
                for name in step.event_names() {
                    if has_exclusive(name) {
                        pools.push((implicit_asset_pool(name), 1));
                    }
                }
                // A graph asset's own step is composition-only and never
                // executes — its inner tasks do the writing, so they carry
                // the claim. Inner tasks of one such graph therefore also
                // serialize against each other.
                if let Some((parent, _)) = step.name.split_once('/')
                    && has_exclusive(parent)
                {
                    pools.push((implicit_asset_pool(parent), 1));
                }
            }
        }
        let scope = (pools.len() > user_pools).then(|| self.asset_scope(exclusive));
        (pools, scope)
    }

    pub(crate) fn event_sender(&self) -> mpsc::UnboundedSender<WriterMsg> {
        self.sink.writer.sender()
    }

    pub(crate) fn emit_start(&self, step_name: &str, ts: i64) {
        ops::emit_step_start(self.sink.writer, self.scope.run_id, step_name, ts);
    }

    pub(crate) fn emit_success(&self, step_name: &str) {
        ops::emit_step_success(self.sink.writer, self.scope.run_id, step_name, now_ts());
    }

    /// Emit only the event — no hooks, no recording.
    pub(crate) fn emit_step_failure(
        &self,
        step_name: &str,
        msg: &str,
        classified: Option<&(rivers_core::execution::retry::FailureReason, Vec<String>)>,
    ) {
        ops::emit_step_failure(
            self.sink.writer,
            self.scope.run_id,
            step_name,
            msg,
            classified,
            now_ts(),
        );
    }

    pub(crate) fn emit_step_retry(
        &self,
        step_name: &str,
        attempt: u32,
        reason: rivers_core::execution::retry::FailureReason,
        delay: std::time::Duration,
    ) {
        ops::emit_step_retry(
            self.sink.writer,
            self.scope.run_id,
            step_name,
            attempt,
            reason,
            delay,
            now_ts(),
        );
    }

    /// Effective retry policy for a plan step (`None` = fail fast). Multi-asset
    /// steps aren't node keys themselves — their outputs are; per-output
    /// policies are validated uniform at resolve, so any output's works.
    ///
    /// Action runs never inherit the asset's materialize policy — an action
    /// declares its own retry, defaulting to none.
    pub(crate) fn retry_policy_for(
        &self,
        step: &rivers_core::execution::plan::ExecutionStep,
    ) -> Option<rivers_core::execution::retry::RetryPolicy> {
        if let Some(verb) = self.scope.plan.verb() {
            return self
                .repo
                .node_map
                .get(&step.name)
                .and_then(|n| n.find_action(verb))
                .and_then(|a| a.retry.as_ref())
                .and_then(|r| match r {
                    rivers_core::execution::retry::RetryRef::Inline(p) => Some(p.clone()),
                    rivers_core::execution::retry::RetryRef::Named(key) => {
                        self.repo.retries.get(key).cloned()
                    }
                });
        }
        self.retry_policy(&step.name)
            .or_else(|| step.outputs.iter().find_map(|n| self.retry_policy(n)))
            .cloned()
    }

    /// Retry policy of one resolved node (asset-level). Inside a K8s step pod
    /// this is always `None`: the orchestrator owns the attempt ladder there,
    /// and the pod re-applying the policy would nest retries (N pods × N
    /// in-process attempts).
    pub(crate) fn retry_policy(
        &self,
        step_name: &str,
    ) -> Option<&rivers_core::execution::retry::RetryPolicy> {
        if in_step_pod() {
            return None;
        }
        self.repo.node_map.get(step_name).and_then(|n| n.retry())
    }

    /// Per-asset compute for a plan step; multi steps read their outputs'
    /// nodes (each carries the multi-asset's single compute — declared on
    /// the multi, one step is one pod).
    pub(crate) fn compute_for(
        &self,
        step: &rivers_core::execution::plan::ExecutionStep,
    ) -> Option<rivers_core::execution::compute::Compute> {
        let lookup = |name: &str| {
            self.repo
                .node_map
                .get(name)
                .and_then(|n| n.compute().cloned())
        };
        lookup(&step.name).or_else(|| step.outputs.iter().find_map(|n| lookup(n)))
    }

    /// A failed partitioned step materialized none of its partitions, so record
    /// them all with one StepFailure carrying the whole key (a `Set` for a batched
    /// run); `get_failed_partitions` expands it. The None-keyed step-level failure
    /// (emitted separately) still drives run/step status.
    ///
    /// Action runs are skipped because the run record already says every member
    /// failed — backfill accounting reads `RunStatus::Failure` directly and never
    /// consults events for it, so the keyed event would be pure noise. Contrast
    /// `surviving_members`, which *does* emit keyed failures on action runs: a
    /// per-key `mark_partition_failed` inside an otherwise-successful run has no
    /// other record. Neither one floors a materialization — `get_failed_partitions`
    /// filters action runs out on the read side.
    pub(crate) fn emit_partition_failures(&self, step_name: &str, error: &str, ts: i64) {
        if self.scope.plan.is_action() {
            return;
        }
        if let Some(pk) = self.scope.partition_key {
            ops::emit_partition_failure(
                self.sink.writer,
                self.scope.run_id,
                step_name,
                pk,
                error,
                ts,
            );
        }
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
                for member in self.surviving_members(step_name, pk, ts) {
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

    /// Members of a batched key the step actually completed, with the keys the
    /// user marked failed peeled off into their own `StepFailure`. Unmarked
    /// keys succeeded, so only they get the step's success event.
    fn surviving_members(
        &self,
        step_name: &str,
        pk: &PyPartitionKey,
        ts: i64,
    ) -> Vec<PyPartitionKey> {
        let failed: HashMap<&PyPartitionKey, &str> = self
            .state
            .failed_partitions
            .get(step_name)
            .map(|f| f.iter().map(|(k, e)| (k, e.as_str())).collect())
            .unwrap_or_default();
        pk.members()
            .into_iter()
            .filter(|member| match failed.get(member) {
                Some(&error) => {
                    ops::emit_partition_failure(
                        self.sink.writer,
                        self.scope.run_id,
                        step_name,
                        member,
                        error,
                        ts,
                    );
                    false
                }
                None => true,
            })
            .collect()
    }

    /// Emit `Deletion` for one step (per member key when partitioned) — the
    /// storage layer clears the matching materialization state on consume.
    pub(crate) fn emit_deletion(&self, step_name: &str, action: &str, ts: i64) {
        match self.scope.partition_key {
            Some(pk) => {
                for member in self.surviving_members(step_name, pk, ts) {
                    ops::emit_deletion(
                        self.sink.writer,
                        self.scope.run_id,
                        step_name,
                        &Some(member),
                        action,
                        ts,
                    );
                }
            }
            None => ops::emit_deletion(
                self.sink.writer,
                self.scope.run_id,
                step_name,
                &None,
                action,
                ts,
            ),
        }
    }

    /// Emit `ActionCompleted` for one step (per member key when partitioned).
    pub(crate) fn emit_action_completed(
        &self,
        step_name: &str,
        action: &str,
        metadata: &[(String, MetadataValue)],
        ts: i64,
    ) {
        match self.scope.partition_key {
            Some(pk) => {
                for member in self.surviving_members(step_name, pk, ts) {
                    ops::emit_action_completed(
                        self.sink.writer,
                        self.scope.run_id,
                        step_name,
                        &Some(member),
                        action,
                        metadata,
                        ts,
                    );
                }
            }
            None => ops::emit_action_completed(
                self.sink.writer,
                self.scope.run_id,
                step_name,
                &None,
                action,
                metadata,
                ts,
            ),
        }
    }

    pub(crate) fn emit_observation(
        &self,
        step_name: &str,
        metadata: &[(String, MetadataValue)],
        data_version: Option<String>,
        ts: i64,
    ) {
        match self.scope.partition_key {
            Some(pk) => {
                for member in self.surviving_members(step_name, pk, ts) {
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
        let err_msg = error.to_string();
        let classified = Python::attach(|py| super::failure::classify_pyerr(py, &error));
        let ts = now_ts();
        ops::emit_step_failure(
            self.sink.writer,
            self.scope.run_id,
            step_name,
            &err_msg,
            Some(&classified),
            ts,
        );
        self.emit_partition_failures(step_name, &err_msg, ts);
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
                None,
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
