//! Condition evaluation state and context types.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::storage::{AssetRecord, PartitionKey};

use super::cache::BackfillState;
use super::partition::{PartitionEvalContext, PartitionSelection, PartitionState};

/// Baseline of an upstream dep as of the last tick the OBSERVING asset fired
/// (or was initial). Scoped per downstream so one asset's fire cannot consume
/// a dep-change trigger a gated sibling has not acted on yet.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct DepBaseline {
    pub last_materialized_timestamp: Option<i64>,
    pub last_data_version: Option<String>,
}

/// Per-asset state persisted across daemon ticks.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AssetConditionState {
    /// Previous tick's evaluation results keyed by node index (u32).
    pub previous_results: HashMap<u32, bool>,
    /// Previous tick's results for stateful operators evaluated inside a dep-aggregate.
    #[serde(default)]
    pub dep_previous_results: HashMap<String, HashMap<u32, bool>>,
    /// Per-dep baselines consumed by this asset's own fires (see [`DepBaseline`]).
    #[serde(default)]
    pub dep_baselines: HashMap<String, DepBaseline>,
    /// Timestamp of the last tick where this asset's condition was "handled".
    pub last_handled_timestamp: Option<i64>,
    /// The asset's `last_timestamp` as seen on the previous tick.
    pub last_materialized_timestamp: Option<i64>,
    /// The asset's `last_data_version` as seen on the previous tick.
    pub last_data_version: Option<String>,
    /// Timestamp (`ctx.now`) of the previous evaluation tick.
    pub last_tick_timestamp: Option<i64>,
    /// Fingerprint of the `ConditionNode` tree that produced this state.
    pub condition_fingerprint: String,
    /// Per-asset initial flag. Set when state is invalidated due to tree change.
    pub is_initial: bool,
    /// Partition-level state. None for unpartitioned assets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_state: Option<PartitionState>,
}

impl AssetConditionState {
    /// Reset tree-dependent state when the condition tree has changed.
    pub fn reset_for_new_tree(&mut self, new_fingerprint: String) {
        self.previous_results.clear();
        self.dep_previous_results.clear();
        self.last_handled_timestamp = None;
        self.last_tick_timestamp = None;
        self.condition_fingerprint = new_fingerprint;
        self.is_initial = true;
        self.partition_state = None;
    }

    /// Clone everything except `partition_state` (which can hold a
    /// per-partition timestamp map far too large to copy around).
    pub fn scalar_clone(&self) -> Self {
        Self {
            previous_results: self.previous_results.clone(),
            dep_previous_results: self.dep_previous_results.clone(),
            dep_baselines: self.dep_baselines.clone(),
            last_handled_timestamp: self.last_handled_timestamp,
            last_materialized_timestamp: self.last_materialized_timestamp,
            last_data_version: self.last_data_version.clone(),
            last_tick_timestamp: self.last_tick_timestamp,
            condition_fingerprint: self.condition_fingerprint.clone(),
            is_initial: self.is_initial,
            partition_state: None,
        }
    }
}

/// Dispatch intent for one tick, persisted BEFORE its runs/backfills go out
/// and cleared after the tick's eval state persists. If the daemon dies in
/// between, restart recovery replays the consumed scalar latches for every
/// entry whose dispatch demonstrably happened (its runs or a covering
/// backfill exist in storage) — without it the stale latches re-fire and
/// double-materialize (see `recover_pending_dispatch`).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PendingDispatch {
    pub tick_timestamp: i64,
    pub entries: Vec<PendingDispatchEntry>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PendingDispatchEntry {
    pub asset_key: String,
    /// Run ids minted before dispatch (run-shaped dispatches).
    pub run_ids: Vec<String>,
    /// Backfill ids pre-minted before dispatch (backfill-shaped dispatches), so
    /// recovery matches the tick's own backfill exactly instead of by asset +
    /// create time — which would falsely match an unrelated concurrent backfill.
    /// Empty for legacy intents, which fall back to the create-time heuristic.
    #[serde(default)]
    pub backfill_ids: Vec<String>,
    /// Scalar condition state as `commit_tick` would persist it. The large,
    /// derivable partition timestamp map is deliberately absent.
    pub committed: AssetConditionState,
    /// Partition keys dispatched this tick. Recovery merges these into the
    /// asset's `partition_state.handled` so a `SinceLastHandled` latch (which
    /// keeps no `previous_selections`) does not re-dispatch them — the
    /// preserved pre-crash partition state lacks this tick's keys.
    #[serde(default)]
    pub dispatched_keys: Vec<PartitionKey>,
}

/// Persisted eval-state schema version.
pub const EVAL_STATE_SCHEMA_VERSION: u32 = 1;

/// Global condition evaluation state persisted across daemon restarts (via KV store).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct ConditionEvalState {
    /// Schema stamp of the blob.
    #[serde(default)]
    pub schema_version: u32,
    /// Per-asset evaluation state.
    pub assets: HashMap<String, AssetConditionState>,
    /// Whether this is the very first evaluation (no previous tick).
    pub is_initial: bool,
    /// Asset-level failure floors (latest still-current failure timestamp),
    /// persisted so `ExecutionFailed` survives a daemon restart.
    #[serde(default)]
    pub failed_assets: HashMap<String, i64>,
}

impl Default for ConditionEvalState {
    fn default() -> Self {
        Self {
            schema_version: EVAL_STATE_SCHEMA_VERSION,
            assets: HashMap::new(),
            is_initial: false,
            failed_assets: HashMap::new(),
        }
    }
}

impl ConditionEvalState {
    /// Upgrade a just-loaded blob to the current schema in place.
    pub fn migrate_loaded(&mut self) {
        self.schema_version = EVAL_STATE_SCHEMA_VERSION;
    }
}

/// Borrowed snapshot of `AssetConditionCache` fields used during evaluation.
#[derive(Clone, Copy)]
pub struct CacheSnapshot<'a> {
    /// All (relevant) asset records, keyed by asset_key.
    pub records: &'a HashMap<String, AssetRecord>,
    /// Upstream dependencies per asset (precomputed from graph edges).
    pub upstream_deps: &'a HashMap<String, Vec<String>>,
    /// Assets currently part of in-progress (Started) runs.
    pub in_progress_assets: &'a HashSet<String>,
    /// Assets whose latest run failed.
    pub failed_assets: &'a HashSet<String>,
    /// Latest failure timestamp per currently-failed asset.
    pub failed_asset_timestamps: &'a HashMap<String, i64>,
    /// Active backfill state: asset→backfill_ids + backfill_id→partition_keys.
    pub backfill: &'a BackfillState,
}

/// Borrowed snapshot of run metadata from the cache. Each map is slotted by
/// `(asset, Option<PartitionKey>)` — the `None` slot carries unpartitioned
/// assets and unkeyed runs.
#[derive(Clone, Copy)]
pub struct RunTagSnapshot<'a> {
    /// Tags from the latest completed run per (asset, slot).
    pub last_run_tags: &'a super::cache::SlotMap<super::cache::RunTags>,
    /// Run tag sets from materializations completed this tick, per (asset, slot).
    pub tick_materialization_tags: &'a super::cache::SlotMap<Vec<super::cache::RunTags>>,
    /// Full `asset_names` from the latest completed run per (asset, slot).
    pub last_run_asset_names: &'a super::cache::SlotMap<Arc<[String]>>,
}

/// Read-only context for evaluating conditions on a single asset during one tick.
pub struct EvalContext<'a> {
    /// The asset being evaluated.
    pub target_key: &'a str,
    /// The top-level asset whose condition tree is being evaluated.
    pub root_key: &'a str,
    /// The target asset's record from storage.
    pub target_record: &'a AssetRecord,
    /// Cached asset records, graph topology, and run status.
    pub cache: CacheSnapshot<'a>,
    /// Run tag data for tag-based conditions.
    pub tags: RunTagSnapshot<'a>,
    /// Per-asset state from the previous tick.
    pub prev_state: &'a AssetConditionState,
    /// All per-asset condition states (for dep lookups in AnyDepsMatch/AllDepsMatch).
    pub all_asset_states: &'a HashMap<String, AssetConditionState>,
    /// Fired selections of assets whose conditions already fired earlier in this tick.
    pub requested_this_tick: &'a HashMap<String, PartitionSelection>,
    /// Current timestamp (nanoseconds).
    pub now: i64,
    /// Whether this is the initial evaluation (daemon just started).
    pub is_initial: bool,
    /// Partition-level evaluation context. None for unpartitioned assets.
    pub partitions: Option<&'a PartitionEvalContext<'a>>,
    /// Staleness floor for a partitioned root reading an unpartitioned dep.
    pub root_partition_floor: Option<Option<i64>>,
}

/// Result of evaluating a condition tree.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EvalResult {
    /// Whether the condition fired (asset should be materialized).
    pub fired: bool,
    /// Sub-condition results keyed by node index, for `NewlyTrue`/`Since` tracking.
    pub sub_results: HashMap<u32, bool>,
    /// Which partitions satisfy the condition (for partitioned assets).
    pub selection: Option<PartitionSelection>,
    /// Per-node partition selections for state tracking operators.
    pub sub_selections: Option<HashMap<u32, PartitionSelection>>,
    /// Per-dep results for stateful operators inside dep-aggregates (unpartitioned).
    pub dep_sub_results: HashMap<String, HashMap<u32, bool>>,
    /// Per-dep partition selections inside dep-aggregates (partitioned).
    pub dep_sub_selections: Option<HashMap<String, HashMap<u32, PartitionSelection>>>,
}

/// Status of a single node's evaluation in the tree.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum NodeStatus {
    True,
    False,
    /// Node was skipped due to short-circuit (parent And/Or).
    Skipped,
}

/// Full evaluation tree node — one per `ConditionNode`, mirroring the tree structure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EvalNodeResult {
    /// Index of this node in the pre-order traversal (matches the u32 counter system).
    pub node_idx: u32,
    /// Human-readable label (e.g. "missing", "All of", "newly_true").
    pub label: String,
    /// Type tag for UI rendering (e.g. "And", "Or", "Not", "Leaf").
    pub node_type: String,
    /// Whether this node evaluated to true, false, or was skipped.
    pub status: NodeStatus,
    /// Child evaluation results (empty for leaf nodes).
    pub children: Vec<EvalNodeResult>,
    /// For partition-aware evaluation: how many partitions matched at this node.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_partitions: Option<usize>,
}

impl EvalNodeResult {
    pub fn new(
        node: &super::node::ConditionNode,
        idx: u32,
        status: NodeStatus,
        children: Vec<Self>,
        num_partitions: Option<usize>,
    ) -> Self {
        Self {
            node_idx: idx,
            label: node.display_label(),
            node_type: node.node_type_str().to_string(),
            status,
            children,
            num_partitions,
        }
    }
}

/// Lightweight context for updating `AssetConditionState` after evaluation.
pub struct StateUpdateContext<'a> {
    pub target_record_timestamp: Option<i64>,
    pub target_data_version: Option<&'a String>,
    pub now: i64,
    pub is_initial: bool,
    pub partition_timestamps: Option<&'a HashMap<crate::storage::PartitionKey, i64>>,
}

impl<'a> StateUpdateContext<'a> {
    /// Build from an existing `EvalContext` (convenience for tests).
    pub fn from_eval_context(ctx: &'a EvalContext<'a>) -> Self {
        Self {
            target_record_timestamp: ctx.target_record.last_timestamp,
            target_data_version: ctx.target_record.last_data_version.as_ref(),
            now: ctx.now,
            is_initial: ctx.is_initial,
            partition_timestamps: ctx.partitions.map(|p| p.timestamps),
        }
    }
}

/// Update `AssetConditionState` after evaluation.
pub fn update_condition_state(
    state: &mut AssetConditionState,
    ctx: &StateUpdateContext,
    result: &EvalResult,
) {
    state.is_initial = false;
    state.last_materialized_timestamp = ctx.target_record_timestamp;
    state.last_data_version = ctx.target_data_version.cloned();
    state.last_tick_timestamp = Some(ctx.now);
    state.previous_results = result.sub_results.clone();
    state.dep_previous_results = result.dep_sub_results.clone();

    if let Some(sub_selections) = &result.sub_selections {
        if let Some(timestamps) = ctx.partition_timestamps {
            let ps = state
                .partition_state
                .get_or_insert_with(PartitionState::default);
            // Latches are stable on most ticks; comparing first skips a
            // potentially 1M-key deep clone (equality is allocation-free).
            if ps.previous_selections != *sub_selections {
                ps.previous_selections = sub_selections.clone();
            }
            if let Some(dep_sub_selections) = &result.dep_sub_selections
                && ps.dep_previous_selections != *dep_sub_selections
            {
                ps.dep_previous_selections = dep_sub_selections.clone();
            }
            ps.timestamps.retain(|key, _| timestamps.contains_key(key));
            for (key, ts) in timestamps {
                match ps.timestamps.get_mut(key) {
                    Some(v) => *v = *ts,
                    None => {
                        ps.timestamps.insert(key.clone(), *ts);
                    }
                }
            }
            ps.handled.clear();
        }
    } else {
        state.partition_state = None;
    }
}

/// Establish timestamp / data-version baselines for upstream deps that don't
/// have their own automation condition — scoped to the roots that fired (or
/// were initial) this tick, so an unrelated asset's fire cannot consume a
/// pending dep-change trigger.
pub fn update_dep_baselines(
    eval_state: &mut HashMap<String, AssetConditionState>,
    fired_or_initial: &[String],
    upstream_deps: &HashMap<String, Vec<String>>,
    conditioned_assets: &HashSet<String>,
    partition_statuses: &HashMap<String, super::cache::PartitionStatusEntry>,
    records: &HashMap<String, crate::storage::AssetRecord>,
) {
    let mut touched_deps: HashSet<&String> = HashSet::new();
    for root in fired_or_initial {
        let Some(deps) = upstream_deps.get(root) else {
            continue;
        };
        let eligible: Vec<&String> = deps
            .iter()
            .filter(|dep| !conditioned_assets.contains(*dep))
            .collect();
        if let Some(root_state) = eval_state.get_mut(root) {
            for dep in &eligible {
                root_state.dep_baselines.insert(
                    (*dep).clone(),
                    DepBaseline {
                        last_materialized_timestamp: records
                            .get(*dep)
                            .and_then(|r| r.last_timestamp),
                        last_data_version: records
                            .get(*dep)
                            .and_then(|r| r.last_data_version.clone()),
                    },
                );
            }
        }
        touched_deps.extend(eligible);
    }

    // The dep's global entry still carries partition timestamps (and the
    // scalar fallback for state written before per-dep baselines existed) —
    // written once per dep, not once per root sharing it: the timestamp map
    // can hold a million entries.
    for dep in touched_deps {
        let dep_state = eval_state.entry(dep.clone()).or_default();
        dep_state.last_materialized_timestamp = records.get(dep).and_then(|r| r.last_timestamp);
        dep_state.last_data_version = records.get(dep).and_then(|r| r.last_data_version.clone());
        if let Some(entry) = partition_statuses.get(dep) {
            let ps = dep_state
                .partition_state
                .get_or_insert_with(Default::default);
            ps.timestamps = entry.timestamps.clone();
        }
    }
}

impl NodeStatus {
    pub(crate) fn from_bool(val: bool) -> Self {
        if val {
            NodeStatus::True
        } else {
            NodeStatus::False
        }
    }
}
