//! Leaf and context helpers shared by both twin evaluators, plus the
//! output abstractions that let one match serve bool and tree modes.
use std::collections::{HashMap, HashSet};

use crate::storage::PartitionKey;

use super::super::node::ConditionNode;
use super::super::partition::{PartitionEvalContext, PartitionSelection};
use super::super::state::{EvalContext, EvalNodeResult, NodeStatus};
use super::{DepScope, count_nodes};

/// The (data version, materialized ts) baseline `DataVersionChanged` compares
/// against: in a dep pivot, the ROOT's per-dep baseline (so one asset's fire
/// can't consume another's pending trigger), falling back to the dep's global
/// state for blobs written before per-dep baselines existed; at root level,
/// the asset's own previous-tick state.
pub(crate) fn data_version_baseline<'a>(ctx: &'a EvalContext) -> (Option<&'a String>, Option<i64>) {
    if ctx.target_key != ctx.root_key
        && let Some(b) = ctx
            .all_asset_states
            .get(ctx.root_key)
            .and_then(|s| s.dep_baselines.get(ctx.target_key))
        {
            return (b.last_data_version.as_ref(), b.last_materialized_timestamp);
        }
    (
        ctx.prev_state.last_data_version.as_ref(),
        ctx.prev_state.last_materialized_timestamp,
    )
}

/// The root asset's previous-tick evaluation time.
pub(crate) fn root_last_tick(ctx: &EvalContext) -> Option<i64> {
    ctx.all_asset_states
        .get(ctx.root_key)
        .and_then(|s| s.last_tick_timestamp)
        .or_else(|| {
            (ctx.target_key == ctx.root_key)
                .then_some(ctx.prev_state.last_tick_timestamp)
                .flatten()
        })
}

/// The root asset's previous-tick (handled, tick) timestamps, as a pair.
pub(crate) fn root_handled_state(ctx: &EvalContext) -> (Option<i64>, Option<i64>) {
    match ctx.all_asset_states.get(ctx.root_key) {
        Some(s) => (s.last_handled_timestamp, s.last_tick_timestamp),
        None if ctx.target_key == ctx.root_key => (
            ctx.prev_state.last_handled_timestamp,
            ctx.prev_state.last_tick_timestamp,
        ),
        None => (None, None),
    }
}

/// Merge (not replace) a dep's stateful-node results into the per-dep accumulator.
pub(crate) fn collect_dep_latch<V>(scope: &mut DepScope<V>, dep_key: &str, local: HashMap<u32, V>) {
    if local.is_empty() {
        return;
    }
    if let Some(marks) = scope.bridged.get_mut(dep_key) {
        for idx in local.keys() {
            marks.remove(idx);
        }
    }
    scope
        .acc
        .entry(dep_key.to_string())
        .or_default()
        .extend(local);
}

/// Merge latches bridged out of the unpartitioned bool fallback (`from_bool`: true → `All`).
pub(crate) fn collect_bridged_latch(
    scope: &mut DepScope<PartitionSelection>,
    dep_key: &str,
    local: HashMap<u32, PartitionSelection>,
) {
    if local.is_empty() {
        return;
    }
    let slot = scope.acc.entry(dep_key.to_string()).or_default();
    let marks = scope.bridged.entry(dep_key.to_string()).or_default();
    for (idx, sel) in local {
        match slot.entry(idx) {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(sel);
                marks.insert(idx);
            }
            std::collections::hash_map::Entry::Occupied(mut e) => {
                if marks.contains(&idx) {
                    let merged = e.get().union(&sel);
                    e.insert(merged);
                }
            }
        }
    }
}

/// Restrict a partition-status set (failed / in_progress) to the live universe.
pub(crate) fn select_in_universe(
    keys: &HashSet<PartitionKey>,
    pctx: &PartitionEvalContext,
) -> PartitionSelection {
    let live: HashSet<PartitionKey> = keys
        .iter()
        .filter(|k| pctx.all_keys.contains(*k))
        .cloned()
        .collect();
    PartitionSelection::from_keys(live)
}

/// Previous-tick selection for stateful node `my_idx`: the per-dep latch when
/// pivoting into a dep (`cur_prev`), otherwise the asset's own state.
pub(crate) fn prev_partition_latch(
    dep_selections: &DepScope<PartitionSelection>,
    ctx: &EvalContext,
    my_idx: u32,
) -> PartitionSelection {
    match dep_selections.cur_prev {
        Some(map) => map.get(&my_idx).cloned(),
        None => ctx
            .prev_state
            .partition_state
            .as_ref()
            .and_then(|ps| ps.previous_selections.get(&my_idx))
            .cloned(),
    }
    .unwrap_or(PartitionSelection::Empty)
}

/// Output mode for the unpartitioned evaluator.
pub(crate) trait EvalOutput: Sized {
    /// Whether composite nodes should collect child outputs into a Vec.
    /// False for bool mode (zero-allocation), true for tree mode.
    const COLLECTS_CHILDREN: bool;
    fn leaf(val: bool, idx: u32, node: &ConditionNode) -> Self;
    fn val(&self) -> bool;
    fn composite(val: bool, idx: u32, node: &ConditionNode, children: Vec<Self>) -> Self;
    fn skipped(node: &ConditionNode, counter: &mut u32) -> Self;
}

impl EvalOutput for bool {
    const COLLECTS_CHILDREN: bool = false;
    fn leaf(val: bool, _idx: u32, _node: &ConditionNode) -> Self {
        val
    }
    fn val(&self) -> bool {
        *self
    }
    fn composite(val: bool, _idx: u32, _node: &ConditionNode, _children: Vec<Self>) -> Self {
        val
    }
    fn skipped(node: &ConditionNode, counter: &mut u32) -> Self {
        count_nodes(node, counter);
        false
    }
}

impl EvalOutput for EvalNodeResult {
    const COLLECTS_CHILDREN: bool = true;
    fn leaf(val: bool, idx: u32, node: &ConditionNode) -> Self {
        EvalNodeResult::new(node, idx, NodeStatus::from_bool(val), vec![], None)
    }
    fn val(&self) -> bool {
        self.status == NodeStatus::True
    }
    fn composite(val: bool, idx: u32, node: &ConditionNode, children: Vec<Self>) -> Self {
        EvalNodeResult::new(node, idx, NodeStatus::from_bool(val), children, None)
    }
    fn skipped(node: &ConditionNode, counter: &mut u32) -> Self {
        build_skipped_subtree(node, counter)
    }
}

/// Output mode for the partition-aware evaluator.
pub(crate) trait PartEvalOutput: Sized {
    type Child;
    fn leaf(sel: PartitionSelection, idx: u32, node: &ConditionNode, total: usize) -> Self;
    fn into_parts(self) -> (PartitionSelection, Self::Child);
    fn composite(
        sel: PartitionSelection,
        idx: u32,
        node: &ConditionNode,
        total: usize,
        children: Vec<Self::Child>,
    ) -> Self;
    fn skipped_child(node: &ConditionNode, counter: &mut u32) -> Self::Child;
}

impl PartEvalOutput for PartitionSelection {
    type Child = ();
    fn leaf(sel: PartitionSelection, _idx: u32, _node: &ConditionNode, _total: usize) -> Self {
        sel
    }
    fn into_parts(self) -> (PartitionSelection, ()) {
        (self, ())
    }
    fn composite(
        sel: PartitionSelection,
        _idx: u32,
        _node: &ConditionNode,
        _total: usize,
        _children: Vec<()>,
    ) -> Self {
        sel
    }
    fn skipped_child(node: &ConditionNode, counter: &mut u32) {
        count_nodes(node, counter);
    }
}

impl PartEvalOutput for (PartitionSelection, EvalNodeResult) {
    type Child = EvalNodeResult;
    fn leaf(sel: PartitionSelection, idx: u32, node: &ConditionNode, total: usize) -> Self {
        let n = sel.key_count(total);
        let tree = EvalNodeResult::new(
            node,
            idx,
            NodeStatus::from_bool(!sel.is_empty()),
            vec![],
            Some(n),
        );
        (sel, tree)
    }
    fn into_parts(self) -> (PartitionSelection, EvalNodeResult) {
        self
    }
    fn composite(
        sel: PartitionSelection,
        idx: u32,
        node: &ConditionNode,
        total: usize,
        children: Vec<EvalNodeResult>,
    ) -> Self {
        let n = sel.key_count(total);
        let tree = EvalNodeResult::new(
            node,
            idx,
            NodeStatus::from_bool(!sel.is_empty()),
            children,
            Some(n),
        );
        (sel, tree)
    }
    fn skipped_child(node: &ConditionNode, counter: &mut u32) -> EvalNodeResult {
        build_skipped_subtree(node, counter)
    }
}

/// Build a skipped tree for a subtree that was not evaluated due to short-circuiting.
pub(crate) fn build_skipped_subtree(node: &ConditionNode, counter: &mut u32) -> EvalNodeResult {
    let my_idx = *counter;
    *counter += 1;
    // Dep-aggregates emit a childless leaf but advance the counter past the
    // inner nodes; mirror that when skipped so node indices stay aligned.
    let is_dep_aggregate = matches!(
        node,
        ConditionNode::AnyDepsMatch { .. }
            | ConditionNode::AllDepsMatch { .. }
            | ConditionNode::AssetMatches { .. }
    );
    let children = if is_dep_aggregate {
        for child in node.children() {
            let _ = build_skipped_subtree(child, counter);
        }
        Vec::new()
    } else {
        node.children()
            .map(|c| build_skipped_subtree(c, counter))
            .collect()
    };
    EvalNodeResult::new(node, my_idx, NodeStatus::Skipped, children, None)
}

/// Partition-aware BackfillInProgress: collect targeted partition keys from all
/// active backfills for this asset, intersect with the asset's partition space.
pub(crate) fn eval_backfill_in_progress_partitioned(
    ctx: &EvalContext,
    pctx: &PartitionEvalContext,
) -> PartitionSelection {
    let backfill_ids = match ctx.cache.backfill.assets.get(ctx.target_key) {
        Some(ids) => ids,
        None => return PartitionSelection::Empty,
    };

    let mut targeted: HashSet<PartitionKey> = HashSet::new();
    for bf_id in backfill_ids {
        match ctx.cache.backfill.partition_keys.get(bf_id) {
            Some(bf_partitions) if !bf_partitions.is_empty() => {
                let bf_set: HashSet<PartitionKey> = bf_partitions.iter().cloned().collect();
                targeted.extend(pctx.all_keys.intersection(&bf_set).cloned());
            }
            _ => {
                return PartitionSelection::All;
            }
        }
    }

    PartitionSelection::from_keys(targeted)
}

/// Filter an asset's slotted map, selecting the `Some(partition)` slots that
/// match a predicate and are in the asset's partition space (the `None` slot
/// belongs to the unpartitioned eval path).
pub(crate) fn partition_filter_select<V>(
    slots: Option<&HashMap<Option<PartitionKey>, V>>,
    pctx: &PartitionEvalContext,
    pred: impl Fn(&V) -> bool,
) -> PartitionSelection {
    let data = match slots {
        Some(d) => d,
        None => return PartitionSelection::Empty,
    };
    let matching: HashSet<PartitionKey> = data
        .iter()
        .filter_map(|(pk, val)| match pk {
            Some(pk) if pctx.all_keys.contains(pk) && pred(val) => Some(pk.clone()),
            _ => None,
        })
        .collect();
    PartitionSelection::from_keys(matching)
}

/// Unpartitioned HasRunWithTags / AllRunsHaveTags evaluator.
pub(crate) fn eval_new_update_tags(
    ctx: &EvalContext,
    tag_keys: &[String],
    tag_values: &[(String, String)],
    require_all: bool,
) -> bool {
    ctx.tags
        .tick_materialization_tags
        .get(ctx.target_key)
        .and_then(|slots| slots.get(&None))
        .map(|tag_sets| {
            !tag_sets.is_empty()
                && if require_all {
                    tag_sets
                        .iter()
                        .all(|tags| run_tags_match(tags, tag_keys, tag_values))
                } else {
                    tag_sets
                        .iter()
                        .any(|tags| run_tags_match(tags, tag_keys, tag_values))
                }
        })
        .unwrap_or(false)
}

/// Partition-aware HasRunWithTags: for each partition, check if any of the
/// tick's materializations came from runs with matching tags.
pub(crate) fn eval_new_update_tags_partitioned(
    ctx: &EvalContext,
    pctx: &PartitionEvalContext,
    tag_keys: &[String],
    tag_values: &[(String, String)],
    require_all: bool,
) -> PartitionSelection {
    partition_filter_select(
        ctx.tags.tick_materialization_tags.get(ctx.target_key),
        pctx,
        |tag_sets| {
            !tag_sets.is_empty()
                && if require_all {
                    tag_sets
                        .iter()
                        .all(|tags| run_tags_match(tags, tag_keys, tag_values))
                } else {
                    tag_sets
                        .iter()
                        .any(|tags| run_tags_match(tags, tag_keys, tag_values))
                }
        },
    )
}

/// Check if `run_tags` satisfies the `tag_keys` (key-presence) and `tag_values` (exact k-v match).
pub(crate) fn run_tags_match(
    run_tags: &[(String, String)],
    tag_keys: &[String],
    tag_values: &[(String, String)],
) -> bool {
    tag_keys
        .iter()
        .all(|k| run_tags.iter().any(|(rk, _)| rk == k))
        && tag_values.iter().all(|t| run_tags.contains(t))
}

/// True if the tree contains `NewlyUpdated` — the only consumer of
/// `dep_root_floor`, whose construction walks every upstream timestamp.
pub(crate) fn contains_newly_updated(node: &ConditionNode) -> bool {
    matches!(node, ConditionNode::NewlyUpdated) || node.children().any(contains_newly_updated)
}

/// Whether a dep at `dep_ts` counts as newly-updated against a downstream
/// key's effective staleness `floor`: fire when the key was never attempted
/// (`None`) or the dep is strictly newer than the floor.
pub(crate) fn dep_newer_than_floor(dep_ts: i64, floor: Option<i64>) -> bool {
    floor.is_none_or(|f| dep_ts > f)
}

/// A key's effective attempt timestamp: the later of its last
/// materialization and last (still-current) failure; `None` if never attempted.
pub(crate) fn effective_attempt_ts(
    root_status: &crate::condition::cache::PartitionStatusEntry,
    k: &PartitionKey,
) -> Option<i64> {
    match (
        root_status.timestamps.get(k),
        root_status.failed_timestamps.get(k),
    ) {
        (None, None) => None,
        (Some(&m), None) => Some(m),
        (None, Some(&f)) => Some(f),
        (Some(&m), Some(&f)) => Some(m.max(f)),
    }
}

/// The staleness floor across the downstream keys a dep key maps to: the
/// minimum per-key effective timestamp. Any never-attempted key collapses
/// the floor to `None` (that key must be built, so the dep counts as newer).
pub(crate) fn root_floor_over<'k>(
    keys: impl Iterator<Item = &'k PartitionKey>,
    root_status: &crate::condition::cache::PartitionStatusEntry,
) -> Option<i64> {
    let mut floor: Option<i64> = None;
    for k in keys {
        let effective = effective_attempt_ts(root_status, k)?;
        floor = Some(floor.map_or(effective, |fl| fl.min(effective)));
    }
    floor
}

/// Like `root_floor_over` but for fan-out (`AllPartitions` and bridged
/// unpartitioned-dep) edges: floor only over keys actually attempted,
/// ignoring never-attempted ones instead of collapsing to `None`.
pub(crate) fn root_floor_over_attempted<'k>(
    keys: impl Iterator<Item = &'k PartitionKey>,
    root_status: &crate::condition::cache::PartitionStatusEntry,
) -> Option<i64> {
    keys.filter_map(|k| effective_attempt_ts(root_status, k))
        .min()
}
