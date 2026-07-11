//! Core condition evaluator.

use std::collections::{HashMap, HashSet};

use crate::storage::PartitionKey;

use super::node::ConditionNode;
use super::partition::{PartitionEvalContext, PartitionResolver, PartitionSelection};
use super::state::{AssetConditionState, EvalContext, EvalNodeResult, EvalResult, NodeStatus};

/// Per-dep latch state for stateful ops inside dep-aggregates, keyed by dep then node index.
struct DepScope<'a, V> {
    prev: &'a HashMap<String, HashMap<u32, V>>,
    acc: &'a mut HashMap<String, HashMap<u32, V>>,
    cur_prev: Option<&'a HashMap<u32, V>>,
    bridged: HashMap<String, HashSet<u32>>,
}

static EMPTY_DEP_SELECTIONS: std::sync::LazyLock<
    HashMap<String, HashMap<u32, PartitionSelection>>,
> = std::sync::LazyLock::new(HashMap::new);

/// Empty per-dep latch maps for pivots into a dep with no persisted latch yet.
static EMPTY_BOOL_LATCH: std::sync::LazyLock<HashMap<u32, bool>> =
    std::sync::LazyLock::new(HashMap::new);
static EMPTY_SELECTION_LATCH: std::sync::LazyLock<HashMap<u32, PartitionSelection>> =
    std::sync::LazyLock::new(HashMap::new);

/// Root's previous-tick per-dep partition latches (empty when unpartitioned).
fn root_dep_selections<'a>(
    ctx: &'a EvalContext,
) -> &'a HashMap<String, HashMap<u32, PartitionSelection>> {
    ctx.prev_state
        .partition_state
        .as_ref()
        .map(|ps| &ps.dep_previous_selections)
        .unwrap_or(&EMPTY_DEP_SELECTIONS)
}

/// The (data version, materialized ts) baseline `DataVersionChanged` compares
/// against: in a dep pivot, the ROOT's per-dep baseline (so one asset's fire
/// can't consume another's pending trigger), falling back to the dep's global
/// state for blobs written before per-dep baselines existed; at root level,
/// the asset's own previous-tick state.
fn data_version_baseline<'a>(ctx: &'a EvalContext) -> (Option<&'a String>, Option<i64>) {
    if ctx.target_key != ctx.root_key {
        if let Some(b) = ctx
            .all_asset_states
            .get(ctx.root_key)
            .and_then(|s| s.dep_baselines.get(ctx.target_key))
        {
            return (b.last_data_version.as_ref(), b.last_materialized_timestamp);
        }
    }
    (
        ctx.prev_state.last_data_version.as_ref(),
        ctx.prev_state.last_materialized_timestamp,
    )
}

/// The root asset's previous-tick evaluation time.
fn root_last_tick(ctx: &EvalContext) -> Option<i64> {
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
fn root_handled_state(ctx: &EvalContext) -> (Option<i64>, Option<i64>) {
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
fn collect_dep_latch<V>(scope: &mut DepScope<V>, dep_key: &str, local: HashMap<u32, V>) {
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
fn collect_bridged_latch(
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
fn select_in_universe(
    keys: &HashSet<PartitionKey>,
    pctx: &PartitionEvalContext,
) -> PartitionSelection {
    let live: HashSet<PartitionKey> = keys
        .iter()
        .filter(|k| pctx.all_keys.contains(*k))
        .cloned()
        .collect();
    if live.is_empty() {
        PartitionSelection::Empty
    } else {
        PartitionSelection::Keys(live)
    }
}

/// Previous-tick selection for stateful node `my_idx`: the per-dep latch when
/// pivoting into a dep (`cur_prev`), otherwise the asset's own state.
fn prev_partition_latch(
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
trait EvalOutput: Sized {
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
trait PartEvalOutput: Sized {
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
fn build_skipped_subtree(node: &ConditionNode, counter: &mut u32) -> EvalNodeResult {
    let my_idx = *counter;
    *counter += 1;
    let children = node
        .children()
        .map(|c| build_skipped_subtree(c, counter))
        .collect();
    EvalNodeResult::new(node, my_idx, NodeStatus::Skipped, children, None)
}

/// Evaluate a `ConditionNode` tree against the given context.
pub fn evaluate(node: &ConditionNode, ctx: &EvalContext) -> EvalResult {
    if let Some(pctx) = ctx.partitions {
        let mut counter = 0u32;
        let mut sub_selections = HashMap::new();
        let mut dep_selections = HashMap::new();
        let mut dep_scope = DepScope {
            prev: root_dep_selections(ctx),
            acc: &mut dep_selections,
            cur_prev: None,
            bridged: HashMap::new(),
        };
        let selection: PartitionSelection = eval_partitioned(
            node,
            ctx,
            pctx,
            &mut counter,
            &mut sub_selections,
            &mut dep_scope,
        );
        // `All` of an empty universe selects nothing (mirrors
        // `evaluate_with_tree`): reporting fired would leak a full
        // WillBeRequested signal downstream.
        let fired = match &selection {
            PartitionSelection::All => !pctx.all_keys.is_empty(),
            other => !other.is_empty(),
        };
        tracing::debug!(
            target: "rivers::condition",
            asset_key = %ctx.target_key,
            fired,
            "partition condition evaluated"
        );
        EvalResult {
            fired,
            sub_results: HashMap::new(),
            selection: Some(selection),
            sub_selections: Some(sub_selections),
            dep_sub_results: HashMap::new(),
            dep_sub_selections: Some(dep_selections),
        }
    } else {
        let mut counter = 0u32;
        let mut sub_results = HashMap::new();
        let mut dep_results = HashMap::new();
        let mut dep_scope = DepScope {
            prev: &ctx.prev_state.dep_previous_results,
            acc: &mut dep_results,
            cur_prev: None,
            bridged: HashMap::new(),
        };
        let fired: bool = eval_inner(node, ctx, &mut counter, &mut sub_results, &mut dep_scope);
        tracing::debug!(
            target: "rivers::condition",
            asset_key = %ctx.target_key,
            fired,
            "condition evaluated"
        );
        EvalResult {
            fired,
            sub_results,
            selection: None,
            sub_selections: None,
            dep_sub_results: dep_results,
            dep_sub_selections: None,
        }
    }
}

/// Recursive inner evaluator (unpartitioned / bool semantics).
///
/// UNIFICATION: this match and [`eval_partitioned`] are parallel 25-arm
/// evaluators reconciled by the `DepScope::bridged` machinery; a semantics
/// change landing on only one side makes partitioned and unpartitioned assets
/// take different fire decisions. Bool is a `PartitionSelection` over a unit
/// universe (`from_bool`/`to_bool`), so a single selection-based evaluator can
/// subsume both — that refactor needs a persisted-latch migration
/// (`previous_results` → selections) and an output-trait merge; until then,
/// mirror every semantic change in BOTH matches.
fn eval_inner<O: EvalOutput>(
    node: &ConditionNode,
    ctx: &EvalContext,
    counter: &mut u32,
    sub_results: &mut HashMap<u32, bool>,
    dep_results: &mut DepScope<bool>,
) -> O {
    let my_idx = *counter;
    *counter += 1;

    match node {
        ConditionNode::Missing => O::leaf(ctx.target_record.last_run_id.is_none(), my_idx, node),
        ConditionNode::InProgress => O::leaf(
            ctx.cache.in_progress_assets.contains(ctx.target_key),
            my_idx,
            node,
        ),
        ConditionNode::ExecutionFailed => O::leaf(
            ctx.cache.failed_assets.contains(ctx.target_key),
            my_idx,
            node,
        ),
        ConditionNode::CodeVersionChanged => {
            let expr = ctx.target_record.code_version.is_some()
                && ctx.target_record.code_version
                    != ctx.target_record.last_materialization_code_version;
            O::leaf(expr, my_idx, node)
        }
        ConditionNode::NewlyUpdated => {
            if ctx.target_key != ctx.root_key {
                let expr = match ctx.target_record.last_timestamp {
                    None => false,
                    Some(dep_ts) => match ctx.root_partition_floor {
                        Some(floor) => dep_newer_than_floor(dep_ts, floor),
                        None => {
                            let root_mat = ctx
                                .cache
                                .records
                                .get(ctx.root_key)
                                .and_then(|r| r.last_timestamp);
                            let root_failed =
                                ctx.cache.failed_asset_timestamps.get(ctx.root_key).copied();
                            match (root_mat, root_failed) {
                                (None, None) => true,
                                (Some(m), None) => dep_ts > m,
                                (None, Some(f)) => dep_ts > f,
                                (Some(m), Some(f)) => dep_ts > m.max(f),
                            }
                        }
                    },
                };
                return O::leaf(expr, my_idx, node);
            }
            let expr = match (
                ctx.target_record.last_timestamp,
                ctx.prev_state.last_materialized_timestamp,
            ) {
                (Some(current), Some(prev)) => current > prev,
                (Some(_), None) => !ctx.is_initial,
                _ => false,
            };
            O::leaf(expr, my_idx, node)
        }
        ConditionNode::NewlyRequested => {
            let expr = ctx.prev_state.last_handled_timestamp.is_some()
                && ctx.prev_state.last_handled_timestamp == ctx.prev_state.last_tick_timestamp;
            O::leaf(expr, my_idx, node)
        }

        ConditionNode::CronTickPassed {
            cron_schedule,
            timezone,
        } => {
            let prev_ts = root_last_tick(ctx).unwrap_or(ctx.now);
            O::leaf(
                cron_tick_between(cron_schedule, prev_ts, ctx.now, timezone.as_deref()),
                my_idx,
                node,
            )
        }

        ConditionNode::InLatestTimeWindow { .. } => O::leaf(true, my_idx, node),

        ConditionNode::InitialEvaluation => O::leaf(ctx.is_initial, my_idx, node),

        ConditionNode::DataVersionChanged => {
            let (prev_dv, prev_ts) = data_version_baseline(ctx);
            let expr = match (ctx.target_record.last_data_version.as_ref(), prev_dv) {
                (Some(current), Some(prev)) => current != prev,
                (Some(_), None) => {
                    !ctx.is_initial && prev_ts != ctx.target_record.last_timestamp
                }
                _ => false,
            };
            O::leaf(expr, my_idx, node)
        }

        ConditionNode::BackfillInProgress => O::leaf(
            ctx.cache.backfill.assets.contains_key(ctx.target_key),
            my_idx,
            node,
        ),

        ConditionNode::LastExecutedWithTags {
            tag_keys,
            tag_values,
        } => {
            let expr = ctx
                .tags
                .last_run_tags
                .get(ctx.target_key)
                .map(|run_tags| run_tags_match(run_tags, tag_keys, tag_values))
                .unwrap_or(false);
            O::leaf(expr, my_idx, node)
        }

        ConditionNode::LastRunIncludesTarget => {
            let expr = if ctx.target_key == ctx.root_key {
                false
            } else {
                ctx.tags
                    .last_run_asset_names
                    .get(ctx.target_key)
                    .map(|names| names.iter().any(|n| n == ctx.root_key))
                    .unwrap_or(false)
            };
            O::leaf(expr, my_idx, node)
        }

        ConditionNode::WillBeRequested => O::leaf(
            ctx.requested_this_tick.contains_key(ctx.target_key),
            my_idx,
            node,
        ),

        ConditionNode::HasRunWithTags {
            tag_keys,
            tag_values,
        } => O::leaf(
            eval_new_update_tags(ctx, tag_keys, tag_values, false),
            my_idx,
            node,
        ),

        ConditionNode::AllRunsHaveTags {
            tag_keys,
            tag_values,
        } => O::leaf(
            eval_new_update_tags(ctx, tag_keys, tag_values, true),
            my_idx,
            node,
        ),

        ConditionNode::AnyDepsMatch { condition, .. } => {
            let base = *counter;
            let eval_all = condition.has_stateful_nodes();
            let val = ctx
                .cache
                .upstream_deps
                .get(ctx.target_key)
                .map(|deps| {
                    let mut any = false;
                    for dep in deps {
                        *counter = base;
                        if eval_on_dep(dep, condition, ctx, counter, dep_results) {
                            any = true;
                            if !eval_all {
                                break;
                            }
                        }
                    }
                    any
                })
                .unwrap_or(false);
            finalize_dep_counter(counter, base, condition);
            O::leaf(val, my_idx, node)
        }

        ConditionNode::AllDepsMatch { condition, .. } => {
            let base = *counter;
            let eval_all = condition.has_stateful_nodes();
            let val = ctx
                .cache
                .upstream_deps
                .get(ctx.target_key)
                .map(|deps| {
                    let mut all = true;
                    for dep in deps {
                        *counter = base;
                        if !eval_on_dep(dep, condition, ctx, counter, dep_results) {
                            all = false;
                            if !eval_all {
                                break;
                            }
                        }
                    }
                    all
                })
                .unwrap_or(true);
            finalize_dep_counter(counter, base, condition);
            O::leaf(val, my_idx, node)
        }

        ConditionNode::AssetMatches { keys, condition } => {
            let base = *counter;
            let eval_all = condition.has_stateful_nodes();
            let mut val = false;
            for key in keys {
                *counter = base;
                if eval_on_dep(key, condition, ctx, counter, dep_results) {
                    val = true;
                    if !eval_all {
                        break;
                    }
                }
            }
            finalize_dep_counter(counter, base, condition);
            O::leaf(val, my_idx, node)
        }

        ConditionNode::And(children) => {
            let mut result = true;
            let mut child_outs = if O::COLLECTS_CHILDREN {
                Vec::with_capacity(children.len())
            } else {
                Vec::new()
            };
            for child in children {
                if result {
                    let out = eval_inner::<O>(child, ctx, counter, sub_results, dep_results);
                    result = out.val();
                    if O::COLLECTS_CHILDREN {
                        child_outs.push(out);
                    }
                } else if child.has_stateful_nodes() {
                    let out = eval_inner::<O>(child, ctx, counter, sub_results, dep_results);
                    if O::COLLECTS_CHILDREN {
                        child_outs.push(out);
                    }
                } else if O::COLLECTS_CHILDREN {
                    child_outs.push(O::skipped(child, counter));
                } else {
                    count_nodes(child, counter);
                }
            }
            O::composite(result, my_idx, node, child_outs)
        }

        ConditionNode::Or(children) => {
            let mut result = false;
            let mut child_outs = if O::COLLECTS_CHILDREN {
                Vec::with_capacity(children.len())
            } else {
                Vec::new()
            };
            for child in children {
                if !result {
                    let out = eval_inner::<O>(child, ctx, counter, sub_results, dep_results);
                    result = out.val();
                    if O::COLLECTS_CHILDREN {
                        child_outs.push(out);
                    }
                } else if child.has_stateful_nodes() {
                    let out = eval_inner::<O>(child, ctx, counter, sub_results, dep_results);
                    if O::COLLECTS_CHILDREN {
                        child_outs.push(out);
                    }
                } else if O::COLLECTS_CHILDREN {
                    child_outs.push(O::skipped(child, counter));
                } else {
                    count_nodes(child, counter);
                }
            }
            O::composite(result, my_idx, node, child_outs)
        }

        ConditionNode::Not(child) => {
            let child_out = eval_inner::<O>(child, ctx, counter, sub_results, dep_results);
            let val = !child_out.val();
            if O::COLLECTS_CHILDREN {
                O::composite(val, my_idx, node, vec![child_out])
            } else {
                O::leaf(val, my_idx, node)
            }
        }

        ConditionNode::NewlyTrue(child) => {
            let child_out = eval_inner::<O>(child, ctx, counter, sub_results, dep_results);
            let current = child_out.val();
            let previous = dep_results
                .cur_prev
                .unwrap_or(&ctx.prev_state.previous_results)
                .get(&my_idx)
                .copied()
                .unwrap_or(false);
            let result = current && !previous;
            sub_results.insert(my_idx, current);
            if O::COLLECTS_CHILDREN {
                O::composite(result, my_idx, node, vec![child_out])
            } else {
                O::leaf(result, my_idx, node)
            }
        }

        ConditionNode::Since { trigger, reset } => {
            let trigger_out = eval_inner::<O>(trigger, ctx, counter, sub_results, dep_results);
            let reset_out = eval_inner::<O>(reset, ctx, counter, sub_results, dep_results);
            let trigger_val = trigger_out.val();
            let reset_val = reset_out.val();
            let prev_latch = dep_results
                .cur_prev
                .unwrap_or(&ctx.prev_state.previous_results)
                .get(&my_idx)
                .copied()
                .unwrap_or(false);
            let result = if reset_val {
                false
            } else {
                trigger_val || prev_latch
            };
            sub_results.insert(my_idx, result);
            if O::COLLECTS_CHILDREN {
                O::composite(result, my_idx, node, vec![trigger_out, reset_out])
            } else {
                O::leaf(result, my_idx, node)
            }
        }

        ConditionNode::SinceLastHandled(child) => {
            let child_out = eval_inner::<O>(child, ctx, counter, sub_results, dep_results);
            let current = child_out.val();
            let result = if !current {
                false
            } else {
                let (last_handled, last_tick) = root_handled_state(ctx);
                match last_handled {
                    None => true,
                    Some(handled) => last_tick.map(|lt| handled < lt).unwrap_or(true),
                }
            };
            if O::COLLECTS_CHILDREN {
                O::composite(result, my_idx, node, vec![child_out])
            } else {
                O::leaf(result, my_idx, node)
            }
        }
    }
}

/// Increment the counter for every node in a subtree without evaluating.
fn count_nodes(node: &ConditionNode, counter: &mut u32) {
    *counter += 1;
    for child in node.children() {
        count_nodes(child, counter);
    }
}

/// Advance `counter` to `base + count_nodes(condition)`, the deterministic number
/// of node-index slots a dep-aggregate consumes.
fn finalize_dep_counter(counter: &mut u32, base: u32, condition: &ConditionNode) {
    *counter = base;
    count_nodes(condition, counter);
}

/// Evaluate a `ConditionNode` tree and return both the compact result (for
/// state tracking) and a full evaluation tree (for UI visualization).
pub fn evaluate_with_tree(node: &ConditionNode, ctx: &EvalContext) -> (EvalResult, EvalNodeResult) {
    if let Some(pctx) = ctx.partitions {
        let mut counter = 0u32;
        let mut sub_selections = HashMap::new();
        let mut dep_selections = HashMap::new();
        let mut dep_scope = DepScope {
            prev: root_dep_selections(ctx),
            acc: &mut dep_selections,
            cur_prev: None,
            bridged: HashMap::new(),
        };
        let (selection, tree): (PartitionSelection, EvalNodeResult) = eval_partitioned(
            node,
            ctx,
            pctx,
            &mut counter,
            &mut sub_selections,
            &mut dep_scope,
        );
        // `All` of an empty universe selects nothing: reporting fired would
        // leak a full WillBeRequested signal downstream with nothing to
        // materialize behind it.
        let fired = match &selection {
            PartitionSelection::All => !pctx.all_keys.is_empty(),
            other => !other.is_empty(),
        };
        (
            EvalResult {
                fired,
                sub_results: HashMap::new(),
                selection: Some(selection),
                sub_selections: Some(sub_selections),
                dep_sub_results: HashMap::new(),
                dep_sub_selections: Some(dep_selections),
            },
            tree,
        )
    } else {
        let mut counter = 0u32;
        let mut sub_results = HashMap::new();
        let mut dep_results = HashMap::new();
        let mut dep_scope = DepScope {
            prev: &ctx.prev_state.dep_previous_results,
            acc: &mut dep_results,
            cur_prev: None,
            bridged: HashMap::new(),
        };
        let tree =
            eval_inner::<EvalNodeResult>(node, ctx, &mut counter, &mut sub_results, &mut dep_scope);
        let fired = tree.status == NodeStatus::True;
        (
            EvalResult {
                fired,
                sub_results,
                selection: None,
                sub_selections: None,
                dep_sub_results: dep_results,
                dep_sub_selections: None,
            },
            tree,
        )
    }
}

/// Partition-aware BackfillInProgress: collect targeted partition keys from all
/// active backfills for this asset, intersect with the asset's partition space.
fn eval_backfill_in_progress_partitioned(
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

    if targeted.is_empty() {
        PartitionSelection::Empty
    } else {
        PartitionSelection::Keys(targeted)
    }
}

/// Filter a per-partition map, selecting partitions that match a predicate
/// and are in the asset's partition space.
fn partition_filter_select<V>(
    map: Option<&HashMap<PartitionKey, V>>,
    pctx: &PartitionEvalContext,
    pred: impl Fn(&V) -> bool,
) -> PartitionSelection {
    let data = match map {
        Some(d) => d,
        None => return PartitionSelection::Empty,
    };
    let matching: HashSet<PartitionKey> = data
        .iter()
        .filter(|(pk, val)| pctx.all_keys.contains(pk) && pred(val))
        .map(|(pk, _)| pk.clone())
        .collect();
    if matching.is_empty() {
        PartitionSelection::Empty
    } else {
        PartitionSelection::Keys(matching)
    }
}

/// Unpartitioned HasRunWithTags / AllRunsHaveTags evaluator.
fn eval_new_update_tags(
    ctx: &EvalContext,
    tag_keys: &[String],
    tag_values: &[(String, String)],
    require_all: bool,
) -> bool {
    ctx.tags
        .tick_materialization_tags
        .get(ctx.target_key)
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
fn eval_new_update_tags_partitioned(
    ctx: &EvalContext,
    pctx: &PartitionEvalContext,
    tag_keys: &[String],
    tag_values: &[(String, String)],
    require_all: bool,
) -> PartitionSelection {
    let pk_tag_sets = match ctx
        .tags
        .tick_partition_materialization_tags
        .get(ctx.target_key)
    {
        Some(tags) => tags,
        None => return PartitionSelection::Empty,
    };
    let matching: HashSet<PartitionKey> = pk_tag_sets
        .iter()
        .filter(|(pk, tag_sets)| {
            pctx.all_keys.contains(pk)
                && !tag_sets.is_empty()
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
        .map(|(pk, _)| pk.clone())
        .collect();
    if matching.is_empty() {
        PartitionSelection::Empty
    } else {
        PartitionSelection::Keys(matching)
    }
}

/// Parse a cron schedule (seconds optional) in the shared rivers dialect.
fn build_cron(schedule: &str) -> Result<croner::Cron, String> {
    crate::timegrid::parse_cron(schedule).map_err(|e| e.to_string())
}

/// Validate a cron schedule at construction so bad input is rejected up front.
pub fn validate_cron(schedule: &str) -> Result<(), String> {
    build_cron(schedule).map(|_| ())
}

/// Validate an IANA timezone name at construction (parsed via `chrono-tz`).
pub fn validate_timezone(tz: &str) -> Result<(), String> {
    tz.parse::<chrono_tz::Tz>()
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Next cron occurrence strictly after `after`, as a real UTC instant, evaluated
/// against the declared `timezone`'s WALL CLOCK.
pub fn next_cron_occurrence_utc(
    cron: &croner::Cron,
    after: chrono::DateTime<chrono::Utc>,
    timezone: Option<&str>,
) -> Option<chrono::DateTime<chrono::Utc>> {
    use chrono::TimeZone;

    let Some(tz) = timezone.and_then(|t| t.parse::<chrono_tz::Tz>().ok()) else {
        return cron.find_next_occurrence(&after, false).ok();
    };

    let resolve_ambiguous = |earliest: chrono::DateTime<chrono_tz::Tz>,
                             latest: chrono::DateTime<chrono_tz::Tz>| {
        let e = earliest.with_timezone(&chrono::Utc);
        if e > after {
            e
        } else {
            latest.with_timezone(&chrono::Utc)
        }
    };

    let naive_after = after.with_timezone(&tz).naive_local();
    let fake_after = chrono::Utc.from_utc_datetime(&naive_after);
    let fake_next = cron.find_next_occurrence(&fake_after, false).ok()?;
    let wall_next = fake_next.naive_utc();

    resolve_wall_instant(&tz, wall_next, resolve_ambiguous)
        .or_else(|| Some(fake_next.max(after + chrono::Duration::minutes(1))))
}

/// Resolve a wall-clock datetime in `tz` to a real UTC instant, walking
/// forward minute-by-minute through a DST gap (up to 4h) when the wall time
/// does not exist. `on_ambiguous` picks the instant when the wall time
/// repeats (fall-back).
fn resolve_wall_instant(
    tz: &chrono_tz::Tz,
    wall: chrono::NaiveDateTime,
    mut on_ambiguous: impl FnMut(
        chrono::DateTime<chrono_tz::Tz>,
        chrono::DateTime<chrono_tz::Tz>,
    ) -> chrono::DateTime<chrono::Utc>,
) -> Option<chrono::DateTime<chrono::Utc>> {
    use chrono::TimeZone;
    let mut probe = wall;
    for _ in 0..=240 {
        match tz.from_local_datetime(&probe) {
            chrono::LocalResult::Single(dt) => return Some(dt.with_timezone(&chrono::Utc)),
            chrono::LocalResult::Ambiguous(earliest, latest) => {
                return Some(on_ambiguous(earliest, latest));
            }
            chrono::LocalResult::None => {}
        }
        probe += chrono::Duration::minutes(1);
    }
    None
}

/// The first real UTC instant of a wall-clock datetime in `tz`: the earlier
/// instant when the wall time repeats (fall-back), the first instant after
/// the gap when it does not exist (spring-forward).
fn first_real_instant(
    tz: &chrono_tz::Tz,
    wall: chrono::NaiveDateTime,
) -> Option<chrono::DateTime<chrono::Utc>> {
    resolve_wall_instant(tz, wall, |earliest, _| earliest.with_timezone(&chrono::Utc))
}

/// True when a cron occurrence falls within `(prev, now]`, compared as real
/// UTC instants. A wall time that repeats during a DST fall-back counts once,
/// at its first real instant — never twice.
fn cron_tick_between(
    cron_schedule: &str,
    prev_nanos: i64,
    now_nanos: i64,
    timezone: Option<&str>,
) -> bool {
    use chrono::TimeZone;
    use std::cell::RefCell;

    thread_local! {
        static CRON_CACHE: RefCell<HashMap<String, croner::Cron>> = RefCell::new(HashMap::new());
        static TZ_CACHE: RefCell<HashMap<String, Option<chrono_tz::Tz>>> =
            RefCell::new(HashMap::new());
    }

    let prev_secs = prev_nanos / 1_000_000_000;
    let now_secs = now_nanos / 1_000_000_000;

    let tz: Option<chrono_tz::Tz> = timezone.and_then(|t| {
        TZ_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            if !cache.contains_key(t) {
                cache.insert(t.to_string(), t.parse().ok());
            }
            cache[t]
        })
    });

    CRON_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if !cache.contains_key(cron_schedule) {
            cache.insert(
                cron_schedule.to_string(),
                build_cron(cron_schedule).expect("cron schedule validated at construction"),
            );
        }
        let cron = &cache[cron_schedule];
        let (Some(prev), Some(now)) = (
            chrono::DateTime::from_timestamp(prev_secs, 0),
            chrono::DateTime::from_timestamp(now_secs, 0),
        ) else {
            return false;
        };

        let Some(tz) = tz else {
            return cron
                .find_next_occurrence(&prev, false)
                .map(|next| next <= now)
                .unwrap_or(false);
        };

        // Walk wall-clock occurrences from prev's wall projection, mapping
        // each to its first real instant; skip occurrences whose instant is
        // already in the past (the repeated fall-back hour projects the wall
        // clock behind real time).
        let mut fake_cursor =
            chrono::Utc.from_utc_datetime(&prev.with_timezone(&tz).naive_local());
        for _ in 0..2000 {
            let Ok(fake_next) = cron.find_next_occurrence(&fake_cursor, false) else {
                return false;
            };
            match first_real_instant(&tz, fake_next.naive_utc()) {
                Some(real) if real <= prev => fake_cursor = fake_next,
                Some(real) => return real <= now,
                None => fake_cursor = fake_next,
            }
        }
        false
    })
}

/// Check if `run_tags` satisfies the `tag_keys` (key-presence) and `tag_values` (exact k-v match).
fn run_tags_match(
    run_tags: &[(String, String)],
    tag_keys: &[String],
    tag_values: &[(String, String)],
) -> bool {
    tag_keys
        .iter()
        .all(|k| run_tags.iter().any(|(rk, _)| rk == k))
        && tag_values.iter().all(|t| run_tags.contains(t))
}

/// Empty state used for dep evaluations (avoids constructing HashMaps per call).
static EMPTY_CONDITION_STATE: std::sync::LazyLock<AssetConditionState> =
    std::sync::LazyLock::new(AssetConditionState::default);

/// Evaluate `condition` as if `dep_key` were the target asset.
fn eval_on_dep(
    dep_key: &str,
    condition: &ConditionNode,
    ctx: &EvalContext,
    counter: &mut u32,
    dep_results: &mut DepScope<bool>,
) -> bool {
    let dep_record = match ctx.cache.records.get(dep_key) {
        Some(r) => r,
        None => return false,
    };
    let dep_state = ctx
        .all_asset_states
        .get(dep_key)
        .unwrap_or(&EMPTY_CONDITION_STATE);
    let dep_ctx = EvalContext {
        target_key: dep_key,
        root_key: ctx.root_key,
        target_record: dep_record,
        cache: ctx.cache,
        tags: ctx.tags,
        prev_state: dep_state,
        all_asset_states: ctx.all_asset_states,
        requested_this_tick: ctx.requested_this_tick,
        now: ctx.now,
        is_initial: ctx.is_initial,
        partitions: None,
        root_partition_floor: None,
    };
    let mut local = HashMap::new();
    let saved = dep_results.cur_prev;
    let latch = dep_results.prev.get(dep_key).unwrap_or(&EMPTY_BOOL_LATCH);
    dep_results.cur_prev = Some(latch);
    let val = eval_inner(condition, &dep_ctx, counter, &mut local, dep_results);
    dep_results.cur_prev = saved;
    collect_dep_latch(dep_results, dep_key, local);
    val
}

/// Recursive partition-aware evaluator. Returns an `O` indicating which
/// partitions satisfy the condition.
///
/// UNIFICATION: parallel twin of [`eval_inner`] — see the note there; mirror
/// every semantic change in BOTH matches.
fn eval_partitioned<O: PartEvalOutput>(
    node: &ConditionNode,
    ctx: &EvalContext,
    pctx: &PartitionEvalContext,
    counter: &mut u32,
    sub_selections: &mut HashMap<u32, PartitionSelection>,
    dep_selections: &mut DepScope<PartitionSelection>,
) -> O {
    let my_idx = *counter;
    *counter += 1;

    let total = pctx.all_keys.len();

    match node {
        ConditionNode::Missing => {
            let missing: HashSet<PartitionKey> = pctx
                .all_keys
                .difference(pctx.materialized)
                .cloned()
                .collect();
            let sel = if missing.is_empty() {
                PartitionSelection::Empty
            } else {
                PartitionSelection::Keys(missing)
            };
            O::leaf(sel, my_idx, node, total)
        }

        ConditionNode::InProgress => O::leaf(
            select_in_universe(pctx.in_progress, pctx),
            my_idx,
            node,
            total,
        ),

        ConditionNode::ExecutionFailed => {
            O::leaf(select_in_universe(pctx.failed, pctx), my_idx, node, total)
        }

        ConditionNode::CodeVersionChanged => {
            let changed = ctx.target_record.code_version.is_some()
                && ctx.target_record.code_version
                    != ctx.target_record.last_materialization_code_version;
            let sel = if changed {
                PartitionSelection::All
            } else {
                PartitionSelection::Empty
            };
            O::leaf(sel, my_idx, node, total)
        }

        ConditionNode::NewlyUpdated => {
            let prev_timestamps = ctx
                .prev_state
                .partition_state
                .as_ref()
                .map(|ps| &ps.timestamps);
            let updated: HashSet<PartitionKey> = pctx
                .timestamps
                .iter()
                .filter(|&(pk, &ts)| {
                    pctx.all_keys.contains(pk)
                        && match pctx.dep_root_floor {
                            Some(floor) => match floor.get(pk) {
                                None => false,
                                Some(&inner) => dep_newer_than_floor(ts, inner),
                            },
                            None => match prev_timestamps.and_then(|pt| pt.get(pk)) {
                                Some(&prev) => ts > prev,
                                None => !ctx.is_initial,
                            },
                        }
                })
                .map(|(pk, _)| pk.clone())
                .collect();
            let sel = if updated.is_empty() {
                PartitionSelection::Empty
            } else {
                PartitionSelection::Keys(updated)
            };
            O::leaf(sel, my_idx, node, total)
        }

        ConditionNode::NewlyRequested => {
            let requested_last_tick = ctx.prev_state.last_handled_timestamp.is_some()
                && ctx.prev_state.last_handled_timestamp == ctx.prev_state.last_tick_timestamp;
            let sel = if requested_last_tick {
                match ctx.prev_state.partition_state.as_ref() {
                    Some(ps) => {
                        let keys: HashSet<PartitionKey> = ps
                            .handled
                            .iter()
                            .filter(|k| pctx.all_keys.contains(*k))
                            .cloned()
                            .collect();
                        if keys.is_empty() {
                            PartitionSelection::Empty
                        } else {
                            PartitionSelection::Keys(keys)
                        }
                    }
                    None => PartitionSelection::Empty,
                }
            } else {
                PartitionSelection::Empty
            };
            O::leaf(sel, my_idx, node, total)
        }

        ConditionNode::CronTickPassed {
            cron_schedule,
            timezone,
        } => {
            let prev_ts = root_last_tick(ctx).unwrap_or(ctx.now);
            let val = cron_tick_between(cron_schedule, prev_ts, ctx.now, timezone.as_deref());
            let sel = if val {
                PartitionSelection::All
            } else {
                PartitionSelection::Empty
            };
            O::leaf(sel, my_idx, node, total)
        }

        ConditionNode::InLatestTimeWindow { lookback_delta } => {
            let sel = match pctx
                .time_windows
                .and_then(|tw| tw.keys_for(ctx.target_key, pctx.all_keys, *lookback_delta))
            {
                Some(keys) if !keys.is_empty() => PartitionSelection::Keys((*keys).clone()),
                _ => PartitionSelection::Empty,
            };
            O::leaf(sel, my_idx, node, total)
        }

        ConditionNode::InitialEvaluation => {
            let sel = if ctx.is_initial {
                PartitionSelection::All
            } else {
                PartitionSelection::Empty
            };
            O::leaf(sel, my_idx, node, total)
        }

        ConditionNode::DataVersionChanged => {
            let (prev_dv, prev_ts) = data_version_baseline(ctx);
            let changed = match (ctx.target_record.last_data_version.as_ref(), prev_dv) {
                (Some(current), Some(prev)) => current != prev,
                (Some(_), None) => {
                    !ctx.is_initial && prev_ts != ctx.target_record.last_timestamp
                }
                _ => false,
            };
            let sel = if changed {
                PartitionSelection::All
            } else {
                PartitionSelection::Empty
            };
            O::leaf(sel, my_idx, node, total)
        }

        ConditionNode::BackfillInProgress => O::leaf(
            eval_backfill_in_progress_partitioned(ctx, pctx),
            my_idx,
            node,
            total,
        ),

        ConditionNode::LastExecutedWithTags {
            tag_keys,
            tag_values,
        } => O::leaf(
            partition_filter_select(
                ctx.tags.partition_last_run_tags.get(ctx.target_key),
                pctx,
                |tags| run_tags_match(tags, tag_keys, tag_values),
            ),
            my_idx,
            node,
            total,
        ),

        ConditionNode::LastRunIncludesTarget => {
            if ctx.target_key == ctx.root_key {
                O::leaf(PartitionSelection::Empty, my_idx, node, total)
            } else {
                O::leaf(
                    partition_filter_select(
                        ctx.tags.partition_last_run_asset_names.get(ctx.target_key),
                        pctx,
                        |names| names.iter().any(|n| n == ctx.root_key),
                    ),
                    my_idx,
                    node,
                    total,
                )
            }
        }

        ConditionNode::WillBeRequested => {
            let sel = match ctx.requested_this_tick.get(ctx.target_key) {
                None | Some(PartitionSelection::Empty) => PartitionSelection::Empty,
                Some(PartitionSelection::All) => PartitionSelection::All,
                Some(s @ PartitionSelection::Keys(_)) => s.clone(),
            };
            O::leaf(sel, my_idx, node, total)
        }

        ConditionNode::HasRunWithTags {
            tag_keys,
            tag_values,
        } => O::leaf(
            eval_new_update_tags_partitioned(ctx, pctx, tag_keys, tag_values, false),
            my_idx,
            node,
            total,
        ),

        ConditionNode::AllRunsHaveTags {
            tag_keys,
            tag_values,
        } => O::leaf(
            eval_new_update_tags_partitioned(ctx, pctx, tag_keys, tag_values, true),
            my_idx,
            node,
            total,
        ),

        ConditionNode::AnyDepsMatch { condition, .. } => {
            let sel = eval_partitioned_any_deps(ctx, pctx, condition, counter, dep_selections);
            O::leaf(sel, my_idx, node, total)
        }

        ConditionNode::AllDepsMatch { condition, .. } => {
            let sel = eval_partitioned_all_deps(ctx, pctx, condition, counter, dep_selections);
            O::leaf(sel, my_idx, node, total)
        }

        ConditionNode::AssetMatches { keys, condition } => {
            let base = *counter;
            let mut sel = PartitionSelection::Empty;
            for key in keys {
                *counter = base;
                let key_sel =
                    eval_partitioned_on_dep(key, condition, ctx, pctx, counter, dep_selections);
                sel = sel.union(&key_sel);
            }
            finalize_dep_counter(counter, base, condition);
            O::leaf(sel, my_idx, node, total)
        }

        ConditionNode::And(children) => {
            let mut result = PartitionSelection::All;
            let mut child_parts = Vec::with_capacity(children.len());
            for child in children {
                if result.is_empty() && !child.has_stateful_nodes() {
                    child_parts.push(O::skipped_child(child, counter));
                } else {
                    let child_out = eval_partitioned::<O>(
                        child,
                        ctx,
                        pctx,
                        counter,
                        sub_selections,
                        dep_selections,
                    );
                    let (child_sel, child_part) = O::into_parts(child_out);
                    if !result.is_empty() {
                        result = result.intersect(&child_sel);
                    }
                    child_parts.push(child_part);
                }
            }
            O::composite(result, my_idx, node, total, child_parts)
        }

        ConditionNode::Or(children) => {
            let mut result = PartitionSelection::Empty;
            let mut child_parts = Vec::with_capacity(children.len());
            for child in children {
                if result.is_all() && !child.has_stateful_nodes() {
                    child_parts.push(O::skipped_child(child, counter));
                } else {
                    let child_out = eval_partitioned::<O>(
                        child,
                        ctx,
                        pctx,
                        counter,
                        sub_selections,
                        dep_selections,
                    );
                    let (child_sel, child_part) = O::into_parts(child_out);
                    if !result.is_all() {
                        result = result.union(&child_sel);
                    }
                    child_parts.push(child_part);
                }
            }
            O::composite(result, my_idx, node, total, child_parts)
        }

        ConditionNode::Not(child) => {
            let child_out =
                eval_partitioned::<O>(child, ctx, pctx, counter, sub_selections, dep_selections);
            let (child_sel, child_part) = O::into_parts(child_out);
            let result = child_sel.complement(pctx.all_keys);
            O::composite(result, my_idx, node, total, vec![child_part])
        }

        ConditionNode::NewlyTrue(child) => {
            let child_out =
                eval_partitioned::<O>(child, ctx, pctx, counter, sub_selections, dep_selections);
            let (current, child_part) = O::into_parts(child_out);
            let previous = prev_partition_latch(dep_selections, ctx, my_idx);
            let result = current.difference(&previous, pctx.all_keys);
            sub_selections.insert(my_idx, current);
            O::composite(result, my_idx, node, total, vec![child_part])
        }

        ConditionNode::Since { trigger, reset } => {
            let trigger_out =
                eval_partitioned::<O>(trigger, ctx, pctx, counter, sub_selections, dep_selections);
            let reset_out =
                eval_partitioned::<O>(reset, ctx, pctx, counter, sub_selections, dep_selections);
            let (trigger_sel, trigger_part) = O::into_parts(trigger_out);
            let (reset_sel, reset_part) = O::into_parts(reset_out);
            let prev_latch = prev_partition_latch(dep_selections, ctx, my_idx);
            let result = prev_latch
                .union(&trigger_sel)
                .difference(&reset_sel, pctx.all_keys);
            sub_selections.insert(my_idx, result.clone());
            O::composite(result, my_idx, node, total, vec![trigger_part, reset_part])
        }

        ConditionNode::SinceLastHandled(child) => {
            let child_out =
                eval_partitioned::<O>(child, ctx, pctx, counter, sub_selections, dep_selections);
            let (current, child_part) = O::into_parts(child_out);
            let result = if current.is_empty() {
                PartitionSelection::Empty
            } else {
                let (last_handled, last_tick) = root_handled_state(ctx);
                let was_just_handled = last_handled
                    .map(|h| last_tick.map(|lt| h >= lt).unwrap_or(false))
                    .unwrap_or(false);
                if !was_just_handled {
                    current
                } else if ctx.target_key != ctx.root_key {
                    PartitionSelection::Empty
                } else {
                    match ctx
                        .prev_state
                        .partition_state
                        .as_ref()
                        .map(|ps| &ps.handled)
                    {
                        None => current,
                        Some(handled_set) => {
                            let handled_sel = if handled_set.is_empty() {
                                PartitionSelection::Empty
                            } else {
                                PartitionSelection::Keys(handled_set.clone())
                            };
                            current.difference(&handled_sel, pctx.all_keys)
                        }
                    }
                }
            };
            O::composite(result, my_idx, node, total, vec![child_part])
        }
    }
}

/// Partition-aware AnyDeps: evaluate condition on each upstream dep,
/// map result back to downstream partition space, union all.
fn eval_partitioned_any_deps(
    ctx: &EvalContext,
    pctx: &PartitionEvalContext,
    condition: &ConditionNode,
    counter: &mut u32,
    dep_selections: &mut DepScope<PartitionSelection>,
) -> PartitionSelection {
    let base = *counter;
    let mut result = PartitionSelection::Empty;
    if let Some(deps) = ctx.cache.upstream_deps.get(ctx.target_key) {
        for dep in deps {
            *counter = base;
            let dep_sel =
                eval_partitioned_on_dep(dep, condition, ctx, pctx, counter, dep_selections);
            result = result.union(&dep_sel);
        }
    }
    finalize_dep_counter(counter, base, condition);
    result
}

/// Partition-aware AllDeps: evaluate condition on each upstream dep,
/// map result back to downstream partition space, intersect all.
fn eval_partitioned_all_deps(
    ctx: &EvalContext,
    pctx: &PartitionEvalContext,
    condition: &ConditionNode,
    counter: &mut u32,
    dep_selections: &mut DepScope<PartitionSelection>,
) -> PartitionSelection {
    let base = *counter;
    let eval_all = condition.has_stateful_nodes();
    let result = match ctx.cache.upstream_deps.get(ctx.target_key) {
        Some(deps) if !deps.is_empty() => {
            let mut result = PartitionSelection::All;
            for dep in deps {
                *counter = base;
                let dep_sel =
                    eval_partitioned_on_dep(dep, condition, ctx, pctx, counter, dep_selections);
                result = result.intersect(&dep_sel);
                if result.is_empty() && !eval_all {
                    break;
                }
            }
            result
        }
        _ => PartitionSelection::All,
    };
    finalize_dep_counter(counter, base, condition);
    result
}

/// True if the tree contains `NewlyUpdated` — the only consumer of
/// `dep_root_floor`, whose construction walks every upstream timestamp.
fn contains_newly_updated(node: &ConditionNode) -> bool {
    matches!(node, ConditionNode::NewlyUpdated)
        || node.children().any(contains_newly_updated)
}

/// Whether a dep at `dep_ts` counts as newly-updated against a downstream
/// key's effective staleness `floor`: fire when the key was never attempted
/// (`None`) or the dep is strictly newer than the floor.
fn dep_newer_than_floor(dep_ts: i64, floor: Option<i64>) -> bool {
    floor.is_none_or(|f| dep_ts > f)
}

/// A key's effective attempt timestamp: the later of its last
/// materialization and last (still-current) failure; `None` if never attempted.
fn effective_attempt_ts(
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
fn root_floor_over<'k>(
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
fn root_floor_over_attempted<'k>(
    keys: impl Iterator<Item = &'k PartitionKey>,
    root_status: &crate::condition::cache::PartitionStatusEntry,
) -> Option<i64> {
    keys.filter_map(|k| effective_attempt_ts(root_status, k)).min()
}

/// Evaluate a condition on an upstream dep in partition-aware mode.
/// Maps partitions: downstream → upstream, evaluate, upstream → downstream.
fn eval_partitioned_on_dep(
    dep_key: &str,
    condition: &ConditionNode,
    ctx: &EvalContext,
    pctx: &PartitionEvalContext,
    counter: &mut u32,
    dep_selections: &mut DepScope<PartitionSelection>,
) -> PartitionSelection {
    let dep_record = match ctx.cache.records.get(dep_key) {
        Some(r) => r,
        None => return PartitionSelection::Empty,
    };

    let prev_dep_sel: Option<&HashMap<u32, PartitionSelection>> = dep_selections.prev.get(dep_key);

    let upstream_entry = pctx.resolver.upstream_partition_keys.get(dep_key);

    if upstream_entry.is_none() {
        // In a nested pivot `pctx.all_keys` is the intermediate dep's key
        // space, not the root's — use the floor precomputed at the outer
        // pivot over the true root universe.
        let root_floor = match ctx.root_partition_floor {
            Some(f) => f,
            // Fan-out over the whole root universe: ignore never-attempted
            // frontier keys like an AllPartitions edge, or a freshly-minted
            // key drags the floor to None and refires everything.
            None => pctx
                .all_partition_statuses
                .get(ctx.root_key)
                .and_then(|status| root_floor_over_attempted(pctx.all_keys.iter(), status)),
        };
        let dep_state = ctx
            .all_asset_states
            .get(dep_key)
            .unwrap_or(&EMPTY_CONDITION_STATE);
        let dep_ctx = EvalContext {
            target_key: dep_key,
            root_key: ctx.root_key,
            target_record: dep_record,
            cache: ctx.cache,
            tags: ctx.tags,
            prev_state: dep_state,
            all_asset_states: ctx.all_asset_states,
            requested_this_tick: ctx.requested_this_tick,
            now: ctx.now,
            is_initial: ctx.is_initial,
            partitions: None,
            root_partition_floor: Some(root_floor),
        };
        let bool_latch: HashMap<u32, bool> = prev_dep_sel
            .map(|m| m.iter().map(|(idx, sel)| (*idx, !sel.is_empty())).collect())
            .unwrap_or_default();
        let mut local = HashMap::new();
        let nested_prev: HashMap<String, HashMap<u32, bool>> = dep_selections
            .prev
            .iter()
            .map(|(k, m)| {
                (
                    k.clone(),
                    m.iter().map(|(idx, sel)| (*idx, !sel.is_empty())).collect(),
                )
            })
            .collect();
        let mut nested_acc = HashMap::new();
        let mut bool_scope = DepScope {
            prev: &nested_prev,
            acc: &mut nested_acc,
            cur_prev: Some(&bool_latch),
            bridged: HashMap::new(),
        };
        let val = eval_inner(condition, &dep_ctx, counter, &mut local, &mut bool_scope);
        collect_bridged_latch(
            dep_selections,
            dep_key,
            local
                .into_iter()
                .map(|(idx, b)| (idx, PartitionSelection::from_bool(b)))
                .collect(),
        );
        for (nested_key, idx_map) in nested_acc {
            collect_bridged_latch(
                dep_selections,
                &nested_key,
                idx_map
                    .into_iter()
                    .map(|(idx, b)| (idx, PartitionSelection::from_bool(b)))
                    .collect(),
            );
        }
        return PartitionSelection::from_bool(val);
    }

    // Borrow — cloning a 1M-key universe per dep evaluation is pure waste.
    let upstream_all_keys = upstream_entry.expect("bridged branch returned above");

    let empty_status = crate::condition::cache::PartitionStatusEntry::default();
    let upstream_status = pctx
        .all_partition_statuses
        .get(dep_key)
        .unwrap_or(&empty_status);

    let mapping_kind = pctx.resolver.mapping_kind(dep_key, ctx.target_key);
    let is_identity = matches!(
        mapping_kind,
        None | Some(crate::condition::partition::PartitionMappingKind::Identity)
    );
    let needs_floor = contains_newly_updated(condition);
    let dep_root_floor = pctx
        .all_partition_statuses
        .get(ctx.root_key)
        .filter(|_| needs_floor)
        .map(|root_status| {
            upstream_status
                .timestamps
                .keys()
                .filter_map(|uk| {
                    if is_identity {
                        if !pctx.all_keys.contains(uk) {
                            return None;
                        }
                        return Some((
                            uk.clone(),
                            root_floor_over(std::iter::once(uk), root_status),
                        ));
                    }
                    let mapped = pctx.resolver.map_downstream(
                        dep_key,
                        ctx.target_key,
                        &PartitionSelection::Keys(HashSet::from([uk.clone()])),
                        Some(pctx.all_keys),
                    );
                    let floor = match &mapped {
                        PartitionSelection::Empty => return None,
                        PartitionSelection::All => {
                            root_floor_over_attempted(pctx.all_keys.iter(), root_status)
                        }
                        PartitionSelection::Keys(ks) => {
                            let in_universe: Vec<&PartitionKey> =
                                ks.iter().filter(|k| pctx.all_keys.contains(*k)).collect();
                            if in_universe.is_empty() {
                                return None;
                            }
                            root_floor_over(in_universe.into_iter(), root_status)
                        }
                    };
                    Some((uk.clone(), floor))
                })
                .collect::<HashMap<PartitionKey, Option<i64>>>()
        });

    let upstream_pctx = PartitionEvalContext {
        all_keys: &upstream_all_keys,
        materialized: &upstream_status.materialized,
        in_progress: &upstream_status.in_progress,
        failed: &upstream_status.failed,
        timestamps: &upstream_status.timestamps,
        resolver: PartitionResolver::empty(),
        time_windows: pctx.time_windows,
        all_partition_statuses: pctx.all_partition_statuses,
        dep_root_floor: dep_root_floor.as_ref(),
    };

    let dep_state = ctx
        .all_asset_states
        .get(dep_key)
        .unwrap_or(&EMPTY_CONDITION_STATE);
    // Nested dep-aggregates lose sight of the root's universe; carry the
    // bridge floor computed over it so their unpartitioned-dep pivots don't
    // recompute it over this dep's key space.
    let nested_bridge_floor = condition.has_dep_aggregate().then(|| {
        ctx.root_partition_floor.unwrap_or_else(|| {
            pctx.all_partition_statuses
                .get(ctx.root_key)
                .and_then(|status| root_floor_over_attempted(pctx.all_keys.iter(), status))
        })
    });
    let dep_ctx = EvalContext {
        target_key: dep_key,
        root_key: ctx.root_key,
        target_record: dep_record,
        cache: ctx.cache,
        tags: ctx.tags,
        prev_state: dep_state,
        all_asset_states: ctx.all_asset_states,
        requested_this_tick: ctx.requested_this_tick,
        now: ctx.now,
        is_initial: ctx.is_initial,
        partitions: Some(&upstream_pctx),
        root_partition_floor: nested_bridge_floor,
    };

    let mut local = HashMap::new();
    let saved = dep_selections.cur_prev;
    dep_selections.cur_prev = Some(prev_dep_sel.unwrap_or(&EMPTY_SELECTION_LATCH));
    let upstream_result: PartitionSelection = eval_partitioned(
        condition,
        &dep_ctx,
        &upstream_pctx,
        counter,
        &mut local,
        dep_selections,
    );
    dep_selections.cur_prev = saved;
    collect_dep_latch(dep_selections, dep_key, local);

    pctx.resolver
        .map_downstream(dep_key, ctx.target_key, &upstream_result, Some(pctx.all_keys))
}

#[cfg(test)]
mod latch_merge_tests {
    use super::*;

    fn scope<'a>(
        prev: &'a HashMap<String, HashMap<u32, PartitionSelection>>,
        acc: &'a mut HashMap<String, HashMap<u32, PartitionSelection>>,
    ) -> DepScope<'a, PartitionSelection> {
        DepScope {
            prev,
            acc,
            cur_prev: None,
            bridged: HashMap::new(),
        }
    }

    fn keys(s: &str) -> PartitionSelection {
        PartitionSelection::Keys(std::collections::HashSet::from([
            crate::storage::PartitionKey::Single {
                keys: vec![s.to_string()],
            },
        ]))
    }

    #[test]
    fn bridged_write_never_clobbers_precise_latch() {
        let prev = HashMap::new();
        let mut acc = HashMap::new();
        let mut sc = scope(&prev, &mut acc);

        collect_dep_latch(&mut sc, "d", HashMap::from([(2u32, keys("d1"))]));
        collect_bridged_latch(
            &mut sc,
            "d",
            HashMap::from([(2u32, PartitionSelection::All)]),
        );
        assert_eq!(
            sc.acc["d"][&2],
            keys("d1"),
            "a bridged All must not widen a precise Keys latch"
        );

        collect_bridged_latch(
            &mut sc,
            "d",
            HashMap::from([(3u32, PartitionSelection::All)]),
        );
        collect_dep_latch(&mut sc, "d", HashMap::from([(3u32, keys("d2"))]));
        assert_eq!(sc.acc["d"][&3], keys("d2"));
        collect_bridged_latch(
            &mut sc,
            "d",
            HashMap::from([(3u32, PartitionSelection::Empty)]),
        );
        assert_eq!(sc.acc["d"][&3], keys("d2"));
    }

    #[test]
    fn bridged_writes_union_instead_of_clobbering() {
        let prev = HashMap::new();
        let mut acc = HashMap::new();
        let mut sc = scope(&prev, &mut acc);

        collect_bridged_latch(
            &mut sc,
            "d",
            HashMap::from([(1u32, PartitionSelection::All)]),
        );
        collect_bridged_latch(
            &mut sc,
            "d",
            HashMap::from([(1u32, PartitionSelection::Empty)]),
        );
        assert_eq!(
            sc.acc["d"][&1],
            PartitionSelection::All,
            "a sibling's false latch must not erase a latched true"
        );
    }
}
