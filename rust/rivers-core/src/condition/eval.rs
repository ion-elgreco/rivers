//! Core condition evaluator.

use std::collections::{HashMap, HashSet};

use crate::storage::PartitionKey;

use super::node::ConditionNode;
use super::partition::{PartitionEvalContext, PartitionResolver, PartitionSelection};
use super::state::{AssetConditionState, EvalContext, EvalNodeResult, EvalResult, NodeStatus};

// These traits abstract over "bool only" vs "bool + tree" output modes,
// allowing a single generic evaluator to replace the duplicated _with_tree variants.

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
/// `Child` is the type collected into children vectors — `()` when no tree is
/// needed, `EvalNodeResult` when building a visualization tree.
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
/// Mirrors `count_nodes()` to maintain stable counter indices.
fn build_skipped_subtree(node: &ConditionNode, counter: &mut u32) -> EvalNodeResult {
    let my_idx = *counter;
    *counter += 1;

    let children = match node {
        ConditionNode::And(children) | ConditionNode::Or(children) => children
            .iter()
            .map(|c| build_skipped_subtree(c, counter))
            .collect(),
        ConditionNode::Not(child)
        | ConditionNode::NewlyTrue(child)
        | ConditionNode::SinceLastHandled(child)
        | ConditionNode::AnyDepsMatch {
            condition: child, ..
        }
        | ConditionNode::AllDepsMatch {
            condition: child, ..
        }
        | ConditionNode::AssetMatches {
            condition: child, ..
        } => {
            vec![build_skipped_subtree(child, counter)]
        }
        ConditionNode::Since { trigger, reset } => {
            vec![
                build_skipped_subtree(trigger, counter),
                build_skipped_subtree(reset, counter),
            ]
        }
        _ => vec![],
    };

    EvalNodeResult::new(node, my_idx, NodeStatus::Skipped, children, None)
}

/// Evaluate a `ConditionNode` tree against the given context.
///
/// Returns an `EvalResult` whose `fired` field indicates whether the
/// condition is true for this tick. `sub_results` uses monotonic u32 indices
/// as keys (no string allocation in the hot path).
///
/// When `ctx.partitions` is `Some`, evaluates in partition-aware mode and
/// populates `selection` and `sub_selections` on the result.
pub fn evaluate(node: &ConditionNode, ctx: &EvalContext) -> EvalResult {
    if let Some(pctx) = ctx.partitions {
        let mut counter = 0u32;
        let mut sub_selections = HashMap::new();
        let selection: PartitionSelection =
            eval_partitioned(node, ctx, pctx, &mut counter, &mut sub_selections);
        let fired = !selection.is_empty();
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
        }
    } else {
        let mut counter = 0u32;
        let mut sub_results = HashMap::new();
        let fired: bool = eval_inner(node, ctx, &mut counter, &mut sub_results);
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
        }
    }
}

/// Recursive inner evaluator. Uses a monotonic counter for node indexing
/// instead of string path formatting (zero allocation in the hot path).
/// Only `NewlyTrue` and `Since` nodes record/read from `sub_results`.
///
/// Generic over `O`: when `O = bool` we get the fast path (no tree allocation);
/// when `O = EvalNodeResult` we get a full visualization tree.
fn eval_inner<O: EvalOutput>(
    node: &ConditionNode,
    ctx: &EvalContext,
    counter: &mut u32,
    sub_results: &mut HashMap<u32, bool>,
) -> O {
    let my_idx = *counter;
    *counter += 1;

    match node {
        ConditionNode::Missing => {
            O::leaf(ctx.target_record.last_data_version.is_none(), my_idx, node)
        }
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
            // In a dep pivot, fire-time baselines are unsound (async event
            // drains double-fire; global re-baselining loses fires) — same
            // contract as the partitioned arm: a dep counts as updated while
            // strictly newer than the root's last successful OR failed
            // attempt, so the triggered run self-suppresses and a failed one
            // consumes the update instead of retrying every tick.
            if ctx.target_key != ctx.root_key {
                let expr = match ctx.target_record.last_timestamp {
                    None => false,
                    Some(dep_ts) => {
                        let root_mat = ctx
                            .cache
                            .records
                            .get(ctx.root_key)
                            .and_then(|r| r.last_timestamp);
                        let root_failed = ctx
                            .cache
                            .failed_asset_timestamps
                            .get(ctx.root_key)
                            .copied();
                        match (root_mat, root_failed) {
                            (None, None) => true,
                            (Some(m), None) => dep_ts > m,
                            (None, Some(f)) => dep_ts > f,
                            (Some(m), Some(f)) => dep_ts > m.max(f),
                        }
                    }
                };
                return O::leaf(expr, my_idx, node);
            }
            let expr = match (
                ctx.target_record.last_timestamp,
                ctx.prev_state.last_materialized_timestamp,
            ) {
                (Some(current), Some(prev)) => current > prev,
                // No previous state: a missing prev_state means the asset
                // appeared between ticks → treat as updated.
                (Some(_), None) => true,
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
            timezone: _,
        } => {
            // Use the previous evaluation tick as the left boundary.
            // Default to ctx.now on first eval (zero-width window → no match),
            // so cron conditions don't spuriously fire on daemon startup.
            let prev_ts = ctx.prev_state.last_tick_timestamp.unwrap_or(ctx.now);
            O::leaf(
                cron_tick_between(cron_schedule, prev_ts, ctx.now),
                my_idx,
                node,
            )
        }

        ConditionNode::InLatestTimeWindow { .. } => O::leaf(true, my_idx, node),

        ConditionNode::InitialEvaluation => O::leaf(ctx.is_initial, my_idx, node),

        ConditionNode::DataVersionChanged => {
            let expr = match (
                ctx.target_record.last_data_version.as_ref(),
                ctx.prev_state.last_data_version.as_ref(),
            ) {
                (Some(current), Some(prev)) => current != prev,
                (Some(_), None) => true,
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
                false // self-referential guard: only meaningful on deps, not the root itself
            } else {
                ctx.tags
                    .last_run_asset_names
                    .get(ctx.target_key)
                    .map(|names| names.iter().any(|n| n == ctx.root_key))
                    .unwrap_or(false)
            };
            O::leaf(expr, my_idx, node)
        }

        ConditionNode::WillBeRequested => {
            // Only meaningful inside dep pivots (eval_on_dep); at the root level
            // it's always false because the root hasn't produced a result yet.
            O::leaf(
                ctx.requested_this_tick.contains_key(ctx.target_key),
                my_idx,
                node,
            )
        }

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
            let val = ctx
                .cache
                .upstream_deps
                .get(ctx.target_key)
                .map(|deps| {
                    deps.iter()
                        .any(|dep| eval_on_dep(dep, condition, ctx, counter, sub_results))
                })
                .unwrap_or(false);
            O::leaf(val, my_idx, node)
        }

        ConditionNode::AllDepsMatch { condition, .. } => {
            let val = ctx
                .cache
                .upstream_deps
                .get(ctx.target_key)
                .map(|deps| {
                    deps.iter()
                        .all(|dep| eval_on_dep(dep, condition, ctx, counter, sub_results))
                })
                .unwrap_or(true);
            O::leaf(val, my_idx, node)
        }

        ConditionNode::AssetMatches { keys, condition } => {
            let val = keys
                .iter()
                .any(|key| eval_on_dep(key, condition, ctx, counter, sub_results));
            O::leaf(val, my_idx, node)
        }

        // We still short-circuit, but always increment the counter for
        // skipped children via `count_nodes()`. This ensures stable my_idx
        // assignments across ticks even when short-circuiting changes, so
        // NewlyTrue/Since nodes always read the correct previous result.
        ConditionNode::And(children) => {
            let mut result = true;
            let mut child_outs = if O::COLLECTS_CHILDREN {
                Vec::with_capacity(children.len())
            } else {
                Vec::new()
            };
            for child in children {
                if result {
                    let out = eval_inner::<O>(child, ctx, counter, sub_results);
                    result = out.val();
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
                    let out = eval_inner::<O>(child, ctx, counter, sub_results);
                    result = out.val();
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
            let child_out = eval_inner::<O>(child, ctx, counter, sub_results);
            let val = !child_out.val();
            if O::COLLECTS_CHILDREN {
                O::composite(val, my_idx, node, vec![child_out])
            } else {
                O::leaf(val, my_idx, node)
            }
        }

        // State-tracking operators — the only ones that read/write sub_results.
        ConditionNode::NewlyTrue(child) => {
            let child_out = eval_inner::<O>(child, ctx, counter, sub_results);
            let current = child_out.val();
            let previous = ctx
                .prev_state
                .previous_results
                .get(&my_idx)
                .copied()
                .unwrap_or(false);
            // Pure rising-edge detector: true only when child transitions false→true.
            // First-tick behavior (firing when there's no previous state) should be
            // handled via InitialEvaluation composition in presets, not special-cased here.
            let result = current && !previous;
            sub_results.insert(my_idx, current);
            if O::COLLECTS_CHILDREN {
                O::composite(result, my_idx, node, vec![child_out])
            } else {
                O::leaf(result, my_idx, node)
            }
        }

        ConditionNode::Since { trigger, reset } => {
            let trigger_out = eval_inner::<O>(trigger, ctx, counter, sub_results);
            let reset_out = eval_inner::<O>(reset, ctx, counter, sub_results);
            let trigger_val = trigger_out.val();
            let reset_val = reset_out.val();
            let prev_latch = ctx
                .prev_state
                .previous_results
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
            let child_out = eval_inner::<O>(child, ctx, counter, sub_results);
            let current = child_out.val();
            let result = if !current {
                false
            } else {
                match ctx.prev_state.last_handled_timestamp {
                    None => true,
                    Some(handled) => ctx
                        .prev_state
                        .last_tick_timestamp
                        .map(|last_tick| handled < last_tick)
                        .unwrap_or(true),
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
/// Used by `And`/`Or` to maintain stable node indices when short-circuiting.
fn count_nodes(node: &ConditionNode, counter: &mut u32) {
    *counter += 1;
    match node {
        ConditionNode::And(children) | ConditionNode::Or(children) => {
            for child in children {
                count_nodes(child, counter);
            }
        }
        ConditionNode::Not(child)
        | ConditionNode::NewlyTrue(child)
        | ConditionNode::SinceLastHandled(child)
        | ConditionNode::AnyDepsMatch {
            condition: child, ..
        }
        | ConditionNode::AllDepsMatch {
            condition: child, ..
        }
        | ConditionNode::AssetMatches {
            condition: child, ..
        } => {
            count_nodes(child, counter);
        }
        ConditionNode::Since { trigger, reset } => {
            count_nodes(trigger, counter);
            count_nodes(reset, counter);
        }
        _ => {}
    }
}

/// Evaluate a `ConditionNode` tree and return both the compact result (for
/// state tracking) and a full evaluation tree (for UI visualization).
///
/// When `ctx.partitions` is `Some`, evaluates in partition-aware mode in a
/// single pass, producing both the `PartitionSelection` result and the
/// `EvalNodeResult` tree with `num_partitions` populated at each node.
pub fn evaluate_with_tree(node: &ConditionNode, ctx: &EvalContext) -> (EvalResult, EvalNodeResult) {
    if let Some(pctx) = ctx.partitions {
        let mut counter = 0u32;
        let mut sub_selections = HashMap::new();
        let (selection, tree): (PartitionSelection, EvalNodeResult) =
            eval_partitioned(node, ctx, pctx, &mut counter, &mut sub_selections);
        let fired = !selection.is_empty();
        (
            EvalResult {
                fired,
                sub_results: HashMap::new(),
                selection: Some(selection),
                sub_selections: Some(sub_selections),
            },
            tree,
        )
    } else {
        let mut counter = 0u32;
        let mut sub_results = HashMap::new();
        let tree = eval_inner::<EvalNodeResult>(node, ctx, &mut counter, &mut sub_results);
        let fired = tree.status == NodeStatus::True;
        (
            EvalResult {
                fired,
                sub_results,
                selection: None,
                sub_selections: None,
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
                // Empty or missing: backfill targets the whole asset.
                return PartitionSelection::Keys(pctx.all_keys.clone());
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

/// Check if a cron tick occurred between `prev_nanos` and `now_nanos`.
/// Caches parsed cron expressions in a thread-local to avoid re-parsing
/// the same schedule string on every eval.
fn cron_tick_between(cron_schedule: &str, prev_nanos: i64, now_nanos: i64) -> bool {
    use std::cell::RefCell;

    thread_local! {
        static CRON_CACHE: RefCell<HashMap<String, croner::Cron>> = RefCell::new(HashMap::new());
    }

    let prev_secs = prev_nanos / 1_000_000_000;
    let now_secs = now_nanos / 1_000_000_000;

    CRON_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let cron = cache.entry(cron_schedule.to_string()).or_insert_with(|| {
            croner::parser::CronParser::builder()
                .seconds(croner::parser::Seconds::Optional)
                .build()
                .parse(cron_schedule)
                .expect("invalid cron schedule")
        });
        let prev_dt = chrono::DateTime::from_timestamp(prev_secs, 0);
        let now_dt = chrono::DateTime::from_timestamp(now_secs, 0);
        match (prev_dt, now_dt) {
            (Some(prev), Some(now)) => cron
                .find_next_occurrence(&prev, false)
                .map(|next| next <= now)
                .unwrap_or(false),
            _ => false,
        }
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

/// Evaluate a condition as if `dep_key` were the target asset.
/// Creates a temporary EvalContext pointing to the dep's record.
fn eval_on_dep(
    dep_key: &str,
    condition: &ConditionNode,
    ctx: &EvalContext,
    counter: &mut u32,
    sub_results: &mut HashMap<u32, bool>,
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
    };
    eval_inner(condition, &dep_ctx, counter, sub_results)
}

/// Recursive partition-aware evaluator. Returns an `O` indicating which
/// partitions satisfy the condition. Uses the same monotonic counter system
/// as `eval_inner` for node indexing stability.
///
/// Generic over `O`: when `O = PartitionSelection` we get the fast path;
/// when `O = (PartitionSelection, EvalNodeResult)` we get a full tree.
fn eval_partitioned<O: PartEvalOutput>(
    node: &ConditionNode,
    ctx: &EvalContext,
    pctx: &PartitionEvalContext,
    counter: &mut u32,
    sub_selections: &mut HashMap<u32, PartitionSelection>,
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

        ConditionNode::InProgress => {
            let sel = if pctx.in_progress.is_empty() {
                PartitionSelection::Empty
            } else {
                PartitionSelection::Keys(pctx.in_progress.clone())
            };
            O::leaf(sel, my_idx, node, total)
        }

        ConditionNode::ExecutionFailed => {
            let sel = if pctx.failed.is_empty() {
                PartitionSelection::Empty
            } else {
                PartitionSelection::Keys(pctx.failed.clone())
            };
            O::leaf(sel, my_idx, node, total)
        }

        ConditionNode::CodeVersionChanged => {
            let changed = ctx.target_record.code_version.is_some()
                && ctx.target_record.code_version
                    != ctx.target_record.last_materialization_code_version;
            let sel = if changed {
                PartitionSelection::Keys(pctx.all_keys.clone())
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
            // In a dep pivot (target ≠ root) observation baselines are
            // unsound both ways: async event drains re-surface already-
            // acted-on events (double fire), and a fire can baseline a key
            // that a sibling clause suppressed that tick (lost fire,
            // forever). Compare staleness instead: a dep key counts as
            // updated while it is strictly newer than the root's
            // materialization of the downstream key(s) the partition mapping
            // resolves it to — the dispatched run advances those keys past
            // the dep so the trigger self-suppresses, and a missed tick
            // simply retries. Mapped keys never materialized pass; dep keys
            // with no counterpart in the downstream universe never count
            // (nothing could ever be dispatched for them). The floor map is
            // built per dep by `eval_partitioned_on_dep`; outside dep pivots
            // (and for roots without partition status) baselines remain.
            let updated: HashSet<PartitionKey> = pctx
                .timestamps
                .iter()
                .filter(|&(pk, &ts)| match pctx.dep_root_floor {
                    Some(floor) => match floor.get(pk) {
                        None => false,
                        Some(None) => true,
                        Some(Some(root_ts)) => ts > *root_ts,
                    },
                    None => match prev_timestamps.and_then(|pt| pt.get(pk)) {
                        Some(&prev) => ts > prev,
                        None => true, // newly appeared partition
                    },
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
            let val = ctx.prev_state.last_handled_timestamp.is_some()
                && ctx.prev_state.last_handled_timestamp == ctx.prev_state.last_tick_timestamp;
            O::leaf(PartitionSelection::from_bool(val), my_idx, node, total)
        }

        ConditionNode::CronTickPassed {
            cron_schedule,
            timezone: _,
        } => {
            let prev_ts = ctx.prev_state.last_tick_timestamp.unwrap_or(ctx.now);
            let val = cron_tick_between(cron_schedule, prev_ts, ctx.now);
            let sel = if val {
                PartitionSelection::Keys(pctx.all_keys.clone())
            } else {
                PartitionSelection::Empty
            };
            O::leaf(sel, my_idx, node, total)
        }

        ConditionNode::InLatestTimeWindow { .. } => {
            // For time-windowed partitions, select only partitions whose time window
            // is recent (precomputed by the daemon). For non-time partitions (static,
            // dynamic), select all partitions
            let sel = match &pctx.latest_time_window_keys {
                Some(keys) => {
                    if keys.is_empty() {
                        PartitionSelection::Empty
                    } else {
                        PartitionSelection::Keys(keys.iter().cloned().collect())
                    }
                }
                None => PartitionSelection::Keys(pctx.all_keys.clone()),
            };
            O::leaf(sel, my_idx, node, total)
        }

        ConditionNode::InitialEvaluation => {
            let sel = if ctx.is_initial {
                PartitionSelection::Keys(pctx.all_keys.clone())
            } else {
                PartitionSelection::Empty
            };
            O::leaf(sel, my_idx, node, total)
        }

        ConditionNode::DataVersionChanged => {
            let changed = match (
                ctx.target_record.last_data_version.as_ref(),
                ctx.prev_state.last_data_version.as_ref(),
            ) {
                (Some(current), Some(prev)) => current != prev,
                (Some(_), None) => true,
                _ => false,
            };
            let sel = if changed {
                PartitionSelection::Keys(pctx.all_keys.clone())
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
            // The target's fired selection from earlier this tick, in its own
            // key space — a dep pivot maps it downstream like any other
            // selection. Unpartitioned fires arrive as `All` and widen to the
            // full universe.
            let sel = match ctx.requested_this_tick.get(ctx.target_key) {
                None | Some(PartitionSelection::Empty) => PartitionSelection::Empty,
                Some(PartitionSelection::All) => PartitionSelection::Keys(pctx.all_keys.clone()),
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
            let sel = eval_partitioned_any_deps(ctx, pctx, condition, counter, sub_selections);
            O::leaf(sel, my_idx, node, total)
        }

        ConditionNode::AllDepsMatch { condition, .. } => {
            let sel = eval_partitioned_all_deps(ctx, pctx, condition, counter, sub_selections);
            O::leaf(sel, my_idx, node, total)
        }

        ConditionNode::AssetMatches { keys, condition } => {
            let mut sel = PartitionSelection::Empty;
            for key in keys {
                let key_sel =
                    eval_partitioned_on_dep(key, condition, ctx, pctx, counter, sub_selections);
                sel = sel.union(&key_sel);
            }
            O::leaf(sel, my_idx, node, total)
        }

        ConditionNode::And(children) => {
            let mut result = PartitionSelection::Keys(pctx.all_keys.clone());
            let mut child_parts = Vec::with_capacity(children.len());
            for child in children {
                if result.is_empty() {
                    child_parts.push(O::skipped_child(child, counter));
                } else {
                    let child_out =
                        eval_partitioned::<O>(child, ctx, pctx, counter, sub_selections);
                    let (child_sel, child_part) = O::into_parts(child_out);
                    result = result.intersect(&child_sel);
                    child_parts.push(child_part);
                }
            }
            O::composite(result, my_idx, node, total, child_parts)
        }

        ConditionNode::Or(children) => {
            let mut result = PartitionSelection::Empty;
            let mut child_parts = Vec::with_capacity(children.len());
            for child in children {
                if result.is_all() {
                    child_parts.push(O::skipped_child(child, counter));
                } else {
                    let child_out =
                        eval_partitioned::<O>(child, ctx, pctx, counter, sub_selections);
                    let (child_sel, child_part) = O::into_parts(child_out);
                    result = result.union(&child_sel);
                    child_parts.push(child_part);
                }
            }
            O::composite(result, my_idx, node, total, child_parts)
        }

        ConditionNode::Not(child) => {
            let child_out = eval_partitioned::<O>(child, ctx, pctx, counter, sub_selections);
            let (child_sel, child_part) = O::into_parts(child_out);
            let result = child_sel.complement(pctx.all_keys);
            O::composite(result, my_idx, node, total, vec![child_part])
        }

        ConditionNode::NewlyTrue(child) => {
            let child_out = eval_partitioned::<O>(child, ctx, pctx, counter, sub_selections);
            let (current, child_part) = O::into_parts(child_out);
            let previous = ctx
                .prev_state
                .partition_state
                .as_ref()
                .and_then(|ps| ps.previous_selections.get(&my_idx))
                .cloned()
                .unwrap_or(PartitionSelection::Empty);
            // First-tick behavior handled via InitialEvaluation composition.
            let result = current.difference(&previous, pctx.all_keys);
            sub_selections.insert(my_idx, current);
            O::composite(result, my_idx, node, total, vec![child_part])
        }

        ConditionNode::Since { trigger, reset } => {
            let trigger_out = eval_partitioned::<O>(trigger, ctx, pctx, counter, sub_selections);
            let reset_out = eval_partitioned::<O>(reset, ctx, pctx, counter, sub_selections);
            let (trigger_sel, trigger_part) = O::into_parts(trigger_out);
            let (reset_sel, reset_part) = O::into_parts(reset_out);
            let prev_latch = ctx
                .prev_state
                .partition_state
                .as_ref()
                .and_then(|ps| ps.previous_selections.get(&my_idx))
                .cloned()
                .unwrap_or(PartitionSelection::Empty);
            // (prev_latched ∪ trigger) - reset
            let result = prev_latch
                .union(&trigger_sel)
                .difference(&reset_sel, pctx.all_keys);
            sub_selections.insert(my_idx, result.clone());
            O::composite(result, my_idx, node, total, vec![trigger_part, reset_part])
        }

        ConditionNode::SinceLastHandled(child) => {
            let child_out = eval_partitioned::<O>(child, ctx, pctx, counter, sub_selections);
            let (current, child_part) = O::into_parts(child_out);
            let result = if current.is_empty() {
                PartitionSelection::Empty
            } else {
                let handled = ctx
                    .prev_state
                    .partition_state
                    .as_ref()
                    .map(|ps| &ps.handled);
                match handled {
                    None => current, // never handled
                    Some(handled_set) => {
                        // Remove handled partitions, but only if handling happened
                        // on the previous tick (same debounce logic as unpartitioned).
                        let was_just_handled = ctx
                            .prev_state
                            .last_handled_timestamp
                            .map(|h| {
                                ctx.prev_state
                                    .last_tick_timestamp
                                    .map(|lt| h >= lt)
                                    .unwrap_or(false)
                            })
                            .unwrap_or(false);
                        if was_just_handled {
                            let handled_sel = if handled_set.is_empty() {
                                PartitionSelection::Empty
                            } else {
                                PartitionSelection::Keys(handled_set.clone())
                            };
                            current.difference(&handled_sel, pctx.all_keys)
                        } else {
                            current
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
    sub_selections: &mut HashMap<u32, PartitionSelection>,
) -> PartitionSelection {
    let deps = match ctx.cache.upstream_deps.get(ctx.target_key) {
        Some(deps) => deps,
        None => return PartitionSelection::Empty,
    };

    let mut result = PartitionSelection::Empty;
    for dep in deps {
        let dep_sel = eval_partitioned_on_dep(dep, condition, ctx, pctx, counter, sub_selections);
        result = result.union(&dep_sel);
    }
    result
}

/// Partition-aware AllDeps: evaluate condition on each upstream dep,
/// map result back to downstream partition space, intersect all.
fn eval_partitioned_all_deps(
    ctx: &EvalContext,
    pctx: &PartitionEvalContext,
    condition: &ConditionNode,
    counter: &mut u32,
    sub_selections: &mut HashMap<u32, PartitionSelection>,
) -> PartitionSelection {
    let deps = match ctx.cache.upstream_deps.get(ctx.target_key) {
        Some(deps) => deps,
        None => return PartitionSelection::Keys(pctx.all_keys.clone()), // vacuous truth
    };
    if deps.is_empty() {
        return PartitionSelection::Keys(pctx.all_keys.clone());
    }

    let mut result = PartitionSelection::Keys(pctx.all_keys.clone());
    for dep in deps {
        let dep_sel = eval_partitioned_on_dep(dep, condition, ctx, pctx, counter, sub_selections);
        result = result.intersect(&dep_sel);
        if result.is_empty() {
            break;
        }
    }
    result
}

/// The staleness floor across the downstream keys a dep key maps to: the
/// minimum per-key effective timestamp, where effective = the later of the
/// last materialization and the last (still-current) failure — a failed
/// attempt consumes the dep update that triggered it, or the daemon would
/// re-dispatch a failing partition every tick. `None` as soon as any mapped
/// key was never attempted at all (that keeps the dep "updated").
fn root_floor_over<'k>(
    keys: impl Iterator<Item = &'k PartitionKey>,
    root_status: &crate::condition::cache::PartitionStatusEntry,
) -> Option<i64> {
    let mut floor: Option<i64> = None;
    for k in keys {
        let effective = match (
            root_status.timestamps.get(k),
            root_status.failed_timestamps.get(k),
        ) {
            (None, None) => return None,
            (Some(&m), None) => m,
            (None, Some(&f)) => f,
            (Some(&m), Some(&f)) => m.max(f),
        };
        floor = Some(floor.map_or(effective, |fl| fl.min(effective)));
    }
    floor
}

/// Evaluate a condition on an upstream dep in partition-aware mode.
/// Maps partitions: downstream → upstream, evaluate, upstream → downstream.
fn eval_partitioned_on_dep(
    dep_key: &str,
    condition: &ConditionNode,
    ctx: &EvalContext,
    pctx: &PartitionEvalContext,
    counter: &mut u32,
    sub_selections: &mut HashMap<u32, PartitionSelection>,
) -> PartitionSelection {
    let dep_record = match ctx.cache.records.get(dep_key) {
        Some(r) => r,
        None => return PartitionSelection::Empty,
    };

    let upstream_all_keys: HashSet<PartitionKey> = pctx
        .resolver
        .upstream_partition_keys
        .get(dep_key)
        .cloned()
        .unwrap_or_default();

    if upstream_all_keys.is_empty() {
        // Upstream is unpartitioned — fall back to bool evaluation
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
        };
        let mut sub_results = HashMap::new();
        let val = eval_inner(condition, &dep_ctx, counter, &mut sub_results);
        return PartitionSelection::from_bool(val);
    }

    let empty_status = crate::condition::cache::PartitionStatusEntry::default();
    let upstream_status = pctx
        .all_partition_statuses
        .get(dep_key)
        .unwrap_or(&empty_status);

    // Staleness floor for `NewlyUpdated`, translated into the dep's key
    // space: for each dep key, the root's materialization state of the
    // downstream key(s) it maps to. Built here because the mapping and both
    // universes are only visible at the pivot boundary. Identity edges (the
    // default) skip the per-key selection round-trip — the dep key IS the
    // downstream key.
    let mapping_kind = pctx.resolver.mapping_kind(dep_key, ctx.target_key);
    let is_identity = matches!(
        mapping_kind,
        None | Some(crate::condition::partition::PartitionMappingKind::Identity)
    );
    let dep_root_floor = pctx
        .all_partition_statuses
        .get(ctx.root_key)
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
                    );
                    let floor = match &mapped {
                        PartitionSelection::Empty => return None,
                        PartitionSelection::All => {
                            root_floor_over(pctx.all_keys.iter(), root_status)
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
        latest_time_window_keys: None,
        all_partition_statuses: pctx.all_partition_statuses,
        dep_root_floor: dep_root_floor.as_ref(),
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
        partitions: Some(&upstream_pctx),
    };

    let upstream_result: PartitionSelection =
        eval_partitioned(condition, &dep_ctx, &upstream_pctx, counter, sub_selections);

    // Map result back to downstream partition space.
    pctx.resolver
        .map_downstream(dep_key, ctx.target_key, &upstream_result)
}
