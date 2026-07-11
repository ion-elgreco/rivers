//! Core condition evaluator.

use std::collections::{HashMap, HashSet};

use crate::storage::PartitionKey;

use super::node::ConditionNode;
use super::partition::{PartitionEvalContext, PartitionResolver, PartitionSelection};
use super::state::{AssetConditionState, EvalContext, EvalNodeResult, EvalResult, NodeStatus};

/// Per-dep latch state for stateful ops inside dep-aggregates, keyed by dep then node index.
pub(crate) struct DepScope<'a, V> {
    pub(crate) prev: &'a HashMap<String, HashMap<u32, V>>,
    pub(crate) acc: &'a mut HashMap<String, HashMap<u32, V>>,
    pub(crate) cur_prev: Option<&'a HashMap<u32, V>>,
    pub(crate) bridged: HashMap<String, HashSet<u32>>,
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

mod cron;
mod support;

pub use cron::*;
pub(crate) use support::*;

#[cfg(test)]
mod latch_merge_tests;

/// Empty state used for dep evaluations (avoids constructing HashMaps per call).
static EMPTY_CONDITION_STATE: std::sync::LazyLock<AssetConditionState> =
    std::sync::LazyLock::new(AssetConditionState::default);

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
                (Some(_), None) => !ctx.is_initial && prev_ts != ctx.target_record.last_timestamp,
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
pub(crate) fn count_nodes(node: &ConditionNode, counter: &mut u32) {
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
            // Materialized == has a timestamp; the cache keeps them in lockstep.
            let missing: HashSet<PartitionKey> = pctx
                .all_keys
                .iter()
                .filter(|k| !pctx.timestamps.contains_key(*k))
                .cloned()
                .collect();
            let sel = PartitionSelection::from_keys(missing);
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
            let sel = PartitionSelection::from_keys(updated);
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
                        PartitionSelection::from_keys(keys)
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
                (Some(_), None) => !ctx.is_initial && prev_ts != ctx.target_record.last_timestamp,
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
            // Reused across keys: a fresh single-element set per upstream
            // partition is pure allocator churn at large partition counts.
            let mut scratch = PartitionSelection::Keys(HashSet::with_capacity(1));
            upstream_status
                .timestamps
                .keys()
                .filter_map(move |uk| {
                    if is_identity {
                        if !pctx.all_keys.contains(uk) {
                            return None;
                        }
                        return Some((
                            uk.clone(),
                            root_floor_over(std::iter::once(uk), root_status),
                        ));
                    }
                    if let PartitionSelection::Keys(s) = &mut scratch {
                        s.clear();
                        s.insert(uk.clone());
                    }
                    let mapped = pctx.resolver.map_downstream(
                        dep_key,
                        ctx.target_key,
                        &scratch,
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

    pctx.resolver.map_downstream(
        dep_key,
        ctx.target_key,
        &upstream_result,
        Some(pctx.all_keys),
    )
}
