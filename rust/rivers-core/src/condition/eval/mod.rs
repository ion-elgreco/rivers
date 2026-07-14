//! Core condition evaluator.

use std::collections::{HashMap, HashSet};

use crate::storage::{AssetRecord, PartitionKey};

use super::node::ConditionNode;
use super::partition::{PartitionEvalContext, PartitionResolver, PartitionSelection};
use super::state::{AssetConditionState, EvalContext, EvalNodeResult, EvalResult};

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
mod domain;
mod support;

pub use cron::*;
use domain::{BoolDomain, DomainVal, EvalDomain, EvalOut, PartitionDomain};
pub(crate) use support::*;

#[cfg(test)]
mod latch_merge_tests;

/// Empty state used for dep evaluations (avoids constructing HashMaps per call).
static EMPTY_CONDITION_STATE: std::sync::LazyLock<AssetConditionState> =
    std::sync::LazyLock::new(AssetConditionState::default);

pub fn evaluate(node: &ConditionNode, ctx: &EvalContext) -> EvalResult {
    if ctx.partitions.is_some() {
        let mut counter = 0u32;
        let mut sub_selections = HashMap::new();
        let mut dep_selections = HashMap::new();
        let mut dep_scope = DepScope {
            prev: root_dep_selections(ctx),
            acc: &mut dep_selections,
            cur_prev: None,
            bridged: HashMap::new(),
        };
        let selection: PartitionSelection = eval::<PartitionDomain, PartitionSelection>(
            node,
            ctx,
            &mut counter,
            &mut sub_selections,
            &mut dep_scope,
        );
        let fired = PartitionDomain::fired(&selection, ctx);
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
        let fired: bool =
            eval::<BoolDomain, bool>(node, ctx, &mut counter, &mut sub_results, &mut dep_scope);
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

/// The single evaluator, generic over the domain `D` and output mode `O`. Node
/// indices must advance identically across domains — persisted latches key off them.
fn eval<D: EvalDomain, O: EvalOut<D::Sel>>(
    node: &ConditionNode,
    ctx: &EvalContext,
    counter: &mut u32,
    sub: &mut HashMap<u32, D::Sel>,
    dep_scope: &mut DepScope<D::Sel>,
) -> O {
    let my_idx = *counter;
    *counter += 1;
    let total = ctx.partitions.map(|p| p.all_keys.len()).unwrap_or(0);

    match node {
        // leaves
        ConditionNode::Missing => O::leaf(D::missing(ctx), my_idx, node, total),
        ConditionNode::InProgress => O::leaf(D::in_progress(ctx), my_idx, node, total),
        ConditionNode::ExecutionFailed => O::leaf(D::failed(ctx), my_idx, node, total),
        ConditionNode::NewlyUpdated => O::leaf(D::newly_updated(ctx), my_idx, node, total),
        ConditionNode::NewlyRequested => O::leaf(D::newly_requested(ctx), my_idx, node, total),
        ConditionNode::InLatestTimeWindow { lookback_delta } => O::leaf(
            D::in_latest_window(ctx, *lookback_delta),
            my_idx,
            node,
            total,
        ),
        ConditionNode::BackfillInProgress => {
            O::leaf(D::backfill_in_progress(ctx), my_idx, node, total)
        }
        ConditionNode::LastExecutedWithTags {
            tag_keys,
            tag_values,
        } => O::leaf(
            D::last_executed_with_tags(ctx, tag_keys, tag_values),
            my_idx,
            node,
            total,
        ),
        ConditionNode::LastRunIncludesTarget => {
            O::leaf(D::last_run_includes_target(ctx), my_idx, node, total)
        }
        ConditionNode::WillBeRequested => O::leaf(D::will_be_requested(ctx), my_idx, node, total),
        ConditionNode::HasRunWithTags {
            tag_keys,
            tag_values,
        } => O::leaf(
            D::update_tags(ctx, tag_keys, tag_values, false),
            my_idx,
            node,
            total,
        ),
        ConditionNode::AllRunsHaveTags {
            tag_keys,
            tag_values,
        } => O::leaf(
            D::update_tags(ctx, tag_keys, tag_values, true),
            my_idx,
            node,
            total,
        ),

        // scalar predicates
        ConditionNode::CodeVersionChanged => {
            let changed = ctx.target_record.code_version.is_some()
                && ctx.target_record.code_version
                    != ctx.target_record.last_materialization_code_version;
            O::leaf(D::from_bool(changed), my_idx, node, total)
        }
        ConditionNode::DataVersionChanged => {
            let (prev_dv, prev_ts) = data_version_baseline(ctx);
            let changed = match (ctx.target_record.last_data_version.as_ref(), prev_dv) {
                (Some(current), Some(prev)) => current != prev,
                (Some(_), None) => !ctx.is_initial && prev_ts != ctx.target_record.last_timestamp,
                _ => false,
            };
            O::leaf(D::from_bool(changed), my_idx, node, total)
        }
        ConditionNode::InitialEvaluation => {
            O::leaf(D::from_bool(ctx.is_initial), my_idx, node, total)
        }
        ConditionNode::CronTickPassed {
            cron_schedule,
            timezone,
        } => {
            let prev_ts = root_last_tick(ctx).unwrap_or(ctx.now);
            let val = cron_tick_between(cron_schedule, prev_ts, ctx.now, timezone.as_deref());
            O::leaf(D::from_bool(val), my_idx, node, total)
        }

        // dep-aggregates
        ConditionNode::AnyDepsMatch { condition, .. } => {
            let deps = ctx
                .cache
                .upstream_deps
                .get(ctx.target_key)
                .into_iter()
                .flatten();
            let result = fold_deps::<D>(
                deps,
                condition,
                ctx,
                counter,
                dep_scope,
                D::empty(),
                D::or,
                |s| s.is_all(),
            );
            O::leaf(result, my_idx, node, total)
        }
        ConditionNode::AllDepsMatch { condition, .. } => {
            let deps = ctx
                .cache
                .upstream_deps
                .get(ctx.target_key)
                .into_iter()
                .flatten();
            let result = fold_deps::<D>(
                deps,
                condition,
                ctx,
                counter,
                dep_scope,
                D::all(ctx),
                D::and,
                |s| !s.is_true(),
            );
            O::leaf(result, my_idx, node, total)
        }
        ConditionNode::AssetMatches { keys, condition } => {
            let result = fold_deps::<D>(
                keys.iter(),
                condition,
                ctx,
                counter,
                dep_scope,
                D::empty(),
                D::or,
                |s| s.is_all(),
            );
            O::leaf(result, my_idx, node, total)
        }

        // combinators
        ConditionNode::And(children) => {
            let mut result = D::all(ctx);
            let mut child_parts = if O::COLLECTS_CHILDREN {
                Vec::with_capacity(children.len())
            } else {
                Vec::new()
            };
            for child in children {
                if !result.is_true() && !child.has_stateful_nodes() {
                    let skipped = O::skipped_child(child, counter);
                    if O::COLLECTS_CHILDREN {
                        child_parts.push(skipped);
                    }
                } else {
                    let child_out: O = eval::<D, O>(child, ctx, counter, sub, dep_scope);
                    let (child_sel, child_part) = child_out.into_parts();
                    if result.is_true() {
                        result = D::and(result, &child_sel);
                    }
                    if O::COLLECTS_CHILDREN {
                        child_parts.push(child_part);
                    }
                }
            }
            O::composite(result, my_idx, node, total, child_parts)
        }
        ConditionNode::Or(children) => {
            let mut result = D::empty();
            let mut child_parts = if O::COLLECTS_CHILDREN {
                Vec::with_capacity(children.len())
            } else {
                Vec::new()
            };
            for child in children {
                if result.is_all() && !child.has_stateful_nodes() {
                    let skipped = O::skipped_child(child, counter);
                    if O::COLLECTS_CHILDREN {
                        child_parts.push(skipped);
                    }
                } else {
                    let child_out: O = eval::<D, O>(child, ctx, counter, sub, dep_scope);
                    let (child_sel, child_part) = child_out.into_parts();
                    if !result.is_all() {
                        result = D::or(result, &child_sel);
                    }
                    if O::COLLECTS_CHILDREN {
                        child_parts.push(child_part);
                    }
                }
            }
            O::composite(result, my_idx, node, total, child_parts)
        }
        ConditionNode::Not(child) => {
            let child_out: O = eval::<D, O>(child, ctx, counter, sub, dep_scope);
            let (child_sel, child_part) = child_out.into_parts();
            let result = D::not(child_sel, ctx);
            O::composite(result, my_idx, node, total, vec![child_part])
        }

        // stateful ops
        ConditionNode::NewlyTrue(child) => {
            let child_out: O = eval::<D, O>(child, ctx, counter, sub, dep_scope);
            let (current, child_part) = child_out.into_parts();
            let previous = D::prev_latch(dep_scope, ctx, my_idx);
            let result = D::difference(current.clone(), &previous, ctx);
            sub.insert(my_idx, current);
            O::composite(result, my_idx, node, total, vec![child_part])
        }
        ConditionNode::Since { trigger, reset } => {
            let trigger_out: O = eval::<D, O>(trigger, ctx, counter, sub, dep_scope);
            let reset_out: O = eval::<D, O>(reset, ctx, counter, sub, dep_scope);
            let (trigger_sel, trigger_part) = trigger_out.into_parts();
            let (reset_sel, reset_part) = reset_out.into_parts();
            let prev = D::prev_latch(dep_scope, ctx, my_idx);
            // Restrict to the current universe so a latch can't carry forward
            // keys retired from the partition set (unpartitioned: a no-op).
            let result = D::restrict(
                D::difference(D::or(prev, &trigger_sel), &reset_sel, ctx),
                ctx,
            );
            sub.insert(my_idx, result.clone());
            O::composite(result, my_idx, node, total, vec![trigger_part, reset_part])
        }
        ConditionNode::SinceLastHandled(child) => {
            let child_out: O = eval::<D, O>(child, ctx, counter, sub, dep_scope);
            let (current, child_part) = child_out.into_parts();
            let result = D::since_last_handled(current, ctx);
            O::composite(result, my_idx, node, total, vec![child_part])
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

/// Fold a dep-aggregate: pivot `condition` into each dep/key and combine the
/// results from `init` with `combine`, short-circuiting once `saturated` — but
/// evaluating every dep when a stateful child needs its per-dep latch recorded.
/// An empty `deps` iterator returns `init` (so an all-quantifier over no deps is
/// vacuously true).
#[allow(clippy::too_many_arguments)]
fn fold_deps<'a, D: EvalDomain>(
    deps: impl IntoIterator<Item = &'a String>,
    condition: &ConditionNode,
    ctx: &EvalContext,
    counter: &mut u32,
    dep_scope: &mut DepScope<D::Sel>,
    init: D::Sel,
    combine: fn(D::Sel, &D::Sel) -> D::Sel,
    saturated: impl Fn(&D::Sel) -> bool,
) -> D::Sel {
    let base = *counter;
    let eval_all = condition.has_stateful_nodes();
    let mut result = init;
    for dep in deps {
        *counter = base;
        let dep_val = D::pivot_into_dep(dep, condition, ctx, counter, dep_scope);
        result = combine(result, &dep_val);
        if saturated(&result) && !eval_all {
            break;
        }
    }
    finalize_dep_counter(counter, base, condition);
    result
}

/// Evaluate a `ConditionNode` tree and return both the compact result (for
/// state tracking) and a full evaluation tree (for UI visualization).
pub fn evaluate_with_tree(node: &ConditionNode, ctx: &EvalContext) -> (EvalResult, EvalNodeResult) {
    if ctx.partitions.is_some() {
        let mut counter = 0u32;
        let mut sub_selections = HashMap::new();
        let mut dep_selections = HashMap::new();
        let mut dep_scope = DepScope {
            prev: root_dep_selections(ctx),
            acc: &mut dep_selections,
            cur_prev: None,
            bridged: HashMap::new(),
        };
        let (selection, tree): (PartitionSelection, EvalNodeResult) =
            eval::<PartitionDomain, (PartitionSelection, EvalNodeResult)>(
                node,
                ctx,
                &mut counter,
                &mut sub_selections,
                &mut dep_scope,
            );
        let fired = PartitionDomain::fired(&selection, ctx);
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
        let (fired, tree): (bool, EvalNodeResult) = eval::<BoolDomain, (bool, EvalNodeResult)>(
            node,
            ctx,
            &mut counter,
            &mut sub_results,
            &mut dep_scope,
        );
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

/// Build an `EvalContext` for evaluating a condition as if `dep_key` were the
/// target, inheriting the root-invariant fields from `ctx`. Only `partitions`
/// and `root_partition_floor` vary per pivot kind.
fn dep_eval_context<'a>(
    ctx: &EvalContext<'a>,
    dep_key: &'a str,
    dep_record: &'a AssetRecord,
    partitions: Option<&'a PartitionEvalContext<'a>>,
    root_partition_floor: Option<Option<i64>>,
) -> EvalContext<'a> {
    EvalContext {
        target_key: dep_key,
        root_key: ctx.root_key,
        target_record: dep_record,
        cache: ctx.cache,
        tags: ctx.tags,
        prev_state: ctx
            .all_asset_states
            .get(dep_key)
            .unwrap_or(&EMPTY_CONDITION_STATE),
        all_asset_states: ctx.all_asset_states,
        requested_this_tick: ctx.requested_this_tick,
        now: ctx.now,
        is_initial: ctx.is_initial,
        partitions,
        root_partition_floor,
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
    // Inherit the root-universe staleness floor unchanged: once a partitioned
    // pivot has bridged into the bool world there is no partition universe to
    // recompute from, so a `NewlyUpdated` leaf behind further unpartitioned
    // dep-aggregates keeps comparing against the root's floor (seeded at the
    // bridge). `None` in → `None` out for a genuinely unpartitioned root.
    let dep_ctx = dep_eval_context(ctx, dep_key, dep_record, None, ctx.root_partition_floor);
    let mut local = HashMap::new();
    let saved = dep_results.cur_prev;
    let latch = dep_results.prev.get(dep_key).unwrap_or(&EMPTY_BOOL_LATCH);
    dep_results.cur_prev = Some(latch);
    let val = eval::<BoolDomain, bool>(condition, &dep_ctx, counter, &mut local, dep_results);
    dep_results.cur_prev = saved;
    collect_dep_latch(dep_results, dep_key, local);
    val
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
        let dep_ctx = dep_eval_context(ctx, dep_key, dep_record, None, Some(root_floor));
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
        let val =
            eval::<BoolDomain, bool>(condition, &dep_ctx, counter, &mut local, &mut bool_scope);
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
            // Key-independent across upstream keys: compute the fan-out floor once.
            let all_floor: std::cell::OnceCell<Option<i64>> = std::cell::OnceCell::new();
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
                        PartitionSelection::All => *all_floor.get_or_init(|| {
                            root_floor_over_attempted(pctx.all_keys.iter(), root_status)
                        }),
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
        all_keys: upstream_all_keys,
        in_progress: &upstream_status.in_progress,
        failed: &upstream_status.failed,
        timestamps: &upstream_status.timestamps,
        resolver: PartitionResolver::empty(),
        time_windows: pctx.time_windows,
        all_partition_statuses: pctx.all_partition_statuses,
        dep_root_floor: dep_root_floor.as_ref(),
    };

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
    let dep_ctx = dep_eval_context(
        ctx,
        dep_key,
        dep_record,
        Some(&upstream_pctx),
        nested_bridge_floor,
    );

    let mut local = HashMap::new();
    let saved = dep_selections.cur_prev;
    dep_selections.cur_prev = Some(prev_dep_sel.unwrap_or(&EMPTY_SELECTION_LATCH));
    let upstream_result: PartitionSelection = eval::<PartitionDomain, PartitionSelection>(
        condition,
        &dep_ctx,
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
