//! One condition evaluator over the bool and `PartitionSelection` domains.

use std::collections::{HashMap, HashSet};

use crate::storage::PartitionKey;

use super::super::node::ConditionNode;
use super::super::partition::{PartitionEvalContext, PartitionSelection};
use super::super::state::{EvalContext, EvalResult};
use super::support::*;
use super::{DepScope, eval_on_dep, eval_partitioned_on_dep};

fn require_pctx<'a>(ctx: &EvalContext<'a>) -> &'a PartitionEvalContext<'a> {
    ctx.partitions
        .expect("PartitionDomain evaluated without a PartitionEvalContext")
}

pub(crate) trait DomainVal: Clone {
    fn is_true(&self) -> bool;
    fn is_all(&self) -> bool;
    fn num_partitions(&self, total: usize) -> Option<usize>;
}

impl DomainVal for bool {
    fn is_true(&self) -> bool {
        *self
    }
    fn is_all(&self) -> bool {
        *self
    }
    fn num_partitions(&self, _total: usize) -> Option<usize> {
        None
    }
}

impl DomainVal for PartitionSelection {
    fn is_true(&self) -> bool {
        !self.is_empty()
    }
    fn is_all(&self) -> bool {
        self.is_all()
    }
    fn num_partitions(&self, total: usize) -> Option<usize> {
        Some(self.key_count(total))
    }
}

pub(crate) trait EvalDomain {
    type Sel: DomainVal;

    // algebra
    fn all(ctx: &EvalContext) -> Self::Sel;
    fn empty() -> Self::Sel;
    fn from_bool(b: bool) -> Self::Sel;
    fn and(a: Self::Sel, b: &Self::Sel) -> Self::Sel;
    fn or(a: Self::Sel, b: &Self::Sel) -> Self::Sel;
    fn not(a: Self::Sel, ctx: &EvalContext) -> Self::Sel;
    fn difference(a: Self::Sel, b: &Self::Sel, ctx: &EvalContext) -> Self::Sel;
    fn restrict(a: Self::Sel, ctx: &EvalContext) -> Self::Sel;
    fn fired(sel: &Self::Sel, ctx: &EvalContext) -> bool;

    // leaf status sources
    fn missing(ctx: &EvalContext) -> Self::Sel;
    fn in_progress(ctx: &EvalContext) -> Self::Sel;
    fn failed(ctx: &EvalContext) -> Self::Sel;
    fn newly_updated(ctx: &EvalContext) -> Self::Sel;
    fn newly_requested(ctx: &EvalContext) -> Self::Sel;
    fn in_latest_window(ctx: &EvalContext, lookback: Option<f64>) -> Self::Sel;
    fn backfill_in_progress(ctx: &EvalContext) -> Self::Sel;
    fn last_executed_with_tags(
        ctx: &EvalContext,
        tag_keys: &[String],
        tag_values: &[(String, String)],
    ) -> Self::Sel;
    fn update_tags(
        ctx: &EvalContext,
        tag_keys: &[String],
        tag_values: &[(String, String)],
        require_all: bool,
    ) -> Self::Sel;
    fn last_run_includes_target(ctx: &EvalContext) -> Self::Sel;
    fn will_be_requested(ctx: &EvalContext) -> Self::Sel;

    // stateful
    fn prev_latch(dep_scope: &DepScope<Self::Sel>, ctx: &EvalContext, idx: u32) -> Self::Sel;
    fn since_last_handled(current: Self::Sel, ctx: &EvalContext) -> Self::Sel;

    // Evaluate `condition` as if `dep_key` were the target. The partition impl
    // bridges an unpartitioned dep through the bool evaluator.
    fn pivot_into_dep(
        dep_key: &str,
        condition: &ConditionNode,
        ctx: &EvalContext,
        counter: &mut u32,
        dep_scope: &mut DepScope<Self::Sel>,
    ) -> Self::Sel;

    /// Previous-tick per-dep latches to seed the root `DepScope` with.
    fn root_dep_prev<'a>(ctx: &'a EvalContext) -> &'a HashMap<String, HashMap<u32, Self::Sel>>;

    /// Assemble the tick's `EvalResult` from the root result plus the per-node
    /// (`sub`) and per-dep (`dep`) latches this domain populated.
    fn assemble(
        top: Self::Sel,
        ctx: &EvalContext,
        sub: HashMap<u32, Self::Sel>,
        dep: HashMap<String, HashMap<u32, Self::Sel>>,
    ) -> EvalResult;
}

pub(crate) struct BoolDomain;

impl EvalDomain for BoolDomain {
    type Sel = bool;

    fn all(_ctx: &EvalContext) -> bool {
        true
    }
    fn empty() -> bool {
        false
    }
    fn from_bool(b: bool) -> bool {
        b
    }
    fn and(a: bool, b: &bool) -> bool {
        a && *b
    }
    fn or(a: bool, b: &bool) -> bool {
        a || *b
    }
    fn not(a: bool, _ctx: &EvalContext) -> bool {
        !a
    }
    fn difference(a: bool, b: &bool, _ctx: &EvalContext) -> bool {
        a && !*b
    }
    fn restrict(a: bool, _ctx: &EvalContext) -> bool {
        a
    }
    fn fired(sel: &bool, _ctx: &EvalContext) -> bool {
        *sel
    }

    fn missing(ctx: &EvalContext) -> bool {
        ctx.target_record.last_run_id.is_none()
    }
    fn in_progress(ctx: &EvalContext) -> bool {
        ctx.cache.in_progress_assets.contains(ctx.target_key)
    }
    fn failed(ctx: &EvalContext) -> bool {
        ctx.cache.failed_assets.contains(ctx.target_key)
    }
    fn newly_updated(ctx: &EvalContext) -> bool {
        if ctx.target_key != ctx.root_key {
            return match ctx.target_record.last_timestamp {
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
        }
        match (
            ctx.target_record.last_timestamp,
            ctx.prev_state.last_materialized_timestamp,
        ) {
            (Some(current), Some(prev)) => current > prev,
            (Some(_), None) => !ctx.is_initial,
            _ => false,
        }
    }
    fn newly_requested(ctx: &EvalContext) -> bool {
        requested_last_tick(ctx)
    }
    fn in_latest_window(_ctx: &EvalContext, _lookback: Option<f64>) -> bool {
        true
    }
    fn backfill_in_progress(ctx: &EvalContext) -> bool {
        ctx.cache.backfill.assets.contains_key(ctx.target_key)
    }
    fn last_executed_with_tags(
        ctx: &EvalContext,
        tag_keys: &[String],
        tag_values: &[(String, String)],
    ) -> bool {
        ctx.tags
            .last_run_tags
            .get(ctx.target_key)
            .and_then(|slots| slots.get(&None))
            .map(|run_tags| run_tags_match(run_tags, tag_keys, tag_values))
            .unwrap_or(false)
    }
    fn update_tags(
        ctx: &EvalContext,
        tag_keys: &[String],
        tag_values: &[(String, String)],
        require_all: bool,
    ) -> bool {
        eval_new_update_tags(ctx, tag_keys, tag_values, require_all)
    }
    fn last_run_includes_target(ctx: &EvalContext) -> bool {
        if ctx.target_key == ctx.root_key {
            false
        } else {
            ctx.tags
                .last_run_asset_names
                .get(ctx.target_key)
                .and_then(|slots| slots.get(&None))
                .map(|names| names.iter().any(|n| n == ctx.root_key))
                .unwrap_or(false)
        }
    }
    fn will_be_requested(ctx: &EvalContext) -> bool {
        ctx.requested_this_tick.contains_key(ctx.target_key)
    }

    fn prev_latch(dep_scope: &DepScope<bool>, ctx: &EvalContext, idx: u32) -> bool {
        dep_scope
            .cur_prev
            .unwrap_or(&ctx.prev_state.previous_results)
            .get(&idx)
            .copied()
            .unwrap_or(false)
    }
    fn since_last_handled(current: bool, ctx: &EvalContext) -> bool {
        if !current {
            return false;
        }
        let (last_handled, last_tick) = root_handled_state(ctx);
        match last_handled {
            None => true,
            Some(handled) => last_tick.map(|lt| handled < lt).unwrap_or(true),
        }
    }

    fn pivot_into_dep(
        dep_key: &str,
        condition: &ConditionNode,
        ctx: &EvalContext,
        counter: &mut u32,
        dep_scope: &mut DepScope<bool>,
    ) -> bool {
        eval_on_dep(dep_key, condition, ctx, counter, dep_scope)
    }

    fn root_dep_prev<'a>(ctx: &'a EvalContext) -> &'a HashMap<String, HashMap<u32, bool>> {
        &ctx.prev_state.dep_previous_results
    }

    fn assemble(
        top: bool,
        _ctx: &EvalContext,
        sub: HashMap<u32, bool>,
        dep: HashMap<String, HashMap<u32, bool>>,
    ) -> EvalResult {
        EvalResult {
            fired: top,
            sub_results: sub,
            dep_sub_results: dep,
            ..Default::default()
        }
    }
}

pub(crate) struct PartitionDomain;

impl EvalDomain for PartitionDomain {
    type Sel = PartitionSelection;

    fn all(_ctx: &EvalContext) -> PartitionSelection {
        PartitionSelection::All
    }
    fn empty() -> PartitionSelection {
        PartitionSelection::Empty
    }
    fn from_bool(b: bool) -> PartitionSelection {
        PartitionSelection::from_bool(b)
    }
    fn and(a: PartitionSelection, b: &PartitionSelection) -> PartitionSelection {
        a.intersect(b)
    }
    fn or(a: PartitionSelection, b: &PartitionSelection) -> PartitionSelection {
        a.union(b)
    }
    fn not(a: PartitionSelection, ctx: &EvalContext) -> PartitionSelection {
        a.complement(require_pctx(ctx).all_keys)
    }
    fn difference(
        a: PartitionSelection,
        b: &PartitionSelection,
        ctx: &EvalContext,
    ) -> PartitionSelection {
        a.difference(b, require_pctx(ctx).all_keys)
    }
    fn restrict(a: PartitionSelection, ctx: &EvalContext) -> PartitionSelection {
        a.restrict_to(require_pctx(ctx).all_keys)
    }
    fn fired(sel: &PartitionSelection, ctx: &EvalContext) -> bool {
        match sel {
            PartitionSelection::All => !require_pctx(ctx).all_keys.is_empty(),
            other => !other.is_empty(),
        }
    }

    fn missing(ctx: &EvalContext) -> PartitionSelection {
        let pctx = require_pctx(ctx);
        let missing: HashSet<PartitionKey> = pctx
            .all_keys
            .iter()
            .filter(|k| !pctx.timestamps.contains_key(*k))
            .cloned()
            .collect();
        PartitionSelection::from_keys(missing)
    }
    fn in_progress(ctx: &EvalContext) -> PartitionSelection {
        let pctx = require_pctx(ctx);
        select_in_universe(pctx.in_progress, pctx)
    }
    fn failed(ctx: &EvalContext) -> PartitionSelection {
        let pctx = require_pctx(ctx);
        select_in_universe(pctx.failed, pctx)
    }
    fn newly_updated(ctx: &EvalContext) -> PartitionSelection {
        let pctx = require_pctx(ctx);
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
        PartitionSelection::from_keys(updated)
    }
    fn newly_requested(ctx: &EvalContext) -> PartitionSelection {
        let pctx = require_pctx(ctx);
        if !requested_last_tick(ctx) {
            return PartitionSelection::Empty;
        }
        match ctx.prev_state.partition_state.as_ref() {
            Some(ps) => select_in_universe(&ps.handled, pctx),
            None => PartitionSelection::Empty,
        }
    }
    fn in_latest_window(ctx: &EvalContext, lookback: Option<f64>) -> PartitionSelection {
        let pctx = require_pctx(ctx);
        match pctx
            .time_windows
            .and_then(|tw| tw.keys_for(ctx.target_key, pctx.all_keys, lookback))
        {
            Some(keys) if !keys.is_empty() => PartitionSelection::Keys((*keys).clone()),
            _ => PartitionSelection::Empty,
        }
    }
    fn backfill_in_progress(ctx: &EvalContext) -> PartitionSelection {
        eval_backfill_in_progress_partitioned(ctx, require_pctx(ctx))
    }
    fn last_executed_with_tags(
        ctx: &EvalContext,
        tag_keys: &[String],
        tag_values: &[(String, String)],
    ) -> PartitionSelection {
        partition_filter_select(
            ctx.tags.last_run_tags.get(ctx.target_key),
            require_pctx(ctx),
            |tags| run_tags_match(tags, tag_keys, tag_values),
        )
    }
    fn update_tags(
        ctx: &EvalContext,
        tag_keys: &[String],
        tag_values: &[(String, String)],
        require_all: bool,
    ) -> PartitionSelection {
        eval_new_update_tags_partitioned(ctx, require_pctx(ctx), tag_keys, tag_values, require_all)
    }
    fn last_run_includes_target(ctx: &EvalContext) -> PartitionSelection {
        if ctx.target_key == ctx.root_key {
            return PartitionSelection::Empty;
        }
        partition_filter_select(
            ctx.tags.last_run_asset_names.get(ctx.target_key),
            require_pctx(ctx),
            |names| names.iter().any(|n| n == ctx.root_key),
        )
    }
    fn will_be_requested(ctx: &EvalContext) -> PartitionSelection {
        ctx.requested_this_tick
            .get(ctx.target_key)
            .cloned()
            .unwrap_or(PartitionSelection::Empty)
    }

    fn prev_latch(
        dep_scope: &DepScope<PartitionSelection>,
        ctx: &EvalContext,
        idx: u32,
    ) -> PartitionSelection {
        prev_partition_latch(dep_scope, ctx, idx)
    }
    fn since_last_handled(current: PartitionSelection, ctx: &EvalContext) -> PartitionSelection {
        if current.is_empty() {
            return PartitionSelection::Empty;
        }
        let pctx = require_pctx(ctx);
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
                    let handled_sel = PartitionSelection::from_keys(handled_set.clone());
                    current.difference(&handled_sel, pctx.all_keys)
                }
            }
        }
    }

    fn pivot_into_dep(
        dep_key: &str,
        condition: &ConditionNode,
        ctx: &EvalContext,
        counter: &mut u32,
        dep_scope: &mut DepScope<PartitionSelection>,
    ) -> PartitionSelection {
        eval_partitioned_on_dep(
            dep_key,
            condition,
            ctx,
            require_pctx(ctx),
            counter,
            dep_scope,
        )
    }

    fn root_dep_prev<'a>(
        ctx: &'a EvalContext,
    ) -> &'a HashMap<String, HashMap<u32, PartitionSelection>> {
        super::root_dep_selections(ctx)
    }

    fn assemble(
        top: PartitionSelection,
        ctx: &EvalContext,
        sub: HashMap<u32, PartitionSelection>,
        dep: HashMap<String, HashMap<u32, PartitionSelection>>,
    ) -> EvalResult {
        let fired = Self::fired(&top, ctx);
        EvalResult {
            fired,
            selection: Some(top),
            sub_selections: Some(sub),
            dep_sub_selections: Some(dep),
            ..Default::default()
        }
    }
}
