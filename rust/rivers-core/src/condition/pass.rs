//! Per-tick orchestration for the condition evaluation engine.
//!
//! [`ConditionPass`] owns long-lived state across ticks. One tick is
//! `refresh_cache` → `ensure_time_based_eval_set` → `should_skip` → `run`.
//! `run` returns a [`MaterializationPlan`] classified into the three dispatch
//! shapes; dispatch back to Python is the caller's responsibility.
use std::collections::{HashMap, HashSet};

use anyhow::Result;
use ordermap::OrderSet;

use crate::condition::cache::AssetConditionCache;
use crate::condition::eval::evaluate_with_tree;
use crate::condition::node::ConditionNode;
use crate::condition::partition::{
    PartitionEvalContext, PartitionMappingKind, PartitionResolver, PartitionSelection,
    PartitionState,
};
use crate::condition::state::{
    CacheSnapshot, ConditionEvalState, EvalContext, EvalNodeResult, EvalResult, RunTagSnapshot,
    StateUpdateContext, update_condition_state, update_dep_baselines,
};
use crate::storage::{BackfillStrategy, PartitionKey, StorageBackend};
use crate::timegrid::TimeGrid;
use crate::util::parse_key_datetime;
use chrono::{NaiveDateTime, TimeZone};

/// Info about an asset with an automation condition, extracted at daemon
/// start. Pure-Rust so the per-tick evaluation loop can run inside
/// [`ConditionPass`] without crossing the FFI per asset.
pub struct AssetConditionInfo {
    pub asset_key: String,
    pub condition: ConditionNode,
    /// Partition info for this asset. `None` if unpartitioned.
    pub partition_info: Option<PartitionInfo>,
    /// Backfill strategy from the `@Asset` decorator (controls how
    /// multi-partition condition selections are dispatched).
    pub backfill_strategy: Option<BackfillStrategy>,
}

/// Partition-level info for a partitioned asset, extracted at daemon start.
pub struct PartitionInfo {
    /// All valid partition keys for this asset, advanced per tick by
    /// [`ConditionPass::refresh_partition_universes`].
    pub all_keys: HashSet<PartitionKey>,
    /// Partition mappings for upstream deps. Key = `(this_asset, upstream_asset)`.
    pub mappings: HashMap<(String, String), PartitionMappingKind>,
    /// For time-windowed partitions: the format string used to parse keys.
    /// Used by `InLatestTimeWindow` to select recent partitions per-tick.
    pub time_window_fmt: Option<String>,
    /// How `all_keys` evolves after extraction.
    pub universe: PartitionUniverse,
}

/// How an asset's partition universe evolves after extraction. A universe
/// frozen at daemon start silently stops automation for open-ended time
/// windows and never starts it for dynamic partitions.
#[derive(Clone, Debug)]
pub enum PartitionUniverse {
    /// Fixed key set (Static definitions).
    Frozen,
    /// Window starts enter the universe as wall-clock time passes
    /// (and stop at the grid's explicit end, when there is one).
    TimeWindow {
        grid: TimeGrid,
        enumerated_to: NaiveDateTime,
    },
    /// Storage-managed: the key set mirrors the `dynamic_partitions`
    /// namespace, including retirements.
    Dynamic { namespace: String },
    /// Cartesian product over dimensions; recomputed when any dimension's
    /// key list changes.
    Multi {
        dims: Vec<(String, DimensionUniverse)>,
    },
}

/// One dimension of a Multi universe: its current key list plus how it evolves.
#[derive(Clone, Debug)]
pub struct DimensionUniverse {
    /// Dim values in definition/seed order; a set so refresh re-yields
    /// (explicit future end, seed/watermark races) cannot duplicate.
    pub keys: OrderSet<String>,
    pub kind: DimensionKind,
}

#[derive(Clone, Debug)]
pub enum DimensionKind {
    Frozen,
    TimeWindow {
        grid: TimeGrid,
        enumerated_to: NaiveDateTime,
    },
    Dynamic {
        namespace: String,
    },
}

/// Advance one universe, mutating `all_keys` in place. Returns whether the
/// key set changed. `dynamic_keys` maps namespace → currently registered
/// keys; a namespace missing from the map (storage failure) leaves the
/// previous set untouched — stale beats empty.
fn refresh_universe(
    universe: &mut PartitionUniverse,
    all_keys: &mut HashSet<PartitionKey>,
    now: NaiveDateTime,
    dynamic_keys: &HashMap<String, HashSet<String>>,
) -> bool {
    match universe {
        PartitionUniverse::Frozen => false,
        PartitionUniverse::TimeWindow {
            grid,
            enumerated_to,
        } => {
            let bound = grid.end.map_or(now, |e| e.min(now));
            if bound <= *enumerated_to {
                return false;
            }
            let mut changed = false;
            match grid.window_starts_in(*enumerated_to, bound) {
                Ok(new_keys) => {
                    for k in new_keys {
                        changed |= all_keys.insert(PartitionKey::Single { keys: vec![k] });
                    }
                    *enumerated_to = bound;
                }
                Err(e) => {
                    // Leave the watermark so the range is retried next tick.
                    tracing::warn!(target: "rivers::daemon", error = %e, "time-window universe refresh failed");
                }
            }
            changed
        }
        PartitionUniverse::Dynamic { namespace } => {
            let Some(keys) = dynamic_keys.get(namespace) else {
                return false;
            };
            let new_set: HashSet<PartitionKey> = keys
                .iter()
                .map(|k| PartitionKey::Single {
                    keys: vec![k.clone()],
                })
                .collect();
            if new_set == *all_keys {
                return false;
            }
            *all_keys = new_set;
            true
        }
        PartitionUniverse::Multi { dims } => {
            let mut changed = false;
            for (_, du) in dims.iter_mut() {
                match &mut du.kind {
                    DimensionKind::Frozen => {}
                    DimensionKind::TimeWindow {
                        grid,
                        enumerated_to,
                    } => {
                        let bound = grid.end.map_or(now, |e| e.min(now));
                        if bound <= *enumerated_to {
                            continue;
                        }
                        match grid.window_starts_in(*enumerated_to, bound) {
                            Ok(new_keys) => {
                                // Seeding can enumerate past the watermark
                                // (explicit future end; start/enumerate race),
                                // so re-yielded starts insert as no-ops.
                                for k in new_keys {
                                    changed |= du.keys.insert(k);
                                }
                                *enumerated_to = bound;
                            }
                            Err(e) => {
                                // Leave the watermark so the range is retried.
                                tracing::warn!(target: "rivers::daemon", error = %e, "time-window dimension refresh failed");
                            }
                        }
                    }
                    DimensionKind::Dynamic { namespace } => {
                        if let Some(keys) = dynamic_keys.get(namespace)
                            && (keys.len() != du.keys.len()
                                || !du.keys.iter().all(|k| keys.contains(k)))
                        {
                            let mut sorted: Vec<String> = keys.iter().cloned().collect();
                            sorted.sort();
                            du.keys = sorted.into_iter().collect();
                            changed = true;
                        }
                    }
                }
            }
            if changed {
                *all_keys = cartesian_universe(dims);
            }
            changed
        }
    }
}

/// Cartesian product of dimension key lists as `Multi` keys (def order).
/// Walks an index odometer so each key is built exactly once — no
/// intermediate combo vectors.
fn cartesian_universe(dims: &[(String, DimensionUniverse)]) -> HashSet<PartitionKey> {
    if dims.is_empty() || dims.iter().any(|(_, du)| du.keys.is_empty()) {
        return HashSet::new();
    }
    let mut out = HashSet::new();
    let mut idx = vec![0usize; dims.len()];
    loop {
        out.insert(PartitionKey::Multi {
            dims: dims
                .iter()
                .zip(&idx)
                .map(|((name, du), &i)| (name.clone(), vec![du.keys[i].clone()]))
                .collect(),
        });
        let mut d = dims.len();
        loop {
            if d == 0 {
                return out;
            }
            d -= 1;
            idx[d] += 1;
            if idx[d] < dims[d].1.keys.len() {
                break;
            }
            idx[d] = 0;
        }
    }
}

/// Collect the dynamic namespaces a universe depends on.
fn universe_namespaces(universe: &PartitionUniverse, out: &mut HashSet<String>) {
    match universe {
        PartitionUniverse::Dynamic { namespace } => {
            out.insert(namespace.clone());
        }
        PartitionUniverse::Multi { dims } => {
            for (_, du) in dims {
                if let DimensionKind::Dynamic { namespace } = &du.kind {
                    out.insert(namespace.clone());
                }
            }
        }
        _ => {}
    }
}

/// One row of the per-asset evaluation result list, accumulated during
/// `evaluate` and consumed by `apply_results`.
pub struct EvalResultRow {
    pub info_idx: usize,
    pub result: EvalResult,
    pub tree: EvalNodeResult,
    pub duration_us: u64,
}

/// One asset to materialize this tick, with its (optional) partition selection.
pub struct ToMaterialize {
    pub asset_key: String,
    pub selection: Option<PartitionSelection>,
}

/// Materializations classified into the three dispatch shapes.
///
/// * `unpartitioned` — assets that materialize as a single combined run.
/// * `single_partition_groups` — assets that fired with one partition key,
///   bucketed by key so they share a combined run.
/// * `multi_partition_backfills` — assets that fired across 2+ partition keys,
///   each becoming its own backfill.
pub struct MaterializationPlan {
    pub unpartitioned: Vec<String>,
    pub single_partition_groups: HashMap<PartitionKey, Vec<String>>,
    pub multi_partition_backfills: Vec<(String, Vec<PartitionKey>)>,
}

impl MaterializationPlan {
    pub fn is_empty(&self) -> bool {
        self.unpartitioned.is_empty()
            && self.single_partition_groups.is_empty()
            && self.multi_partition_backfills.is_empty()
    }
}

/// Output of [`ConditionPass::run`]: per-asset eval rows plus the dispatch plan.
pub struct PassOutput {
    pub results: Vec<EvalResultRow>,
    pub plan: MaterializationPlan,
}

/// Compute partition keys that fall within the latest time window.
///
/// Partition keys are wall-clock labels, so the comparison happens on the
/// wall-clock timeline: `now_local` is the current local naive datetime. If
/// `lookback_delta` is `None`, only the single latest started window is
/// returned. With a lookback the cutoff anchors at that latest window's
/// START — keys within `[latest_start - lookback_delta, latest_start]` are
/// selected (`lookback_delta` in seconds) — so the selection never depends
/// on how far into the current window `now` falls, and a lookback of one
/// period reaches exactly one window back.
pub fn compute_latest_time_window_keys(
    all_keys: &HashSet<PartitionKey>,
    fmt: &str,
    now_local: NaiveDateTime,
    lookback_delta: Option<f64>,
) -> HashSet<PartitionKey> {
    let mut parsed: Vec<(&PartitionKey, NaiveDateTime)> = all_keys
        .iter()
        .filter_map(|pk| {
            let key_str = match pk {
                PartitionKey::Single { keys } if !keys.is_empty() => &keys[0],
                _ => return None,
            };
            let dt = parse_key_datetime(key_str, fmt).ok()?;
            if dt <= now_local {
                Some((pk, dt))
            } else {
                None
            }
        })
        .collect();

    if parsed.is_empty() {
        return HashSet::new();
    }

    parsed.sort_by(|a, b| b.1.cmp(&a.1));

    match lookback_delta {
        Some(delta_secs) => {
            let latest_start = parsed[0].1;
            let lookback_nanos = (delta_secs * 1_000_000_000.0) as i64;
            // A lookback too large to subtract means "no cutoff".
            let cutoff =
                latest_start.checked_sub_signed(chrono::Duration::nanoseconds(lookback_nanos));
            parsed
                .into_iter()
                .filter(|(_, dt)| cutoff.is_none_or(|c| *dt >= c))
                .map(|(pk, _)| pk.clone())
                .collect()
        }
        None => HashSet::from([parsed[0].0.clone()]),
    }
}

/// Owns the long-lived state of the condition evaluation loop and exposes a
/// per-tick `run` that returns evaluation results plus a classified
/// [`MaterializationPlan`]. The outer Python orchestrator drives `refresh_cache`
/// and `run`, then dispatches the plan via PyO3-bound launchers.
pub struct ConditionPass {
    pub cache: AssetConditionCache,
    pub eval_state: ConditionEvalState,
    pub conditions: Vec<AssetConditionInfo>,
    /// Index into `conditions` keyed by asset name; built once at construction.
    pub conditions_by_key: HashMap<String, usize>,
    /// Set of asset keys with active conditions, used by `update_dep_baselines`.
    pub active: HashSet<String>,
    pub has_time_based: bool,
    /// Lazily computed once `cache.initialized`; the asset subset to evaluate
    /// when storage hasn't changed but time-based conditions need re-eval.
    pub time_based_eval_set: Option<HashSet<String>>,
    pub upstream_partition_keys: HashMap<String, HashSet<PartitionKey>>,
    /// How each `upstream_partition_keys` entry evolves. Entries for
    /// conditioned assets are re-synced from their `PartitionInfo` instead.
    pub upstream_universes: HashMap<String, PartitionUniverse>,
}

impl ConditionPass {
    /// Build a new pass. `conditions` is expected to be in topological order
    /// (deps before downstreams) so single-tick `WillBeRequested` cascading
    /// works correctly.
    pub fn new(
        cache: AssetConditionCache,
        eval_state: ConditionEvalState,
        conditions: Vec<AssetConditionInfo>,
        upstream_partition_keys: HashMap<String, (HashSet<PartitionKey>, PartitionUniverse)>,
    ) -> Self {
        let conditions_by_key: HashMap<String, usize> = conditions
            .iter()
            .enumerate()
            .map(|(i, c)| (c.asset_key.clone(), i))
            .collect();
        let active: HashSet<String> = conditions.iter().map(|c| c.asset_key.clone()).collect();
        let has_time_based = conditions
            .iter()
            .any(|c| c.condition.has_time_based_conditions());
        let mut keys_map = HashMap::with_capacity(upstream_partition_keys.len());
        let mut universes_map = HashMap::with_capacity(upstream_partition_keys.len());
        for (k, (keys, universe)) in upstream_partition_keys {
            keys_map.insert(k.clone(), keys);
            universes_map.insert(k, universe);
        }
        Self {
            cache,
            eval_state,
            conditions,
            conditions_by_key,
            active,
            has_time_based,
            time_based_eval_set: None,
            upstream_partition_keys: keys_map,
            upstream_universes: universes_map,
        }
    }

    /// Dynamic namespaces any tracked universe depends on — the caller
    /// fetches their current key sets and passes them to
    /// [`refresh_partition_universes`](Self::refresh_partition_universes).
    pub fn dynamic_universe_namespaces(&self) -> HashSet<String> {
        let mut out = HashSet::new();
        for info in &self.conditions {
            if let Some(pi) = &info.partition_info {
                universe_namespaces(&pi.universe, &mut out);
            }
        }
        for universe in self.upstream_universes.values() {
            universe_namespaces(universe, &mut out);
        }
        out
    }

    /// Advance every tracked partition universe to `now`: open time grids
    /// gain newly started windows, dynamic namespaces mirror storage, and
    /// conditioned assets' upstream entries re-sync from their refreshed
    /// `PartitionInfo`. Returns whether any universe changed, so the caller
    /// can force an eval even when the storage cache reports no changes.
    pub fn refresh_partition_universes(
        &mut self,
        now: NaiveDateTime,
        dynamic_keys: &HashMap<String, HashSet<String>>,
    ) -> bool {
        let mut changed = false;
        for info in &mut self.conditions {
            if let Some(pi) = info.partition_info.as_mut() {
                let pi_changed =
                    refresh_universe(&mut pi.universe, &mut pi.all_keys, now, dynamic_keys);
                changed |= pi_changed;
                // The conditioned asset's upstream entry mirrors its
                // PartitionInfo; it was synced at extraction, so only a
                // change needs a re-copy.
                if pi_changed
                    && let Some(entry) = self.upstream_partition_keys.get_mut(&info.asset_key)
                {
                    entry.clone_from(&pi.all_keys);
                }
            }
        }
        for (asset, universe) in &mut self.upstream_universes {
            if let Some(keys) = self.upstream_partition_keys.get_mut(asset) {
                changed |= refresh_universe(universe, keys, now, dynamic_keys);
            }
        }
        changed
    }

    /// Refresh `cache` from storage atomically. Returns whether anything
    /// changed; surface errors so the caller can decide whether to skip
    /// the tick. `now` (wall-clock nanos) is used by the cache to evict
    /// dispatched-but-never-confirmed run_ids that have outlived the
    /// pending-grace window.
    pub async fn refresh_cache<S: StorageBackend>(
        &mut self,
        storage: &S,
        now: i64,
    ) -> Result<bool> {
        self.cache.refresh(storage, now).await
    }

    /// Lazily compute the time-based eval subset once cache is initialized.
    /// Only relevant when conditions contain time-based nodes (e.g.
    /// `CronTickPassed`). The subset includes their downstream descendants
    /// so cron cascades fire same-tick.
    pub fn ensure_time_based_eval_set(&mut self) {
        if !self.has_time_based || !self.cache.initialized || self.time_based_eval_set.is_some() {
            return;
        }
        let conds_for_set: Vec<(String, ConditionNode)> = self
            .conditions
            .iter()
            .map(|c| (c.asset_key.clone(), c.condition.clone()))
            .collect();
        let eval_set = self.cache.compute_time_based_eval_set(&conds_for_set);
        self.time_based_eval_set = Some(eval_set);
    }

    /// Skip this tick when nothing has changed in storage AND we're past
    /// the initial tick AND no time-based conditions need re-eval.
    pub fn should_skip(&self, has_changes: bool) -> bool {
        !has_changes
            && self.cache.initialized
            && !self.eval_state.is_initial
            && !self.has_time_based
    }

    /// One full tick: evaluate every condition tree (or the time-based subset
    /// when `selective`), apply per-asset state mutations, and return the
    /// classified materialization plan.
    pub fn run(&mut self, now: i64, selective: bool) -> PassOutput {
        if self.eval_state.is_initial {
            self.eval_state.is_initial = false;
        }
        let results = self.evaluate(now, selective);
        let to_materialize = self.apply_results(&results, now);
        let plan = self.classify_materializations(to_materialize);
        self.stamp_dispatched_handled(&plan, now);
        PassOutput { results, plan }
    }

    fn evaluate(&self, now: i64, selective: bool) -> Vec<EvalResultRow> {
        // Partition keys are wall-clock labels; latest-window math compares
        // them against the local naive reading of the tick instant.
        let now_local = chrono::Local.timestamp_nanos(now).naive_local();
        let in_progress_keys: HashSet<String> =
            self.cache.in_progress_assets.keys().cloned().collect();

        // WillBeRequested reads this set inside dep pivots for single-tick cascading.
        let mut requested_this_tick: HashMap<String, PartitionSelection> = HashMap::new();
        let mut results: Vec<EvalResultRow> = Vec::new();

        for (idx, info) in self.conditions.iter().enumerate() {
            if selective
                && let Some(ref eval_set) = self.time_based_eval_set
                && !eval_set.contains(&info.asset_key)
            {
                continue;
            }
            let record = match self.cache.records.get(&info.asset_key) {
                Some(r) => r,
                None => continue,
            };
            let prev = &self.eval_state.assets[&info.asset_key];

            let latest_tw_keys = info.partition_info.as_ref().and_then(|pi| {
                let fmt = pi.time_window_fmt.as_ref()?;
                let lookback = info.condition.find_lookback_delta()?;
                Some(compute_latest_time_window_keys(
                    &pi.all_keys,
                    fmt,
                    now_local,
                    lookback,
                ))
            });

            let pctx = info.partition_info.as_ref().and_then(|pi| {
                self.cache
                    .partition_status
                    .get(&info.asset_key)
                    .map(|status| PartitionEvalContext {
                        all_keys: &pi.all_keys,
                        materialized: &status.materialized,
                        in_progress: &status.in_progress,
                        failed: &status.failed,
                        timestamps: &status.timestamps,
                        resolver: PartitionResolver::new(
                            &pi.mappings,
                            &self.upstream_partition_keys,
                        ),
                        latest_time_window_keys: latest_tw_keys.as_ref(),
                        all_partition_statuses: &self.cache.partition_status,
                        dep_root_floor: None,
                    })
            });

            let ctx = EvalContext {
                target_key: &info.asset_key,
                root_key: &info.asset_key,
                target_record: record,
                cache: CacheSnapshot {
                    records: &self.cache.records,
                    upstream_deps: &self.cache.upstream_deps,
                    in_progress_assets: &in_progress_keys,
                    failed_assets: &self.cache.failed_assets,
                    failed_asset_timestamps: &self.cache.failed_asset_timestamps,
                    backfill: &self.cache.backfill,
                },
                tags: RunTagSnapshot {
                    last_run_tags: &self.cache.last_run_tags,
                    partition_last_run_tags: &self.cache.partition_last_run_tags,
                    tick_materialization_tags: &self.cache.tick_materialization_tags,
                    tick_partition_materialization_tags: &self
                        .cache
                        .tick_partition_materialization_tags,
                    last_run_asset_names: &self.cache.last_run_asset_names,
                    partition_last_run_asset_names: &self.cache.partition_last_run_asset_names,
                },
                prev_state: prev,
                all_asset_states: &self.eval_state.assets,
                requested_this_tick: &requested_this_tick,
                now,
                is_initial: self.eval_state.is_initial || prev.is_initial,
                partitions: pctx.as_ref(),
                root_partition_floor: None,
            };
            let start = std::time::Instant::now();
            let (eval_result, tree) = evaluate_with_tree(&info.condition, &ctx);
            let duration_us = start.elapsed().as_micros() as u64;

            if eval_result.fired {
                requested_this_tick.insert(
                    info.asset_key.clone(),
                    eval_result
                        .selection
                        .clone()
                        .unwrap_or(PartitionSelection::All),
                );
            }

            results.push(EvalResultRow {
                info_idx: idx,
                result: eval_result,
                tree,
                duration_us,
            });
        }
        results
    }

    fn apply_results(&mut self, results: &[EvalResultRow], now: i64) -> Vec<ToMaterialize> {
        let mut to_materialize: Vec<ToMaterialize> = Vec::new();

        for row in results {
            let info = &self.conditions[row.info_idx];
            let record = match self.cache.records.get(&info.asset_key) {
                Some(r) => r,
                None => continue,
            };
            let prev = self.eval_state.assets.get_mut(&info.asset_key).unwrap();

            let partition_timestamps = info.partition_info.as_ref().and_then(|_| {
                self.cache
                    .partition_status
                    .get(&info.asset_key)
                    .map(|status| &status.timestamps)
            });
            let was_initial = prev.is_initial;
            let update_ctx = StateUpdateContext {
                target_record_timestamp: record.last_timestamp,
                target_data_version: record.last_data_version.as_ref(),
                now,
                is_initial: was_initial,
                partition_timestamps,
            };
            update_condition_state(prev, &update_ctx, &row.result);

            // Baseline dep state when fired or on initial tick so that
            // NewlyUpdated has previous timestamps to compare against.
            if row.result.fired || was_initial {
                update_dep_baselines(
                    &mut self.eval_state.assets,
                    &self.cache.upstream_deps,
                    &self.active,
                    &self.cache.partition_status,
                    &self.cache.records,
                );
            }

            if row.result.fired && !self.cache.in_progress_assets.contains_key(&info.asset_key) {
                to_materialize.push(ToMaterialize {
                    asset_key: info.asset_key.clone(),
                    selection: row.result.selection.clone(),
                });
            }
        }

        for tm in &to_materialize {
            if let Some(PartitionSelection::Keys(keys)) = &tm.selection {
                tracing::info!(
                    target: "rivers::daemon",
                    asset_key = %tm.asset_key,
                    partitions = keys.len(),
                    "condition fired, triggering partition materialization"
                );
            } else {
                tracing::info!(
                    target: "rivers::daemon",
                    asset_key = %tm.asset_key,
                    "condition fired, triggering materialization"
                );
            }
        }

        to_materialize
    }

    /// Record dispatched partition keys in the asset's `handled` set so
    /// since-last-handled semantics don't re-fire them on the next tick.
    fn mark_partitions_handled(&mut self, asset_key: &str, keys: &[PartitionKey]) {
        if let Some(prev) = self.eval_state.assets.get_mut(asset_key) {
            prev.partition_state
                .get_or_insert_with(PartitionState::default)
                .handled
                .extend(keys.iter().cloned());
        }
    }

    /// Split the materialization list into the three dispatch shapes:
    /// * unpartitioned bulk
    /// * single-partition groups (bucketed by partition key)
    /// * multi-partition backfills
    ///
    /// Selections of `Some(All)` or `None` for a partitioned asset are
    /// resolved to all of the asset's partition keys; for an unpartitioned
    /// asset they collapse to the bulk path. Marks each materialized asset
    /// present in `cache.in_progress_assets` so subsequent ticks see it as
    /// in-progress before the dispatch loop pushes any run ids, and records
    /// the surviving keys in the asset's `handled` set — only keys actually
    /// dispatched may suppress future fires.
    fn classify_materializations(
        &mut self,
        to_materialize: Vec<ToMaterialize>,
    ) -> MaterializationPlan {
        let mut unpartitioned: Vec<String> = Vec::new();
        let mut partitioned_mats: Vec<(String, Vec<PartitionKey>)> = Vec::new();

        for tm in to_materialize {
            // The in-progress entry gates next tick's dispatch until run ids
            // land on it, so it may only exist for assets that actually
            // produce a dispatch — a fully-dropped selection would leave an
            // empty entry nothing ever clears, wedging the asset.
            match tm.selection {
                Some(PartitionSelection::Keys(keys)) if !keys.is_empty() => {
                    // A mapped selection can name keys this asset doesn't
                    // have (e.g. a time_window shift when the upstream's
                    // range extends past this asset's) — constrain to real
                    // partitions instead of dispatching phantom ones.
                    let total = keys.len();
                    let surviving: Vec<PartitionKey> = match self
                        .conditions_by_key
                        .get(tm.asset_key.as_str())
                        .map(|&idx| &self.conditions[idx])
                        .and_then(|info| info.partition_info.as_ref())
                    {
                        Some(pi) => keys
                            .into_iter()
                            .filter(|k| pi.all_keys.contains(k))
                            .collect(),
                        None => keys.into_iter().collect(),
                    };
                    if surviving.len() < total {
                        tracing::warn!(
                            target: "rivers::daemon",
                            asset_key = %tm.asset_key,
                            dropped = total - surviving.len(),
                            "condition selection named partition keys the asset does not have; dropping them"
                        );
                    }
                    if !surviving.is_empty() {
                        self.cache
                            .in_progress_assets
                            .entry(tm.asset_key.clone())
                            .or_default();
                        self.mark_partitions_handled(&tm.asset_key, &surviving);
                        partitioned_mats.push((tm.asset_key, surviving));
                    }
                }
                Some(PartitionSelection::All) | None => {
                    let resolved = self
                        .conditions_by_key
                        .get(tm.asset_key.as_str())
                        .map(|&idx| &self.conditions[idx])
                        .and_then(|info| info.partition_info.as_ref())
                        .map(|pi| pi.all_keys.iter().cloned().collect::<Vec<_>>());
                    match resolved {
                        Some(all_keys) if !all_keys.is_empty() => {
                            self.cache
                                .in_progress_assets
                                .entry(tm.asset_key.clone())
                                .or_default();
                            self.mark_partitions_handled(&tm.asset_key, &all_keys);
                            partitioned_mats.push((tm.asset_key, all_keys));
                        }
                        Some(_) => {} // partitioned asset with zero keys — drop
                        None => {
                            self.cache
                                .in_progress_assets
                                .entry(tm.asset_key.clone())
                                .or_default();
                            unpartitioned.push(tm.asset_key);
                        }
                    }
                }
                _ => {
                    self.cache
                        .in_progress_assets
                        .entry(tm.asset_key.clone())
                        .or_default();
                    unpartitioned.push(tm.asset_key);
                }
            }
        }

        let mut single_partition_groups: HashMap<PartitionKey, Vec<String>> = HashMap::new();
        let mut multi_partition_backfills: Vec<(String, Vec<PartitionKey>)> = Vec::new();
        for (asset_key, mut partition_keys) in partitioned_mats {
            // Selections come out of HashSets (per-process random order); the
            // list persists into backfill records and drives dispatch order.
            partition_keys.sort_by_cached_key(|k| k.to_display());
            if partition_keys.len() == 1 {
                single_partition_groups
                    .entry(partition_keys.into_iter().next().unwrap())
                    .or_default()
                    .push(asset_key);
            } else {
                multi_partition_backfills.push((asset_key, partition_keys));
            }
        }

        MaterializationPlan {
            unpartitioned,
            single_partition_groups,
            multi_partition_backfills,
        }
    }

    /// Advance the asset-level handled cursor only for assets the plan actually
    /// dispatched — a selection `classify` trimmed to zero must not advance it,
    /// or `NewlyRequested` reads true next tick with nothing dispatched. Mirrors
    /// the per-partition `handled` set classify already restricts to survivors.
    fn stamp_dispatched_handled(&mut self, plan: &MaterializationPlan, now: i64) {
        let dispatched: HashSet<&str> = plan
            .unpartitioned
            .iter()
            .map(String::as_str)
            .chain(
                plan.single_partition_groups
                    .values()
                    .flatten()
                    .map(String::as_str),
            )
            .chain(
                plan.multi_partition_backfills
                    .iter()
                    .map(|(asset, _)| asset.as_str()),
            )
            .collect();
        for asset_key in dispatched {
            if let Some(prev) = self.eval_state.assets.get_mut(asset_key) {
                prev.last_handled_timestamp = Some(now);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spk(s: &str) -> PartitionKey {
        PartitionKey::Single {
            keys: vec![s.to_string()],
        }
    }

    fn make_daily_keys(keys: &[&str]) -> HashSet<PartitionKey> {
        keys.iter().map(|k| spk(k)).collect()
    }

    /// Helper: parse "YYYY-MM-DD HH:MM" as a wall-clock naive datetime.
    fn to_wall(dt_str: &str) -> NaiveDateTime {
        chrono::NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%d %H:%M").unwrap()
    }

    fn test_record(key: &str) -> crate::storage::AssetRecord {
        crate::storage::AssetRecord {
            code_location_id: crate::storage::default_code_location_id(),
            asset_key: key.to_string(),
            tags: vec![],
            kinds: vec![],
            asset_group: None,
            code_version: None,
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![],
        }
    }

    /// Build a one-partitioned-asset pass and run a full
    /// apply_results → classify → stamp tick with the given fired selection.
    /// Returns the asset's handled cursor after the tick.
    fn handled_after_fired_selection(selection: PartitionSelection) -> Option<i64> {
        let mut cache = AssetConditionCache::default();
        cache.records.insert("down".to_string(), test_record("down"));
        let mut pass = ConditionPass::new(
            cache,
            ConditionEvalState::default(),
            vec![AssetConditionInfo {
                asset_key: "down".to_string(),
                condition: ConditionNode::InProgress,
                partition_info: Some(PartitionInfo {
                    all_keys: make_daily_keys(&["2024-01-01"]),
                    mappings: HashMap::new(),
                    time_window_fmt: None,
                    universe: PartitionUniverse::Frozen,
                }),
                backfill_strategy: None,
            }],
            HashMap::new(),
        );
        pass.eval_state.assets.insert(
            "down".to_string(),
            crate::condition::state::AssetConditionState::default(),
        );
        let row = EvalResultRow {
            info_idx: 0,
            result: EvalResult {
                fired: true,
                selection: Some(selection),
                ..Default::default()
            },
            tree: EvalNodeResult::new(
                &ConditionNode::InProgress,
                0,
                crate::condition::state::NodeStatus::True,
                vec![],
                None,
            ),
            duration_us: 0,
        };
        let to_mat = pass.apply_results(&[row], 5000);
        let plan = pass.classify_materializations(to_mat);
        pass.stamp_dispatched_handled(&plan, 5000);
        pass.eval_state.assets["down"].last_handled_timestamp
    }

    #[test]
    fn handled_cursor_skips_fully_dropped_selection() {
        // An asset fires for a key outside its universe; classify trims the
        // selection to zero, so nothing dispatches. The handled cursor must
        // NOT advance — apply_results used to stamp it before classify ran, so
        // NewlyRequested read true next tick with nothing dispatched.
        let handled = handled_after_fired_selection(PartitionSelection::Keys(
            [spk("2099-12-31")].into_iter().collect(),
        ));
        assert_eq!(
            handled, None,
            "a fully-dropped selection must not advance the handled cursor"
        );
    }

    #[test]
    fn handled_cursor_advances_for_surviving_selection() {
        // The boundary: a selection with a real key DOES dispatch and so must
        // advance the handled cursor.
        let handled = handled_after_fired_selection(PartitionSelection::Keys(
            [spk("2024-01-01")].into_iter().collect(),
        ));
        assert_eq!(
            handled,
            Some(5000),
            "a dispatched selection must advance the handled cursor"
        );
    }

    #[test]
    fn classify_drops_mapped_keys_the_asset_does_not_have() {
        // A mapped selection can name keys outside the asset's own range
        // (e.g. a time_window shift when the upstream's range extends past
        // this asset's) — those must not dispatch a materialization of a
        // partition that doesn't exist.
        let mut pass = ConditionPass::new(
            AssetConditionCache::default(),
            ConditionEvalState::default(),
            vec![AssetConditionInfo {
                asset_key: "down".to_string(),
                condition: ConditionNode::InProgress,
                partition_info: Some(PartitionInfo {
                    all_keys: make_daily_keys(&["2024-01-01", "2024-01-02"]),
                    mappings: HashMap::new(),
                    time_window_fmt: None,
                    universe: PartitionUniverse::Frozen,
                }),
                backfill_strategy: None,
            }],
            HashMap::new(),
        );
        let plan = pass.classify_materializations(vec![ToMaterialize {
            asset_key: "down".to_string(),
            selection: Some(PartitionSelection::Keys(
                [spk("2024-01-02"), spk("2024-08-16")].into_iter().collect(),
            )),
        }]);
        assert_eq!(
            plan.single_partition_groups,
            HashMap::from([(spk("2024-01-02"), vec!["down".to_string()])])
        );
        assert!(plan.multi_partition_backfills.is_empty());
        assert!(plan.unpartitioned.is_empty());
    }

    #[test]
    fn classify_skips_asset_when_no_mapped_key_survives() {
        let mut pass = ConditionPass::new(
            AssetConditionCache::default(),
            ConditionEvalState::default(),
            vec![AssetConditionInfo {
                asset_key: "down".to_string(),
                condition: ConditionNode::InProgress,
                partition_info: Some(PartitionInfo {
                    all_keys: make_daily_keys(&["2024-01-01", "2024-01-02"]),
                    mappings: HashMap::new(),
                    time_window_fmt: None,
                    universe: PartitionUniverse::Frozen,
                }),
                backfill_strategy: None,
            }],
            HashMap::new(),
        );
        let plan = pass.classify_materializations(vec![ToMaterialize {
            asset_key: "down".to_string(),
            selection: Some(PartitionSelection::Keys(
                [spk("2024-08-16")].into_iter().collect(),
            )),
        }]);
        assert!(plan.is_empty());
    }

    fn naive(s: &str) -> NaiveDateTime {
        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").unwrap()
    }

    fn hourly_grid() -> TimeGrid {
        TimeGrid {
            cron_schedule: None,
            interval_seconds: Some(3600.0),
            start: naive("2024-01-01T00:00:00"),
            end: None,
            fmt: "%Y-%m-%dT%H:%M:%S".into(),
        }
    }

    #[test]
    fn time_window_universe_gains_new_windows() {
        let mut universe = PartitionUniverse::TimeWindow {
            grid: hourly_grid(),
            enumerated_to: naive("2024-01-01T01:30:00"),
        };
        let mut all_keys: HashSet<PartitionKey> =
            [spk("2024-01-01T00:00:00"), spk("2024-01-01T01:00:00")]
                .into_iter()
                .collect();
        refresh_universe(
            &mut universe,
            &mut all_keys,
            naive("2024-01-01T03:10:00"),
            &HashMap::new(),
        );
        assert!(all_keys.contains(&spk("2024-01-01T02:00:00")));
        assert!(all_keys.contains(&spk("2024-01-01T03:00:00")));
        assert_eq!(all_keys.len(), 4);
        // Idempotent: a second refresh at the same now adds nothing.
        refresh_universe(
            &mut universe,
            &mut all_keys,
            naive("2024-01-01T03:10:00"),
            &HashMap::new(),
        );
        assert_eq!(all_keys.len(), 4);
    }

    #[test]
    fn refresh_universe_holds_watermark_on_enumeration_error() {
        // A grid that can't enumerate must not advance `enumerated_to` past
        // the failed range — those windows would be skipped forever once the
        // grid recovers. Unreachable through validated constructors today;
        // guards against a future Err source.
        let broken = TimeGrid {
            cron_schedule: None,
            interval_seconds: None,
            start: naive("2024-01-01T00:00:00"),
            end: None,
            fmt: "%Y-%m-%dT%H:%M:%S".into(),
        };
        let t0 = naive("2024-01-01T00:00:00");
        let mut universe = PartitionUniverse::TimeWindow {
            grid: broken.clone(),
            enumerated_to: t0,
        };
        let mut all_keys: HashSet<PartitionKey> = HashSet::new();
        let changed = refresh_universe(
            &mut universe,
            &mut all_keys,
            naive("2024-01-01T05:00:00"),
            &HashMap::new(),
        );
        assert!(!changed);
        assert!(all_keys.is_empty());
        let PartitionUniverse::TimeWindow { enumerated_to, .. } = &universe else {
            unreachable!()
        };
        assert_eq!(*enumerated_to, t0, "failed ranges must be retried");

        let mut multi = PartitionUniverse::Multi {
            dims: vec![(
                "date".to_string(),
                DimensionUniverse {
                    keys: OrderSet::new(),
                    kind: DimensionKind::TimeWindow {
                        grid: broken,
                        enumerated_to: t0,
                    },
                },
            )],
        };
        let changed = refresh_universe(
            &mut multi,
            &mut all_keys,
            naive("2024-01-01T05:00:00"),
            &HashMap::new(),
        );
        assert!(!changed);
        let PartitionUniverse::Multi { dims } = &multi else {
            unreachable!()
        };
        let DimensionKind::TimeWindow { enumerated_to, .. } = &dims[0].1.kind else {
            unreachable!()
        };
        assert_eq!(*enumerated_to, t0, "failed dim ranges must be retried");
    }

    #[test]
    fn dynamic_universe_mirrors_storage_including_retirement() {
        let mut universe = PartitionUniverse::Dynamic {
            namespace: "colors".into(),
        };
        let mut all_keys: HashSet<PartitionKey> = HashSet::new();
        let now = naive("2024-01-01T00:00:00");
        let registered: HashMap<String, HashSet<String>> = [(
            "colors".to_string(),
            ["red".to_string(), "blue".to_string()]
                .into_iter()
                .collect(),
        )]
        .into_iter()
        .collect();
        refresh_universe(&mut universe, &mut all_keys, now, &registered);
        assert_eq!(all_keys, [spk("red"), spk("blue")].into_iter().collect());
        // Retirement: the set mirrors storage, it doesn't only grow.
        let shrunk: HashMap<String, HashSet<String>> = [(
            "colors".to_string(),
            ["blue".to_string()].into_iter().collect(),
        )]
        .into_iter()
        .collect();
        refresh_universe(&mut universe, &mut all_keys, now, &shrunk);
        assert_eq!(all_keys, [spk("blue")].into_iter().collect());
        // A namespace missing from the fetch (storage failure) keeps the
        // previous set — stale beats empty.
        refresh_universe(&mut universe, &mut all_keys, now, &HashMap::new());
        assert_eq!(all_keys, [spk("blue")].into_iter().collect());
    }

    #[test]
    fn multi_universe_recomputes_cartesian_on_dimension_change() {
        let mut universe = PartitionUniverse::Multi {
            dims: vec![
                (
                    "date".to_string(),
                    DimensionUniverse {
                        keys: ["2024-01-01T00:00:00".to_string()].into_iter().collect(),
                        kind: DimensionKind::TimeWindow {
                            grid: hourly_grid(),
                            enumerated_to: naive("2024-01-01T00:30:00"),
                        },
                    },
                ),
                (
                    "region".to_string(),
                    DimensionUniverse {
                        keys: ["eu".to_string(), "us".to_string()].into_iter().collect(),
                        kind: DimensionKind::Frozen,
                    },
                ),
            ],
        };
        let mut all_keys: HashSet<PartitionKey> = HashSet::new();
        refresh_universe(
            &mut universe,
            &mut all_keys,
            naive("2024-01-01T01:10:00"),
            &HashMap::new(),
        );
        assert_eq!(all_keys.len(), 4);
        let expected = PartitionKey::Multi {
            dims: vec![
                ("date".to_string(), vec!["2024-01-01T01:00:00".to_string()]),
                ("region".to_string(), vec!["eu".to_string()]),
            ],
        };
        assert!(all_keys.contains(&expected));
    }

    #[test]
    fn multi_dim_refresh_never_duplicates_seeded_window_starts() {
        // Seeding enumerates a dim through its explicit (future) end while
        // the watermark starts at daemon-start `now`; later refreshes re-yield
        // already-seeded starts and must neither append duplicates nor report
        // a spurious change.
        let grid = TimeGrid {
            cron_schedule: None,
            interval_seconds: Some(3600.0),
            start: naive("2024-01-01T00:00:00"),
            end: Some(naive("2024-01-02T00:00:00")),
            fmt: "%Y-%m-%dT%H:%M:%S".into(),
        };
        let seeded: Vec<String> = (0..24)
            .map(|h| format!("2024-01-01T{h:02}:00:00"))
            .collect();
        let mut universe = PartitionUniverse::Multi {
            dims: vec![(
                "date".to_string(),
                DimensionUniverse {
                    keys: seeded.iter().cloned().collect(),
                    kind: DimensionKind::TimeWindow {
                        grid,
                        enumerated_to: naive("2024-01-01T01:30:00"),
                    },
                },
            )],
        };
        let mut all_keys: HashSet<PartitionKey> = HashSet::new();
        let changed = refresh_universe(
            &mut universe,
            &mut all_keys,
            naive("2024-01-01T03:10:00"),
            &HashMap::new(),
        );
        let PartitionUniverse::Multi { dims } = &universe else {
            unreachable!()
        };
        assert!(
            !changed,
            "re-yielding already-seeded starts is not a change"
        );
        assert!(
            dims[0].1.keys.iter().eq(seeded.iter()),
            "no duplicate dim keys"
        );
    }

    #[test]
    fn update_state_leaves_handled_to_classification() {
        // The raw selection may name keys classification will drop (mapped
        // keys the asset doesn't have). Marking them handled here would
        // suppress them forever once they become real.
        let mut state = crate::condition::state::AssetConditionState::default();
        let timestamps: HashMap<PartitionKey, i64> = HashMap::new();
        let ctx = StateUpdateContext {
            target_record_timestamp: None,
            target_data_version: None,
            now: 1,
            is_initial: false,
            partition_timestamps: Some(&timestamps),
        };
        let result = EvalResult {
            fired: true,
            selection: Some(PartitionSelection::Keys(
                [spk("2024-08-16")].into_iter().collect(),
            )),
            sub_selections: Some(HashMap::new()),
            ..Default::default()
        };
        update_condition_state(&mut state, &ctx, &result);
        let ps = state.partition_state.as_ref().expect("partition state");
        assert!(
            ps.handled.is_empty(),
            "handled must be extended from the classified plan, not the raw selection"
        );
    }

    #[test]
    fn classify_marks_only_surviving_keys_handled() {
        let mut pass = ConditionPass::new(
            AssetConditionCache::default(),
            ConditionEvalState::default(),
            vec![AssetConditionInfo {
                asset_key: "down".to_string(),
                condition: ConditionNode::InProgress,
                partition_info: Some(PartitionInfo {
                    all_keys: make_daily_keys(&["2024-01-01", "2024-01-02"]),
                    mappings: HashMap::new(),
                    time_window_fmt: None,
                    universe: PartitionUniverse::Frozen,
                }),
                backfill_strategy: None,
            }],
            HashMap::new(),
        );
        pass.eval_state.assets.insert(
            "down".to_string(),
            crate::condition::state::AssetConditionState::default(),
        );
        pass.classify_materializations(vec![ToMaterialize {
            asset_key: "down".to_string(),
            selection: Some(PartitionSelection::Keys(
                [spk("2024-01-02"), spk("2024-08-16")].into_iter().collect(),
            )),
        }]);
        let handled = pass.eval_state.assets["down"]
            .partition_state
            .as_ref()
            .expect("partition state")
            .handled
            .clone();
        assert!(handled.contains(&spk("2024-01-02")));
        assert!(
            !handled.contains(&spk("2024-08-16")),
            "dropped keys must not be marked handled"
        );
    }

    #[test]
    fn classify_orders_partition_keys_canonically() {
        // HashSet iteration order is per-process random; the dispatched key
        // list persists into durable backfill records and drives run order,
        // so it must come out in canonical display order.
        let days: Vec<String> = (1..=12).map(|d| format!("2024-01-{d:02}")).collect();
        let day_refs: Vec<&str> = days.iter().map(String::as_str).collect();
        let mut pass = ConditionPass::new(
            AssetConditionCache::default(),
            ConditionEvalState::default(),
            vec![AssetConditionInfo {
                asset_key: "down".to_string(),
                condition: ConditionNode::InProgress,
                partition_info: Some(PartitionInfo {
                    all_keys: make_daily_keys(&day_refs),
                    mappings: HashMap::new(),
                    time_window_fmt: None,
                    universe: PartitionUniverse::Frozen,
                }),
                backfill_strategy: None,
            }],
            HashMap::new(),
        );
        pass.eval_state.assets.insert(
            "down".to_string(),
            crate::condition::state::AssetConditionState::default(),
        );
        let plan = pass.classify_materializations(vec![ToMaterialize {
            asset_key: "down".to_string(),
            selection: Some(PartitionSelection::All),
        }]);
        let mut backfills = plan.multi_partition_backfills;
        let (_, keys) = backfills.pop().unwrap();
        let display: Vec<String> = keys.iter().map(|k| k.to_display()).collect();
        let mut sorted = display.clone();
        sorted.sort_unstable();
        assert_eq!(display, sorted, "dispatched keys must be display-ordered");
    }

    #[test]
    fn classify_fully_dropped_selection_leaves_no_in_progress_entry() {
        // A selection whose keys are all outside the asset's universe
        // dispatches nothing — it must not pre-mark the asset in-progress.
        // The empty entry has no clear path (no run ids ever land), so it
        // would gate every future dispatch for the asset forever.
        let mut pass = ConditionPass::new(
            AssetConditionCache::default(),
            ConditionEvalState::default(),
            vec![AssetConditionInfo {
                asset_key: "down".to_string(),
                condition: ConditionNode::InProgress,
                partition_info: Some(PartitionInfo {
                    all_keys: make_daily_keys(&["2024-01-01", "2024-01-02"]),
                    mappings: HashMap::new(),
                    time_window_fmt: None,
                    universe: PartitionUniverse::Frozen,
                }),
                backfill_strategy: None,
            }],
            HashMap::new(),
        );
        pass.eval_state.assets.insert(
            "down".to_string(),
            crate::condition::state::AssetConditionState::default(),
        );
        let plan = pass.classify_materializations(vec![ToMaterialize {
            asset_key: "down".to_string(),
            selection: Some(PartitionSelection::Keys(make_daily_keys(&["1999-01-01"]))),
        }]);
        assert!(plan.unpartitioned.is_empty());
        assert!(plan.single_partition_groups.is_empty());
        assert!(plan.multi_partition_backfills.is_empty());
        assert!(
            !pass.cache.in_progress_assets.contains_key("down"),
            "a fully-dropped selection must not wedge the asset behind an \
             in-progress entry nothing will ever clear"
        );

        // Control: a surviving selection still pre-marks the asset.
        let plan = pass.classify_materializations(vec![ToMaterialize {
            asset_key: "down".to_string(),
            selection: Some(PartitionSelection::Keys(make_daily_keys(&["2024-01-01"]))),
        }]);
        assert_eq!(plan.single_partition_groups.len(), 1);
        assert!(pass.cache.in_progress_assets.contains_key("down"));
    }

    #[test]
    fn classify_marks_all_selection_keys_handled() {
        // A partitioned asset can fire with `All` — e.g. an unpartitioned
        // upstream dep evaluates to a bool selection that widens to every
        // partition. The dispatched keys must land in `handled` exactly like
        // a Keys selection, or since-last-handled semantics re-fire the
        // whole asset on every tick.
        let mut pass = ConditionPass::new(
            AssetConditionCache::default(),
            ConditionEvalState::default(),
            vec![AssetConditionInfo {
                asset_key: "down".to_string(),
                condition: ConditionNode::InProgress,
                partition_info: Some(PartitionInfo {
                    all_keys: make_daily_keys(&["2024-01-01", "2024-01-02"]),
                    mappings: HashMap::new(),
                    time_window_fmt: None,
                    universe: PartitionUniverse::Frozen,
                }),
                backfill_strategy: None,
            }],
            HashMap::new(),
        );
        pass.eval_state.assets.insert(
            "down".to_string(),
            crate::condition::state::AssetConditionState::default(),
        );
        let plan = pass.classify_materializations(vec![ToMaterialize {
            asset_key: "down".to_string(),
            selection: Some(PartitionSelection::All),
        }]);
        let mut backfills = plan.multi_partition_backfills;
        assert_eq!(
            backfills.len(),
            1,
            "All must dispatch the asset's partitions"
        );
        let (asset, mut keys) = backfills.pop().unwrap();
        assert_eq!(asset, "down");
        keys.sort_by_key(|k| format!("{k:?}"));
        assert_eq!(keys, vec![spk("2024-01-01"), spk("2024-01-02")]);
        let handled = pass.eval_state.assets["down"]
            .partition_state
            .as_ref()
            .expect("partition state")
            .handled
            .clone();
        assert!(handled.contains(&spk("2024-01-01")));
        assert!(handled.contains(&spk("2024-01-02")));
    }

    #[test]
    fn latest_window_tracks_wall_clock_on_non_utc_hosts() {
        // Partition keys are wall-clock labels. On a UTC+2 host at 10:30
        // local, now_ts() reads 08:30Z — the latest window is still the
        // 10:00 wall-clock one, not 08:00. `evaluate` converts the tick
        // instant to local naive before calling this, so the helper itself
        // compares wall clock to wall clock.
        let all_keys =
            make_daily_keys(&["2026-06-11T08:00", "2026-06-11T09:00", "2026-06-11T10:00"]);
        let now_local = to_wall("2026-06-11 10:30");
        let result = compute_latest_time_window_keys(&all_keys, "%Y-%m-%dT%H:%M", now_local, None);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&spk("2026-06-11T10:00")));
    }

    #[test]
    fn test_compute_at_start_date_includes_current() {
        let all_keys = make_daily_keys(&["2026-03-01"]);
        let now = to_wall("2026-03-01 00:00");
        let result = compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now, None);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&spk("2026-03-01")));
    }

    #[test]
    fn test_compute_latest_single_partition_no_lookback() {
        let all_keys = make_daily_keys(&["2026-03-20", "2026-03-21", "2026-03-22", "2026-03-23"]);
        let now = to_wall("2026-03-23 01:00");
        let result = compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now, None);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&spk("2026-03-23")));
    }

    #[test]
    fn test_compute_latest_advances_with_time() {
        let all_keys = make_daily_keys(&[
            "2026-03-20",
            "2026-03-21",
            "2026-03-22",
            "2026-03-23",
            "2026-03-24",
            "2026-03-25",
            "2026-03-26",
        ]);
        let now1 = to_wall("2026-03-22 01:00");
        let r1 = compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now1, None);
        assert_eq!(r1.len(), 1);
        assert!(r1.contains(&spk("2026-03-22")));

        let now2 = to_wall("2026-03-26 01:00");
        let r2 = compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now2, None);
        assert_eq!(r2.len(), 1);
        assert!(r2.contains(&spk("2026-03-26")));
    }

    #[test]
    fn test_compute_lookback_3_days() {
        let all_keys = make_daily_keys(&[
            "2026-03-20",
            "2026-03-21",
            "2026-03-22",
            "2026-03-23",
            "2026-03-24",
            "2026-03-25",
            "2026-03-26",
        ]);
        let now = to_wall("2026-03-26 01:00");
        let lookback_secs = 3.0 * 86400.0;
        let result =
            compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now, Some(lookback_secs));
        // cutoff = latest start (03-26) - 3d = 2026-03-23 00:00, inclusive.
        assert_eq!(result.len(), 4);
        assert!(result.contains(&spk("2026-03-23")));
        assert!(result.contains(&spk("2026-03-24")));
        assert!(result.contains(&spk("2026-03-25")));
        assert!(result.contains(&spk("2026-03-26")));
    }

    #[test]
    fn test_compute_lookback_advances_with_time() {
        let all_keys = make_daily_keys(&[
            "2026-03-15",
            "2026-03-16",
            "2026-03-17",
            "2026-03-18",
            "2026-03-19",
            "2026-03-20",
            "2026-03-21",
        ]);
        let now = to_wall("2026-03-21 01:00");
        let lookback_secs = 3.0 * 86400.0;
        let result =
            compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now, Some(lookback_secs));
        assert_eq!(result.len(), 4);
        assert!(result.contains(&spk("2026-03-18")));
        assert!(result.contains(&spk("2026-03-19")));
        assert!(result.contains(&spk("2026-03-20")));
        assert!(result.contains(&spk("2026-03-21")));
    }

    #[test]
    fn lookback_smaller_than_period_still_selects_latest_window() {
        // The cutoff anchors at the latest window START, not `now`: a lookback
        // smaller than the period must select exactly the latest window (same
        // as lookback=None), never an empty set.
        let all_keys = make_daily_keys(&["2026-03-24", "2026-03-25", "2026-03-26"]);
        let now = to_wall("2026-03-26 12:00");
        let result = compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now, Some(3600.0));
        assert_eq!(result.len(), 1, "1h lookback at 12:00 must not go empty");
        assert!(result.contains(&spk("2026-03-26")));
    }

    #[test]
    fn lookback_of_one_period_selects_previous_window_all_day() {
        // lookback = one period reaches exactly one window back regardless of
        // where inside the latest window `now` falls.
        let all_keys = make_daily_keys(&["2026-03-24", "2026-03-25", "2026-03-26"]);
        let now = to_wall("2026-03-26 23:59");
        let result = compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now, Some(86400.0));
        assert_eq!(result.len(), 2);
        assert!(result.contains(&spk("2026-03-25")));
        assert!(result.contains(&spk("2026-03-26")));
    }

    #[test]
    fn test_compute_future_partitions_excluded() {
        let all_keys = make_daily_keys(&["2026-03-24", "2026-03-25", "2027-12-31"]);
        let now = to_wall("2026-03-25 12:00");
        let result = compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now, None);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&spk("2026-03-25")));
        assert!(!result.contains(&spk("2027-12-31")));
    }

    #[test]
    fn test_compute_empty_keys() {
        let all_keys: HashSet<PartitionKey> = HashSet::new();
        let now = to_wall("2026-03-25 12:00");
        let result = compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now, None);
        assert!(result.is_empty());
    }

    #[test]
    fn test_compute_hourly_partitions() {
        let all_keys = make_daily_keys(&[
            "2026-03-25T08:00",
            "2026-03-25T09:00",
            "2026-03-25T10:00",
            "2026-03-25T11:00",
            "2026-03-25T12:00",
        ]);
        let now = to_wall("2026-03-25 12:30");
        let lookback_secs = 3.0 * 3600.0;
        let result =
            compute_latest_time_window_keys(&all_keys, "%Y-%m-%dT%H:%M", now, Some(lookback_secs));
        // cutoff = latest start (12:00) - 3h = 09:00, inclusive.
        assert_eq!(result.len(), 4);
        assert!(result.contains(&spk("2026-03-25T09:00")));
        assert!(result.contains(&spk("2026-03-25T10:00")));
        assert!(result.contains(&spk("2026-03-25T11:00")));
        assert!(result.contains(&spk("2026-03-25T12:00")));
    }

    #[test]
    fn test_classify_unpartitioned_only() {
        let mut pass = empty_pass();
        let to_mat = vec![
            ToMaterialize {
                asset_key: "a".into(),
                selection: None,
            },
            ToMaterialize {
                asset_key: "b".into(),
                selection: None,
            },
        ];
        let plan = pass.classify_materializations(to_mat);
        assert_eq!(plan.unpartitioned, vec!["a".to_string(), "b".to_string()]);
        assert!(plan.single_partition_groups.is_empty());
        assert!(plan.multi_partition_backfills.is_empty());
        assert!(pass.cache.in_progress_assets.contains_key("a"));
        assert!(pass.cache.in_progress_assets.contains_key("b"));
    }

    #[test]
    fn test_classify_single_partition_groups_share_run() {
        let mut pass = empty_pass();
        let pk = spk("2026-03-26");
        let to_mat = vec![
            ToMaterialize {
                asset_key: "a".into(),
                selection: Some(PartitionSelection::Keys([pk.clone()].into_iter().collect())),
            },
            ToMaterialize {
                asset_key: "b".into(),
                selection: Some(PartitionSelection::Keys([pk.clone()].into_iter().collect())),
            },
        ];
        let plan = pass.classify_materializations(to_mat);
        assert!(plan.unpartitioned.is_empty());
        assert_eq!(plan.single_partition_groups.len(), 1);
        let assets = plan.single_partition_groups.get(&pk).unwrap();
        assert_eq!(assets.len(), 2);
        assert!(assets.contains(&"a".to_string()));
        assert!(assets.contains(&"b".to_string()));
        assert!(plan.multi_partition_backfills.is_empty());
    }

    #[test]
    fn test_classify_multi_partition_becomes_backfill() {
        let mut pass = empty_pass();
        let keys: HashSet<PartitionKey> =
            [spk("2026-03-25"), spk("2026-03-26")].into_iter().collect();
        let to_mat = vec![ToMaterialize {
            asset_key: "a".into(),
            selection: Some(PartitionSelection::Keys(keys)),
        }];
        let plan = pass.classify_materializations(to_mat);
        assert!(plan.unpartitioned.is_empty());
        assert!(plan.single_partition_groups.is_empty());
        assert_eq!(plan.multi_partition_backfills.len(), 1);
        let (asset, pks) = &plan.multi_partition_backfills[0];
        assert_eq!(asset, "a");
        assert_eq!(pks.len(), 2);
    }

    fn empty_pass() -> ConditionPass {
        ConditionPass::new(
            AssetConditionCache::new("test_cl".into()),
            ConditionEvalState::default(),
            Vec::new(),
            HashMap::new(),
        )
    }
}
