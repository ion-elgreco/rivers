//! Per-tick orchestration for the condition evaluation engine.
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

/// Info about an asset with an automation condition, extracted at daemon start.
pub struct AssetConditionInfo {
    pub asset_key: String,
    pub condition: ConditionNode,
    /// Partition info for this asset. `None` if unpartitioned.
    pub partition_info: Option<PartitionInfo>,
    /// Backfill strategy from the `@Asset` decorator.
    pub backfill_strategy: Option<BackfillStrategy>,
}

/// Partition-level info for a partitioned asset, extracted at daemon start.
pub struct PartitionInfo {
    /// All valid partition keys for this asset.
    pub all_keys: HashSet<PartitionKey>,
    /// Partition mappings for upstream deps. Key = `(this_asset, upstream_asset)`.
    pub mappings: HashMap<(String, String), PartitionMappingKind>,
    /// For time-windowed partitions: the format string used to parse keys.
    pub time_window_fmt: Option<String>,
    /// How `all_keys` evolves after extraction.
    pub universe: PartitionUniverse,
}

/// How an asset's partition universe evolves after extraction.
#[derive(Clone, Debug)]
pub enum PartitionUniverse {
    /// Fixed key set (Static definitions).
    Frozen,
    /// Window starts enter the universe as wall-clock time passes.
    TimeWindow {
        grid: TimeGrid,
        enumerated_to: NaiveDateTime,
    },
    /// Storage-managed: the key set mirrors the `dynamic_partitions` namespace.
    Dynamic { namespace: String },
    /// Cartesian product over dimensions; recomputed when any dimension's key list changes.
    Multi {
        dims: Vec<(String, DimensionUniverse)>,
    },
}

/// One dimension of a Multi universe: its current key list plus how it evolves.
#[derive(Clone, Debug)]
pub struct DimensionUniverse {
    /// Dim values in definition/seed order.
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

/// Advance one universe, mutating `all_keys` in place. Returns whether the key set changed.
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
                                for k in new_keys {
                                    changed |= du.keys.insert(k);
                                }
                                *enumerated_to = bound;
                            }
                            Err(e) => {
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

/// One row of the per-asset evaluation result list.
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
#[derive(Clone)]
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

/// How an asset's latest time window is resolved: its key format, plus the
/// grid when the universe is grid-enumerated (enabling O(window) derivation
/// instead of parsing and sorting every key).
pub struct TimeWindowSource {
    pub fmt: String,
    pub grid: Option<crate::timegrid::TimeGrid>,
}

/// Lazily computes per-(asset, lookback) latest-time-window key sets during
/// one evaluation tick. `sources` spans every time-window-partitioned asset
/// the pass knows about — conditioned assets AND upstream deps — so dep
/// pivots resolve against the dep's own window instead of selecting
/// everything.
pub struct TimeWindowResolver<'a> {
    sources: &'a HashMap<String, TimeWindowSource>,
    now_local: NaiveDateTime,
    #[allow(clippy::type_complexity)]
    memo: std::cell::RefCell<HashMap<(String, u64), std::sync::Arc<HashSet<PartitionKey>>>>,
}

impl<'a> TimeWindowResolver<'a> {
    pub fn new(sources: &'a HashMap<String, TimeWindowSource>, now_local: NaiveDateTime) -> Self {
        Self {
            sources,
            now_local,
            memo: std::cell::RefCell::new(HashMap::new()),
        }
    }

    /// Keys of `asset`'s latest time window (widened by `lookback_delta`);
    /// `None` when the asset is not time-window partitioned.
    pub fn keys_for(
        &self,
        asset: &str,
        all_keys: &HashSet<PartitionKey>,
        lookback_delta: Option<f64>,
    ) -> Option<std::sync::Arc<HashSet<PartitionKey>>> {
        let source = self.sources.get(asset)?;
        let memo_key = (
            asset.to_string(),
            lookback_delta.map(f64::to_bits).unwrap_or(u64::MAX),
        );
        if let Some(hit) = self.memo.borrow().get(&memo_key) {
            return Some(std::sync::Arc::clone(hit));
        }
        let keys = source
            .grid
            .as_ref()
            .and_then(|grid| {
                derive_window_keys_from_grid(grid, all_keys, self.now_local, lookback_delta)
            })
            .unwrap_or_else(|| {
                compute_latest_time_window_keys(
                    all_keys,
                    &source.fmt,
                    self.now_local,
                    lookback_delta,
                )
            });
        let keys = std::sync::Arc::new(keys);
        self.memo
            .borrow_mut()
            .insert(memo_key, std::sync::Arc::clone(&keys));
        Some(keys)
    }
}

/// Derive the latest-window key set in O(window) from the grid instead of
/// parsing and sorting the whole universe. `None` falls back to the scan.
fn derive_window_keys_from_grid(
    grid: &crate::timegrid::TimeGrid,
    all_keys: &HashSet<PartitionKey>,
    now_local: NaiveDateTime,
    lookback_delta: Option<f64>,
) -> Option<HashSet<PartitionKey>> {
    let (latest, _) = grid.nearest_keys(now_local);
    let latest = latest?;
    let latest_dt = parse_key_datetime(&latest, &grid.fmt).ok()?;
    let mut out = HashSet::new();
    let mut insert_if_known = |key: String| {
        let pk = PartitionKey::Single { keys: vec![key] };
        if all_keys.contains(&pk) {
            out.insert(pk);
        }
    };
    match lookback_delta {
        None => insert_if_known(latest),
        Some(delta_secs) => {
            let cutoff = latest_dt
                .checked_sub_signed(chrono::Duration::nanoseconds(
                    (delta_secs * 1_000_000_000.0) as i64,
                ))?;
            for key in grid.keys_in_range(cutoff, latest_dt).ok()? {
                // keys_in_range brackets the window straddling `cutoff`; the
                // scan keeps only keys whose window START is at/after it.
                let dt = parse_key_datetime(&key, &grid.fmt).ok()?;
                if dt >= cutoff {
                    insert_if_known(key);
                }
            }
            insert_if_known(latest);
        }
    }
    Some(out)
}

/// Compute partition keys that fall within the latest time window.
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

/// Owns the long-lived state of the condition evaluation loop.
pub struct ConditionPass {
    pub cache: AssetConditionCache,
    pub eval_state: ConditionEvalState,
    pub conditions: Vec<AssetConditionInfo>,
    /// Index into `conditions` keyed by asset name; built once at construction.
    pub conditions_by_key: HashMap<String, usize>,
    /// Set of asset keys with active conditions, used by `update_dep_baselines`.
    pub active: HashSet<String>,
    pub has_time_based: bool,
    /// Asset subset to evaluate when only time-based conditions need re-eval.
    pub time_based_eval_set: Option<HashSet<String>>,
    pub upstream_partition_keys: HashMap<String, HashSet<PartitionKey>>,
    /// How each `upstream_partition_keys` entry evolves.
    pub upstream_universes: HashMap<String, PartitionUniverse>,
    /// Time-window resolution source per time-window-partitioned asset
    /// (conditioned assets and upstream deps), for `InLatestTimeWindow`.
    pub time_window_sources: HashMap<String, TimeWindowSource>,
    /// Set when the last committed tick had to skip fired assets (failed or
    /// fully-dropped dispatch); forces the next tick to re-evaluate so their
    /// un-consumed triggers can re-fire.
    pub needs_retry: bool,
}

impl ConditionPass {
    /// Build a new pass; `conditions` must be in topological order (deps before downstreams).
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
        let mut eval_state = eval_state;
        for info in &conditions {
            eval_state.assets.entry(info.asset_key.clone()).or_default();
        }
        // Rehydrate asset-level failure floors: the cache only maintains them
        // in steady state, so a fresh cache would silently drop ExecutionFailed.
        let mut cache = cache;
        for (asset, ts) in &eval_state.failed_assets {
            cache.failed_assets.insert(asset.clone());
            cache
                .failed_asset_timestamps
                .entry(asset.clone())
                .or_insert(*ts);
        }
        let mut time_window_sources: HashMap<String, TimeWindowSource> = HashMap::new();
        for info in &conditions {
            if let Some(pi) = &info.partition_info
                && let Some(fmt) = &pi.time_window_fmt
            {
                let grid = match &pi.universe {
                    PartitionUniverse::TimeWindow { grid, .. } => Some(grid.clone()),
                    _ => None,
                };
                time_window_sources.insert(
                    info.asset_key.clone(),
                    TimeWindowSource {
                        fmt: fmt.clone(),
                        grid,
                    },
                );
            }
        }
        for (asset, universe) in &universes_map {
            if let PartitionUniverse::TimeWindow { grid, .. } = universe {
                time_window_sources
                    .entry(asset.clone())
                    .or_insert_with(|| TimeWindowSource {
                        fmt: grid.fmt.clone(),
                        grid: Some(grid.clone()),
                    });
            }
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
            time_window_sources,
            needs_retry: false,
        }
    }

    /// Dynamic namespaces any tracked universe depends on.
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

    /// Advance every tracked partition universe to `now`. Returns whether any universe changed.
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

    /// Refresh `cache` from storage. Returns whether anything changed.
    pub async fn refresh_cache<S: StorageBackend>(
        &mut self,
        storage: &S,
        now: i64,
    ) -> Result<bool> {
        self.cache.refresh(storage, now).await
    }

    /// Lazily compute the time-based eval subset once cache is initialized.
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

    /// Skip this tick when nothing changed, we're past the initial tick, and no time-based conditions need re-eval.
    pub fn should_skip(&self, has_changes: bool) -> bool {
        !self.needs_retry
            && !has_changes
            && self.cache.initialized
            && !self.eval_state.is_initial
            && !self.has_time_based
    }

    /// One full tick: evaluate conditions, apply state mutations, and return the materialization plan.
    ///
    /// Equivalent to [`plan_tick`](Self::plan_tick) followed by an
    /// all-successful [`commit_tick`](Self::commit_tick); dispatching callers
    /// use the split so a failed dispatch never consumes eval-state latches.
    pub fn run(&mut self, now: i64, selective: bool) -> PassOutput {
        let output = self.plan_tick(now, selective);
        self.commit_tick(&output, &HashSet::new(), now);
        output
    }

    /// Evaluate conditions and build the dispatch plan without advancing any
    /// eval-state latches.
    pub fn plan_tick(&mut self, now: i64, selective: bool) -> PassOutput {
        let results = self.evaluate(now, selective);
        // Consume the very-first-evaluation flag only AFTER the evaluation
        // that is supposed to see it (per-asset flags persist until their
        // tick commits).
        if self.eval_state.is_initial {
            self.eval_state.is_initial = false;
        }
        let to_materialize: Vec<ToMaterialize> = results
            .iter()
            .filter(|row| row.result.fired)
            .map(|row| ToMaterialize {
                asset_key: self.conditions[row.info_idx].asset_key.clone(),
                selection: row.result.selection.clone(),
            })
            .collect();
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
        let plan = self.classify_materializations(to_materialize);
        PassOutput { results, plan }
    }

    /// Advance eval-state latches for a planned tick. Fired assets named in
    /// `dispatch_failed` — and fired assets whose selection classified to
    /// nothing dispatchable — keep their pre-tick state so their trigger
    /// re-fires on the retry evaluation this schedules.
    pub fn commit_tick(
        &mut self,
        output: &PassOutput,
        dispatch_failed: &HashSet<String>,
        now: i64,
    ) {
        let planned: HashSet<&str> = output
            .plan
            .unpartitioned
            .iter()
            .map(String::as_str)
            .chain(
                output
                    .plan
                    .single_partition_groups
                    .values()
                    .flatten()
                    .map(String::as_str),
            )
            .chain(
                output
                    .plan
                    .multi_partition_backfills
                    .iter()
                    .map(|(asset, _)| asset.as_str()),
            )
            .collect();
        let mut skip: HashSet<String> = dispatch_failed.clone();
        for row in &output.results {
            if row.result.fired {
                let key = &self.conditions[row.info_idx].asset_key;
                if !planned.contains(key.as_str()) {
                    skip.insert(key.clone());
                }
            }
        }

        let mut baseline_roots: Vec<String> = Vec::new();
        for row in &output.results {
            let info = &self.conditions[row.info_idx];
            if skip.contains(&info.asset_key) {
                continue;
            }
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

            if row.result.fired || was_initial {
                baseline_roots.push(info.asset_key.clone());
            }
        }

        if !baseline_roots.is_empty() {
            update_dep_baselines(
                &mut self.eval_state.assets,
                &baseline_roots,
                &self.cache.upstream_deps,
                &self.active,
                &self.cache.partition_status,
                &self.cache.records,
            );
        }

        // Handled marks and the handled cursor advance only for assets whose
        // dispatch actually went out.
        for (pk, assets) in &output.plan.single_partition_groups {
            for asset in assets {
                if !skip.contains(asset) {
                    self.mark_partitions_handled(asset, std::slice::from_ref(pk));
                }
            }
        }
        for (asset, keys) in &output.plan.multi_partition_backfills {
            if !skip.contains(asset) {
                self.mark_partitions_handled(asset, keys);
            }
        }
        self.stamp_dispatched_handled(&output.plan, &skip, now);
        self.eval_state.failed_assets = self.cache.failed_asset_timestamps.clone();
        self.needs_retry = !skip.is_empty();
    }

    fn evaluate(&self, now: i64, selective: bool) -> Vec<EvalResultRow> {
        let now_local = chrono::Local.timestamp_nanos(now).naive_local();
        let time_windows = TimeWindowResolver::new(&self.time_window_sources, now_local);
        let in_progress_keys: HashSet<String> =
            self.cache.in_progress_assets.keys().cloned().collect();

        let failed_keys: HashSet<String> = self
            .cache
            .failed_assets
            .iter()
            .cloned()
            .chain(
                self.cache
                    .partition_status
                    .iter()
                    .filter(|(_, status)| !status.failed.is_empty())
                    .map(|(key, _)| key.clone()),
            )
            .collect();

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

            let pending = self.cache.in_progress_partition_keys(&info.asset_key);
            let partition_status = self.cache.partition_status.get(&info.asset_key);
            let merged_in_progress: Option<HashSet<PartitionKey>> = if pending.is_empty() {
                None
            } else {
                partition_status.map(|status| {
                    let mut s = status.in_progress.clone();
                    s.extend(pending);
                    s
                })
            };
            let pctx = info.partition_info.as_ref().and_then(|pi| {
                partition_status.map(|status| PartitionEvalContext {
                    all_keys: &pi.all_keys,
                    materialized: &status.materialized,
                    in_progress: merged_in_progress.as_ref().unwrap_or(&status.in_progress),
                    failed: &status.failed,
                    timestamps: &status.timestamps,
                    resolver: PartitionResolver::new(&pi.mappings, &self.upstream_partition_keys),
                    time_windows: Some(&time_windows),
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
                    failed_assets: &failed_keys,
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

    /// Record dispatched partition keys in the asset's `handled` set.
    fn mark_partitions_handled(&mut self, asset_key: &str, keys: &[PartitionKey]) {
        if let Some(prev) = self.eval_state.assets.get_mut(asset_key) {
            prev.partition_state
                .get_or_insert_with(PartitionState::default)
                .handled
                .extend(keys.iter().cloned());
        }
    }

    /// Split the materialization list into the three dispatch shapes.
    fn classify_materializations(
        &mut self,
        to_materialize: Vec<ToMaterialize>,
    ) -> MaterializationPlan {
        let mut unpartitioned: Vec<String> = Vec::new();
        let mut partitioned_mats: Vec<(String, Vec<PartitionKey>)> = Vec::new();

        for tm in to_materialize {
            match tm.selection {
                Some(PartitionSelection::Keys(keys)) if !keys.is_empty() => {
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
                            partitioned_mats.push((tm.asset_key, all_keys));
                        }
                        Some(_) => {}
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

    /// Advance the asset-level handled cursor only for assets the plan actually dispatched.
    fn stamp_dispatched_handled(
        &mut self,
        plan: &MaterializationPlan,
        skip: &HashSet<String>,
        now: i64,
    ) {
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
            if skip.contains(asset_key) {
                continue;
            }
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

    /// Build a one-partitioned-asset pass and run a full tick with the given fired selection.
    fn handled_after_fired_selection(selection: PartitionSelection) -> Option<i64> {
        let mut cache = AssetConditionCache::default();
        cache
            .records
            .insert("down".to_string(), test_record("down"));
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
        let to_mat = vec![ToMaterialize {
            asset_key: "down".to_string(),
            selection: row.result.selection.clone(),
        }];
        let plan = pass.classify_materializations(to_mat);
        let output = PassOutput {
            results: vec![row],
            plan,
        };
        pass.commit_tick(&output, &HashSet::new(), 5000);
        pass.eval_state.assets["down"].last_handled_timestamp
    }

    /// The O(window) grid derivation must produce exactly what the full-scan
    /// fallback produces for grid-enumerated universes.
    #[test]
    fn grid_derivation_matches_the_scan() {
        let keys = make_daily_keys(&[
            "2020-01-01",
            "2020-01-02",
            "2020-01-03",
            "2020-01-04",
            "2020-01-05",
        ]);
        let grid = crate::timegrid::TimeGrid {
            cron_schedule: None,
            interval_seconds: Some(86_400.0),
            start: to_wall("2020-01-01 00:00"),
            end: Some(to_wall("2020-01-06 00:00")),
            fmt: "%Y-%m-%d".to_string(),
        };
        let now = to_wall("2020-01-05 12:00");
        for lookback in [None, Some(86_400.0), Some(2.5 * 86_400.0)] {
            let scanned = compute_latest_time_window_keys(&keys, "%Y-%m-%d", now, lookback);
            let sources = HashMap::from([(
                "a".to_string(),
                TimeWindowSource {
                    fmt: "%Y-%m-%d".to_string(),
                    grid: Some(grid.clone()),
                },
            )]);
            let tw = TimeWindowResolver::new(&sources, now);
            let derived = tw.keys_for("a", &keys, lookback).unwrap();
            assert_eq!(*derived, scanned, "lookback {lookback:?}");
        }
    }

    /// Each InLatestTimeWindow node must select against its OWN lookback — not
    /// a single set computed from the first node found in the tree.
    #[test]
    fn latest_time_window_respects_each_nodes_lookback() {
        let mut cache = AssetConditionCache::default();
        let mut rec = test_record("a");
        rec.last_timestamp = Some(100);
        cache.records.insert("a".to_string(), rec);
        cache.partition_status.insert(
            "a".to_string(),
            crate::condition::cache::PartitionStatusEntry::default(),
        );
        let keys = make_daily_keys(&[
            "2020-01-01",
            "2020-01-02",
            "2020-01-03",
            "2020-01-04",
            "2020-01-05",
        ]);

        let tree = ConditionNode::Or(vec![
            ConditionNode::And(vec![
                ConditionNode::InProgress,
                ConditionNode::InLatestTimeWindow {
                    lookback_delta: None,
                },
            ]),
            ConditionNode::And(vec![
                ConditionNode::Missing,
                ConditionNode::InLatestTimeWindow {
                    lookback_delta: Some(2.0 * 86_400.0),
                },
            ]),
        ]);
        let pass = ConditionPass::new(
            cache,
            ConditionEvalState::default(),
            vec![AssetConditionInfo {
                asset_key: "a".to_string(),
                condition: tree,
                partition_info: Some(PartitionInfo {
                    all_keys: keys,
                    mappings: HashMap::new(),
                    time_window_fmt: Some("%Y-%m-%d".to_string()),
                    universe: PartitionUniverse::Frozen,
                }),
                backfill_strategy: None,
            }],
            HashMap::new(),
        );
        let rows = pass.evaluate(1_700_000_000_000_000_000, false);
        // Missing = all 5 keys; the 2-day lookback selects the latest 3.
        assert_eq!(
            rows[0].result.selection.clone().unwrap(),
            PartitionSelection::Keys(make_daily_keys(&[
                "2020-01-03",
                "2020-01-04",
                "2020-01-05"
            ])),
            "the 2-day-lookback node must select its own window, not the first node's"
        );
    }

    /// in_latest_time_window() inside a dep pivot must filter against the
    /// DEP's latest window, not silently select every dep partition.
    #[test]
    fn dep_pivot_latest_time_window_filters_dep_keys() {
        let day_keys = make_daily_keys(&["2020-01-01", "2020-01-02", "2020-01-03"]);
        let grid = crate::timegrid::TimeGrid {
            cron_schedule: None,
            interval_seconds: Some(86_400.0),
            start: to_wall("2020-01-01 00:00"),
            end: Some(to_wall("2020-01-04 00:00")),
            fmt: "%Y-%m-%d".to_string(),
        };

        let build_pass = |a_ts: &[(&str, i64)]| {
            let mut cache = AssetConditionCache::default();
            let mut rec_b = test_record("b");
            rec_b.last_timestamp = Some(100);
            cache.records.insert("b".to_string(), rec_b);
            let mut rec_a = test_record("a");
            rec_a.last_timestamp = Some(200);
            cache.records.insert("a".to_string(), rec_a);
            cache
                .upstream_deps
                .insert("b".to_string(), vec!["a".to_string()]);
            cache.partition_status.insert(
                "b".to_string(),
                crate::condition::cache::PartitionStatusEntry {
                    materialized: day_keys.clone(),
                    timestamps: day_keys.iter().map(|k| (k.clone(), 100)).collect(),
                    ..Default::default()
                },
            );
            cache.partition_status.insert(
                "a".to_string(),
                crate::condition::cache::PartitionStatusEntry {
                    materialized: day_keys.clone(),
                    timestamps: a_ts.iter().map(|(k, ts)| (spk(k), *ts)).collect(),
                    ..Default::default()
                },
            );
            let tree = ConditionNode::any_deps_match(
                ConditionNode::NewlyUpdated
                    & ConditionNode::InLatestTimeWindow {
                        lookback_delta: None,
                    },
            );
            ConditionPass::new(
                cache,
                ConditionEvalState::default(),
                vec![AssetConditionInfo {
                    asset_key: "b".to_string(),
                    condition: tree,
                    partition_info: Some(PartitionInfo {
                        all_keys: day_keys.clone(),
                        mappings: HashMap::new(),
                        time_window_fmt: Some("%Y-%m-%d".to_string()),
                        universe: PartitionUniverse::Frozen,
                    }),
                    backfill_strategy: None,
                }],
                HashMap::from([(
                    "a".to_string(),
                    (
                        day_keys.clone(),
                        PartitionUniverse::TimeWindow {
                            grid: grid.clone(),
                            enumerated_to: to_wall("2020-01-04 00:00"),
                        },
                    ),
                )]),
            )
        };

        // Only a's OLDEST key was re-materialized (200 > b's floor of 100).
        let pass = build_pass(&[("2020-01-01", 200), ("2020-01-02", 100), ("2020-01-03", 100)]);
        let rows = pass.evaluate(1_700_000_000_000_000_000, false);
        assert!(
            !rows[0].result.fired,
            "an update to a NON-latest dep partition must not pass in_latest_time_window; got {:?}",
            rows[0].result.selection
        );

        // Positive control: the dep's LATEST key updating passes the filter.
        let pass = build_pass(&[("2020-01-01", 100), ("2020-01-02", 100), ("2020-01-03", 200)]);
        let rows = pass.evaluate(1_700_000_000_000_000_000, false);
        assert!(rows[0].result.fired, "latest-key update must fire");
        assert_eq!(
            rows[0].result.selection.clone().unwrap(),
            PartitionSelection::Keys(make_daily_keys(&["2020-01-03"]))
        );
    }

    /// One asset's fire must not consume dep-change evidence a gated sibling
    /// hasn't acted on: dep baselines are per (downstream, dep), not global.
    #[test]
    fn dep_baseline_survives_unrelated_asset_fire() {
        let mut cache = AssetConditionCache::default();
        let mut c = test_record("c");
        c.last_timestamp = Some(100);
        cache.records.insert("c".to_string(), c);
        let mut y = test_record("y");
        y.last_timestamp = Some(50);
        y.last_data_version = Some("v1".to_string());
        cache.records.insert("y".to_string(), y.clone());
        // d is missing → its condition fires every tick.
        cache.records.insert("d".to_string(), test_record("d"));
        cache
            .upstream_deps
            .insert("c".to_string(), vec!["y".to_string()]);

        let mut pass = ConditionPass::new(
            cache,
            ConditionEvalState::default(),
            vec![
                AssetConditionInfo {
                    asset_key: "c".to_string(),
                    condition: ConditionNode::any_deps_match(ConditionNode::DataVersionChanged)
                        & !ConditionNode::InProgress,
                    partition_info: None,
                    backfill_strategy: None,
                },
                AssetConditionInfo {
                    asset_key: "d".to_string(),
                    condition: ConditionNode::Missing,
                    partition_info: None,
                    backfill_strategy: None,
                },
            ],
            HashMap::new(),
        );

        let fired = |out: &PassOutput, pass: &ConditionPass, key: &str| {
            out.results.iter().any(|row| {
                pass.conditions[row.info_idx].asset_key == key && row.result.fired
            })
        };

        // Tick 1: y's version is first-seen → c fires and consumes it.
        let out = pass.run(1000, false);
        assert!(fired(&out, &pass, "c"), "tick 1: first-seen version fires c");

        // Tick 2: y unchanged → no re-fire.
        let out = pass.run(2000, false);
        assert!(!fired(&out, &pass, "c"), "tick 2: stable version must not re-fire");

        // y's version changes while c is gated; unrelated d keeps firing.
        y.last_data_version = Some("v2".to_string());
        y.last_timestamp = Some(60);
        pass.cache.records.insert("y".to_string(), y);
        pass.cache
            .in_progress_assets
            .entry("c".to_string())
            .or_default();
        let out = pass.run(3000, false);
        assert!(!fired(&out, &pass, "c"), "tick 3: c is gated");
        assert!(fired(&out, &pass, "d"), "tick 3: unrelated d fires");

        // Tick 4: c ungated — the pending version change must still be visible.
        pass.cache.in_progress_assets.remove("c");
        let out = pass.run(4000, false);
        assert!(
            fired(&out, &pass, "c"),
            "tick 4: an unrelated asset's fire must not consume c's dep-change trigger"
        );

        // Tick 5: c acted on it → consumed.
        let out = pass.run(5000, false);
        assert!(!fired(&out, &pass, "c"), "tick 5: c consumed the change");
    }

    #[test]
    fn unpartitioned_watcher_sees_partition_failure_of_dep() {
        let mut cache = AssetConditionCache::default();
        let mut down = test_record("down");
        down.last_timestamp = Some(100);
        cache.records.insert("down".to_string(), down);
        let mut up = test_record("up");
        up.last_timestamp = Some(100);
        cache.records.insert("up".to_string(), up);
        cache
            .upstream_deps
            .insert("down".to_string(), vec!["up".to_string()]);
        cache.partition_status.insert(
            "up".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                failed: HashSet::from([spk("2024-01-01")]),
                failed_timestamps: HashMap::from([(spk("2024-01-01"), 200i64)]),
                ..Default::default()
            },
        );

        let pass = ConditionPass::new(
            cache,
            ConditionEvalState::default(),
            vec![AssetConditionInfo {
                asset_key: "down".to_string(),
                condition: ConditionNode::AnyDepsMatch {
                    condition: Box::new(ConditionNode::ExecutionFailed),
                    label: None,
                },
                partition_info: None,
                backfill_strategy: None,
            }],
            HashMap::new(),
        );
        let rows = pass.evaluate(5000, false);
        assert!(
            rows[0].result.fired,
            "an unpartitioned watcher must see a partitioned dep's failed partition"
        );
    }

    #[test]
    fn observation_and_sibling_success_do_not_unsurface_partition_failure() {
        let mut cache = AssetConditionCache::default();
        let mut down = test_record("down");
        down.last_timestamp = Some(100);
        cache.records.insert("down".to_string(), down);
        let mut up = test_record("up");
        up.last_timestamp = Some(300);
        cache.records.insert("up".to_string(), up);
        cache
            .upstream_deps
            .insert("down".to_string(), vec!["up".to_string()]);
        cache.partition_status.insert(
            "up".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                failed: HashSet::from([spk("p")]),
                failed_timestamps: HashMap::from([(spk("p"), 150i64)]),
                timestamps: HashMap::from([(spk("q"), 400i64)]),
                ..Default::default()
            },
        );

        let pass = ConditionPass::new(
            cache,
            ConditionEvalState::default(),
            vec![AssetConditionInfo {
                asset_key: "down".to_string(),
                condition: ConditionNode::AnyDepsMatch {
                    condition: Box::new(ConditionNode::ExecutionFailed),
                    label: None,
                },
                partition_info: None,
                backfill_strategy: None,
            }],
            HashMap::new(),
        );
        let rows = pass.evaluate(5000, false);
        assert!(
            rows[0].result.fired,
            "a still-failed partition must stay surfaced despite an observation \
             or a sibling partition's success bumping the asset-level timestamp"
        );
    }

    #[test]
    fn recovered_dep_partition_failure_does_not_poison_unpartitioned_watcher() {
        let mut cache = AssetConditionCache::default();
        let mut down = test_record("down");
        down.last_timestamp = Some(100);
        cache.records.insert("down".to_string(), down);
        let mut up = test_record("up");
        up.last_timestamp = Some(300);
        cache.records.insert("up".to_string(), up);
        cache
            .upstream_deps
            .insert("down".to_string(), vec!["up".to_string()]);
        cache.partition_status.insert(
            "up".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                failed: HashSet::new(),
                timestamps: HashMap::from([(spk("2024-01-01"), 300i64)]),
                ..Default::default()
            },
        );

        let pass = ConditionPass::new(
            cache,
            ConditionEvalState::default(),
            vec![AssetConditionInfo {
                asset_key: "down".to_string(),
                condition: ConditionNode::AnyDepsMatch {
                    condition: Box::new(ConditionNode::ExecutionFailed),
                    label: None,
                },
                partition_info: None,
                backfill_strategy: None,
            }],
            HashMap::new(),
        );
        let rows = pass.evaluate(5000, false);
        assert!(
            !rows[0].result.fired,
            "a dep whose failed partition re-materialized (empty failed set) \
             must not surface to unpartitioned watchers"
        );
    }

    #[test]
    fn run_seeds_missing_eval_state_instead_of_panicking() {
        let mut cache = AssetConditionCache::default();
        cache.records.insert("a".to_string(), test_record("a"));
        let mut pass = ConditionPass::new(
            cache,
            ConditionEvalState::default(),
            vec![AssetConditionInfo {
                asset_key: "a".to_string(),
                condition: ConditionNode::eager(),
                partition_info: None,
                backfill_strategy: None,
            }],
            HashMap::new(),
        );
        let out = pass.run(1000, false);
        assert_eq!(
            out.results.len(),
            1,
            "conditioned asset must evaluate without panicking on a missing eval_state entry"
        );
    }

    #[test]
    fn handled_cursor_skips_fully_dropped_selection() {
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
        let shrunk: HashMap<String, HashSet<String>> = [(
            "colors".to_string(),
            ["blue".to_string()].into_iter().collect(),
        )]
        .into_iter()
        .collect();
        refresh_universe(&mut universe, &mut all_keys, now, &shrunk);
        assert_eq!(all_keys, [spk("blue")].into_iter().collect());
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
    fn update_state_resets_handled_each_tick() {
        let mut state = crate::condition::state::AssetConditionState {
            partition_state: Some(crate::condition::partition::PartitionState {
                handled: [spk("2024-01-01")].into_iter().collect(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let timestamps: HashMap<PartitionKey, i64> = HashMap::new();
        let ctx = StateUpdateContext {
            target_record_timestamp: None,
            target_data_version: None,
            now: 2,
            is_initial: false,
            partition_timestamps: Some(&timestamps),
        };
        let result = EvalResult {
            fired: false,
            selection: Some(PartitionSelection::Empty),
            sub_selections: Some(HashMap::new()),
            ..Default::default()
        };
        update_condition_state(&mut state, &ctx, &result);
        let ps = state.partition_state.as_ref().expect("partition state");
        assert!(
            ps.handled.is_empty(),
            "stale handled keys from a prior tick must be reset, got {:?}",
            ps.handled
        );
    }

    #[test]
    fn commit_marks_only_surviving_keys_handled() {
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
            selection: Some(PartitionSelection::Keys(
                [spk("2024-01-02"), spk("2024-08-16")].into_iter().collect(),
            )),
        }]);
        pass.commit_tick(
            &PassOutput {
                results: vec![],
                plan,
            },
            &HashSet::new(),
            2,
        );
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

        let plan = pass.classify_materializations(vec![ToMaterialize {
            asset_key: "down".to_string(),
            selection: Some(PartitionSelection::Keys(make_daily_keys(&["2024-01-01"]))),
        }]);
        assert_eq!(plan.single_partition_groups.len(), 1);
        assert!(pass.cache.in_progress_assets.contains_key("down"));
    }

    #[test]
    fn eager_does_not_fire_for_a_partition_an_active_backfill_covers() {
        let keys = |ks: &[&str]| ks.iter().map(|k| spk(k)).collect::<HashSet<_>>();

        let mut cache = AssetConditionCache::default();
        cache.records.insert("src".to_string(), test_record("src"));
        cache.records.insert("dst".to_string(), test_record("dst"));
        cache
            .upstream_deps
            .insert("dst".to_string(), vec!["src".to_string()]);

        cache.partition_status.insert(
            "src".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                materialized: keys(&["a", "b", "c"]),
                timestamps: HashMap::from([(spk("a"), 100i64), (spk("b"), 100), (spk("c"), 100)]),
                ..Default::default()
            },
        );
        cache.partition_status.insert(
            "dst".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                materialized: keys(&["a", "b"]),
                timestamps: HashMap::from([(spk("a"), 200i64), (spk("b"), 200)]),
                ..Default::default()
            },
        );

        cache
            .backfill
            .assets
            .insert("dst".to_string(), vec!["bf1".to_string()]);
        cache
            .backfill
            .partition_keys
            .insert("bf1".to_string(), vec![spk("a"), spk("b"), spk("c")]);
        assert!(
            cache.in_progress_assets.is_empty(),
            "precondition: the gap tick has no tracked sub-run"
        );

        let mut pass = ConditionPass::new(
            cache,
            ConditionEvalState::default(),
            vec![AssetConditionInfo {
                asset_key: "dst".to_string(),
                condition: ConditionNode::eager(),
                partition_info: Some(PartitionInfo {
                    all_keys: keys(&["a", "b", "c", "d", "e"]),
                    mappings: HashMap::from([(
                        ("dst".to_string(), "src".to_string()),
                        PartitionMappingKind::Identity,
                    )]),
                    time_window_fmt: None,
                    universe: PartitionUniverse::Frozen,
                }),
                backfill_strategy: None,
            }],
            HashMap::from([(
                "src".to_string(),
                (keys(&["a", "b", "c", "d", "e"]), PartitionUniverse::Frozen),
            )]),
        );
        pass.eval_state.assets.insert(
            "dst".to_string(),
            crate::condition::state::AssetConditionState::default(),
        );

        let out = pass.run(1000, false);

        let sel = &out.results[0].result.selection;
        let selects_c = matches!(sel, Some(PartitionSelection::Keys(ks)) if ks.contains(&spk("c")));
        assert!(
            !selects_c,
            "eager must not select a partition an active backfill already covers; got {sel:?}"
        );

        assert!(
            out.plan.is_empty(),
            "nothing may dispatch for a backfill-covered partition; plan dispatched \
             unpartitioned={:?} single={:?} backfills={:?}",
            out.plan.unpartitioned,
            out.plan.single_partition_groups,
            out.plan.multi_partition_backfills,
        );
    }

    #[test]
    fn eager_does_not_redispatch_a_just_dispatched_partition_before_storage_catches_up() {
        let keys = |ks: &[&str]| ks.iter().map(|k| spk(k)).collect::<HashSet<_>>();

        let mut cache = AssetConditionCache::default();
        cache.records.insert("src".to_string(), test_record("src"));
        cache.records.insert("dst".to_string(), test_record("dst"));
        cache
            .upstream_deps
            .insert("dst".to_string(), vec!["src".to_string()]);
        cache.partition_status.insert(
            "src".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                materialized: keys(&["a", "b", "c"]),
                timestamps: HashMap::from([(spk("a"), 100i64), (spk("b"), 100), (spk("c"), 100)]),
                ..Default::default()
            },
        );
        cache.partition_status.insert(
            "dst".to_string(),
            crate::condition::cache::PartitionStatusEntry {
                materialized: keys(&["a", "b"]),
                timestamps: HashMap::from([(spk("a"), 200i64), (spk("b"), 200)]),
                ..Default::default()
            },
        );

        cache.register_dispatched_run("dst".into(), "run_c".into(), 1000, Some(spk("c")));
        assert!(
            cache.partition_status["dst"].in_progress.is_empty(),
            "precondition: storage's get_in_progress_partitions hasn't caught up"
        );

        let mut pass = ConditionPass::new(
            cache,
            ConditionEvalState::default(),
            vec![AssetConditionInfo {
                asset_key: "dst".to_string(),
                condition: ConditionNode::eager(),
                partition_info: Some(PartitionInfo {
                    all_keys: keys(&["a", "b", "c", "d", "e"]),
                    mappings: HashMap::from([(
                        ("dst".to_string(), "src".to_string()),
                        PartitionMappingKind::Identity,
                    )]),
                    time_window_fmt: None,
                    universe: PartitionUniverse::Frozen,
                }),
                backfill_strategy: None,
            }],
            HashMap::from([(
                "src".to_string(),
                (keys(&["a", "b", "c", "d", "e"]), PartitionUniverse::Frozen),
            )]),
        );
        pass.eval_state.assets.insert(
            "dst".to_string(),
            crate::condition::state::AssetConditionState::default(),
        );

        let out = pass.run(1000, false);

        let sel = &out.results[0].result.selection;
        let selects_c = matches!(sel, Some(PartitionSelection::Keys(ks)) if ks.contains(&spk("c")));
        assert!(
            !selects_c,
            "eager must not re-dispatch a partition whose run was just dispatched; got {sel:?}"
        );
        assert!(
            out.plan.is_empty(),
            "nothing may dispatch; got single={:?}",
            out.plan.single_partition_groups
        );
    }

    #[test]
    fn commit_marks_all_selection_keys_handled() {
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
        let output = PassOutput {
            results: vec![],
            plan,
        };
        pass.commit_tick(&output, &HashSet::new(), 2);
        let mut backfills = output.plan.multi_partition_backfills;
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
        let all_keys = make_daily_keys(&["2026-03-24", "2026-03-25", "2026-03-26"]);
        let now = to_wall("2026-03-26 12:00");
        let result = compute_latest_time_window_keys(&all_keys, "%Y-%m-%d", now, Some(3600.0));
        assert_eq!(result.len(), 1, "1h lookback at 12:00 must not go empty");
        assert!(result.contains(&spk("2026-03-26")));
    }

    #[test]
    fn lookback_of_one_period_selects_previous_window_all_day() {
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
