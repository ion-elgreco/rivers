//! Per-tick orchestration for the condition evaluation engine.
use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::condition::cache::AssetConditionCache;
use crate::condition::eval::evaluate_with_tree;
use crate::condition::node::ConditionNode;
use crate::condition::partition::{
    PartitionEvalContext, PartitionMappingKind, PartitionResolver, PartitionSelection,
    PartitionState,
};
use crate::condition::state::{
    AssetConditionState, CacheSnapshot, ConditionEvalState, DepBaseline, EvalContext,
    EvalNodeResult, EvalResult, PendingDispatch, RunTagSnapshot, StateUpdateContext,
    update_condition_state, update_dep_baselines,
};
use crate::storage::{BackfillStrategy, PartitionKey, StorageBackend};
use chrono::{NaiveDateTime, TimeZone};

mod universe;
mod window;

pub use universe::*;
pub use window::*;

#[cfg(test)]
mod tests;

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
        let mut cache = cache;
        // The pass owns "which assets are partitioned": the conditioned
        // partitioned assets plus every mapping upstream.
        let mut partitioned: Vec<String> = conditions
            .iter()
            .filter_map(|c| c.partition_info.as_ref().map(|_| c.asset_key.clone()))
            .collect();
        for info in &conditions {
            if let Some(pi) = &info.partition_info {
                for (_, upstream) in pi.mappings.keys() {
                    if !partitioned.contains(upstream) {
                        partitioned.push(upstream.clone());
                    }
                }
            }
        }
        cache.set_partitioned_assets(partitioned);
        cache.set_needs_tick_tags(conditions.iter().map(|c| &c.condition));
        // Rehydrate asset-level failure floors from persisted eval-state;
        // initial_load independently derives them from run history, so this
        // is a fast path, not the only source.
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
    ///
    /// Returns whether the state materially advanced (latches consumed via a
    /// fire or initial baseline seeding, or failure floors moved) — passive
    /// ticks only refresh derivable views and callers may throttle their
    /// persistence.
    pub fn commit_tick(
        &mut self,
        output: &PassOutput,
        dispatch_failed: &HashSet<String>,
        now: i64,
    ) -> bool {
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
        let floors_moved = self.eval_state.failed_assets != self.cache.failed_asset_timestamps;
        self.eval_state.failed_assets = self.cache.failed_asset_timestamps.clone();
        self.needs_retry = !skip.is_empty();
        !baseline_roots.is_empty() || floors_moved
    }

    /// Post-commit scalar states for the planned fired assets of `output`,
    /// computed without mutating the pass — persisted as dispatch intent
    /// before the tick's runs go out (see [`PendingDispatch`]).
    pub fn pending_dispatch_states(
        &self,
        output: &PassOutput,
        now: i64,
    ) -> Vec<(String, AssetConditionState)> {
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
        let mut out = Vec::new();
        for row in &output.results {
            if !row.result.fired {
                continue;
            }
            let info = &self.conditions[row.info_idx];
            if !planned.contains(info.asset_key.as_str()) {
                continue;
            }
            let Some(record) = self.cache.records.get(&info.asset_key) else {
                continue;
            };
            let Some(prev) = self.eval_state.assets.get(&info.asset_key) else {
                continue;
            };
            let mut state = prev.scalar_clone();
            let update_ctx = StateUpdateContext {
                target_record_timestamp: record.last_timestamp,
                target_data_version: record.last_data_version.as_ref(),
                now,
                is_initial: prev.is_initial,
                partition_timestamps: None,
            };
            update_condition_state(&mut state, &update_ctx, &row.result);
            if let Some(deps) = self.cache.upstream_deps.get(&info.asset_key) {
                for dep in deps {
                    if self.active.contains(dep) {
                        continue;
                    }
                    let dep_record = self.cache.records.get(dep);
                    state.dep_baselines.insert(
                        dep.clone(),
                        DepBaseline {
                            last_materialized_timestamp: dep_record.and_then(|r| r.last_timestamp),
                            last_data_version: dep_record.and_then(|r| r.last_data_version.clone()),
                        },
                    );
                }
            }
            state.last_handled_timestamp = Some(now);
            out.push((info.asset_key.clone(), state));
        }
        out
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
                    tick_materialization_tags: &self.cache.tick_materialization_tags,
                    last_run_asset_names: &self.cache.last_run_asset_names,
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

/// Recover a crash-interrupted tick at daemon start: splice the persisted
/// intent's committed scalar states into `eval_state` for every entry whose
/// dispatch demonstrably went out (its runs — or a covering backfill created
/// at/after the tick — exist in storage), persist the result, then clear the
/// intent. Entries without dispatch evidence stay un-consumed so the next
/// tick re-fires them.
pub async fn recover_pending_dispatch<S: StorageBackend>(
    eval_state: &mut ConditionEvalState,
    storage: &crate::storage::ScopedStorageHandle<S>,
) -> Result<()> {
    let scoped = storage.scoped();
    let Some(pending) = scoped.get_condition_pending_dispatch().await? else {
        return Ok(());
    };
    if pending.entries.is_empty() {
        return Ok(());
    }
    let tick_ts = pending.tick_timestamp;
    let run_ids: Vec<String> = pending
        .entries
        .iter()
        .flat_map(|e| e.run_ids.iter().cloned())
        .collect();
    let existing_runs: HashSet<String> = if run_ids.is_empty() {
        HashSet::new()
    } else {
        storage
            .backend()
            .get_runs_by_ids(&run_ids, None)
            .await?
            .into_iter()
            .map(|r| r.run_id)
            .collect()
    };
    let backfills = if pending.entries.iter().any(|e| e.run_ids.is_empty()) {
        scoped.get_backfills(None, None).await?
    } else {
        Vec::new()
    };

    let mut recovered = 0usize;
    for entry in pending.entries {
        let dispatched = if entry.run_ids.is_empty() {
            backfills
                .iter()
                .any(|b| b.create_time >= tick_ts && b.asset_selection.contains(&entry.asset_key))
        } else {
            entry.run_ids.iter().any(|id| existing_runs.contains(id))
        };
        if !dispatched {
            continue;
        }
        let slot = eval_state.assets.entry(entry.asset_key).or_default();
        let partition_state = slot.partition_state.take();
        *slot = entry.committed;
        slot.partition_state = partition_state;
        recovered += 1;
    }
    if recovered > 0 {
        tracing::info!(
            target: "rivers::daemon",
            recovered,
            "recovered condition latches from a crash-interrupted tick"
        );
        // Persist BEFORE clearing the intent — dying between the two just
        // re-runs this (idempotent) recovery.
        scoped.set_condition_eval_state(eval_state).await?;
    }
    scoped
        .set_condition_pending_dispatch(&PendingDispatch::default())
        .await?;
    Ok(())
}
