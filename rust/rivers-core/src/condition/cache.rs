//! Cursor-based incremental cache for condition evaluation.
//!
//! [`AssetConditionCache`] loads all asset records on the first tick, then
//! uses `get_runs_since` on subsequent ticks to detect changes and only
//! re-fetches records for touched assets and their transitive downstream
//! dependents. Minimizes storage queries in the hot evaluation loop.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::node::ConditionNode;

use crate::storage::{
    AssetRecord, BackfillStatus, PartitionKey, RunRecord, RunStatus, StorageBackend,
};

/// Tag pairs from a run, shared by Arc reference for cheap clones.
pub type RunTags = Arc<[(String, String)]>;

/// Latest-run tags per asset (unpartitioned).
pub type LastRunTagsMap = HashMap<String, RunTags>;

/// Latest-run tags per asset+partition.
pub type PartitionLastRunTagsMap = HashMap<String, HashMap<PartitionKey, RunTags>>;

/// Tick-scoped accumulator of materialization tag sets per asset (unpartitioned).
pub type TickMaterializationTagsMap = HashMap<String, Vec<RunTags>>;

/// Tick-scoped accumulator of materialization tag sets per asset+partition.
pub type TickPartitionMaterializationTagsMap = HashMap<String, HashMap<PartitionKey, Vec<RunTags>>>;

/// `asset_names` from the latest completed run per asset+partition.
pub type PartitionLastRunAssetNamesMap = HashMap<String, HashMap<PartitionKey, Arc<[String]>>>;

/// Tag-update entry: `(asset_key, optional partition, tags)`.
type RunTagUpdate = (String, Option<PartitionKey>, RunTags);

/// Asset-names update entry: `(asset_key, optional partition, asset_names)`.
type AssetNamesUpdate = (String, Option<PartitionKey>, Arc<[String]>);

/// Per-asset partition status, loaded from storage and refreshed incrementally.
#[derive(Debug, Default)]
pub struct PartitionStatusEntry {
    /// Which partitions have been materialized at least once.
    pub materialized: HashSet<PartitionKey>,
    /// Which partitions are currently being materialized.
    pub in_progress: HashSet<PartitionKey>,
    /// Which partitions failed in latest execution.
    pub failed: HashSet<PartitionKey>,
    /// Latest failure timestamp for each currently-failed partition.
    pub failed_timestamps: HashMap<PartitionKey, i64>,
    /// Per-partition last materialization timestamp.
    pub timestamps: HashMap<PartitionKey, i64>,
}

/// Backfill tracking state — which assets are in active backfills and which
/// partitions they target. Passed as a unit to the evaluator via `EvalContext`.
#[derive(Default)]
pub struct BackfillState {
    /// Maps asset_key → backfill_ids for targeted completion detection.
    pub assets: HashMap<String, Vec<String>>,
    /// Maps backfill_id → targeted partition keys.
    /// Empty Vec means the backfill targets all partitions (unpartitioned asset).
    pub partition_keys: HashMap<String, Vec<PartitionKey>>,
}

/// A run_id that dispatch eagerly registered into `in_progress_assets` but
/// which hasn't yet been confirmed by a storage `get_runs_since` result.
/// If the corresponding storage write failed (or the OS thread spawned by
/// dispatch panicked before reaching it), the run never appears in storage
/// — but the cache still believes the asset is in-progress, blocking
/// re-evaluation forever. Tracking the dispatch timestamp here lets refresh
/// detect that case and self-recover after a grace period.
#[derive(Clone, Debug)]
pub struct PendingRun {
    pub asset_key: String,
    /// Wall-clock nanos at which dispatch registered this run_id.
    pub first_seen_ts: i64,
}

/// Default grace period before an unconfirmed dispatched run_id is treated
/// as a phantom and evicted: 60 seconds (in nanos). Long enough to swallow
/// typical storage replication lag and slow `materialize_with_launcher`
/// startup, short enough that a real phantom doesn't block the asset for
/// minutes.
pub const DEFAULT_PENDING_GRACE_NANOS: i64 = 60 * 1_000_000_000;

/// One mutation to `in_progress_assets`. Stored in order so the apply phase
/// can replay e.g. "push run R1 to asset A, then clear A entirely" exactly
/// the way the in-place refresh did before the Plan/Apply split.
enum InProgressChange {
    Push {
        asset_key: String,
        run_id: String,
    },
    /// Remove all run_ids for this asset.
    AssetClear(String),
}

/// Description of every mutation a single steady-state refresh wants to make
/// to the cache. Built fallibly by `fetch_refresh_delta` (any storage error
/// → drop the partial delta, cache untouched). Replayed infallibly by
/// `apply_refresh_delta`. The split makes refresh atomic — there's no
/// pre-refactor "refresh_runs succeeded but refresh_observations failed"
/// half-state.
#[derive(Default)]
struct RefreshDelta {
    /// True if any phase observed a meaningful change. Returned from `apply`.
    changed: bool,
    /// Whether to clear `tick_materialization_tags` and
    /// `tick_partition_materialization_tags` before applying tag updates.
    clear_tick_accumulators: bool,
    /// Records to insert (or replace) in `records`.
    record_updates: Vec<AssetRecord>,
    /// In-progress changes, in apply order.
    in_progress_changes: Vec<InProgressChange>,
    /// Asset keys to add to `failed_assets`.
    failed_adds: HashSet<String>,
    /// Asset keys to remove from `failed_assets`.
    failed_removes: HashSet<String>,
    /// Tag updates: `(asset, partition_key, tags)`.
    run_tag_updates: Vec<RunTagUpdate>,
    /// Tick tag updates: `(asset, partition_key, tags)`.
    tick_tag_updates: Vec<RunTagUpdate>,
    /// Asset-names updates: `(asset, partition_key, asset_names)`.
    asset_names_updates: Vec<AssetNamesUpdate>,
    /// Partition-status replacements (asset_key → fresh entry).
    partition_status: HashMap<String, PartitionStatusEntry>,
    /// Replacement `BackfillState`, if any backfill query happened.
    backfill: Option<BackfillState>,
    /// New cursor values; `None` means don't advance.
    new_last_seen_run_ts: Option<i64>,
    new_last_observation_ts: Option<i64>,
    /// Run_ids confirmed by storage (remove from `pending_runs`).
    confirmed_pending: Vec<String>,
    /// Phantom run_ids past their grace period: drop from `pending_runs`
    /// AND from `in_progress_assets[asset_key]`.
    evicted_pending: Vec<(String, String)>,
}

/// Cached state for condition evaluation, minimizing storage queries.
///
/// On first tick, loads everything. On subsequent ticks, uses `get_runs_since`
/// to detect new runs and only re-fetches asset records for touched assets
/// and their transitive downstream dependents.
pub struct AssetConditionCache {
    /// All asset records, keyed by asset_key.
    pub records: HashMap<String, AssetRecord>,
    /// Graph edges from topology: (from, to) where from depends on to.
    pub edges: Vec<(String, String)>,
    /// Upstream deps per asset.
    pub upstream_deps: HashMap<String, Vec<String>>,
    /// Downstream deps per asset (for invalidation).
    pub downstream_deps: HashMap<String, Vec<String>>,
    /// Assets currently in in-progress runs. Maps asset_key → run_ids.
    pub in_progress_assets: HashMap<String, Vec<String>>,
    /// Assets whose latest run failed.
    pub failed_assets: HashSet<String>,
    /// Timestamp of the most recent run seen (for cursor-based queries).
    pub last_seen_run_ts: i64,
    /// Timestamp of the most recent observation event seen (cursor for external assets).
    pub last_observation_ts: i64,
    /// Whether the cache has been initialized.
    pub initialized: bool,
    /// Per-asset partition status (materialized, in-progress, failed, timestamps).
    /// Only populated for assets registered via `set_partitioned_assets`.
    pub partition_status: HashMap<String, PartitionStatusEntry>,
    /// Active backfill tracking: which assets are in backfills and which partitions they target.
    pub backfill: BackfillState,
    /// Tags from the latest completed run per asset (unpartitioned). Used by `LastExecutedWithTags`.
    pub last_run_tags: HashMap<String, Arc<[(String, String)]>>,
    /// Tags from the latest completed run per asset+partition. Used by `LastExecutedWithTags`
    /// for partition-level granularity.
    pub partition_last_run_tags: PartitionLastRunTagsMap,
    /// Run tag sets from materializations completed this tick (unpartitioned).
    /// Each entry is a list of tag sets, one per completed run that materialized the asset.
    /// Cleared at the start of each `refresh()` cycle.
    /// Used by `HasRunWithTags` / `AllRunsHaveTags`.
    pub tick_materialization_tags: TickMaterializationTagsMap,
    /// Run tag sets from materializations completed this tick, per partition.
    /// Used by `HasRunWithTags` / `AllRunsHaveTags` in partition mode.
    pub tick_partition_materialization_tags: TickPartitionMaterializationTagsMap,
    /// Full `asset_names` from the latest completed run per asset.
    /// Used by `LastRunIncludesTarget` to check if a dep's run included the root asset.
    pub last_run_asset_names: HashMap<String, Arc<[String]>>,
    /// Full `asset_names` from the latest completed run per asset+partition.
    pub partition_last_run_asset_names: PartitionLastRunAssetNamesMap,
    /// Asset keys that are partitioned — drives partition status loading/refresh.
    partitioned_asset_keys: Vec<String>,
    /// Whether any condition tree uses HasRunWithTags/AllRunsHaveTags.
    /// When false, `update_tick_materialization_tags` is skipped entirely.
    needs_tick_tags: bool,
    /// Code-location context used to scope every per-CL storage call this
    /// cache makes (graph topology, run/eval queries, partition status).
    /// Bound at construction from the daemon's resolved repo state.
    ctx: crate::storage::CodeLocationContext,
    /// Run_ids inserted by dispatch but not yet confirmed via storage.
    /// `refresh` clears entries that storage reports back, and evicts entries
    /// older than `pending_grace_nanos` (assumed-phantom recovery).
    pub pending_runs: HashMap<String, PendingRun>,
    /// Grace window before an unconfirmed dispatched run_id is treated as a
    /// phantom. Defaults to [`DEFAULT_PENDING_GRACE_NANOS`].
    pub pending_grace_nanos: i64,
}

impl Default for AssetConditionCache {
    fn default() -> Self {
        Self::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string())
    }
}

impl AssetConditionCache {
    pub fn new(code_location_id: String) -> Self {
        Self {
            records: HashMap::new(),
            edges: Vec::new(),
            upstream_deps: HashMap::new(),
            downstream_deps: HashMap::new(),
            in_progress_assets: HashMap::new(),
            failed_assets: HashSet::new(),
            last_seen_run_ts: 0,
            last_observation_ts: 0,
            initialized: false,
            partition_status: HashMap::new(),
            backfill: BackfillState::default(),
            last_run_tags: HashMap::new(),
            partition_last_run_tags: HashMap::new(),
            tick_materialization_tags: HashMap::new(),
            tick_partition_materialization_tags: HashMap::new(),
            last_run_asset_names: HashMap::new(),
            partition_last_run_asset_names: HashMap::new(),
            partitioned_asset_keys: Vec::new(),
            needs_tick_tags: false,
            ctx: crate::storage::CodeLocationContext::new(code_location_id),
            pending_runs: HashMap::new(),
            pending_grace_nanos: DEFAULT_PENDING_GRACE_NANOS,
        }
    }

    /// Record a run_id that dispatch has eagerly inserted into
    /// `in_progress_assets`. The next successful `refresh` will either
    /// confirm it (storage returns the run, entry cleared from
    /// `pending_runs`) or, if `pending_grace_nanos` has elapsed without
    /// confirmation, evict it from both `pending_runs` and
    /// `in_progress_assets[asset_key]` — recovering automatically from
    /// dispatch-time phantom IDs (storage write failed or OS thread
    /// panicked before recording the run).
    pub fn register_dispatched_run(&mut self, asset_key: String, run_id: String, now: i64) {
        self.in_progress_assets
            .entry(asset_key.clone())
            .or_default()
            .push(run_id.clone());
        self.pending_runs.insert(
            run_id,
            PendingRun {
                asset_key,
                first_seen_ts: now,
            },
        );
    }

    /// Code-location identity this cache is bound to.
    pub fn code_location_id(&self) -> &str {
        self.ctx.id()
    }

    /// Register which assets are partitioned. Must be called before the first
    /// `refresh()` so that `initial_load` knows to load partition status.
    pub fn set_partitioned_assets(&mut self, keys: Vec<String>) {
        self.partitioned_asset_keys = keys;
    }

    /// Scan condition trees and enable tick-level tag tracking only if any tree
    /// uses `HasRunWithTags` or `AllRunsHaveTags`.
    pub fn set_needs_tick_tags(&mut self, conditions: &[ConditionNode]) {
        self.needs_tick_tags = conditions.iter().any(|c| c.uses_tick_tags());
    }

    /// Load partition status for all registered partitioned assets.
    async fn load_partition_status_inner<S: StorageBackend>(
        &mut self,
        storage: &S,
    ) -> anyhow::Result<()> {
        let scoped = storage.for_code_location(&self.ctx);
        for asset_key in &self.partitioned_asset_keys.clone() {
            let mut entry = PartitionStatusEntry::default();

            let timestamps = scoped.get_partition_timestamps(asset_key).await?;
            for (pk, ts) in &timestamps {
                entry.materialized.insert(pk.clone());
                entry.timestamps.insert(pk.clone(), *ts);
            }

            let in_progress = scoped.get_in_progress_partitions(asset_key).await?;
            for pk in &in_progress {
                entry.in_progress.insert(pk.clone());
            }

            let failed = scoped
                .get_failed_partitions(asset_key, &entry.timestamps)
                .await?;
            entry.failed = failed.keys().cloned().collect();
            entry.failed_timestamps = failed;

            self.partition_status.insert(asset_key.clone(), entry);
        }
        Ok(())
    }

    /// Refresh the cache from storage. Returns `true` if anything changed
    /// (callers should evaluate conditions), `false` if nothing changed.
    ///
    /// `now` is wall-clock nanos for grace-period tracking on dispatched
    /// run_ids that haven't yet appeared in storage; passing the tick's
    /// `now_ts()` is correct.
    pub async fn refresh<S: StorageBackend>(
        &mut self,
        storage: &S,
        now: i64,
    ) -> anyhow::Result<bool> {
        if !self.initialized {
            return self.initial_load(storage).await.map(|_| true);
        }

        let delta = self.fetch_refresh_delta(storage, now).await?;
        Ok(self.apply_refresh_delta(delta))
    }

    /// Plan phase: all fallible storage I/O for one steady-state refresh.
    /// Borrows `&self` so any unexpected mutation would be a compile error.
    /// On any storage failure, returns `Err` — the partial delta is dropped.
    async fn fetch_refresh_delta<S: StorageBackend>(
        &self,
        storage: &S,
        now: i64,
    ) -> anyhow::Result<RefreshDelta> {
        // Always clear per-tick tag accumulators on a refresh — matches the
        // pre-refactor behavior where the first thing `refresh_runs` did was
        // `tick_materialization_tags.clear()`.
        let mut delta = RefreshDelta {
            clear_tick_accumulators: true,
            ..Default::default()
        };

        let scoped = storage.for_code_location(&self.ctx);

        // `get_runs_since` returns ASC; the per-asset `update_run_asset_names`
        // / `update_run_tags` writes inside `apply_run_effects_to_delta`
        // overwrite, so processing newest-last makes the newest run's state
        // win — which is what `LastRunIncludesTarget` needs to read.
        let new_runs = scoped
            .get_runs_since(self.last_seen_run_ts, None, crate::storage::SortOrder::Asc)
            .await?;
        // Skip in-progress runs: the asset_record write inside `execute_run`
        // lands before the run_record flips to a terminal status, so picking
        // up fresh records here races a real bug (in-progress flag set with a
        // post-completion record burned into the cache). The in-progress
        // completion detector below is the safe path.
        let mut invalidated_keys: Vec<String> = new_runs
            .iter()
            .filter(|r| !matches!(r.status, RunStatus::Started | RunStatus::NotStarted))
            .flat_map(|r| r.node_names.iter().cloned())
            .collect();

        if !self.in_progress_assets.is_empty() {
            let ip_keys: Vec<String> = self.in_progress_assets.keys().cloned().collect();
            let fresh_records = scoped.get_asset_records_by_keys(&ip_keys).await?;
            let mut completed_keys: Vec<String> = Vec::new();
            let mut completed_run_ids: HashSet<String> = HashSet::new();
            for record in fresh_records {
                let old = self.records.get(&record.asset_key);
                let ts_changed = old
                    .map(|o| o.last_timestamp != record.last_timestamp)
                    .unwrap_or(true);
                tracing::trace!(
                    target: "rivers::dbg::cond",
                    asset = %record.asset_key,
                    old_ts = ?old.and_then(|o| o.last_timestamp),
                    new_ts = record.last_timestamp,
                    ts_changed,
                    "refresh: in_progress completion check"
                );
                if ts_changed {
                    if let Some(rid) = &record.last_run_id {
                        completed_run_ids.insert(rid.clone());
                    }
                    completed_keys.push(record.asset_key.clone());
                    delta.record_updates.push(record);
                } else if let Some(run_ids) = self.in_progress_assets.get(&record.asset_key)
                    && storage
                        .has_step_completed(&record.asset_key, run_ids)
                        .await?
                {
                    // ts unchanged but step events say it finished — fall
                    // back to the in_progress map for the run_id since
                    // `record.last_run_id` may still point at the previous run.
                    completed_keys.push(record.asset_key.clone());
                    completed_run_ids.extend(run_ids.iter().cloned());
                }
            }
            for key in &completed_keys {
                delta
                    .in_progress_changes
                    .push(InProgressChange::AssetClear(key.clone()));
                invalidated_keys.push(key.clone());
            }

            // `get_runs_since` uses `>`, so a run only ever seen as Started
            // is never re-observed and its terminal state never reaches the
            // new_runs loop below. Fetch and apply the run-derived effects
            // here so `LastRunIncludesTarget` / `HasRunWithTags` /
            // `failed_assets` reflect the actual completed run. Skip ids that
            // ARE in `new_runs` as terminal — those are about to be applied
            // by the loop and double-applying would issue a redundant query.
            let new_runs_terminal: HashSet<&str> = new_runs
                .iter()
                .filter(|r| !matches!(r.status, RunStatus::Started | RunStatus::NotStarted))
                .map(|r| r.run_id.as_str())
                .collect();
            completed_run_ids.retain(|id| !new_runs_terminal.contains(id.as_str()));
            if !completed_run_ids.is_empty() {
                let ids: Vec<String> = completed_run_ids.into_iter().collect();
                let completed_runs = storage.get_runs_by_ids(&ids, None).await?;
                for run in &completed_runs {
                    self.apply_run_effects_to_delta(run, &mut delta);
                }
            }
        }

        if !new_runs.is_empty() {
            delta.changed = true;

            for run in &new_runs {
                // A run that comes back from storage is "confirmed" — if we
                // had a pending entry for it, it's no longer phantom.
                delta.confirmed_pending.push(run.run_id.clone());

                match run.status {
                    RunStatus::Started | RunStatus::NotStarted => {
                        for asset in &run.node_names {
                            delta.in_progress_changes.push(InProgressChange::Push {
                                asset_key: asset.clone(),
                                run_id: run.run_id.clone(),
                            });
                        }
                    }
                    RunStatus::Success | RunStatus::Failure => {
                        for asset in &run.node_names {
                            delta
                                .in_progress_changes
                                .push(InProgressChange::AssetClear(asset.clone()));
                        }
                        self.apply_run_effects_to_delta(run, &mut delta);
                    }
                    RunStatus::Canceled => {
                        for asset in &run.node_names {
                            delta
                                .in_progress_changes
                                .push(InProgressChange::AssetClear(asset.clone()));
                        }
                    }
                    RunStatus::Queued => {
                        // Queued runs have no in-progress mutation but are
                        // still confirmed (above).
                    }
                }
            }

            if let Some(newest) = new_runs.iter().map(|r| r.start_time).max() {
                delta.new_last_seen_run_ts = Some(newest);
            }
        }

        // Only fetch downstream records / partition status / backfill state
        // when something actually completed; Started-only ticks deliberately
        // keep the previous record state (see filter above).
        if !invalidated_keys.is_empty() {
            delta.changed = true;
            let downstream_records = self
                .fetch_records_with_downstream(storage, &invalidated_keys)
                .await?;
            delta.record_updates.extend(downstream_records);

            delta.partition_status = self
                .fetch_partition_status_for_invalidated(storage, &invalidated_keys)
                .await?;

            delta.backfill = Some(self.fetch_updated_backfill_state(storage).await?);
        }

        self.fetch_refresh_observations_delta(storage, &mut delta)
            .await?;

        // Skip run_ids that storage confirmed THIS refresh (they're being
        // cleared from pending anyway, and we mustn't drop them from
        // in_progress_assets).
        let confirmed_set: HashSet<&str> =
            delta.confirmed_pending.iter().map(String::as_str).collect();
        for (run_id, pending) in &self.pending_runs {
            if confirmed_set.contains(run_id.as_str()) {
                continue;
            }
            if (now - pending.first_seen_ts) > self.pending_grace_nanos {
                delta
                    .evicted_pending
                    .push((pending.asset_key.clone(), run_id.clone()));
            }
        }
        if !delta.evicted_pending.is_empty() {
            delta.changed = true;
        }

        Ok(delta)
    }

    /// Plan-phase helper: observations sub-pass.
    async fn fetch_refresh_observations_delta<S: StorageBackend>(
        &self,
        storage: &S,
        delta: &mut RefreshDelta,
    ) -> anyhow::Result<()> {
        let observations = storage
            .get_observations_since(self.last_observation_ts)
            .await?;
        if observations.is_empty() {
            return Ok(());
        }

        let mut observed_keys: Vec<String> = Vec::new();
        let mut max_ts = self.last_observation_ts;
        for event in &observations {
            if let Some(ref key) = event.asset_key
                && !observed_keys.contains(key)
            {
                observed_keys.push(key.clone());
            }
            if event.timestamp > max_ts {
                max_ts = event.timestamp;
            }
        }

        delta.changed = true;
        if !observed_keys.is_empty() {
            // `fetch_records_with_downstream` covers the observed seed AND
            // their transitive downstream, so a single fetch suffices. The
            // pre-refactor code did two fetches (seed + downstream) — this
            // is a small efficiency win.
            let records = self
                .fetch_records_with_downstream(storage, &observed_keys)
                .await?;
            delta.record_updates.extend(records);

            for key in &observed_keys {
                delta
                    .in_progress_changes
                    .push(InProgressChange::AssetClear(key.clone()));
            }
        }

        delta.new_last_observation_ts = Some(max_ts);
        Ok(())
    }

    /// Plan-phase helper: append the run-derived mutations (failure flag,
    /// run/tick tag updates, asset_names updates) for a completed
    /// Success/Failure run into `delta`. Caller is responsible for emitting
    /// any `InProgressChange::AssetClear` separately.
    fn apply_run_effects_to_delta(&self, run: &RunRecord, delta: &mut RefreshDelta) {
        let run_asset_names: Arc<[String]> = Arc::from(run.node_names.as_slice());
        let run_tags: Arc<[(String, String)]> = Arc::from(run.tags.as_slice());
        let is_failure = matches!(run.status, RunStatus::Failure);
        let run_partitions: Vec<Option<PartitionKey>> = match &run.partition_key {
            Some(pk) => pk.members().into_iter().map(Some).collect(),
            None => vec![None],
        };
        for asset in &run.node_names {
            if is_failure {
                delta.failed_adds.insert(asset.clone());
            } else {
                delta.failed_removes.insert(asset.clone());
            }
            for partition_key in &run_partitions {
                if !run_tags.is_empty() {
                    delta.run_tag_updates.push((
                        asset.clone(),
                        partition_key.clone(),
                        Arc::clone(&run_tags),
                    ));
                }
                if !is_failure && self.needs_tick_tags {
                    delta.tick_tag_updates.push((
                        asset.clone(),
                        partition_key.clone(),
                        Arc::clone(&run_tags),
                    ));
                }
                delta.asset_names_updates.push((
                    asset.clone(),
                    partition_key.clone(),
                    Arc::clone(&run_asset_names),
                ));
            }
        }
    }

    /// Plan-phase helper: fetch fresh records for `keys` and their transitive
    /// downstream dependents. `&self` only — does not mutate the cache.
    async fn fetch_records_with_downstream<S: StorageBackend>(
        &self,
        storage: &S,
        keys: &[String],
    ) -> anyhow::Result<Vec<AssetRecord>> {
        let touched: HashSet<&str> = keys.iter().map(|s| s.as_str()).collect();
        let all_keys: Vec<String> = self
            .expand_downstream(&touched)
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        if all_keys.is_empty() {
            return Ok(Vec::new());
        }
        storage
            .for_code_location(&self.ctx)
            .get_asset_records_by_keys(&all_keys)
            .await
    }

    /// Plan-phase helper: re-fetch partition status for invalidated assets.
    /// Returns a map of replacement entries; `&self` only.
    async fn fetch_partition_status_for_invalidated<S: StorageBackend>(
        &self,
        storage: &S,
        invalidated_keys: &[String],
    ) -> anyhow::Result<HashMap<String, PartitionStatusEntry>> {
        let scoped = storage.for_code_location(&self.ctx);
        let mut out: HashMap<String, PartitionStatusEntry> = HashMap::new();
        for asset_key in invalidated_keys {
            if !self.partition_status.contains_key(asset_key) {
                continue; // not a partitioned asset
            }
            let mut entry = PartitionStatusEntry::default();
            let timestamps = scoped.get_partition_timestamps(asset_key).await?;
            for (pk, ts) in &timestamps {
                entry.materialized.insert(pk.clone());
                entry.timestamps.insert(pk.clone(), *ts);
            }
            entry.in_progress = scoped
                .get_in_progress_partitions(asset_key)
                .await?
                .into_iter()
                .collect();
            let failed = scoped
                .get_failed_partitions(asset_key, &entry.timestamps)
                .await?;
            entry.failed = failed.keys().cloned().collect();
            entry.failed_timestamps = failed;
            out.insert(asset_key.clone(), entry);
        }
        Ok(out)
    }

    /// Plan-phase helper: compute the new backfill state from current tracked
    /// ids + completion checks + a fresh active-backfill load.
    async fn fetch_updated_backfill_state<S: StorageBackend>(
        &self,
        storage: &S,
    ) -> anyhow::Result<BackfillState> {
        let mut tracked_ids: HashSet<String> = self
            .backfill
            .assets
            .values()
            .flat_map(|ids| ids.iter().cloned())
            .collect();

        let mut completed_ids: HashSet<String> = HashSet::new();
        for bf_id in &tracked_ids {
            if let Ok(Some(bf)) = storage.get_backfill(bf_id).await {
                match bf.status {
                    BackfillStatus::CompletedSuccess
                    | BackfillStatus::CompletedFailed
                    | BackfillStatus::Canceled => {
                        completed_ids.insert(bf_id.clone());
                    }
                    _ => {}
                }
            }
        }

        let mut new_state = BackfillState {
            assets: self.backfill.assets.clone(),
            partition_keys: self.backfill.partition_keys.clone(),
        };
        if !completed_ids.is_empty() {
            new_state.assets.retain(|_, ids| {
                ids.retain(|id| !completed_ids.contains(id));
                !ids.is_empty()
            });
            for id in &completed_ids {
                new_state.partition_keys.remove(id);
                tracked_ids.remove(id);
            }
        }

        let fresh = Self::load_active_backfills(storage, &self.ctx).await?;
        for (bf_id, pk) in &fresh.partition_keys {
            if !tracked_ids.contains(bf_id) {
                for (asset, ids) in &fresh.assets {
                    if ids.contains(bf_id) {
                        new_state
                            .assets
                            .entry(asset.clone())
                            .or_default()
                            .push(bf_id.clone());
                    }
                }
                new_state.partition_keys.insert(bf_id.clone(), pk.clone());
            }
        }

        Ok(new_state)
    }

    /// Apply phase: replay the planned delta against the cache. Pure
    /// mutation, infallible. Returns `delta.changed`.
    fn apply_refresh_delta(&mut self, delta: RefreshDelta) -> bool {
        let RefreshDelta {
            changed,
            clear_tick_accumulators,
            record_updates,
            in_progress_changes,
            failed_adds,
            failed_removes,
            run_tag_updates,
            tick_tag_updates,
            asset_names_updates,
            partition_status,
            backfill,
            new_last_seen_run_ts,
            new_last_observation_ts,
            confirmed_pending,
            evicted_pending,
        } = delta;

        if clear_tick_accumulators {
            self.tick_materialization_tags.clear();
            self.tick_partition_materialization_tags.clear();
        }

        for record in record_updates {
            self.records.insert(record.asset_key.clone(), record);
        }

        // Apply in-progress changes IN ORDER — the per-run loop in fetch
        // emits e.g. Push then AssetClear when the same asset has both a
        // newly-Started run and a freshly-completed older run; iterating in
        // order preserves the pre-refactor semantics.
        for change in in_progress_changes {
            match change {
                InProgressChange::Push { asset_key, run_id } => {
                    self.in_progress_assets
                        .entry(asset_key)
                        .or_default()
                        .push(run_id);
                }
                InProgressChange::AssetClear(asset_key) => {
                    self.in_progress_assets.remove(asset_key.as_str());
                }
            }
        }

        for asset in failed_adds {
            self.failed_assets.insert(asset);
        }
        for asset in failed_removes {
            self.failed_assets.remove(asset.as_str());
        }

        for (asset, pk, tags) in run_tag_updates {
            self.update_run_tags(&asset, &pk, &tags);
        }
        for (asset, pk, tags) in tick_tag_updates {
            self.update_tick_materialization_tags(&asset, &pk, &tags);
        }
        for (asset, pk, names) in asset_names_updates {
            self.update_run_asset_names(&asset, &pk, &names);
        }

        for (key, entry) in partition_status {
            self.partition_status.insert(key, entry);
        }

        if let Some(bf) = backfill {
            self.backfill = bf;
        }

        if let Some(ts) = new_last_seen_run_ts {
            self.last_seen_run_ts = ts;
        }
        if let Some(ts) = new_last_observation_ts {
            self.last_observation_ts = ts;
        }

        for run_id in confirmed_pending {
            self.pending_runs.remove(&run_id);
        }

        for (asset_key, run_id) in evicted_pending {
            self.pending_runs.remove(&run_id);
            if let Some(run_ids) = self.in_progress_assets.get_mut(&asset_key) {
                run_ids.retain(|id| id != &run_id);
                if run_ids.is_empty() {
                    self.in_progress_assets.remove(&asset_key);
                }
            }
            tracing::warn!(
                target: "rivers::daemon",
                asset_key = %asset_key,
                run_id = %run_id,
                "evicting phantom run_id from cache after grace period"
            );
        }

        changed
    }

    /// Full initial load — populates everything from scratch.
    async fn initial_load<S: StorageBackend>(&mut self, storage: &S) -> anyhow::Result<()> {
        // Clone the ctx into a local so `scoped` borrows from this stack
        // frame, not from `self` — leaves `self` free to mutate below.
        let ctx = self.ctx.clone();
        let scoped = storage.for_code_location(&ctx);

        let records = scoped.get_asset_records().await?;
        self.records = records
            .into_iter()
            .map(|r| (r.asset_key.clone(), r))
            .collect();

        if let Some(topology) = scoped.get_graph_topology().await? {
            self.edges = topology.edges;
        }
        self.build_adjacency();

        let started_runs = scoped
            .get_runs_since(0, Some(RunStatus::Started), crate::storage::SortOrder::Asc)
            .await?;
        for run in &started_runs {
            for asset in &run.node_names {
                self.in_progress_assets
                    .entry(asset.clone())
                    .or_default()
                    .push(run.run_id.clone());
            }
        }

        let last_run_ids: Vec<String> = self
            .records
            .values()
            .filter_map(|r| r.last_run_id.clone())
            .collect();
        if !last_run_ids.is_empty() {
            let last_runs = storage.get_runs_by_ids(&last_run_ids, None).await?;
            let runs_by_id: HashMap<&str, &crate::storage::RunRecord> =
                last_runs.iter().map(|r| (r.run_id.as_str(), r)).collect();
            type AssetRunRow = (
                String,
                RunStatus,
                Option<PartitionKey>,
                RunTags,
                Arc<[String]>,
            );
            let asset_runs: Vec<AssetRunRow> = self
                .records
                .values()
                .filter_map(|record| {
                    let run = runs_by_id.get(record.last_run_id.as_deref()?)?;
                    let partitions: Vec<Option<PartitionKey>> = match &run.partition_key {
                        Some(pk) => pk.members().into_iter().map(Some).collect(),
                        None => vec![None],
                    };
                    let rows: Vec<AssetRunRow> = partitions
                        .into_iter()
                        .map(|pk| {
                            (
                                record.asset_key.clone(),
                                run.status.clone(),
                                pk,
                                Arc::from(run.tags.as_slice()),
                                Arc::from(run.node_names.as_slice()),
                            )
                        })
                        .collect();
                    Some(rows)
                })
                .flatten()
                .collect();
            for (asset_key, status, partition_key, tags, asset_names) in &asset_runs {
                if *status == RunStatus::Failure {
                    self.failed_assets.insert(asset_key.clone());
                }
                if !tags.is_empty() {
                    self.update_run_tags(asset_key, partition_key, tags);
                }
                if !asset_names.is_empty() {
                    self.update_run_asset_names(asset_key, partition_key, asset_names);
                }
            }
        }

        self.backfill = Self::load_active_backfills(storage, &self.ctx).await?;

        // Set cursor just below the newest known run. `get_runs_since` uses
        // `>` exclusively, so a cursor *equal* to the newest run's start_time
        // hides that run's eventual Started→Success transition from every
        // subsequent delta refresh — bites when a schedule fires a fresh run
        // concurrently with daemon startup.
        let recent = scoped.get_runs(1, None).await?;
        if let Some(newest) = recent.first() {
            self.last_seen_run_ts = newest.start_time.saturating_sub(1);
        }

        self.load_partition_status_inner(storage).await?;

        self.initialized = true;
        Ok(())
    }

    /// Query active backfills (Requested + InProgress) and build a `BackfillState`.
    async fn load_active_backfills<S: StorageBackend>(
        storage: &S,
        ctx: &crate::storage::CodeLocationContext,
    ) -> anyhow::Result<BackfillState> {
        let mut state = BackfillState::default();
        let scoped = storage.for_code_location(ctx);
        for status in [BackfillStatus::Requested, BackfillStatus::InProgress] {
            let backfills = scoped.get_backfills(None, Some(status)).await?;
            for bf in &backfills {
                for asset in &bf.asset_selection {
                    state
                        .assets
                        .entry(asset.clone())
                        .or_default()
                        .push(bf.backfill_id.clone());
                }
                state
                    .partition_keys
                    .insert(bf.backfill_id.clone(), bf.partition_keys.clone());
            }
        }
        Ok(state)
    }

    /// Update run tags for an asset after a completed run.
    /// Routes to asset-level or partition-level storage based on whether the run targets a partition.
    fn update_run_tags(
        &mut self,
        asset: &str,
        partition_key: &Option<PartitionKey>,
        tags: &Arc<[(String, String)]>,
    ) {
        if let Some(pk) = partition_key {
            self.partition_last_run_tags
                .entry(asset.to_string())
                .or_default()
                .insert(pk.clone(), Arc::clone(tags));
        } else {
            self.last_run_tags
                .insert(asset.to_string(), Arc::clone(tags));
        }
    }

    /// Track a materialization's run tags for the current tick.
    /// Used by `HasRunWithTags` / `AllRunsHaveTags`.
    /// Unlike `update_run_tags`, this stores ALL runs' tags (including empty),
    /// because we need to know if a materialization happened without matching tags.
    fn update_tick_materialization_tags(
        &mut self,
        asset: &str,
        partition_key: &Option<PartitionKey>,
        tags: &Arc<[(String, String)]>,
    ) {
        if let Some(pk) = partition_key {
            self.tick_partition_materialization_tags
                .entry(asset.to_string())
                .or_default()
                .entry(pk.clone())
                .or_default()
                .push(Arc::clone(tags));
        } else {
            self.tick_materialization_tags
                .entry(asset.to_string())
                .or_default()
                .push(Arc::clone(tags));
        }
    }

    /// Update the asset_names for an asset after a completed run.
    /// Tracks the full list of assets from the run, used by `LastRunIncludesTarget`
    /// to check if a dep's run also included the root asset.
    pub fn update_run_asset_names(
        &mut self,
        asset: &str,
        partition_key: &Option<PartitionKey>,
        asset_names: &Arc<[String]>,
    ) {
        if let Some(pk) = partition_key {
            self.partition_last_run_asset_names
                .entry(asset.to_string())
                .or_default()
                .insert(pk.clone(), Arc::clone(asset_names));
        } else {
            self.last_run_asset_names
                .insert(asset.to_string(), Arc::clone(asset_names));
        }
    }

    /// Build upstream/downstream adjacency maps from edges.
    pub(crate) fn build_adjacency(&mut self) {
        self.upstream_deps.clear();
        self.downstream_deps.clear();
        for (from, to) in &self.edges {
            self.upstream_deps
                .entry(from.clone())
                .or_default()
                .push(to.clone());
            self.downstream_deps
                .entry(to.clone())
                .or_default()
                .push(from.clone());
        }
    }

    /// Compute the set of asset keys that need evaluation when only time-based
    /// conditions and their transitive downstream dependents should be evaluated.
    /// Uses the pre-built `downstream_deps` map. Should be called once after
    /// `initial_load` completes; result can be reused until graph topology changes.
    pub fn compute_time_based_eval_set(
        &self,
        conditions: &[(String, ConditionNode)],
    ) -> HashSet<String> {
        let condition_keys: HashSet<&str> = conditions.iter().map(|(k, _)| k.as_str()).collect();

        let mut eval_set: HashSet<String> = HashSet::new();
        let mut queue: std::collections::VecDeque<String> = std::collections::VecDeque::new();

        for (key, cond) in conditions {
            if cond.has_time_based_conditions() {
                eval_set.insert(key.clone());
                queue.push_back(key.clone());
            }
        }

        while let Some(key) = queue.pop_front() {
            if let Some(downs) = self.downstream_deps.get(key.as_str()) {
                for d in downs {
                    if condition_keys.contains(d.as_str()) && eval_set.insert(d.clone()) {
                        queue.push_back(d.clone());
                    }
                }
            }
        }

        eval_set
    }

    /// Expand a set of touched assets to include all transitive downstream dependents.
    /// Returns borrowed `&str` references into `self.downstream_deps` — no string cloning.
    fn expand_downstream<'a>(&'a self, touched: &HashSet<&'a str>) -> Vec<&'a str> {
        let mut result: HashSet<&str> = touched.iter().copied().collect();
        let mut queue: Vec<&str> = result.iter().copied().collect();
        while let Some(key) = queue.pop() {
            if let Some(downs) = self.downstream_deps.get(key) {
                for d in downs {
                    if result.insert(d.as_str()) {
                        queue.push(d.as_str());
                    }
                }
            }
        }
        result.into_iter().collect()
    }
}
