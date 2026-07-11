//! Cursor-based incremental cache for condition evaluation.

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

/// Latest-run map update: `(asset_key, optional partition, run_id, run_ts, tags, asset_names)`.
/// Applied only when the run actually materialized the asset, newest run last.
type LastRunUpdate = (
    String,
    Option<PartitionKey>,
    String,
    i64,
    RunTags,
    Arc<[String]>,
);

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

/// Incremental partition-status update for one asset: fresh timestamp rows
/// plus full (cheap) in-progress/failed views, merged into the live entry.
struct PartitionStatusPatch {
    fresh_timestamps: Vec<(PartitionKey, i64)>,
    in_progress: HashSet<PartitionKey>,
    failed: HashMap<PartitionKey, i64>,
}

/// Backfill tracking state — which assets are in active backfills and which partitions they target.
#[derive(Default, PartialEq)]
pub struct BackfillState {
    /// Maps asset_key → backfill_ids for targeted completion detection.
    pub assets: HashMap<String, Vec<String>>,
    /// Maps backfill_id → targeted partition keys.
    pub partition_keys: HashMap<String, Vec<PartitionKey>>,
}

/// A run_id dispatch eagerly registered into `in_progress_assets` but storage hasn't yet confirmed.
#[derive(Clone, Debug)]
pub struct PendingRun {
    /// Every asset the run dispatched (a run_id can cover many — a joint run).
    pub asset_keys: Vec<String>,
    /// Wall-clock nanos at which dispatch registered this run_id.
    pub first_seen_ts: i64,
}

/// Default grace period before an unconfirmed dispatched run_id is evicted as a phantom: 60s in nanos.
pub const DEFAULT_PENDING_GRACE_NANOS: i64 = 60 * 1_000_000_000;

/// One mutation to `in_progress_assets`, applied in order.
enum InProgressChange {
    Push {
        asset_key: String,
        run_id: String,
        partition_key: Option<PartitionKey>,
    },
    /// Drop one completed run, removing the asset entry when its last run clears.
    ClearRun { asset_key: String, run_id: String },
    /// Drop all of an asset's runs.
    AssetClear(String),
}

/// A pending `failed_adds` entry: the failing run's timestamp and id.
struct FailedRun {
    ts: i64,
    run_id: String,
}

/// Every mutation a single steady-state refresh wants to make to the cache.
#[derive(Default)]
struct RefreshDelta {
    /// True if any phase observed a meaningful change. Returned from `apply`.
    changed: bool,
    /// Whether to clear the tick tag accumulators before applying tag updates.
    clear_tick_accumulators: bool,
    /// Records to insert (or replace) in `records`.
    record_updates: Vec<AssetRecord>,
    /// In-progress changes, in apply order.
    in_progress_changes: Vec<InProgressChange>,
    /// Asset keys to add to `failed_assets`, each with the [`FailedRun`] that floored them.
    failed_adds: HashMap<String, FailedRun>,
    /// Asset keys whose success clears `failed_assets`, with the success run's timestamp.
    failed_removes: HashMap<String, i64>,
    /// `asset → run_ids` where the asset's step succeeded but its record write lags.
    materialized_overrides: HashMap<String, HashSet<String>>,
    /// Latest-run tag/asset-name updates, gated on materialization at apply.
    last_run_updates: Vec<LastRunUpdate>,
    /// Tick tag updates: `(asset, partition_key, tags)`.
    tick_tag_updates: Vec<RunTagUpdate>,
    /// Incremental partition-status patches (asset_key → patch).
    partition_status: HashMap<String, PartitionStatusPatch>,
    /// Replacement `BackfillState`, if any backfill query happened.
    backfill: Option<BackfillState>,
    /// New cursor values; `None` means don't advance.
    new_last_seen_run_ts: Option<i64>,
    new_last_observation_ts: Option<i64>,
    /// Run_ids confirmed by storage (remove from `pending_runs`).
    confirmed_pending: Vec<String>,
    /// Phantom run_ids past grace to evict; `(run_id, asset_keys)`.
    evicted_pending: Vec<(String, Vec<String>)>,
    /// Runs whose effects were applied this refresh: `(run_id, start_time)`.
    applied_runs: Vec<(String, i64)>,
    /// Assets whose last active backfill reached a terminal state.
    backfill_ended_assets: Vec<String>,
}

impl RefreshDelta {
    /// Queue a `ClearRun` for every asset the run covered.
    fn clear_run(&mut self, run: &crate::storage::RunRecord) {
        for asset in &run.node_names {
            self.in_progress_changes.push(InProgressChange::ClearRun {
                asset_key: asset.clone(),
                run_id: run.run_id.clone(),
            });
        }
    }
}

/// Cached state for condition evaluation, minimizing storage queries.
pub struct AssetConditionCache {
    /// All asset records, keyed by asset_key.
    pub records: HashMap<String, AssetRecord>,
    /// Graph edges from topology: (from, to) where from depends on to.
    pub edges: Vec<(String, String)>,
    /// Upstream deps per asset.
    pub upstream_deps: HashMap<String, Vec<String>>,
    /// Downstream deps per asset (for invalidation).
    pub downstream_deps: HashMap<String, Vec<String>>,
    /// In-progress runs per asset: asset_key → (run_id → its partition key).
    pub in_progress_assets: HashMap<String, HashMap<String, Option<PartitionKey>>>,
    /// Assets whose latest run failed.
    pub failed_assets: HashSet<String>,
    /// Latest failure timestamp per currently-failed asset.
    pub failed_asset_timestamps: HashMap<String, i64>,
    /// Timestamp of the most recent run seen (for cursor-based queries).
    pub last_seen_run_ts: i64,
    /// Timestamp of the most recent observation event seen (cursor for external assets).
    pub last_observation_ts: i64,
    /// Whether the cache has been initialized.
    pub initialized: bool,
    /// Per-asset partition status (materialized, in-progress, failed, timestamps).
    pub partition_status: HashMap<String, PartitionStatusEntry>,
    /// Active backfill tracking: which assets are in backfills and which partitions they target.
    pub backfill: BackfillState,
    /// Tags from the latest completed run per asset (unpartitioned). Used by `LastExecutedWithTags`.
    pub last_run_tags: HashMap<String, Arc<[(String, String)]>>,
    /// Tags from the latest completed run per asset+partition.
    pub partition_last_run_tags: PartitionLastRunTagsMap,
    /// Run tag sets from materializations completed this tick (unpartitioned).
    pub tick_materialization_tags: TickMaterializationTagsMap,
    /// Run tag sets from materializations completed this tick, per partition.
    pub tick_partition_materialization_tags: TickPartitionMaterializationTagsMap,
    /// Full `asset_names` from the latest completed run per asset.
    pub last_run_asset_names: HashMap<String, Arc<[String]>>,
    /// Full `asset_names` from the latest completed run per asset+partition.
    pub partition_last_run_asset_names: PartitionLastRunAssetNamesMap,
    /// Asset keys that are partitioned.
    partitioned_asset_keys: HashSet<String>,
    /// Whether any condition tree uses HasRunWithTags/AllRunsHaveTags.
    needs_tick_tags: bool,
    /// Code-location context scoping every per-CL storage call this cache makes.
    ctx: crate::storage::CodeLocationContext,
    /// Run_ids inserted by dispatch but not yet confirmed via storage.
    pub pending_runs: HashMap<String, PendingRun>,
    /// Grace window before an unconfirmed dispatched run_id is treated as a phantom.
    pub pending_grace_nanos: i64,
    /// Effects already applied for these run_ids (`run_id → start_time`). The
    /// cursor trails the newest start_time by 1ns so same-timestamp runs that
    /// commit after a refresh are still delivered; this set keeps the
    /// re-delivered, already-applied ones from double-applying.
    applied_run_ids: HashMap<String, i64>,
    /// run_ts of each latest-run map entry, keyed `(asset, partition_key)` —
    /// guards overwrites so an earlier-finishing overlapping run can't clobber
    /// a later one's tags/asset-names.
    last_run_entry_ts: HashMap<(String, Option<PartitionKey>), i64>,
}

impl Default for AssetConditionCache {
    fn default() -> Self {
        Self::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string())
    }
}

/// Statuses whose run effects the refresh applies (terminal). Queued is NOT
/// terminal: classifying it as such makes a run that flips Queued→terminal
/// between the refresh's two queries lose its effects permanently.
pub(crate) fn run_status_is_terminal(status: &RunStatus) -> bool {
    matches!(
        status,
        RunStatus::Success | RunStatus::Failure | RunStatus::Canceled
    )
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
            failed_asset_timestamps: HashMap::new(),
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
            partitioned_asset_keys: HashSet::new(),
            needs_tick_tags: false,
            ctx: crate::storage::CodeLocationContext::new(code_location_id),
            pending_runs: HashMap::new(),
            pending_grace_nanos: DEFAULT_PENDING_GRACE_NANOS,
            applied_run_ids: HashMap::new(),
            last_run_entry_ts: HashMap::new(),
        }
    }

    /// Record a run_id that dispatch eagerly inserted into `in_progress_assets`.
    pub fn register_dispatched_run(
        &mut self,
        asset_key: String,
        run_id: String,
        now: i64,
        partition_key: Option<PartitionKey>,
    ) {
        self.track_in_progress_run(asset_key.clone(), run_id.clone(), partition_key);
        self.pending_runs
            .entry(run_id)
            .or_insert_with(|| PendingRun {
                asset_keys: Vec::new(),
                first_seen_ts: now,
            })
            .asset_keys
            .push(asset_key);
    }

    /// In-flight partitions for `asset` from the cache's own tracking.
    pub fn in_progress_partition_keys(&self, asset: &str) -> HashSet<PartitionKey> {
        self.in_progress_assets
            .get(asset)
            .map(|runs| runs.values().flatten().cloned().collect())
            .unwrap_or_default()
    }

    /// Record an in-progress run.
    fn track_in_progress_run(
        &mut self,
        asset_key: String,
        run_id: String,
        partition_key: Option<PartitionKey>,
    ) {
        self.in_progress_assets
            .entry(asset_key)
            .or_default()
            .insert(run_id, partition_key);
    }

    /// Drop a pre-dispatch placeholder `in_progress_assets` entry when dispatch failed before any run registered.
    pub fn clear_predispatch_mark(&mut self, asset_key: &str) {
        if let Some(runs) = self.in_progress_assets.get(asset_key) {
            if runs.is_empty() {
                self.in_progress_assets.remove(asset_key);
            }
        }
    }

    /// Roll back a `register_dispatched_run` whose dispatch failed synchronously.
    pub fn clear_dispatched_run(&mut self, asset_key: &str, run_id: &str) {
        self.untrack_in_progress_run(asset_key, run_id);
        if let Some(pending) = self.pending_runs.get_mut(run_id) {
            pending.asset_keys.retain(|k| k != asset_key);
            if pending.asset_keys.is_empty() {
                self.pending_runs.remove(run_id);
            }
        }
    }

    /// Drop a run, removing the asset once empty.
    fn untrack_in_progress_run(&mut self, asset_key: &str, run_id: &str) {
        if let Some(runs) = self.in_progress_assets.get_mut(asset_key) {
            runs.remove(run_id);
            if runs.is_empty() {
                self.in_progress_assets.remove(asset_key);
            }
        }
    }

    /// Code-location identity this cache is bound to.
    pub fn code_location_id(&self) -> &str {
        self.ctx.id()
    }

    /// Register which assets are partitioned.
    pub fn set_partitioned_assets(&mut self, keys: Vec<String>) {
        self.partitioned_asset_keys = keys.into_iter().collect();
    }

    /// Whether `asset` is registered partitioned.
    fn is_partitioned(&self, asset: &str) -> bool {
        self.partitioned_asset_keys.contains(asset)
    }

    /// Enable tick-level tag tracking if any tree uses `HasRunWithTags`/`AllRunsHaveTags`.
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

    /// Refresh the cache from storage. Returns `true` if anything changed.
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
    async fn fetch_refresh_delta<S: StorageBackend>(
        &self,
        storage: &S,
        now: i64,
    ) -> anyhow::Result<RefreshDelta> {
        let mut delta = RefreshDelta {
            clear_tick_accumulators: true,
            ..Default::default()
        };

        let scoped = storage.for_code_location(&self.ctx);

        let new_runs = scoped
            .get_runs_since(self.last_seen_run_ts, None, crate::storage::SortOrder::Asc)
            .await?;
        let mut invalidated_keys: Vec<String> = new_runs
            .iter()
            .filter(|r| {
                run_status_is_terminal(&r.status)
                    && !self.applied_run_ids.contains_key(&r.run_id)
            })
            .flat_map(|r| r.node_names.iter().cloned())
            .collect();

        // Run ids the tracked-run sweep below observed as terminal this refresh;
        // the new_runs loop must not re-track them from a stale Queued snapshot.
        let mut swept_terminal: HashSet<String> = HashSet::new();

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
                } else if let Some(runs) = self.in_progress_assets.get(&record.asset_key) {
                    let run_ids: Vec<String> = runs.keys().cloned().collect();
                    let (completed, succeeded_runs) =
                        storage.step_completion(&record.asset_key, &run_ids).await?;
                    if completed {
                        completed_keys.push(record.asset_key.clone());
                        if !succeeded_runs.is_empty() {
                            delta
                                .materialized_overrides
                                .entry(record.asset_key.clone())
                                .or_default()
                                .extend(succeeded_runs);
                        }
                        completed_run_ids.extend(run_ids);
                    }
                }
            }
            for key in &completed_keys {
                invalidated_keys.push(key.clone());
            }

            let new_runs_terminal: HashSet<&str> = new_runs
                .iter()
                .filter(|r| run_status_is_terminal(&r.status))
                .map(|r| r.run_id.as_str())
                .collect();

            let clearable: Vec<String> = self
                .in_progress_assets
                .values()
                .flat_map(|runs| runs.keys().cloned())
                .collect();
            let mut swept_applied: HashSet<String> = HashSet::new();
            if !clearable.is_empty() {
                let tracked_runs = storage.get_runs_by_ids(&clearable, None).await?;
                for run in &tracked_runs {
                    if !run_status_is_terminal(&run.status) {
                        continue;
                    }
                    swept_terminal.insert(run.run_id.clone());
                    delta
                        .applied_runs
                        .push((run.run_id.clone(), run.start_time));
                    delta.clear_run(run);
                    if !new_runs_terminal.contains(run.run_id.as_str())
                        && !completed_run_ids.contains(&run.run_id)
                        && !self.applied_run_ids.contains_key(&run.run_id)
                    {
                        invalidated_keys.extend(run.node_names.iter().cloned());
                        if self.apply_run_effects_to_delta(run, &mut delta) {
                            swept_applied.insert(run.run_id.clone());
                        }
                    }
                }
            }

            completed_run_ids.retain(|id| {
                !new_runs_terminal.contains(id.as_str()) && !swept_applied.contains(id)
            });
            if !completed_run_ids.is_empty() {
                let ids: Vec<String> = completed_run_ids.into_iter().collect();
                let completed_runs = storage.get_runs_by_ids(&ids, None).await?;
                for run in &completed_runs {
                    if self.applied_run_ids.contains_key(&run.run_id) {
                        continue;
                    }
                    self.apply_run_effects_to_delta(run, &mut delta);
                    invalidated_keys.extend(run.node_names.iter().cloned());
                }
            }
        }

        if !new_runs.is_empty() {
            // The cursor trails the newest start_time, so already-processed
            // runs re-deliver every tick; only genuinely new work may defeat
            // should_skip.
            let known_in_flight = |r: &crate::storage::RunRecord| {
                !r.node_names.is_empty()
                    && r.node_names.iter().all(|a| {
                        self.in_progress_assets
                            .get(a)
                            .is_some_and(|runs| runs.contains_key(&r.run_id))
                    })
            };
            let has_new_work = new_runs.iter().any(|r| {
                if run_status_is_terminal(&r.status) {
                    !self.applied_run_ids.contains_key(&r.run_id)
                        && !swept_terminal.contains(&r.run_id)
                } else {
                    !known_in_flight(r)
                }
            });
            if has_new_work {
                delta.changed = true;
            }

            for run in &new_runs {
                delta.confirmed_pending.push(run.run_id.clone());

                match run.status {
                    RunStatus::Started | RunStatus::NotStarted | RunStatus::Queued => {
                        if !swept_terminal.contains(&run.run_id) {
                            for asset in &run.node_names {
                                delta.in_progress_changes.push(InProgressChange::Push {
                                    asset_key: asset.clone(),
                                    run_id: run.run_id.clone(),
                                    partition_key: run.partition_key.clone(),
                                });
                            }
                        }
                    }
                    RunStatus::Success | RunStatus::Failure => {
                        delta.clear_run(run);
                        if !self.applied_run_ids.contains_key(&run.run_id) {
                            self.apply_run_effects_to_delta(run, &mut delta);
                        }
                    }
                    RunStatus::Canceled => {
                        delta.clear_run(run);
                        delta
                            .applied_runs
                            .push((run.run_id.clone(), run.start_time));
                    }
                }
            }

            // Trail the newest start_time by 1ns: dispatchers stamp one `now`
            // across a batch committed record-by-record, so equal-timestamp
            // runs can land after this refresh. `applied_run_ids` dedups the
            // re-delivered ones.
            if let Some(newest) = new_runs.iter().map(|r| r.start_time).max() {
                delta.new_last_seen_run_ts = Some(newest.saturating_sub(1));
            }
        }

        if !invalidated_keys.is_empty() {
            delta.changed = true;
            let downstream_records = self
                .fetch_records_with_downstream(storage, &invalidated_keys)
                .await?;
            delta.record_updates.extend(downstream_records);

            delta.partition_status = self
                .fetch_partition_status_for_invalidated(storage, &invalidated_keys)
                .await?;
        }

        // BackfillStatus has two live states and load_active_backfills returns
        // every backfill in them, so the fresh query IS the new state — a
        // tracked id that is terminal (or deleted) simply stops appearing.
        let new_backfill = Self::load_active_backfills(storage, &self.ctx).await?;
        if new_backfill != self.backfill {
            delta.changed = true;
            // Assets whose last active backfill ended may still carry the
            // empty pre-dispatch in-flight placeholder (a canceled backfill
            // never produces an observed sub-run to clear it).
            for asset in self.backfill.assets.keys() {
                if !new_backfill.assets.contains_key(asset) {
                    delta.backfill_ended_assets.push(asset.clone());
                }
            }
            delta.backfill = Some(new_backfill);
        }

        self.fetch_refresh_observations_delta(storage, &mut delta)
            .await?;

        let confirmed_set: HashSet<&str> =
            delta.confirmed_pending.iter().map(String::as_str).collect();
        for (run_id, pending) in &self.pending_runs {
            if confirmed_set.contains(run_id.as_str()) {
                continue;
            }
            if (now - pending.first_seen_ts) > self.pending_grace_nanos {
                delta
                    .evicted_pending
                    .push((run_id.clone(), pending.asset_keys.clone()));
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
            .get_observations_since(self.ctx.id(), self.last_observation_ts)
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

        if !observed_keys.is_empty() {
            let records = self
                .fetch_records_with_downstream(storage, &observed_keys)
                .await?;
            // Replayed observations (the cursor trails the newest by 1) must
            // be no-ops: only assets whose record actually moved are cleared
            // and count as change — an AssetClear for an unchanged record
            // would wipe live run tracking seeded at initial_load.
            for record in records {
                let unchanged = self
                    .records
                    .get(&record.asset_key)
                    .is_some_and(|cached| *cached == record);
                if unchanged {
                    continue;
                }
                delta.changed = true;
                if observed_keys.contains(&record.asset_key) {
                    delta
                        .in_progress_changes
                        .push(InProgressChange::AssetClear(record.asset_key.clone()));
                }
                delta.record_updates.push(record);
            }
        }

        delta.new_last_observation_ts = Some(max_ts);
        Ok(())
    }

    /// Plan-phase helper: append a completed Success/Failure run's mutations into `delta`.
    fn apply_run_effects_to_delta(&self, run: &RunRecord, delta: &mut RefreshDelta) -> bool {
        if !matches!(run.status, RunStatus::Success | RunStatus::Failure) {
            return false;
        }
        delta
            .applied_runs
            .push((run.run_id.clone(), run.start_time));
        let run_asset_names: Arc<[String]> = Arc::from(run.node_names.as_slice());
        let run_tags: Arc<[(String, String)]> = Arc::from(run.tags.as_slice());
        let is_failure = matches!(run.status, RunStatus::Failure);
        let run_partitions: Vec<Option<PartitionKey>> = match &run.partition_key {
            Some(pk) => pk.members().into_iter().map(Some).collect(),
            None => vec![None],
        };
        let unpartitioned = [None];
        let run_ts = run.end_time.unwrap_or(run.start_time);
        for asset in &run.node_names {
            if run.partition_key.is_none() || !self.is_partitioned(asset) {
                if is_failure {
                    delta
                        .failed_adds
                        .entry(asset.clone())
                        .and_modify(|e| {
                            if run_ts > e.ts {
                                e.run_id = run.run_id.clone();
                                e.ts = run_ts;
                            }
                        })
                        .or_insert_with(|| FailedRun {
                            ts: run_ts,
                            run_id: run.run_id.clone(),
                        });
                } else {
                    delta
                        .failed_removes
                        .entry(asset.clone())
                        .and_modify(|t| *t = (*t).max(run_ts))
                        .or_insert(run_ts);
                }
            }
            // Route by the ASSET's partitioning, not the run's key: a joint
            // partition-keyed run still writes an unpartitioned asset's entry
            // into the scalar maps the unpartitioned eval path reads.
            let asset_partitions: &[Option<PartitionKey>] = if self.is_partitioned(asset) {
                &run_partitions
            } else {
                &unpartitioned
            };
            for partition_key in asset_partitions {
                delta.last_run_updates.push((
                    asset.clone(),
                    partition_key.clone(),
                    run.run_id.clone(),
                    run_ts,
                    Arc::clone(&run_tags),
                    Arc::clone(&run_asset_names),
                ));
                if !is_failure && self.needs_tick_tags {
                    delta.tick_tag_updates.push((
                        asset.clone(),
                        partition_key.clone(),
                        Arc::clone(&run_tags),
                    ));
                }
            }
        }
        true
    }

    /// Plan-phase helper: fetch fresh records for `keys` and their transitive downstream dependents.
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
    async fn fetch_partition_status_for_invalidated<S: StorageBackend>(
        &self,
        storage: &S,
        invalidated_keys: &[String],
    ) -> anyhow::Result<HashMap<String, PartitionStatusPatch>> {
        let scoped = storage.for_code_location(&self.ctx);
        let mut out: HashMap<String, PartitionStatusPatch> = HashMap::new();
        let unique: HashSet<&String> = invalidated_keys.iter().collect();
        for asset_key in unique {
            let Some(current) = self.partition_status.get(asset_key.as_str()) else {
                continue;
            };
            // Incremental: only rows whose last_timestamp advanced past what
            // the cache already knows — a full asset_partitions scan here was
            // the dominant per-tick cost at large partition counts. The cursor
            // trails the max by 1 (like the run cursor): equal stamps from one
            // batched `now` can land in a later refresh.
            let since = current
                .timestamps
                .values()
                .copied()
                .max()
                .map(|m| m - 1)
                .unwrap_or(-1);
            let fresh_timestamps = scoped
                .get_partition_timestamps_since(asset_key, since)
                .await?;
            let in_progress: HashSet<PartitionKey> = scoped
                .get_in_progress_partitions(asset_key)
                .await?
                .into_iter()
                .collect();
            // Supersession against fresh timestamps is reconciled at apply
            // (timestamps only grow, so recomputing there is sound).
            let failed = scoped
                .get_failed_partitions(asset_key, &current.timestamps)
                .await?;
            out.insert(
                asset_key.clone(),
                PartitionStatusPatch {
                    fresh_timestamps,
                    in_progress,
                    failed,
                },
            );
        }
        Ok(out)
    }

    /// Apply phase: replay the planned delta against the cache. Returns `delta.changed`.
    fn apply_refresh_delta(&mut self, delta: RefreshDelta) -> bool {
        let RefreshDelta {
            changed,
            clear_tick_accumulators,
            record_updates,
            in_progress_changes,
            failed_adds,
            failed_removes,
            materialized_overrides,
            last_run_updates,
            tick_tag_updates,
            partition_status,
            backfill,
            new_last_seen_run_ts,
            new_last_observation_ts,
            confirmed_pending,
            evicted_pending,
            applied_runs,
            backfill_ended_assets,
        } = delta;

        if clear_tick_accumulators {
            self.tick_materialization_tags.clear();
            self.tick_partition_materialization_tags.clear();
        }

        for record in record_updates {
            self.records.insert(record.asset_key.clone(), record);
        }

        for change in in_progress_changes {
            match change {
                InProgressChange::Push {
                    asset_key,
                    run_id,
                    partition_key,
                } => self.track_in_progress_run(asset_key, run_id, partition_key),
                InProgressChange::ClearRun { asset_key, run_id } => {
                    self.untrack_in_progress_run(&asset_key, &run_id)
                }
                InProgressChange::AssetClear(asset_key) => {
                    self.in_progress_assets.remove(asset_key.as_str());
                }
            }
        }

        for (asset, FailedRun { ts, run_id }) in failed_adds {
            let materialized_here = self
                .records
                .get(asset.as_str())
                .and_then(|r| r.last_run_id.as_deref())
                == Some(run_id.as_str())
                || materialized_overrides
                    .get(&asset)
                    .is_some_and(|runs| runs.contains(run_id.as_str()));
            if materialized_here {
                if self
                    .failed_asset_timestamps
                    .get(asset.as_str())
                    .is_none_or(|&f| ts >= f)
                {
                    self.failed_assets.remove(asset.as_str());
                    self.failed_asset_timestamps.remove(asset.as_str());
                }
                continue;
            }
            self.failed_assets.insert(asset.clone());
            self.failed_asset_timestamps
                .entry(asset)
                .and_modify(|t| *t = (*t).max(ts))
                .or_insert(ts);
        }
        for (asset, success_ts) in failed_removes {
            let outranked_by_failure = self
                .failed_asset_timestamps
                .get(asset.as_str())
                .is_some_and(|&fail_ts| success_ts < fail_ts);
            if !outranked_by_failure {
                self.failed_assets.remove(asset.as_str());
                self.failed_asset_timestamps.remove(asset.as_str());
            }
        }

        for (asset, pk, run_id, run_ts, tags, names) in last_run_updates {
            // LastExecutedWithTags/LastRunIncludesTarget reflect the latest run
            // that MATERIALIZED the asset — mirror the failure-floor gate.
            let materialized_here = self
                .records
                .get(asset.as_str())
                .and_then(|r| r.last_run_id.as_deref())
                == Some(run_id.as_str())
                || materialized_overrides
                    .get(&asset)
                    .is_some_and(|runs| runs.contains(&run_id));
            if materialized_here {
                self.update_last_run_maps(&asset, &pk, run_ts, &tags, &names);
            }
        }
        for (asset, pk, tags) in tick_tag_updates {
            self.update_tick_materialization_tags(&asset, &pk, &tags);
        }

        for (key, patch) in partition_status {
            let entry = self.partition_status.entry(key).or_default();
            for (pk, ts) in patch.fresh_timestamps {
                entry.materialized.insert(pk.clone());
                entry.timestamps.insert(pk, ts);
            }
            entry.in_progress = patch.in_progress;
            // Drop failures superseded by the freshly merged timestamps (the
            // plan-phase supersession ran against the pre-merge view).
            entry.failed_timestamps = patch
                .failed
                .into_iter()
                .filter(|(pk, fail_ts)| {
                    entry.timestamps.get(pk).is_none_or(|mat| fail_ts > mat)
                })
                .collect();
            entry.failed = entry.failed_timestamps.keys().cloned().collect();
        }

        if let Some(bf) = backfill {
            self.backfill = bf;
        }
        for asset in backfill_ended_assets {
            self.clear_predispatch_mark(&asset);
        }

        for (run_id, start_time) in applied_runs {
            self.applied_run_ids.insert(run_id, start_time);
        }
        if let Some(ts) = new_last_seen_run_ts {
            self.last_seen_run_ts = ts;
        }
        // Runs at or below the cursor can never be re-delivered (`start_time > $since`).
        let cursor = self.last_seen_run_ts;
        self.applied_run_ids.retain(|_, st| *st > cursor);
        if let Some(ts) = new_last_observation_ts {
            self.last_observation_ts = ts;
        }

        for run_id in confirmed_pending {
            self.pending_runs.remove(&run_id);
        }

        for (run_id, asset_keys) in evicted_pending {
            self.pending_runs.remove(&run_id);
            for asset_key in &asset_keys {
                self.untrack_in_progress_run(asset_key, &run_id);
            }
            tracing::warn!(
                target: "rivers::daemon",
                run_id = %run_id,
                assets = asset_keys.len(),
                "evicting phantom run_id from cache after grace period"
            );
        }

        changed
    }

    /// Full initial load — populates everything from scratch.
    async fn initial_load<S: StorageBackend>(&mut self, storage: &S) -> anyhow::Result<()> {
        let ctx = self.ctx.clone();
        let scoped = storage.for_code_location(&ctx);

        // Records loaded below already reflect past observations; replaying
        // the observation history would AssetClear live run tracking. The
        // cursor therefore trails the NEWEST stored observation by 1 (not
        // wall clock): an observation stamped earlier whose write lands after
        // this load must still be seen, and re-processing the newest one is
        // an idempotent record re-fetch.
        self.last_observation_ts = storage
            .get_latest_observation_ts(ctx.id())
            .await?
            .map(|ts| ts - 1)
            .unwrap_or(-1);

        let records = scoped.get_asset_records().await?;
        self.records = records
            .into_iter()
            .map(|r| (r.asset_key.clone(), r))
            .collect();

        if let Some(topology) = scoped.get_graph_topology().await? {
            self.edges = topology.edges;
        }
        self.build_adjacency();

        // Every non-terminal status is in flight: NotStarted/Queued runs alive
        // at restart must suppress duplicate dispatch just like Started ones.
        for status in [
            RunStatus::Started,
            RunStatus::NotStarted,
            RunStatus::Queued,
        ] {
            let live_runs = scoped
                .get_runs_since(0, Some(status), crate::storage::SortOrder::Asc)
                .await?;
            for run in &live_runs {
                for asset in &run.node_names {
                    self.track_in_progress_run(
                        asset.clone(),
                        run.run_id.clone(),
                        run.partition_key.clone(),
                    );
                }
            }
        }

        // Failure floors: a pre-existing failed run must be visible to
        // ExecutionFailed on a fresh cache — persisted eval-state floors are
        // only a rehydration shortcut, not the source of truth. Mirrors the
        // steady-state gates (materialized-by-the-failed-run, or outranked by
        // a newer materialization, clears the floor).
        let failed_runs = scoped
            .get_runs_since(
                0,
                Some(RunStatus::Failure),
                crate::storage::SortOrder::Asc,
            )
            .await?;
        for run in &failed_runs {
            let run_ts = run.end_time.unwrap_or(run.start_time);
            for asset in &run.node_names {
                if run.partition_key.is_some() && self.is_partitioned(asset) {
                    continue;
                }
                let record = self.records.get(asset.as_str());
                let materialized_here = record.and_then(|r| r.last_run_id.as_deref())
                    == Some(run.run_id.as_str());
                let outranked = record
                    .and_then(|r| r.last_timestamp)
                    .is_some_and(|mat| mat >= run_ts);
                if materialized_here || outranked {
                    continue;
                }
                self.failed_assets.insert(asset.clone());
                self.failed_asset_timestamps
                    .entry(asset.clone())
                    .and_modify(|t| *t = (*t).max(run_ts))
                    .or_insert(run_ts);
            }
        }
        // Floors rehydrated from persisted eval-state predate this load; drop
        // any outranked by a newer materialization (the asset recovered while
        // the daemon was down — steady state would have cleared them).
        let records = &self.records;
        self.failed_asset_timestamps.retain(|asset, ts| {
            records
                .get(asset)
                .and_then(|r| r.last_timestamp)
                .is_none_or(|mat| mat < *ts)
        });
        let floors = &self.failed_asset_timestamps;
        self.failed_assets.retain(|asset| floors.contains_key(asset));

        let last_run_ids: Vec<String> = self
            .records
            .values()
            .filter_map(|r| r.last_run_id.clone())
            .collect();
        if !last_run_ids.is_empty() {
            let last_runs = storage.get_runs_by_ids(&last_run_ids, None).await?;
            let runs_by_id: HashMap<&str, &crate::storage::RunRecord> =
                last_runs.iter().map(|r| (r.run_id.as_str(), r)).collect();
            type AssetRunRow = (String, Option<PartitionKey>, i64, RunTags, Arc<[String]>);
            let asset_runs: Vec<AssetRunRow> = self
                .records
                .values()
                .filter_map(|record| {
                    let run = runs_by_id.get(record.last_run_id.as_deref()?)?;
                    let run_ts = run.end_time.unwrap_or(run.start_time);
                    let partitions: Vec<Option<PartitionKey>> =
                        if self.is_partitioned(&record.asset_key) {
                            match &run.partition_key {
                                Some(pk) => pk.members().into_iter().map(Some).collect(),
                                None => vec![None],
                            }
                        } else {
                            vec![None]
                        };
                    let rows: Vec<AssetRunRow> = partitions
                        .into_iter()
                        .map(|pk| {
                            (
                                record.asset_key.clone(),
                                pk,
                                run_ts,
                                Arc::from(run.tags.as_slice()),
                                Arc::from(run.node_names.as_slice()),
                            )
                        })
                        .collect();
                    Some(rows)
                })
                .flatten()
                .collect();
            for (asset_key, partition_key, run_ts, tags, asset_names) in &asset_runs {
                self.update_last_run_maps(asset_key, partition_key, *run_ts, tags, asset_names);
            }
        }

        self.backfill = Self::load_active_backfills(storage, &self.ctx).await?;

        let recent = scoped.get_runs(1, None).await?;
        if let Some(newest) = recent.first() {
            self.last_seen_run_ts = newest.start_time.saturating_sub(1);
            // The newest runs' effects are already reflected in the records
            // loaded above; seed them as applied so the first refresh doesn't
            // replay them into the tick-scoped tag accumulators.
            let ties = scoped
                .get_runs_since(self.last_seen_run_ts, None, crate::storage::SortOrder::Asc)
                .await?;
            for run in &ties {
                if run_status_is_terminal(&run.status) {
                    self.applied_run_ids
                        .insert(run.run_id.clone(), run.start_time);
                }
            }
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

    /// Update the latest-run tag/asset-name maps for one `(asset, partition)`
    /// entry, keeping the entry from the newest run when entries race.
    fn update_last_run_maps(
        &mut self,
        asset: &str,
        partition_key: &Option<PartitionKey>,
        run_ts: i64,
        tags: &Arc<[(String, String)]>,
        asset_names: &Arc<[String]>,
    ) {
        match self
            .last_run_entry_ts
            .entry((asset.to_string(), partition_key.clone()))
        {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                if run_ts < *e.get() {
                    return;
                }
                e.insert(run_ts);
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(run_ts);
            }
        }
        // A newer tagless run CLEARS the entry rather than storing an empty
        // vec — run_tags_match on an empty entry is vacuously true, so a
        // stored empty would make no-arg LastExecutedWithTags fire everywhere.
        if let Some(pk) = partition_key {
            if tags.is_empty() {
                if let Some(m) = self.partition_last_run_tags.get_mut(asset) {
                    m.remove(pk);
                }
            } else {
                self.partition_last_run_tags
                    .entry(asset.to_string())
                    .or_default()
                    .insert(pk.clone(), Arc::clone(tags));
            }
            self.partition_last_run_asset_names
                .entry(asset.to_string())
                .or_default()
                .insert(pk.clone(), Arc::clone(asset_names));
        } else {
            if tags.is_empty() {
                self.last_run_tags.remove(asset);
            } else {
                self.last_run_tags
                    .insert(asset.to_string(), Arc::clone(tags));
            }
            self.last_run_asset_names
                .insert(asset.to_string(), Arc::clone(asset_names));
        }
    }

    /// Track a materialization's run tags for the current tick.
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

    /// Compute the asset keys that need evaluation for time-based conditions and their downstream dependents.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_run(run_id: &str, status: RunStatus, assets: &[&str], ts: i64) -> RunRecord {
        RunRecord {
            run_id: run_id.to_string(),
            code_location_id: crate::storage::default_code_location_id(),
            job_name: None,
            status,
            start_time: ts,
            end_time: Some(ts),
            tags: Vec::new(),
            node_names: assets.iter().map(|s| s.to_string()).collect(),
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: crate::storage::LaunchedBy::default(),
        }
    }

    fn rec_with_run(asset: &str, last_run_id: Option<&str>, ts: i64) -> AssetRecord {
        AssetRecord {
            code_location_id: crate::storage::default_code_location_id(),
            asset_key: asset.to_string(),
            tags: vec![],
            kinds: vec![],
            asset_group: None,
            code_version: None,
            last_event_id: None,
            last_run_id: last_run_id.map(String::from),
            last_timestamp: Some(ts),
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![],
        }
    }

    #[test]
    fn failure_floor_survives_co_batched_older_success() {
        let mut cache = AssetConditionCache::new("default".to_string());
        let mut delta = RefreshDelta::default();
        cache.apply_run_effects_to_delta(&mk_run("s", RunStatus::Success, &["R"], 100), &mut delta);
        cache.apply_run_effects_to_delta(&mk_run("f", RunStatus::Failure, &["R"], 200), &mut delta);
        cache.apply_refresh_delta(delta);

        assert_eq!(
            cache.failed_asset_timestamps.get("R"),
            Some(&200),
            "newer failure floor must survive an older co-batched success"
        );
        assert!(
            cache.failed_assets.contains("R"),
            "asset must remain in failed_assets"
        );
    }

    #[test]
    fn newer_success_clears_failure_floor() {
        let mut cache = AssetConditionCache::new("default".to_string());
        let mut delta = RefreshDelta::default();
        cache.apply_run_effects_to_delta(&mk_run("f", RunStatus::Failure, &["R"], 100), &mut delta);
        cache.apply_run_effects_to_delta(&mk_run("s", RunStatus::Success, &["R"], 200), &mut delta);
        cache.apply_refresh_delta(delta);

        assert_eq!(
            cache.failed_asset_timestamps.get("R"),
            None,
            "a success newer than the failure must clear the floor"
        );
        assert!(!cache.failed_assets.contains("R"));
    }

    #[test]
    fn failure_floor_skips_assets_materialized_in_the_failed_run() {
        let mut cache = AssetConditionCache::new("default".to_string());
        cache
            .records
            .insert("X".to_string(), rec_with_run("X", Some("R"), 150));
        cache
            .records
            .insert("Y".to_string(), rec_with_run("Y", Some("prev"), 50));

        let mut delta = RefreshDelta::default();
        cache.apply_run_effects_to_delta(
            &mk_run("R", RunStatus::Failure, &["X", "Y"], 150),
            &mut delta,
        );
        cache.apply_refresh_delta(delta);

        assert_eq!(
            cache.failed_asset_timestamps.get("Y"),
            Some(&150),
            "Y actually failed → floor at the run ts"
        );
        assert!(
            !cache.failed_asset_timestamps.contains_key("X"),
            "X materialized in the failed joint run → no failure floor"
        );
        assert!(!cache.failed_assets.contains("X"));
        assert!(cache.failed_assets.contains("Y"));
    }

    #[test]
    fn partitioned_failure_does_not_set_asset_level_floor() {
        let mut cache = AssetConditionCache::new("default".to_string());
        cache.set_partitioned_assets(vec!["P".to_string()]);
        let mut run = mk_run("P", RunStatus::Failure, &["P"], 150);
        run.partition_key = Some(PartitionKey::Single {
            keys: vec!["p1".to_string()],
        });
        let mut delta = RefreshDelta::default();
        cache.apply_run_effects_to_delta(&run, &mut delta);
        cache.apply_refresh_delta(delta);

        assert!(
            !cache.failed_assets.contains("P"),
            "a single partition's failure must not floor the whole asset"
        );
        assert!(
            !cache.failed_asset_timestamps.contains_key("P"),
            "no asset-level failure timestamp for a partitioned run"
        );
    }

    #[test]
    fn override_covers_every_succeeded_run_not_the_first_found() {
        let mut cache = AssetConditionCache::new("default".to_string());
        let mut delta = RefreshDelta::default();
        let r1 = mk_run("r1", RunStatus::Failure, &["x", "y"], 3000);
        let r2 = mk_run("r2", RunStatus::Failure, &["x", "z"], 4000);
        cache.apply_run_effects_to_delta(&r1, &mut delta);
        cache.apply_run_effects_to_delta(&r2, &mut delta);
        delta
            .materialized_overrides
            .entry("x".to_string())
            .or_default()
            .extend(["r1".to_string(), "r2".to_string()]);
        cache.apply_refresh_delta(delta);

        assert!(
            !cache.failed_assets.contains("x"),
            "x's step succeeded in the newest failing run — it must not be \
             floored just because an older run's success was discovered first"
        );
    }

    #[test]
    fn partition_keyed_success_clears_unpartitioned_asset_floor() {
        let mut cache = AssetConditionCache::new("default".to_string());
        cache.set_partitioned_assets(vec!["P".to_string()]);

        let fail = mk_run("run-f", RunStatus::Failure, &["D"], 100);
        let mut delta = RefreshDelta::default();
        cache.apply_run_effects_to_delta(&fail, &mut delta);
        cache.apply_refresh_delta(delta);
        assert!(cache.failed_assets.contains("D"));

        let mut ok = mk_run("run-s", RunStatus::Success, &["P", "D"], 200);
        ok.partition_key = Some(PartitionKey::Single {
            keys: vec!["2024-01-01".to_string()],
        });
        let mut delta = RefreshDelta::default();
        cache.apply_run_effects_to_delta(&ok, &mut delta);
        cache.apply_refresh_delta(delta);

        assert!(
            !cache.failed_assets.contains("D"),
            "a partition-keyed success covering unpartitioned D must clear D's floor"
        );
        assert!(
            !cache.failed_assets.contains("P"),
            "the partitioned asset's outcome stays out of the asset-level floor"
        );
    }
}
