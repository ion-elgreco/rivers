//! Cursor-based incremental cache for condition evaluation.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::node::ConditionNode;

use crate::storage::{
    AssetRecord, BackfillStatus, PartitionKey, RunRecord, RunStatus, StorageBackend,
};

mod refresh;
mod types;

pub use types::*;

#[cfg(test)]
mod tests;

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
    /// Per-asset partition status (in-progress, failed, timestamps).
    pub partition_status: HashMap<String, PartitionStatusEntry>,
    /// Active backfill tracking: which assets are in backfills and which partitions they target.
    pub backfill: BackfillState,
    /// Tags from the latest completed run per (asset, slot). Used by `LastExecutedWithTags`.
    pub last_run_tags: SlotMap<RunTags>,
    /// Run tag sets from materializations completed this tick, per (asset, slot).
    pub tick_materialization_tags: SlotMap<Vec<RunTags>>,
    /// Full `asset_names` from the latest completed run per (asset, slot).
    pub last_run_asset_names: SlotMap<Arc<[String]>>,
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
            tick_materialization_tags: HashMap::new(),
            last_run_asset_names: HashMap::new(),
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

    /// How many assets are registered partitioned.
    pub fn partitioned_asset_count(&self) -> usize {
        self.partitioned_asset_keys.len()
    }

    /// Enable tick-level tag tracking if any tree uses `HasRunWithTags`/`AllRunsHaveTags`.
    pub fn set_needs_tick_tags<'a>(
        &mut self,
        conditions: impl IntoIterator<Item = &'a ConditionNode>,
    ) {
        self.needs_tick_tags = conditions.into_iter().any(|c| c.uses_tick_tags());
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
        for status in [RunStatus::Started, RunStatus::NotStarted, RunStatus::Queued] {
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
            .get_runs_since(0, Some(RunStatus::Failure), crate::storage::SortOrder::Asc)
            .await?;
        for run in &failed_runs {
            let run_ts = run.end_time.unwrap_or(run.start_time);
            for asset in &run.node_names {
                if run.partition_key.is_some() && self.is_partitioned(asset) {
                    continue;
                }
                let record = self.records.get(asset.as_str());
                let materialized_here =
                    record.and_then(|r| r.last_run_id.as_deref()) == Some(run.run_id.as_str());
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
        self.failed_assets
            .retain(|asset| floors.contains_key(asset));

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
                    let partitions =
                        run_partition_slots(self.is_partitioned(&record.asset_key), run);
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
        // A newer tagless run CLEARS the slot rather than storing an empty
        // vec — run_tags_match on an empty entry is vacuously true, so a
        // stored empty would make no-arg LastExecutedWithTags fire everywhere.
        if tags.is_empty() {
            if let Some(slots) = self.last_run_tags.get_mut(asset) {
                slots.remove(partition_key);
            }
        } else {
            self.last_run_tags
                .entry(asset.to_string())
                .or_default()
                .insert(partition_key.clone(), Arc::clone(tags));
        }
        self.last_run_asset_names
            .entry(asset.to_string())
            .or_default()
            .insert(partition_key.clone(), Arc::clone(asset_names));
    }

    /// Track a materialization's run tags for the current tick.
    fn update_tick_materialization_tags(
        &mut self,
        asset: &str,
        partition_key: &Option<PartitionKey>,
        tags: &Arc<[(String, String)]>,
    ) {
        self.tick_materialization_tags
            .entry(asset.to_string())
            .or_default()
            .entry(partition_key.clone())
            .or_default()
            .push(Arc::clone(tags));
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
