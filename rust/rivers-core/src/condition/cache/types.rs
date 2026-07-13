//! Cache data shapes: partition status, backfill state, and the refresh delta.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::storage::{AssetRecord, PartitionKey, RunRecord};

/// Tag pairs from a run, shared by Arc reference for cheap clones.
pub type RunTags = Arc<[(String, String)]>;

/// One value per `(asset, slot)`, where a slot is `Some(partition)` or the
/// unpartitioned/unkeyed `None` slot — one family serves partitioned and
/// unpartitioned assets alike.
pub type SlotMap<V> = HashMap<String, HashMap<Option<PartitionKey>, V>>;

/// Tag-update entry: `(asset_key, optional partition, tags)`.
type RunTagUpdate = (String, Option<PartitionKey>, String, bool, RunTags);

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
pub(super) struct PartitionStatusPatch {
    pub(super) fresh_timestamps: Vec<(PartitionKey, i64)>,
    pub(super) in_progress: HashSet<PartitionKey>,
    pub(super) failed: HashMap<PartitionKey, i64>,
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
pub(super) enum InProgressChange {
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
pub(super) struct FailedRun {
    pub(super) ts: i64,
    pub(super) run_id: String,
}

/// The `(asset, partition)` slots a run's effects write to: a partitioned
/// asset gets the run key's members (the unpartitioned slot for unkeyed
/// runs); an unpartitioned asset always the single scalar slot.
pub(super) fn run_partition_slots(
    is_partitioned: bool,
    run: &RunRecord,
) -> Vec<Option<PartitionKey>> {
    if !is_partitioned {
        return vec![None];
    }
    match &run.partition_key {
        Some(pk) => pk.members().into_iter().map(Some).collect(),
        None => vec![None],
    }
}

/// Every mutation a single steady-state refresh wants to make to the cache.
#[derive(Default)]
pub(super) struct RefreshDelta {
    /// True if any phase observed a meaningful change. Returned from `apply`.
    pub(super) changed: bool,
    /// Whether to clear the tick tag accumulators before applying tag updates.
    pub(super) clear_tick_accumulators: bool,
    /// Records to insert (or replace) in `records`.
    pub(super) record_updates: Vec<AssetRecord>,
    /// In-progress changes, in apply order.
    pub(super) in_progress_changes: Vec<InProgressChange>,
    /// Asset keys to add to `failed_assets`, each with the [`FailedRun`] that floored them.
    pub(super) failed_adds: HashMap<String, FailedRun>,
    /// Asset keys whose success clears `failed_assets`, with the success run's timestamp.
    pub(super) failed_removes: HashMap<String, i64>,
    /// `asset → run_ids` where the asset's step succeeded but its record write lags.
    pub(super) materialized_overrides: HashMap<String, HashSet<String>>,
    /// Latest-run tag/asset-name updates, gated on materialization at apply.
    pub(super) last_run_updates: Vec<LastRunUpdate>,
    /// Tick tag updates: `(asset, partition_key, tags)`.
    pub(super) tick_tag_updates: Vec<RunTagUpdate>,
    /// Incremental partition-status patches (asset_key → patch).
    pub(super) partition_status: HashMap<String, PartitionStatusPatch>,
    /// Replacement `BackfillState`, if any backfill query happened.
    pub(super) backfill: Option<BackfillState>,
    /// New cursor values; `None` means don't advance.
    pub(super) new_last_seen_run_ts: Option<i64>,
    pub(super) new_last_observation_ts: Option<i64>,
    /// Run_ids confirmed by storage (remove from `pending_runs`).
    pub(super) confirmed_pending: Vec<String>,
    /// Phantom run_ids past grace to evict; `(run_id, asset_keys)`.
    pub(super) evicted_pending: Vec<(String, Vec<String>)>,
    /// Runs whose effects were applied this refresh: `(run_id, start_time)`.
    pub(super) applied_runs: Vec<(String, i64)>,
    /// Assets whose last active backfill reached a terminal state.
    pub(super) backfill_ended_assets: Vec<String>,
}

impl RefreshDelta {
    /// Queue a `ClearRun` for every asset the run covered.
    pub(super) fn clear_run(&mut self, run: &crate::storage::RunRecord) {
        for asset in &run.node_names {
            self.in_progress_changes.push(InProgressChange::ClearRun {
                asset_key: asset.clone(),
                run_id: run.run_id.clone(),
            });
        }
    }
}
