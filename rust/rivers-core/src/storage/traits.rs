use std::collections::{HashMap, HashSet};
use std::future::Future;

use anyhow::Result;

use super::{
    AssetRecord, AssetScope, BackfillRecord, BackfillStatus, ConcurrencyClaimStatus,
    ConditionEvalRecord, ConditionTickRecord, CoordinatorRunInfo, DEFAULT_CODE_LOCATION_ID,
    EventRecord, LogRecord, PartitionKey, PoolInfo, PoolLimit, RunOutcome, RunProgress, RunRecord,
    RunStatus, ScopedStorage, SlotHolder, SortOrder, StepAttempts, StepOutcome,
    StoredConditionEval, StoredConditionTick, StoredEvent, StoredLog, StoredTick, TickRecord,
};

/// A code-location identity bound for the lifetime of a logical operation.
#[derive(Debug, Clone)]
pub struct CodeLocationContext {
    id: String,
}

impl CodeLocationContext {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// Default context wrapping [`DEFAULT_CODE_LOCATION_ID`].
    pub fn default_for_tests() -> Self {
        Self::new(DEFAULT_CODE_LOCATION_ID)
    }
}

impl From<String> for CodeLocationContext {
    fn from(id: String) -> Self {
        Self::new(id)
    }
}

impl From<&str> for CodeLocationContext {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

/// Per-code-location storage operations.
pub(crate) trait PerCodeLocationStorage: Send + Sync {
    fn get_events_for_asset(
        &self,
        code_location_id: &str,
        asset_key: &str,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<StoredEvent>>> + Send;

    fn get_latest_materialization(
        &self,
        code_location_id: &str,
        asset_key: &str,
        partition: Option<&str>,
    ) -> impl Future<Output = Result<Option<StoredEvent>>> + Send;

    fn register_assets(
        &self,
        code_location_id: &str,
        records: &[AssetRecord],
    ) -> impl Future<Output = Result<()>> + Send;

    fn get_asset_record(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> impl Future<Output = Result<Option<AssetRecord>>> + Send;

    fn get_asset_records(
        &self,
        code_location_id: &str,
    ) -> impl Future<Output = Result<Vec<AssetRecord>>> + Send;

    fn get_asset_records_by_keys(
        &self,
        code_location_id: &str,
        keys: &[String],
    ) -> impl Future<Output = Result<Vec<AssetRecord>>> + Send;

    fn get_assets_by_tag(
        &self,
        code_location_id: &str,
        tag: &str,
    ) -> impl Future<Output = Result<Vec<AssetRecord>>> + Send;

    fn get_assets_by_kind(
        &self,
        code_location_id: &str,
        kind: &str,
    ) -> impl Future<Output = Result<Vec<AssetRecord>>> + Send;

    fn get_assets_by_group(
        &self,
        code_location_id: &str,
        group: &str,
    ) -> impl Future<Output = Result<Vec<AssetRecord>>> + Send;

    fn set_block_reason_by_status(
        &self,
        code_location_id: &str,
        status: RunStatus,
        reason: Option<&str>,
    ) -> impl Future<Output = Result<()>> + Send;

    fn coordinator_tick_query(
        &self,
        code_location_id: &str,
    ) -> impl Future<Output = Result<(u32, Vec<CoordinatorRunInfo>, Vec<CoordinatorRunInfo>)>> + Send;

    /// Run ids stuck in `NotStarted` whose dequeue happened before `cutoff_ns`
    /// (falling back to the run's enqueue time when no RunDequeued event exists).
    fn get_stalled_not_started_runs(
        &self,
        code_location_id: &str,
        cutoff_ns: i64,
    ) -> impl Future<Output = Result<Vec<String>>> + Send;

    fn add_dynamic_partitions(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
        partition_keys: &[String],
    ) -> impl Future<Output = Result<()>> + Send;

    fn delete_dynamic_partition(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
        partition_key: &str,
    ) -> impl Future<Output = Result<()>> + Send;

    fn get_dynamic_partitions(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
    ) -> impl Future<Output = Result<Vec<String>>> + Send;

    fn has_dynamic_partition(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
        partition_key: &str,
    ) -> impl Future<Output = Result<bool>> + Send;

    fn get_ticks(
        &self,
        code_location_id: &str,
        automation_name: &str,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<StoredTick>>> + Send;

    fn prune_ticks(
        &self,
        code_location_id: &str,
        automation_name: &str,
        max_ticks: usize,
    ) -> impl Future<Output = Result<usize>> + Send;

    fn get_condition_ticks(
        &self,
        code_location_id: &str,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<StoredConditionTick>>> + Send;

    /// Drop condition ticks past `max_ticks` along with their evaluations.
    /// Returns the number of ticks dropped.
    fn prune_condition_history(
        &self,
        code_location_id: &str,
        max_ticks: usize,
    ) -> impl Future<Output = Result<usize>> + Send;

    fn get_condition_evals(
        &self,
        code_location_id: &str,
        asset_key: &str,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<StoredConditionEval>>> + Send;

    fn get_condition_evals_for_tick(
        &self,
        code_location_id: &str,
        tick_id: &str,
    ) -> impl Future<Output = Result<Vec<StoredConditionEval>>> + Send;

    fn get_partition_events(
        &self,
        code_location_id: &str,
        asset_key: &str,
        partition_key: &str,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<StoredEvent>>> + Send;

    fn get_materialized_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> impl Future<Output = Result<Vec<PartitionKey>>> + Send;

    fn count_materialized_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> impl Future<Output = Result<u64>> + Send;

    /// Number of registered keys for a dynamic partition namespace.
    fn count_dynamic_partitions(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
    ) -> impl Future<Output = Result<u64>> + Send;

    fn get_partition_timestamps(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> impl Future<Output = Result<Vec<(PartitionKey, i64)>>> + Send;

    /// Partition timestamps that advanced past `since_timestamp` — the
    /// incremental complement of [`Self::get_partition_timestamps`].
    fn get_partition_timestamps_since(
        &self,
        code_location_id: &str,
        asset_key: &str,
        since_timestamp: i64,
    ) -> impl Future<Output = Result<Vec<(PartitionKey, i64)>>> + Send;

    /// Timestamp and last run of exactly `keys` — keys with no row (e.g.
    /// deleted partitions) are simply absent from the result.
    fn get_partition_timestamps_for_keys(
        &self,
        code_location_id: &str,
        asset_key: &str,
        keys: &[PartitionKey],
    ) -> impl Future<Output = Result<Vec<(PartitionKey, i64, Option<String>)>>> + Send;

    fn get_in_progress_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> impl Future<Output = Result<Vec<PartitionKey>>> + Send;

    /// Partitions whose latest failure isn't superseded by a later materialization or deletion, with that failure's timestamp.
    fn get_failed_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
        materialized: &HashMap<PartitionKey, i64>,
    ) -> impl Future<Output = Result<HashMap<PartitionKey, i64>>> + Send;

    /// Latest whole-asset Deletion event timestamp per asset (partition-scoped deletions excluded).
    fn get_asset_deletion_timestamps(
        &self,
        code_location_id: &str,
    ) -> impl Future<Output = Result<HashMap<String, i64>>> + Send;

    fn get_backfills(
        &self,
        code_location_id: &str,
        limit: Option<usize>,
        status: Option<BackfillStatus>,
    ) -> impl Future<Output = Result<Vec<BackfillRecord>>> + Send;

    fn set_pool_limit(
        &self,
        code_location_id: &str,
        pool_key: &str,
        limit: i32,
        lease_duration_secs: u32,
    ) -> impl Future<Output = Result<()>> + Send;

    fn get_pool_limits(
        &self,
        code_location_id: &str,
    ) -> impl Future<Output = Result<Vec<PoolLimit>>> + Send;

    fn get_pool_info(
        &self,
        code_location_id: &str,
        pool_key: &str,
    ) -> impl Future<Output = Result<PoolInfo>> + Send;

    fn get_all_pool_infos(
        &self,
        code_location_id: &str,
    ) -> impl Future<Output = Result<Vec<PoolInfo>>> + Send;

    /// `scope` applies to every `__asset__:`-prefixed pool in `pools`; a step
    /// carries one partition key and one exclusivity, so one scope covers all
    /// of them. `None` on a non-implicit claim.
    fn claim_concurrency_slots(
        &self,
        code_location_id: &str,
        pools: &[(String, u32)],
        run_id: &str,
        step_key: &str,
        priority: i32,
        lease_duration_secs: u32,
        scope: Option<&AssetScope>,
    ) -> impl Future<Output = Result<ConcurrencyClaimStatus>> + Send;

    fn get_pool_slot_holders(
        &self,
        code_location_id: &str,
        pool_key: &str,
    ) -> impl Future<Output = Result<Vec<SlotHolder>>> + Send;

    fn get_runs(
        &self,
        code_location_id: &str,
        limit: usize,
        status: Option<RunStatus>,
    ) -> impl Future<Output = Result<Vec<RunRecord>>> + Send;

    fn get_queued_runs(
        &self,
        code_location_id: &str,
    ) -> impl Future<Output = Result<Vec<RunRecord>>> + Send;

    fn get_runs_since(
        &self,
        code_location_id: &str,
        since_timestamp: i64,
        status: Option<RunStatus>,
        order: SortOrder,
    ) -> impl Future<Output = Result<Vec<RunRecord>>> + Send;

    /// Runs of this code location that ended at or after `since` with
    /// `status`, oldest end first. `job_names` keeps only runs of those jobs.
    fn get_runs_ended_since(
        &self,
        code_location_id: &str,
        since: i64,
        status: RunStatus,
        job_names: Option<&[String]>,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<RunRecord>>> + Send;

    /// Read the persisted condition-daemon eval state for this CL.
    fn get_condition_eval_state(
        &self,
        code_location_id: &str,
    ) -> impl Future<Output = Result<Option<crate::condition::ConditionEvalState>>> + Send;

    /// Persist the condition-daemon eval state for this CL.
    fn set_condition_eval_state(
        &self,
        code_location_id: &str,
        state: &crate::condition::ConditionEvalState,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Read the condition-daemon dispatch intent for this CL (crash recovery).
    fn get_condition_pending_dispatch(
        &self,
        code_location_id: &str,
    ) -> impl Future<Output = Result<Option<crate::condition::PendingDispatch>>> + Send;

    /// Persist the condition-daemon dispatch intent for this CL; an empty
    /// `entries` list clears it.
    fn set_condition_pending_dispatch(
        &self,
        code_location_id: &str,
        pending: &crate::condition::PendingDispatch,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Read the persisted graph topology blob for this CL.
    fn get_graph_topology(
        &self,
        code_location_id: &str,
    ) -> impl Future<Output = Result<Option<crate::assets::graph::GraphTopology>>> + Send;

    /// Persist the graph topology blob for this CL.
    fn set_graph_topology(
        &self,
        code_location_id: &str,
        topology: &crate::assets::graph::GraphTopology,
    ) -> impl Future<Output = Result<()>> + Send;
}

#[allow(private_bounds)]
pub trait StorageBackend: PerCodeLocationStorage {
    /// Bind a [`CodeLocationContext`] to this backend.
    fn for_code_location<'a>(&'a self, ctx: &'a CodeLocationContext) -> ScopedStorage<'a, Self>
    where
        Self: Sized,
    {
        ScopedStorage {
            backend: self,
            code_location_id: ctx.id(),
        }
    }

    // Events
    fn store_event(&self, event: &EventRecord) -> impl Future<Output = Result<String>> + Send;
    fn store_events(
        &self,
        events: &[EventRecord],
    ) -> impl Future<Output = Result<Vec<String>>> + Send;
    fn get_events_for_run(
        &self,
        run_id: &str,
    ) -> impl Future<Output = Result<Vec<StoredEvent>>> + Send;

    // Step logs (`run_logs` — one row per step execution)
    fn store_run_logs(&self, logs: &[LogRecord]) -> impl Future<Output = Result<()>> + Send;
    fn get_run_logs(&self, run_id: &str) -> impl Future<Output = Result<Vec<StoredLog>>> + Send;

    /// Every terminal step among `asset_keys` inside `run_ids`, in one query.
    ///
    /// Takes both as sets because the caller asks about many assets at once. A
    /// per-asset call would read each run's whole log once per asset.
    fn step_outcomes(
        &self,
        asset_keys: &[String],
        run_ids: &[String],
    ) -> impl Future<Output = Result<Vec<StepOutcome>>> + Send;

    /// Which of `run_ids` materialized each of `asset_keys`: asset → runs.
    ///
    /// Read off the runs' own Materialization events: an asset row names only
    /// its newest run, and a delete clears it.
    fn materialized_by_runs(
        &self,
        asset_keys: &[String],
        run_ids: &[String],
    ) -> impl Future<Output = Result<HashMap<String, HashSet<String>>>> + Send;

    // Runs
    fn create_run(&self, run: &RunRecord) -> impl Future<Output = Result<()>> + Send;
    fn create_runs(&self, runs: &[RunRecord]) -> impl Future<Output = Result<()>> + Send;
    fn update_run_status(
        &self,
        run_id: &str,
        status: RunStatus,
        end_time: Option<i64>,
    ) -> impl Future<Output = Result<()>> + Send;
    /// Transition an existing run to `Started` unless it was canceled.
    /// Returns `false` (without touching the record) when the run is
    /// `Canceled` — the executor must skip the run instead of resurrecting it.
    fn try_start_run(&self, run_id: &str) -> impl Future<Output = Result<bool>> + Send;
    /// Set or clear the block reason on a queued run.
    fn update_run_block_reason(
        &self,
        run_id: &str,
        reason: Option<&str>,
    ) -> impl Future<Output = Result<()>> + Send;
    fn get_run(&self, run_id: &str) -> impl Future<Output = Result<Option<RunRecord>>> + Send;
    fn get_runs_by_ids(
        &self,
        run_ids: &[String],
        status: Option<RunStatus>,
    ) -> impl Future<Output = Result<Vec<RunRecord>>> + Send;
    /// List runs across every code location.
    fn get_all_runs(
        &self,
        limit: usize,
        status: Option<RunStatus>,
    ) -> impl Future<Output = Result<Vec<RunRecord>>> + Send;
    /// List runs across every code location created after `since_timestamp` (nanoseconds).
    fn get_all_runs_since(
        &self,
        since_timestamp: i64,
        status: Option<RunStatus>,
    ) -> impl Future<Output = Result<Vec<RunRecord>>> + Send;

    // Run queue
    /// Get all queued runs (unordered) across every code location.
    fn get_all_queued_runs(&self) -> impl Future<Output = Result<Vec<RunRecord>>> + Send;

    /// Count runs in NotStarted or Started status across every code location.
    fn count_in_progress_runs(&self) -> impl Future<Output = Result<usize>> + Send;

    /// Get all runs in NotStarted or Started status across every code location.
    fn get_in_progress_runs(&self) -> impl Future<Output = Result<Vec<RunRecord>>> + Send;

    // Observations
    /// Get the code location's observation events stored after the given timestamp.
    fn get_observations_since(
        &self,
        code_location_id: &str,
        since_timestamp: i64,
    ) -> impl Future<Output = Result<Vec<StoredEvent>>> + Send;

    /// Timestamp of the code location's newest observation event, if any.
    fn get_latest_observation_ts(
        &self,
        code_location_id: &str,
    ) -> impl Future<Output = Result<Option<i64>>> + Send;

    // KV
    fn kv_get(&self, key: &str) -> impl Future<Output = Result<Option<Vec<u8>>>> + Send;
    fn kv_set(&self, key: &str, value: &[u8]) -> impl Future<Output = Result<()>> + Send;

    // Ticks (record-keyed; CL is carried on the record).
    fn store_tick(&self, tick: &TickRecord) -> impl Future<Output = Result<String>> + Send;
    fn store_ticks_batch(
        &self,
        ticks: &[TickRecord],
    ) -> impl Future<Output = Result<Vec<String>>> + Send;

    // Condition ticks + evals (record-keyed; CL is carried on the record).
    fn store_condition_tick(
        &self,
        tick: &ConditionTickRecord,
    ) -> impl Future<Output = Result<String>> + Send;
    fn store_condition_evals_batch(
        &self,
        evals: &[ConditionEvalRecord],
    ) -> impl Future<Output = Result<Vec<String>>> + Send;

    // Backfills
    fn create_backfill(&self, backfill: &BackfillRecord)
    -> impl Future<Output = Result<()>> + Send;
    fn update_backfill_status(
        &self,
        backfill_id: &str,
        status: BackfillStatus,
        end_time: Option<i64>,
    ) -> impl Future<Output = Result<()>> + Send;
    fn update_backfill_progress(
        &self,
        backfill_id: &str,
        run_ids: &[String],
        completed: &[PartitionKey],
        failed: &[PartitionKey],
        canceled: &[PartitionKey],
    ) -> impl Future<Output = Result<()>> + Send;
    fn get_backfill(
        &self,
        backfill_id: &str,
    ) -> impl Future<Output = Result<Option<BackfillRecord>>> + Send;
    /// Check if all runs for a backfill are terminal and finalize it if so.
    fn try_complete_backfill(
        &self,
        backfill_id: &str,
        extra_canceled: &[PartitionKey],
    ) -> impl Future<Output = Result<Option<BackfillStatus>>> + Send;

    /// Cancel a backfill. If every run is already terminal the cancel came
    /// too late to prevent anything — the record settles to its true derived
    /// status instead. Otherwise Requested/InProgress flips to Canceled;
    /// terminal states are never overwritten. Returns the resulting status.
    fn cancel_backfill(
        &self,
        backfill_id: &str,
    ) -> impl Future<Output = Result<BackfillStatus>> + Send;

    /// Release all concurrency slots held by a specific step (across all pools).
    fn free_concurrency_slots(
        &self,
        run_id: &str,
        step_key: &str,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Release all concurrency slots and pending entries for an entire run.
    fn free_concurrency_slots_for_run(
        &self,
        run_id: &str,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Renew the lease on all concurrency slots held by a specific step.
    fn renew_slot_lease(
        &self,
        run_id: &str,
        step_key: &str,
        lease_duration_secs: u32,
    ) -> impl Future<Output = Result<u32>> + Send;

    /// Delete all concurrency slot rows whose lease has expired.
    fn free_expired_leases(&self) -> impl Future<Output = Result<u32>> + Send;

    /// Cancel a run that hasn't started yet (transition from Queued or
    /// NotStarted to Canceled).
    fn cancel_queued_run(&self, run_id: &str) -> impl Future<Output = Result<bool>> + Send;

    /// Delete a terminal run and its history (events, step logs, cancel
    /// flag). `Ok(false)` if no such run exists; errors if the run is still
    /// active — cancel it and let it reach a terminal status first.
    fn delete_run(&self, run_id: &str) -> impl Future<Output = Result<bool>> + Send;

    // ── K8s run coordination ──

    /// Get progress of a run by counting step events.
    fn get_run_progress(&self, run_id: &str) -> impl Future<Output = Result<RunProgress>> + Send;

    /// Get the final outcome written by the executor.
    fn get_run_outcome(
        &self,
        run_id: &str,
    ) -> impl Future<Output = Result<Option<RunOutcome>>> + Send;

    /// Write the final outcome before the executor exits.
    fn set_run_outcome(
        &self,
        run_id: &str,
        outcome: &RunOutcome,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Request cancellation of a run (sets a flag the executor checks between steps).
    fn request_cancellation(&self, run_id: &str) -> impl Future<Output = Result<()>> + Send;

    /// Check if cancellation has been requested for a run.
    fn is_cancelled(&self, run_id: &str) -> impl Future<Output = Result<bool>> + Send;

    /// Get events for a specific step (asset) within a run.
    fn get_events_for_step(
        &self,
        run_id: &str,
        step_key: &str,
    ) -> impl Future<Output = Result<Vec<StoredEvent>>> + Send;

    /// Only a step's terminal events (`StepSuccess` / `StepFailure`), for
    /// pollers that would otherwise re-fetch the whole growing event list.
    fn get_step_terminal_events(
        &self,
        run_id: &str,
        step_key: &str,
    ) -> impl Future<Output = Result<Vec<StoredEvent>>> + Send;

    /// A run's `StepFailure` and `RunLaunchFailed` events, oldest first.
    fn get_run_failure_events(
        &self,
        run_id: &str,
    ) -> impl Future<Output = Result<Vec<StoredEvent>>> + Send;

    /// Get the set of step keys that completed successfully in a run.
    fn get_completed_step_keys(
        &self,
        run_id: &str,
    ) -> impl Future<Output = Result<HashSet<String>>> + Send;

    /// What each step of a run already did, for resuming it: started, failed
    /// at step level, and how many retries it recorded.
    fn get_step_attempts(
        &self,
        run_id: &str,
    ) -> impl Future<Output = Result<HashMap<String, StepAttempts>>> + Send;

    /// Get data versions produced by materialization events in a run.
    fn get_step_data_versions(
        &self,
        run_id: &str,
    ) -> impl Future<Output = Result<HashMap<String, String>>> + Send;
}
