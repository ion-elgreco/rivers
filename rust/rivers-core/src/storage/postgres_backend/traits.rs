//! The storage traits of `PostgresStorage`. Each method forwards to the
//! inherent method of the same name in the module of its area.

use std::future::Future;

use super::*;
use crate::storage::event_batch::{EventStore, EventWithId};

/// Forward each trait method to the inherent method of the same name.
macro_rules! forward {
    ($( fn $name:ident($($arg:ident: $ty:ty),* $(,)?) -> $ret:ty; )*) => {
        $(
            fn $name(&self, $($arg: $ty),*) -> impl Future<Output = $ret> + Send {
                Self::$name(self, $($arg),*)
            }
        )*
    };
}

impl PerCodeLocationStorage for PostgresStorage {
    forward! {
        fn get_events_for_asset(code_location_id: &str, asset_key: &str, limit: usize) -> Result<Vec<StoredEvent>>;
        fn get_latest_materialization(code_location_id: &str, asset_key: &str, partition: Option<&str>) -> Result<Option<StoredEvent>>;
        fn register_assets(code_location_id: &str, records: Vec<AssetRecord>) -> Result<()>;
        fn get_asset_record(code_location_id: &str, asset_key: &str) -> Result<Option<AssetRecord>>;
        fn get_asset_records(code_location_id: &str) -> Result<Vec<AssetRecord>>;
        fn get_asset_records_by_keys(code_location_id: &str, keys: &[String]) -> Result<Vec<AssetRecord>>;
        fn get_assets_by_tag(code_location_id: &str, tag: &str) -> Result<Vec<AssetRecord>>;
        fn get_assets_by_kind(code_location_id: &str, kind: &str) -> Result<Vec<AssetRecord>>;
        fn get_assets_by_group(code_location_id: &str, group: &str) -> Result<Vec<AssetRecord>>;
        fn set_block_reason_by_status(code_location_id: &str, status: RunStatus, reason: Option<&str>) -> Result<()>;
        fn coordinator_tick_query(code_location_id: &str) -> Result<(u32, Vec<CoordinatorRunInfo>, Vec<CoordinatorRunInfo>)>;
        fn get_stalled_not_started_runs(code_location_id: &str, cutoff_ns: i64) -> Result<Vec<String>>;
        fn add_dynamic_partitions(code_location_id: &str, partitions_def_name: &str, partition_keys: &[String]) -> Result<()>;
        fn delete_dynamic_partition(code_location_id: &str, partitions_def_name: &str, partition_key: &str) -> Result<()>;
        fn get_dynamic_partitions(code_location_id: &str, partitions_def_name: &str) -> Result<Vec<String>>;
        fn has_dynamic_partition(code_location_id: &str, partitions_def_name: &str, partition_key: &str) -> Result<bool>;
        fn get_ticks(code_location_id: &str, automation_name: &str, limit: usize) -> Result<Vec<StoredTick>>;
        fn prune_ticks(code_location_id: &str, automation_name: &str, max_ticks: usize) -> Result<usize>;
        fn get_condition_ticks(code_location_id: &str, limit: usize) -> Result<Vec<StoredConditionTick>>;
        fn prune_condition_history(code_location_id: &str, max_ticks: usize) -> Result<usize>;
        fn get_condition_evals(code_location_id: &str, asset_key: &str, limit: usize) -> Result<Vec<StoredConditionEval>>;
        fn get_condition_evals_for_tick(code_location_id: &str, tick_id: &str) -> Result<Vec<StoredConditionEval>>;
        fn get_partition_events(code_location_id: &str, asset_key: &str, partition_key: &str, limit: usize) -> Result<Vec<StoredEvent>>;
        fn get_materialized_partitions(code_location_id: &str, asset_key: &str) -> Result<Vec<PartitionKey>>;
        fn get_materialized_partitions_page(code_location_id: &str, asset_key: &str, offset: u64, limit: u64) -> Result<Vec<PartitionKey>>;
        fn count_materialized_partitions(code_location_id: &str, asset_key: &str) -> Result<u64>;
        fn count_materialized_partitions_in(code_location_id: &str, asset_key: &str, bounds: &[PartitionBounds]) -> Result<Vec<u64>>;
        fn count_dynamic_partitions(code_location_id: &str, partitions_def_name: &str) -> Result<u64>;
        fn get_partition_timestamps(code_location_id: &str, asset_key: &str) -> Result<Vec<(PartitionKey, i64)>>;
        fn get_partition_timestamps_since(code_location_id: &str, asset_key: &str, since_timestamp: i64) -> Result<Vec<(PartitionKey, i64)>>;
        fn get_partition_timestamps_for_keys(code_location_id: &str, asset_key: &str, keys: &[PartitionKey]) -> Result<Vec<(PartitionKey, i64, Option<String>)>>;
        fn get_partition_timestamps_in(code_location_id: &str, asset_key: &str, bounds: &PartitionBounds, limit: u64) -> Result<Vec<(PartitionKey, i64, Option<String>)>>;
        fn get_in_progress_partitions(code_location_id: &str, asset_key: &str) -> Result<Vec<PartitionKey>>;
        fn count_partition_failures(code_location_id: &str, asset_key: &str) -> Result<FailureCount>;
        fn count_partition_failures_in(code_location_id: &str, asset_key: &str, bounds: &[PartitionBounds]) -> Result<Vec<FailureCount>>;
        fn get_partition_failures(code_location_id: &str, asset_key: &str, bounds: Option<&PartitionBounds>, limit: u64) -> Result<Vec<PartitionFailure>>;
        fn get_partition_failures_for_keys(code_location_id: &str, asset_key: &str, keys: &[PartitionKey]) -> Result<Vec<PartitionFailure>>;
        fn get_failed_partitions(code_location_id: &str, asset_key: &str, materialized: &HashMap<PartitionKey, i64>) -> Result<HashMap<PartitionKey, i64>>;
        fn get_asset_deletion_timestamps(code_location_id: &str) -> Result<HashMap<String, i64>>;
        fn get_backfills(code_location_id: &str, limit: Option<usize>, status: Option<BackfillStatus>) -> Result<Vec<BackfillRecord>>;
        fn set_pool_limit(code_location_id: &str, pool_key: &str, limit: i32, lease_duration_secs: u32) -> Result<()>;
        fn get_pool_limits(code_location_id: &str) -> Result<Vec<PoolLimit>>;
        fn get_pool_info(code_location_id: &str, pool_key: &str) -> Result<PoolInfo>;
        fn get_all_pool_infos(code_location_id: &str) -> Result<Vec<PoolInfo>>;
        fn claim_concurrency_slots(code_location_id: &str, pools: &[(String, u32)], run_id: &str, step_key: &str, priority: i32, lease_duration_secs: u32, scope: Option<&AssetScope>) -> Result<ConcurrencyClaimStatus>;
        fn get_pool_slot_holders(code_location_id: &str, pool_key: &str) -> Result<Vec<SlotHolder>>;
        fn get_runs(code_location_id: &str, limit: usize, status: Option<RunStatus>) -> Result<Vec<RunRecord>>;
        fn get_queued_runs(code_location_id: &str) -> Result<Vec<RunRecord>>;
        fn get_runs_since(code_location_id: &str, since_timestamp: i64, status: Option<RunStatus>, order: SortOrder) -> Result<Vec<RunRecord>>;
        fn get_runs_ended_since(code_location_id: &str, since: i64, status: RunStatus, job_names: Option<&[String]>, limit: usize) -> Result<Vec<RunRecord>>;
        fn get_condition_eval_state(code_location_id: &str) -> Result<Option<crate::condition::ConditionEvalState>>;
        fn set_condition_eval_state(code_location_id: &str, state: &crate::condition::ConditionEvalState) -> Result<()>;
        fn get_condition_pending_dispatch(code_location_id: &str) -> Result<Option<crate::condition::PendingDispatch>>;
        fn set_condition_pending_dispatch(code_location_id: &str, pending: &crate::condition::PendingDispatch) -> Result<()>;
        fn get_graph_topology(code_location_id: &str) -> Result<Option<crate::assets::graph::GraphTopology>>;
        fn set_graph_topology(code_location_id: &str, topology: &crate::assets::graph::GraphTopology, key_lists: &[crate::assets::partitions_fingerprint::KeyList<'_>]) -> Result<()>;
        fn get_partition_keys(code_location_id: &str, digest: &str) -> Result<Option<Vec<String>>>;
    }
}

impl StorageBackend for PostgresStorage {
    forward! {
        fn store_event(event: &EventRecord) -> Result<String>;
        fn store_events(events: &[EventRecord]) -> Result<Vec<String>>;
        fn get_events_for_run(run_id: &str) -> Result<Vec<StoredEvent>>;
        fn store_run_logs(logs: &[LogRecord]) -> Result<()>;
        fn get_run_logs(run_id: &str) -> Result<Vec<StoredLog>>;
        fn get_step_tracebacks(run_id: &str, step_key: &str, timestamp: i64) -> Result<Vec<String>>;
        fn step_outcomes(asset_keys: &[String], run_ids: &[String]) -> Result<Vec<StepOutcome>>;
        fn materialized_by_runs(asset_keys: &[String], run_ids: &[String]) -> Result<HashMap<String, HashSet<String>>>;
        fn create_run(run: &RunRecord) -> Result<()>;
        fn create_runs(runs: &[RunRecord]) -> Result<()>;
        fn update_run_status(run_id: &str, status: RunStatus, end_time: Option<i64>) -> Result<()>;
        fn try_start_run(run_id: &str) -> Result<bool>;
        fn update_run_block_reason(run_id: &str, reason: Option<&str>) -> Result<()>;
        fn get_run(run_id: &str) -> Result<Option<RunRecord>>;
        fn get_runs_by_ids(run_ids: &[String], status: Option<RunStatus>) -> Result<Vec<RunRecord>>;
        fn get_all_runs(limit: usize, status: Option<RunStatus>) -> Result<Vec<RunRecord>>;
        fn get_all_runs_since(since_timestamp: i64, status: Option<RunStatus>) -> Result<Vec<RunRecord>>;
        fn get_all_queued_runs() -> Result<Vec<RunRecord>>;
        fn count_in_progress_runs() -> Result<usize>;
        fn get_in_progress_runs() -> Result<Vec<RunRecord>>;
        fn get_observations_since(code_location_id: &str, since_timestamp: i64) -> Result<Vec<StoredEvent>>;
        fn get_latest_observation_ts(code_location_id: &str) -> Result<Option<i64>>;
        fn kv_get(key: &str) -> Result<Option<Vec<u8>>>;
        fn kv_set(key: &str, value: &[u8]) -> Result<()>;
        fn list_code_locations() -> Result<Vec<(LocationRef, CodeLocationDirectoryEntry)>>;
        fn get_code_location_entry(location: &LocationRef) -> Result<Option<CodeLocationDirectoryEntry>>;
        fn upsert_code_location_registration(location: &LocationRef, entry: CodeLocationDirectoryEntry) -> Result<()>;
        fn touch_code_location(location: &LocationRef, identity: &str, grpc_endpoint: &str, now: i64) -> Result<bool>;
        fn delete_code_location_if(location: &LocationRef, identity: &str) -> Result<bool>;
        fn record_component(name: &str, writes: bool, now: i64) -> Result<()>;
        fn list_components() -> Result<Vec<(String, ComponentRecord)>>;
        fn store_tick(tick: &TickRecord) -> Result<String>;
        fn store_ticks_batch(ticks: &[TickRecord]) -> Result<Vec<String>>;
        fn store_condition_tick(tick: &ConditionTickRecord) -> Result<String>;
        fn store_condition_evals_batch(evals: &[ConditionEvalRecord]) -> Result<Vec<String>>;
        fn create_backfill(backfill: &BackfillRecord, partition_keys: &[PartitionKey]) -> Result<()>;
        fn get_backfill_keys(backfill_id: &str) -> Result<Vec<PartitionKey>>;
        fn start_backfill(backfill_id: &str, planned_runs: usize) -> Result<bool>;
        fn settle_unstarted_backfill_runs(backfill_id: &str, runs: &[UnstartedBackfillRun]) -> Result<()>;
        fn get_backfill(backfill_id: &str) -> Result<Option<BackfillRecord>>;
        fn get_ended_backfill_runs(backfill_id: &str, first: usize, last: usize) -> Result<Vec<EndedBackfillRun>>;
        fn get_backfill_partition_outcomes(backfill_id: &str, partitions: &[PartitionKey]) -> Result<Vec<(PartitionKey, BackfillRunOutcome)>>;
        fn get_run_failed_partitions(run_id: &str) -> Result<Vec<PartitionKey>>;
        fn get_open_backfill_runs(backfill_id: &str) -> Result<Vec<String>>;
        fn try_complete_backfill(backfill_id: &str) -> Result<Option<BackfillStatus>>;
        fn cancel_backfill(backfill_id: &str) -> Result<BackfillStatus>;
        fn free_concurrency_slots(run_id: &str, step_key: &str) -> Result<()>;
        fn free_concurrency_slots_for_run(run_id: &str) -> Result<()>;
        fn renew_slot_lease(run_id: &str, step_key: &str, lease_duration_secs: u32) -> Result<u32>;
        fn free_expired_leases() -> Result<u32>;
        fn cancel_queued_run(run_id: &str) -> Result<bool>;
        fn delete_run(run_id: &str) -> Result<bool>;
        fn get_run_progress(run_id: &str) -> Result<RunProgress>;
        fn get_run_outcome(run_id: &str) -> Result<Option<RunOutcome>>;
        fn set_run_outcome(run_id: &str, outcome: &RunOutcome) -> Result<()>;
        fn request_cancellation(run_id: &str) -> Result<()>;
        fn is_cancelled(run_id: &str) -> Result<bool>;
        fn get_events_for_step(run_id: &str, step_key: &str) -> Result<Vec<StoredEvent>>;
        fn get_step_terminal_events(run_id: &str, step_key: &str) -> Result<Vec<StoredEvent>>;
        fn get_run_failure_events(run_id: &str) -> Result<Vec<StoredEvent>>;
        fn get_completed_step_keys(run_id: &str) -> Result<HashSet<String>>;
        fn get_step_attempts(run_id: &str) -> Result<HashMap<String, StepAttempts>>;
        fn get_step_data_versions(run_id: &str) -> Result<HashMap<String, String>>;
    }
}

impl EventStore for PostgresStorage {
    forward! {
        fn store_events_with_ids(events: &[EventWithId]) -> Result<()>;
    }
}
