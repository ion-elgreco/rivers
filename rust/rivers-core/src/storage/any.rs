//! Backend dispatch across the supported storage engines.
//!
//! An enum rather than `dyn StorageBackend` because the traits are RPITIT and
//! so are not object-safe, and rather than a type parameter because
//! `#[pyclass] PyStorage` cannot be generic.
//!
//! Adding a backend means adding one arm to each `delegate_*` macro below.
//! Nothing here needs hand-syncing with the traits: a new trait method fails
//! this impl until it is listed, and a new variant makes every match
//! non-exhaustive.

use std::future::Future;

use anyhow::Result;

use super::surrealdb_backend::{Capability, SurrealConnectConfig, SurrealStorage};
use super::url::StorageUrl;
use super::*;

/// A storage backend, chosen at connect time.
pub enum AnyStorage {
    Surreal(SurrealStorage),
}

impl std::fmt::Debug for AnyStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.label())
    }
}

impl From<SurrealStorage> for AnyStorage {
    fn from(inner: SurrealStorage) -> Self {
        Self::Surreal(inner)
    }
}

/// Delegate async trait methods to the active backend.
///
/// Adding a backend means adding one arm here, not one arm in every method.
macro_rules! delegate_trait {
    ($( $(#[$m:meta])* fn $name:ident($($arg:ident: $ty:ty),* $(,)?) -> $ret:ty; )*) => {
        $(
            $(#[$m])*
            fn $name(&self, $($arg: $ty),*) -> impl Future<Output = $ret> + Send {
                async move {
                    match self {
                        Self::Surreal(inner) => inner.$name($($arg),*).await,
                    }
                }
            }
        )*
    };
}

/// Same, for the inherent methods the UI and daemon call directly.
macro_rules! delegate_inherent {
    ($( $(#[$m:meta])* fn $name:ident($($arg:ident: $ty:ty),* $(,)?) -> $ret:ty; )*) => {
        $(
            $(#[$m])*
            pub async fn $name(&self, $($arg: $ty),*) -> $ret {
                match self {
                    Self::Surreal(inner) => inner.$name($($arg),*).await,
                }
            }
        )*
    };
}

impl AnyStorage {
    /// Embedded SurrealDB over RocksDB at `path` — the local-dev backend.
    pub async fn surreal_embedded(path: &str) -> Result<Self> {
        Ok(Self::Surreal(SurrealStorage::new_embedded(path).await?))
    }

    pub async fn surreal_embedded_with_capability(path: &str, cap: Capability) -> Result<Self> {
        Ok(Self::Surreal(
            SurrealStorage::new_embedded_with_capability(path, cap).await?,
        ))
    }

    pub fn surreal_embedded_blocking(path: &str) -> Result<Self> {
        Ok(Self::Surreal(SurrealStorage::new_embedded_blocking(path)?))
    }

    /// In-memory SurrealDB — tests only, no durability.
    pub async fn surreal_memory() -> Result<Self> {
        Ok(Self::Surreal(SurrealStorage::new_memory().await?))
    }

    pub fn surreal_memory_blocking() -> Result<Self> {
        Ok(Self::Surreal(SurrealStorage::new_memory_blocking()?))
    }

    /// Remote SurrealDB server.
    pub async fn surreal_connect(config: SurrealConnectConfig) -> Result<Self> {
        Ok(Self::Surreal(SurrealStorage::connect(config).await?))
    }

    pub async fn surreal_connect_with_capability(
        config: SurrealConnectConfig,
        cap: Capability,
    ) -> Result<Self> {
        Ok(Self::Surreal(
            SurrealStorage::connect_with_capability(config, cap).await?,
        ))
    }

    /// Open whichever backend `url` names.
    ///
    /// The SurrealDB variants carry their own scope and credentials on the
    /// parsed config, so a caller that needs authentication attaches them to
    /// the [`StorageUrl`] before calling this.
    pub async fn open(url: StorageUrl, cap: Capability) -> Result<Self> {
        match url {
            StorageUrl::SurrealEmbedded { path } => {
                Self::surreal_embedded_with_capability(&path, cap).await
            }
            StorageUrl::SurrealMemory => Self::surreal_memory().await,
            StorageUrl::SurrealRemote(config) => {
                Self::surreal_connect_with_capability(config, cap).await
            }
            StorageUrl::Postgres { url } => {
                anyhow::bail!(
                    "PostgreSQL storage ({url}) is not implemented yet;                      the schema and migrations exist but the backend does not"
                )
            }
        }
    }

    /// Human-readable backend label for logs.
    pub fn label(&self) -> String {
        match self {
            Self::Surreal(inner) => inner.backend_kind().label(),
        }
    }

    delegate_inherent! {
    fn enqueue_backfill_runs(records: &[RunRecord], backfill_id: &str) -> Result<bool>;
    fn enqueue_run(record: &RunRecord) -> Result<()>;
    fn enqueue_runs(records: &[RunRecord]) -> Result<()>;
    fn fail_backfill(backfill_id: &str, error: &str) -> Result<()>;
    fn get_all_backfills_page(offset: u64, limit: u64, filter: &BackfillFilter) -> Result<BackfillsPage>;
    fn get_all_backfills_summary() -> Result<BackfillsSummary>;
    fn get_all_last_run_per_job(job_names: &[String]) -> Result<Vec<(String, RunRecord)>>;
    fn get_all_runs_page(offset: u64, limit: u64, filter: &RunFilter) -> Result<RunsPage>;
    fn get_all_runs_summary(cutoff_24h_ns: i64) -> Result<RunsSummary>;
    fn get_backfills_page(code_location_id: &str, offset: u64, limit: u64, filter: &BackfillFilter) -> Result<BackfillsPage>;
    fn get_backfills_summary(code_location_id: &str) -> Result<BackfillsSummary>;
    fn get_events_for_asset_page(code_location_id: &str, asset_key: &str, event_types: &[String], offset: u64, limit: u64) -> Result<(Vec<StoredEvent>, u64)>;
    fn get_last_run_per_job(code_location_id: &str, job_names: &[String]) -> Result<Vec<(String, RunRecord)>>;
    fn get_run_asset_events_page(run_id: &str, asset_key: &str, event_type: &str, offset: u64, limit: u64) -> Result<(Vec<StoredEvent>, u64)>;
    fn get_run_step_events(run_id: &str) -> Result<Vec<StoredEvent>>;
    fn get_run_structured_events_page(run_id: &str, asset_key: Option<&str>, offset: u64, limit: u64) -> Result<(Vec<StoredEvent>, u64)>;
    fn get_runs_page(code_location_id: &str, offset: u64, limit: u64, filter: &RunFilter) -> Result<RunsPage>;
    fn get_runs_summary(code_location_id: &str, cutoff_24h_ns: i64) -> Result<RunsSummary>;
    fn link_backfill_run(backfill_id: &str, run_id: &str) -> Result<bool>;
    fn resume_stalled_backfill(backfill_id: &str) -> Result<bool>;
    fn subscribe_table(table: &str) -> Result<futures_util::stream::BoxStream<'static, ()>>;
    }
}

impl StorageBackend for AnyStorage {
    delegate_trait! {
    fn store_event(event: &EventRecord) -> Result<String>;
    fn store_events(events: &[EventRecord]) -> Result<Vec<String>>;
    fn get_events_for_run(run_id: &str) -> Result<Vec<StoredEvent>>;
    fn store_run_logs(logs: &[LogRecord]) -> Result<()>;
    fn get_run_logs(run_id: &str) -> Result<Vec<StoredLog>>;
    fn step_completion(asset_key: &str, run_ids: &[String]) -> Result<(bool, Vec<String>)>;
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
    fn store_tick(tick: &TickRecord) -> Result<String>;
    fn store_ticks_batch(ticks: &[TickRecord]) -> Result<Vec<String>>;
    fn store_condition_tick(tick: &ConditionTickRecord) -> Result<String>;
    fn store_condition_evals_batch(evals: &[ConditionEvalRecord]) -> Result<Vec<String>>;
    fn create_backfill(backfill: &BackfillRecord) -> Result<()>;
    fn update_backfill_status(backfill_id: &str, status: BackfillStatus, end_time: Option<i64>) -> Result<()>;
    fn update_backfill_progress(backfill_id: &str, run_ids: &[String], completed: &[PartitionKey], failed: &[PartitionKey], canceled: &[PartitionKey]) -> Result<()>;
    fn get_backfill(backfill_id: &str) -> Result<Option<BackfillRecord>>;
    fn try_complete_backfill(backfill_id: &str, extra_canceled: &[PartitionKey]) -> Result<Option<BackfillStatus>>;
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
    fn get_completed_step_keys(run_id: &str) -> Result<HashSet<String>>;
    fn get_step_data_versions(run_id: &str) -> Result<HashMap<String, String>>;
    }
}

impl PerCodeLocationStorage for AnyStorage {
    delegate_trait! {
    fn get_events_for_asset(code_location_id: &str, asset_key: &str, limit: usize) -> Result<Vec<StoredEvent>>;
    fn get_latest_materialization(code_location_id: &str, asset_key: &str, partition: Option<&str>) -> Result<Option<StoredEvent>>;
    fn register_assets(code_location_id: &str, records: &[AssetRecord]) -> Result<()>;
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
    fn prune_condition_ticks(code_location_id: &str, max_ticks: usize) -> Result<usize>;
    fn get_condition_evals(code_location_id: &str, asset_key: &str, limit: usize) -> Result<Vec<StoredConditionEval>>;
    fn get_condition_evals_for_tick(code_location_id: &str, tick_id: &str) -> Result<Vec<StoredConditionEval>>;
    fn prune_condition_evals(code_location_id: &str, asset_key: &str, max_evals: usize) -> Result<usize>;
    fn get_partition_events(code_location_id: &str, asset_key: &str, partition_key: &str, limit: usize) -> Result<Vec<StoredEvent>>;
    fn get_materialized_partitions(code_location_id: &str, asset_key: &str) -> Result<Vec<PartitionKey>>;
    fn count_materialized_partitions(code_location_id: &str, asset_key: &str) -> Result<u64>;
    fn count_dynamic_partitions(code_location_id: &str, partitions_def_name: &str) -> Result<u64>;
    fn get_partition_timestamps(code_location_id: &str, asset_key: &str) -> Result<Vec<(PartitionKey, i64)>>;
    fn get_partition_timestamps_since(code_location_id: &str, asset_key: &str, since_timestamp: i64) -> Result<Vec<(PartitionKey, i64)>>;
    fn get_in_progress_partitions(code_location_id: &str, asset_key: &str) -> Result<Vec<PartitionKey>>;
    fn get_failed_partitions(code_location_id: &str, asset_key: &str, materialized: &HashMap<PartitionKey, i64>) -> Result<HashMap<PartitionKey, i64>>;
    fn get_backfills(code_location_id: &str, limit: Option<usize>, status: Option<BackfillStatus>) -> Result<Vec<BackfillRecord>>;
    fn set_pool_limit(code_location_id: &str, pool_key: &str, limit: i32, lease_duration_secs: u32) -> Result<()>;
    fn get_pool_limits(code_location_id: &str) -> Result<Vec<PoolLimit>>;
    fn get_pool_info(code_location_id: &str, pool_key: &str) -> Result<PoolInfo>;
    fn get_all_pool_infos(code_location_id: &str) -> Result<Vec<PoolInfo>>;
    fn claim_concurrency_slots(code_location_id: &str, pools: &[(String, u32)], run_id: &str, step_key: &str, priority: i32, lease_duration_secs: u32) -> Result<ConcurrencyClaimStatus>;
    fn get_pool_slot_holders(code_location_id: &str, pool_key: &str) -> Result<Vec<SlotHolder>>;
    fn get_runs(code_location_id: &str, limit: usize, status: Option<RunStatus>) -> Result<Vec<RunRecord>>;
    fn get_queued_runs(code_location_id: &str) -> Result<Vec<RunRecord>>;
    fn get_runs_since(code_location_id: &str, since_timestamp: i64, status: Option<RunStatus>, order: SortOrder) -> Result<Vec<RunRecord>>;
    fn get_condition_eval_state(code_location_id: &str) -> Result<Option<crate::condition::ConditionEvalState>>;
    fn set_condition_eval_state(code_location_id: &str, state: &crate::condition::ConditionEvalState) -> Result<()>;
    fn get_condition_pending_dispatch(code_location_id: &str) -> Result<Option<crate::condition::PendingDispatch>>;
    fn set_condition_pending_dispatch(code_location_id: &str, pending: &crate::condition::PendingDispatch) -> Result<()>;
    fn get_graph_topology(code_location_id: &str) -> Result<Option<crate::assets::graph::GraphTopology>>;
    fn set_graph_topology(code_location_id: &str, topology: &crate::assets::graph::GraphTopology) -> Result<()>;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn open_memory_url_yields_working_storage() {
        let storage = AnyStorage::open("mem://".parse().unwrap(), Capability::ReadWrite)
            .await
            .expect("open mem://");
        assert!(storage.label().contains("In-Memory"), "{}", storage.label());
        // Prove it is usable, not merely constructed.
        storage.kv_set("k", b"v").await.expect("write");
        assert_eq!(
            storage.kv_get("k").await.expect("read").as_deref(),
            Some(&b"v"[..])
        );
    }

    #[tokio::test]
    async fn open_rocksdb_url_yields_working_storage() {
        let temp = test_temp_dir::test_temp_dir!();
        let url = format!("rocksdb://{}", temp.as_path_untracked().to_str().unwrap());
        let storage = AnyStorage::open(url.parse().unwrap(), Capability::ReadWrite)
            .await
            .expect("open rocksdb://");
        assert!(storage.label().contains("Embedded"), "{}", storage.label());
        storage.kv_set("k", b"v").await.expect("write");
        assert_eq!(
            storage.kv_get("k").await.expect("read").as_deref(),
            Some(&b"v"[..])
        );
    }

    #[tokio::test]
    async fn open_postgres_url_reports_it_is_not_implemented() {
        let err = AnyStorage::open(
            "postgres://db/rivers".parse().unwrap(),
            Capability::ReadWrite,
        )
        .await
        .expect_err("postgres backend does not exist yet");
        let msg = err.to_string();
        assert!(
            msg.contains("PostgreSQL"),
            "error should name the backend: {msg}"
        );
        assert!(
            msg.contains("postgres://db/rivers"),
            "error should echo the url: {msg}"
        );
    }
}
