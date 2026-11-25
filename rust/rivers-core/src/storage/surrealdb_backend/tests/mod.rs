use std::collections::HashSet;

use super::super::any::AnyStorage;
use super::super::postgres_backend::PostgresStorage;
use super::super::retry::StorageRetryConfig;
use super::super::{
    AssetRecord, AssetScope, BackfillFailurePolicy, BackfillFilter, BackfillRecord, BackfillStatus,
    BackfillStrategy, BlockReason, ConcurrencyClaimStatus, ConditionEvalRecord,
    ConditionTickRecord, DEFAULT_CODE_LOCATION_ID, EventType, LaunchedBy, LogRecord,
    PerCodeLocationStorage, RunFilter, RunOutcome, RunRecord, RunStatus, StaleStatus,
    StoredConditionEval, StoredConditionTick, StoredEvent, StoredTick, TickRecord,
};
use super::*;
use crate::assets::graph::GraphTopology;

mod asset_events;
mod asset_versions;
mod backfills;
mod claims;
mod consolidation;
mod coverage;
mod crud;
mod directory;
mod event_write_contention;
mod executor_patterns;
mod failure_rows;
mod holds;
mod isolation;
mod live_queries;
mod message_limit;
mod observations;
mod partition_reads;
mod pools;
mod run_progress;
mod run_queue;
mod run_status;
mod span_keys;
mod ui_queries;

/// The store under test: PostgreSQL when `RIVERS_TEST_POSTGRES_URL` is set,
/// else in-memory SurrealDB.
async fn make_storage() -> AnyStorage {
    AnyStorage::new_memory()
        .await
        .expect("failed to create storage")
}

/// In-memory SurrealDB, for a test of SurrealDB itself.
async fn make_surreal() -> SurrealStorage {
    SurrealStorage::new_memory()
        .await
        .expect("failed to create in-memory storage")
}

/// A store that sees every conflict of two writers: SurrealDB on disk in
/// `dir` (the in-memory store misses some), or PostgreSQL when its URL is
/// set. The caller keeps `dir` alive.
async fn make_disk_storage(dir: &std::path::Path) -> AnyStorage {
    make_disk_storage_with_retry(dir, StorageRetryConfig::default()).await
}

async fn make_disk_storage_with_retry(
    dir: &std::path::Path,
    retry: StorageRetryConfig,
) -> AnyStorage {
    match PostgresStorage::test_url() {
        Some(url) => PostgresStorage::connect_test_schema_with_retry(&url, retry)
            .await
            .expect("failed to create PostgreSQL storage")
            .into(),
        None => SurrealStorage::new_embedded_with_retry(
            dir.to_str().unwrap(),
            retry,
            Capability::ReadWrite,
        )
        .await
        .expect("failed to create embedded storage")
        .into(),
    }
}

/// Add a slot row of the default code location, as a claim writes it.
async fn insert_slot(
    storage: &AnyStorage,
    pool_key: &str,
    run_id: &str,
    step_key: &str,
    slots: i64,
    claimed_at: i64,
    lease_expires_at: i64,
) {
    match storage {
        AnyStorage::Surreal(surreal) => {
            surreal
                .db
                .query(
                    "INSERT INTO concurrency_slots { \
                         pool_key: $pool_key, run_id: $run_id, step_key: $step_key, \
                         slots_consumed: $slots, claimed_at: $now, \
                         lease_expires_at: $expires, last_heartbeat: $now \
                     }",
                )
                .bind(("pool_key", pool_key.to_string()))
                .bind(("run_id", run_id.to_string()))
                .bind(("step_key", step_key.to_string()))
                .bind(("slots", slots))
                .bind(("now", claimed_at))
                .bind(("expires", lease_expires_at))
                .await
                .unwrap()
                .check()
                .unwrap();
        }
        AnyStorage::Postgres(postgres) => postgres
            .insert_slot(
                pool_key,
                run_id,
                step_key,
                slots,
                claimed_at,
                lease_expires_at,
            )
            .await
            .unwrap(),
    }
}

/// Add a step of the default code location that waits for `pool_key`.
async fn insert_pending_step(
    storage: &AnyStorage,
    pool_key: &str,
    run_id: &str,
    step_key: &str,
    enqueued_at: i64,
) {
    match storage {
        AnyStorage::Surreal(surreal) => {
            surreal
                .db
                .query(
                    "INSERT INTO pending_steps { \
                         pool_key: $pool_key, run_id: $run_id, step_key: $step_key, \
                         priority: 0, enqueued_at: $now, block_reason: 'PoolFull' \
                     }",
                )
                .bind(("pool_key", pool_key.to_string()))
                .bind(("run_id", run_id.to_string()))
                .bind(("step_key", step_key.to_string()))
                .bind(("now", enqueued_at))
                .await
                .unwrap()
                .check()
                .unwrap();
        }
        AnyStorage::Postgres(postgres) => postgres
            .insert_pending_step(pool_key, run_id, step_key, enqueued_at)
            .await
            .unwrap(),
    }
}

fn make_event(asset_key: &str, run_id: &str, ts: i64) -> EventRecord {
    EventRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type: EventType::Materialization { data_version: None },
        asset_key: Some(asset_key.to_string()),
        run_id: run_id.to_string(),
        partition_key: None,
        timestamp: ts,
        metadata: vec![],
        input_data_versions: vec![],
    }
}

fn make_asset_record(key: &str) -> AssetRecord {
    AssetRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
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

/// Register assets before store_event.
async fn register(storage: &impl PerCodeLocationStorage, keys: &[&str]) {
    let records: Vec<AssetRecord> = keys.iter().map(|k| make_asset_record(k)).collect();
    storage
        .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, records)
        .await
        .unwrap();
}

fn make_backfill(id: &str, status: BackfillStatus, create_time: i64) -> BackfillRecord {
    BackfillRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        backfill_id: id.to_string(),
        status,
        strategy: BackfillStrategy::MultiRun,
        failure_policy: BackfillFailurePolicy::Continue,
        asset_selection: vec!["a".to_string()],
        job_name: None,
        num_partitions: 0,
        planned_runs: 0,
        run_ids: vec![],
        completed_partitions: 0,
        failed_partitions: 0,
        canceled_partitions: 0,
        max_concurrency: 1,
        tags: vec![],
        create_time,
        end_time: None,
        error: None,
        launched_by: LaunchedBy::default(),
        action: None,
        config: None,
    }
}

fn minimal_run(run_id: &str, status: RunStatus) -> RunRecord {
    RunRecord {
        run_id: run_id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("j".into()),
        status,
        start_time: 1000,
        end_time: None,
        execution_start_time: None,
        tags: vec![],
        node_names: vec![],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    }
}

fn mk_isolation_backfill(
    id: &str,
    cl: &str,
    status: BackfillStatus,
    create_time: i64,
) -> BackfillRecord {
    BackfillRecord {
        backfill_id: id.to_string(),
        code_location_id: cl.to_string(),
        status,
        strategy: BackfillStrategy::MultiRun,
        failure_policy: BackfillFailurePolicy::Continue,
        asset_selection: vec!["a".to_string()],
        job_name: None,
        num_partitions: 0,
        planned_runs: 0,
        run_ids: vec![],
        completed_partitions: 0,
        failed_partitions: 0,
        canceled_partitions: 0,
        max_concurrency: 1,
        tags: vec![],
        create_time,
        end_time: None,
        error: None,
        launched_by: LaunchedBy::default(),
        action: None,
        config: None,
    }
}
