use std::collections::HashSet;

use super::super::{
    AssetRecord, AssetScope, BackfillFailurePolicy, BackfillFilter, BackfillRecord, BackfillStatus,
    BackfillStrategy, BlockReason, ConcurrencyClaimStatus, ConditionEvalRecord,
    ConditionTickRecord, DEFAULT_CODE_LOCATION_ID, EventType, LaunchedBy, LogRecord,
    PerCodeLocationStorage, RunFilter, RunOutcome, RunRecord, RunStatus, StaleStatus,
    StoredConditionEval, StoredConditionTick, StoredEvent, StoredTick, TickRecord,
};
use super::*;
use crate::assets::graph::GraphTopology;

mod backfills;
mod claims;
mod consolidation;
mod coverage;
mod crud;
mod executor_patterns;
mod isolation;
mod observations;
mod pools;
mod run_progress;
mod run_queue;
mod ui_queries;

async fn make_storage() -> SurrealStorage {
    SurrealStorage::new_memory()
        .await
        .expect("failed to create in-memory storage")
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
async fn register(storage: &SurrealStorage, keys: &[&str]) {
    let records: Vec<AssetRecord> = keys.iter().map(|k| make_asset_record(k)).collect();
    storage
        .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, &records)
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
        partition_keys: vec![],
        run_ids: vec![],
        completed_partitions: vec![],
        failed_partitions: vec![],
        canceled_partitions: vec![],
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
        partition_keys: vec![],
        run_ids: vec![],
        completed_partitions: vec![],
        failed_partitions: vec![],
        canceled_partitions: vec![],
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
