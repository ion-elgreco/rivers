use surrealdb::types::{Bytes, RecordId, SurrealValue};

use crate::storage::{
    ConditionEvalRecord, ConditionTickRecord, EventRecord, EventType, LogRecord, PartitionKey,
    StoredConditionEval, StoredConditionTick, StoredEvent, StoredLog, StoredTick, TickRecord,
};

#[derive(Debug, SurrealValue)]
pub(super) struct DbKv {
    key: String,
    pub(super) value: Bytes,
}

#[derive(Debug, SurrealValue)]
pub(super) struct DbDynamicPartition {
    pub(super) code_location_id: String,
    pub(super) partitions_def_name: String,
    pub(super) partition_key: String,
    pub(super) create_timestamp: i64,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
pub(super) struct DbEventWrite {
    /// Client-generated so a retried closure re-inserts the same record id —
    /// `INSERT IGNORE` then makes the replay a no-op instead of a duplicate.
    pub(super) id: RecordId,
    pub(super) code_location_id: String,
    pub(super) event_type: String,
    pub(super) asset_key: Option<String>,
    pub(super) run_id: String,
    pub(super) partition_key: Option<PartitionKey>,
    pub(super) timestamp: i64,
    pub(super) sort_order: i64,
    pub(super) metadata: Vec<(String, String)>,
    pub(super) data_version: Option<String>,
    pub(super) code_version: Option<String>,
    pub(super) input_data_versions: Vec<(String, String)>,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
pub(super) struct DbRunLogWrite {
    code_location_id: String,
    run_id: String,
    step_key: String,
    timestamp: i64,
    stdout: Option<String>,
    stderr: Option<String>,
    logs: Option<String>,
    traceback: Option<String>,
}

impl From<&LogRecord> for DbRunLogWrite {
    fn from(l: &LogRecord) -> Self {
        Self {
            code_location_id: l.code_location_id.clone(),
            run_id: l.run_id.clone(),
            step_key: l.step_key.clone(),
            timestamp: l.timestamp,
            stdout: l.stdout.clone(),
            stderr: l.stderr.clone(),
            logs: l.logs.clone(),
            traceback: l.traceback.clone(),
        }
    }
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
pub(super) struct DbStoredRunLog {
    id: RecordId,
    #[serde(default = "crate::storage::default_code_location_id")]
    code_location_id: String,
    run_id: String,
    step_key: String,
    timestamp: i64,
    stdout: Option<String>,
    stderr: Option<String>,
    logs: Option<String>,
    #[serde(default)]
    traceback: Option<String>,
}

impl DbStoredRunLog {
    pub(super) fn into_stored_log(self) -> StoredLog {
        StoredLog {
            id: self.id,
            code_location_id: self.code_location_id,
            run_id: self.run_id,
            step_key: self.step_key,
            timestamp: self.timestamp,
            stdout: self.stdout,
            stderr: self.stderr,
            logs: self.logs,
            traceback: self.traceback,
        }
    }
}

/// One partition deletion tombstone, written by [`RECORD_PARTITION_TOMBSTONES`].
#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
pub(super) struct DbPartitionDeletion {
    pub(super) code_location_id: String,
    pub(super) asset_key: String,
    pub(super) partition_key: PartitionKey,
    pub(super) timestamp: i64,
}

/// One `asset_partitions` row, upserted via [`UPSERT_ASSET_PARTITIONS`] in
/// the same transaction as its asset's row update.
#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
pub(super) struct DbAssetPartitionWrite {
    pub(super) code_location_id: String,
    pub(super) asset_key: String,
    pub(super) partition_key: PartitionKey,
    pub(super) last_event_id: String,
    pub(super) last_run_id: String,
    pub(super) last_timestamp: i64,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
pub(super) struct DbStoredEvent {
    id: RecordId,
    #[serde(default = "crate::storage::default_code_location_id")]
    code_location_id: String,
    event_type: String,
    asset_key: Option<String>,
    run_id: String,
    partition_key: Option<PartitionKey>,
    timestamp: i64,
    sort_order: i64,
    metadata: Vec<(String, String)>,
    data_version: Option<String>,
    #[serde(default)]
    code_version: Option<String>,
    #[serde(default)]
    input_data_versions: Vec<(String, String)>,
}

/// Projection behind `SurrealStorage::step_outcomes`. A step event always
/// carries an `asset_key`, but the column is optional on the table, so a row
/// without one is dropped rather than guessed at.
#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
pub(super) struct DbStepOutcome {
    pub(super) asset_key: Option<String>,
    pub(super) run_id: String,
    pub(super) event_type: String,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
pub(super) struct DbTickWrite {
    code_location_id: String,
    automation_name: String,
    automation_type: String,
    status: String,
    timestamp: i64,
    run_ids: Vec<String>,
    backfill_ids: Vec<String>,
    skip_reason: Option<String>,
    error: Option<String>,
    cursor: Option<String>,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
pub(super) struct DbStoredTick {
    pub(super) id: RecordId,
    #[serde(default = "crate::storage::default_code_location_id")]
    code_location_id: String,
    automation_name: String,
    automation_type: String,
    status: String,
    timestamp: i64,
    run_ids: Vec<String>,
    #[serde(default)]
    backfill_ids: Vec<String>,
    skip_reason: Option<String>,
    error: Option<String>,
    cursor: Option<String>,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
pub(super) struct DbConditionTickWrite {
    code_location_id: String,
    timestamp: i64,
    total_evaluated: i64,
    total_fired: i64,
    eval_duration_us: i64,
    run_ids: Vec<String>,
    backfill_ids: Vec<String>,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
pub(super) struct DbStoredConditionTick {
    pub(super) id: RecordId,
    #[serde(default = "crate::storage::default_code_location_id")]
    code_location_id: String,
    timestamp: i64,
    total_evaluated: i64,
    total_fired: i64,
    eval_duration_us: i64,
    run_ids: Vec<String>,
    #[serde(default)]
    backfill_ids: Vec<String>,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
pub(super) struct DbConditionEvalWrite {
    code_location_id: String,
    asset_key: String,
    tick_id: String,
    timestamp: i64,
    fired: bool,
    eval_duration_us: i64,
    run_ids: Vec<String>,
    tree_json: Bytes,
    selection_json: Option<Bytes>,
}

#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
pub(super) struct DbStoredConditionEval {
    pub(super) id: RecordId,
    #[serde(default = "crate::storage::default_code_location_id")]
    code_location_id: String,
    asset_key: String,
    tick_id: String,
    timestamp: i64,
    fired: bool,
    eval_duration_us: i64,
    run_ids: Vec<String>,
    tree_json: Bytes,
    #[serde(default)]
    selection_json: Option<Bytes>,
}

impl From<&ConditionTickRecord> for DbConditionTickWrite {
    fn from(t: &ConditionTickRecord) -> Self {
        Self {
            code_location_id: t.code_location_id.clone(),
            timestamp: t.timestamp,
            total_evaluated: t.total_evaluated as i64,
            total_fired: t.total_fired as i64,
            eval_duration_us: t.eval_duration_us as i64,
            run_ids: t.run_ids.clone(),
            backfill_ids: t.backfill_ids.clone(),
        }
    }
}

impl DbStoredConditionTick {
    pub(super) fn into_stored(self) -> StoredConditionTick {
        StoredConditionTick {
            id: self.id,
            code_location_id: self.code_location_id,
            timestamp: self.timestamp,
            total_evaluated: self.total_evaluated as u32,
            total_fired: self.total_fired as u32,
            eval_duration_us: self.eval_duration_us as u64,
            run_ids: self.run_ids,
            backfill_ids: self.backfill_ids,
        }
    }
}

impl From<&ConditionEvalRecord> for DbConditionEvalWrite {
    fn from(e: &ConditionEvalRecord) -> Self {
        Self {
            code_location_id: e.code_location_id.clone(),
            asset_key: e.asset_key.clone(),
            tick_id: e.tick_id.clone(),
            timestamp: e.timestamp,
            fired: e.fired,
            eval_duration_us: e.eval_duration_us as i64,
            run_ids: e.run_ids.clone(),
            tree_json: Bytes::from(e.tree_json.clone()),
            selection_json: e.selection_json.as_ref().map(|b| Bytes::from(b.clone())),
        }
    }
}

impl DbStoredConditionEval {
    pub(super) fn into_stored(self) -> StoredConditionEval {
        StoredConditionEval {
            id: self.id,
            code_location_id: self.code_location_id,
            asset_key: self.asset_key,
            tick_id: self.tick_id,
            timestamp: self.timestamp,
            fired: self.fired,
            eval_duration_us: self.eval_duration_us as u64,
            run_ids: self.run_ids,
            tree_json: self.tree_json.to_vec(),
            selection_json: self.selection_json.map(|b| b.to_vec()),
        }
    }
}

impl From<&TickRecord> for DbTickWrite {
    fn from(t: &TickRecord) -> Self {
        Self {
            code_location_id: t.code_location_id.clone(),
            automation_name: t.automation_name.clone(),
            automation_type: t.automation_type.clone(),
            status: t.status.clone(),
            timestamp: t.timestamp,
            run_ids: t.run_ids.clone(),
            backfill_ids: t.backfill_ids.clone(),
            skip_reason: t.skip_reason.clone(),
            error: t.error.clone(),
            cursor: t.cursor.clone(),
        }
    }
}

impl DbStoredTick {
    pub(super) fn into_stored_tick(self) -> StoredTick {
        StoredTick {
            id: self.id,
            code_location_id: self.code_location_id,
            automation_name: self.automation_name,
            automation_type: self.automation_type,
            status: self.status,
            timestamp: self.timestamp,
            run_ids: self.run_ids,
            backfill_ids: self.backfill_ids,
            skip_reason: self.skip_reason,
            error: self.error,
            cursor: self.cursor,
        }
    }
}

impl DbEventWrite {
    pub(super) fn from_event(e: &EventRecord, id: RecordId) -> Self {
        Self {
            id,
            code_location_id: e.code_location_id.clone(),
            event_type: e.event_type.type_name().to_string(),
            asset_key: e.asset_key.clone(),
            run_id: e.run_id.clone(),
            partition_key: e.partition_key.clone(),
            timestamp: e.timestamp,
            sort_order: e.event_type.sort_order(),
            metadata: e.metadata.clone(),
            data_version: e.event_type.data_version().map(|s| s.to_string()),
            code_version: None,
            input_data_versions: Vec::new(),
        }
    }
}

impl DbStoredEvent {
    pub(super) fn into_stored_event(self) -> StoredEvent {
        let event_type = EventType::from_type_name(&self.event_type, self.data_version)
            .unwrap_or(EventType::StepFailure);
        StoredEvent {
            id: self.id,
            event_type,
            asset_key: self.asset_key,
            run_id: self.run_id,
            partition_key: self.partition_key,
            timestamp: self.timestamp,
            metadata: self.metadata,
            code_version: self.code_version,
            input_data_versions: self.input_data_versions,
        }
    }
}
