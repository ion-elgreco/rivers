use super::default_code_location_id;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TickRecord {
    /// Owning code location; index composite on `(code_location_id, automation_name)`.
    #[serde(default = "default_code_location_id")]
    pub code_location_id: String,
    pub automation_name: String,
    pub automation_type: String, // "Schedule" or "Sensor"
    pub status: String,          // "Success", "Skipped", "Failed"
    pub timestamp: i64,
    pub run_ids: Vec<String>,
    /// Backfills spawned by this tick (from a returned `BackfillRequest`).
    #[serde(default)]
    pub backfill_ids: Vec<String>,
    pub skip_reason: Option<String>,
    pub error: Option<String>,
    pub cursor: Option<String>,
}

/// Stored tick with its database-assigned ID.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredTick {
    pub id: surrealdb::types::RecordId,
    #[serde(default = "default_code_location_id")]
    pub code_location_id: String,
    pub automation_name: String,
    pub automation_type: String,
    pub status: String,
    pub timestamp: i64,
    pub run_ids: Vec<String>,
    #[serde(default)]
    pub backfill_ids: Vec<String>,
    pub skip_reason: Option<String>,
    pub error: Option<String>,
    pub cursor: Option<String>,
}

/// A global condition evaluation tick — groups all per-asset evaluations from one daemon cycle.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ConditionTickRecord {
    /// Owning code location; each daemon's cycle counter advances independently.
    #[serde(default = "default_code_location_id")]
    pub code_location_id: String,
    pub timestamp: i64,
    pub total_evaluated: u32,
    pub total_fired: u32,
    pub eval_duration_us: u64,
    pub run_ids: Vec<String>,
    /// Backfills this tick spawned.
    #[serde(default)]
    pub backfill_ids: Vec<String>,
}

/// Stored global condition tick with database-assigned ID.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredConditionTick {
    pub id: surrealdb::types::RecordId,
    #[serde(default = "default_code_location_id")]
    pub code_location_id: String,
    pub timestamp: i64,
    pub total_evaluated: u32,
    pub total_fired: u32,
    pub eval_duration_us: u64,
    pub run_ids: Vec<String>,
    #[serde(default)]
    pub backfill_ids: Vec<String>,
}

/// A condition evaluation record for one asset in one tick.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ConditionEvalRecord {
    /// Owning code location; eval rows filter by `(code_location_id, asset_key)`.
    #[serde(default = "default_code_location_id")]
    pub code_location_id: String,
    pub asset_key: String,
    pub tick_id: String,
    pub timestamp: i64,
    pub fired: bool,
    pub eval_duration_us: u64,
    pub run_ids: Vec<String>,
    /// Serialized evaluation tree (JSON bytes of `condition::EvalNodeResult`).
    pub tree_json: Vec<u8>,
    /// Serialized partition selection (JSON bytes of `condition::PartitionSelection`).
    #[serde(default)]
    pub selection_json: Option<Vec<u8>>,
}

/// Stored condition evaluation with database-assigned ID.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredConditionEval {
    pub id: surrealdb::types::RecordId,
    #[serde(default = "default_code_location_id")]
    pub code_location_id: String,
    pub asset_key: String,
    pub tick_id: String,
    pub timestamp: i64,
    pub fired: bool,
    pub eval_duration_us: u64,
    pub run_ids: Vec<String>,
    pub tree_json: Vec<u8>,
    #[serde(default)]
    pub selection_json: Option<Vec<u8>>,
}
