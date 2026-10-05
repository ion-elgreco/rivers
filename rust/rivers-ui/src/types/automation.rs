use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleRecord {
    pub name: String,
    pub cron_schedule: String,
    pub cron_description: Option<String>,
    pub job_name: String,
    pub status: String,
    pub timezone: Option<String>,
    pub description: Option<String>,
    pub tags: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensorRecord {
    pub name: String,
    pub job_name: Option<String>,
    pub status: String,
    pub minimum_interval: Option<String>,
    pub description: Option<String>,
    pub asset_selection: Vec<String>,
    pub tags: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRecord {
    pub name: String,
    pub asset_selection: Vec<String>,
    pub executor_type: String,
    /// The verb this job's runs execute; `None` means materialize.
    #[serde(default)]
    pub action: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickRecord {
    pub id: String,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeStatus {
    True,
    False,
    Skipped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalNodeResult {
    pub node_idx: u32,
    pub label: String,
    pub node_type: String,
    pub status: NodeStatus,
    pub children: Vec<EvalNodeResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_partitions: Option<usize>,
}

/// Detail of an expanded condition tick — per-asset evaluations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConditionTickDetail {
    pub evals: Vec<ConditionEvalRecord>,
}

/// A global condition evaluation tick — summary of one daemon evaluation cycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionTickRecord {
    pub id: String,
    pub timestamp: i64,
    pub total_evaluated: u32,
    pub total_fired: u32,
    pub eval_duration_us: u64,
    pub run_ids: Vec<String>,
    /// Populated at read-time by joining `BackfillRecord.create_time` within a
    /// short window after `timestamp`. Empty in storage.
    #[serde(default)]
    pub backfill_ids: Vec<String>,
}

/// A per-asset condition evaluation record linked to a global tick.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionEvalRecord {
    pub id: String,
    pub asset_key: String,
    pub tick_id: String,
    pub timestamp: i64,
    pub fired: bool,
    pub eval_duration_us: u64,
    /// Populated at read-time from the join `RunRecord.node_names → run_id`.
    /// Stored eval records always have this empty.
    #[serde(default)]
    pub run_ids: Vec<String>,
    /// Populated at read-time from the join `BackfillRecord.asset_selection → backfill_id`.
    /// Only backfill-driven materializations (multi-partition) have these.
    #[serde(default)]
    pub backfill_ids: Vec<String>,
    pub tree: EvalNodeResult,
    /// Which partitions were selected (None for unpartitioned assets).
    pub selected_partitions: Option<Vec<String>>,
}
