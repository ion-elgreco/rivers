//! Storage trait and types for persisting orchestration state.

pub mod retry;
pub mod surrealdb_backend;

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use surrealdb::types::{Error as SurrealError, Kind, SurrealValue, Value};

/// Well-known rivers tag keys used on `RunRecord.tags` and run/event metadata.
pub mod tag_keys {
    /// Run priority. Higher = dequeued first.
    pub const PRIORITY: &str = "rivers/priority";
    /// Set on a backfill's tags when created via `rerun_backfill` —
    /// points to the original backfill id.
    pub const RERUN_OF: &str = "rivers/rerun_of";
}

// ── Staleness types ──

/// Whether an asset needs re-materialization.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub enum StaleStatus {
    UpToDate,
    Stale,
    #[default]
    Missing,
}

impl SurrealValue for StaleStatus {
    fn kind_of() -> Kind {
        String::kind_of()
    }

    fn into_value(self) -> Value {
        let s = match self {
            Self::UpToDate => "UpToDate",
            Self::Stale => "Stale",
            Self::Missing => "Missing",
        };
        s.to_string().into_value()
    }

    fn from_value(value: Value) -> std::result::Result<Self, SurrealError> {
        let s = String::from_value(value)?;
        match s.as_str() {
            "UpToDate" => Ok(Self::UpToDate),
            "Stale" => Ok(Self::Stale),
            "Missing" => Ok(Self::Missing),
            _ => Err(SurrealError::internal(format!("unknown StaleStatus: {s}"))),
        }
    }
}

/// Category of staleness cause.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum StaleCauseCategory {
    /// Asset's code version changed since last materialization.
    Code,
    /// Upstream dependency has newer data.
    Data {
        /// The upstream asset that caused staleness.
        dependency: String,
    },
}

impl StaleCauseCategory {
    /// Return the dependency name if this is a Data cause.
    pub fn dependency(&self) -> Option<&str> {
        match self {
            Self::Data { dependency } => Some(dependency),
            Self::Code => None,
        }
    }
}

impl SurrealValue for StaleCauseCategory {
    fn kind_of() -> Kind {
        String::kind_of()
    }

    fn into_value(self) -> Value {
        let s = match self {
            Self::Code => "Code".to_string(),
            Self::Data { dependency } => format!("Data:{}", dependency),
        };
        s.into_value()
    }

    fn from_value(value: Value) -> std::result::Result<Self, SurrealError> {
        let s = String::from_value(value)?;
        if s == "Code" {
            Ok(Self::Code)
        } else if let Some(dep) = s.strip_prefix("Data:") {
            Ok(Self::Data {
                dependency: dep.to_string(),
            })
        } else {
            Err(SurrealError::internal(format!(
                "unknown StaleCauseCategory: {s}"
            )))
        }
    }
}

/// A single reason why an asset is stale.
#[derive(Debug, Clone, PartialEq, SurrealValue, serde::Serialize, serde::Deserialize)]
pub struct StaleCause {
    pub asset_key: String,
    pub category: StaleCauseCategory,
    pub reason: String,
}

// ── Event types ──

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum EventType {
    Materialization {
        data_version: Option<String>,
    },
    Observation {
        data_version: Option<String>,
    },
    StepStart,
    StepSuccess,
    StepFailure,
    /// Captured stdout/stderr output from a step execution.
    LogOutput,
    // ── Concurrency observability events ──
    /// Run entered the queue (Queued status).
    RunQueued,
    /// Coordinator dequeued a run (Queued → NotStarted).
    RunDequeued,
    /// Step successfully claimed pool slots.
    StepSlotClaimed,
    /// Step waiting for pool slots (claim returned Pending).
    StepSlotWaiting,
    /// Background lease renewal succeeded.
    StepSlotRenewed,
    /// Step released pool slots.
    StepSlotReleased,
}

impl EventType {
    /// Returns the data version if this is a Materialization or Observation event.
    pub fn data_version(&self) -> Option<&str> {
        match self {
            Self::Materialization { data_version } | Self::Observation { data_version } => {
                data_version.as_deref()
            }
            _ => None,
        }
    }

    /// Returns the type name as a string (for DB serialization).
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Materialization { .. } => "Materialization",
            Self::Observation { .. } => "Observation",
            Self::StepStart => "StepStart",
            Self::StepSuccess => "StepSuccess",
            Self::StepFailure => "StepFailure",
            Self::LogOutput => "LogOutput",
            Self::RunQueued => "RunQueued",
            Self::RunDequeued => "RunDequeued",
            Self::StepSlotClaimed => "StepSlotClaimed",
            Self::StepSlotWaiting => "StepSlotWaiting",
            Self::StepSlotRenewed => "StepSlotRenewed",
            Self::StepSlotReleased => "StepSlotReleased",
        }
    }

    /// Reconstruct an EventType from a type name string and optional data_version.
    pub fn from_type_name(
        name: &str,
        data_version: Option<String>,
    ) -> std::result::Result<Self, String> {
        match name {
            "Materialization" => Ok(Self::Materialization { data_version }),
            "Observation" => Ok(Self::Observation { data_version }),
            "StepStart" => Ok(Self::StepStart),
            "StepSuccess" => Ok(Self::StepSuccess),
            "StepFailure" => Ok(Self::StepFailure),
            "LogOutput" => Ok(Self::LogOutput),
            "RunQueued" => Ok(Self::RunQueued),
            "RunDequeued" => Ok(Self::RunDequeued),
            "StepSlotClaimed" => Ok(Self::StepSlotClaimed),
            "StepSlotWaiting" => Ok(Self::StepSlotWaiting),
            "StepSlotRenewed" => Ok(Self::StepSlotRenewed),
            "StepSlotReleased" => Ok(Self::StepSlotReleased),
            _ => Err(format!("unknown EventType: {name}")),
        }
    }

    pub fn is_materialization(&self) -> bool {
        matches!(self, Self::Materialization { .. })
    }

    pub fn is_observation(&self) -> bool {
        matches!(self, Self::Observation { .. })
    }

    /// Sort priority within the same timestamp.
    /// Lower = earlier. Ensures
    /// StepStart < LogOutput < Observation < Materialization < StepSuccess/Failure.
    /// Concurrency events sort after step lifecycle events.
    pub fn sort_order(&self) -> i64 {
        match self {
            Self::StepStart => 0,
            Self::LogOutput => 1,
            Self::Observation { .. } => 2,
            Self::Materialization { .. } => 3,
            Self::StepSuccess | Self::StepFailure => 4,
            Self::RunQueued | Self::RunDequeued => 5,
            Self::StepSlotClaimed
            | Self::StepSlotWaiting
            | Self::StepSlotRenewed
            | Self::StepSlotReleased => 6,
        }
    }
}

/// Partition key stored alongside events and asset partition records.
///
/// Stored natively in SurrealDB as an object with `variant` + `keys` fields,
/// avoiding fragile string serialization.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum PartitionKey {
    /// Single-dimension key (e.g. `"2024-01-01"` or `["a", "b"]`).
    Single { keys: Vec<String> },
    /// Multi-dimension key (e.g. `{"date": ["2024-01-01"], "region": ["us"]}`).
    Multi { dims: Vec<(String, Vec<String>)> },
    /// Explicit set of concrete keys (each member a single Single/Multi key,
    /// never nested). Internal/transport only: bundles a sparse backfill group
    /// no cartesian Multi can express; expanded via `members()` at execution.
    Set { keys: Vec<PartitionKey> },
}

impl PartitionKey {
    /// Expand a possibly-batched key into its individual single-valued members
    /// (`Single`: one per value; `Multi`: the cartesian product).
    pub fn members(&self) -> Vec<PartitionKey> {
        // The full expansion is `members_preview` with no cap; keep the
        // cartesian logic in one place.
        self.members_preview(usize::MAX)
    }

    /// Number of members this key expands to, without building them.
    pub fn member_count(&self) -> usize {
        match self {
            Self::Single { keys } => keys.len(),
            Self::Multi { dims } => dims
                .iter()
                .map(|(_, vs)| vs.len())
                .fold(1usize, |a, n| a.saturating_mul(n)),
            Self::Set { keys } => keys.iter().map(Self::member_count).sum(),
        }
    }

    /// The first `limit` members in `members()` order, without building the rest.
    pub fn members_preview(&self, limit: usize) -> Vec<PartitionKey> {
        if limit == 0 {
            return Vec::new();
        }
        match self {
            Self::Single { keys } => keys
                .iter()
                .take(limit)
                .map(|k| Self::Single {
                    keys: vec![k.clone()],
                })
                .collect(),
            Self::Multi { dims } => {
                let mut sorted = dims.clone();
                sorted.sort_by(|a, b| a.0.cmp(&b.0));
                // Extend the cartesian product one dimension at a time, capping
                // at `limit` after each (`take` is lazy, so we never build the
                // full product of a huge key).
                let mut combos: Vec<Vec<(String, Vec<String>)>> = vec![Vec::new()];
                for (name, vals) in &sorted {
                    combos = combos
                        .into_iter()
                        .flat_map(|combo| {
                            vals.iter().map(move |v| {
                                let mut c = combo.clone();
                                c.push((name.clone(), vec![v.clone()]));
                                c
                            })
                        })
                        .take(limit)
                        .collect();
                }
                combos
                    .into_iter()
                    .map(|dims| Self::Multi { dims })
                    .collect()
            }
            Self::Set { keys } => {
                let mut out = Vec::new();
                for k in keys {
                    for m in k.members_preview(limit - out.len()) {
                        out.push(m);
                        if out.len() >= limit {
                            return out;
                        }
                    }
                }
                out
            }
        }
    }

    /// Serialize to JSON for CLI args / K8s CRD fields.
    /// Single: `{"single":["2025-01-16"]}`, Multi: `{"multi":{"region":["us"],"date":["2025-03"]}}`.
    pub fn to_json(&self) -> String {
        match self {
            Self::Single { keys } => serde_json::json!({"single": keys}).to_string(),
            Self::Multi { dims } => {
                let map: std::collections::BTreeMap<&str, &Vec<String>> =
                    dims.iter().map(|(k, v)| (k.as_str(), v)).collect();
                serde_json::json!({"multi": map}).to_string()
            }
            Self::Set { keys } => {
                let members: Vec<serde_json::Value> = keys
                    .iter()
                    .filter_map(|k| serde_json::from_str(&k.to_json()).ok())
                    .collect();
                serde_json::json!({ "set": members }).to_string()
            }
        }
    }

    /// Deserialize from JSON produced by `to_json()`.
    pub fn from_json(s: &str) -> Result<Self> {
        let v: serde_json::Value = serde_json::from_str(s)
            .map_err(|e| anyhow::anyhow!("invalid partition key JSON: {e}"))?;
        if let Some(keys) = v.get("single") {
            let keys: Vec<String> = serde_json::from_value(keys.clone())
                .map_err(|e| anyhow::anyhow!("invalid single partition key: {e}"))?;
            Ok(Self::Single { keys })
        } else if let Some(multi) = v.get("multi") {
            let map: std::collections::BTreeMap<String, Vec<String>> =
                serde_json::from_value(multi.clone())
                    .map_err(|e| anyhow::anyhow!("invalid multi partition key: {e}"))?;
            Ok(Self::Multi {
                dims: map.into_iter().collect(),
            })
        } else if let Some(set) = v.get("set") {
            let arr = set
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("invalid set partition key: expected array"))?;
            let keys = arr
                .iter()
                .map(|m| Self::from_json(&m.to_string()))
                .collect::<Result<Vec<_>>>()?;
            Ok(Self::Set { keys })
        } else {
            anyhow::bail!("partition key JSON must have 'single', 'multi', or 'set' key")
        }
    }
}

impl PartialEq for PartitionKey {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Single { keys: a }, Self::Single { keys: b }) => a == b,
            (Self::Multi { dims: a }, Self::Multi { dims: b }) => {
                if a.len() != b.len() {
                    return false;
                }
                let mut a_sorted = a.clone();
                let mut b_sorted = b.clone();
                a_sorted.sort_by(|x, y| x.0.cmp(&y.0));
                b_sorted.sort_by(|x, y| x.0.cmp(&y.0));
                a_sorted == b_sorted
            }
            (Self::Set { keys: a }, Self::Set { keys: b }) => a == b,
            _ => false,
        }
    }
}

impl Eq for PartitionKey {}

impl std::hash::Hash for PartitionKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Single { keys } => keys.hash(state),
            Self::Multi { dims } => {
                let mut sorted = dims.clone();
                sorted.sort_by(|a, b| a.0.cmp(&b.0));
                sorted.hash(state);
            }
            Self::Set { keys } => keys.hash(state),
        }
    }
}

impl SurrealValue for PartitionKey {
    fn kind_of() -> Kind {
        <std::collections::HashMap<String, Value>>::kind_of()
    }

    fn into_value(self) -> Value {
        let mut map = std::collections::BTreeMap::new();
        match self {
            Self::Single { keys } => {
                map.insert("variant".to_string(), "Single".to_string().into_value());
                map.insert("keys".to_string(), keys.into_value());
            }
            Self::Multi { dims } => {
                map.insert("variant".to_string(), "Multi".to_string().into_value());
                map.insert("dims".to_string(), dims.into_value());
            }
            Self::Set { keys } => {
                map.insert("variant".to_string(), "Set".to_string().into_value());
                map.insert("keys".to_string(), keys.into_value());
            }
        }
        Value::Object(map.into())
    }

    fn from_value(value: Value) -> std::result::Result<Self, SurrealError> {
        // Backward compat with old string-serialized data.
        if let Ok(s) = String::from_value(value.clone()) {
            return Ok(Self::Single { keys: vec![s] });
        }

        let map = <std::collections::BTreeMap<String, Value>>::from_value(value)?;
        let variant = map
            .get("variant")
            .and_then(|v| String::from_value(v.clone()).ok())
            .unwrap_or_default();
        match variant.as_str() {
            "Single" => {
                let keys = map
                    .get("keys")
                    .map(|v| Vec::<String>::from_value(v.clone()))
                    .transpose()?
                    .unwrap_or_default();
                Ok(Self::Single { keys })
            }
            "Multi" => {
                let dims = map
                    .get("dims")
                    .map(|v| Vec::<(String, Vec<String>)>::from_value(v.clone()))
                    .transpose()?
                    .unwrap_or_default();
                Ok(Self::Multi { dims })
            }
            "Set" => {
                let keys = map
                    .get("keys")
                    .map(|v| Vec::<PartitionKey>::from_value(v.clone()))
                    .transpose()?
                    .unwrap_or_default();
                Ok(Self::Set { keys })
            }
            _ => Err(SurrealError::internal(format!(
                "unknown PartitionKey variant: {variant}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EventRecord {
    /// Owning code location. Routes asset / partition row updates triggered
    /// by materialization side-effects in `store_event` to the correct CL.
    #[serde(default = "default_code_location_id")]
    pub code_location_id: String,
    pub event_type: EventType,
    pub asset_key: Option<String>,
    pub run_id: String,
    pub partition_key: Option<PartitionKey>,
    pub timestamp: i64,
    pub metadata: Vec<(String, String)>,
    /// Upstream input data versions consumed during this materialization.
    /// Captured at read time by the executor: `(dep_name, data_version)`.
    /// Empty for non-Materialization events.
    #[serde(default)]
    pub input_data_versions: Vec<(String, String)>,
}

// ── Asset / Run / Tick records ──

#[derive(Debug, Clone, PartialEq, SurrealValue, serde::Serialize, serde::Deserialize)]
pub struct AssetRecord {
    /// Owning code location; uniqueness is per-CL via composite index on `(code_location_id, asset_key)`.
    #[serde(default = "default_code_location_id")]
    pub code_location_id: String,
    pub asset_key: String,
    pub tags: Vec<String>,
    pub kinds: Vec<String>,
    pub asset_group: Option<String>,
    pub code_version: Option<String>,
    pub last_event_id: Option<String>,
    pub last_run_id: Option<String>,
    pub last_timestamp: Option<i64>,
    pub last_data_version: Option<String>,
    /// Code version used when this asset was last materialized.
    #[serde(default)]
    pub last_materialization_code_version: Option<String>,
    /// Input data versions consumed during last materialization: (upstream_key, data_version).
    #[serde(default)]
    pub last_input_data_versions: Vec<(String, String)>,
    /// Pool membership: (pool_key, slots_consumed) pairs. Empty = no pool constraint.
    #[serde(default)]
    pub pool: Vec<(String, u32)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Asc,
    Desc,
}

impl SortOrder {
    /// Render as a SQL `ORDER BY` direction.
    pub fn as_sql(self) -> &'static str {
        match self {
            Self::Asc => "ASC",
            Self::Desc => "DESC",
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum RunStatus {
    Queued,
    NotStarted,
    Started,
    Success,
    Failure,
    Canceled,
}

impl SurrealValue for RunStatus {
    fn kind_of() -> Kind {
        String::kind_of()
    }

    fn into_value(self) -> Value {
        let s = match self {
            Self::Queued => "Queued",
            Self::NotStarted => "NotStarted",
            Self::Started => "Started",
            Self::Success => "Success",
            Self::Failure => "Failure",
            Self::Canceled => "Canceled",
        };
        s.to_string().into_value()
    }

    fn from_value(value: Value) -> std::result::Result<Self, SurrealError> {
        let s = String::from_value(value)?;
        match s.as_str() {
            "Queued" => Ok(Self::Queued),
            "NotStarted" => Ok(Self::NotStarted),
            "Started" => Ok(Self::Started),
            "Success" => Ok(Self::Success),
            "Failure" => Ok(Self::Failure),
            "Canceled" => Ok(Self::Canceled),
            _ => Err(SurrealError::internal(format!("unknown RunStatus: {s}"))),
        }
    }
}

/// Origin of a run — what caused it to be created.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[derive(Default)]
pub enum LaunchedBy {
    /// User-triggered via CLI / API / UI.
    #[default]
    Manual,
    /// Spawned by a schedule tick.
    Schedule { name: String },
    /// Spawned by a sensor tick.
    Sensor { name: String },
    /// Spawned as part of a backfill.
    Backfill { backfill_id: String },
    /// Spawned by the automation condition evaluation loop.
    Condition,
}

impl SurrealValue for LaunchedBy {
    fn kind_of() -> Kind {
        <std::collections::HashMap<String, Value>>::kind_of()
    }

    fn into_value(self) -> Value {
        let mut map = std::collections::BTreeMap::new();
        match self {
            Self::Manual => {
                map.insert("kind".to_string(), "manual".to_string().into_value());
            }
            Self::Schedule { name } => {
                map.insert("kind".to_string(), "schedule".to_string().into_value());
                map.insert("name".to_string(), name.into_value());
            }
            Self::Sensor { name } => {
                map.insert("kind".to_string(), "sensor".to_string().into_value());
                map.insert("name".to_string(), name.into_value());
            }
            Self::Backfill { backfill_id } => {
                map.insert("kind".to_string(), "backfill".to_string().into_value());
                map.insert("backfill_id".to_string(), backfill_id.into_value());
            }
            Self::Condition => {
                map.insert("kind".to_string(), "condition".to_string().into_value());
            }
        }
        Value::Object(map.into())
    }

    fn from_value(value: Value) -> std::result::Result<Self, SurrealError> {
        let map = <std::collections::BTreeMap<String, Value>>::from_value(value)?;
        let kind = map
            .get("kind")
            .and_then(|v| String::from_value(v.clone()).ok())
            .unwrap_or_default();
        match kind.as_str() {
            "" | "manual" => Ok(Self::Manual),
            "schedule" => {
                let name = map
                    .get("name")
                    .map(|v| String::from_value(v.clone()))
                    .transpose()?
                    .unwrap_or_default();
                Ok(Self::Schedule { name })
            }
            "sensor" => {
                let name = map
                    .get("name")
                    .map(|v| String::from_value(v.clone()))
                    .transpose()?
                    .unwrap_or_default();
                Ok(Self::Sensor { name })
            }
            "backfill" => {
                let backfill_id = map
                    .get("backfill_id")
                    .map(|v| String::from_value(v.clone()))
                    .transpose()?
                    .unwrap_or_default();
                Ok(Self::Backfill { backfill_id })
            }
            "condition" => Ok(Self::Condition),
            other => Err(SurrealError::internal(format!(
                "unknown LaunchedBy kind: {other}"
            ))),
        }
    }
}

/// Default code-location identity used when the daemon has neither
/// `RIVERS_CODE_LOCATION_ID` nor `RIVERS_CODE_LOCATION_NAME` set (local
/// single-CL dev, tests, in-process embedded usage).
pub const DEFAULT_CODE_LOCATION_ID: &str = "default";

#[derive(Debug, Clone, PartialEq, SurrealValue, serde::Serialize, serde::Deserialize)]
pub struct RunRecord {
    pub run_id: String,
    /// Identity of the code location that owns this run. Filters the run
    /// queue so daemons sharing a SurrealDB only dequeue their own runs.
    /// Defaults to [`DEFAULT_CODE_LOCATION_ID`] for rows written
    /// by older binaries or bare in-process usage.
    #[serde(default = "default_code_location_id")]
    pub code_location_id: String,
    /// Name of the user-defined `Job` this run targets. `None` for ad-hoc
    /// runs from `repo.materialize()` and for sensors that drive an asset
    /// selection without a job.
    #[serde(default)]
    pub job_name: Option<String>,
    pub status: RunStatus,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub tags: Vec<(String, String)>,
    pub node_names: Vec<String>,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub partition_key: Option<PartitionKey>,
    /// Why this run is blocked (set by coordinator when tag/global limits hit, cleared on dequeue).
    #[serde(default)]
    pub block_reason: Option<String>,
    #[serde(default)]
    pub launched_by: LaunchedBy,
}

pub fn default_code_location_id() -> String {
    DEFAULT_CODE_LOCATION_ID.to_string()
}

impl crate::concurrency::Tagged for RunRecord {
    fn tags(&self) -> &[(String, String)] {
        &self.tags
    }
}

/// Lightweight projection of a run for the coordinator tick — only the fields
/// needed for tag checking, dequeue, and launch.
#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
pub struct CoordinatorRunInfo {
    pub run_id: String,
    /// See [`RunRecord::code_location_id`].
    #[serde(default = "default_code_location_id")]
    pub code_location_id: String,
    pub tags: Vec<(String, String)>,
    #[serde(default)]
    pub node_names: Vec<String>,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub partition_key: Option<PartitionKey>,
    #[serde(default)]
    pub start_time: i64,
}

impl crate::concurrency::Tagged for CoordinatorRunInfo {
    fn tags(&self) -> &[(String, String)] {
        &self.tags
    }
}

/// Filter passed to paginated run queries. Empty substrings mean "match any".
#[derive(Debug, Clone, Default)]
pub struct RunFilter {
    pub status: Option<RunStatus>,
    /// Exact match on `job_name`. Used by the job-detail page to scope run
    /// history to one job without the substring ambiguity of e.g. `foo` vs
    /// `foo_bar`.
    pub job_name: Option<String>,
    pub job_substring: Option<String>,
    pub asset_substring: Option<String>,
    pub partition_substring: Option<String>,
}

/// One page of run records plus the total number of rows matching the filter
/// (so callers can render pagination controls without a second query).
#[derive(Debug, Clone)]
pub struct RunsPage {
    pub rows: Vec<RunRecord>,
    pub total: u64,
}

/// Aggregate run counts for the runs-list page header. Covers all runs — the
/// UI uses these to drive status filter pill badges and the page subtitle.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RunsSummary {
    pub total: u64,
    pub in_progress: u64,
    pub queued: u64,
    pub failure: u64,
    pub success: u64,
    pub last_24h: u64,
}

// ── Backfill records ──

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum BackfillStatus {
    Requested,
    InProgress,
    CompletedSuccess,
    CompletedFailed,
    Canceled,
}

impl SurrealValue for BackfillStatus {
    fn kind_of() -> Kind {
        String::kind_of()
    }

    fn into_value(self) -> Value {
        let s = match self {
            Self::Requested => "Requested",
            Self::InProgress => "InProgress",
            Self::CompletedSuccess => "CompletedSuccess",
            Self::CompletedFailed => "CompletedFailed",
            Self::Canceled => "Canceled",
        };
        s.to_string().into_value()
    }

    fn from_value(value: Value) -> std::result::Result<Self, SurrealError> {
        let s = String::from_value(value)?;
        match s.as_str() {
            "Requested" => Ok(Self::Requested),
            "InProgress" => Ok(Self::InProgress),
            "CompletedSuccess" => Ok(Self::CompletedSuccess),
            "CompletedFailed" => Ok(Self::CompletedFailed),
            "Canceled" => Ok(Self::Canceled),
            _ => Err(SurrealError::internal(format!(
                "unknown BackfillStatus: {s}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum BackfillFailurePolicy {
    Continue,
    StopOnFailure,
}

impl SurrealValue for BackfillFailurePolicy {
    fn kind_of() -> Kind {
        String::kind_of()
    }

    fn into_value(self) -> Value {
        let s = match self {
            Self::Continue => "Continue",
            Self::StopOnFailure => "StopOnFailure",
        };
        s.to_string().into_value()
    }

    fn from_value(value: Value) -> std::result::Result<Self, SurrealError> {
        let s = String::from_value(value)?;
        match s.as_str() {
            "Continue" => Ok(Self::Continue),
            "StopOnFailure" => Ok(Self::StopOnFailure),
            _ => Err(SurrealError::internal(format!(
                "unknown BackfillFailurePolicy: {s}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub enum BackfillStrategy {
    #[default]
    MultiRun,
    SingleRun,
    PerDimension {
        multi_run: Vec<String>,
        single_run: Vec<String>,
    },
}

impl SurrealValue for BackfillStrategy {
    fn kind_of() -> Kind {
        <std::collections::HashMap<String, Value>>::kind_of()
    }

    fn into_value(self) -> Value {
        let mut map = std::collections::BTreeMap::new();
        match self {
            Self::MultiRun => {
                map.insert("variant".to_string(), "MultiRun".to_string().into_value());
            }
            Self::SingleRun => {
                map.insert("variant".to_string(), "SingleRun".to_string().into_value());
            }
            Self::PerDimension {
                multi_run,
                single_run,
            } => {
                map.insert(
                    "variant".to_string(),
                    "PerDimension".to_string().into_value(),
                );
                map.insert("multi_run".to_string(), multi_run.into_value());
                map.insert("single_run".to_string(), single_run.into_value());
            }
        }
        Value::Object(map.into())
    }

    fn from_value(value: Value) -> std::result::Result<Self, SurrealError> {
        let map = <std::collections::BTreeMap<String, Value>>::from_value(value)?;
        let variant = map
            .get("variant")
            .and_then(|v| String::from_value(v.clone()).ok())
            .unwrap_or_default();
        match variant.as_str() {
            "SingleRun" => Ok(Self::SingleRun),
            "PerDimension" => {
                let multi_run = map
                    .get("multi_run")
                    .map(|v| Vec::<String>::from_value(v.clone()))
                    .transpose()?
                    .unwrap_or_default();
                let single_run = map
                    .get("single_run")
                    .map(|v| Vec::<String>::from_value(v.clone()))
                    .transpose()?
                    .unwrap_or_default();
                Ok(Self::PerDimension {
                    multi_run,
                    single_run,
                })
            }
            _ => Ok(Self::MultiRun),
        }
    }
}

/// Filter passed to the paginated backfills query.
#[derive(Debug, Clone, Default)]
pub struct BackfillFilter {
    pub status: Option<BackfillStatus>,
}

/// One page of backfill records plus the total row count matching the filter.
#[derive(Debug, Clone)]
pub struct BackfillsPage {
    pub rows: Vec<BackfillRecord>,
    pub total: u64,
}

/// Aggregate backfill counts for the list-page header pills. Unfiltered so the
/// pill badges stay stable while the user flips between status tabs.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BackfillsSummary {
    pub total: u64,
    pub in_progress: u64,
    pub completed_success: u64,
    pub completed_failed: u64,
    pub canceled: u64,
}

#[derive(Debug, Clone, PartialEq, SurrealValue, serde::Serialize, serde::Deserialize)]
pub struct BackfillRecord {
    pub backfill_id: String,
    /// Owning code location; the backfill pickup loop filters by this so each daemon only picks up its own.
    #[serde(default = "default_code_location_id")]
    pub code_location_id: String,
    pub status: BackfillStatus,
    pub strategy: BackfillStrategy,
    pub failure_policy: BackfillFailurePolicy,
    pub asset_selection: Vec<String>,
    /// Set when the backfill targets a named `Job` — each partition runs with the
    /// job's own plan + executor. `None` for ad-hoc asset-selection backfills.
    #[serde(default)]
    pub job_name: Option<String>,
    pub partition_keys: Vec<PartitionKey>,
    pub run_ids: Vec<String>,
    pub completed_partitions: Vec<PartitionKey>,
    pub failed_partitions: Vec<PartitionKey>,
    pub canceled_partitions: Vec<PartitionKey>,
    pub max_concurrency: i64,
    pub tags: Vec<(String, String)>,
    pub create_time: i64,
    pub end_time: Option<i64>,
    pub error: Option<String>,
}

// ── Concurrency pool records ──

/// Pool configuration as stored in the `concurrency_pools` table.
#[derive(Debug, Clone, PartialEq, SurrealValue, serde::Serialize, serde::Deserialize)]
pub struct PoolLimit {
    /// Owning code location; pools are per-CL — CL-A's `default` is independent of CL-B's.
    #[serde(default = "default_code_location_id")]
    pub code_location_id: String,
    pub pool_key: String,
    /// Slot limit. `-1` means unlimited (no capacity enforcement).
    pub slot_limit: i32,
    #[serde(default = "default_lease_duration")]
    pub lease_duration_secs: u32,
}

pub const DEFAULT_LEASE_DURATION_SECS: u32 = 300;

fn default_lease_duration() -> u32 {
    DEFAULT_LEASE_DURATION_SECS
}

/// Runtime pool info: configuration + current usage.
#[derive(Debug, Clone, PartialEq)]
pub struct PoolInfo {
    pub pool_key: String,
    /// Slot limit. `-1` means unlimited (no capacity enforcement).
    pub slot_limit: i32,
    pub lease_duration_secs: u32,
    /// Sum of `slots_consumed` for active (non-expired) leases.
    pub claimed_count: u32,
    /// Number of steps waiting in `pending_steps`.
    pub pending_count: u32,
}

/// A single active slot holder in a concurrency pool.
#[derive(Debug, Clone, PartialEq, SurrealValue, serde::Serialize, serde::Deserialize)]
pub struct SlotHolder {
    pub run_id: String,
    pub step_key: String,
    pub slots_consumed: u32,
    pub claimed_at: i64,
    pub lease_expires_at: i64,
}

/// Why a step is blocked from claiming concurrency slots.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum BlockReason {
    /// Single pool is at capacity.
    PoolFull {
        pool_key: String,
        claimed: u32,
        limit: i32,
    },
    /// Multiple pools are at capacity (multi-pool claim).
    PoolsFull { pools: Vec<PoolBlockDetail> },
}

impl std::fmt::Display for BlockReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockReason::PoolFull {
                pool_key,
                claimed,
                limit,
            } => write!(f, "pool '{}' full ({}/{})", pool_key, claimed, limit),
            BlockReason::PoolsFull { pools } => {
                write!(f, "pools full: ")?;
                for (i, p) in pools.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "'{}' ({}/{})", p.pool_key, p.claimed, p.limit)?;
                }
                Ok(())
            }
        }
    }
}

/// Detail for a single pool that is blocking a claim.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PoolBlockDetail {
    pub pool_key: String,
    pub claimed: u32,
    pub limit: i32,
}

/// Result of attempting to claim concurrency slots.
#[derive(Debug, Clone, PartialEq)]
pub enum ConcurrencyClaimStatus {
    /// Slots successfully claimed in all requested pools.
    Claimed,
    /// Step is pending — at least one pool is full.
    Pending { position: u32, reason: BlockReason },
}

// ── Tick records ──

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
    /// Backfills this tick spawned. Multi-partition condition fires create
    /// backfills (one per asset).
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
    /// None for unpartitioned assets.
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

// ── Run progress / outcome (K8s operator + executor coordination) ──

/// Progress of a run's step execution, computed from step events.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RunProgress {
    pub completed_steps: u32,
    pub total_steps: u32,
    pub last_step_completed_at: Option<i64>,
    pub last_completed_step: Option<String>,
}

/// Final outcome of a run, written by the executor before exit.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum RunOutcome {
    Success {
        completed_steps: u32,
        total_steps: u32,
    },
    Failure {
        message: String,
        completed_steps: u32,
        total_steps: u32,
    },
    Cancelled {
        completed_steps: u32,
        total_steps: u32,
    },
}

/// Stored event with its database-assigned ID.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredEvent {
    pub id: surrealdb::types::RecordId,
    pub event_type: EventType,
    pub asset_key: Option<String>,
    pub run_id: String,
    pub partition_key: Option<PartitionKey>,
    pub timestamp: i64,
    pub metadata: Vec<(String, String)>,
    /// Code version of the asset at materialization time (set by storage layer).
    #[serde(default)]
    pub code_version: Option<String>,
    /// Upstream input data versions at materialization time (set by storage layer).
    #[serde(default)]
    pub input_data_versions: Vec<(String, String)>,
}

/// A code-location identity bound for the lifetime of a logical operation.
/// Borrowed by [`ScopedStorage`] returned from [`StorageBackend::for_code_location`].
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
///
/// Crate-private (`pub(crate)`) so callers outside `rivers-core` can only
/// reach these methods through [`ScopedStorage`], returned by
/// [`StorageBackend::for_code_location`]. That makes "forgot to scope a
/// query" a compile-time error at every external call site rather than a
/// silent cross-CL data leak.
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

    fn prune_condition_ticks(
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

    fn prune_condition_evals(
        &self,
        code_location_id: &str,
        asset_key: &str,
        max_evals: usize,
    ) -> impl Future<Output = Result<usize>> + Send;

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

    /// Number of registered keys for a dynamic partition namespace (aggregate
    /// count, not the keys). Dynamic partitions are storage-managed, so this is
    /// the authoritative count the UI shows (the def-level `partition_count` is 0).
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

    fn get_in_progress_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> impl Future<Output = Result<Vec<PartitionKey>>> + Send;

    /// Partitions whose latest `StepFailure` isn't superseded by a later
    /// materialization. `materialized` is the caller's per-partition timestamps
    /// (from `get_partition_timestamps`), so we don't re-read `asset_partitions`.
    fn get_failed_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
        materialized: &HashMap<PartitionKey, i64>,
    ) -> impl Future<Output = Result<Vec<PartitionKey>>> + Send;

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

    fn claim_concurrency_slots(
        &self,
        code_location_id: &str,
        pools: &[(String, u32)],
        run_id: &str,
        step_key: &str,
        priority: i32,
        lease_duration_secs: u32,
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

    /// Read the persisted condition-daemon eval state for this CL. Returns
    /// `None` if the daemon has never persisted state (first run).
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

    /// Read the persisted graph topology blob for this CL. Returns `None` if
    /// the CL has never been resolved into storage.
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

/// A backend reference pre-bound to a [`CodeLocationContext`].
///
/// Returned by [`StorageBackend::for_code_location`]. All per-CL operations
/// live as inherent methods here — the caller can't drop the scope and
/// forgetting it is a compile-time error rather than a silent cross-CL
/// query.
pub struct ScopedStorage<'a, S: ?Sized> {
    backend: &'a S,
    code_location_id: &'a str,
}

impl<'a, S: ?Sized> ScopedStorage<'a, S> {
    pub fn code_location_id(&self) -> &str {
        self.code_location_id
    }
}

/// Owned counterpart to [`ScopedStorage`] — bundles `Arc<S>` with a
/// [`CodeLocationContext`] so a single value can be moved into spawned
/// tasks instead of threading the storage Arc and identity separately.
///
/// `Clone` is cheap (Arc bump + String clone). Call [`Self::scoped`] to get
/// a borrowed [`ScopedStorage`] for per-CL methods, or [`Self::backend`] to
/// reach unscoped (UUID-keyed) methods on the underlying [`StorageBackend`].
pub struct ScopedStorageHandle<S> {
    backend: Arc<S>,
    ctx: CodeLocationContext,
}

impl<S> Clone for ScopedStorageHandle<S> {
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            ctx: self.ctx.clone(),
        }
    }
}

impl<S> ScopedStorageHandle<S> {
    pub fn new(backend: Arc<S>, ctx: CodeLocationContext) -> Self {
        Self { backend, ctx }
    }

    pub fn backend(&self) -> &Arc<S> {
        &self.backend
    }

    pub fn ctx(&self) -> &CodeLocationContext {
        &self.ctx
    }

    pub fn code_location_id(&self) -> &str {
        self.ctx.id()
    }
}

impl<S: StorageBackend> ScopedStorageHandle<S> {
    pub fn scoped(&self) -> ScopedStorage<'_, S> {
        self.backend.for_code_location(&self.ctx)
    }
}

#[allow(private_bounds)]
impl<'a, S: PerCodeLocationStorage + ?Sized> ScopedStorage<'a, S> {
    pub async fn get_events_for_asset(
        &self,
        asset_key: &str,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        self.backend
            .get_events_for_asset(self.code_location_id, asset_key, limit)
            .await
    }

    pub async fn get_latest_materialization(
        &self,
        asset_key: &str,
        partition: Option<&str>,
    ) -> Result<Option<StoredEvent>> {
        self.backend
            .get_latest_materialization(self.code_location_id, asset_key, partition)
            .await
    }

    pub async fn register_assets(&self, records: &[AssetRecord]) -> Result<()> {
        self.backend
            .register_assets(self.code_location_id, records)
            .await
    }

    pub async fn get_asset_record(&self, asset_key: &str) -> Result<Option<AssetRecord>> {
        self.backend
            .get_asset_record(self.code_location_id, asset_key)
            .await
    }

    pub async fn get_asset_records(&self) -> Result<Vec<AssetRecord>> {
        self.backend.get_asset_records(self.code_location_id).await
    }

    pub async fn get_asset_records_by_keys(&self, keys: &[String]) -> Result<Vec<AssetRecord>> {
        self.backend
            .get_asset_records_by_keys(self.code_location_id, keys)
            .await
    }

    pub async fn get_assets_by_tag(&self, tag: &str) -> Result<Vec<AssetRecord>> {
        self.backend
            .get_assets_by_tag(self.code_location_id, tag)
            .await
    }

    pub async fn get_assets_by_kind(&self, kind: &str) -> Result<Vec<AssetRecord>> {
        self.backend
            .get_assets_by_kind(self.code_location_id, kind)
            .await
    }

    pub async fn get_assets_by_group(&self, group: &str) -> Result<Vec<AssetRecord>> {
        self.backend
            .get_assets_by_group(self.code_location_id, group)
            .await
    }

    pub async fn set_block_reason_by_status(
        &self,
        status: RunStatus,
        reason: Option<&str>,
    ) -> Result<()> {
        self.backend
            .set_block_reason_by_status(self.code_location_id, status, reason)
            .await
    }

    pub async fn coordinator_tick_query(
        &self,
    ) -> Result<(u32, Vec<CoordinatorRunInfo>, Vec<CoordinatorRunInfo>)> {
        self.backend
            .coordinator_tick_query(self.code_location_id)
            .await
    }

    pub async fn add_dynamic_partitions(
        &self,
        partitions_def_name: &str,
        partition_keys: &[String],
    ) -> Result<()> {
        self.backend
            .add_dynamic_partitions(self.code_location_id, partitions_def_name, partition_keys)
            .await
    }

    pub async fn delete_dynamic_partition(
        &self,
        partitions_def_name: &str,
        partition_key: &str,
    ) -> Result<()> {
        self.backend
            .delete_dynamic_partition(self.code_location_id, partitions_def_name, partition_key)
            .await
    }

    pub async fn get_dynamic_partitions(&self, partitions_def_name: &str) -> Result<Vec<String>> {
        self.backend
            .get_dynamic_partitions(self.code_location_id, partitions_def_name)
            .await
    }

    pub async fn has_dynamic_partition(
        &self,
        partitions_def_name: &str,
        partition_key: &str,
    ) -> Result<bool> {
        self.backend
            .has_dynamic_partition(self.code_location_id, partitions_def_name, partition_key)
            .await
    }

    pub async fn get_ticks(&self, automation_name: &str, limit: usize) -> Result<Vec<StoredTick>> {
        self.backend
            .get_ticks(self.code_location_id, automation_name, limit)
            .await
    }

    pub async fn prune_ticks(&self, automation_name: &str, max_ticks: usize) -> Result<usize> {
        self.backend
            .prune_ticks(self.code_location_id, automation_name, max_ticks)
            .await
    }

    pub async fn get_condition_ticks(&self, limit: usize) -> Result<Vec<StoredConditionTick>> {
        self.backend
            .get_condition_ticks(self.code_location_id, limit)
            .await
    }

    pub async fn prune_condition_ticks(&self, max_ticks: usize) -> Result<usize> {
        self.backend
            .prune_condition_ticks(self.code_location_id, max_ticks)
            .await
    }

    pub async fn get_condition_evals(
        &self,
        asset_key: &str,
        limit: usize,
    ) -> Result<Vec<StoredConditionEval>> {
        self.backend
            .get_condition_evals(self.code_location_id, asset_key, limit)
            .await
    }

    pub async fn get_condition_evals_for_tick(
        &self,
        tick_id: &str,
    ) -> Result<Vec<StoredConditionEval>> {
        self.backend
            .get_condition_evals_for_tick(self.code_location_id, tick_id)
            .await
    }

    pub async fn prune_condition_evals(&self, asset_key: &str, max_evals: usize) -> Result<usize> {
        self.backend
            .prune_condition_evals(self.code_location_id, asset_key, max_evals)
            .await
    }

    pub async fn get_partition_events(
        &self,
        asset_key: &str,
        partition_key: &str,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        self.backend
            .get_partition_events(self.code_location_id, asset_key, partition_key, limit)
            .await
    }

    pub async fn get_materialized_partitions(&self, asset_key: &str) -> Result<Vec<PartitionKey>> {
        self.backend
            .get_materialized_partitions(self.code_location_id, asset_key)
            .await
    }

    pub async fn count_materialized_partitions(&self, asset_key: &str) -> Result<u64> {
        self.backend
            .count_materialized_partitions(self.code_location_id, asset_key)
            .await
    }

    pub async fn count_dynamic_partitions(&self, partitions_def_name: &str) -> Result<u64> {
        self.backend
            .count_dynamic_partitions(self.code_location_id, partitions_def_name)
            .await
    }

    pub async fn get_partition_timestamps(
        &self,
        asset_key: &str,
    ) -> Result<Vec<(PartitionKey, i64)>> {
        self.backend
            .get_partition_timestamps(self.code_location_id, asset_key)
            .await
    }

    pub async fn get_in_progress_partitions(&self, asset_key: &str) -> Result<Vec<PartitionKey>> {
        self.backend
            .get_in_progress_partitions(self.code_location_id, asset_key)
            .await
    }

    pub async fn get_failed_partitions(
        &self,
        asset_key: &str,
        materialized: &HashMap<PartitionKey, i64>,
    ) -> Result<Vec<PartitionKey>> {
        self.backend
            .get_failed_partitions(self.code_location_id, asset_key, materialized)
            .await
    }

    pub async fn get_backfills(
        &self,
        limit: Option<usize>,
        status: Option<BackfillStatus>,
    ) -> Result<Vec<BackfillRecord>> {
        self.backend
            .get_backfills(self.code_location_id, limit, status)
            .await
    }

    pub async fn set_pool_limit(
        &self,
        pool_key: &str,
        limit: i32,
        lease_duration_secs: u32,
    ) -> Result<()> {
        self.backend
            .set_pool_limit(self.code_location_id, pool_key, limit, lease_duration_secs)
            .await
    }

    pub async fn get_pool_limits(&self) -> Result<Vec<PoolLimit>> {
        self.backend.get_pool_limits(self.code_location_id).await
    }

    pub async fn get_pool_info(&self, pool_key: &str) -> Result<PoolInfo> {
        self.backend
            .get_pool_info(self.code_location_id, pool_key)
            .await
    }

    pub async fn get_all_pool_infos(&self) -> Result<Vec<PoolInfo>> {
        self.backend.get_all_pool_infos(self.code_location_id).await
    }

    pub async fn claim_concurrency_slots(
        &self,
        pools: &[(String, u32)],
        run_id: &str,
        step_key: &str,
        priority: i32,
        lease_duration_secs: u32,
    ) -> Result<ConcurrencyClaimStatus> {
        self.backend
            .claim_concurrency_slots(
                self.code_location_id,
                pools,
                run_id,
                step_key,
                priority,
                lease_duration_secs,
            )
            .await
    }

    pub async fn get_pool_slot_holders(&self, pool_key: &str) -> Result<Vec<SlotHolder>> {
        self.backend
            .get_pool_slot_holders(self.code_location_id, pool_key)
            .await
    }

    pub async fn get_runs(
        &self,
        limit: usize,
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        PerCodeLocationStorage::get_runs(self.backend, self.code_location_id, limit, status).await
    }

    pub async fn get_queued_runs(&self) -> Result<Vec<RunRecord>> {
        PerCodeLocationStorage::get_queued_runs(self.backend, self.code_location_id).await
    }

    pub async fn get_runs_since(
        &self,
        since_timestamp: i64,
        status: Option<RunStatus>,
        order: SortOrder,
    ) -> Result<Vec<RunRecord>> {
        PerCodeLocationStorage::get_runs_since(
            self.backend,
            self.code_location_id,
            since_timestamp,
            status,
            order,
        )
        .await
    }

    pub async fn get_condition_eval_state(
        &self,
    ) -> Result<Option<crate::condition::ConditionEvalState>> {
        self.backend
            .get_condition_eval_state(self.code_location_id)
            .await
    }

    pub async fn set_condition_eval_state(
        &self,
        state: &crate::condition::ConditionEvalState,
    ) -> Result<()> {
        self.backend
            .set_condition_eval_state(self.code_location_id, state)
            .await
    }

    pub async fn get_graph_topology(&self) -> Result<Option<crate::assets::graph::GraphTopology>> {
        self.backend.get_graph_topology(self.code_location_id).await
    }

    pub async fn set_graph_topology(
        &self,
        topology: &crate::assets::graph::GraphTopology,
    ) -> Result<()> {
        self.backend
            .set_graph_topology(self.code_location_id, topology)
            .await
    }

    /// Compute staleness for every asset in this code location. Nothing is
    /// persisted — the canonical entry point for UI / CLI / daemon staleness reads.
    pub async fn compute_staleness(
        &self,
    ) -> Result<std::collections::HashMap<String, (StaleStatus, Vec<StaleCause>)>> {
        let records = self.get_asset_records().await?;
        let edges = self
            .get_graph_topology()
            .await?
            .map(|t| t.edges)
            .unwrap_or_default();
        Ok(crate::staleness::compute_staleness(&records, &edges))
    }
}

/// Per-CL methods that need to reach the unscoped `StorageBackend` KV API.
impl<'a, S: StorageBackend + ?Sized> ScopedStorage<'a, S> {
    fn dynamic_keys_kv_key(
        &self,
        asset_key: &str,
        partition: Option<&PartitionKey>,
        data_version: &str,
    ) -> String {
        let partition_str = partition.map(|p| p.to_json());
        crate::dynamic_keys_key(
            self.code_location_id,
            asset_key,
            partition_str.as_deref(),
            data_version,
        )
    }

    /// Persist a fan-out source's mapping keys, scoped by `data_version` so
    /// concurrent runs of the same asset+partition can't collide and a
    /// plain-values run leaves no entry to confuse a subsequent read.
    /// Callers should only invoke this when the materialization actually
    /// produced `DynamicOutput`s — absent KV entries are the signal for
    /// "ran with plain values, use synthetic indices."
    pub async fn set_dynamic_keys(
        &self,
        asset_key: &str,
        partition: Option<&PartitionKey>,
        data_version: &str,
        keys: &[String],
    ) -> Result<()> {
        let key = self.dynamic_keys_kv_key(asset_key, partition, data_version);
        let bytes = serde_json::to_vec(keys)?;
        self.backend.kv_set(&key, &bytes).await
    }

    /// Read the fan-out mapping keys for a specific materialization
    /// (`asset_key` + `partition` + `data_version`). `Ok(None)` means no
    /// entry exists — the source either ran with plain values or hasn't
    /// been materialized at this `data_version`.
    pub async fn get_dynamic_keys(
        &self,
        asset_key: &str,
        partition: Option<&PartitionKey>,
        data_version: &str,
    ) -> Result<Option<Vec<String>>> {
        let key = self.dynamic_keys_kv_key(asset_key, partition, data_version);
        match self.backend.kv_get(&key).await? {
            None => Ok(None),
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        }
    }
}

#[allow(private_bounds)]
pub trait StorageBackend: PerCodeLocationStorage {
    /// Bind a [`CodeLocationContext`] to this backend. The only way external
    /// crates can reach per-CL operations.
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
    /// Check if a step completed (StepSuccess or StepFailure event exists)
    /// for a specific asset in any of the given runs.
    fn has_step_completed(
        &self,
        asset_key: &str,
        run_ids: &[String],
    ) -> impl Future<Output = Result<bool>> + Send;

    // Runs
    fn create_run(&self, run: &RunRecord) -> impl Future<Output = Result<()>> + Send;
    fn create_runs(&self, runs: &[RunRecord]) -> impl Future<Output = Result<()>> + Send;
    fn update_run_status(
        &self,
        run_id: &str,
        status: RunStatus,
        end_time: Option<i64>,
    ) -> impl Future<Output = Result<()>> + Send;
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
    /// List runs across every code location. Use [`ScopedStorage::get_runs`]
    /// for per-CL queries; this method is for global UI / CLI views.
    fn get_all_runs(
        &self,
        limit: usize,
        status: Option<RunStatus>,
    ) -> impl Future<Output = Result<Vec<RunRecord>>> + Send;
    /// List runs across every code location created after `since_timestamp`
    /// (nanoseconds). Per-CL callers should use
    /// [`ScopedStorage::get_runs_since`].
    fn get_all_runs_since(
        &self,
        since_timestamp: i64,
        status: Option<RunStatus>,
    ) -> impl Future<Output = Result<Vec<RunRecord>>> + Send;

    // Run queue
    /// Get all queued runs (unordered) across every code location. Caller
    /// sorts. The daemon coordinator uses
    /// [`ScopedStorage::coordinator_tick_query`] for per-CL queries instead.
    fn get_all_queued_runs(&self) -> impl Future<Output = Result<Vec<RunRecord>>> + Send;

    /// Count runs in NotStarted or Started status across every code location.
    fn count_in_progress_runs(&self) -> impl Future<Output = Result<usize>> + Send;

    /// Get all runs in NotStarted or Started status across every code location.
    fn get_in_progress_runs(&self) -> impl Future<Output = Result<Vec<RunRecord>>> + Send;

    // Observations
    /// Get all observation events stored after the given timestamp.
    fn get_observations_since(
        &self,
        since_timestamp: i64,
    ) -> impl Future<Output = Result<Vec<StoredEvent>>> + Send;

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

    // Backfills (record-keyed by UUID for the writes; queries that need
    // CL filtering live on ScopedStorage instead).
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
    /// Check if all runs for a backfill are terminal. If so, transition the
    /// backfill to CompletedSuccess/CompletedFailed/Canceled and set end_time.
    /// Returns the new status if finalized, None if still in progress.
    /// `extra_canceled`: never-launched partitions (no run record) to count canceled.
    fn try_complete_backfill(
        &self,
        backfill_id: &str,
        extra_canceled: &[PartitionKey],
    ) -> impl Future<Output = Result<Option<BackfillStatus>>> + Send;

    /// Release all concurrency slots held by a specific step (across all pools).
    /// Also removes any pending_steps entry for this step.
    fn free_concurrency_slots(
        &self,
        run_id: &str,
        step_key: &str,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Release all concurrency slots and pending entries for an entire run.
    /// Called when a run terminates.
    fn free_concurrency_slots_for_run(
        &self,
        run_id: &str,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Renew the lease on all concurrency slots held by a specific step.
    /// Updates `lease_expires_at` and `last_heartbeat`. Returns the number
    /// of slot rows renewed (0 if the step has no active slots).
    fn renew_slot_lease(
        &self,
        run_id: &str,
        step_key: &str,
        lease_duration_secs: u32,
    ) -> impl Future<Output = Result<u32>> + Send;

    /// Delete all concurrency slot rows whose lease has expired.
    /// Returns the number of expired slot rows removed.
    fn free_expired_leases(&self) -> impl Future<Output = Result<u32>> + Send;

    /// Cancel a queued run (transition from Queued to Canceled).
    /// Returns true if the run was actually canceled, false if not found or not Queued.
    fn cancel_queued_run(&self, run_id: &str) -> impl Future<Output = Result<bool>> + Send;

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

    /// Get the set of step keys that completed successfully in a run.
    fn get_completed_step_keys(
        &self,
        run_id: &str,
    ) -> impl Future<Output = Result<HashSet<String>>> + Send;

    /// Get data versions produced by materialization events in a run.
    /// Returns a map of asset_key → data_version for steps that emitted a data version.
    fn get_step_data_versions(
        &self,
        run_id: &str,
    ) -> impl Future<Output = Result<HashMap<String, String>>> + Send;
}
