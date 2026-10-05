use surrealdb::types::{Error as SurrealError, Kind, SurrealValue, Value};

use super::PartitionKey;

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

/// Who performed a manual action. `subject` is the stable identifier
/// (OIDC `sub` / forward-auth user header); email/name are launch-time
/// display snapshots.
#[derive(Debug, Clone, PartialEq, Eq, SurrealValue, serde::Serialize, serde::Deserialize)]
pub struct UserRef {
    pub subject: String,
    #[serde(default)]
    #[surreal(default)]
    pub email: Option<String>,
    #[serde(default)]
    #[surreal(default)]
    pub name: Option<String>,
}

impl UserRef {
    pub fn display(&self) -> &str {
        self.name
            .as_deref()
            .or(self.email.as_deref())
            .unwrap_or(&self.subject)
    }
}

/// Origin of a run — what caused it to be created.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LaunchedBy {
    /// User-triggered via CLI / API / UI; `user` is set only for
    /// authenticated UI sessions.
    Manual {
        #[serde(default)]
        user: Option<UserRef>,
    },
    /// Spawned by a schedule tick.
    Schedule { name: String },
    /// Spawned by a sensor tick.
    Sensor { name: String },
    /// Spawned as part of a backfill.
    Backfill { backfill_id: String },
    /// Spawned by the automation condition evaluation loop.
    Condition,
}

impl Default for LaunchedBy {
    fn default() -> Self {
        Self::Manual { user: None }
    }
}

impl SurrealValue for LaunchedBy {
    fn kind_of() -> Kind {
        <std::collections::HashMap<String, Value>>::kind_of()
    }

    fn into_value(self) -> Value {
        let mut map = std::collections::BTreeMap::new();
        match self {
            Self::Manual { user } => {
                map.insert("kind".to_string(), "manual".to_string().into_value());
                // Omitted when None — matches the pre-user row shape.
                if let Some(user) = user {
                    map.insert("user".to_string(), user.into_value());
                }
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
            "" | "manual" => {
                let user = map
                    .get("user")
                    .filter(|v| !matches!(v, Value::None | Value::Null))
                    .map(|v| UserRef::from_value(v.clone()))
                    .transpose()?;
                Ok(Self::Manual { user })
            }
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

/// Default code-location identity used when neither `RIVERS_CODE_LOCATION_ID` nor `RIVERS_CODE_LOCATION_NAME` is set.
pub const DEFAULT_CODE_LOCATION_ID: &str = "default";

#[derive(Debug, Clone, PartialEq, SurrealValue, serde::Serialize, serde::Deserialize)]
pub struct RunRecord {
    pub run_id: String,
    /// Identity of the code location that owns this run.
    #[serde(default = "default_code_location_id")]
    pub code_location_id: String,
    /// Name of the user-defined `Job` this run targets.
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
    /// The verb this run executes. `None` means materialize (rows predate actions).
    #[serde(default)]
    pub action: Option<String>,
    /// The launch document the run was launched with, as JSON text:
    /// `{"assets": {...}, "resources": {...}, "execution": {...}}` (see the
    /// `run_config` module of the Python crate). `None` means the definitions
    /// as they are. Every launcher that starts from a stored run (run queue,
    /// `rivers execute`, backfill children, reruns) reads it here.
    #[serde(default)]
    pub config: Option<String>,
}

impl RunRecord {
    /// True when this run executes a named action rather than materializing.
    /// Mirrors [`crate::execution::plan::ExecutionPlan::is_action`].
    pub fn is_action(&self) -> bool {
        self.action.is_some()
    }
}

pub fn default_code_location_id() -> String {
    DEFAULT_CODE_LOCATION_ID.to_string()
}

impl crate::concurrency::Tagged for RunRecord {
    fn tags(&self) -> &[(String, String)] {
        &self.tags
    }
}

/// Lightweight projection of a run for the coordinator tick.
#[derive(Debug, Clone, SurrealValue, serde::Serialize, serde::Deserialize)]
pub struct CoordinatorRunInfo {
    pub run_id: String,
    /// See [`RunRecord::code_location_id`].
    #[serde(default = "default_code_location_id")]
    pub code_location_id: String,
    pub tags: Vec<(String, String)>,
    #[serde(default)]
    pub node_names: Vec<String>,
    /// Job this run executes, when it was dispatched as one — carried through
    /// to the run backend so job-level config (retry, executor) survives.
    #[serde(default)]
    pub job_name: Option<String>,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub partition_key: Option<PartitionKey>,
    #[serde(default)]
    pub start_time: i64,
    /// See [`RunRecord::action`] — the backend must execute this verb, not materialize.
    #[serde(default)]
    pub action: Option<String>,
    /// See [`RunRecord::config`] — the backend applies this document.
    #[serde(default)]
    pub config: Option<String>,
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
    /// Exact match on `job_name`.
    pub job_name: Option<String>,
    pub job_substring: Option<String>,
    pub asset_substring: Option<String>,
    pub partition_substring: Option<String>,
    /// Which verb the run executes. `Some(None)` selects materialize runs
    /// (rows with no action), `Some(Some(verb))` that verb.
    pub action: Option<Option<String>>,
}

/// One page of run records plus the total number of rows matching the filter.
#[derive(Debug, Clone)]
pub struct RunsPage {
    pub rows: Vec<RunRecord>,
    pub total: u64,
}

/// Aggregate run counts for the runs-list page header.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RunsSummary {
    pub total: u64,
    pub in_progress: u64,
    pub queued: u64,
    pub failure: u64,
    pub success: u64,
    pub last_24h: u64,
}

/// A step's recorded attempts in one run (see `get_step_attempts`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StepAttempts {
    /// `StepStart` events recorded: one per attempt begun, crashed ones too.
    pub starts: u32,
    /// A step-level `StepFailure` was recorded: the retry ladder is over.
    pub failed: bool,
    /// `StepRetry` events recorded.
    pub retries: u32,
    /// Keys the step failed one by one (keyed `StepFailure`), which ordering
    /// keeps its dependents off.
    pub failed_keys: Vec<PartitionKey>,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn user() -> UserRef {
        UserRef {
            subject: "sub-1".into(),
            email: Some("john.doe@example.com".into()),
            name: Some("John Doe".into()),
        }
    }

    #[test]
    fn manual_without_user_roundtrips_as_pre_user_shape() {
        let v = LaunchedBy::Manual { user: None }.into_value();
        let Value::Object(map) = &v else {
            panic!("expected object")
        };
        assert!(!map.contains_key("user"), "None user must be omitted");
        assert_eq!(
            LaunchedBy::from_value(v).unwrap(),
            LaunchedBy::Manual { user: None }
        );
    }

    #[test]
    fn manual_with_user_roundtrips() {
        let launched = LaunchedBy::Manual { user: Some(user()) };
        let back = LaunchedBy::from_value(launched.clone().into_value()).unwrap();
        assert_eq!(back, launched);
    }

    #[test]
    fn pre_user_row_shape_deserializes() {
        // Exactly what pre-user rows carry: {"kind": "manual"}.
        let mut map = std::collections::BTreeMap::new();
        map.insert("kind".to_string(), "manual".to_string().into_value());
        let v = Value::Object(map.into());
        assert_eq!(
            LaunchedBy::from_value(v).unwrap(),
            LaunchedBy::Manual { user: None }
        );
    }

    #[test]
    fn user_ref_display_precedence() {
        let mut u = user();
        assert_eq!(u.display(), "John Doe");
        u.name = None;
        assert_eq!(u.display(), "john.doe@example.com");
        u.email = None;
        assert_eq!(u.display(), "sub-1");
    }

    #[test]
    fn serde_json_pre_user_manual_deserializes() {
        // UI DTOs and event payloads travel through serde; the pre-user JSON
        // shape must keep parsing.
        let l: LaunchedBy = serde_json::from_str(r#"{"kind":"manual"}"#).unwrap();
        assert_eq!(l, LaunchedBy::Manual { user: None });
    }
}
