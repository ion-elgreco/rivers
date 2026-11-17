use surrealdb::types::{Error as SurrealError, Kind, SurrealValue, Value};

use super::{LaunchedBy, default_code_location_id};

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

/// Aggregate backfill counts for the list-page header pills.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BackfillsSummary {
    pub total: u64,
    pub in_progress: u64,
    pub completed_success: u64,
    pub completed_failed: u64,
    pub canceled: u64,
}

/// How a planned run of a backfill ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillRunOutcome {
    Completed,
    Failed,
    Canceled,
}

impl BackfillRunOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "Completed",
            Self::Failed => "Failed",
            Self::Canceled => "Canceled",
        }
    }

    pub fn parse(outcome: &str) -> anyhow::Result<Self> {
        match outcome {
            "Completed" => Ok(Self::Completed),
            "Failed" => Ok(Self::Failed),
            "Canceled" => Ok(Self::Canceled),
            other => anyhow::bail!("unknown backfill outcome '{other}'"),
        }
    }
}

/// A planned run of a backfill that ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndedBackfillRun {
    /// Place of the run in the plan.
    pub run_number: usize,
    /// The linked run. `None` when the run was not linked before it ended.
    pub run_id: Option<String>,
    pub outcome: BackfillRunOutcome,
    /// Partitions the run holds.
    pub member_count: usize,
    /// Partitions that failed alone inside a run that succeeded.
    pub failed_members: usize,
}

/// A planned run of a backfill that ended before it started: canceled before
/// its launch, or a launch that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnstartedBackfillRun {
    /// Place of the run in the plan.
    pub run_number: usize,
    /// Partitions the run holds.
    pub member_count: usize,
    pub outcome: BackfillRunOutcome,
}

/// A backfill without its requested keys: those are read with
/// `get_backfill_keys`, by the callers that need them.
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
    /// Set when the backfill targets a named `Job`.
    #[serde(default)]
    pub job_name: Option<String>,
    /// Partitions the requested keys name.
    #[serde(default)]
    #[surreal(default)]
    pub num_partitions: u64,
    /// Runs in the plan. Zero until the backfill starts.
    #[serde(default)]
    #[surreal(default)]
    pub planned_runs: u64,
    /// This and the three counts after it are kept in their own tables, not
    /// on the `backfills` row; storage fills them in on read.
    #[serde(default)]
    #[surreal(default)]
    pub run_ids: Vec<String>,
    #[serde(default)]
    #[surreal(default)]
    pub completed_partitions: u64,
    #[serde(default)]
    #[surreal(default)]
    pub failed_partitions: u64,
    #[serde(default)]
    #[surreal(default)]
    pub canceled_partitions: u64,
    pub max_concurrency: i64,
    pub tags: Vec<(String, String)>,
    pub create_time: i64,
    pub end_time: Option<i64>,
    pub error: Option<String>,
    /// Defaults cover rows written before V3.
    #[serde(default)]
    #[surreal(default)]
    pub launched_by: LaunchedBy,
    /// The verb child runs execute. `None` means materialize.
    #[serde(default)]
    pub action: Option<String>,
    /// The launch document every child run is launched with; see
    /// [`RunRecord::config`].
    #[serde(default)]
    pub config: Option<String>,
}
