use serde::{Deserialize, Serialize};

use super::{Page, display_name};

/// Lifecycle states a run progresses through. `Queued` → `NotStarted` →
/// `Started` → terminal (`Success` / `Failure` / `Canceled`). Mirrors
/// `rivers_core::storage::RunStatus`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RunStatus {
    Queued,
    NotStarted,
    Started,
    Success,
    Failure,
    Canceled,
}

/// Who performed a manual action — mirrors `rivers_core::storage::UserRef`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserRef {
    pub subject: String,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
}

impl UserRef {
    pub fn display(&self) -> &str {
        display_name(self.name.as_deref(), self.email.as_deref(), &self.subject)
    }
}

/// Origin of a run — mirrors `rivers_core::storage::LaunchedBy`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LaunchedBy {
    Manual {
        #[serde(default)]
        user: Option<UserRef>,
    },
    Schedule {
        name: String,
    },
    Sensor {
        name: String,
    },
    Backfill {
        backfill_id: String,
    },
    Condition,
}

impl Default for LaunchedBy {
    fn default() -> Self {
        Self::Manual { user: None }
    }
}

/// A run's partition members, capped: a few keys + the total, so the run list
/// renders "a, b, c +N more" without shipping a whole `single_run` batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionPreview {
    pub preview: Vec<String>,
    pub total: usize,
}

impl PartitionPreview {
    /// "a, b, c +N more".
    pub fn label(&self) -> String {
        let more = self.total.saturating_sub(self.preview.len());
        if more > 0 {
            format!("{} +{more} more", self.preview.join(", "))
        } else {
            self.preview.join(", ")
        }
    }
}

/// One row in the runs table — produced by every `materialize` /
/// `execute_job` / queued backfill submission. Mirrors
/// `rivers_core::storage::RunRecord`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub run_id: String,
    /// `None` for ad-hoc runs (e.g. `repo.materialize()`, asset-selection
    /// sensors). `Some` when the run targets a user-defined `Job`.
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
    pub partition_key: Option<PartitionPreview>,
    #[serde(default)]
    pub block_reason: Option<String>,
    #[serde(default)]
    pub launched_by: LaunchedBy,
    #[serde(default)]
    pub code_location_id: String,
    /// The verb this run executes. `None` means materialize.
    #[serde(default)]
    pub action: Option<String>,
    /// The launch document the run was launched with, as JSON text. `None`
    /// means the definitions as they are.
    #[serde(default)]
    pub config: Option<String>,
}

/// Which verb a run filter selects. Explicit variants rather than a nested
/// `Option` so every state survives a JSON round-trip.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub enum VerbFilter {
    /// No restriction — materializes and actions alike.
    #[default]
    Any,
    /// Materialize runs only (the ones storing no verb).
    MaterializeOnly,
    /// Runs executing exactly this verb.
    Verb(String),
}

impl VerbFilter {
    /// Map to the core filter's nested-`Option` encoding, which is in-process
    /// only and so never has to survive serialization.
    #[cfg(feature = "ssr")]
    pub(super) fn into_core(self) -> Option<Option<String>> {
        match self {
            Self::Any => None,
            Self::MaterializeOnly => Some(None),
            Self::Verb(v) => Some(Some(v)),
        }
    }
}

/// Filter passed to the paginated runs server fn. Empty/`None` means no
/// restriction on that dimension. Mirrors `rivers_core::storage::RunFilter`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunFilter {
    #[serde(default)]
    pub status: Option<RunStatus>,
    /// Exact `job_name` match. Used by the job-detail page.
    #[serde(default)]
    pub job_name: Option<String>,
    #[serde(default)]
    pub job_substring: Option<String>,
    #[serde(default)]
    pub asset_substring: Option<String>,
    #[serde(default)]
    pub partition_substring: Option<String>,
    /// Verb the run executes. A nested `Option` cannot cross this boundary:
    /// `get_runs_page` is `#[server(input = Json)]`, and both `None` and
    /// `Some(None)` encode as `null`, so "materialize only" was unrepresentable.
    #[serde(default)]
    pub action: VerbFilter,
}

/// One page of runs plus the total number of rows matching the filter.
pub type RunsPage = Page<RunRecord>;

/// Aggregate run counts for the runs-list page header.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunsSummary {
    pub total: u64,
    pub in_progress: u64,
    pub queued: u64,
    pub failure: u64,
    pub success: u64,
    pub last_24h: u64,
}
