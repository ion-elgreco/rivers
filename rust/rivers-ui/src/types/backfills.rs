use serde::{Deserialize, Serialize};

use super::{LaunchedBy, Page};

/// One windowed partition of a backfill: its display key and status
/// ("done" | "failed" | "canceled" | "pending").
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillPartitionCell {
    pub key: String,
    pub status: String,
}

/// A window of a backfill's partitions plus the total partition count.
pub type BackfillPartitionsPage = Page<BackfillPartitionCell>;

/// Filter passed to the paginated backfills server fn. Empty/`None` means no
/// restriction. Mirrors `rivers_core::storage::BackfillFilter`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BackfillFilter {
    #[serde(default)]
    pub status: Option<String>,
}

/// One page of backfills plus the total row count matching the filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillsPage {
    pub rows: Vec<BackfillInfo>,
    pub total: u64,
}

/// Aggregate backfill counts for the list-page status pills.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillsSummary {
    pub total: u64,
    pub in_progress: u64,
    pub completed_success: u64,
    pub completed_failed: u64,
    pub canceled: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillInfo {
    pub backfill_id: String,
    pub status: String,
    /// Plain-language summary, e.g. "one run per region".
    pub strategy: String,
    /// The Python API form, shown as a tooltip.
    #[serde(default)]
    pub strategy_code: String,
    /// `Some` when the backfill targets a named job (runs use the job's plan +
    /// executor); `None` for an ad-hoc asset-selection backfill.
    #[serde(default)]
    pub job_name: Option<String>,
    pub asset_selection: Vec<String>,
    pub total_partitions: u32,
    pub completed_partitions: u32,
    pub failed_partitions: u32,
    pub canceled_partitions: u32,
    pub max_concurrency: u32,
    pub run_ids: Vec<String>,
    pub tags: Vec<(String, String)>,
    pub create_time: i64,
    pub end_time: Option<i64>,
    pub error: Option<String>,
    #[serde(default)]
    pub code_location_id: String,
    #[serde(default)]
    pub launched_by: LaunchedBy,
    /// Verb the child runs execute. `None` means materialize. Without it a
    /// destructive backfill is indistinguishable from a rebuild, and re-running
    /// one from the detail page repeats the destruction.
    #[serde(default)]
    pub action: Option<String>,
}
