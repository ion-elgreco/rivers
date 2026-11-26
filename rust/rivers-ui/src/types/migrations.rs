//! The migrations of the stored data of assets.

use serde::{Deserialize, Serialize};

/// The state of a migration that has not ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationState {
    /// Waits for a daemon to start it.
    Pending,
    /// Waits for the started runs that write or read the asset, and for the
    /// assets that it reads to migrate first.
    Draining,
    /// Its convert or rebuild runs.
    Running,
    /// Its work failed; it waits for a retry.
    Failed,
    /// It cannot start without a change to the code.
    Blocked,
}

/// A migration of the stored data of an asset that has not ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetMigration {
    pub asset_key: String,
    /// The version of the stored data.
    pub applied_version: u32,
    pub state: MigrationState,
    /// The version that the migration moves the data to.
    pub target_version: Option<u32>,
    /// Why the migration failed or is blocked.
    pub reason: Option<String>,
    /// The run of the convert or rebuild.
    pub run_id: Option<String>,
    /// The backfill of the rebuild of a partitioned asset.
    pub backfill_id: Option<String>,
    /// While draining: the started runs that write or read the asset.
    pub drain_runs: Vec<String>,
    /// While draining: the assets that it reads, which migrate first.
    pub upstream: Vec<String>,
    /// While running: the partitions that its work fills.
    pub fill: Option<FillProgress>,
}

/// The partitions that the work of a running migration fills.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FillProgress {
    pub partitions: u64,
    /// Of a backfill: the partitions that it filled so far, and the ones
    /// that failed. A run fills all of its partitions when it ends.
    pub filled: Option<u64>,
    pub failed: Option<u64>,
}

impl AssetMigration {
    /// The kind of its status chip: `migrating`, `migration-failed`, or
    /// `migration-needed` for an asset whose partition keys changed.
    pub fn chip_kind(&self) -> &'static str {
        match self.state {
            MigrationState::Failed => "migration-failed",
            MigrationState::Blocked => "migration-needed",
            _ => "migrating",
        }
    }
}
