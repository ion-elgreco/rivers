//! The version of the stored data of an asset, and the versions it reached.

/// Where an asset's stored data is against the version that its code
/// declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetVersionState {
    Ready,
    Pending,
    Draining,
    Running,
    Failed,
    Blocked,
}

impl AssetVersionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Pending => "pending",
            Self::Draining => "draining",
            Self::Running => "running",
            Self::Failed => "failed",
            Self::Blocked => "blocked",
        }
    }

    pub fn parse(state: &str) -> anyhow::Result<Self> {
        match state {
            "ready" => Ok(Self::Ready),
            "pending" => Ok(Self::Pending),
            "draining" => Ok(Self::Draining),
            "running" => Ok(Self::Running),
            "failed" => Ok(Self::Failed),
            "blocked" => Ok(Self::Blocked),
            other => anyhow::bail!("unknown asset version state '{other}'"),
        }
    }
}

/// The version row of an asset. Times are nanoseconds since the Unix epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetVersionRecord {
    pub code_location_id: String,
    pub asset_key: String,
    /// The version of the stored data.
    pub applied_version: u32,
    /// The partitions definition of the stored data, as JSON.
    pub partitions: Option<String>,
    pub state: AssetVersionState,
    /// The version that a migration moves the data to. Set unless `Ready`.
    pub target_version: Option<u32>,
    /// Why the state is `Failed` or `Blocked`.
    pub reason: Option<String>,
    /// When the migration began to run.
    pub started_at: Option<i64>,
    pub run_id: Option<String>,
    pub backfill_id: Option<String>,
    /// The daemon that runs the migration, and until when it holds it.
    pub owner: Option<String>,
    pub lease_expires_at: Option<i64>,
    /// Storage sets `revision` and `updated_at` on each write: the revision is
    /// 1 for a new row and goes up by one with each write.
    pub revision: u64,
    pub updated_at: i64,
}

/// How a migration moved the stored data to its version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationKind {
    Convert,
    Rebuild,
    /// Only the version changed.
    VersionOnly,
}

impl MigrationKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Convert => "convert",
            Self::Rebuild => "rebuild",
            Self::VersionOnly => "version_only",
        }
    }

    pub fn parse(kind: &str) -> anyhow::Result<Self> {
        match kind {
            "convert" => Ok(Self::Convert),
            "rebuild" => Ok(Self::Rebuild),
            "version_only" => Ok(Self::VersionOnly),
            other => anyhow::bail!("unknown migration kind '{other}'"),
        }
    }
}

/// A version that the stored data of an asset reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetVersionHistoryEntry {
    pub code_location_id: String,
    pub asset_key: String,
    pub version: u32,
    pub kind: MigrationKind,
    /// Readers of the version before cannot read this one.
    pub breaking: bool,
    pub run_id: Option<String>,
    pub backfill_id: Option<String>,
    /// The partitions that the migration filled, and those that failed.
    pub filled: Option<u64>,
    pub failed: Option<u64>,
    /// In nanoseconds since the Unix epoch.
    pub finished_at: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn states_and_kinds_are_stored_as_lowercase_text() {
        let states = [
            (AssetVersionState::Ready, "ready"),
            (AssetVersionState::Pending, "pending"),
            (AssetVersionState::Draining, "draining"),
            (AssetVersionState::Running, "running"),
            (AssetVersionState::Failed, "failed"),
            (AssetVersionState::Blocked, "blocked"),
        ];
        for (state, text) in states {
            assert_eq!(state.as_str(), text);
            assert_eq!(AssetVersionState::parse(text).unwrap(), state);
        }

        let kinds = [
            (MigrationKind::Convert, "convert"),
            (MigrationKind::Rebuild, "rebuild"),
            (MigrationKind::VersionOnly, "version_only"),
        ];
        for (kind, text) in kinds {
            assert_eq!(kind.as_str(), text);
            assert_eq!(MigrationKind::parse(text).unwrap(), kind);
        }

        assert!(AssetVersionState::parse("Ready").is_err());
        assert!(MigrationKind::parse("none").is_err());
    }
}
