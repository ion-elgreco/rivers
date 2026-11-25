//! Schema compatibility rules shared by every storage backend.
//!
//! Each backend has its own migration runner and its own way to read
//! [`SchemaStamps`]. The policy those stamps feed (who may open a store at
//! which version, and what a migration stops) is here, so the backends
//! cannot differ on it.

use super::StorageBackend;

/// Highest embedded migration version. Each backend embeds its migrations up to
/// this version; a test of each backend pins it to that max.
pub const SCHEMA_VERSION: u32 = 18;

/// The floors the [`SCHEMA_VERSION`] migration records; a test pins them to it.
pub const MIN_READER: u32 = 14;
pub const MIN_WRITER: u32 = 17;

/// What a migration to this build stamps on the store.
pub const BUILD_STAMPS: SchemaStamps = SchemaStamps {
    version: SCHEMA_VERSION,
    min_reader: MIN_READER,
    min_writer: MIN_WRITER,
};

/// What a caller intends to do — selects which floor [`check_compatibility`] enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    /// Read-only consumer (the UI). Gated by `min_reader`.
    Read,
    /// Reads and writes (code locations, daemon, executors). Gated by `min_writer`.
    ReadWrite,
    /// The migrator (`rivers db migrate`) — may advance `version`, exempt from the refusal.
    Migrate,
    /// Reads the schema status (`rivers db status`). Not
    /// gated, and never initializes the store.
    Inspect,
}

/// The compat triple the open guard checks: the applied version and its floors,
/// read from the latest `migration_meta` row (floors are cumulative — they only rise).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaStamps {
    pub version: u32,
    pub min_reader: u32,
    pub min_writer: u32,
}

/// A process that recorded its build in the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessStatus {
    /// `namespace/name` of a code location, or the name of a component.
    pub name: String,
    pub code_location: bool,
    /// Whether it opens the store to write. Otherwise it only reads.
    pub writes: bool,
    /// `None`: a build that does not record it.
    pub schema_version: Option<u32>,
    pub rivers_version: Option<String>,
    /// When a code location was last alive, or a component started, in nanoseconds.
    pub seen: Option<i64>,
    /// Whether a migration to this build stops it. Always `false` for a
    /// component.
    pub stops: bool,
}

/// The stamps of a store and the processes that use it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaStatus {
    /// `None`: the store is not initialized.
    pub stamps: Option<SchemaStamps>,
    pub processes: Vec<ProcessStatus>,
}

/// Whether a migration from `stamps` to `target` stops a process on a build
/// at `schema_version`: its floor rises above that build. An unknown build
/// counts as stopped.
pub(crate) fn migration_stops(
    stamps: SchemaStamps,
    target: SchemaStamps,
    writes: bool,
    schema_version: Option<u32>,
) -> bool {
    let floor = |s: SchemaStamps| if writes { s.min_writer } else { s.min_reader };
    if target.version <= stamps.version || floor(target) <= floor(stamps) {
        return false;
    }
    schema_version.is_none_or(|build| build < floor(target))
}

/// `stamps` of a store, and each code location and component with what a
/// migration to this build does to it. `None`: the store is not initialized.
pub(crate) async fn schema_status<S: StorageBackend>(
    storage: &S,
    stamps: Option<SchemaStamps>,
) -> anyhow::Result<SchemaStatus> {
    let Some(stamps) = stamps else {
        return Ok(SchemaStatus {
            stamps: None,
            processes: Vec::new(),
        });
    };

    let code_locations =
        storage
            .list_code_locations()
            .await?
            .into_iter()
            .map(|(location, entry)| ProcessStatus {
                name: location.to_string(),
                code_location: true,
                writes: true,
                schema_version: entry.schema_version,
                rivers_version: entry.rivers_version,
                seen: entry.last_seen,
                stops: false,
            });
    let components = storage
        .list_components()
        .await?
        .into_iter()
        .map(|(name, record)| ProcessStatus {
            name,
            code_location: false,
            writes: record.writes,
            schema_version: Some(record.schema_version),
            rivers_version: Some(record.rivers_version),
            seen: Some(record.started_at),
            stops: false,
        });

    let processes = code_locations
        .chain(components)
        .map(|process| ProcessStatus {
            // A component's record is from its last start, and the chart
            // replaces the operator and the UI with the migration.
            stops: process.code_location
                && migration_stops(stamps, BUILD_STAMPS, process.writes, process.schema_version),
            ..process
        })
        .collect();
    Ok(SchemaStatus {
        stamps: Some(stamps),
        processes,
    })
}

/// Fails with [`MigrationBlocked`] when a migration to this build stops a
/// code location in `status`.
pub(crate) fn ensure_migration_stops_nothing(status: SchemaStatus) -> anyhow::Result<()> {
    let mut processes = status.processes;
    processes.retain(|process| process.stops);
    if processes.is_empty() {
        return Ok(());
    }
    Err(MigrationBlocked { processes }.into())
}

/// A migration to this build stops `processes`. Typed so the PyO3 boundary
/// maps it to `MigrationBlockedError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationBlocked {
    pub processes: Vec<ProcessStatus>,
}

impl std::fmt::Display for MigrationBlocked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<_> = self.processes.iter().map(|p| p.name.as_str()).collect();
        write!(
            f,
            "the migration stops {}: update them first, or migrate with force",
            names.join(", ")
        )
    }
}

impl std::error::Error for MigrationBlocked {}

/// DB is older than this build. Typed so the PyO3 boundary maps it to
/// `SchemaMigrationNeededError` for the `rivers dev` prompt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaMigrationNeeded {
    /// Schema version the database is stamped at.
    pub db_version: u32,
    /// Schema version this build expects.
    pub build_version: u32,
}

impl std::fmt::Display for SchemaMigrationNeeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "database needs migration: it is at schema v{} but this rivers build expects v{}; run `rivers db migrate`",
            self.db_version, self.build_version
        )
    }
}

impl std::error::Error for SchemaMigrationNeeded {}

/// Open-time floor guard: decide whether `cap` may proceed against `stamps`.
/// Pure: the I/O that reads `stamps` lives in each backend.
pub(crate) fn check_compatibility(
    stamps: SchemaStamps,
    cap: Capability,
    build: u32,
) -> anyhow::Result<()> {
    // The migrator is the one caller that may run ahead of the database (it is
    // about to advance it); it must only refuse a downgrade.
    if cap == Capability::Migrate {
        if build < stamps.version {
            anyhow::bail!(
                "cannot migrate: database is at schema v{} but this rivers build understands only v{build}; upgrade rivers first",
                stamps.version
            );
        }
        return Ok(());
    }
    // Past the Migrate early-return, only Read/ReadWrite reach here.
    let (floor, verb) = if cap == Capability::Read {
        (stamps.min_reader, "read")
    } else {
        (stamps.min_writer, "write")
    };
    if build < floor {
        anyhow::bail!(
            "this rivers build (schema v{build}) is too old to {verb} a database at schema v{} (requires v{floor}); upgrade rivers",
            stamps.version
        );
    }
    if build > stamps.version {
        return Err(SchemaMigrationNeeded {
            db_version: stamps.version,
            build_version: build,
        }
        .into());
    }
    if build < stamps.version {
        tracing::info!(
            build,
            db_version = stamps.version,
            capability = ?cap,
            "database schema is ahead of this build but compatible for its capability"
        );
    }
    Ok(())
}
