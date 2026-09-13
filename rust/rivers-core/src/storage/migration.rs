//! Schema compatibility rules shared by every storage backend.
//!
//! Each backend supplies its own migration runner and its own way of reading
//! [`SchemaStamps`]; the policy those stamps feed — who may open a database at
//! which version — lives here so the two backends cannot drift apart on it.

/// What a caller intends to do — selects which floor [`check_compatibility`] enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    /// Read-only consumer (the UI). Gated by `min_reader`.
    Read,
    /// Reads and writes (code locations, daemon, executors). Gated by `min_writer`.
    ReadWrite,
    /// The migrator (`rivers db migrate`) — may advance `version`, exempt from the refusal.
    Migrate,
}

/// The compat triple the open guard checks: the applied version and its floors,
/// read from the latest `migration_meta` row (floors are cumulative — they only rise).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SchemaStamps {
    pub version: u32,
    pub min_reader: u32,
    pub min_writer: u32,
}

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
/// Pure — the I/O that reads `stamps` lives in each backend.
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
