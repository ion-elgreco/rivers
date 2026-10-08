//! Schema versioning and migration: refinery applies the ordered SurrealQL
//! migrations and records them; each migration writes its compat metadata into
//! `migration_meta`; the capability floor guard and cross-process lease wrap it.

use anyhow::Context;
use refinery_core::traits::r#async::{AsyncMigrate, AsyncQuery, AsyncTransaction};
use refinery_core::{Migration, Target};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb::types::SurrealValue;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::now_nanos;

/// refinery's history table — one checksummed row per applied migration.
const REFINERY_HISTORY_TABLE: &str = "refinery_schema_history";
/// refinery's history columns (fixed by its `Migration` model).
const HISTORY_COLS: &str = "version, name, applied_on, checksum";

/// A refinery backend over a SurrealDB connection: `migrate()` drives
/// `execute`/`query`; we translate the history SQL to SurrealQL (dialect overrides).
struct SurrealMigrate {
    db: Surreal<Any>,
}

#[async_trait::async_trait]
impl AsyncTransaction for SurrealMigrate {
    type Error = surrealdb::Error;

    async fn execute<'a, T: Iterator<Item = &'a str> + Send>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        // refinery hands us a migration's SQL + its history INSERT; one
        // transaction so a history row never lands without its migration applied.
        let stmts: Vec<&str> = queries
            .map(|q| q.trim().trim_end_matches(';').trim())
            .filter(|q| !q.is_empty())
            .collect();
        let count = stmts.len();
        let batch = format!(
            "BEGIN TRANSACTION;\n{};\nCOMMIT TRANSACTION;",
            stmts.join(";\n")
        );
        self.db.query(batch).await?.check()?;
        Ok(count)
    }
}

#[async_trait::async_trait]
impl AsyncQuery<Vec<Migration>> for SurrealMigrate {
    async fn query(&mut self, query: &str) -> Result<Vec<Migration>, Self::Error> {
        #[derive(SurrealValue)]
        struct HistRow {
            version: i64,
            name: String,
            applied_on: String,
            checksum: String,
        }
        let rows: Vec<HistRow> = self.db.query(query).await?.take(0)?;
        let applied = rows
            .into_iter()
            .map(|r| {
                // applied_on/checksum were written by refinery in RFC3339/u64 form.
                let applied_on =
                    OffsetDateTime::parse(&r.applied_on, &Rfc3339).expect("applied_on is RFC3339");
                Migration::applied(
                    r.version as i32,
                    r.name,
                    applied_on,
                    r.checksum.parse::<u64>().expect("checksum is a u64"),
                )
            })
            .collect();
        Ok(applied)
    }
}

impl AsyncMigrate for SurrealMigrate {
    // SurrealQL dialect: `DEFINE TABLE`, not `CREATE TABLE`.
    fn assert_migrations_table_query(table: &str) -> String {
        format!(
            "DEFINE TABLE IF NOT EXISTS {table} SCHEMAFULL; \
             DEFINE FIELD IF NOT EXISTS version ON {table} TYPE int; \
             DEFINE FIELD IF NOT EXISTS name ON {table} TYPE string; \
             DEFINE FIELD IF NOT EXISTS applied_on ON {table} TYPE string; \
             DEFINE FIELD IF NOT EXISTS checksum ON {table} TYPE string; \
             DEFINE INDEX IF NOT EXISTS idx_{table}_version ON {table} FIELDS version UNIQUE;"
        )
    }

    // SurrealQL has no `MAX()` subquery; order + limit instead.
    fn get_last_applied_migration_query(table: &str) -> String {
        format!("SELECT {HISTORY_COLS} FROM {table} ORDER BY version DESC LIMIT 1")
    }

    fn get_applied_migrations_query(table: &str) -> String {
        format!("SELECT {HISTORY_COLS} FROM {table} ORDER BY version ASC")
    }
}

/// Highest embedded migration version. Bump by adding a `Vn__*.surql` + an
/// [`embedded_migrations`] entry; a test pins this to that max.
const SCHEMA_VERSION: u32 = 11;

/// One compat row per migration (the floors it set), folded by the open guard.
const MIGRATION_META_TABLE: &str = "migration_meta";

/// The migrations embedded in this build, applied in order; refinery checksums each.
fn embedded_migrations() -> Vec<Migration> {
    vec![
        Migration::unapplied("V1__base", include_str!("migrations/V1__base.surql"))
            .expect("V1__base migration name is well-formed"),
        Migration::unapplied(
            "V2__run_logs",
            include_str!("migrations/V2__run_logs.surql"),
        )
        .expect("V2__run_logs migration name is well-formed"),
        Migration::unapplied(
            "V3__backfill_launched_by",
            include_str!("migrations/V3__backfill_launched_by.surql"),
        )
        .expect("V3__backfill_launched_by migration name is well-formed"),
        Migration::unapplied(
            "V4__run_action",
            include_str!("migrations/V4__run_action.surql"),
        )
        .expect("V4__run_action migration name is well-formed"),
        Migration::unapplied(
            "V5__slot_partition_scope",
            include_str!("migrations/V5__slot_partition_scope.surql"),
        )
        .expect("V5__slot_partition_scope migration name is well-formed"),
        Migration::unapplied(
            "V6__deletion_tombstones",
            include_str!("migrations/V6__deletion_tombstones.surql"),
        )
        .expect("V6__deletion_tombstones migration name is well-formed"),
        Migration::unapplied(
            "V7__provenance_timestamp",
            include_str!("migrations/V7__provenance_timestamp.surql"),
        )
        .expect("V7__provenance_timestamp migration name is well-formed"),
        Migration::unapplied(
            "V8__run_logs_traceback",
            include_str!("migrations/V8__run_logs_traceback.surql"),
        )
        .expect("V8__run_logs_traceback migration name is well-formed"),
        Migration::unapplied(
            "V9__run_config",
            include_str!("migrations/V9__run_config.surql"),
        )
        .expect("V9__run_config migration name is well-formed"),
        Migration::unapplied(
            "V10__launch_document",
            include_str!("migrations/V10__launch_document.surql"),
        )
        .expect("V10__launch_document migration name is well-formed"),
        Migration::unapplied(
            "V11__runs_end_time_index",
            include_str!("migrations/V11__runs_end_time_index.surql"),
        )
        .expect("V11__runs_end_time_index migration name is well-formed"),
    ]
}

/// Apply all pending migrations via refinery (idempotent — applied ones are
/// skipped; an edited applied one aborts on a checksum mismatch).
async fn apply_migrations(db: &Surreal<Any>) -> anyhow::Result<()> {
    let migrations = embedded_migrations();
    let before = read_schema_stamps(db).await?.map_or(0, |s| s.version);
    let mut backend = SurrealMigrate { db: db.clone() };
    let apply = async |backend: &mut SurrealMigrate, target: Target| {
        backend
            .migrate(
                &migrations,
                true,  // abort_divergent: an edited applied migration is an error
                false, // abort_missing: tolerate a DB carrying migrations we don't embed
                false, // grouped: one transaction per migration
                target,
                REFINERY_HISTORY_TABLE,
            )
            .await
            .context("applying storage migrations")
    };
    // V10 changes the shape of stored rows. They are rewritten between the
    // DDL before it and its own stamp, so a store never reads as V10 while
    // holding V9 rows.
    if before < 10 {
        apply(&mut backend, Target::Version(9)).await?;
        rewrite_v10_launch_documents(db).await?;
    }
    apply(&mut backend, Target::Latest).await?;
    Ok(())
}

/// The V9 `config` (`{"<asset>": {fields}}`) on runs and backfills as the
/// launch document. A row already in the new shape is left alone.
async fn rewrite_v10_launch_documents(db: &Surreal<Any>) -> anyhow::Result<()> {
    #[derive(SurrealValue)]
    struct Row {
        ident: String,
        config: String,
    }
    for (table, key) in [("runs", "run_id"), ("backfills", "backfill_id")] {
        let rows: Vec<Row> = db
            .query(format!(
                "SELECT {key} AS ident, config FROM {table} WHERE config != NONE"
            ))
            .await?
            .check()?
            .take(0)?;
        for row in rows {
            let Some(document) = launch_document_from_v9(&row.config) else {
                continue;
            };
            db.query(format!(
                "UPDATE {table} SET config = $config WHERE {key} = $ident"
            ))
            .bind(("config", document))
            .bind(("ident", row.ident))
            .await?
            .check()?;
        }
    }
    Ok(())
}

/// `{"<asset>": {fields}}` as `{"assets": {"<asset>": {"config": {fields}}}}`;
/// `None` for text that is not the V9 shape, the new shape included.
fn launch_document_from_v9(text: &str) -> Option<String> {
    let serde_json::Value::Object(old) = serde_json::from_str(text).ok()? else {
        return None;
    };
    if old
        .keys()
        .all(|k| ["assets", "resources", "execution"].contains(&k.as_str()))
    {
        return None;
    }
    let assets: serde_json::Map<String, serde_json::Value> = old
        .into_iter()
        .map(|(asset, fields)| (asset, serde_json::json!({ "config": fields })))
        .collect();
    Some(serde_json::json!({ "assets": assets }).to_string())
}

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
struct SchemaStamps {
    version: u32,
    min_reader: u32,
    min_writer: u32,
}

/// Read the compat stamps — the latest applied migration's `migration_meta` row,
/// whose floors are the current contract (each migration records the cumulative
/// floors). `None` if uninitialized: an undefined `migration_meta` table errors
/// (not empties), and that maps to `None` so the open path inits.
async fn read_schema_stamps(db: &Surreal<Any>) -> anyhow::Result<Option<SchemaStamps>> {
    #[derive(SurrealValue)]
    struct Row {
        version: i64,
        min_reader: i64,
        min_writer: i64,
    }
    let query = format!(
        "SELECT version, min_reader, min_writer \
         FROM {MIGRATION_META_TABLE} ORDER BY version DESC LIMIT 1"
    );
    let mut response = match db.query(query).await {
        Ok(response) => response,
        Err(err) if is_undefined_table_error(&err) => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let rows: Vec<Row> = match response.take(0) {
        Ok(rows) => rows,
        Err(err) if is_undefined_table_error(&err) => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    // No rows ⇒ defined but empty ⇒ uninitialized.
    Ok(rows.into_iter().next().map(|r| SchemaStamps {
        version: r.version as u32,
        min_reader: r.min_reader as u32,
        min_writer: r.min_writer as u32,
    }))
}

/// True if `err` is specifically "table not defined" (how a fresh store presents).
/// Matched structurally, not by a bare substring other errors share.
fn is_undefined_table_error(err: &surrealdb::Error) -> bool {
    use surrealdb::types::{ErrorDetails, NotFoundError};
    if matches!(
        err.details(),
        ErrorDetails::NotFound(Some(NotFoundError::Table { .. }))
    ) {
        return true;
    }
    // Some engines surface this as a plain message; still require "table" so the
    // narrowing keeps excluding the other "<x> does not exist" errors.
    let msg = err.to_string().to_ascii_lowercase();
    msg.contains("table") && msg.contains("does not exist")
}

/// The `migration_lock` table the lease needs *before* any migration runs — the
/// lease serializes who applies migrations, so its own table can't be one
const MIGRATION_LOCK_SCHEMA: &str = "\
DEFINE TABLE IF NOT EXISTS migration_lock SCHEMAFULL; \
DEFINE FIELD IF NOT EXISTS holder ON migration_lock TYPE string; \
DEFINE FIELD IF NOT EXISTS expires_at ON migration_lock TYPE int;";

/// Define the `migration_lock` table ([`MIGRATION_LOCK_SCHEMA`]) before the lease
/// is taken. Idempotent; init/migrate path only, never a normal connect.
async fn ensure_lock_table(db: &Surreal<Any>) -> anyhow::Result<()> {
    db.query(MIGRATION_LOCK_SCHEMA).await?.check()?;
    Ok(())
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
/// Pure — I/O lives in [`ensure_compatible`].
fn check_compatibility(stamps: SchemaStamps, cap: Capability, build: u32) -> anyhow::Result<()> {
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

/// Open-time gate. Schema isn't applied here (only in [`migrate_to_current`]):
/// a stamped store is judged by [`check_compatibility`], an uninitialized one is
/// bootstrapped by the first opener (UI included).
pub(super) async fn ensure_compatible(db: &Surreal<Any>, cap: Capability) -> anyhow::Result<()> {
    // `rivers db migrate` opens `Migrate`: always run the setup/upgrade.
    if cap == Capability::Migrate {
        return migrate_to_current(db).await;
    }
    match read_schema_stamps(db).await? {
        Some(stamps) => check_compatibility(stamps, cap, SCHEMA_VERSION),
        // First opener of an uninitialized store bootstraps it (any capability,
        // UI included); a stamped-but-behind DB was already refused above.
        None => migrate_to_current(db).await,
    }
}

/// Cross-process migration lease — a migration must run alone. One
/// `migration_lock:lease` record, heartbeat-renewed; a crash frees it after one TTL.
const MIGRATION_LEASE_TTL_SECS: i64 = 30;
/// Renew well inside the TTL so a slow round-trip never lets an active holder's
/// lease lapse.
const MIGRATION_LEASE_RENEW_SECS: u64 = 10;
/// How long a process waiting on the lease sleeps between attempts.
const MIGRATION_LEASE_POLL_SECS: u64 = 1;

/// True if `CREATE migration_lock:lease` failed because the id is already taken
/// (another opener holds it). Structural match, with a message fallback.
fn is_lease_taken_error(err: &anyhow::Error) -> bool {
    use surrealdb::types::ErrorDetails;
    if err
        .chain()
        .find_map(|e| e.downcast_ref::<surrealdb::Error>())
        .is_some_and(|se| matches!(se.details(), ErrorDetails::AlreadyExists(_)))
    {
        return true;
    }
    let m = err.to_string();
    m.contains("already exists") || m.contains("already contains")
}

/// Take the lease: clear any expired one, then `CREATE` the fixed id — one caller
/// wins, losers get already-exists → `None`. Returns the holder token, or `None`.
async fn try_acquire_migration_lease(db: &Surreal<Any>) -> anyhow::Result<Option<String>> {
    let holder = uuid::Uuid::new_v4().to_string();
    let now_ns = now_nanos();
    let exp_ns = now_ns + MIGRATION_LEASE_TTL_SECS * 1_000_000_000;
    let outcome: anyhow::Result<()> = async {
        db.query(
            "DELETE migration_lock:lease WHERE expires_at <= $now; \
             CREATE migration_lock:lease SET holder = $holder, expires_at = $exp;",
        )
        .bind(("now", now_ns))
        .bind(("exp", exp_ns))
        .bind(("holder", holder.clone()))
        .await?
        .check()?;
        Ok(())
    }
    .await;
    match outcome {
        Ok(()) => Ok(Some(holder)),
        Err(err) if is_lease_taken_error(&err) => Ok(None),
        Err(err) => Err(err),
    }
}

/// Push the expiry forward. Errors if we no longer hold it (aborts the migration).
async fn renew_migration_lease(db: &Surreal<Any>, holder: &str) -> anyhow::Result<()> {
    let exp_ns = now_nanos() + MIGRATION_LEASE_TTL_SECS * 1_000_000_000;
    // UPDATE doesn't return a usable row count via the Rust SDK, so a follow-up
    // SELECT counts whether we still hold the record (see `renew_slot_lease`).
    let mut response = db
        .query(
            "UPDATE migration_lock:lease SET expires_at = $exp WHERE holder = $holder; \
             SELECT count() AS total FROM migration_lock:lease WHERE holder = $holder GROUP ALL",
        )
        .bind(("holder", holder.to_string()))
        .bind(("exp", exp_ns))
        .await?
        .check()?;
    let renewed: Option<u32> = response.take((1, "total"))?;
    if renewed.unwrap_or(0) == 0 {
        anyhow::bail!("lost the migration lease (another process took over)");
    }
    Ok(())
}

/// Release the lease (best-effort; a crash is covered by the TTL).
async fn release_migration_lease(db: &Surreal<Any>, holder: &str) -> anyhow::Result<()> {
    db.query("DELETE migration_lock:lease WHERE holder = $holder")
        .bind(("holder", holder.to_string()))
        .await?
        .check()?;
    Ok(())
}

/// Drive `work` while renewing the lease on a timer (so it outlives the TTL).
/// A failed renewal drops `work`, cancelling the migration.
async fn with_lease_renewal<F>(db: &Surreal<Any>, holder: &str, work: F) -> anyhow::Result<()>
where
    F: std::future::Future<Output = anyhow::Result<()>>,
{
    tokio::pin!(work);
    let mut ticker =
        tokio::time::interval(std::time::Duration::from_secs(MIGRATION_LEASE_RENEW_SECS));
    ticker.tick().await; // the first tick fires immediately; skip it
    loop {
        tokio::select! {
            result = &mut work => return result,
            _ = ticker.tick() => renew_migration_lease(db, holder).await?,
        }
    }
}

/// Initialize or upgrade a store to [`SCHEMA_VERSION`] — backs first-opener init
/// and `rivers db migrate`. refinery applies the pending migrations (idempotent);
/// the lease serializes openers and a downgrade is refused before locking.
async fn migrate_to_current(db: &Surreal<Any>) -> anyhow::Result<()> {
    ensure_lock_table(db).await?;
    let mut waited = false;
    loop {
        // Fast path: already current → nothing to do, no lease needed. A
        // downgrade (database ahead of this build) is refused here, before locking.
        if let Some(stamps) = read_schema_stamps(db).await? {
            check_compatibility(stamps, Capability::Migrate, SCHEMA_VERSION)?;
            if stamps.version == SCHEMA_VERSION {
                return Ok(());
            }
        }
        match try_acquire_migration_lease(db).await? {
            Some(holder) => {
                let result = with_lease_renewal(db, &holder, apply_migrations(db)).await;
                // Release on every outcome so a transient failure doesn't wedge
                // the next opener; a crash is handled by the lease TTL instead.
                if let Err(err) = release_migration_lease(db, &holder).await {
                    tracing::warn!(error = %err, "failed to release migration lease");
                }
                return result;
            }
            // Another process is migrating. Wait, then re-loop: it either
            // finishes (the fast path returns) or its lease lapses (we retry).
            None => {
                if !waited {
                    tracing::info!("another process is migrating storage; waiting for it");
                    waited = true;
                }
                tokio::time::sleep(std::time::Duration::from_secs(MIGRATION_LEASE_POLL_SECS)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests;
