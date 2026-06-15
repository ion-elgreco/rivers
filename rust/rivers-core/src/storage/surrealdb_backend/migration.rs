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
const SCHEMA_VERSION: u32 = 1;

/// One compat row per migration (the floors it set), folded by the open guard.
const MIGRATION_META_TABLE: &str = "migration_meta";

/// The migrations embedded in this build, applied in order; refinery checksums each.
fn embedded_migrations() -> Vec<Migration> {
    vec![
        Migration::unapplied("V1__base", include_str!("migrations/V1__base.surql"))
            .expect("V1__base migration name is well-formed"),
    ]
}

/// Apply all pending migrations via refinery (idempotent — applied ones are
/// skipped; an edited applied one aborts on a checksum mismatch).
async fn apply_migrations(db: &Surreal<Any>) -> anyhow::Result<()> {
    let migrations = embedded_migrations();
    let mut backend = SurrealMigrate { db: db.clone() };
    backend
        .migrate(
            &migrations,
            true,  // abort_divergent: an edited applied migration is an error
            false, // abort_missing: tolerate a DB carrying migrations we don't embed
            false, // grouped: one transaction per migration
            Target::Latest,
            REFINERY_HISTORY_TABLE,
        )
        .await
        .context("applying storage migrations")?;
    Ok(())
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
mod tests {
    use super::super::*;
    use super::*;

    async fn count_rows(db: &Surreal<Any>, table: &str) -> u64 {
        let mut r = db
            .query(format!("SELECT count() AS n FROM {table} GROUP ALL"))
            .await
            .unwrap();
        let n: Option<u64> = r.take((0, "n")).unwrap();
        n.unwrap_or(0)
    }

    /// Plant a `migration_meta` row so the open guard reads the given stamps. The
    /// fold takes the max, so values dominating any existing row set the result.
    async fn plant_stamps(db: &Surreal<Any>, version: u32, min_reader: u32, min_writer: u32) {
        db.query(format!(
            "UPSERT migration_meta:{version} CONTENT \
             {{ version: {version}, class: 'planted', min_reader: {min_reader}, min_writer: {min_writer} }}"
        ))
        .await
        .unwrap()
        .check()
        .unwrap();
    }

    /// `SCHEMA_VERSION` must equal the highest embedded migration — the guard
    /// compares the DB's applied version against it.
    #[test]
    fn test_schema_version_matches_embedded() {
        let max = embedded_migrations()
            .iter()
            .map(|m| m.version() as u64)
            .max()
            .expect("at least one migration");
        assert_eq!(
            max, SCHEMA_VERSION as u64,
            "SCHEMA_VERSION must track the highest embedded migration"
        );
    }

    /// The refinery backend applies a migration end-to-end through refinery's
    /// real `migrate()` loop: schema lands, refinery records it, the
    /// `migration_meta` floor row is written, and a re-run is idempotent.
    #[tokio::test]
    async fn test_refinery_backend_applies_and_is_idempotent() {
        let db = any::connect("mem://").await.unwrap();
        db.use_ns(DEFAULT_NAMESPACE)
            .use_db(DEFAULT_DATABASE)
            .await
            .unwrap();

        const V1: &str = "\
            DEFINE TABLE probe_assets SCHEMAFULL; \
            DEFINE FIELD name ON probe_assets TYPE string; \
            DEFINE TABLE migration_meta SCHEMAFULL; \
            DEFINE FIELD version ON migration_meta TYPE int; \
            DEFINE FIELD class ON migration_meta TYPE string; \
            DEFINE FIELD min_reader ON migration_meta TYPE int; \
            DEFINE FIELD min_writer ON migration_meta TYPE int; \
            UPSERT migration_meta:1 CONTENT { version: 1, class: 'additive', min_reader: 1, min_writer: 1 };";

        let migrations = vec![Migration::unapplied("V1__base", V1).unwrap()];
        let mut backend = SurrealMigrate { db: db.clone() };

        backend
            .migrate(
                &migrations,
                true,
                false,
                false,
                Target::Latest,
                REFINERY_HISTORY_TABLE,
            )
            .await
            .expect("first migrate");

        assert_eq!(
            count_rows(&db, REFINERY_HISTORY_TABLE).await,
            1,
            "one history row"
        );
        assert_eq!(
            count_rows(&db, "probe_assets").await,
            0,
            "schema defined (empty)"
        );
        assert_eq!(count_rows(&db, "migration_meta").await, 1, "one floor row");

        backend
            .migrate(
                &migrations,
                true,
                false,
                false,
                Target::Latest,
                REFINERY_HISTORY_TABLE,
            )
            .await
            .expect("second migrate");
        assert_eq!(
            count_rows(&db, REFINERY_HISTORY_TABLE).await,
            1,
            "idempotent: still one history row"
        );
    }

    /// Editing an already-applied migration is caught by refinery's checksum
    /// (the frozen-baseline guard — a different V1 body diverges).
    #[tokio::test]
    async fn test_edited_migration_is_refused_as_divergent() {
        let db = any::connect("mem://").await.unwrap();
        db.use_ns(DEFAULT_NAMESPACE)
            .use_db(DEFAULT_DATABASE)
            .await
            .unwrap();
        apply_migrations(&db).await.expect("apply real V1");

        // Same version + name, different body → different checksum → divergent.
        let edited =
            vec![Migration::unapplied("V1__base", "DEFINE TABLE tampered SCHEMAFULL;").unwrap()];
        let mut backend = SurrealMigrate { db: db.clone() };
        let res = backend
            .migrate(
                &edited,
                true,
                false,
                false,
                Target::Latest,
                REFINERY_HISTORY_TABLE,
            )
            .await;
        assert!(res.is_err(), "an edited applied migration must be refused");
    }

    /// The cross-process lease serializes openers: a second acquire returns
    /// `None`, releasing hands it on, and renewal only works for the holder.
    #[tokio::test]
    async fn test_migration_lease_serializes_and_hands_off() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        let db = &storage.db;

        let holder = try_acquire_migration_lease(db)
            .await
            .unwrap()
            .expect("first acquire must win");
        assert!(
            try_acquire_migration_lease(db).await.unwrap().is_none(),
            "a held, unexpired lease must block a second acquire"
        );
        renew_migration_lease(db, &holder).await.unwrap();
        assert!(
            renew_migration_lease(db, "not-the-holder").await.is_err(),
            "renewing a lease we do not hold must fail"
        );
        release_migration_lease(db, &holder).await.unwrap();
        let next = try_acquire_migration_lease(db)
            .await
            .unwrap()
            .expect("a released lease must be re-acquirable");
        assert_ne!(next, holder, "each acquire mints a fresh holder token");
    }

    /// The open-time floor guard, exhaustively. `build` is a literal here, so the
    /// matrix is independent of `SCHEMA_VERSION`.
    #[test]
    fn test_check_compatibility_matrix() {
        let s = |version, min_reader, min_writer| SchemaStamps {
            version,
            min_reader,
            min_writer,
        };
        use Capability::{Migrate, Read, ReadWrite};
        assert!(check_compatibility(s(3, 1, 2), ReadWrite, 3).is_ok());
        assert!(check_compatibility(s(3, 1, 2), Read, 3).is_ok());
        assert!(check_compatibility(s(5, 1, 5), Read, 3).is_ok());
        assert!(check_compatibility(s(5, 1, 5), ReadWrite, 3).is_err());
        assert!(check_compatibility(s(5, 4, 5), Read, 3).is_err());
        assert!(check_compatibility(s(2, 1, 2), ReadWrite, 3).is_err());
        assert!(check_compatibility(s(2, 1, 2), Read, 3).is_err());
        assert!(check_compatibility(s(2, 1, 2), Migrate, 3).is_ok());
        assert!(check_compatibility(s(3, 1, 2), Migrate, 3).is_ok());
        assert!(check_compatibility(s(5, 1, 5), Migrate, 3).is_err());
    }

    /// A build newer than the database surfaces the typed `SchemaMigrationNeeded`
    /// (the contract the PyO3 boundary downcasts) and names the fix.
    #[test]
    fn test_behind_build_surfaces_typed_error() {
        let stamps = SchemaStamps {
            version: 1,
            min_reader: 1,
            min_writer: 1,
        };
        let err = check_compatibility(stamps, Capability::ReadWrite, 2).unwrap_err();
        assert!(
            err.downcast_ref::<SchemaMigrationNeeded>().is_some(),
            "{err}"
        );
        assert!(err.to_string().contains("rivers db migrate"), "{err}");
    }

    /// On a brand-new store `migration_meta` is undefined; `read_schema_stamps`
    /// maps that to `None` (uninitialized) so the first opener can bootstrap.
    #[tokio::test]
    async fn test_read_stamps_on_undefined_table_is_none() {
        let db = any::connect("mem://").await.unwrap();
        db.use_ns(DEFAULT_NAMESPACE)
            .use_db(DEFAULT_DATABASE)
            .await
            .unwrap();
        let stamps = read_schema_stamps(&db).await.unwrap();
        assert!(
            stamps.is_none(),
            "an undefined migration_meta table reads as uninitialized, got {stamps:?}"
        );
    }

    /// Stamps come from the *latest* migration's row, not a per-column max — so a
    /// higher floor on an older row never leaks into the current contract.
    #[tokio::test]
    async fn test_read_stamps_uses_latest_row_not_column_max() {
        let db = any::connect("mem://").await.unwrap();
        db.use_ns(DEFAULT_NAMESPACE)
            .use_db(DEFAULT_DATABASE)
            .await
            .unwrap();
        db.query(
            "DEFINE TABLE migration_meta SCHEMAFULL; \
             DEFINE FIELD version ON migration_meta TYPE int; \
             DEFINE FIELD class ON migration_meta TYPE string; \
             DEFINE FIELD min_reader ON migration_meta TYPE int; \
             DEFINE FIELD min_writer ON migration_meta TYPE int; \
             UPSERT migration_meta:1 CONTENT { version: 1, class: 'x', min_reader: 1, min_writer: 9 }; \
             UPSERT migration_meta:2 CONTENT { version: 2, class: 'x', min_reader: 1, min_writer: 2 };",
        )
        .await
        .unwrap()
        .check()
        .unwrap();
        let s = read_schema_stamps(&db).await.unwrap().unwrap();
        assert_eq!(
            (s.version, s.min_reader, s.min_writer),
            (2, 1, 2),
            "stamps must be the latest (v2) row, not a per-column max (which would give min_writer 9)"
        );
    }

    /// An uninitialized store is built by the first opener (the read-only UI
    /// included): `ensure_compatible` runs the migrations and the guard reads current.
    #[tokio::test]
    async fn test_uninitialized_store_is_initialized_by_a_reader() {
        let db = any::connect("mem://").await.unwrap();
        db.use_ns(DEFAULT_NAMESPACE)
            .use_db(DEFAULT_DATABASE)
            .await
            .unwrap();
        ensure_compatible(&db, Capability::Read).await.unwrap();
        let stamps = read_schema_stamps(&db).await.unwrap();
        assert!(
            matches!(stamps, Some(s) if s.version == SCHEMA_VERSION),
            "first-opener init must reach the current version, got {stamps:?}"
        );
    }

    /// Reader/writer split: a DB whose writer floor is far ahead refuses an old
    /// `ReadWrite` opener but keeps an old read-only one (the UI) running.
    #[tokio::test]
    async fn test_open_allows_old_reader_but_refuses_old_writer() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        plant_stamps(&storage.db, SCHEMA_VERSION + 2, 1, SCHEMA_VERSION + 2).await;
        assert!(
            ensure_compatible(&storage.db, Capability::Read)
                .await
                .is_ok(),
            "an older read-only opener must keep working"
        );
        assert!(
            ensure_compatible(&storage.db, Capability::ReadWrite)
                .await
                .is_err(),
            "an older writer must be refused"
        );
    }

    /// Explicit migrate is idempotent and refuses a downgrade (DB ahead of build).
    #[tokio::test]
    async fn test_migrate_is_idempotent_and_refuses_downgrade() {
        let temp_dir = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage");
        migrate_to_current(&storage.db).await.unwrap();
        assert!(matches!(
            read_schema_stamps(&storage.db).await.unwrap(),
            Some(s) if s.version == SCHEMA_VERSION
        ));
        plant_stamps(&storage.db, SCHEMA_VERSION + 1, 1, 1).await;
        assert!(
            migrate_to_current(&storage.db).await.is_err(),
            "downgrade must be refused"
        );
    }
}
