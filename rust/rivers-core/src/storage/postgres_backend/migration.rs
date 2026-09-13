//! Schema versioning and migration for the PostgreSQL backend.
//!
//! refinery applies the ordered `.sql` migrations and records them; each
//! migration writes its compat metadata into `migration_meta`; the shared
//! capability floor guard in [`crate::storage::migration`] wraps it.
//!
//! Unlike the SurrealDB backend, no dialect overrides are needed — refinery's
//! default history SQL is already PostgreSQL — and no heartbeat lease, because
//! `pg_advisory_lock` releases on disconnect.

// Reached only from the tests until `PostgresStorage` lands and calls
// `ensure_compatible` on open.
#![allow(dead_code)]

use anyhow::Context;
use refinery_core::traits::r#async::{AsyncMigrate, AsyncQuery, AsyncTransaction};
use refinery_core::{Migration, Target};
use sqlx::AssertSqlSafe;
use sqlx::{PgPool, Row};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::storage::migration::{Capability, SchemaStamps, check_compatibility};

/// refinery's history table — one checksummed row per applied migration.
const REFINERY_HISTORY_TABLE: &str = "refinery_schema_history";

/// Highest embedded migration version. Bump by adding a `Vn__*.sql` + an
/// [`embedded_migrations`] entry; a test pins this to that max.
const SCHEMA_VERSION: u32 = 3;

/// One compat row per migration (the floors it set), folded by the open guard.
const MIGRATION_META_TABLE: &str = "migration_meta";

/// Advisory-lock key serialising migrators across processes. Any constant works
/// as long as every rivers build uses the same one; the value is arbitrary.
const MIGRATION_LOCK_KEY: i64 = 0x7269_7665_7273_0001;

/// PostgreSQL's `undefined_table`, which is how a fresh database presents.
const UNDEFINED_TABLE: &str = "42P01";

/// A refinery backend over a PostgreSQL pool.
struct PgMigrate {
    pool: PgPool,
}

#[async_trait::async_trait]
impl AsyncTransaction for PgMigrate {
    type Error = sqlx::Error;

    async fn execute<'a, T: Iterator<Item = &'a str> + Send>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        // refinery hands us a migration's SQL + its history INSERT; one
        // transaction so a history row never lands without its migration applied.
        // Owned, because the borrow lives only as long as the call while the
        // async body outlives it.
        let stmts: Vec<String> = queries
            .map(|q| q.trim().to_owned())
            .filter(|q| !q.is_empty())
            .collect();
        let count = stmts.len();
        let mut tx = self.pool.begin().await?;
        for stmt in stmts {
            // AssertSqlSafe: the text is refinery's, built from the embedded
            // migration files and its own history INSERT — never user input.
            sqlx::raw_sql(AssertSqlSafe(stmt)).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(count)
    }
}

#[async_trait::async_trait]
impl AsyncQuery<Vec<Migration>> for PgMigrate {
    async fn query(&mut self, query: &str) -> Result<Vec<Migration>, Self::Error> {
        // AssertSqlSafe: refinery's own history SELECT.
        let rows = sqlx::query(AssertSqlSafe(query.to_owned()))
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                let version: i32 = r.get("version");
                let name: String = r.get("name");
                let applied_on: String = r.get("applied_on");
                let checksum: String = r.get("checksum");
                // applied_on/checksum were written by refinery in RFC3339/u64 form.
                let applied_on =
                    OffsetDateTime::parse(&applied_on, &Rfc3339).expect("applied_on is RFC3339");
                Migration::applied(
                    version,
                    name,
                    applied_on,
                    checksum.parse::<u64>().expect("checksum is a u64"),
                )
            })
            .collect())
    }
}

// refinery's default history SQL is already PostgreSQL, so no dialect overrides.
impl AsyncMigrate for PgMigrate {}

/// The migrations embedded in this build, applied in order; refinery checksums each.
fn embedded_migrations() -> Vec<Migration> {
    vec![
        Migration::unapplied("V1__base", include_str!("migrations/V1__base.sql"))
            .expect("V1__base migration name is well-formed"),
        Migration::unapplied("V2__run_logs", include_str!("migrations/V2__run_logs.sql"))
            .expect("V2__run_logs migration name is well-formed"),
        Migration::unapplied(
            "V3__backfill_launched_by",
            include_str!("migrations/V3__backfill_launched_by.sql"),
        )
        .expect("V3__backfill_launched_by migration name is well-formed"),
    ]
}

/// Read the compat stamps — the latest applied migration's `migration_meta` row,
/// whose floors are the current contract. `None` if uninitialized.
async fn read_schema_stamps(pool: &PgPool) -> anyhow::Result<Option<SchemaStamps>> {
    let sql = format!(
        "SELECT version, min_reader, min_writer \
         FROM {MIGRATION_META_TABLE} ORDER BY version DESC LIMIT 1"
    );
    // AssertSqlSafe: interpolates only MIGRATION_META_TABLE, a crate constant.
    let row = match sqlx::query(AssertSqlSafe(sql)).fetch_optional(pool).await {
        Ok(row) => row,
        Err(err) if is_undefined_table_error(&err) => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    Ok(row.map(|r| SchemaStamps {
        version: r.get::<i64, _>("version") as u32,
        min_reader: r.get::<i64, _>("min_reader") as u32,
        min_writer: r.get::<i64, _>("min_writer") as u32,
    }))
}

/// True if `err` is specifically `undefined_table`, which is how a database
/// with no rivers schema presents. Matched on SQLSTATE, not on message text.
fn is_undefined_table_error(err: &sqlx::Error) -> bool {
    matches!(err.as_database_error().and_then(|e| e.code()), Some(code) if code == UNDEFINED_TABLE)
}

/// Apply all pending migrations, alone.
///
/// The advisory lock is held on a dedicated connection for the whole run.
/// PostgreSQL drops it when that connection closes, so a crashed migrator
/// frees it without the TTL the SurrealDB backend's lease needs.
async fn migrate_to_current(pool: &PgPool) -> anyhow::Result<()> {
    let mut lock_conn = pool
        .acquire()
        .await
        .context("acquiring a connection for the migration lock")?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(MIGRATION_LOCK_KEY)
        .execute(&mut *lock_conn)
        .await
        .context("taking the migration advisory lock")?;

    let result = apply_migrations(pool).await;

    // Best-effort: dropping `lock_conn` releases the lock regardless.
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(MIGRATION_LOCK_KEY)
        .execute(&mut *lock_conn)
        .await;
    result
}

/// Apply all pending migrations via refinery (idempotent — applied ones are
/// skipped; an edited applied one aborts on a checksum mismatch).
async fn apply_migrations(pool: &PgPool) -> anyhow::Result<()> {
    let migrations = embedded_migrations();
    let mut backend = PgMigrate { pool: pool.clone() };
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

/// Open-time gate. Schema isn't applied here (only in [`migrate_to_current`]):
/// a stamped store is judged by the shared guard, an uninitialized one is
/// bootstrapped by the first opener.
pub(super) async fn ensure_compatible(pool: &PgPool, cap: Capability) -> anyhow::Result<()> {
    // `rivers db migrate` opens `Migrate`: always run the setup/upgrade.
    if cap == Capability::Migrate {
        return migrate_to_current(pool).await;
    }
    match read_schema_stamps(pool).await? {
        Some(stamps) => check_compatibility(stamps, cap, SCHEMA_VERSION),
        None => migrate_to_current(pool).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A live PostgreSQL to migrate against, or `None` to skip. CI sets this;
    /// locally, `docker run -e POSTGRES_PASSWORD=… -p 55432:5432 postgres:18-alpine`.
    ///
    /// The skip prints, because the harness reports a skipped test as `ok` and
    /// a silent pass would hide a PostgreSQL leg that never ran.
    fn test_url() -> Option<String> {
        let url = std::env::var("RIVERS_TEST_POSTGRES_URL")
            .ok()
            .filter(|u| !u.is_empty());
        if url.is_none() {
            eprintln!("SKIPPED: RIVERS_TEST_POSTGRES_URL is unset, no PostgreSQL to test against");
        }
        url
    }

    /// A pool over an empty schema of its own, so tests do not see each other.
    ///
    /// The schema must come from connect options, not a `SET search_path`:
    /// that only binds the one connection it runs on, and the next query from
    /// the pool would land back in `public`.
    async fn fresh_pool(url: &str, schema: &str) -> PgPool {
        let admin = PgPool::connect(url).await.expect("connect");
        sqlx::raw_sql(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema};"
        )))
        .execute(&admin)
        .await
        .expect("fresh schema");
        admin.close().await;

        let opts: sqlx::postgres::PgConnectOptions = url.parse().expect("valid url");
        PgPool::connect_with(opts.options([("search_path", schema)]))
            .await
            .expect("connect to the test schema")
    }

    /// Migrate a fresh database, then migrate it again: the schema lands, the
    /// stamps match the embedded migrations, and re-running changes nothing.
    #[tokio::test]
    async fn migrate_is_idempotent_and_stamps_the_schema() {
        let Some(url) = test_url() else { return };
        let pool = fresh_pool(&url, "rivers_mig_test").await;

        assert_eq!(
            read_schema_stamps(&pool).await.expect("stamps"),
            None,
            "a database with no rivers tables must read as uninitialized"
        );

        ensure_compatible(&pool, Capability::Migrate)
            .await
            .expect("first migrate");
        let after_first = read_schema_stamps(&pool)
            .await
            .expect("stamps")
            .expect("stamped");
        assert_eq!(after_first.version, SCHEMA_VERSION);
        assert_eq!(after_first.min_reader, 2, "V2 raised the reader floor");
        assert_eq!(after_first.min_writer, 2, "V2 raised the writer floor");

        ensure_compatible(&pool, Capability::Migrate)
            .await
            .expect("second migrate");
        assert_eq!(
            read_schema_stamps(&pool)
                .await
                .expect("stamps")
                .expect("stamped"),
            after_first,
            "re-migrating an up-to-date database must not move the stamps"
        );

        let applied: i64 = sqlx::query(AssertSqlSafe(format!(
            "SELECT count(*) FROM {REFINERY_HISTORY_TABLE}"
        )))
        .fetch_one(&pool)
        .await
        .expect("history")
        .get(0);
        assert_eq!(applied, embedded_migrations().len() as i64);
    }

    /// A migrated database admits readers and writers at this build's version.
    #[tokio::test]
    async fn migrated_database_admits_read_and_write() {
        let Some(url) = test_url() else { return };
        let pool = fresh_pool(&url, "rivers_cap_test").await;

        ensure_compatible(&pool, Capability::Migrate)
            .await
            .expect("migrate");
        ensure_compatible(&pool, Capability::Read)
            .await
            .expect("read is admitted");
        ensure_compatible(&pool, Capability::ReadWrite)
            .await
            .expect("write is admitted");
    }

    #[test]
    fn schema_version_matches_the_highest_embedded_migration() {
        let highest = embedded_migrations()
            .iter()
            .map(|m| m.version())
            .max()
            .expect("at least one embedded migration");
        assert_eq!(highest as u32, SCHEMA_VERSION);
    }

    #[test]
    fn every_surrealql_migration_has_a_sql_twin() {
        // The two backends must stay at the same schema version, or a database
        // migrated under one and opened under the other would disagree.
        let surreal = std::fs::read_dir(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/storage/surrealdb_backend/migrations"
        ))
        .expect("surrealdb migrations dir")
        .count();
        assert_eq!(surreal, embedded_migrations().len());
    }
}
