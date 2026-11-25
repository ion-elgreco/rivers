//! Schema versioning and migration for the PostgreSQL backend.
//!
//! refinery applies the migrations and records them. Each migration writes
//! its compat metadata into `migration_meta`, and the shared floor guard of
//! [`crate::storage::migration`] reads it.
//!
//! The baseline is the schema at version 17, the first version that had a
//! PostgreSQL backend, so there are no migrations for the versions before it.
//!
//! refinery's default history SQL is PostgreSQL already. A migration takes
//! `pg_advisory_lock`, which the server frees when the connection closes, so
//! there is no lease to renew.

use anyhow::Context;
use refinery_core::traits::r#async::{AsyncMigrate, AsyncQuery, AsyncTransaction};
use refinery_core::{Migration, Target};
use sqlx::{AssertSqlSafe, PgPool, Row};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::storage::migration::{Capability, SCHEMA_VERSION, SchemaStamps, check_compatibility};

/// refinery's history table: one checksummed row per applied migration.
const REFINERY_HISTORY_TABLE: &str = "refinery_schema_history";

/// One advisory lock for all migrators of a schema: this key with the hash
/// of the schema name. Each build must use the same key; its value has no
/// meaning.
const MIGRATION_LOCK_KEY: i32 = 0x7269_7601;

/// PostgreSQL's `undefined_table`: how a database without the schema answers.
const UNDEFINED_TABLE: &str = "42P01";

/// The schema at version 17, one file per area of the backend. The triggers
/// come last: they need the tables.
const V17_BASE: &str = concat!(
    include_str!("schema/core.sql"),
    include_str!("schema/directory.sql"),
    include_str!("schema/runs.sql"),
    include_str!("schema/events.sql"),
    include_str!("schema/run_logs.sql"),
    include_str!("schema/assets.sql"),
    include_str!("schema/partitions.sql"),
    include_str!("schema/backfills.sql"),
    include_str!("schema/automation.sql"),
    include_str!("schema/pools.sql"),
    include_str!("schema/read_indexes.sql"),
    include_str!("schema/notify.sql"),
);

/// A refinery backend over a PostgreSQL pool.
struct PgMigrate {
    pool: PgPool,
}

impl AsyncTransaction for PgMigrate {
    type Error = sqlx::Error;

    async fn execute<'a, T: Iterator<Item = &'a str> + Send + 'a>(
        &'a mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        // refinery sends a migration's SQL and its history INSERT: one
        // transaction, so no history row exists without its migration.
        let statements: Vec<String> = queries
            .map(|q| q.trim().to_owned())
            .filter(|q| !q.is_empty())
            .collect();
        let count = statements.len();
        let mut tx = self.pool.begin().await?;
        for statement in statements {
            // The text is refinery's, from the embedded migrations and its
            // own history INSERT: no user input.
            sqlx::raw_sql(AssertSqlSafe(statement))
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(count)
    }
}

impl AsyncQuery<Vec<Migration>> for PgMigrate {
    async fn query<'a>(&'a mut self, query: &'a str) -> Result<Vec<Migration>, Self::Error> {
        let rows = sqlx::query(AssertSqlSafe(query.to_owned()))
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let applied_on: String = row.get("applied_on");
                let checksum: String = row.get("checksum");
                // refinery wrote both, as RFC 3339 and as a u64.
                Migration::applied(
                    row.get::<i32, _>("version"),
                    row.get("name"),
                    OffsetDateTime::parse(&applied_on, &Rfc3339).expect("applied_on is RFC3339"),
                    checksum.parse::<u64>().expect("checksum is a u64"),
                )
            })
            .collect())
    }
}

impl AsyncMigrate for PgMigrate {}

/// The migrations of this build, in order. refinery checksums each.
fn embedded_migrations() -> Vec<Migration> {
    vec![
        Migration::unapplied("V17__base", V17_BASE)
            .expect("V17__base migration name is well-formed"),
        Migration::unapplied(
            "V18__asset_versions",
            include_str!("migrations/V18__asset_versions.sql"),
        )
        .expect("V18__asset_versions migration name is well-formed"),
    ]
}

/// The stamps of the newest applied migration, whose floors are the current
/// contract. `None`: the database has no schema yet.
pub(super) async fn read_schema_stamps(pool: &PgPool) -> anyhow::Result<Option<SchemaStamps>> {
    let row = sqlx::query(
        "SELECT version, min_reader, min_writer FROM migration_meta \
         ORDER BY version DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await;
    let row = match row {
        Ok(row) => row,
        Err(err) if is_undefined_table_error(&err) => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    Ok(row.map(|row| SchemaStamps {
        version: row.get::<i64, _>("version") as u32,
        min_reader: row.get::<i64, _>("min_reader") as u32,
        min_writer: row.get::<i64, _>("min_writer") as u32,
    }))
}

fn is_undefined_table_error(err: &sqlx::Error) -> bool {
    matches!(err.as_database_error().and_then(|e| e.code()), Some(code) if code == UNDEFINED_TABLE)
}

/// Bring the database to the schema of this build, alone. A database that
/// is ahead of the build is refused.
pub(super) async fn migrate_to_current(pool: &PgPool) -> anyhow::Result<()> {
    if is_current(pool).await? {
        return Ok(());
    }
    let mut lock = pool
        .acquire()
        .await
        .context("acquiring a connection for the migration lock")?;
    let free: bool =
        sqlx::query_scalar("SELECT pg_try_advisory_lock($1, hashtext(current_schema()))")
            .bind(MIGRATION_LOCK_KEY)
            .fetch_one(&mut *lock)
            .await
            .context("taking the migration advisory lock")?;
    if !free {
        tracing::info!("another process is migrating storage; waiting for it");
        sqlx::query("SELECT pg_advisory_lock($1, hashtext(current_schema()))")
            .bind(MIGRATION_LOCK_KEY)
            .execute(&mut *lock)
            .await
            .context("waiting for the migration advisory lock")?;
    }

    // Another process can have migrated while this one waited for the lock.
    let result = match is_current(pool).await {
        Ok(true) => Ok(()),
        Ok(false) => apply_migrations(pool).await,
        Err(err) => Err(err),
    };

    // Closing the connection frees the lock too.
    let _ = sqlx::query("SELECT pg_advisory_unlock($1, hashtext(current_schema()))")
        .bind(MIGRATION_LOCK_KEY)
        .execute(&mut *lock)
        .await;
    result
}

/// True when the database is at the schema of this build. Fails for a
/// database that is ahead of it: a migration never goes back.
async fn is_current(pool: &PgPool) -> anyhow::Result<bool> {
    match read_schema_stamps(pool).await? {
        Some(stamps) => {
            check_compatibility(stamps, Capability::Migrate, SCHEMA_VERSION)?;
            Ok(stamps.version == SCHEMA_VERSION)
        }
        None => Ok(false),
    }
}

/// refinery skips the applied migrations, and stops on one whose text
/// changed after it was applied.
async fn apply_migrations(pool: &PgPool) -> anyhow::Result<()> {
    let migrations = embedded_migrations();
    let mut backend = PgMigrate { pool: pool.clone() };
    backend
        .migrate(
            &migrations,
            true,  // abort_divergent
            false, // abort_missing: a newer build may have applied more
            false, // grouped: one transaction per migration
            Target::Latest,
            REFINERY_HISTORY_TABLE,
        )
        .await
        .context("applying storage migrations")?;
    Ok(())
}

/// The gate of each open. The first opener of a database without the schema
/// creates it; a database with stamps goes through the shared guard.
pub(super) async fn ensure_compatible(pool: &PgPool, cap: Capability) -> anyhow::Result<()> {
    match cap {
        Capability::Migrate => migrate_to_current(pool).await,
        Capability::Inspect => Ok(()),
        Capability::Read | Capability::ReadWrite => match read_schema_stamps(pool).await? {
            Some(stamps) => check_compatibility(stamps, cap, SCHEMA_VERSION),
            None => migrate_to_current(pool).await,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_version_matches_the_highest_embedded_migration() {
        let highest = embedded_migrations()
            .iter()
            .map(|m| m.version())
            .max()
            .expect("at least one embedded migration");
        assert_eq!(highest as u32, SCHEMA_VERSION);
    }

    #[tokio::test]
    async fn a_database_ahead_of_the_build_is_not_migrated() {
        let Some(url) = super::super::PostgresStorage::test_url() else {
            return;
        };
        let storage = super::super::PostgresStorage::connect_test_schema(&url)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO migration_meta (version, class, min_reader, min_writer) \
             VALUES ($1, 'additive', 14, 17)",
        )
        .bind(i64::from(SCHEMA_VERSION) + 1)
        .execute(storage.pool())
        .await
        .unwrap();

        let error = migrate_to_current(storage.pool()).await.unwrap_err();
        assert!(error.to_string().contains("cannot migrate"), "{error:#}");
    }

    /// V18 adds the version rows of the assets and their history to a schema
    /// at V17, with the triggers that notify the UI server of changed version
    /// rows. No older build reads or writes them, so a V17 build still reads
    /// and writes.
    #[tokio::test]
    async fn test_v18_adds_the_asset_version_tables_and_keeps_the_floors() {
        let Some(url) = super::super::PostgresStorage::test_url() else {
            return;
        };
        let options: sqlx::postgres::PgConnectOptions = url.parse().unwrap();
        let schema = super::super::test_schema::TestSchema::create(options.clone())
            .await
            .unwrap();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_with(options.options([("search_path", schema.name())]))
            .await
            .unwrap();
        PgMigrate { pool: pool.clone() }
            .migrate(
                &embedded_migrations(),
                true,
                false,
                false,
                Target::Version(17),
                REFINERY_HISTORY_TABLE,
            )
            .await
            .unwrap();
        let v17 = read_schema_stamps(&pool).await.unwrap();
        assert_eq!(v17.map(|s| s.version), Some(17));

        migrate_to_current(&pool).await.unwrap();

        let tables: Vec<String> = sqlx::query_scalar(
            "SELECT table_name::text FROM information_schema.tables \
             WHERE table_schema = current_schema() AND table_name LIKE 'asset_version%' \
             ORDER BY 1",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(tables, ["asset_version_history", "asset_versions"]);
        // The UI server listens to the version rows.
        let triggers: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT trigger_name::text FROM information_schema.triggers \
             WHERE trigger_schema = current_schema() AND event_object_table = 'asset_versions' \
             ORDER BY 1",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            triggers,
            [
                "asset_versions_notify_delete",
                "asset_versions_notify_insert",
                "asset_versions_notify_update",
            ]
        );

        let stamps = read_schema_stamps(&pool).await.unwrap().unwrap();
        assert_eq!(
            (stamps.version, stamps.min_reader, stamps.min_writer),
            (18, 14, 17)
        );
        assert!(check_compatibility(stamps, Capability::Read, 17).is_ok());
        assert!(check_compatibility(stamps, Capability::ReadWrite, 17).is_ok());
        pool.close().await;
    }
}
