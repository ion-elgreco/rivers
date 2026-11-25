//! PostgreSQL backend: the version row of each asset, and the versions its
//! stored data reached. A writer that read a version row writes it back only
//! if its revision did not change in between.

use sqlx::postgres::{PgArguments, PgRow};
use sqlx::query::Query;

use super::*;

/// The version rows of the code location `$1` that pass `filter`, by key.
fn versions_where(filter: &str) -> sqlx::AssertSqlSafe<String> {
    sqlx::AssertSqlSafe(format!(
        "SELECT code_location_id, asset_key, applied_version, partitions, state, \
             target_version, reason, started_at, run_id, backfill_id, owner, \
             lease_expires_at, revision, updated_at \
         FROM asset_versions WHERE code_location_id = $1 {filter} \
         ORDER BY asset_key COLLATE \"C\""
    ))
}

/// `$1` to `$13` are bound by [`bind_version`].
const INSERT_VERSION: &str = "\
    INSERT INTO asset_versions (code_location_id, asset_key, applied_version, partitions, \
        state, target_version, reason, started_at, run_id, backfill_id, owner, \
        lease_expires_at, updated_at, revision) \
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 1) \
    ON CONFLICT (code_location_id, asset_key) DO NOTHING";

/// The same, and `$14` the revision that the row must still have.
const UPDATE_VERSION: &str = "\
    UPDATE asset_versions SET applied_version = $3, partitions = $4, state = $5, \
        target_version = $6, reason = $7, started_at = $8, run_id = $9, backfill_id = $10, \
        owner = $11, lease_expires_at = $12, updated_at = $13, revision = revision + 1 \
    WHERE code_location_id = $1 AND asset_key = $2 AND revision = $14";

const UPSERT_HISTORY: &str = "\
    INSERT INTO asset_version_history (code_location_id, asset_key, version, kind, breaking, \
        run_id, backfill_id, filled, failed, finished_at) \
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
    ON CONFLICT (code_location_id, asset_key, version) DO UPDATE SET kind = EXCLUDED.kind, \
        breaking = EXCLUDED.breaking, run_id = EXCLUDED.run_id, \
        backfill_id = EXCLUDED.backfill_id, filled = EXCLUDED.filled, \
        failed = EXCLUDED.failed, finished_at = EXCLUDED.finished_at";

const SELECT_HISTORY: &str = "\
    SELECT code_location_id, asset_key, version, kind, breaking, run_id, backfill_id, filled, \
        failed, finished_at \
    FROM asset_version_history WHERE code_location_id = $1 AND asset_key = $2 \
    ORDER BY version";

fn bind_version<'q>(
    query: Query<'q, sqlx::Postgres, PgArguments>,
    code_location_id: &str,
    record: &AssetVersionRecord,
    updated_at: i64,
) -> Query<'q, sqlx::Postgres, PgArguments> {
    query
        .bind(code_location_id)
        .bind(&record.asset_key)
        .bind(i64::from(record.applied_version))
        .bind(&record.partitions)
        .bind(record.state.as_str())
        .bind(record.target_version.map(i64::from))
        .bind(&record.reason)
        .bind(record.started_at)
        .bind(&record.run_id)
        .bind(&record.backfill_id)
        .bind(&record.owner)
        .bind(record.lease_expires_at)
        .bind(updated_at)
}

fn version_from_row(row: &PgRow) -> Result<AssetVersionRecord> {
    Ok(AssetVersionRecord {
        code_location_id: row.try_get("code_location_id")?,
        asset_key: row.try_get("asset_key")?,
        applied_version: u32::try_from(row.try_get::<i64, _>("applied_version")?)?,
        partitions: row.try_get("partitions")?,
        state: AssetVersionState::parse(row.try_get("state")?)?,
        target_version: row
            .try_get::<Option<i64>, _>("target_version")?
            .map(u32::try_from)
            .transpose()?,
        reason: row.try_get("reason")?,
        started_at: row.try_get("started_at")?,
        run_id: row.try_get("run_id")?,
        backfill_id: row.try_get("backfill_id")?,
        owner: row.try_get("owner")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        revision: u64::try_from(row.try_get::<i64, _>("revision")?)?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn history_from_row(row: &PgRow) -> Result<AssetVersionHistoryEntry> {
    let count = |column: &str| -> Result<Option<u64>> {
        Ok(row
            .try_get::<Option<i64>, _>(column)?
            .map(u64::try_from)
            .transpose()?)
    };
    Ok(AssetVersionHistoryEntry {
        code_location_id: row.try_get("code_location_id")?,
        asset_key: row.try_get("asset_key")?,
        version: u32::try_from(row.try_get::<i64, _>("version")?)?,
        kind: MigrationKind::parse(row.try_get("kind")?)?,
        breaking: row.try_get("breaking")?,
        run_id: row.try_get("run_id")?,
        backfill_id: row.try_get("backfill_id")?,
        filled: count("filled")?,
        failed: count("failed")?,
        finished_at: row.try_get("finished_at")?,
    })
}

impl PostgresStorage {
    pub(super) async fn get_asset_versions(
        &self,
        code_location_id: &str,
        asset_keys: Option<&[String]>,
    ) -> Result<Vec<AssetVersionRecord>> {
        let rows = match asset_keys {
            None => {
                sqlx::query(versions_where(""))
                    .bind(code_location_id)
                    .fetch_all(&self.pool)
                    .await?
            }
            Some([]) => return Ok(vec![]),
            Some(keys) => {
                sqlx::query(versions_where("AND asset_key = ANY($2)"))
                    .bind(code_location_id)
                    .bind(keys)
                    .fetch_all(&self.pool)
                    .await?
            }
        };
        rows.iter().map(version_from_row).collect()
    }

    pub(super) async fn put_asset_version(
        &self,
        code_location_id: &str,
        record: &AssetVersionRecord,
        expected_revision: Option<u64>,
    ) -> Result<bool> {
        let expected = expected_revision.map(i64::try_from).transpose()?;
        let now = now_nanos();

        self.retry(|| async {
            let query = match expected {
                None => bind_version(sqlx::query(INSERT_VERSION), code_location_id, record, now),
                Some(expected) => {
                    bind_version(sqlx::query(UPDATE_VERSION), code_location_id, record, now)
                        .bind(expected)
                }
            };
            let written = query.execute(&self.pool).await?;
            Ok(written.rows_affected() == 1)
        })
        .await
    }

    pub(super) async fn add_asset_version_history(
        &self,
        code_location_id: &str,
        entry: &AssetVersionHistoryEntry,
    ) -> Result<()> {
        let filled = entry.filled.map(i64::try_from).transpose()?;
        let failed = entry.failed.map(i64::try_from).transpose()?;

        self.retry(|| async {
            sqlx::query(UPSERT_HISTORY)
                .bind(code_location_id)
                .bind(&entry.asset_key)
                .bind(i64::from(entry.version))
                .bind(entry.kind.as_str())
                .bind(entry.breaking)
                .bind(&entry.run_id)
                .bind(&entry.backfill_id)
                .bind(filled)
                .bind(failed)
                .bind(entry.finished_at)
                .execute(&self.pool)
                .await?;
            Ok(())
        })
        .await
    }

    pub(super) async fn get_asset_version_history(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Vec<AssetVersionHistoryEntry>> {
        sqlx::query(SELECT_HISTORY)
            .bind(code_location_id)
            .bind(asset_key)
            .fetch_all(&self.pool)
            .await?
            .iter()
            .map(history_from_row)
            .collect()
    }
}
