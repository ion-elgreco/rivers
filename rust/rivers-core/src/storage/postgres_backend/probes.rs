//! Reads and writes of stored rows for the storage tests: the PostgreSQL
//! form of the probes in `surrealdb_backend/tests`, so that each test runs
//! on both backends and the SQL stays in this backend.

use std::collections::BTreeMap;

use serde_json::Value;
use surrealdb::types::RecordIdKey;

use super::runs::status_text;
use super::*;

mod failures;

/// A slot row as a claim wrote it: pool, slots, claim time, lease expiry,
/// partitions (sorted, each once) and whether it is exclusive.
pub(crate) type SlotRow = (String, i64, i64, i64, Option<Vec<String>>, bool);

impl PostgresStorage {
    /// The row of each partition of `asset`: its key, run and time, in the
    /// order of the store.
    pub(crate) async fn partition_rows(
        &self,
        cl: &str,
        asset: &str,
    ) -> Result<Vec<(PartitionKey, String, i64)>> {
        let rows: Vec<(Value, String, i64)> = sqlx::query_as(
            "SELECT partition_key, last_run_id, last_timestamp FROM asset_partitions \
             WHERE code_location_id = $1 AND asset_key = $2 ORDER BY partition_ord",
        )
        .bind(cl)
        .bind(asset)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(key, run_id, at)| Ok((from_json(key)?, run_id, at)))
            .collect()
    }

    /// Add the row of a partition without its event.
    pub(crate) async fn insert_partition_row(
        &self,
        cl: &str,
        asset: &str,
        key: &PartitionKey,
        at: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO asset_partitions \
             (code_location_id, asset_key, partition_ord, partition_key, last_timestamp) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(cl)
        .bind(asset)
        .bind(partition_ord(key))
        .bind(key_json(key)?)
        .bind(at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Clear the time of the row of a partition, as the row of an older
    /// writer can be.
    pub(crate) async fn clear_partition_time(
        &self,
        cl: &str,
        asset: &str,
        key: &PartitionKey,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE asset_partitions SET last_timestamp = NULL \
             WHERE code_location_id = $1 AND asset_key = $2 AND partition_ord = $3",
        )
        .bind(cl)
        .bind(asset)
        .bind(partition_ord(key))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The time of the newest deletion of one partition of `asset`.
    pub(crate) async fn partition_deletion_at(
        &self,
        cl: &str,
        asset: &str,
        key: &PartitionKey,
    ) -> Result<Option<i64>> {
        Ok(sqlx::query_scalar(
            "SELECT timestamp FROM asset_partition_deletions \
             WHERE code_location_id = $1 AND asset_key = $2 AND partition_ord = $3",
        )
        .bind(cl)
        .bind(asset)
        .bind(partition_ord(key))
        .fetch_optional(&self.pool)
        .await?)
    }

    /// The time of the materialization that the provenance of `asset` comes
    /// from.
    pub(crate) async fn provenance_at(&self, cl: &str, asset: &str) -> Result<Option<i64>> {
        Ok(sqlx::query_scalar(
            "SELECT last_provenance_timestamp FROM assets \
             WHERE code_location_id = $1 AND asset_key = $2",
        )
        .bind(cl)
        .bind(asset)
        .fetch_optional(&self.pool)
        .await?
        .flatten())
    }

    /// Store the rows of `events` and nothing else, as an attempt that
    /// stopped before it applied them.
    pub(crate) async fn insert_event_rows(&self, events: &[EventWithId]) -> Result<()> {
        for EventWithId { event, id } in events {
            let RecordIdKey::String(id) = &id.key else {
                anyhow::bail!("an event id has a text key, not {:?}", id.key);
            };
            let key = event.partition_key.as_ref();
            sqlx::query(
                "INSERT INTO events (id, code_location_id, event_type, asset_key, run_id, \
                 partition_key, partition_ord, timestamp, sort_order, metadata, data_version, \
                 input_data_versions) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
            )
            .bind(id)
            .bind(&event.code_location_id)
            .bind(event.event_type.type_name())
            .bind(&event.asset_key)
            .bind(&event.run_id)
            .bind(key.map(key_json).transpose()?)
            .bind(key.map(partition_ord))
            .bind(event.timestamp)
            .bind(event.event_type.sort_order())
            .bind(to_json(&event.metadata)?)
            .bind(event.event_type.data_version())
            .bind(to_json(&event.input_data_versions)?)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    /// Write the status or the end of a run as a write of its status does,
    /// and stop there: as if the process stopped before the repair after it.
    pub(crate) async fn write_run_status_only(
        &self,
        run_id: &str,
        status: Option<&RunStatus>,
        end_time: Option<i64>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE runs SET status = COALESCE($2, status), end_time = COALESCE($3, end_time) \
             WHERE run_id = $1",
        )
        .bind(run_id)
        .bind(status.map(status_text))
        .bind(end_time)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The slot rows of one step, by pool.
    pub(crate) async fn held_slots(&self, run_id: &str, step_key: &str) -> Result<Vec<SlotRow>> {
        let mut rows: Vec<SlotRow> = sqlx::query_as(
            "SELECT pool_key, slots_consumed, claimed_at, lease_expires_at, partitions, exclusive \
             FROM concurrency_slots WHERE run_id = $1 AND step_key = $2 \
             ORDER BY pool_key COLLATE \"C\"",
        )
        .bind(run_id)
        .bind(step_key)
        .fetch_all(&self.pool)
        .await?;
        // SurrealDB keeps the partitions as a set, this backend as given.
        for partitions in rows.iter_mut().filter_map(|row| row.4.as_mut()) {
            partitions.sort();
            partitions.dedup();
        }
        Ok(rows)
    }

    /// Add a slot row of the default code location, as a claim writes it.
    pub(crate) async fn insert_slot(
        &self,
        pool_key: &str,
        run_id: &str,
        step_key: &str,
        slots: i64,
        claimed_at: i64,
        lease_expires_at: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO concurrency_slots (pool_key, run_id, step_key, slots_consumed, \
             claimed_at, lease_expires_at, last_heartbeat) VALUES ($1, $2, $3, $4, $5, $6, $5)",
        )
        .bind(pool_key)
        .bind(run_id)
        .bind(step_key)
        .bind(slots)
        .bind(claimed_at)
        .bind(lease_expires_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Add a step of the default code location that waits for `pool_key`.
    pub(crate) async fn insert_pending_step(
        &self,
        pool_key: &str,
        run_id: &str,
        step_key: &str,
        enqueued_at: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO pending_steps (pool_key, run_id, step_key, priority, enqueued_at, \
             block_reason) VALUES ($1, $2, $3, 0, $4, 'PoolFull')",
        )
        .bind(pool_key)
        .bind(run_id)
        .bind(step_key)
        .bind(enqueued_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Remove every run, slot row and waiting step.
    pub(crate) async fn clear_runs_and_slots(&self) -> Result<()> {
        sqlx::query("TRUNCATE runs, concurrency_slots, pending_steps")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// `(run_id, run_number, member_count)` of the links of a backfill, in
    /// link order.
    pub(crate) async fn link_rows(&self, backfill_id: &str) -> Result<Vec<(String, i64, i64)>> {
        Ok(sqlx::query_as(
            "SELECT run_id, run_number, member_count FROM backfill_runs \
             WHERE backfill_id = $1 ORDER BY linked_at",
        )
        .bind(backfill_id)
        .fetch_all(&self.pool)
        .await?)
    }

    /// The numbers of the pages of the requested keys of a backfill.
    pub(crate) async fn key_pages(&self, backfill_id: &str) -> Result<Vec<i64>> {
        Ok(sqlx::query_scalar(
            "SELECT page::bigint FROM backfill_key_pages WHERE backfill_id = $1 ORDER BY page",
        )
        .bind(backfill_id)
        .fetch_all(&self.pool)
        .await?)
    }

    /// The outcome of each run of every backfill.
    pub(crate) async fn run_outcomes(&self) -> Result<Vec<String>> {
        Ok(
            sqlx::query_scalar("SELECT outcome FROM backfill_run_outcomes")
                .fetch_all(&self.pool)
                .await?,
        )
    }

    /// The fields `names` of each backfill row. A field without a column
    /// reads as null.
    pub(crate) async fn backfill_row_fields(&self, names: &[&str]) -> Result<Vec<Value>> {
        let rows: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(b) FROM backfills b")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .iter()
            .map(|row| names.iter().map(|name| row[*name].clone()).collect())
            .collect())
    }

    /// How many `kv` rows hold `key`.
    pub(crate) async fn kv_rows(&self, key: &str) -> Result<i64> {
        Ok(sqlx::query_scalar("SELECT count(*) FROM kv WHERE key = $1")
            .bind(key)
            .fetch_one(&self.pool)
            .await?)
    }

    /// The key lists of a code location, by digest.
    pub(crate) async fn key_lists(&self, cl: &str) -> Result<BTreeMap<String, Vec<String>>> {
        let rows: Vec<(String, Vec<String>)> = sqlx::query_as(
            "SELECT digest, keys FROM partition_key_lists WHERE code_location_id = $1",
        )
        .bind(cl)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().collect())
    }

    /// Write `keys` over the stored key list `digest` of a code location.
    pub(crate) async fn overwrite_key_list(
        &self,
        cl: &str,
        digest: &str,
        keys: &[String],
    ) -> Result<()> {
        sqlx::query(
            "UPDATE partition_key_lists SET keys = $3 WHERE code_location_id = $1 AND digest = $2",
        )
        .bind(cl)
        .bind(digest)
        .bind(keys)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
