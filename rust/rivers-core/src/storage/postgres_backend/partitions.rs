//! PostgreSQL backend: partition reads. The rows of `asset_partitions` come
//! in key order, as SurrealDB reads them through its index of the keys.

use super::*;

#[cfg(test)]
mod tests;

/// A count of rows as a `LIMIT` or `OFFSET` bind.
fn rows_bind(count: u64) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

fn keys_from_rows(rows: Vec<serde_json::Value>) -> Result<Vec<PartitionKey>> {
    rows.into_iter().map(from_json).collect()
}

fn timestamps_from_rows(rows: Vec<(serde_json::Value, i64)>) -> Result<Vec<(PartitionKey, i64)>> {
    rows.into_iter()
        .map(|(key, at)| Ok((from_json(key)?, at)))
        .collect()
}

fn timestamps_with_runs(
    rows: Vec<(serde_json::Value, i64, Option<String>)>,
) -> Result<Vec<(PartitionKey, i64, Option<String>)>> {
    rows.into_iter()
        .map(|(key, at, run_id)| Ok((from_json(key)?, at, run_id)))
        .collect()
}

impl PostgresStorage {
    pub(super) async fn get_materialized_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Vec<PartitionKey>> {
        let rows = sqlx::query_scalar(
            "SELECT partition_key FROM asset_partitions \
             WHERE code_location_id = $1 AND asset_key = $2 ORDER BY partition_ord",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .fetch_all(&self.pool)
        .await?;
        keys_from_rows(rows)
    }

    pub(super) async fn get_materialized_partitions_page(
        &self,
        code_location_id: &str,
        asset_key: &str,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<PartitionKey>> {
        let rows = sqlx::query_scalar(
            "SELECT partition_key FROM asset_partitions \
             WHERE code_location_id = $1 AND asset_key = $2 \
             ORDER BY partition_ord LIMIT $3 OFFSET $4",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .bind(rows_bind(limit))
        .bind(rows_bind(offset))
        .fetch_all(&self.pool)
        .await?;
        keys_from_rows(rows)
    }

    pub(super) async fn count_materialized_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<u64> {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM asset_partitions \
             WHERE code_location_id = $1 AND asset_key = $2",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .fetch_one(&self.pool)
        .await?;
        Ok(count as u64)
    }

    /// The counts, in the order of `bounds`.
    pub(super) async fn count_materialized_partitions_in(
        &self,
        code_location_id: &str,
        asset_key: &str,
        bounds: &[PartitionBounds],
    ) -> Result<Vec<u64>> {
        if bounds.is_empty() {
            return Ok(Vec::new());
        }
        let (lower, upper): (Vec<Vec<u8>>, Vec<Vec<u8>>) = bounds.iter().map(bounds_ord).unzip();
        let counts: Vec<i64> = sqlx::query_scalar(
            "SELECT (SELECT count(*) FROM asset_partitions p \
                     WHERE p.code_location_id = $1 AND p.asset_key = $2 \
                     AND p.partition_ord BETWEEN b.lower AND b.upper) \
             FROM UNNEST($3::bytea[], $4::bytea[]) WITH ORDINALITY AS b(lower, upper, n) \
             ORDER BY b.n",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .bind(lower)
        .bind(upper)
        .fetch_all(&self.pool)
        .await?;
        Ok(counts.into_iter().map(|count| count as u64).collect())
    }

    pub(super) async fn get_partition_timestamps(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Vec<(PartitionKey, i64)>> {
        let rows = sqlx::query_as(
            "SELECT partition_key, last_timestamp FROM asset_partitions \
             WHERE code_location_id = $1 AND asset_key = $2 AND last_timestamp IS NOT NULL \
             ORDER BY partition_ord",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .fetch_all(&self.pool)
        .await?;
        timestamps_from_rows(rows)
    }

    pub(super) async fn get_partition_timestamps_since(
        &self,
        code_location_id: &str,
        asset_key: &str,
        since_timestamp: i64,
    ) -> Result<Vec<(PartitionKey, i64)>> {
        let rows = sqlx::query_as(
            "SELECT partition_key, last_timestamp FROM asset_partitions \
             WHERE code_location_id = $1 AND asset_key = $2 AND last_timestamp > $3 \
             ORDER BY partition_ord",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .bind(since_timestamp)
        .fetch_all(&self.pool)
        .await?;
        timestamps_from_rows(rows)
    }

    /// The rows of the keys, in the order of `keys`.
    pub(super) async fn get_partition_timestamps_for_keys(
        &self,
        code_location_id: &str,
        asset_key: &str,
        keys: &[PartitionKey],
    ) -> Result<Vec<(PartitionKey, i64, Option<String>)>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let ords: Vec<Vec<u8>> = keys.iter().map(partition_ord).collect();
        let rows = sqlx::query_as(
            "SELECT p.partition_key, p.last_timestamp, p.last_run_id \
             FROM UNNEST($3::bytea[]) WITH ORDINALITY AS k(ord, n) \
             JOIN asset_partitions p ON p.code_location_id = $1 AND p.asset_key = $2 \
             AND p.partition_ord = k.ord \
             WHERE p.last_timestamp IS NOT NULL ORDER BY k.n",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .bind(ords)
        .fetch_all(&self.pool)
        .await?;
        timestamps_with_runs(rows)
    }

    pub(super) async fn get_partition_timestamps_in(
        &self,
        code_location_id: &str,
        asset_key: &str,
        bounds: &PartitionBounds,
        limit: u64,
    ) -> Result<Vec<(PartitionKey, i64, Option<String>)>> {
        let (lower, upper) = bounds_ord(bounds);
        let rows = sqlx::query_as(
            "SELECT partition_key, last_timestamp, last_run_id FROM asset_partitions \
             WHERE code_location_id = $1 AND asset_key = $2 \
             AND partition_ord BETWEEN $3 AND $4 AND last_timestamp IS NOT NULL \
             ORDER BY partition_ord LIMIT $5",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .bind(lower)
        .bind(upper)
        .bind(rows_bind(limit))
        .fetch_all(&self.pool)
        .await?;
        timestamps_with_runs(rows)
    }

    /// The keys of the `StepStart` events of the started runs of the asset.
    /// A run of an action does not count: an in-flight `optimize` is not an
    /// in-flight materialization.
    pub(super) async fn get_in_progress_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Vec<PartitionKey>> {
        let rows = sqlx::query_scalar(
            "SELECT DISTINCT ON (partition_ord) partition_key FROM events \
             WHERE code_location_id = $1 AND asset_key = $2 AND event_type = 'StepStart' \
             AND partition_ord IS NOT NULL \
             AND run_id IN (SELECT run_id FROM runs WHERE code_location_id = $1 \
                 AND status = 'Started' AND action IS NULL AND $2 = ANY(node_names)) \
             ORDER BY partition_ord",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .fetch_all(&self.pool)
        .await?;
        keys_from_rows(rows)
    }

    /// The failure rows of the asset, less those that a materialization in
    /// `materialized` at or after the failure replaces.
    pub(super) async fn get_failed_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
        materialized: &HashMap<PartitionKey, i64>,
    ) -> Result<HashMap<PartitionKey, i64>> {
        let rows = sqlx::query_as(
            "SELECT partition_key, failed_at FROM asset_partition_failures \
             WHERE code_location_id = $1 AND asset_key = $2",
        )
        .bind(code_location_id)
        .bind(asset_key)
        .fetch_all(&self.pool)
        .await?;
        Ok(timestamps_from_rows(rows)?
            .into_iter()
            .filter(|(key, failed_at)| materialized.get(key).is_none_or(|built| built < failed_at))
            .collect())
    }
}
