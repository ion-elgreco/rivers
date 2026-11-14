//! Reads of the partitions of one asset: the rows of given keys and one page
//! of the materialized keys.

use anyhow::Result;
use futures_util::stream::{self, StreamExt, TryStreamExt};
use surrealdb::types::SurrealValue;

use crate::storage::{PartitionBounds, PartitionKey, retry};

use super::*;

/// Keys in one query.
pub(super) const KEYS_PER_QUERY: usize = 100;

/// Queries of one read that run at the same time.
pub(super) const QUERIES_AT_ONCE: usize = 8;

/// The statements that read `count` partitions with `select`: statement `i`
/// names its partition as `$partition_i`.
///
/// Each statement has a parameter of its own. The engine handles one list
/// of all keys again for every statement that reads a key from it.
pub(super) fn keyed_statements(select: &str, count: usize) -> String {
    (0..count)
        .map(|i| select.replace("$partition", &format!("$partition_{i}")) + ";")
        .collect()
}

/// The statements that run `select` for `count` bounds: statement `i` names
/// its bounds as `$lower_i` and `$upper_i`.
pub(super) fn bounded_statements(select: &str, count: usize) -> String {
    (0..count)
        .map(|i| {
            select
                .replace("$lower", &format!("$lower_{i}"))
                .replace("$upper", &format!("$upper_{i}"))
                + ";"
        })
        .collect()
}

impl SurrealStorage {
    /// The rows that `select` returns for each of `partitions`, in the order
    /// of the partitions. `select` is one `SELECT` that names its partition
    /// as `$partition`; `binds` are its other parameters.
    ///
    /// One statement per partition: the engine reaches a row by index only
    /// when it knows the value before the statement runs. A `partition_key IN
    /// $keys` filter, a closure over the keys and a subquery per key all read
    /// every row under the leading index columns.
    pub(super) async fn select_by_partitions<R: SurrealValue>(
        &self,
        select: &str,
        binds: &[(&'static str, &str)],
        partitions: &[PartitionKey],
    ) -> Result<Vec<R>> {
        let queries: Vec<_> = partitions
            .chunks(KEYS_PER_QUERY)
            .map(|chunk| self.select_by_partitions_query(select, binds, chunk))
            .collect();

        let rows: Vec<Vec<R>> = stream::iter(queries)
            .buffered(QUERIES_AT_ONCE)
            .try_collect()
            .await?;

        Ok(rows.into_iter().flatten().collect())
    }

    /// One query of [`Self::select_by_partitions`].
    async fn select_by_partitions_query<R: SurrealValue>(
        &self,
        select: &str,
        binds: &[(&'static str, &str)],
        partitions: &[PartitionKey],
    ) -> Result<Vec<R>> {
        let statements = keyed_statements(select, partitions.len());

        let mut result = retry::with_retry(&self.retry_config, || async {
            let mut query = self.db.query(statements.as_str());
            for (name, value) in binds {
                query = query.bind((*name, value.to_string()));
            }
            for (i, partition) in partitions.iter().enumerate() {
                query = query.bind((format!("partition_{i}"), partition.clone()));
            }
            Ok(query.await?.check()?)
        })
        .await?;

        let mut rows: Vec<R> = Vec::new();
        for statement in 0..partitions.len() {
            rows.extend(result.take::<Vec<R>>(statement)?);
        }

        Ok(rows)
    }

    pub(super) async fn partition_timestamps_for_keys(
        &self,
        code_location_id: &str,
        asset_key: &str,
        keys: &[PartitionKey],
    ) -> Result<Vec<(PartitionKey, i64, Option<String>)>> {
        #[derive(Debug, SurrealValue)]
        struct PartTsRow {
            partition_key: PartitionKey,
            last_timestamp: i64,
            last_run_id: Option<String>,
        }

        let rows: Vec<PartTsRow> = self
            .select_by_partitions(
                PARTITION_TIMESTAMPS_OF_KEY,
                &[("cl", code_location_id), ("asset_key", asset_key)],
                keys,
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.partition_key, r.last_timestamp, r.last_run_id))
            .collect())
    }

    pub(super) async fn count_partitions_in_bounds(
        &self,
        code_location_id: &str,
        asset_key: &str,
        bounds: &[PartitionBounds],
    ) -> Result<Vec<u64>> {
        let queries: Vec<_> = bounds
            .chunks(KEYS_PER_QUERY)
            .map(|chunk| self.count_partitions_in_bounds_query(code_location_id, asset_key, chunk))
            .collect();

        let counts: Vec<Vec<u64>> = stream::iter(queries)
            .buffered(QUERIES_AT_ONCE)
            .try_collect()
            .await?;

        Ok(counts.into_iter().flatten().collect())
    }

    /// One query of [`Self::count_partitions_in_bounds`].
    async fn count_partitions_in_bounds_query(
        &self,
        code_location_id: &str,
        asset_key: &str,
        bounds: &[PartitionBounds],
    ) -> Result<Vec<u64>> {
        let statements = bounded_statements(COUNT_PARTITIONS_IN_BOUNDS, bounds.len());

        let mut result = retry::with_retry(&self.retry_config, || async {
            let mut query = self
                .db
                .query(statements.as_str())
                .bind(("cl", code_location_id.to_string()))
                .bind(("asset_key", asset_key.to_string()));
            for (i, bounds) in bounds.iter().enumerate() {
                query = query
                    .bind((format!("lower_{i}"), bounds.lower.clone()))
                    .bind((format!("upper_{i}"), bounds.upper.clone()));
            }
            Ok(query.await?.check()?)
        })
        .await?;

        (0..bounds.len())
            .map(|statement| {
                let total: Option<u64> = result.take((statement, "total"))?;
                Ok(total.unwrap_or(0))
            })
            .collect()
    }

    pub(super) async fn partition_timestamps_in_bounds(
        &self,
        code_location_id: &str,
        asset_key: &str,
        bounds: &PartitionBounds,
        limit: u64,
    ) -> Result<Vec<(PartitionKey, i64, Option<String>)>> {
        #[derive(Debug, SurrealValue)]
        struct PartTsRow {
            partition_key: PartitionKey,
            last_timestamp: i64,
            last_run_id: Option<String>,
        }

        let rows: Vec<PartTsRow> = retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(PARTITION_TIMESTAMPS_IN_BOUNDS)
                .bind(("cl", code_location_id.to_string()))
                .bind(("asset_key", asset_key.to_string()))
                .bind(("lower", bounds.lower.clone()))
                .bind(("upper", bounds.upper.clone()))
                .bind(("limit", limit))
                .await?
                .check()?;
            Ok(result.take(0)?)
        })
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| (r.partition_key, r.last_timestamp, r.last_run_id))
            .collect())
    }

    pub(super) async fn materialized_partitions_page(
        &self,
        code_location_id: &str,
        asset_key: &str,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<PartitionKey>> {
        let mut result = self
            .db
            .query(
                "SELECT VALUE partition_key FROM asset_partitions \
                 WHERE code_location_id = $cl AND asset_key = $asset_key \
                 LIMIT $limit START $offset",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .bind(("limit", limit))
            .bind(("offset", offset))
            .await?;
        Ok(result.take(0)?)
    }
}
