//! Reads of the partitions of one asset: the rows of given keys, one page of
//! the materialized keys, and the failures.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use surrealdb::types::SurrealValue;

use crate::storage::{PartitionKey, retry};

use super::*;

/// Keys in one query. The text of the query grows with them: 500 statements
/// are about 100 KB.
const KEYS_PER_QUERY: usize = 500;

impl SurrealStorage {
    /// The rows that `select` returns for each of `partitions`. `select` is
    /// one `SELECT` that names its partition as `$partition`; `binds` are its
    /// other parameters.
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
        let mut rows: Vec<R> = Vec::new();

        for chunk in partitions.chunks(KEYS_PER_QUERY) {
            let statements: String = (0..chunk.len())
                .map(|i| select.replace("$partition", &format!("$partitions[{i}]")) + ";")
                .collect();

            let mut result = retry::with_retry(&self.retry_config, || async {
                let mut query = self
                    .db
                    .query(statements.as_str())
                    .bind(("partitions", chunk.to_vec()));
                for (name, value) in binds {
                    query = query.bind((*name, value.to_string()));
                }
                Ok(query.await?.check()?)
            })
            .await?;

            for statement in 0..chunk.len() {
                rows.extend(result.take::<Vec<R>>(statement)?);
            }
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

    /// The time of the latest failure of each partition of an asset, unless a
    /// later deletion replaced the failure. A later materialization does not
    /// remove a partition here: the caller compares the times.
    pub(super) async fn partition_failures(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<HashMap<PartitionKey, i64>> {
        // One narrow scan serves both consumers below — `$asset_key IN
        // node_names` defeats every index, so each extra runs query here is a
        // full table walk per invalidated asset per tick. (An inline subquery
        // is worse, not better: SurrealDB re-evaluates a non-planner-computable
        // WHERE per outer row.) The predicate keeps only the rows the two
        // consumers read — action runs and failures — so a hot asset's
        // thousands of clean materializes never leave the database.
        let mut result = self
            .db
            .query(
                "SELECT run_id, status, action, partition_key, start_time, end_time \
                 FROM runs \
                 WHERE code_location_id = $cl AND $asset_key IN node_names \
                 AND (action IS NOT NONE OR status = 'Failure')",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .await?;

        #[derive(Debug, SurrealValue)]
        struct RunScanRow {
            run_id: String,
            status: String,
            action: Option<String>,
            partition_key: Option<PartitionKey>,
            start_time: i64,
            end_time: Option<i64>,
        }

        let scan: Vec<RunScanRow> = result.take(0)?;

        // A failed action did not fail to *materialize* anything, so it must not
        // floor a partition here: the floor is cleared only by a materialization,
        // which the floor itself then suppresses. Keyed StepFailure events from
        // action runs stay in storage — backfill accounting reads them — so the
        // filtering belongs on this reader, not on the emitter.
        let action_runs: Vec<String> = scan
            .iter()
            .filter(|r| r.action.is_some())
            .map(|r| r.run_id.clone())
            .collect();

        let mut result = self
            .db
            .query(
                "SELECT partition_key, math::max(timestamp) AS ts FROM events \
                 WHERE code_location_id = $cl AND asset_key = $asset_key \
                 AND event_type = 'StepFailure' AND partition_key IS NOT NONE \
                 AND run_id NOT IN $action_runs \
                 GROUP BY partition_key",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .bind(("action_runs", action_runs))
            .await?;

        #[derive(Debug, SurrealValue)]
        struct FailRow {
            partition_key: PartitionKey,
            ts: i64,
        }

        let failed_rows: Vec<FailRow> = result.take(0)?;
        let mut latest_failure: HashMap<PartitionKey, i64> = HashMap::new();
        for row in failed_rows {
            for member in row.partition_key.members() {
                latest_failure
                    .entry(member)
                    .and_modify(|t| *t = (*t).max(row.ts))
                    .or_insert(row.ts);
            }
        }

        // A run that materialized one of this asset's partitions and then failed
        // on another asset did not fail that partition — the unpartitioned
        // path's `materialized_here` rule. Read off the run's own events, not
        // the partition row, which a delete or a newer run moves off the run.
        // The index hint anchors the scan on the runs, as in
        // `materialized_by_runs`.
        let failed_keyed_runs: Vec<String> = scan
            .iter()
            .filter(|r| r.status == "Failure" && r.action.is_none() && r.partition_key.is_some())
            .map(|r| r.run_id.clone())
            .collect();
        let mut materialized_by: HashMap<PartitionKey, HashSet<String>> = HashMap::new();
        if !failed_keyed_runs.is_empty() {
            let mut result = self
                .db
                .query(
                    "SELECT partition_key, run_id FROM events WITH INDEX idx_events_run_type \
                     WHERE run_id IN $runs AND event_type = 'Materialization' \
                     AND asset_key = $asset_key AND partition_key IS NOT NONE",
                )
                .bind(("asset_key", asset_key.to_string()))
                .bind(("runs", failed_keyed_runs))
                .await?;

            #[derive(Debug, SurrealValue)]
            struct BuiltRow {
                partition_key: PartitionKey,
                run_id: String,
            }

            let built: Vec<BuiltRow> = result.take(0)?;
            for row in built {
                for member in row.partition_key.members() {
                    materialized_by
                        .entry(member)
                        .or_default()
                        .insert(row.run_id.clone());
                }
            }
        }

        for row in &scan {
            if row.status != "Failure" || row.action.is_some() {
                continue;
            }
            let Some(pk) = &row.partition_key else {
                continue;
            };
            // End-time basis, like every other reader of the supersession
            // rule: the failure happens when the run ends, so a deletion
            // during the run must not outrank it.
            let ts = row.end_time.unwrap_or(row.start_time);
            for member in pk.members() {
                if materialized_by
                    .get(&member)
                    .is_some_and(|runs| runs.contains(&row.run_id))
                {
                    continue;
                }
                latest_failure
                    .entry(member)
                    .and_modify(|t| *t = (*t).max(ts))
                    .or_insert(ts);
            }
        }

        // A deletion supersedes older failures the same way a newer
        // materialization does: deleting a partition removes the timestamp row
        // that superseded them, so without this the partition resurrects as
        // failed on the next load — and `eager()`'s failure gate then keeps it
        // from ever being rebuilt. A whole-asset deletion covers every key.
        // Read off the tombstone rows, which outlive the delete run's events.
        let mut result = self
            .db
            .query(
                "SELECT partition_key, timestamp AS ts FROM asset_partition_deletions \
                 WHERE code_location_id = $cl AND asset_key = $asset_key; \
                 SELECT last_deletion_timestamp AS ts FROM assets \
                 WHERE code_location_id = $cl AND asset_key = $asset_key \
                 AND last_deletion_timestamp IS NOT NONE;",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .await?;

        #[derive(Debug, SurrealValue)]
        struct DelRow {
            partition_key: PartitionKey,
            ts: i64,
        }
        #[derive(Debug, SurrealValue)]
        struct AssetDelRow {
            ts: i64,
        }

        let del_rows: Vec<DelRow> = result.take(0)?;
        let mut latest_deletion: HashMap<PartitionKey, i64> = HashMap::new();
        for row in del_rows {
            for member in row.partition_key.members() {
                latest_deletion
                    .entry(member)
                    .and_modify(|t| *t = (*t).max(row.ts))
                    .or_insert(row.ts);
            }
        }
        let asset_rows: Vec<AssetDelRow> = result.take(1)?;
        let asset_deletion_ts: Option<i64> = asset_rows.into_iter().map(|r| r.ts).max();

        Ok(latest_failure
            .into_iter()
            .filter(|(pk, ts)| {
                let deleted_at = latest_deletion.get(pk).copied().max(asset_deletion_ts);
                deleted_at.is_none_or(|del_ts| del_ts < *ts)
            })
            .collect())
    }
}
