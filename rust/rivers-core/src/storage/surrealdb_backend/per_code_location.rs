use std::collections::HashSet;

use anyhow::Result;
use surrealdb::types::{RecordId, SurrealValue};

use crate::storage::retry;
use crate::storage::{
    AssetRecord, AssetScope, BackfillRecord, BackfillStatus, ConcurrencyClaimStatus,
    CoordinatorRunInfo, PartitionKey, PerCodeLocationStorage, PoolInfo, PoolLimit, RunRecord,
    RunStatus, SlotHolder, StoredConditionEval, StoredConditionTick, StoredEvent, StoredTick,
};

use super::partition_keys::KEY_ROWS_CHANGED;
use super::pools::PoolContended;
use super::*;

impl PerCodeLocationStorage for SurrealStorage {
    async fn get_events_for_asset(
        &self,
        code_location_id: &str,
        asset_key: &str,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        let mut result = self
            .db
            .query("SELECT * FROM events WHERE code_location_id = $cl AND asset_key = $asset_key ORDER BY timestamp DESC, sort_order DESC, id DESC LIMIT $limit")
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .bind(("limit", limit))
            .await?;
        let events: Vec<DbStoredEvent> = result.take(0)?;
        Ok(events.into_iter().map(|e| e.into_stored_event()).collect())
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(%code_location_id, %asset_key))]
    async fn get_latest_materialization(
        &self,
        code_location_id: &str,
        asset_key: &str,
        partition: Option<&str>,
    ) -> Result<Option<StoredEvent>> {
        let event_type_str = "Materialization".to_string();
        let mut result = if let Some(partition_key) = partition {
            for cand in PartitionKey::display_candidates(partition_key)
                .into_iter()
                .rev()
            {
                let mut result = self.db
                    .query("SELECT * FROM events WHERE code_location_id = $cl AND asset_key = $asset_key AND partition_key = $pk AND event_type = $event_type ORDER BY timestamp DESC LIMIT 1")
                    .bind(("cl", code_location_id.to_string()))
                    .bind(("asset_key", asset_key.to_string()))
                    .bind(("pk", cand))
                    .bind(("event_type", event_type_str.clone()))
                    .await?;
                let events: Vec<DbStoredEvent> = result.take(0)?;
                if let Some(e) = events.into_iter().next() {
                    return Ok(Some(e.into_stored_event()));
                }
            }
            return Ok(None);
        } else {
            self.db
                .query("SELECT * FROM events WHERE code_location_id = $cl AND asset_key = $asset_key AND event_type = $event_type ORDER BY timestamp DESC LIMIT 1")
                .bind(("cl", code_location_id.to_string()))
                .bind(("asset_key", asset_key.to_string()))
                .bind(("event_type", event_type_str))
                .await?
        };
        let events: Vec<DbStoredEvent> = result.take(0)?;
        Ok(events.into_iter().next().map(|e| e.into_stored_event()))
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(%code_location_id, %asset_key))]
    async fn get_asset_record(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Option<AssetRecord>> {
        let mut result = self
            .db
            .query("SELECT * FROM assets WHERE code_location_id = $cl AND asset_key = $asset_key LIMIT 1")
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .await?;
        let assets: Vec<AssetRecord> = result.take(0)?;
        Ok(assets.into_iter().next())
    }

    async fn get_asset_records(&self, code_location_id: &str) -> Result<Vec<AssetRecord>> {
        let mut result = self
            .db
            .query("SELECT * FROM assets WHERE code_location_id = $cl")
            .bind(("cl", code_location_id.to_string()))
            .await?;
        let assets: Vec<AssetRecord> = result.take(0)?;
        Ok(assets)
    }

    /// The plan for this is `IndexScan idx_assets_loc` either way — the code
    /// location's whole asset set — so an `asset_key IN $keys` clause saves no
    /// reads and costs one list comparison per scanned row. At 3,200 keys that
    /// clause was 473 ms against 9 ms for the same scan filtered here.
    ///
    /// One key goes through `get_asset_record`: at 3,000 assets the scan took
    /// 13 ms against 4 ms for that query.
    async fn get_asset_records_by_keys(
        &self,
        code_location_id: &str,
        keys: &[String],
    ) -> Result<Vec<AssetRecord>> {
        match keys {
            [] => return Ok(vec![]),
            [key] => {
                let record = self.get_asset_record(code_location_id, key).await?;
                return Ok(record.into_iter().collect());
            }
            _ => {}
        }
        let wanted: HashSet<&str> = keys.iter().map(String::as_str).collect();
        let mut result = self
            .db
            .query("SELECT * FROM assets WHERE code_location_id = $cl")
            .bind(("cl", code_location_id.to_string()))
            .await?;
        let assets: Vec<AssetRecord> = result.take(0)?;
        Ok(assets
            .into_iter()
            .filter(|record| wanted.contains(record.asset_key.as_str()))
            .collect())
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(%code_location_id, count = records.len()))]
    /// Bulk upsert `assets` rows, matched on the table's UNIQUE
    /// (code_location_id, asset_key) index.
    ///
    /// The update clause lists only the fields that describe the asset's
    /// *definition*. Everything else on an existing row — last event, last run,
    /// data versions — is materialization history, and re-resolving a code
    /// location must not wipe it.
    async fn register_assets(&self, code_location_id: &str, records: &[AssetRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        for chunk in records.chunks(REGISTER_ASSETS_CHUNK) {
            let rows: Vec<AssetRecord> = chunk
                .iter()
                .map(|record| AssetRecord {
                    code_location_id: code_location_id.to_string(),
                    ..record.clone()
                })
                .collect();
            self.db
                .query(
                    "INSERT INTO assets $rows ON DUPLICATE KEY UPDATE \
                     tags = $input.tags, \
                     kinds = $input.kinds, \
                     asset_group = $input.asset_group, \
                     code_version = $input.code_version, \
                     pool = $input.pool",
                )
                .bind(("rows", rows))
                .await?
                .check()?;
        }

        Ok(())
    }

    async fn get_assets_by_tag(
        &self,
        code_location_id: &str,
        tag: &str,
    ) -> Result<Vec<AssetRecord>> {
        let mut result = self
            .db
            .query("SELECT * FROM assets WHERE code_location_id = $cl AND tags CONTAINS $tag")
            .bind(("cl", code_location_id.to_string()))
            .bind(("tag", tag.to_string()))
            .await?;
        let assets: Vec<AssetRecord> = result.take(0)?;
        Ok(assets)
    }

    async fn get_assets_by_kind(
        &self,
        code_location_id: &str,
        kind: &str,
    ) -> Result<Vec<AssetRecord>> {
        let mut result = self
            .db
            .query("SELECT * FROM assets WHERE code_location_id = $cl AND kinds CONTAINS $kind")
            .bind(("cl", code_location_id.to_string()))
            .bind(("kind", kind.to_string()))
            .await?;
        let assets: Vec<AssetRecord> = result.take(0)?;
        Ok(assets)
    }

    async fn get_assets_by_group(
        &self,
        code_location_id: &str,
        group: &str,
    ) -> Result<Vec<AssetRecord>> {
        let mut result = self
            .db
            .query("SELECT * FROM assets WHERE code_location_id = $cl AND asset_group = $group")
            .bind(("cl", code_location_id.to_string()))
            .bind(("group", group.to_string()))
            .await?;
        let assets: Vec<AssetRecord> = result.take(0)?;
        Ok(assets)
    }

    async fn set_block_reason_by_status(
        &self,
        code_location_id: &str,
        status: RunStatus,
        reason: Option<&str>,
    ) -> Result<()> {
        self.db
            .query(
                "UPDATE runs SET block_reason = $reason \
                 WHERE status = $status AND code_location_id = $cl",
            )
            .bind(("status", format!("{:?}", status)))
            .bind(("reason", reason.map(|s| s.to_string())))
            .bind(("cl", code_location_id.to_string()))
            .await?;
        Ok(())
    }

    async fn coordinator_tick_query(
        &self,
        code_location_id: &str,
    ) -> Result<(u32, Vec<CoordinatorRunInfo>, Vec<CoordinatorRunInfo>)> {
        let now_ns = now_nanos();
        let mut result = self
            .db
            .query(
                "SELECT count() AS total FROM concurrency_slots \
                     WHERE lease_expires_at <= $now GROUP ALL; \
                 DELETE FROM concurrency_slots WHERE lease_expires_at <= $now; \
                 SELECT run_id, code_location_id, tags, node_names, job_name, priority, start_time, action, config \
                     FROM runs WHERE status IN ['NotStarted', 'Started'] AND code_location_id = $cl; \
                 SELECT run_id, code_location_id, tags, node_names, job_name, priority, start_time, action, config \
                     FROM runs WHERE status = 'Queued' AND code_location_id = $cl",
            )
            .bind(("now", now_ns))
            .bind(("cl", code_location_id.to_string()))
            .await?;
        let expired: Option<u32> = result.take((0, "total"))?;
        // Statement 1 is the DELETE (no result needed)
        let in_progress: Vec<CoordinatorRunInfo> = result.take(2)?;
        let queued: Vec<CoordinatorRunInfo> = result.take(3)?;
        Ok((expired.unwrap_or(0), in_progress, queued))
    }

    async fn get_stalled_not_started_runs(
        &self,
        code_location_id: &str,
        cutoff_ns: i64,
    ) -> Result<Vec<String>> {
        retry::with_retry(&self.retry_config, || async {
            #[derive(Debug, SurrealValue, serde::Deserialize)]
            struct Candidate {
                run_id: String,
                start_time: i64,
            }
            let mut result = self
                .db
                .query(
                    "SELECT run_id, start_time FROM runs \
                         WHERE code_location_id = $cl AND status = 'NotStarted'",
                )
                .bind(("cl", code_location_id.to_string()))
                .await?;
            let candidates: Vec<Candidate> = result.take(0)?;
            if candidates.is_empty() {
                return Ok(vec![]);
            }

            let ids: Vec<String> = candidates.iter().map(|c| c.run_id.clone()).collect();
            #[derive(Debug, SurrealValue, serde::Deserialize)]
            struct Dequeue {
                run_id: String,
                ts: i64,
            }
            let mut result = self
                .db
                .query(
                    "SELECT run_id, math::max(timestamp) AS ts FROM events \
                         WHERE event_type = 'RunDequeued' AND run_id IN $ids \
                         GROUP BY run_id",
                )
                .bind(("ids", ids))
                .await?;
            let dequeues: Vec<Dequeue> = result.take(0)?;
            let dequeue_ts: std::collections::HashMap<String, i64> =
                dequeues.into_iter().map(|d| (d.run_id, d.ts)).collect();

            Ok(candidates
                .into_iter()
                .filter(|c| *dequeue_ts.get(&c.run_id).unwrap_or(&c.start_time) < cutoff_ns)
                .map(|c| c.run_id)
                .collect())
        })
        .await
    }

    async fn add_dynamic_partitions(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
        partition_keys: &[String],
    ) -> Result<()> {
        for key in partition_keys {
            if key.is_empty() {
                anyhow::bail!("dynamic partition keys must not be empty");
            }
            if let Some(ch) = PartitionKey::reserved_display_char(key) {
                anyhow::bail!(
                    "partition key '{key}' contains reserved character '{ch}' \
                     (used by the canonical display form)"
                );
            }
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        for key in partition_keys {
            let mut result = self
                .db
                .query("SELECT * FROM dynamic_partitions WHERE code_location_id = $cl AND partitions_def_name = $name AND partition_key = $key LIMIT 1")
                .bind(("cl", code_location_id.to_string()))
                .bind(("name", partitions_def_name.to_string()))
                .bind(("key", key.clone()))
                .await?;
            let existing: Vec<DbDynamicPartition> = result.take(0)?;
            if existing.is_empty() {
                let _: Option<DbDynamicPartition> = self
                    .db
                    .create("dynamic_partitions")
                    .content(DbDynamicPartition {
                        code_location_id: code_location_id.to_string(),
                        partitions_def_name: partitions_def_name.to_string(),
                        partition_key: key.clone(),
                        create_timestamp: now,
                    })
                    .await?;
            }
        }
        Ok(())
    }

    async fn delete_dynamic_partition(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
        partition_key: &str,
    ) -> Result<()> {
        self.db
            .query("DELETE FROM dynamic_partitions WHERE code_location_id = $cl AND partitions_def_name = $name AND partition_key = $key")
            .bind(("cl", code_location_id.to_string()))
            .bind(("name", partitions_def_name.to_string()))
            .bind(("key", partition_key.to_string()))
            .await?;
        Ok(())
    }

    async fn get_dynamic_partitions(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
    ) -> Result<Vec<String>> {
        let mut result = self
            .db
            .query("SELECT * FROM dynamic_partitions WHERE code_location_id = $cl AND partitions_def_name = $name ORDER BY partition_key ASC")
            .bind(("cl", code_location_id.to_string()))
            .bind(("name", partitions_def_name.to_string()))
            .await?;
        let rows: Vec<DbDynamicPartition> = result.take(0)?;
        Ok(rows.into_iter().map(|r| r.partition_key).collect())
    }

    async fn has_dynamic_partition(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
        partition_key: &str,
    ) -> Result<bool> {
        let mut result = self
            .db
            .query("SELECT * FROM dynamic_partitions WHERE code_location_id = $cl AND partitions_def_name = $name AND partition_key = $key LIMIT 1")
            .bind(("cl", code_location_id.to_string()))
            .bind(("name", partitions_def_name.to_string()))
            .bind(("key", partition_key.to_string()))
            .await?;
        let rows: Vec<DbDynamicPartition> = result.take(0)?;
        Ok(!rows.is_empty())
    }

    async fn get_ticks(
        &self,
        code_location_id: &str,
        automation_name: &str,
        limit: usize,
    ) -> Result<Vec<StoredTick>> {
        let mut result = self
            .db
            .query("SELECT * FROM ticks WHERE code_location_id = $cl AND automation_name = $name ORDER BY timestamp DESC LIMIT $limit")
            .bind(("cl", code_location_id.to_string()))
            .bind(("name", automation_name.to_string()))
            .bind(("limit", limit))
            .await?;
        let ticks: Vec<DbStoredTick> = result.take(0)?;
        Ok(ticks.into_iter().map(|t| t.into_stored_tick()).collect())
    }

    async fn prune_ticks(
        &self,
        code_location_id: &str,
        automation_name: &str,
        max_ticks: usize,
    ) -> Result<usize> {
        let mut result = self
            .db
            .query(
                "LET $keep = (SELECT * FROM ticks WHERE code_location_id = $cl AND automation_name = $name ORDER BY timestamp DESC LIMIT $max);\
                 DELETE FROM ticks WHERE code_location_id = $cl AND automation_name = $name AND id NOT IN $keep.id RETURN BEFORE;"
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("name", automation_name.to_string()))
            .bind(("max", max_ticks))
            .await?;
        let deleted: Vec<DbStoredTick> = result.take(1)?;
        Ok(deleted.len())
    }

    async fn get_condition_ticks(
        &self,
        code_location_id: &str,
        limit: usize,
    ) -> Result<Vec<StoredConditionTick>> {
        let mut result = self
            .db
            .query("SELECT * FROM condition_ticks WHERE code_location_id = $cl ORDER BY timestamp DESC LIMIT $limit")
            .bind(("cl", code_location_id.to_string()))
            .bind(("limit", limit))
            .await?;
        let ticks: Vec<DbStoredConditionTick> = result.take(0)?;
        Ok(ticks.into_iter().map(|t| t.into_stored()).collect())
    }

    /// Drop condition ticks past `max_ticks`, and the per-asset evaluations
    /// that belong to them.
    ///
    /// An evaluation is written once per asset per tick and carries that
    /// tick's id, so how long it lives is a property of the tick. Pruning it
    /// per asset instead cost one query per asset on every flush: at 10,000
    /// assets that was 5 s of pruning to remove what one tick had added, and
    /// the writer never caught up. Evaluations go first, because a crash
    /// between the two leaves ticks the next pass drops again, where the
    /// reverse strands evaluations nothing will ever collect.
    async fn prune_condition_history(
        &self,
        code_location_id: &str,
        max_ticks: usize,
    ) -> Result<usize> {
        let mut result = self
            .db
            .query(
                "LET $keep = (SELECT * FROM condition_ticks WHERE code_location_id = $cl ORDER BY timestamp DESC LIMIT $max);\
                 SELECT * FROM condition_ticks WHERE code_location_id = $cl AND id NOT IN $keep.id;",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("max", max_ticks))
            .await?;
        let dropped: Vec<DbStoredConditionTick> = result.take(1)?;
        if dropped.is_empty() {
            return Ok(0);
        }

        for tick in &dropped {
            self.db
                .query("DELETE FROM condition_evals WHERE tick_id = $tick_id")
                .bind(("tick_id", record_id_str(&tick.id)))
                .await?
                .check()?;
        }

        // By id, not by re-running the window: a tick written since the SELECT
        // would shift it and drop a tick whose evaluations are still here.
        let ids: Vec<RecordId> = dropped.iter().map(|t| t.id.clone()).collect();
        self.db
            .query("DELETE FROM condition_ticks WHERE id IN $ids")
            .bind(("ids", ids))
            .await?
            .check()?;
        Ok(dropped.len())
    }

    async fn get_condition_evals(
        &self,
        code_location_id: &str,
        asset_key: &str,
        limit: usize,
    ) -> Result<Vec<StoredConditionEval>> {
        let mut result = self
            .db
            .query("SELECT * FROM condition_evals WHERE code_location_id = $cl AND asset_key = $key ORDER BY timestamp DESC LIMIT $limit")
            .bind(("cl", code_location_id.to_string()))
            .bind(("key", asset_key.to_string()))
            .bind(("limit", limit))
            .await?;
        let evals: Vec<DbStoredConditionEval> = result.take(0)?;
        Ok(evals.into_iter().map(|e| e.into_stored()).collect())
    }

    async fn get_condition_evals_for_tick(
        &self,
        code_location_id: &str,
        tick_id: &str,
    ) -> Result<Vec<StoredConditionEval>> {
        let mut result = self
            .db
            .query("SELECT * FROM condition_evals WHERE code_location_id = $cl AND tick_id = $tick_id ORDER BY asset_key ASC")
            .bind(("cl", code_location_id.to_string()))
            .bind(("tick_id", tick_id.to_string()))
            .await?;
        let evals: Vec<DbStoredConditionEval> = result.take(0)?;
        Ok(evals.into_iter().map(|e| e.into_stored()).collect())
    }

    async fn get_partition_events(
        &self,
        code_location_id: &str,
        asset_key: &str,
        partition_key: &str,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        for cand in PartitionKey::display_candidates(partition_key)
            .into_iter()
            .rev()
        {
            let mut result = self
                .db
                .query("SELECT * FROM events WHERE code_location_id = $cl AND asset_key = $asset_key AND partition_key = $pk ORDER BY timestamp DESC LIMIT $limit")
                .bind(("cl", code_location_id.to_string()))
                .bind(("asset_key", asset_key.to_string()))
                .bind(("pk", cand))
                .bind(("limit", limit))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            if !events.is_empty() {
                return Ok(events.into_iter().map(|e| e.into_stored_event()).collect());
            }
        }
        Ok(Vec::new())
    }

    async fn get_materialized_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Vec<PartitionKey>> {
        let mut result = self
            .db
            .query("SELECT partition_key FROM asset_partitions WHERE code_location_id = $cl AND asset_key = $asset_key")
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .await?;

        #[derive(Debug, SurrealValue)]
        struct PartRow {
            partition_key: PartitionKey,
        }

        let rows: Vec<PartRow> = result.take(0)?;
        Ok(rows.into_iter().map(|r| r.partition_key).collect())
    }

    async fn count_materialized_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<u64> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT count() AS total FROM asset_partitions WHERE code_location_id = $cl AND asset_key = $asset_key GROUP ALL")
                .bind(("cl", code_location_id.to_string()))
                .bind(("asset_key", asset_key.to_string()))
                .await?;
            let total: Option<u64> = result.take((0, "total"))?;
            Ok(total.unwrap_or(0))
        })
        .await
    }

    async fn count_dynamic_partitions(
        &self,
        code_location_id: &str,
        partitions_def_name: &str,
    ) -> Result<u64> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT count() AS total FROM dynamic_partitions WHERE code_location_id = $cl AND partitions_def_name = $name GROUP ALL")
                .bind(("cl", code_location_id.to_string()))
                .bind(("name", partitions_def_name.to_string()))
                .await?;
            let total: Option<u64> = result.take((0, "total"))?;
            Ok(total.unwrap_or(0))
        })
        .await
    }

    async fn get_partition_timestamps(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Vec<(PartitionKey, i64)>> {
        let mut result = self
            .db
            .query("SELECT partition_key, last_timestamp FROM asset_partitions WHERE code_location_id = $cl AND asset_key = $asset_key AND last_timestamp IS NOT NONE")
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .await?;

        #[derive(Debug, SurrealValue)]
        struct PartTsRow {
            partition_key: PartitionKey,
            last_timestamp: i64,
        }

        let rows: Vec<PartTsRow> = result.take(0)?;
        Ok(rows
            .into_iter()
            .map(|r| (r.partition_key, r.last_timestamp))
            .collect())
    }

    async fn get_partition_timestamps_since(
        &self,
        code_location_id: &str,
        asset_key: &str,
        since_timestamp: i64,
    ) -> Result<Vec<(PartitionKey, i64)>> {
        let mut result = self
            .db
            .query(
                "SELECT partition_key, last_timestamp FROM asset_partitions \
                 WHERE code_location_id = $cl AND asset_key = $asset_key \
                 AND last_timestamp > $since",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .bind(("since", since_timestamp))
            .await?;

        #[derive(Debug, SurrealValue)]
        struct PartTsRow {
            partition_key: PartitionKey,
            last_timestamp: i64,
        }

        let rows: Vec<PartTsRow> = result.take(0)?;
        Ok(rows
            .into_iter()
            .map(|r| (r.partition_key, r.last_timestamp))
            .collect())
    }

    async fn get_partition_timestamps_for_keys(
        &self,
        code_location_id: &str,
        asset_key: &str,
        keys: &[PartitionKey],
    ) -> Result<Vec<(PartitionKey, i64, Option<String>)>> {
        self.partition_timestamps_for_keys(code_location_id, asset_key, keys)
            .await
    }

    async fn get_materialized_partitions_page(
        &self,
        code_location_id: &str,
        asset_key: &str,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<PartitionKey>> {
        self.materialized_partitions_page(code_location_id, asset_key, offset, limit)
            .await
    }

    async fn get_in_progress_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<Vec<PartitionKey>> {
        // `action IS NONE`: an in-flight action is not an in-flight
        // materialization. Without it a long `optimize` reads as in-flight and
        // `eager()` suppresses the asset and every dependent for its duration.
        let mut result = self
            .db
            .query(
                "SELECT partition_key FROM events WHERE code_location_id = $cl AND asset_key = $asset_key \
                 AND event_type = 'StepStart' AND partition_key IS NOT NONE \
                 AND run_id IN (SELECT VALUE run_id FROM runs WHERE code_location_id = $cl AND status = 'Started' \
                 AND action IS NONE AND $asset_key IN node_names) \
                 GROUP BY partition_key",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("asset_key", asset_key.to_string()))
            .await?;

        #[derive(Debug, SurrealValue)]
        struct PartRow {
            partition_key: PartitionKey,
        }

        let rows: Vec<PartRow> = result.take(0)?;
        Ok(rows.into_iter().map(|r| r.partition_key).collect())
    }

    async fn get_partition_failures(
        &self,
        code_location_id: &str,
        asset_key: &str,
    ) -> Result<std::collections::HashMap<PartitionKey, i64>> {
        self.partition_failures(code_location_id, asset_key).await
    }

    async fn get_failed_partitions(
        &self,
        code_location_id: &str,
        asset_key: &str,
        materialized: &std::collections::HashMap<PartitionKey, i64>,
    ) -> Result<std::collections::HashMap<PartitionKey, i64>> {
        let failures = self.partition_failures(code_location_id, asset_key).await?;
        Ok(failures
            .into_iter()
            .filter(|(partition, failed)| {
                materialized
                    .get(partition)
                    .is_none_or(|built| built < failed)
            })
            .collect())
    }

    async fn get_asset_deletion_timestamps(
        &self,
        code_location_id: &str,
    ) -> Result<std::collections::HashMap<String, i64>> {
        // The asset rows, not Deletion events: the time outlives the delete
        // run, and a code location has far fewer assets than deletions.
        let mut result = self
            .db
            .query(
                "SELECT asset_key, last_deletion_timestamp AS ts FROM assets \
                 WHERE code_location_id = $cl AND last_deletion_timestamp IS NOT NONE",
            )
            .bind(("cl", code_location_id.to_string()))
            .await?;

        #[derive(Debug, SurrealValue)]
        struct DelTsRow {
            asset_key: String,
            ts: i64,
        }

        let rows: Vec<DelTsRow> = result.take(0)?;
        Ok(rows.into_iter().map(|r| (r.asset_key, r.ts)).collect())
    }

    async fn get_backfills(
        &self,
        code_location_id: &str,
        limit: Option<usize>,
        status: Option<BackfillStatus>,
    ) -> Result<Vec<BackfillRecord>> {
        let mut query = "SELECT * FROM backfills WHERE code_location_id = $cl".to_string();
        if status.is_some() {
            query.push_str(" AND status = $status");
        }
        query.push_str(" ORDER BY create_time DESC");
        if limit.is_some() {
            query.push_str(" LIMIT $limit");
        }
        let mut q = self
            .db
            .query(&query)
            .bind(("cl", code_location_id.to_string()));
        if let Some(s) = status {
            q = q.bind(("status", format!("{:?}", s)));
        }
        if let Some(lim) = limit {
            q = q.bind(("limit", lim));
        }
        let mut result = q.await?;
        let rows: Vec<BackfillRecord> = result.take(0)?;
        load_backfill_progress(&self.db, rows).await
    }

    async fn set_pool_limit(
        &self,
        code_location_id: &str,
        pool_key: &str,
        limit: i32,
        lease_duration_secs: u32,
    ) -> Result<()> {
        // These rows are load-bearing: a lost `__asset__:` registration makes
        // every claim on that asset hard-fail "pool not configured". Bare
        // `.await?` swallows per-statement errors (conflicts included), and
        // without them surfacing the retry never fires.
        retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "UPSERT concurrency_pools SET \
                         code_location_id = $cl, \
                         pool_key = $pool_key, \
                         slot_limit = $slot_limit, \
                         lease_duration_secs = $lease_duration_secs \
                     WHERE code_location_id = $cl AND pool_key = $pool_key",
                )
                .bind(("cl", code_location_id.to_string()))
                .bind(("pool_key", pool_key.to_string()))
                .bind(("slot_limit", limit))
                .bind(("lease_duration_secs", lease_duration_secs))
                .await?
                .check()?;
            Ok(())
        })
        .await
    }

    async fn get_pool_limits(&self, code_location_id: &str) -> Result<Vec<PoolLimit>> {
        let mut result = self
            .db
            .query("SELECT * FROM concurrency_pools WHERE code_location_id = $cl ORDER BY pool_key ASC")
            .bind(("cl", code_location_id.to_string()))
            .await?;
        let pools: Vec<PoolLimit> = result.take(0)?;
        Ok(pools)
    }

    async fn get_pool_info(&self, code_location_id: &str, pool_key: &str) -> Result<PoolInfo> {
        let now_ns = now_nanos();
        let (pool, claimed_count) = self
            .query_pool_usage(code_location_id, pool_key, now_ns, None)
            .await?;

        let mut result = self
            .db
            .query(
                "SELECT count() AS total FROM pending_steps \
                     WHERE code_location_id = $cl AND pool_key = $pool_key GROUP ALL",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("pool_key", pool_key.to_string()))
            .await?;
        let pending_count: Option<u32> = result.take((0, "total"))?;

        Ok(PoolInfo {
            pool_key: pool.pool_key,
            slot_limit: pool.slot_limit,
            lease_duration_secs: pool.lease_duration_secs,
            claimed_count,
            pending_count: pending_count.unwrap_or(0),
        })
    }

    async fn get_all_pool_infos(&self, code_location_id: &str) -> Result<Vec<PoolInfo>> {
        let now_ns = now_nanos();
        let mut result = self
            .db
            .query(
                "SELECT * FROM concurrency_pools WHERE code_location_id = $cl ORDER BY pool_key ASC; \
                 SELECT pool_key, math::sum(slots_consumed) AS claimed \
                     FROM concurrency_slots WHERE code_location_id = $cl AND lease_expires_at > $now \
                     GROUP BY pool_key; \
                 SELECT pool_key, count() AS pending \
                     FROM pending_steps WHERE code_location_id = $cl GROUP BY pool_key",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("now", now_ns))
            .await?;

        let pools: Vec<PoolLimit> = result.take(0)?;

        #[derive(SurrealValue, serde::Deserialize)]
        struct ClaimedRow {
            pool_key: String,
            claimed: u32,
        }
        let claimed_rows: Vec<ClaimedRow> = result.take(1)?;
        let claimed_map: std::collections::HashMap<String, u32> = claimed_rows
            .into_iter()
            .map(|r| (r.pool_key, r.claimed))
            .collect();

        #[derive(SurrealValue, serde::Deserialize)]
        struct PendingRow {
            pool_key: String,
            pending: u32,
        }
        let pending_rows: Vec<PendingRow> = result.take(2)?;
        let pending_map: std::collections::HashMap<String, u32> = pending_rows
            .into_iter()
            .map(|r| (r.pool_key, r.pending))
            .collect();

        Ok(pools
            .into_iter()
            .map(|p| PoolInfo {
                claimed_count: claimed_map.get(&p.pool_key).copied().unwrap_or(0),
                pending_count: pending_map.get(&p.pool_key).copied().unwrap_or(0),
                pool_key: p.pool_key,
                slot_limit: p.slot_limit,
                lease_duration_secs: p.lease_duration_secs,
            })
            .collect())
    }

    async fn claim_concurrency_slots(
        &self,
        code_location_id: &str,
        pools: &[(String, u32)],
        run_id: &str,
        step_key: &str,
        priority: i32,
        lease_duration_secs: u32,
        scope: Option<&AssetScope>,
    ) -> Result<ConcurrencyClaimStatus> {
        anyhow::ensure!(!pools.is_empty(), "pools must not be empty");

        let predicate =
            |e: &anyhow::Error| retry::default_should_retry(e) || e.is::<PoolContended>();

        // Both retryable outcomes here are lost races, not a sick backend: the
        // write-write conflict on `claim_version` and `PoolContended` alike
        // clear as soon as the winning claim commits.
        let retry_config = self.retry_config.contended();
        let result = retry::with_retry_if(&retry_config, predicate, || async {
            self.try_claim_concurrency_slots_once(
                code_location_id,
                pools,
                run_id,
                step_key,
                priority,
                lease_duration_secs,
                scope,
            )
            .await
        })
        .await;

        match result {
            Ok(status) => Ok(status),
            Err(e) if e.is::<PoolContended>() => {
                anyhow::bail!("failed to claim concurrency slots — extreme contention on pool")
            }
            Err(e) => Err(e),
        }
    }

    async fn get_pool_slot_holders(
        &self,
        code_location_id: &str,
        pool_key: &str,
    ) -> Result<Vec<SlotHolder>> {
        let now_ns = now_nanos();
        let mut result = self
            .db
            .query(
                "SELECT run_id, step_key, slots_consumed, claimed_at, lease_expires_at \
                     FROM concurrency_slots \
                     WHERE code_location_id = $cl AND pool_key = $pool_key \
                     AND lease_expires_at > $now \
                     ORDER BY claimed_at ASC",
            )
            .bind(("cl", code_location_id.to_string()))
            .bind(("pool_key", pool_key.to_string()))
            .bind(("now", now_ns))
            .await?;
        let holders: Vec<SlotHolder> = result.take(0)?;
        Ok(holders)
    }

    async fn get_runs(
        &self,
        code_location_id: &str,
        limit: usize,
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        let mut result = if let Some(s) = status {
            let status_str = format!("{:?}", s);
            self.db
                .query("SELECT * FROM runs WHERE code_location_id = $cl AND status = $status ORDER BY start_time DESC LIMIT $limit")
                .bind(("cl", code_location_id.to_string()))
                .bind(("status", status_str))
                .bind(("limit", limit))
                .await?
        } else {
            self.db
                .query("SELECT * FROM runs WHERE code_location_id = $cl ORDER BY start_time DESC LIMIT $limit")
                .bind(("cl", code_location_id.to_string()))
                .bind(("limit", limit))
                .await?
        };
        let runs: Vec<RunRecord> = result.take(0)?;
        Ok(runs)
    }

    async fn get_queued_runs(&self, code_location_id: &str) -> Result<Vec<RunRecord>> {
        let mut result = self
            .db
            .query("SELECT * FROM runs WHERE code_location_id = $cl AND status = 'Queued'")
            .bind(("cl", code_location_id.to_string()))
            .await?;
        let runs: Vec<RunRecord> = result.take(0)?;
        Ok(runs)
    }

    async fn get_runs_since(
        &self,
        code_location_id: &str,
        since_timestamp: i64,
        status: Option<RunStatus>,
        order: crate::storage::SortOrder,
    ) -> Result<Vec<RunRecord>> {
        let mut result = if let Some(s) = status {
            let status_str = format!("{:?}", s);
            self.db
                .query(format!(
                    "SELECT * FROM runs WHERE code_location_id = $cl AND start_time > $since AND status = $status ORDER BY start_time {}",
                    order.as_sql()
                ))
                .bind(("cl", code_location_id.to_string()))
                .bind(("since", since_timestamp))
                .bind(("status", status_str))
                .await?
        } else {
            self.db
                .query(format!(
                    "SELECT * FROM runs WHERE code_location_id = $cl AND start_time > $since ORDER BY start_time {}",
                    order.as_sql()
                ))
                .bind(("cl", code_location_id.to_string()))
                .bind(("since", since_timestamp))
                .await?
        };
        let runs: Vec<RunRecord> = result.take(0)?;
        Ok(runs)
    }

    async fn get_runs_ended_since(
        &self,
        code_location_id: &str,
        since: i64,
        status: RunStatus,
        job_names: Option<&[String]>,
        limit: usize,
    ) -> Result<Vec<RunRecord>> {
        let job_filter = if job_names.is_some() {
            " AND job_name IN $jobs"
        } else {
            ""
        };
        let query = format!(
            "SELECT * FROM runs \
                 WHERE code_location_id = $cl AND end_time >= $since AND status = $status{job_filter} \
                 ORDER BY end_time ASC LIMIT $limit"
        );
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(query.as_str())
                .bind(("cl", code_location_id.to_string()))
                .bind(("since", since))
                .bind(("status", status.clone()))
                .bind(("jobs", job_names.map(<[String]>::to_vec)))
                .bind(("limit", limit))
                .await?;
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs)
        })
        .await
    }

    async fn get_condition_eval_state(
        &self,
        code_location_id: &str,
    ) -> Result<Option<crate::condition::ConditionEvalState>> {
        // Retry: the caller resets to fresh state on error, discarding all latches.
        let key = crate::condition_eval_state_key(code_location_id);
        retry::with_retry(&self.retry_config, || async {
            self.kv_get_json(&key).await
        })
        .await
    }

    async fn set_condition_eval_state(
        &self,
        code_location_id: &str,
        state: &crate::condition::ConditionEvalState,
    ) -> Result<()> {
        self.kv_set_json(&crate::condition_eval_state_key(code_location_id), state)
            .await
    }

    async fn get_condition_pending_dispatch(
        &self,
        code_location_id: &str,
    ) -> Result<Option<crate::condition::PendingDispatch>> {
        self.kv_get_json(&crate::condition_pending_dispatch_key(code_location_id))
            .await
    }

    async fn set_condition_pending_dispatch(
        &self,
        code_location_id: &str,
        pending: &crate::condition::PendingDispatch,
    ) -> Result<()> {
        self.kv_set_json(
            &crate::condition_pending_dispatch_key(code_location_id),
            pending,
        )
        .await
    }

    async fn get_graph_topology(
        &self,
        code_location_id: &str,
    ) -> Result<Option<crate::assets::graph::GraphTopology>> {
        self.kv_get_json(&crate::graph_topology_key(code_location_id))
            .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(%code_location_id, key_lists = key_lists.len()))]
    async fn set_graph_topology(
        &self,
        code_location_id: &str,
        topology: &crate::assets::graph::GraphTopology,
        key_lists: &[crate::assets::partitions_fingerprint::KeyList<'_>],
    ) -> Result<()> {
        let topology = surrealdb::types::Bytes::from(serde_json::to_vec(topology)?);

        let retry_if = |e: &anyhow::Error| {
            retry::default_should_retry(e) || format!("{e:#}").contains(KEY_ROWS_CHANGED)
        };
        retry::with_retry_if(&self.retry_config, retry_if, || async {
            let stored = self.stored_key_rows(code_location_id).await?;
            self.write_topology(code_location_id, &topology, key_lists, &stored)
                .await
        })
        .await
    }

    async fn get_partition_keys(
        &self,
        code_location_id: &str,
        digest: &str,
    ) -> Result<Option<Vec<String>>> {
        self.kv_get_json(&crate::partition_keys_key(code_location_id, digest))
            .await
    }
}
