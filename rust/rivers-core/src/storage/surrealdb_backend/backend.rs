use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use surrealdb::types::{Bytes, SurrealValue};

use crate::storage::retry;
use crate::storage::{
    BackfillRecord, BackfillStatus, ConditionEvalRecord, ConditionTickRecord, EventRecord,
    EventType, LogRecord, PartitionKey, RunOutcome, RunProgress, RunRecord, RunStatus, StepOutcome,
    StorageBackend, StoredEvent, StoredLog, TickRecord,
};

use super::*;

impl StorageBackend for SurrealStorage {
    #[tracing::instrument(skip_all, target = "rivers::storage", fields(cl = %event.code_location_id, asset_key = event.asset_key))]
    async fn store_event(&self, event: &EventRecord) -> Result<String> {
        // Generated outside the retry closure: a replayed attempt re-inserts
        // the same record id, which INSERT IGNORE turns into a no-op.
        let record_id = new_event_record_id();
        let event_id = record_id_str(&record_id);
        retry::with_retry(&self.retry_config, || async {
        let cl = event.code_location_id.as_str();
        let mut db_event = DbEventWrite::from_event(event, record_id.clone());

        let mut materialization_code_version: Option<String> = None;
        if let Some(asset_key) = &event.asset_key
            && event.event_type.is_materialization() {
                let cv = self.get_code_version(cl, asset_key).await?;
                db_event.code_version = cv.clone();
                db_event.input_data_versions = event.input_data_versions.clone();
                materialization_code_version = cv;
            }

        self.db
            .query("INSERT IGNORE INTO events $rows RETURN NONE")
            .bind(("rows", vec![db_event]))
            .await
            .context("failed to store event")?
            .check()?;
        let event_id = event_id.clone();

        if let Some(asset_key) = &event.asset_key
            && event.event_type.is_materialization() {
                let input_data_versions = event.input_data_versions.clone();
                let is_action = !self
                    .action_run_ids(vec![event.run_id.clone()])
                    .await?
                    .is_empty();
                let idv = (!input_data_versions.is_empty()).then_some(input_data_versions);
                let parts = event
                    .partition_key
                    .as_ref()
                    .map(|partition_key| {
                        vec![DbAssetPartitionWrite {
                            code_location_id: cl.to_string(),
                            asset_key: asset_key.clone(),
                            partition_key: partition_key.clone(),
                            last_event_id: event_id.clone(),
                            last_run_id: event.run_id.clone(),
                            last_timestamp: event.timestamp,
                        }]
                    })
                    .unwrap_or_default();
                self.apply_materialization(
                    cl,
                    asset_key,
                    &event_id,
                    &event.run_id,
                    event.timestamp,
                    event.event_type.data_version().map(|s| s.to_string()),
                    event.timestamp,
                    materialization_code_version,
                    idv,
                    is_action,
                    parts,
                )
                .await?;
            }

        if let Some(asset_key) = &event.asset_key
            && event.event_type.is_observation() {
                let data_version = event.event_type.data_version().map(|s| s.to_string());
                self.db
                    .query("UPDATE assets SET last_event_id = $event_id, last_timestamp = $timestamp, last_data_version = $data_version WHERE code_location_id = $cl AND asset_key = $asset_key")
                    .bind(("cl", cl.to_string()))
                    .bind(("asset_key", asset_key.clone()))
                    .bind(("event_id", event_id.clone()))
                    .bind(("timestamp", event.timestamp))
                    .bind(("data_version", data_version))
                    .await?
                    .check()?;
            }

        if let Some(asset_key) = &event.asset_key
            && event.event_type.is_deletion() {
                self.consolidate_deletion(cl, asset_key, event, &event_id).await?;
            }

        Ok(event_id)
        })
        .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(count = events.len()))]
    async fn store_events(&self, events: &[EventRecord]) -> Result<Vec<String>> {
        if events.is_empty() {
            return Ok(vec![]);
        }
        // Ids generated outside the retry closure: one consolidation conflict
        // replays the whole drained batch, and INSERT IGNORE needs the same
        // record ids to turn the re-insert into a no-op.
        let rows: Vec<DbEventWrite> = events
            .iter()
            .map(|e| DbEventWrite::from_event(e, new_event_record_id()))
            .collect();
        let event_ids: Vec<String> = rows.iter().map(|r| record_id_str(&r.id)).collect();
        retry::with_retry(&self.retry_config, || async {
        self.db
            .query("INSERT IGNORE INTO events $rows RETURN NONE")
            .bind(("rows", rows.clone()))
            .await
            .context("failed to batch store events")?
            .check()?;

        // Group materializations by asset, then one bulk upsert. The newest
        // event wins, by time, not by place in the drain: the `assets` row
        // must hold a time no older than any of its partition rows.
        let mut latest_mat: std::collections::HashMap<(&str, &str), usize> =
            std::collections::HashMap::new();
        // Latest event that actually consumed inputs, tracked apart from
        // `latest_mat` so an action's provenance-free materialization landing
        // in the same drain doesn't drop a real materialize's provenance.
        let mut latest_idv: std::collections::HashMap<(&str, &str), usize> =
            std::collections::HashMap::new();
        let mut part_rows: std::collections::HashMap<
            (&str, &str, &PartitionKey),
            DbAssetPartitionWrite,
        > = std::collections::HashMap::new();

        for (idx, (event, event_id)) in events.iter().zip(event_ids.iter()).enumerate() {
            let Some(asset_key) = &event.asset_key else {
                continue;
            };
            if !event.event_type.is_materialization() {
                continue;
            }
            let cl = event.code_location_id.as_str();
            let key = (cl, asset_key.as_str());
            let newest = |held: Option<&usize>| {
                held.is_none_or(|&i| events[i].timestamp <= event.timestamp)
            };
            if newest(latest_mat.get(&key)) {
                latest_mat.insert(key, idx);
            }
            if !event.input_data_versions.is_empty() && newest(latest_idv.get(&key)) {
                latest_idv.insert(key, idx);
            }
            if let Some(partition_key) = &event.partition_key {
                let part = (cl, asset_key.as_str(), partition_key);
                if part_rows
                    .get(&part)
                    .is_none_or(|held| held.last_timestamp <= event.timestamp)
                {
                    part_rows.insert(
                        part,
                        DbAssetPartitionWrite {
                            code_location_id: cl.to_string(),
                            asset_key: asset_key.clone(),
                            partition_key: partition_key.clone(),
                            last_event_id: event_id.clone(),
                            last_run_id: event.run_id.clone(),
                            last_timestamp: event.timestamp,
                        },
                    );
                }
            }
        }

        // One `assets` row update per materialized asset (newest event wins).
        let update_run_ids: Vec<String> = latest_mat
            .values()
            .map(|&idx| events[idx].run_id.clone())
            .collect::<std::collections::HashSet<String>>()
            .into_iter()
            .collect();
        let action_ids = self.action_run_ids(update_run_ids).await?;
        // Each asset's partition rows commit in one transaction with its
        // `assets` row update — a concurrent whole-asset deletion lands
        // wholly before or wholly after, never between the two. Every
        // partition row comes from a materialization event, so its asset is
        // always in `latest_mat`.
        let mut parts_by_asset: std::collections::HashMap<(&str, &str), Vec<DbAssetPartitionWrite>> =
            std::collections::HashMap::new();
        for ((cl, ak, _), row) in part_rows {
            parts_by_asset.entry((cl, ak)).or_default().push(row);
        }
        for (&(cl, asset_key), &idx) in &latest_mat {
            let event = &events[idx];
            let event_id = &event_ids[idx];
            let idv_event = latest_idv.get(&(cl, asset_key)).map(|&i| &events[i]);
            let idv = idv_event.map(|e| e.input_data_versions.clone());
            let provenance_timestamp = idv_event.map_or(event.timestamp, |e| e.timestamp);
            // A co-drained real materialize (the `idv` entry) legitimately
            // owns provenance and the code-version stamp even when the
            // action's event is the newer one.
            let action_only = action_ids.contains(&event.run_id) && idv.is_none();
            let code_version = if action_only {
                None
            } else {
                self.get_code_version(cl, asset_key).await?
            };
            let parts = parts_by_asset.remove(&(cl, asset_key)).unwrap_or_default();
            self.apply_materialization(
                cl,
                asset_key,
                event_id,
                &event.run_id,
                event.timestamp,
                event.event_type.data_version().map(|s| s.to_string()),
                provenance_timestamp,
                code_version,
                idv,
                action_only,
                parts,
            )
            .await?;
        }

        // Partition-scoped deletions collapse to one DELETE per asset — a
        // purge over a date range lands thousands of them in one drain.
        let mut part_deletes: HashMap<(&str, &str), Vec<(PartitionKey, i64)>> = HashMap::new();
        let mut whole_deletes: HashMap<(&str, &str), (&EventRecord, &String)> = HashMap::new();
        for (event, event_id) in events.iter().zip(event_ids.iter()) {
            let cl = event.code_location_id.as_str();
            if let Some(asset_key) = &event.asset_key
                && event.event_type.is_observation() {
                    let data_version = event.event_type.data_version().map(|s| s.to_string());
                    self.db
                        .query("UPDATE assets SET last_event_id = $event_id, last_timestamp = $timestamp, last_data_version = $data_version WHERE code_location_id = $cl AND asset_key = $asset_key")
                        .bind(("cl", cl.to_string()))
                        .bind(("asset_key", asset_key.clone()))
                        .bind(("event_id", event_id.clone()))
                        .bind(("timestamp", event.timestamp))
                        .bind(("data_version", data_version))
                        .await?
                        .check()?;
                }
            if let Some(asset_key) = &event.asset_key
                && event.event_type.is_deletion() {
                    match &event.partition_key {
                        Some(pk) => part_deletes
                            .entry((cl, asset_key.as_str()))
                            .or_default()
                            .push((pk.clone(), event.timestamp)),
                        // The newest deletion per asset wins — repeats in one
                        // drain only re-clear the same row.
                        None => {
                            let key = (cl, asset_key.as_str());
                            if whole_deletes
                                .get(&key)
                                .is_none_or(|(held, _)| held.timestamp <= event.timestamp)
                            {
                                whole_deletes.insert(key, (event, event_id));
                            }
                        }
                    }
                }
        }
        for ((cl, asset_key), (event, event_id)) in whole_deletes {
            self.consolidate_deletion(cl, asset_key, event, event_id)
                .await?;
        }
        for ((cl, asset_key), deletions) in part_deletes {
            self.delete_partitions(cl, asset_key, deletions).await?;
        }

        Ok(event_ids.clone())
        })
        .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(count = logs.len()))]
    async fn store_run_logs(&self, logs: &[LogRecord]) -> Result<()> {
        if logs.is_empty() {
            return Ok(());
        }
        let rows: Vec<DbRunLogWrite> = logs.iter().map(DbRunLogWrite::from).collect();
        retry::with_retry(&self.retry_config, || async {
            self.db
                .query("INSERT INTO run_logs $rows RETURN NONE")
                .bind(("rows", rows.clone()))
                .await
                .context("failed to store run logs")?
                .check()
                .context("failed to store run logs")?;
            Ok(())
        })
        .await
    }

    async fn get_run_logs(&self, run_id: &str) -> Result<Vec<StoredLog>> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM run_logs WHERE run_id = $id ORDER BY timestamp ASC, id ASC")
                .bind(("id", run_id.to_string()))
                .await?;
            let rows: Vec<DbStoredRunLog> = result.take(0)?;
            Ok(rows.into_iter().map(|l| l.into_stored_log()).collect())
        })
        .await
    }

    async fn get_events_for_run(&self, run_id: &str) -> Result<Vec<StoredEvent>> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM events WHERE run_id = $run_id ORDER BY timestamp ASC, sort_order ASC, id ASC")
                .bind(("run_id", run_id.to_string()))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            Ok(events.into_iter().map(|e| e.into_stored_event()).collect())
        })
        .await
    }

    /// `run_id IN $run_ids` becomes a union of index scans over
    /// `idx_events_run`, one branch per run. `asset_key` is deliberately not in
    /// the query: SurrealDB cannot index an `IN` against a long list, so it
    /// would test every scanned row against the whole list — which is the
    /// quadratic this method exists to remove. Matching here is a hash lookup.
    async fn step_outcomes(
        &self,
        asset_keys: &[String],
        run_ids: &[String],
    ) -> Result<Vec<StepOutcome>> {
        if asset_keys.is_empty() || run_ids.is_empty() {
            return Ok(Vec::new());
        }
        let wanted: HashSet<&str> = asset_keys.iter().map(String::as_str).collect();
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT asset_key, run_id, event_type FROM events \
                     WHERE run_id IN $run_ids \
                     AND event_type IN ['StepSuccess', 'StepFailure']",
                )
                .bind(("run_ids", run_ids.to_vec()))
                .await?;
            let rows: Vec<DbStepOutcome> = result.take(0)?;
            Ok(rows
                .into_iter()
                .filter_map(|row| {
                    let asset_key = row.asset_key?;
                    if !wanted.contains(asset_key.as_str()) {
                        return None;
                    }
                    Some(StepOutcome {
                        asset_key,
                        run_id: row.run_id,
                        succeeded: row.event_type == EventType::StepSuccess.type_name(),
                    })
                })
                .collect())
        })
        .await
    }

    /// The index hint makes `run_id IN $run_ids` a union of index scans, one
    /// per run; left to itself the planner scans every Materialization event
    /// on `idx_events_type`. `asset_key` is matched here, as in `step_outcomes`.
    async fn materialized_by_runs(
        &self,
        asset_keys: &[String],
        run_ids: &[String],
    ) -> Result<HashMap<String, HashSet<String>>> {
        if asset_keys.is_empty() || run_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let wanted: HashSet<&str> = asset_keys.iter().map(String::as_str).collect();
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT asset_key, run_id FROM events WITH INDEX idx_events_run_type \
                     WHERE run_id IN $run_ids AND event_type = 'Materialization' \
                     GROUP BY asset_key, run_id",
                )
                .bind(("run_ids", run_ids.to_vec()))
                .await?;

            #[derive(SurrealValue)]
            struct Row {
                asset_key: Option<String>,
                run_id: String,
            }
            let rows: Vec<Row> = result.take(0)?;
            let mut out: HashMap<String, HashSet<String>> = HashMap::new();
            for row in rows {
                if let Some(asset_key) = row.asset_key
                    && wanted.contains(asset_key.as_str())
                {
                    out.entry(asset_key).or_default().insert(row.run_id);
                }
            }
            Ok(out)
        })
        .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(run_id = %run.run_id))]
    async fn create_run(&self, run: &RunRecord) -> Result<()> {
        let result = retry::with_retry(&self.retry_config, || async {
            let _: Option<RunRecord> = self
                .db
                .create("runs")
                .content(run.clone())
                .await
                .context("failed to create run")?;
            Ok(())
        })
        .await;
        swallow_phantom_commit(result, "create_run", &run.run_id)
    }

    async fn create_runs(&self, runs: &[RunRecord]) -> Result<()> {
        let result = retry::with_retry(&self.retry_config, || async {
            if runs.is_empty() {
                return Ok(());
            }
            let mut q = String::new();
            for (i, _run) in runs.iter().enumerate() {
                q += &format!("CREATE runs CONTENT $r{i};\n");
            }
            let mut query = self.db.query(&q);
            for (i, run) in runs.iter().enumerate() {
                query = query.bind((format!("r{i}"), run.clone()));
            }
            query.await.context("failed to create runs batch")?;
            Ok(())
        })
        .await;
        swallow_phantom_commit(result, "create_runs", &format!("batch[{}]", runs.len()))
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(%run_id, ?status))]
    async fn update_run_status(
        &self,
        run_id: &str,
        status: RunStatus,
        end_time: Option<i64>,
    ) -> Result<()> {
        retry::with_retry(&self.retry_config, || async {
            let status_str = format!("{:?}", status);
            if let Some(end) = end_time {
                self.db
                    .query(
                        "UPDATE runs SET status = $status, end_time = $end_time WHERE run_id = $run_id",
                    )
                    .bind(("run_id", run_id.to_string()))
                    .bind(("status", status_str))
                    .bind(("end_time", end))
                    .await?;
            } else {
                self.db
                    .query("UPDATE runs SET status = $status WHERE run_id = $run_id")
                    .bind(("run_id", run_id.to_string()))
                    .bind(("status", status_str))
                    .await?;
            }
            Ok(())
        })
        .await
    }

    async fn update_run_block_reason(&self, run_id: &str, reason: Option<&str>) -> Result<()> {
        retry::with_retry(&self.retry_config, || async {
            self.db
                .query("UPDATE runs SET block_reason = $reason WHERE run_id = $run_id")
                .bind(("run_id", run_id.to_string()))
                .bind(("reason", reason.map(|s| s.to_string())))
                .await?;
            Ok(())
        })
        .await
    }

    async fn try_start_run(&self, run_id: &str) -> Result<bool> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "UPDATE runs SET status = 'Started' \
                         WHERE run_id = $run_id AND status != 'Canceled'; \
                     SELECT status FROM runs WHERE run_id = $run_id LIMIT 1",
                )
                .bind(("run_id", run_id.to_string()))
                .await?;
            let status: Option<String> = result.take((1, "status"))?;
            match status.as_deref() {
                Some("Started") => Ok(true),
                Some(_) => Ok(false),
                None => anyhow::bail!("run {run_id} not found"),
            }
        })
        .await
    }

    async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM runs WHERE run_id = $run_id LIMIT 1")
                .bind(("run_id", run_id.to_string()))
                .await?;
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs.into_iter().next())
        })
        .await
    }

    async fn get_runs_by_ids(
        &self,
        run_ids: &[String],
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        retry::with_retry(&self.retry_config, || async {
            if run_ids.is_empty() {
                return Ok(Vec::new());
            }
            let mut result = if let Some(s) = status.clone() {
                let status_str = format!("{:?}", s);
                self.db
                    .query("SELECT * FROM runs WHERE run_id IN $ids AND status = $status ORDER BY start_time ASC, run_id ASC")
                    .bind(("ids", run_ids.to_vec()))
                    .bind(("status", status_str))
                    .await?
            } else {
                self.db
                    .query("SELECT * FROM runs WHERE run_id IN $ids ORDER BY start_time ASC, run_id ASC")
                    .bind(("ids", run_ids.to_vec()))
                    .await?
            };
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs)
        })
        .await
    }

    async fn get_all_runs(
        &self,
        limit: usize,
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = if let Some(s) = status.clone() {
                let status_str = format!("{:?}", s);
                self.db
                    .query("SELECT * FROM runs WHERE status = $status ORDER BY start_time DESC LIMIT $limit")
                    .bind(("status", status_str))
                    .bind(("limit", limit))
                    .await?
            } else {
                self.db
                    .query("SELECT * FROM runs ORDER BY start_time DESC LIMIT $limit")
                    .bind(("limit", limit))
                    .await?
            };
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs)
        })
        .await
    }

    async fn get_all_runs_since(
        &self,
        since_timestamp: i64,
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = if let Some(s) = status.clone() {
                let status_str = format!("{:?}", s);
                self.db
                    .query("SELECT * FROM runs WHERE start_time > $since AND status = $status ORDER BY start_time DESC")
                    .bind(("since", since_timestamp))
                    .bind(("status", status_str))
                    .await?
            } else {
                self.db
                    .query("SELECT * FROM runs WHERE start_time > $since ORDER BY start_time DESC")
                    .bind(("since", since_timestamp))
                    .await?
            };
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs)
        })
        .await
    }

    async fn get_all_queued_runs(&self) -> Result<Vec<RunRecord>> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM runs WHERE status IN ['Queued', 'NotStarted']")
                .await?;
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs)
        })
        .await
    }

    async fn count_in_progress_runs(&self) -> Result<usize> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM runs WHERE status IN ['NotStarted', 'Started']")
                .await?;
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs.len())
        })
        .await
    }

    async fn get_in_progress_runs(&self) -> Result<Vec<RunRecord>> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM runs WHERE status IN ['NotStarted', 'Started']")
                .await?;
            let runs: Vec<RunRecord> = result.take(0)?;
            Ok(runs)
        })
        .await
    }

    async fn get_observations_since(
        &self,
        code_location_id: &str,
        since_timestamp: i64,
    ) -> Result<Vec<StoredEvent>> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT * FROM events WHERE event_type = $etype \
                     AND (code_location_id = $cl OR code_location_id = NONE) \
                     AND timestamp > $since ORDER BY timestamp DESC",
                )
                .bind(("etype", "Observation".to_string()))
                .bind(("cl", code_location_id.to_string()))
                .bind(("since", since_timestamp))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            Ok(events.into_iter().map(|e| e.into_stored_event()).collect())
        })
        .await
    }

    async fn get_latest_observation_ts(&self, code_location_id: &str) -> Result<Option<i64>> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT timestamp FROM events WHERE event_type = $etype \
                     AND (code_location_id = $cl OR code_location_id = NONE) \
                     ORDER BY timestamp DESC LIMIT 1",
                )
                .bind(("etype", "Observation".to_string()))
                .bind(("cl", code_location_id.to_string()))
                .await?;

            #[derive(Debug, SurrealValue)]
            struct TsRow {
                timestamp: i64,
            }
            let rows: Vec<TsRow> = result.take(0)?;
            Ok(rows.into_iter().next().map(|r| r.timestamp))
        })
        .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(%key))]
    async fn kv_get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM kv WHERE key = $key LIMIT 1")
                .bind(("key", key.to_string()))
                .await?;
            let kvs: Vec<DbKv> = result.take(0)?;
            Ok(kvs.into_iter().next().map(|kv| kv.value.to_vec()))
        })
        .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(%key))]
    async fn kv_set(&self, key: &str, value: &[u8]) -> Result<()> {
        // Single atomic upsert on the UNIQUE `kv.key` index — a crash must
        // never leave the key deleted (the old DELETE+CREATE pair could).
        retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "INSERT INTO kv { key: $key, value: $value } \
                     ON DUPLICATE KEY UPDATE value = $input.value",
                )
                .bind(("key", key.to_string()))
                .bind(("value", Bytes::from(value.to_vec())))
                .await?
                .check()?;
            Ok(())
        })
        .await
    }

    async fn store_tick(&self, tick: &TickRecord) -> Result<String> {
        retry::with_retry(&self.retry_config, || async {
            let db_tick = DbTickWrite::from(tick);
            let result: Option<DbStoredTick> = self
                .db
                .create("ticks")
                .content(db_tick)
                .await
                .context("failed to store tick")?;
            let stored = result.context("no tick returned from create")?;
            Ok(record_id_str(&stored.id))
        })
        .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(count = ticks.len()))]
    async fn store_ticks_batch(&self, ticks: &[TickRecord]) -> Result<Vec<String>> {
        retry::with_retry(&self.retry_config, || async {
            if ticks.is_empty() {
                return Ok(vec![]);
            }
            let db_ticks: Vec<DbTickWrite> = ticks.iter().map(DbTickWrite::from).collect();
            let results: Vec<DbStoredTick> = self
                .db
                .insert("ticks")
                .content(db_ticks)
                .await
                .context("failed to batch store ticks")?;
            Ok(results.iter().map(|t| record_id_str(&t.id)).collect())
        })
        .await
    }

    async fn store_condition_tick(&self, tick: &ConditionTickRecord) -> Result<String> {
        retry::with_retry(&self.retry_config, || async {
            let db_tick = DbConditionTickWrite::from(tick);
            let result: Option<DbStoredConditionTick> =
                self.db.create("condition_ticks").content(db_tick).await?;
            let stored = result.context("no tick returned from create")?;
            Ok(record_id_str(&stored.id))
        })
        .await
    }

    #[tracing::instrument(skip_all, target = "rivers::storage", fields(count = evals.len()))]
    async fn store_condition_evals_batch(
        &self,
        evals: &[ConditionEvalRecord],
    ) -> Result<Vec<String>> {
        retry::with_retry(&self.retry_config, || async {
            if evals.is_empty() {
                return Ok(vec![]);
            }
            let db_evals: Vec<DbConditionEvalWrite> =
                evals.iter().map(DbConditionEvalWrite::from).collect();
            let results: Vec<DbStoredConditionEval> =
                self.db.insert("condition_evals").content(db_evals).await?;
            Ok(results.iter().map(|e| record_id_str(&e.id)).collect())
        })
        .await
    }

    // ── Backfills ──

    async fn create_backfill(&self, backfill: &BackfillRecord) -> Result<()> {
        let result = retry::with_retry(&self.retry_config, || async {
            let _: Option<BackfillRecord> = self
                .db
                .create("backfills")
                .content(backfill.clone())
                .await
                .context("failed to create backfill")?;
            Ok(())
        })
        .await;
        swallow_phantom_commit(result, "create_backfill", &backfill.backfill_id)
    }

    async fn update_backfill_status(
        &self,
        backfill_id: &str,
        status: BackfillStatus,
        end_time: Option<i64>,
    ) -> Result<()> {
        retry::with_retry(&self.retry_config, || async {
            let status_str = format!("{:?}", status);
            if let Some(end) = end_time {
                self.db
                    .query("UPDATE backfills SET status = $status, end_time = $end_time WHERE backfill_id = $id")
                    .bind(("id", backfill_id.to_string()))
                    .bind(("status", status_str))
                    .bind(("end_time", end))
                    .await?;
            } else {
                self.db
                    .query("UPDATE backfills SET status = $status WHERE backfill_id = $id")
                    .bind(("id", backfill_id.to_string()))
                    .bind(("status", status_str))
                    .await?;
            }
            Ok(())
        })
        .await
    }

    async fn update_backfill_progress(
        &self,
        backfill_id: &str,
        run_ids: &[String],
        completed: &[PartitionKey],
        failed: &[PartitionKey],
        canceled: &[PartitionKey],
    ) -> Result<()> {
        retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "UPDATE backfills SET \
                     run_ids = array::union(run_ids, $run_ids), \
                     completed_partitions = array::union(completed_partitions, $completed), \
                     failed_partitions = array::union(failed_partitions, $failed), \
                     canceled_partitions = array::union(canceled_partitions, $canceled) \
                     WHERE backfill_id = $id",
                )
                .bind(("id", backfill_id.to_string()))
                .bind(("run_ids", run_ids.to_vec()))
                .bind(("completed", completed.to_vec()))
                .bind(("failed", failed.to_vec()))
                .bind(("canceled", canceled.to_vec()))
                .await?;
            Ok(())
        })
        .await
    }

    async fn get_backfill(&self, backfill_id: &str) -> Result<Option<BackfillRecord>> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query("SELECT * FROM backfills WHERE backfill_id = $id LIMIT 1")
                .bind(("id", backfill_id.to_string()))
                .await?;
            let rows: Vec<BackfillRecord> = result.take(0)?;
            Ok(rows.into_iter().next())
        })
        .await
    }

    async fn try_complete_backfill(
        &self,
        backfill_id: &str,
        extra_canceled: &[PartitionKey],
    ) -> Result<Option<BackfillStatus>> {
        let backfill = self
            .get_backfill(backfill_id)
            .await?
            .context("backfill not found")?;

        if backfill.status != BackfillStatus::InProgress {
            return Ok(None);
        }

        let runs = if backfill.run_ids.is_empty() {
            Vec::new()
        } else {
            self.get_runs_by_ids(&backfill.run_ids, None).await?
        };
        let all_terminal = runs.iter().all(|r| {
            matches!(
                r.status,
                RunStatus::Success | RunStatus::Failure | RunStatus::Canceled
            )
        });
        if !all_terminal {
            return Ok(None);
        }
        // Nothing to finalize: no terminal runs and no externally-canceled keys.
        if runs.is_empty() && extra_canceled.is_empty() {
            return Ok(None);
        }

        let mut completed_pks: Vec<PartitionKey> = Vec::new();
        let mut failed_pks: Vec<PartitionKey> = Vec::new();
        let mut canceled_pks: Vec<PartitionKey> = Vec::new();
        let mut any_failed = false;
        let mut any_canceled = false;

        #[derive(SurrealValue)]
        struct FailRow {
            run_id: String,
            partition_key: PartitionKey,
        }
        let success_run_ids: Vec<String> = runs
            .iter()
            .filter(|r| matches!(r.status, RunStatus::Success))
            .map(|r| r.run_id.clone())
            .collect();
        let mut failed_by_run: std::collections::HashMap<
            String,
            std::collections::HashSet<PartitionKey>,
        > = std::collections::HashMap::new();
        if !success_run_ids.is_empty() {
            let rows: Vec<FailRow> = retry::with_retry(&self.retry_config, || async {
                let mut res = self
                    .db
                    .query(
                        "SELECT run_id, partition_key FROM events \
                         WHERE run_id IN $rids AND event_type = 'StepFailure' \
                         AND partition_key IS NOT NONE GROUP BY run_id, partition_key",
                    )
                    .bind(("rids", success_run_ids.clone()))
                    .await?;
                Ok(res.take(0)?)
            })
            .await?;
            for row in rows {
                failed_by_run
                    .entry(row.run_id)
                    .or_default()
                    .insert(row.partition_key);
            }
        }

        for run in &runs {
            let Some(ref pk) = run.partition_key else {
                match run.status {
                    RunStatus::Failure => any_failed = true,
                    RunStatus::Canceled => any_canceled = true,
                    _ => {}
                }
                continue;
            };
            match run.status {
                RunStatus::Success => {
                    let failed_members = failed_by_run.get(&run.run_id);
                    for member in pk.members() {
                        if failed_members.is_some_and(|f| f.contains(&member)) {
                            failed_pks.push(member);
                            any_failed = true;
                        } else {
                            completed_pks.push(member);
                        }
                    }
                }
                RunStatus::Failure => {
                    failed_pks.extend(pk.members());
                    any_failed = true;
                }
                RunStatus::Canceled => {
                    canceled_pks.extend(pk.members());
                    any_canceled = true;
                }
                _ => {}
            }
        }

        if !extra_canceled.is_empty() {
            canceled_pks.extend(extra_canceled.iter().cloned());
            any_canceled = true;
        }

        retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "UPDATE backfills SET \
                     completed_partitions = $completed, \
                     failed_partitions = $failed, \
                     canceled_partitions = $canceled \
                     WHERE backfill_id = $id",
                )
                .bind(("id", backfill_id.to_string()))
                .bind(("completed", completed_pks.clone()))
                .bind(("failed", failed_pks.clone()))
                .bind(("canceled", canceled_pks.clone()))
                .await?;
            Ok(())
        })
        .await?;

        let new_status = if any_failed {
            BackfillStatus::CompletedFailed
        } else if any_canceled {
            BackfillStatus::Canceled
        } else {
            BackfillStatus::CompletedSuccess
        };

        let now = now_nanos();
        self.update_backfill_status(backfill_id, new_status.clone(), Some(now))
            .await?;
        Ok(Some(new_status))
    }

    async fn cancel_backfill(&self, backfill_id: &str) -> Result<BackfillStatus> {
        // Settle first: if every run already finished, the cancel prevented
        // nothing and the derived outcome wins.
        if let Some(settled) = self.try_complete_backfill(backfill_id, &[]).await? {
            return Ok(settled);
        }
        let now = now_nanos();
        retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "UPDATE backfills SET status = 'Canceled', end_time = $now \
                     WHERE backfill_id = $id AND status IN ['Requested', 'InProgress']",
                )
                .bind(("id", backfill_id.to_string()))
                .bind(("now", now))
                .await?
                .check()?;
            Ok(())
        })
        .await?;
        Ok(self
            .get_backfill(backfill_id)
            .await?
            .with_context(|| format!("backfill '{backfill_id}' not found"))?
            .status)
    }

    // Concurrency pools

    async fn free_concurrency_slots(&self, run_id: &str, step_key: &str) -> Result<()> {
        retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "DELETE FROM concurrency_slots \
                     WHERE run_id = $run_id AND step_key = $step_key; \
                     DELETE FROM pending_steps \
                     WHERE run_id = $run_id AND step_key = $step_key",
                )
                .bind(("run_id", run_id.to_string()))
                .bind(("step_key", step_key.to_string()))
                .await?;
            Ok(())
        })
        .await
    }

    async fn free_concurrency_slots_for_run(&self, run_id: &str) -> Result<()> {
        retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "DELETE FROM concurrency_slots WHERE run_id = $run_id; \
                     DELETE FROM pending_steps WHERE run_id = $run_id",
                )
                .bind(("run_id", run_id.to_string()))
                .await?;
            Ok(())
        })
        .await
    }

    async fn renew_slot_lease(
        &self,
        run_id: &str,
        step_key: &str,
        lease_duration_secs: u32,
    ) -> Result<u32> {
        retry::with_retry(&self.retry_config, || async {
            let now_ns = now_nanos();
            let lease_exp = now_ns + (lease_duration_secs as i64) * 1_000_000_000;

            let mut result = self
                .db
                .query(
                    "UPDATE concurrency_slots \
                         SET lease_expires_at = $lease_exp, last_heartbeat = $now \
                         WHERE run_id = $run_id AND step_key = $step_key; \
                     SELECT count() AS total FROM concurrency_slots \
                         WHERE run_id = $run_id AND step_key = $step_key GROUP ALL",
                )
                .bind(("run_id", run_id.to_string()))
                .bind(("step_key", step_key.to_string()))
                .bind(("now", now_ns))
                .bind(("lease_exp", lease_exp))
                .await?;
            let renewed: Option<u32> = result.take((1, "total"))?;
            Ok(renewed.unwrap_or(0))
        })
        .await
    }

    async fn free_expired_leases(&self) -> Result<u32> {
        retry::with_retry(&self.retry_config, || async {
            let now_ns = now_nanos();
            let mut result = self
                .db
                .query(
                    "SELECT count() AS total FROM concurrency_slots \
                         WHERE lease_expires_at <= $now GROUP ALL; \
                     DELETE FROM concurrency_slots WHERE lease_expires_at <= $now",
                )
                .bind(("now", now_ns))
                .await?;
            let expired: Option<u32> = result.take((0, "total"))?;
            Ok(expired.unwrap_or(0))
        })
        .await
    }

    async fn cancel_queued_run(&self, run_id: &str) -> Result<bool> {
        retry::with_retry(&self.retry_config, || async {
            let now_ns = now_nanos();
            let mut result = self
                .db
                .query(
                    "UPDATE runs SET status = $new_status, end_time = $now \
                         WHERE run_id = $run_id AND status IN ['Queued', 'NotStarted']; \
                     DELETE FROM pending_steps WHERE run_id = $run_id; \
                     SELECT count() AS total FROM runs \
                         WHERE run_id = $run_id AND status = $new_status GROUP ALL",
                )
                .bind(("run_id", run_id.to_string()))
                .bind(("new_status", RunStatus::Canceled))
                .bind(("now", now_ns))
                .await?;
            let count: Option<u32> = result.take((2, "total"))?;
            Ok(count.unwrap_or(0) > 0)
        })
        .await
    }

    async fn delete_run(&self, run_id: &str) -> Result<bool> {
        // Check-then-delete is race-free here: terminal statuses are
        // permanent (re-execution mints a new run_id), so a run observed
        // terminal can't be picked up by the coordinator afterwards. The
        // runs row goes last so a partial failure stays re-deletable.
        let run = match self.get_run(run_id).await? {
            None => return Ok(false),
            Some(r) => r,
        };
        if !matches!(
            run.status,
            RunStatus::Success | RunStatus::Failure | RunStatus::Canceled
        ) {
            anyhow::bail!(
                "run '{run_id}' is {:?} — cancel it and let it finish before deleting",
                run.status
            );
        }
        retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "DELETE FROM events WHERE run_id = $run_id; \
                     DELETE FROM run_logs WHERE run_id = $run_id; \
                     DELETE FROM concurrency_slots WHERE run_id = $run_id; \
                     DELETE FROM pending_steps WHERE run_id = $run_id; \
                     DELETE FROM kv WHERE key = $cancel_key; \
                     DELETE FROM runs WHERE run_id = $run_id",
                )
                .bind(("run_id", run_id.to_string()))
                .bind(("cancel_key", format!("cancel:{run_id}")))
                .await?
                .check()?;
            Ok(())
        })
        .await?;
        Ok(true)
    }

    async fn get_run_progress(&self, run_id: &str) -> Result<RunProgress> {
        retry::with_retry(&self.retry_config, || async {
            // Distinct steps, not raw events — a retried step re-emits
            // StepStart/StepFailure per attempt and must count once. The dedup
            // stays in-query so only scalars cross the DB hop (the operator
            // polls this every reconcile pass).
            let mut result = self
                .db
                .query(
                    "SELECT count() AS n FROM \
                         (SELECT asset_key FROM events \
                          WHERE run_id = $run_id AND event_type = 'StepStart' \
                          AND asset_key IS NOT NONE \
                          GROUP BY asset_key) \
                         GROUP ALL; \
                     SELECT count() AS n FROM \
                         (SELECT asset_key FROM events \
                          WHERE run_id = $run_id \
                          AND (event_type = 'StepSuccess' \
                               OR (event_type = 'StepFailure' AND partition_key IS NONE)) \
                          AND asset_key IS NOT NONE \
                          GROUP BY asset_key) \
                         GROUP ALL; \
                     SELECT asset_key, timestamp FROM events \
                         WHERE run_id = $run_id \
                         AND (event_type = 'StepSuccess' OR (event_type = 'StepFailure' AND partition_key IS NONE)) \
                         ORDER BY timestamp DESC LIMIT 1",
                )
                .bind(("run_id", run_id.to_string()))
                .await?;

            let started: Option<u32> = result.take((0, "n"))?;
            let terminal: Option<u32> = result.take((1, "n"))?;

            #[derive(Debug, SurrealValue, serde::Deserialize)]
            struct LastStep {
                asset_key: Option<String>,
                timestamp: i64,
            }
            let last_steps: Vec<LastStep> = result.take(2)?;
            let last = last_steps.into_iter().next();

            Ok(RunProgress {
                completed_steps: terminal.unwrap_or(0),
                total_steps: started.unwrap_or(0),
                last_step_completed_at: last.as_ref().map(|s| s.timestamp),
                last_completed_step: last.and_then(|s| s.asset_key),
            })
        })
        .await
    }

    async fn get_run_outcome(&self, run_id: &str) -> Result<Option<RunOutcome>> {
        let key = format!("run_outcome:{run_id}");
        let data = self.kv_get(&key).await?;
        match data {
            Some(bytes) => {
                let outcome: RunOutcome = serde_json::from_slice(&bytes)?;
                Ok(Some(outcome))
            }
            None => Ok(None),
        }
    }

    async fn set_run_outcome(&self, run_id: &str, outcome: &RunOutcome) -> Result<()> {
        let key = format!("run_outcome:{run_id}");
        let bytes = serde_json::to_vec(outcome)?;
        self.kv_set(&key, &bytes).await
    }

    async fn request_cancellation(&self, run_id: &str) -> Result<()> {
        let key = format!("cancel:{run_id}");
        self.kv_set(&key, b"1").await
    }

    async fn is_cancelled(&self, run_id: &str) -> Result<bool> {
        let key = format!("cancel:{run_id}");
        Ok(self.kv_get(&key).await?.is_some())
    }

    async fn get_events_for_step(&self, run_id: &str, step_key: &str) -> Result<Vec<StoredEvent>> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT * FROM events \
                         WHERE run_id = $run_id AND asset_key = $step_key \
                         ORDER BY timestamp ASC, sort_order ASC, id ASC",
                )
                .bind(("run_id", run_id.to_string()))
                .bind(("step_key", step_key.to_string()))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            Ok(events.into_iter().map(|e| e.into_stored_event()).collect())
        })
        .await
    }

    async fn get_step_terminal_events(
        &self,
        run_id: &str,
        step_key: &str,
    ) -> Result<Vec<StoredEvent>> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT * FROM events \
                         WHERE run_id = $run_id AND asset_key = $step_key \
                         AND (event_type = 'StepSuccess' OR event_type = 'StepFailure') \
                         ORDER BY timestamp ASC, sort_order ASC, id ASC",
                )
                .bind(("run_id", run_id.to_string()))
                .bind(("step_key", step_key.to_string()))
                .await?;
            let events: Vec<DbStoredEvent> = result.take(0)?;
            Ok(events.into_iter().map(|e| e.into_stored_event()).collect())
        })
        .await
    }

    async fn get_completed_step_keys(&self, run_id: &str) -> Result<HashSet<String>> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT asset_key FROM events \
                         WHERE run_id = $run_id AND event_type = 'StepSuccess'",
                )
                .bind(("run_id", run_id.to_string()))
                .await?;

            #[derive(SurrealValue, serde::Deserialize)]
            struct Row {
                asset_key: Option<String>,
            }
            let rows: Vec<Row> = result.take(0)?;
            Ok(rows.into_iter().filter_map(|r| r.asset_key).collect())
        })
        .await
    }

    async fn get_step_attempts(
        &self,
        run_id: &str,
    ) -> Result<HashMap<String, crate::storage::StepAttempts>> {
        retry::with_retry(&self.retry_config, || async {
            // One statement per type so each anchors on idx_events_run_type.
            let mut result = self
                .db
                .query(
                    "SELECT asset_key FROM events \
                         WHERE run_id = $run_id AND event_type = 'StepStart'; \
                     SELECT asset_key FROM events \
                         WHERE run_id = $run_id AND event_type = 'StepFailure' \
                         AND partition_key IS NONE; \
                     SELECT asset_key FROM events \
                         WHERE run_id = $run_id AND event_type = 'StepRetry'; \
                     SELECT asset_key, partition_key FROM events \
                         WHERE run_id = $run_id AND event_type = 'StepFailure' \
                         AND partition_key IS NOT NONE;",
                )
                .bind(("run_id", run_id.to_string()))
                .await?;

            #[derive(SurrealValue, serde::Deserialize)]
            struct Row {
                asset_key: Option<String>,
            }
            #[derive(SurrealValue)]
            struct KeyedRow {
                asset_key: Option<String>,
                partition_key: PartitionKey,
            }
            let mut out: HashMap<String, crate::storage::StepAttempts> = HashMap::new();
            let started: Vec<Row> = result.take(0)?;
            for key in started.into_iter().filter_map(|r| r.asset_key) {
                out.entry(key).or_default().starts += 1;
            }
            let failed: Vec<Row> = result.take(1)?;
            for key in failed.into_iter().filter_map(|r| r.asset_key) {
                out.entry(key).or_default().failed = true;
            }
            let retries: Vec<Row> = result.take(2)?;
            for key in retries.into_iter().filter_map(|r| r.asset_key) {
                out.entry(key).or_default().retries += 1;
            }
            let keyed: Vec<KeyedRow> = result.take(3)?;
            for row in keyed {
                if let Some(key) = row.asset_key {
                    out.entry(key)
                        .or_default()
                        .failed_keys
                        .extend(row.partition_key.members());
                }
            }
            Ok(out)
        })
        .await
    }

    async fn get_step_data_versions(&self, run_id: &str) -> Result<HashMap<String, String>> {
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT asset_key, data_version FROM events \
                         WHERE run_id = $run_id AND event_type = 'Materialization' \
                           AND data_version IS NOT NULL",
                )
                .bind(("run_id", run_id.to_string()))
                .await?;

            #[derive(SurrealValue, serde::Deserialize)]
            struct Row {
                asset_key: Option<String>,
                data_version: Option<String>,
            }
            let rows: Vec<Row> = result.take(0)?;
            Ok(rows
                .into_iter()
                .filter_map(|r| Some((r.asset_key?, r.data_version?)))
                .collect())
        })
        .await
    }
}
