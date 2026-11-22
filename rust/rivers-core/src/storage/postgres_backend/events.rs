//! PostgreSQL backend: event reads and run logs.

use sqlx::postgres::PgRow;
use surrealdb::types::RecordId;

use super::event_store::storable;
use super::*;

#[cfg(test)]
mod tests;

/// The columns that [`event_from_row`] reads. A macro, so that the queries
/// of this module can `concat!` the list.
macro_rules! event_columns {
    () => {
        "id, event_type, asset_key, run_id, partition_key, timestamp, metadata, \
         data_version, code_version, input_data_versions"
    };
}

pub(super) const EVENT_COLUMNS: &str = event_columns!();

/// The order of the events of a run or of a step, oldest first.
macro_rules! oldest_first {
    () => {
        " ORDER BY timestamp, sort_order, id COLLATE \"C\""
    };
}

/// The order of the events of an asset, newest first.
macro_rules! newest_first {
    () => {
        " ORDER BY timestamp DESC, sort_order DESC, id COLLATE \"C\" DESC"
    };
}

pub(super) fn event_from_row(row: &PgRow) -> Result<StoredEvent> {
    let id: String = row.try_get("id")?;
    let event_type: String = row.try_get("event_type")?;
    let partition_key: Option<serde_json::Value> = row.try_get("partition_key")?;
    Ok(StoredEvent {
        id: RecordId::new("events", id),
        // As on SurrealDB: a type this build does not know reads as a failure.
        event_type: EventType::from_type_name(&event_type, row.try_get("data_version")?)
            .unwrap_or(EventType::StepFailure),
        asset_key: row.try_get("asset_key")?,
        run_id: row.try_get("run_id")?,
        partition_key: partition_key.map(from_json).transpose()?,
        timestamp: row.try_get("timestamp")?,
        metadata: from_json(row.try_get("metadata")?)?,
        code_version: row.try_get("code_version")?,
        input_data_versions: from_json(row.try_get("input_data_versions")?)?,
    })
}

/// A `LIMIT` bind. A limit past `i64::MAX` is no limit.
fn limit_bind(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}

fn events_from_rows(rows: Vec<PgRow>) -> Result<Vec<StoredEvent>> {
    rows.iter().map(event_from_row).collect()
}

impl PostgresStorage {
    pub(super) async fn get_events_for_asset(
        &self,
        code_location_id: &str,
        asset_key: &str,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        let rows = sqlx::query(concat!(
            "SELECT ",
            event_columns!(),
            " FROM events WHERE code_location_id = $1 AND asset_key = $2",
            newest_first!(),
            " LIMIT $3"
        ))
        .bind(code_location_id)
        .bind(asset_key)
        .bind(limit_bind(limit))
        .fetch_all(&self.pool)
        .await?;
        events_from_rows(rows)
    }

    pub(super) async fn get_latest_materialization(
        &self,
        code_location_id: &str,
        asset_key: &str,
        partition: Option<&str>,
    ) -> Result<Option<StoredEvent>> {
        let Some(partition) = partition else {
            let row = sqlx::query(concat!(
                "SELECT ",
                event_columns!(),
                " FROM events WHERE code_location_id = $1 AND asset_key = $2 \
                 AND event_type = 'Materialization'",
                newest_first!(),
                " LIMIT 1"
            ))
            .bind(code_location_id)
            .bind(asset_key)
            .fetch_optional(&self.pool)
            .await?;
            return row.as_ref().map(event_from_row).transpose();
        };
        // The structured reading of a display string wins over the plain one.
        for candidate in PartitionKey::display_candidates(partition).iter().rev() {
            let row = sqlx::query(concat!(
                "SELECT ",
                event_columns!(),
                " FROM events WHERE code_location_id = $1 AND asset_key = $2 \
                 AND md5(partition_ord)::uuid = md5($3)::uuid AND partition_ord = $3 \
                 AND event_type = 'Materialization'",
                newest_first!(),
                " LIMIT 1"
            ))
            .bind(code_location_id)
            .bind(asset_key)
            .bind(partition_ord(candidate))
            .fetch_optional(&self.pool)
            .await?;
            if let Some(row) = row {
                return event_from_row(&row).map(Some);
            }
        }
        Ok(None)
    }

    pub(super) async fn get_partition_events(
        &self,
        code_location_id: &str,
        asset_key: &str,
        partition_key: &str,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        for candidate in PartitionKey::display_candidates(partition_key).iter().rev() {
            let rows = sqlx::query(concat!(
                "SELECT ",
                event_columns!(),
                " FROM events WHERE code_location_id = $1 AND asset_key = $2 \
                 AND md5(partition_ord)::uuid = md5($3)::uuid AND partition_ord = $3",
                newest_first!(),
                " LIMIT $4"
            ))
            .bind(code_location_id)
            .bind(asset_key)
            .bind(partition_ord(candidate))
            .bind(limit_bind(limit))
            .fetch_all(&self.pool)
            .await?;
            if !rows.is_empty() {
                return events_from_rows(rows);
            }
        }
        Ok(Vec::new())
    }

    pub(super) async fn store_run_logs(&self, logs: &[LogRecord]) -> Result<()> {
        if logs.is_empty() {
            return Ok(());
        }
        let text = |f: fn(&LogRecord) -> &str| logs.iter().map(f).map(storable).collect::<Vec<_>>();
        let maybe = |f: fn(&LogRecord) -> Option<&str>| {
            logs.iter().map(|l| f(l).map(storable)).collect::<Vec<_>>()
        };
        sqlx::query(
            "INSERT INTO run_logs \
             (code_location_id, run_id, step_key, timestamp, stdout, stderr, logs, traceback) \
             SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::bigint[], \
             $5::text[], $6::text[], $7::text[], $8::text[])",
        )
        .bind(text(|l| &l.code_location_id))
        .bind(text(|l| &l.run_id))
        .bind(text(|l| &l.step_key))
        .bind(logs.iter().map(|l| l.timestamp).collect::<Vec<i64>>())
        .bind(maybe(|l| l.stdout.as_deref()))
        .bind(maybe(|l| l.stderr.as_deref()))
        .bind(maybe(|l| l.logs.as_deref()))
        .bind(maybe(|l| l.traceback.as_deref()))
        .execute(&self.pool)
        .await
        .context("failed to store run logs")?;
        Ok(())
    }

    pub(super) async fn get_run_logs(&self, run_id: &str) -> Result<Vec<StoredLog>> {
        let rows = sqlx::query(
            "SELECT id, code_location_id, run_id, step_key, timestamp, stdout, stderr, logs, \
             traceback FROM run_logs WHERE run_id = $1 ORDER BY timestamp, id COLLATE \"C\"",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(StoredLog {
                    id: RecordId::new("run_logs", row.try_get::<String, _>("id")?),
                    code_location_id: row.try_get("code_location_id")?,
                    run_id: row.try_get("run_id")?,
                    step_key: row.try_get("step_key")?,
                    timestamp: row.try_get("timestamp")?,
                    stdout: row.try_get("stdout")?,
                    stderr: row.try_get("stderr")?,
                    logs: row.try_get("logs")?,
                    traceback: row.try_get("traceback")?,
                })
            })
            .collect()
    }

    /// In the order of `get_run_logs`.
    pub(super) async fn get_step_tracebacks(
        &self,
        run_id: &str,
        step_key: &str,
        timestamp: i64,
    ) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT traceback FROM run_logs WHERE run_id = $1 AND step_key = $2 \
             AND timestamp = $3 AND traceback IS NOT NULL ORDER BY id COLLATE \"C\"",
        )
        .bind(run_id)
        .bind(step_key)
        .bind(timestamp)
        .fetch_all(&self.pool)
        .await?)
    }

    pub(super) async fn get_events_for_run(&self, run_id: &str) -> Result<Vec<StoredEvent>> {
        let rows = sqlx::query(concat!(
            "SELECT ",
            event_columns!(),
            " FROM events WHERE run_id = $1",
            oldest_first!()
        ))
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        events_from_rows(rows)
    }

    /// The asset keys are matched here, not in the query: a long list in
    /// `= ANY` is compared row by row.
    pub(super) async fn step_outcomes(
        &self,
        asset_keys: &[String],
        run_ids: &[String],
    ) -> Result<Vec<StepOutcome>> {
        if asset_keys.is_empty() || run_ids.is_empty() {
            return Ok(Vec::new());
        }
        let wanted: HashSet<&str> = asset_keys.iter().map(String::as_str).collect();
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT asset_key, run_id, event_type FROM events \
             WHERE run_id = ANY($1) AND event_type IN ('StepSuccess', 'StepFailure') \
             AND asset_key IS NOT NULL",
        )
        .bind(run_ids)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .filter(|(asset_key, _, _)| wanted.contains(asset_key.as_str()))
            .map(|(asset_key, run_id, event_type)| StepOutcome {
                asset_key,
                run_id,
                succeeded: event_type == EventType::StepSuccess.type_name(),
            })
            .collect())
    }

    /// The asset keys are matched here, as in `step_outcomes`.
    pub(super) async fn materialized_by_runs(
        &self,
        asset_keys: &[String],
        run_ids: &[String],
    ) -> Result<HashMap<String, HashSet<String>>> {
        if asset_keys.is_empty() || run_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let wanted: HashSet<&str> = asset_keys.iter().map(String::as_str).collect();
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT DISTINCT asset_key, run_id FROM events \
             WHERE run_id = ANY($1) AND event_type = 'Materialization' \
             AND asset_key IS NOT NULL",
        )
        .bind(run_ids)
        .fetch_all(&self.pool)
        .await?;
        let mut out: HashMap<String, HashSet<String>> = HashMap::new();
        for (asset_key, run_id) in rows {
            if wanted.contains(asset_key.as_str()) {
                out.entry(asset_key).or_default().insert(run_id);
            }
        }
        Ok(out)
    }

    pub(super) async fn get_observations_since(
        &self,
        code_location_id: &str,
        since_timestamp: i64,
    ) -> Result<Vec<StoredEvent>> {
        let rows = sqlx::query(concat!(
            "SELECT ",
            event_columns!(),
            " FROM events WHERE event_type = 'Observation' AND code_location_id = $1 \
             AND timestamp > $2",
            newest_first!()
        ))
        .bind(code_location_id)
        .bind(since_timestamp)
        .fetch_all(&self.pool)
        .await?;
        events_from_rows(rows)
    }

    pub(super) async fn get_latest_observation_ts(
        &self,
        code_location_id: &str,
    ) -> Result<Option<i64>> {
        Ok(sqlx::query_scalar(
            "SELECT max(timestamp) FROM events \
             WHERE event_type = 'Observation' AND code_location_id = $1",
        )
        .bind(code_location_id)
        .fetch_one(&self.pool)
        .await?)
    }

    pub(super) async fn get_events_for_step(
        &self,
        run_id: &str,
        step_key: &str,
    ) -> Result<Vec<StoredEvent>> {
        let rows = sqlx::query(concat!(
            "SELECT ",
            event_columns!(),
            " FROM events WHERE run_id = $1 AND asset_key = $2",
            oldest_first!()
        ))
        .bind(run_id)
        .bind(step_key)
        .fetch_all(&self.pool)
        .await?;
        events_from_rows(rows)
    }

    pub(super) async fn get_step_terminal_events(
        &self,
        run_id: &str,
        step_key: &str,
    ) -> Result<Vec<StoredEvent>> {
        let rows = sqlx::query(concat!(
            "SELECT ",
            event_columns!(),
            " FROM events WHERE run_id = $1 AND asset_key = $2 \
             AND event_type IN ('StepSuccess', 'StepFailure')",
            oldest_first!()
        ))
        .bind(run_id)
        .bind(step_key)
        .fetch_all(&self.pool)
        .await?;
        events_from_rows(rows)
    }

    pub(super) async fn get_run_failure_events(&self, run_id: &str) -> Result<Vec<StoredEvent>> {
        let rows = sqlx::query(concat!(
            "SELECT ",
            event_columns!(),
            " FROM events WHERE run_id = $1 \
             AND event_type IN ('StepFailure', 'RunLaunchFailed')",
            oldest_first!()
        ))
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        events_from_rows(rows)
    }

    pub(super) async fn get_completed_step_keys(&self, run_id: &str) -> Result<HashSet<String>> {
        let keys: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT asset_key FROM events \
             WHERE run_id = $1 AND event_type = 'StepSuccess' AND asset_key IS NOT NULL",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(keys.into_iter().collect())
    }

    pub(super) async fn get_step_attempts(
        &self,
        run_id: &str,
    ) -> Result<HashMap<String, StepAttempts>> {
        let rows: Vec<(String, String, Option<serde_json::Value>)> = sqlx::query_as(concat!(
            "SELECT asset_key, event_type, partition_key FROM events \
             WHERE run_id = $1 AND event_type IN ('StepStart', 'StepFailure', 'StepRetry') \
             AND asset_key IS NOT NULL",
            oldest_first!()
        ))
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        let mut out: HashMap<String, StepAttempts> = HashMap::new();
        for (asset_key, event_type, partition_key) in rows {
            let attempts = out.entry(asset_key).or_default();
            match (event_type.as_str(), partition_key) {
                ("StepStart", _) => attempts.starts += 1,
                ("StepRetry", _) => attempts.retries += 1,
                // A keyed failure is one partition of a batch, not the step.
                (_, Some(key)) => attempts
                    .failed_keys
                    .extend(from_json::<PartitionKey>(key)?.members()),
                (_, None) => attempts.failed = true,
            }
        }
        Ok(out)
    }

    /// With more than one materialization of an asset in the run, the newest
    /// wins.
    pub(super) async fn get_step_data_versions(
        &self,
        run_id: &str,
    ) -> Result<HashMap<String, String>> {
        let rows: Vec<(String, String)> = sqlx::query_as(concat!(
            "SELECT asset_key, data_version FROM events \
             WHERE run_id = $1 AND event_type = 'Materialization' \
             AND asset_key IS NOT NULL AND data_version IS NOT NULL",
            oldest_first!()
        ))
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().collect())
    }
}
