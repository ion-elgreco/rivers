//! PostgreSQL backend: runs.
//!
//! A run lists in the order of its start time. Runs that start at the same
//! time list by run id, so a list reads the same each time.

use sqlx::postgres::PgRow;

use super::*;

/// The columns of `runs`, named as the fields of `RunRecord`.
macro_rules! run_columns {
    () => {
        "run_id, code_location_id, job_name, status, start_time, end_time, \
         execution_start_time, tags, node_names, priority, partition_key, block_reason, \
         launched_by, action, config"
    };
}

/// Insert the runs of `$1`, a jsonb list of `RunRecord`s. A run that exists
/// keeps its row.
macro_rules! insert_runs_sql {
    () => {
        concat!(
            "INSERT INTO runs (",
            run_columns!(),
            ") SELECT ",
            run_columns!(),
            " FROM jsonb_populate_recordset(NULL::runs, $1) ON CONFLICT (run_id) DO NOTHING"
        )
    };
}

mod progress;
mod queue;
mod status;
#[cfg(test)]
mod tests;

pub(super) const RUN_COLUMNS: &str = run_columns!();

/// A row of [`RUN_COLUMNS`] as its run.
pub(super) fn run_from_row(row: &PgRow) -> Result<RunRecord> {
    Ok(RunRecord {
        run_id: row.try_get("run_id")?,
        code_location_id: row.try_get("code_location_id")?,
        job_name: row.try_get("job_name")?,
        status: parse_status(row.try_get("status")?)?,
        start_time: row.try_get("start_time")?,
        end_time: row.try_get("end_time")?,
        execution_start_time: row.try_get("execution_start_time")?,
        tags: from_json(row.try_get("tags")?)?,
        node_names: row.try_get("node_names")?,
        priority: row.try_get("priority")?,
        partition_key: row
            .try_get::<Option<serde_json::Value>, _>("partition_key")?
            .map(from_json)
            .transpose()?,
        block_reason: row.try_get("block_reason")?,
        launched_by: from_json(row.try_get("launched_by")?)?,
        action: row.try_get("action")?,
        config: row.try_get("config")?,
    })
}

fn runs_from_rows(rows: &[PgRow]) -> Result<Vec<RunRecord>> {
    rows.iter().map(run_from_row).collect()
}

/// The text of `status` in the `status` column.
pub(super) fn status_text(status: &RunStatus) -> &'static str {
    match status {
        RunStatus::Queued => "Queued",
        RunStatus::NotStarted => "NotStarted",
        RunStatus::Started => "Started",
        RunStatus::Success => "Success",
        RunStatus::Failure => "Failure",
        RunStatus::Canceled => "Canceled",
    }
}

pub(super) fn parse_status(text: &str) -> Result<RunStatus> {
    Ok(match text {
        "Queued" => RunStatus::Queued,
        "NotStarted" => RunStatus::NotStarted,
        "Started" => RunStatus::Started,
        "Success" => RunStatus::Success,
        "Failure" => RunStatus::Failure,
        "Canceled" => RunStatus::Canceled,
        other => anyhow::bail!("unknown RunStatus: {other}"),
    })
}

/// `runs` as the jsonb list that `insert_runs_sql!` reads, each partition
/// key in its stored form.
fn runs_json(runs: &[RunRecord]) -> Result<serde_json::Value> {
    runs.iter()
        .map(|run| {
            let mut value = to_json(run)?;
            if let Some(key) = &run.partition_key {
                value["partition_key"] = key_json(key)?;
            }
            Ok(value)
        })
        .collect()
}

/// Insert `runs` in one statement. A run that exists keeps its row: a
/// second create of a run changes nothing.
pub(super) async fn insert_runs(conn: &mut sqlx::PgConnection, runs: &[RunRecord]) -> Result<()> {
    if runs.is_empty() {
        return Ok(());
    }
    sqlx::query(insert_runs_sql!())
        .bind(runs_json(runs)?)
        .execute(conn)
        .await
        .context("failed to create runs")?;
    Ok(())
}

/// Insert the queued runs `records` and the `RunQueued` event of each in one
/// statement. A run that exists keeps its row and gets no second event.
pub(super) async fn insert_queued_runs(
    conn: &mut sqlx::PgConnection,
    records: &[RunRecord],
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    let mut event_ids = Vec::with_capacity(records.len());
    let mut run_ids = Vec::with_capacity(records.len());
    let mut keys = Vec::with_capacity(records.len());
    let mut ords = Vec::with_capacity(records.len());
    let mut metadata = Vec::with_capacity(records.len());
    for record in records {
        event_ids.push(uuid::Uuid::new_v4().simple().to_string());
        run_ids.push(record.run_id.as_str());
        keys.push(record.partition_key.as_ref().map(key_json).transpose()?);
        ords.push(record.partition_key.as_ref().map(partition_ord));
        metadata.push(to_json(&[(
            tag_keys::PRIORITY,
            record.priority.to_string(),
        )])?);
    }
    sqlx::query(concat!(
        "WITH new_runs AS (",
        insert_runs_sql!(),
        " RETURNING run_id, code_location_id, start_time) \
         INSERT INTO events (id, code_location_id, event_type, run_id, partition_key, \
             partition_ord, timestamp, sort_order, metadata) \
         SELECT queued.id, new_runs.code_location_id, $2, queued.run_id, \
             queued.partition_key, queued.partition_ord, new_runs.start_time, $3, \
             queued.metadata \
         FROM UNNEST($4::text[], $5::text[], $6::jsonb[], $7::bytea[], $8::jsonb[]) \
             AS queued(id, run_id, partition_key, partition_ord, metadata) \
         JOIN new_runs USING (run_id)"
    ))
    .bind(runs_json(records)?)
    .bind(EventType::RunQueued.type_name())
    .bind(EventType::RunQueued.sort_order())
    .bind(&event_ids)
    .bind(&run_ids)
    .bind(&keys)
    .bind(&ords)
    .bind(&metadata)
    .execute(conn)
    .await
    .context("failed to enqueue runs")?;
    Ok(())
}

impl PostgresStorage {
    pub(super) async fn create_run(&self, run: &RunRecord) -> Result<()> {
        self.create_runs(std::slice::from_ref(run)).await
    }

    pub(super) async fn create_runs(&self, runs: &[RunRecord]) -> Result<()> {
        insert_runs(&mut *self.pool.acquire().await?, runs).await?;
        self.record_failures_of_created(runs).await
    }

    pub(super) async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>> {
        let row = sqlx::query(concat!(
            "SELECT ",
            run_columns!(),
            " FROM runs WHERE run_id = $1"
        ))
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(run_from_row).transpose()
    }

    pub(super) async fn get_runs_by_ids(
        &self,
        run_ids: &[String],
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        if run_ids.is_empty() {
            return Ok(Vec::new());
        }
        let query = match &status {
            Some(status) => sqlx::query(concat!(
                "SELECT ",
                run_columns!(),
                " FROM runs WHERE run_id = ANY($1) AND status = $2 \
                 ORDER BY start_time, run_id COLLATE \"C\""
            ))
            .bind(run_ids)
            .bind(status_text(status)),
            None => sqlx::query(concat!(
                "SELECT ",
                run_columns!(),
                " FROM runs WHERE run_id = ANY($1) ORDER BY start_time, run_id COLLATE \"C\""
            ))
            .bind(run_ids),
        };
        runs_from_rows(&query.fetch_all(&self.pool).await?)
    }

    pub(super) async fn get_runs(
        &self,
        code_location_id: &str,
        limit: usize,
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        let query = match &status {
            Some(status) => sqlx::query(concat!(
                "SELECT ",
                run_columns!(),
                " FROM runs WHERE code_location_id = $1 AND status = $3 \
                 ORDER BY start_time DESC, run_id COLLATE \"C\" DESC LIMIT $2"
            ))
            .bind(code_location_id)
            .bind(limit as i64)
            .bind(status_text(status)),
            None => sqlx::query(concat!(
                "SELECT ",
                run_columns!(),
                " FROM runs WHERE code_location_id = $1 \
                 ORDER BY start_time DESC, run_id COLLATE \"C\" DESC LIMIT $2"
            ))
            .bind(code_location_id)
            .bind(limit as i64),
        };
        runs_from_rows(&query.fetch_all(&self.pool).await?)
    }

    pub(super) async fn get_all_runs(
        &self,
        limit: usize,
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        let query = match &status {
            Some(status) => sqlx::query(concat!(
                "SELECT ",
                run_columns!(),
                " FROM runs WHERE status = $2 \
                 ORDER BY start_time DESC, run_id COLLATE \"C\" DESC LIMIT $1"
            ))
            .bind(limit as i64)
            .bind(status_text(status)),
            None => sqlx::query(concat!(
                "SELECT ",
                run_columns!(),
                " FROM runs ORDER BY start_time DESC, run_id COLLATE \"C\" DESC LIMIT $1"
            ))
            .bind(limit as i64),
        };
        runs_from_rows(&query.fetch_all(&self.pool).await?)
    }

    pub(super) async fn get_runs_since(
        &self,
        code_location_id: &str,
        since_timestamp: i64,
        status: Option<RunStatus>,
        order: SortOrder,
    ) -> Result<Vec<RunRecord>> {
        let status_filter = if status.is_some() {
            " AND status = $3"
        } else {
            ""
        };
        let direction = order.as_sql();
        let sql = format!(
            "SELECT {RUN_COLUMNS} FROM runs \
             WHERE code_location_id = $1 AND start_time > $2{status_filter} \
             ORDER BY start_time {direction}, run_id COLLATE \"C\" {direction}"
        );
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(code_location_id)
            .bind(since_timestamp);
        if let Some(status) = &status {
            query = query.bind(status_text(status));
        }
        runs_from_rows(&query.fetch_all(&self.pool).await?)
    }

    pub(super) async fn get_all_runs_since(
        &self,
        since_timestamp: i64,
        status: Option<RunStatus>,
    ) -> Result<Vec<RunRecord>> {
        let query = match &status {
            Some(status) => sqlx::query(concat!(
                "SELECT ",
                run_columns!(),
                " FROM runs WHERE start_time > $1 AND status = $2 \
                 ORDER BY start_time DESC, run_id COLLATE \"C\" DESC"
            ))
            .bind(since_timestamp)
            .bind(status_text(status)),
            None => sqlx::query(concat!(
                "SELECT ",
                run_columns!(),
                " FROM runs WHERE start_time > $1 \
                 ORDER BY start_time DESC, run_id COLLATE \"C\" DESC"
            ))
            .bind(since_timestamp),
        };
        runs_from_rows(&query.fetch_all(&self.pool).await?)
    }

    /// Oldest end first. A run without an end time never matches.
    pub(super) async fn get_runs_ended_since(
        &self,
        code_location_id: &str,
        since: i64,
        status: RunStatus,
        job_names: Option<&[String]>,
        limit: usize,
    ) -> Result<Vec<RunRecord>> {
        let query = match job_names {
            Some(job_names) => sqlx::query(concat!(
                "SELECT ",
                run_columns!(),
                " FROM runs WHERE code_location_id = $1 AND status = $2 AND end_time >= $3 \
                 AND job_name = ANY($5) ORDER BY end_time, run_id COLLATE \"C\" LIMIT $4"
            ))
            .bind(code_location_id)
            .bind(status_text(&status))
            .bind(since)
            .bind(limit as i64)
            .bind(job_names),
            None => sqlx::query(concat!(
                "SELECT ",
                run_columns!(),
                " FROM runs WHERE code_location_id = $1 AND status = $2 AND end_time >= $3 \
                 ORDER BY end_time, run_id COLLATE \"C\" LIMIT $4"
            ))
            .bind(code_location_id)
            .bind(status_text(&status))
            .bind(since)
            .bind(limit as i64),
        };
        runs_from_rows(&query.fetch_all(&self.pool).await?)
    }

    pub(super) async fn get_queued_runs(&self, code_location_id: &str) -> Result<Vec<RunRecord>> {
        let rows = sqlx::query(concat!(
            "SELECT ",
            run_columns!(),
            " FROM runs WHERE code_location_id = $1 AND status = 'Queued'"
        ))
        .bind(code_location_id)
        .fetch_all(&self.pool)
        .await?;
        runs_from_rows(&rows)
    }

    /// The runs that wait in the queue and those dequeued but not launched yet.
    pub(super) async fn get_all_queued_runs(&self) -> Result<Vec<RunRecord>> {
        let rows = sqlx::query(concat!(
            "SELECT ",
            run_columns!(),
            " FROM runs WHERE status IN ('Queued', 'NotStarted')"
        ))
        .fetch_all(&self.pool)
        .await?;
        runs_from_rows(&rows)
    }

    pub(super) async fn count_in_progress_runs(&self) -> Result<usize> {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM runs WHERE status IN ('NotStarted', 'Started')",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(count as usize)
    }

    pub(super) async fn get_in_progress_runs(&self) -> Result<Vec<RunRecord>> {
        let rows = sqlx::query(concat!(
            "SELECT ",
            run_columns!(),
            " FROM runs WHERE status IN ('NotStarted', 'Started')"
        ))
        .fetch_all(&self.pool)
        .await?;
        runs_from_rows(&rows)
    }

    pub(super) async fn delete_run(&self, run_id: &str) -> Result<bool> {
        // Check-then-delete is safe: a run that ended stays ended (a new
        // execution is a new run), so the coordinator cannot pick it up after
        // the check. The runs row goes last, so a deletion that fails part
        // way can run again.
        let Some(run) = self.get_run(run_id).await? else {
            return Ok(false);
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
        self.retry(|| async {
            sqlx::query(
                "WITH run_events AS (DELETE FROM events WHERE run_id = $1), \
                 run_log_rows AS (DELETE FROM run_logs WHERE run_id = $1), \
                 run_slots AS (DELETE FROM concurrency_slots WHERE run_id = $1), \
                 run_steps AS (DELETE FROM pending_steps WHERE run_id = $1) \
                 DELETE FROM run_cancellations WHERE run_id = $1",
            )
            .bind(run_id)
            .execute(&self.pool)
            .await?;
            Ok(())
        })
        .await?;
        // The failures of the run go with it; older failures of its
        // partitions come back.
        self.repair_failures_of_run(run_id, RunChange::Deleted)
            .await?;
        sqlx::query("DELETE FROM runs WHERE run_id = $1")
            .bind(run_id)
            .execute(&self.pool)
            .await?;
        Ok(true)
    }
}
