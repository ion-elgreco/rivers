//! Status changes of runs. A change that a status guards is one `UPDATE`
//! with the statuses it may leave in its `WHERE`: the row lock makes a second
//! writer wait and then test the status the first one left.

use super::*;

/// After `WITH changed AS (UPDATE runs .. RETURNING run_id, status,
/// launched_by)`: record the outcome of the run when a backfill planned it.
/// In the statement that writes the status, so the count of outcome rows
/// tells when a backfill is complete. A run settled again takes the new
/// outcome. A successful run counts the partitions it failed one by one: the
/// distinct keys of its keyed `StepFailure` events.
macro_rules! record_backfill_run_outcome {
    () => {
        ", outcome AS ( \
             INSERT INTO backfill_run_outcomes \
                 (backfill_id, run_number, outcome, member_count, failed_members) \
             SELECT link.backfill_id, link.run_number, \
                 CASE changed.status WHEN 'Success' THEN 'Completed' \
                     WHEN 'Failure' THEN 'Failed' ELSE 'Canceled' END, \
                 COALESCE(link.member_count, 0), \
                 CASE WHEN changed.status = 'Success' THEN LEAST( \
                     COALESCE(link.member_count, 0), \
                     (SELECT count(DISTINCT e.partition_ord) FROM events e \
                      WHERE e.run_id = changed.run_id AND e.event_type = 'StepFailure' \
                      AND e.partition_ord IS NOT NULL)) \
                 ELSE 0 END \
             FROM changed JOIN backfill_runs link \
                 ON link.backfill_id = changed.launched_by->>'backfill_id' \
                 AND link.run_id = changed.run_id \
             WHERE changed.launched_by->>'kind' = 'backfill' \
                 AND changed.status IN ('Success', 'Failure', 'Canceled') \
                 AND link.run_number IS NOT NULL \
             ON CONFLICT (backfill_id, run_number) DO UPDATE SET \
                 outcome = excluded.outcome, member_count = excluded.member_count, \
                 failed_members = excluded.failed_members) "
    };
}

fn stored_status(row: &PgRow) -> Result<StoredStatus> {
    Ok(StoredStatus {
        status: row.try_get("status")?,
        start_time: row.try_get("start_time")?,
        end_time: row.try_get("end_time")?,
    })
}

impl PostgresStorage {
    /// The status and the times of the run `run_id` as the store holds them now.
    async fn status_of_run(&self, run_id: &str) -> Result<Option<StoredStatus>> {
        let row = sqlx::query("SELECT status, start_time, end_time FROM runs WHERE run_id = $1")
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(stored_status).transpose()
    }

    pub(in crate::storage::postgres_backend) async fn update_run_status(
        &self,
        run_id: &str,
        status: RunStatus,
        end_time: Option<i64>,
    ) -> Result<()> {
        // The failures before the status: a run that reads as failed has them.
        // An earlier end is repaired after the status.
        if status == RunStatus::Failure
            && let Some(run) = self.run_of_failures(run_id).await?
        {
            let at = end_time.or(run.end_time).unwrap_or(run.start_time);
            let ended = (run.status == "Failure").then(|| run.end_time.unwrap_or(run.start_time));
            if ended.is_none_or(|ended| ended < at) {
                self.record_failures_of_run(&run, at).await?;
            }
        }

        let stored = self
            .retry(|| async {
                let row = sqlx::query(concat!(
                    "WITH changed AS (UPDATE runs SET status = $2, \
                         end_time = COALESCE($3, end_time) WHERE run_id = $1 \
                         RETURNING run_id, status, launched_by, start_time, end_time)",
                    record_backfill_run_outcome!(),
                    "SELECT status, start_time, end_time FROM changed"
                ))
                .bind(run_id)
                .bind(status_text(&status))
                .bind(end_time)
                .fetch_optional(&self.pool)
                .await?;
                row.as_ref().map(stored_status).transpose()
            })
            .await?;

        self.repair_failures_after_status(run_id, stored.as_ref())
            .await
    }

    /// Start the run unless it was canceled. A run that started already
    /// starts again and keeps the time it began.
    pub(in crate::storage::postgres_backend) async fn try_start_run(
        &self,
        run_id: &str,
    ) -> Result<bool> {
        let started = sqlx::query(
            "UPDATE runs SET status = 'Started', \
                 execution_start_time = COALESCE(execution_start_time, $2) \
             WHERE run_id = $1 AND status <> 'Canceled' \
             RETURNING status, start_time, end_time",
        )
        .bind(run_id)
        .bind(now_nanos())
        .fetch_optional(&self.pool)
        .await?;
        let stored = match started {
            Some(row) => stored_status(&row)?,
            None => self
                .status_of_run(run_id)
                .await?
                .with_context(|| format!("run {run_id} not found"))?,
        };

        self.repair_failures_after_status(run_id, Some(&stored))
            .await?;
        Ok(stored.status == "Started")
    }

    /// Mark `Failure` only while the run has not ended. `false`: the run
    /// ended already and keeps its status (a late launch error must not
    /// replace a `Canceled` or `Success` written at the same time).
    pub async fn fail_run_if_active(&self, run_id: &str, end_time: i64) -> Result<bool> {
        // The failures before the status: a run that reads as failed has them.
        if let Some(run) = self.run_of_failures(run_id).await?
            && matches!(run.status.as_str(), "NotStarted" | "Queued" | "Started")
        {
            self.record_failures_of_run(&run, end_time).await?;
        }

        let failed = self
            .retry(|| async {
                let row = sqlx::query(concat!(
                    "WITH changed AS (UPDATE runs SET status = 'Failure', end_time = $2 \
                         WHERE run_id = $1 AND status IN ('NotStarted', 'Queued', 'Started') \
                         RETURNING run_id, status, launched_by, start_time, end_time)",
                    record_backfill_run_outcome!(),
                    "SELECT status, start_time, end_time FROM changed"
                ))
                .bind(run_id)
                .bind(end_time)
                .fetch_optional(&self.pool)
                .await
                .context("failed to fail-out run")?;
                row.as_ref().map(stored_status).transpose()
            })
            .await?;
        let was_active = failed.is_some();

        // The run may have ended otherwise in the meantime.
        let stored = match failed {
            Some(stored) => Some(stored),
            None => self.status_of_run(run_id).await?,
        };
        self.repair_failures_after_status(run_id, stored.as_ref())
            .await?;
        Ok(was_active)
    }

    /// Cancel the run while it waits in the queue or has not launched yet.
    /// `true` when the run is canceled afterwards, also by an earlier cancel.
    pub(in crate::storage::postgres_backend) async fn cancel_queued_run(
        &self,
        run_id: &str,
    ) -> Result<bool> {
        let canceled: bool = self
            .retry(|| async {
                Ok(sqlx::query_scalar(concat!(
                    "WITH changed AS (UPDATE runs SET status = 'Canceled', end_time = $2 \
                         WHERE run_id = $1 AND status IN ('Queued', 'NotStarted') \
                         RETURNING run_id, status, launched_by), \
                     waiting_steps AS (DELETE FROM pending_steps WHERE run_id = $1)",
                    record_backfill_run_outcome!(),
                    "SELECT count(*) > 0 FROM changed"
                ))
                .bind(run_id)
                .bind(now_nanos())
                .fetch_one(&self.pool)
                .await?)
            })
            .await?;
        if canceled {
            return Ok(true);
        }
        Ok(self
            .status_of_run(run_id)
            .await?
            .is_some_and(|stored| stored.status == "Canceled"))
    }

    pub(in crate::storage::postgres_backend) async fn update_run_block_reason(
        &self,
        run_id: &str,
        reason: Option<&str>,
    ) -> Result<()> {
        // The coordinator sends the reason on each of its passes: a row
        // that has it is not written again.
        sqlx::query(
            "UPDATE runs SET block_reason = $2 \
             WHERE run_id = $1 AND block_reason IS DISTINCT FROM $2",
        )
        .bind(run_id)
        .bind(reason)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(in crate::storage::postgres_backend) async fn set_block_reason_by_status(
        &self,
        code_location_id: &str,
        status: RunStatus,
        reason: Option<&str>,
    ) -> Result<()> {
        // Many rows: two such statements can lock them in another order.
        self.retry(|| async {
            sqlx::query(
                "UPDATE runs SET block_reason = $3 \
                 WHERE code_location_id = $1 AND status = $2 \
                    AND block_reason IS DISTINCT FROM $3",
            )
            .bind(code_location_id)
            .bind(status_text(&status))
            .bind(reason)
            .execute(&self.pool)
            .await?;
            Ok(())
        })
        .await
    }
}
