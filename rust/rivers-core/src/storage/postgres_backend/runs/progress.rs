//! What the executor and the operator of a run tell each other: the progress
//! of its steps, its final outcome, and a request to cancel it. An outcome or
//! a cancellation can be written before the run row exists.

use super::*;

impl PostgresStorage {
    /// Distinct steps, not events: a retried step writes a `StepStart` and a
    /// `StepFailure` per attempt and counts once.
    pub(in crate::storage::postgres_backend) async fn get_run_progress(
        &self,
        run_id: &str,
    ) -> Result<RunProgress> {
        let row = sqlx::query(
            "WITH ended AS (SELECT asset_key, timestamp, sort_order, id FROM events \
                 WHERE run_id = $1 AND (event_type = 'StepSuccess' \
                     OR (event_type = 'StepFailure' AND partition_key IS NULL))), \
             last AS (SELECT asset_key, timestamp FROM ended \
                 ORDER BY timestamp DESC, sort_order DESC, id COLLATE \"C\" DESC LIMIT 1) \
             SELECT \
                 (SELECT count(DISTINCT asset_key) FROM events \
                     WHERE run_id = $1 AND event_type = 'StepStart') AS started, \
                 (SELECT count(DISTINCT asset_key) FROM ended) AS completed, \
                 (SELECT asset_key FROM last) AS last_step, \
                 (SELECT timestamp FROM last) AS last_at",
        )
        .bind(run_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(RunProgress {
            completed_steps: row.try_get::<i64, _>("completed")? as u32,
            total_steps: row.try_get::<i64, _>("started")? as u32,
            last_step_completed_at: row.try_get("last_at")?,
            last_completed_step: row.try_get("last_step")?,
        })
    }

    pub(in crate::storage::postgres_backend) async fn get_run_outcome(
        &self,
        run_id: &str,
    ) -> Result<Option<RunOutcome>> {
        let outcome: Option<serde_json::Value> =
            sqlx::query_scalar("SELECT outcome FROM run_outcomes WHERE run_id = $1")
                .bind(run_id)
                .fetch_optional(&self.pool)
                .await?;
        outcome.map(from_json).transpose()
    }

    pub(in crate::storage::postgres_backend) async fn set_run_outcome(
        &self,
        run_id: &str,
        outcome: &RunOutcome,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO run_outcomes (run_id, outcome) VALUES ($1, $2) \
             ON CONFLICT (run_id) DO UPDATE SET outcome = excluded.outcome",
        )
        .bind(run_id)
        .bind(to_json(outcome)?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(in crate::storage::postgres_backend) async fn request_cancellation(
        &self,
        run_id: &str,
    ) -> Result<()> {
        sqlx::query("INSERT INTO run_cancellations (run_id) VALUES ($1) ON CONFLICT DO NOTHING")
            .bind(run_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub(in crate::storage::postgres_backend) async fn is_cancelled(
        &self,
        run_id: &str,
    ) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM run_cancellations WHERE run_id = $1)")
                .bind(run_id)
                .fetch_one(&self.pool)
                .await?,
        )
    }
}
