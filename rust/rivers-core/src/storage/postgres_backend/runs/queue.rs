//! The run queue: queued runs with their `RunQueued` events, the poll of the
//! coordinator, and the runs that were dequeued but never launched.

use super::*;

impl PostgresStorage {
    /// Write a queued run and its `RunQueued` event in one statement.
    pub async fn enqueue_run(&self, record: &RunRecord) -> Result<()> {
        self.enqueue_runs(std::slice::from_ref(record)).await
    }

    /// Batch counterpart to [`Self::enqueue_run`].
    pub async fn enqueue_runs(&self, records: &[RunRecord]) -> Result<()> {
        insert_queued_runs(&mut *self.pool.acquire().await?, records).await
    }

    /// Free the expired slot leases of every code location, then read the
    /// runs of `code_location_id` in progress and in the queue. The count is
    /// of the leases freed.
    pub(in crate::storage::postgres_backend) async fn coordinator_tick_query(
        &self,
        code_location_id: &str,
    ) -> Result<(u32, Vec<CoordinatorRunInfo>, Vec<CoordinatorRunInfo>)> {
        let now = now_nanos();
        // `free_expired_leases` can delete the same rows at the same time, in
        // another order.
        let expired: i64 = self
            .retry(|| async {
                Ok(sqlx::query_scalar(
                    "WITH expired AS (DELETE FROM concurrency_slots \
                         WHERE lease_expires_at <= $1 RETURNING 1) \
                     SELECT count(*) FROM expired",
                )
                .bind(now)
                .fetch_one(&self.pool)
                .await?)
            })
            .await?;

        let rows = sqlx::query(
            "SELECT run_id, code_location_id, tags, node_names, job_name, priority, \
                 start_time, action, config, launched_by, status \
             FROM runs WHERE code_location_id = $1 \
             AND status IN ('Queued', 'NotStarted', 'Started')",
        )
        .bind(code_location_id)
        .fetch_all(&self.pool)
        .await?;
        let mut in_progress = Vec::new();
        let mut queued = Vec::new();
        for row in &rows {
            let info = CoordinatorRunInfo {
                run_id: row.try_get("run_id")?,
                code_location_id: row.try_get("code_location_id")?,
                tags: from_json(row.try_get("tags")?)?,
                node_names: row.try_get("node_names")?,
                job_name: row.try_get("job_name")?,
                priority: row.try_get("priority")?,
                partition_key: None,
                start_time: row.try_get("start_time")?,
                action: row.try_get("action")?,
                config: row.try_get("config")?,
                launched_by: from_json(row.try_get("launched_by")?)?,
            };
            if row.try_get::<&str, _>("status")? == "Queued" {
                queued.push(info);
            } else {
                in_progress.push(info);
            }
        }
        Ok((expired as u32, in_progress, queued))
    }

    /// The `NotStarted` runs of `code_location_id` that were dequeued before
    /// `cutoff_ns`. A run without a `RunDequeued` event counts from its start
    /// time.
    pub(in crate::storage::postgres_backend) async fn get_stalled_not_started_runs(
        &self,
        code_location_id: &str,
        cutoff_ns: i64,
    ) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT run_id FROM runs \
             WHERE code_location_id = $1 AND status = 'NotStarted' \
             AND COALESCE((SELECT max(timestamp) FROM events \
                 WHERE events.run_id = runs.run_id AND event_type = 'RunDequeued'), \
                 start_time) < $2",
        )
        .bind(code_location_id)
        .bind(cutoff_ns)
        .fetch_all(&self.pool)
        .await?)
    }
}
