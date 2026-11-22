//! The runs of a backfill: the link of each run with its run number, and one
//! outcome row per planned run. The run statuses write the outcome rows of
//! the runs that ended (`runs/status.rs`).

use super::super::runs::insert_queued_runs;
use super::*;

/// Most links or outcome rows written in one statement.
pub(super) const ROWS_PER_WRITE: usize = 1000;

/// Partitions the key of `run` names.
fn member_count(run: &RunRecord) -> i64 {
    run.partition_key
        .as_ref()
        .map_or(0, |key| key.member_count() as i64)
}

impl PostgresStorage {
    pub(in crate::storage::postgres_backend) async fn settle_unstarted_backfill_runs(
        &self,
        backfill_id: &str,
        runs: &[UnstartedBackfillRun],
    ) -> Result<()> {
        if runs.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await?;
        // `resume_stalled_backfill` waits for these rows.
        share_backfill(&mut tx, backfill_id).await?;
        for chunk in runs.chunks(ROWS_PER_WRITE) {
            let run_numbers: Vec<i64> = chunk.iter().map(|run| run.run_number as i64).collect();
            let outcomes: Vec<&str> = chunk.iter().map(|run| run.outcome.as_str()).collect();
            let member_counts: Vec<i64> = chunk.iter().map(|run| run.member_count as i64).collect();
            sqlx::query(
                "INSERT INTO backfill_run_outcomes \
                     (backfill_id, run_number, outcome, member_count, failed_members) \
                 SELECT $1, run_number, outcome, member_count, 0 \
                 FROM UNNEST($2::bigint[], $3::text[], $4::bigint[]) \
                     AS run(run_number, outcome, member_count) \
                 ON CONFLICT (backfill_id, run_number) DO NOTHING",
            )
            .bind(backfill_id)
            .bind(&run_numbers)
            .bind(&outcomes)
            .bind(&member_counts)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub(in crate::storage::postgres_backend) async fn get_ended_backfill_runs(
        &self,
        backfill_id: &str,
        first: usize,
        last: usize,
    ) -> Result<Vec<EndedBackfillRun>> {
        let rows = sqlx::query(
            "SELECT outcome.run_number, outcome.outcome, outcome.member_count, \
                 outcome.failed_members, link.run_id \
             FROM backfill_run_outcomes outcome LEFT JOIN backfill_runs link \
                 ON link.backfill_id = outcome.backfill_id \
                 AND link.run_number = outcome.run_number \
             WHERE outcome.backfill_id = $1 AND outcome.run_number BETWEEN $2 AND $3 \
             ORDER BY outcome.run_number",
        )
        .bind(backfill_id)
        .bind(i64::try_from(first).unwrap_or(i64::MAX))
        .bind(i64::try_from(last).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(EndedBackfillRun {
                    run_number: row.try_get::<i64, _>("run_number")? as usize,
                    run_id: row.try_get("run_id")?,
                    outcome: BackfillRunOutcome::parse(row.try_get("outcome")?)?,
                    member_count: row.try_get::<i64, _>("member_count")? as usize,
                    failed_members: row.try_get::<i64, _>("failed_members")? as usize,
                })
            })
            .collect()
    }

    /// Outcomes per partition belong to backfills that ended before the
    /// SurrealDB schema kept outcomes per run (V14). A PostgreSQL store
    /// starts at V17, so it has no such backfill.
    pub(in crate::storage::postgres_backend) async fn get_backfill_partition_outcomes(
        &self,
        _backfill_id: &str,
        _partitions: &[PartitionKey],
    ) -> Result<Vec<(PartitionKey, BackfillRunOutcome)>> {
        Ok(Vec::new())
    }

    pub(in crate::storage::postgres_backend) async fn get_run_failed_partitions(
        &self,
        run_id: &str,
    ) -> Result<Vec<PartitionKey>> {
        let keys: Vec<serde_json::Value> = sqlx::query_scalar(
            "SELECT DISTINCT ON (partition_ord) partition_key FROM events \
             WHERE run_id = $1 AND event_type = 'StepFailure' AND partition_ord IS NOT NULL \
             ORDER BY partition_ord",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        keys.into_iter().map(from_json).collect()
    }

    pub(in crate::storage::postgres_backend) async fn get_open_backfill_runs(
        &self,
        backfill_id: &str,
    ) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT link.run_id FROM backfill_runs link WHERE link.backfill_id = $1 \
             AND NOT EXISTS (SELECT 1 FROM backfill_run_outcomes outcome \
                 WHERE outcome.backfill_id = link.backfill_id \
                 AND outcome.run_number = link.run_number) \
             ORDER BY link.run_number",
        )
        .bind(backfill_id)
        .fetch_all(&self.pool)
        .await?)
    }

    /// [`Self::enqueue_runs`] plus linking the runs to the owning backfill,
    /// all in one transaction, so the links can never disagree with the runs
    /// table. `records` are the planned runs of the backfill, in plan order:
    /// the place of a record is its run number. A run number that has a run
    /// already gets no second one.
    /// Returns `false` when the backfill was no longer `InProgress`: the runs
    /// are committed but swept back out of the queue at once, so no child
    /// outlives a cancel.
    pub async fn enqueue_backfill_runs(
        &self,
        records: &[RunRecord],
        backfill_id: &str,
    ) -> Result<bool> {
        if records.is_empty() {
            return Ok(true);
        }
        let now = now_nanos();
        let mut tx = self.pool.begin().await?;
        // A cancel waits for this batch: it comes before the batch, and the
        // status shows it, or after it, and its cascade reaches these runs.
        let status = share_backfill(&mut tx, backfill_id)
            .await?
            .with_context(|| format!("backfill '{backfill_id}' not found"))?;
        for (chunk_index, chunk) in records.chunks(ROWS_PER_WRITE).enumerate() {
            let first = (chunk_index * ROWS_PER_WRITE) as i64;
            let run_ids: Vec<&str> = chunk.iter().map(|run| run.run_id.as_str()).collect();
            let run_numbers: Vec<i64> = (first..first + chunk.len() as i64).collect();
            let linked_at: Vec<i64> = run_numbers.iter().map(|n| now + n).collect();
            let member_counts: Vec<i64> = chunk.iter().map(member_count).collect();
            let linked: HashSet<String> = sqlx::query_scalar(
                "INSERT INTO backfill_runs \
                     (backfill_id, run_id, linked_at, run_number, member_count) \
                 SELECT $1, run_id, linked_at, run_number, member_count \
                 FROM UNNEST($2::text[], $3::bigint[], $4::bigint[], $5::bigint[]) \
                     AS link(run_id, linked_at, run_number, member_count) \
                 ON CONFLICT DO NOTHING RETURNING run_id",
            )
            .bind(backfill_id)
            .bind(&run_ids)
            .bind(&linked_at)
            .bind(&run_numbers)
            .bind(&member_counts)
            .fetch_all(&mut *tx)
            .await
            .context("failed to enqueue backfill runs batch")?
            .into_iter()
            .collect();
            let new_runs: Vec<RunRecord> = chunk
                .iter()
                .filter(|run| linked.contains(&run.run_id))
                .cloned()
                .collect();
            insert_queued_runs(&mut tx, &new_runs).await?;
        }
        tx.commit().await?;

        if status == BackfillStatus::InProgress {
            return Ok(true);
        }
        for record in records {
            self.cancel_queued_run(&record.run_id).await?;
        }
        Ok(false)
    }

    /// Link a run id to a live backfill: `false` when the backfill is no
    /// longer `InProgress` or another run holds the run number, and then the
    /// caller must not create the run. Linked before the run exists, so a
    /// cancel cascade can always reach every child that will ever exist.
    /// `run_number` is the place of the run in the plan and `member_count`
    /// the number of partitions it holds.
    pub async fn link_backfill_run(
        &self,
        backfill_id: &str,
        run_id: &str,
        run_number: usize,
        member_count: usize,
    ) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        // A cancel waits for this link, and its cascade then reaches the run.
        if share_backfill(&mut tx, backfill_id).await? != Some(BackfillStatus::InProgress) {
            return Ok(false);
        }
        sqlx::query(
            "INSERT INTO backfill_runs (backfill_id, run_id, linked_at, run_number, member_count) \
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
        )
        .bind(backfill_id)
        .bind(run_id)
        .bind(now_nanos())
        .bind(run_number as i64)
        .bind(member_count as i64)
        .execute(&mut *tx)
        .await?;
        let linked: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM backfill_runs WHERE backfill_id = $1 AND run_id = $2)",
        )
        .bind(backfill_id)
        .bind(run_id)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(linked)
    }
}
