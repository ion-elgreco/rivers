//! The runs of a backfill: the link of each run with its run number, and one
//! outcome row per planned run.

use anyhow::{Context, Result};
use surrealdb::types::SurrealValue;

use crate::storage::retry;
use crate::storage::{
    BackfillRunOutcome, BackfillStatus, EndedBackfillRun, PartitionKey, RunRecord,
    UnstartedBackfillRun,
};

use super::*;

/// Most outcome rows written in one statement.
const OUTCOME_ROWS_PER_WRITE: usize = 1000;

impl SurrealStorage {
    pub(super) async fn record_unstarted_backfill_runs(
        &self,
        backfill_id: &str,
        runs: &[UnstartedBackfillRun],
    ) -> Result<()> {
        for chunk in runs.chunks(OUTCOME_ROWS_PER_WRITE) {
            let outcomes: Vec<BackfillRunOutcomeRow> = chunk
                .iter()
                .map(|run| BackfillRunOutcomeRow {
                    backfill_id: backfill_id.to_string(),
                    run_number: run.run_number as i64,
                    outcome: run.outcome.as_str().to_string(),
                    member_count: run.member_count as i64,
                    failed_members: 0,
                })
                .collect();
            retry::with_retry(&self.retry_config, || async {
                self.db
                    .query(RECORD_UNSTARTED_BACKFILL_RUNS)
                    .bind(("outcomes", outcomes.clone()))
                    .await?
                    .check()?;
                Ok(())
            })
            .await?;
        }
        Ok(())
    }

    pub(super) async fn ended_backfill_runs(
        &self,
        backfill_id: &str,
        first: usize,
        last: usize,
    ) -> Result<Vec<EndedBackfillRun>> {
        #[derive(SurrealValue)]
        struct Link {
            run_id: String,
            run_number: i64,
        }

        let (outcomes, links): (Vec<BackfillRunOutcomeRow>, Vec<Link>) =
            retry::with_retry(&self.retry_config, || async {
                let mut result = self
                    .db
                    .query(
                        "SELECT backfill_id, run_number, outcome, member_count, failed_members \
                             FROM backfill_run_outcomes WHERE backfill_id = $id \
                             AND run_number >= $first AND run_number <= $last ORDER BY run_number;\n\
                         SELECT run_id, run_number FROM backfill_runs WHERE backfill_id = $id \
                             AND run_number >= $first AND run_number <= $last;",
                    )
                    .bind(("id", backfill_id.to_string()))
                    .bind(("first", first as i64))
                    .bind(("last", last as i64))
                    .await?
                    .check()?;
                Ok((result.take(0)?, result.take(1)?))
            })
            .await?;

        let mut run_ids: HashMap<i64, String> = links
            .into_iter()
            .map(|link| (link.run_number, link.run_id))
            .collect();
        outcomes
            .into_iter()
            .map(|row| {
                Ok(EndedBackfillRun {
                    run_number: row.run_number as usize,
                    run_id: run_ids.remove(&row.run_number),
                    outcome: BackfillRunOutcome::parse(&row.outcome)?,
                    member_count: row.member_count as usize,
                    failed_members: row.failed_members as usize,
                })
            })
            .collect()
    }

    pub(super) async fn backfill_partition_outcomes(
        &self,
        backfill_id: &str,
        partitions: &[PartitionKey],
    ) -> Result<Vec<(PartitionKey, BackfillRunOutcome)>> {
        #[derive(SurrealValue)]
        struct Row {
            partition_key: PartitionKey,
            outcome: String,
        }

        let rows: Vec<Row> = self
            .select_by_partitions(
                "SELECT partition_key, outcome FROM backfill_outcomes \
                 WHERE backfill_id = $id AND partition_key = $partition",
                &[("id", backfill_id)],
                partitions,
            )
            .await?;

        rows.into_iter()
            .map(|row| Ok((row.partition_key, BackfillRunOutcome::parse(&row.outcome)?)))
            .collect()
    }

    pub(super) async fn run_failed_partitions(&self, run_id: &str) -> Result<Vec<PartitionKey>> {
        let keys: Vec<PartitionKey> = retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "SELECT VALUE partition_key FROM events WHERE run_id = $run_id \
                     AND event_type = 'StepFailure' AND partition_key IS NOT NONE",
                )
                .bind(("run_id", run_id.to_string()))
                .await?
                .check()?;
            Ok(result.take(0)?)
        })
        .await?;

        let distinct: std::collections::HashSet<PartitionKey> = keys.into_iter().collect();
        Ok(distinct.into_iter().collect())
    }

    pub(super) async fn open_backfill_runs(&self, backfill_id: &str) -> Result<Vec<String>> {
        #[derive(SurrealValue)]
        struct Link {
            run_id: String,
            run_number: i64,
        }

        let (links, ended): (Vec<Link>, Vec<i64>) =
            retry::with_retry(&self.retry_config, || async {
                let mut result = self
                    .db
                    .query(
                        "SELECT run_id, run_number FROM backfill_runs \
                             WHERE backfill_id = $id ORDER BY run_number;\n\
                         SELECT VALUE run_number FROM backfill_run_outcomes \
                             WHERE backfill_id = $id;",
                    )
                    .bind(("id", backfill_id.to_string()))
                    .await?
                    .check()?;
                Ok((result.take(0)?, result.take(1)?))
            })
            .await?;

        let ended: std::collections::HashSet<i64> = ended.into_iter().collect();
        Ok(links
            .into_iter()
            .filter(|link| !ended.contains(&link.run_number))
            .map(|link| link.run_id)
            .collect())
    }

    /// [`Self::enqueue_runs`] plus linking the runs to the owning backfill,
    /// all in one transaction, so the links can never disagree with the runs
    /// table. `records` are the planned runs of the backfill, in plan order:
    /// the place of a record is its run number.
    /// Returns `false` when the backfill was canceled while the batch was in
    /// flight — the runs are committed but immediately swept back out of the
    /// queue, so no child outlives the cancel regardless of which side of
    /// the race committed first.
    pub async fn enqueue_backfill_runs(
        &self,
        records: &[RunRecord],
        backfill_id: &str,
    ) -> Result<bool> {
        if records.is_empty() {
            return Ok(true);
        }
        let events: Vec<DbEventWrite> = records
            .iter()
            .map(|r| DbEventWrite::from_event(&run_queued_event(r), new_event_record_id()))
            .collect();
        let now = now_nanos();
        let links: Vec<BackfillRunRow> = records
            .iter()
            .enumerate()
            .map(|(run_number, record)| BackfillRunRow {
                backfill_id: backfill_id.to_string(),
                run_id: record.run_id.clone(),
                linked_at: now + run_number as i64,
                run_number: run_number as i64,
                member_count: member_count(record),
            })
            .collect();

        let result = retry::with_retry(&self.retry_config, || async {
            self.db
                .query(format!(
                    "BEGIN TRANSACTION;\n\
                     INSERT INTO runs $runs;\n\
                     INSERT INTO events $events;\n\
                     {LINK_BACKFILL_RUNS};\n\
                     COMMIT TRANSACTION;"
                ))
                .bind(("runs", records.to_vec()))
                .bind(("events", events.clone()))
                .bind(("links", links.clone()))
                .await
                .context("failed to enqueue backfill runs batch")?
                .check()?;
            Ok(())
        })
        .await;
        swallow_phantom_commit(
            result,
            "enqueue_backfill_runs",
            &format!("batch[{}]", records.len()),
        )?;

        // Cancel-vs-submit race repair: a cancel can land between the
        // InProgress flip and this commit, and its cascade over the links
        // ran before these rows existed.
        let status = self
            .backfill_status(backfill_id)
            .await?
            .with_context(|| format!("backfill '{backfill_id}' not found"))?;
        if status == BackfillStatus::InProgress {
            return Ok(true);
        }
        for record in records {
            self.cancel_queued_run(&record.run_id).await?;
        }
        Ok(false)
    }

    /// Conditionally link a run id to a live backfill — `false` when the
    /// backfill is no longer `InProgress`, in which case the caller must not
    /// create the run. Linked before the run exists, so a cancel cascade can
    /// always reach every child that will ever exist. `run_number` is the
    /// place of the run in the plan and `member_count` the number of
    /// partitions it holds.
    pub async fn link_backfill_run(
        &self,
        backfill_id: &str,
        run_id: &str,
        run_number: usize,
        member_count: usize,
    ) -> Result<bool> {
        let links = vec![BackfillRunRow {
            backfill_id: backfill_id.to_string(),
            run_id: run_id.to_string(),
            linked_at: now_nanos(),
            run_number: run_number as i64,
            member_count: member_count as i64,
        }];
        retry::with_retry(&self.retry_config, || async {
            // The status is read again after the commit: a cancel writes the
            // status before it reads the links, so one of the two sees the other.
            let mut result = self
                .db
                .query(format!(
                    "BEGIN TRANSACTION;\n\
                     IF (SELECT VALUE status FROM backfills WHERE backfill_id = $id)[0] = 'InProgress' {{\n\
                         {LINK_BACKFILL_RUNS};\n\
                     }};\n\
                     COMMIT TRANSACTION;\n\
                     RETURN (SELECT VALUE status FROM backfills WHERE backfill_id = $id)[0] = 'InProgress' \
                         AND (SELECT VALUE run_id FROM backfill_runs \
                             WHERE backfill_id = $id AND run_id = $run_id) != [];"
                ))
                .bind(("id", backfill_id.to_string()))
                .bind(("run_id", run_id.to_string()))
                .bind(("links", links.clone()))
                .await?
                .check()?;
            let linked: Option<bool> = result.take(3)?;
            Ok(linked.unwrap_or(false))
        })
        .await
    }
}

/// Partitions the key of `run` names.
fn member_count(run: &RunRecord) -> i64 {
    run.partition_key
        .as_ref()
        .map_or(0, |key| key.member_count() as i64)
}
