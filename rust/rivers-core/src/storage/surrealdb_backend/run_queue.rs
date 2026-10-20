use anyhow::{Context, Result};
use surrealdb::types::SurrealValue;

use crate::storage::retry;
use std::collections::{HashMap, HashSet};

use crate::storage::{
    BackfillStatus, EventRecord, EventType, LaunchedBy, PartitionKey, RunRecord, RunStatus,
};

use super::*;

/// Build the `RunQueued` event row that pairs with a freshly-written queued `RunRecord`.
fn run_queued_event(record: &RunRecord) -> EventRecord {
    EventRecord {
        code_location_id: record.code_location_id.clone(),
        event_type: EventType::RunQueued,
        asset_key: None,
        run_id: record.run_id.clone(),
        partition_key: record.partition_key.clone(),
        timestamp: record.start_time,
        metadata: vec![(
            crate::storage::tag_keys::PRIORITY.to_string(),
            record.priority.to_string(),
        )],
        input_data_versions: vec![],
    }
}

/// The partition members a finished run settles, by outcome.
pub(super) struct SettledRunKeys {
    pub(super) completed: Vec<PartitionKey>,
    pub(super) failed: Vec<PartitionKey>,
    pub(super) canceled: Vec<PartitionKey>,
}

impl SettledRunKeys {
    /// A successful run completes each member of its key, except the members
    /// in `step_failed`; a failed or canceled run settles every member so.
    pub(super) fn of(run: &RunRecord, step_failed: Option<&HashSet<PartitionKey>>) -> Self {
        let mut settled = Self {
            completed: Vec::new(),
            failed: Vec::new(),
            canceled: Vec::new(),
        };
        let Some(pk) = &run.partition_key else {
            return settled;
        };
        match run.status {
            RunStatus::Success => {
                for member in pk.members() {
                    if step_failed.is_some_and(|f| f.contains(&member)) {
                        settled.failed.push(member);
                    } else {
                        settled.completed.push(member);
                    }
                }
            }
            RunStatus::Failure => settled.failed = pk.members(),
            RunStatus::Canceled => settled.canceled = pk.members(),
            _ => {}
        }
        settled
    }
}

impl SurrealStorage {
    /// Conditional `update_run_status`: mark `Failure` only while the run is
    /// still non-terminal. Returns whether a row matched — `false` means the
    /// run already reached a terminal status and was left alone (a late
    /// launch-error must not stomp a concurrent Canceled/Success write).
    pub async fn fail_run_if_active(&self, run_id: &str, end_time: i64) -> Result<bool> {
        let rows: Vec<RunRecord> = retry::with_retry(&self.retry_config, || async {
            let mut response = self
                .db
                .query(
                    "UPDATE runs SET status = 'Failure', end_time = $end_time \
                     WHERE run_id = $run_id \
                     AND status IN ['NotStarted', 'Queued', 'Started'] \
                     RETURN AFTER",
                )
                .bind(("run_id", run_id.to_string()))
                .bind(("end_time", end_time))
                .await
                .context("failed to fail-out run")?
                .check()?;
            let rows: Vec<RunRecord> = response.take(0)?;
            Ok(rows)
        })
        .await?;

        for run in &rows {
            self.record_backfill_run_outcome(run).await;
        }
        Ok(!rows.is_empty())
    }

    /// Persist a queued `RunRecord` and emit its `RunQueued` event in one step.
    pub async fn enqueue_run(&self, record: &RunRecord) -> Result<()> {
        let event = DbEventWrite::from_event(&run_queued_event(record), new_event_record_id());
        let result = retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "BEGIN TRANSACTION;\n\
                     CREATE runs CONTENT $run;\n\
                     CREATE events CONTENT $event;\n\
                     COMMIT TRANSACTION;",
                )
                .bind(("run", record.clone()))
                .bind(("event", event.clone()))
                .await
                .context("failed to enqueue run")?;
            Ok(())
        })
        .await;
        swallow_phantom_commit(result, "enqueue_run", &record.run_id)
    }

    /// Batch counterpart to [`Self::enqueue_run`].
    pub async fn enqueue_runs(&self, records: &[RunRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let events: Vec<DbEventWrite> = records
            .iter()
            .map(|r| DbEventWrite::from_event(&run_queued_event(r), new_event_record_id()))
            .collect();
        let result = retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "BEGIN TRANSACTION;\n\
                     INSERT INTO runs $runs;\n\
                     INSERT INTO events $events;\n\
                     COMMIT TRANSACTION;",
                )
                .bind(("runs", records.to_vec()))
                .bind(("events", events.clone()))
                .await
                .context("failed to enqueue runs batch")?;
            Ok(())
        })
        .await;
        swallow_phantom_commit(result, "enqueue_runs", &format!("batch[{}]", records.len()))
    }

    /// [`Self::enqueue_runs`] plus linking the run ids onto the owning
    /// backfill, all in one transaction — `run_ids` can never disagree with
    /// the runs table.
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
        let run_ids: Vec<String> = records.iter().map(|r| r.run_id.clone()).collect();
        let links = BackfillRunRow::linked_now(backfill_id, &run_ids);
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
        // InProgress flip and this commit, and its cascade over `run_ids`
        // ran before these rows existed.
        let backfill = self
            .get_backfill(backfill_id)
            .await?
            .with_context(|| format!("backfill '{backfill_id}' not found"))?;
        if backfill.status == BackfillStatus::InProgress {
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
    /// always reach every child that will ever exist.
    pub async fn link_backfill_run(&self, backfill_id: &str, run_id: &str) -> Result<bool> {
        let links = BackfillRunRow::linked_now(backfill_id, &[run_id.to_string()]);
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

    /// Record the keys of a finished backfill run as settled, so the backfill
    /// shows its progress run by run. A key settled again takes the new
    /// outcome. Errors are only logged: the run's status is already written,
    /// and the backfill's final settle derives every key from its runs again.
    pub(super) async fn record_backfill_run_outcome(&self, run: &RunRecord) {
        let LaunchedBy::Backfill { backfill_id } = &run.launched_by else {
            return;
        };
        if run.partition_key.is_none()
            || !matches!(
                run.status,
                RunStatus::Success | RunStatus::Failure | RunStatus::Canceled
            )
        {
            return;
        }

        let result = async {
            let failed_by_run = if run.status == RunStatus::Success {
                self.step_failed_members(vec![run.run_id.clone()]).await?
            } else {
                HashMap::new()
            };
            let settled = SettledRunKeys::of(run, failed_by_run.get(&run.run_id));
            let outcomes = BackfillOutcomeRow::settled(
                backfill_id,
                &settled.completed,
                &settled.failed,
                &settled.canceled,
            );

            // A deleted backfill gets no rows back.
            retry::with_retry(&self.retry_config, || async {
                self.db
                    .query(format!(
                        "BEGIN TRANSACTION;\n\
                         IF (SELECT VALUE backfill_id FROM backfills WHERE backfill_id = $id) != [] {{\n\
                             {RECORD_BACKFILL_OUTCOMES};\n\
                         }};\n\
                         COMMIT TRANSACTION;"
                    ))
                    .bind(("id", backfill_id.clone()))
                    .bind(("outcomes", outcomes.clone()))
                    .await?
                    .check()?;
                Ok(())
            })
            .await
        }
        .await;
        if let Err(e) = result {
            tracing::warn!(
                target: "rivers::storage",
                run_id = %run.run_id,
                backfill_id = %backfill_id,
                error = %e,
                "failed to record the backfill run's outcome"
            );
        }
    }

    /// The partition members each of `run_ids` recorded a `StepFailure` for.
    pub(super) async fn step_failed_members(
        &self,
        run_ids: Vec<String>,
    ) -> Result<HashMap<String, HashSet<PartitionKey>>> {
        #[derive(SurrealValue)]
        struct FailRow {
            run_id: String,
            partition_key: PartitionKey,
        }
        let mut failed_by_run: HashMap<String, HashSet<PartitionKey>> = HashMap::new();
        if run_ids.is_empty() {
            return Ok(failed_by_run);
        }
        let rows: Vec<FailRow> = retry::with_retry(&self.retry_config, || async {
            let mut res = self
                .db
                .query(
                    "SELECT run_id, partition_key FROM events \
                     WHERE run_id IN $rids AND event_type = 'StepFailure' \
                     AND partition_key IS NOT NONE GROUP BY run_id, partition_key",
                )
                .bind(("rids", run_ids.clone()))
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
        Ok(failed_by_run)
    }

    /// Flip a zero-run `InProgress` backfill back to `Requested` so the
    /// pickup loop re-executes it (guarded — a backfill that gained runs or
    /// moved on is left alone). Returns whether the flip applied.
    pub async fn resume_stalled_backfill(&self, backfill_id: &str) -> Result<bool> {
        retry::with_retry(&self.retry_config, || async {
            #[derive(Debug, SurrealValue, serde::Deserialize)]
            struct IdRow {
                #[allow(dead_code)]
                backfill_id: String,
            }
            let mut result = self
                .db
                .query(
                    "UPDATE backfills SET status = 'Requested' \
                         WHERE backfill_id = $id AND status = 'InProgress' \
                         AND (SELECT VALUE run_id FROM backfill_runs \
                             WHERE backfill_id = $id LIMIT 1) = [] \
                         RETURN backfill_id",
                )
                .bind(("id", backfill_id.to_string()))
                .await?;
            let flipped: Vec<IdRow> = result.take(0)?;
            Ok(!flipped.is_empty())
        })
        .await
    }

    /// Mark a backfill `CompletedFailed` with the submission error recorded.
    pub async fn fail_backfill(&self, backfill_id: &str, error: &str) -> Result<()> {
        retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "UPDATE backfills SET status = 'CompletedFailed', \
                         end_time = $end_time, error = $error \
                         WHERE backfill_id = $id",
                )
                .bind(("id", backfill_id.to_string()))
                .bind(("end_time", now_nanos()))
                .bind(("error", error.to_string()))
                .await?;
            Ok(())
        })
        .await
    }
}
