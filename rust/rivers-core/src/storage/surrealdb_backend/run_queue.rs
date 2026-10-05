use anyhow::{Context, Result};
use surrealdb::types::SurrealValue;

use crate::storage::retry;
use crate::storage::{BackfillStatus, EventRecord, EventType, RunRecord};

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

impl SurrealStorage {
    /// Conditional `update_run_status`: mark `Failure` only while the run is
    /// still non-terminal. Returns whether a row matched — `false` means the
    /// run already reached a terminal status and was left alone (a late
    /// launch-error must not stomp a concurrent Canceled/Success write).
    pub async fn fail_run_if_active(&self, run_id: &str, end_time: i64) -> Result<bool> {
        retry::with_retry(&self.retry_config, || async {
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
            Ok(!rows.is_empty())
        })
        .await
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
        let result = retry::with_retry(&self.retry_config, || async {
            self.db
                .query(
                    "BEGIN TRANSACTION;\n\
                     INSERT INTO runs $runs;\n\
                     INSERT INTO events $events;\n\
                     UPDATE backfills SET run_ids = array::union(run_ids, $run_ids) \
                         WHERE backfill_id = $backfill_id;\n\
                     COMMIT TRANSACTION;",
                )
                .bind(("runs", records.to_vec()))
                .bind(("events", events.clone()))
                .bind(("run_ids", run_ids.clone()))
                .bind(("backfill_id", backfill_id.to_string()))
                .await
                .context("failed to enqueue backfill runs batch")?;
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
        retry::with_retry(&self.retry_config, || async {
            let mut result = self
                .db
                .query(
                    "UPDATE backfills SET run_ids = array::union(run_ids, [$run_id]) \
                         WHERE backfill_id = $id AND status = 'InProgress'; \
                     SELECT count() AS total FROM backfills \
                         WHERE backfill_id = $id AND status = 'InProgress' \
                         AND $run_id IN run_ids GROUP ALL",
                )
                .bind(("id", backfill_id.to_string()))
                .bind(("run_id", run_id.to_string()))
                .await?;
            let count: Option<u32> = result.take((1, "total"))?;
            Ok(count.unwrap_or(0) > 0)
        })
        .await
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
                         AND array::len(run_ids) = 0 \
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
