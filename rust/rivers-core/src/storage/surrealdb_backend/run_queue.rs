use anyhow::{Context, Result};

use crate::storage::retry;
use crate::storage::{EventRecord, EventType, RunRecord};

use super::*;

/// Build the `RunQueued` event row that pairs with a freshly-written queued `RunRecord`.
pub(super) fn run_queued_event(record: &RunRecord) -> EventRecord {
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
                .query(format!(
                    "BEGIN TRANSACTION;\n\
                     LET $final = (UPDATE runs SET status = 'Failure', end_time = $end_time \
                         WHERE run_id = $run_id \
                         AND status IN ['NotStarted', 'Queued', 'Started'] \
                         RETURN run_id, status, launched_by);\n\
                     {RECORD_BACKFILL_RUN_OUTCOME}\n\
                     COMMIT TRANSACTION;\n\
                     RETURN array::len($final ?? []) > 0;"
                ))
                .bind(("run_id", run_id.to_string()))
                .bind(("end_time", end_time))
                .await
                .context("failed to fail-out run")?
                .check()?;
            let last = response.num_statements() - 1;
            let failed: Option<bool> = response.take(last)?;
            Ok(failed.unwrap_or(false))
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
}
