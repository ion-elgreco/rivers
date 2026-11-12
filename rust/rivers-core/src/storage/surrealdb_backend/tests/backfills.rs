use super::*;
use crate::storage::{BackfillRunOutcome, UnstartedBackfillRun};

// ── backfill launch recovery ──

#[tokio::test]
async fn test_enqueue_backfill_runs_links_atomically() {
    let storage = make_storage().await;
    storage
        .create_backfill(&make_backfill("bf1", BackfillStatus::InProgress, 100), &[])
        .await
        .unwrap();

    let mut records = vec![
        minimal_run("bf1-r1", RunStatus::Queued),
        minimal_run("bf1-r2", RunStatus::Queued),
    ];
    records[0].partition_key = Some(keys(&["a"]));
    records[1].partition_key = Some(keys(&["b", "c", "d"]));
    let live = storage
        .enqueue_backfill_runs(&records, "bf1")
        .await
        .unwrap();
    assert!(live);

    let bf = storage.get_backfill("bf1").await.unwrap().unwrap();
    assert_eq!(bf.run_ids, vec!["bf1-r1", "bf1-r2"], "plan order");
    assert_eq!(
        link_rows(&storage, "bf1").await,
        vec![("bf1-r1".to_string(), 0, 1), ("bf1-r2".to_string(), 1, 3)],
        "each run is linked with its place in the plan and its number of partitions"
    );

    for run_id in ["bf1-r1", "bf1-r2"] {
        let run = storage.get_run(run_id).await.unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Queued);
        let (events, total) = storage
            .get_run_structured_events_page(run_id, None, 0, 10)
            .await
            .unwrap();
        assert_eq!(total, 1);
        assert_eq!(events[0].event_type, EventType::RunQueued);
    }
}

#[tokio::test]
async fn test_enqueue_backfill_runs_sweeps_batch_after_cancel() {
    let storage = make_storage().await;
    storage
        .create_backfill(
            &make_backfill("bf-race", BackfillStatus::InProgress, 100),
            &[keys(&["a", "b", "c"])],
        )
        .await
        .unwrap();
    assert_eq!(
        storage.cancel_backfill("bf-race").await.unwrap(),
        BackfillStatus::Canceled
    );

    // The batch commits after the cancel — it must be swept, not left
    // sitting in the queue.
    let mut records = vec![
        minimal_run("bfr-r1", RunStatus::Queued),
        minimal_run("bfr-r2", RunStatus::Queued),
    ];
    records[0].partition_key = Some(keys(&["a"]));
    records[1].partition_key = Some(keys(&["b", "c"]));
    for record in &mut records {
        record.launched_by = LaunchedBy::Backfill {
            backfill_id: "bf-race".into(),
        };
    }
    let live = storage
        .enqueue_backfill_runs(&records, "bf-race")
        .await
        .unwrap();
    assert!(!live);

    for run_id in ["bfr-r1", "bfr-r2"] {
        let run = storage.get_run(run_id).await.unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Canceled);
    }
    let bf = storage.get_backfill("bf-race").await.unwrap().unwrap();
    assert_eq!(
        bf.canceled_partitions, 3,
        "the swept runs count as canceled"
    );
}

#[tokio::test]
async fn test_link_backfill_run_only_while_in_progress() {
    let storage = make_storage().await;
    storage
        .create_backfill(
            &make_backfill("bf-link", BackfillStatus::InProgress, 100),
            &[],
        )
        .await
        .unwrap();

    assert!(
        storage
            .link_backfill_run("bf-link", "lr1", 0, 1)
            .await
            .unwrap()
    );
    assert_eq!(
        storage.cancel_backfill("bf-link").await.unwrap(),
        BackfillStatus::Canceled
    );
    assert!(
        !storage
            .link_backfill_run("bf-link", "lr2", 1, 1)
            .await
            .unwrap()
    );

    let bf = storage.get_backfill("bf-link").await.unwrap().unwrap();
    assert_eq!(bf.run_ids, vec!["lr1".to_string()]);
}

#[tokio::test]
async fn test_resume_stalled_backfill_flips_only_zero_run_in_progress() {
    let storage = make_storage().await;

    // Zero-run InProgress → flips back to Requested.
    storage
        .create_backfill(
            &make_backfill("stuck", BackfillStatus::InProgress, 100),
            &[],
        )
        .await
        .unwrap();
    assert!(storage.resume_stalled_backfill("stuck").await.unwrap());
    let bf = storage.get_backfill("stuck").await.unwrap().unwrap();
    assert_eq!(bf.status, BackfillStatus::Requested);

    // InProgress with runs → left alone.
    storage
        .create_backfill(
            &make_backfill("linked", BackfillStatus::InProgress, 100),
            &[],
        )
        .await
        .unwrap();
    assert!(
        storage
            .link_backfill_run("linked", "r1", 0, 1)
            .await
            .unwrap()
    );
    assert!(!storage.resume_stalled_backfill("linked").await.unwrap());
    let bf = storage.get_backfill("linked").await.unwrap().unwrap();
    assert_eq!(bf.status, BackfillStatus::InProgress);

    // InProgress with a planned run that ended before it started → left alone.
    storage
        .create_backfill(
            &make_backfill("settled", BackfillStatus::InProgress, 100),
            &[],
        )
        .await
        .unwrap();
    storage
        .settle_unstarted_backfill_runs("settled", &[unstarted(0, 1, BackfillRunOutcome::Failed)])
        .await
        .unwrap();
    assert!(!storage.resume_stalled_backfill("settled").await.unwrap());

    // Already Requested → no-op.
    assert!(!storage.resume_stalled_backfill("stuck").await.unwrap());
}

#[tokio::test]
async fn test_fail_backfill_records_error() {
    let storage = make_storage().await;
    storage
        .create_backfill(
            &make_backfill("doomed", BackfillStatus::InProgress, 100),
            &[],
        )
        .await
        .unwrap();

    storage
        .fail_backfill("doomed", "submit exploded")
        .await
        .unwrap();

    let bf = storage.get_backfill("doomed").await.unwrap().unwrap();
    assert_eq!(bf.status, BackfillStatus::CompletedFailed);
    assert!(bf.end_time.is_some());
    assert_eq!(bf.error.as_deref(), Some("submit exploded"));
}

#[tokio::test]
async fn test_fail_backfill_keeps_the_status_of_a_backfill_that_ended() {
    let storage = make_storage().await;
    storage
        .create_backfill(&make_backfill("bf", BackfillStatus::InProgress, 100), &[])
        .await
        .unwrap();
    assert_eq!(
        storage.cancel_backfill("bf").await.unwrap(),
        BackfillStatus::Canceled
    );

    storage
        .fail_backfill("bf", "submit exploded")
        .await
        .unwrap();

    let bf = storage.get_backfill("bf").await.unwrap().unwrap();
    assert_eq!(bf.status, BackfillStatus::Canceled);
    assert_eq!(bf.error, None);
}

#[tokio::test]
async fn test_start_backfill_only_from_requested() {
    let storage = make_storage().await;
    storage
        .create_backfill(&make_backfill("bf", BackfillStatus::Requested, 100), &[])
        .await
        .unwrap();

    assert!(storage.start_backfill("bf", 265).await.unwrap());
    let bf = storage.get_backfill("bf").await.unwrap().unwrap();
    assert_eq!(
        (bf.status.clone(), bf.planned_runs),
        (BackfillStatus::InProgress, 265)
    );

    assert!(
        !storage.start_backfill("bf", 7).await.unwrap(),
        "a backfill starts one time"
    );
    assert!(!storage.start_backfill("missing", 1).await.unwrap());
    let bf = storage.get_backfill("bf").await.unwrap().unwrap();
    assert_eq!(bf.planned_runs, 265);
}

// ── backfill rows ──

async fn embedded_storage(temp_dir: &test_temp_dir::TestTempDir) -> SurrealStorage {
    SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
        .await
        .expect("failed to create embedded storage")
}

fn key(k: &str) -> PartitionKey {
    keys(&[k])
}

/// One key that names each of `values`.
fn keys(values: &[&str]) -> PartitionKey {
    PartitionKey::Single {
        keys: values.iter().map(|v| v.to_string()).collect(),
    }
}

fn unstarted(
    run_number: usize,
    member_count: usize,
    outcome: BackfillRunOutcome,
) -> UnstartedBackfillRun {
    UnstartedBackfillRun {
        run_number,
        member_count,
        outcome,
    }
}

/// `(run_id, run_number, member_count)` of the links of a backfill, in link order.
async fn link_rows(storage: &SurrealStorage, backfill_id: &str) -> Vec<(String, i64, i64)> {
    #[derive(SurrealValue)]
    struct Row {
        run_id: String,
        run_number: i64,
        member_count: i64,
    }
    let rows: Vec<Row> = storage
        .db
        .query(
            "SELECT run_id, run_number, member_count, linked_at FROM backfill_runs \
             WHERE backfill_id = $id ORDER BY linked_at",
        )
        .bind(("id", backfill_id.to_string()))
        .await
        .unwrap()
        .take(0)
        .unwrap();
    rows.into_iter()
        .map(|r| (r.run_id, r.run_number, r.member_count))
        .collect()
}

/// `(completed, failed, canceled)` partitions of backfill `bf`.
async fn counts(storage: &SurrealStorage) -> (u64, u64, u64) {
    let bf = storage.get_backfill("bf").await.unwrap().unwrap();
    (
        bf.completed_partitions,
        bf.failed_partitions,
        bf.canceled_partitions,
    )
}

/// An `InProgress` backfill `bf` requested for `requested`, planned as
/// `planned_runs` runs.
async fn in_progress_backfill(
    storage: &SurrealStorage,
    requested: &[PartitionKey],
    planned_runs: u64,
) {
    let mut bf = make_backfill("bf", BackfillStatus::InProgress, 1);
    bf.planned_runs = planned_runs;
    storage.create_backfill(&bf, requested).await.unwrap();
}

/// Link run `run_number` of backfill `bf` and create it over `run_keys`.
async fn backfill_child(
    storage: &SurrealStorage,
    run_id: &str,
    run_number: usize,
    run_keys: &[&str],
    status: RunStatus,
) {
    assert!(
        storage
            .link_backfill_run("bf", run_id, run_number, run_keys.len())
            .await
            .unwrap()
    );
    let mut run = minimal_run(run_id, status);
    run.partition_key = Some(keys(run_keys));
    run.launched_by = LaunchedBy::Backfill {
        backfill_id: "bf".into(),
    };
    storage.create_run(&run).await.unwrap();
}

async fn step_failure(storage: &SurrealStorage, run_id: &str, asset: &str, partition: &str) {
    storage
        .store_event(&EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepFailure,
            asset_key: Some(asset.to_string()),
            run_id: run_id.to_string(),
            partition_key: Some(key(partition)),
            timestamp: 1000,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn test_backfill_row_holds_no_keys_runs_or_counts() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded_storage(&temp_dir).await;
    let requested = vec![key("a"), keys(&["b", "c"])];
    in_progress_backfill(&storage, &requested, 2).await;

    for (run_number, run_id) in ["r2", "r1"].into_iter().enumerate() {
        assert!(
            storage
                .link_backfill_run("bf", run_id, run_number, 1)
                .await
                .unwrap()
        );
    }
    assert!(storage.link_backfill_run("bf", "r2", 0, 1).await.unwrap());

    let read = storage.get_backfill("bf").await.unwrap().unwrap();
    assert_eq!(
        read.num_partitions, 3,
        "a key counts once per partition it names"
    );
    assert_eq!(read.planned_runs, 2);
    assert_eq!(read.run_ids, vec!["r2", "r1"], "link order, once each");
    assert_eq!(storage.get_backfill_keys("bf").await.unwrap(), requested);

    let row: Vec<serde_json::Value> = storage
        .db
        .query(
            "SELECT VALUE [partition_keys, run_ids, completed_partitions, \
             failed_partitions, canceled_partitions] FROM backfills",
        )
        .await
        .unwrap()
        .take(0)
        .unwrap();
    assert_eq!(row, vec![serde_json::json!([null, null, null, null, null])]);
}

#[tokio::test]
async fn test_requested_keys_are_stored_in_pages() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded_storage(&temp_dir).await;
    // 40 keys of about 26 KiB each: more than one page of 256 KiB.
    let requested: Vec<PartitionKey> = (0..40)
        .map(|k| PartitionKey::Single {
            keys: (0..2000).map(|v| format!("key-{k:02}-{v:04}")).collect(),
        })
        .collect();
    storage
        .create_backfill(
            &make_backfill("bf", BackfillStatus::Requested, 1),
            &requested,
        )
        .await
        .unwrap();

    assert_eq!(storage.get_backfill_keys("bf").await.unwrap(), requested);
    let read = storage.get_backfill("bf").await.unwrap().unwrap();
    assert_eq!(read.num_partitions, 80_000);

    let pages: Vec<i64> = storage
        .db
        .query("SELECT VALUE page FROM backfill_key_pages WHERE backfill_id = 'bf' ORDER BY page")
        .await
        .unwrap()
        .take(0)
        .unwrap();
    assert!(pages.len() > 1, "the keys take more than one page");
    assert_eq!(pages, (0..pages.len() as i64).collect::<Vec<_>>());
    assert!(
        storage
            .get_backfill_keys("missing")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn test_link_backfill_run_needs_a_live_backfill() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded_storage(&temp_dir).await;
    storage
        .create_backfill(
            &make_backfill("requested", BackfillStatus::Requested, 1),
            &[],
        )
        .await
        .unwrap();

    assert!(
        !storage
            .link_backfill_run("requested", "r1", 0, 1)
            .await
            .unwrap()
    );
    assert!(
        !storage
            .link_backfill_run("missing", "r1", 0, 1)
            .await
            .unwrap()
    );
    let read = storage.get_backfill("requested").await.unwrap().unwrap();
    assert!(read.run_ids.is_empty());
}

#[tokio::test]
async fn test_backfill_lists_read_each_backfills_own_progress() {
    let storage = make_storage().await;
    for (id, create_time) in [("bf1", 1), ("bf2", 2), ("bf3", 3)] {
        let bf = make_backfill(id, BackfillStatus::InProgress, create_time);
        storage
            .create_backfill(&bf, &[keys(&["a", "b"])])
            .await
            .unwrap();
    }
    for (id, run_id, status) in [
        ("bf1", "bf1-r", RunStatus::Success),
        ("bf2", "bf2-r", RunStatus::Failure),
    ] {
        assert!(storage.link_backfill_run(id, run_id, 0, 2).await.unwrap());
        let mut run = minimal_run(run_id, RunStatus::Started);
        run.launched_by = LaunchedBy::Backfill {
            backfill_id: id.into(),
        };
        storage.create_run(&run).await.unwrap();
        storage
            .update_run_status(run_id, status, Some(2))
            .await
            .unwrap();
    }

    let check = |rows: &[BackfillRecord]| {
        let by_id: std::collections::HashMap<&str, &BackfillRecord> =
            rows.iter().map(|b| (b.backfill_id.as_str(), b)).collect();
        assert_eq!(by_id.len(), 3);
        for bf in by_id.values() {
            assert_eq!(bf.num_partitions, 2);
        }
        assert_eq!(by_id["bf1"].run_ids, vec!["bf1-r"]);
        assert_eq!(by_id["bf1"].completed_partitions, 2);
        assert_eq!(by_id["bf1"].failed_partitions, 0);
        assert_eq!(by_id["bf2"].run_ids, vec!["bf2-r"]);
        assert_eq!(by_id["bf2"].failed_partitions, 2);
        assert!(by_id["bf3"].run_ids.is_empty());
        assert_eq!(by_id["bf3"].completed_partitions, 0);
    };

    let listed = storage
        .get_backfills(DEFAULT_CODE_LOCATION_ID, None, None)
        .await
        .unwrap();
    check(&listed);
    let page = storage
        .get_backfills_page(DEFAULT_CODE_LOCATION_ID, 0, 10, &BackfillFilter::default())
        .await
        .unwrap();
    check(&page.rows);
}

#[tokio::test]
async fn test_rows_per_partition_of_an_older_backfill_count_one_partition_each() {
    let storage = make_storage().await;
    let mut bf = make_backfill("bf", BackfillStatus::CompletedFailed, 1);
    bf.end_time = Some(2);
    storage
        .create_backfill(&bf, &[key("a"), key("b"), key("c"), key("d")])
        .await
        .unwrap();
    storage
        .db
        .query(
            "INSERT INTO backfill_outcomes [\
                { backfill_id: 'bf', partition_key: { variant: 'Single', keys: ['a'] }, outcome: 'Completed' }, \
                { backfill_id: 'bf', partition_key: { variant: 'Single', keys: ['b'] }, outcome: 'Completed' }, \
                { backfill_id: 'bf', partition_key: { variant: 'Single', keys: ['c'] }, outcome: 'Failed' }, \
                { backfill_id: 'bf', partition_key: { variant: 'Single', keys: ['d'] }, outcome: 'Canceled' } \
             ] RETURN NONE",
        )
        .await
        .unwrap()
        .check()
        .unwrap();

    assert_eq!(counts(&storage).await, (2, 1, 1));
}

// ── a backfill is complete when each planned run has an outcome ──

#[tokio::test]
async fn test_try_complete_backfill_waits_for_open_planned_runs() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded_storage(&temp_dir).await;
    in_progress_backfill(&storage, &[keys(&["a", "b", "c", "d"])], 4).await;
    backfill_child(&storage, "ra", 0, &["a"], RunStatus::Started).await;
    backfill_child(&storage, "rb", 1, &["b"], RunStatus::Started).await;
    // Linked but not created yet.
    assert!(storage.link_backfill_run("bf", "rc", 2, 1).await.unwrap());
    for (run_id, status) in [("ra", RunStatus::Success), ("rb", RunStatus::Failure)] {
        storage
            .update_run_status(run_id, status, Some(2))
            .await
            .unwrap();
    }

    assert_eq!(storage.try_complete_backfill("bf").await.unwrap(), None);
    let read = storage.get_backfill("bf").await.unwrap().unwrap();
    assert_eq!(read.status, BackfillStatus::InProgress);
    assert_eq!(read.end_time, None);
}

#[tokio::test]
async fn test_try_complete_backfill_completes_once_every_planned_run_ended() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded_storage(&temp_dir).await;
    in_progress_backfill(&storage, &[keys(&["a", "b", "c"])], 2).await;
    backfill_child(&storage, "ra", 0, &["a"], RunStatus::Started).await;
    backfill_child(&storage, "rbc", 1, &["b", "c"], RunStatus::Started).await;

    storage
        .update_run_status("ra", RunStatus::Success, Some(2))
        .await
        .unwrap();
    assert_eq!(storage.try_complete_backfill("bf").await.unwrap(), None);
    storage
        .update_run_status("rbc", RunStatus::Success, Some(3))
        .await
        .unwrap();

    assert_eq!(
        storage.try_complete_backfill("bf").await.unwrap(),
        Some(BackfillStatus::CompletedSuccess)
    );
    let read = storage.get_backfill("bf").await.unwrap().unwrap();
    assert_eq!(read.status, BackfillStatus::CompletedSuccess);
    assert!(read.end_time.is_some());
    assert_eq!(counts(&storage).await, (3, 0, 0));
    assert_eq!(
        storage.try_complete_backfill("bf").await.unwrap(),
        None,
        "a backfill completes one time"
    );
}

#[tokio::test]
async fn test_try_complete_backfill_counts_planned_runs_that_never_started() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded_storage(&temp_dir).await;
    in_progress_backfill(&storage, &[keys(&["a", "b", "c", "d"])], 4).await;
    backfill_child(&storage, "ra", 0, &["a"], RunStatus::Started).await;
    storage
        .update_run_status("ra", RunStatus::Failure, Some(2))
        .await
        .unwrap();
    // A run whose launch failed, and two runs canceled before their launch.
    storage
        .settle_unstarted_backfill_runs(
            "bf",
            &[
                unstarted(1, 1, BackfillRunOutcome::Failed),
                unstarted(2, 1, BackfillRunOutcome::Canceled),
                unstarted(3, 1, BackfillRunOutcome::Canceled),
            ],
        )
        .await
        .unwrap();

    assert_eq!(
        storage.try_complete_backfill("bf").await.unwrap(),
        Some(BackfillStatus::CompletedFailed)
    );
    assert_eq!(counts(&storage).await, (0, 2, 2));
}

#[tokio::test]
async fn test_try_complete_backfill_is_canceled_when_no_run_failed() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded_storage(&temp_dir).await;
    in_progress_backfill(&storage, &[keys(&["a", "b"])], 2).await;
    backfill_child(&storage, "ra", 0, &["a"], RunStatus::Started).await;
    backfill_child(&storage, "rb", 1, &["b"], RunStatus::Queued).await;
    storage
        .update_run_status("ra", RunStatus::Success, Some(2))
        .await
        .unwrap();
    assert!(storage.cancel_queued_run("rb").await.unwrap());

    assert_eq!(
        storage.try_complete_backfill("bf").await.unwrap(),
        Some(BackfillStatus::Canceled)
    );
    assert_eq!(counts(&storage).await, (1, 0, 1));
}

#[tokio::test]
async fn test_a_planned_run_that_has_an_outcome_keeps_it() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded_storage(&temp_dir).await;
    in_progress_backfill(&storage, &[keys(&["a"])], 1).await;
    backfill_child(&storage, "ra", 0, &["a"], RunStatus::Started).await;
    storage
        .update_run_status("ra", RunStatus::Success, Some(2))
        .await
        .unwrap();

    storage
        .settle_unstarted_backfill_runs("bf", &[unstarted(0, 1, BackfillRunOutcome::Canceled)])
        .await
        .unwrap();

    assert_eq!(counts(&storage).await, (1, 0, 0));
}

#[tokio::test]
async fn test_runs_that_end_after_a_cancel_leave_the_backfill_canceled() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded_storage(&temp_dir).await;
    in_progress_backfill(&storage, &[keys(&["a", "b"])], 2).await;
    backfill_child(&storage, "ra", 0, &["a"], RunStatus::Started).await;
    backfill_child(&storage, "rb", 1, &["b"], RunStatus::Started).await;
    assert_eq!(
        storage.cancel_backfill("bf").await.unwrap(),
        BackfillStatus::Canceled
    );
    let canceled_at = storage.get_backfill("bf").await.unwrap().unwrap().end_time;

    for run_id in ["ra", "rb"] {
        storage
            .update_run_status(run_id, RunStatus::Failure, Some(5))
            .await
            .unwrap();
    }

    assert_eq!(storage.try_complete_backfill("bf").await.unwrap(), None);
    let read = storage.get_backfill("bf").await.unwrap().unwrap();
    assert_eq!(read.status, BackfillStatus::Canceled);
    assert_eq!(read.end_time, canceled_at);
    assert_eq!(counts(&storage).await, (0, 2, 0));
}

// ── a backfill run records its own outcome as it finishes ──

#[tokio::test]
async fn test_finished_backfill_run_records_its_outcome() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded_storage(&temp_dir).await;
    in_progress_backfill(&storage, &[keys(&["a", "b", "c", "d"])], 3).await;
    backfill_child(&storage, "ra", 0, &["a"], RunStatus::Started).await;
    backfill_child(&storage, "rb", 1, &["b"], RunStatus::Started).await;
    backfill_child(&storage, "rcd", 2, &["c", "d"], RunStatus::Queued).await;

    storage
        .update_run_status("ra", RunStatus::Success, Some(2))
        .await
        .unwrap();
    assert_eq!(counts(&storage).await, (1, 0, 0));
    let read = storage.get_backfill("bf").await.unwrap().unwrap();
    assert_eq!(read.status, BackfillStatus::InProgress);

    storage
        .update_run_status("rb", RunStatus::Failure, Some(3))
        .await
        .unwrap();
    assert_eq!(counts(&storage).await, (1, 1, 0));

    assert!(storage.cancel_queued_run("rcd").await.unwrap());
    assert_eq!(counts(&storage).await, (1, 1, 2));

    assert_eq!(
        storage.try_complete_backfill("bf").await.unwrap(),
        Some(BackfillStatus::CompletedFailed)
    );
    assert_eq!(counts(&storage).await, (1, 1, 2));
}

#[tokio::test]
async fn test_finished_backfill_run_keeps_its_latest_outcome() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded_storage(&temp_dir).await;
    in_progress_backfill(&storage, &[keys(&["a"])], 1).await;
    backfill_child(&storage, "ra", 0, &["a"], RunStatus::Started).await;

    assert!(storage.fail_run_if_active("ra", 2).await.unwrap());
    assert_eq!(counts(&storage).await, (0, 1, 0));
    assert!(
        !storage.fail_run_if_active("ra", 3).await.unwrap(),
        "a run that ended is left alone"
    );

    storage
        .update_run_status("ra", RunStatus::Success, Some(3))
        .await
        .unwrap();
    assert_eq!(counts(&storage).await, (1, 0, 0));
}

#[tokio::test]
async fn test_successful_run_counts_the_partitions_it_failed_one_by_one() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded_storage(&temp_dir).await;
    in_progress_backfill(&storage, &[keys(&["a", "b", "c", "d"])], 2).await;
    backfill_child(&storage, "rabc", 0, &["a", "b", "c"], RunStatus::Started).await;
    backfill_child(&storage, "rd", 1, &["d"], RunStatus::Started).await;
    // Two steps of one run fail the same partition: it counts one time.
    step_failure(&storage, "rabc", "first", "b").await;
    step_failure(&storage, "rabc", "second", "b").await;
    step_failure(&storage, "rabc", "first", "c").await;
    // A failed run fails each of its partitions, with or without such events.
    step_failure(&storage, "rd", "first", "d").await;

    storage
        .update_run_status("rabc", RunStatus::Success, Some(2))
        .await
        .unwrap();
    assert_eq!(counts(&storage).await, (1, 2, 0));
    storage
        .update_run_status("rd", RunStatus::Failure, Some(3))
        .await
        .unwrap();
    assert_eq!(counts(&storage).await, (1, 3, 0));

    assert_eq!(
        storage.try_complete_backfill("bf").await.unwrap(),
        Some(BackfillStatus::CompletedFailed),
        "a partition that failed inside a successful run fails the backfill"
    );
}

#[tokio::test]
async fn test_finished_run_writes_no_outcome_outside_a_backfill() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded_storage(&temp_dir).await;
    let mut orphan = minimal_run("orphan", RunStatus::Started);
    orphan.partition_key = Some(key("a"));
    orphan.launched_by = LaunchedBy::Backfill {
        backfill_id: "gone".into(),
    };
    storage.create_run(&orphan).await.unwrap();
    let mut manual = minimal_run("manual", RunStatus::Started);
    manual.partition_key = Some(key("a"));
    storage.create_run(&manual).await.unwrap();

    for run_id in ["orphan", "manual"] {
        storage
            .update_run_status(run_id, RunStatus::Success, Some(2))
            .await
            .unwrap();
        let run = storage.get_run(run_id).await.unwrap().unwrap();
        assert_eq!((run.status, run.end_time), (RunStatus::Success, Some(2)));
    }
    let rows: Vec<serde_json::Value> = storage
        .db
        .query("SELECT VALUE outcome FROM backfill_run_outcomes")
        .await
        .unwrap()
        .take(0)
        .unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_run_status_that_is_not_final_writes_no_outcome() {
    let storage = make_storage().await;
    in_progress_backfill(&storage, &[keys(&["a"])], 1).await;
    backfill_child(&storage, "ra", 0, &["a"], RunStatus::Queued).await;

    for status in [RunStatus::NotStarted, RunStatus::Started] {
        storage
            .update_run_status("ra", status.clone(), None)
            .await
            .unwrap();
        let run = storage.get_run("ra").await.unwrap().unwrap();
        assert_eq!((run.status, run.end_time), (status, None));
    }
    assert_eq!(counts(&storage).await, (0, 0, 0));
}

#[tokio::test]
async fn test_open_backfill_runs_are_the_linked_runs_with_no_outcome() {
    let storage = make_storage().await;
    in_progress_backfill(&storage, &[keys(&["a", "b", "c", "d"])], 4).await;

    backfill_child(&storage, "ended", 0, &["a"], RunStatus::Started).await;
    storage
        .update_run_status("ended", RunStatus::Success, Some(2))
        .await
        .unwrap();
    backfill_child(&storage, "live", 1, &["b"], RunStatus::Started).await;
    // Linked, and its run does not exist yet.
    assert!(
        storage
            .link_backfill_run("bf", "in-creation", 2, 1)
            .await
            .unwrap()
    );
    // Linked, and the launch failed.
    assert!(
        storage
            .link_backfill_run("bf", "never-started", 3, 1)
            .await
            .unwrap()
    );
    storage
        .settle_unstarted_backfill_runs("bf", &[unstarted(3, 1, BackfillRunOutcome::Failed)])
        .await
        .unwrap();

    assert_eq!(
        storage.get_open_backfill_runs("bf").await.unwrap(),
        vec!["live", "in-creation"]
    );
    assert!(
        storage
            .get_open_backfill_runs("no-such-backfill")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn test_ended_backfill_runs_are_read_by_run_number() {
    let storage = make_storage().await;
    in_progress_backfill(&storage, &[keys(&["a", "b", "c", "d", "e"])], 4).await;

    backfill_child(&storage, "r0", 0, &["a"], RunStatus::Started).await;
    backfill_child(&storage, "r1", 1, &["b", "c"], RunStatus::Started).await;
    backfill_child(&storage, "r2", 2, &["d"], RunStatus::Started).await;
    step_failure(&storage, "r1", "asset", "c").await;
    for (run_id, status) in [
        ("r0", RunStatus::Failure),
        ("r1", RunStatus::Success),
        ("r2", RunStatus::Canceled),
    ] {
        storage
            .update_run_status(run_id, status, Some(2))
            .await
            .unwrap();
    }
    storage
        .settle_unstarted_backfill_runs("bf", &[unstarted(3, 1, BackfillRunOutcome::Canceled)])
        .await
        .unwrap();

    let ended = |run_number, run_id: Option<&str>, outcome, member_count, failed_members| {
        crate::storage::EndedBackfillRun {
            run_number,
            run_id: run_id.map(String::from),
            outcome,
            member_count,
            failed_members,
        }
    };
    assert_eq!(
        storage.get_ended_backfill_runs("bf", 1, 3).await.unwrap(),
        vec![
            ended(1, Some("r1"), BackfillRunOutcome::Completed, 2, 1),
            ended(2, Some("r2"), BackfillRunOutcome::Canceled, 1, 0),
            ended(3, None, BackfillRunOutcome::Canceled, 1, 0),
        ]
    );
    assert_eq!(
        storage.get_ended_backfill_runs("bf", 0, 0).await.unwrap(),
        vec![ended(0, Some("r0"), BackfillRunOutcome::Failed, 1, 0)]
    );
    assert_eq!(
        storage.get_run_failed_partitions("r1").await.unwrap(),
        vec![key("c")]
    );
    assert!(
        storage
            .get_run_failed_partitions("r0")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn test_partition_page_of_an_older_backfill_reads_its_rows_per_partition() {
    use crate::execution::backfill_partitions::{BackfillPartitionState, backfill_partitions_page};

    let storage = make_storage().await;
    let requested = [key("a"), key("b"), key("c"), key("d")];
    storage
        .create_backfill(
            &make_backfill("old", BackfillStatus::CompletedFailed, 1),
            &requested,
        )
        .await
        .unwrap();
    storage
        .create_backfill(
            &make_backfill("other", BackfillStatus::CompletedFailed, 1),
            &requested,
        )
        .await
        .unwrap();
    storage
        .db
        .query(
            "INSERT INTO backfill_outcomes [\
                { backfill_id: 'old', partition_key: $a, outcome: 'Completed' }, \
                { backfill_id: 'old', partition_key: $b, outcome: 'Failed' }, \
                { backfill_id: 'old', partition_key: $c, outcome: 'Canceled' }, \
                { backfill_id: 'other', partition_key: $d, outcome: 'Failed' }\
             ] RETURN NONE",
        )
        .bind(("a", key("a")))
        .bind(("b", key("b")))
        .bind(("c", key("c")))
        .bind(("d", key("d")))
        .await
        .unwrap()
        .check()
        .unwrap();

    let mut outcomes = storage
        .get_backfill_partition_outcomes("old", &[key("b"), key("d"), key("c")])
        .await
        .unwrap();
    outcomes.sort_by_key(|(partition, _)| partition.to_display());
    assert_eq!(
        outcomes,
        vec![
            (key("b"), BackfillRunOutcome::Failed),
            (key("c"), BackfillRunOutcome::Canceled),
        ]
    );

    let old = storage.get_backfill("old").await.unwrap().unwrap();
    let page: Vec<(String, BackfillPartitionState)> =
        backfill_partitions_page(&storage, &old, 1, 3)
            .await
            .unwrap()
            .into_iter()
            .map(|(partition, state)| (partition.to_display(), state))
            .collect();
    assert_eq!(
        page,
        vec![
            ("b".to_string(), BackfillPartitionState::Failed),
            ("c".to_string(), BackfillPartitionState::Canceled),
            ("d".to_string(), BackfillPartitionState::Pending),
        ]
    );
}
