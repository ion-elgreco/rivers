use super::*;

// ── backfill launch recovery ──

#[tokio::test]
async fn test_enqueue_backfill_runs_links_atomically() {
    let storage = make_storage().await;
    storage
        .create_backfill(&make_backfill("bf1", BackfillStatus::InProgress, 100))
        .await
        .unwrap();

    let records = vec![
        minimal_run("bf1-r1", RunStatus::Queued),
        minimal_run("bf1-r2", RunStatus::Queued),
    ];
    let live = storage
        .enqueue_backfill_runs(&records, "bf1")
        .await
        .unwrap();
    assert!(live);

    let bf = storage.get_backfill("bf1").await.unwrap().unwrap();
    let mut linked = bf.run_ids.clone();
    linked.sort();
    assert_eq!(linked, vec!["bf1-r1".to_string(), "bf1-r2".to_string()]);

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
        .create_backfill(&make_backfill("bf-race", BackfillStatus::InProgress, 100))
        .await
        .unwrap();
    assert_eq!(
        storage.cancel_backfill("bf-race").await.unwrap(),
        BackfillStatus::Canceled
    );

    // The batch commits after the cancel — it must be swept, not left
    // sitting in the queue.
    let records = vec![
        minimal_run("bfr-r1", RunStatus::Queued),
        minimal_run("bfr-r2", RunStatus::Queued),
    ];
    let live = storage
        .enqueue_backfill_runs(&records, "bf-race")
        .await
        .unwrap();
    assert!(!live);

    for run_id in ["bfr-r1", "bfr-r2"] {
        let run = storage.get_run(run_id).await.unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Canceled);
    }
}

#[tokio::test]
async fn test_link_backfill_run_only_while_in_progress() {
    let storage = make_storage().await;
    storage
        .create_backfill(&make_backfill("bf-link", BackfillStatus::InProgress, 100))
        .await
        .unwrap();

    assert!(storage.link_backfill_run("bf-link", "lr1").await.unwrap());
    assert_eq!(
        storage.cancel_backfill("bf-link").await.unwrap(),
        BackfillStatus::Canceled
    );
    assert!(!storage.link_backfill_run("bf-link", "lr2").await.unwrap());

    let bf = storage.get_backfill("bf-link").await.unwrap().unwrap();
    assert_eq!(bf.run_ids, vec!["lr1".to_string()]);
}

#[tokio::test]
async fn test_resume_stalled_backfill_flips_only_zero_run_in_progress() {
    let storage = make_storage().await;

    // Zero-run InProgress → flips back to Requested.
    storage
        .create_backfill(&make_backfill("stuck", BackfillStatus::InProgress, 100))
        .await
        .unwrap();
    assert!(storage.resume_stalled_backfill("stuck").await.unwrap());
    let bf = storage.get_backfill("stuck").await.unwrap().unwrap();
    assert_eq!(bf.status, BackfillStatus::Requested);

    // InProgress with runs → left alone.
    let mut with_runs = make_backfill("linked", BackfillStatus::InProgress, 100);
    with_runs.run_ids = vec!["r1".to_string()];
    storage.create_backfill(&with_runs).await.unwrap();
    assert!(!storage.resume_stalled_backfill("linked").await.unwrap());
    let bf = storage.get_backfill("linked").await.unwrap().unwrap();
    assert_eq!(bf.status, BackfillStatus::InProgress);

    // Already Requested → no-op.
    assert!(!storage.resume_stalled_backfill("stuck").await.unwrap());
}

#[tokio::test]
async fn test_fail_backfill_records_error() {
    let storage = make_storage().await;
    storage
        .create_backfill(&make_backfill("doomed", BackfillStatus::InProgress, 100))
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

// ── backfill progress rows ──

async fn embedded_storage(temp_dir: &test_temp_dir::TestTempDir) -> SurrealStorage {
    SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
        .await
        .expect("failed to create rocksdb storage")
}

fn key(k: &str) -> PartitionKey {
    PartitionKey::Single {
        keys: vec![k.to_string()],
    }
}

#[tokio::test]
async fn test_backfill_progress_is_kept_off_the_backfill_row() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded_storage(&temp_dir).await;
    let mut bf = make_backfill("bf", BackfillStatus::InProgress, 1);
    bf.partition_keys = vec![key("a"), key("b"), key("c")];
    storage.create_backfill(&bf).await.unwrap();

    for run_id in ["r2", "r1", "r3"] {
        assert!(storage.link_backfill_run("bf", run_id).await.unwrap());
    }
    assert!(storage.link_backfill_run("bf", "r2").await.unwrap());
    storage
        .update_backfill_progress("bf", &["r2".into()], &[key("a")], &[], &[])
        .await
        .unwrap();
    storage
        .update_backfill_progress("bf", &["r1".into()], &[], &[key("b")], &[])
        .await
        .unwrap();
    storage
        .update_backfill_progress("bf", &["r2".into()], &[key("a")], &[], &[key("c")])
        .await
        .unwrap();

    let read = storage.get_backfill("bf").await.unwrap().unwrap();
    assert_eq!(read.partition_keys, bf.partition_keys);
    assert_eq!(
        read.run_ids,
        vec!["r2", "r1", "r3"],
        "link order, once each"
    );
    assert_eq!(read.completed_partitions, vec![key("a")]);
    assert_eq!(read.failed_partitions, vec![key("b")]);
    assert_eq!(read.canceled_partitions, vec![key("c")]);

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
async fn test_link_backfill_run_needs_a_live_backfill() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded_storage(&temp_dir).await;
    storage
        .create_backfill(&make_backfill("requested", BackfillStatus::Requested, 1))
        .await
        .unwrap();

    assert!(!storage.link_backfill_run("requested", "r1").await.unwrap());
    assert!(!storage.link_backfill_run("missing", "r1").await.unwrap());
    let read = storage.get_backfill("requested").await.unwrap().unwrap();
    assert!(read.run_ids.is_empty());
}

#[tokio::test]
async fn test_try_complete_backfill_replaces_settled_keys() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded_storage(&temp_dir).await;
    let mut bf = make_backfill("bf", BackfillStatus::InProgress, 1);
    bf.partition_keys = vec![key("a"), key("b"), key("c")];
    storage.create_backfill(&bf).await.unwrap();

    for (run_id, pk, status) in [
        ("ra", "a", RunStatus::Success),
        ("rb", "b", RunStatus::Failure),
    ] {
        assert!(storage.link_backfill_run("bf", run_id).await.unwrap());
        let mut run = minimal_run(run_id, status);
        run.partition_key = Some(key(pk));
        storage.create_run(&run).await.unwrap();
    }
    // Reported before the runs settled: the finalizer derives from the runs.
    storage
        .update_backfill_progress("bf", &[], &[], &[key("a")], &[])
        .await
        .unwrap();

    let status = storage
        .try_complete_backfill("bf", &[key("c")])
        .await
        .unwrap();
    assert_eq!(status, Some(BackfillStatus::CompletedFailed));

    let read = storage.get_backfill("bf").await.unwrap().unwrap();
    assert_eq!(read.status, BackfillStatus::CompletedFailed);
    assert!(read.end_time.is_some());
    assert_eq!(read.completed_partitions, vec![key("a")]);
    assert_eq!(read.failed_partitions, vec![key("b")]);
    assert_eq!(read.canceled_partitions, vec![key("c")]);
}

#[tokio::test]
async fn test_backfill_lists_read_each_backfills_own_progress() {
    let storage = make_storage().await;
    for (id, create_time) in [("bf1", 1), ("bf2", 2), ("bf3", 3)] {
        let mut bf = make_backfill(id, BackfillStatus::InProgress, create_time);
        bf.partition_keys = vec![key(id)];
        storage.create_backfill(&bf).await.unwrap();
    }
    storage
        .update_backfill_progress("bf1", &["bf1-r".into()], &[key("bf1")], &[], &[])
        .await
        .unwrap();
    storage
        .update_backfill_progress("bf2", &["bf2-r".into()], &[], &[key("bf2")], &[])
        .await
        .unwrap();

    let check = |rows: &[BackfillRecord]| {
        let by_id: std::collections::HashMap<&str, &BackfillRecord> =
            rows.iter().map(|b| (b.backfill_id.as_str(), b)).collect();
        assert_eq!(by_id.len(), 3);
        for (id, bf) in &by_id {
            assert_eq!(bf.partition_keys, vec![key(id)]);
        }
        assert_eq!(by_id["bf1"].run_ids, vec!["bf1-r"]);
        assert_eq!(by_id["bf1"].completed_partitions, vec![key("bf1")]);
        assert!(by_id["bf1"].failed_partitions.is_empty());
        assert_eq!(by_id["bf2"].run_ids, vec!["bf2-r"]);
        assert_eq!(by_id["bf2"].failed_partitions, vec![key("bf2")]);
        assert!(by_id["bf3"].run_ids.is_empty());
        assert!(by_id["bf3"].completed_partitions.is_empty());
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
