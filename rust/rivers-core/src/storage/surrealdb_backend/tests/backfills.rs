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
