use super::*;

// ── Run queue tests ──

#[tokio::test]
async fn test_queued_run_round_trip() {
    let storage = make_storage().await;
    storage
        .create_run(&RunRecord {
            run_id: "q1".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".to_string()),
            status: RunStatus::Queued,
            start_time: 1000,
            end_time: None,
            tags: vec![],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();

    // Should appear in get_queued_runs
    let queued = storage.get_all_queued_runs().await.unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].run_id, "q1");
    assert_eq!(queued[0].status, RunStatus::Queued);

    // Should NOT count as in-progress
    let in_progress = storage.count_in_progress_runs().await.unwrap();
    assert_eq!(in_progress, 0);
}

#[tokio::test]
async fn test_get_queued_runs_priority_ordering() {
    let storage = make_storage().await;

    // Run A: priority 10, start_time 3000
    storage
        .create_run(&RunRecord {
            run_id: "a".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".to_string()),
            status: RunStatus::Queued,
            start_time: 3000,
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: 10,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();

    // Run B: priority 0, start_time 1000
    storage
        .create_run(&RunRecord {
            run_id: "b".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".to_string()),
            status: RunStatus::Queued,
            start_time: 1000,
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();

    // Run C: priority 10, start_time 1000 (same priority as A, earlier time)
    storage
        .create_run(&RunRecord {
            run_id: "c".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".to_string()),
            status: RunStatus::Queued,
            start_time: 1000,
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: 10,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();

    let mut queued = storage.get_all_queued_runs().await.unwrap();
    assert_eq!(queued.len(), 3);
    // get_queued_runs is unordered — sort by priority DESC, start_time ASC (like coordinator)
    queued.sort_by(|a, b| {
        b.priority
            .cmp(&a.priority)
            .then(a.start_time.cmp(&b.start_time))
    });
    // Order: C (priority 10, earliest), A (priority 10, later), B (priority 0)
    assert_eq!(queued[0].run_id, "c");
    assert_eq!(queued[1].run_id, "a");
    assert_eq!(queued[2].run_id, "b");
}

#[tokio::test]
async fn test_get_queued_runs_returns_all() {
    let storage = make_storage().await;

    for i in 0..5 {
        storage
            .create_run(&RunRecord {
                run_id: format!("q{i}"),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("j".to_string()),
                status: RunStatus::Queued,
                start_time: i * 100,
                end_time: None,
                tags: vec![],
                node_names: vec![],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual { user: None },
                action: None,
                config: None,
            })
            .await
            .unwrap();
    }

    let all = storage.get_all_queued_runs().await.unwrap();
    assert_eq!(all.len(), 5);
}

#[tokio::test]
async fn test_enqueue_runs_bulk_round_trip() {
    let storage = make_storage().await;

    let records: Vec<RunRecord> = (0..4)
        .map(|i| RunRecord {
            run_id: format!("bulk{i}"),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("jb".to_string()),
            status: RunStatus::Queued,
            start_time: 1000 + i,
            end_time: None,
            tags: vec![("k".to_string(), format!("v{i}"))],
            node_names: vec![format!("asset{i}")],
            priority: i as i32,
            partition_key: Some(PartitionKey::Single {
                keys: vec![format!("p{i}")],
            }),
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .collect();

    storage.enqueue_runs(&records).await.unwrap();

    let mut all = storage.get_all_queued_runs().await.unwrap();
    assert_eq!(all.len(), 4);
    all.sort_by(|a, b| a.run_id.cmp(&b.run_id));
    let r = &all[2];
    assert_eq!(r.run_id, "bulk2");
    assert_eq!(r.priority, 2);
    assert_eq!(r.job_name.as_deref(), Some("jb"));
    assert_eq!(r.node_names, vec!["asset2".to_string()]);
    assert_eq!(r.tags, vec![("k".to_string(), "v2".to_string())]);
    assert_eq!(
        r.partition_key,
        Some(PartitionKey::Single {
            keys: vec!["p2".to_string()]
        })
    );
}

#[tokio::test]
async fn test_count_in_progress_runs() {
    let storage = make_storage().await;

    let statuses = [
        ("r1", RunStatus::Queued),
        ("r2", RunStatus::NotStarted),
        ("r3", RunStatus::Started),
        ("r4", RunStatus::Success),
        ("r5", RunStatus::Failure),
        ("r6", RunStatus::Canceled),
    ];
    for (id, status) in &statuses {
        storage
            .create_run(&RunRecord {
                run_id: id.to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("j".to_string()),
                status: status.clone(),
                start_time: 1000,
                end_time: None,
                tags: vec![],
                node_names: vec![],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual { user: None },
                action: None,
                config: None,
            })
            .await
            .unwrap();
    }

    // Only NotStarted + Started = 2
    let count = storage.count_in_progress_runs().await.unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_queued_to_not_started_transition() {
    let storage = make_storage().await;
    storage
        .create_run(&RunRecord {
            run_id: "q1".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".to_string()),
            status: RunStatus::Queued,
            start_time: 1000,
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: 5,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();

    // Transition to NotStarted (coordinator dequeued it)
    storage
        .update_run_status("q1", RunStatus::NotStarted, None)
        .await
        .unwrap();

    // Still visible in the queue view (as dequeued-but-launching)…
    let queued = storage.get_all_queued_runs().await.unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].status, RunStatus::NotStarted);

    // …while also counting as in-progress for capacity.
    let count = storage.count_in_progress_runs().await.unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_negative_priority_backfill() {
    let storage = make_storage().await;

    // Backfill run with negative priority
    storage
        .create_run(&RunRecord {
            run_id: "backfill".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".to_string()),
            status: RunStatus::Queued,
            start_time: 1000,
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: -10,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();

    // Normal run with default priority
    storage
        .create_run(&RunRecord {
            run_id: "normal".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".to_string()),
            status: RunStatus::Queued,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();

    let mut queued = storage.get_all_queued_runs().await.unwrap();
    assert_eq!(queued.len(), 2);
    // get_queued_runs is unordered — sort by priority DESC (like coordinator)
    queued.sort_by(|a, b| {
        b.priority
            .cmp(&a.priority)
            .then(a.start_time.cmp(&b.start_time))
    });
    // Normal (priority 0) comes before backfill (priority -10)
    assert_eq!(queued[0].run_id, "normal");
    assert_eq!(queued[1].run_id, "backfill");
}

// ── cancel_queued_run ──

#[tokio::test]
async fn test_cancel_queued_run_cleans_pending_steps() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 1, 300)
        .await
        .unwrap();

    // Create a queued run and put a step in pending_steps
    storage
        .create_run(&RunRecord {
            run_id: "q1".into(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".into()),
            status: RunStatus::Queued,
            start_time: 1000,
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();

    // Fill the pool so the claim goes to pending
    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".into(), 1)],
            "other",
            "blocker",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    let status = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".into(), 1)],
            "q1",
            "step_a",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(status, ConcurrencyClaimStatus::Pending { .. }));

    // Verify pending count before cancel
    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(info.pending_count, 1);

    // Cancel the queued run
    let canceled = storage.cancel_queued_run("q1").await.unwrap();
    assert!(canceled);

    // Pending steps should be cleaned up
    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(info.pending_count, 0);
}

#[tokio::test]
async fn test_cancel_queued_run_transitions_to_canceled() {
    let storage = make_storage().await;
    storage
        .create_run(&RunRecord {
            run_id: "q1".into(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".into()),
            status: RunStatus::Queued,
            start_time: 1000,
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: Some("global limit".into()),
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();

    let canceled = storage.cancel_queued_run("q1").await.unwrap();
    assert!(canceled);

    let run = storage.get_run("q1").await.unwrap().unwrap();
    assert_eq!(run.status, RunStatus::Canceled);
    assert!(run.end_time.is_some());
}

#[tokio::test]
async fn test_cancel_queued_run_noop_for_non_queued() {
    let storage = make_storage().await;
    storage
        .create_run(&RunRecord {
            run_id: "r1".into(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".into()),
            status: RunStatus::Started,
            start_time: 1000,
            end_time: None,
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();

    let canceled = storage.cancel_queued_run("r1").await.unwrap();
    assert!(!canceled);

    let run = storage.get_run("r1").await.unwrap().unwrap();
    assert_eq!(run.status, RunStatus::Started);
}

#[tokio::test]
async fn test_cancel_queued_run_not_found() {
    let storage = make_storage().await;
    let canceled = storage.cancel_queued_run("nonexistent").await.unwrap();
    assert!(!canceled);
}

#[tokio::test]
async fn test_delete_run_removes_run_events_logs_and_cancel_flag() {
    let storage = make_storage().await;
    let mut run = minimal_run("del1", RunStatus::Success);
    run.end_time = Some(2000);
    storage.create_run(&run).await.unwrap();
    storage
        .store_event(&make_event("asset_a", "del1", 1500))
        .await
        .unwrap();
    storage
        .store_run_logs(&[LogRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            run_id: "del1".into(),
            step_key: "asset_a".into(),
            timestamp: 1500,
            stdout: Some("out".into()),
            stderr: None,
            logs: None,
            traceback: None,
        }])
        .await
        .unwrap();
    storage.request_cancellation("del1").await.unwrap();

    let deleted = storage.delete_run("del1").await.unwrap();
    assert!(deleted);

    assert!(storage.get_run("del1").await.unwrap().is_none());
    assert!(storage.get_events_for_run("del1").await.unwrap().is_empty());
    assert!(storage.get_run_logs("del1").await.unwrap().is_empty());
    assert!(!storage.is_cancelled("del1").await.unwrap());
}

#[tokio::test]
async fn test_delete_run_leaves_other_runs_untouched() {
    let storage = make_storage().await;
    let mut doomed = minimal_run("del2", RunStatus::Failure);
    doomed.end_time = Some(2000);
    storage.create_run(&doomed).await.unwrap();
    storage
        .store_event(&make_event("asset_a", "del2", 1500))
        .await
        .unwrap();
    let mut kept = minimal_run("keep1", RunStatus::Success);
    kept.end_time = Some(2000);
    storage.create_run(&kept).await.unwrap();
    storage
        .store_event(&make_event("asset_a", "keep1", 1600))
        .await
        .unwrap();

    assert!(storage.delete_run("del2").await.unwrap());

    let kept_run = storage.get_run("keep1").await.unwrap().unwrap();
    assert_eq!(kept_run.status, RunStatus::Success);
    let kept_events = storage.get_events_for_run("keep1").await.unwrap();
    assert_eq!(kept_events.len(), 1);
    assert_eq!(kept_events[0].run_id, "keep1");
}

#[tokio::test]
async fn test_delete_run_refuses_active_runs() {
    let storage = make_storage().await;
    for (id, status) in [
        ("act1", RunStatus::Started),
        ("act2", RunStatus::Queued),
        ("act3", RunStatus::NotStarted),
    ] {
        storage.create_run(&minimal_run(id, status)).await.unwrap();
        let err = storage.delete_run(id).await.unwrap_err();
        assert!(
            err.to_string().contains("cancel it"),
            "unexpected error for {id}: {err}"
        );
        assert!(
            storage.get_run(id).await.unwrap().is_some(),
            "active run {id} must survive a delete attempt"
        );
    }
}

#[tokio::test]
async fn test_delete_run_deletes_canceled_run() {
    let storage = make_storage().await;
    let mut run = minimal_run("del3", RunStatus::Canceled);
    run.end_time = Some(2000);
    storage.create_run(&run).await.unwrap();
    assert!(storage.delete_run("del3").await.unwrap());
    assert!(storage.get_run("del3").await.unwrap().is_none());
}

#[tokio::test]
async fn test_delete_run_not_found() {
    let storage = make_storage().await;
    assert!(!storage.delete_run("nonexistent").await.unwrap());
}

#[tokio::test]
async fn test_cancel_backfill_late_cancel_settles_to_success() {
    let storage = make_storage().await;
    let mut bf = mk_isolation_backfill(
        "bf-late",
        DEFAULT_CODE_LOCATION_ID,
        BackfillStatus::InProgress,
        100,
    );
    bf.run_ids = vec!["bfr1".into(), "bfr2".into()];
    storage.create_backfill(&bf).await.unwrap();
    for id in ["bfr1", "bfr2"] {
        let mut run = minimal_run(id, RunStatus::Success);
        run.end_time = Some(2000);
        storage.create_run(&run).await.unwrap();
    }

    let status = storage.cancel_backfill("bf-late").await.unwrap();
    assert_eq!(status, BackfillStatus::CompletedSuccess);
    let record = storage.get_backfill("bf-late").await.unwrap().unwrap();
    assert_eq!(record.status, BackfillStatus::CompletedSuccess);
}

#[tokio::test]
async fn test_cancel_backfill_late_cancel_keeps_failure_outcome() {
    let storage = make_storage().await;
    let mut bf = mk_isolation_backfill(
        "bf-fail",
        DEFAULT_CODE_LOCATION_ID,
        BackfillStatus::InProgress,
        100,
    );
    bf.run_ids = vec!["bff1".into()];
    storage.create_backfill(&bf).await.unwrap();
    let mut run = minimal_run("bff1", RunStatus::Failure);
    run.end_time = Some(2000);
    storage.create_run(&run).await.unwrap();

    let status = storage.cancel_backfill("bf-fail").await.unwrap();
    assert_eq!(status, BackfillStatus::CompletedFailed);
}

#[tokio::test]
async fn test_cancel_backfill_never_overwrites_terminal_status() {
    let storage = make_storage().await;
    let mut bf = mk_isolation_backfill(
        "bf-done",
        DEFAULT_CODE_LOCATION_ID,
        BackfillStatus::CompletedSuccess,
        100,
    );
    bf.end_time = Some(2000);
    storage.create_backfill(&bf).await.unwrap();

    let status = storage.cancel_backfill("bf-done").await.unwrap();
    assert_eq!(status, BackfillStatus::CompletedSuccess);
    let record = storage.get_backfill("bf-done").await.unwrap().unwrap();
    assert_eq!(record.status, BackfillStatus::CompletedSuccess);
}

#[tokio::test]
async fn test_cancel_backfill_cancels_live_backfill() {
    let storage = make_storage().await;
    let mut bf = mk_isolation_backfill(
        "bf-live",
        DEFAULT_CODE_LOCATION_ID,
        BackfillStatus::InProgress,
        100,
    );
    bf.run_ids = vec!["bfl1".into()];
    storage.create_backfill(&bf).await.unwrap();
    storage
        .create_run(&minimal_run("bfl1", RunStatus::Started))
        .await
        .unwrap();

    let status = storage.cancel_backfill("bf-live").await.unwrap();
    assert_eq!(status, BackfillStatus::Canceled);
    let record = storage.get_backfill("bf-live").await.unwrap().unwrap();
    assert_eq!(record.status, BackfillStatus::Canceled);
    assert!(record.end_time.is_some());
}

#[tokio::test]
async fn test_cancel_backfill_requested_flips_to_canceled() {
    let storage = make_storage().await;
    let bf = mk_isolation_backfill(
        "bf-req",
        DEFAULT_CODE_LOCATION_ID,
        BackfillStatus::Requested,
        100,
    );
    storage.create_backfill(&bf).await.unwrap();

    let status = storage.cancel_backfill("bf-req").await.unwrap();
    assert_eq!(status, BackfillStatus::Canceled);
}

#[tokio::test]
async fn test_cancel_backfill_not_found() {
    let storage = make_storage().await;
    assert!(storage.cancel_backfill("nonexistent").await.is_err());
}

#[tokio::test]
async fn test_cancel_queued_run_cancels_not_started() {
    let storage = make_storage().await;
    storage
        .create_run(&minimal_run("ns1", RunStatus::NotStarted))
        .await
        .unwrap();

    let canceled = storage.cancel_queued_run("ns1").await.unwrap();
    assert!(canceled);

    let run = storage.get_run("ns1").await.unwrap().unwrap();
    assert_eq!(run.status, RunStatus::Canceled);
    assert!(run.end_time.is_some());
}

// ── try_start_run ──

#[tokio::test]
async fn test_try_start_run_starts_not_started() {
    let storage = make_storage().await;
    storage
        .create_run(&minimal_run("r1", RunStatus::NotStarted))
        .await
        .unwrap();

    assert!(storage.try_start_run("r1").await.unwrap());

    let run = storage.get_run("r1").await.unwrap().unwrap();
    assert_eq!(run.status, RunStatus::Started);
}

#[tokio::test]
async fn test_try_start_run_refuses_canceled() {
    let storage = make_storage().await;
    let mut record = minimal_run("r1", RunStatus::Canceled);
    record.end_time = Some(2000);
    storage.create_run(&record).await.unwrap();

    assert!(!storage.try_start_run("r1").await.unwrap());

    let run = storage.get_run("r1").await.unwrap().unwrap();
    assert_eq!(run.status, RunStatus::Canceled);
    assert_eq!(run.end_time, Some(2000));
}

#[tokio::test]
async fn test_try_start_run_missing_run_errors() {
    let storage = make_storage().await;
    assert!(storage.try_start_run("nonexistent").await.is_err());
}

/// Concurrent cancel vs start on the same NotStarted run must settle on
/// exactly one winner. Embedded backend — kv-mem misses some write-write
/// conflicts.
#[tokio::test]
async fn test_cancel_vs_start_race_settles_consistently() {
    let temp = test_temp_dir::test_temp_dir!();
    let storage = SurrealStorage::new_embedded(temp.as_path_untracked().to_str().unwrap())
        .await
        .unwrap();
    storage
        .create_run(&minimal_run("r1", RunStatus::NotStarted))
        .await
        .unwrap();

    let (canceled, started) =
        tokio::join!(storage.cancel_queued_run("r1"), storage.try_start_run("r1"));
    let (canceled, started) = (canceled.unwrap(), started.unwrap());
    assert_ne!(canceled, started, "exactly one side must win");

    let run = storage.get_run("r1").await.unwrap().unwrap();
    let expected = if canceled {
        RunStatus::Canceled
    } else {
        RunStatus::Started
    };
    assert_eq!(run.status, expected);
}

// ── get_stalled_not_started_runs ──

#[tokio::test]
async fn test_get_stalled_not_started_runs() {
    let storage = make_storage().await;
    for (id, status) in [
        ("stale", RunStatus::NotStarted),
        ("fresh", RunStatus::NotStarted),
        ("orphan", RunStatus::NotStarted),
        ("waiting", RunStatus::Queued),
        ("running", RunStatus::Started),
    ] {
        let mut record = minimal_run(id, status);
        record.start_time = if id == "orphan" { 2000 } else { 1000 };
        storage.create_run(&record).await.unwrap();
    }
    for (run_id, ts) in [("stale", 5_000i64), ("fresh", 50_000)] {
        storage
            .store_event(&EventRecord {
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::RunDequeued,
                asset_key: None,
                run_id: run_id.to_string(),
                partition_key: None,
                timestamp: ts,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();
    }

    // "stale" dequeued before the cutoff; "orphan" has no RunDequeued
    // event, so its enqueue time counts; "fresh" dequeued after.
    let mut stalled = storage
        .get_stalled_not_started_runs(DEFAULT_CODE_LOCATION_ID, 10_000)
        .await
        .unwrap();
    stalled.sort();
    assert_eq!(stalled, vec!["orphan".to_string(), "stale".to_string()]);
}

#[tokio::test]
async fn test_get_stalled_not_started_runs_scoped_to_cl() {
    let storage = make_storage().await;
    let mut record = minimal_run("other-cl-run", RunStatus::NotStarted);
    record.code_location_id = "other".to_string();
    storage.create_run(&record).await.unwrap();

    let stalled = storage
        .get_stalled_not_started_runs(DEFAULT_CODE_LOCATION_ID, 10_000)
        .await
        .unwrap();
    assert!(stalled.is_empty());

    let stalled = storage
        .get_stalled_not_started_runs("other", 10_000)
        .await
        .unwrap();
    assert_eq!(stalled, vec!["other-cl-run".to_string()]);
}

// ── queued view includes NotStarted ──

#[tokio::test]
async fn test_runs_page_queued_filter_includes_not_started() {
    let storage = make_storage().await;
    for (id, status) in [
        ("w", RunStatus::Queued),
        ("l", RunStatus::NotStarted),
        ("s", RunStatus::Started),
    ] {
        storage.create_run(&minimal_run(id, status)).await.unwrap();
    }

    let filter = RunFilter {
        status: Some(RunStatus::Queued),
        ..Default::default()
    };
    let page = storage.get_all_runs_page(0, 10, &filter).await.unwrap();
    assert_eq!(page.total, 2);
    let mut ids: Vec<_> = page.rows.iter().map(|r| r.run_id.clone()).collect();
    ids.sort();
    assert_eq!(ids, vec!["l".to_string(), "w".to_string()]);

    // Non-Queued filters stay exact.
    let filter = RunFilter {
        status: Some(RunStatus::Started),
        ..Default::default()
    };
    let page = storage.get_all_runs_page(0, 10, &filter).await.unwrap();
    assert_eq!(page.total, 1);
    assert_eq!(page.rows[0].run_id, "s");
}

#[tokio::test]
async fn test_run_launch_failed_event_round_trip() {
    let storage = make_storage().await;
    storage
        .store_event(&EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::RunLaunchFailed,
            asset_key: None,
            run_id: "r1".to_string(),
            partition_key: None,
            timestamp: 1000,
            metadata: vec![("error".to_string(), "launch failed: boom".to_string())],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    let (events, total) = storage
        .get_run_structured_events_page("r1", None, 0, 10)
        .await
        .unwrap();
    assert_eq!(total, 1);
    assert_eq!(events[0].event_type, EventType::RunLaunchFailed);
    assert_eq!(
        events[0].metadata,
        vec![("error".to_string(), "launch failed: boom".to_string())]
    );
}
