use super::*;

// ── Zero-coverage function tests ──

#[tokio::test]
async fn test_step_outcomes_terminal_steps_only() {
    let storage = make_storage().await;
    register(&storage, &["asset_a", "asset_b"]).await;

    // run_1: only StepStart for asset_a
    storage
        .store_event(&EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepStart,
            asset_key: Some("asset_a".to_string()),
            run_id: "run_1".to_string(),
            partition_key: None,
            timestamp: 100,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    // run_2: StepSuccess for asset_a
    storage
        .store_event(&EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepSuccess,
            asset_key: Some("asset_a".to_string()),
            run_id: "run_2".to_string(),
            partition_key: None,
            timestamp: 200,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    // run_3: StepFailure for asset_b
    storage
        .store_event(&EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepFailure,
            asset_key: Some("asset_b".to_string()),
            run_id: "run_3".to_string(),
            partition_key: None,
            timestamp: 300,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    // A returned row is a terminal step, so "completed" is "any row".
    let completed = async |asset: &str, runs: &[String]| {
        !storage
            .step_outcomes(std::slice::from_ref(&asset.to_string()), runs)
            .await
            .unwrap()
            .is_empty()
    };

    // Only StepStart — not completed
    assert!(!completed("asset_a", &["run_1".to_string()]).await);
    // StepSuccess — completed
    assert!(completed("asset_a", &["run_2".to_string()]).await);
    // StepFailure — completed
    assert!(completed("asset_b", &["run_3".to_string()]).await);
    // Unknown run
    assert!(!completed("asset_a", &["run_99".to_string()]).await);
    // Empty slice
    assert!(!completed("asset_a", &[]).await);
    // Multiple runs — finds it in run_2
    assert!(completed("asset_a", &["run_1".to_string(), "run_2".to_string()]).await);

    // Asking about both assets at once must not credit asset_a with
    // asset_b's failure, nor either with the other's run.
    let mut rows: Vec<(String, String, bool)> = storage
        .step_outcomes(
            &["asset_a".to_string(), "asset_b".to_string()],
            &[
                "run_1".to_string(),
                "run_2".to_string(),
                "run_3".to_string(),
            ],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|o| (o.asset_key, o.run_id, o.succeeded))
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            ("asset_a".to_string(), "run_2".to_string(), true),
            ("asset_b".to_string(), "run_3".to_string(), false),
        ]
    );
}

/// Only the runs' own Materialization events count, for the assets asked
/// about, and a later delete of the asset does not erase them.
#[tokio::test]
async fn materialized_by_runs_reads_the_runs_own_materializations() {
    let storage = make_storage().await;
    register(&storage, &["a", "b", "c"]).await;
    let mut deletion = make_event("a", "delete", 400);
    deletion.event_type = EventType::Deletion;
    storage
        .store_events(&[
            make_event("a", "r1", 100),
            make_event("b", "r1", 100),
            EventRecord {
                event_type: EventType::StepSuccess,
                ..make_event("b", "r2", 200)
            },
            make_event("c", "r3", 300),
            deletion,
        ])
        .await
        .unwrap();

    let built = storage
        .materialized_by_runs(
            &["a".to_string(), "b".to_string()],
            &["r1".to_string(), "r2".to_string(), "r3".to_string()],
        )
        .await
        .unwrap();
    assert_eq!(
        built,
        HashMap::from([
            ("a".to_string(), HashSet::from(["r1".to_string()])),
            ("b".to_string(), HashSet::from(["r1".to_string()])),
        ])
    );
}

#[tokio::test]
async fn test_get_asset_records_by_keys() {
    let storage = make_storage().await;
    register(&storage, &["a", "b", "c"]).await;

    let records = storage
        .get_asset_records_by_keys(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &["a".to_string(), "c".to_string()],
        )
        .await
        .unwrap();
    assert_eq!(records.len(), 2);
    let mut keys: Vec<&str> = records.iter().map(|r| r.asset_key.as_str()).collect();
    keys.sort();
    assert_eq!(keys, vec!["a", "c"]);

    // Struct equality
    for r in &records {
        let expected = make_asset_record(&r.asset_key);
        assert_eq!(*r, expected);
    }

    // Unknown keys
    let empty = storage
        .get_asset_records_by_keys(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &["unknown".to_string()],
        )
        .await
        .unwrap();
    assert!(empty.is_empty());

    // Empty slice
    let empty = storage
        .get_asset_records_by_keys(crate::storage::DEFAULT_CODE_LOCATION_ID, &[])
        .await
        .unwrap();
    assert!(empty.is_empty());
}

#[tokio::test]
async fn test_get_runs_by_ids() {
    let storage = make_storage().await;

    let run1 = RunRecord {
        run_id: "run_1".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("j1".to_string()),
        status: RunStatus::Success,
        start_time: 1000,
        end_time: Some(1500),
        tags: vec![],
        node_names: vec!["a".to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    let run2 = RunRecord {
        run_id: "run_2".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("j1".to_string()),
        status: RunStatus::Started,
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
    };
    let run3 = RunRecord {
        run_id: "run_3".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("j2".to_string()),
        status: RunStatus::Failure,
        start_time: 3000,
        end_time: Some(3500),
        tags: vec![("env".to_string(), "prod".to_string())],
        node_names: vec!["b".to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    storage.create_run(&run1).await.unwrap();
    storage.create_run(&run2).await.unwrap();
    storage.create_run(&run3).await.unwrap();

    // Query subset
    let mut results = storage
        .get_runs_by_ids(&["run_1".to_string(), "run_3".to_string()], None)
        .await
        .unwrap();
    results.sort_by(|a, b| a.run_id.cmp(&b.run_id));
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], run1);
    assert_eq!(results[1], run3);

    // With status filter
    let results = storage
        .get_runs_by_ids(
            &["run_1".to_string(), "run_2".to_string()],
            Some(RunStatus::Started),
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], run2);

    // Empty IDs
    let results = storage.get_runs_by_ids(&[], None).await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn test_get_runs_by_ids_orders_by_start_time() {
    let storage = make_storage().await;
    let mk = |id: &str, start: i64| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("j".to_string()),
        status: RunStatus::Success,
        start_time: start,
        end_time: Some(start + 100),
        tags: vec![],
        node_names: vec!["a".to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    storage.create_run(&mk("zzz", 1000)).await.unwrap();
    storage.create_run(&mk("mmm", 2000)).await.unwrap();
    storage.create_run(&mk("aaa", 3000)).await.unwrap();

    let ordered = storage
        .get_runs_by_ids(
            &["aaa".to_string(), "zzz".to_string(), "mmm".to_string()],
            None,
        )
        .await
        .unwrap();
    let start_times: Vec<i64> = ordered.iter().map(|r| r.start_time).collect();
    assert_eq!(
        start_times,
        vec![1000, 2000, 3000],
        "get_runs_by_ids must return runs ordered by start_time ASC"
    );
}

#[tokio::test]
async fn test_get_runs_since() {
    let storage = make_storage().await;

    let run1 = RunRecord {
        run_id: "run_1".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("j".to_string()),
        status: RunStatus::Success,
        start_time: 1000,
        end_time: Some(1500),
        tags: vec![],
        node_names: vec![],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    let run2 = RunRecord {
        run_id: "run_2".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("j".to_string()),
        status: RunStatus::Started,
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
    };
    let run3 = RunRecord {
        run_id: "run_3".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("j".to_string()),
        status: RunStatus::Success,
        start_time: 3000,
        end_time: Some(3500),
        tags: vec![],
        node_names: vec![],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    storage.create_run(&run1).await.unwrap();
    storage.create_run(&run2).await.unwrap();
    storage.create_run(&run3).await.unwrap();

    // Since 1500 — run2 (2000) and run3 (3000), DESC order
    let results = storage.get_all_runs_since(1500, None).await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], run3);
    assert_eq!(results[1], run2);

    // Since 1500 with status filter
    let results = storage
        .get_all_runs_since(1500, Some(RunStatus::Success))
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], run3);

    // Since 5000 — empty
    let results = storage.get_all_runs_since(5000, None).await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn test_store_tick_single() {
    let storage = make_storage().await;

    let tick = TickRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        automation_name: "my_sensor".to_string(),
        automation_type: "Sensor".to_string(),
        status: "Skipped".to_string(),
        timestamp: 5000,
        run_ids: vec!["r1".to_string()],
        backfill_ids: vec![],
        skip_reason: Some("No new data".to_string()),
        error: None,
        cursor: Some("cursor_42".to_string()),
    };
    let id = storage.store_tick(&tick).await.unwrap();
    assert!(!id.is_empty());

    let stored = storage
        .get_ticks(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_sensor", 10)
        .await
        .unwrap();
    assert_eq!(stored.len(), 1);
    let actual = &stored[0];
    let expected = StoredTick {
        id: actual.id.clone(),
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        automation_name: "my_sensor".to_string(),
        automation_type: "Sensor".to_string(),
        status: "Skipped".to_string(),
        timestamp: 5000,
        run_ids: vec!["r1".to_string()],
        backfill_ids: vec![],
        skip_reason: Some("No new data".to_string()),
        error: None,
        cursor: Some("cursor_42".to_string()),
    };
    assert_eq!(*actual, expected);
}

#[tokio::test]
async fn test_prune_ticks() {
    let storage = make_storage().await;

    // Store 10 ticks for sensor_a
    let ticks_a: Vec<TickRecord> = (0..10)
        .map(|i| TickRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            automation_name: "sensor_a".to_string(),
            automation_type: "Sensor".to_string(),
            status: "Success".to_string(),
            timestamp: 1000 + i * 100,
            run_ids: vec![],
            backfill_ids: vec![],
            skip_reason: None,
            error: None,
            cursor: None,
        })
        .collect();
    storage.store_ticks_batch(&ticks_a).await.unwrap();

    // Store 3 ticks for sensor_b
    let ticks_b: Vec<TickRecord> = (0..3)
        .map(|i| TickRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            automation_name: "sensor_b".to_string(),
            automation_type: "Sensor".to_string(),
            status: "Success".to_string(),
            timestamp: 2000 + i * 100,
            run_ids: vec![],
            backfill_ids: vec![],
            skip_reason: None,
            error: None,
            cursor: None,
        })
        .collect();
    storage.store_ticks_batch(&ticks_b).await.unwrap();

    // Prune sensor_a to 3
    let deleted = storage
        .prune_ticks(crate::storage::DEFAULT_CODE_LOCATION_ID, "sensor_a", 3)
        .await
        .unwrap();
    assert_eq!(deleted, 7);

    // Only 3 newest remain
    let remaining = storage
        .get_ticks(crate::storage::DEFAULT_CODE_LOCATION_ID, "sensor_a", 100)
        .await
        .unwrap();
    assert_eq!(remaining.len(), 3);
    assert_eq!(remaining[0].timestamp, 1900); // 1000 + 9*100
    assert_eq!(remaining[1].timestamp, 1800);
    assert_eq!(remaining[2].timestamp, 1700);

    // sensor_b unaffected
    let b_remaining = storage
        .get_ticks(crate::storage::DEFAULT_CODE_LOCATION_ID, "sensor_b", 100)
        .await
        .unwrap();
    assert_eq!(b_remaining.len(), 3);
}

#[tokio::test]
async fn test_store_condition_tick() {
    let storage = make_storage().await;

    let tick = ConditionTickRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        timestamp: 1000,
        total_evaluated: 5,
        total_fired: 2,
        eval_duration_us: 500,
        run_ids: vec!["r1".to_string()],
        backfill_ids: vec![],
    };
    let id = storage.store_condition_tick(&tick).await.unwrap();
    assert!(!id.is_empty());

    let stored = storage
        .get_condition_ticks(crate::storage::DEFAULT_CODE_LOCATION_ID, 10)
        .await
        .unwrap();
    assert_eq!(stored.len(), 1);
    let actual = &stored[0];
    let expected = StoredConditionTick {
        id: actual.id.clone(),
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        timestamp: 1000,
        total_evaluated: 5,
        total_fired: 2,
        eval_duration_us: 500,
        run_ids: vec!["r1".to_string()],
        backfill_ids: vec![],
    };
    assert_eq!(*actual, expected);
}

#[tokio::test]
async fn test_get_condition_ticks_ordering_and_limit() {
    let storage = make_storage().await;

    for i in 0..5 {
        storage
            .store_condition_tick(&ConditionTickRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                timestamp: 100 + i * 100,
                total_evaluated: i as u32,
                total_fired: 0,
                eval_duration_us: 50,
                run_ids: vec![],
                backfill_ids: vec![],
            })
            .await
            .unwrap();
    }

    // Limit 3 — ordered DESC
    let stored = storage
        .get_condition_ticks(crate::storage::DEFAULT_CODE_LOCATION_ID, 3)
        .await
        .unwrap();
    assert_eq!(stored.len(), 3);
    assert_eq!(stored[0].timestamp, 500); // 100 + 4*100
    assert_eq!(stored[1].timestamp, 400);
    assert_eq!(stored[2].timestamp, 300);

    // Struct equality on each
    for (idx, actual) in stored.iter().enumerate() {
        let i = 4 - idx; // maps to original i
        let expected = StoredConditionTick {
            id: actual.id.clone(),
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            timestamp: 100 + (i as i64) * 100,
            total_evaluated: i as u32,
            total_fired: 0,
            eval_duration_us: 50,
            run_ids: vec![],
            backfill_ids: vec![],
        };
        assert_eq!(*actual, expected);
    }
}

/// Pruning keeps the newest ticks and takes each dropped tick's
/// evaluations with it. An evaluation the UI cannot join back to a tick
/// shows no runs, so outliving the tick buys nothing.
#[tokio::test]
async fn test_prune_condition_history_drops_evals_with_their_tick() {
    let storage = make_storage().await;
    let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;

    let mut tick_ids = Vec::new();
    for i in 0..10 {
        let tick_id = storage
            .store_condition_tick(&ConditionTickRecord {
                code_location_id: cl.to_string(),
                timestamp: 1000 + i * 100,
                total_evaluated: 2,
                total_fired: 0,
                eval_duration_us: 10,
                run_ids: vec![],
                backfill_ids: vec![],
            })
            .await
            .unwrap();
        let evals: Vec<ConditionEvalRecord> = ["asset_a", "asset_b"]
            .iter()
            .map(|key| ConditionEvalRecord {
                code_location_id: cl.to_string(),
                asset_key: (*key).to_string(),
                tick_id: tick_id.clone(),
                timestamp: 1000 + i * 100,
                fired: false,
                eval_duration_us: 5,
                run_ids: vec![],
                tree_json: b"{}".to_vec(),
                selection_json: None,
            })
            .collect();
        storage.store_condition_evals_batch(&evals).await.unwrap();
        tick_ids.push(tick_id);
    }

    let deleted = storage.prune_condition_history(cl, 3).await.unwrap();
    assert_eq!(deleted, 7);

    let remaining = storage.get_condition_ticks(cl, 100).await.unwrap();
    assert_eq!(remaining.len(), 3);
    assert_eq!(remaining[0].timestamp, 1900);
    assert_eq!(remaining[1].timestamp, 1800);
    assert_eq!(remaining[2].timestamp, 1700);

    for asset in ["asset_a", "asset_b"] {
        let evals = storage.get_condition_evals(cl, asset, 100).await.unwrap();
        assert_eq!(
            evals.len(),
            3,
            "{asset} must keep exactly the evals of the retained ticks"
        );
        let kept: Vec<i64> = evals.iter().map(|e| e.timestamp).collect();
        assert_eq!(kept, vec![1900, 1800, 1700]);
    }
    for dropped in &tick_ids[..7] {
        assert!(
            storage
                .get_condition_evals_for_tick(cl, dropped)
                .await
                .unwrap()
                .is_empty(),
            "a dropped tick must leave no evals behind"
        );
    }
}

#[tokio::test]
async fn test_get_condition_evals_for_tick() {
    let storage = make_storage().await;

    let tree_json = b"{}".to_vec();

    // 3 evals for tick t1 with different asset keys
    let evals_t1: Vec<ConditionEvalRecord> = ["asset_a", "asset_c", "asset_b"]
        .iter()
        .enumerate()
        .map(|(i, key)| ConditionEvalRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: key.to_string(),
            tick_id: "t1".to_string(),
            timestamp: 1000 + i as i64 * 10,
            fired: i == 0,
            eval_duration_us: 50,
            run_ids: vec![],
            tree_json: tree_json.clone(),
            selection_json: None,
        })
        .collect();
    storage
        .store_condition_evals_batch(&evals_t1)
        .await
        .unwrap();

    // 2 evals for tick t2
    let evals_t2: Vec<ConditionEvalRecord> = ["asset_x", "asset_y"]
        .iter()
        .map(|key| ConditionEvalRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: key.to_string(),
            tick_id: "t2".to_string(),
            timestamp: 2000,
            fired: false,
            eval_duration_us: 30,
            run_ids: vec![],
            tree_json: tree_json.clone(),
            selection_json: None,
        })
        .collect();
    storage
        .store_condition_evals_batch(&evals_t2)
        .await
        .unwrap();

    // Query for tick t1 — ordered ASC by asset_key
    let t1_results = storage
        .get_condition_evals_for_tick(crate::storage::DEFAULT_CODE_LOCATION_ID, "t1")
        .await
        .unwrap();
    assert_eq!(t1_results.len(), 3);
    assert_eq!(t1_results[0].asset_key, "asset_a");
    assert_eq!(t1_results[1].asset_key, "asset_b");
    assert_eq!(t1_results[2].asset_key, "asset_c");
    // All belong to t1
    for e in &t1_results {
        assert_eq!(e.tick_id, "t1");
    }

    // Query for tick t2
    let t2_results = storage
        .get_condition_evals_for_tick(crate::storage::DEFAULT_CODE_LOCATION_ID, "t2")
        .await
        .unwrap();
    assert_eq!(t2_results.len(), 2);

    // Unknown tick
    let empty = storage
        .get_condition_evals_for_tick(crate::storage::DEFAULT_CODE_LOCATION_ID, "t99")
        .await
        .unwrap();
    assert!(empty.is_empty());
}

#[tokio::test]
async fn test_get_partition_events() {
    let storage = make_storage().await;
    register(&storage, &["asset"]).await;

    // Two events for partition p1 at different times
    for (ts, run) in [(100, "r1"), (200, "r2")] {
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization { data_version: None },
                asset_key: Some("asset".to_string()),
                run_id: run.to_string(),
                partition_key: Some(PartitionKey::Single {
                    keys: vec!["p1".to_string()],
                }),
                timestamp: ts,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();
    }
    // One event for partition p2
    storage
        .store_event(&EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization { data_version: None },
            asset_key: Some("asset".to_string()),
            run_id: "r3".to_string(),
            partition_key: Some(PartitionKey::Single {
                keys: vec!["p2".to_string()],
            }),
            timestamp: 300,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    // Query p1 — 2 events, DESC by timestamp
    let p1_events = storage
        .get_partition_events(crate::storage::DEFAULT_CODE_LOCATION_ID, "asset", "p1", 10)
        .await
        .unwrap();
    assert_eq!(p1_events.len(), 2);
    assert_eq!(p1_events[0].timestamp, 200);
    assert_eq!(p1_events[1].timestamp, 100);

    // Query p2 — 1 event
    let p2_events = storage
        .get_partition_events(crate::storage::DEFAULT_CODE_LOCATION_ID, "asset", "p2", 10)
        .await
        .unwrap();
    assert_eq!(p2_events.len(), 1);
    assert_eq!(p2_events[0].run_id, "r3");
}

#[tokio::test]
async fn test_get_materialized_partitions() {
    let storage = make_storage().await;
    register(&storage, &["asset", "empty_asset"]).await;

    // Store materialization events with partitions
    for pk in ["p1", "p2", "p3"] {
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization { data_version: None },
                asset_key: Some("asset".to_string()),
                run_id: "r1".to_string(),
                partition_key: Some(PartitionKey::Single {
                    keys: vec![pk.to_string()],
                }),
                timestamp: 1000,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();
    }

    let partitions = storage
        .get_materialized_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "asset")
        .await
        .unwrap();
    assert_eq!(partitions.len(), 3);
    let mut pk_strs: Vec<String> = partitions
        .iter()
        .map(|pk| match pk {
            PartitionKey::Single { keys } => keys[0].clone(),
            _ => panic!("expected Single partition key"),
        })
        .collect();
    pk_strs.sort();
    assert_eq!(pk_strs, vec!["p1", "p2", "p3"]);

    // Empty asset
    let empty = storage
        .get_materialized_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "empty_asset")
        .await
        .unwrap();
    assert!(empty.is_empty());
}

#[tokio::test]
async fn test_get_partition_timestamps() {
    let storage = make_storage().await;
    register(&storage, &["asset"]).await;

    // p1 at ts=1000, then p1 again at ts=2000 (should keep latest)
    for ts in [1000, 2000] {
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization { data_version: None },
                asset_key: Some("asset".to_string()),
                run_id: "r1".to_string(),
                partition_key: Some(PartitionKey::Single {
                    keys: vec!["p1".to_string()],
                }),
                timestamp: ts,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();
    }
    // p2 at ts=1500
    storage
        .store_event(&EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization { data_version: None },
            asset_key: Some("asset".to_string()),
            run_id: "r2".to_string(),
            partition_key: Some(PartitionKey::Single {
                keys: vec!["p2".to_string()],
            }),
            timestamp: 1500,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    let mut timestamps = storage
        .get_partition_timestamps(crate::storage::DEFAULT_CODE_LOCATION_ID, "asset")
        .await
        .unwrap();
    timestamps.sort_by_key(|(pk, _)| match pk {
        PartitionKey::Single { keys } => keys[0].clone(),
        _ => String::new(),
    });
    assert_eq!(timestamps.len(), 2);
    // p1 → latest is 2000
    assert_eq!(
        timestamps[0].0,
        PartitionKey::Single {
            keys: vec!["p1".to_string()]
        }
    );
    assert_eq!(timestamps[0].1, 2000);
    // p2 → 1500
    assert_eq!(
        timestamps[1].0,
        PartitionKey::Single {
            keys: vec!["p2".to_string()]
        }
    );
    assert_eq!(timestamps[1].1, 1500);
}

#[tokio::test]
async fn test_get_partition_timestamps_for_keys_skips_null_timestamps() {
    // `last_timestamp` is `option<int>` in the schema, so an unset row is
    // schema-legal and reads back as NONE. Every reader here deserializes
    // into a non-Option `i64`, so one such row fails the whole
    // condition-cache refresh tick for the asset rather than one partition.
    // `IS NOT NULL` does *not* exclude NONE in SurrealDB — the guard has to
    // be `IS NOT NONE`, which is why both readers are asserted below.
    let storage = make_storage().await;
    register(&storage, &["asset"]).await;

    let single = |k: &str| PartitionKey::Single {
        keys: vec![k.to_string()],
    };
    for (pk, ts) in [("p1", 1000), ("p2", 1500)] {
        storage
            .store_event(&EventRecord {
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization { data_version: None },
                asset_key: Some("asset".to_string()),
                run_id: "r1".to_string(),
                partition_key: Some(single(pk)),
                timestamp: ts,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();
    }

    // Legacy/foreign writer shape: the row exists with no timestamp.
    storage
        .db
        .query(
            "UPDATE asset_partitions SET last_timestamp = NONE \
                 WHERE code_location_id = $cl AND asset_key = 'asset' \
                 AND partition_key = $pk",
        )
        .bind(("cl", DEFAULT_CODE_LOCATION_ID.to_string()))
        .bind(("pk", single("p1")))
        .await
        .unwrap()
        .check()
        .unwrap();

    let timestamps = storage
        .get_partition_timestamps_for_keys(
            DEFAULT_CODE_LOCATION_ID,
            "asset",
            &[single("p1"), single("p2")],
        )
        .await
        .expect("an unset last_timestamp must not fail the whole keyed read");
    assert_eq!(
        timestamps,
        vec![(single("p2"), 1500, Some("r1".to_string()))],
        "the unset row must be skipped, the real one still returned"
    );

    let all = storage
        .get_partition_timestamps(DEFAULT_CODE_LOCATION_ID, "asset")
        .await
        .expect("an unset last_timestamp must not fail the whole-asset read");
    assert_eq!(
        all,
        vec![(single("p2"), 1500)],
        "the whole-asset reader needs the same NONE guard"
    );
}

#[tokio::test]
async fn test_get_in_progress_partitions() {
    let storage = make_storage().await;
    register(&storage, &["asset"]).await;

    // Create a run in Started status
    let run = RunRecord {
        run_id: "run_ip".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("j".to_string()),
        status: RunStatus::Started,
        start_time: 1000,
        end_time: None,
        tags: vec![],
        node_names: vec!["asset".to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    storage.create_run(&run).await.unwrap();

    // Store StepStart events with partition keys
    for pk in ["p1", "p2"] {
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepStart,
                asset_key: Some("asset".to_string()),
                run_id: "run_ip".to_string(),
                partition_key: Some(PartitionKey::Single {
                    keys: vec![pk.to_string()],
                }),
                timestamp: 1000,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();
    }

    let in_progress = storage
        .get_in_progress_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "asset")
        .await
        .unwrap();
    assert_eq!(in_progress.len(), 2);
    let mut pk_strs: Vec<String> = in_progress
        .iter()
        .map(|pk| match pk {
            PartitionKey::Single { keys } => keys[0].clone(),
            _ => panic!("expected Single partition key"),
        })
        .collect();
    pk_strs.sort();
    assert_eq!(pk_strs, vec!["p1", "p2"]);

    // Mark run as Success → no longer in progress
    storage
        .update_run_status("run_ip", RunStatus::Success, Some(2000))
        .await
        .unwrap();
    let in_progress = storage
        .get_in_progress_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "asset")
        .await
        .unwrap();
    assert!(in_progress.is_empty());
}

#[tokio::test]
async fn test_get_in_progress_partitions_ignores_action_runs() {
    // An in-flight `optimize` is not an in-flight *materialization*. Counting
    // it here makes `eager()`'s `!in_flight()` (and `!any_deps_in_progress()`)
    // suppress the asset and every dependent for the action's whole duration.
    // Mirrors the action filter `get_failed_partitions` already applies.
    let storage = make_storage().await;
    register(&storage, &["asset"]).await;

    let single = |k: &str| PartitionKey::Single {
        keys: vec![k.to_string()],
    };
    let run = |id: &str, action: Option<&str>| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: None,
        status: RunStatus::Started,
        start_time: 1000,
        end_time: None,
        tags: vec![],
        node_names: vec!["asset".to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: action.map(str::to_string),
        config: None,
    };

    storage
        .create_run(&run("run_opt", Some("optimize")))
        .await
        .unwrap();
    storage.create_run(&run("run_mat", None)).await.unwrap();

    for (run_id, pk) in [("run_opt", "p1"), ("run_mat", "p2")] {
        storage
            .store_event(&EventRecord {
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepStart,
                asset_key: Some("asset".to_string()),
                run_id: run_id.to_string(),
                partition_key: Some(single(pk)),
                timestamp: 1000,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();
    }

    let in_progress = storage
        .get_in_progress_partitions(DEFAULT_CODE_LOCATION_ID, "asset")
        .await
        .unwrap();
    let keys: Vec<String> = in_progress
        .iter()
        .map(|pk| match pk {
            PartitionKey::Single { keys } => keys[0].clone(),
            other => panic!("expected Single, got {other:?}"),
        })
        .collect();
    assert_eq!(
        keys,
        vec!["p2"],
        "an in-flight action must not count as an in-flight materialization"
    );
}

#[tokio::test]
async fn test_get_failed_partitions() {
    let storage = make_storage().await;
    register(&storage, &["asset"]).await;

    // Create a run in Failure status
    let run = RunRecord {
        run_id: "run_fail".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("j".to_string()),
        status: RunStatus::Failure,
        start_time: 1000,
        end_time: Some(1500),
        tags: vec![],
        node_names: vec!["asset".to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    storage.create_run(&run).await.unwrap();

    // Store StepFailure events with partition keys
    for pk in ["p1", "p2"] {
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepFailure,
                asset_key: Some("asset".to_string()),
                run_id: "run_fail".to_string(),
                partition_key: Some(PartitionKey::Single {
                    keys: vec![pk.to_string()],
                }),
                timestamp: 1000,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();
    }

    let failed = storage
        .get_failed_partitions(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "asset",
            &std::collections::HashMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(failed.len(), 2);
    let mut pk_strs: Vec<String> = failed
        .keys()
        .map(|pk| match pk {
            PartitionKey::Single { keys } => keys[0].clone(),
            _ => panic!("expected Single partition key"),
        })
        .collect();
    pk_strs.sort();
    assert_eq!(pk_strs, vec!["p1", "p2"]);

    // Asset with no failures
    register(&storage, &["clean_asset"]).await;
    let empty = storage
        .get_failed_partitions(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "clean_asset",
            &std::collections::HashMap::new(),
        )
        .await
        .unwrap();
    assert!(empty.is_empty());
}

/// A late launch-error must not stomp a terminal status written by a
/// concurrent canceller — the fail-out only lands on active runs.
#[tokio::test]
async fn fail_run_if_active_leaves_terminal_runs_alone() {
    let storage = make_storage().await;

    let mut canceled = RunRecord {
        run_id: "gone".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: None,
        status: RunStatus::Canceled,
        start_time: 100,
        end_time: Some(150),
        tags: vec![],
        node_names: vec!["a".to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    storage.create_run(&canceled).await.unwrap();
    assert!(
        !storage.fail_run_if_active("gone", 200).await.unwrap(),
        "terminal run must be left alone"
    );
    let rec = storage.get_run("gone").await.unwrap().unwrap();
    assert_eq!(rec.status, RunStatus::Canceled, "status stomped");
    assert_eq!(rec.end_time, Some(150));

    canceled.run_id = "fresh".to_string();
    canceled.status = RunStatus::NotStarted;
    canceled.end_time = None;
    storage.create_run(&canceled).await.unwrap();
    assert!(
        storage.fail_run_if_active("fresh", 200).await.unwrap(),
        "active run must fail out"
    );
    let rec = storage.get_run("fresh").await.unwrap().unwrap();
    assert_eq!(rec.status, RunStatus::Failure);
    assert_eq!(rec.end_time, Some(200));
}

/// Status-only keyed failures floor at run END: a whole-asset deletion
/// landing mid-run (start < deletion < end) must not outrank the failure —
/// every other reader of the supersession rule uses the end-time basis.
#[tokio::test]
async fn deletion_during_a_run_does_not_outrank_its_failure() {
    let storage = make_storage().await;
    register(&storage, &["orders"]).await;

    let run = RunRecord {
        run_id: "late_fail".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: None,
        status: RunStatus::Failure,
        start_time: 90,
        end_time: Some(110),
        tags: vec![],
        node_names: vec!["orders".to_string()],
        priority: 0,
        partition_key: Some(PartitionKey::Single {
            keys: vec!["p1".to_string()],
        }),
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    storage.create_run(&run).await.unwrap();

    let mut deletion = make_event("orders", "cleanup", 100);
    deletion.event_type = EventType::Deletion;
    storage.store_event(&deletion).await.unwrap();

    let failed = storage
        .get_failed_partitions(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "orders",
            &std::collections::HashMap::new(),
        )
        .await
        .unwrap();
    assert!(
        failed.contains_key(&PartitionKey::Single {
            keys: vec!["p1".to_string()]
        }),
        "a deletion at 100 must not clear a failure that ended at 110"
    );
}

/// A joint run that materialized `raw`'s partition, then failed on `clean`,
/// did not fail `raw`'s partition: the end-time floor would otherwise
/// outrank the run's own materialization and stop `eager()` for good.
#[tokio::test]
async fn a_failed_run_does_not_fail_the_partitions_it_materialized() {
    let storage = make_storage().await;
    register(&storage, &["raw", "clean"]).await;
    let p1 = PartitionKey::Single {
        keys: vec!["p1".to_string()],
    };

    let run = RunRecord {
        run_id: "joint".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: None,
        status: RunStatus::Failure,
        start_time: 90,
        end_time: Some(110),
        tags: vec![],
        node_names: vec!["raw".to_string(), "clean".to_string()],
        priority: 0,
        partition_key: Some(p1.clone()),
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    storage.create_run(&run).await.unwrap();

    let mut mat = make_event("raw", "joint", 100);
    mat.partition_key = Some(p1.clone());
    storage.store_event(&mat).await.unwrap();

    let raw_failed = storage
        .get_failed_partitions(
            DEFAULT_CODE_LOCATION_ID,
            "raw",
            &std::collections::HashMap::from([(p1.clone(), 100)]),
        )
        .await
        .unwrap();
    assert!(
        !raw_failed.contains_key(&p1),
        "raw/p1 materialized in the run that later failed on clean"
    );

    let clean_failed = storage
        .get_failed_partitions(
            DEFAULT_CODE_LOCATION_ID,
            "clean",
            &std::collections::HashMap::new(),
        )
        .await
        .unwrap();
    assert!(clean_failed.contains_key(&p1), "clean really failed");
}

/// The failed run's own events say which keys it built, not the key's
/// row: a delete, or another run's newer materialization, of `raw/p1`
/// after the run built it must not bring the run's floor back. `raw/p2`,
/// which the run did not build, keeps its floor.
#[tokio::test]
async fn a_failed_run_does_not_fail_the_partitions_it_materialized_after_they_moved() {
    let single = |k: &str| PartitionKey::Single {
        keys: vec![k.to_string()],
    };
    let run =
        |run_id: &str, status, start, end, nodes: &[&str], pk: Option<PartitionKey>| RunRecord {
            run_id: run_id.to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: None,
            status,
            start_time: start,
            end_time: Some(end),
            tags: vec![],
            node_names: nodes.iter().map(|n| n.to_string()).collect(),
            priority: 0,
            partition_key: pk,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        };
    for case in [
        "keyed delete",
        "whole-asset delete",
        "rebuilt by another run",
    ] {
        let temp = test_temp_dir::test_temp_dir!();
        let storage = SurrealStorage::new_embedded(temp.as_path_untracked().to_str().unwrap())
            .await
            .unwrap();
        register(&storage, &["raw", "clean"]).await;

        let joint = PartitionKey::Single {
            keys: vec!["p1".to_string(), "p2".to_string()],
        };
        storage
            .create_run(&run(
                "joint",
                RunStatus::Failure,
                50,
                200,
                &["raw", "clean"],
                Some(joint),
            ))
            .await
            .unwrap();
        let mut built = make_event("raw", "joint", 100);
        built.partition_key = Some(single("p1"));
        storage.store_event(&built).await.unwrap();

        if case == "rebuilt by another run" {
            storage
                .create_run(&run(
                    "other",
                    RunStatus::Success,
                    140,
                    150,
                    &["raw"],
                    Some(single("p1")),
                ))
                .await
                .unwrap();
            let mut rebuilt = make_event("raw", "other", 150);
            rebuilt.partition_key = Some(single("p1"));
            storage.store_event(&rebuilt).await.unwrap();
        } else {
            let pk = (case == "keyed delete").then(|| single("p1"));
            let mut delete = run("delete", RunStatus::Success, 140, 150, &["raw"], pk.clone());
            delete.action = Some("delete".to_string());
            storage.create_run(&delete).await.unwrap();
            let mut deletion = make_event("raw", "delete", 150);
            deletion.event_type = EventType::Deletion;
            deletion.partition_key = pk;
            storage.store_event(&deletion).await.unwrap();
        }

        let materialized: std::collections::HashMap<PartitionKey, i64> = storage
            .get_partition_timestamps(DEFAULT_CODE_LOCATION_ID, "raw")
            .await
            .unwrap()
            .into_iter()
            .collect();
        let raw_failed = storage
            .get_failed_partitions(DEFAULT_CODE_LOCATION_ID, "raw", &materialized)
            .await
            .unwrap();
        assert_eq!(
            raw_failed,
            std::collections::HashMap::from([(single("p2"), 200)]),
            "{case}: the failed run built raw/p1, not raw/p2"
        );

        let clean_failed = storage
            .get_failed_partitions(
                DEFAULT_CODE_LOCATION_ID,
                "clean",
                &std::collections::HashMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            clean_failed,
            std::collections::HashMap::from([(single("p1"), 200), (single("p2"), 200)]),
            "{case}: clean really failed"
        );
    }
}

#[tokio::test]
async fn test_get_failed_partitions_includes_marked_in_success_run() {
    let storage = make_storage().await;
    register(&storage, &["asset"]).await;

    let run = RunRecord {
        run_id: "run_ok".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: None,
        status: RunStatus::Success,
        start_time: 1000,
        end_time: Some(1500),
        tags: vec![],
        node_names: vec!["asset".to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    storage.create_run(&run).await.unwrap();

    storage
        .store_event(&EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepFailure,
            asset_key: Some("asset".to_string()),
            run_id: "run_ok".to_string(),
            partition_key: Some(PartitionKey::Single {
                keys: vec!["b".to_string()],
            }),
            timestamp: 1000,
            metadata: vec![("error".to_string(), "boom".to_string())],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    let failed = storage
        .get_failed_partitions(
            DEFAULT_CODE_LOCATION_ID,
            "asset",
            &std::collections::HashMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(failed.len(), 1);
    assert!(failed.contains_key(&PartitionKey::Single {
        keys: vec!["b".to_string()]
    }));
}

#[tokio::test]
async fn test_get_failed_partitions_uses_latest_event_per_partition() {
    // Latest event wins: failed-then-materialized clears; materialized-then-failed stays failed.
    let storage = make_storage().await;
    register(&storage, &["asset"]).await;

    let run = RunRecord {
        run_id: "run_ok".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: None,
        status: RunStatus::Success,
        start_time: 1000,
        end_time: Some(2000),
        tags: vec![],
        node_names: vec!["asset".to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    storage.create_run(&run).await.unwrap();

    let single = |k: &str| PartitionKey::Single {
        keys: vec![k.to_string()],
    };
    let event = |event_type: EventType, pk: &str, ts: i64| EventRecord {
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type,
        asset_key: Some("asset".to_string()),
        run_id: "run_ok".to_string(),
        partition_key: Some(single(pk)),
        timestamp: ts,
        metadata: vec![],
        input_data_versions: vec![],
    };
    let mat = || EventType::Materialization {
        data_version: Some("v".to_string()),
    };

    // b: fail@1000 then materialize@1500 → cleared.
    storage
        .store_event(&event(EventType::StepFailure, "b", 1000))
        .await
        .unwrap();
    storage.store_event(&event(mat(), "b", 1500)).await.unwrap();
    // c: materialize@1000 then fail@1500 → still failed.
    storage.store_event(&event(mat(), "c", 1000)).await.unwrap();
    storage
        .store_event(&event(EventType::StepFailure, "c", 1500))
        .await
        .unwrap();

    // Supersede uses the caller's materialization map (as the condition cache does).
    let materialized: std::collections::HashMap<_, _> = storage
        .get_partition_timestamps(DEFAULT_CODE_LOCATION_ID, "asset")
        .await
        .unwrap()
        .into_iter()
        .collect();
    let failed = storage
        .get_failed_partitions(DEFAULT_CODE_LOCATION_ID, "asset", &materialized)
        .await
        .unwrap();
    assert_eq!(failed.len(), 1, "only c (latest event = failure)");
    assert!(failed.contains_key(&single("c")));
}

#[tokio::test]
async fn test_get_failed_partitions_ignores_step_level_failures() {
    // A whole-step raise emits a None-keyed StepFailure; it must be excluded
    // (not reported, and not breaking FailRow deserialization).
    let storage = make_storage().await;
    register(&storage, &["asset"]).await;

    let run = RunRecord {
        run_id: "run_fail".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: None,
        status: RunStatus::Failure,
        start_time: 1000,
        end_time: Some(1500),
        tags: vec![],
        node_names: vec!["asset".to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    storage.create_run(&run).await.unwrap();

    // step-level failure (a raise) — partition_key None
    storage
        .store_event(&EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepFailure,
            asset_key: Some("asset".to_string()),
            run_id: "run_fail".to_string(),
            partition_key: None,
            timestamp: 1000,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();
    // per-partition failure (a mark) — partition_key Some(b)
    storage
        .store_event(&EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepFailure,
            asset_key: Some("asset".to_string()),
            run_id: "run_fail".to_string(),
            partition_key: Some(PartitionKey::Single {
                keys: vec!["b".to_string()],
            }),
            timestamp: 1000,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    let failed = storage
        .get_failed_partitions(
            DEFAULT_CODE_LOCATION_ID,
            "asset",
            &std::collections::HashMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        failed.len(),
        1,
        "only the per-partition failure; None-keyed step-level failure must be excluded"
    );
    assert!(failed.contains_key(&PartitionKey::Single {
        keys: vec!["b".to_string()]
    }));
}

#[tokio::test]
async fn test_get_failed_partitions_expands_set_failure() {
    // A raised batch records one Set-keyed StepFailure; expanded back to members here.
    let storage = make_storage().await;
    register(&storage, &["asset"]).await;

    let run = RunRecord {
        run_id: "run_fail".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: None,
        status: RunStatus::Failure,
        start_time: 1000,
        end_time: Some(1500),
        tags: vec![],
        node_names: vec!["asset".to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    storage.create_run(&run).await.unwrap();

    let single = |k: &str| PartitionKey::Single {
        keys: vec![k.to_string()],
    };
    storage
        .store_event(&EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepFailure,
            asset_key: Some("asset".to_string()),
            run_id: "run_fail".to_string(),
            partition_key: Some(PartitionKey::Set {
                keys: vec![single("a"), single("b"), single("c")],
            }),
            timestamp: 1000,
            metadata: vec![("error".to_string(), "boom".to_string())],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    let failed = storage
        .get_failed_partitions(
            DEFAULT_CODE_LOCATION_ID,
            "asset",
            &std::collections::HashMap::new(),
        )
        .await
        .unwrap();
    let mut keys: Vec<String> = failed
        .keys()
        .map(|pk| match pk {
            PartitionKey::Single { keys } => keys[0].clone(),
            _ => panic!("expected Single after expansion"),
        })
        .collect();
    keys.sort();
    assert_eq!(
        keys,
        vec!["a", "b", "c"],
        "Set failure expands to its members"
    );
}

#[tokio::test]
async fn test_get_failed_partitions_ignores_action_runs() {
    // A failed `delete` did not fail to *materialize* anything. Both sources
    // this reads — keyed StepFailure events and Failure run records — must
    // skip action runs, or the partition takes a materialization floor that
    // only a materialization can clear (and the floor suppresses it).
    let storage = make_storage().await;
    register(&storage, &["asset"]).await;

    let single = |k: &str| PartitionKey::Single {
        keys: vec![k.to_string()],
    };
    let run = |id: &str, action: Option<&str>, status, pk: &str| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: None,
        status,
        start_time: 1000,
        end_time: Some(1500),
        tags: vec![],
        node_names: vec!["asset".to_string()],
        priority: 0,
        partition_key: Some(single(pk)),
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: action.map(str::to_string),
        config: None,
    };

    // A failed partitioned `delete` — floors p1 via the runs query.
    storage
        .create_run(&run("run_del", Some("delete"), RunStatus::Failure, "p1"))
        .await
        .unwrap();
    // A *successful* batched `delete` that marked p2 failed — floors p2 via
    // the events query. The event itself is legitimate (backfill accounting
    // reads it); only the failure-floor reader must ignore it.
    storage
        .create_run(&run("run_mark", Some("delete"), RunStatus::Success, "p2"))
        .await
        .unwrap();
    storage
        .store_event(&EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepFailure,
            asset_key: Some("asset".to_string()),
            run_id: "run_mark".to_string(),
            partition_key: Some(single("p2")),
            timestamp: 1000,
            metadata: vec![("error".to_string(), "corrupt".to_string())],
            input_data_versions: vec![],
        })
        .await
        .unwrap();
    // A real materialization failure on p3 — must still be reported.
    storage
        .create_run(&run("run_mat", None, RunStatus::Failure, "p3"))
        .await
        .unwrap();
    storage
        .store_event(&EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepFailure,
            asset_key: Some("asset".to_string()),
            run_id: "run_mat".to_string(),
            partition_key: Some(single("p3")),
            timestamp: 1000,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    let failed = storage
        .get_failed_partitions(
            DEFAULT_CODE_LOCATION_ID,
            "asset",
            &std::collections::HashMap::new(),
        )
        .await
        .unwrap();
    let mut keys: Vec<String> = failed
        .keys()
        .map(|pk| match pk {
            PartitionKey::Single { keys } => keys[0].clone(),
            other => panic!("expected Single, got {other:?}"),
        })
        .collect();
    keys.sort();
    assert_eq!(
        keys,
        vec!["p3"],
        "only the materialization failure floors a partition; \
             action runs (failed or marking) must not"
    );
}

#[tokio::test]
async fn test_new_memory_schema_tables() {
    let storage = make_storage().await;

    // Verify all 8 tables exist by querying INFO FOR DB
    let mut result = storage.db.query("INFO FOR DB").await.unwrap();
    let info: Option<serde_json::Value> = result.take(0).unwrap();
    let info = info.unwrap();
    let tables = info["tables"].as_object().unwrap();

    let expected_tables = [
        "events",
        "assets",
        "asset_partitions",
        "runs",
        "kv",
        "dynamic_partitions",
        "ticks",
        "condition_ticks",
        "condition_evals",
    ];
    for table in expected_tables {
        assert!(tables.contains_key(table), "missing table: {table}");
    }
}

#[tokio::test]
async fn test_new_memory_schema_indexes() {
    let storage = make_storage().await;

    let mut result = storage.db.query("INFO FOR TABLE events").await.unwrap();
    let info: Option<serde_json::Value> = result.take(0).unwrap();
    let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
    for idx in [
        "idx_events_run",
        "idx_events_type",
        "idx_events_run_type",
        "idx_events_run_ts",
        "idx_events_loc_asset",
        "idx_events_loc_asset_part",
        "idx_events_loc_asset_type",
        "idx_events_loc_asset_ts",
    ] {
        assert!(indexes.contains_key(idx), "events missing index: {idx}");
    }

    // assets: 3 indexes — composite (loc, key) UNIQUE + composite (loc, group) + (loc).
    let mut result = storage.db.query("INFO FOR TABLE assets").await.unwrap();
    let info: Option<serde_json::Value> = result.take(0).unwrap();
    let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
    for idx in [
        "idx_assets_loc_key",
        "idx_assets_loc_group",
        "idx_assets_loc",
    ] {
        assert!(indexes.contains_key(idx), "assets missing index: {idx}");
    }

    // runs: 6 indexes
    let mut result = storage.db.query("INFO FOR TABLE runs").await.unwrap();
    let info: Option<serde_json::Value> = result.take(0).unwrap();
    let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
    for idx in [
        "idx_runs_status",
        "idx_runs_job",
        "idx_runs_id",
        "idx_runs_start_time",
        "idx_runs_priority",
        "idx_runs_job_time",
    ] {
        assert!(indexes.contains_key(idx), "runs missing index: {idx}");
    }

    // kv: 1 index
    let mut result = storage.db.query("INFO FOR TABLE kv").await.unwrap();
    let info: Option<serde_json::Value> = result.take(0).unwrap();
    let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
    assert!(
        indexes.contains_key("idx_kv_key"),
        "kv missing index: idx_kv_key"
    );

    // dynamic_partitions: 2 indexes
    let mut result = storage
        .db
        .query("INFO FOR TABLE dynamic_partitions")
        .await
        .unwrap();
    let info: Option<serde_json::Value> = result.take(0).unwrap();
    let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
    for idx in ["idx_dyn_part", "idx_dyn_part_unique"] {
        assert!(
            indexes.contains_key(idx),
            "dynamic_partitions missing index: {idx}"
        );
    }

    // ticks: 2 composite indexes (keyed per CL).
    let mut result = storage.db.query("INFO FOR TABLE ticks").await.unwrap();
    let info: Option<serde_json::Value> = result.take(0).unwrap();
    let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
    for idx in ["idx_ticks_loc_name", "idx_ticks_loc_name_ts"] {
        assert!(indexes.contains_key(idx), "ticks missing index: {idx}");
    }

    // condition_ticks: 1 composite index.
    let mut result = storage
        .db
        .query("INFO FOR TABLE condition_ticks")
        .await
        .unwrap();
    let info: Option<serde_json::Value> = result.take(0).unwrap();
    let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
    assert!(
        indexes.contains_key("idx_cond_ticks_loc_ts"),
        "condition_ticks missing index: idx_cond_ticks_loc_ts"
    );

    // condition_evals: 3 indexes (2 composite + tick_id).
    let mut result = storage
        .db
        .query("INFO FOR TABLE condition_evals")
        .await
        .unwrap();
    let info: Option<serde_json::Value> = result.take(0).unwrap();
    let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
    for idx in [
        "idx_cond_evals_loc_key",
        "idx_cond_evals_loc_key_ts",
        "idx_cond_evals_tick",
    ] {
        assert!(
            indexes.contains_key(idx),
            "condition_evals missing index: {idx}"
        );
    }

    // asset_partitions: 1 index
    let mut result = storage
        .db
        .query("INFO FOR TABLE asset_partitions")
        .await
        .unwrap();
    let info: Option<serde_json::Value> = result.take(0).unwrap();
    let indexes = info.unwrap()["indexes"].as_object().unwrap().clone();
    assert!(
        indexes.contains_key("idx_asset_part"),
        "asset_partitions missing index: idx_asset_part"
    );
}

#[tokio::test]
async fn test_new_embedded_schema() {
    let dir = std::env::temp_dir().join(format!("rivers_test_{}", std::process::id()));
    // Clean up from any previous failed run
    let _ = std::fs::remove_dir_all(&dir);

    let storage = SurrealStorage::new_embedded(dir.to_str().unwrap())
        .await
        .unwrap();

    // Verify tables exist by running a simple query on each
    let mut result = storage.db.query("INFO FOR DB").await.unwrap();
    let info: Option<serde_json::Value> = result.take(0).unwrap();
    let tables = info.unwrap()["tables"].as_object().unwrap().clone();

    let expected_tables = [
        "events",
        "assets",
        "asset_partitions",
        "runs",
        "kv",
        "dynamic_partitions",
        "ticks",
        "condition_ticks",
        "condition_evals",
    ];
    for table in expected_tables {
        assert!(tables.contains_key(table), "missing table: {table}");
    }

    // Verify it's functional — write and read back
    storage.kv_set("test_key", b"hello").await.unwrap();
    let val = storage.kv_get("test_key").await.unwrap().unwrap();
    assert_eq!(val, b"hello");

    // Clean up
    drop(storage);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_backfill_crud() {
    let storage = make_storage().await;
    let record = BackfillRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        backfill_id: "bf-001".to_string(),
        status: BackfillStatus::Requested,
        strategy: BackfillStrategy::MultiRun,
        failure_policy: BackfillFailurePolicy::Continue,
        asset_selection: vec!["my_asset".to_string()],
        job_name: None,
        partition_keys: vec![PartitionKey::Single {
            keys: vec!["2024-01-15".to_string()],
        }],
        run_ids: vec![],
        completed_partitions: vec![],
        failed_partitions: vec![],
        canceled_partitions: vec![],
        max_concurrency: 4,
        tags: vec![("team".to_string(), "data".to_string())],
        create_time: 1000,
        end_time: None,
        error: None,
        launched_by: LaunchedBy::Manual {
            user: Some(crate::storage::UserRef {
                subject: "sub-42".to_string(),
                email: Some("john.doe@example.com".to_string()),
                name: None,
            }),
        },
        action: None,
        config: None,
    };
    storage.create_backfill(&record).await.unwrap();

    let retrieved = storage.get_backfill("bf-001").await.unwrap();
    assert!(retrieved.is_some());
    let r = retrieved.unwrap();
    assert_eq!(r.backfill_id, "bf-001");
    assert_eq!(r.status, BackfillStatus::Requested);
    assert_eq!(r.partition_keys.len(), 1);
    assert_eq!(r.launched_by, record.launched_by, "provenance roundtrips");

    // A row created without launched_by (a v2 writer) gets the V3 DDL
    // default and must deserialize back to the struct default. The
    // genuinely-absent pre-V3 read path is covered by the migration-order
    // test `test_v3_backfill_launched_by_defaults_for_legacy_rows`.
    storage
            .db
            .query("CREATE backfills CONTENT { backfill_id: 'bf-old', code_location_id: 'default', status: 'Requested', strategy: { kind: 'MultiRun' }, failure_policy: 'Continue', asset_selection: [], partition_keys: [], run_ids: [], completed_partitions: [], failed_partitions: [], canceled_partitions: [], max_concurrency: 1, tags: [], create_time: 1, end_time: NONE, error: NONE }")
            .await
            .unwrap()
            .check()
            .unwrap();
    let old_row = storage
        .get_backfill("bf-old")
        .await
        .unwrap()
        .expect("row without an explicit launched_by must deserialize");
    assert_eq!(
        old_row.launched_by,
        LaunchedBy::Manual { user: None },
        "the DDL default round-trips to the struct default"
    );
}

#[tokio::test]
async fn test_get_all_backfills_page_pagination_and_filter() {
    let storage = make_storage().await;
    let specs = [
        ("bf0", BackfillStatus::InProgress, 100i64),
        ("bf1", BackfillStatus::CompletedSuccess, 200),
        ("bf2", BackfillStatus::CompletedFailed, 300),
        ("bf3", BackfillStatus::InProgress, 400),
        ("bf4", BackfillStatus::Canceled, 500),
    ];
    for (id, status, ct) in specs {
        storage
            .create_backfill(&make_backfill(id, status, ct))
            .await
            .unwrap();
    }

    // No filter — ordered DESC by create_time.
    let page = storage
        .get_all_backfills_page(0, 3, &BackfillFilter::default())
        .await
        .unwrap();
    assert_eq!(page.total, 5);
    assert_eq!(page.rows.len(), 3);
    assert_eq!(page.rows[0].backfill_id, "bf4");
    assert_eq!(page.rows[2].backfill_id, "bf2");

    // Offset.
    let page = storage
        .get_all_backfills_page(3, 3, &BackfillFilter::default())
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 2);
    assert_eq!(page.rows[0].backfill_id, "bf1");

    // Status filter.
    let filter = BackfillFilter {
        status: Some(BackfillStatus::InProgress),
    };
    let page = storage
        .get_all_backfills_page(0, 10, &filter)
        .await
        .unwrap();
    assert_eq!(page.total, 2);
    assert!(
        page.rows
            .iter()
            .all(|r| r.status == BackfillStatus::InProgress)
    );
}

#[tokio::test]
async fn test_get_all_backfills_summary_counts() {
    let storage = make_storage().await;
    let specs = [
        ("a", BackfillStatus::InProgress),
        ("b", BackfillStatus::InProgress),
        ("c", BackfillStatus::CompletedSuccess),
        ("d", BackfillStatus::CompletedFailed),
        ("e", BackfillStatus::Canceled),
        ("f", BackfillStatus::Requested),
    ];
    for (id, status) in specs {
        storage
            .create_backfill(&make_backfill(id, status, 0))
            .await
            .unwrap();
    }

    let s = storage.get_all_backfills_summary().await.unwrap();
    assert_eq!(s.total, 6);
    assert_eq!(s.in_progress, 2);
    assert_eq!(s.completed_success, 1);
    assert_eq!(s.completed_failed, 1);
    assert_eq!(s.canceled, 1);
}
