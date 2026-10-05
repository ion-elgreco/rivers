use super::*;

// ── Run progress, outcome, cancellation, step events ──

#[tokio::test]
async fn test_get_run_progress_empty() {
    let storage = make_storage().await;
    let progress = storage.get_run_progress("no-such-run").await.unwrap();
    assert_eq!(progress.completed_steps, 0);
    assert_eq!(progress.total_steps, 0);
    assert!(progress.last_step_completed_at.is_none());
    assert!(progress.last_completed_step.is_none());
}

#[tokio::test]
async fn test_get_run_progress_counts_steps() {
    let storage = make_storage().await;
    let run_id = "run-progress-1";

    // 3 StepStart events
    for (asset, ts) in [("a", 100), ("b", 200), ("c", 300)] {
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepStart,
                asset_key: Some(asset.to_string()),
                run_id: run_id.to_string(),
                partition_key: None,
                timestamp: ts,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();
    }
    // 2 completed (1 success, 1 failure)
    storage
        .store_event(&EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepSuccess,
            asset_key: Some("a".to_string()),
            run_id: run_id.to_string(),
            partition_key: None,
            timestamp: 150,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();
    storage
        .store_event(&EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepFailure,
            asset_key: Some("b".to_string()),
            run_id: run_id.to_string(),
            partition_key: None,
            timestamp: 250,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    let progress = storage.get_run_progress(run_id).await.unwrap();
    assert_eq!(progress.total_steps, 3);
    assert_eq!(progress.completed_steps, 2);
    assert_eq!(progress.last_step_completed_at, Some(250));
    assert_eq!(progress.last_completed_step.as_deref(), Some("b"));
}

#[tokio::test]
async fn test_get_run_progress_excludes_per_partition_failures() {
    let storage = make_storage().await;
    let run_id = "run-progress-partial";

    // One step: StepStart + StepSuccess.
    for (event_type, ts) in [(EventType::StepStart, 100), (EventType::StepSuccess, 200)] {
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type,
                asset_key: Some("a".to_string()),
                run_id: run_id.to_string(),
                partition_key: None,
                timestamp: ts,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();
    }
    for (key, ts) in [("p1", 150), ("p2", 160)] {
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepFailure,
                asset_key: Some("a".to_string()),
                run_id: run_id.to_string(),
                partition_key: Some(PartitionKey::Single {
                    keys: vec![key.to_string()],
                }),
                timestamp: ts,
                metadata: vec![("error".to_string(), "boom".to_string())],
                input_data_versions: vec![],
            })
            .await
            .unwrap();
    }

    let progress = storage.get_run_progress(run_id).await.unwrap();
    assert_eq!(progress.total_steps, 1);
    assert_eq!(progress.completed_steps, 1);
    assert_eq!(progress.last_step_completed_at, Some(200));
    assert_eq!(progress.last_completed_step.as_deref(), Some("a"));
}

#[tokio::test]
async fn test_get_step_terminal_events_filters_types() {
    let storage = make_storage().await;
    let run_id = "terminal-events-run";

    for (event_type, pk, ts) in [
        (EventType::StepStart, None, 100),
        (EventType::StepRetry, None, 150),
        (
            EventType::StepFailure,
            Some(PartitionKey::Single {
                keys: vec!["p1".to_string()],
            }),
            160,
        ),
        (EventType::StepFailure, None, 170),
        (EventType::StepSuccess, None, 200),
    ] {
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type,
                asset_key: Some("a".to_string()),
                run_id: run_id.to_string(),
                partition_key: pk,
                timestamp: ts,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();
    }

    let events = storage.get_step_terminal_events(run_id, "a").await.unwrap();
    let types: Vec<_> = events.iter().map(|e| e.event_type.clone()).collect();
    assert_eq!(
        types,
        vec![
            EventType::StepFailure,
            EventType::StepFailure,
            EventType::StepSuccess
        ]
    );
    // partition-scoped failure keeps its key so callers can filter it
    assert!(events[0].partition_key.is_some());
    assert!(events[1].partition_key.is_none());
}

#[tokio::test]
async fn test_get_run_progress_counts_retried_step_once() {
    let storage = make_storage().await;
    let run_id = "run-progress-retry";

    // One step retried once: two StepStarts, a step-level failure, the
    // StepRetry marker, then the succeeding attempt.
    for (event_type, ts) in [
        (EventType::StepStart, 100),
        (EventType::StepFailure, 150),
        (EventType::StepRetry, 160),
        (EventType::StepStart, 200),
        (EventType::StepSuccess, 250),
    ] {
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type,
                asset_key: Some("a".to_string()),
                run_id: run_id.to_string(),
                partition_key: None,
                timestamp: ts,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();
    }

    let progress = storage.get_run_progress(run_id).await.unwrap();
    assert_eq!(progress.total_steps, 1);
    assert_eq!(progress.completed_steps, 1);
    assert_eq!(progress.last_step_completed_at, Some(250));
    assert_eq!(progress.last_completed_step.as_deref(), Some("a"));
}

#[tokio::test]
async fn test_run_outcome_roundtrip() {
    let storage = make_storage().await;
    let run_id = "run-outcome-1";

    assert!(storage.get_run_outcome(run_id).await.unwrap().is_none());

    let outcome = RunOutcome::Success {
        completed_steps: 5,
        total_steps: 5,
    };
    storage.set_run_outcome(run_id, &outcome).await.unwrap();

    let retrieved = storage.get_run_outcome(run_id).await.unwrap().unwrap();
    assert_eq!(retrieved, outcome);
}

#[tokio::test]
async fn test_run_outcome_failure() {
    let storage = make_storage().await;
    let outcome = RunOutcome::Failure {
        message: "step X exploded".to_string(),
        completed_steps: 2,
        total_steps: 5,
    };
    storage.set_run_outcome("r1", &outcome).await.unwrap();
    assert_eq!(
        storage.get_run_outcome("r1").await.unwrap().unwrap(),
        outcome
    );
}

#[tokio::test]
async fn test_run_outcome_cancelled() {
    let storage = make_storage().await;
    let outcome = RunOutcome::Cancelled {
        completed_steps: 1,
        total_steps: 3,
    };
    storage.set_run_outcome("r1", &outcome).await.unwrap();
    assert_eq!(
        storage.get_run_outcome("r1").await.unwrap().unwrap(),
        outcome
    );
}

#[tokio::test]
async fn test_run_outcome_overwrite() {
    let storage = make_storage().await;
    let first = RunOutcome::Success {
        completed_steps: 3,
        total_steps: 3,
    };
    storage.set_run_outcome("r1", &first).await.unwrap();

    let second = RunOutcome::Failure {
        message: "oops".to_string(),
        completed_steps: 2,
        total_steps: 3,
    };
    storage.set_run_outcome("r1", &second).await.unwrap();
    assert_eq!(
        storage.get_run_outcome("r1").await.unwrap().unwrap(),
        second
    );
}

#[tokio::test]
async fn test_cancellation_flag() {
    let storage = make_storage().await;
    let run_id = "cancel-test-1";

    assert!(!storage.is_cancelled(run_id).await.unwrap());

    storage.request_cancellation(run_id).await.unwrap();
    assert!(storage.is_cancelled(run_id).await.unwrap());

    // Other runs are unaffected
    assert!(!storage.is_cancelled("other-run").await.unwrap());
}

#[tokio::test]
async fn test_get_events_for_step() {
    let storage = make_storage().await;
    let run_id = "step-events-1";

    // Events for step "asset_a"
    for (etype, ts) in [
        (EventType::StepStart, 100),
        (
            EventType::Materialization {
                data_version: Some("v1".to_string()),
            },
            150,
        ),
        (EventType::StepSuccess, 200),
    ] {
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: etype,
                asset_key: Some("asset_a".to_string()),
                run_id: run_id.to_string(),
                partition_key: None,
                timestamp: ts,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();
    }

    // Event for different step "asset_b"
    storage
        .store_event(&EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepStart,
            asset_key: Some("asset_b".to_string()),
            run_id: run_id.to_string(),
            partition_key: None,
            timestamp: 300,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    let step_a_events = storage
        .get_events_for_step(run_id, "asset_a")
        .await
        .unwrap();
    assert_eq!(step_a_events.len(), 3);
    assert!(matches!(step_a_events[0].event_type, EventType::StepStart));
    assert!(matches!(
        step_a_events[1].event_type,
        EventType::Materialization { .. }
    ));
    assert!(matches!(
        step_a_events[2].event_type,
        EventType::StepSuccess
    ));

    let step_b_events = storage
        .get_events_for_step(run_id, "asset_b")
        .await
        .unwrap();
    assert_eq!(step_b_events.len(), 1);

    let step_c_events = storage
        .get_events_for_step(run_id, "nonexistent")
        .await
        .unwrap();
    assert!(step_c_events.is_empty());
}

#[tokio::test]
async fn test_get_events_for_step_different_runs() {
    let storage = make_storage().await;

    // Same asset in two different runs
    for run_id in ["run-1", "run-2"] {
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepStart,
                asset_key: Some("shared_asset".to_string()),
                run_id: run_id.to_string(),
                partition_key: None,
                timestamp: 100,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();
    }

    let run1_events = storage
        .get_events_for_step("run-1", "shared_asset")
        .await
        .unwrap();
    assert_eq!(run1_events.len(), 1);
    assert_eq!(run1_events[0].run_id, "run-1");

    let run2_events = storage
        .get_events_for_step("run-2", "shared_asset")
        .await
        .unwrap();
    assert_eq!(run2_events.len(), 1);
    assert_eq!(run2_events[0].run_id, "run-2");
}

#[tokio::test]
async fn test_get_step_attempts() {
    let storage = make_storage().await;
    let event = |event_type, asset: &str, pk: Option<&str>| EventRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type,
        asset_key: Some(asset.to_string()),
        run_id: "run-1".to_string(),
        partition_key: pk.map(|k| PartitionKey::Single {
            keys: vec![k.to_string()],
        }),
        timestamp: 100,
        metadata: vec![],
        input_data_versions: vec![],
    };
    storage
        .store_events(&[
            event(EventType::StepStart, "cut_off", None),
            event(EventType::StepRetry, "cut_off", None),
            event(EventType::StepStart, "cut_off", None),
            event(EventType::StepStart, "failed", None),
            event(EventType::StepFailure, "failed", None),
            // A keyed failure is one partition of a batch, not the step.
            event(EventType::StepStart, "one_key", None),
            event(EventType::StepFailure, "one_key", Some("p1")),
        ])
        .await
        .unwrap();

    let attempts = storage.get_step_attempts("run-1").await.unwrap();
    let get = |k: &str| attempts.get(k).cloned().unwrap_or_default();
    assert_eq!(
        get("cut_off"),
        crate::storage::StepAttempts {
            starts: 2,
            failed: false,
            retries: 1,
            failed_keys: vec![],
        }
    );
    assert!(get("failed").failed);
    assert!(get("one_key").starts == 1 && !get("one_key").failed);
    assert_eq!(
        get("one_key").failed_keys,
        vec![PartitionKey::Single {
            keys: vec!["p1".to_string()]
        }]
    );
    assert!(storage.get_step_attempts("other").await.unwrap().is_empty());
}

#[tokio::test]
async fn test_get_completed_step_keys_empty() {
    let storage = make_storage().await;
    let keys = storage
        .get_completed_step_keys("no-such-run")
        .await
        .unwrap();
    assert!(keys.is_empty());
}

#[tokio::test]
async fn test_get_completed_step_keys() {
    let storage = make_storage().await;
    let run_id = "run-resume-1";

    for (asset, event_type) in [
        ("a", EventType::StepSuccess),
        ("b", EventType::StepFailure),
        ("c", EventType::StepSuccess),
        ("d", EventType::StepStart),
    ] {
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type,
                asset_key: Some(asset.to_string()),
                run_id: run_id.to_string(),
                partition_key: None,
                timestamp: 100,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();
    }

    let keys = storage.get_completed_step_keys(run_id).await.unwrap();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains("a"));
    assert!(keys.contains("c"));
    assert!(!keys.contains("b"));
    assert!(!keys.contains("d"));
}

#[tokio::test]
async fn test_get_step_data_versions_empty() {
    let storage = make_storage().await;
    let dvs = storage.get_step_data_versions("no-such-run").await.unwrap();
    assert!(dvs.is_empty());
}

#[tokio::test]
async fn test_get_step_data_versions() {
    let storage = make_storage().await;
    let run_id = "run-resume-dv-1";

    storage
        .store_event(&EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization {
                data_version: Some("v1".to_string()),
            },
            asset_key: Some("a".to_string()),
            run_id: run_id.to_string(),
            partition_key: None,
            timestamp: 100,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    storage
        .store_event(&EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization {
                data_version: Some("v2".to_string()),
            },
            asset_key: Some("b".to_string()),
            run_id: run_id.to_string(),
            partition_key: None,
            timestamp: 200,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    // Materialization without data_version should be excluded
    storage
        .store_event(&EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization { data_version: None },
            asset_key: Some("c".to_string()),
            run_id: run_id.to_string(),
            partition_key: None,
            timestamp: 300,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    // Different run should not appear
    storage
        .store_event(&EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization {
                data_version: Some("other".to_string()),
            },
            asset_key: Some("a".to_string()),
            run_id: "other-run".to_string(),
            partition_key: None,
            timestamp: 100,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    let dvs = storage.get_step_data_versions(run_id).await.unwrap();
    assert_eq!(dvs.len(), 2);
    assert_eq!(dvs.get("a").unwrap(), "v1");
    assert_eq!(dvs.get("b").unwrap(), "v2");
}

#[tokio::test]
async fn test_completed_step_keys_ignores_other_runs() {
    let storage = make_storage().await;

    storage
        .store_event(&EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepSuccess,
            asset_key: Some("x".to_string()),
            run_id: "run-A".to_string(),
            partition_key: None,
            timestamp: 100,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    storage
        .store_event(&EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepSuccess,
            asset_key: Some("y".to_string()),
            run_id: "run-B".to_string(),
            partition_key: None,
            timestamp: 100,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    let keys_a = storage.get_completed_step_keys("run-A").await.unwrap();
    assert_eq!(keys_a.len(), 1);
    assert!(keys_a.contains("x"));

    let keys_b = storage.get_completed_step_keys("run-B").await.unwrap();
    assert_eq!(keys_b.len(), 1);
    assert!(keys_b.contains("y"));
}
