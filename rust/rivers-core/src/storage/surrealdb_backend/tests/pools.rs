use super::*;

// ── Concurrency pool tests ──

#[tokio::test]
async fn test_set_and_get_pool_limits() {
    let storage = make_storage().await;

    // Initially empty
    let pools = storage
        .get_pool_limits(crate::storage::DEFAULT_CODE_LOCATION_ID)
        .await
        .unwrap();
    assert!(pools.is_empty());

    // Set a pool
    storage
        .set_pool_limit(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "database",
            10,
            300,
        )
        .await
        .unwrap();
    let pools = storage
        .get_pool_limits(crate::storage::DEFAULT_CODE_LOCATION_ID)
        .await
        .unwrap();
    assert_eq!(pools.len(), 1);
    assert_eq!(pools[0].pool_key, "database");
    assert_eq!(pools[0].slot_limit, 10);
    assert_eq!(pools[0].lease_duration_secs, 300);

    // Set another pool
    storage
        .set_pool_limit(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "api_quota",
            5,
            600,
        )
        .await
        .unwrap();
    let pools = storage
        .get_pool_limits(crate::storage::DEFAULT_CODE_LOCATION_ID)
        .await
        .unwrap();
    assert_eq!(pools.len(), 2);
    assert_eq!(pools[0].pool_key, "api_quota"); // alphabetical order
    assert_eq!(pools[1].pool_key, "database");
}

#[tokio::test]
async fn test_set_pool_limit_upsert() {
    let storage = make_storage().await;

    storage
        .set_pool_limit(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "database",
            10,
            300,
        )
        .await
        .unwrap();
    storage
        .set_pool_limit(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "database",
            20,
            600,
        )
        .await
        .unwrap();

    let pools = storage
        .get_pool_limits(crate::storage::DEFAULT_CODE_LOCATION_ID)
        .await
        .unwrap();
    assert_eq!(pools.len(), 1);
    assert_eq!(pools[0].slot_limit, 20);
    assert_eq!(pools[0].lease_duration_secs, 600);
}

#[tokio::test]
async fn test_get_pool_info_empty() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "database",
            10,
            300,
        )
        .await
        .unwrap();

    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "database")
        .await
        .unwrap();
    assert_eq!(info.pool_key, "database");
    assert_eq!(info.slot_limit, 10);
    assert_eq!(info.lease_duration_secs, 300);
    assert_eq!(info.claimed_count, 0);
    assert_eq!(info.pending_count, 0);
}

#[tokio::test]
async fn test_get_pool_info_with_slots_and_pending() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "database",
            10,
            300,
        )
        .await
        .unwrap();

    let now_ns = now_nanos();
    let future_ns = now_ns + 600_000_000_000; // 10 min from now
    let past_ns = now_ns - 60_000_000_000; // 1 min ago (expired)

    for (run_id, step_key, slots, expires) in [
        ("run1", "step_a", 2i64, future_ns),
        ("run2", "step_b", 3, future_ns),
        ("run3", "step_c", 1, past_ns), // expired
    ] {
        let resp = storage
            .db
            .query(
                "INSERT INTO concurrency_slots { \
                         pool_key: $pool_key, run_id: $run_id, step_key: $step_key, \
                         slots_consumed: $slots, claimed_at: $now, \
                         lease_expires_at: $expires, last_heartbeat: $now \
                     }",
            )
            .bind(("pool_key", "database".to_string()))
            .bind(("run_id", run_id.to_string()))
            .bind(("step_key", step_key.to_string()))
            .bind(("slots", slots))
            .bind(("now", now_ns))
            .bind(("expires", expires))
            .await
            .unwrap();
        resp.check().unwrap();
    }

    // Insert pending step
    let resp = storage
        .db
        .query(
            "INSERT INTO pending_steps { \
                     pool_key: $pool_key, run_id: $run_id, step_key: $step_key, \
                     priority: 0, enqueued_at: $now, block_reason: 'PoolFull' \
                 }",
        )
        .bind(("pool_key", "database".to_string()))
        .bind(("run_id", "run4".to_string()))
        .bind(("step_key", "step_d".to_string()))
        .bind(("now", now_ns))
        .await
        .unwrap();
    resp.check().unwrap();

    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "database")
        .await
        .unwrap();
    assert_eq!(info.slot_limit, 10);
    // 2 + 3 = 5 (expired slot with slots_consumed=1 should NOT be counted)
    assert_eq!(info.claimed_count, 5);
    assert_eq!(info.pending_count, 1);
}

#[tokio::test]
async fn test_get_pool_info_not_found() {
    let storage = make_storage().await;
    let result = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "nonexistent")
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_asset_record_with_pool() {
    let storage = make_storage().await;

    let record = AssetRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        asset_key: "my_asset".to_string(),
        tags: vec![],
        kinds: vec!["table".to_string()],
        asset_group: None,
        code_version: None,
        last_event_id: None,
        last_run_id: None,
        last_timestamp: None,
        last_data_version: None,
        last_materialization_code_version: None,
        last_input_data_versions: vec![],
        pool: vec![("database".to_string(), 1), ("api_quota".to_string(), 2)],
    };
    storage
        .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, &[record])
        .await
        .unwrap();

    let stored = storage
        .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_asset")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.pool.len(), 2);
    assert_eq!(stored.pool[0], ("database".to_string(), 1));
    assert_eq!(stored.pool[1], ("api_quota".to_string(), 2));
}

#[tokio::test]
async fn test_asset_pool_preserved_on_re_register() {
    let storage = make_storage().await;

    // Register with pool
    let record = AssetRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        asset_key: "my_asset".to_string(),
        tags: vec![],
        kinds: vec![],
        asset_group: None,
        code_version: Some("v1".to_string()),
        last_event_id: None,
        last_run_id: None,
        last_timestamp: None,
        last_data_version: None,
        last_materialization_code_version: None,
        last_input_data_versions: vec![],
        pool: vec![("database".to_string(), 3)],
    };
    storage
        .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, &[record])
        .await
        .unwrap();

    // Re-register with different pool config
    let record2 = AssetRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        asset_key: "my_asset".to_string(),
        tags: vec![],
        kinds: vec![],
        asset_group: None,
        code_version: Some("v2".to_string()),
        last_event_id: None,
        last_run_id: None,
        last_timestamp: None,
        last_data_version: None,
        last_materialization_code_version: None,
        last_input_data_versions: vec![],
        pool: vec![("api".to_string(), 1)],
    };
    storage
        .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, &[record2])
        .await
        .unwrap();

    let stored = storage
        .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_asset")
        .await
        .unwrap()
        .unwrap();
    // Pool should be updated to new config
    assert_eq!(stored.pool, vec![("api".to_string(), 1)]);
    assert_eq!(stored.code_version, Some("v2".to_string()));
}

/// Pins `get_failed_partitions` across every source class in one place:
/// keyed failure runs floor, action runs never do (neither their run
/// status nor their keyed StepFailure events), and a newer
/// materialization or deletion supersedes. Guards the single-scan shape —
/// the runs table has no usable index for `$asset_key IN node_names`, so
/// each extra scan is a full table walk per invalidated asset per tick.
#[tokio::test]
async fn failed_partitions_sources_and_supersession() {
    let storage = make_storage().await;
    let cl = "default";
    let single = |k: &str| PartitionKey::Single {
        keys: vec![k.to_string()],
    };
    let run = |id: &str, status: RunStatus, action: Option<&str>, pk: &str, ts: i64| RunRecord {
        run_id: id.to_string(),
        code_location_id: cl.to_string(),
        job_name: None,
        status,
        start_time: ts,
        end_time: Some(ts),
        tags: vec![],
        node_names: vec!["events".to_string()],
        priority: 0,
        partition_key: Some(single(pk)),
        block_reason: None,
        launched_by: LaunchedBy::default(),
        action: action.map(String::from),
        config: None,
    };

    // p1: a real keyed failure — floors.
    storage
        .create_run(&run("rf", RunStatus::Failure, None, "p1", 1000))
        .await
        .unwrap();
    // p2: an action run failed — not a failure to materialize.
    storage
        .create_run(&run("ra", RunStatus::Failure, Some("compact"), "p2", 1000))
        .await
        .unwrap();
    // p3: a keyed StepFailure event from an action run — same rule.
    storage
        .create_run(&run("rb", RunStatus::Success, Some("compact"), "p3", 1000))
        .await
        .unwrap();
    storage
        .store_event(&EventRecord {
            code_location_id: cl.to_string(),
            event_type: EventType::StepFailure,
            asset_key: Some("events".to_string()),
            run_id: "rb".to_string(),
            partition_key: Some(single("p3")),
            timestamp: 1000,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();
    // p4: failed, then superseded by a newer materialization timestamp.
    storage
        .create_run(&run("rs", RunStatus::Failure, None, "p4", 1000))
        .await
        .unwrap();

    let materialized: std::collections::HashMap<PartitionKey, i64> =
        std::iter::once((single("p4"), 2000)).collect();
    let failed = storage
        .get_failed_partitions(cl, "events", &materialized)
        .await
        .unwrap();

    assert_eq!(
        failed.get(&single("p1")),
        Some(&1000),
        "a keyed failure run must floor its partition"
    );
    assert!(
        !failed.contains_key(&single("p2")),
        "an action run's failure is not a failure to materialize"
    );
    assert!(
        !failed.contains_key(&single("p3")),
        "an action run's StepFailure events must not floor"
    );
    assert!(
        !failed.contains_key(&single("p4")),
        "a newer materialization supersedes the floor"
    );
}

/// The pre-check probe and the claim transaction enforce the same
/// conflict rule — a drift between them turns "wait like any blocked
/// claim" into hard PoolContended failures (or vice versa admits a
/// conflicting writer). Both must embed the one shared predicate.
#[test]
fn conflict_predicate_shared_between_precheck_and_transaction() {
    for scope in [
        None,
        Some(AssetScope {
            exclusive: true,
            partitions: Some(vec!["p1".to_string()].into_iter().collect()),
        }),
    ] {
        let clause = SurrealStorage::asset_conflict_clause(0, scope.as_ref());
        let predicate = SurrealStorage::asset_conflict_predicate("p0", "parts0", scope.as_ref());
        assert!(
            clause.contains(&predicate),
            "transaction LET must embed the shared predicate:\n{clause}\nvs\n{predicate}"
        );
    }
}

/// Asset pools are admitted by partition overlap, not capacity — the
/// claim transaction must not compute `$lim`/`$used` for them (a second
/// full slot aggregate inside the write transaction, referenced by
/// nothing, paid per attempt and per ~1s blocked poll).
#[test]
fn claim_transaction_skips_capacity_lets_for_asset_pools() {
    let pools = vec![
        ("__asset__:orders".to_string(), 1u32),
        ("db".to_string(), 2u32),
    ];
    let q = SurrealStorage::build_claim_transaction(&pools, &[0], None);
    assert!(
        !q.contains("LET $lim_0") && !q.contains("LET $used_0"),
        "asset pool must not compute unused capacity:\n{q}"
    );
    assert!(
        q.contains("LET $lim_1") && q.contains("LET $used_1"),
        "counted pool must keep its capacity check:\n{q}"
    );
    assert!(
        q.contains("$conf_0") && q.contains("array::len($conf_0) == 0"),
        "asset pool must keep its conflict clause:\n{q}"
    );
    assert!(
        q.contains("($used_1 + 2) <= $lim_1"),
        "counted pool must keep its admission condition:\n{q}"
    );
}

/// `set_pool_limit` rows are load-bearing: a lost `__asset__:` registration
/// makes every claim on that asset hard-fail "pool not configured". Under
/// same-key contention every call must land or error loudly — with the
/// retry + check inside the storage layer, they all land.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_pool_limit_survives_same_key_contention() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = std::sync::Arc::new(
        SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage"),
    );

    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(50));
    let mut handles = Vec::new();
    for i in 0..50u32 {
        let storage = storage.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            storage
                .set_pool_limit(
                    crate::storage::DEFAULT_CODE_LOCATION_ID,
                    "__asset__:orders",
                    (i % 7) as i32,
                    300,
                )
                .await
        }));
    }
    for h in handles {
        h.await
            .unwrap()
            .expect("set_pool_limit must not fail under same-key contention");
    }

    let pools = storage
        .get_pool_limits(crate::storage::DEFAULT_CODE_LOCATION_ID)
        .await
        .unwrap();
    let rows: Vec<_> = pools
        .iter()
        .filter(|p| p.pool_key == "__asset__:orders")
        .collect();
    assert_eq!(rows.len(), 1, "exactly one row must survive contention");
}

// ── Observability event tests ──

#[tokio::test]
async fn test_concurrency_event_types_roundtrip() {
    let storage = make_storage().await;

    let run_id = "run-events-test";
    storage
        .create_run(&RunRecord {
            run_id: run_id.to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".into()),
            status: RunStatus::Queued,
            start_time: now_nanos(),
            end_time: None,
            tags: vec![],
            node_names: vec!["asset_a".into()],
            priority: 5,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();

    // Store one of each new event type
    let event_types = vec![
        (EventType::RunQueued, vec![("priority".into(), "5".into())]),
        (
            EventType::RunDequeued,
            vec![("priority".into(), "5".into())],
        ),
        (
            EventType::StepSlotClaimed,
            vec![("pools".into(), "db,api".into())],
        ),
        (
            EventType::StepSlotWaiting,
            vec![("reason".into(), "pool 'db' full (5/5)".into())],
        ),
        (EventType::StepSlotRenewed, vec![]),
        (EventType::StepSlotReleased, vec![]),
    ];

    for (evt_type, metadata) in &event_types {
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: evt_type.clone(),
                asset_key: Some("asset_a".into()),
                run_id: run_id.to_string(),
                partition_key: None,
                timestamp: now_nanos(),
                metadata: metadata.clone(),
                input_data_versions: vec![],
            })
            .await
            .unwrap();
    }

    // Retrieve and verify
    let events = storage.get_events_for_run(run_id).await.unwrap();
    assert_eq!(events.len(), 6, "expected 6 concurrency events");

    let type_names: Vec<&str> = events.iter().map(|e| e.event_type.type_name()).collect();
    assert!(type_names.contains(&"RunQueued"));
    assert!(type_names.contains(&"RunDequeued"));
    assert!(type_names.contains(&"StepSlotClaimed"));
    assert!(type_names.contains(&"StepSlotWaiting"));
    assert!(type_names.contains(&"StepSlotRenewed"));
    assert!(type_names.contains(&"StepSlotReleased"));

    // Verify metadata survives roundtrip
    let claimed = events
        .iter()
        .find(|e| e.event_type.type_name() == "StepSlotClaimed")
        .unwrap();
    assert_eq!(
        claimed
            .metadata
            .iter()
            .find(|(k, _)| k == "pools")
            .map(|(_, v)| v.as_str()),
        Some("db,api")
    );

    let waiting = events
        .iter()
        .find(|e| e.event_type.type_name() == "StepSlotWaiting")
        .unwrap();
    assert_eq!(
        waiting
            .metadata
            .iter()
            .find(|(k, _)| k == "reason")
            .map(|(_, v)| v.as_str()),
        Some("pool 'db' full (5/5)")
    );
}

#[tokio::test]
async fn test_run_block_reason_persistence() {
    let storage = make_storage().await;
    let run_id = "run-block-reason";

    storage
        .create_run(&RunRecord {
            run_id: run_id.to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".into()),
            status: RunStatus::Queued,
            start_time: now_nanos(),
            end_time: None,
            tags: vec![("env".into(), "prod".into())],
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

    // Initially no block reason
    let run = storage.get_run(run_id).await.unwrap().unwrap();
    assert!(run.block_reason.is_none());

    // Set block reason
    storage
        .update_run_block_reason(run_id, Some("tag limit: env=prod (2/2)"))
        .await
        .unwrap();
    let run = storage.get_run(run_id).await.unwrap().unwrap();
    assert_eq!(
        run.block_reason.as_deref(),
        Some("tag limit: env=prod (2/2)")
    );

    // Clear block reason (on dequeue)
    storage.update_run_block_reason(run_id, None).await.unwrap();
    let run = storage.get_run(run_id).await.unwrap().unwrap();
    assert!(run.block_reason.is_none());
}

#[tokio::test]
async fn test_event_type_from_type_name_roundtrip() {
    // Verify all new event types can roundtrip through type_name / from_type_name
    let types = vec![
        EventType::RunQueued,
        EventType::RunDequeued,
        EventType::StepSlotClaimed,
        EventType::StepSlotWaiting,
        EventType::StepSlotRenewed,
        EventType::StepSlotReleased,
        EventType::ActionCompleted,
    ];

    for evt in types {
        let name = evt.type_name();
        let reconstructed = EventType::from_type_name(name, None).unwrap();
        assert_eq!(evt, reconstructed, "roundtrip failed for {name}");
    }
}

// ── get_pool_slot_holders ──

#[tokio::test]
async fn test_get_pool_slot_holders_returns_active_slots() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 10, 300)
        .await
        .unwrap();

    // Claim 2 slots in the "db" pool from different steps
    let status1 = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".into(), 1)],
            "r1",
            "step_a",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(status1, ConcurrencyClaimStatus::Claimed));

    let status2 = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".into(), 2)],
            "r2",
            "step_b",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(status2, ConcurrencyClaimStatus::Claimed));

    let holders = storage
        .get_pool_slot_holders(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(holders.len(), 2);

    let h1 = holders.iter().find(|h| h.run_id == "r1").unwrap();
    assert_eq!(h1.step_key, "step_a");
    assert_eq!(h1.slots_consumed, 1);
    assert!(h1.lease_expires_at > h1.claimed_at);

    let h2 = holders.iter().find(|h| h.run_id == "r2").unwrap();
    assert_eq!(h2.step_key, "step_b");
    assert_eq!(h2.slots_consumed, 2);
}

#[tokio::test]
async fn test_get_pool_slot_holders_excludes_expired() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 10, 1)
        .await
        .unwrap();

    // Claim with 1-second lease
    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".into(), 1)],
            "r1",
            "step_a",
            0,
            1,
            None,
        )
        .await
        .unwrap();

    // Wait for expiry
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let holders = storage
        .get_pool_slot_holders(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert!(holders.is_empty(), "expired slots should not be returned");
}

#[tokio::test]
async fn test_get_pool_slot_holders_empty_pool() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 5, 300)
        .await
        .unwrap();

    let holders = storage
        .get_pool_slot_holders(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert!(holders.is_empty());
}

// ── get_all_pool_infos ──

#[tokio::test]
async fn test_get_all_pool_infos_batched() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 5, 300)
        .await
        .unwrap();
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "api", 10, 300)
        .await
        .unwrap();

    // Claim 2 slots in "db"
    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".into(), 2)],
            "r1",
            "step_a",
            0,
            300,
            None,
        )
        .await
        .unwrap();

    let infos = storage
        .get_all_pool_infos(crate::storage::DEFAULT_CODE_LOCATION_ID)
        .await
        .unwrap();
    assert_eq!(infos.len(), 2);

    let db_info = infos.iter().find(|i| i.pool_key == "api").unwrap();
    assert_eq!(db_info.slot_limit, 10);
    assert_eq!(db_info.claimed_count, 0);
    assert_eq!(db_info.pending_count, 0);

    let api_info = infos.iter().find(|i| i.pool_key == "db").unwrap();
    assert_eq!(api_info.slot_limit, 5);
    assert_eq!(api_info.claimed_count, 2);
    assert_eq!(api_info.pending_count, 0);
}

#[tokio::test]
async fn test_get_all_pool_infos_empty() {
    let storage = make_storage().await;
    let infos = storage
        .get_all_pool_infos(crate::storage::DEFAULT_CODE_LOCATION_ID)
        .await
        .unwrap();
    assert!(infos.is_empty());
}
