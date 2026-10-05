use super::*;

#[test]
fn surreal_connect_config_unauthenticated_uses_default_scope() {
    let cfg = SurrealConnectConfig::unauthenticated("ws://surrealdb:8000");
    assert_eq!(cfg.endpoint, "ws://surrealdb:8000");
    assert_eq!(cfg.namespace, DEFAULT_NAMESPACE);
    assert_eq!(cfg.database, DEFAULT_DATABASE);
    assert!(cfg.credentials.is_none());
}

#[test]
fn surreal_connect_config_with_credentials_attaches_database_creds() {
    let cfg = SurrealConnectConfig::unauthenticated("ws://surrealdb:8000")
        .with_credentials("rivers".into(), "topsecret".into());
    match cfg.credentials {
        Some(SurrealCredentials::Database { username, password }) => {
            assert_eq!(username, "rivers");
            assert_eq!(password, "topsecret");
        }
        None => panic!("credentials should be set"),
    }
}

/// An action's `materialized()` must not advance
/// `last_materialization_code_version`: the verb body ran, not the
/// asset's materialize function, so a pending code-change rebuild (and
/// the Stale(Code) badge) must survive the action. Keyed off the run's
/// verb — idv-emptiness also matches every source asset's legitimate
/// materializations, which must keep stamping the version.
#[tokio::test]
async fn action_materialization_preserves_code_version() {
    let storage = make_storage().await;
    let cl = "default";
    let record = |cv: &str| AssetRecord {
        code_location_id: cl.to_string(),
        asset_key: "orders".to_string(),
        tags: vec![],
        kinds: vec![],
        asset_group: None,
        code_version: Some(cv.to_string()),
        last_event_id: None,
        last_run_id: None,
        last_timestamp: None,
        last_data_version: None,
        last_materialization_code_version: None,
        last_input_data_versions: vec![],
        pool: vec![],
    };
    let run = |id: &str, action: Option<&str>, ts: i64| RunRecord {
        run_id: id.to_string(),
        code_location_id: cl.to_string(),
        job_name: None,
        status: RunStatus::Success,
        start_time: ts,
        end_time: Some(ts),
        tags: vec![],
        node_names: vec!["orders".to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::default(),
        action: action.map(String::from),
        config: None,
    };
    let mat_event = |run_id: &str, ts: i64| EventRecord {
        code_location_id: cl.to_string(),
        event_type: EventType::Materialization {
            data_version: Some(format!("dv_{ts}")),
        },
        asset_key: Some("orders".to_string()),
        run_id: run_id.to_string(),
        partition_key: None,
        timestamp: ts,
        metadata: vec![],
        input_data_versions: vec![],
    };
    let mcv = |records: &[AssetRecord]| {
        records
            .iter()
            .find(|r| r.asset_key == "orders")
            .and_then(|r| r.last_materialization_code_version.clone())
    };

    // A real materialize under v1 stamps the version…
    storage.register_assets(cl, &[record("v1")]).await.unwrap();
    storage.create_run(&run("r1", None, 1000)).await.unwrap();
    storage.store_event(&mat_event("r1", 1000)).await.unwrap();
    let records = storage.get_asset_records(cl).await.unwrap();
    assert_eq!(mcv(&records).as_deref(), Some("v1"));

    // …the code changes (re-resolve registers v2, leaves mcv at v1)…
    storage.register_assets(cl, &[record("v2")]).await.unwrap();

    // …then an action run's materialized() lands, via both store paths.
    storage
        .create_run(&run("r2", Some("merge"), 2000))
        .await
        .unwrap();
    storage.store_event(&mat_event("r2", 2000)).await.unwrap();
    let records = storage.get_asset_records(cl).await.unwrap();
    assert_eq!(
        mcv(&records).as_deref(),
        Some("v1"),
        "store_event: an action's materialized() must not clear the pending rebuild"
    );

    storage
        .store_events(&[mat_event("r2", 3000)])
        .await
        .unwrap();
    let records = storage.get_asset_records(cl).await.unwrap();
    assert_eq!(
        mcv(&records).as_deref(),
        Some("v1"),
        "store_events: an action's materialized() must not clear the pending rebuild"
    );

    // A later real materialize under v2 re-arms as usual.
    storage.create_run(&run("r3", None, 4000)).await.unwrap();
    storage.store_event(&mat_event("r3", 4000)).await.unwrap();
    let records = storage.get_asset_records(cl).await.unwrap();
    assert_eq!(mcv(&records).as_deref(), Some("v2"));
}

/// The run-events page must scan `idx_events_run_ts` (timestamp order), not sort.
#[tokio::test]
async fn run_events_page_uses_ordering_index() {
    let temp = test_temp_dir::test_temp_dir!();
    let s = SurrealStorage::new_embedded(temp.as_path_untracked().to_str().unwrap())
        .await
        .unwrap();
    let rows: Vec<DbEventWrite> = (0..2000i64)
        .map(|i| DbEventWrite {
            id: new_event_record_id(),
            code_location_id: "default".into(),
            event_type: if i % 50 == 0 {
                "StepStart"
            } else {
                "Materialization"
            }
            .into(),
            asset_key: Some("a".into()),
            run_id: "r".into(),
            partition_key: None,
            timestamp: i,
            sort_order: 0,
            metadata: vec![],
            data_version: None,
            code_version: None,
            input_data_versions: vec![],
        })
        .collect();
    s.db.query("INSERT INTO events $rows RETURN NONE")
        .bind(("rows", rows))
        .await
        .unwrap()
        .check()
        .unwrap();

    let plan: Vec<serde_json::Value> =
        s.db.query(
            "SELECT * FROM events WHERE run_id = 'r' \
                 ORDER BY timestamp ASC, sort_order ASC, id ASC LIMIT 50 START 0 EXPLAIN",
        )
        .await
        .unwrap()
        .take(0)
        .unwrap();
    let plan = serde_json::to_string(&plan).unwrap();
    assert!(
        plan.contains("idx_events_run_ts"),
        "page should scan idx_events_run_ts: {plan}"
    );
    assert!(
        !plan.contains("SortTopKByKey") && !plan.contains("\"operator\":\"Sort\""),
        "page should not sort — the ordering index covers it: {plan}"
    );
}

/// `store_run_logs` / `get_run_logs` round-trip: per-run isolation,
/// timestamp ordering, and stream content survive intact.
#[tokio::test]
async fn run_logs_roundtrip() {
    let storage = make_storage().await;
    storage
        .store_run_logs(&[
            LogRecord {
                code_location_id: "default".into(),
                run_id: "run-1".into(),
                step_key: "b".into(),
                timestamp: 20,
                stdout: Some("b out".into()),
                stderr: None,
                logs: None,
                traceback: None,
            },
            LogRecord {
                code_location_id: "default".into(),
                run_id: "run-1".into(),
                step_key: "a".into(),
                timestamp: 10,
                stdout: Some("a out".into()),
                stderr: Some("a err".into()),
                logs: Some("a tracing".into()),
                traceback: None,
            },
            LogRecord {
                code_location_id: "default".into(),
                run_id: "run-1".into(),
                step_key: "b".into(),
                timestamp: 30,
                stdout: None,
                stderr: None,
                logs: None,
                traceback: Some(r#"{"exceptions":[],"text":"Traceback"}"#.into()),
            },
            LogRecord {
                code_location_id: "default".into(),
                run_id: "run-2".into(),
                step_key: "c".into(),
                timestamp: 5,
                stdout: None,
                stderr: Some("c err".into()),
                logs: None,
                traceback: None,
            },
        ])
        .await
        .unwrap();

    let logs = storage.get_run_logs("run-1").await.unwrap();
    assert_eq!(
        logs.iter()
            .map(|l| (l.step_key.as_str(), l.timestamp))
            .collect::<Vec<_>>(),
        vec![("a", 10), ("b", 20), ("b", 30)],
        "rows come back in timestamp order"
    );
    assert_eq!(logs[0].stdout.as_deref(), Some("a out"));
    assert_eq!(logs[0].stderr.as_deref(), Some("a err"));
    assert_eq!(logs[0].logs.as_deref(), Some("a tracing"));
    assert_eq!(logs[0].traceback, None);
    assert_eq!(logs[1].stdout.as_deref(), Some("b out"));
    assert_eq!(logs[1].stderr, None);
    assert_eq!(logs[2].stdout, None);
    assert_eq!(
        logs[2].traceback.as_deref(),
        Some(r#"{"exceptions":[],"text":"Traceback"}"#)
    );

    let logs2 = storage.get_run_logs("run-2").await.unwrap();
    assert_eq!(logs2.len(), 1);
    assert_eq!(logs2[0].stderr.as_deref(), Some("c err"));

    assert!(storage.get_run_logs("missing").await.unwrap().is_empty());
    storage.store_run_logs(&[]).await.unwrap();
}

/// The asset-events page must scan `idx_events_loc_asset_ts`, not sort every matching event.
#[tokio::test]
async fn asset_events_page_uses_ordering_index() {
    let temp = test_temp_dir::test_temp_dir!();
    let s = SurrealStorage::new_embedded(temp.as_path_untracked().to_str().unwrap())
        .await
        .unwrap();
    let rows: Vec<DbEventWrite> = (0..2000i64)
        .map(|i| DbEventWrite {
            id: new_event_record_id(),
            code_location_id: "default".into(),
            event_type: if i % 3 == 0 {
                "Observation"
            } else {
                "Materialization"
            }
            .into(),
            asset_key: Some("a".into()),
            run_id: "r".into(),
            partition_key: None,
            timestamp: i,
            sort_order: 0,
            metadata: vec![],
            data_version: None,
            code_version: None,
            input_data_versions: vec![],
        })
        .collect();
    s.db.query("INSERT INTO events $rows RETURN NONE")
        .bind(("rows", rows))
        .await
        .unwrap()
        .check()
        .unwrap();

    let plan: Vec<serde_json::Value> =
        s.db.query(
            "SELECT * FROM events WHERE code_location_id = 'default' AND asset_key = 'a' \
                 AND event_type IN ['Materialization', 'Observation'] \
                 ORDER BY timestamp DESC, sort_order DESC, id DESC LIMIT 50 START 0 EXPLAIN",
        )
        .await
        .unwrap()
        .take(0)
        .unwrap();
    let plan = serde_json::to_string(&plan).unwrap();
    assert!(
        plan.contains("idx_events_loc_asset_ts"),
        "asset page should scan idx_events_loc_asset_ts: {plan}"
    );
    assert!(
        !plan.contains("SortTopKByKey") && !plan.contains("\"operator\":\"Sort\""),
        "asset page should not sort: {plan}"
    );
}

/// Which assets and keys the failed runs materialized is read one run at a
/// time off `idx_events_run_type`, not by scanning every Materialization.
#[tokio::test]
async fn failed_run_materializations_scan_only_those_runs() {
    let temp = test_temp_dir::test_temp_dir!();
    let s = SurrealStorage::new_embedded(temp.as_path_untracked().to_str().unwrap())
        .await
        .unwrap();
    let rows: Vec<DbEventWrite> = (0..2000i64)
        .map(|i| DbEventWrite {
            id: new_event_record_id(),
            code_location_id: "default".into(),
            event_type: if i % 2 == 0 {
                "Materialization"
            } else {
                "StepSuccess"
            }
            .into(),
            asset_key: Some("a".into()),
            run_id: format!("r{}", i % 100),
            partition_key: Some(PartitionKey::Single {
                keys: vec![format!("p{i}")],
            }),
            timestamp: i,
            sort_order: 0,
            metadata: vec![],
            data_version: None,
            code_version: None,
            input_data_versions: vec![],
        })
        .collect();
    s.db.query("INSERT INTO events $rows RETURN NONE")
        .bind(("rows", rows))
        .await
        .unwrap()
        .check()
        .unwrap();

    for query in [
        "SELECT asset_key, run_id FROM events WITH INDEX idx_events_run_type \
             WHERE run_id IN $runs AND event_type = 'Materialization' \
             GROUP BY asset_key, run_id EXPLAIN",
        "SELECT partition_key, run_id FROM events WITH INDEX idx_events_run_type \
             WHERE run_id IN $runs AND event_type = 'Materialization' \
             AND asset_key = $asset_key AND partition_key IS NOT NONE EXPLAIN",
    ] {
        let plan: Vec<serde_json::Value> =
            s.db.query(query)
                .bind(("runs", vec!["r1".to_string(), "r2".to_string()]))
                .bind(("asset_key", "a".to_string()))
                .await
                .unwrap()
                .take(0)
                .unwrap();
        let plan = serde_json::to_string(&plan).unwrap();
        assert!(
            plan.contains("UnionIndexScan")
                && plan.contains("idx_events_run_type")
                && !plan.contains("\"idx_events_type\""),
            "one index scan per run, not every Materialization: {plan}"
        );
    }
}

/// The UNIQUE index compares the SERIALIZED partition_key, so a reordered-dims Multi key canonicalizes to one row.
#[tokio::test]
async fn test_multi_partition_key_dims_order_canonicalized() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
        .await
        .expect("failed to create rocksdb storage");
    let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
    let mk = |dims: Vec<(&str, &str)>| PartitionKey::Multi {
        dims: dims
            .into_iter()
            .map(|(d, v)| (d.to_string(), vec![v.to_string()]))
            .collect(),
    };

    let date_first = mk(vec![("date", "2024-01-01"), ("region", "eu")]);
    let region_first = mk(vec![("region", "eu"), ("date", "2024-01-01")]);
    let mut first = make_event("inventory", "r", 1);
    first.partition_key = Some(date_first.clone());
    storage.store_event(&first).await.unwrap();
    let mut second = make_event("inventory", "r", 2);
    second.partition_key = Some(region_first);
    storage.store_event(&second).await.unwrap();

    let parts = storage
        .get_materialized_partitions(cl, "inventory")
        .await
        .unwrap();
    assert_eq!(
        parts,
        vec![date_first],
        "dims order must canonicalize to one row"
    );
    assert_eq!(
        storage
            .count_materialized_partitions(cl, "inventory")
            .await
            .unwrap(),
        1,
        "the count must agree with the deduped key set"
    );
}

#[tokio::test]
async fn test_register_assets_upserts_definition_and_keeps_history() {
    // register_assets bulk-upserts on the UNIQUE (code_location_id,
    // asset_key) index. Re-resolving a code location must refresh the
    // asset's definition without wiping what it has materialized.
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
        .await
        .expect("failed to create rocksdb storage");
    let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;

    let materialized = AssetRecord {
        code_location_id: cl.to_string(),
        asset_key: "orders".to_string(),
        tags: vec!["old".to_string()],
        kinds: vec!["table".to_string()],
        asset_group: Some("raw".to_string()),
        code_version: Some("v1".to_string()),
        last_event_id: Some("event-1".to_string()),
        last_run_id: Some("run-1".to_string()),
        last_timestamp: Some(1_700_000_000),
        last_data_version: Some("data-1".to_string()),
        last_materialization_code_version: Some("v1".to_string()),
        last_input_data_versions: vec![("upstream".to_string(), "data-0".to_string())],
        pool: vec![("cpu".to_string(), 1)],
    };
    storage
        .register_assets(cl, std::slice::from_ref(&materialized))
        .await
        .expect("first registration failed");

    // A re-resolve carries the new definition and no materialization state.
    let redefined = AssetRecord {
        tags: vec!["new".to_string()],
        kinds: vec!["view".to_string()],
        asset_group: Some("curated".to_string()),
        code_version: Some("v2".to_string()),
        last_event_id: None,
        last_run_id: None,
        last_timestamp: None,
        last_data_version: None,
        last_materialization_code_version: None,
        last_input_data_versions: Vec::new(),
        pool: vec![("cpu".to_string(), 4)],
        ..materialized.clone()
    };
    storage
        .register_assets(cl, std::slice::from_ref(&redefined))
        .await
        .expect("re-registration failed");

    let stored = storage
        .get_asset_record(cl, "orders")
        .await
        .expect("lookup failed")
        .expect("asset missing after re-registration");

    // Definition fields take the new values.
    assert_eq!(stored.tags, vec!["new".to_string()]);
    assert_eq!(stored.kinds, vec!["view".to_string()]);
    assert_eq!(stored.asset_group, Some("curated".to_string()));
    assert_eq!(stored.code_version, Some("v2".to_string()));
    assert_eq!(stored.pool, vec![("cpu".to_string(), 4)]);

    // Materialization history survives.
    assert_eq!(stored.last_event_id, Some("event-1".to_string()));
    assert_eq!(stored.last_run_id, Some("run-1".to_string()));
    assert_eq!(stored.last_timestamp, Some(1_700_000_000));
    assert_eq!(stored.last_data_version, Some("data-1".to_string()));
    assert_eq!(
        stored.last_materialization_code_version,
        Some("v1".to_string())
    );
    assert_eq!(
        stored.last_input_data_versions,
        vec![("upstream".to_string(), "data-0".to_string())]
    );

    // Still one row, not two.
    let all = storage.get_asset_records(cl).await.expect("list failed");
    assert_eq!(all.len(), 1, "upsert created a duplicate row");
}

/// `store_events`/`store_event` upsert `asset_partitions` on the UNIQUE index to replace rather than duplicate.
#[tokio::test]
async fn test_partition_row_replaces_on_unique_index() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
        .await
        .expect("failed to create rocksdb storage");
    let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
    let pk = PartitionKey::Single {
        keys: vec!["p1".to_string()],
    };

    let mut first = make_event("inventory", "r", 1);
    first.partition_key = Some(pk.clone());
    storage.store_event(&first).await.unwrap();
    let mut second = make_event("inventory", "r", 2);
    second.partition_key = Some(pk.clone());
    storage.store_event(&second).await.unwrap();

    // Upsert on the unique index updates in place: one row, latest values.
    let parts = storage
        .get_materialized_partitions(cl, "inventory")
        .await
        .unwrap();
    assert_eq!(
        parts,
        vec![pk.clone()],
        "must not duplicate the partition row"
    );
    let ts = storage
        .get_partition_timestamps(cl, "inventory")
        .await
        .unwrap();
    assert_eq!(
        ts,
        vec![(pk.clone(), 2)],
        "must update the existing row in place"
    );
}

/// Per-partition lookups receive the display string and must still match a persisted Multi key.
#[tokio::test]
async fn test_partition_string_lookup_matches_multi_keys() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
        .await
        .expect("failed to create rocksdb storage");
    let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
    let pk = PartitionKey::Multi {
        dims: vec![
            ("date".to_string(), vec!["2024-01-01".to_string()]),
            ("region".to_string(), vec!["eu".to_string()]),
        ],
    };
    let mut event = make_event("inv", "r1", 100);
    event.partition_key = Some(pk.clone());
    storage.store_event(&event).await.unwrap();

    let display = pk.to_display();
    assert_eq!(display, "date=2024-01-01|region=eu");
    let latest = storage
        .get_latest_materialization(cl, "inv", Some(&display))
        .await
        .unwrap();
    assert!(
        latest.is_some(),
        "display-form lookup must match the Multi event"
    );
    let events = storage
        .get_partition_events(cl, "inv", &display, 10)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);

    // Single-dim lookups keep working.
    let mut single = make_event("inv", "r2", 200);
    single.partition_key = Some(PartitionKey::Single {
        keys: vec!["p1".to_string()],
    });
    storage.store_event(&single).await.unwrap();
    assert!(
        storage
            .get_latest_materialization(cl, "inv", Some("p1"))
            .await
            .unwrap()
            .is_some()
    );
}

/// Display lookups must prefer the structured (Multi) reading over a legacy Single event with a Multi-looking key.
#[tokio::test]
async fn test_partition_string_lookup_prefers_structured_multi() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
        .await
        .expect("failed to create rocksdb storage");
    let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
    let multi = PartitionKey::Multi {
        dims: vec![
            ("date".to_string(), vec!["2024-01-01".to_string()]),
            ("region".to_string(), vec!["eu".to_string()]),
        ],
    };
    let display = multi.to_display();

    // Legacy event from the asset's static-keyed era — NEWER timestamp.
    let mut old_single = make_event("inv", "r1", 200);
    old_single.partition_key = Some(PartitionKey::Single {
        keys: vec![display.clone()],
    });
    storage.store_event(&old_single).await.unwrap();

    let mut multi_event = make_event("inv", "r2", 100);
    multi_event.partition_key = Some(multi.clone());
    storage.store_event(&multi_event).await.unwrap();

    let latest = storage
        .get_latest_materialization(cl, "inv", Some(&display))
        .await
        .unwrap()
        .expect("lookup must match");
    assert_eq!(
        latest.run_id, "r2",
        "the structured Multi reading wins over a newer legacy Single row"
    );
    let events = storage
        .get_partition_events(cl, "inv", &display, 10)
        .await
        .unwrap();
    assert_eq!(events.len(), 1, "only the Multi partition's events return");
    assert_eq!(events[0].run_id, "r2");

    // The Single reading still works when no Multi rows exist.
    let mut plain = make_event("inv2", "r3", 100);
    plain.partition_key = Some(PartitionKey::Single {
        keys: vec![display.clone()],
    });
    storage.store_event(&plain).await.unwrap();
    assert!(
        storage
            .get_latest_materialization(cl, "inv2", Some(&display))
            .await
            .unwrap()
            .is_some(),
        "falls back to the Single reading"
    );
}

#[tokio::test]
async fn test_store_and_retrieve_event() {
    let storage = make_storage().await;
    register(&storage, &["my_asset"]).await;
    let event = make_event("my_asset", "run_1", 1000);
    let event_id = storage.store_event(&event).await.unwrap();
    assert!(!event_id.is_empty());

    let events = storage
        .get_events_for_asset(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_asset", 10)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    let actual = &events[0];
    let expected = StoredEvent {
        id: actual.id.clone(),
        event_type: EventType::Materialization { data_version: None },
        asset_key: Some("my_asset".to_string()),
        run_id: "run_1".to_string(),
        partition_key: None,
        timestamp: 1000,
        metadata: vec![],
        code_version: None,
        input_data_versions: vec![],
    };
    assert_eq!(*actual, expected);
}

#[tokio::test]
async fn test_events_ordered_by_timestamp_desc() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    storage
        .store_event(&make_event("a", "r1", 100))
        .await
        .unwrap();
    storage
        .store_event(&make_event("a", "r2", 300))
        .await
        .unwrap();
    storage
        .store_event(&make_event("a", "r3", 200))
        .await
        .unwrap();

    let events = storage
        .get_events_for_asset(crate::storage::DEFAULT_CODE_LOCATION_ID, "a", 10)
        .await
        .unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].timestamp, 300);
    assert_eq!(events[1].timestamp, 200);
    assert_eq!(events[2].timestamp, 100);
}

#[tokio::test]
async fn test_events_for_run() {
    let storage = make_storage().await;
    register(&storage, &["a", "b", "c"]).await;
    storage
        .store_event(&make_event("a", "run_x", 100))
        .await
        .unwrap();
    storage
        .store_event(&make_event("b", "run_x", 200))
        .await
        .unwrap();
    storage
        .store_event(&make_event("c", "run_y", 300))
        .await
        .unwrap();

    let events = storage.get_events_for_run("run_x").await.unwrap();
    assert_eq!(events.len(), 2);
    // Ordered ASC by timestamp
    let expected_0 = StoredEvent {
        id: events[0].id.clone(),
        event_type: EventType::Materialization { data_version: None },
        asset_key: Some("a".to_string()),
        run_id: "run_x".to_string(),
        partition_key: None,
        timestamp: 100,
        metadata: vec![],
        code_version: None,
        input_data_versions: vec![],
    };
    let expected_1 = StoredEvent {
        id: events[1].id.clone(),
        event_type: EventType::Materialization { data_version: None },
        asset_key: Some("b".to_string()),
        run_id: "run_x".to_string(),
        partition_key: None,
        timestamp: 200,
        metadata: vec![],
        code_version: None,
        input_data_versions: vec![],
    };
    assert_eq!(events[0], expected_0);
    assert_eq!(events[1], expected_1);
}

#[tokio::test]
async fn test_asset_record_upsert_on_materialization() {
    let storage = make_storage().await;
    register(&storage, &["my_asset"]).await;
    let event_id_1 = storage
        .store_event(&make_event("my_asset", "r1", 100))
        .await
        .unwrap();

    let record = storage
        .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_asset")
        .await
        .unwrap()
        .unwrap();
    let expected = AssetRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        asset_key: "my_asset".to_string(),
        tags: vec![],
        kinds: vec![],
        asset_group: None,
        code_version: None,
        last_event_id: Some(event_id_1),
        last_run_id: Some("r1".to_string()),
        last_timestamp: Some(100),
        last_data_version: None,
        last_materialization_code_version: None,
        last_input_data_versions: vec![],
        pool: vec![],
    };
    assert_eq!(record, expected);

    // Store another materialization — should update
    let event_id_2 = storage
        .store_event(&make_event("my_asset", "r2", 200))
        .await
        .unwrap();
    let record = storage
        .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_asset")
        .await
        .unwrap()
        .unwrap();
    let expected = AssetRecord {
        last_event_id: Some(event_id_2),
        last_run_id: Some("r2".to_string()),
        last_timestamp: Some(200),
        ..expected
    };
    assert_eq!(record, expected);
}

#[tokio::test]
async fn test_get_asset_records() {
    let storage = make_storage().await;
    register(&storage, &["a", "b"]).await;
    let eid_a = storage
        .store_event(&make_event("a", "r1", 100))
        .await
        .unwrap();
    let eid_b = storage
        .store_event(&make_event("b", "r1", 200))
        .await
        .unwrap();

    let mut records = storage
        .get_asset_records(crate::storage::DEFAULT_CODE_LOCATION_ID)
        .await
        .unwrap();
    records.sort_by(|a, b| a.asset_key.cmp(&b.asset_key));
    assert_eq!(records.len(), 2);

    let expected_a = AssetRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        asset_key: "a".to_string(),
        tags: vec![],
        kinds: vec![],
        asset_group: None,
        code_version: None,
        last_event_id: Some(eid_a),
        last_run_id: Some("r1".to_string()),
        last_timestamp: Some(100),
        last_data_version: None,
        last_materialization_code_version: None,
        last_input_data_versions: vec![],
        pool: vec![],
    };
    let expected_b = AssetRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        asset_key: "b".to_string(),
        last_event_id: Some(eid_b),
        last_run_id: Some("r1".to_string()),
        last_timestamp: Some(200),
        ..expected_a.clone()
    };
    assert_eq!(records[0], expected_a);
    assert_eq!(records[1], expected_b);
}

#[tokio::test]
async fn test_latest_materialization() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    storage
        .store_event(&make_event("a", "r1", 100))
        .await
        .unwrap();
    storage
        .store_event(&make_event("a", "r2", 200))
        .await
        .unwrap();

    let latest = storage
        .get_latest_materialization(crate::storage::DEFAULT_CODE_LOCATION_ID, "a", None)
        .await
        .unwrap()
        .unwrap();
    let expected = StoredEvent {
        id: latest.id.clone(),
        event_type: EventType::Materialization { data_version: None },
        asset_key: Some("a".to_string()),
        run_id: "r2".to_string(),
        partition_key: None,
        timestamp: 200,
        metadata: vec![],
        code_version: None,
        input_data_versions: vec![],
    };
    assert_eq!(latest, expected);
}

#[tokio::test]
async fn test_count_materialized_partitions() {
    let storage = make_storage().await;
    register(&storage, &["a", "b"]).await;

    let n = storage
        .count_materialized_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "a")
        .await
        .unwrap();
    assert_eq!(n, 0);

    let materialize = |asset: &str, run: &str, key: &str, ts: i64| EventRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type: EventType::Materialization { data_version: None },
        asset_key: Some(asset.to_string()),
        run_id: run.to_string(),
        partition_key: Some(PartitionKey::Single {
            keys: vec![key.to_string()],
        }),
        timestamp: ts,
        metadata: vec![],
        input_data_versions: vec![],
    };

    for ev in [
        materialize("a", "r1", "p1", 100),
        materialize("a", "r2", "p2", 200),
        materialize("a", "r3", "p3", 300),
        materialize("a", "r4", "p1", 400),
        // A different asset's partition must not leak into "a"'s count.
        materialize("b", "r5", "p9", 500),
    ] {
        storage.store_event(&ev).await.unwrap();
    }

    let n = storage
        .count_materialized_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "a")
        .await
        .unwrap();
    assert_eq!(
        n, 3,
        "3 distinct partitions despite the p1 re-materialization"
    );

    // The aggregate agrees with the full row enumeration.
    let rows = storage
        .get_materialized_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "a")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);

    // Scoped per asset.
    let nb = storage
        .count_materialized_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "b")
        .await
        .unwrap();
    assert_eq!(nb, 1);
}

#[tokio::test]
async fn test_count_dynamic_partitions() {
    let storage = make_storage().await;
    let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;

    // Empty namespace → 0 (GROUP ALL returns no row → None → 0).
    assert_eq!(
        storage
            .count_dynamic_partitions(cl, "customers")
            .await
            .unwrap(),
        0
    );

    // add_dynamic_partitions dedupes, so a repeated key must not double-count.
    storage
        .add_dynamic_partitions(
            cl,
            "customers",
            &["acme".into(), "globex".into(), "initech".into()],
        )
        .await
        .unwrap();
    storage
        .add_dynamic_partitions(cl, "customers", &["acme".into()])
        .await
        .unwrap();
    assert_eq!(
        storage
            .count_dynamic_partitions(cl, "customers")
            .await
            .unwrap(),
        3,
        "3 distinct customers despite the duplicate add"
    );

    // Scoped per namespace.
    storage
        .add_dynamic_partitions(cl, "regions", &["us".into(), "eu".into()])
        .await
        .unwrap();
    assert_eq!(
        storage
            .count_dynamic_partitions(cl, "regions")
            .await
            .unwrap(),
        2
    );

    // Agrees with the full key enumeration.
    assert_eq!(
        storage
            .get_dynamic_partitions(cl, "customers")
            .await
            .unwrap()
            .len(),
        3
    );
}

/// Dynamic keys feed the canonical display form (`dim=v|dim=v`, values
/// joined with ','); the storage write path is the choke point for every
/// transport, so reserved separator characters and empty keys must be
/// rejected here.
#[tokio::test]
async fn test_add_dynamic_partitions_rejects_reserved_and_empty_keys() {
    let storage = make_storage().await;
    let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
    for bad in ["us|eu", "a,b", ""] {
        let result = storage
            .add_dynamic_partitions(cl, "users", &[bad.to_string()])
            .await;
        assert!(result.is_err(), "key {bad:?} must be rejected");
    }
    assert!(
        storage
            .get_dynamic_partitions(cl, "users")
            .await
            .unwrap()
            .is_empty(),
        "rejected keys must not be persisted"
    );
}

#[tokio::test]
async fn test_latest_materialization_with_partition() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;

    let event1 = EventRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type: EventType::Materialization { data_version: None },
        asset_key: Some("a".to_string()),
        run_id: "r1".to_string(),
        partition_key: Some(PartitionKey::Single {
            keys: vec!["2024-01".to_string()],
        }),
        timestamp: 100,
        metadata: vec![],
        input_data_versions: vec![],
    };
    let event2 = EventRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type: EventType::Materialization { data_version: None },
        asset_key: Some("a".to_string()),
        run_id: "r2".to_string(),
        partition_key: Some(PartitionKey::Single {
            keys: vec!["2024-02".to_string()],
        }),
        timestamp: 200,
        metadata: vec![],
        input_data_versions: vec![],
    };
    storage.store_event(&event1).await.unwrap();
    storage.store_event(&event2).await.unwrap();

    let latest = storage
        .get_latest_materialization(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "a",
            Some("2024-01"),
        )
        .await
        .unwrap();
    assert!(latest.is_some());
    assert_eq!(latest.unwrap().timestamp, 100);

    let latest = storage
        .get_latest_materialization(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "a",
            Some("2024-02"),
        )
        .await
        .unwrap();
    assert_eq!(latest.unwrap().timestamp, 200);
}

#[tokio::test]
async fn test_observation_does_not_upsert_asset() {
    let storage = make_storage().await;

    let event = EventRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type: EventType::Observation { data_version: None },
        asset_key: Some("obs_asset".to_string()),
        run_id: "r1".to_string(),
        partition_key: None,
        timestamp: 100,
        metadata: vec![],
        input_data_versions: vec![],
    };
    storage.store_event(&event).await.unwrap();

    // Observation should not create an asset record
    let record = storage
        .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "obs_asset")
        .await
        .unwrap();
    assert!(record.is_none());
}

#[tokio::test]
async fn test_observation_updates_registered_asset() {
    let storage = make_storage().await;

    // Register the asset first
    let record = AssetRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        asset_key: "ext_feed".to_string(),
        tags: vec![],
        kinds: vec![],
        asset_group: None,
        code_version: None,
        last_event_id: None,
        last_run_id: None,
        last_timestamp: None,
        last_data_version: None,
        last_materialization_code_version: None,
        last_input_data_versions: vec![],
        pool: vec![],
    };
    storage
        .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, &[record])
        .await
        .unwrap();

    // Store an observation event
    let event = EventRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type: EventType::Observation {
            data_version: Some("obs_v1".to_string()),
        },
        asset_key: Some("ext_feed".to_string()),
        run_id: String::new(),
        partition_key: None,
        timestamp: 5000,
        metadata: vec![],
        input_data_versions: vec![],
    };
    storage.store_event(&event).await.unwrap();

    // Verify observation updated last_timestamp and last_data_version
    let updated = storage
        .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "ext_feed")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated.last_timestamp, Some(5000));
    assert_eq!(updated.last_data_version.as_deref(), Some("obs_v1"));
    assert!(updated.last_event_id.is_some());
    // Observation should NOT set materialization-specific fields
    assert!(updated.last_run_id.is_none());
    assert!(updated.last_materialization_code_version.is_none());
    assert!(updated.last_input_data_versions.is_empty());
}

/// An action's `ActionResult.materialized()` reports no upstream
/// provenance — it merged existing data rather than consuming inputs.
/// Writing that empty list through would erase the asset's real
/// provenance and flip it to Stale forever.
#[tokio::test]
async fn test_empty_input_data_versions_preserves_existing() {
    let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
    let consumed = vec![("a".to_string(), "dv-a".to_string())];

    for batched in [false, true] {
        let storage = make_storage().await;
        register(&storage, &["a", "b"]).await;

        let mut materialize = make_event("b", "run-1", 1000);
        materialize.input_data_versions = consumed.clone();
        let mut action = make_event("b", "run-2", 2000);
        action.event_type = EventType::Materialization {
            data_version: Some("merged-v2".to_string()),
        };

        if batched {
            storage.store_events(&[materialize, action]).await.unwrap();
        } else {
            storage.store_event(&materialize).await.unwrap();
            storage.store_event(&action).await.unwrap();
        }

        let record = storage.get_asset_record(cl, "b").await.unwrap().unwrap();
        assert_eq!(
            record.last_input_data_versions, consumed,
            "provenance erased by an empty write (batched={batched})"
        );
        assert_eq!(record.last_data_version.as_deref(), Some("merged-v2"));
    }
}

#[tokio::test]
async fn test_observation_does_not_overwrite_materialization_fields() {
    let storage = make_storage().await;

    let record = AssetRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        asset_key: "ext_feed".to_string(),
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
        pool: vec![],
    };
    storage
        .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, &[record])
        .await
        .unwrap();

    // First: materialize (sets run_id, materialization_code_version)
    let run = RunRecord {
        run_id: "run_1".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("j".to_string()),
        status: RunStatus::Success,
        start_time: 1000,
        end_time: Some(2000),
        tags: vec![],
        node_names: vec!["ext_feed".to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    storage.create_run(&run).await.unwrap();
    let mat_event = EventRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type: EventType::Materialization {
            data_version: Some("mat_v1".to_string()),
        },
        asset_key: Some("ext_feed".to_string()),
        run_id: "run_1".to_string(),
        partition_key: None,
        timestamp: 3000,
        metadata: vec![],
        input_data_versions: vec![],
    };
    storage.store_event(&mat_event).await.unwrap();

    let after_mat = storage
        .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "ext_feed")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_mat.last_run_id.as_deref(), Some("run_1"));
    assert_eq!(
        after_mat.last_materialization_code_version.as_deref(),
        Some("v1")
    );

    // Then: observe (should update timestamp/data_version but NOT run_id/mcv)
    let obs_event = EventRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type: EventType::Observation {
            data_version: Some("obs_v2".to_string()),
        },
        asset_key: Some("ext_feed".to_string()),
        run_id: String::new(),
        partition_key: None,
        timestamp: 6000,
        metadata: vec![],
        input_data_versions: vec![],
    };
    storage.store_event(&obs_event).await.unwrap();

    let after_obs = storage
        .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "ext_feed")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_obs.last_timestamp, Some(6000));
    assert_eq!(after_obs.last_data_version.as_deref(), Some("obs_v2"));
    // Materialization fields preserved
    assert_eq!(after_obs.last_run_id.as_deref(), Some("run_1"));
    assert_eq!(
        after_obs.last_materialization_code_version.as_deref(),
        Some("v1")
    );
}

#[tokio::test]
async fn test_run_lifecycle() {
    let storage = make_storage().await;
    let run = RunRecord {
        run_id: "run_1".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("my_job".to_string()),
        status: RunStatus::NotStarted,
        start_time: 1000,
        end_time: None,
        tags: vec![("env".to_string(), "prod".to_string())],
        node_names: vec!["a".to_string(), "b".to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    storage.create_run(&run).await.unwrap();

    let fetched = storage.get_run("run_1").await.unwrap().unwrap();
    assert_eq!(fetched, run);

    // Update to started
    storage
        .update_run_status("run_1", RunStatus::Started, None)
        .await
        .unwrap();
    let fetched = storage.get_run("run_1").await.unwrap().unwrap();
    assert_eq!(
        fetched,
        RunRecord {
            status: RunStatus::Started,
            ..run.clone()
        }
    );

    // Update to success with end_time
    storage
        .update_run_status("run_1", RunStatus::Success, Some(2000))
        .await
        .unwrap();
    let fetched = storage.get_run("run_1").await.unwrap().unwrap();
    assert_eq!(
        fetched,
        RunRecord {
            status: RunStatus::Success,
            end_time: Some(2000),
            ..run
        }
    );
}

#[tokio::test]
async fn test_create_run_swallows_duplicate_id_after_retry() {
    use crate::storage::retry;

    let storage = make_storage().await;
    let run = RunRecord {
        run_id: "duplicate_run".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("j".to_string()),
        status: RunStatus::NotStarted,
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
    };

    // First CREATE succeeds.
    storage.create_run(&run).await.unwrap();

    // Second CREATE with the same run_id is swallowed (treated as success).
    storage
        .create_run(&run)
        .await
        .expect("duplicate create_run should be swallowed as phantom-commit success");

    // The row is still the original (no overwrite, no extra row).
    let fetched = storage.get_run("duplicate_run").await.unwrap().unwrap();
    assert_eq!(fetched, run);

    let raw_err = storage
        .db
        .create::<Option<RunRecord>>("runs")
        .content(run.clone())
        .await
        .expect_err("direct CREATE bypassing swallow must still error");
    assert!(raw_err.is_internal());
    assert!(raw_err.message().contains("already contains"));
    let anyhow_err = anyhow::Error::from(raw_err);
    assert!(retry::is_unique_index_violation(&anyhow_err));
    assert!(!retry::default_should_retry(&anyhow_err));
}

#[tokio::test]
async fn test_get_runs_with_limit() {
    let storage = make_storage().await;
    let mut all_runs = Vec::new();
    for i in 0..5i64 {
        let run = RunRecord {
            run_id: format!("run_{}", i),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("job".to_string()),
            status: RunStatus::Success,
            start_time: i * 100,
            end_time: Some(i * 100 + 50),
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        };
        storage.create_run(&run).await.unwrap();
        all_runs.push(run);
    }

    // get_runs returns DESC by start_time, limit 3 → newest 3
    let runs = storage.get_all_runs(3, None).await.unwrap();
    assert_eq!(runs.len(), 3);
    assert_eq!(runs[0], all_runs[4]); // run_4, start_time=400
    assert_eq!(runs[1], all_runs[3]); // run_3, start_time=300
    assert_eq!(runs[2], all_runs[2]); // run_2, start_time=200
}

#[tokio::test]
async fn test_get_runs_filtered_by_status() {
    let storage = make_storage().await;

    let run_ok = RunRecord {
        run_id: "ok".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("j".to_string()),
        status: RunStatus::Success,
        start_time: 100,
        end_time: Some(200),
        tags: vec![],
        node_names: vec![],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    let run_fail = RunRecord {
        run_id: "fail".to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("j".to_string()),
        status: RunStatus::Failure,
        start_time: 300,
        end_time: Some(400),
        tags: vec![],
        node_names: vec![],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    storage.create_run(&run_ok).await.unwrap();
    storage.create_run(&run_fail).await.unwrap();

    let failures = storage
        .get_all_runs(10, Some(RunStatus::Failure))
        .await
        .unwrap();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0], run_fail);
}

#[tokio::test]
async fn test_get_all_runs_page_pagination_and_total() {
    let storage = make_storage().await;
    for i in 0..7i64 {
        let run = RunRecord {
            run_id: format!("page_{}", i),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".into()),
            status: RunStatus::Success,
            start_time: i * 100,
            end_time: Some(i * 100 + 10),
            tags: vec![],
            node_names: vec![],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        };
        storage.create_run(&run).await.unwrap();
    }

    let page = storage
        .get_all_runs_page(0, 3, &RunFilter::default())
        .await
        .unwrap();
    assert_eq!(page.total, 7);
    assert_eq!(page.rows.len(), 3);
    // DESC by start_time → newest first.
    assert_eq!(page.rows[0].run_id, "page_6");
    assert_eq!(page.rows[1].run_id, "page_5");
    assert_eq!(page.rows[2].run_id, "page_4");

    let page = storage
        .get_all_runs_page(3, 3, &RunFilter::default())
        .await
        .unwrap();
    assert_eq!(page.total, 7);
    assert_eq!(page.rows.len(), 3);
    assert_eq!(page.rows[0].run_id, "page_3");
    assert_eq!(page.rows[2].run_id, "page_1");

    let page = storage
        .get_all_runs_page(6, 3, &RunFilter::default())
        .await
        .unwrap();
    assert_eq!(page.total, 7);
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].run_id, "page_0");
}

#[tokio::test]
async fn test_get_all_runs_page_filters() {
    let storage = make_storage().await;
    for (i, (job, status, assets, tags)) in [
        (
            "daily_ingest",
            RunStatus::Success,
            vec!["orders"],
            vec![("partition", "2024-01-01")],
        ),
        (
            "daily_export",
            RunStatus::Failure,
            vec!["orders", "revenue"],
            vec![("partition_key", "2024-01-02")],
        ),
        (
            "weekly_report",
            RunStatus::Success,
            vec!["users"],
            vec![("other", "x")],
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let run = RunRecord {
            run_id: format!("r{}", i),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some(job.to_string()),
            status,
            start_time: i as i64 * 100,
            end_time: None,
            tags: tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            node_names: assets.iter().map(|s| s.to_string()).collect(),
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        };
        storage.create_run(&run).await.unwrap();
    }

    // Status filter
    let filter = RunFilter {
        status: Some(RunStatus::Success),
        ..Default::default()
    };
    let page = storage.get_all_runs_page(0, 10, &filter).await.unwrap();
    assert_eq!(page.total, 2);
    assert!(page.rows.iter().all(|r| r.status == RunStatus::Success));

    // Verb filter: action runs are separable from materializations, so a
    // "what did the purge touch" question doesn't scan every run.
    let mut action_run = RunRecord {
        run_id: "run-delete".into(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: None,
        status: RunStatus::Success,
        start_time: 9000,
        end_time: Some(9001),
        tags: vec![],
        node_names: vec!["events".into()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: Some("delete".into()),
        config: None,
    };
    storage.create_run(&action_run).await.unwrap();
    action_run.run_id = "run-compact".into();
    action_run.action = Some("compact".into());
    storage.create_run(&action_run).await.unwrap();

    let filter = RunFilter {
        action: Some(Some("delete".into())),
        ..Default::default()
    };
    let page = storage.get_all_runs_page(0, 10, &filter).await.unwrap();
    assert_eq!(page.total, 1);
    assert_eq!(page.rows[0].run_id, "run-delete");

    let filter = RunFilter {
        action: Some(None),
        ..Default::default()
    };
    let page = storage.get_all_runs_page(0, 50, &filter).await.unwrap();
    assert!(
        page.rows.iter().all(|r| r.action.is_none()),
        "materialize filter must exclude action runs"
    );
    assert!(
        page.total >= 3,
        "materialize runs still match, got {}",
        page.total
    );

    // Job substring (case-insensitive)
    let filter = RunFilter {
        job_substring: Some("DAILY".into()),
        ..Default::default()
    };
    let page = storage.get_all_runs_page(0, 10, &filter).await.unwrap();
    assert_eq!(page.total, 2);
    assert!(
        page.rows
            .iter()
            .all(|r| r.job_name.as_deref().is_some_and(|n| n.contains("daily")))
    );

    // Asset substring
    let filter = RunFilter {
        asset_substring: Some("ORDER".into()),
        ..Default::default()
    };
    let page = storage.get_all_runs_page(0, 10, &filter).await.unwrap();
    assert_eq!(page.total, 2);

    // Partition tag substring — matches both "partition" and "partition_key" keys
    let filter = RunFilter {
        partition_substring: Some("2024-01".into()),
        ..Default::default()
    };
    let page = storage.get_all_runs_page(0, 10, &filter).await.unwrap();
    assert_eq!(page.total, 2);

    // Combined: status + job substring
    let filter = RunFilter {
        status: Some(RunStatus::Success),
        job_substring: Some("daily".into()),
        ..Default::default()
    };
    let page = storage.get_all_runs_page(0, 10, &filter).await.unwrap();
    assert_eq!(page.total, 1);
    assert_eq!(page.rows[0].job_name.as_deref(), Some("daily_ingest"));

    let filter = RunFilter {
        job_name: Some("daily_ingest".into()),
        ..Default::default()
    };
    let page = storage.get_all_runs_page(0, 10, &filter).await.unwrap();
    assert_eq!(page.total, 1);
    assert_eq!(page.rows[0].job_name.as_deref(), Some("daily_ingest"));
}

#[tokio::test]
async fn test_get_all_last_run_per_job() {
    let storage = make_storage().await;
    let specs = [
        ("job_a", "r1", 100i64),
        ("job_a", "r2", 300),
        ("job_a", "r3", 200),
        ("job_b", "r4", 500),
        ("job_c_no_runs_expected", "", -1),
    ];
    for (job, rid, start) in &specs[..4] {
        storage
            .create_run(&RunRecord {
                run_id: rid.to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some(job.to_string()),
                status: RunStatus::Success,
                start_time: *start,
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

    let out = storage
        .get_all_last_run_per_job(&[
            "job_a".into(),
            "job_b".into(),
            "job_c_no_runs_expected".into(),
        ])
        .await
        .unwrap();
    // job_c has no runs → excluded.
    assert_eq!(out.len(), 2);
    let a = out.iter().find(|(n, _)| n == "job_a").unwrap();
    assert_eq!(a.1.run_id, "r2"); // highest start_time
    let b = out.iter().find(|(n, _)| n == "job_b").unwrap();
    assert_eq!(b.1.run_id, "r4");
}

/// Empty input must round-trip without a query.
#[tokio::test]
async fn test_get_all_last_run_per_job_empty_input() {
    let storage = make_storage().await;
    let out = storage.get_all_last_run_per_job(&[]).await.unwrap();
    assert!(out.is_empty());
}

/// Guard for the multi-statement batching: distinct job names must all resolve in a single `.query()` call.
#[tokio::test]
async fn test_get_all_last_run_per_job_batches_many_jobs_correctly() {
    let storage = make_storage().await;
    const N_JOBS: usize = 50;
    for i in 0..N_JOBS {
        for k in 0..3 {
            let start = (i * 1_000 + k * 10) as i64;
            storage
                .create_run(&RunRecord {
                    run_id: format!("j{i}_r{k}"),
                    code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                    job_name: Some(format!("batched_job_{i}")),
                    status: RunStatus::Success,
                    start_time: start,
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
    }

    let mut names: Vec<String> = (0..N_JOBS).map(|i| format!("batched_job_{i}")).collect();
    names.rotate_left(17);
    let out = storage.get_all_last_run_per_job(&names).await.unwrap();

    assert_eq!(out.len(), N_JOBS, "every job should resolve");
    for (job_name, run) in &out {
        let expected_suffix = "_r2";
        assert!(
            run.run_id.ends_with(expected_suffix),
            "job {job_name} resolved to wrong run_id {}",
            run.run_id
        );
        assert_eq!(
            run.job_name.as_deref(),
            Some(job_name.as_str()),
            "bind key / statement index mismatch: request {job_name} got {:?}",
            run.job_name
        );
    }
}

#[tokio::test]
async fn test_get_all_runs_summary_counts() {
    let storage = make_storage().await;
    let now_ns = 10_000_000_000i64;
    let cutoff = now_ns - 3_000_000_000;
    // 2 Success (one inside 24h cutoff, one outside)
    // 1 Failure inside
    // 1 Started inside
    // 1 Queued inside, 1 NotStarted inside
    let spec = [
        (RunStatus::Success, now_ns - 1_000_000_000),
        (RunStatus::Success, now_ns - 10_000_000_000),
        (RunStatus::Failure, now_ns - 500_000_000),
        (RunStatus::Started, now_ns - 200_000_000),
        (RunStatus::Queued, now_ns - 100_000_000),
        (RunStatus::NotStarted, now_ns - 50_000_000),
    ];
    for (i, (status, start)) in spec.into_iter().enumerate() {
        let run = RunRecord {
            run_id: format!("s{i}"),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".into()),
            status,
            start_time: start,
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
        storage.create_run(&run).await.unwrap();
    }

    let summary = storage.get_all_runs_summary(cutoff).await.unwrap();
    assert_eq!(summary.total, 6);
    assert_eq!(summary.success, 2);
    assert_eq!(summary.failure, 1);
    assert_eq!(summary.in_progress, 1);
    assert_eq!(summary.queued, 2);
    // 5 of 6 runs are inside the 24h window (Success outside is excluded).
    assert_eq!(summary.last_24h, 5);
}

#[tokio::test]
async fn test_subscribe_table_yields_on_change() {
    use futures_util::StreamExt;
    use std::time::Duration;
    let storage = std::sync::Arc::new(make_storage().await);

    let mut stream = storage.subscribe_table("runs").await.unwrap();

    // Create a run from another task; then poll the stream with a timeout.
    let storage_w = storage.clone();
    tokio::spawn(async move {
        // Small delay so the live query is registered before we write.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let run = RunRecord {
            run_id: "live_r".into(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".into()),
            status: RunStatus::Queued,
            start_time: 1,
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
        storage_w.create_run(&run).await.unwrap();
    });

    let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("timed out waiting for notification");
    assert!(first.is_some(), "expected a notification, got None");
}

/// Each table fed by an `rivers-ui` LIVE channel must wake its `subscribe_table` stream on a write.
#[tokio::test]
async fn test_subscribe_table_wakes_for_every_live_channel_table() {
    use futures_util::StreamExt;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::timeout;

    // Helper: subscribe → brief delay to let the LIVE query register →
    // run the write → assert the stream yields within the deadline.
    async fn expect_yields(
        storage: &Arc<SurrealStorage>,
        table: &'static str,
        write: impl std::future::Future<Output = ()>,
    ) {
        let mut stream = storage
            .subscribe_table(table)
            .await
            .unwrap_or_else(|e| panic!("subscribe_table({table}) failed: {e}"));
        tokio::time::sleep(Duration::from_millis(100)).await;
        write.await;
        let first = timeout(Duration::from_secs(3), stream.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for notification on `{table}`"));
        assert!(
            first.is_some(),
            "live-query stream for `{table}` ended before any notification"
        );
    }

    let storage = Arc::new(make_storage().await);

    // `runs` table.
    expect_yields(&storage, "runs", async {
        let run = RunRecord {
            run_id: "live_runs".into(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("j".into()),
            status: RunStatus::Queued,
            start_time: 1,
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
        storage.create_run(&run).await.unwrap();
    })
    .await;

    // `assets` table.
    expect_yields(&storage, "assets", async {
        storage
            .register_assets(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[make_asset_record("live_asset")],
            )
            .await
            .unwrap();
    })
    .await;

    expect_yields(&storage, "asset_partitions", async {
        storage
            .db
            .query(
                "CREATE asset_partitions SET \
                     asset_key = 'live_asset', \
                     partition_key = {kind: 'Single', keys: ['2024-01-01']}, \
                     last_timestamp = 1",
            )
            .await
            .unwrap();
    })
    .await;

    // `events` table.
    expect_yields(&storage, "events", async {
        let ev = make_event("live_asset", "live_runs", 1);
        storage.store_event(&ev).await.unwrap();
    })
    .await;

    // `backfills` table.
    expect_yields(&storage, "backfills", async {
        let bf = BackfillRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            backfill_id: "live_bf".into(),
            status: BackfillStatus::Requested,
            strategy: BackfillStrategy::MultiRun,
            failure_policy: BackfillFailurePolicy::Continue,
            asset_selection: vec!["live_asset".into()],
            job_name: None,
            partition_keys: vec![],
            run_ids: vec![],
            completed_partitions: vec![],
            failed_partitions: vec![],
            canceled_partitions: vec![],
            max_concurrency: 1,
            tags: vec![],
            create_time: 1,
            end_time: None,
            error: None,
            launched_by: LaunchedBy::default(),
            action: None,
            config: None,
        };
        storage.create_backfill(&bf).await.unwrap();
    })
    .await;

    // `ticks` table.
    expect_yields(&storage, "ticks", async {
        let tick = TickRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            automation_name: "live_sched".into(),
            automation_type: "Schedule".into(),
            status: "Success".into(),
            timestamp: 1,
            run_ids: vec![],
            backfill_ids: vec![],
            skip_reason: None,
            error: None,
            cursor: None,
        };
        storage.store_tick(&tick).await.unwrap();
    })
    .await;

    // `condition_ticks` table.
    expect_yields(&storage, "condition_ticks", async {
        let ct = ConditionTickRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            timestamp: 1,
            total_evaluated: 0,
            total_fired: 0,
            eval_duration_us: 0,
            run_ids: vec![],
            backfill_ids: vec![],
        };
        storage.store_condition_tick(&ct).await.unwrap();
    })
    .await;

    // `condition_evals` table.
    expect_yields(&storage, "condition_evals", async {
        let ev = ConditionEvalRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: "live_asset".into(),
            tick_id: "live_ct".into(),
            timestamp: 1,
            fired: false,
            eval_duration_us: 0,
            run_ids: vec![],
            tree_json: b"{}".to_vec(),
            selection_json: None,
        };
        storage.store_condition_evals_batch(&[ev]).await.unwrap();
    })
    .await;

    // `concurrency_pools` table.
    expect_yields(&storage, "concurrency_pools", async {
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "live_pool", 2, 60)
            .await
            .unwrap();
    })
    .await;

    expect_yields(&storage, "concurrency_slots", async {
        storage
            .db
            .query(
                "CREATE concurrency_slots SET \
                     pool_key = 'live_pool', run_id = 'live_runs', step_key = 's1', \
                     slots_consumed = 1, claimed_at = 1, \
                     lease_expires_at = 9999999999, last_heartbeat = 1",
            )
            .await
            .unwrap();
    })
    .await;

    // `pending_steps` table — same argument as `concurrency_slots`.
    expect_yields(&storage, "pending_steps", async {
        storage
            .db
            .query(
                "CREATE pending_steps SET \
                     pool_key = 'live_pool', run_id = 'live_pending', step_key = 's2', \
                     priority = 0, enqueued_at = 1, block_reason = 'PoolFull'",
            )
            .await
            .unwrap();
    })
    .await;
}

#[tokio::test]
async fn test_create_runs_batch() {
    let storage = make_storage().await;
    let runs: Vec<RunRecord> = (0..5)
        .map(|i| RunRecord {
            run_id: format!("batch_{i}"),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("job".to_string()),
            status: RunStatus::Queued,
            start_time: i * 100,
            end_time: None,
            tags: vec![],
            node_names: vec![format!("asset_{i}")],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .collect();

    storage.create_runs(&runs).await.unwrap();

    let stored = storage.get_all_runs(10, None).await.unwrap();
    assert_eq!(stored.len(), 5);
    for run in &runs {
        assert!(stored.iter().any(|r| r.run_id == run.run_id));
    }
}

#[tokio::test]
async fn test_create_runs_batch_empty() {
    let storage = make_storage().await;
    storage.create_runs(&[]).await.unwrap();
    let stored = storage.get_all_runs(10, None).await.unwrap();
    assert_eq!(stored.len(), 0);
}

#[tokio::test]
async fn test_create_runs_batch_preserves_fields() {
    let storage = make_storage().await;
    let runs = vec![
        RunRecord {
            run_id: "queued_1".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("__default__".to_string()),
            status: RunStatus::Queued,
            start_time: 1000,
            end_time: None,
            tags: vec![("env".to_string(), "prod".to_string())],
            node_names: vec!["a".to_string(), "b".to_string()],
            priority: 5,
            partition_key: Some(PartitionKey::Single {
                keys: vec!["2025-01-01".to_string()],
            }),
            block_reason: Some("global run limit".to_string()),
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        },
        RunRecord {
            run_id: "queued_2".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("my_job".to_string()),
            status: RunStatus::Queued,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec!["c".to_string()],
            priority: -10,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        },
    ];

    storage.create_runs(&runs).await.unwrap();

    let r1 = storage.get_run("queued_1").await.unwrap().unwrap();
    assert_eq!(r1.status, RunStatus::Queued);
    assert_eq!(r1.priority, 5);
    assert_eq!(r1.tags, vec![("env".to_string(), "prod".to_string())]);
    assert_eq!(r1.node_names, vec!["a".to_string(), "b".to_string()]);
    assert!(r1.partition_key.is_some());
    assert_eq!(r1.block_reason.as_deref(), Some("global run limit"));

    let r2 = storage.get_run("queued_2").await.unwrap().unwrap();
    assert_eq!(r2.job_name.as_deref(), Some("my_job"));
    assert_eq!(r2.priority, -10);
    assert!(r2.block_reason.is_none());
}

/// A run keeps the config overrides it was launched with on every path a
/// launcher reads from: the record (`create_run`), the queue
/// (`enqueue_run`) and the coordinator's projection. A run launched with
/// the defaults reads back `None` on all of them.
#[tokio::test]
async fn test_run_config_round_trips_to_every_launcher_read() {
    let storage = make_storage().await;
    let config = r#"{"a":{"threshold":0.9,"mode":"full"}}"#;
    let run = |id: &str, config: Option<&str>| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: None,
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
        config: config.map(str::to_string),
    };
    storage
        .create_run(&run("created", Some(config)))
        .await
        .unwrap();
    storage
        .enqueue_run(&run("queued", Some(config)))
        .await
        .unwrap();
    storage.create_run(&run("defaults", None)).await.unwrap();

    for id in ["created", "queued"] {
        let got = storage.get_run(id).await.unwrap().unwrap();
        assert_eq!(got.config.as_deref(), Some(config), "{id}");
    }
    let defaults = storage.get_run("defaults").await.unwrap().unwrap();
    assert_eq!(defaults.config, None);

    let (_, _, queued) = storage
        .coordinator_tick_query(DEFAULT_CODE_LOCATION_ID)
        .await
        .unwrap();
    assert_eq!(queued.len(), 3);
    for info in queued {
        let expected = (info.run_id != "defaults").then(|| config.to_string());
        assert_eq!(info.config, expected, "{}", info.run_id);
    }
}

/// A backfill keeps the config its child runs are launched with.
#[tokio::test]
async fn test_backfill_config_round_trips() {
    let storage = make_storage().await;
    let config = r#"{"a":{"mode":"full_refresh"}}"#;
    let bf = BackfillRecord {
        config: Some(config.to_string()),
        ..make_backfill("bf_cfg", BackfillStatus::Requested, 1)
    };
    storage.create_backfill(&bf).await.unwrap();
    let got = storage.get_backfill("bf_cfg").await.unwrap().unwrap();
    assert_eq!(got.config.as_deref(), Some(config));
}

#[tokio::test]
async fn test_kv_get_set() {
    let storage = make_storage().await;

    assert!(storage.kv_get("key1").await.unwrap().is_none());

    storage.kv_set("key1", b"hello").await.unwrap();
    let val = storage.kv_get("key1").await.unwrap().unwrap();
    assert_eq!(val, b"hello");

    // Overwrite
    storage.kv_set("key1", b"world").await.unwrap();
    let val = storage.kv_get("key1").await.unwrap().unwrap();
    assert_eq!(val, b"world");
}

#[tokio::test]
async fn test_kv_independent_keys() {
    let storage = make_storage().await;
    storage.kv_set("a", b"1").await.unwrap();
    storage.kv_set("b", b"2").await.unwrap();

    assert_eq!(storage.kv_get("a").await.unwrap().unwrap(), b"1");
    assert_eq!(storage.kv_get("b").await.unwrap().unwrap(), b"2");
}

#[tokio::test]
async fn test_kv_set_upserts_single_row() {
    // kv_set must be a single-statement upsert on the UNIQUE key index —
    // repeated writes keep exactly one row, and a crash can never observe
    // the key deleted (unlike the old DELETE+CREATE pair).
    let storage = make_storage().await;
    storage.kv_set("k", b"v1").await.unwrap();
    storage.kv_set("k", b"v2").await.unwrap();
    storage.kv_set("k", b"v3").await.unwrap();
    assert_eq!(storage.kv_get("k").await.unwrap().unwrap(), b"v3");
    let mut res = storage
        .db
        .query("SELECT * FROM kv WHERE key = $key")
        .bind(("key", "k".to_string()))
        .await
        .unwrap();
    let rows: Vec<DbKv> = res.take(0).unwrap();
    assert_eq!(rows.len(), 1, "upsert must keep exactly one row per key");
}

#[tokio::test]
async fn test_dynamic_keys_round_trip() {
    let storage = make_storage().await;
    let ctx = crate::storage::CodeLocationContext::default_for_tests();
    let scoped = storage.for_code_location(&ctx);

    // Absent → None.
    assert!(
        scoped
            .get_dynamic_keys("src", None, "dv-1")
            .await
            .unwrap()
            .is_none()
    );

    // Persist + read back.
    scoped
        .set_dynamic_keys("src", None, "dv-1", &["a".into(), "b".into()])
        .await
        .unwrap();
    assert_eq!(
        scoped.get_dynamic_keys("src", None, "dv-1").await.unwrap(),
        Some(vec!["a".into(), "b".into()])
    );

    // Different data_version → independent slot, prior is invisible.
    assert!(
        scoped
            .get_dynamic_keys("src", None, "dv-2")
            .await
            .unwrap()
            .is_none()
    );

    // Partition-scoped: same asset+dv but different partition is its own slot.
    let part = PartitionKey::Single {
        keys: vec!["2024-01-01".into()],
    };
    assert!(
        scoped
            .get_dynamic_keys("src", Some(&part), "dv-1")
            .await
            .unwrap()
            .is_none()
    );
    scoped
        .set_dynamic_keys("src", Some(&part), "dv-1", &["x".into()])
        .await
        .unwrap();
    assert_eq!(
        scoped
            .get_dynamic_keys("src", Some(&part), "dv-1")
            .await
            .unwrap(),
        Some(vec!["x".into()])
    );
    // Unpartitioned slot still intact.
    assert_eq!(
        scoped.get_dynamic_keys("src", None, "dv-1").await.unwrap(),
        Some(vec!["a".into(), "b".into()])
    );
}

#[tokio::test]
async fn test_dynamic_keys_isolated_per_code_location() {
    let storage = make_storage().await;
    let ctx_a = crate::storage::CodeLocationContext::new("cl-a");
    let ctx_b = crate::storage::CodeLocationContext::new("cl-b");

    storage
        .for_code_location(&ctx_a)
        .set_dynamic_keys("src", None, "dv", &["from-a".into()])
        .await
        .unwrap();

    // CL-B sees nothing under the same asset/dv.
    assert!(
        storage
            .for_code_location(&ctx_b)
            .get_dynamic_keys("src", None, "dv")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn test_event_with_metadata() {
    let storage = make_storage().await;
    register(&storage, &["m"]).await;
    let event = EventRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type: EventType::Materialization { data_version: None },
        asset_key: Some("m".to_string()),
        run_id: "r".to_string(),
        partition_key: None,
        timestamp: 500,
        metadata: vec![
            ("path".to_string(), "/data/out.parquet".to_string()),
            ("rows".to_string(), "1000".to_string()),
        ],
        input_data_versions: vec![],
    };
    storage.store_event(&event).await.unwrap();

    let events = storage
        .get_events_for_asset(crate::storage::DEFAULT_CODE_LOCATION_ID, "m", 1)
        .await
        .unwrap();
    assert_eq!(events[0].metadata.len(), 2);
    assert_eq!(
        events[0].metadata[0],
        ("path".to_string(), "/data/out.parquet".to_string())
    );
    assert_eq!(
        events[0].metadata[1],
        ("rows".to_string(), "1000".to_string())
    );
}

#[tokio::test]
async fn test_events_limit() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    for i in 0..10 {
        storage
            .store_event(&make_event("a", &format!("r{}", i), i))
            .await
            .unwrap();
    }

    let events = storage
        .get_events_for_asset(crate::storage::DEFAULT_CODE_LOCATION_ID, "a", 3)
        .await
        .unwrap();
    assert_eq!(events.len(), 3);
}

#[tokio::test]
async fn test_nonexistent_asset_returns_none() {
    let storage = make_storage().await;
    assert!(
        storage
            .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "nope")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        storage
            .get_latest_materialization(crate::storage::DEFAULT_CODE_LOCATION_ID, "nope", None)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn test_nonexistent_run_returns_none() {
    let storage = make_storage().await;
    assert!(storage.get_run("nope").await.unwrap().is_none());
}

#[tokio::test]
async fn test_register_assets_with_catalog_fields() {
    let storage = make_storage().await;
    let records = vec![
        AssetRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: "x".to_string(),
            tags: vec!["etl".to_string(), "daily".to_string()],
            kinds: vec!["table".to_string()],
            asset_group: Some("analytics".to_string()),
            code_version: Some("v1".to_string()),
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![],
        },
        AssetRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            asset_key: "y".to_string(),
            tags: vec!["ml".to_string()],
            kinds: vec!["model".to_string()],
            asset_group: None,
            code_version: None,
            last_event_id: None,
            last_run_id: None,
            last_timestamp: None,
            last_data_version: None,
            last_materialization_code_version: None,
            last_input_data_versions: vec![],
            pool: vec![],
        },
    ];
    storage
        .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, &records)
        .await
        .unwrap();

    let x = storage
        .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "x")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(x, records[0]);

    let y = storage
        .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "y")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(y, records[1]);
}

#[tokio::test]
async fn test_register_preserves_materialization_fields() {
    let storage = make_storage().await;
    let records = vec![AssetRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        asset_key: "a".to_string(),
        tags: vec!["etl".to_string()],
        kinds: vec!["table".to_string()],
        asset_group: None,
        code_version: None,
        last_event_id: None,
        last_run_id: None,
        last_timestamp: None,
        last_data_version: None,
        last_materialization_code_version: None,
        last_input_data_versions: vec![],
        pool: vec![],
    }];
    storage
        .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, &records)
        .await
        .unwrap();

    // Materialize
    storage
        .store_event(&make_event("a", "r1", 100))
        .await
        .unwrap();

    // Re-register (e.g. next run) — should preserve last_run_id etc.
    let records = vec![AssetRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        asset_key: "a".to_string(),
        tags: vec!["etl".to_string(), "new_tag".to_string()],
        kinds: vec!["table".to_string()],
        asset_group: Some("warehouse".to_string()),
        code_version: Some("v2".to_string()),
        last_event_id: None,
        last_run_id: None,
        last_timestamp: None,
        last_data_version: None,
        last_materialization_code_version: None,
        last_input_data_versions: vec![],
        pool: vec![],
    }];
    storage
        .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, &records)
        .await
        .unwrap();

    let record = storage
        .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "a")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.tags, vec!["etl", "new_tag"]);
    assert_eq!(record.asset_group.as_deref(), Some("warehouse"));
    assert_eq!(record.code_version.as_deref(), Some("v2"));
    // Materialization fields preserved
    assert_eq!(record.last_run_id.as_deref(), Some("r1"));
    assert_eq!(record.last_timestamp, Some(100));
}

#[tokio::test]
async fn test_materialization_preserves_catalog_fields() {
    let storage = make_storage().await;
    let records = vec![AssetRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        asset_key: "a".to_string(),
        tags: vec!["etl".to_string()],
        kinds: vec!["table".to_string()],
        asset_group: Some("analytics".to_string()),
        code_version: Some("v1".to_string()),
        last_event_id: None,
        last_run_id: None,
        last_timestamp: None,
        last_data_version: None,
        last_materialization_code_version: None,
        last_input_data_versions: vec![],
        pool: vec![],
    }];
    storage
        .register_assets(crate::storage::DEFAULT_CODE_LOCATION_ID, &records)
        .await
        .unwrap();

    // Materialize — should update last_* but preserve tags/kind/group
    storage
        .store_event(&make_event("a", "r1", 500))
        .await
        .unwrap();

    let record = storage
        .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "a")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.tags, vec!["etl"]);
    assert_eq!(record.kinds, vec!["table"]);
    assert_eq!(record.asset_group.as_deref(), Some("analytics"));
    assert_eq!(record.code_version.as_deref(), Some("v1"));
    assert_eq!(record.last_run_id.as_deref(), Some("r1"));
    assert_eq!(record.last_timestamp, Some(500));
}

#[tokio::test]
async fn test_get_assets_by_tag() {
    let storage = make_storage().await;
    storage
        .register_assets(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[
                AssetRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    asset_key: "a".to_string(),
                    tags: vec!["etl".to_string(), "daily".to_string()],
                    kinds: vec![],
                    asset_group: None,
                    code_version: None,
                    last_event_id: None,
                    last_run_id: None,
                    last_timestamp: None,
                    last_data_version: None,
                    last_materialization_code_version: None,
                    last_input_data_versions: vec![],
                    pool: vec![],
                },
                AssetRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    asset_key: "b".to_string(),
                    tags: vec!["ml".to_string()],
                    kinds: vec![],
                    asset_group: None,
                    code_version: None,
                    last_event_id: None,
                    last_run_id: None,
                    last_timestamp: None,
                    last_data_version: None,
                    last_materialization_code_version: None,
                    last_input_data_versions: vec![],
                    pool: vec![],
                },
                AssetRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    asset_key: "c".to_string(),
                    tags: vec!["etl".to_string()],
                    kinds: vec![],
                    asset_group: None,
                    code_version: None,
                    last_event_id: None,
                    last_run_id: None,
                    last_timestamp: None,
                    last_data_version: None,
                    last_materialization_code_version: None,
                    last_input_data_versions: vec![],
                    pool: vec![],
                },
            ],
        )
        .await
        .unwrap();

    let etl = storage
        .get_assets_by_tag(crate::storage::DEFAULT_CODE_LOCATION_ID, "etl")
        .await
        .unwrap();
    assert_eq!(etl.len(), 2);

    let ml = storage
        .get_assets_by_tag(crate::storage::DEFAULT_CODE_LOCATION_ID, "ml")
        .await
        .unwrap();
    assert_eq!(ml.len(), 1);
    assert_eq!(ml[0].asset_key, "b");

    let none = storage
        .get_assets_by_tag(crate::storage::DEFAULT_CODE_LOCATION_ID, "nonexistent")
        .await
        .unwrap();
    assert!(none.is_empty());
}

#[tokio::test]
async fn test_get_assets_by_kind() {
    let storage = make_storage().await;
    storage
        .register_assets(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[
                AssetRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    asset_key: "a".to_string(),
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
                    pool: vec![],
                },
                AssetRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    asset_key: "b".to_string(),
                    tags: vec![],
                    kinds: vec!["model".to_string()],
                    asset_group: None,
                    code_version: None,
                    last_event_id: None,
                    last_run_id: None,
                    last_timestamp: None,
                    last_data_version: None,
                    last_materialization_code_version: None,
                    last_input_data_versions: vec![],
                    pool: vec![],
                },
            ],
        )
        .await
        .unwrap();

    let tables = storage
        .get_assets_by_kind(crate::storage::DEFAULT_CODE_LOCATION_ID, "table")
        .await
        .unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].asset_key, "a");
}

#[tokio::test]
async fn test_dynamic_partitions_add_and_get() {
    let storage = make_storage().await;
    storage
        .add_dynamic_partitions(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "users",
            &["u1".into(), "u2".into(), "u3".into()],
        )
        .await
        .unwrap();

    let keys = storage
        .get_dynamic_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "users")
        .await
        .unwrap();
    assert_eq!(keys, vec!["u1", "u2", "u3"]);
}

#[tokio::test]
async fn test_dynamic_partitions_idempotent_add() {
    let storage = make_storage().await;
    storage
        .add_dynamic_partitions(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "users",
            &["u1".into(), "u2".into()],
        )
        .await
        .unwrap();
    // Add again with overlap — should not duplicate
    storage
        .add_dynamic_partitions(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "users",
            &["u2".into(), "u3".into()],
        )
        .await
        .unwrap();

    let keys = storage
        .get_dynamic_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "users")
        .await
        .unwrap();
    assert_eq!(keys, vec!["u1", "u2", "u3"]);
}

#[tokio::test]
async fn test_dynamic_partitions_delete() {
    let storage = make_storage().await;
    storage
        .add_dynamic_partitions(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "users",
            &["u1".into(), "u2".into(), "u3".into()],
        )
        .await
        .unwrap();

    storage
        .delete_dynamic_partition(crate::storage::DEFAULT_CODE_LOCATION_ID, "users", "u2")
        .await
        .unwrap();

    let keys = storage
        .get_dynamic_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "users")
        .await
        .unwrap();
    assert_eq!(keys, vec!["u1", "u3"]);
}

#[tokio::test]
async fn test_dynamic_partitions_delete_nonexistent() {
    let storage = make_storage().await;
    // Should not error when deleting non-existent partition
    storage
        .delete_dynamic_partition(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "users",
            "nonexistent",
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dynamic_partitions_has() {
    let storage = make_storage().await;
    storage
        .add_dynamic_partitions(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "users",
            &["u1".into()],
        )
        .await
        .unwrap();

    assert!(
        storage
            .has_dynamic_partition(crate::storage::DEFAULT_CODE_LOCATION_ID, "users", "u1")
            .await
            .unwrap()
    );
    assert!(
        !storage
            .has_dynamic_partition(crate::storage::DEFAULT_CODE_LOCATION_ID, "users", "u2")
            .await
            .unwrap()
    );
    assert!(
        !storage
            .has_dynamic_partition(crate::storage::DEFAULT_CODE_LOCATION_ID, "other", "u1")
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_dynamic_partitions_isolated_by_name() {
    let storage = make_storage().await;
    storage
        .add_dynamic_partitions(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "users",
            &["u1".into()],
        )
        .await
        .unwrap();
    storage
        .add_dynamic_partitions(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "products",
            &["p1".into(), "p2".into()],
        )
        .await
        .unwrap();

    let users = storage
        .get_dynamic_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "users")
        .await
        .unwrap();
    assert_eq!(users, vec!["u1"]);

    let products = storage
        .get_dynamic_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "products")
        .await
        .unwrap();
    assert_eq!(products, vec!["p1", "p2"]);
}

#[tokio::test]
async fn test_dynamic_partitions_empty() {
    let storage = make_storage().await;
    let keys = storage
        .get_dynamic_partitions(crate::storage::DEFAULT_CODE_LOCATION_ID, "nonexistent")
        .await
        .unwrap();
    assert!(keys.is_empty());
}

#[tokio::test]
async fn test_get_assets_by_group() {
    let storage = make_storage().await;
    storage
        .register_assets(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[
                AssetRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    asset_key: "a".to_string(),
                    tags: vec![],
                    kinds: vec![],
                    asset_group: Some("analytics".to_string()),
                    code_version: None,
                    last_event_id: None,
                    last_run_id: None,
                    last_timestamp: None,
                    last_data_version: None,
                    last_materialization_code_version: None,
                    last_input_data_versions: vec![],
                    pool: vec![],
                },
                AssetRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    asset_key: "b".to_string(),
                    tags: vec![],
                    kinds: vec![],
                    asset_group: Some("analytics".to_string()),
                    code_version: None,
                    last_event_id: None,
                    last_run_id: None,
                    last_timestamp: None,
                    last_data_version: None,
                    last_materialization_code_version: None,
                    last_input_data_versions: vec![],
                    pool: vec![],
                },
                AssetRecord {
                    code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                    asset_key: "c".to_string(),
                    tags: vec![],
                    kinds: vec![],
                    asset_group: Some("ml".to_string()),
                    code_version: None,
                    last_event_id: None,
                    last_run_id: None,
                    last_timestamp: None,
                    last_data_version: None,
                    last_materialization_code_version: None,
                    last_input_data_versions: vec![],
                    pool: vec![],
                },
            ],
        )
        .await
        .unwrap();

    let analytics = storage
        .get_assets_by_group(crate::storage::DEFAULT_CODE_LOCATION_ID, "analytics")
        .await
        .unwrap();
    assert_eq!(analytics.len(), 2);

    let ml = storage
        .get_assets_by_group(crate::storage::DEFAULT_CODE_LOCATION_ID, "ml")
        .await
        .unwrap();
    assert_eq!(ml.len(), 1);
}
