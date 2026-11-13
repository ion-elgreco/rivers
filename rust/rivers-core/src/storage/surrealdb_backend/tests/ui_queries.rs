use super::*;

// ── UI integration regression tests ──────────────────────────────────

#[tokio::test]
async fn test_events_same_timestamp_deterministic_order() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;

    for etype in [
        EventType::StepStart,
        EventType::Materialization { data_version: None },
        EventType::StepSuccess,
    ] {
        storage
            .store_event(&EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: etype,
                asset_key: Some("a".to_string()),
                run_id: "r1".to_string(),
                partition_key: None,
                timestamp: 1000,
                metadata: vec![],
                input_data_versions: vec![],
            })
            .await
            .unwrap();
    }

    // Query twice — order must be identical (deterministic via id sort)
    let run_a = storage.get_events_for_run("r1").await.unwrap();
    let run_b = storage.get_events_for_run("r1").await.unwrap();
    assert_eq!(run_a.len(), 3);
    for (a, b) in run_a.iter().zip(run_b.iter()) {
        assert_eq!(a.event_type, b.event_type);
    }

    let asset_a = storage
        .get_events_for_asset(crate::storage::DEFAULT_CODE_LOCATION_ID, "a", 10)
        .await
        .unwrap();
    let asset_b = storage
        .get_events_for_asset(crate::storage::DEFAULT_CODE_LOCATION_ID, "a", 10)
        .await
        .unwrap();
    assert_eq!(asset_a.len(), 3);
    for (a, b) in asset_a.iter().zip(asset_b.iter()) {
        assert_eq!(a.event_type, b.event_type);
    }
}

#[tokio::test]
async fn test_runs_filtered_by_status() {
    let storage = make_storage().await;

    for (id, status) in [
        ("r1", RunStatus::Success),
        ("r2", RunStatus::Success),
        ("r3", RunStatus::Failure),
        ("r4", RunStatus::Started),
        ("r5", RunStatus::NotStarted),
    ] {
        storage
            .create_run(&RunRecord {
                run_id: id.to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: Some("job1".to_string()),
                status,
                start_time: 1000,
                end_time: None,
                execution_start_time: None,
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

    let all = storage.get_all_runs(100, None).await.unwrap();
    assert_eq!(all.len(), 5);

    let success = storage
        .get_all_runs(100, Some(RunStatus::Success))
        .await
        .unwrap();
    assert_eq!(success.len(), 2);
    assert!(success.iter().all(|r| r.status == RunStatus::Success));

    let failure = storage
        .get_all_runs(100, Some(RunStatus::Failure))
        .await
        .unwrap();
    assert_eq!(failure.len(), 1);
    assert_eq!(failure[0].run_id, "r3");

    let started = storage
        .get_all_runs(100, Some(RunStatus::Started))
        .await
        .unwrap();
    assert_eq!(started.len(), 1);

    let not_started = storage
        .get_all_runs(100, Some(RunStatus::NotStarted))
        .await
        .unwrap();
    assert_eq!(not_started.len(), 1);
}

#[tokio::test]
async fn test_runs_for_asset_filtering() {
    let storage = make_storage().await;
    register(&storage, &["asset_a", "asset_b"]).await;

    storage
        .create_run(&RunRecord {
            run_id: "r1".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("job1".to_string()),
            status: RunStatus::Success,
            start_time: 1000,
            end_time: Some(1010),
            execution_start_time: None,
            tags: vec![],
            node_names: vec!["asset_a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();
    storage
        .create_run(&RunRecord {
            run_id: "r2".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("job1".to_string()),
            status: RunStatus::Success,
            start_time: 2000,
            end_time: Some(2010),
            execution_start_time: None,
            tags: vec![],
            node_names: vec!["asset_a".to_string(), "asset_b".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();
    storage
        .create_run(&RunRecord {
            run_id: "r3".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("job2".to_string()),
            status: RunStatus::Failure,
            start_time: 3000,
            end_time: Some(3005),
            execution_start_time: None,
            tags: vec![],
            node_names: vec!["asset_b".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();

    // Filter by asset_a — should get r1 and r2
    let all = storage.get_all_runs(1000, None).await.unwrap();
    let for_a: Vec<_> = all
        .iter()
        .filter(|r| r.node_names.contains(&"asset_a".to_string()))
        .collect();
    assert_eq!(for_a.len(), 2);

    // Filter by asset_b — should get r2 and r3
    let for_b: Vec<_> = all
        .iter()
        .filter(|r| r.node_names.contains(&"asset_b".to_string()))
        .collect();
    assert_eq!(for_b.len(), 2);
}

#[tokio::test]
async fn test_ticks_ordered_desc_and_counted() {
    let storage = make_storage().await;

    let ticks: Vec<TickRecord> = (0..5)
        .map(|i| TickRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            automation_name: "my_sensor".to_string(),
            automation_type: "Sensor".to_string(),
            status: "Success".to_string(),
            timestamp: 1000 + i * 60,
            run_ids: vec![],
            backfill_ids: vec![],
            skip_reason: None,
            error: None,
            cursor: Some(format!("cursor_{i}")),
        })
        .collect();
    storage.store_ticks_batch(&ticks).await.unwrap();

    let stored = storage
        .get_ticks(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_sensor", 100)
        .await
        .unwrap();
    assert_eq!(stored.len(), 5);
    // Should be ordered DESC by timestamp — full struct equality
    for (idx, actual) in stored.iter().enumerate() {
        let i = 4 - idx as i64; // maps to original index (DESC)
        let expected = StoredTick {
            id: actual.id.clone(),
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            automation_name: "my_sensor".to_string(),
            automation_type: "Sensor".to_string(),
            status: "Success".to_string(),
            timestamp: 1000 + i * 60,
            run_ids: vec![],
            backfill_ids: vec![],
            skip_reason: None,
            error: None,
            cursor: Some(format!("cursor_{i}")),
        };
        assert_eq!(*actual, expected);
    }

    // Limit works
    let limited = storage
        .get_ticks(crate::storage::DEFAULT_CODE_LOCATION_ID, "my_sensor", 2)
        .await
        .unwrap();
    assert_eq!(limited.len(), 2);
    assert_eq!(limited[0].timestamp, 1240);

    // Different automation name returns empty
    let other = storage
        .get_ticks(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "other_sensor",
            100,
        )
        .await
        .unwrap();
    assert!(other.is_empty());
}

#[tokio::test]
async fn test_runs_with_tags_and_node_names() {
    let storage = make_storage().await;

    storage
        .create_run(&RunRecord {
            run_id: "r1".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("daily_job".to_string()),
            status: RunStatus::Success,
            start_time: 1000,
            end_time: Some(1060),
            execution_start_time: None,
            tags: vec![("partition".to_string(), "2024-01-01".to_string())],
            node_names: vec!["orders".to_string(), "revenue".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();
    storage
        .create_run(&RunRecord {
            run_id: "r2".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("weekly_job".to_string()),
            status: RunStatus::Failure,
            start_time: 2000,
            end_time: Some(2120),
            execution_start_time: None,
            tags: vec![("partition".to_string(), "2024-01-07".to_string())],
            node_names: vec!["summary".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();

    let all = storage.get_all_runs(100, None).await.unwrap();
    assert_eq!(all.len(), 2);

    // Verify tags are preserved
    let r1 = all.iter().find(|r| r.run_id == "r1").unwrap();
    assert_eq!(r1.tags.len(), 1);
    assert_eq!(
        r1.tags[0],
        ("partition".to_string(), "2024-01-01".to_string())
    );
    assert_eq!(r1.node_names, vec!["orders", "revenue"]);

    // Verify node_names on r2
    let r2 = all.iter().find(|r| r.run_id == "r2").unwrap();
    assert_eq!(r2.node_names, vec!["summary"]);
}

#[tokio::test]
async fn test_events_batch_same_timestamp_deterministic() {
    let storage = make_storage().await;
    register(&storage, &["x"]).await;

    let events = vec![
        EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepStart,
            asset_key: Some("x".to_string()),
            run_id: "batch_run".to_string(),
            partition_key: None,
            timestamp: 5000,
            metadata: vec![],
            input_data_versions: vec![],
        },
        EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization {
                data_version: Some("v1".to_string()),
            },
            asset_key: Some("x".to_string()),
            run_id: "batch_run".to_string(),
            partition_key: None,
            timestamp: 5000,
            metadata: vec![],
            input_data_versions: vec![],
        },
        EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepSuccess,
            asset_key: Some("x".to_string()),
            run_id: "batch_run".to_string(),
            partition_key: None,
            timestamp: 5000,
            metadata: vec![],
            input_data_versions: vec![],
        },
    ];
    let ids = storage.store_events(&events).await.unwrap();
    assert_eq!(ids.len(), 3);

    // Query twice — order must be deterministic and full struct equality
    let run_a = storage.get_events_for_run("batch_run").await.unwrap();
    let run_b = storage.get_events_for_run("batch_run").await.unwrap();
    assert_eq!(run_a.len(), 3);
    for (a, b) in run_a.iter().zip(run_b.iter()) {
        assert_eq!(a, b);
    }
}

#[tokio::test]
async fn test_step_retry_event_round_trips_with_metadata() {
    use crate::execution::retry::meta;
    let storage = make_storage().await;
    register(&storage, &["flaky"]).await;

    let events = vec![
        EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepFailure,
            asset_key: Some("flaky".to_string()),
            run_id: "retry_run".to_string(),
            partition_key: None,
            timestamp: 1000,
            metadata: vec![
                (meta::ATTEMPT.to_string(), "1".to_string()),
                (meta::REASON.to_string(), "out_of_memory".to_string()),
            ],
            input_data_versions: vec![],
        },
        EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepRetry,
            asset_key: Some("flaky".to_string()),
            run_id: "retry_run".to_string(),
            partition_key: None,
            timestamp: 1001,
            metadata: vec![
                (meta::ATTEMPT.to_string(), "1".to_string()),
                (meta::REASON.to_string(), "out_of_memory".to_string()),
                (meta::NEXT_DELAY_MS.to_string(), "5000".to_string()),
                (
                    meta::NEXT_COMPUTE.to_string(),
                    r#"{"memory":"16Gi"}"#.to_string(),
                ),
            ],
            input_data_versions: vec![],
        },
    ];
    storage.store_events(&events).await.unwrap();

    let read = storage.get_events_for_run("retry_run").await.unwrap();
    assert_eq!(read.len(), 2);
    let retry = read
        .iter()
        .find(|e| e.event_type == EventType::StepRetry)
        .expect("StepRetry event must survive the SurrealDB + SurrealKV round-trip");
    let get = |k: &str| {
        retry
            .metadata
            .iter()
            .find(|(mk, _)| mk == k)
            .map(|(_, v)| v.as_str())
    };
    assert_eq!(get(meta::NEXT_DELAY_MS), Some("5000"));
    assert_eq!(get(meta::NEXT_COMPUTE), Some(r#"{"memory":"16Gi"}"#));
    assert_eq!(get(meta::REASON), Some("out_of_memory"));
}

#[tokio::test]
async fn test_input_versions_from_event_not_storage() {
    let storage = make_storage().await;
    register(&storage, &["upstream", "downstream"]).await;

    // Store graph topology so staleness computation knows the dependency
    use crate::assets::graph::TopologyNode;
    let topology = GraphTopology {
        nodes: vec![
            TopologyNode::new("upstream", crate::assets::graph::NodeKind::Asset),
            TopologyNode::new("downstream", crate::assets::graph::NodeKind::Asset),
        ],
        edges: vec![("downstream".to_string(), "upstream".to_string())],
    };
    let topo_json = serde_json::to_vec(&topology).unwrap();
    storage
        .kv_set(
            &crate::graph_topology_key(crate::storage::DEFAULT_CODE_LOCATION_ID),
            &topo_json,
        )
        .await
        .unwrap();

    // 1. Materialize upstream with data_version "v1"
    storage
        .store_event(&EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization {
                data_version: Some("v1".to_string()),
            },
            asset_key: Some("upstream".to_string()),
            run_id: "r1".to_string(),
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
            asset_key: Some("upstream".to_string()),
            run_id: "r2".to_string(),
            partition_key: None,
            timestamp: 200,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    // At this point upstream's last_data_version in storage is "v2".
    // But downstream actually read "v1" during its execution.

    storage
        .store_event(&EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization {
                data_version: Some("dv_down".to_string()),
            },
            asset_key: Some("downstream".to_string()),
            run_id: "r1".to_string(),
            partition_key: None,
            timestamp: 300,
            metadata: vec![],
            input_data_versions: vec![("upstream".to_string(), "v1".to_string())],
        })
        .await
        .unwrap();

    // 4. Verify: downstream recorded that it consumed "v1"
    let down = storage
        .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "downstream")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        down.last_input_data_versions,
        vec![("upstream".to_string(), "v1".to_string())],
        "Should record the version the executor actually read, not the current storage value"
    );

    let staleness = crate::staleness::compute_staleness(
        &[
            storage
                .get_asset_record(crate::storage::DEFAULT_CODE_LOCATION_ID, "upstream")
                .await
                .unwrap()
                .unwrap(),
            down.clone(),
        ],
        &[("downstream".to_string(), "upstream".to_string())],
    );
    let (status, causes) = staleness.get("downstream").unwrap();
    assert_eq!(status, &StaleStatus::Stale);
    assert!(!causes.is_empty());
}

mod remote_staleness {
    use std::collections::HashMap;

    use super::*;
    use crate::assets::graph::{NodeKind, TopologyNode};
    use crate::assets::remote::{LocationRef, RemoteRef};
    use crate::storage::{CodeLocationContext, CodeLocationDirectoryEntry, StaleCauseCategory};

    fn remote(local: &str, location: &str, asset_key: &str) -> TopologyNode {
        TopologyNode {
            remote: Some(RemoteRef {
                code_location: LocationRef::new("dev", location).unwrap(),
                asset_key: asset_key.to_string(),
            }),
            ..TopologyNode::new(local, NodeKind::Asset)
        }
    }

    async fn materialize(
        storage: &SurrealStorage,
        identity: &str,
        asset: &str,
        dv: &str,
        inputs: &[(&str, &str)],
        ts: i64,
    ) {
        storage
            .store_event(&EventRecord {
                code_location_id: identity.to_string(),
                event_type: EventType::Materialization {
                    data_version: Some(dv.to_string()),
                },
                input_data_versions: inputs
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                ..make_event(asset, &format!("{identity}-{ts}"), ts)
            })
            .await
            .unwrap();
    }

    async fn register_as(storage: &SurrealStorage, identity: &str, keys: &[&str]) {
        let records: Vec<AssetRecord> = keys.iter().map(|k| make_asset_record(k)).collect();
        storage.register_assets(identity, &records).await.unwrap();
    }

    /// `analytics` reads `raw_orders` of `warehouse` as `orders`, and
    /// `ghost` of a location that never registered.
    async fn setup() -> SurrealStorage {
        let storage = make_storage().await;
        storage
            .upsert_code_location_registration(
                &LocationRef::new("dev", "warehouse").unwrap(),
                CodeLocationDirectoryEntry::new("warehouse"),
            )
            .await
            .unwrap();
        register_as(&storage, "warehouse", &["raw_orders"]).await;
        // `orders` was local once and left a row.
        register_as(&storage, "analytics", &["orders", "order_count"]).await;
        materialize(&storage, "analytics", "orders", "local-dv", &[], 50).await;
        storage
            .set_graph_topology(
                "analytics",
                &GraphTopology {
                    nodes: vec![
                        remote("orders", "warehouse", "raw_orders"),
                        remote("ghost", "nowhere", "ghost"),
                        TopologyNode::new("order_count", NodeKind::Asset),
                    ],
                    edges: vec![
                        ("order_count".into(), "orders".into()),
                        ("order_count".into(), "ghost".into()),
                    ],
                },
                &[],
            )
            .await
            .unwrap();
        storage
    }

    async fn staleness(
        storage: &SurrealStorage,
    ) -> HashMap<String, (StaleStatus, Vec<crate::storage::StaleCause>)> {
        let ctx = CodeLocationContext::new("analytics");
        storage
            .for_code_location(&ctx)
            .compute_staleness()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn consumer_follows_the_owners_data_version() {
        let storage = setup().await;
        materialize(&storage, "warehouse", "raw_orders", "a1", &[], 100).await;
        materialize(
            &storage,
            "analytics",
            "order_count",
            "b1",
            &[("orders", "a1")],
            200,
        )
        .await;

        let result = staleness(&storage).await;
        assert_eq!(result.keys().collect::<Vec<_>>(), vec!["order_count"]);
        assert_eq!(result["order_count"], (StaleStatus::UpToDate, vec![]));

        materialize(&storage, "warehouse", "raw_orders", "a2", &[], 300).await;
        let (status, causes) = &staleness(&storage).await["order_count"];
        assert_eq!(status, &StaleStatus::Stale);
        assert_eq!(
            causes
                .iter()
                .map(|c| (&c.category, c.reason.as_str()))
                .collect::<Vec<_>>(),
            vec![(
                &StaleCauseCategory::Data {
                    dependency: "orders".into()
                },
                "upstream 'orders' has new data version"
            )]
        );

        materialize(
            &storage,
            "analytics",
            "order_count",
            "b2",
            &[("orders", "a2")],
            400,
        )
        .await;
        assert_eq!(
            staleness(&storage).await["order_count"],
            (StaleStatus::UpToDate, vec![])
        );
    }

    /// The owner's code and inputs make the owner stale, not the consumer.
    #[tokio::test]
    async fn owners_code_version_is_not_the_consumers() {
        let storage = setup().await;
        let mut owner = make_asset_record("raw_orders");
        owner.code_version = Some("v1".into());
        storage
            .register_assets("warehouse", &[owner.clone()])
            .await
            .unwrap();
        materialize(&storage, "warehouse", "raw_orders", "a1", &[], 100).await;
        owner.code_version = Some("v2".into());
        storage
            .register_assets("warehouse", &[owner])
            .await
            .unwrap();
        let owner_scope = CodeLocationContext::new("warehouse");
        let owner_staleness = storage
            .for_code_location(&owner_scope)
            .compute_staleness()
            .await
            .unwrap();
        assert_eq!(owner_staleness["raw_orders"].0, StaleStatus::Stale);

        materialize(
            &storage,
            "analytics",
            "order_count",
            "b1",
            &[("orders", "a1")],
            200,
        )
        .await;

        assert_eq!(
            staleness(&storage).await["order_count"],
            (StaleStatus::UpToDate, vec![])
        );
    }

    /// One record per remote node that has an owner record, in node order.
    #[tokio::test]
    async fn upstream_records_follow_the_node_order_across_owners() {
        let storage = make_storage().await;
        for (location, identity) in [("warehouse", "wh"), ("shop", "sh")] {
            storage
                .upsert_code_location_registration(
                    &LocationRef::new("dev", location).unwrap(),
                    CodeLocationDirectoryEntry::new(identity),
                )
                .await
                .unwrap();
        }
        register_as(&storage, "wh", &["raw_orders", "customers"]).await;
        register_as(&storage, "sh", &["raw_orders"]).await;
        materialize(&storage, "wh", "raw_orders", "wh-orders", &[], 100).await;
        materialize(&storage, "wh", "customers", "wh-customers", &[], 110).await;
        materialize(&storage, "sh", "raw_orders", "sh-orders", &[], 120).await;
        let topology = GraphTopology {
            nodes: vec![
                remote("shop_orders", "shop", "raw_orders"),
                remote("orders", "warehouse", "raw_orders"),
                remote("missing", "warehouse", "no_such_asset"),
                TopologyNode::new("local", NodeKind::Asset),
                remote("customers", "warehouse", "customers"),
                remote("ghost", "nowhere", "ghost"),
                remote("orders_again", "warehouse", "raw_orders"),
            ],
            ..Default::default()
        };

        let ctx = CodeLocationContext::new("analytics");
        let records = storage
            .for_code_location(&ctx)
            .remote_upstream_records(&topology)
            .await
            .unwrap();

        assert_eq!(
            records
                .iter()
                .map(|r| (
                    r.asset_key.as_str(),
                    r.code_location_id.as_str(),
                    r.last_data_version.as_deref()
                ))
                .collect::<Vec<_>>(),
            vec![
                ("shop_orders", "sh", Some("sh-orders")),
                ("orders", "wh", Some("wh-orders")),
                ("customers", "wh", Some("wh-customers")),
                ("orders_again", "wh", Some("wh-orders")),
            ]
        );
    }

    /// `report` is stale through `seed`, two steps up; `order_count` through
    /// the remote `orders`. `orders` keeps the row from when it was local.
    #[tokio::test]
    async fn staleness_of_keys_equals_the_staleness_of_all_assets() {
        let storage = setup().await;
        let mut seed = make_asset_record("seed");
        seed.code_version = Some("v1".into());
        storage
            .register_assets("analytics", &[seed.clone()])
            .await
            .unwrap();
        register_as(
            &storage,
            "analytics",
            &["clean", "report", "fresh", "never", "unrelated"],
        )
        .await;
        materialize(&storage, "analytics", "seed", "s1", &[], 100).await;
        seed.code_version = Some("v2".into());
        storage.register_assets("analytics", &[seed]).await.unwrap();
        materialize(&storage, "analytics", "clean", "c1", &[("seed", "s1")], 110).await;
        materialize(
            &storage,
            "analytics",
            "report",
            "r1",
            &[("clean", "c1")],
            120,
        )
        .await;
        materialize(&storage, "analytics", "fresh", "f1", &[], 130).await;
        materialize(
            &storage,
            "analytics",
            "unrelated",
            "u1",
            &[("report", "r1")],
            140,
        )
        .await;
        materialize(&storage, "warehouse", "raw_orders", "a1", &[], 150).await;
        materialize(
            &storage,
            "analytics",
            "order_count",
            "b1",
            &[("orders", "a1")],
            160,
        )
        .await;
        materialize(&storage, "warehouse", "raw_orders", "a2", &[], 170).await;

        let edge = |from: &str, to: &str| (from.to_string(), to.to_string());
        let topology = GraphTopology {
            nodes: vec![
                remote("orders", "warehouse", "raw_orders"),
                remote("ghost", "nowhere", "ghost"),
                TopologyNode::new("order_count", NodeKind::Asset),
                TopologyNode::new("seed", NodeKind::Asset),
                TopologyNode::new("clean", NodeKind::Asset),
                TopologyNode::new("report", NodeKind::Asset),
                TopologyNode::new("fresh", NodeKind::Asset),
                TopologyNode::new("never", NodeKind::Asset),
                TopologyNode::new("unrelated", NodeKind::Asset),
            ],
            edges: vec![
                edge("order_count", "orders"),
                edge("order_count", "ghost"),
                edge("clean", "seed"),
                edge("report", "clean"),
                edge("unrelated", "report"),
            ],
        };
        storage
            .set_graph_topology("analytics", &topology, &[])
            .await
            .unwrap();
        let ctx = CodeLocationContext::new("analytics");
        let scoped = storage.for_code_location(&ctx);
        let keys = [
            "report",
            "order_count",
            "fresh",
            "never",
            "orders",
            "no_record",
        ]
        .map(String::from);
        let only_keys = |all: HashMap<String, (StaleStatus, Vec<crate::storage::StaleCause>)>| {
            all.into_iter()
                .filter(|(key, _)| keys.contains(key))
                .collect::<HashMap<_, _>>()
        };

        let some = scoped.staleness_of(&keys, Some(&topology)).await.unwrap();

        let mut statuses: Vec<(&str, &StaleStatus)> = some
            .iter()
            .map(|(key, (status, _))| (key.as_str(), status))
            .collect();
        statuses.sort_by_key(|(key, _)| *key);
        assert_eq!(
            statuses,
            [
                ("fresh", &StaleStatus::UpToDate),
                ("never", &StaleStatus::Missing),
                ("order_count", &StaleStatus::Stale),
                ("report", &StaleStatus::Stale),
            ]
        );
        assert_eq!(some, only_keys(scoped.compute_staleness().await.unwrap()));

        let records = scoped.get_asset_records().await.unwrap();
        let without_topology = scoped.staleness_of(&keys, None).await.unwrap();
        assert_eq!(without_topology["report"].0, StaleStatus::UpToDate);
        assert_eq!(
            without_topology,
            only_keys(scoped.staleness_for(&records, None).await.unwrap())
        );
    }

    #[tokio::test]
    async fn unmaterialized_owner_asset_makes_the_consumer_stale() {
        let storage = setup().await;
        materialize(&storage, "analytics", "order_count", "b1", &[], 200).await;

        let (status, causes) = &staleness(&storage).await["order_count"];
        assert_eq!(status, &StaleStatus::Stale);
        assert_eq!(causes[0].reason, "upstream 'orders' is stale");
    }
}
