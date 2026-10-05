use super::*;

#[tokio::test]
async fn test_cache_detects_in_progress_completion_as_change() {
    // a → b, both materialized; a run re-materializes a. Tick 1 detects Started (a in-progress);
    // Tick 2 a completes → cache.refresh must return true so b's eval isn't skipped.

    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();

    // Register assets
    let rec_a = make_materialized_record("a", 1000);
    let rec_b = make_materialized_record("b", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_a, rec_b])
        .await
        .unwrap();

    // Store graph topology so cache knows a → b
    use crate::assets::graph::TopologyNode;
    let topo = GraphTopology {
        nodes: vec![
            TopologyNode {
                name: "a".into(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            },
            TopologyNode {
                name: "b".into(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            },
        ],
        edges: vec![("b".to_string(), "a".to_string())],
    };
    storage
        .kv_set(
            &crate::graph_topology_key(crate::storage::DEFAULT_CODE_LOCATION_ID),
            &serde_json::to_vec(&topo).unwrap(),
        )
        .await
        .unwrap();

    // Initial cache load
    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());
    let changed = cache.refresh(&storage, 0).await.unwrap();
    assert!(changed, "initial load should report changes");

    // Verify baseline: no in-progress assets
    assert!(cache.in_progress_assets.is_empty());

    // Create a Started run for a
    let run_id = "run-1".to_string();
    storage
        .create_run(&RunRecord {
            run_id: run_id.clone(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
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

    // Tick 1: detect the new run
    let changed = cache.refresh(&storage, 0).await.unwrap();
    assert!(
        changed,
        "tick 1: new Started run should be detected as change"
    );
    assert!(
        cache.in_progress_assets.contains_key("a"),
        "tick 1: a should be in-progress"
    );

    // Complete the run: update status and a's record with new timestamp
    storage
        .update_run_status(&run_id, RunStatus::Success, Some(3000))
        .await
        .unwrap();

    // Simulate a's materialization updating its record (the executor writes a
    // Materialization event that updates last_timestamp via recompute_staleness).
    storage
        .store_events(&[crate::storage::EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: crate::storage::EventType::Materialization {
                data_version: Some("new-dv".to_string()),
            },
            asset_key: Some("a".to_string()),
            run_id: run_id.clone(),
            partition_key: None,
            timestamp: 3000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();

    // Tick 2: detect a's completion
    let changed = cache.refresh(&storage, 0).await.unwrap();
    assert!(
        changed,
        "tick 2: a's run completing should be detected as change. \
         Without this, the eval loop skips and b never fires."
    );
    assert!(
        !cache.in_progress_assets.contains_key("a"),
        "tick 2: a should no longer be in-progress"
    );

    // Verify a's record was refreshed with new timestamp
    let a_record = cache.records.get("a").unwrap();
    assert!(
        a_record.last_timestamp.is_some(),
        "a should have updated timestamp in cache"
    );
}

#[tokio::test]
async fn test_cache_keeps_sibling_backfill_runs_in_progress_on_partial_completion() {
    // A backfill registers one run per partition on the same asset; when the first
    // completes, refresh must clear only that run — wiping the whole asset reopens
    // the dispatch gate for still-running siblings.

    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, EventRecord, EventType, PartitionKey, RunRecord, RunStatus,
        StorageBackend,
    };

    let storage = SurrealStorage::new_memory().await.unwrap();
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);

    // Downstream asset with a baseline materialization.
    storage
        .for_code_location(&ctx)
        .register_assets(&[make_materialized_record("dst", 1000)])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        cache.in_progress_assets.is_empty(),
        "baseline: nothing in flight"
    );

    // Backfill dispatch: one Started run per partition (a, b, c), all on `dst`.
    let part = |k: &str| PartitionKey::Single {
        keys: vec![k.to_string()],
    };
    let mk_run = |id: &str, k: &str| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("backfill".to_string()),
        status: RunStatus::Started,
        start_time: 2000,
        end_time: None,
        tags: vec![],
        node_names: vec!["dst".to_string()],
        priority: 0,
        partition_key: Some(part(k)),
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    storage
        .create_runs(&[
            mk_run("run_a", "a"),
            mk_run("run_b", "b"),
            mk_run("run_c", "c"),
        ])
        .await
        .unwrap();

    // Tick 1: all three observed Started → tracked under `dst`.
    cache.refresh(&storage, 0).await.unwrap();
    let tracked = cache
        .in_progress_assets
        .get("dst")
        .expect("dst should be in-progress after dispatch");
    assert_eq!(tracked.len(), 3, "all three backfill runs are tracked");

    // Partition a finishes: its run flips to Success and its materialization lands (advancing dst's timestamp).
    storage
        .update_run_status("run_a", RunStatus::Success, Some(3000))
        .await
        .unwrap();
    storage
        .store_events(&[EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization {
                data_version: Some("dv_a".to_string()),
            },
            asset_key: Some("dst".to_string()),
            run_id: "run_a".to_string(),
            partition_key: Some(part("a")),
            timestamp: 3000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();

    // Tick 2: a's completion detected; only run_a clears — b and c still running, so dst stays gated.
    cache.refresh(&storage, 0).await.unwrap();
    let tracked = cache
        .in_progress_assets
        .get("dst")
        .expect("dst must stay in-progress while runs b and c are still running");
    assert!(
        !tracked.contains_key("run_a"),
        "the completed run a should be cleared"
    );
    assert!(
        tracked.contains_key("run_b"),
        "still-running run b must stay tracked"
    );
    assert!(
        tracked.contains_key("run_c"),
        "still-running run c must stay tracked"
    );
}

#[tokio::test]
async fn test_observation_committing_after_load_is_still_seen() {
    // The observation cursor must derive from storage, not wall clock: an
    // observation stamped before daemon start whose write commits only after
    // the initial load must still trigger a refresh (re-processing is an
    // idempotent record re-fetch).
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, EventRecord, EventType, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .for_code_location(&ctx)
        .register_assets(&[make_record("ext")])
        .await
        .unwrap();

    let mk_obs = |run_id: &str, ts: i64| EventRecord {
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type: EventType::Observation {
            data_version: Some(format!("dv-{ts}")),
        },
        asset_key: Some("ext".to_string()),
        run_id: run_id.to_string(),
        partition_key: None,
        timestamp: ts,
        metadata: vec![],
        input_data_versions: vec![],
    };

    // Prior observation history, committed before the load.
    storage
        .store_events(&[mk_obs("obs-1", 1_000)])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 5_000).await.unwrap(); // wall clock well past the stamp

    // An equal-stamped observation carrying a NEW data version (batched
    // `now`, lagging write) commits only after the load.
    let mut late = mk_obs("obs-2", 1_000);
    late.event_type = EventType::Observation {
        data_version: Some("dv-late".to_string()),
    };
    storage.store_events(&[late]).await.unwrap();

    let changed = cache.refresh(&storage, 6_000).await.unwrap();
    assert!(
        changed,
        "an observation whose write landed after the initial load must still be seen"
    );
    assert_eq!(
        cache
            .records
            .get("ext")
            .and_then(|r| r.last_data_version.as_deref()),
        Some("dv-late"),
        "the late observation's data version must reach the cached record"
    );
}

#[tokio::test]
async fn test_steady_state_observation_cursor_trails_like_initial_load() {
    // V-12: the steady-state refresh observation cursor must trail the newest
    // stamp by 1 (like the run cursor and the initial-load cursor), or a
    // co-timestamped observation committing after a refresh is lost forever.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, EventRecord, EventType, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .for_code_location(&ctx)
        .register_assets(&[make_record("ext")])
        .await
        .unwrap();

    let mk_obs = |run_id: &str, ts: i64, dv: &str| EventRecord {
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type: EventType::Observation {
            data_version: Some(dv.to_string()),
        },
        asset_key: Some("ext".to_string()),
        run_id: run_id.to_string(),
        partition_key: None,
        timestamp: ts,
        metadata: vec![],
        input_data_versions: vec![],
    };

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    // Initial load establishes the cursor trailing 1 behind obs-1@1000.
    storage
        .store_events(&[mk_obs("obs-1", 1_000, "dv-1")])
        .await
        .unwrap();
    cache.refresh(&storage, 5_000).await.unwrap();

    // First late equal-stamped observation: this is the STEADY-STATE refresh
    // that must re-trail the cursor rather than jump to the exact max.
    storage
        .store_events(&[mk_obs("obs-2", 1_000, "dv-2")])
        .await
        .unwrap();
    assert!(cache.refresh(&storage, 6_000).await.unwrap());

    // A SECOND co-timestamped observation committing after that steady-state
    // refresh must still be seen — only possible if the cursor trailed by 1.
    storage
        .store_events(&[mk_obs("obs-3", 1_000, "dv-3")])
        .await
        .unwrap();
    let changed = cache.refresh(&storage, 7_000).await.unwrap();
    assert!(
        changed,
        "a co-timestamped observation after a steady-state refresh must still be seen"
    );
    assert_eq!(
        cache
            .records
            .get("ext")
            .and_then(|r| r.last_data_version.as_deref()),
        Some("dv-3"),
    );
}

#[tokio::test]
async fn test_incremental_partition_refresh_keeps_equal_timestamp_partitions() {
    // Materialization events can share one stamped `now`. A partition whose
    // row lands in a later refresh with a timestamp EQUAL to the cache's
    // current max must still be picked up — the incremental cursor has to
    // trail the max like the run cursor does, not query strictly past it.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, EventRecord, EventType, PartitionKey, RunRecord, RunStatus,
        StorageBackend,
    };

    let storage = SurrealStorage::new_memory().await.unwrap();
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .for_code_location(&ctx)
        .register_assets(&[make_materialized_record("dst", 1000)])
        .await
        .unwrap();

    let part = |k: &str| PartitionKey::Single {
        keys: vec![k.to_string()],
    };
    let mk_run = |id: &str, k: &str, start: i64| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("test".to_string()),
        status: RunStatus::Started,
        start_time: start,
        end_time: None,
        tags: vec![],
        node_names: vec!["dst".to_string()],
        priority: 0,
        partition_key: Some(part(k)),
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    let mk_event = |run_id: &str, k: &str, ts: i64| EventRecord {
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type: EventType::Materialization { data_version: None },
        asset_key: Some("dst".to_string()),
        run_id: run_id.to_string(),
        partition_key: Some(part(k)),
        timestamp: ts,
        metadata: vec![],
        input_data_versions: vec![],
    };

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_partitioned_assets(vec!["dst".to_string()]);
    cache.refresh(&storage, 0).await.unwrap();

    // Partition a: run + materialization stamped 3000, observed by one refresh.
    storage
        .create_run(&mk_run("run_a", "a", 2000))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    storage
        .update_run_status("run_a", RunStatus::Success, Some(3000))
        .await
        .unwrap();
    storage
        .store_events(&[mk_event("run_a", "a", 3000)])
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    assert_eq!(
        cache.partition_status["dst"].timestamps.get(&part("a")),
        Some(&3000),
        "precondition: partition a's timestamp is cached"
    );

    // Partition b: a separate run whose materialization carries the SAME
    // stamped timestamp, landing in a later refresh.
    storage
        .create_run(&mk_run("run_b", "b", 4000))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    storage
        .update_run_status("run_b", RunStatus::Success, Some(5000))
        .await
        .unwrap();
    storage
        .store_events(&[mk_event("run_b", "b", 3000)])
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    assert_eq!(
        cache.partition_status["dst"].timestamps.get(&part("b")),
        Some(&3000),
        "a partition update equal to the cached max timestamp must not be dropped"
    );
}

#[test]
fn test_clear_predispatch_mark_drops_empty_entry_only() {
    // A multi-partition backfill pre-marks an empty in_progress entry (classify) with no
    // phantom-eviction net; on dispatch failure it must be cleared, but never when real runs exist.
    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());

    // Pre-mark (empty placeholder, as classify does) → cleared.
    cache
        .in_progress_assets
        .entry("dst".to_string())
        .or_default();
    assert!(cache.in_progress_assets.contains_key("dst"));
    cache.clear_predispatch_mark("dst");
    assert!(
        !cache.in_progress_assets.contains_key("dst"),
        "empty pre-dispatch placeholder must be cleared"
    );

    // A real run was registered → clear is a no-op (must not wipe live runs).
    cache.register_dispatched_run("dst".to_string(), "run-1".to_string(), 0, None);
    cache.clear_predispatch_mark("dst");
    assert!(
        cache
            .in_progress_assets
            .get("dst")
            .is_some_and(|r| r.contains_key("run-1")),
        "an entry with a real run must NOT be cleared"
    );
}

#[tokio::test]
async fn test_cache_completion_fallback_skips_still_started_sibling_effects() {
    // In the ts-unchanged completion fallback the effects loop must skip still-Started
    // siblings — applying a still-running run's effects would record its incomplete
    // tags as that partition's last-run tags prematurely.

    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, EventRecord, EventType, PartitionKey, RunRecord, RunStatus,
        StorageBackend,
    };

    let storage = SurrealStorage::new_memory().await.unwrap();
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .for_code_location(&ctx)
        .register_assets(&[make_materialized_record("dst", 1000)])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_partitioned_assets(vec!["dst".to_string()]);
    cache.refresh(&storage, 0).await.unwrap();

    let part = |k: &str| PartitionKey::Single {
        keys: vec![k.to_string()],
    };
    let mk_run = |id: &str, k: &str| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("backfill".to_string()),
        status: RunStatus::Started,
        start_time: 2000,
        end_time: None,
        tags: vec![("batch".to_string(), k.to_string())],
        node_names: vec!["dst".to_string()],
        priority: 0,
        partition_key: Some(part(k)),
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    storage
        .create_runs(&[mk_run("run_a", "a"), mk_run("run_b", "b")])
        .await
        .unwrap();

    // Tick 1: both observed Started, tracked, no tags recorded yet.
    cache.refresh(&storage, 0).await.unwrap();

    // Partition a finishes (StepSuccess) but dst's timestamp is unchanged (idempotent)
    // → forces the ts-unchanged fallback; run_b stays Started.
    storage
        .update_run_status("run_a", RunStatus::Success, Some(3000))
        .await
        .unwrap();
    storage
        .store_events(&[EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepSuccess,
            asset_key: Some("dst".to_string()),
            run_id: "run_a".to_string(),
            partition_key: Some(part("a")),
            timestamp: 3000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();

    // Tick 2: fallback fires; run_a's tags recorded for partition a, but run_b still Started → its tags must not be recorded.
    cache.refresh(&storage, 0).await.unwrap();

    let tracked = cache.in_progress_assets.get("dst").unwrap();
    assert!(!tracked.contains_key("run_a"), "completed run a is cleared");
    assert!(tracked.contains_key("run_b"), "still-running run b stays");

    let dst_tags = cache.last_run_tags.get("dst");
    assert!(
        dst_tags.is_some_and(|m| m.contains_key(&Some(part("a")))),
        "partition a (completed) should have recorded tags"
    );
    assert!(
        dst_tags.is_none_or(|m| !m.contains_key(&Some(part("b")))),
        "partition b is still running — its tags must NOT be recorded yet, got: {:?}",
        dst_tags.and_then(|m| m.get(&Some(part("b")))),
    );
}

#[tokio::test]
async fn test_cache_clears_in_progress_when_run_succeeds_but_timestamp_unchanged() {
    // a → b materialized at 1000; a schedule re-materializes a with identical output
    // (last_timestamp unchanged). The in-progress check must also detect the
    // Started→Success status change and clear a, or b never fires.

    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();

    // Register assets with same timestamp
    let rec_a = make_materialized_record("a", 1000);
    let rec_b = make_materialized_record("b", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_a, rec_b])
        .await
        .unwrap();

    // Store graph topology
    use crate::assets::graph::TopologyNode;
    let topo = GraphTopology {
        nodes: vec![
            TopologyNode {
                name: "a".into(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            },
            TopologyNode {
                name: "b".into(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            },
        ],
        edges: vec![("b".to_string(), "a".to_string())],
    };
    storage
        .kv_set(
            &crate::graph_topology_key(crate::storage::DEFAULT_CODE_LOCATION_ID),
            &serde_json::to_vec(&topo).unwrap(),
        )
        .await
        .unwrap();

    // Initial cache load
    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();
    assert!(cache.in_progress_assets.is_empty());

    // Create a Started run for a
    let run_id = "run-idem".to_string();
    storage
        .create_run(&RunRecord {
            run_id: run_id.clone(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
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

    // Tick: detect the new run → a is in-progress
    let changed = cache.refresh(&storage, 0).await.unwrap();
    assert!(changed);
    assert!(cache.in_progress_assets.contains_key("a"));

    // Run completes Success but a's timestamp is not updated (idempotent);
    // the executor writes a StepSuccess event for a.
    storage
        .update_run_status(&run_id, RunStatus::Success, Some(3000))
        .await
        .unwrap();
    storage
        .store_events(&[crate::storage::EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: crate::storage::EventType::StepSuccess,
            asset_key: Some("a".to_string()),
            run_id: run_id.clone(),
            partition_key: None,
            timestamp: 3000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();
    // Deliberately not updating a's last_timestamp (identical output).

    // Next tick: cache detects a's StepSuccess and removes it from in_progress, even though last_timestamp didn't change.
    let changed = cache.refresh(&storage, 0).await.unwrap();
    assert!(
        !cache.in_progress_assets.contains_key("a"),
        "a should be removed from in_progress_assets when its StepSuccess event exists, \
         even if last_timestamp didn't change. Got in_progress={:?}",
        cache.in_progress_assets,
    );
    assert!(
        changed,
        "cache should report changes when an in-progress asset's step completes"
    );
}

#[tokio::test]
async fn test_step_success_clears_floor_for_lagging_record_in_joint_failed_run() {
    // A joint run R=[x,y] fails on y but x materialized (StepSuccess); with x's record
    // write lagging, the step-completion fallback must treat x as materialized-here
    // (no floor) while y (StepFailure) is floored.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, EventRecord, EventType, RunRecord, RunStatus, StorageBackend,
    };

    let storage = SurrealStorage::new_memory().await.unwrap();
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[
            make_materialized_record("x", 1000),
            make_materialized_record("y", 1000),
        ])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();

    let run_id = "run-joint".to_string();
    storage
        .create_run(&RunRecord {
            run_id: run_id.clone(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec!["x".to_string(), "y".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    assert!(cache.in_progress_assets.contains_key("x"));
    assert!(cache.in_progress_assets.contains_key("y"));

    // Run fails: x StepSuccess, y StepFailure; neither record updated this tick (write lag, ts stays 1000).
    storage
        .update_run_status(&run_id, RunStatus::Failure, Some(3000))
        .await
        .unwrap();
    storage
        .store_events(&[
            EventRecord {
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepSuccess,
                asset_key: Some("x".to_string()),
                run_id: run_id.clone(),
                partition_key: None,
                timestamp: 3000,
                metadata: vec![],
                input_data_versions: vec![],
            },
            EventRecord {
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepFailure,
                asset_key: Some("y".to_string()),
                run_id: run_id.clone(),
                partition_key: None,
                timestamp: 3000,
                metadata: vec![],
                input_data_versions: vec![],
            },
        ])
        .await
        .unwrap();

    cache.refresh(&storage, 0).await.unwrap();

    assert!(
        !cache.failed_assets.contains("x"),
        "x materialized (StepSuccess) in the failed joint run → must not be floored \
         despite the lagging record; got failed_assets={:?}",
        cache.failed_assets,
    );
    assert!(
        cache.failed_assets.contains("y"),
        "y failed (StepFailure) → must be floored"
    );
    assert_eq!(cache.failed_asset_timestamps.get("y"), Some(&3000));
}

#[tokio::test]
async fn test_failed_joint_run_step_success_records_tick_tags() {
    // V-13: a step that succeeded inside an overall-Failure joint run still
    // materialized its asset, so HasRunWithTags/AllRunsHaveTags must see the
    // run's tags for that asset on the tick it materialized. y (StepFailure)
    // must contribute nothing.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, EventRecord, EventType, RunRecord, RunStatus, StorageBackend,
    };

    let storage = SurrealStorage::new_memory().await.unwrap();
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[
            make_materialized_record("x", 1000),
            make_materialized_record("y", 1000),
        ])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_needs_tick_tags(&[ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("team".to_string(), "x".to_string())],
    }]);
    cache.refresh(&storage, 0).await.unwrap();

    let run_id = "run-joint".to_string();
    storage
        .create_run(&RunRecord {
            run_id: run_id.clone(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![("team".to_string(), "x".to_string())],
            node_names: vec!["x".to_string(), "y".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    // Run fails overall: x StepSuccess (materialized), y StepFailure; records lag.
    storage
        .update_run_status(&run_id, RunStatus::Failure, Some(3000))
        .await
        .unwrap();
    storage
        .store_events(&[
            EventRecord {
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepSuccess,
                asset_key: Some("x".to_string()),
                run_id: run_id.clone(),
                partition_key: None,
                timestamp: 3000,
                metadata: vec![],
                input_data_versions: vec![],
            },
            EventRecord {
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepFailure,
                asset_key: Some("y".to_string()),
                run_id: run_id.clone(),
                partition_key: None,
                timestamp: 3000,
                metadata: vec![],
                input_data_versions: vec![],
            },
        ])
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    assert!(
        cache
            .tick_materialization_tags
            .get("x")
            .and_then(|slots| slots.get(&None))
            .is_some_and(|v| v
                .iter()
                .any(|t| t.contains(&("team".to_string(), "x".to_string())))),
        "x materialized (StepSuccess) in the failed joint run → its tags must be \
         recorded for tick-tag conditions; got {:?}",
        cache.tick_materialization_tags
    );
    assert!(
        !cache.tick_materialization_tags.contains_key("y"),
        "y failed (StepFailure) → must not contribute tick tags"
    );
}

#[tokio::test]
async fn test_cache_clears_in_progress_when_run_canceled_after_cursor_advanced() {
    // A run reported once advances the cursor past its start_time; a later CANCEL changes
    // neither the record nor start_time, so get_runs_since (`>`) never re-delivers it.
    // The cache must re-check tracked in-progress runs by id so a missed terminal transition still clears them.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();

    let rec_a = make_materialized_record("a", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_a])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();
    assert!(cache.in_progress_assets.is_empty());

    // Started run for a → observed, in_progress set, cursor advances to 2000.
    let run_id = "run-cancel".to_string();
    storage
        .create_run(&RunRecord {
            run_id: run_id.clone(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
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
    let changed = cache.refresh(&storage, 0).await.unwrap();
    assert!(changed);
    assert!(cache.in_progress_assets.contains_key("a"));

    // Run canceled; start_time stays 2000 → the cursor `> 2000` never re-delivers it.
    storage
        .update_run_status(&run_id, RunStatus::Canceled, Some(3000))
        .await
        .unwrap();

    // Next refresh must clear a from in_progress despite the cursor miss.
    let changed = cache.refresh(&storage, 0).await.unwrap();
    assert!(
        !cache.in_progress_assets.contains_key("a"),
        "a should be cleared from in_progress when its run is canceled, even though \
         the run cursor never re-delivers the cancel. Got in_progress={:?}",
        cache.in_progress_assets,
    );
    // The sweep mutated eval-visible state, so refresh must report a change, or
    // should_skip suppresses evaluation and the un-wedged asset never re-fires.
    assert!(
        changed,
        "a sweep-only terminal transition must report the refresh as changed"
    );
}

#[tokio::test]
async fn test_queued_run_from_scheduler_is_tracked_and_applies_effects() {
    // A run first observed while Queued (schedule/sensor dispatch — never
    // registered via register_dispatched_run) must be tracked as in-flight and
    // its completion effects applied, even though the cursor advances past its
    // immutable start_time on first sight.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let rec_a = make_materialized_record("a", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_a])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();

    let run_id = "run-queued".to_string();
    storage
        .create_run(&RunRecord {
            run_id: run_id.clone(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Queued,
            start_time: 2000,
            end_time: None,
            tags: vec![("team".to_string(), "x".to_string())],
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

    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        cache.in_progress_assets.contains_key("a"),
        "a queued run must suppress in_flight-gated conditions; got {:?}",
        cache.in_progress_assets
    );

    // The run completes; start_time never changes, so only the tracked-run
    // sweep can observe the transition. The materialization event credits the
    // record with the run.
    storage
        .update_run_status(&run_id, RunStatus::Success, Some(3000))
        .await
        .unwrap();
    storage
        .store_event(&crate::storage::EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: crate::storage::EventType::Materialization {
                data_version: Some("dvq".to_string()),
            },
            asset_key: Some("a".to_string()),
            run_id: "run-queued".to_string(),
            partition_key: None,
            timestamp: 3000,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        !cache.in_progress_assets.contains_key("a"),
        "completion must clear in-flight tracking; got {:?}",
        cache.in_progress_assets
    );
    assert!(
        cache
            .last_run_tags
            .get("a")
            .and_then(|slots| slots.get(&None))
            .is_some_and(|t| t.contains(&("team".to_string(), "x".to_string()))),
        "completion effects (run tags) must be applied; got {:?}",
        cache.last_run_tags
    );
}

#[tokio::test]
async fn test_initial_load_tracks_queued_and_not_started_runs() {
    // Runs alive as Queued/NotStarted at daemon restart must be reloaded into
    // in-flight tracking; loading only Started runs re-dispatches their assets.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let recs = [
        make_materialized_record("a", 1000),
        make_materialized_record("b", 1000),
        make_materialized_record("c", 1000),
    ];
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&recs)
        .await
        .unwrap();

    let mk_run = |id: &str, status: RunStatus, start: i64, asset: &str| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("test".to_string()),
        status,
        start_time: start,
        end_time: None,
        tags: vec![],
        node_names: vec![asset.to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    // run-s is the newest, so the seeded cursor sits above run-q / run-n.
    storage
        .create_run(&mk_run("run-q", RunStatus::Queued, 2000, "a"))
        .await
        .unwrap();
    storage
        .create_run(&mk_run("run-n", RunStatus::NotStarted, 2100, "b"))
        .await
        .unwrap();
    storage
        .create_run(&mk_run("run-s", RunStatus::Started, 3000, "c"))
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();
    for asset in ["a", "b", "c"] {
        assert!(
            cache.in_progress_assets.contains_key(asset),
            "{asset} has a live run at restart and must be tracked in-flight; got {:?}",
            cache.in_progress_assets
        );
    }

    // The queued run's completion must be observed via the tracked-run sweep.
    storage
        .update_run_status("run-q", RunStatus::Success, Some(4000))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        !cache.in_progress_assets.contains_key("a"),
        "queued run's completion must clear tracking; got {:?}",
        cache.in_progress_assets
    );
    assert!(cache.in_progress_assets.contains_key("b"));
    assert!(cache.in_progress_assets.contains_key("c"));
}

#[tokio::test]
async fn test_foreign_code_location_observations_do_not_clear_in_flight() {
    // Code locations can share one SurrealDB; another location observing a
    // SAME-NAMED asset must not wipe this location's in-flight run tracking.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{EventRecord, EventType, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let rec_x = make_materialized_record("x", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new("cl-a"))
        .register_assets(&[rec_x])
        .await
        .unwrap();
    storage
        .create_run(&RunRecord {
            run_id: "run-x".to_string(),
            code_location_id: "cl-a".to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec!["x".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new("cl-a".to_string());
    cache.refresh(&storage, 3_000).await.unwrap();
    assert!(cache.in_progress_assets.contains_key("x"));

    // The OTHER code location observes its own asset named "x".
    storage
        .store_event(&EventRecord {
            code_location_id: "cl-b".to_string(),
            event_type: EventType::Observation {
                data_version: Some("v1".to_string()),
            },
            asset_key: Some("x".to_string()),
            run_id: String::new(),
            partition_key: None,
            timestamp: 4_000,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();
    cache.refresh(&storage, 5_000).await.unwrap();

    assert!(
        cache.in_progress_assets.contains_key("x"),
        "a foreign location's observation must not clear this location's tracking; got {:?}",
        cache.in_progress_assets
    );
}

#[tokio::test]
async fn test_backfill_terminal_clears_predispatch_placeholder() {
    // A backfill-shaped dispatch inserts an empty in-flight placeholder; when
    // the backfill ends without any observed sub-run (e.g. canceled before its
    // first wave) the asset must not stay in-flight forever.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        BackfillFailurePolicy, BackfillRecord, BackfillStatus, BackfillStrategy,
        DEFAULT_CODE_LOCATION_ID, StorageBackend,
    };

    let storage = SurrealStorage::new_memory().await.unwrap();
    let rec_p = make_materialized_record("P", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_p])
        .await
        .unwrap();
    storage
        .create_backfill(&BackfillRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            backfill_id: "bf1".to_string(),
            status: BackfillStatus::Requested,
            strategy: BackfillStrategy::MultiRun,
            failure_policy: BackfillFailurePolicy::Continue,
            asset_selection: vec!["P".to_string()],
            job_name: None,
            partition_keys: vec![spk("k1"), spk("k2")],
            run_ids: vec![],
            completed_partitions: vec![],
            failed_partitions: vec![],
            canceled_partitions: vec![],
            max_concurrency: 1,
            tags: vec![],
            create_time: 1000,
            end_time: None,
            error: None,
            launched_by: LaunchedBy::default(),
            action: None,
            config: None,
        })
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();
    assert!(cache.backfill.assets.contains_key("P"), "precondition");

    // The dispatch path's pre-dispatch placeholder.
    cache.in_progress_assets.entry("P".to_string()).or_default();

    // Canceled before any sub-run was ever observed.
    storage
        .update_backfill_status("bf1", BackfillStatus::Canceled, Some(2000))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    assert!(
        !cache.backfill.assets.contains_key("P"),
        "terminal backfill untracked"
    );
    assert!(
        !cache.in_progress_assets.contains_key("P"),
        "the empty placeholder must be cleared when the backfill ends; got {:?}",
        cache.in_progress_assets
    );
}

#[tokio::test]
async fn test_joint_partitioned_run_updates_unpartitioned_assets_scalar_tags() {
    // A partition-keyed joint run spanning a partitioned and an unpartitioned
    // asset must write the unpartitioned asset's tags into the SCALAR maps the
    // unpartitioned eval path reads — not only into the partition maps.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let mut rec_p = make_materialized_record("P", 1000);
    rec_p.last_run_id = Some("run-joint".to_string());
    let mut rec_d = make_materialized_record("D", 1000);
    rec_d.last_run_id = Some("run-joint".to_string());
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_p, rec_d])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_partitioned_assets(vec!["P".to_string()]);
    cache.refresh(&storage, 0).await.unwrap();

    storage
        .create_run(&RunRecord {
            run_id: "run-joint".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![("team".to_string(), "x".to_string())],
            node_names: vec!["P".to_string(), "D".to_string()],
            priority: 0,
            partition_key: Some(spk("2024-01-01")),
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    storage
        .update_run_status("run-joint", RunStatus::Success, Some(3000))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    assert!(
        cache
            .last_run_tags
            .get("P")
            .and_then(|m| m.get(&Some(spk("2024-01-01"))))
            .is_some_and(|t| t.contains(&("team".to_string(), "x".to_string()))),
        "partitioned asset keeps per-partition tags; got {:?}",
        cache.last_run_tags
    );
    assert!(
        cache
            .last_run_tags
            .get("D")
            .and_then(|slots| slots.get(&None))
            .is_some_and(|t| t.contains(&("team".to_string(), "x".to_string()))),
        "unpartitioned asset must get scalar tags; got {:?}",
        cache.last_run_tags
    );
}

#[tokio::test]
async fn test_two_partition_runs_same_asset_both_update_slots() {
    // V-14: when two runs of the SAME partitioned asset (different partitions)
    // complete within one refresh, both partition slots must get their tags —
    // not just the newest run the scalar record.last_run_id credits.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let mut rec_p = make_materialized_record("P", 1000);
    rec_p.last_run_id = Some("R2".to_string()); // scalar credits only the newest
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_p])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_partitioned_assets(vec!["P".to_string()]);
    cache.refresh(&storage, 0).await.unwrap();

    let mk = |id: &str, pk: &str, who: &str, start: i64| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("test".to_string()),
        status: RunStatus::Started,
        start_time: start,
        end_time: None,
        tags: vec![("who".to_string(), who.to_string())],
        node_names: vec!["P".to_string()],
        priority: 0,
        partition_key: Some(spk(pk)),
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };
    storage
        .create_run(&mk("R1", "p1", "a", 2000))
        .await
        .unwrap();
    storage
        .create_run(&mk("R2", "p2", "b", 2001))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    // Both complete; applied in one refresh.
    storage
        .update_run_status("R1", RunStatus::Success, Some(3000))
        .await
        .unwrap();
    storage
        .update_run_status("R2", RunStatus::Success, Some(3001))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    assert!(
        cache
            .last_run_tags
            .get("P")
            .and_then(|m| m.get(&Some(spk("p2"))))
            .is_some_and(|t| t.contains(&("who".to_string(), "b".to_string()))),
        "newest run's slot present; got {:?}",
        cache.last_run_tags
    );
    assert!(
        cache
            .last_run_tags
            .get("P")
            .and_then(|m| m.get(&Some(spk("p1"))))
            .is_some_and(|t| t.contains(&("who".to_string(), "a".to_string()))),
        "older same-asset run's partition slot must NOT be dropped; got {:?}",
        cache.last_run_tags
    );
}

#[tokio::test]
async fn test_in_progress_partition_keys_expands_batched_members() {
    // V-15: a run over a batched multi-member key (single_run backfill bundle_keys
    // or a manual multi-key materialize) must mark each MEMBER partition in
    // progress, not the composite key that select_in_universe would drop.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, PartitionKey, RunRecord, RunStatus, StorageBackend,
    };

    let storage = SurrealStorage::new_memory().await.unwrap();
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[make_materialized_record("a", 1000)])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_partitioned_assets(vec!["a".to_string()]);
    cache.refresh(&storage, 0).await.unwrap();

    // A single Started run bundling two partitions {p1, p2}.
    storage
        .create_run(&RunRecord {
            run_id: "run-bundle".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: Some(PartitionKey::Single {
                keys: vec!["p1".to_string(), "p2".to_string()],
            }),
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    let ip = cache.in_progress_partition_keys("a");
    assert!(
        ip.contains(&spk("p1")) && ip.contains(&spk("p2")),
        "each member of a batched in-progress key must be marked in progress; got {:?}",
        ip
    );
}

#[tokio::test]
async fn test_failed_run_does_not_clobber_latest_materializing_tags() {
    // LastExecutedWithTags reflects the latest run that MATERIALIZED the
    // asset; a later run that failed without materializing it must not
    // overwrite the tags (and must match what a restart rebuilds).
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let mut rec_a = make_materialized_record("A", 1000);
    rec_a.last_run_id = Some("r1".to_string());
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_a])
        .await
        .unwrap();

    let mk_run = |id: &str, start: i64, tags: Vec<(String, String)>| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("test".to_string()),
        status: RunStatus::Started,
        start_time: start,
        end_time: None,
        tags,
        node_names: vec!["A".to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();

    // r1 materializes A with env=prod.
    storage
        .create_run(&mk_run("r1", 2000, vec![("env".into(), "prod".into())]))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    storage
        .update_run_status("r1", RunStatus::Success, Some(2100))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        cache
            .last_run_tags
            .get("A")
            .and_then(|slots| slots.get(&None))
            .is_some_and(|t| t.contains(&("env".to_string(), "prod".to_string()))),
        "precondition: r1's tags recorded"
    );

    // r2 covers A but FAILS without materializing it (record.last_run_id stays r1).
    storage
        .create_run(&mk_run("r2", 3000, vec![("env".into(), "dev".into())]))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    storage
        .update_run_status("r2", RunStatus::Failure, Some(3100))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    assert!(
        cache
            .last_run_tags
            .get("A")
            .and_then(|slots| slots.get(&None))
            .is_some_and(|t| t.contains(&("env".to_string(), "prod".to_string()))),
        "a failed non-materializing run must not clobber the tags; got {:?}",
        cache.last_run_tags
    );
}

#[tokio::test]
async fn test_later_finishing_run_keeps_latest_tags() {
    // Overlapping runs: once the later-finishing materializing run's tags are
    // recorded, an earlier-finishing run applied afterwards must not win.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let mut rec_x = make_materialized_record("X", 1000);
    rec_x.last_run_id = Some("run-a".to_string());
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_x])
        .await
        .unwrap();

    let mk_run = |id: &str, start: i64, tags: Vec<(String, String)>| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("test".to_string()),
        status: RunStatus::Started,
        start_time: start,
        end_time: None,
        tags,
        node_names: vec!["X".to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();

    storage
        .create_run(&mk_run("run-a", 100, vec![("who".into(), "a".into())]))
        .await
        .unwrap();
    storage
        .create_run(&mk_run("run-b", 200, vec![("who".into(), "b".into())]))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    // run-a (the run the record credits with the materialization) finishes
    // later (305) and is applied first; run-b (300) applied in a later refresh
    // must not overwrite.
    storage
        .update_run_status("run-a", RunStatus::Success, Some(305))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    storage
        .update_run_status("run-b", RunStatus::Success, Some(300))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    assert!(
        cache
            .last_run_tags
            .get("X")
            .and_then(|slots| slots.get(&None))
            .is_some_and(|t| t.contains(&("who".to_string(), "a".to_string()))),
        "the later-finishing materializing run's tags must win; got {:?}",
        cache.last_run_tags
    );
}

/// Helper: memory-backed storage with `a` and `b` registered at `ts`, `b` depending on `a`.
async fn race_test_setup(ts: i64) -> crate::storage::surrealdb_backend::SurrealStorage {
    use crate::storage::surrealdb_backend::SurrealStorage;
    let storage = SurrealStorage::new_memory().await.unwrap();
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[
            make_materialized_record("a", ts),
            make_materialized_record("b", ts),
        ])
        .await
        .unwrap();
    storage
        .kv_set(
            &crate::graph_topology_key(DEFAULT_CODE_LOCATION_ID),
            &serde_json::to_vec(&GraphTopology {
                nodes: vec![
                    crate::assets::graph::TopologyNode {
                        name: "a".into(),
                        kind: crate::assets::graph::NodeKind::Asset,
                        group: None,
                        parent_graph: None,
                    },
                    crate::assets::graph::TopologyNode {
                        name: "b".into(),
                        kind: crate::assets::graph::NodeKind::Asset,
                        group: None,
                        parent_graph: None,
                    },
                ],
                edges: vec![("b".to_string(), "a".to_string())],
            })
            .unwrap(),
        )
        .await
        .unwrap();
    storage
}

fn run_record(
    run_id: &str,
    status: RunStatus,
    start_time: i64,
    node_names: Vec<&str>,
) -> RunRecord {
    RunRecord {
        run_id: run_id.into(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.into(),
        job_name: Some("test".into()),
        status,
        start_time,
        end_time: None,
        tags: vec![],
        node_names: node_names.into_iter().map(String::from).collect(),
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    }
}

/// initial_load backs the cursor off 1ns so the next `get_runs_since(>cursor)` re-includes
/// the run; a Started run must not pile up duplicate in-progress run_ids across both queries.
#[tokio::test]
async fn test_cursor_backoff_doesnt_duplicate_in_progress_entries() {
    let storage = race_test_setup(1000).await;
    let started = run_record("r1", RunStatus::Started, 2000, vec!["a"]);
    storage.create_run(&started).await.unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.into());
    cache.refresh(&storage, 5000).await.unwrap();

    let entries = cache
        .in_progress_assets
        .get("a")
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        entries.keys().filter(|id| id.as_str() == "r1").count(),
        1,
        "run_id should appear exactly once in in_progress_assets despite \
         both initial_load and the cursor-rewound refresh observing the run"
    );
}

/// With two runs for one asset in a single delta, `apply_run_effects_to_delta` does
/// last-write-wins on `last_run_asset_names`; ASC iteration lands the newest run's state
/// last, as `LastRunIncludesTarget` needs.
#[tokio::test]
async fn test_asc_iteration_makes_newest_run_win_per_asset_state() {
    use crate::storage::surrealdb_backend::SurrealStorage;
    let storage = SurrealStorage::new_memory().await.unwrap();

    // Older "manual bulk" run materializes [a,b]; stamp it on both records so initial_load picks up its state.
    storage
        .create_run(&run_record("old", RunStatus::Success, 2000, vec!["a", "b"]))
        .await
        .unwrap();
    let mut rec_a = make_materialized_record("a", 1000);
    rec_a.last_run_id = Some("old".into());
    let mut rec_b = make_materialized_record("b", 1000);
    rec_b.last_run_id = Some("old".into());
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_a, rec_b])
        .await
        .unwrap();
    storage
        .kv_set(
            &crate::graph_topology_key(DEFAULT_CODE_LOCATION_ID),
            &serde_json::to_vec(&GraphTopology {
                nodes: vec![
                    crate::assets::graph::TopologyNode {
                        name: "a".into(),
                        kind: crate::assets::graph::NodeKind::Asset,
                        group: None,
                        parent_graph: None,
                    },
                    crate::assets::graph::TopologyNode {
                        name: "b".into(),
                        kind: crate::assets::graph::NodeKind::Asset,
                        group: None,
                        parent_graph: None,
                    },
                ],
                edges: vec![("b".to_string(), "a".to_string())],
            })
            .unwrap(),
        )
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.into());
    cache.refresh(&storage, 0).await.unwrap();
    assert_eq!(
        cache
            .last_run_asset_names
            .get("a")
            .and_then(|slots| slots.get(&None))
            .map(|n| n.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default(),
        vec!["a".to_string(), "b".to_string()],
        "sanity: initial_load reflects the manual bulk run for asset 'a'"
    );

    // After init, a newer schedule run for 'a' lands; the cursor backoff makes both visible to the next refresh.
    storage
        .create_run(&run_record("new", RunStatus::Success, 3000, vec!["a"]))
        .await
        .unwrap();
    storage
        .store_event(&crate::storage::EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: crate::storage::EventType::Materialization {
                data_version: Some("dv-new".to_string()),
            },
            asset_key: Some("a".to_string()),
            run_id: "new".to_string(),
            partition_key: None,
            timestamp: 3000,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();
    cache.refresh(&storage, 5000).await.unwrap();

    let names_for_a = cache
        .last_run_asset_names
        .get("a")
        .and_then(|slots| slots.get(&None))
        .map(|n| n.iter().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    assert_eq!(
        names_for_a,
        vec!["a".to_string()],
        "after both runs are processed in ASC order, the newer schedule \
         run must win the per-asset overwrite; if this fails, \
         LastRunIncludesTarget wrongly reports 'b' was included in a's \
         last run and eager() never fires"
    );
}

/// An older Success + newer Started run in the same refresh: ASC iteration (old first)
/// leaves the asset in-progress — Success clears it, then Started re-adds it.
#[tokio::test]
async fn test_mixed_status_order_started_after_success_lands_in_progress() {
    let storage = race_test_setup(1000).await;
    storage
        .create_run(&run_record(
            "old-success",
            RunStatus::Success,
            2000,
            vec!["a"],
        ))
        .await
        .unwrap();
    storage
        .create_run(&run_record(
            "new-started",
            RunStatus::Started,
            3000,
            vec!["a"],
        ))
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.into());
    cache.refresh(&storage, 0).await.unwrap();

    let ids = cache
        .in_progress_assets
        .get("a")
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        ids.keys().cloned().collect::<Vec<_>>(),
        vec!["new-started".to_string()],
        "newer Started run must leave 'a' in-progress after older Success \
         run cleared it; if iteration order flipped, 'a' would incorrectly \
         be reported as not-in-progress"
    );
}

/// A run racing daemon init: `initial_load` sees it as the newest, so without the cursor
/// backoff `get_runs_since(>newest)` excludes it forever; the backoff re-includes it so its terminal state is picked up.
#[tokio::test]
async fn test_cursor_backoff_lets_init_racing_run_be_observed_terminal() {
    let storage = race_test_setup(1000).await;
    storage
        .create_run(&run_record("racing", RunStatus::Started, 2000, vec!["a"]))
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.into());
    cache.refresh(&storage, 5000).await.unwrap();
    assert!(
        cache.in_progress_assets.contains_key("a"),
        "after initial load, the racing Started run leaves 'a' in-progress"
    );

    // Run completes — flip to Success and update the asset record.
    storage
        .update_run_status("racing", RunStatus::Success, Some(6000))
        .await
        .unwrap();
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[make_materialized_record("a", 6000)])
        .await
        .unwrap();

    let changed = cache.refresh(&storage, 7000).await.unwrap();
    assert!(
        changed,
        "completion of the init-racing run must surface as a delta change"
    );
    assert!(
        !cache.in_progress_assets.contains_key("a"),
        "'a' should be cleared from in-progress after its terminal status \
         is observed; without the cursor backoff + ASC ordering this fails"
    );
}

#[tokio::test]
async fn test_completed_run_invalidates_event_less_partitioned_sibling() {
    // Joint keyed run R=[x,y] over p: x materializes p (R enters completed_run_ids), y dies
    // event-less. The completed_run_ids path handles R and must invalidate y so its partition
    // failure (run-status union) reaches partition_status[y].
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, EventRecord, EventType, RunRecord, RunStatus, StorageBackend,
    };

    let storage = SurrealStorage::new_memory().await.unwrap();
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[
            make_materialized_record("x", 1000),
            make_materialized_record("y", 1000),
        ])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_partitioned_assets(vec!["x".to_string(), "y".to_string()]);
    cache.refresh(&storage, 0).await.unwrap();

    let run_id = "run-joint-part".to_string();
    storage
        .create_run(&RunRecord {
            run_id: run_id.clone(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec!["x".to_string(), "y".to_string()],
            priority: 0,
            partition_key: Some(spk("p")),
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: None,
            config: None,
        })
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    assert!(cache.in_progress_assets.contains_key("x"));
    assert!(cache.in_progress_assets.contains_key("y"));

    // R fails: x materialized p (R enters completed_run_ids), y is event-less.
    storage
        .update_run_status(&run_id, RunStatus::Failure, Some(3000))
        .await
        .unwrap();
    storage
        .store_events(&[EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Materialization {
                data_version: Some("dv-xp".to_string()),
            },
            asset_key: Some("x".to_string()),
            run_id: run_id.clone(),
            partition_key: Some(spk("p")),
            timestamp: 3000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();

    cache.refresh(&storage, 0).await.unwrap();

    let y_status = cache
        .partition_status
        .get("y")
        .expect("y is a registered partitioned asset");
    assert!(
        y_status.failed.contains(&spk("p")),
        "y's event-less partition failure in the joint run must surface in \
         partition_status via completed-path invalidation; got failed={:?}",
        y_status.failed,
    );
}
