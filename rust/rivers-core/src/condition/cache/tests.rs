use super::*;
use crate::storage::RunRecord;

fn mk_run(run_id: &str, status: RunStatus, assets: &[&str], ts: i64) -> RunRecord {
    RunRecord {
        run_id: run_id.to_string(),
        code_location_id: crate::storage::default_code_location_id(),
        job_name: None,
        status,
        start_time: ts,
        end_time: Some(ts),
        tags: Vec::new(),
        node_names: assets.iter().map(|s| s.to_string()).collect(),
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: crate::storage::LaunchedBy::default(),
    }
}

fn rec_with_run(asset: &str, last_run_id: Option<&str>, ts: i64) -> AssetRecord {
    AssetRecord {
        code_location_id: crate::storage::default_code_location_id(),
        asset_key: asset.to_string(),
        tags: vec![],
        kinds: vec![],
        asset_group: None,
        code_version: None,
        last_event_id: None,
        last_run_id: last_run_id.map(String::from),
        last_timestamp: Some(ts),
        last_data_version: None,
        last_materialization_code_version: None,
        last_input_data_versions: vec![],
        pool: vec![],
    }
}

#[test]
fn failure_floor_survives_co_batched_older_success() {
    let mut cache = AssetConditionCache::new("default".to_string());
    let mut delta = RefreshDelta::default();
    cache.apply_run_effects_to_delta(&mk_run("s", RunStatus::Success, &["R"], 100), &mut delta);
    cache.apply_run_effects_to_delta(&mk_run("f", RunStatus::Failure, &["R"], 200), &mut delta);
    cache.apply_refresh_delta(delta);

    assert_eq!(
        cache.failed_asset_timestamps.get("R"),
        Some(&200),
        "newer failure floor must survive an older co-batched success"
    );
    assert!(
        cache.failed_assets.contains("R"),
        "asset must remain in failed_assets"
    );
}

#[test]
fn newer_success_clears_failure_floor() {
    let mut cache = AssetConditionCache::new("default".to_string());
    let mut delta = RefreshDelta::default();
    cache.apply_run_effects_to_delta(&mk_run("f", RunStatus::Failure, &["R"], 100), &mut delta);
    cache.apply_run_effects_to_delta(&mk_run("s", RunStatus::Success, &["R"], 200), &mut delta);
    cache.apply_refresh_delta(delta);

    assert_eq!(
        cache.failed_asset_timestamps.get("R"),
        None,
        "a success newer than the failure must clear the floor"
    );
    assert!(!cache.failed_assets.contains("R"));
}

#[test]
fn failure_floor_skips_assets_materialized_in_the_failed_run() {
    let mut cache = AssetConditionCache::new("default".to_string());
    cache
        .records
        .insert("X".to_string(), rec_with_run("X", Some("R"), 150));
    cache
        .records
        .insert("Y".to_string(), rec_with_run("Y", Some("prev"), 50));

    let mut delta = RefreshDelta::default();
    cache.apply_run_effects_to_delta(
        &mk_run("R", RunStatus::Failure, &["X", "Y"], 150),
        &mut delta,
    );
    cache.apply_refresh_delta(delta);

    assert_eq!(
        cache.failed_asset_timestamps.get("Y"),
        Some(&150),
        "Y actually failed → floor at the run ts"
    );
    assert!(
        !cache.failed_asset_timestamps.contains_key("X"),
        "X materialized in the failed joint run → no failure floor"
    );
    assert!(!cache.failed_assets.contains("X"));
    assert!(cache.failed_assets.contains("Y"));
}

#[test]
fn partitioned_failure_does_not_set_asset_level_floor() {
    let mut cache = AssetConditionCache::new("default".to_string());
    cache.set_partitioned_assets(vec!["P".to_string()]);
    let mut run = mk_run("P", RunStatus::Failure, &["P"], 150);
    run.partition_key = Some(PartitionKey::Single {
        keys: vec!["p1".to_string()],
    });
    let mut delta = RefreshDelta::default();
    cache.apply_run_effects_to_delta(&run, &mut delta);
    cache.apply_refresh_delta(delta);

    assert!(
        !cache.failed_assets.contains("P"),
        "a single partition's failure must not floor the whole asset"
    );
    assert!(
        !cache.failed_asset_timestamps.contains_key("P"),
        "no asset-level failure timestamp for a partitioned run"
    );
}

#[test]
fn override_covers_every_succeeded_run_not_the_first_found() {
    let mut cache = AssetConditionCache::new("default".to_string());
    let mut delta = RefreshDelta::default();
    let r1 = mk_run("r1", RunStatus::Failure, &["x", "y"], 3000);
    let r2 = mk_run("r2", RunStatus::Failure, &["x", "z"], 4000);
    cache.apply_run_effects_to_delta(&r1, &mut delta);
    cache.apply_run_effects_to_delta(&r2, &mut delta);
    delta
        .materialized_overrides
        .entry("x".to_string())
        .or_default()
        .extend(["r1".to_string(), "r2".to_string()]);
    cache.apply_refresh_delta(delta);

    assert!(
        !cache.failed_assets.contains("x"),
        "x's step succeeded in the newest failing run — it must not be \
         floored just because an older run's success was discovered first"
    );
}

#[tokio::test]
async fn initial_load_run_completing_mid_load_is_not_lost() {
    // source → processed, both materialized by joint run r0. Schedule run r1
    // (source only) is Started when initial_load begins and completes between
    // the records read and the run-status reads. r1 must not be marked
    // applied off that late terminal read: that skipped its effects forever,
    // leaving last_run_asset_names[source] at r0's joint names, which kept
    // LastRunIncludesTarget true and permanently suppressed eager() downstream.
    use crate::storage::surrealdb_backend::SurrealStorage;

    let storage = SurrealStorage::new_memory().await.unwrap();
    let cl = crate::storage::default_code_location_id();
    let ctx = crate::storage::CodeLocationContext::new(cl.clone());

    storage
        .for_code_location(&ctx)
        .register_assets(&[
            rec_with_run("source", Some("r0"), 1000),
            rec_with_run("processed", Some("r0"), 1000),
        ])
        .await
        .unwrap();

    use crate::assets::graph::{GraphTopology, NodeKind, TopologyNode};
    let node = |name: &str| TopologyNode {
        name: name.into(),
        kind: NodeKind::Asset,
        group: None,
        parent_graph: None,
    };
    let topo = GraphTopology {
        nodes: vec![node("source"), node("processed")],
        edges: vec![("processed".to_string(), "source".to_string())],
    };
    storage
        .kv_set(
            &crate::graph_topology_key(&cl),
            &serde_json::to_vec(&topo).unwrap(),
        )
        .await
        .unwrap();

    let run =
        |run_id: &str, status: RunStatus, names: &[&str], start: i64, end: Option<i64>| RunRecord {
            run_id: run_id.to_string(),
            code_location_id: cl.clone(),
            job_name: None,
            status,
            start_time: start,
            end_time: end,
            tags: Vec::new(),
            node_names: names.iter().map(|s| s.to_string()).collect(),
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: crate::storage::LaunchedBy::Manual,
        };
    let mat_event = |asset: &str, run_id: &str, ts: i64| crate::storage::EventRecord {
        code_location_id: cl.clone(),
        event_type: crate::storage::EventType::Materialization {
            data_version: Some(format!("dv_{asset}_{ts}")),
        },
        asset_key: Some(asset.to_string()),
        run_id: run_id.to_string(),
        partition_key: None,
        timestamp: ts,
        metadata: vec![],
        input_data_versions: vec![],
    };

    storage
        .create_run(&run(
            "r0",
            RunStatus::Success,
            &["source", "processed"],
            1000,
            Some(1000),
        ))
        .await
        .unwrap();
    storage
        .store_events(&[
            mat_event("source", "r0", 1000),
            mat_event("processed", "r0", 1000),
        ])
        .await
        .unwrap();
    storage
        .create_run(&run("r1", RunStatus::Started, &["source"], 2000, None))
        .await
        .unwrap();

    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let mut cache = AssetConditionCache::new(cl.clone());
    cache.initial_load_gate = Some(Arc::clone(&gate));

    let (load, ()) = tokio::join!(cache.refresh(&storage, 0), async {
        gate.wait().await;
        // r1 completes at the poison point: materialization first, then the
        // terminal status — the executor's write order.
        storage
            .store_events(&[mat_event("source", "r1", 3000)])
            .await
            .unwrap();
        storage
            .update_run_status("r1", RunStatus::Success, Some(3000))
            .await
            .unwrap();
        gate.wait().await;
    });
    load.unwrap();

    assert!(
        !cache.applied_run_ids.contains_key("r1"),
        "a run completing mid-load must not be pre-marked applied"
    );

    let changed = cache.refresh(&storage, 0).await.unwrap();
    assert!(changed, "first refresh must deliver r1's completion");
    assert_eq!(
        cache.records.get("source").unwrap().last_run_id.as_deref(),
        Some("r1"),
        "source's record must reflect the run that completed mid-load"
    );
    let names = cache
        .last_run_asset_names
        .get("source")
        .and_then(|slots| slots.get(&None))
        .expect("source must have last-run names once r1's effects apply");
    assert_eq!(
        names.as_ref(),
        ["source".to_string()],
        "last-run names must come from r1, not the stale joint run r0"
    );
}

#[test]
fn partition_keyed_success_clears_unpartitioned_asset_floor() {
    let mut cache = AssetConditionCache::new("default".to_string());
    cache.set_partitioned_assets(vec!["P".to_string()]);

    let fail = mk_run("run-f", RunStatus::Failure, &["D"], 100);
    let mut delta = RefreshDelta::default();
    cache.apply_run_effects_to_delta(&fail, &mut delta);
    cache.apply_refresh_delta(delta);
    assert!(cache.failed_assets.contains("D"));

    let mut ok = mk_run("run-s", RunStatus::Success, &["P", "D"], 200);
    ok.partition_key = Some(PartitionKey::Single {
        keys: vec!["2024-01-01".to_string()],
    });
    let mut delta = RefreshDelta::default();
    cache.apply_run_effects_to_delta(&ok, &mut delta);
    cache.apply_refresh_delta(delta);

    assert!(
        !cache.failed_assets.contains("D"),
        "a partition-keyed success covering unpartitioned D must clear D's floor"
    );
    assert!(
        !cache.failed_assets.contains("P"),
        "the partitioned asset's outcome stays out of the asset-level floor"
    );
}
