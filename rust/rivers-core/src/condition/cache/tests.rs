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
        action: None,
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
            launched_by: crate::storage::LaunchedBy::Manual { user: None },
            action: None,
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

/// A tracked run whose row is deleted between completion and the next sweep
/// must not wedge the in-progress guard: `delete_run` only removes terminal
/// runs, so a vanished row implies the run finished — the sweep clears its
/// tracking. A dispatched-but-unconfirmed (pending) id is NOT treated as
/// vanished; the pending grace-period eviction owns that case.
#[tokio::test]
async fn deleted_tracked_run_clears_in_progress_guard() {
    use crate::storage::surrealdb_backend::SurrealStorage;

    let storage = SurrealStorage::new_memory().await.unwrap();
    let cl = crate::storage::default_code_location_id();
    let mut cache = AssetConditionCache::new(cl.clone());

    // Initial load on empty storage, then dispatch registers the run id
    // eagerly (before any row exists).
    cache.refresh(&storage, 0).await.unwrap();
    cache.register_dispatched_run("a".to_string(), "run-del".to_string(), 1, None);
    cache.register_dispatched_run("b".to_string(), "run-pending".to_string(), 1, None);

    // run-del's row lands and a refresh confirms it; run-pending never lands.
    let mut rec = mk_run("run-del", RunStatus::Started, &["a"], 1000);
    rec.code_location_id = cl.clone();
    storage.create_run(&rec).await.unwrap();
    cache.refresh(&storage, 2).await.unwrap();
    assert!(
        !cache.pending_runs.contains_key("run-del"),
        "observed run must leave pending"
    );
    assert!(
        cache
            .in_progress_assets
            .get("a")
            .is_some_and(|runs| runs.contains_key("run-del")),
        "confirmed run stays tracked while active"
    );

    // The run completes and is deleted before the next sweep observes the
    // terminal status.
    storage
        .update_run_status("run-del", RunStatus::Success, Some(2000))
        .await
        .unwrap();
    assert!(storage.delete_run("run-del").await.unwrap());

    let changed = cache.refresh(&storage, 3).await.unwrap();
    assert!(changed, "clearing a vanished run is a meaningful change");
    assert!(
        cache
            .in_progress_assets
            .get("a")
            .is_none_or(|runs| !runs.contains_key("run-del")),
        "deleted run must not stay tracked as in-progress"
    );
    assert!(
        cache
            .in_progress_assets
            .get("b")
            .is_some_and(|runs| runs.contains_key("run-pending")),
        "a pending id inside its grace window must not be swept as vanished"
    );
}

#[test]
fn action_runs_are_not_materialization_attempts() {
    // a successful action run (optimize/delete/observe) must not
    // count as a materialization for condition bookkeeping — its effects
    // reach the cache through the record refresh instead. A failed action
    // run must not raise the failure floor either.
    let cache = AssetConditionCache::new("default".to_string());
    let mut delta = RefreshDelta::default();

    let mut ok = mk_run("act-ok", RunStatus::Success, &["R"], 100);
    ok.action = Some("optimize".to_string());
    let mut boom = mk_run("act-boom", RunStatus::Failure, &["R"], 200);
    boom.action = Some("optimize".to_string());

    assert!(cache.apply_run_effects_to_delta(&ok, &mut delta));
    assert!(cache.apply_run_effects_to_delta(&boom, &mut delta));

    assert!(
        delta.materialized_overrides.is_empty(),
        "action success must not register a materialized override"
    );
    assert!(
        delta.failed_adds.is_empty(),
        "action failure must not raise the failure floor"
    );
    assert!(
        delta.last_run_updates.is_empty(),
        "action runs must not become per-partition materialization attempts"
    );
    // Both runs are still marked applied so the cursor dedup works.
    assert_eq!(delta.applied_runs.len(), 2);
}

/// A deleted partition must leave the cached partition status: the
/// incremental timestamp fetch only sees rows whose timestamp advanced, so
/// a row deletion is invisible to it — completed action runs get their
/// touched keys re-checked by key and evicted when the row is gone.
#[tokio::test]
async fn deleted_partition_evicted_from_partition_status() {
    use crate::storage::surrealdb_backend::SurrealStorage;

    let storage = SurrealStorage::new_memory().await.unwrap();
    let cl = crate::storage::default_code_location_id();
    let single = |k: &str| PartitionKey::Single {
        keys: vec![k.to_string()],
    };

    storage
        .create_run(&mk_run("r0", RunStatus::Success, &["events"], 1000))
        .await
        .unwrap();
    let mat_event = |pk: &str, ts: i64| crate::storage::EventRecord {
        code_location_id: cl.clone(),
        event_type: crate::storage::EventType::Materialization {
            data_version: Some(format!("dv_{pk}_{ts}")),
        },
        asset_key: Some("events".to_string()),
        run_id: "r0".to_string(),
        partition_key: Some(single(pk)),
        timestamp: ts,
        metadata: vec![],
        input_data_versions: vec![],
    };
    storage
        .store_events(&[mat_event("p1", 1000), mat_event("p2", 1000)])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(cl.clone());
    cache.set_partitioned_assets(vec!["events".to_string()]);
    cache.refresh(&storage, 0).await.unwrap();

    let ts = &cache.partition_status.get("events").unwrap().timestamps;
    assert!(
        ts.contains_key(&single("p1")) && ts.contains_key(&single("p2")),
        "both materialized partitions must be cached after initial load"
    );

    // Delete p1 via an action run — executor write order: events, then status.
    let mut del = mk_run("r1", RunStatus::Started, &["events"], 2000);
    del.end_time = None;
    del.partition_key = Some(single("p1"));
    del.action = Some("delete".to_string());
    storage.create_run(&del).await.unwrap();
    storage
        .store_events(&[crate::storage::EventRecord {
            code_location_id: cl.clone(),
            event_type: crate::storage::EventType::Deletion,
            asset_key: Some("events".to_string()),
            run_id: "r1".to_string(),
            partition_key: Some(single("p1")),
            timestamp: 2000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();
    storage
        .update_run_status("r1", RunStatus::Success, Some(2000))
        .await
        .unwrap();

    let changed = cache.refresh(&storage, 1).await.unwrap();
    assert!(changed, "a completed delete is a meaningful change");
    let status = cache.partition_status.get("events").unwrap();
    assert!(
        !status.timestamps.contains_key(&single("p1")),
        "deleted partition must be evicted from the cached timestamps"
    );
    assert!(
        status.timestamps.contains_key(&single("p2")),
        "untouched partition must stay cached"
    );
}

fn mat_events_for(cl: &str, asset: &str, keys: &[&str], ts: i64) -> Vec<crate::storage::EventRecord> {
    keys.iter()
        .map(|pk| crate::storage::EventRecord {
            code_location_id: cl.to_string(),
            event_type: crate::storage::EventType::Materialization {
                data_version: Some(format!("dv_{pk}_{ts}")),
            },
            asset_key: Some(asset.to_string()),
            run_id: "r0".to_string(),
            partition_key: Some(PartitionKey::Single {
                keys: vec![pk.to_string()],
            }),
            timestamp: ts,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .collect()
}

fn deletion_event(cl: &str, asset: &str, run_id: &str, pk: &str, ts: i64) -> crate::storage::EventRecord {
    crate::storage::EventRecord {
        code_location_id: cl.to_string(),
        event_type: crate::storage::EventType::Deletion,
        asset_key: Some(asset.to_string()),
        run_id: run_id.to_string(),
        partition_key: Some(PartitionKey::Single {
            keys: vec![pk.to_string()],
        }),
        timestamp: ts,
        metadata: vec![],
        input_data_versions: vec![],
    }
}

/// The run cursor trails the newest start_time, so an action run that is
/// still live when any newer run starts falls below the cursor and is never
/// re-delivered. Its terminal transition — and the deleted-partition
/// eviction only a completed action run can drive — must not be lost.
#[tokio::test]
async fn action_completion_survives_cursor_passing_it() {
    use crate::storage::surrealdb_backend::SurrealStorage;

    let storage = SurrealStorage::new_memory().await.unwrap();
    let cl = crate::storage::default_code_location_id();
    let single = |k: &str| PartitionKey::Single {
        keys: vec![k.to_string()],
    };

    storage
        .create_run(&mk_run("r0", RunStatus::Success, &["events"], 1000))
        .await
        .unwrap();
    storage
        .store_events(&mat_events_for(&cl, "events", &["p1", "p2"], 1000))
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(cl.clone());
    cache.set_partitioned_assets(vec!["events".to_string()]);
    cache.refresh(&storage, 0).await.unwrap();

    // A delete of p1 starts…
    let mut del = mk_run("r1", RunStatus::Started, &["events"], 2000);
    del.end_time = None;
    del.partition_key = Some(single("p1"));
    del.action = Some("delete".to_string());
    storage.create_run(&del).await.unwrap();
    // …and a newer unrelated run pushes the cursor past its start_time.
    let mut newer = mk_run("r2", RunStatus::Started, &["other"], 3000);
    newer.end_time = None;
    storage.create_run(&newer).await.unwrap();
    cache.refresh(&storage, 1).await.unwrap();

    // The delete completes only after the cursor moved past it.
    storage
        .store_events(&[deletion_event(&cl, "events", "r1", "p1", 3500)])
        .await
        .unwrap();
    storage
        .update_run_status("r1", RunStatus::Success, Some(3500))
        .await
        .unwrap();

    let changed = cache.refresh(&storage, 2).await.unwrap();
    assert!(changed, "the verb's completion is a meaningful change");
    let status = cache.partition_status.get("events").unwrap();
    assert!(
        !status.timestamps.contains_key(&single("p1")),
        "deleted partition must be evicted even though the run fell below the cursor"
    );
    assert!(
        status.timestamps.contains_key(&single("p2")),
        "untouched partition must stay cached"
    );
}

/// Same gap across a restart: initial load sets the cursor from the newest
/// run, so a live action run started earlier is already below it — its
/// completion must still arrive.
#[tokio::test]
async fn action_completion_survives_daemon_restart() {
    use crate::storage::surrealdb_backend::SurrealStorage;

    let storage = SurrealStorage::new_memory().await.unwrap();
    let cl = crate::storage::default_code_location_id();
    let single = |k: &str| PartitionKey::Single {
        keys: vec![k.to_string()],
    };

    storage
        .create_run(&mk_run("r0", RunStatus::Success, &["events"], 1000))
        .await
        .unwrap();
    storage
        .store_events(&mat_events_for(&cl, "events", &["p1", "p2"], 1000))
        .await
        .unwrap();
    let mut del = mk_run("r1", RunStatus::Started, &["events"], 2000);
    del.end_time = None;
    del.partition_key = Some(single("p1"));
    del.action = Some("delete".to_string());
    storage.create_run(&del).await.unwrap();
    let mut newer = mk_run("r2", RunStatus::Started, &["other"], 3000);
    newer.end_time = None;
    storage.create_run(&newer).await.unwrap();

    // Daemon starts while the verb is running.
    let mut cache = AssetConditionCache::new(cl.clone());
    cache.set_partitioned_assets(vec!["events".to_string()]);
    cache.refresh(&storage, 0).await.unwrap();

    storage
        .store_events(&[deletion_event(&cl, "events", "r1", "p1", 3500)])
        .await
        .unwrap();
    storage
        .update_run_status("r1", RunStatus::Success, Some(3500))
        .await
        .unwrap();

    let changed = cache.refresh(&storage, 1).await.unwrap();
    assert!(changed, "the verb's completion is a meaningful change");
    let status = cache.partition_status.get("events").unwrap();
    assert!(
        !status.timestamps.contains_key(&single("p1")),
        "deleted partition must be evicted after a restart mid-verb"
    );
    assert!(
        status.timestamps.contains_key(&single("p2")),
        "untouched partition must stay cached"
    );
}

/// Deleting a partition wipes the timestamp row that superseded its old
/// failures, so a daemon restart would resurrect it as failed — and
/// `eager()`'s `!ExecutionFailed` gate then keeps it from ever being
/// rebuilt. A deletion supersedes a failure like a newer materialization.
#[tokio::test]
async fn deleted_partition_does_not_resurrect_as_failed() {
    use crate::storage::surrealdb_backend::SurrealStorage;

    let storage = SurrealStorage::new_memory().await.unwrap();
    let cl = crate::storage::default_code_location_id();
    let single = |k: &str| PartitionKey::Single {
        keys: vec![k.to_string()],
    };

    // p1 fails, then materializes successfully (failure superseded)…
    let mut fail = mk_run("rf", RunStatus::Failure, &["events"], 1000);
    fail.partition_key = Some(single("p1"));
    storage.create_run(&fail).await.unwrap();
    storage
        .create_run(&mk_run("r0", RunStatus::Success, &["events"], 2000))
        .await
        .unwrap();
    storage
        .store_events(&mat_events_for(&cl, "events", &["p1"], 2000))
        .await
        .unwrap();
    // …then an action deletes it.
    let mut del = mk_run("rd", RunStatus::Success, &["events"], 3000);
    del.partition_key = Some(single("p1"));
    del.action = Some("delete".to_string());
    storage.create_run(&del).await.unwrap();
    storage
        .store_events(&[deletion_event(&cl, "events", "rd", "p1", 3000)])
        .await
        .unwrap();

    // Daemon restart: the partition is missing, not failed.
    let mut cache = AssetConditionCache::new(cl.clone());
    cache.set_partitioned_assets(vec!["events".to_string()]);
    cache.refresh(&storage, 0).await.unwrap();
    let status = cache.partition_status.get("events").unwrap();
    assert!(
        !status.failed.contains(&single("p1")),
        "deleted partition must not resurrect as failed on restart"
    );
    assert!(
        !status.timestamps.contains_key(&single("p1")),
        "deleted partition must not be cached as materialized"
    );
}

/// Whole-asset analog: deletion clears the record's `last_timestamp`, so
/// the initial-load failure floors would re-apply a long-superseded failed
/// run after every restart.
#[tokio::test]
async fn deleted_asset_does_not_resurrect_failure_floor() {
    use crate::storage::surrealdb_backend::SurrealStorage;

    let storage = SurrealStorage::new_memory().await.unwrap();
    let cl = crate::storage::default_code_location_id();

    storage
        .create_run(&mk_run("rf", RunStatus::Failure, &["report"], 1000))
        .await
        .unwrap();
    storage
        .create_run(&mk_run("r0", RunStatus::Success, &["report"], 2000))
        .await
        .unwrap();
    storage
        .store_events(&[crate::storage::EventRecord {
            code_location_id: cl.clone(),
            event_type: crate::storage::EventType::Materialization {
                data_version: Some("dv_1".to_string()),
            },
            asset_key: Some("report".to_string()),
            run_id: "r0".to_string(),
            partition_key: None,
            timestamp: 2000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();
    let mut del = mk_run("rd", RunStatus::Success, &["report"], 3000);
    del.action = Some("delete".to_string());
    storage.create_run(&del).await.unwrap();
    storage
        .store_events(&[crate::storage::EventRecord {
            code_location_id: cl.clone(),
            event_type: crate::storage::EventType::Deletion,
            asset_key: Some("report".to_string()),
            run_id: "rd".to_string(),
            partition_key: None,
            timestamp: 3000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(cl.clone());
    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        !cache.failed_assets.contains("report"),
        "deleted asset must not resurrect its failure floor on restart"
    );
}
