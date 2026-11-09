//! Remote deps read the owner's state from its scope, through the code
//! location directory. This location is `DEFAULT_CODE_LOCATION_ID`.

use super::*;
use crate::assets::graph::{NodeKind, TopologyNode};
use crate::assets::remote::LocationRef;
use crate::condition::cache::RemoteDep;
use crate::condition::pass::{AssetConditionInfo, ConditionPass};
use crate::storage::surrealdb_backend::SurrealStorage;
use crate::storage::{CodeLocationContext, EventRecord, EventType};

const SEC: i64 = 1_000_000_000;

fn warehouse() -> LocationRef {
    LocationRef::new("dev", "warehouse").unwrap()
}

fn remote(local: &str, asset_key: &str) -> RemoteDep {
    RemoteDep {
        local: local.into(),
        location: warehouse(),
        asset_key: asset_key.into(),
    }
}

fn cache_with(remotes: Vec<RemoteDep>) -> AssetConditionCache {
    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.into());
    cache.set_remote_deps(remotes);
    cache
}

fn pass_with(
    target: &str,
    condition: ConditionNode,
    remotes: Vec<RemoteDep>,
    eval_state: ConditionEvalState,
) -> ConditionPass {
    ConditionPass::new(
        cache_with(remotes),
        eval_state,
        vec![AssetConditionInfo {
            asset_key: target.into(),
            condition,
            partition_info: None,
            backfill_strategy: None,
        }],
        HashMap::new(),
    )
}

async fn register(storage: &SurrealStorage, identity: &str, records: Vec<AssetRecord>) {
    let records: Vec<AssetRecord> = records
        .into_iter()
        .map(|mut r| {
            r.code_location_id = identity.into();
            r
        })
        .collect();
    storage
        .for_code_location(&CodeLocationContext::new(identity))
        .register_assets(&records)
        .await
        .unwrap();
}

/// This location's graph, as `(downstream, upstream)` edges.
async fn set_edges(storage: &SurrealStorage, edges: &[(&str, &str)]) {
    let names: HashSet<&str> = edges.iter().flat_map(|(down, up)| [*down, *up]).collect();
    let topology = GraphTopology {
        nodes: names
            .into_iter()
            .map(|name| TopologyNode::new(name, NodeKind::Asset))
            .collect(),
        edges: edges
            .iter()
            .map(|(down, up)| (down.to_string(), up.to_string()))
            .collect(),
    };
    storage
        .kv_set(
            &crate::graph_topology_key(DEFAULT_CODE_LOCATION_ID),
            &serde_json::to_vec(&topology).unwrap(),
        )
        .await
        .unwrap();
}

async fn materialize(
    storage: &SurrealStorage,
    identity: &str,
    asset: &str,
    run_id: &str,
    partition_key: Option<PartitionKey>,
    ts: i64,
) {
    storage
        .store_events(&[EventRecord {
            code_location_id: identity.into(),
            event_type: EventType::Materialization {
                data_version: Some(format!("dv_{asset}_{ts}")),
            },
            asset_key: Some(asset.into()),
            run_id: run_id.into(),
            partition_key,
            timestamp: ts,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();
}

fn run(
    identity: &str,
    run_id: &str,
    status: RunStatus,
    node_names: &[&str],
    partition_key: Option<PartitionKey>,
    start_time: i64,
) -> RunRecord {
    RunRecord {
        run_id: run_id.into(),
        code_location_id: identity.into(),
        job_name: Some("test".into()),
        status,
        start_time,
        end_time: None,
        tags: vec![],
        node_names: node_names.iter().map(|n| n.to_string()).collect(),
        priority: 0,
        partition_key,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    }
}

#[tokio::test]
async fn unregistered_location_blocks_eager_until_it_registers() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    register(
        &storage,
        DEFAULT_CODE_LOCATION_ID,
        vec![
            make_record("summary"),
            make_materialized_record("local_dep", 1_000),
        ],
    )
    .await;
    set_edges(&storage, &[("summary", "orders"), ("summary", "local_dep")]).await;

    let mut pass = pass_with(
        "summary",
        ConditionNode::eager(),
        vec![remote("orders", "raw_orders")],
        ConditionEvalState::default(),
    );
    pass.refresh_cache(&storage, 0).await.unwrap();

    assert_eq!(pass.cache.records["orders"].last_timestamp, None);
    assert!(
        pass.run(0, false).plan.is_empty(),
        "a dep on an unregistered location counts as missing"
    );

    storage
        .set_code_location_identity(&warehouse(), "wh")
        .await
        .unwrap();
    register(
        &storage,
        "wh",
        vec![make_materialized_record("raw_orders", 1_000)],
    )
    .await;
    pass.refresh_cache(&storage, 5 * SEC).await.unwrap();

    assert_eq!(
        pass.run(5 * SEC, false).plan.unpartitioned,
        vec!["summary".to_string()]
    );
}

#[tokio::test]
async fn resolved_location_supplies_the_owners_record_under_the_local_name() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    storage
        .set_code_location_identity(&warehouse(), "wh")
        .await
        .unwrap();
    register(
        &storage,
        "wh",
        vec![make_materialized_record("raw_orders", 1_000)],
    )
    .await;

    let mut cache = cache_with(vec![
        remote("orders", "raw_orders"),
        remote("gone", "not_in_warehouse"),
    ]);
    cache.refresh(&storage, 0).await.unwrap();

    let orders = &cache.records["orders"];
    assert_eq!(orders.asset_key, "orders");
    assert_eq!(orders.last_timestamp, Some(1_000));
    assert_eq!(orders.last_data_version.as_deref(), Some("dv_raw_orders"));
    assert!(!cache.records.contains_key("raw_orders"));

    let gone = &cache.records["gone"];
    assert_eq!(
        gone.last_timestamp, None,
        "a key the owner lacks is missing"
    );
}

#[tokio::test]
async fn location_registering_later_resolves_after_the_retry_wait() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    let mut cache = cache_with(vec![remote("orders", "raw_orders")]);
    cache.refresh(&storage, 0).await.unwrap();

    storage
        .set_code_location_identity(&warehouse(), "wh")
        .await
        .unwrap();
    register(
        &storage,
        "wh",
        vec![make_materialized_record("raw_orders", 1_000)],
    )
    .await;

    assert!(!cache.refresh(&storage, 4 * SEC).await.unwrap());
    assert_eq!(cache.records["orders"].last_timestamp, None);

    assert!(cache.refresh(&storage, 5 * SEC).await.unwrap());
    assert_eq!(cache.records["orders"].last_timestamp, Some(1_000));
}

#[tokio::test]
async fn identity_change_reloads_the_location() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    register(
        &storage,
        "wh-1",
        vec![
            make_materialized_record("raw_orders", 1_000),
            make_materialized_record("retired", 1_000),
        ],
    )
    .await;
    register(
        &storage,
        "wh-2",
        vec![make_materialized_record("raw_orders", 2_000)],
    )
    .await;
    storage
        .set_code_location_identity(&warehouse(), "wh-1")
        .await
        .unwrap();

    let mut cache = cache_with(vec![
        remote("orders", "raw_orders"),
        remote("retired", "retired"),
    ]);
    cache.refresh(&storage, 0).await.unwrap();
    assert_eq!(cache.records["orders"].last_timestamp, Some(1_000));

    storage
        .set_code_location_identity(&warehouse(), "wh-2")
        .await
        .unwrap();
    assert!(!cache.refresh(&storage, 29 * SEC).await.unwrap());
    assert_eq!(cache.records["orders"].last_timestamp, Some(1_000));

    assert!(cache.refresh(&storage, 30 * SEC).await.unwrap());
    assert_eq!(cache.records["orders"].last_timestamp, Some(2_000));
    assert_eq!(cache.records["retired"].last_timestamp, None);
}

#[tokio::test]
async fn two_local_names_can_bind_one_key() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    storage
        .set_code_location_identity(&warehouse(), "wh")
        .await
        .unwrap();
    register(
        &storage,
        "wh",
        vec![make_materialized_record("raw_orders", 1_000)],
    )
    .await;

    let mut cache = cache_with(vec![
        remote("orders", "raw_orders"),
        remote("orders_again", "raw_orders"),
    ]);
    cache.refresh(&storage, 0).await.unwrap();

    for local in ["orders", "orders_again"] {
        assert_eq!(cache.records[local].asset_key, local);
        assert_eq!(cache.records[local].last_timestamp, Some(1_000));
    }
}

#[tokio::test]
async fn partitioned_remote_takes_the_owners_partition_status() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    storage
        .set_code_location_identity(&warehouse(), "wh")
        .await
        .unwrap();
    register(&storage, "wh", vec![make_record("events")]).await;
    materialize(
        &storage,
        "wh",
        "events",
        "run-1",
        Some(spk("2024-01-01")),
        1_000,
    )
    .await;
    storage
        .create_run(&run(
            "wh",
            "run-2",
            RunStatus::Started,
            &["events"],
            Some(spk("2024-01-02")),
            2_000,
        ))
        .await
        .unwrap();
    storage
        .store_events(&[EventRecord {
            code_location_id: "wh".into(),
            event_type: EventType::StepStart,
            asset_key: Some("events".into()),
            run_id: "run-2".into(),
            partition_key: Some(spk("2024-01-02")),
            timestamp: 2_000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();

    let mut cache = cache_with(vec![remote("orders", "events")]);
    cache.set_partitioned_assets(vec!["orders".into()]);
    cache.refresh(&storage, 0).await.unwrap();

    let status = &cache.partition_status["orders"];
    assert_eq!(
        status.timestamps,
        HashMap::from([(spk("2024-01-01"), 1_000)])
    );
    assert_eq!(status.in_progress, HashSet::from([spk("2024-01-02")]));
    assert!(!cache.partition_status.contains_key("events"));
}

#[tokio::test]
async fn unregistered_partitioned_remote_has_no_materialized_partitions() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    let mut cache = cache_with(vec![remote("orders", "events")]);
    cache.set_partitioned_assets(vec!["orders".into()]);
    cache.refresh(&storage, 0).await.unwrap();

    assert!(cache.partition_status["orders"].timestamps.is_empty());
}

#[tokio::test]
async fn neither_the_owners_runs_nor_a_left_over_local_row_hide_an_update() {
    // The owner's run also built an asset named like this location's
    // `summary`, and a row `orders` is left here from when the asset was
    // local. Neither may make `orders`' last run "include" `summary`, which
    // would hide the owner's update from `any_deps_updated`.
    let storage = SurrealStorage::new_memory().await.unwrap();
    let mut left_over = make_materialized_record("orders", 500);
    left_over.last_run_id = Some("run-local".into());
    register(
        &storage,
        DEFAULT_CODE_LOCATION_ID,
        vec![left_over, make_materialized_record("summary", 500)],
    )
    .await;
    storage
        .create_run(&run(
            DEFAULT_CODE_LOCATION_ID,
            "run-local",
            RunStatus::Success,
            &["orders", "summary"],
            None,
            500,
        ))
        .await
        .unwrap();
    set_edges(&storage, &[("summary", "orders")]).await;

    storage
        .set_code_location_identity(&warehouse(), "wh")
        .await
        .unwrap();
    register(&storage, "wh", vec![make_record("raw_orders")]).await;
    storage
        .create_run(&run(
            "wh",
            "run-1",
            RunStatus::Success,
            &["raw_orders", "summary"],
            None,
            1_000,
        ))
        .await
        .unwrap();
    materialize(&storage, "wh", "raw_orders", "run-1", None, 1_000).await;

    let mut pass = pass_with(
        "summary",
        ConditionNode::any_deps_updated(),
        vec![remote("orders", "raw_orders")],
        ConditionEvalState::default(),
    );
    pass.refresh_cache(&storage, 0).await.unwrap();

    assert!(!pass.cache.last_run_asset_names.contains_key("orders"));
    assert_eq!(
        pass.cache.records["orders"].last_run_id.as_deref(),
        Some("run-1")
    );
    assert_eq!(
        pass.run(0, false).plan.unpartitioned,
        vec!["summary".to_string()],
        "the owner's data is newer than summary"
    );
}

/// `warehouse` registered as `wh`, owning `records`.
async fn warehouse_owning(storage: &SurrealStorage, records: Vec<AssetRecord>) {
    storage
        .set_code_location_identity(&warehouse(), "wh")
        .await
        .unwrap();
    register(storage, "wh", records).await;
}

fn in_progress(cache: &AssetConditionCache) -> HashSet<&str> {
    cache
        .remote_in_progress_keys()
        .map(String::as_str)
        .collect()
}

#[tokio::test]
async fn owners_run_is_in_progress_only_for_the_bound_local_until_it_ends() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    register(
        &storage,
        DEFAULT_CODE_LOCATION_ID,
        vec![make_materialized_record("other", 500)],
    )
    .await;
    warehouse_owning(
        &storage,
        vec![
            make_materialized_record("raw_orders", 1_000),
            make_materialized_record("other", 1_000),
        ],
    )
    .await;
    let mut cache = cache_with(vec![remote("orders", "raw_orders")]);
    cache.refresh(&storage, 0).await.unwrap();

    let started = run(
        "wh",
        "run-2",
        RunStatus::Started,
        &["raw_orders", "other"],
        None,
        2_000,
    );
    storage.create_run(&started).await.unwrap();
    assert!(cache.refresh(&storage, SEC).await.unwrap());
    assert_eq!(in_progress(&cache), HashSet::from(["orders"]));
    assert!(cache.in_progress_assets.is_empty());

    materialize(&storage, "wh", "raw_orders", "run-2", None, 3_000).await;
    storage
        .update_run_status("run-2", RunStatus::Success, Some(3_000))
        .await
        .unwrap();
    assert!(cache.refresh(&storage, 2 * SEC).await.unwrap());
    assert!(in_progress(&cache).is_empty());
    assert_eq!(cache.records["orders"].last_timestamp, Some(3_000));
    assert_eq!(cache.records["other"].last_timestamp, Some(500));

    assert!(
        !cache.refresh(&storage, 3 * SEC).await.unwrap(),
        "a redelivered run changes nothing"
    );
}

#[tokio::test]
async fn owners_run_that_ended_between_ticks_moves_the_record() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    warehouse_owning(
        &storage,
        vec![make_materialized_record("raw_orders", 1_000)],
    )
    .await;
    let mut cache = cache_with(vec![remote("orders", "raw_orders")]);
    cache.refresh(&storage, 0).await.unwrap();

    storage
        .create_run(&run(
            "wh",
            "run-2",
            RunStatus::Success,
            &["raw_orders"],
            None,
            2_000,
        ))
        .await
        .unwrap();
    materialize(&storage, "wh", "raw_orders", "run-2", None, 2_000).await;

    assert!(cache.refresh(&storage, SEC).await.unwrap());
    assert_eq!(cache.records["orders"].last_timestamp, Some(2_000));
}

#[tokio::test]
async fn deleted_owner_run_stops_counting_as_in_progress() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    warehouse_owning(
        &storage,
        vec![make_materialized_record("raw_orders", 1_000)],
    )
    .await;
    let mut cache = cache_with(vec![remote("orders", "raw_orders")]);
    cache.refresh(&storage, 0).await.unwrap();

    storage
        .create_run(&run(
            "wh",
            "run-2",
            RunStatus::Started,
            &["raw_orders"],
            None,
            2_000,
        ))
        .await
        .unwrap();
    cache.refresh(&storage, SEC).await.unwrap();
    assert_eq!(in_progress(&cache), HashSet::from(["orders"]));

    storage
        .update_run_status("run-2", RunStatus::Success, Some(3_000))
        .await
        .unwrap();
    assert!(storage.delete_run("run-2").await.unwrap());
    assert!(cache.refresh(&storage, 2 * SEC).await.unwrap());
    assert!(in_progress(&cache).is_empty());
}

#[tokio::test]
async fn live_run_at_load_is_in_progress() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    warehouse_owning(
        &storage,
        vec![make_materialized_record("raw_orders", 1_000)],
    )
    .await;
    storage
        .create_run(&run(
            "wh",
            "run-1",
            RunStatus::Started,
            &["raw_orders"],
            None,
            500,
        ))
        .await
        .unwrap();
    storage
        .create_run(&run(
            "wh",
            "run-2",
            RunStatus::Success,
            &["unrelated"],
            None,
            2_000,
        ))
        .await
        .unwrap();

    let mut cache = cache_with(vec![remote("orders", "raw_orders")]);
    cache.refresh(&storage, 0).await.unwrap();
    assert_eq!(in_progress(&cache), HashSet::from(["orders"]));

    storage
        .update_run_status("run-1", RunStatus::Failure, Some(3_000))
        .await
        .unwrap();
    assert!(cache.refresh(&storage, SEC).await.unwrap());
    assert!(in_progress(&cache).is_empty());
}

#[tokio::test]
async fn owners_failed_run_sets_no_floor_on_same_named_local_assets() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    register(
        &storage,
        DEFAULT_CODE_LOCATION_ID,
        vec![make_materialized_record("summary", 500)],
    )
    .await;
    warehouse_owning(
        &storage,
        vec![make_materialized_record("raw_orders", 1_000)],
    )
    .await;
    let mut cache = cache_with(vec![remote("orders", "raw_orders")]);
    cache.refresh(&storage, 0).await.unwrap();

    storage
        .create_run(&run(
            "wh",
            "run-2",
            RunStatus::Failure,
            &["raw_orders", "summary"],
            None,
            2_000,
        ))
        .await
        .unwrap();
    cache.refresh(&storage, SEC).await.unwrap();

    assert!(cache.failed_assets.is_empty());
    assert!(cache.in_progress_assets.is_empty());
}

#[tokio::test]
async fn owners_observation_updates_the_record() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    warehouse_owning(&storage, vec![make_record("raw_orders")]).await;
    let mut cache = cache_with(vec![remote("orders", "raw_orders")]);
    cache.refresh(&storage, 0).await.unwrap();

    storage
        .store_events(&[EventRecord {
            code_location_id: "wh".into(),
            event_type: EventType::Observation {
                data_version: Some("v2".into()),
            },
            asset_key: Some("raw_orders".into()),
            run_id: "observe".into(),
            partition_key: None,
            timestamp: 2_000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();

    assert!(cache.refresh(&storage, SEC).await.unwrap());
    assert_eq!(
        cache.records["orders"].last_data_version.as_deref(),
        Some("v2")
    );
    assert!(!cache.refresh(&storage, 2 * SEC).await.unwrap());
}

#[tokio::test]
async fn owners_keyed_live_run_marks_the_partition_in_progress() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    warehouse_owning(&storage, vec![make_record("events")]).await;
    let mut cache = cache_with(vec![remote("orders", "events")]);
    cache.set_partitioned_assets(vec!["orders".into()]);
    cache.refresh(&storage, 0).await.unwrap();

    let day = spk("2024-01-02");
    storage
        .create_run(&run(
            "wh",
            "run-2",
            RunStatus::Started,
            &["events"],
            Some(day.clone()),
            2_000,
        ))
        .await
        .unwrap();
    assert!(cache.refresh(&storage, SEC).await.unwrap());
    assert_eq!(
        cache.partition_status["orders"].in_progress,
        HashSet::from([day.clone()])
    );

    materialize(&storage, "wh", "events", "run-2", Some(day.clone()), 3_000).await;
    storage
        .update_run_status("run-2", RunStatus::Success, Some(3_000))
        .await
        .unwrap();
    assert!(cache.refresh(&storage, 2 * SEC).await.unwrap());
    let status = &cache.partition_status["orders"];
    assert!(status.in_progress.is_empty());
    assert_eq!(status.timestamps, HashMap::from([(day, 3_000)]));
}

#[tokio::test]
async fn eager_waits_for_the_owners_run_then_fires() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    register(
        &storage,
        DEFAULT_CODE_LOCATION_ID,
        vec![make_materialized_record("summary", 2_000)],
    )
    .await;
    set_edges(&storage, &[("summary", "orders")]).await;
    warehouse_owning(
        &storage,
        vec![make_materialized_record("raw_orders", 1_000)],
    )
    .await;

    let mut pass = pass_with(
        "summary",
        ConditionNode::eager(),
        vec![remote("orders", "raw_orders")],
        ConditionEvalState::default(),
    );
    pass.refresh_cache(&storage, 0).await.unwrap();
    assert!(pass.run(0, false).plan.is_empty());

    storage
        .create_run(&run(
            "wh",
            "run-2",
            RunStatus::Started,
            &["raw_orders"],
            None,
            3_000,
        ))
        .await
        .unwrap();
    pass.refresh_cache(&storage, SEC).await.unwrap();
    assert!(pass.run(SEC, false).plan.is_empty());

    // Another owner run moves the record while run-2 is still live.
    storage
        .create_run(&run(
            "wh",
            "run-3",
            RunStatus::Success,
            &["raw_orders"],
            None,
            3_500,
        ))
        .await
        .unwrap();
    materialize(&storage, "wh", "raw_orders", "run-3", None, 4_000).await;
    pass.refresh_cache(&storage, 2 * SEC).await.unwrap();
    assert_eq!(pass.cache.records["orders"].last_timestamp, Some(4_000));
    assert!(
        pass.run(2 * SEC, false).plan.is_empty(),
        "quiet while an owner run is in flight"
    );

    storage
        .update_run_status("run-2", RunStatus::Success, Some(5_000))
        .await
        .unwrap();
    pass.refresh_cache(&storage, 3 * SEC).await.unwrap();
    assert_eq!(
        pass.run(3 * SEC, false).plan.unpartitioned,
        vec!["summary".to_string()]
    );
}

fn planned_keys(plan: &crate::condition::pass::MaterializationPlan) -> HashSet<String> {
    let single = plan.single_partition_groups.keys().cloned();
    let multi = plan
        .multi_partition_backfills
        .iter()
        .flat_map(|(_, keys)| keys.iter().cloned());
    single.chain(multi).map(|pk| pk.to_display()).collect()
}

#[tokio::test]
async fn eager_maps_the_previous_day_onto_the_owners_partitions() {
    use crate::condition::pass::{PartitionInfo, PartitionUniverse};

    let storage = SurrealStorage::new_memory().await.unwrap();
    register(
        &storage,
        DEFAULT_CODE_LOCATION_ID,
        vec![make_record("summary")],
    )
    .await;
    set_edges(&storage, &[("summary", "events")]).await;
    warehouse_owning(&storage, vec![make_record("events")]).await;
    materialize(
        &storage,
        "wh",
        "events",
        "run-1",
        Some(spk("2024-01-02")),
        1_000,
    )
    .await;

    let grid = crate::timegrid::TimeGrid {
        cron_schedule: None,
        interval_seconds: Some(86_400.0),
        start: jiff::civil::date(2024, 1, 1).at(0, 0, 0, 0),
        end: Some(jiff::civil::date(2024, 1, 5).at(0, 0, 0, 0)),
        fmt: "%Y-%m-%d".into(),
    };
    let days: HashSet<PartitionKey> = ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]
        .into_iter()
        .map(spk)
        .collect();
    let mut pass = ConditionPass::new(
        cache_with(vec![remote("events", "events")]),
        ConditionEvalState::default(),
        vec![AssetConditionInfo {
            asset_key: "summary".into(),
            condition: ConditionNode::eager(),
            partition_info: Some(PartitionInfo {
                all_keys: days.clone(),
                mappings: HashMap::from([(
                    ("summary".into(), "events".into()),
                    PartitionMappingKind::TimeWindow {
                        offset: -1,
                        grid: Some(grid.clone()),
                    },
                )]),
                time_window_fmt: Some(grid.fmt.clone()),
                universe: PartitionUniverse::Frozen,
            }),
            backfill_strategy: None,
        }],
        HashMap::from([("events".into(), (days, PartitionUniverse::Frozen))]),
    );

    pass.refresh_cache(&storage, 0).await.unwrap();
    let fired = planned_keys(&pass.run(0, false).plan);
    assert!(fired.contains("2024-01-03"), "{fired:?}");
    assert!(
        !fired.contains("2024-01-04"),
        "the owner has no 2024-01-03: {fired:?}"
    );

    storage
        .create_run(&run(
            "wh",
            "run-2",
            RunStatus::Success,
            &["events"],
            Some(spk("2024-01-03")),
            2_000,
        ))
        .await
        .unwrap();
    materialize(
        &storage,
        "wh",
        "events",
        "run-2",
        Some(spk("2024-01-03")),
        2_000,
    )
    .await;
    pass.refresh_cache(&storage, SEC).await.unwrap();
    let fired = planned_keys(&pass.run(SEC, false).plan);
    assert!(fired.contains("2024-01-04"), "{fired:?}");
}

fn failed(cache: &AssetConditionCache) -> HashSet<&str> {
    cache.remote_failed_keys().map(String::as_str).collect()
}

#[tokio::test]
async fn execution_failed_follows_the_owners_runs() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    register(
        &storage,
        DEFAULT_CODE_LOCATION_ID,
        vec![make_materialized_record("summary", 500)],
    )
    .await;
    set_edges(&storage, &[("summary", "orders")]).await;
    warehouse_owning(
        &storage,
        vec![make_materialized_record("raw_orders", 1_000)],
    )
    .await;

    let mut pass = pass_with(
        "summary",
        ConditionNode::any_deps_match(ConditionNode::ExecutionFailed),
        vec![remote("orders", "raw_orders")],
        ConditionEvalState::default(),
    );
    pass.refresh_cache(&storage, 0).await.unwrap();
    assert!(pass.run(0, false).plan.is_empty());

    storage
        .create_run(&run(
            "wh",
            "run-2",
            RunStatus::Failure,
            &["raw_orders"],
            None,
            2_000,
        ))
        .await
        .unwrap();
    assert!(pass.refresh_cache(&storage, SEC).await.unwrap());
    assert_eq!(
        pass.run(SEC, false).plan.unpartitioned,
        vec!["summary".to_string()]
    );

    storage
        .create_run(&run(
            "wh",
            "run-3",
            RunStatus::Success,
            &["raw_orders"],
            None,
            3_000,
        ))
        .await
        .unwrap();
    materialize(&storage, "wh", "raw_orders", "run-3", None, 3_000).await;
    assert!(pass.refresh_cache(&storage, 2 * SEC).await.unwrap());
    assert!(failed(&pass.cache).is_empty());
    assert!(pass.run(2 * SEC, false).plan.is_empty());
}

#[tokio::test]
async fn owner_failure_floors_its_key_at_load() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    warehouse_owning(
        &storage,
        vec![make_materialized_record("raw_orders", 1_000)],
    )
    .await;
    storage
        .create_run(&run(
            "wh",
            "run-2",
            RunStatus::Failure,
            &["raw_orders"],
            None,
            2_000,
        ))
        .await
        .unwrap();

    let mut cache = cache_with(vec![
        remote("orders", "raw_orders"),
        remote("orders_again", "raw_orders"),
    ]);
    cache.refresh(&storage, 0).await.unwrap();
    assert_eq!(failed(&cache), HashSet::from(["orders", "orders_again"]));
    assert!(cache.failed_assets.is_empty());
}

#[tokio::test]
async fn owner_failure_that_materialized_the_key_sets_no_floor() {
    let storage = SurrealStorage::new_memory().await.unwrap();
    warehouse_owning(
        &storage,
        vec![make_materialized_record("raw_orders", 1_000)],
    )
    .await;
    let mut cache = cache_with(vec![remote("orders", "raw_orders")]);
    cache.refresh(&storage, 0).await.unwrap();

    storage
        .create_run(&run(
            "wh",
            "run-2",
            RunStatus::Failure,
            &["raw_orders", "later_step"],
            None,
            2_000,
        ))
        .await
        .unwrap();
    materialize(&storage, "wh", "raw_orders", "run-2", None, 2_000).await;
    cache.refresh(&storage, SEC).await.unwrap();
    assert!(failed(&cache).is_empty());

    let mut reloaded = cache_with(vec![remote("orders", "raw_orders")]);
    reloaded.refresh(&storage, 0).await.unwrap();
    assert!(failed(&reloaded).is_empty());
}
