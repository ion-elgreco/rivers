use super::*;

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
