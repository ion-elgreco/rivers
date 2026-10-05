use super::*;

#[tokio::test]
async fn test_restart_does_not_replay_newest_run_tick_tags() {
    // The newest pre-restart run must not repopulate the tick-scoped tag
    // accumulators on the first steady refresh — HasRunWithTags would report
    // a days-old run as "completed this tick" and spuriously fire.
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
    storage
        .create_run(&RunRecord {
            run_id: "run-old".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Success,
            start_time: 2000,
            end_time: Some(3000),
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

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_needs_tick_tags(&[ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("team".to_string(), "x".to_string())],
    }]);
    cache.refresh(&storage, 4000).await.unwrap();
    cache.refresh(&storage, 4001).await.unwrap();
    assert!(
        cache.tick_materialization_tags.is_empty(),
        "a pre-restart run must not be reported as completed this tick; got {:?}",
        cache.tick_materialization_tags
    );
}

#[tokio::test]
async fn test_same_timestamp_run_committed_after_refresh_is_seen() {
    // Dispatchers stamp one `now` across a batch committed record-by-record;
    // a refresh landing mid-batch must not permanently lose the runs that
    // commit afterward with the same start_time.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let recs = [
        make_materialized_record("a", 1000),
        make_materialized_record("b", 1000),
    ];
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&recs)
        .await
        .unwrap();

    let mk_run = |id: &str, asset: &str| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("test".to_string()),
        status: RunStatus::Started,
        start_time: 2000,
        end_time: None,
        tags: vec![("team".to_string(), "x".to_string())],
        node_names: vec![asset.to_string()],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    };

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_needs_tick_tags(&[ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![("team".to_string(), "x".to_string())],
    }]);
    cache.refresh(&storage, 0).await.unwrap();

    storage.create_run(&mk_run("run-1", "a")).await.unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    assert!(cache.in_progress_assets.contains_key("a"));

    // run-2 commits after the refresh with the SAME start_time.
    storage.create_run(&mk_run("run-2", "b")).await.unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        cache.in_progress_assets.contains_key("b"),
        "a same-timestamp run committed after the refresh must still be seen; got {:?}",
        cache.in_progress_assets
    );

    // Completion effects apply exactly once despite re-delivery.
    storage
        .update_run_status("run-1", RunStatus::Success, Some(3000))
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();
    assert!(!cache.in_progress_assets.contains_key("a"));
    let first_report = cache.tick_materialization_tags.clone();
    assert!(
        !first_report.is_empty(),
        "completion reports tick tags once"
    );
    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        cache.tick_materialization_tags.is_empty(),
        "re-delivered runs must not double-report tick tags; got {:?}",
        cache.tick_materialization_tags
    );
}

#[tokio::test]
async fn test_failure_floor_survives_daemon_restart() {
    // ExecutionFailed state is maintained by the steady-state refresh only;
    // it must survive a restart via the persisted eval state or failed assets
    // silently auto-retry after every daemon restart.
    use crate::condition::pass::ConditionPass;
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let recs = [
        make_materialized_record("a", 1000),
        make_materialized_record("b", 1000),
    ];
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&recs)
        .await
        .unwrap();

    let mk_run = |id: &str, start: i64, asset: &str| RunRecord {
        run_id: id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some("test".to_string()),
        status: RunStatus::Started,
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

    let mut pass = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        ConditionEvalState::default(),
        vec![],
        HashMap::new(),
    );
    pass.refresh_cache(&storage, 0).await.unwrap();

    // a's run fails without materializing it → floor set by the steady path.
    storage
        .create_run(&mk_run("run-fail", 2000, "a"))
        .await
        .unwrap();
    pass.refresh_cache(&storage, 0).await.unwrap();
    storage
        .update_run_status("run-fail", RunStatus::Failure, Some(3000))
        .await
        .unwrap();
    pass.refresh_cache(&storage, 0).await.unwrap();
    // A later unrelated run makes a's failure non-newest.
    storage
        .create_run(&mk_run("run-b", 4000, "b"))
        .await
        .unwrap();
    pass.refresh_cache(&storage, 0).await.unwrap();
    assert!(
        pass.cache.failed_assets.contains("a"),
        "precondition: floor set"
    );

    pass.run(5000, false);

    // Restart: fresh cache, persisted eval state.
    let mut pass2 = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        pass.eval_state.clone(),
        vec![],
        HashMap::new(),
    );
    pass2.refresh_cache(&storage, 6000).await.unwrap();
    assert!(
        pass2.cache.failed_assets.contains("a"),
        "failure floor must survive a daemon restart; got {:?}",
        pass2.cache.failed_assets
    );
    assert_eq!(pass2.cache.failed_asset_timestamps.get("a"), Some(&3000));
}

#[tokio::test]
async fn test_initial_load_seeds_observation_cursor() {
    // Historical observation events must not be replayed by the first
    // steady-state refresh — the replay's AssetClear wipes live Started-run
    // tracking established at initial_load.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, EventRecord, EventType, RunRecord, RunStatus, StorageBackend,
    };

    let storage = SurrealStorage::new_memory().await.unwrap();
    let rec_x = make_materialized_record("x", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_x])
        .await
        .unwrap();

    storage
        .store_event(&EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::Observation {
                data_version: Some("v1".to_string()),
            },
            asset_key: Some("x".to_string()),
            run_id: String::new(),
            partition_key: None,
            timestamp: 500,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();

    storage
        .create_run(&RunRecord {
            run_id: "run-x".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
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

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 3_000).await.unwrap();
    assert!(cache.in_progress_assets.contains_key("x"));

    cache.refresh(&storage, 3_001).await.unwrap();
    assert!(
        cache.in_progress_assets.contains_key("x"),
        "a historical observation must not wipe live in-flight tracking; got {:?}",
        cache.in_progress_assets
    );
}

#[tokio::test]
async fn test_clearable_sweep_sets_failure_floor_on_missed_terminal_failure() {
    // A Started run reported once fails with no materialization and no StepFailure event;
    // only the clearable sweep catches it. That sweep must also set the failure floor,
    // or eager/on_missing re-dispatches the failing run every tick.
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

    // Started run for a → observed, in_progress set, cursor advances to 2000.
    let run_id = "run-fail".to_string();
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
    cache.refresh(&storage, 0).await.unwrap();
    assert!(cache.in_progress_assets.contains_key("a"));

    // Run fails; start_time stays 2000, no StepFailure event → only the clearable sweep sees the failure.
    storage
        .update_run_status(&run_id, RunStatus::Failure, Some(3000))
        .await
        .unwrap();

    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        !cache.in_progress_assets.contains_key("a"),
        "a must be cleared from in_progress after the failure"
    );
    assert!(
        cache.failed_assets.contains("a"),
        "the clearable sweep must set the failure floor for a terminal failure \
         caught only there; got failed_assets={:?}",
        cache.failed_assets,
    );
    assert_eq!(cache.failed_asset_timestamps.get("a"), Some(&3000));
}

#[tokio::test]
async fn test_clearable_sweep_records_partitioned_failure_in_partition_status() {
    // Partitioned sibling of the sweep-floor case: a partitioned run fails with no
    // StepFailure event or record change; only the clearable sweep sees it. Since the
    // asset-level floor isn't set for partitioned runs, the failure must land in partition_status.failed.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();

    let rec_p = make_materialized_record("p", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_p])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_partitioned_assets(vec!["p".to_string()]);
    cache.refresh(&storage, 0).await.unwrap();

    let run_id = "run-part-fail".to_string();
    storage
        .create_run(&RunRecord {
            run_id: run_id.clone(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2000,
            end_time: None,
            tags: vec![],
            node_names: vec!["p".to_string()],
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
    assert!(cache.in_progress_assets.contains_key("p"));

    // Run fails; start_time stays 2000, no StepFailure event → only the clearable sweep sees the terminal transition.
    storage
        .update_run_status(&run_id, RunStatus::Failure, Some(3000))
        .await
        .unwrap();

    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        !cache.in_progress_assets.contains_key("p"),
        "p must be cleared from in_progress after the failure"
    );
    let status = cache
        .partition_status
        .get("p")
        .expect("p is a registered partitioned asset");
    assert!(
        status.failed.contains(&spk("2024-01-01")),
        "a partitioned terminal failure caught only by the sweep must surface in \
         partition_status.failed; got failed={:?}",
        status.failed,
    );
    assert!(
        status.failed_timestamps.contains_key(&spk("2024-01-01")),
        "the failed partition needs a floor timestamp for root_floor comparisons"
    );
    assert!(
        !cache.failed_assets.contains("p"),
        "the asset-level floor stays scoped to unpartitioned runs"
    );
}

#[tokio::test]
async fn test_queued_run_is_not_cleared_by_sweep() {
    // A run dispatched in run_queue mode is written as Queued; the clearable sweep must
    // not treat Queued as terminal, or eager/on_missing re-enqueues a duplicate every
    // tick. Only Success/Failure/Canceled are terminal.
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

    // Dispatch registers the asset; the queued dispatcher writes a Queued record.
    let run_id = "run-queued".to_string();
    cache.register_dispatched_run("a".to_string(), run_id.clone(), 0, None);
    assert!(cache.in_progress_assets.contains_key("a"));
    storage
        .create_run(&RunRecord {
            run_id: run_id.clone(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Queued,
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

    // Refresh must keep `a` gated — its run is still queued, not terminal.
    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        cache.in_progress_assets.contains_key("a"),
        "a queued run must keep the asset in_progress; got in_progress={:?}",
        cache.in_progress_assets,
    );
}

#[tokio::test]
async fn test_cache_does_not_store_empty_run_tags() {
    // The cache must not store empty tag vecs: an empty entry makes run_tags_match(&[],&[],&[])
    // vacuously true, so a no-arg LastExecutedWithTags would fire on every asset with any completed run.

    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();

    let rec = make_materialized_record("a", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();
    assert!(cache.last_run_tags.is_empty());

    // Create a completed run with no tags (arrives Success, a fast run completing between ticks).
    storage
        .create_run(&RunRecord {
            run_id: "run-no-tags".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Success,
            start_time: 2000,
            end_time: Some(3000),
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
    // The materialization event credits the record with the run.
    storage
        .store_event(&crate::storage::EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: crate::storage::EventType::Materialization {
                data_version: Some("dv1".to_string()),
            },
            asset_key: Some("a".to_string()),
            run_id: "run-no-tags".to_string(),
            partition_key: None,
            timestamp: 3000,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    // Cache must NOT have an entry for "a" — empty tags should be skipped
    assert!(
        !cache.last_run_tags.contains_key("a"),
        "cache should not store empty tags; got: {:?}",
        cache.last_run_tags.get("a"),
    );

    // Now create a run with tags arriving already-completed (Success), a fast run between ticks.
    storage
        .create_run(&RunRecord {
            run_id: "run-tagged".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Success,
            start_time: 4000,
            end_time: Some(5000),
            tags: vec![("env".to_string(), "prod".to_string())],
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
    storage
        .store_event(&crate::storage::EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: crate::storage::EventType::Materialization {
                data_version: Some("dv2".to_string()),
            },
            asset_key: Some("a".to_string()),
            run_id: "run-tagged".to_string(),
            partition_key: None,
            timestamp: 5000,
            metadata: vec![],
            input_data_versions: vec![],
        })
        .await
        .unwrap();
    cache.refresh(&storage, 0).await.unwrap();

    assert_eq!(
        cache
            .last_run_tags
            .get("a")
            .and_then(|slots| slots.get(&None)),
        Some(&Arc::from(vec![("env".to_string(), "prod".to_string())])),
    );
}

#[tokio::test]
async fn test_cache_tick_materialization_tags() {
    // Verify that tick_materialization_tags is populated on refresh and cleared on next refresh.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();

    let rec = make_materialized_record("a", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_needs_tick_tags(&[ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![],
    }]);
    cache.refresh(&storage, 0).await.unwrap();
    assert!(cache.tick_materialization_tags.is_empty());

    // Create a completed run with tags
    storage
        .create_run(&RunRecord {
            run_id: "run-tagged".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Success,
            start_time: 2000,
            end_time: Some(3000),
            tags: vec![("env".to_string(), "prod".to_string())],
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

    // tick_materialization_tags should have the run's tags
    assert_eq!(
        cache
            .tick_materialization_tags
            .get("a")
            .and_then(|slots| slots.get(&None)),
        Some(&vec![Arc::from(vec![(
            "env".to_string(),
            "prod".to_string()
        )])]),
    );

    // On next refresh with no new runs, tick tags should be cleared
    cache.refresh(&storage, 0).await.unwrap();
    assert!(
        cache.tick_materialization_tags.is_empty()
            || !cache.tick_materialization_tags.contains_key("a"),
        "tick_materialization_tags should be cleared on next refresh"
    );
}

#[tokio::test]
async fn test_cache_tick_materialization_tags_includes_empty_tags() {
    // A run with no tags is still recorded (empty vec) so AllRunsHaveTags returns false.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();

    let rec = make_materialized_record("a", 1000);
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec])
        .await
        .unwrap();

    let mut cache = AssetConditionCache::new(crate::storage::DEFAULT_CODE_LOCATION_ID.to_string());
    cache.set_needs_tick_tags(&[ConditionNode::HasRunWithTags {
        tag_keys: vec![],
        tag_values: vec![],
    }]);
    cache.refresh(&storage, 0).await.unwrap();

    storage
        .create_run(&RunRecord {
            run_id: "run-no-tags".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Success,
            start_time: 2000,
            end_time: Some(3000),
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
    cache.refresh(&storage, 0).await.unwrap();

    // tick_materialization_tags should have an entry with empty tags
    let tick_tags = cache
        .tick_materialization_tags
        .get("a")
        .and_then(|slots| slots.get(&None));
    assert!(
        tick_tags.is_some(),
        "should record materializations even with empty tags"
    );
    assert_eq!(tick_tags.unwrap(), &vec![Arc::from(vec![])]);
}

/// The question the old per-asset `step_completion` answered, rebuilt from a
/// batch result: did this asset finish in any of these runs, and which of them
/// did its step succeed in?
async fn completion(
    storage: &crate::storage::surrealdb_backend::SurrealStorage,
    asset: &str,
    runs: &[String],
) -> (bool, Vec<String>) {
    use crate::storage::StorageBackend;
    let outcomes = storage
        .step_outcomes(std::slice::from_ref(&asset.to_string()), runs)
        .await
        .unwrap();
    let mut succeeded: Vec<String> = outcomes
        .iter()
        .filter(|o| o.succeeded)
        .map(|o| o.run_id.clone())
        .collect();
    succeeded.sort();
    succeeded.dedup();
    (!outcomes.is_empty(), succeeded)
}

#[tokio::test]
async fn test_step_completion_sql_query() {
    // Direct test of the step_completion event scan against SurrealDB.

    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{EventRecord, EventType, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();

    let run_id = "run-query-test".to_string();
    let other_run = "run-other".to_string();

    // Write events for asset "a" in run-query-test
    storage
        .store_events(&[
            EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepStart,
                asset_key: Some("a".to_string()),
                run_id: run_id.clone(),
                partition_key: None,
                timestamp: 1000,
                metadata: vec![],
                input_data_versions: vec![],
            },
            EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization {
                    data_version: Some("dv1".to_string()),
                },
                asset_key: Some("a".to_string()),
                run_id: run_id.clone(),
                partition_key: None,
                timestamp: 2000,
                metadata: vec![],
                input_data_versions: vec![],
            },
            EventRecord {
                code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::StepSuccess,
                asset_key: Some("a".to_string()),
                run_id: run_id.clone(),
                partition_key: None,
                timestamp: 3000,
                metadata: vec![],
                input_data_versions: vec![],
            },
        ])
        .await
        .unwrap();

    // Write StepSuccess for different asset "b" in same run
    storage
        .store_events(&[EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepSuccess,
            asset_key: Some("b".to_string()),
            run_id: run_id.clone(),
            partition_key: None,
            timestamp: 3000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();

    // Write StepSuccess for "a" in a DIFFERENT run
    storage
        .store_events(&[EventRecord {
            code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: EventType::StepSuccess,
            asset_key: Some("a".to_string()),
            run_id: other_run.clone(),
            partition_key: None,
            timestamp: 4000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();

    // Test 1: asset "a" in run-query-test → true
    assert!(
        completion(&storage, "a", std::slice::from_ref(&run_id))
            .await
            .0,
        "should find StepSuccess for 'a' in run-query-test"
    );

    // Test 2: asset "b" in run-query-test → true
    assert!(
        completion(&storage, "b", std::slice::from_ref(&run_id))
            .await
            .0,
        "should find StepSuccess for 'b' in run-query-test"
    );

    // Test 3: asset "a" in other-run → true
    assert!(
        completion(&storage, "a", std::slice::from_ref(&other_run))
            .await
            .0,
        "should find StepSuccess for 'a' in run-other"
    );

    // Test 4: asset "c" (doesn't exist) → false
    assert!(
        !completion(&storage, "c", std::slice::from_ref(&run_id))
            .await
            .0,
        "should NOT find StepSuccess for 'c'"
    );

    // Test 5: asset "a" in non-existent run → false
    assert!(
        !completion(&storage, "a", &["run-nonexistent".to_string()])
            .await
            .0,
        "should NOT find StepSuccess in non-existent run"
    );

    // Test 6: asset "a" in multiple run_ids → true (matches first)
    assert!(
        completion(&storage, "a", &[run_id.clone(), other_run.clone()])
            .await
            .0,
        "should find StepSuccess for 'a' across multiple run_ids"
    );

    // Test 7: asset "b" in other_run only → false (b only has events in run_id)
    assert!(
        !completion(&storage, "b", std::slice::from_ref(&other_run))
            .await
            .0,
        "should NOT find StepSuccess for 'b' in run-other"
    );

    // Test 8: both assets in one call, each answered from its own runs.
    let outcomes = storage
        .step_outcomes(
            &["a".to_string(), "b".to_string()],
            &[run_id.clone(), other_run.clone()],
        )
        .await
        .unwrap();
    let mut pairs: Vec<(String, String)> = outcomes
        .iter()
        .map(|o| (o.asset_key.clone(), o.run_id.clone()))
        .collect();
    pairs.sort();
    pairs.dedup();
    assert_eq!(
        pairs,
        vec![
            ("a".to_string(), other_run.clone()),
            ("a".to_string(), run_id.clone()),
            ("b".to_string(), run_id.clone()),
        ],
        "one call must answer for every asset without inventing a 'b' in run-other"
    );
}

#[tokio::test]
async fn test_step_completion_single_pass() {
    // One storage call answers both "did any step complete" and "which run succeeded"
    // (avoids the per-run N+1 during backfills).
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{EventRecord, EventType, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let ev = |run: &str, asset: &str, event_type: EventType| EventRecord {
        code_location_id: crate::storage::DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type,
        asset_key: Some(asset.to_string()),
        run_id: run.to_string(),
        partition_key: None,
        timestamp: 1000,
        metadata: vec![],
        input_data_versions: vec![],
    };
    storage
        .store_events(&[
            ev("run-fail", "a", EventType::StepFailure),
            ev("run-ok", "a", EventType::StepSuccess),
            ev("run-ok", "b", EventType::StepFailure),
        ])
        .await
        .unwrap();

    let runs = vec!["run-fail".to_string(), "run-ok".to_string()];
    let (completed, succeeded) = completion(&storage, "a", &runs).await;
    assert!(completed, "a completed a step in the given runs");
    assert_eq!(
        succeeded,
        vec!["run-ok".to_string()],
        "every succeeding run must be identified"
    );

    let (completed, succeeded) = completion(&storage, "b", &runs).await;
    assert!(completed, "a failure is still a completion");
    assert!(
        succeeded.is_empty(),
        "no success run for a failed-only asset"
    );

    let (completed, succeeded) = completion(&storage, "c", &runs).await;
    assert!(!completed, "no events for 'c' in the given runs");
    assert!(succeeded.is_empty());

    // The point of the batch: three assets, one round trip, each still keyed
    // to the run its own step finished in.
    let outcomes = storage
        .step_outcomes(&["a".to_string(), "b".to_string(), "c".to_string()], &runs)
        .await
        .unwrap();
    let mut seen: Vec<(String, String, bool)> = outcomes
        .iter()
        .map(|o| (o.asset_key.clone(), o.run_id.clone(), o.succeeded))
        .collect();
    seen.sort();
    assert_eq!(
        seen,
        vec![
            ("a".to_string(), "run-fail".to_string(), false),
            ("a".to_string(), "run-ok".to_string(), true),
            ("b".to_string(), "run-ok".to_string(), false),
        ],
        "one call must carry each asset's own outcome per run"
    );
}
