use super::*;

#[tokio::test]
async fn test_stale_eval_state_with_live_queued_run_does_not_redispatch() {
    // Crash window: a condition fired and its run was durably enqueued, but
    // the daemon died before persisting eval state. On restart with the stale
    // (pre-fire) latches, the live queued run must suppress a re-dispatch.
    use crate::condition::pass::{AssetConditionInfo, ConditionPass};
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let rec_a = make_record("a"); // missing → eager's missing arm fires
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[rec_a])
        .await
        .unwrap();

    let conditions = || {
        vec![AssetConditionInfo {
            asset_key: "a".to_string(),
            condition: ConditionNode::eager(),
            partition_info: None,
            backfill_strategy: None,
        }]
    };

    let mut pass1 = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        ConditionEvalState::default(),
        conditions(),
        HashMap::new(),
    );
    pass1.refresh_cache(&storage, 1_000).await.unwrap();
    let out = pass1.run(1_000, false);
    assert!(
        out.plan.unpartitioned.contains(&"a".to_string()),
        "precondition: eager fires for the missing asset"
    );

    // Dispatch durably enqueued the run; the daemon crashed before
    // set_condition_eval_state, so pass1.eval_state is never persisted.
    storage
        .create_run(&RunRecord {
            run_id: "run-crash".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Queued,
            start_time: 2_000,
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

    // Restart with the STALE (pre-fire) eval state.
    let mut pass2 = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        ConditionEvalState::default(),
        conditions(),
        HashMap::new(),
    );
    pass2.refresh_cache(&storage, 3_000).await.unwrap();
    let out2 = pass2.run(3_000, false);
    assert!(
        out2.plan.unpartitioned.is_empty(),
        "the live queued run must suppress a duplicate dispatch; got {:?}",
        out2.plan.unpartitioned
    );
}

#[tokio::test]
async fn test_dispatch_failure_preserves_edge_trigger_for_retry() {
    // A fired root's edge-trigger latches (dep baselines, previous_results,
    // handled cursor) must survive a failed dispatch: the tick commits only
    // for assets that actually dispatched, and the failure forces a retry
    // evaluation on the next tick even with no new upstream changes.
    use crate::condition::pass::{AssetConditionInfo, ConditionPass};
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[
            make_materialized_record("raw", 1_000),
            make_materialized_record("dst", 1_000),
        ])
        .await
        .unwrap();
    use crate::assets::graph::TopologyNode;
    let topo = GraphTopology {
        nodes: vec![
            TopologyNode {
                name: "raw".into(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            },
            TopologyNode {
                name: "dst".into(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            },
        ],
        edges: vec![("dst".to_string(), "raw".to_string())],
    };
    storage
        .kv_set(
            &crate::graph_topology_key(DEFAULT_CODE_LOCATION_ID),
            &serde_json::to_vec(&topo).unwrap(),
        )
        .await
        .unwrap();

    let mut pass = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        ConditionEvalState {
            is_initial: true,
            ..Default::default()
        },
        vec![AssetConditionInfo {
            asset_key: "dst".to_string(),
            condition: ConditionNode::any_deps_match(ConditionNode::DataVersionChanged),
            partition_info: None,
            backfill_strategy: None,
        }],
        HashMap::new(),
    );

    // Initial tick seeds the dep baselines; nothing fires.
    pass.refresh_cache(&storage, 2_000).await.unwrap();
    let out = pass.run(2_000, false);
    assert!(out.plan.is_empty(), "initial tick must not fire");

    // raw re-materializes.
    storage
        .create_run(&RunRecord {
            run_id: "run-raw".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2_500,
            end_time: None,
            tags: vec![],
            node_names: vec!["raw".to_string()],
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
        .update_run_status("run-raw", RunStatus::Success, Some(3_000))
        .await
        .unwrap();
    storage
        .store_events(&[crate::storage::EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: crate::storage::EventType::Materialization {
                data_version: Some("v2".to_string()),
            },
            asset_key: Some("raw".to_string()),
            run_id: "run-raw".to_string(),
            partition_key: None,
            timestamp: 3_000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();
    pass.refresh_cache(&storage, 4_000).await.unwrap();

    // The dep update fires dst, but its dispatch fails.
    let out = pass.plan_tick(4_000, false);
    assert!(
        out.plan.unpartitioned.contains(&"dst".to_string()),
        "precondition: the dep update must fire dst"
    );
    // Mimic the daemon's failure path on the shared cache…
    pass.cache
        .register_dispatched_run("dst".to_string(), "run-fail".to_string(), 4_000, None);
    pass.cache.clear_dispatched_run("dst", "run-fail");
    // …and commit the tick with dst's dispatch marked failed.
    let failed: HashSet<String> = ["dst".to_string()].into_iter().collect();
    let dirty = pass.commit_tick(&out, &failed, 4_000);
    assert!(
        !dirty,
        "an all-failed tick leaves no latch state to persist"
    );

    assert!(
        !pass.should_skip(false),
        "a failed dispatch must force a retry evaluation even with no new changes"
    );
    let retry = pass.plan_tick(5_000, false);
    assert!(
        retry.plan.unpartitioned.contains(&"dst".to_string()),
        "the un-consumed dep trigger must re-fire on the retry tick"
    );
    let dirty = pass.commit_tick(&retry, &HashSet::new(), 5_000);
    assert!(dirty, "a committed fire consumes latches and must persist");

    assert!(
        pass.should_skip(false),
        "after a successful commit the pass may skip no-change ticks again"
    );
    let done = pass.plan_tick(6_000, false);
    assert!(
        done.plan.is_empty(),
        "the trigger must be consumed exactly once after a successful dispatch; got {:?}",
        done.plan.unpartitioned
    );
    let dirty = pass.commit_tick(&done, &HashSet::new(), 6_000);
    assert!(
        !dirty,
        "a passive tick has nothing latch-bearing to persist"
    );
}

#[tokio::test]
async fn test_initial_evaluation_fires_once_not_per_restart() {
    // Pins initial_evaluation()'s documented semantics: it fires on the very
    // first evaluation tick (fresh eval state) and NOT again after a normal
    // restart with intact persisted state.
    use crate::condition::pass::{AssetConditionInfo, ConditionPass};
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .for_code_location(&ctx)
        .register_assets(&[make_materialized_record("a", 1_000)])
        .await
        .unwrap();

    let conditions = || {
        vec![AssetConditionInfo {
            asset_key: "a".to_string(),
            condition: ConditionNode::InitialEvaluation,
            partition_info: None,
            backfill_strategy: None,
        }]
    };

    let mut pass1 = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        ConditionEvalState {
            is_initial: true,
            ..Default::default()
        },
        conditions(),
        HashMap::new(),
    );
    pass1.refresh_cache(&storage, 2_000).await.unwrap();
    let out = pass1.run(2_000, false);
    assert!(
        out.plan.unpartitioned.contains(&"a".to_string()),
        "the very first evaluation must fire initial_evaluation()"
    );
    storage
        .for_code_location(&ctx)
        .set_condition_eval_state(&pass1.eval_state)
        .await
        .unwrap();

    // Normal restart: intact persisted state, same condition tree.
    let mut eval_state = storage
        .for_code_location(&ctx)
        .get_condition_eval_state()
        .await
        .unwrap()
        .expect("persisted");
    eval_state.migrate_loaded();
    let mut pass2 = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        eval_state,
        conditions(),
        HashMap::new(),
    );
    pass2.refresh_cache(&storage, 3_000).await.unwrap();
    let out2 = pass2.run(3_000, false);
    assert!(
        out2.plan.unpartitioned.is_empty(),
        "a restart with intact persisted state must not re-fire initial_evaluation(); got {:?}",
        out2.plan.unpartitioned
    );
}

#[tokio::test]
async fn test_initial_load_derives_failure_floor_from_run_history() {
    // First-ever daemon start (no persisted eval state to rehydrate from):
    // an asset whose most recent run failed must still be visible to
    // ExecutionFailed — the floor has to come from run history, not only
    // from persisted eval-state. A failure outranked by a newer
    // materialization must NOT floor.
    use crate::condition::pass::{AssetConditionInfo, ConditionPass};
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .for_code_location(&ctx)
        .register_assets(&[make_record("a"), make_materialized_record("b", 5_000)])
        .await
        .unwrap();

    let mk_run = |id: &str, asset: &str, start: i64| RunRecord {
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
    // a: failed, never materialized afterwards → floor stands.
    storage
        .create_run(&mk_run("run-fail-a", "a", 2_000))
        .await
        .unwrap();
    storage
        .update_run_status("run-fail-a", RunStatus::Failure, Some(2_500))
        .await
        .unwrap();
    // b: failed at 2_500 but re-materialized at 5_000 → floor cleared.
    storage
        .create_run(&mk_run("run-fail-b", "b", 2_000))
        .await
        .unwrap();
    storage
        .update_run_status("run-fail-b", RunStatus::Failure, Some(2_500))
        .await
        .unwrap();

    let conditions = vec![
        AssetConditionInfo {
            asset_key: "a".to_string(),
            condition: ConditionNode::ExecutionFailed,
            partition_info: None,
            backfill_strategy: None,
        },
        AssetConditionInfo {
            asset_key: "b".to_string(),
            condition: ConditionNode::ExecutionFailed,
            partition_info: None,
            backfill_strategy: None,
        },
    ];
    // b also carries a stale PERSISTED floor (from an eval-state snapshot
    // taken before it recovered while the daemon was down) — the load must
    // drop it, not trust it.
    let mut eval_state = ConditionEvalState {
        is_initial: true,
        ..Default::default()
    };
    eval_state.failed_assets.insert("b".to_string(), 2_500);
    let mut pass = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        eval_state,
        conditions,
        HashMap::new(),
    );
    pass.refresh_cache(&storage, 10_000).await.unwrap();
    let out = pass.run(10_000, false);
    assert!(
        out.plan.unpartitioned.contains(&"a".to_string()),
        "a pre-existing failure must fire ExecutionFailed on first start; got {:?}",
        out.plan.unpartitioned
    );
    assert!(
        !out.plan.unpartitioned.contains(&"b".to_string()),
        "a failure outranked by a newer materialization must not fire, even when \
         a stale persisted floor rehydrated it"
    );
}

#[tokio::test]
async fn test_initial_load_ignores_failed_action_runs() {
    // A failed `compact` is not a failed materialization attempt. The steady
    // state already knows that (apply_run_effects_to_delta); after a daemon
    // restart initial_load rebuilds the floor from run history and must reach
    // the same answer, or a failed action latches ExecutionFailed forever.
    use crate::condition::pass::{AssetConditionInfo, ConditionPass};
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .for_code_location(&ctx)
        .register_assets(&[make_record("a")])
        .await
        .unwrap();

    storage
        .create_run(&RunRecord {
            run_id: "run-compact".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: None,
            status: RunStatus::Started,
            start_time: 2_000,
            end_time: None,
            tags: vec![],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: Some("compact".to_string()),
            config: None,
        })
        .await
        .unwrap();
    storage
        .update_run_status("run-compact", RunStatus::Failure, Some(2_500))
        .await
        .unwrap();

    let mut pass = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        ConditionEvalState {
            is_initial: true,
            ..Default::default()
        },
        vec![AssetConditionInfo {
            asset_key: "a".to_string(),
            condition: ConditionNode::ExecutionFailed,
            partition_info: None,
            backfill_strategy: None,
        }],
        HashMap::new(),
    );
    pass.refresh_cache(&storage, 10_000).await.unwrap();
    let out = pass.run(10_000, false);
    assert!(
        !out.plan.unpartitioned.contains(&"a".to_string()),
        "a failed action run must not raise the materialization failure floor"
    );
}

#[tokio::test]
async fn test_materializing_action_triggers_downstream_eager() {
    // events → rollup, both built by the joint run r1. A `refresh` verb whose
    // `materialized()` rewrote events is new data rollup has not seen, like a
    // materialize of events alone: eager() must request rollup, in steady
    // state and after a restart.
    use crate::assets::graph::{NodeKind, TopologyNode};
    use crate::condition::pass::{AssetConditionInfo, ConditionPass};
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{EventRecord, EventType};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .for_code_location(&ctx)
        .register_assets(&[make_record("events"), make_record("rollup")])
        .await
        .unwrap();
    let node = |name: &str| TopologyNode {
        name: name.into(),
        kind: NodeKind::Asset,
        group: None,
        parent_graph: None,
    };
    storage
        .kv_set(
            &crate::graph_topology_key(DEFAULT_CODE_LOCATION_ID),
            &serde_json::to_vec(&GraphTopology {
                nodes: vec![node("events"), node("rollup")],
                edges: vec![("rollup".to_string(), "events".to_string())],
            })
            .unwrap(),
        )
        .await
        .unwrap();
    let run = |run_id: &str, assets: &[&str], ts: i64, action: Option<&str>| RunRecord {
        run_id: run_id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: None,
        status: RunStatus::Success,
        start_time: ts,
        end_time: Some(ts),
        tags: vec![],
        node_names: assets.iter().map(|a| a.to_string()).collect(),
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: action.map(str::to_string),
        config: None,
    };
    let materialization = |run_id: &str, asset: &str, ts: i64| EventRecord {
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type: EventType::Materialization {
            data_version: Some(format!("dv_{run_id}")),
        },
        asset_key: Some(asset.to_string()),
        run_id: run_id.to_string(),
        partition_key: None,
        timestamp: ts,
        metadata: vec![],
        input_data_versions: vec![],
    };
    storage
        .create_run(&run("r1", &["events", "rollup"], 1_000, None))
        .await
        .unwrap();
    storage
        .store_events(&[
            materialization("r1", "events", 1_000),
            materialization("r1", "rollup", 1_000),
        ])
        .await
        .unwrap();

    let eager_rollup = || {
        ConditionPass::new(
            AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
            ConditionEvalState::default(),
            vec![AssetConditionInfo {
                asset_key: "rollup".to_string(),
                condition: ConditionNode::eager(),
                partition_info: None,
                backfill_strategy: None,
            }],
            HashMap::new(),
        )
    };
    let mut pass = eager_rollup();
    pass.refresh_cache(&storage, 1_500).await.unwrap();
    assert!(
        pass.run(1_500, false).plan.unpartitioned.is_empty(),
        "precondition: r1 built rollup together with events"
    );

    storage
        .create_run(&run("r2", &["events"], 2_000, Some("refresh")))
        .await
        .unwrap();
    storage
        .store_events(&[materialization("r2", "events", 2_000)])
        .await
        .unwrap();
    pass.refresh_cache(&storage, 2_500).await.unwrap();
    assert_eq!(
        pass.run(2_500, false).plan.unpartitioned,
        ["rollup"],
        "the verb's materialized() must trigger eager() on rollup"
    );

    let mut restarted = eager_rollup();
    restarted.refresh_cache(&storage, 3_000).await.unwrap();
    assert_eq!(
        restarted.run(3_000, false).plan.unpartitioned,
        ["rollup"],
        "a restarted daemon must reach the same answer"
    );
}

#[tokio::test]
async fn test_verb_run_requests_downstream_it_did_not_build_after_dep() {
    // events → daily_summary, both built by r1. `refresh` over both runs its
    // targets one at a time in name order, so daily_summary's step runs first.
    // A daily_summary that materialized before events, returned unchanged()
    // or failed has not seen the new events: eager() must request it. One
    // that materialized after events needs no second build. Steady state and
    // a restart agree.
    use crate::assets::graph::{NodeKind, TopologyNode};
    use crate::condition::pass::{AssetConditionInfo, ConditionPass};
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{EventRecord, EventType};

    let node = |name: &str| TopologyNode {
        name: name.into(),
        kind: NodeKind::Asset,
        group: None,
        parent_graph: None,
    };
    let event = |run_id: &str, asset: &str, ts: i64, materialized: bool| EventRecord {
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type: if materialized {
            EventType::Materialization {
                data_version: Some(format!("dv_{run_id}")),
            }
        } else {
            EventType::ActionCompleted
        },
        asset_key: Some(asset.to_string()),
        run_id: run_id.to_string(),
        partition_key: None,
        timestamp: ts,
        metadata: vec![],
        input_data_versions: vec![],
    };
    let eager_summary = || {
        ConditionPass::new(
            AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
            ConditionEvalState::default(),
            vec![AssetConditionInfo {
                asset_key: "daily_summary".to_string(),
                condition: ConditionNode::eager(),
                partition_info: None,
                backfill_strategy: None,
            }],
            HashMap::new(),
        )
    };

    for (case, summary, status, requested) in [
        (
            "built before events",
            Some((2_000, true)),
            RunStatus::Success,
            &["daily_summary"][..],
        ),
        (
            "unchanged",
            Some((2_000, false)),
            RunStatus::Success,
            &["daily_summary"],
        ),
        ("failed", None, RunStatus::Failure, &["daily_summary"]),
        (
            "built after events",
            Some((2_200, true)),
            RunStatus::Success,
            &[],
        ),
    ] {
        let storage = SurrealStorage::new_memory().await.unwrap();
        let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
        storage
            .for_code_location(&ctx)
            .register_assets(&[make_record("events"), make_record("daily_summary")])
            .await
            .unwrap();
        storage
            .kv_set(
                &crate::graph_topology_key(DEFAULT_CODE_LOCATION_ID),
                &serde_json::to_vec(&GraphTopology {
                    nodes: vec![node("events"), node("daily_summary")],
                    edges: vec![("daily_summary".to_string(), "events".to_string())],
                })
                .unwrap(),
            )
            .await
            .unwrap();
        let run = |run_id: &str, status: RunStatus, start: i64, end: i64, action: Option<&str>| {
            RunRecord {
                run_id: run_id.to_string(),
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                job_name: None,
                status,
                start_time: start,
                end_time: Some(end),
                tags: vec![],
                node_names: vec!["daily_summary".to_string(), "events".to_string()],
                priority: 0,
                partition_key: None,
                block_reason: None,
                launched_by: LaunchedBy::Manual { user: None },
                action: action.map(str::to_string),
                config: None,
            }
        };
        storage
            .create_run(&run("r1", RunStatus::Success, 1_000, 1_000, None))
            .await
            .unwrap();
        storage
            .store_events(&[
                event("r1", "events", 1_000, true),
                event("r1", "daily_summary", 1_000, true),
            ])
            .await
            .unwrap();

        let mut pass = eager_summary();
        pass.refresh_cache(&storage, 1_500).await.unwrap();
        assert!(
            pass.run(1_500, false).plan.unpartitioned.is_empty(),
            "{case}: precondition: r1 built daily_summary together with events"
        );

        storage
            .create_run(&run("r2", status, 1_900, 2_300, Some("refresh")))
            .await
            .unwrap();
        let mut events = vec![event("r2", "events", 2_100, true)];
        events.extend(
            summary.map(|(ts, materialized)| event("r2", "daily_summary", ts, materialized)),
        );
        storage.store_events(&events).await.unwrap();
        pass.refresh_cache(&storage, 2_500).await.unwrap();
        assert_eq!(
            pass.run(2_500, false).plan.unpartitioned,
            requested,
            "{case}"
        );

        let mut restarted = eager_summary();
        restarted.refresh_cache(&storage, 3_000).await.unwrap();
        assert_eq!(
            restarted.run(3_000, false).plan.unpartitioned,
            requested,
            "{case}: a restarted daemon must reach the same answer"
        );
    }
}

/// Storage + cache fixture for the live-action-run tests: one asset `a` and a
/// `Started` run carrying `action`, created before the caller's first refresh.
async fn storage_with_live_action_run(
    action: Option<&str>,
) -> crate::storage::surrealdb_backend::SurrealStorage {
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .for_code_location(&ctx)
        .register_assets(&[make_record("a")])
        .await
        .unwrap();
    storage
        .create_run(&crate::storage::RunRecord {
            run_id: "run-compact".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: None,
            status: crate::storage::RunStatus::Started,
            start_time: 2_000,
            end_time: None,
            tags: vec![],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: action.map(str::to_string),
            config: None,
        })
        .await
        .unwrap();
    storage
}

fn in_progress_pass(cache: AssetConditionCache) -> crate::condition::pass::ConditionPass {
    use crate::condition::pass::{AssetConditionInfo, ConditionPass};
    ConditionPass::new(
        cache,
        ConditionEvalState::default(),
        vec![AssetConditionInfo {
            asset_key: "a".to_string(),
            condition: ConditionNode::InProgress,
            partition_info: None,
            backfill_strategy: None,
        }],
        HashMap::new(),
    )
}

#[tokio::test]
async fn test_initial_load_ignores_live_action_runs() {
    // An in-flight `compact` is not an in-flight *materialization*. If
    // initial_load tracks it, `eager()`'s `!in_flight()` — and every dependent's
    // `!any_deps_in_progress()` — stays suppressed until the action finishes.
    // Twin of test_initial_load_ignores_failed_action_runs, live side.
    use crate::storage::DEFAULT_CODE_LOCATION_ID;

    let storage = storage_with_live_action_run(Some("compact")).await;
    let mut pass = in_progress_pass(AssetConditionCache::new(
        DEFAULT_CODE_LOCATION_ID.to_string(),
    ));
    pass.refresh_cache(&storage, 10_000).await.unwrap();
    let out = pass.run(10_000, false);
    assert!(
        !out.plan.unpartitioned.contains(&"a".to_string()),
        "a live action run must not read as an in-flight materialization"
    );
}

#[tokio::test]
async fn test_initial_load_still_tracks_live_materialize_runs() {
    // Falsifies the guard above: the same fixture without a verb must still be
    // tracked, or the fix would simply disable in-flight tracking.
    use crate::storage::DEFAULT_CODE_LOCATION_ID;

    let storage = storage_with_live_action_run(None).await;
    let mut pass = in_progress_pass(AssetConditionCache::new(
        DEFAULT_CODE_LOCATION_ID.to_string(),
    ));
    pass.refresh_cache(&storage, 10_000).await.unwrap();
    let out = pass.run(10_000, false);
    assert!(
        out.plan.unpartitioned.contains(&"a".to_string()),
        "a live materialize run must still read as in-flight"
    );
}

#[tokio::test]
async fn test_steady_state_refresh_ignores_live_action_runs() {
    // Steady-state twin: the new-runs path in fetch_refresh_delta pushes
    // InProgressChange::Push for every non-terminal run and must apply the same
    // guard initial_load does, or the answer flips depending on whether the
    // action started before or after the daemon did.
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{DEFAULT_CODE_LOCATION_ID, StorageBackend};

    let storage = SurrealStorage::new_memory().await.unwrap();
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .for_code_location(&ctx)
        .register_assets(&[make_record("a")])
        .await
        .unwrap();

    let mut pass = in_progress_pass(AssetConditionCache::new(
        DEFAULT_CODE_LOCATION_ID.to_string(),
    ));
    // First refresh initializes the cache with no runs at all.
    pass.refresh_cache(&storage, 1_000).await.unwrap();

    storage
        .create_run(&crate::storage::RunRecord {
            run_id: "run-compact".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: None,
            status: crate::storage::RunStatus::Started,
            start_time: 2_000,
            end_time: None,
            tags: vec![],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Manual { user: None },
            action: Some("compact".to_string()),
            config: None,
        })
        .await
        .unwrap();

    // Second refresh takes the steady-state delta path.
    pass.refresh_cache(&storage, 10_000).await.unwrap();
    let out = pass.run(10_000, false);
    assert!(
        !out.plan.unpartitioned.contains(&"a".to_string()),
        "a live action run must not read as in-flight on the steady-state path"
    );
}

#[tokio::test]
async fn test_recover_pending_dispatch_clears_is_initial() {
    // V-06: a first-tick crash restarts with a fresh state (is_initial=true) plus
    // a persisted intent. Recovery must clear the global is_initial, or the next
    // tick re-fires InitialEvaluation for every recovered asset (double-dispatch).
    use crate::condition::pass::recover_pending_dispatch;
    use crate::condition::state::{PendingDispatch, PendingDispatchEntry};
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, ScopedStorageHandle, StorageBackend,
    };

    let storage = std::sync::Arc::new(SurrealStorage::new_memory().await.unwrap());
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    // The run the intent references, so recovery sees dispatch evidence.
    storage
        .create_run(&RunRecord {
            run_id: "run-1".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("t".to_string()),
            status: RunStatus::Started,
            start_time: 100,
            end_time: None,
            tags: vec![],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Condition,
            action: None,
            config: None,
        })
        .await
        .unwrap();
    let pending = PendingDispatch {
        tick_timestamp: 100,
        entries: vec![PendingDispatchEntry {
            asset_key: "a".to_string(),
            run_ids: vec!["run-1".to_string()],
            committed: AssetConditionState::default(),
            dispatched_keys: vec![],
            backfill_ids: vec![],
        }],
    };
    storage
        .for_code_location(&ctx)
        .set_condition_pending_dispatch(&pending)
        .await
        .unwrap();

    // First-tick crash: restart loads None → fresh state with is_initial=true.
    let mut eval_state = ConditionEvalState {
        is_initial: true,
        ..Default::default()
    };
    let handle = ScopedStorageHandle::new(std::sync::Arc::clone(&storage), ctx.clone());
    recover_pending_dispatch(&mut eval_state, &handle)
        .await
        .unwrap();

    assert!(
        !eval_state.is_initial,
        "an existing dispatch intent proves a tick ran; is_initial must be cleared"
    );
}

#[tokio::test]
async fn test_recover_pending_dispatch_skips_stale_intent() {
    // V-10: a failed intent-clear can survive an idle stretch while passive
    // commits advance and persist newer self-state. On a later crash, recovery
    // must NOT splice the stale intent's older committed state (which would
    // regress last_materialized_timestamp and spuriously re-fire).
    use crate::condition::pass::recover_pending_dispatch;
    use crate::condition::state::{PendingDispatch, PendingDispatchEntry};
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, ScopedStorageHandle, StorageBackend,
    };

    let storage = std::sync::Arc::new(SurrealStorage::new_memory().await.unwrap());
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .create_run(&RunRecord {
            run_id: "run-old".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("t".to_string()),
            status: RunStatus::Started,
            start_time: 100,
            end_time: None,
            tags: vec![],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: None,
            block_reason: None,
            launched_by: LaunchedBy::Condition,
            action: None,
            config: None,
        })
        .await
        .unwrap();

    // A STALE intent from tick 1_000 that was never cleared.
    let mut committed = AssetConditionState::default();
    committed.last_materialized_timestamp = Some(500);
    committed.last_tick_timestamp = Some(1_000);
    let pending = PendingDispatch {
        tick_timestamp: 1_000,
        entries: vec![PendingDispatchEntry {
            asset_key: "a".to_string(),
            run_ids: vec!["run-old".to_string()],
            committed,
            dispatched_keys: vec![],
            backfill_ids: vec![],
        }],
    };
    storage
        .for_code_location(&ctx)
        .set_condition_pending_dispatch(&pending)
        .await
        .unwrap();

    // The loaded state has since been passively advanced well past tick 1_000.
    let mut advanced = AssetConditionState::default();
    advanced.last_materialized_timestamp = Some(9_999);
    advanced.last_tick_timestamp = Some(5_000);
    let mut eval_state = ConditionEvalState::default();
    eval_state.assets.insert("a".to_string(), advanced);

    let handle = ScopedStorageHandle::new(std::sync::Arc::clone(&storage), ctx.clone());
    recover_pending_dispatch(&mut eval_state, &handle)
        .await
        .unwrap();

    assert_eq!(
        eval_state
            .assets
            .get("a")
            .and_then(|s| s.last_materialized_timestamp),
        Some(9_999),
        "a stale intent must not regress the passively-advanced self-state"
    );
}

#[tokio::test]
async fn test_recover_pending_dispatch_restores_handled_keys() {
    // V-08: recovery must merge the tick's dispatched partition keys into the
    // asset's handled set, or a partitioned SinceLastHandled latch (which keeps
    // no previous_selections) re-dispatches them after a crash.
    use crate::condition::partition::PartitionState;
    use crate::condition::pass::recover_pending_dispatch;
    use crate::condition::state::{PendingDispatch, PendingDispatchEntry};
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, ScopedStorageHandle, StorageBackend,
    };

    let storage = std::sync::Arc::new(SurrealStorage::new_memory().await.unwrap());
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .create_run(&RunRecord {
            run_id: "run-k".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("t".to_string()),
            status: RunStatus::Started,
            start_time: 100,
            end_time: None,
            tags: vec![],
            node_names: vec!["a".to_string()],
            priority: 0,
            partition_key: Some(spk("k")),
            block_reason: None,
            launched_by: LaunchedBy::Condition,
            action: None,
            config: None,
        })
        .await
        .unwrap();

    let pending = PendingDispatch {
        tick_timestamp: 1_000,
        entries: vec![PendingDispatchEntry {
            asset_key: "a".to_string(),
            run_ids: vec!["run-k".to_string()],
            committed: AssetConditionState::default(),
            dispatched_keys: vec![spk("k")],
            backfill_ids: vec![],
        }],
    };
    storage
        .for_code_location(&ctx)
        .set_condition_pending_dispatch(&pending)
        .await
        .unwrap();

    // Loaded (pre-crash) state: a partition_state whose handled set lacks k.
    let mut eval_state = ConditionEvalState::default();
    eval_state.assets.insert(
        "a".to_string(),
        AssetConditionState {
            partition_state: Some(PartitionState::default()),
            ..Default::default()
        },
    );

    let handle = ScopedStorageHandle::new(std::sync::Arc::clone(&storage), ctx.clone());
    recover_pending_dispatch(&mut eval_state, &handle)
        .await
        .unwrap();

    assert!(
        eval_state
            .assets
            .get("a")
            .and_then(|s| s.partition_state.as_ref())
            .is_some_and(|ps| ps.handled.contains(&spk("k"))),
        "recovery must restore the dispatched key into the handled set"
    );
}

#[tokio::test]
async fn test_recover_pending_dispatch_backfill_id_no_false_match() {
    // V-09: a backfill-shaped intent must match the tick's OWN pre-minted
    // backfill id, not any backfill covering the asset — an unrelated concurrent
    // backfill in the crash window must NOT count as dispatch evidence and
    // consume the trigger.
    use crate::condition::pass::recover_pending_dispatch;
    use crate::condition::state::{PendingDispatch, PendingDispatchEntry};
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        BackfillFailurePolicy, BackfillRecord, BackfillStatus, BackfillStrategy,
        DEFAULT_CODE_LOCATION_ID, ScopedStorageHandle, StorageBackend,
    };

    let storage = std::sync::Arc::new(SurrealStorage::new_memory().await.unwrap());
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    // An UNRELATED backfill covering "a", created within the crash window; the
    // tick's own backfill ("bf-real") never dispatched.
    storage
        .create_backfill(&BackfillRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            backfill_id: "bf-user".to_string(),
            status: BackfillStatus::Requested,
            strategy: BackfillStrategy::MultiRun,
            failure_policy: BackfillFailurePolicy::Continue,
            asset_selection: vec!["a".to_string()],
            job_name: None,
            partition_keys: vec![spk("k1")],
            run_ids: vec![],
            completed_partitions: vec![],
            failed_partitions: vec![],
            canceled_partitions: vec![],
            max_concurrency: 1,
            tags: vec![],
            create_time: 1_500,
            end_time: None,
            error: None,
            launched_by: LaunchedBy::default(),
            action: None,
            config: None,
        })
        .await
        .unwrap();

    let mut committed = AssetConditionState::default();
    committed.last_materialized_timestamp = Some(999);
    let pending = PendingDispatch {
        tick_timestamp: 1_000,
        entries: vec![PendingDispatchEntry {
            asset_key: "a".to_string(),
            run_ids: vec![],
            backfill_ids: vec!["bf-real".to_string()],
            committed,
            dispatched_keys: vec![],
        }],
    };
    storage
        .for_code_location(&ctx)
        .set_condition_pending_dispatch(&pending)
        .await
        .unwrap();

    let mut pre = AssetConditionState::default();
    pre.last_materialized_timestamp = Some(500);
    pre.last_tick_timestamp = Some(50);
    let mut eval_state = ConditionEvalState::default();
    eval_state.assets.insert("a".to_string(), pre);

    let handle = ScopedStorageHandle::new(std::sync::Arc::clone(&storage), ctx.clone());
    recover_pending_dispatch(&mut eval_state, &handle)
        .await
        .unwrap();

    assert_eq!(
        eval_state
            .assets
            .get("a")
            .and_then(|s| s.last_materialized_timestamp),
        Some(500),
        "an unrelated backfill must not be treated as the tick's dispatch evidence"
    );
}

#[tokio::test]
async fn test_crash_after_dispatch_recovers_latches_from_intent() {
    // Crash window: a tick's run was durably dispatched (and even completed),
    // but the daemon died before persisting eval state. The pre-dispatch
    // intent must replay the consumed latches on restart so the tick's
    // trigger doesn't re-fire and double-materialize.
    use crate::condition::pass::{AssetConditionInfo, ConditionPass, recover_pending_dispatch};
    use crate::condition::state::PendingDispatch;
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, ScopedStorageHandle, StorageBackend,
    };

    let storage = std::sync::Arc::new(SurrealStorage::new_memory().await.unwrap());
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .for_code_location(&ctx)
        .register_assets(&[
            make_materialized_record("raw", 1_000),
            make_materialized_record("dst", 1_000),
        ])
        .await
        .unwrap();
    use crate::assets::graph::TopologyNode;
    let topo = GraphTopology {
        nodes: vec![
            TopologyNode {
                name: "raw".into(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            },
            TopologyNode {
                name: "dst".into(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            },
        ],
        edges: vec![("dst".to_string(), "raw".to_string())],
    };
    storage
        .kv_set(
            &crate::graph_topology_key(DEFAULT_CODE_LOCATION_ID),
            &serde_json::to_vec(&topo).unwrap(),
        )
        .await
        .unwrap();

    let conditions = || {
        vec![AssetConditionInfo {
            asset_key: "dst".to_string(),
            condition: ConditionNode::any_deps_match(ConditionNode::DataVersionChanged),
            partition_info: None,
            backfill_strategy: None,
        }]
    };
    let mk_run = |id: &str, asset: &str, start: i64| RunRecord {
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
        launched_by: LaunchedBy::Condition,
        action: None,
        config: None,
    };
    let mk_event = |run_id: &str, asset: &str, dv: &str, ts: i64| crate::storage::EventRecord {
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        event_type: crate::storage::EventType::Materialization {
            data_version: Some(dv.to_string()),
        },
        asset_key: Some(asset.to_string()),
        run_id: run_id.to_string(),
        partition_key: None,
        timestamp: ts,
        metadata: vec![],
        input_data_versions: vec![],
    };

    let mut pass1 = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        ConditionEvalState {
            is_initial: true,
            ..Default::default()
        },
        conditions(),
        HashMap::new(),
    );
    pass1.refresh_cache(storage.as_ref(), 2_000).await.unwrap();
    let out0 = pass1.run(2_000, false);
    assert!(out0.plan.is_empty(), "initial tick must not fire");
    // The pre-fire eval state is what a restart will load.
    storage
        .for_code_location(&ctx)
        .set_condition_eval_state(&pass1.eval_state)
        .await
        .unwrap();

    // raw's data version changes → dst fires.
    storage
        .create_run(&mk_run("run-raw", "raw", 2_500))
        .await
        .unwrap();
    storage
        .update_run_status("run-raw", RunStatus::Success, Some(3_000))
        .await
        .unwrap();
    storage
        .store_events(&[mk_event("run-raw", "raw", "v2", 3_000)])
        .await
        .unwrap();
    pass1.refresh_cache(storage.as_ref(), 4_000).await.unwrap();
    let out = pass1.plan_tick(4_000, false);
    assert!(
        out.plan.unpartitioned.contains(&"dst".to_string()),
        "precondition: the dep dv change must fire dst"
    );

    // The engine persists the intent, then dispatch goes out durably and the
    // run even completes, materializing dst…
    let pending = PendingDispatch {
        tick_timestamp: 4_000,
        entries: pass1
            .pending_dispatch_states(&out, 4_000)
            .into_iter()
            .map(|(asset_key, committed, dispatched_keys)| {
                crate::condition::state::PendingDispatchEntry {
                    asset_key,
                    run_ids: vec!["run-dst".to_string()],
                    committed,
                    dispatched_keys,
                    backfill_ids: vec![],
                }
            })
            .collect(),
    };
    storage
        .for_code_location(&ctx)
        .set_condition_pending_dispatch(&pending)
        .await
        .unwrap();
    storage
        .create_run(&mk_run("run-dst", "dst", 4_500))
        .await
        .unwrap();
    storage
        .update_run_status("run-dst", RunStatus::Success, Some(5_000))
        .await
        .unwrap();
    storage
        .store_events(&[mk_event("run-dst", "dst", "dst-v2", 5_000)])
        .await
        .unwrap();
    // …and the daemon dies before set_condition_eval_state. pass1 is gone.

    // Restart: load the STALE eval state, recover from the intent.
    let mut eval_state2 = storage
        .for_code_location(&ctx)
        .get_condition_eval_state()
        .await
        .unwrap()
        .expect("pre-fire state was persisted");
    eval_state2.migrate_loaded();
    let handle = ScopedStorageHandle::new(std::sync::Arc::clone(&storage), ctx.clone());
    recover_pending_dispatch(&mut eval_state2, &handle)
        .await
        .unwrap();

    let mut pass2 = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        eval_state2,
        conditions(),
        HashMap::new(),
    );
    pass2.refresh_cache(storage.as_ref(), 6_000).await.unwrap();
    let out2 = pass2.plan_tick(6_000, false);
    assert!(
        out2.plan.unpartitioned.is_empty(),
        "recovered latches must suppress the replayed fire; got {:?}",
        out2.plan.unpartitioned
    );

    let cleared = storage
        .for_code_location(&ctx)
        .get_condition_pending_dispatch()
        .await
        .unwrap()
        .unwrap_or_default();
    assert!(
        cleared.entries.is_empty(),
        "the intent must be cleared after recovery"
    );
}

#[tokio::test]
async fn test_crash_before_dispatch_leaves_trigger_armed() {
    // The symmetric case: the intent was written but the run never reached
    // storage (crash before dispatch). Recovery must NOT consume the latches
    // — the next tick re-fires as the retry.
    use crate::condition::pass::{AssetConditionInfo, ConditionPass, recover_pending_dispatch};
    use crate::condition::state::{PendingDispatch, PendingDispatchEntry};
    use crate::storage::surrealdb_backend::SurrealStorage;
    use crate::storage::{
        DEFAULT_CODE_LOCATION_ID, RunRecord, RunStatus, ScopedStorageHandle, StorageBackend,
    };

    let storage = std::sync::Arc::new(SurrealStorage::new_memory().await.unwrap());
    let ctx = crate::storage::CodeLocationContext::new(DEFAULT_CODE_LOCATION_ID);
    storage
        .for_code_location(&ctx)
        .register_assets(&[
            make_materialized_record("raw", 1_000),
            make_materialized_record("dst", 1_000),
        ])
        .await
        .unwrap();
    use crate::assets::graph::TopologyNode;
    let topo = GraphTopology {
        nodes: vec![
            TopologyNode {
                name: "raw".into(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            },
            TopologyNode {
                name: "dst".into(),
                kind: crate::assets::graph::NodeKind::Asset,
                group: None,
                parent_graph: None,
            },
        ],
        edges: vec![("dst".to_string(), "raw".to_string())],
    };
    storage
        .kv_set(
            &crate::graph_topology_key(DEFAULT_CODE_LOCATION_ID),
            &serde_json::to_vec(&topo).unwrap(),
        )
        .await
        .unwrap();

    let conditions = || {
        vec![AssetConditionInfo {
            asset_key: "dst".to_string(),
            condition: ConditionNode::any_deps_match(ConditionNode::DataVersionChanged),
            partition_info: None,
            backfill_strategy: None,
        }]
    };

    let mut pass1 = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        ConditionEvalState {
            is_initial: true,
            ..Default::default()
        },
        conditions(),
        HashMap::new(),
    );
    pass1.refresh_cache(storage.as_ref(), 2_000).await.unwrap();
    pass1.run(2_000, false);
    storage
        .for_code_location(&ctx)
        .set_condition_eval_state(&pass1.eval_state)
        .await
        .unwrap();

    storage
        .create_run(&RunRecord {
            run_id: "run-raw".to_string(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Started,
            start_time: 2_500,
            end_time: None,
            tags: vec![],
            node_names: vec!["raw".to_string()],
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
        .update_run_status("run-raw", RunStatus::Success, Some(3_000))
        .await
        .unwrap();
    storage
        .store_events(&[crate::storage::EventRecord {
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            event_type: crate::storage::EventType::Materialization {
                data_version: Some("v2".to_string()),
            },
            asset_key: Some("raw".to_string()),
            run_id: "run-raw".to_string(),
            partition_key: None,
            timestamp: 3_000,
            metadata: vec![],
            input_data_versions: vec![],
        }])
        .await
        .unwrap();
    pass1.refresh_cache(storage.as_ref(), 4_000).await.unwrap();
    let out = pass1.plan_tick(4_000, false);
    assert!(out.plan.unpartitioned.contains(&"dst".to_string()));

    // Intent written; the run never reached storage (crash before dispatch).
    let pending = PendingDispatch {
        tick_timestamp: 4_000,
        entries: pass1
            .pending_dispatch_states(&out, 4_000)
            .into_iter()
            .map(
                |(asset_key, committed, dispatched_keys)| PendingDispatchEntry {
                    asset_key,
                    run_ids: vec!["run-never-created".to_string()],
                    committed,
                    dispatched_keys,
                    backfill_ids: vec![],
                },
            )
            .collect(),
    };
    storage
        .for_code_location(&ctx)
        .set_condition_pending_dispatch(&pending)
        .await
        .unwrap();

    let mut eval_state2 = storage
        .for_code_location(&ctx)
        .get_condition_eval_state()
        .await
        .unwrap()
        .expect("pre-fire state was persisted");
    eval_state2.migrate_loaded();
    let handle = ScopedStorageHandle::new(std::sync::Arc::clone(&storage), ctx.clone());
    recover_pending_dispatch(&mut eval_state2, &handle)
        .await
        .unwrap();

    let mut pass2 = ConditionPass::new(
        AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string()),
        eval_state2,
        conditions(),
        HashMap::new(),
    );
    pass2.refresh_cache(storage.as_ref(), 6_000).await.unwrap();
    let out2 = pass2.plan_tick(6_000, false);
    assert!(
        out2.plan.unpartitioned.contains(&"dst".to_string()),
        "a dispatch that never happened must stay armed and re-fire"
    );
}

/// Helper: memory-backed storage with asset `a` registered and an initialized cache.
async fn pending_test_setup() -> (
    crate::storage::surrealdb_backend::SurrealStorage,
    AssetConditionCache,
) {
    use crate::storage::surrealdb_backend::SurrealStorage;
    let storage = SurrealStorage::new_memory().await.unwrap();
    storage
        .for_code_location(&crate::storage::CodeLocationContext::new(
            DEFAULT_CODE_LOCATION_ID,
        ))
        .register_assets(&[make_materialized_record("a", 1000)])
        .await
        .unwrap();
    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();
    (storage, cache)
}

#[tokio::test]
async fn test_register_dispatched_run_populates_pending_and_in_progress() {
    let (_storage, mut cache) = pending_test_setup().await;
    cache.register_dispatched_run("a".into(), "run-1".into(), 1_000_000, None);
    assert_eq!(
        cache.in_progress_assets.get("a"),
        Some(&HashMap::from([(
            "run-1".to_string(),
            None::<PartitionKey>
        )])),
        "in_progress_assets should hold the run_id with no partition key"
    );
    assert!(
        cache.pending_runs.contains_key("run-1"),
        "pending_runs should hold the run_id"
    );
    assert_eq!(
        cache.pending_runs.get("run-1").unwrap().asset_keys,
        vec!["a".to_string()],
        "pending entry should track the asset"
    );
}

#[tokio::test]
async fn test_pending_run_confirmed_by_storage_clears_pending() {
    let (storage, mut cache) = pending_test_setup().await;
    cache.register_dispatched_run("a".into(), "run-1".into(), 1_000_000, None);

    // Storage now reports the run as Started — phantom is no longer phantom.
    storage
        .create_run(&RunRecord {
            run_id: "run-1".to_string(),
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

    cache.refresh(&storage, 1_000_000).await.unwrap();
    assert!(
        !cache.pending_runs.contains_key("run-1"),
        "pending should be cleared once storage confirms the run"
    );
    // The Started run propagates a fresh in-progress entry, so the asset stays in-progress.
    assert!(
        cache.in_progress_assets.contains_key("a"),
        "asset should remain in-progress while the Started run is live"
    );
}

#[tokio::test]
async fn test_pending_run_evicted_after_grace() {
    let (storage, mut cache) = pending_test_setup().await;
    cache.pending_grace_nanos = 10_000; // 10 microseconds, easy to exceed
    cache.register_dispatched_run("a".into(), "phantom-run".into(), 1_000_000, None);

    // Refresh well past the grace window with NO matching run in storage.
    cache.refresh(&storage, 1_000_000 + 100_000).await.unwrap();

    assert!(
        !cache.pending_runs.contains_key("phantom-run"),
        "phantom past grace should be removed from pending_runs"
    );
    assert!(
        !cache.in_progress_assets.contains_key("a"),
        "phantom past grace should also be removed from in_progress_assets so the asset can re-fire"
    );
}

#[tokio::test]
async fn test_pending_eviction_untracks_all_assets_of_a_multi_asset_run() {
    // A phantom joint run must untrack every asset it covered, not just the last.
    let (storage, mut cache) = pending_test_setup().await;
    cache.pending_grace_nanos = 10_000;
    cache.register_dispatched_run("a".into(), "joint-run".into(), 1_000_000, None);
    cache.register_dispatched_run("b".into(), "joint-run".into(), 1_000_000, None);

    cache.refresh(&storage, 1_000_000 + 100_000).await.unwrap();

    assert!(
        !cache.pending_runs.contains_key("joint-run"),
        "phantom joint run should be removed from pending_runs"
    );
    assert!(
        !cache.in_progress_assets.contains_key("a"),
        "asset a (registered first) must be untracked on phantom eviction"
    );
    assert!(
        !cache.in_progress_assets.contains_key("b"),
        "asset b (registered second) must be untracked on phantom eviction"
    );
}

#[tokio::test]
async fn test_pending_run_not_evicted_within_grace() {
    let (storage, mut cache) = pending_test_setup().await;
    cache.pending_grace_nanos = 1_000_000_000; // 1 second
    cache.register_dispatched_run("a".into(), "pending-run".into(), 1_000_000, None);

    // Refresh well within the grace window — no eviction yet.
    cache.refresh(&storage, 1_000_000 + 500).await.unwrap();
    assert!(
        cache.pending_runs.contains_key("pending-run"),
        "pending entry within grace should still be tracked"
    );
    assert!(
        cache
            .in_progress_assets
            .get("a")
            .is_some_and(|v| v.contains_key("pending-run")),
        "in_progress entry within grace should remain"
    );
}

#[tokio::test]
async fn test_clear_dispatched_run_rolls_back_failed_dispatch() {
    // A synchronous dispatch failure never reaches storage; the mark must drop
    // immediately, not wait out the phantom-eviction grace.
    let (_storage, mut cache) = pending_test_setup().await;
    cache.register_dispatched_run("a".into(), "joint-run".into(), 1_000_000, None);
    cache.register_dispatched_run("b".into(), "joint-run".into(), 1_000_000, None);
    cache.register_dispatched_run("c".into(), "solo-run".into(), 1_000_000, None);

    cache.clear_dispatched_run("a", "joint-run");
    assert!(
        !cache.in_progress_assets.contains_key("a"),
        "cleared asset must be untracked immediately"
    );
    assert!(
        cache
            .in_progress_assets
            .get("b")
            .is_some_and(|v| v.contains_key("joint-run")),
        "other assets of the run keep their mark until cleared themselves"
    );
    assert!(
        cache
            .pending_runs
            .get("joint-run")
            .is_some_and(|p| p.asset_keys == vec!["b".to_string()]),
        "pending entry drops only the cleared asset"
    );

    cache.clear_dispatched_run("b", "joint-run");
    assert!(
        !cache.pending_runs.contains_key("joint-run"),
        "pending entry is removed once its last asset is cleared"
    );
    assert!(!cache.in_progress_assets.contains_key("b"));

    cache.clear_dispatched_run("c", "solo-run");
    assert!(!cache.in_progress_assets.contains_key("c"));
    assert!(!cache.pending_runs.contains_key("solo-run"));
}

#[tokio::test]
async fn test_pending_eviction_only_drops_phantom_run_id_not_other_runs() {
    // Two run_ids on one asset — one phantom (grace expired), one real (storage-confirmed);
    // eviction drops only the phantom.
    let (storage, mut cache) = pending_test_setup().await;
    cache.pending_grace_nanos = 10_000;
    cache.register_dispatched_run("a".into(), "phantom-run".into(), 1_000_000, None);
    cache.register_dispatched_run("a".into(), "real-run".into(), 1_000_000, None);

    // Confirm `real-run` via storage.
    storage
        .create_run(&RunRecord {
            run_id: "real-run".to_string(),
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

    cache.refresh(&storage, 1_000_000 + 100_000).await.unwrap();

    assert!(
        !cache.pending_runs.contains_key("phantom-run"),
        "phantom-run evicted from pending"
    );
    assert!(
        !cache.pending_runs.contains_key("real-run"),
        "real-run confirmed (cleared from pending)"
    );
    let in_progress = cache
        .in_progress_assets
        .get("a")
        .cloned()
        .unwrap_or_default();
    assert!(
        in_progress.contains_key("real-run"),
        "real-run survives in in_progress_assets"
    );
    assert!(
        !in_progress.contains_key("phantom-run"),
        "phantom-run evicted from in_progress_assets"
    );
}

#[tokio::test]
async fn test_pending_eviction_reports_changed_so_eval_runs() {
    // After phantom eviction, refresh must return `true` so the unblocked asset is re-evaluated.
    let (storage, mut cache) = pending_test_setup().await;
    cache.pending_grace_nanos = 10_000;
    cache.register_dispatched_run("a".into(), "phantom-run".into(), 1_000_000, None);

    let changed = cache.refresh(&storage, 1_000_000 + 100_000).await.unwrap();
    assert!(
        changed,
        "phantom eviction should flip `changed` so the eval pass runs"
    );
}

#[tokio::test]
async fn test_initial_load_does_not_floor_asset_materialized_in_failed_joint_run() {
    // A joint run R=[x,y] fails on y but x materialized (its last_run_id → R). On restart,
    // initial_load rebuilds floors from last_run_id; since last_run_id is written only by
    // materializations, an asset whose last_run_id names a failed run materialized in it and must not be floored.
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

    let run_id = "run-joint-fail".to_string();
    storage
        .create_run(&RunRecord {
            run_id: run_id.clone(),
            code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
            job_name: Some("test".to_string()),
            status: RunStatus::Failure,
            start_time: 2000,
            end_time: Some(3000),
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
    // x materialized in R (advances x.last_run_id to R); y's step failed.
    storage
        .store_events(&[
            EventRecord {
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization {
                    data_version: Some("dv-x2".to_string()),
                },
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

    // Fresh cache = daemon restart → initial_load.
    let mut cache = AssetConditionCache::new(DEFAULT_CODE_LOCATION_ID.to_string());
    cache.refresh(&storage, 0).await.unwrap();

    assert!(
        !cache.failed_assets.contains("x"),
        "x materialized in the failed joint run → must not be floored on restart; \
         got failed_assets={:?}",
        cache.failed_assets,
    );
}
