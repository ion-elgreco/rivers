use super::*;

/// Two CodeLocations writing topology under distinct identities must not overwrite each other.
#[tokio::test]
async fn graph_topology_isolated_per_code_location() {
    use crate::assets::graph::{NodeKind, TopologyNode};

    let storage = make_storage().await;
    let ctx_a = crate::storage::CodeLocationContext::new("11111111-1111-4111-8111-111111111111");
    let ctx_b = crate::storage::CodeLocationContext::new("22222222-2222-4222-8222-222222222222");

    let topo_a = GraphTopology {
        nodes: vec![TopologyNode {
            name: "a_only".to_string(),
            kind: NodeKind::Asset,
            group: None,
            parent_graph: None,
        }],
        edges: vec![("a_only".to_string(), "shared".to_string())],
    };
    let topo_b = GraphTopology {
        nodes: vec![TopologyNode {
            name: "b_only".to_string(),
            kind: NodeKind::Asset,
            group: None,
            parent_graph: None,
        }],
        edges: vec![("b_only".to_string(), "shared".to_string())],
    };

    storage
        .for_code_location(&ctx_a)
        .set_graph_topology(&topo_a)
        .await
        .unwrap();
    storage
        .for_code_location(&ctx_b)
        .set_graph_topology(&topo_b)
        .await
        .unwrap();

    let read_a = storage
        .for_code_location(&ctx_a)
        .get_graph_topology()
        .await
        .unwrap()
        .expect("CL-A topology");
    assert_eq!(read_a.nodes.len(), 1);
    assert_eq!(read_a.nodes[0].name, "a_only");

    let read_b = storage
        .for_code_location(&ctx_b)
        .get_graph_topology()
        .await
        .unwrap()
        .expect("CL-B topology");
    assert_eq!(read_b.nodes.len(), 1);
    assert_eq!(read_b.nodes[0].name, "b_only");
}

/// `condition_eval_state` is keyed per CL.
#[tokio::test]
async fn condition_eval_state_isolated_per_code_location() {
    use crate::condition::ConditionEvalState;
    let storage = make_storage().await;
    let ctx_a = crate::storage::CodeLocationContext::new("cl-a");
    let ctx_b = crate::storage::CodeLocationContext::new("cl-b");

    let state_a = ConditionEvalState {
        is_initial: false,
        ..Default::default()
    };
    let state_b = ConditionEvalState {
        is_initial: true,
        ..Default::default()
    };

    storage
        .for_code_location(&ctx_a)
        .set_condition_eval_state(&state_a)
        .await
        .unwrap();
    storage
        .for_code_location(&ctx_b)
        .set_condition_eval_state(&state_b)
        .await
        .unwrap();

    let read_a = storage
        .for_code_location(&ctx_a)
        .get_condition_eval_state()
        .await
        .unwrap()
        .expect("CL-A state");
    let read_b = storage
        .for_code_location(&ctx_b)
        .get_condition_eval_state()
        .await
        .unwrap()
        .expect("CL-B state");
    assert!(!read_a.is_initial, "CL-A keeps its own snapshot");
    assert!(read_b.is_initial, "CL-B keeps its own snapshot");
}

/// Scoped `get_runs` / `get_queued_runs` / `get_runs_since` only return runs owned by the calling CL.
#[tokio::test]
async fn scoped_run_queries_isolated_per_code_location() {
    let storage = make_storage().await;
    let now = now_nanos();

    // CL-A: 1 queued + 1 success
    for (id, status, ts) in [
        ("a-queued", RunStatus::Queued, now),
        ("a-success", RunStatus::Success, now - 1_000_000),
    ] {
        storage
            .create_run(&RunRecord {
                run_id: id.to_string(),
                code_location_id: "cl-a".to_string(),
                job_name: Some("j".into()),
                status,
                start_time: ts,
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
    // CL-B: 1 queued
    storage
        .create_run(&RunRecord {
            run_id: "b-queued".to_string(),
            code_location_id: "cl-b".to_string(),
            job_name: Some("j".into()),
            status: RunStatus::Queued,
            start_time: now,
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

    let ctx_a = crate::storage::CodeLocationContext::new("cl-a");
    let ctx_b = crate::storage::CodeLocationContext::new("cl-b");

    // get_runs (scoped)
    let runs_a: Vec<String> = storage
        .for_code_location(&ctx_a)
        .get_runs(100, None)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.run_id)
        .collect();
    assert_eq!(runs_a.len(), 2);
    assert!(runs_a.iter().all(|id| id.starts_with("a-")));

    // get_queued_runs (scoped)
    let queued_a: Vec<String> = storage
        .for_code_location(&ctx_a)
        .get_queued_runs()
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.run_id)
        .collect();
    assert_eq!(queued_a, vec!["a-queued".to_string()]);
    let queued_b: Vec<String> = storage
        .for_code_location(&ctx_b)
        .get_queued_runs()
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.run_id)
        .collect();
    assert_eq!(queued_b, vec!["b-queued".to_string()]);

    // get_runs_since (scoped) with a status filter
    let started_a = storage
        .for_code_location(&ctx_a)
        .get_runs_since(0, Some(RunStatus::Success), crate::storage::SortOrder::Desc)
        .await
        .unwrap();
    assert_eq!(started_a.len(), 1);
    assert_eq!(started_a[0].run_id, "a-success");

    // Unscoped get_all_queued_runs returns both CLs' queued runs.
    let all_queued: Vec<String> = storage
        .get_all_queued_runs()
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.run_id)
        .collect();
    assert_eq!(all_queued.len(), 2);
}

/// The per-CL `get_runs_page` only returns rows for the calling CL, while the global `get_all_runs_page` returns both.
#[tokio::test]
async fn runs_page_isolated_per_code_location() {
    let storage = make_storage().await;
    let now = now_nanos();
    for (id, cl, ts) in [
        ("a-1", "cl-a", now),
        ("a-2", "cl-a", now - 1_000_000),
        ("b-1", "cl-b", now - 500_000),
    ] {
        storage
            .create_run(&RunRecord {
                run_id: id.to_string(),
                code_location_id: cl.to_string(),
                job_name: Some("j".into()),
                status: RunStatus::Success,
                start_time: ts,
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

    let page_a = storage
        .get_runs_page("cl-a", 0, 10, &RunFilter::default())
        .await
        .unwrap();
    assert_eq!(page_a.total, 2);
    assert!(page_a.rows.iter().all(|r| r.run_id.starts_with("a-")));

    let page_b = storage
        .get_runs_page("cl-b", 0, 10, &RunFilter::default())
        .await
        .unwrap();
    assert_eq!(page_b.total, 1);
    assert_eq!(page_b.rows[0].run_id, "b-1");

    let all = storage
        .get_all_runs_page(0, 10, &RunFilter::default())
        .await
        .unwrap();
    assert_eq!(all.total, 3);
}

#[tokio::test]
async fn runs_summary_isolated_per_code_location() {
    let storage = make_storage().await;
    let now = now_nanos();
    let mk = |id: &str, cl: &str, status: RunStatus| RunRecord {
        run_id: id.to_string(),
        code_location_id: cl.to_string(),
        job_name: Some("j".into()),
        status,
        start_time: now,
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
    // CL-A: 2 success, 1 failure
    for r in [
        mk("a-1", "cl-a", RunStatus::Success),
        mk("a-2", "cl-a", RunStatus::Success),
        mk("a-3", "cl-a", RunStatus::Failure),
    ] {
        storage.create_run(&r).await.unwrap();
    }
    // CL-B: 1 queued
    storage
        .create_run(&mk("b-1", "cl-b", RunStatus::Queued))
        .await
        .unwrap();

    let cutoff = now - 86_400_000_000_000;

    let sum_a = storage.get_runs_summary("cl-a", cutoff).await.unwrap();
    assert_eq!(sum_a.total, 3);
    assert_eq!(sum_a.success, 2);
    assert_eq!(sum_a.failure, 1);
    assert_eq!(sum_a.queued, 0);

    let sum_b = storage.get_runs_summary("cl-b", cutoff).await.unwrap();
    assert_eq!(sum_b.total, 1);
    assert_eq!(sum_b.queued, 1);
    assert_eq!(sum_b.success, 0);

    let all = storage.get_all_runs_summary(cutoff).await.unwrap();
    assert_eq!(all.total, 4);
}

#[tokio::test]
async fn last_run_per_job_isolated_per_code_location() {
    let storage = make_storage().await;
    let now = now_nanos();
    for (id, cl, ts) in [
        ("a-old", "cl-a", now - 10_000_000),
        ("a-new", "cl-a", now),
        ("b-mid", "cl-b", now - 5_000_000),
    ] {
        storage
            .create_run(&RunRecord {
                run_id: id.to_string(),
                code_location_id: cl.to_string(),
                job_name: Some("shared".into()),
                status: RunStatus::Success,
                start_time: ts,
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
    let names = vec!["shared".to_string()];

    let a = storage.get_last_run_per_job("cl-a", &names).await.unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].1.run_id, "a-new");

    let b = storage.get_last_run_per_job("cl-b", &names).await.unwrap();
    assert_eq!(b.len(), 1);
    assert_eq!(b[0].1.run_id, "b-mid");

    // Global: returns whichever has the most recent start_time.
    let all = storage.get_all_last_run_per_job(&names).await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].1.run_id, "a-new");
}

#[tokio::test]
async fn backfills_page_isolated_per_code_location() {
    let storage = make_storage().await;
    for r in [
        mk_isolation_backfill("a-1", "cl-a", BackfillStatus::Requested, 100),
        mk_isolation_backfill("a-2", "cl-a", BackfillStatus::Requested, 200),
        mk_isolation_backfill("b-1", "cl-b", BackfillStatus::Requested, 300),
    ] {
        storage.create_backfill(&r).await.unwrap();
    }

    let page_a = storage
        .get_backfills_page("cl-a", 0, 10, &BackfillFilter::default())
        .await
        .unwrap();
    assert_eq!(page_a.total, 2);
    assert!(page_a.rows.iter().all(|r| r.backfill_id.starts_with("a-")));

    let page_b = storage
        .get_backfills_page("cl-b", 0, 10, &BackfillFilter::default())
        .await
        .unwrap();
    assert_eq!(page_b.total, 1);

    let all = storage
        .get_all_backfills_page(0, 10, &BackfillFilter::default())
        .await
        .unwrap();
    assert_eq!(all.total, 3);
}

#[tokio::test]
async fn backfills_summary_isolated_per_code_location() {
    let storage = make_storage().await;
    for r in [
        mk_isolation_backfill("a-prog", "cl-a", BackfillStatus::InProgress, 100),
        mk_isolation_backfill("a-done", "cl-a", BackfillStatus::CompletedSuccess, 200),
        mk_isolation_backfill("b-fail", "cl-b", BackfillStatus::CompletedFailed, 300),
    ] {
        storage.create_backfill(&r).await.unwrap();
    }

    let sum_a = storage.get_backfills_summary("cl-a").await.unwrap();
    assert_eq!(sum_a.total, 2);
    assert_eq!(sum_a.in_progress, 1);
    assert_eq!(sum_a.completed_success, 1);
    assert_eq!(sum_a.completed_failed, 0);

    let sum_b = storage.get_backfills_summary("cl-b").await.unwrap();
    assert_eq!(sum_b.total, 1);
    assert_eq!(sum_b.completed_failed, 1);

    let all = storage.get_all_backfills_summary().await.unwrap();
    assert_eq!(all.total, 3);
}
