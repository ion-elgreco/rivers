//! PostgreSQL-only tests of the pages: the order across pages, the reads
//! that no shared test covers, and the plan of each page query.

use super::backfills::push_backfills_page;
use super::events::{
    push_asset_events_page, push_count_asset_events, push_run_events_page, push_run_step_events,
};
use super::*;

async fn store() -> Option<PostgresStorage> {
    let url = PostgresStorage::test_url()?;
    Some(PostgresStorage::connect_test_schema(&url).await.unwrap())
}

fn run(run_id: &str, job_name: &str, start_time: i64) -> RunRecord {
    RunRecord {
        run_id: run_id.to_string(),
        code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
        job_name: Some(job_name.to_string()),
        status: RunStatus::Success,
        start_time,
        end_time: None,
        execution_start_time: None,
        tags: vec![],
        node_names: vec![],
        priority: 0,
        partition_key: None,
        block_reason: None,
        launched_by: LaunchedBy::default(),
        action: None,
        config: None,
    }
}

/// Insert events of run `r1`: `(id, type, asset, timestamp, sort order)`.
async fn insert_events(storage: &PostgresStorage, events: &[(&str, &str, &str, i64, i64)]) {
    let ids: Vec<&str> = events.iter().map(|e| e.0).collect();
    let types: Vec<&str> = events.iter().map(|e| e.1).collect();
    let assets: Vec<&str> = events.iter().map(|e| e.2).collect();
    let timestamps: Vec<i64> = events.iter().map(|e| e.3).collect();
    let sort_orders: Vec<i64> = events.iter().map(|e| e.4).collect();
    sqlx::query(
        "INSERT INTO events (id, event_type, asset_key, run_id, timestamp, sort_order) \
         SELECT id, event_type, asset_key, 'r1', timestamp, sort_order \
         FROM UNNEST($1::text[], $2::text[], $3::text[], $4::bigint[], $5::bigint[]) \
             AS e(id, event_type, asset_key, timestamp, sort_order)",
    )
    .bind(&ids)
    .bind(&types)
    .bind(&assets)
    .bind(&timestamps)
    .bind(&sort_orders)
    .execute(storage.pool())
    .await
    .unwrap();
}

fn ids(events: &[StoredEvent]) -> Vec<String> {
    events
        .iter()
        .map(|event| match &event.id.key {
            surrealdb::types::RecordIdKey::String(key) => key.clone(),
            other => panic!("event id {other:?}"),
        })
        .collect()
}

/// Runs that start at the same time list by run id, newest first, so the
/// pages of a list hold each run once and in the order of `get_all_runs`.
#[tokio::test]
async fn pages_of_runs_that_start_at_the_same_time_hold_each_run_once() {
    let Some(storage) = store().await else {
        return;
    };
    let runs: Vec<RunRecord> = ["b", "d", "a", "e", "c", "f", "g"]
        .iter()
        .map(|id| run(id, "j", 100))
        .chain([run("late", "j", 200)])
        .collect();
    storage.create_runs(&runs).await.unwrap();

    let mut paged = Vec::new();
    for offset in (0..9).step_by(3) {
        let page = storage
            .get_all_runs_page(offset, 3, &RunFilter::default())
            .await
            .unwrap();
        assert_eq!(page.total, 8);
        paged.extend(page.rows.into_iter().map(|r| r.run_id));
    }
    assert_eq!(paged, ["late", "g", "f", "e", "d", "c", "b", "a"]);
    let listed: Vec<String> = storage
        .get_all_runs(10, None)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.run_id)
        .collect();
    assert_eq!(paged, listed);
}

/// A name asked for twice gets its run twice, in the order of the names, as
/// one statement per name does on SurrealDB.
#[tokio::test]
async fn last_run_per_job_follows_the_order_of_the_names() {
    let Some(storage) = store().await else {
        return;
    };
    storage
        .create_runs(&[
            run("a1", "a", 100),
            run("b1", "b", 100),
            run("b2", "b", 200),
        ])
        .await
        .unwrap();

    let names = ["b", "none", "a", "b"].map(String::from);
    let found: Vec<(String, String)> = storage
        .get_all_last_run_per_job(&names)
        .await
        .unwrap()
        .into_iter()
        .map(|(name, run)| (name, run.run_id))
        .collect();
    let expected = [("b", "b2"), ("a", "a1"), ("b", "b2")];
    assert_eq!(found, expected.map(|(n, r)| (n.to_string(), r.to_string())));
}

#[tokio::test]
async fn run_event_reads_are_oldest_first_and_counted() {
    let Some(storage) = store().await else {
        return;
    };
    insert_events(
        &storage,
        &[
            ("e5", "StepSuccess", "a", 30, 3),
            ("e1", "StepStart", "a", 10, 1),
            ("e3", "Materialization", "a", 20, 2),
            ("e2", "StepStart", "b", 10, 1),
            ("e4", "Materialization", "b", 20, 2),
            ("e7", "Materialization", "a", 20, 2),
            ("e6", "RunStart", "a", 5, 0),
            ("e8", "StepFailure", "b", 30, 3),
        ],
    )
    .await;

    let steps = storage.get_run_step_events("r1").await.unwrap();
    assert_eq!(ids(&steps), ["e1", "e2", "e5", "e8"]);
    assert!(
        storage
            .get_run_step_events("other")
            .await
            .unwrap()
            .is_empty()
    );

    let (page, total) = storage
        .get_run_structured_events_page("r1", None, 2, 3)
        .await
        .unwrap();
    assert_eq!(
        (ids(&page), total),
        (vec!["e2".into(), "e3".into(), "e4".into()], 8)
    );
    let (page, total) = storage
        .get_run_structured_events_page("r1", Some("a"), 0, 10)
        .await
        .unwrap();
    assert_eq!(
        (ids(&page), total),
        (
            vec![
                "e6".into(),
                "e1".into(),
                "e3".into(),
                "e7".into(),
                "e5".into()
            ],
            5
        )
    );

    let (page, total) = storage
        .get_run_asset_events_page("r1", "a", "Materialization", 1, 10)
        .await
        .unwrap();
    assert_eq!((ids(&page), total), (vec!["e7".into()], 2));
    let (page, total) = storage
        .get_run_asset_events_page("r1", "a", "Observation", 0, 10)
        .await
        .unwrap();
    assert_eq!((page.len(), total), (0, 0));
}

/// A node of a plan: its type, the table it reads and the index it uses.
#[derive(Debug)]
struct Node {
    kind: String,
    table: String,
    index: String,
}

async fn plan(storage: &PostgresStorage, mut query: QueryBuilder<Postgres>) -> Vec<Node> {
    fn walk(node: &serde_json::Value, nodes: &mut Vec<Node>) {
        let text = |key: &str| node[key].as_str().unwrap_or_default().to_string();
        nodes.push(Node {
            kind: text("Node Type"),
            table: text("Relation Name"),
            index: text("Index Name"),
        });
        for child in node["Plans"].as_array().into_iter().flatten() {
            walk(child, nodes);
        }
    }
    let explained: serde_json::Value = query
        .build_query_scalar()
        .fetch_one(storage.pool())
        .await
        .unwrap();
    let mut nodes = Vec::new();
    walk(&explained[0]["Plan"], &mut nodes);
    nodes
}

fn explain() -> QueryBuilder<Postgres> {
    QueryBuilder::new("EXPLAIN (FORMAT JSON) ")
}

fn reads_through_index(nodes: &[Node], table: &str) -> bool {
    nodes.iter().any(|node| {
        (node.kind.starts_with("Index") || node.kind == "Bitmap Heap Scan") && node.table == table
    })
}

fn uses_index(nodes: &[Node], index: &str) -> bool {
    nodes.iter().any(|node| node.index == index)
}

fn has_node(nodes: &[Node], kind: &str) -> bool {
    nodes.iter().any(|node| node.kind == kind)
}

/// The page reads `table` through an index in the order of the page.
fn assert_ordered_index_scan(page: &str, nodes: &[Node], table: &str) {
    assert!(
        reads_through_index(nodes, table),
        "{page} should read {table} through an index: {nodes:?}"
    );
    assert!(
        !has_node(nodes, "Seq Scan") && !nodes.iter().any(|node| node.kind.ends_with("Sort")),
        "{page} should neither scan {table} nor sort: {nodes:?}"
    );
}

/// The page reads `table` through an index and sorts at most the rows of
/// one time, which an index without the event id leaves in any order.
fn assert_index_scan_without_full_sort(page: &str, nodes: &[Node], table: &str) {
    assert!(
        reads_through_index(nodes, table),
        "{page} should read {table} through an index: {nodes:?}"
    );
    assert!(
        !has_node(nodes, "Seq Scan") && !has_node(nodes, "Sort"),
        "{page} should neither scan {table} nor sort it all: {nodes:?}"
    );
}

#[tokio::test]
async fn run_pages_read_runs_in_index_order() {
    let Some(storage) = store().await else {
        return;
    };
    sqlx::query(
        "INSERT INTO runs (run_id, code_location_id, job_name, status, start_time) \
         SELECT 'run-' || i, CASE WHEN i % 4 = 0 THEN 'other' ELSE 'default' END, \
             'job-' || (i % 50), \
             (ARRAY['Success', 'Failure', 'Started', 'Queued', 'NotStarted', 'Canceled'])[1 + i % 6], \
             i * 1000 \
         FROM generate_series(1, 20000) AS i",
    )
    .execute(storage.pool())
    .await
    .unwrap();
    sqlx::query("ANALYZE runs")
        .execute(storage.pool())
        .await
        .unwrap();

    let all = RunFilter::default();
    let failures = RunFilter {
        status: Some(RunStatus::Failure),
        ..Default::default()
    };
    let job = RunFilter {
        job_name: Some("job-7".into()),
        ..Default::default()
    };
    for (page, cl, filter) in [
        ("all runs", None, &all),
        ("runs of a code location", Some("default"), &all),
        ("failed runs", None, &failures),
        ("failed runs of a code location", Some("default"), &failures),
        ("runs of a job", None, &job),
    ] {
        let mut query = explain();
        push_runs_page(&mut query, cl, 100, 50, filter);
        assert_ordered_index_scan(page, &plan(&storage, query).await, "runs");
    }

    let names: Vec<String> = (0..50).map(|i| format!("job-{i}")).collect();
    for cl in [None, Some("default")] {
        let mut query = explain();
        push_last_run_per_job(&mut query, cl, &names);
        assert_ordered_index_scan("last run per job", &plan(&storage, query).await, "runs");
    }
}

#[tokio::test]
async fn event_pages_read_events_by_index() {
    let Some(storage) = store().await else {
        return;
    };
    // Many small runs, and one run that materializes two assets 20,000
    // times: a page of it must not sort all its events.
    sqlx::query(
        "INSERT INTO events (id, event_type, asset_key, run_id, timestamp, sort_order) \
         SELECT 'e' || i, \
             (ARRAY['StepStart', 'Materialization', 'StepSuccess', 'Observation', 'StepFailure'])[1 + i % 5], \
             'asset-' || (i % 20), 'run-' || (i % 499), i, i % 7 \
         FROM generate_series(1, 50000) AS i \
         UNION ALL \
         SELECT 'big' || i, \
             CASE WHEN i % 1000 = 0 THEN 'StepFailure' WHEN i % 3 = 0 THEN 'StepStart' \
                 ELSE 'Materialization' END, \
             'asset-' || (i % 2), 'big', 50000 + i, i % 7 \
         FROM generate_series(1, 20000) AS i",
    )
    .execute(storage.pool())
    .await
    .unwrap();
    sqlx::query("ANALYZE events")
        .execute(storage.pool())
        .await
        .unwrap();

    let shown = ["Materialization", "Observation", "StepFailure"].map(String::from);
    for (page, types, index) in [
        ("events of an asset", &shown[..], "idx_events_loc_asset_ts"),
        // A rare type: the page reads the events of that type only.
        (
            "events of an asset of one type",
            &shown[2..],
            "idx_events_loc_asset_type",
        ),
    ] {
        let mut query = explain();
        push_asset_events_page(&mut query, "default", "asset-1", types, 100, 50);
        let nodes = plan(&storage, query).await;
        assert_index_scan_without_full_sort(page, &nodes, "events");
        assert!(
            uses_index(&nodes, index),
            "{page} should use {index}: {nodes:?}"
        );
    }

    let mut query = explain();
    push_count_asset_events(&mut query, "default", "asset-1", &shown);
    let nodes = plan(&storage, query).await;
    assert!(
        reads_through_index(&nodes, "events") && !has_node(&nodes, "Seq Scan"),
        "the count should read the events of the asset through an index: {nodes:?}"
    );

    // All step events of a run are the answer, so a sort of them sorts
    // only the rows that the read returns.
    let mut query = explain();
    push_run_step_events(&mut query, "big");
    let nodes = plan(&storage, query).await;
    assert!(
        reads_through_index(&nodes, "events") && !has_node(&nodes, "Seq Scan"),
        "step events should read events through an index: {nodes:?}"
    );

    for (page, asset, event_type) in [
        ("events of a run", None, None),
        ("events of an asset in a run", Some("asset-1"), None),
        (
            "events of one type of an asset in a run",
            Some("asset-1"),
            Some("Materialization"),
        ),
    ] {
        let mut query = explain();
        push_run_events_page(&mut query, "big", asset, event_type, 10, 50);
        assert_index_scan_without_full_sort(page, &plan(&storage, query).await, "events");
    }
}

#[tokio::test]
async fn backfill_pages_read_backfills_in_index_order() {
    let Some(storage) = store().await else {
        return;
    };
    sqlx::query(
        "INSERT INTO backfills (backfill_id, code_location_id, status, strategy, \
             failure_policy, max_concurrency, create_time) \
         SELECT 'bf-' || i, CASE WHEN i % 4 = 0 THEN 'other' ELSE 'default' END, \
             (ARRAY['Requested', 'InProgress', 'CompletedSuccess', 'CompletedFailed', 'Canceled'])[1 + i % 5], \
             '\"SingleRun\"'::jsonb, 'Continue', 1, i * 1000 \
         FROM generate_series(1, 20000) AS i",
    )
    .execute(storage.pool())
    .await
    .unwrap();
    sqlx::query("ANALYZE backfills")
        .execute(storage.pool())
        .await
        .unwrap();

    let all = BackfillFilter::default();
    let failed = BackfillFilter {
        status: Some(BackfillStatus::CompletedFailed),
    };
    for (page, cl, filter) in [
        ("all backfills", None, &all),
        ("backfills of a code location", Some("default"), &all),
        ("failed backfills", None, &failed),
        (
            "failed backfills of a code location",
            Some("default"),
            &failed,
        ),
    ] {
        let mut query = explain();
        push_backfills_page(&mut query, cl, 100, 50, filter);
        assert_ordered_index_scan(page, &plan(&storage, query).await, "backfills");
    }
}
