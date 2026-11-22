//! PostgreSQL-only tests of the event reads and the run logs. The events are
//! rows written here, so the tests pin the reads without the write path.

use super::*;

const CL: &str = "default";

async fn store() -> Option<PostgresStorage> {
    let url = PostgresStorage::test_url()?;
    Some(PostgresStorage::connect_test_schema(&url).await.unwrap())
}

fn single(key: &str) -> PartitionKey {
    PartitionKey::Single {
        keys: vec![key.to_string()],
    }
}

fn materialization(data_version: Option<&str>) -> EventType {
    EventType::Materialization {
        data_version: data_version.map(str::to_string),
    }
}

struct Row<'a> {
    id: &'a str,
    event_type: EventType,
    asset_key: &'a str,
    run_id: &'a str,
    partition_key: Option<PartitionKey>,
    timestamp: i64,
}

fn row<'a>(id: &'a str, event_type: EventType, asset_key: &'a str, run_id: &'a str) -> Row<'a> {
    Row {
        id,
        event_type,
        asset_key,
        run_id,
        partition_key: None,
        timestamp: 100,
    }
}

impl Row<'_> {
    fn at(self, timestamp: i64) -> Self {
        Self { timestamp, ..self }
    }

    fn of(self, key: PartitionKey) -> Self {
        Self {
            partition_key: Some(key),
            ..self
        }
    }
}

async fn insert(storage: &PostgresStorage, rows: Vec<Row<'_>>) {
    for row in rows {
        sqlx::query(
            "INSERT INTO events (id, code_location_id, event_type, asset_key, run_id, \
             partition_key, partition_ord, timestamp, sort_order, data_version) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(row.id)
        .bind(CL)
        .bind(row.event_type.type_name())
        .bind(row.asset_key)
        .bind(row.run_id)
        .bind(row.partition_key.as_ref().map(|k| key_json(k).unwrap()))
        .bind(row.partition_key.as_ref().map(partition_ord))
        .bind(row.timestamp)
        .bind(row.event_type.sort_order())
        .bind(row.event_type.data_version())
        .execute(storage.pool())
        .await
        .unwrap();
    }
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

/// Ties of time go by the place of the event type in a step, then by id.
#[tokio::test]
async fn events_of_a_run_come_oldest_first_and_of_an_asset_newest_first() {
    let Some(storage) = store().await else { return };
    insert(
        &storage,
        vec![
            row("e3", EventType::StepSuccess, "a", "r1"),
            row("e2", materialization(Some("v1")), "a", "r1"),
            row("e1", EventType::StepStart, "a", "r1"),
            row("e0", EventType::StepStart, "b", "r1").at(200),
            row("e4", EventType::StepSuccess, "a", "r1").at(50),
            row("e5", materialization(None), "a", "r2"),
        ],
    )
    .await;

    let of_run = storage.get_events_for_run("r1").await.unwrap();
    assert_eq!(ids(&of_run), ["e4", "e1", "e2", "e3", "e0"]);
    assert_eq!(
        of_run[2].event_type,
        materialization(Some("v1")),
        "the data version comes back in the event type"
    );

    let of_asset = storage.get_events_for_asset(CL, "a", 3).await.unwrap();
    assert_eq!(ids(&of_asset), ["e3", "e5", "e2"]);
    let unlimited = storage.get_events_for_asset(CL, "a", usize::MAX).await;
    assert_eq!(unlimited.unwrap().len(), 5);

    let of_step = storage.get_events_for_step("r1", "a").await.unwrap();
    assert_eq!(ids(&of_step), ["e4", "e1", "e2", "e3"]);
    let terminal = storage.get_step_terminal_events("r1", "a").await.unwrap();
    assert_eq!(ids(&terminal), ["e4", "e3"]);
    assert!(
        storage
            .get_events_for_run("missing")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn latest_materialization_prefers_the_structured_reading() {
    let Some(storage) = store().await else { return };
    let multi = PartitionKey::Multi {
        dims: vec![
            ("date".to_string(), vec!["2024-01-01".to_string()]),
            ("region".to_string(), vec!["eu".to_string()]),
        ],
    };
    let display = multi.to_display();
    insert(
        &storage,
        vec![
            row("old-single", materialization(None), "inv", "r1")
                .of(single(&display))
                .at(200),
            row("multi", materialization(None), "inv", "r2")
                .of(multi.clone())
                .at(100),
            row("start", EventType::StepStart, "inv", "r2")
                .of(multi)
                .at(300),
            row("plain", materialization(None), "inv2", "r3").of(single(&display)),
        ],
    )
    .await;

    let latest = storage
        .get_latest_materialization(CL, "inv", Some(&display))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.run_id, "r2");
    let events = storage
        .get_partition_events(CL, "inv", &display, 10)
        .await
        .unwrap();
    assert_eq!(ids(&events), ["start", "multi"]);
    let fallback = storage
        .get_latest_materialization(CL, "inv2", Some(&display))
        .await
        .unwrap();
    assert_eq!(fallback.map(|event| event.run_id), Some("r3".to_string()));

    let newest = storage
        .get_latest_materialization(CL, "inv", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(newest.run_id, "r1");
    assert!(
        storage
            .get_latest_materialization(CL, "inv", Some("nothing"))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn step_attempts_count_starts_retries_and_failures() {
    let Some(storage) = store().await else { return };
    insert(
        &storage,
        vec![
            row("1", EventType::StepStart, "cut_off", "r1"),
            row("2", EventType::StepRetry, "cut_off", "r1"),
            row("3", EventType::StepStart, "cut_off", "r1"),
            row("4", EventType::StepStart, "failed", "r1"),
            row("5", EventType::StepFailure, "failed", "r1"),
            row("6", EventType::StepStart, "keyed", "r1"),
            row("7", EventType::StepFailure, "keyed", "r1")
                .of(single("p2"))
                .at(200),
            row("8", EventType::StepFailure, "keyed", "r1").of(PartitionKey::Set {
                keys: vec![single("p0"), single("p1")],
            }),
            row("9", EventType::StepStart, "other", "r2"),
        ],
    )
    .await;

    let attempts = storage.get_step_attempts("r1").await.unwrap();
    assert_eq!(attempts.len(), 3);
    assert_eq!(
        attempts["cut_off"],
        StepAttempts {
            starts: 2,
            failed: false,
            retries: 1,
            failed_keys: vec![],
        }
    );
    assert!(attempts["failed"].failed);
    assert_eq!(
        attempts["keyed"],
        StepAttempts {
            starts: 1,
            failed: false,
            retries: 0,
            failed_keys: vec![single("p0"), single("p1"), single("p2")],
        }
    );
    assert!(storage.get_step_attempts("none").await.unwrap().is_empty());
}

#[tokio::test]
async fn step_keys_and_data_versions_of_a_run() {
    let Some(storage) = store().await else { return };
    insert(
        &storage,
        vec![
            row("1", materialization(Some("v1")), "a", "r1"),
            row("2", materialization(Some("v2")), "a", "r1").at(300),
            row("3", materialization(None), "b", "r1"),
            row("4", materialization(Some("x")), "a", "r2"),
            row("5", EventType::StepSuccess, "a", "r1"),
            row("6", EventType::StepSuccess, "a", "r1"),
            row("7", EventType::StepFailure, "b", "r1"),
        ],
    )
    .await;

    let versions = storage.get_step_data_versions("r1").await.unwrap();
    assert_eq!(
        versions,
        HashMap::from([("a".to_string(), "v2".to_string())])
    );
    let completed = storage.get_completed_step_keys("r1").await.unwrap();
    assert_eq!(completed, HashSet::from(["a".to_string()]));
    let failures = storage.get_run_failure_events("r1").await.unwrap();
    assert_eq!(ids(&failures), ["7"]);
}

#[tokio::test]
async fn outcomes_and_materializations_of_runs_answer_for_the_assets_asked() {
    let Some(storage) = store().await else { return };
    insert(
        &storage,
        vec![
            row("1", EventType::StepFailure, "a", "run-fail"),
            row("2", EventType::StepSuccess, "a", "run-ok"),
            row("3", EventType::StepFailure, "b", "run-ok"),
            row("4", EventType::StepSuccess, "c", "run-other"),
            row("5", materialization(None), "a", "run-ok"),
            row("6", materialization(None), "a", "run-ok").of(single("p1")),
            row("7", materialization(None), "b", "run-fail"),
            row("8", materialization(None), "a", "run-other"),
        ],
    )
    .await;
    let runs = vec!["run-fail".to_string(), "run-ok".to_string()];
    let assets = |keys: &[&str]| keys.iter().map(|k| k.to_string()).collect::<Vec<_>>();

    let mut outcomes: Vec<(String, String, bool)> = storage
        .step_outcomes(&assets(&["a", "b", "c"]), &runs)
        .await
        .unwrap()
        .into_iter()
        .map(|o| (o.asset_key, o.run_id, o.succeeded))
        .collect();
    outcomes.sort();
    assert_eq!(
        outcomes,
        [
            ("a".to_string(), "run-fail".to_string(), false),
            ("a".to_string(), "run-ok".to_string(), true),
            ("b".to_string(), "run-ok".to_string(), false),
        ]
    );

    let built = storage
        .materialized_by_runs(&assets(&["a", "c"]), &runs)
        .await
        .unwrap();
    assert_eq!(
        built,
        HashMap::from([("a".to_string(), HashSet::from(["run-ok".to_string()]))])
    );
    assert!(storage.step_outcomes(&[], &runs).await.unwrap().is_empty());
    assert!(
        storage
            .materialized_by_runs(&assets(&["a"]), &[])
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn observations_come_newest_first() {
    let Some(storage) = store().await else { return };
    assert_eq!(storage.get_latest_observation_ts(CL).await.unwrap(), None);
    let observation = |v: &str| EventType::Observation {
        data_version: Some(v.to_string()),
    };
    insert(
        &storage,
        vec![
            row("1", observation("v1"), "ext_a", "").at(1000),
            row("2", observation("v2"), "ext_b", "").at(2000),
            row("3", materialization(Some("m1")), "ext_a", "r1").at(3000),
        ],
    )
    .await;

    let all = storage.get_observations_since(CL, 0).await.unwrap();
    assert_eq!(ids(&all), ["2", "1"]);
    assert_eq!(all[0].event_type, observation("v2"));
    assert_eq!(
        ids(&storage.get_observations_since(CL, 1000).await.unwrap()),
        ["2"]
    );
    assert!(
        storage
            .get_observations_since("other", 0)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        storage.get_latest_observation_ts(CL).await.unwrap(),
        Some(2000)
    );
}

#[tokio::test]
async fn run_logs_round_trip_and_tracebacks_follow_their_order() {
    let Some(storage) = store().await else { return };
    let log = |run: &str, step: &str, at: i64, traceback: Option<&str>| LogRecord {
        code_location_id: CL.to_string(),
        run_id: run.to_string(),
        step_key: step.to_string(),
        timestamp: at,
        stdout: traceback.is_none().then(|| format!("{step} out")),
        stderr: None,
        logs: None,
        traceback: traceback.map(str::to_string),
    };
    let mut rows = vec![
        log("run-1", "b", 20, None),
        log("run-1", "a", 10, None),
        log("run-1", "b", 30, Some("b later")),
        log("run-2", "c", 5, Some("other run")),
    ];
    for i in 0..8 {
        rows.push(log("run-1", "a", 10, Some(&format!("a {i}"))));
    }
    storage.store_run_logs(&rows).await.unwrap();
    storage.store_run_logs(&[]).await.unwrap();

    let logs = storage.get_run_logs("run-1").await.unwrap();
    let times: Vec<i64> = logs.iter().map(|l| l.timestamp).collect();
    assert_eq!(times, [10, 10, 10, 10, 10, 10, 10, 10, 10, 20, 30]);
    let first = logs.iter().find(|l| l.traceback.is_none()).unwrap();
    assert_eq!(first.stdout.as_deref(), Some("a out"));
    assert_eq!(first.stderr, None);
    assert_eq!(first.id.table.as_str(), "run_logs");

    let in_run_logs_order: Vec<String> = logs
        .into_iter()
        .filter(|l| l.step_key == "a")
        .filter_map(|l| l.traceback)
        .collect();
    assert_eq!(in_run_logs_order.len(), 8);
    assert_eq!(
        storage.get_step_tracebacks("run-1", "a", 10).await.unwrap(),
        in_run_logs_order
    );
    assert_eq!(
        storage.get_step_tracebacks("run-1", "b", 30).await.unwrap(),
        ["b later"]
    );
    assert!(
        storage
            .get_step_tracebacks("run-1", "b", 20)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(storage.get_run_logs("run-2").await.unwrap().len(), 1);
    assert!(storage.get_run_logs("missing").await.unwrap().is_empty());
}
