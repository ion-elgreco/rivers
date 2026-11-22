//! PostgreSQL-only tests of the partition reads. The rows are written here,
//! so the tests pin the reads without the write path.

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

fn cell(hour: &str, zone: &str) -> PartitionKey {
    PartitionKey::Multi {
        dims: vec![
            ("hour".to_string(), vec![hour.to_string()]),
            ("zone".to_string(), vec![zone.to_string()]),
        ],
    }
}

fn hours(first: &str, last: &str) -> PartitionBounds {
    PartitionBounds::of_dimension(&[], "hour", first, last)
}

/// The partition row of `key`, materialized by `run_id` at `at`.
async fn materialized(
    storage: &PostgresStorage,
    asset_key: &str,
    key: &PartitionKey,
    run_id: &str,
    at: Option<i64>,
) {
    sqlx::query(
        "INSERT INTO asset_partitions (code_location_id, asset_key, partition_ord, \
         partition_key, last_event_id, last_run_id, last_timestamp) \
         VALUES ($1, $2, $3, $4, 'e', $5, $6)",
    )
    .bind(CL)
    .bind(asset_key)
    .bind(partition_ord(key))
    .bind(key_json(key).unwrap())
    .bind(run_id)
    .bind(at)
    .execute(storage.pool())
    .await
    .unwrap();
}

/// `trips` has three zones for most hours; `2024-06-30T01:00` has one.
async fn store_with_trips() -> Option<PostgresStorage> {
    let storage = store().await?;
    for hour in [
        "2024-06-29T23:00",
        "2024-06-30T00:00",
        "2024-06-30T01:00",
        "2024-07-01T00:00",
    ] {
        for zone in ["001", "002", "130"] {
            if hour == "2024-06-30T01:00" && zone != "002" {
                continue;
            }
            materialized(&storage, "trips", &cell(hour, zone), "r1", Some(100)).await;
        }
    }
    let other = cell("2024-06-30T00:00", "001");
    materialized(&storage, "other", &other, "r1", Some(100)).await;
    materialized(
        &storage,
        "plain",
        &single("2024-06-30T00:00"),
        "r1",
        Some(100),
    )
    .await;
    Some(storage)
}

#[tokio::test]
async fn counts_in_bounds_come_in_the_order_of_the_bounds() {
    let Some(storage) = store_with_trips().await else {
        return;
    };
    let hour = ("hour".to_string(), "2024-06-30T00:00".to_string());

    let counts = storage
        .count_materialized_partitions_in(
            CL,
            "trips",
            &[
                hours("2024-06-29T23:00", "2024-06-30T01:00"),
                hours("2024-06-30T00:00", "2024-06-30T01:00"),
                hours("2024-06-30T01:00", "2024-06-30T01:00"),
                hours("2024-06-30T02:00", "2024-06-30T23:00"),
                hours("2024-01-01T00:00", "2024-12-31T23:00"),
                PartitionBounds::of_dimension(std::slice::from_ref(&hour), "zone", "001", "002"),
                PartitionBounds::of_dimension(std::slice::from_ref(&hour), "zone", "130", "130"),
                PartitionBounds::of_dimension(&[hour], "zone", "131", "999"),
                hours("2024-07-01T00:00", "2024-06-30T00:00"),
            ],
        )
        .await
        .unwrap();

    assert_eq!(counts, [7, 4, 1, 0, 10, 2, 1, 0, 0]);
    assert!(
        storage
            .count_materialized_partitions_in(CL, "trips", &[])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        storage
            .count_materialized_partitions(CL, "trips")
            .await
            .unwrap(),
        10
    );
    assert_eq!(
        storage
            .count_materialized_partitions(CL, "none")
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn timestamps_of_keys_keep_the_order_of_the_keys() {
    let Some(storage) = store().await else { return };
    materialized(&storage, "trips", &cell("00:00", "a"), "r1", Some(100)).await;
    materialized(&storage, "trips", &cell("01:00", "a"), "r2", Some(200)).await;
    materialized(&storage, "trips", &cell("02:00", "a"), "r3", None).await;
    materialized(&storage, "plain", &single("p1"), "r4", Some(300)).await;
    let reordered = PartitionKey::Multi {
        dims: vec![
            ("zone".to_string(), vec!["a".to_string()]),
            ("hour".to_string(), vec!["00:00".to_string()]),
        ],
    };

    let rows = storage
        .get_partition_timestamps_for_keys(
            CL,
            "trips",
            &[
                cell("01:00", "a"),
                cell("01:00", "b"),
                cell("02:00", "a"),
                reordered,
                single("p1"),
            ],
        )
        .await
        .unwrap();

    assert_eq!(
        rows,
        [
            (cell("01:00", "a"), 200, Some("r2".to_string())),
            (cell("00:00", "a"), 100, Some("r1".to_string())),
        ]
    );
    assert_eq!(
        storage.get_partition_timestamps(CL, "trips").await.unwrap(),
        [(cell("00:00", "a"), 100), (cell("01:00", "a"), 200)],
        "a row without a time is left out"
    );
    assert_eq!(
        storage
            .get_partition_timestamps_since(CL, "trips", 100)
            .await
            .unwrap(),
        [(cell("01:00", "a"), 200)]
    );
    assert!(
        storage
            .get_partition_timestamps_for_keys(CL, "trips", &[])
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn pages_and_bounded_reads_go_in_key_order() {
    let Some(storage) = store().await else { return };
    for key in ["p3", "p1", "p0", "p4", "p2"] {
        materialized(&storage, "plain", &single(key), "r1", Some(100)).await;
    }
    materialized(&storage, "other", &single("x"), "r1", Some(100)).await;

    let mut pages = Vec::new();
    for offset in [0, 2, 4, 6] {
        pages.push(
            storage
                .get_materialized_partitions_page(CL, "plain", offset, 2)
                .await
                .unwrap(),
        );
    }
    let all: Vec<PartitionKey> = ["p0", "p1", "p2", "p3", "p4"].map(single).into();
    assert_eq!(pages.concat(), all);
    assert!(pages[3].is_empty());
    let unlimited = storage
        .get_materialized_partitions_page(CL, "plain", 1, u64::MAX)
        .await;
    assert_eq!(unlimited.unwrap(), all[1..]);
    assert_eq!(
        storage
            .get_materialized_partitions(CL, "plain")
            .await
            .unwrap(),
        all
    );

    let capped = storage
        .get_partition_timestamps_in(CL, "plain", &PartitionBounds::of_keys("p1", "p9"), 2)
        .await
        .unwrap();
    assert_eq!(
        capped,
        [
            (single("p1"), 100, Some("r1".to_string())),
            (single("p2"), 100, Some("r1".to_string())),
        ]
    );
}

async fn run(storage: &PostgresStorage, run_id: &str, status: &str, action: Option<&str>) {
    sqlx::query(
        "INSERT INTO runs (run_id, code_location_id, status, start_time, node_names, action) \
         VALUES ($1, $2, $3, 1000, ARRAY['asset'], $4)",
    )
    .bind(run_id)
    .bind(CL)
    .bind(status)
    .bind(action)
    .execute(storage.pool())
    .await
    .unwrap();
}

async fn step_start(storage: &PostgresStorage, id: &str, run_id: &str, key: &PartitionKey) {
    sqlx::query(
        "INSERT INTO events (id, code_location_id, event_type, asset_key, run_id, \
         partition_key, partition_ord, timestamp) \
         VALUES ($1, $2, 'StepStart', 'asset', $3, $4, $5, 1000)",
    )
    .bind(id)
    .bind(CL)
    .bind(run_id)
    .bind(key_json(key).unwrap())
    .bind(partition_ord(key))
    .execute(storage.pool())
    .await
    .unwrap();
}

#[tokio::test]
async fn in_progress_partitions_are_those_of_started_runs_without_an_action() {
    let Some(storage) = store().await else { return };
    run(&storage, "started", "Started", None).await;
    run(&storage, "again", "Started", None).await;
    run(&storage, "optimize", "Started", Some("optimize")).await;
    run(&storage, "done", "Success", None).await;
    step_start(&storage, "1", "started", &single("p2")).await;
    step_start(&storage, "2", "started", &single("p1")).await;
    step_start(&storage, "3", "again", &single("p2")).await;
    step_start(&storage, "4", "optimize", &single("p3")).await;
    step_start(&storage, "5", "done", &single("p4")).await;
    step_start(&storage, "6", "unknown", &single("p5")).await;

    let in_progress = storage
        .get_in_progress_partitions(CL, "asset")
        .await
        .unwrap();

    assert_eq!(in_progress, [single("p1"), single("p2")]);
    assert!(
        storage
            .get_in_progress_partitions(CL, "other")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn failed_partitions_leave_out_the_partitions_built_since() {
    let Some(storage) = store().await else { return };
    for (key, failed_at) in [("p1", 100), ("p2", 200), ("p3", 300)] {
        let key = single(key);
        sqlx::query(
            "INSERT INTO asset_partition_failures (code_location_id, asset_key, \
             partition_ord, partition_key, failed_at, run_id, has_row) \
             VALUES ($1, 'asset', $2, $3, $4, 'r1', false)",
        )
        .bind(CL)
        .bind(partition_ord(&key))
        .bind(key_json(&key).unwrap())
        .bind(failed_at)
        .execute(storage.pool())
        .await
        .unwrap();
    }
    let materialized = HashMap::from([(single("p1"), 100), (single("p2"), 150)]);

    let failed = storage
        .get_failed_partitions(CL, "asset", &materialized)
        .await
        .unwrap();

    assert_eq!(
        failed,
        HashMap::from([(single("p2"), 200), (single("p3"), 300)])
    );
}
