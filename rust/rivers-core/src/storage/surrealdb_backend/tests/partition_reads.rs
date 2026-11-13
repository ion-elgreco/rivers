//! Reads of an asset's partitions that do not grow with the asset: the rows
//! of the keys of one page, one page of the materialized keys, and the
//! failures without the times of every partition.

use std::collections::HashMap;

use super::*;
use crate::storage::PartitionBounds;
use crate::storage::surrealdb_backend::partition_reads::keyed_statements;
use crate::storage::surrealdb_backend::queries::{
    COUNT_PARTITIONS_IN_BOUNDS, PARTITION_TIMESTAMPS_OF_KEY,
};

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

fn event(
    asset: &str,
    run_id: &str,
    event_type: EventType,
    partition: PartitionKey,
    timestamp: i64,
) -> EventRecord {
    EventRecord {
        event_type,
        partition_key: Some(partition),
        ..make_event(asset, run_id, timestamp)
    }
}

fn materialization(asset: &str, run_id: &str, partition: PartitionKey, at: i64) -> EventRecord {
    event(
        asset,
        run_id,
        EventType::Materialization { data_version: None },
        partition,
        at,
    )
}

/// The `access` of every index scan in `plan`.
fn index_accesses(plan: &serde_json::Value, found: &mut Vec<String>) {
    match plan {
        serde_json::Value::Object(node) => {
            if node.get("operator").and_then(|o| o.as_str()) == Some("IndexScan") {
                let access = &node["attributes"]["access"];
                found.push(access.as_str().unwrap_or_default().to_string());
            }
            node.values().for_each(|child| index_accesses(child, found));
        }
        serde_json::Value::Array(nodes) => {
            nodes.iter().for_each(|child| index_accesses(child, found));
        }
        _ => {}
    }
}

#[tokio::test]
async fn partition_timestamps_for_keys_returns_the_rows_of_the_keys() {
    let storage = make_storage().await;
    register(&storage, &["trips", "plain"]).await;
    storage
        .store_events(&[
            materialization("trips", "r1", cell("00:00", "a"), 100),
            materialization("trips", "r1", cell("00:00", "b"), 100),
            materialization("trips", "r2", cell("01:00", "a"), 200),
            materialization("plain", "r3", single("p1"), 300),
        ])
        .await
        .unwrap();

    let mut rows = storage
        .get_partition_timestamps_for_keys(
            DEFAULT_CODE_LOCATION_ID,
            "trips",
            &[
                cell("01:00", "a"),
                cell("01:00", "b"),
                cell("00:00", "a"),
                single("p1"),
            ],
        )
        .await
        .unwrap();
    rows.sort_by_key(|(_, time, _)| *time);

    assert_eq!(
        rows,
        vec![
            (cell("00:00", "a"), 100, Some("r1".to_string())),
            (cell("01:00", "a"), 200, Some("r2".to_string())),
        ]
    );
}

#[tokio::test]
async fn partition_timestamps_for_keys_reads_more_keys_than_one_query_holds() {
    let storage = make_storage().await;
    register(&storage, &["plain"]).await;
    let keys: Vec<PartitionKey> = (0..1_200).map(|i| single(&format!("p{i:04}"))).collect();
    let events: Vec<EventRecord> = keys
        .iter()
        .enumerate()
        .map(|(i, key)| materialization("plain", "r1", key.clone(), 1_000 + i as i64))
        .collect();
    for batch in events.chunks(200) {
        storage.store_events(batch).await.unwrap();
    }

    let mut wanted = keys.clone();
    wanted.insert(600, single("not-there"));
    let rows: Vec<(PartitionKey, i64)> = storage
        .get_partition_timestamps_for_keys(DEFAULT_CODE_LOCATION_ID, "plain", &wanted)
        .await
        .unwrap()
        .into_iter()
        .map(|(key, time, _)| (key, time))
        .collect();

    let expected: Vec<(PartitionKey, i64)> = keys
        .into_iter()
        .enumerate()
        .map(|(i, key)| (key, 1_000 + i as i64))
        .collect();
    assert_eq!(
        rows, expected,
        "the rows of the keys, in the order of the keys"
    );
}

/// The engine handles a list of keys again for every statement that takes
/// its key from the list: a query of 500 keys cost 0.40 s on an asset with a
/// million partitions, and 0.09 s with a parameter for each key.
#[test]
fn each_statement_of_a_keyed_read_has_a_parameter_of_its_own() {
    assert_eq!(
        keyed_statements("SELECT v FROM t WHERE k = $partition AND v > 0", 2),
        "SELECT v FROM t WHERE k = $partition_0 AND v > 0;\
         SELECT v FROM t WHERE k = $partition_1 AND v > 0;"
    );
}

/// An asset with a million partitions has a million rows under the first two
/// columns of the index. A read that does not reach the row by its key reads
/// them all.
#[tokio::test]
async fn partition_timestamps_of_a_key_are_read_by_the_whole_index() {
    let storage = make_storage().await;
    register(&storage, &["trips"]).await;
    storage
        .store_events(&[materialization("trips", "r1", cell("00:00", "a"), 100)])
        .await
        .unwrap();

    let statement = keyed_statements(PARTITION_TIMESTAMPS_OF_KEY, 1);
    let statement = statement.trim_end_matches(';');
    let plan: Vec<serde_json::Value> = storage
        .db
        .query(format!("{statement} EXPLAIN"))
        .bind(("cl", DEFAULT_CODE_LOCATION_ID.to_string()))
        .bind(("asset_key", "trips".to_string()))
        .bind(("partition_0", cell("00:00", "a")))
        .await
        .unwrap()
        .take(0)
        .unwrap();

    let mut accesses = Vec::new();
    index_accesses(&serde_json::Value::Array(plan.clone()), &mut accesses);
    assert_eq!(accesses.len(), 1, "one index scan: {plan:?}");
    assert!(
        accesses[0].contains("'trips'") && accesses[0].contains("'00:00'"),
        "the scan should reach the row by asset and key: {plan:?}"
    );
}

#[tokio::test]
async fn materialized_partitions_page_reads_each_partition_once() {
    let storage = make_storage().await;
    register(&storage, &["plain", "other"]).await;
    let keys: Vec<PartitionKey> = (0..5).map(|i| single(&format!("p{i}"))).collect();
    let mut events: Vec<EventRecord> = keys
        .iter()
        .map(|key| materialization("plain", "r1", key.clone(), 100))
        .collect();
    events.push(materialization("other", "r1", single("x"), 100));
    storage.store_events(&events).await.unwrap();

    let mut read: Vec<PartitionKey> = Vec::new();
    for offset in [0, 2, 4, 6] {
        let page = storage
            .get_materialized_partitions_page(DEFAULT_CODE_LOCATION_ID, "plain", offset, 2)
            .await
            .unwrap();
        assert!(page.len() <= 2);
        read.extend(page);
    }

    read.sort_by_key(|key| key.to_display());
    assert_eq!(read, keys);
}

#[tokio::test]
async fn partition_failures_are_not_compared_with_materializations() {
    let storage = make_storage().await;
    register(&storage, &["plain"]).await;
    storage
        .store_events(&[
            event(
                "plain",
                "r1",
                EventType::StepFailure,
                single("rebuilt"),
                100,
            ),
            event("plain", "r1", EventType::StepFailure, single("broken"), 100),
            event(
                "plain",
                "r1",
                EventType::StepFailure,
                single("deleted"),
                100,
            ),
            materialization("plain", "r2", single("rebuilt"), 200),
        ])
        .await
        .unwrap();
    storage
        .store_events(&[event(
            "plain",
            "r3",
            EventType::Deletion,
            single("deleted"),
            300,
        )])
        .await
        .unwrap();

    let failures = storage
        .get_partition_failures(DEFAULT_CODE_LOCATION_ID, "plain")
        .await
        .unwrap();
    assert_eq!(
        failures,
        HashMap::from([(single("rebuilt"), 100), (single("broken"), 100)])
    );

    let times = HashMap::from([(single("rebuilt"), 200)]);
    let failed = storage
        .get_failed_partitions(DEFAULT_CODE_LOCATION_ID, "plain", &times)
        .await
        .unwrap();
    assert_eq!(failed, HashMap::from([(single("broken"), 100)]));
}

/// The `operator` of every node of `plan`.
fn operators(plan: &serde_json::Value, found: &mut Vec<String>) {
    match plan {
        serde_json::Value::Object(node) => {
            if let Some(operator) = node.get("operator").and_then(|o| o.as_str()) {
                found.push(operator.to_string());
            }
            node.values().for_each(|child| operators(child, found));
        }
        serde_json::Value::Array(nodes) => {
            nodes.iter().for_each(|child| operators(child, found));
        }
        _ => {}
    }
}

/// `trips` has three zones for most hours; `2024-06-30T01:00` has one.
async fn storage_with_trips() -> SurrealStorage {
    let storage = make_storage().await;
    register(&storage, &["trips", "other", "plain"]).await;
    let mut events = Vec::new();
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
            events.push(materialization("trips", "r1", cell(hour, zone), 100));
        }
    }
    events.push(materialization(
        "other",
        "r1",
        cell("2024-06-30T00:00", "001"),
        100,
    ));
    events.push(materialization(
        "plain",
        "r1",
        single("2024-06-30T00:00"),
        100,
    ));
    storage.store_events(&events).await.unwrap();
    storage
}

fn hours(first: &str, last: &str) -> PartitionBounds {
    PartitionBounds::of_dimension(&[], "hour", first, last)
}

#[tokio::test]
async fn count_in_bounds_counts_the_rows_of_each_run_of_keys() {
    let storage = storage_with_trips().await;
    let hour = ("hour".to_string(), "2024-06-30T00:00".to_string());

    let counts = storage
        .count_materialized_partitions_in(
            DEFAULT_CODE_LOCATION_ID,
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
            ],
        )
        .await
        .unwrap();

    assert_eq!(counts, vec![7, 4, 1, 0, 10, 2, 1, 0]);
}

#[tokio::test]
async fn count_in_bounds_of_keys_with_one_dimension() {
    let storage = make_storage().await;
    register(&storage, &["plain", "trips"]).await;
    let mut events: Vec<EventRecord> = (1..=5)
        .map(|day| materialization("plain", "r1", single(&format!("2024-01-0{day}")), 100))
        .collect();
    events.push(materialization("trips", "r1", cell("2024-01-02", "a"), 100));
    storage.store_events(&events).await.unwrap();

    let counts = storage
        .count_materialized_partitions_in(
            DEFAULT_CODE_LOCATION_ID,
            "plain",
            &[
                PartitionBounds::of_keys("2024-01-02", "2024-01-04"),
                PartitionBounds::of_keys("2024-01-05", "2024-01-05"),
                PartitionBounds::of_keys("2024-01-06", "2024-01-31"),
                PartitionBounds::of_keys("2023", "2025"),
            ],
        )
        .await
        .unwrap();

    assert_eq!(counts, vec![3, 1, 0, 5]);
}

/// A key can start with another key, and can hold the characters that sort
/// after letters. The bounds hold the keys from `first` to `last` only.
#[tokio::test]
async fn bounds_hold_exactly_the_keys_from_first_to_last() {
    let storage = make_storage().await;
    register(&storage, &["trips", "plain"]).await;
    let values = ["a", "a b", "a1", "a~", "a\u{10FFFF}", "ä", "b", "~"];
    let mut events = Vec::new();
    for value in values {
        events.push(materialization("plain", "r1", single(value), 100));
        for zone in ["", "z", "~", "\u{10FFFF}"] {
            events.push(materialization("trips", "r1", cell(value, zone), 100));
        }
    }
    storage.store_events(&events).await.unwrap();

    for (first, last) in [
        ("a", "a"),
        ("a", "a1"),
        ("a1", "a~"),
        ("a ", "b"),
        ("b", "~"),
    ] {
        let inside = values
            .iter()
            .filter(|value| (first..=last).contains(*value))
            .count() as u64;

        let bounds = [
            PartitionBounds::of_keys(first, last),
            PartitionBounds::of_dimension(&[], "hour", first, last),
        ];
        let plain = storage
            .count_materialized_partitions_in(DEFAULT_CODE_LOCATION_ID, "plain", &bounds[..1])
            .await
            .unwrap();
        let trips = storage
            .count_materialized_partitions_in(DEFAULT_CODE_LOCATION_ID, "trips", &bounds[1..])
            .await
            .unwrap();

        assert_eq!(plain, vec![inside], "keys from {first:?} to {last:?}");
        assert_eq!(trips, vec![inside * 4], "hours from {first:?} to {last:?}");

        for value in values {
            let expected = (first..=last).contains(&value);
            assert_eq!(bounds[0].contains(&single(value)), expected);
            assert_eq!(bounds[1].contains(&cell(value, "\u{10FFFF}")), expected);
            assert!(!bounds[0].contains(&cell(value, "z")));
            assert!(!bounds[1].contains(&single(value)));
        }
    }
}

#[tokio::test]
async fn count_in_bounds_reads_more_bounds_than_one_query_holds() {
    let storage = make_storage().await;
    register(&storage, &["plain"]).await;
    let events: Vec<EventRecord> = (0..250)
        .flat_map(|group| {
            (0..=group % 3).map(move |member| {
                let key = single(&format!("g{group:03}-{member}"));
                materialization("plain", "r1", key, 100)
            })
        })
        .collect();
    for batch in events.chunks(200) {
        storage.store_events(batch).await.unwrap();
    }

    let bounds: Vec<PartitionBounds> = (0..250)
        .map(|group| PartitionBounds::of_keys(&format!("g{group:03}-0"), &format!("g{group:03}-9")))
        .collect();
    let counts = storage
        .count_materialized_partitions_in(DEFAULT_CODE_LOCATION_ID, "plain", &bounds)
        .await
        .unwrap();

    let expected: Vec<u64> = (0..250).map(|group| group % 3 + 1).collect();
    assert_eq!(counts, expected, "the counts, in the order of the bounds");
}

/// An asset with a million partitions has a million rows. A count that
/// fetches the rows of its bounds, or the rows of the asset, is too slow for
/// a page that counts a few hundred runs of keys.
#[tokio::test]
async fn count_in_bounds_is_counted_on_the_index() {
    let storage = storage_with_trips().await;
    let bounds = hours("2024-06-30T00:00", "2024-06-30T01:00");

    let plan: Vec<serde_json::Value> = storage
        .db
        .query(format!("{COUNT_PARTITIONS_IN_BOUNDS} EXPLAIN"))
        .bind(("cl", DEFAULT_CODE_LOCATION_ID.to_string()))
        .bind(("asset_key", "trips".to_string()))
        .bind(("lower", bounds.lower))
        .bind(("upper", bounds.upper))
        .await
        .unwrap()
        .take(0)
        .unwrap();

    let mut found = Vec::new();
    operators(&serde_json::Value::Array(plan.clone()), &mut found);
    assert!(
        found.iter().any(|operator| operator == "IndexCountScan"),
        "the count should come from the index alone: {plan:?}"
    );
}

#[tokio::test]
async fn rows_in_bounds_are_read_through_the_index() {
    let storage = storage_with_trips().await;
    let bounds = hours("2024-06-30T00:00", "2024-06-30T01:00");

    let plan: Vec<serde_json::Value> = storage
        .db
        .query(format!("{PARTITION_TIMESTAMPS_IN_BOUNDS} EXPLAIN"))
        .bind(("cl", DEFAULT_CODE_LOCATION_ID.to_string()))
        .bind(("asset_key", "trips".to_string()))
        .bind(("lower", bounds.lower))
        .bind(("upper", bounds.upper))
        .bind(("limit", 100u64))
        .await
        .unwrap()
        .take(0)
        .unwrap();

    let mut found = Vec::new();
    operators(&serde_json::Value::Array(plan.clone()), &mut found);
    assert_eq!(
        found.last().map(String::as_str),
        Some("IndexScan"),
        "the rows should be read through the index: {plan:?}"
    );
    assert!(
        format!("{plan:?}").contains("idx_asset_part"),
        "the bounds should be a range of the index of the keys: {plan:?}"
    );
}

#[tokio::test]
async fn partition_timestamps_in_bounds_returns_the_rows_of_a_run_of_keys() {
    let storage = make_storage().await;
    register(&storage, &["trips"]).await;
    storage
        .store_events(&[
            materialization("trips", "r1", cell("00:00", "a"), 100),
            materialization("trips", "r1", cell("00:00", "b"), 100),
            materialization("trips", "r2", cell("01:00", "a"), 200),
            materialization("trips", "r2", cell("01:00", "c"), 200),
            materialization("trips", "r3", cell("02:00", "a"), 300),
        ])
        .await
        .unwrap();
    let hour = ("hour".to_string(), "01:00".to_string());

    let mut of_hours = storage
        .get_partition_timestamps_in(
            DEFAULT_CODE_LOCATION_ID,
            "trips",
            &hours("00:00", "01:00"),
            10,
        )
        .await
        .unwrap();
    of_hours.sort_by_key(|(key, _, _)| key.to_display());
    let of_zones = storage
        .get_partition_timestamps_in(
            DEFAULT_CODE_LOCATION_ID,
            "trips",
            &PartitionBounds::of_dimension(&[hour], "zone", "b", "z"),
            10,
        )
        .await
        .unwrap();
    let capped = storage
        .get_partition_timestamps_in(
            DEFAULT_CODE_LOCATION_ID,
            "trips",
            &hours("00:00", "02:00"),
            2,
        )
        .await
        .unwrap();

    assert_eq!(
        of_hours,
        vec![
            (cell("00:00", "a"), 100, Some("r1".to_string())),
            (cell("00:00", "b"), 100, Some("r1".to_string())),
            (cell("01:00", "a"), 200, Some("r2".to_string())),
            (cell("01:00", "c"), 200, Some("r2".to_string())),
        ]
    );
    assert_eq!(
        of_zones,
        vec![(cell("01:00", "c"), 200, Some("r2".to_string()))]
    );
    assert_eq!(capped.len(), 2);
}
