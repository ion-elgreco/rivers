//! Reads of an asset's partitions that do not grow with the asset: the rows
//! of the keys of one page, one page of the materialized keys, and the
//! failures without the times of every partition.

use std::collections::HashMap;

use super::*;
use crate::storage::surrealdb_backend::queries::PARTITION_TIMESTAMPS_OF_KEY;

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
    wanted.push(single("not-there"));
    let rows: HashMap<PartitionKey, i64> = storage
        .get_partition_timestamps_for_keys(DEFAULT_CODE_LOCATION_ID, "plain", &wanted)
        .await
        .unwrap()
        .into_iter()
        .map(|(key, time, _)| (key, time))
        .collect();

    let expected: HashMap<PartitionKey, i64> = keys
        .into_iter()
        .enumerate()
        .map(|(i, key)| (key, 1_000 + i as i64))
        .collect();
    assert_eq!(rows, expected);
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

    let statement = PARTITION_TIMESTAMPS_OF_KEY.replace("$partition", "$partitions[0]");
    let plan: Vec<serde_json::Value> = storage
        .db
        .query(format!("{statement} EXPLAIN"))
        .bind(("cl", DEFAULT_CODE_LOCATION_ID.to_string()))
        .bind(("asset_key", "trips".to_string()))
        .bind(("partitions", vec![cell("00:00", "a")]))
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
