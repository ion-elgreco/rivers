//! The Events tab of an asset: one page of its events and their number.

use super::*;
use crate::storage::surrealdb_backend::queries::{
    ASSET_EVENTS_PAGE_OF_RARE_TYPE, COUNT_ASSET_EVENTS_OF_TYPE,
};

const SHOWN: [&str; 5] = [
    "Materialization",
    "Observation",
    "StepFailure",
    "ActionCompleted",
    "Deletion",
];

fn typed(asset: &str, event_type: EventType, timestamp: i64) -> EventRecord {
    EventRecord {
        event_type,
        ..make_event(asset, "r1", timestamp)
    }
}

/// Five materializations, two failures and one observation of `orders`, the
/// step events of their runs, and events of another asset.
async fn store_orders_events(storage: &SurrealStorage) {
    register(storage, &["orders", "other"]).await;
    let materialization = || EventType::Materialization { data_version: None };
    storage
        .store_events(&[
            typed("orders", materialization(), 10),
            typed("orders", materialization(), 20),
            typed("orders", EventType::StepFailure, 25),
            typed("orders", materialization(), 30),
            typed("orders", EventType::Observation { data_version: None }, 35),
            typed("orders", materialization(), 40),
            typed("orders", EventType::StepFailure, 45),
            typed("orders", materialization(), 50),
            typed("orders", EventType::StepStart, 9),
            typed("orders", EventType::StepSuccess, 11),
            typed("other", materialization(), 60),
            typed("other", EventType::StepFailure, 70),
        ])
        .await
        .unwrap();
}

/// The page of `orders` for `types`: the type and time of each row, and the total.
async fn page(
    storage: &SurrealStorage,
    types: &[&str],
    offset: u64,
    limit: u64,
) -> (Vec<(String, i64)>, u64) {
    let types: Vec<String> = types.iter().map(|t| t.to_string()).collect();
    let (rows, total) = storage
        .get_events_for_asset_page(DEFAULT_CODE_LOCATION_ID, "orders", &types, offset, limit)
        .await
        .unwrap();
    let rows = rows
        .into_iter()
        .map(|row| (row.event_type.type_name().to_string(), row.timestamp))
        .collect();
    (rows, total)
}

fn rows(expected: &[(&str, i64)]) -> Vec<(String, i64)> {
    expected
        .iter()
        .map(|(kind, timestamp)| (kind.to_string(), *timestamp))
        .collect()
}

#[tokio::test]
async fn asset_events_page_holds_the_newest_events_of_its_types() {
    let storage = make_storage().await;
    store_orders_events(&storage).await;

    assert_eq!(
        page(&storage, &SHOWN, 0, 3).await,
        (
            rows(&[
                ("Materialization", 50),
                ("StepFailure", 45),
                ("Materialization", 40)
            ]),
            8
        )
    );
    assert_eq!(
        page(&storage, &SHOWN, 6, 3).await,
        (rows(&[("Materialization", 20), ("Materialization", 10)]), 8)
    );
    assert_eq!(page(&storage, &SHOWN, 8, 3).await, (vec![], 8));

    assert_eq!(
        page(&storage, &["Materialization"], 1, 2).await,
        (rows(&[("Materialization", 40), ("Materialization", 30)]), 5)
    );
    assert_eq!(
        page(&storage, &["StepFailure"], 0, 50).await,
        (rows(&[("StepFailure", 45), ("StepFailure", 25)]), 2)
    );
    assert_eq!(
        page(&storage, &["StepFailure"], 1, 50).await,
        (rows(&[("StepFailure", 25)]), 2)
    );
    assert_eq!(
        page(&storage, &["ActionCompleted"], 0, 50).await,
        (vec![], 0)
    );
    assert_eq!(page(&storage, &[], 0, 50).await, (vec![], 0));
}

/// The plan of `statement` for the events of `orders`.
async fn plan_of(storage: &SurrealStorage, statement: &str, types: &[&str]) -> String {
    let types: Vec<String> = types.iter().map(|t| t.to_string()).collect();
    let plan: Vec<serde_json::Value> = storage
        .db
        .query(format!("{statement} EXPLAIN"))
        .bind(("cl", DEFAULT_CODE_LOCATION_ID.to_string()))
        .bind(("ak", "orders".to_string()))
        .bind(("types", types))
        .bind(("limit", 50))
        .bind(("offset", 0))
        .await
        .unwrap()
        .take(0)
        .unwrap();
    serde_json::to_string(&plan).unwrap()
}

/// An asset with a million partitions has a million materialization events.
/// The number of its events is counted on the index, without a read of each.
#[tokio::test]
async fn asset_events_are_counted_on_the_index() {
    let storage = make_storage().await;
    store_orders_events(&storage).await;

    let count = COUNT_ASSET_EVENTS_OF_TYPE.replace("$type", "$types[0]");
    let plan = plan_of(&storage, &count, &SHOWN).await;

    assert!(
        plan.contains(r#""operator":"IndexCountScan""#) && !plan.contains("Filter"),
        "the count should not read the events: {plan}"
    );
}

/// A healthy asset has few failures among its events. The page of the
/// failures alone reads them by their type, not every event of the asset in
/// time order.
#[tokio::test]
async fn a_page_of_a_rare_event_type_reads_the_events_of_that_type_only() {
    let storage = make_storage().await;
    store_orders_events(&storage).await;

    let page = ASSET_EVENTS_PAGE_OF_RARE_TYPE.replace("$type", "$types[0]");
    let plan = plan_of(&storage, &page, &["StepFailure"]).await;

    assert!(
        plan.contains("idx_events_loc_asset_type") && plan.contains("'StepFailure']"),
        "the page should reach the events by their type: {plan}"
    );
}
