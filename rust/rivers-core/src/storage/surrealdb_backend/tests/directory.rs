use super::*;
use crate::assets::remote::LocationRef;

fn loc(namespace: &str, name: &str) -> LocationRef {
    LocationRef::new(namespace, name).unwrap()
}

#[tokio::test]
async fn code_location_identity_round_trips() {
    let storage = make_storage().await;
    let wh = loc("team", "wh");

    assert_eq!(storage.get_code_location_identity(&wh).await.unwrap(), None);

    storage
        .set_code_location_identity(&wh, "uuid-1")
        .await
        .unwrap();
    assert_eq!(
        storage.get_code_location_identity(&wh).await.unwrap(),
        Some("uuid-1".to_string())
    );
}

/// A recreated CodeLocation gets a new identity; the entry follows it.
#[tokio::test]
async fn code_location_identity_is_replaced() {
    let storage = make_storage().await;
    let wh = loc("team", "wh");

    storage
        .set_code_location_identity(&wh, "uuid-1")
        .await
        .unwrap();
    storage
        .set_code_location_identity(&wh, "uuid-2")
        .await
        .unwrap();

    assert_eq!(
        storage.get_code_location_identity(&wh).await.unwrap(),
        Some("uuid-2".to_string())
    );
}

#[tokio::test]
async fn code_location_identity_is_per_namespace() {
    let storage = make_storage().await;

    storage
        .set_code_location_identity(&loc("team", "wh"), "uuid-team")
        .await
        .unwrap();
    storage
        .set_code_location_identity(&loc("dev", "wh"), "uuid-dev")
        .await
        .unwrap();

    assert_eq!(
        storage
            .get_code_location_identity(&loc("team", "wh"))
            .await
            .unwrap(),
        Some("uuid-team".to_string())
    );
    assert_eq!(
        storage
            .get_code_location_identity(&loc("dev", "wh"))
            .await
            .unwrap(),
        Some("uuid-dev".to_string())
    );
}

/// The entry is a JSON object, so later fields can be added without a
/// format change.
#[tokio::test]
async fn code_location_directory_wire_format() {
    let storage = make_storage().await;
    storage
        .set_code_location_identity(&loc("team", "wh"), "uuid-1")
        .await
        .unwrap();

    let raw = storage
        .kv_get("code_location:team/wh")
        .await
        .unwrap()
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(value, serde_json::json!({ "identity": "uuid-1" }));
}

/// Listing every directory entry must range-scan `idx_kv_key`, not walk the
/// kv table: it also holds one `dynamic_keys:*` row per fan-out
/// materialization.
#[tokio::test]
async fn code_location_directory_range_uses_kv_index() {
    let storage = make_storage().await;
    for (namespace, name) in [("team", "wh"), ("team", "analytics"), ("dev", "default")] {
        storage
            .set_code_location_identity(&loc(namespace, name), name)
            .await
            .unwrap();
    }
    for key in [
        "graph_topology:uuid-1",
        "cancel:run-1",
        "code_locatio",
        "code_location;",
        "dynamic_keys:uuid-1:a:_:dv1",
    ] {
        storage.kv_set(key, b"{}").await.unwrap();
    }

    let range = "SELECT VALUE key FROM kv \
                 WHERE key >= 'code_location:' AND key < 'code_location;'";

    let mut keys: Vec<String> = storage.db.query(range).await.unwrap().take(0).unwrap();
    keys.sort();
    assert_eq!(
        keys,
        [
            "code_location:dev/default",
            "code_location:team/analytics",
            "code_location:team/wh",
        ]
    );

    let plan: Vec<serde_json::Value> = storage
        .db
        .query(format!("{range} EXPLAIN"))
        .await
        .unwrap()
        .take(0)
        .unwrap();
    let plan = serde_json::to_string(&plan).unwrap();
    assert!(
        plan.contains("\"index\":\"idx_kv_key\"")
            && plan.contains(">='code_location:' <'code_location;'"),
        "directory listing should range-scan idx_kv_key: {plan}"
    );
    assert!(
        !plan.contains("Filter") && !plan.contains("TableScan"),
        "the range bounds alone should select directory rows: {plan}"
    );
}
