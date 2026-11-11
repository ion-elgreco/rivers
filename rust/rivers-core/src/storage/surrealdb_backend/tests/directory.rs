use super::*;
use crate::assets::remote::LocationRef;
use crate::storage::{CodeLocationDirectoryEntry, ComponentRecord, RemoteConsumer};

fn loc(namespace: &str, name: &str) -> LocationRef {
    LocationRef::new(namespace, name).unwrap()
}

#[tokio::test]
async fn code_location_identity_round_trips() {
    let storage = make_storage().await;
    let wh = loc("team", "wh");

    assert_eq!(storage.get_code_location_identity(&wh).await.unwrap(), None);

    storage
        .upsert_code_location_registration(&wh, CodeLocationDirectoryEntry::new("uuid-1"))
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
        .upsert_code_location_registration(&wh, CodeLocationDirectoryEntry::new("uuid-1"))
        .await
        .unwrap();
    storage
        .upsert_code_location_registration(&wh, CodeLocationDirectoryEntry::new("uuid-2"))
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
        .upsert_code_location_registration(
            &loc("team", "wh"),
            CodeLocationDirectoryEntry::new("uuid-team"),
        )
        .await
        .unwrap();
    storage
        .upsert_code_location_registration(
            &loc("dev", "wh"),
            CodeLocationDirectoryEntry::new("uuid-dev"),
        )
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
        .upsert_code_location_registration(
            &loc("team", "wh"),
            CodeLocationDirectoryEntry::new("uuid-1"),
        )
        .await
        .unwrap();

    let raw = storage
        .kv_get("code_location:team/wh")
        .await
        .unwrap()
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(
        value,
        serde_json::json!({
            "identity": "uuid-1",
            "schema_version": SCHEMA_VERSION,
            "rivers_version": crate::RIVERS_VERSION,
        })
    );
}

/// Listing every directory entry must range-scan `idx_kv_key`, not walk the
/// kv table: it also holds one `dynamic_keys:*` row per fan-out
/// materialization.
#[tokio::test]
async fn code_location_directory_range_uses_kv_index() {
    let storage = make_storage().await;
    for (namespace, name) in [("team", "wh"), ("team", "analytics"), ("dev", "default")] {
        storage
            .upsert_code_location_registration(
                &loc(namespace, name),
                CodeLocationDirectoryEntry::new(name),
            )
            .await
            .unwrap();
    }
    for key in [
        "graph_topology:uuid-1",
        "cancel:run-1",
        "code_location",
        "code_location;",
        "dynamic_keys:uuid-1:a:_:dv1",
    ] {
        storage.kv_set(key, b"{}").await.unwrap();
    }

    let range = super::super::directory::DIRECTORY_RANGE;

    let mut locations: Vec<String> = storage
        .list_code_locations()
        .await
        .unwrap()
        .into_iter()
        .map(|(location, _)| location.to_string())
        .collect();
    locations.sort();
    assert_eq!(locations, ["dev/default", "team/analytics", "team/wh"]);

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

fn entry(identity: &str) -> CodeLocationDirectoryEntry {
    CodeLocationDirectoryEntry {
        module: Some("pipelines".to_string()),
        remotes: vec![RemoteConsumer {
            local: "orders".to_string(),
            code_location: loc("team", "wh"),
            asset_key: "raw_orders".to_string(),
            downstream: vec!["order_count".to_string()],
        }],
        ..CodeLocationDirectoryEntry::new(identity)
    }
}

#[tokio::test]
async fn phase_one_entry_decodes() {
    let storage = make_storage().await;
    storage
        .kv_set("code_location:team/wh", br#"{"identity":"uuid-1"}"#)
        .await
        .unwrap();

    assert_eq!(
        storage
            .get_code_location_entry(&loc("team", "wh"))
            .await
            .unwrap(),
        Some(CodeLocationDirectoryEntry {
            identity: "uuid-1".to_string(),
            ..Default::default()
        })
    );
}

/// A build that does not know the version fields writes the entry back
/// without them. The next heartbeat of this build records them again.
#[tokio::test]
async fn touch_records_the_build_again() {
    let storage = make_storage().await;
    let wh = loc("team", "wh");
    storage
        .kv_set("code_location:team/wh", br#"{"identity":"uuid-1"}"#)
        .await
        .unwrap();

    assert!(
        storage
            .touch_code_location(&wh, "uuid-1", "127.0.0.1:50051", 100)
            .await
            .unwrap()
    );

    let entry = storage.get_code_location_entry(&wh).await.unwrap().unwrap();
    assert_eq!(entry.schema_version, Some(SCHEMA_VERSION));
    assert_eq!(entry.rivers_version.as_deref(), Some(crate::RIVERS_VERSION));
}

#[tokio::test]
async fn components_round_trip() {
    let storage = make_storage().await;
    assert_eq!(storage.list_components().await.unwrap(), vec![]);

    storage.record_component("ui", false, 100).await.unwrap();
    storage
        .record_component("operator", true, 200)
        .await
        .unwrap();
    storage
        .record_component("operator", true, 300)
        .await
        .unwrap();
    storage
        .upsert_code_location_registration(&wh_entry_location(), entry("uuid-1"))
        .await
        .unwrap();

    let build = |writes, started_at| ComponentRecord {
        schema_version: SCHEMA_VERSION,
        rivers_version: crate::RIVERS_VERSION.to_string(),
        writes,
        started_at,
    };
    assert_eq!(
        storage.list_components().await.unwrap(),
        vec![
            ("operator".to_string(), build(true, 300)),
            ("ui".to_string(), build(false, 100)),
        ]
    );
    assert_eq!(storage.list_code_locations().await.unwrap().len(), 1);
}

fn wh_entry_location() -> LocationRef {
    loc("team", "wh")
}

#[tokio::test]
async fn listing_skips_an_entry_that_does_not_decode() {
    let storage = make_storage().await;
    storage
        .upsert_code_location_registration(&loc("team", "wh"), entry("uuid-1"))
        .await
        .unwrap();
    storage
        .kv_set("code_location:team/broken", b"not json")
        .await
        .unwrap();

    assert_eq!(
        storage.list_code_locations().await.unwrap(),
        vec![(loc("team", "wh"), entry("uuid-1"))]
    );
}

#[tokio::test]
async fn registration_keeps_the_endpoint_of_the_same_identity() {
    let storage = make_storage().await;
    let wh = loc("team", "wh");
    storage
        .upsert_code_location_registration(&wh, CodeLocationDirectoryEntry::new("uuid-1"))
        .await
        .unwrap();
    assert!(
        storage
            .touch_code_location(&wh, "uuid-1", "127.0.0.1:50051", 100)
            .await
            .unwrap()
    );

    storage
        .upsert_code_location_registration(&wh, entry("uuid-1"))
        .await
        .unwrap();
    let kept = storage.get_code_location_entry(&wh).await.unwrap().unwrap();
    assert_eq!(kept.grpc_endpoint.as_deref(), Some("127.0.0.1:50051"));
    assert_eq!(kept.last_seen, Some(100));
    assert_eq!(kept.remotes, entry("uuid-1").remotes);

    storage
        .upsert_code_location_registration(&wh, entry("uuid-2"))
        .await
        .unwrap();
    assert_eq!(
        storage.get_code_location_entry(&wh).await.unwrap(),
        Some(entry("uuid-2"))
    );
}

/// A process without `RIVERS_MODULE` (a one-off `rivers materialize`)
/// registers without a module; the serving location's module stays. Its
/// remotes follow the registering process.
#[tokio::test]
async fn registration_without_a_module_keeps_the_module_of_the_same_identity() {
    let storage = make_storage().await;
    let wh = loc("team", "wh");
    storage
        .upsert_code_location_registration(&wh, entry("uuid-1"))
        .await
        .unwrap();

    storage
        .upsert_code_location_registration(&wh, CodeLocationDirectoryEntry::new("uuid-1"))
        .await
        .unwrap();
    assert_eq!(
        storage.get_code_location_entry(&wh).await.unwrap(),
        Some(CodeLocationDirectoryEntry {
            module: Some("pipelines".to_string()),
            ..CodeLocationDirectoryEntry::new("uuid-1")
        })
    );

    let renamed = CodeLocationDirectoryEntry {
        module: Some("warehouse.defs".to_string()),
        ..entry("uuid-1")
    };
    storage
        .upsert_code_location_registration(&wh, renamed.clone())
        .await
        .unwrap();
    assert_eq!(
        storage.get_code_location_entry(&wh).await.unwrap(),
        Some(renamed)
    );

    storage
        .upsert_code_location_registration(&wh, CodeLocationDirectoryEntry::new("uuid-2"))
        .await
        .unwrap();
    assert_eq!(
        storage.get_code_location_entry(&wh).await.unwrap(),
        Some(CodeLocationDirectoryEntry::new("uuid-2"))
    );
}

#[tokio::test]
async fn touch_needs_an_entry_of_the_same_identity() {
    let storage = make_storage().await;
    let wh = loc("team", "wh");

    assert!(
        !storage
            .touch_code_location(&wh, "uuid-1", "127.0.0.1:50051", 100)
            .await
            .unwrap()
    );
    assert_eq!(storage.get_code_location_entry(&wh).await.unwrap(), None);

    storage
        .upsert_code_location_registration(&wh, entry("uuid-2"))
        .await
        .unwrap();
    assert!(
        !storage
            .touch_code_location(&wh, "uuid-1", "127.0.0.1:50051", 100)
            .await
            .unwrap()
    );
    assert_eq!(
        storage.get_code_location_entry(&wh).await.unwrap(),
        Some(entry("uuid-2"))
    );
}

#[tokio::test]
async fn delete_needs_the_same_identity() {
    let storage = make_storage().await;
    let wh = loc("team", "wh");
    storage
        .upsert_code_location_registration(&wh, entry("uuid-2"))
        .await
        .unwrap();

    assert!(
        !storage
            .delete_code_location_if(&wh, "uuid-1")
            .await
            .unwrap()
    );
    assert!(
        storage
            .get_code_location_entry(&wh)
            .await
            .unwrap()
            .is_some()
    );

    assert!(
        storage
            .delete_code_location_if(&wh, "uuid-2")
            .await
            .unwrap()
    );
    assert_eq!(storage.get_code_location_entry(&wh).await.unwrap(), None);
    assert!(
        !storage
            .delete_code_location_if(&wh, "uuid-2")
            .await
            .unwrap()
    );
}

/// A writer that read the entry before another writer changed it writes
/// nothing.
#[tokio::test]
async fn compare_and_set_leaves_a_changed_entry() {
    let storage = make_storage().await;
    let wh = loc("team", "wh");
    let key = crate::code_location_directory_key(&wh);
    storage
        .upsert_code_location_registration(&wh, entry("uuid-1"))
        .await
        .unwrap();
    let read = storage.kv_get(&key).await.unwrap().unwrap();
    storage
        .touch_code_location(&wh, "uuid-1", "127.0.0.1:50051", 100)
        .await
        .unwrap();
    let changed = storage.kv_get(&key).await.unwrap().unwrap();

    assert!(!storage.kv_compare_and_delete(&key, &read).await.unwrap());
    assert!(
        !storage
            .kv_compare_and_set(&key, Some(&read), b"{}")
            .await
            .unwrap()
    );
    assert!(!storage.kv_compare_and_set(&key, None, b"{}").await.unwrap());
    assert_eq!(storage.kv_get(&key).await.unwrap().unwrap(), changed);

    assert!(
        storage
            .kv_compare_and_set(&key, Some(&changed), &read)
            .await
            .unwrap()
    );
    assert_eq!(storage.kv_get(&key).await.unwrap().unwrap(), read);
}

/// Concurrent heartbeats and registrations of one location lose neither the
/// endpoint nor the remotes. Embedded, as kv-mem misses some write-write
/// conflicts.
#[tokio::test]
async fn concurrent_writers_lose_no_update() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = std::sync::Arc::new(
        SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .unwrap(),
    );
    let wh = loc("team", "wh");
    storage
        .upsert_code_location_registration(&wh, CodeLocationDirectoryEntry::new("uuid-1"))
        .await
        .unwrap();

    let mut writers = tokio::task::JoinSet::new();
    for i in 0..8 {
        let storage = std::sync::Arc::clone(&storage);
        let wh = wh.clone();
        writers.spawn(async move {
            if i % 2 == 0 {
                storage
                    .touch_code_location(&wh, "uuid-1", "127.0.0.1:50051", i)
                    .await
                    .map(drop)
            } else {
                storage
                    .upsert_code_location_registration(&wh, entry("uuid-1"))
                    .await
            }
        });
    }
    while let Some(written) = writers.join_next().await {
        written.unwrap().unwrap();
    }

    let last = storage.get_code_location_entry(&wh).await.unwrap().unwrap();
    assert_eq!(last.grpc_endpoint.as_deref(), Some("127.0.0.1:50051"));
    assert_eq!(last.remotes, entry("uuid-1").remotes);
}
