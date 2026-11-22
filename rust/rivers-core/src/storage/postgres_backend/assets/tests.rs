use std::collections::BTreeMap;
use std::sync::Arc;

use super::*;
use crate::assets::graph::{NodeKind, TopologyNode};

async fn storage() -> Option<PostgresStorage> {
    let url = PostgresStorage::test_url()?;
    Some(PostgresStorage::connect_test_schema(&url).await.unwrap())
}

fn record(key: &str) -> AssetRecord {
    AssetRecord {
        code_location_id: "ignored".to_string(),
        asset_key: key.to_string(),
        tags: vec![],
        kinds: vec![],
        asset_group: None,
        code_version: None,
        last_event_id: None,
        last_run_id: None,
        last_timestamp: None,
        last_data_version: None,
        last_materialization_code_version: None,
        last_input_data_versions: vec![],
        pool: vec![],
    }
}

fn topology(name: &str) -> GraphTopology {
    GraphTopology {
        nodes: vec![TopologyNode::new(name, NodeKind::Asset)],
        edges: vec![],
    }
}

/// One statement takes the whole list, and a key given twice registers as
/// it would one record after the other.
#[tokio::test]
async fn register_assets_takes_a_large_list_with_repeated_keys() {
    let Some(storage) = storage().await else {
        return;
    };
    let first = AssetRecord {
        tags: vec!["old".to_string()],
        last_run_id: Some("run-1".to_string()),
        ..record("orders")
    };
    let tags: Vec<String> = ["new", "a,b", "{x}", "\"q\"", "NULL", ""]
        .iter()
        .map(|tag| tag.to_string())
        .collect();
    let second = AssetRecord {
        tags: tags.clone(),
        pool: vec![("cpu".to_string(), 2)],
        last_run_id: Some("run-2".to_string()),
        ..record("orders")
    };
    let mut records: Vec<AssetRecord> = (0..5_000).map(|i| record(&format!("a{i:05}"))).collect();
    records.extend([first, second]);
    storage.register_assets("cl", records).await.unwrap();

    let orders = storage
        .get_asset_record("cl", "orders")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(orders.code_location_id, "cl");
    assert_eq!(orders.tags, tags);
    assert_eq!(orders.pool, [("cpu".to_string(), 2)]);
    assert_eq!(orders.last_run_id.as_deref(), Some("run-1"));
    assert_eq!(storage.get_asset_records("cl").await.unwrap().len(), 5_001);
}

#[tokio::test]
async fn lists_are_in_byte_order() {
    let Some(storage) = storage().await else {
        return;
    };
    let keys = ["b", "B", "a", "_x", "é", "Z"];
    storage
        .register_assets("cl", keys.iter().map(|key| record(key)).collect())
        .await
        .unwrap();
    let owned: Vec<String> = keys.iter().map(|key| key.to_string()).collect();
    storage
        .add_dynamic_partitions("cl", "users", &owned)
        .await
        .unwrap();

    let mut sorted = owned.clone();
    sorted.sort();
    let assets: Vec<String> = storage
        .get_asset_records("cl")
        .await
        .unwrap()
        .into_iter()
        .map(|record| record.asset_key)
        .collect();
    assert_eq!(assets, sorted);
    assert_eq!(
        storage.get_dynamic_partitions("cl", "users").await.unwrap(),
        sorted
    );
}

/// Whichever topology write is the last, the key lists are the ones it names.
#[tokio::test]
async fn concurrent_topology_writes_leave_the_lists_of_the_last() {
    let Some(storage) = storage().await else {
        return;
    };
    let storage = Arc::new(storage);
    let list = |i: usize| vec![format!("k{i}"), "shared".to_string()];

    let mut writers = tokio::task::JoinSet::new();
    for i in 0..8 {
        let storage = Arc::clone(&storage);
        writers.spawn(async move {
            let keys = list(i);
            let lists = [
                KeyList::new(keys.iter().map(String::as_str)),
                KeyList::new(["shared"]),
            ];
            storage
                .set_graph_topology("cl", &topology(&i.to_string()), &lists)
                .await
        });
    }
    while let Some(written) = writers.join_next().await {
        written.unwrap().unwrap();
    }

    let last: usize = storage
        .get_graph_topology("cl")
        .await
        .unwrap()
        .unwrap()
        .nodes[0]
        .name
        .parse()
        .unwrap();
    let keys = list(last);
    let mut expected: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for list in [
        KeyList::new(keys.iter().map(String::as_str)),
        KeyList::new(["shared"]),
    ] {
        expected.insert(
            list.digest,
            list.keys.iter().map(|key| key.to_string()).collect(),
        );
    }
    assert_eq!(storage.key_lists("cl").await.unwrap(), expected);
}

/// Writers that give the same keys in opposite orders wait on each other
/// instead of failing on a deadlock.
#[tokio::test]
async fn writes_in_opposite_orders_do_not_deadlock() {
    let Some(storage) = storage().await else {
        return;
    };
    let storage = Arc::new(storage);
    let keys: Vec<String> = (0..2_000).map(|i| format!("k{i:04}")).collect();
    let start = Arc::new(tokio::sync::Barrier::new(8));

    let mut writers = tokio::task::JoinSet::new();
    for i in 0..8 {
        let (storage, start) = (Arc::clone(&storage), Arc::clone(&start));
        let mut keys = keys.clone();
        if i % 2 == 0 {
            keys.reverse();
        }
        writers.spawn(async move {
            start.wait().await;
            let records = keys.iter().map(|key| record(key)).collect();
            storage.register_assets("cl", records).await?;
            start.wait().await;
            storage.add_dynamic_partitions("cl", "users", &keys).await
        });
    }
    while let Some(written) = writers.join_next().await {
        written.unwrap().unwrap();
    }

    assert_eq!(storage.get_asset_records("cl").await.unwrap().len(), 2_000);
    assert_eq!(
        storage
            .count_dynamic_partitions("cl", "users")
            .await
            .unwrap(),
        2_000
    );
}
