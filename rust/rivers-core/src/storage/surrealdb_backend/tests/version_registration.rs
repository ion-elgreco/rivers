use std::sync::Arc;

use super::*;
use crate::assets::graph::{NodeKind, TopologyNode};
use crate::assets::partitions_fingerprint::PartitionsFingerprint;
use crate::assets::versions::register_asset_versions;
use crate::storage::{AssetVersionRecord, AssetVersionState, CodeLocationContext};

const CL: &str = DEFAULT_CODE_LOCATION_ID;

/// A topology of assets, each with its partitions definition.
fn topology(nodes: &[(&str, Option<&PartitionsFingerprint>)]) -> GraphTopology {
    GraphTopology {
        nodes: nodes
            .iter()
            .map(|(name, partitions)| TopologyNode {
                partitions: partitions.cloned(),
                ..TopologyNode::new(*name, NodeKind::Asset)
            })
            .collect(),
        edges: vec![],
    }
}

async fn register_versions(storage: &AnyStorage, declared: &[(&str, u32)], code: &GraphTopology) {
    let cl = CodeLocationContext::new(CL);
    register_asset_versions(&storage.for_code_location(&cl), declared, code)
        .await
        .unwrap();
}

async fn version_row(storage: &AnyStorage, asset_key: &str) -> AssetVersionRecord {
    let mut rows = storage
        .get_asset_versions(CL, Some(&[asset_key.to_string()]))
        .await
        .unwrap();
    rows.pop().expect("a version row")
}

/// The applied version, the state and the target version of `row`.
fn version(row: &AssetVersionRecord) -> (u32, AssetVersionState, Option<u32>) {
    (row.applied_version, row.state, row.target_version)
}

fn json(partitions: &PartitionsFingerprint) -> Option<String> {
    Some(serde_json::to_string(partitions).unwrap())
}

#[tokio::test]
async fn only_assets_that_declare_a_version_above_1_get_a_row() {
    let storage = make_storage().await;
    let code = topology(&[("orders", None), ("customers", None)]);

    register_versions(&storage, &[("orders", 3), ("customers", 1)], &code).await;

    let rows = storage.get_asset_versions(CL, None).await.unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| (row.asset_key.as_str(), version(row)))
            .collect::<Vec<_>>(),
        vec![("orders", (3, AssetVersionState::Ready, None))]
    );
}

/// Data stored before the first row is at version 1, in the partitions
/// definition of the stored topology; an asset without stored data starts
/// at its declared version, in the code's definition.
#[tokio::test]
async fn stored_data_waits_for_a_migration_from_version_1() {
    let storage = make_storage().await;
    register(&storage, &["orders", "payments", "customers"]).await;
    for asset in ["orders", "payments"] {
        storage
            .store_event(&make_event(asset, "run-1", 100))
            .await
            .unwrap();
    }
    let stored_keys = PartitionsFingerprint::static_keys(["eu", "us"]);
    let code_keys = PartitionsFingerprint::static_keys(["eu", "us", "apac"]);
    storage
        .set_graph_topology(CL, &topology(&[("orders", Some(&stored_keys))]), &[])
        .await
        .unwrap();
    let code = topology(&[
        ("orders", Some(&code_keys)),
        ("payments", Some(&code_keys)),
        ("customers", Some(&code_keys)),
    ]);

    register_versions(
        &storage,
        &[("orders", 2), ("payments", 3), ("customers", 2)],
        &code,
    )
    .await;

    let orders = version_row(&storage, "orders").await;
    assert_eq!(version(&orders), (1, AssetVersionState::Pending, Some(2)));
    assert_eq!(orders.partitions, json(&stored_keys));
    // The stored topology has no node for it: the code's definition.
    let payments = version_row(&storage, "payments").await;
    assert_eq!(version(&payments), (1, AssetVersionState::Pending, Some(3)));
    assert_eq!(payments.partitions, json(&code_keys));
    let customers = version_row(&storage, "customers").await;
    assert_eq!(version(&customers), (2, AssetVersionState::Ready, None));
    assert_eq!(customers.partitions, json(&code_keys));
}

#[tokio::test]
async fn registration_changes_a_row_only_for_a_higher_version() {
    let storage = make_storage().await;
    let code = topology(&[("orders", None)]);
    register_versions(&storage, &[("orders", 2)], &code).await;

    // The same code again, as at a dev reload.
    register_versions(&storage, &[("orders", 2)], &code).await;
    let row = version_row(&storage, "orders").await;
    assert_eq!(version(&row), (2, AssetVersionState::Ready, None));
    assert_eq!(row.revision, 1);

    register_versions(&storage, &[("orders", 4)], &code).await;
    let row = version_row(&storage, "orders").await;
    assert_eq!(version(&row), (2, AssetVersionState::Pending, Some(4)));
    assert_eq!(row.revision, 2);

    // Old code during a rollout.
    register_versions(&storage, &[("orders", 3)], &code).await;
    assert_eq!(version_row(&storage, "orders").await, row);
}

/// Writers that lose a race read the row again and decide again, so the
/// highest declared version wins in any order. On disk, as the in-memory
/// store misses some write-write conflicts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_registrations_end_at_the_highest_version() {
    const WRITERS: u32 = 8;
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = Arc::new(make_disk_storage(temp_dir.as_path_untracked()).await);
    register(storage.as_ref(), &["orders"]).await;
    storage
        .store_event(&make_event("orders", "run-1", 100))
        .await
        .unwrap();
    let code = topology(&[("orders", None)]);

    let barrier = Arc::new(tokio::sync::Barrier::new(WRITERS as usize));
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..WRITERS {
        let (storage, barrier, code) = (Arc::clone(&storage), Arc::clone(&barrier), code.clone());
        tasks.spawn(async move {
            barrier.wait().await;
            let cl = CodeLocationContext::new(CL);
            let declared = [("orders", 2 + i % 3)];
            register_asset_versions(&storage.for_code_location(&cl), &declared, &code).await
        });
    }
    while let Some(registered) = tasks.join_next().await {
        registered.unwrap().unwrap();
    }

    let row = version_row(&storage, "orders").await;
    assert_eq!(version(&row), (1, AssetVersionState::Pending, Some(4)));
}
