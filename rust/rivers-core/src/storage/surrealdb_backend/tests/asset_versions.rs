use std::sync::Arc;

use super::*;
use crate::storage::{
    AssetVersionHistoryEntry, AssetVersionRecord, AssetVersionState, MigrationKind,
};

const CL: &str = DEFAULT_CODE_LOCATION_ID;

fn version(asset_key: &str, applied_version: u32) -> AssetVersionRecord {
    AssetVersionRecord {
        code_location_id: "ignored".to_string(),
        asset_key: asset_key.to_string(),
        applied_version,
        partitions: None,
        state: AssetVersionState::Ready,
        target_version: None,
        reason: None,
        started_at: None,
        run_id: None,
        backfill_id: None,
        owner: None,
        lease_expires_at: None,
        revision: 0,
        updated_at: 0,
    }
}

/// `record` as it is stored in `cl` at `revision`, written at `updated_at`.
fn stored(
    cl: &str,
    record: &AssetVersionRecord,
    revision: u64,
    updated_at: i64,
) -> AssetVersionRecord {
    AssetVersionRecord {
        code_location_id: cl.to_string(),
        revision,
        updated_at,
        ..record.clone()
    }
}

fn history(asset_key: &str, version: u32, kind: MigrationKind) -> AssetVersionHistoryEntry {
    AssetVersionHistoryEntry {
        code_location_id: "ignored".to_string(),
        asset_key: asset_key.to_string(),
        version,
        kind,
        breaking: true,
        run_id: None,
        backfill_id: None,
        filled: None,
        failed: None,
        finished_at: 1_000,
    }
}

async fn read_one(storage: &AnyStorage, cl: &str, asset_key: &str) -> Option<AssetVersionRecord> {
    let mut rows = storage
        .get_asset_versions(cl, Some(&[asset_key.to_string()]))
        .await
        .unwrap();
    assert!(rows.len() <= 1, "{rows:?}");
    rows.pop()
}

#[tokio::test]
async fn a_new_row_is_written_once() {
    let storage = make_storage().await;
    let record = AssetVersionRecord {
        partitions: Some(r#"{"daily":{"start":"2026-01-01"}}"#.to_string()),
        revision: 41,
        updated_at: 5,
        ..version("orders", 2)
    };

    let before = now_nanos();
    assert!(storage.put_asset_version(CL, &record, None).await.unwrap());
    let after = now_nanos();

    let row = read_one(&storage, CL, "orders").await.unwrap();
    assert!(
        (before..=after).contains(&row.updated_at),
        "{before} <= {} <= {after}",
        row.updated_at
    );
    assert_eq!(row, stored(CL, &record, 1, row.updated_at));

    let again = AssetVersionRecord {
        state: AssetVersionState::Pending,
        ..record.clone()
    };
    assert!(!storage.put_asset_version(CL, &again, None).await.unwrap());
    assert_eq!(read_one(&storage, CL, "orders").await.unwrap(), row);
}

#[tokio::test]
async fn a_write_needs_the_revision_that_it_read() {
    let storage = make_storage().await;
    let ready = version("orders", 1);
    assert!(storage.put_asset_version(CL, &ready, None).await.unwrap());

    let draining = AssetVersionRecord {
        partitions: Some("{}".to_string()),
        state: AssetVersionState::Draining,
        target_version: Some(3),
        reason: Some("orders is migrating to version 3".to_string()),
        started_at: Some(100),
        run_id: Some("run-1".to_string()),
        backfill_id: Some("backfill-1".to_string()),
        owner: Some("daemon-1".to_string()),
        lease_expires_at: Some(200),
        ..version("orders", 1)
    };
    assert!(
        storage
            .put_asset_version(CL, &draining, Some(1))
            .await
            .unwrap()
    );
    let row = read_one(&storage, CL, "orders").await.unwrap();
    assert_eq!(row, stored(CL, &draining, 2, row.updated_at));

    // A writer that read another revision writes nothing.
    let stale = AssetVersionRecord {
        owner: Some("daemon-2".to_string()),
        ..draining.clone()
    };
    assert!(
        !storage
            .put_asset_version(CL, &stale, Some(1))
            .await
            .unwrap()
    );
    assert!(
        !storage
            .put_asset_version(CL, &stale, Some(3))
            .await
            .unwrap()
    );
    assert_eq!(read_one(&storage, CL, "orders").await.unwrap(), row);

    // A write clears the fields that the record leaves out.
    let migrated = version("orders", 3);
    assert!(
        storage
            .put_asset_version(CL, &migrated, Some(2))
            .await
            .unwrap()
    );
    let row = read_one(&storage, CL, "orders").await.unwrap();
    assert_eq!(row, stored(CL, &migrated, 3, row.updated_at));

    // An update never creates a row.
    assert!(
        !storage
            .put_asset_version(CL, &version("customers", 1), Some(1))
            .await
            .unwrap()
    );
    assert_eq!(read_one(&storage, CL, "customers").await, None);
}

#[tokio::test]
async fn rows_are_read_by_key_or_all_in_key_order() {
    let storage = make_storage().await;
    for (key, applied) in [("orders", 2), ("customers", 1), ("payments", 4)] {
        assert!(
            storage
                .put_asset_version(CL, &version(key, applied), None)
                .await
                .unwrap()
        );
    }
    let keys_and_versions = |rows: Vec<AssetVersionRecord>| -> Vec<(String, u32)> {
        rows.into_iter()
            .map(|row| (row.asset_key, row.applied_version))
            .collect()
    };

    let all = storage.get_asset_versions(CL, None).await.unwrap();
    assert_eq!(
        keys_and_versions(all),
        vec![
            ("customers".to_string(), 1),
            ("orders".to_string(), 2),
            ("payments".to_string(), 4),
        ]
    );

    let some = storage
        .get_asset_versions(
            CL,
            Some(&[
                "payments".to_string(),
                "missing".to_string(),
                "customers".to_string(),
            ]),
        )
        .await
        .unwrap();
    assert_eq!(
        keys_and_versions(some),
        vec![("customers".to_string(), 1), ("payments".to_string(), 4)]
    );

    assert_eq!(
        storage.get_asset_versions(CL, Some(&[])).await.unwrap(),
        vec![]
    );
}

#[tokio::test]
async fn history_is_upserted_by_version_and_read_oldest_first() {
    let storage = make_storage().await;
    let rebuild = AssetVersionHistoryEntry {
        backfill_id: Some("backfill-1".to_string()),
        filled: Some(1_000_000),
        failed: Some(3),
        finished_at: 3_000,
        ..history("orders", 3, MigrationKind::Rebuild)
    };
    let convert = AssetVersionHistoryEntry {
        run_id: Some("run-1".to_string()),
        breaking: false,
        ..history("orders", 2, MigrationKind::Convert)
    };
    for entry in [
        &rebuild,
        &convert,
        &history("customers", 2, MigrationKind::VersionOnly),
    ] {
        storage.add_asset_version_history(CL, entry).await.unwrap();
    }

    // A second entry of version 2: a retried migration that recorded again.
    let retried = AssetVersionHistoryEntry {
        run_id: Some("run-2".to_string()),
        finished_at: 2_000,
        ..convert.clone()
    };
    storage
        .add_asset_version_history(CL, &retried)
        .await
        .unwrap();

    let in_cl = |entry: &AssetVersionHistoryEntry| AssetVersionHistoryEntry {
        code_location_id: CL.to_string(),
        ..entry.clone()
    };
    assert_eq!(
        storage
            .get_asset_version_history(CL, "orders")
            .await
            .unwrap(),
        vec![in_cl(&retried), in_cl(&rebuild)]
    );
    assert_eq!(
        storage
            .get_asset_version_history(CL, "missing")
            .await
            .unwrap(),
        vec![]
    );
}

#[tokio::test]
async fn code_locations_do_not_see_the_rows_of_each_other() {
    let storage = make_storage().await;
    let (a, b) = ("team-a", "team-b");
    assert!(
        storage
            .put_asset_version(a, &version("orders", 2), None)
            .await
            .unwrap()
    );
    assert!(
        storage
            .put_asset_version(b, &version("orders", 5), None)
            .await
            .unwrap()
    );

    let pending = AssetVersionRecord {
        state: AssetVersionState::Pending,
        target_version: Some(3),
        ..version("orders", 2)
    };
    assert!(
        storage
            .put_asset_version(a, &pending, Some(1))
            .await
            .unwrap()
    );

    let row_a = read_one(&storage, a, "orders").await.unwrap();
    assert_eq!(row_a, stored(a, &pending, 2, row_a.updated_at));
    let row_b = read_one(&storage, b, "orders").await.unwrap();
    assert_eq!(row_b, stored(b, &version("orders", 5), 1, row_b.updated_at));
    assert_eq!(
        storage.get_asset_versions(b, None).await.unwrap(),
        vec![row_b]
    );
    assert_eq!(storage.get_asset_versions(CL, None).await.unwrap(), vec![]);

    let entry = history("orders", 2, MigrationKind::Convert);
    storage.add_asset_version_history(a, &entry).await.unwrap();
    assert_eq!(
        storage
            .get_asset_version_history(a, "orders")
            .await
            .unwrap(),
        vec![AssetVersionHistoryEntry {
            code_location_id: a.to_string(),
            ..entry
        }]
    );
    assert_eq!(
        storage
            .get_asset_version_history(b, "orders")
            .await
            .unwrap(),
        vec![]
    );
}

/// `writers` writers of `orders`, each its own owner, write at once and all
/// expect `revision`. Returns the owners whose write went through.
async fn race(storage: &Arc<AnyStorage>, revision: Option<u64>, writers: usize) -> Vec<String> {
    let barrier = Arc::new(tokio::sync::Barrier::new(writers));
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..writers {
        let (storage, barrier) = (Arc::clone(storage), Arc::clone(&barrier));
        tasks.spawn(async move {
            let owner = format!("daemon-{i}");
            let record = AssetVersionRecord {
                state: AssetVersionState::Draining,
                owner: Some(owner.clone()),
                ..version("orders", 1)
            };
            barrier.wait().await;
            let written = storage
                .put_asset_version(CL, &record, revision)
                .await
                .unwrap();
            written.then_some(owner)
        });
    }

    let mut winners = Vec::new();
    while let Some(winner) = tasks.join_next().await {
        winners.extend(winner.unwrap());
    }
    winners
}

/// Of the writers that read one revision, one writes and the others learn
/// that they lost. On disk, as the in-memory store misses some write-write
/// conflicts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_of_one_revision_write_once() {
    const WRITERS: usize = 8;
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = Arc::new(make_disk_storage(temp_dir.as_path_untracked()).await);

    let created = race(&storage, None, WRITERS).await;
    assert_eq!(created.len(), 1, "{created:?}");

    for revision in 1..=5 {
        let winners = race(&storage, Some(revision), WRITERS).await;
        assert_eq!(winners.len(), 1, "revision {revision}: {winners:?}");

        let row = read_one(&storage, CL, "orders").await.unwrap();
        assert_eq!(row.revision, revision + 1);
        assert_eq!(row.owner.as_ref(), winners.first());
    }
}
