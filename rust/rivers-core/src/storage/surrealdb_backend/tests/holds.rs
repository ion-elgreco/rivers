//! The hold row of an asset: the writers that change it and what each does
//! once more after its commit, and the materializations that read it.

use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use surrealdb::engine::any::Any;
use surrealdb::method::Transaction;

use super::failure_rows::{
    Numbers, built, deleted, failed, failure, keys, race, rows, run, single, store_on_disk,
};
use super::*;
use crate::storage::retry;

const CL: &str = DEFAULT_CODE_LOCATION_ID;

/// How many times the hold row of `asset` changed.
async fn hold_changes(storage: &SurrealStorage, asset: &str) -> i64 {
    let changes: Vec<i64> = storage
        .db
        .query("SELECT VALUE changes FROM type::record('asset_holds', [$cl, $asset_key])")
        .bind(("cl", CL.to_string()))
        .bind(("asset_key", asset.to_string()))
        .await
        .unwrap()
        .take(0)
        .unwrap();
    changes.first().copied().unwrap_or(0)
}

/// Add the row of a partition as a materialization does that commits while
/// another writer's transaction runs: that transaction does not see the row,
/// and the materialization does not see what that transaction writes.
async fn commit_unseen_row(
    storage: &SurrealStorage,
    asset: &str,
    key: &str,
    run_id: &str,
    at: i64,
) {
    storage
        .db
        .query("INSERT INTO asset_partitions $rows RETURN NONE")
        .bind(("rows", vec![partition_row(asset, key, run_id, at)]))
        .await
        .unwrap()
        .check()
        .unwrap();
}

fn partition_row(asset: &str, key: &str, run_id: &str, at: i64) -> DbAssetPartitionWrite {
    DbAssetPartitionWrite {
        code_location_id: CL.to_string(),
        asset_key: asset.to_string(),
        partition_key: single(key),
        last_event_id: format!("{run_id}-{key}"),
        last_run_id: run_id.to_string(),
        last_timestamp: at,
    }
}

/// A transaction with the statements of a materialization of one partition,
/// not committed.
async fn begin_materialization(
    storage: &SurrealStorage,
    asset: &str,
    key: &str,
    run_id: &str,
    at: i64,
) -> Transaction<Any> {
    let txn = storage.db.clone().begin().await.unwrap();
    txn.query(format!(
        "{UPSERT_ASSET_PARTITIONS}; {MATERIALIZE_ASSET_DATA} {MATERIALIZATION_IS_NEWER};"
    ))
    .bind(("cl", CL.to_string()))
    .bind(("asset_key", asset.to_string()))
    .bind(("event_id", format!("{run_id}-{key}")))
    .bind(("run_id", run_id.to_string()))
    .bind(("timestamp", at))
    .bind(("data_version", None::<String>))
    .bind(("keys", vec![single(key)]))
    .bind(("oldest", at))
    .bind(("rows", vec![partition_row(asset, key, run_id, at)]))
    .await
    .unwrap()
    .check()
    .unwrap();
    txn
}

/// The row of each partition of `asset`: its key, run and time, in the order
/// of the store.
async fn partition_rows(storage: &SurrealStorage, asset: &str) -> Vec<(PartitionKey, String, i64)> {
    let mut rows: Vec<(PartitionKey, String, i64)> = storage
        .db
        .query(
            "SELECT VALUE [partition_key, last_run_id, last_timestamp] FROM asset_partitions \
             WHERE code_location_id = $cl AND asset_key = $asset_key",
        )
        .bind(("cl", CL.to_string()))
        .bind(("asset_key", asset.to_string()))
        .await
        .unwrap()
        .take(0)
        .unwrap();
    rows.sort_by(|a, b| crate::storage::stored_order(&a.0, &b.0));
    rows
}

fn row(key: &str, run_id: &str, at: i64) -> (PartitionKey, String, i64) {
    (single(key), run_id.to_string(), at)
}

async fn embedded(temp_dir: &test_temp_dir::TestTempDir) -> SurrealStorage {
    SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
        .await
        .expect("failed to create embedded storage")
}

/// Add a materialization event as [`commit_unseen_row`] adds its row.
async fn commit_unseen_event(
    storage: &SurrealStorage,
    asset: &str,
    key: &str,
    run_id: &str,
    at: i64,
) {
    storage
        .db
        .query(
            "INSERT INTO events [{ code_location_id: $cl, run_id: $run_id, asset_key: $asset_key, \
             event_type: 'Materialization', partition_key: $key, timestamp: $at, metadata: [] }] \
             RETURN NONE",
        )
        .bind(("cl", CL.to_string()))
        .bind(("run_id", run_id.to_string()))
        .bind(("asset_key", asset.to_string()))
        .bind(("key", single(key)))
        .bind(("at", at))
        .await
        .unwrap()
        .check()
        .unwrap();
}

/// The partitions of `asset` that have a row, in the order of the store.
async fn materialized(storage: &SurrealStorage, asset: &str) -> Vec<PartitionKey> {
    let mut keys = storage
        .get_materialized_partitions(CL, asset)
        .await
        .unwrap();
    keys.sort_by(crate::storage::stored_order);
    keys
}

#[tokio::test]
async fn deletions_and_failures_change_the_hold_row_and_materializations_do_not() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;

    storage
        .store_event(&built("a", "r1", "p1", 100))
        .await
        .unwrap();
    storage
        .store_events(&[built("a", "r1", "p2", 101)])
        .await
        .unwrap();
    assert_eq!(hold_changes(&storage, "a").await, 0, "materializations");

    let mut before = 0;
    for (what, event) in [
        ("partition deletion", deleted("a", Some("p1"), 200)),
        ("step failure", failed("a", "r2", single("p2"), 300)),
        ("asset deletion", deleted("a", None, 400)),
    ] {
        for batched in [false, true] {
            let event = EventRecord {
                timestamp: event.timestamp + i64::from(batched),
                ..event.clone()
            };
            if batched {
                storage.store_events(&[event]).await.unwrap();
            } else {
                storage.store_event(&event).await.unwrap();
            }

            let after = hold_changes(&storage, "a").await;
            assert!(
                after > before,
                "{what}, batched={batched}: {before} -> {after}"
            );
            before = after;
        }
    }

    let failing = run("r3", RunStatus::Started, Some(keys(&["p5", "p6"])), &["a"]);
    storage.create_run(&failing).await.unwrap();
    storage
        .update_run_status("r3", RunStatus::Failure, Some(500))
        .await
        .unwrap();
    let after = hold_changes(&storage, "a").await;
    assert!(
        after > before,
        "end of a run in failure: {before} -> {after}"
    );
    before = after;

    assert!(storage.delete_run("r3").await.unwrap());
    let after = hold_changes(&storage, "a").await;
    assert!(
        after > before,
        "deletion of a failed run: {before} -> {after}"
    );
}

async fn assert_stopped(open: Transaction<Any>, what: &str) {
    let lost = open.commit().await.expect_err(what);
    assert!(retry::is_transient_surrealdb_error(&lost), "{what}: {lost}");
}

/// The transaction of a materialization stays open while a deletion or a
/// write of failure rows of its asset commits. Those write no row that the
/// materialization writes: the change of the hold row stops it.
#[tokio::test]
async fn a_change_of_the_hold_row_stops_a_materialization_that_began_before_it() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded(&temp_dir).await;
    register(&storage, &["a"]).await;
    storage
        .store_event(&built("a", "newest", "p0", 300))
        .await
        .unwrap();
    let failing = run("r3", RunStatus::Started, Some(keys(&["p5", "p6"])), &["a"]);
    storage.create_run(&failing).await.unwrap();

    for (what, event) in [
        ("partition deletion", deleted("a", Some("p9"), 200)),
        ("step failure", failed("a", "r2", single("p8"), 200)),
        ("asset deletion", deleted("a", None, 200)),
    ] {
        let open = begin_materialization(&storage, "a", "p1", "r1", 100).await;
        storage.store_event(&event).await.unwrap();
        assert_stopped(open, what).await;
    }

    let open = begin_materialization(&storage, "a", "p1", "r1", 100).await;
    storage
        .update_run_status("r3", RunStatus::Failure, Some(500))
        .await
        .unwrap();
    assert_stopped(open, "end of a run in failure").await;

    let open = begin_materialization(&storage, "a", "p1", "r1", 100).await;
    assert!(storage.delete_run("r3").await.unwrap());
    assert_stopped(open, "deletion of a failed run").await;
}

#[tokio::test]
async fn an_asset_deletion_done_once_more_removes_the_row_it_did_not_see() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    storage
        .store_event(&built("a", "r1", "p1", 300))
        .await
        .unwrap();
    storage.store_event(&deleted("a", None, 200)).await.unwrap();
    commit_unseen_row(&storage, "a", "p0", "r0", 100).await;
    assert_eq!(
        materialized(&storage, "a").await,
        [single("p0"), single("p1")]
    );
    let changes = hold_changes(&storage, "a").await;

    storage
        .clear_deleted_asset(CL, "a", "deletion", 200)
        .await
        .unwrap();

    assert_eq!(materialized(&storage, "a").await, [single("p1")]);
    assert_eq!(hold_changes(&storage, "a").await, changes + 1);
}

/// `with_failure`: a failure row is in the range of the keys, so the
/// deletion also brings the failure rows in line.
#[tokio::test]
async fn partition_deletions_done_once_more_remove_the_rows_they_did_not_see() {
    for with_failure in [false, true] {
        let storage = make_storage().await;
        register(&storage, &["a"]).await;
        if with_failure {
            storage
                .store_event(&failed("a", "r9", single("p0"), 250))
                .await
                .unwrap();
        }
        storage
            .store_events(&[deleted("a", Some("p0"), 200), deleted("a", Some("p1"), 200)])
            .await
            .unwrap();
        commit_unseen_row(&storage, "a", "p0", "r0", 100).await;
        commit_unseen_row(&storage, "a", "p1", "r1", 300).await;
        let changes = hold_changes(&storage, "a").await;

        let deletions = [(single("p0"), 200), (single("p1"), 200)];
        storage
            .delete_partitions_once(CL, "a", &deletions)
            .await
            .unwrap();

        assert_eq!(
            materialized(&storage, "a").await,
            [single("p1")],
            "with_failure={with_failure}"
        );
        assert_eq!(hold_changes(&storage, "a").await, changes + 1);
        if with_failure {
            assert_eq!(rows(&storage, "a").await, [failure("p0", 250, "r9", false)]);
        }
    }
}

#[tokio::test]
async fn failures_checked_once_more_take_in_the_rows_their_write_did_not_see() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    storage
        .store_events(&[
            failed("a", "r2", single("p1"), 200),
            failed("a", "r2", single("p2"), 200),
            failed("a", "r2", single("p3"), 200),
        ])
        .await
        .unwrap();
    commit_unseen_row(&storage, "a", "p1", "r1", 100).await;
    commit_unseen_row(&storage, "a", "p2", "r1", 300).await;
    let changes = hold_changes(&storage, "a").await;

    let keys = [single("p1"), single("p2"), single("p3")];
    storage
        .check_failures_again(CL, "a", &keys, None)
        .await
        .unwrap();

    assert_eq!(
        rows(&storage, "a").await,
        [
            failure("p1", 200, "r2", true),
            failure("p3", 200, "r2", false)
        ]
    );
    assert_eq!(hold_changes(&storage, "a").await, changes + 1);
}

/// The failures of a partition are checked once more while the transaction
/// of a deletion of the asset is open: the check takes in the partition row
/// that the deletion removes. Both change the hold row, so the deletion
/// fails to commit and runs again.
#[tokio::test]
async fn a_deletion_and_failures_checked_once_more_stop_each_other() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded(&temp_dir).await;
    register(&storage, &["a"]).await;
    storage
        .store_event(&failed("a", "r2", single("p1"), 300))
        .await
        .unwrap();
    commit_unseen_row(&storage, "a", "p1", "r1", 100).await;

    let deletion = storage.db.clone().begin().await.unwrap();
    deletion
        .query(format!("{CHANGE_HOLD}{CLEAR_DELETED_ASSET}"))
        .bind(("cl", CL.to_string()))
        .bind(("asset_key", "a".to_string()))
        .bind(("event_id", "deletion".to_string()))
        .bind(("ts", 200))
        .await
        .unwrap()
        .check()
        .unwrap();
    storage
        .check_failures_again(CL, "a", &[single("p1")], None)
        .await
        .unwrap();
    assert_eq!(rows(&storage, "a").await, [failure("p1", 300, "r2", true)]);

    let lost = deletion
        .commit()
        .await
        .expect_err("both change the hold row");
    assert!(retry::is_transient_surrealdb_error(&lost), "{lost}");
    storage.store_event(&deleted("a", None, 200)).await.unwrap();

    assert_eq!(rows(&storage, "a").await, [failure("p1", 300, "r2", false)]);
    assert_eq!(materialized(&storage, "a").await, []);
}

/// The event of a run comes in while the end of the run in failure is
/// written: the run does not fail the partition that it materialized.
#[tokio::test]
async fn the_end_of_a_run_checked_once_more_does_not_fail_what_the_run_materialized() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    let failing = run("r3", RunStatus::Started, Some(keys(&["p5", "p6"])), &["a"]);
    storage.create_run(&failing).await.unwrap();
    storage
        .update_run_status("r3", RunStatus::Failure, Some(500))
        .await
        .unwrap();
    assert_eq!(
        rows(&storage, "a").await,
        [
            failure("p5", 500, "r3", false),
            failure("p6", 500, "r3", false)
        ]
    );
    commit_unseen_event(&storage, "a", "p5", "r3", 100).await;
    commit_unseen_row(&storage, "a", "p5", "r3", 100).await;

    let keys = [single("p5"), single("p6")];
    storage
        .check_failures_again(CL, "a", &keys, Some("r3"))
        .await
        .unwrap();

    assert_eq!(rows(&storage, "a").await, [failure("p6", 500, "r3", false)]);
}

/// The transaction of a materialization stays open while a materialization
/// of another partition commits. `at`: older than the `assets` row, and the
/// time of that row, as the next send of the run that wrote it.
#[tokio::test]
async fn materializations_of_other_partitions_do_not_stop_each_other() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded(&temp_dir).await;
    register(&storage, &["a"]).await;
    storage
        .store_event(&built("a", "newest", "p0", 300))
        .await
        .unwrap();

    for (run_id, at) in [("older", 100), ("newest", 300)] {
        let open = begin_materialization(&storage, "a", &format!("p-{run_id}"), run_id, at).await;
        storage
            .store_event(&built("a", "other", &format!("q-{run_id}"), 200))
            .await
            .unwrap();

        open.commit()
            .await
            .unwrap_or_else(|e| panic!("{run_id}: {e}"));
    }

    assert_eq!(
        partition_rows(&storage, "a").await,
        [
            row("p-newest", "newest", 300),
            row("p-older", "older", 100),
            row("p0", "newest", 300),
            row("q-newest", "other", 200),
            row("q-older", "other", 200),
        ]
    );
}

#[tokio::test]
async fn of_two_materializations_of_one_partition_at_the_same_time_one_tries_again() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded(&temp_dir).await;
    register(&storage, &["a"]).await;
    storage
        .store_event(&built("a", "newest", "p0", 300))
        .await
        .unwrap();

    let open = begin_materialization(&storage, "a", "p1", "r1", 100).await;
    storage
        .store_event(&built("a", "r2", "p1", 200))
        .await
        .unwrap();
    let lost = open.commit().await.expect_err("both add the row of p1");
    assert!(retry::is_transient_surrealdb_error(&lost), "{lost}");
    storage
        .store_event(&built("a", "r1", "p1", 100))
        .await
        .unwrap();

    assert_eq!(
        partition_rows(&storage, "a").await,
        [row("p0", "newest", 300), row("p1", "r2", 200)]
    );
}

/// A materialization commits while the transaction of a newer deletion of
/// its partition is open: neither sees what the other writes, and the hold
/// row does not stop the deletion. The deletion once more removes the row.
#[tokio::test]
async fn a_partition_deletion_leaves_no_older_row_that_committed_while_it_ran() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = embedded(&temp_dir).await;
    register(&storage, &["a"]).await;
    storage
        .store_event(&built("a", "newest", "p0", 300))
        .await
        .unwrap();

    let deletion = storage.db.clone().begin().await.unwrap();
    deletion
        .query(format!("{CHANGE_HOLD}{}", partition_deletion_statements(1)))
        .bind(("cl", CL.to_string()))
        .bind(("asset_key", "a".to_string()))
        .bind(("keys_0", vec![single("p1")]))
        .bind(("deleted_at_0", 200))
        .bind((
            "rows",
            vec![DbPartitionDeletion {
                code_location_id: CL.to_string(),
                asset_key: "a".to_string(),
                partition_key: single("p1"),
                timestamp: 200,
            }],
        ))
        .await
        .unwrap()
        .check()
        .unwrap();
    storage
        .store_event(&built("a", "r1", "p1", 100))
        .await
        .unwrap();
    deletion.commit().await.unwrap();
    assert_eq!(
        partition_rows(&storage, "a").await,
        [row("p0", "newest", 300), row("p1", "r1", 100)]
    );

    storage
        .delete_partitions_once(CL, "a", &[(single("p1"), 200)])
        .await
        .unwrap();

    assert_eq!(
        partition_rows(&storage, "a").await,
        [row("p0", "newest", 300)]
    );
}

/// Materializations that are not the newest of their asset come in while a
/// newer deletion runs, of the asset or of their partitions: no row of them
/// stays. Such a materialization and the deletion write other rows, so
/// neither stops the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn older_materializations_that_come_in_during_a_deletion_leave_no_row() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = store_on_disk(temp_dir.as_path_untracked()).await;
    let names: Vec<String> = (0..32).map(|i| format!("k{i:02}")).collect();
    let mut numbers = Numbers(5);

    for round in 0..30 {
        let asset = format!("a{round}");
        register(&storage, &[asset.as_str()]).await;
        storage
            .store_event(&built(&asset, "newest", "z", 1_000))
            .await
            .unwrap();

        let deletions: Vec<EventRecord> = match round % 2 {
            0 => vec![deleted(&asset, None, 500)],
            _ => names
                .iter()
                .map(|name| deleted(&asset, Some(name), 500))
                .collect(),
        };
        let mut delay = |below| std::time::Duration::from_micros(numbers.below(below) as u64);
        let mut writes: Vec<BoxFuture<'static, ()>> = names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let storage = storage.clone();
                let event = built(&asset, "older", name, 100 + i as i64);
                let delay = delay(10_000);
                async move {
                    tokio::time::sleep(delay).await;
                    storage.store_event(&event).await.unwrap();
                }
                .boxed()
            })
            .collect();
        let (deleting, delay) = (storage.clone(), delay(5_000));
        writes.push(
            async move {
                tokio::time::sleep(delay).await;
                deleting.store_events(&deletions).await.unwrap();
            }
            .boxed(),
        );
        race(writes).await;

        assert_eq!(
            partition_rows(&storage, &asset).await,
            [row("z", "newest", 1_000)],
            "round {round}"
        );
    }
}

/// Materializations and deletions that race: a partition keeps the row of
/// its newest materialization when that is newer than its deletion and the
/// deletion of the asset.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_materializations_and_deletions_leave_the_rows_that_the_events_give() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = store_on_disk(temp_dir.as_path_untracked()).await;
    let mut numbers = Numbers(2_024);
    let names = ["k0", "k1", "k2", "k3", "k4", "k5"];

    for round in 0..40 {
        let asset = format!("a{round}");
        register(&storage, &[asset.as_str()]).await;

        // Distinct times: three per key, and one of the asset.
        let mut times: Vec<i64> = (1..=3 * names.len() as i64 + 1).map(|t| 100 + t).collect();
        for j in (1..times.len()).rev() {
            times.swap(j, numbers.below(j + 1));
        }
        let asset_deleted_at = (numbers.below(3) == 0).then(|| times[3 * names.len()]);
        let mut events = Vec::new();
        let mut expected = Vec::new();
        for (i, name) in names.iter().enumerate() {
            let (first, second, deleted_at) = (times[3 * i], times[3 * i + 1], times[3 * i + 2]);
            let deleted_at = (numbers.below(2) == 0).then_some(deleted_at);
            events.push(built(&asset, &format!("m{i}a"), name, first));
            events.push(built(&asset, &format!("m{i}b"), name, second));
            events.extend(deleted_at.map(|at| deleted(&asset, Some(name), at)));

            let (newest, run_id) = match first > second {
                true => (first, format!("m{i}a")),
                false => (second, format!("m{i}b")),
            };
            if deleted_at.max(asset_deleted_at) < Some(newest) {
                expected.push((single(name), run_id, newest));
            }
        }
        events.extend(asset_deleted_at.map(|at| deleted(&asset, None, at)));

        let writes: Vec<BoxFuture<'static, ()>> = events
            .into_iter()
            .map(|event| {
                let storage = storage.clone();
                async move {
                    storage.store_event(&event).await.unwrap();
                }
                .boxed()
            })
            .collect();
        race(writes).await;

        assert_eq!(
            partition_rows(&storage, &asset).await,
            expected,
            "round {round}"
        );
    }
}
