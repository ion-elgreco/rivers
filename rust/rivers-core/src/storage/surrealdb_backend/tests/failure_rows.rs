//! The failure rows that the writers keep, against the failures that the
//! events and the runs give.

use std::collections::HashSet;
use std::sync::Arc;

use futures_util::FutureExt;
use futures_util::future::BoxFuture;

use super::*;
use crate::storage::event_batch::{EventStore, EventWithId};
use crate::storage::surrealdb_backend::partition_failures::{derived_rows, run_failure_rows};
use crate::storage::{PartitionFailure, retry, stored_order};

const CL: &str = DEFAULT_CODE_LOCATION_ID;

fn single(key: &str) -> PartitionKey {
    PartitionKey::Single {
        keys: vec![key.to_string()],
    }
}

/// One key that names each of `keys`.
fn keys(keys: &[&str]) -> PartitionKey {
    PartitionKey::Single {
        keys: keys.iter().map(|key| key.to_string()).collect(),
    }
}

fn event(
    asset: &str,
    run_id: &str,
    event_type: EventType,
    partition: Option<PartitionKey>,
    at: i64,
) -> EventRecord {
    EventRecord {
        code_location_id: CL.to_string(),
        event_type,
        asset_key: Some(asset.to_string()),
        run_id: run_id.to_string(),
        partition_key: partition,
        timestamp: at,
        metadata: vec![],
        input_data_versions: vec![],
    }
}

fn built(asset: &str, run_id: &str, key: &str, at: i64) -> EventRecord {
    let event_type = EventType::Materialization { data_version: None };
    event(asset, run_id, event_type, Some(single(key)), at)
}

fn failed(asset: &str, run_id: &str, key: PartitionKey, at: i64) -> EventRecord {
    event(asset, run_id, EventType::StepFailure, Some(key), at)
}

fn deleted(asset: &str, key: Option<&str>, at: i64) -> EventRecord {
    event(asset, "deleter", EventType::Deletion, key.map(single), at)
}

fn run(id: &str, status: RunStatus, partition: Option<PartitionKey>, assets: &[&str]) -> RunRecord {
    RunRecord {
        run_id: id.to_string(),
        code_location_id: CL.to_string(),
        job_name: None,
        status,
        start_time: 1,
        end_time: None,
        execution_start_time: None,
        tags: vec![],
        node_names: assets.iter().map(|asset| asset.to_string()).collect(),
        priority: 0,
        partition_key: partition,
        block_reason: None,
        launched_by: LaunchedBy::Manual { user: None },
        action: None,
        config: None,
    }
}

fn failure(key: &str, failed_at: i64, run_id: &str, has_row: bool) -> PartitionFailure {
    PartitionFailure {
        key: single(key),
        failed_at,
        run_id: run_id.to_string(),
        has_row,
    }
}

/// The failure rows of `asset`, in the order of the store.
async fn rows(storage: &SurrealStorage, asset: &str) -> Vec<PartitionFailure> {
    let mut rows = storage
        .get_partition_failures(CL, asset, None, 1_000_000)
        .await
        .unwrap();
    rows.sort_by(|a, b| stored_order(&a.key, &b.key));
    rows
}

/// The rows of `asset` are the failures that the events and the runs give,
/// and so are its failures by run. Returns how many rows there are.
async fn assert_derived(storage: &SurrealStorage, asset: &str, step: &str) -> usize {
    let derived = derived_rows(&storage.db, CL, asset).await.unwrap();
    assert_eq!(rows(storage, asset).await, derived, "{step}");
    let (kept, derived_by_run) = run_failure_rows(&storage.db, CL, asset).await.unwrap();
    assert_eq!(kept, derived_by_run, "{step}: failures by run");
    derived.len()
}

#[tokio::test]
async fn a_failure_takes_a_row_until_a_newer_materialization() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    storage
        .store_events(&[
            built("a", "r1", "p1", 100),
            failed("a", "r2", single("p1"), 200),
            failed("a", "r2", single("p2"), 200),
            failed("a", "r3", single("p3"), 50),
            built("a", "r1", "p3", 100),
        ])
        .await
        .unwrap();

    assert_eq!(
        rows(&storage, "a").await,
        vec![
            failure("p1", 200, "r2", true),
            failure("p2", 200, "r2", false)
        ]
    );

    storage
        .store_event(&built("a", "r4", "p1", 300))
        .await
        .unwrap();
    storage
        .store_event(&built("a", "r4", "p2", 150))
        .await
        .unwrap();

    // `p2` was materialized before its failure: the row keeps the failure.
    assert_eq!(
        rows(&storage, "a").await,
        vec![failure("p2", 200, "r2", true)]
    );
}

#[tokio::test]
async fn a_failed_run_fails_the_partitions_it_did_not_materialize() {
    let storage = make_storage().await;
    register(&storage, &["a", "b"]).await;
    let span = keys(&["p1", "p2", "p3"]);
    storage
        .create_run(&run("r1", RunStatus::Started, Some(span), &["a", "b"]))
        .await
        .unwrap();
    storage
        .store_event(&built("a", "r1", "p1", 100))
        .await
        .unwrap();

    storage
        .update_run_status("r1", RunStatus::Failure, Some(500))
        .await
        .unwrap();

    assert_eq!(
        rows(&storage, "a").await,
        vec![
            failure("p2", 500, "r1", false),
            failure("p3", 500, "r1", false)
        ]
    );
    assert_eq!(rows(&storage, "b").await.len(), 3);
    assert_derived(&storage, "a", "after the run").await;
}

#[tokio::test]
async fn a_new_end_of_a_failed_run_moves_its_failures() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    storage
        .create_run(&run(
            "r1",
            RunStatus::Started,
            Some(keys(&["p1", "p2", "p3"])),
            &["a"],
        ))
        .await
        .unwrap();
    storage
        .update_run_status("r1", RunStatus::Failure, Some(500))
        .await
        .unwrap();
    storage
        .store_events(&[
            built("a", "r2", "p1", 600),
            failed("a", "r3", single("p3"), 550),
        ])
        .await
        .unwrap();

    // The operator writes the end in failure again, with its own time.
    storage
        .update_run_status("r1", RunStatus::Failure, Some(700))
        .await
        .unwrap();

    assert_eq!(
        rows(&storage, "a").await,
        vec![
            failure("p1", 700, "r1", true),
            failure("p2", 700, "r1", false),
            failure("p3", 700, "r1", false)
        ]
    );
    assert_derived(&storage, "a", "after the later end").await;

    storage
        .update_run_status("r1", RunStatus::Failure, Some(400))
        .await
        .unwrap();

    assert_eq!(
        rows(&storage, "a").await,
        vec![
            failure("p2", 400, "r1", false),
            failure("p3", 550, "r3", false)
        ]
    );
    assert_derived(&storage, "a", "after the earlier end").await;
}

#[tokio::test]
async fn a_materialization_after_its_failed_run_ended_replaces_the_failure() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    storage
        .create_run(&run(
            "r1",
            RunStatus::Started,
            Some(keys(&["p1", "p2"])),
            &["a"],
        ))
        .await
        .unwrap();
    storage
        .update_run_status("r1", RunStatus::Failure, Some(500))
        .await
        .unwrap();

    // The event of the run comes in after its end.
    storage
        .store_event(&built("a", "r1", "p1", 100))
        .await
        .unwrap();

    assert_eq!(
        rows(&storage, "a").await,
        vec![failure("p2", 500, "r1", false)]
    );
    assert_derived(&storage, "a", "after the late event").await;
}

/// The late event of a failed run comes in with a newer materialization of
/// the same partition by another run: the failed run still does not fail
/// the partition it materialized.
#[tokio::test]
async fn a_late_materialization_with_a_newer_one_replaces_the_failure() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    storage
        .create_run(&run("x", RunStatus::Started, Some(single("p1")), &["a"]))
        .await
        .unwrap();
    storage
        .update_run_status("x", RunStatus::Failure, Some(500))
        .await
        .unwrap();

    storage
        .store_events(&[built("a", "x", "p1", 100), built("a", "y", "p1", 200)])
        .await
        .unwrap();

    assert_eq!(assert_derived(&storage, "a", "after the drain").await, 0);
}

/// Two failures of a partition at the same time: each path that writes the
/// row gives it the run that sorts first ("r10" before "r9").
#[tokio::test]
async fn of_two_failures_at_the_same_time_the_run_that_sorts_first_takes_the_row() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    storage
        .create_run(&run("r8", RunStatus::Success, None, &["a"]))
        .await
        .unwrap();
    let tied = vec![
        failure("p1", 100, "r10", false),
        failure("p2", 100, "r10", false),
    ];

    // One after the other, and in one drain.
    storage
        .store_event(&failed("a", "r9", single("p1"), 100))
        .await
        .unwrap();
    storage
        .store_event(&failed("a", "r10", single("p1"), 100))
        .await
        .unwrap();
    storage
        .store_events(&[
            failed("a", "r9", single("p2"), 100),
            failed("a", "r10", single("p2"), 100),
        ])
        .await
        .unwrap();
    assert_eq!(rows(&storage, "a").await, tied);
    assert_derived(&storage, "a", "after the failures").await;

    // The row is computed again when a newer failure goes.
    storage
        .store_event(&failed("a", "r8", single("p1"), 300))
        .await
        .unwrap();
    assert!(storage.delete_run("r8").await.unwrap());
    assert_eq!(rows(&storage, "a").await, tied);
    assert_derived(&storage, "a", "after the deletion").await;
}

#[tokio::test]
async fn deleting_a_run_brings_back_an_older_failure() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    storage
        .create_run(&run("r1", RunStatus::Success, None, &["a"]))
        .await
        .unwrap();
    storage
        .create_run(&run("r2", RunStatus::Success, None, &["a"]))
        .await
        .unwrap();
    storage
        .store_events(&[
            failed("a", "r1", single("p1"), 100),
            failed("a", "r2", single("p1"), 200),
            failed("a", "r2", single("p2"), 200),
        ])
        .await
        .unwrap();

    assert!(storage.delete_run("r2").await.unwrap());

    assert_eq!(
        rows(&storage, "a").await,
        vec![failure("p1", 100, "r1", false)]
    );
    assert_derived(&storage, "a", "after the deletion").await;
}

#[tokio::test]
async fn a_run_that_no_longer_ends_in_failure_fails_no_partition() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    storage
        .create_run(&run(
            "r1",
            RunStatus::Started,
            Some(keys(&["p1", "p2"])),
            &["a"],
        ))
        .await
        .unwrap();
    storage
        .store_event(&failed("a", "r1", single("p2"), 300))
        .await
        .unwrap();
    storage
        .update_run_status("r1", RunStatus::Failure, Some(500))
        .await
        .unwrap();
    assert_eq!(rows(&storage, "a").await.len(), 2);

    storage
        .update_run_status("r1", RunStatus::Success, Some(600))
        .await
        .unwrap();

    // The keyed failure stays.
    assert_eq!(
        rows(&storage, "a").await,
        vec![failure("p2", 300, "r1", false)]
    );
    assert_derived(&storage, "a", "after the success").await;
}

#[tokio::test]
async fn a_run_failed_before_it_started_fails_no_partition_when_it_starts() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    storage
        .create_run(&run("r1", RunStatus::Queued, Some(single("p1")), &["a"]))
        .await
        .unwrap();
    assert!(storage.fail_run_if_active("r1", 500).await.unwrap());
    assert_eq!(
        rows(&storage, "a").await,
        vec![failure("p1", 500, "r1", false)]
    );

    assert!(storage.try_start_run("r1").await.unwrap());

    assert!(rows(&storage, "a").await.is_empty());
    assert_derived(&storage, "a", "after the start").await;
}

#[tokio::test]
async fn a_run_that_ended_otherwise_fails_no_partition() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    storage
        .create_run(&run("r1", RunStatus::Success, Some(single("p1")), &["a"]))
        .await
        .unwrap();

    assert!(!storage.fail_run_if_active("r1", 500).await.unwrap());

    assert!(rows(&storage, "a").await.is_empty());
    assert_derived(&storage, "a", "after the late failure").await;
}

/// Write `set` on the run `run_id` as a write of its status does, and stop
/// there: as if the process stopped before the repair after the status.
async fn write_status_only(storage: &SurrealStorage, run_id: &str, set: &str) {
    storage
        .db
        .query(format!("UPDATE runs SET {set} WHERE run_id = $run_id"))
        .bind(("run_id", run_id.to_string()))
        .await
        .unwrap()
        .check()
        .unwrap();
}

/// A final status and one that is not final: each writes the status in a
/// query of its own.
#[tokio::test]
async fn a_status_written_again_after_a_stop_clears_the_end_in_failure() {
    for status in [
        RunStatus::Success,
        RunStatus::Canceled,
        RunStatus::NotStarted,
    ] {
        let storage = make_storage().await;
        register(&storage, &["a"]).await;
        storage
            .create_run(&run(
                "r1",
                RunStatus::Started,
                Some(keys(&["p1", "p2", "p3"])),
                &["a"],
            ))
            .await
            .unwrap();
        storage
            .store_events(&[
                built("a", "r1", "p1", 100),
                failed("a", "r1", single("p2"), 300),
            ])
            .await
            .unwrap();
        storage
            .update_run_status("r1", RunStatus::Failure, Some(500))
            .await
            .unwrap();
        assert_eq!(rows(&storage, "a").await.len(), 2);

        write_status_only(&storage, "r1", &format!("status = '{status:?}'")).await;
        storage
            .update_run_status("r1", status.clone(), None)
            .await
            .unwrap();

        assert_eq!(
            rows(&storage, "a").await,
            vec![failure("p2", 300, "r1", false)],
            "{status:?}"
        );
        assert_derived(&storage, "a", &format!("after {status:?} again")).await;
    }
}

#[tokio::test]
async fn a_start_again_after_a_stop_clears_the_end_in_failure() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    storage
        .create_run(&run("r1", RunStatus::Queued, Some(single("p1")), &["a"]))
        .await
        .unwrap();
    assert!(storage.fail_run_if_active("r1", 500).await.unwrap());
    assert_eq!(rows(&storage, "a").await.len(), 1);

    write_status_only(&storage, "r1", "status = 'Started'").await;
    assert!(storage.try_start_run("r1").await.unwrap());

    assert!(rows(&storage, "a").await.is_empty());
    assert_derived(&storage, "a", "after the start again").await;
}

#[tokio::test]
async fn an_earlier_end_written_again_after_a_stop_moves_the_failures() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    storage
        .create_run(&run(
            "r1",
            RunStatus::Started,
            Some(keys(&["p1", "p2", "p3"])),
            &["a"],
        ))
        .await
        .unwrap();
    storage
        .update_run_status("r1", RunStatus::Failure, Some(700))
        .await
        .unwrap();
    storage
        .store_events(&[
            built("a", "r2", "p1", 600),
            failed("a", "r3", single("p3"), 550),
        ])
        .await
        .unwrap();

    write_status_only(&storage, "r1", "end_time = 400").await;
    storage
        .update_run_status("r1", RunStatus::Failure, Some(400))
        .await
        .unwrap();

    assert_eq!(
        rows(&storage, "a").await,
        vec![
            failure("p2", 400, "r1", false),
            failure("p3", 550, "r3", false)
        ]
    );
    assert_derived(&storage, "a", "after the earlier end again").await;
}

/// A failure that comes after the run ended otherwise writes no status, and
/// still brings the rows in line with the stored status.
#[tokio::test]
async fn a_failure_of_a_run_that_ended_otherwise_after_a_stop_clears_the_end_in_failure() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    storage
        .create_run(&run("r1", RunStatus::Started, Some(single("p1")), &["a"]))
        .await
        .unwrap();
    storage
        .update_run_status("r1", RunStatus::Failure, Some(500))
        .await
        .unwrap();
    assert_eq!(rows(&storage, "a").await.len(), 1);

    write_status_only(&storage, "r1", "status = 'Success', end_time = 600").await;
    assert!(!storage.fail_run_if_active("r1", 700).await.unwrap());

    assert!(rows(&storage, "a").await.is_empty());
    assert_derived(&storage, "a", "after the late failure").await;
}

#[tokio::test]
async fn a_run_created_as_failed_fails_its_partitions() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    let mut failed_run = run("r1", RunStatus::Failure, Some(single("p1")), &["a"]);
    failed_run.end_time = Some(500);

    storage.create_runs(&[failed_run]).await.unwrap();

    assert_eq!(
        rows(&storage, "a").await,
        vec![failure("p1", 500, "r1", false)]
    );
}

#[tokio::test]
async fn a_deletion_replaces_an_older_failure_and_the_row_of_a_newer_one() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    storage
        .store_events(&[
            built("a", "r1", "p1", 100),
            built("a", "r1", "p2", 100),
            failed("a", "r2", single("p1"), 300),
            failed("a", "r2", single("p2"), 300),
        ])
        .await
        .unwrap();

    // A deletion of `p1` older than the failure, one of `p2` newer.
    storage
        .store_events(&[deleted("a", Some("p1"), 200), deleted("a", Some("p2"), 400)])
        .await
        .unwrap();

    assert_eq!(
        rows(&storage, "a").await,
        vec![failure("p1", 300, "r2", false)]
    );
    assert_derived(&storage, "a", "after the deletions").await;
}

#[tokio::test]
async fn an_asset_deletion_replaces_older_failures_and_their_rows() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    storage
        .store_events(&[
            built("a", "r1", "p1", 100),
            built("a", "r1", "p2", 100),
            failed("a", "r2", single("p1"), 200),
            failed("a", "r2", single("p2"), 400),
        ])
        .await
        .unwrap();

    storage.store_event(&deleted("a", None, 300)).await.unwrap();

    assert_eq!(
        rows(&storage, "a").await,
        vec![failure("p2", 400, "r2", false)]
    );
    assert_derived(&storage, "a", "after the deletion").await;
}

#[tokio::test]
async fn an_action_run_fails_no_partition() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    let mut action = run("act", RunStatus::Started, Some(single("p1")), &["a"]);
    action.action = Some("optimize".to_string());
    storage.create_run(&action).await.unwrap();

    storage
        .store_event(&failed("a", "act", single("p1"), 100))
        .await
        .unwrap();
    storage
        .update_run_status("act", RunStatus::Failure, Some(200))
        .await
        .unwrap();

    assert!(rows(&storage, "a").await.is_empty());
}

#[tokio::test]
async fn failures_of_more_partitions_than_one_transaction_holds() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    let names: Vec<String> = (0..2_500).map(|i| format!("p{i:05}")).collect();
    let mut events: Vec<EventRecord> = names
        .iter()
        .step_by(2)
        .map(|name| built("a", "r1", name, 100))
        .collect();
    let all: Vec<&str> = names.iter().map(String::as_str).collect();
    events.push(failed("a", "r2", keys(&all), 200));

    storage.store_events(&events).await.unwrap();

    let rows = rows(&storage, "a").await;
    assert_eq!(rows.len(), 2_500);
    assert_eq!(rows.iter().filter(|row| row.has_row).count(), 1_250);
    assert_derived(&storage, "a", "after the failure").await;
}

/// An attempt that stored the events of a batch and then stopped, and a send
/// of the same batch again: each event is stored once, and the failures of
/// the batch take their rows.
#[tokio::test]
async fn a_batch_sent_again_after_a_stopped_attempt_stores_each_event_once() {
    let storage = make_storage().await;
    register(&storage, &["a"]).await;
    let batch: Vec<EventWithId> = [
        built("a", "r1", "p1", 100),
        failed("a", "r2", single("p2"), 200),
        failed("a", "r2", keys(&["p3", "p4"]), 300),
    ]
    .into_iter()
    .map(EventWithId::new)
    .collect();
    let inserted: Vec<DbEventWrite> = batch
        .iter()
        .map(|e| DbEventWrite::from_event(&e.event, e.id.clone()))
        .collect();
    storage
        .db
        .query("INSERT IGNORE INTO events $rows RETURN NONE")
        .bind(("rows", inserted))
        .await
        .unwrap()
        .check()
        .unwrap();

    storage.store_events_with_ids(&batch).await.unwrap();

    let mut stored = Vec::new();
    for run_id in ["r1", "r2"] {
        for event in storage.get_events_for_run(run_id).await.unwrap() {
            stored.push((event.run_id, event.timestamp));
        }
    }
    let once: Vec<(String, i64)> = batch
        .iter()
        .map(|e| (e.event.run_id.clone(), e.event.timestamp))
        .collect();
    assert_eq!(stored, once);
    assert_eq!(
        rows(&storage, "a").await,
        vec![
            failure("p2", 200, "r2", false),
            failure("p3", 300, "r2", false),
            failure("p4", 300, "r2", false)
        ]
    );
    assert_derived(&storage, "a", "after the send again").await;
}

/// A small generator of numbers, the same on every run of the test.
struct Numbers(u64);

impl Numbers {
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % n as u64) as usize
    }
}

/// Random changes of two assets, each compared with the failures that the
/// events and the runs give: materializations, failures of steps and of
/// runs, deletions, deleted runs, runs that end in failure again and runs
/// that end otherwise, some of them late.
#[tokio::test]
async fn the_rows_follow_the_failures_of_the_events_and_the_runs() {
    for seed in [7, 42, 1_234] {
        let mut compared = 0;
        let storage = make_storage().await;
        register(&storage, &["a", "b"]).await;
        let mut numbers = Numbers(seed);
        let names = ["k0", "k1", "k2", "k3", "k4", "k5"];
        let assets = ["a", "b"];
        let mut clock: i64 = 1_000;
        let mut used: HashSet<i64> = HashSet::new();
        let mut time = |numbers: &mut Numbers, late: bool| {
            clock += 10;
            let mut at = match late {
                true => clock - 1 - numbers.below(200) as i64,
                false => clock,
            };
            while !used.insert(at) {
                at -= 1;
            }
            at
        };
        // Runs with a row that ended, for the deletions and status changes.
        let mut ended: Vec<String> = Vec::new();
        // Runs that ended in failure: their partition and assets.
        let mut failed_runs: Vec<(String, Vec<&str>, Vec<&str>)> = Vec::new();

        for step in 0..120 {
            let asset = assets[numbers.below(2)];
            let name = names[numbers.below(names.len())];
            let late = numbers.below(4) == 0;
            let id = format!("s{seed}-{step}");
            let what = numbers.below(11);
            match what {
                0 | 1 => {
                    let at = time(&mut numbers, late);
                    storage
                        .store_event(&built(asset, &id, name, at))
                        .await
                        .unwrap();
                }
                2 | 3 => {
                    let other = names[numbers.below(names.len())];
                    let at = time(&mut numbers, late);
                    if numbers.below(2) == 0 {
                        let ended_run = run(&id, RunStatus::Success, None, &[asset]);
                        storage.create_run(&ended_run).await.unwrap();
                        ended.push(id.clone());
                    }
                    let event = failed(asset, &id, keys(&[name, other]), at);
                    storage.store_event(&event).await.unwrap();
                }
                4 => {
                    let mut action = run(&id, RunStatus::Success, None, &[asset]);
                    action.action = Some("optimize".to_string());
                    storage.create_run(&action).await.unwrap();
                    let at = time(&mut numbers, late);
                    let event = failed(asset, &id, single(name), at);
                    storage.store_event(&event).await.unwrap();
                }
                5 => {
                    let members: Vec<&str> = names
                        .iter()
                        .copied()
                        .filter(|_| numbers.below(2) == 0)
                        .collect();
                    let members = if members.is_empty() {
                        vec![name]
                    } else {
                        members
                    };
                    let run_assets = match numbers.below(3) {
                        0 => vec!["a", "b"],
                        _ => vec![asset],
                    };
                    let started = run(&id, RunStatus::Started, Some(keys(&members)), &run_assets);
                    storage.create_run(&started).await.unwrap();
                    for member in &members {
                        if numbers.below(2) == 0 {
                            let at = time(&mut numbers, false);
                            storage
                                .store_event(&built(asset, &id, member, at))
                                .await
                                .unwrap();
                        }
                    }
                    let at = time(&mut numbers, false);
                    storage
                        .update_run_status(&id, RunStatus::Failure, Some(at))
                        .await
                        .unwrap();
                    ended.push(id.clone());
                    failed_runs.push((id.clone(), members, run_assets));
                }
                6 if !failed_runs.is_empty() => {
                    // A materialization of a failed run that comes in late.
                    let (run_id, members, run_assets) =
                        &failed_runs[numbers.below(failed_runs.len())];
                    let member = members[numbers.below(members.len())];
                    let run_asset = run_assets[numbers.below(run_assets.len())];
                    let at = time(&mut numbers, true);
                    storage
                        .store_event(&built(run_asset, run_id, member, at))
                        .await
                        .unwrap();
                }
                7 => {
                    let at = time(&mut numbers, late);
                    let whole = numbers.below(6) == 0;
                    let key = (!whole).then_some(name);
                    storage.store_event(&deleted(asset, key, at)).await.unwrap();
                }
                8 if !ended.is_empty() => {
                    let run_id = ended.swap_remove(numbers.below(ended.len()));
                    failed_runs.retain(|(id, _, _)| *id != run_id);
                    storage.delete_run(&run_id).await.unwrap();
                }
                9 if !failed_runs.is_empty() => {
                    let pick = numbers.below(failed_runs.len());
                    if numbers.below(2) == 0 {
                        // The end in failure again, as the operator writes it.
                        let run_id = failed_runs[pick].0.clone();
                        let at = time(&mut numbers, late);
                        storage
                            .update_run_status(&run_id, RunStatus::Failure, Some(at))
                            .await
                            .unwrap();
                    } else {
                        let (run_id, _, _) = failed_runs.swap_remove(pick);
                        let at = time(&mut numbers, false);
                        storage
                            .update_run_status(&run_id, RunStatus::Success, Some(at))
                            .await
                            .unwrap();
                    }
                }
                _ => {
                    // One drain of several events.
                    let mut drain = Vec::new();
                    for i in 0..2 + numbers.below(5) {
                        let asset = assets[numbers.below(2)];
                        let name = names[numbers.below(names.len())];
                        let late = numbers.below(3) == 0;
                        let at = time(&mut numbers, late);
                        let run_id = format!("{id}-{i}");
                        drain.push(match numbers.below(3) {
                            0 => built(asset, &run_id, name, at),
                            1 => failed(asset, &run_id, single(name), at),
                            _ => deleted(asset, Some(name), at),
                        });
                    }
                    storage.store_events(&drain).await.unwrap();
                }
            }

            for asset in assets {
                let label = format!("seed {seed}, step {step} ({what}), asset {asset}");
                compared += assert_derived(&storage, asset, &label).await;
            }
        }
        assert!(compared > 200, "seed {seed}: {compared} rows compared");
    }
}

/// A store on disk, which sees every conflict of two writers, with retries
/// quick enough for many writers of one asset.
async fn store_on_disk(dir: &std::path::Path) -> Arc<SurrealStorage> {
    let retry = retry::StorageRetryConfig {
        max_retries: 50,
        initial_backoff: std::time::Duration::from_millis(1),
        max_backoff: std::time::Duration::from_millis(5),
        backoff_multiplier: 1.0,
        max_elapsed: None,
    };
    let storage = SurrealStorage::new_embedded_with_retry(
        dir.to_str().unwrap(),
        retry,
        Capability::ReadWrite,
    )
    .await
    .expect("failed to create embedded storage");
    Arc::new(storage)
}

/// Start all of `writes` at the same time, and wait for them.
async fn race(writes: Vec<BoxFuture<'static, ()>>) {
    let barrier = Arc::new(tokio::sync::Barrier::new(writes.len()));
    let tasks: Vec<_> = writes
        .into_iter()
        .map(|write| {
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                write.await;
            })
        })
        .collect();
    for task in tasks {
        task.await.unwrap();
    }
}

/// A run ends in failure while its materializations come in: the run fails
/// the partitions it did not materialize only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_that_fails_while_its_materializations_come_in_leaves_the_derived_rows() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = store_on_disk(temp_dir.as_path_untracked()).await;
    let members: Vec<String> = (0..32).map(|i| format!("k{i:02}")).collect();
    let members: Vec<&str> = members.iter().map(String::as_str).collect();
    let mut numbers = Numbers(7);

    for round in 0..30 {
        let (asset, run_id) = (format!("a{round}"), format!("r{round}"));
        register(&storage, &[asset.as_str()]).await;
        let started = run(&run_id, RunStatus::Started, Some(keys(&members)), &[&asset]);
        storage.create_run(&started).await.unwrap();

        // Every fourth partition is materialized, at a random point of the
        // end of the run.
        let mut writes: Vec<BoxFuture<'static, ()>> = members
            .iter()
            .enumerate()
            .step_by(4)
            .map(|(i, member)| {
                let storage = storage.clone();
                let event = built(&asset, &run_id, member, 100 + i as i64);
                let delay = std::time::Duration::from_micros(numbers.below(20_000) as u64);
                async move {
                    tokio::time::sleep(delay).await;
                    storage.store_event(&event).await.unwrap();
                }
                .boxed()
            })
            .collect();
        let (failing, run_id) = (storage.clone(), run_id.clone());
        writes.push(
            async move {
                failing
                    .update_run_status(&run_id, RunStatus::Failure, Some(500))
                    .await
                    .unwrap()
            }
            .boxed(),
        );
        race(writes).await;

        assert_eq!(
            assert_derived(&storage, &asset, &format!("round {round}")).await,
            24
        );
    }
}

/// A run is deleted while older failures of its partitions come in from
/// another run: the older failures take the rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_deleted_while_older_failures_come_in_leaves_the_derived_rows() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = store_on_disk(temp_dir.as_path_untracked()).await;
    let members = ["k0", "k1", "k2", "k3"];

    for round in 0..30 {
        let (asset, deleted_run) = (format!("a{round}"), format!("d{round}"));
        register(&storage, &[asset.as_str()]).await;
        let ended = run(&deleted_run, RunStatus::Success, None, &[&asset]);
        storage.create_run(&ended).await.unwrap();
        let newer: Vec<EventRecord> = members
            .iter()
            .map(|member| failed(&asset, &deleted_run, single(member), 300))
            .collect();
        storage.store_events(&newer).await.unwrap();

        let mut writes: Vec<BoxFuture<'static, ()>> = members
            .iter()
            .enumerate()
            .map(|(i, member)| {
                let event = failed(&asset, &format!("o{round}"), single(member), 200 + i as i64);
                let storage = storage.clone();
                async move {
                    storage.store_event(&event).await.unwrap();
                }
                .boxed()
            })
            .collect();
        let deleting = storage.clone();
        writes
            .push(async move { assert!(deleting.delete_run(&deleted_run).await.unwrap()) }.boxed());
        race(writes).await;

        assert_eq!(
            assert_derived(&storage, &asset, &format!("round {round}")).await,
            4
        );
    }
}

/// A run is deleted while another run keeps materializing other partitions
/// of the same asset, which has many failed partitions: the deletion
/// finishes. A deletion that derived the failures of the whole asset while
/// it held the asset row started again on each materialization.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_deleted_while_other_partitions_are_materialized_finishes() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = store_on_disk(temp_dir.as_path_untracked()).await;
    register(&storage, &["a"]).await;
    let names: Vec<String> = (0..20_000).map(|i| format!("k{i:05}")).collect();
    for (i, chunk) in names.chunks(1_000).enumerate() {
        let chunk: Vec<&str> = chunk.iter().map(String::as_str).collect();
        storage
            .store_event(&failed("a", &format!("old{i}"), keys(&chunk), 100))
            .await
            .unwrap();
    }
    storage
        .create_run(&run("deleted", RunStatus::Success, None, &["a"]))
        .await
        .unwrap();
    let of_run: Vec<&str> = names[..24].iter().map(String::as_str).collect();
    storage
        .store_event(&failed("a", "deleted", keys(&of_run), 200))
        .await
        .unwrap();

    let started = Arc::new(tokio::sync::Notify::new());
    let stop = Arc::new(AtomicBool::new(false));
    let writer = tokio::spawn({
        let (storage, started, stop) = (storage.clone(), started.clone(), stop.clone());
        async move {
            let mut written = 0;
            while !stop.load(Ordering::Relaxed) {
                let event = built("a", "writer", &format!("z{written:06}"), 1_000 + written);
                storage.store_event(&event).await.unwrap();
                written += 1;
                if written == 3 {
                    started.notify_one();
                }
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
        }
    });
    started.notified().await;

    let deleted = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        storage.delete_run("deleted"),
    )
    .await;
    stop.store(true, Ordering::Relaxed);
    writer.await.unwrap();

    let deleted = deleted.expect("the deletion should finish while the asset is materialized");
    assert!(deleted.unwrap());
    assert_eq!(
        assert_derived(&storage, "a", "after the deletion").await,
        20_000
    );
}

/// Failures, materializations and deletions of one asset that race: the
/// rows that remain are the failures that the events give.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_writers_of_one_asset_leave_the_derived_rows() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = store_on_disk(temp_dir.as_path_untracked()).await;
    let mut numbers = Numbers(99);

    for round in 0..30 {
        let asset = format!("a{round}");
        register(&storage, &[asset.as_str()]).await;

        let mut writes = Vec::new();
        for (i, name) in ["k0", "k1", "k2", "k3"].iter().enumerate() {
            // Distinct times in a random order per key.
            let base = 100 * (i as i64 + 1);
            let mut times = [base + 1, base + 2, base + 3];
            for j in (1..times.len()).rev() {
                times.swap(j, numbers.below(j + 1));
            }
            writes.push(built(&asset, &format!("m{i}"), name, times[0]));
            writes.push(failed(&asset, &format!("f{i}"), single(name), times[1]));
            if numbers.below(2) == 0 {
                writes.push(deleted(&asset, Some(name), times[2]));
            }
        }

        let writes: Vec<BoxFuture<'static, ()>> = writes
            .into_iter()
            .map(|write| {
                let storage = storage.clone();
                async move {
                    storage.store_event(&write).await.unwrap();
                }
                .boxed()
            })
            .collect();
        race(writes).await;

        assert_derived(&storage, &asset, &format!("round {round}")).await;
    }
}
