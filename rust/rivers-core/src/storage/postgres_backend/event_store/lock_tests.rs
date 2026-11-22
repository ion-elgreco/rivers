//! PostgreSQL only: the locks and the stored text of the event write path.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use super::*;
use crate::storage::DEFAULT_CODE_LOCATION_ID as CL;
use crate::storage::migration::Capability;
use crate::storage::retry::StorageRetryConfig;

async fn store() -> Option<PostgresStorage> {
    let url = PostgresStorage::test_url()?;
    let storage = PostgresStorage::connect_test_schema(&url).await.unwrap();
    let asset = AssetRecord {
        code_location_id: CL.to_string(),
        asset_key: "a".to_string(),
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
    };
    storage.register_assets(CL, vec![asset]).await.unwrap();
    Some(storage)
}

/// Another store on the schema of `storage`, with connections of its own.
async fn same_schema(storage: &PostgresStorage) -> PostgresStorage {
    let (options, endpoint) = (storage.options.clone(), storage.endpoint.clone());
    let retry = StorageRetryConfig::default();
    PostgresStorage::connect_with_options(options, endpoint, retry, Capability::ReadWrite)
        .await
        .unwrap()
}

fn event(run_id: &str, event_type: EventType, key: Option<&str>, at: i64) -> EventRecord {
    EventRecord {
        code_location_id: CL.to_string(),
        event_type,
        asset_key: Some("a".to_string()),
        run_id: run_id.to_string(),
        partition_key: key.map(|key| PartitionKey::Single {
            keys: vec![key.to_string()],
        }),
        timestamp: at,
        metadata: vec![],
        input_data_versions: vec![],
    }
}

fn built(run_id: &str, key: &str, at: i64) -> EventRecord {
    let event_type = EventType::Materialization { data_version: None };
    event(run_id, event_type, Some(key), at)
}

/// Runs keep sending materializations of one asset, so that a send of them
/// is open at almost every moment. A deletion and a keyed failure of the
/// asset, which need it alone, still finish soon: a send that waits to have
/// the asset alone goes before the sends that come after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_writer_alone_finishes_while_materializations_stream_in() {
    const BOUND: Duration = Duration::from_secs(5);
    const RUNS: usize = 8;
    const PER_SEND: i64 = 64;
    let Some(storage) = store().await else {
        return;
    };
    let (stop, sent) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicUsize::new(0)),
    );
    let mut senders = Vec::new();
    for run in 0..RUNS {
        let sender = same_schema(&storage).await;
        let (stop, sent) = (stop.clone(), sent.clone());
        senders.push(tokio::spawn(async move {
            let mut round = 0;
            while !stop.load(Ordering::Relaxed) {
                let events: Vec<EventRecord> = (0..PER_SEND)
                    .map(|i| {
                        built(
                            &format!("r{run}"),
                            &format!("{run}-{round}-{i}"),
                            1_000 + round,
                        )
                    })
                    .collect();
                sender.store_events(&events).await.unwrap();
                sent.fetch_add(1, Ordering::Relaxed);
                round += 1;
            }
        }));
    }
    while sent.load(Ordering::Relaxed) < 16 {
        // A sender stops before `stop` only by a panic: show it.
        if let Some(sender) = senders.iter_mut().find(|sender| sender.is_finished()) {
            sender.await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let mut waits = Vec::new();
    for (what, alone) in [
        (
            "partition deletion",
            event("deleter", EventType::Deletion, Some("0-0-0"), 500),
        ),
        (
            "keyed failure",
            event("f", EventType::StepFailure, Some("0-0-1"), 2_000_000),
        ),
    ] {
        let (started, before) = (Instant::now(), sent.load(Ordering::Relaxed));
        let done = tokio::time::timeout(BOUND, storage.store_event(&alone)).await;
        waits.push((
            what,
            done.is_ok(),
            started.elapsed(),
            sent.load(Ordering::Relaxed) - before,
        ));
    }
    stop.store(true, Ordering::Relaxed);
    for sender in senders {
        sender.await.unwrap();
    }

    for (what, done, waited, sent_meanwhile) in waits {
        assert!(
            done,
            "{what} waited {waited:?} while {sent_meanwhile} sends of materializations went in"
        );
    }
}

/// A send of materializations that comes while a writer waits to have the
/// asset alone goes after that writer, also when the sends before it are
/// still open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_send_after_a_waiting_writer_alone_goes_after_it() {
    const SETTLE: Duration = Duration::from_millis(500);
    let Some(storage) = store().await else {
        return;
    };
    let storage = Arc::new(storage);
    storage.store_event(&built("r0", "p0", 50)).await.unwrap();
    // A send of materializations stays open while this holds the asset row.
    let mut holding = storage.pool().begin().await.unwrap();
    sqlx::query("SELECT 1 FROM assets WHERE asset_key = 'a' FOR UPDATE")
        .execute(&mut *holding)
        .await
        .unwrap();

    let started = Instant::now();
    let write = |event: EventRecord| {
        let storage = storage.clone();
        tokio::spawn(async move {
            storage.store_event(&event).await.unwrap();
            started.elapsed()
        })
    };
    let open = write(built("r1", "p1", 100));
    tokio::time::sleep(SETTLE).await;
    let alone = write(event("deleter", EventType::Deletion, Some("p0"), 150));
    tokio::time::sleep(SETTLE).await;
    let after = write(built("r2", "p2", 200));
    tokio::time::sleep(SETTLE).await;
    holding.commit().await.unwrap();

    let (open, alone, after) = (
        open.await.unwrap(),
        alone.await.unwrap(),
        after.await.unwrap(),
    );
    assert!(
        open <= alone,
        "the open send ends at {open:?}, the deletion at {alone:?}"
    );
    assert!(
        alone < after,
        "the deletion ends at {alone:?}, the later send at {after:?}"
    );
}

/// The end of a run in failure does not lock an asset of which the run
/// built every member of its partition.
#[tokio::test]
async fn the_end_of_a_run_skips_an_asset_it_built_whole() {
    let Some(storage) = store().await else {
        return;
    };
    storage.store_event(&built("r", "p1", 10)).await.unwrap();
    let mut holding = storage.pool().begin().await.unwrap();
    sqlx::query("SELECT rivers_lock_assets(ARRAY[$1], ARRAY['a'], ARRAY[true])")
        .bind(CL)
        .execute(&mut *holding)
        .await
        .unwrap();

    let run = RunOfFailures {
        run_id: "r".to_string(),
        code_location_id: CL.to_string(),
        status: "Failure".to_string(),
        action: None,
        partition_key: Some(PartitionKey::Single {
            keys: vec!["p1".to_string()],
        }),
        node_names: vec!["a".to_string()],
        start_time: 0,
        end_time: Some(20),
    };
    let done = tokio::time::timeout(
        Duration::from_secs(2),
        storage.record_failures_of_run(&run, 20),
    )
    .await;
    holding.rollback().await.unwrap();
    done.expect("the end of the run waited for the asset")
        .unwrap();
    assert_eq!(
        storage
            .count_partition_failures(CL, "a")
            .await
            .unwrap()
            .failed,
        0
    );
}

/// A send of only step events of an asset applies nothing to it: no
/// statement on its partition rows, so no notice to the pages that watch
/// them.
#[tokio::test]
async fn a_send_of_step_events_leaves_the_partition_rows_alone() {
    let Some(storage) = store().await else {
        return;
    };
    let schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(storage.pool())
        .await
        .unwrap();
    let mut listener = sqlx::postgres::PgListener::connect_with(storage.pool())
        .await
        .unwrap();
    listener.listen("rivers_asset_partitions").await.unwrap();
    let mut notice_of_this_schema = async |within: Duration| {
        tokio::time::timeout(within, async {
            while listener.recv().await.unwrap().payload() != schema {}
        })
        .await
        .is_ok()
    };

    let steps = [
        event("r", EventType::StepStart, Some("p1"), 10),
        event("r", EventType::StepSuccess, Some("p1"), 20),
    ];
    storage.store_events(&steps).await.unwrap();
    assert!(!notice_of_this_schema(Duration::from_secs(1)).await);
    storage.store_event(&built("r", "p1", 30)).await.unwrap();
    assert!(notice_of_this_schema(Duration::from_secs(10)).await);
}

/// A NUL in a log line does not lose the batch: the line is stored with the
/// replacement character, and the other lines with it.
#[tokio::test]
async fn a_log_line_with_a_nul_is_stored_with_the_rest_of_its_batch() {
    let Some(storage) = store().await else {
        return;
    };
    let line = |step: &str, stdout: &str| LogRecord {
        code_location_id: CL.to_string(),
        run_id: "r".to_string(),
        step_key: step.to_string(),
        timestamp: 10,
        stdout: Some(stdout.to_string()),
        stderr: None,
        logs: Some("log\0".to_string()),
        traceback: None,
    };
    storage
        .store_run_logs(&[
            line("before", "fine"),
            line("nul", "a\0b"),
            line("after", "fine"),
        ])
        .await
        .unwrap();

    let stored: Vec<(String, Option<String>, Option<String>)> = storage
        .get_run_logs("r")
        .await
        .unwrap()
        .into_iter()
        .map(|log| (log.step_key, log.stdout, log.logs))
        .collect();
    let at = |step: &str, stdout: &str| {
        (
            step.to_string(),
            Some(stdout.to_string()),
            Some("log\u{FFFD}".to_string()),
        )
    };
    let mut want = vec![
        at("before", "fine"),
        at("nul", "a\u{FFFD}b"),
        at("after", "fine"),
    ];
    want.sort();
    let mut stored = stored;
    stored.sort();
    assert_eq!(stored, want);
}

/// A NUL in the metadata, the data version or the partition key of an event
/// does not fail the send: the text is stored with the replacement
/// character, and the other events of the send with it.
#[tokio::test]
async fn an_event_with_a_nul_is_stored_with_the_rest_of_its_send() {
    let Some(storage) = store().await else {
        return;
    };
    let mut odd = built("r", "p\0", 20);
    odd.event_type = EventType::Materialization {
        data_version: Some("v\0".to_string()),
    };
    odd.metadata = vec![("path\0".to_string(), "a\0b".to_string())];
    storage
        .store_events(&[built("r", "p1", 10), odd, built("r", "p2", 30)])
        .await
        .unwrap();

    let stored = storage.get_events_for_run("r").await.unwrap();
    let keys: Vec<PartitionKey> = stored
        .iter()
        .filter_map(|event| event.partition_key.clone())
        .collect();
    let single = |key: &str| PartitionKey::Single {
        keys: vec![key.to_string()],
    };
    assert_eq!(keys, [single("p1"), single("p\u{FFFD}"), single("p2")]);
    assert_eq!(
        stored[1].metadata,
        [("path\u{FFFD}".to_string(), "a\u{FFFD}b".to_string())]
    );
    assert_eq!(
        stored[1].event_type,
        EventType::Materialization {
            data_version: Some("v\u{FFFD}".to_string())
        }
    );
}
