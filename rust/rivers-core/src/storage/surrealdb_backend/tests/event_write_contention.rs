//! Several runs that store the materializations of one asset at the same
//! time, as in a backfill of 8 runs with 4,368 partitions each. Prints the
//! time of one run alone and of the runs at once, for each size of a send.
//!
//! Needs a SurrealDB server (`RIVERS_TEST_SURREAL_ENDPOINT`): an embedded
//! memory store misses some write conflicts.
//!
//! ```sh
//! RIVERS_TEST_SURREAL_ENDPOINT=ws://127.0.0.1:18200 cargo test -p rivers-core --profile ci \
//!     --lib event_write_contention -- --ignored --nocapture
//! ```

use std::time::Instant;

use futures_util::future::join_all;

use super::*;
use crate::storage::event_batch::{EventStore, EventWithId};

const PARTITIONS_PER_RUN: usize = 4368;

/// Runs that store at the same time (`RIVERS_BENCH_RUNS`, 8).
fn runs() -> usize {
    std::env::var("RIVERS_BENCH_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8)
}

/// Sizes of a send (`RIVERS_BENCH_CHUNKS`, comma-separated).
fn chunks() -> Vec<usize> {
    match std::env::var("RIVERS_BENCH_CHUNKS") {
        Ok(v) => v.split(',').filter_map(|c| c.trim().parse().ok()).collect(),
        Err(_) => vec![64, 256, 512, 1024, PARTITIONS_PER_RUN],
    }
}
const ASSET: &str = "trips_by_hour_zone";

/// A store in `database` on the test server, on a connection of its own.
async fn server_storage(database: &str) -> Option<SurrealStorage> {
    let endpoint = std::env::var("RIVERS_TEST_SURREAL_ENDPOINT").ok()?;
    let mut config = SurrealConnectConfig::unauthenticated(endpoint);
    config.database = database.to_string();
    Some(
        SurrealStorage::connect(config)
            .await
            .expect("failed to connect to the test server"),
    )
}

fn now_ns() -> i64 {
    let since_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    since_epoch.as_nanos() as i64
}

/// The materializations of one run: one partition per hour of its zone. As
/// in a run pod, all of them get the time the asset returned.
fn run_events(run: usize) -> Vec<EventWithId> {
    let timestamp = now_ns();
    let zone = format!("{run:03}");
    let run_id = format!("run-{run}");
    (0..PARTITIONS_PER_RUN)
        .map(|hour| {
            EventWithId::new(EventRecord {
                code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                event_type: EventType::Materialization { data_version: None },
                asset_key: Some(ASSET.to_string()),
                run_id: run_id.clone(),
                partition_key: Some(PartitionKey::Multi {
                    dims: vec![
                        ("hour".to_string(), vec![format!("h{hour:05}")]),
                        ("zone".to_string(), vec![zone.clone()]),
                    ],
                }),
                timestamp,
                metadata: vec![("rows".to_string(), "{\"Int\":{\"value\":2595}}".to_string())],
                input_data_versions: vec![],
            })
        })
        .collect()
}

/// The time of each run to store its events in sends of `chunk`. Each run
/// has a connection of its own, as each run pod has.
async fn store_concurrently(database: &str, chunk: usize) -> Vec<f64> {
    let mut storages = Vec::new();
    for _ in 0..runs() {
        storages.push(server_storage(database).await.unwrap());
    }
    let runs = storages.into_iter().enumerate().map(|(run, storage)| {
        let events = run_events(run);
        tokio::spawn(async move {
            let started = Instant::now();
            for send in events.chunks(chunk) {
                storage
                    .store_events_with_ids(send)
                    .await
                    .expect("send failed");
            }
            started.elapsed().as_secs_f64()
        })
    });
    join_all(runs)
        .await
        .into_iter()
        .map(|time| time.unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "benchmark; needs RIVERS_TEST_SURREAL_ENDPOINT"]
async fn event_write_contention() {
    // Each failed attempt logs a warning: count them in the output.
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .try_init();
    for chunk in chunks() {
        let database = format!("t{}", uuid::Uuid::new_v4().simple());
        let Some(storage) = server_storage(&database).await else {
            return;
        };
        // The code location registers the asset: without its row, a send
        // has no row to update.
        storage
            .register_assets(DEFAULT_CODE_LOCATION_ID, vec![make_asset_record(ASSET)])
            .await
            .unwrap();
        let alone = {
            let started = Instant::now();
            for send in run_events(99).chunks(chunk) {
                storage
                    .store_events_with_ids(send)
                    .await
                    .expect("send failed");
            }
            started.elapsed().as_secs_f64()
        };

        let started = Instant::now();
        let times = store_concurrently(&database, chunk).await;
        let wall = started.elapsed().as_secs_f64();
        let mean = times.iter().sum::<f64>() / times.len() as f64;
        println!(
            "chunk {chunk:>5}: one run alone {alone:5.2} s | {} runs at once: wall {wall:5.2} s, \
             run mean {mean:5.2} s, slowest {:5.2} s",
            runs(),
            times.iter().cloned().fold(0.0, f64::max)
        );
    }
}
