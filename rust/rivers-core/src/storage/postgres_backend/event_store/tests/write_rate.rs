//! PostgreSQL only: the write rate of runs that store the materializations
//! of one asset at the same time, as `event_write_contention.rs` measures it
//! on a SurrealDB server.

use std::time::Instant;

use super::*;
use crate::storage::migration::Capability;
use crate::storage::retry::StorageRetryConfig;

const PARTITIONS_PER_RUN: usize = 4368;

fn run_events(run: usize) -> Vec<EventWithId> {
    let (zone, run_id) = (format!("{run:03}"), format!("run-{run}"));
    (0..PARTITIONS_PER_RUN)
        .map(|hour| {
            let key = PartitionKey::Multi {
                dims: vec![
                    ("hour".to_string(), vec![format!("h{hour:05}")]),
                    ("zone".to_string(), vec![zone.clone()]),
                ],
            };
            let mut event = built("trips", &run_id, key, now_nanos());
            event.metadata = vec![("rows".to_string(), "{\"Int\":{\"value\":2595}}".to_string())];
            EventWithId::new(event)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "benchmark; needs RIVERS_TEST_POSTGRES_URL"]
async fn event_write_rate() {
    for (runs, chunk) in [(1, 512), (8, 64), (8, 512), (16, 512)] {
        let Some(storage) = store().await else {
            return;
        };
        register(&storage, "trips").await;
        let mut stores = Vec::new();
        for _ in 0..runs {
            let options = storage.options.clone();
            let endpoint = storage.endpoint.clone();
            let retry = StorageRetryConfig::default();
            let store = PostgresStorage::connect_with_options(
                options,
                endpoint,
                retry,
                Capability::ReadWrite,
            )
            .await
            .unwrap();
            stores.push(store);
        }
        let sends: Vec<Vec<EventWithId>> = (0..runs).map(run_events).collect();
        let started = Instant::now();
        let writes = stores
            .into_iter()
            .zip(sends)
            .map(|(store, events)| {
                async move {
                    for send in events.chunks(chunk) {
                        store.store_events_with_ids(send).await.unwrap();
                    }
                }
                .boxed()
            })
            .collect();
        race(writes).await;
        let wall = started.elapsed().as_secs_f64();
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM asset_partitions")
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(rows as usize, runs * PARTITIONS_PER_RUN);
        println!(
            "{runs:>2} runs, chunk {chunk:>4}: {wall:6.2} s, {:>7.0} partitions/s",
            rows as f64 / wall
        );
    }
}
