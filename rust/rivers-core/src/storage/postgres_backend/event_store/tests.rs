//! PostgreSQL only: runs that store the materializations of one asset at
//! the same time.

use std::sync::Arc;

use futures_util::FutureExt;
use futures_util::future::BoxFuture;

use super::*;
use crate::storage::DEFAULT_CODE_LOCATION_ID as CL;

mod write_rate;

async fn store() -> Option<Arc<PostgresStorage>> {
    let url = PostgresStorage::test_url()?;
    Some(Arc::new(
        PostgresStorage::connect_test_schema(&url).await.unwrap(),
    ))
}

async fn register(storage: &PostgresStorage, asset: &str) {
    let record = AssetRecord {
        code_location_id: CL.to_string(),
        asset_key: asset.to_string(),
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
    storage.register_assets(CL, vec![record]).await.unwrap();
}

fn built(asset: &str, run_id: &str, key: PartitionKey, at: i64) -> EventRecord {
    EventRecord {
        code_location_id: CL.to_string(),
        event_type: EventType::Materialization { data_version: None },
        asset_key: Some(asset.to_string()),
        run_id: run_id.to_string(),
        partition_key: Some(key),
        timestamp: at,
        metadata: vec![],
        input_data_versions: vec![],
    }
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

/// Eight runs store the materializations of one asset at the same time, in
/// several sends each, out of time order: each materialization keeps its
/// row, and the asset row holds the newest.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn eight_runs_of_one_asset_lose_no_materialization() {
    const RUNS: i64 = 8;
    const PARTITIONS: i64 = 96;
    let Some(storage) = store().await else {
        return;
    };
    register(&storage, "a").await;
    let runs: Vec<Vec<EventWithId>> = (0..RUNS)
        .map(|run| {
            (0..PARTITIONS)
                .map(|hour| {
                    let key = PartitionKey::Multi {
                        dims: vec![
                            ("zone".to_string(), vec![format!("z{run}")]),
                            ("hour".to_string(), vec![format!("h{hour:03}")]),
                        ],
                    };
                    let at = 1_000 + (hour * 37 % PARTITIONS) * RUNS + run;
                    EventWithId::new(built("a", &format!("run-{run}"), key, at))
                })
                .collect()
        })
        .collect();
    let mut expected: Vec<(String, String, i64)> = runs
        .iter()
        .flatten()
        .map(|e| {
            (
                record_id_str(&e.id),
                e.event.run_id.clone(),
                e.event.timestamp,
            )
        })
        .collect();
    expected.sort();
    let newest = expected.iter().max_by_key(|row| row.2).unwrap().clone();

    let writes = runs
        .into_iter()
        .map(|events| {
            let storage = storage.clone();
            async move {
                for send in events.chunks(16) {
                    storage.store_events_with_ids(send).await.unwrap();
                }
            }
            .boxed()
        })
        .collect();
    race(writes).await;

    let stored: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM events WHERE asset_key = 'a' AND event_type = 'Materialization'",
    )
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!(stored, RUNS * PARTITIONS);
    let mut rows: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT last_event_id, last_run_id, last_timestamp FROM asset_partitions \
         WHERE code_location_id = $1 AND asset_key = 'a'",
    )
    .bind(CL)
    .fetch_all(storage.pool())
    .await
    .unwrap();
    rows.sort();
    assert_eq!(rows, expected);
    let asset: (String, String, i64) = sqlx::query_as(
        "SELECT last_event_id, last_run_id, last_timestamp FROM assets \
         WHERE code_location_id = $1 AND asset_key = 'a'",
    )
    .bind(CL)
    .fetch_one(storage.pool())
    .await
    .unwrap();
    assert_eq!(asset, newest);
}
