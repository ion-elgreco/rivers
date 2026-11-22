//! PostgreSQL only: claims that race, and the place of a waiting step in
//! the queue.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use super::*;

const CL: &str = crate::storage::DEFAULT_CODE_LOCATION_ID;

async fn claim(
    storage: &PostgresStorage,
    pools: &[(String, u32)],
    run: &str,
    step: &str,
    lease: u32,
    scope: Option<&AssetScope>,
) -> ConcurrencyClaimStatus {
    storage
        .claim_concurrency_slots(CL, pools, run, step, 0, lease, scope)
        .await
        .unwrap_or_else(|e| panic!("claim by {run}/{step} failed: {e:#}"))
}

fn is_claimed(status: &ConcurrencyClaimStatus) -> bool {
    matches!(status, ConcurrencyClaimStatus::Claimed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sixteen_claimers_never_take_more_than_a_limit_of_three() {
    let Some(url) = PostgresStorage::test_url() else {
        return;
    };
    let storage = Arc::new(PostgresStorage::connect_test_schema(&url).await.unwrap());
    storage.set_pool_limit(CL, "db", 3, 300).await.unwrap();
    let pools = Arc::new([("db".to_string(), 1)]);

    // All at once: three get in, thirteen wait.
    let barrier = Arc::new(tokio::sync::Barrier::new(16));
    let mut tasks = Vec::new();
    for i in 0..16 {
        let (storage, pools, barrier) = (storage.clone(), pools.clone(), barrier.clone());
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            claim(&storage, &pools[..], &format!("run_{i}"), "load", 300, None).await
        }));
    }
    let mut claimed = Vec::new();
    for (i, task) in tasks.into_iter().enumerate() {
        if is_claimed(&task.await.unwrap()) {
            claimed.push(format!("run_{i}"));
        }
    }
    assert_eq!(claimed.len(), 3, "claimed: {claimed:?}");
    let info = storage.get_pool_info(CL, "db").await.unwrap();
    assert_eq!((info.claimed_count, info.pending_count), (3, 13));
    for run in &claimed {
        storage.free_concurrency_slots(run, "load").await.unwrap();
    }

    // Then each claims, holds a while and frees, until all have run.
    let in_flight = Arc::new(AtomicU32::new(0));
    let most = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();
    for i in 0..16 {
        let (storage, pools) = (storage.clone(), pools.clone());
        let (in_flight, most) = (in_flight.clone(), most.clone());
        tasks.push(tokio::spawn(async move {
            let run = format!("run_{i}");
            while !is_claimed(&claim(&storage, &pools[..], &run, "load", 300, None).await) {
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
            most.fetch_max(
                in_flight.fetch_add(1, Ordering::SeqCst) + 1,
                Ordering::SeqCst,
            );
            let info = storage.get_pool_info(CL, "db").await.unwrap();
            assert!(info.claimed_count <= 3, "{info:?}");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            in_flight.fetch_sub(1, Ordering::SeqCst);
            storage.free_concurrency_slots(&run, "load").await.unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert!(
        most.load(Ordering::SeqCst) <= 3,
        "more than 3 held the pool"
    );
    let info = storage.get_pool_info(CL, "db").await.unwrap();
    assert_eq!((info.claimed_count, info.pending_count), (0, 0));
}

/// A step that claims again while it waits stays one row in the queue,
/// with the time it was first queued.
#[tokio::test]
async fn a_waiting_step_keeps_its_place() {
    let Some(url) = PostgresStorage::test_url() else {
        return;
    };
    let storage = PostgresStorage::connect_test_schema(&url).await.unwrap();
    storage.set_pool_limit(CL, "db", 1, 300).await.unwrap();
    let pools = [("db".to_string(), 1)];
    let position = async |run: &str| match claim(&storage, &pools, run, "load", 300, None).await {
        ConcurrencyClaimStatus::Pending { position, .. } => position,
        ConcurrencyClaimStatus::Claimed => panic!("{run} must wait"),
    };
    let enqueued_at = async |run: &str| -> i64 {
        sqlx::query_scalar("SELECT enqueued_at FROM pending_steps WHERE run_id = $1")
            .bind(run)
            .fetch_one(storage.pool())
            .await
            .unwrap()
    };

    assert!(is_claimed(
        &claim(&storage, &pools, "run_0", "load", 300, None).await
    ));
    assert_eq!(position("run_1").await, 1);
    assert_eq!(position("run_2").await, 2);
    let first = enqueued_at("run_1").await;
    assert_eq!(position("run_1").await, 2);
    assert_eq!(enqueued_at("run_1").await, first);
    let info = storage.get_pool_info(CL, "db").await.unwrap();
    assert_eq!(info.pending_count, 2);
}
