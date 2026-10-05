use super::*;

// -----------------------------------------------------------------------
// Executor integration pattern tests
// -----------------------------------------------------------------------

/// Simulates InProcess executor: sequential claim → execute → release cycle.
#[tokio::test]
async fn test_executor_sequential_claim_release() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "api", 1, 300)
        .await
        .unwrap();

    let steps = ["step_a", "step_b", "step_c"];
    for step in &steps {
        // Claim
        let status = storage
            .claim_concurrency_slots(
                crate::storage::DEFAULT_CODE_LOCATION_ID,
                &[("api".into(), 1)],
                "run1",
                step,
                0,
                300,
                None,
            )
            .await
            .unwrap();
        assert_eq!(status, ConcurrencyClaimStatus::Claimed);

        // Verify pool is at capacity
        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "api")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 1);

        // Release (simulates step completion)
        storage.free_concurrency_slots("run1", step).await.unwrap();

        let info = storage
            .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "api")
            .await
            .unwrap();
        assert_eq!(info.claimed_count, 0);
    }
}

/// Simulates Async executor: concurrent claims with pool limit controlling throughput.
#[tokio::test]
async fn test_executor_concurrent_claim_limit() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 2, 300)
        .await
        .unwrap();

    // Claim 2 slots (should succeed)
    let s1 = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".into(), 1)],
            "run1",
            "step_1",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    let s2 = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".into(), 1)],
            "run1",
            "step_2",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert_eq!(s1, ConcurrencyClaimStatus::Claimed);
    assert_eq!(s2, ConcurrencyClaimStatus::Claimed);

    // 3rd claim should pend
    let s3 = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".into(), 1)],
            "run1",
            "step_3",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(s3, ConcurrencyClaimStatus::Pending { .. }));

    // Release step_1, retry step_3 — should now succeed
    storage
        .free_concurrency_slots("run1", "step_1")
        .await
        .unwrap();
    let s3_retry = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".into(), 1)],
            "run1",
            "step_3",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert_eq!(s3_retry, ConcurrencyClaimStatus::Claimed);
}

/// Simulates run-level cleanup: free_concurrency_slots_for_run removes all slots held by a run.
#[tokio::test]
async fn test_executor_run_level_cleanup() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 5, 300)
        .await
        .unwrap();
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "api", 5, 300)
        .await
        .unwrap();

    // Claim slots for multiple steps across multiple pools
    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".into(), 1)],
            "run1",
            "step_a",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".into(), 1), ("api".into(), 1)],
            "run1",
            "step_b",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    // Also enqueue a pending step
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "tiny", 0, 300)
        .await
        .unwrap();
    // Can't claim with limit=0, let's set limit=1 and fill it first
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "tiny", 1, 300)
        .await
        .unwrap();
    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("tiny".into(), 1)],
            "run2",
            "other",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    let pending = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("tiny".into(), 1)],
            "run1",
            "step_c",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(pending, ConcurrencyClaimStatus::Pending { .. }));

    // Run-level cleanup: free everything for run1
    storage
        .free_concurrency_slots_for_run("run1")
        .await
        .unwrap();

    // All run1 slots should be gone
    let db_info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(db_info.claimed_count, 0, "run1 db slots should be freed");
    let api_info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "api")
        .await
        .unwrap();
    assert_eq!(api_info.claimed_count, 0, "run1 api slots should be freed");

    // run2's slot should be unaffected
    let tiny_info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "tiny")
        .await
        .unwrap();
    assert_eq!(tiny_info.claimed_count, 1, "run2 tiny slot should remain");
}

/// Simulates lease renewal pattern: claim, renew multiple times, verify lease stays alive.
#[tokio::test]
async fn test_executor_lease_renewal_pattern() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 1, 10)
        .await
        .unwrap(); // 10s lease

    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".into(), 1)],
            "run1",
            "step_a",
            0,
            10,
            None,
        )
        .await
        .unwrap();

    // Renew 3 times (simulates renewal interval of lease/3 ≈ 3.3s)
    for _ in 0..3 {
        let renewed = storage
            .renew_slot_lease("run1", "step_a", 10)
            .await
            .unwrap();
        assert_eq!(renewed, 1);
    }

    // Slot should still be active
    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(info.claimed_count, 1);

    // Release
    storage
        .free_concurrency_slots("run1", "step_a")
        .await
        .unwrap();
    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(info.claimed_count, 0);
}

/// Coordinator GC pattern: expired leases freed by free_expired_leases during tick.
#[tokio::test]
async fn test_coordinator_gc_frees_crashed_slots() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 2, 300)
        .await
        .unwrap();

    // Claim with very short lease (1 nanosecond — already expired)
    let now_ns = now_nanos();
    let expired_lease = now_ns - 1_000_000_000; // 1 second in the past
    storage
        .db
        .query(
            "CREATE concurrency_slots SET \
                 pool_key = 'db', run_id = 'crashed_run', step_key = 'step_x', \
                 slots_consumed = 1, claimed_at = $now, \
                 lease_expires_at = $exp, last_heartbeat = $now",
        )
        .bind(("now", now_ns))
        .bind(("exp", expired_lease))
        .await
        .unwrap();

    // Pool shows 0 claimed (expired excluded from capacity check)
    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(info.claimed_count, 0);

    // GC sweep removes the physical row
    let freed = storage.free_expired_leases().await.unwrap();
    assert_eq!(freed, 1);
}

// -----------------------------------------------------------------------
// Coordinator tick overhead stress test
// -----------------------------------------------------------------------

/// Simulates a full coordinator tick cycle at various queue/run sizes and measures wall-clock time.
#[tokio::test]
async fn coordinator_tick_stress() {
    use std::time::Instant;

    let storage = make_storage().await;

    // Setup: 5 pools with active slots + pending steps
    for pool in ["db", "api", "gpu", "cpu", "net"] {
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, pool, 10, 300)
            .await
            .unwrap();
    }

    // Scenario matrix: (in_progress_runs, queued_runs, active_pool_slots)
    let scenarios: &[(usize, usize, usize)] = &[
        (0, 0, 0),          // idle system
        (5, 10, 20),        // moderate queue
        (10, 50, 50),       // busy system
        (10, 200, 100),     // large queue
        (10, 500, 200),     // 500 queued
        (10, 1_000, 500),   // 1k queued
        (10, 2_000, 500),   // 2k queued
        (10, 5_000, 1000),  // 5k queued
        (10, 10_000, 1000), // 10k queued
    ];

    for &(n_in_progress, n_queued, n_slots) in scenarios {
        // Clean slate per scenario
        storage
            .db
            .query("DELETE FROM runs; DELETE FROM concurrency_slots; DELETE FROM pending_steps")
            .await
            .unwrap();

        let now = now_nanos();
        let lease_exp = now + 300_000_000_000i64; // 5 min from now

        // Create in-progress runs
        for i in 0..n_in_progress {
            storage
                .create_run(&RunRecord {
                    run_id: format!("ip-{i}"),
                    code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                    job_name: Some("bench".into()),
                    status: RunStatus::Started,
                    start_time: now,
                    end_time: None,
                    tags: vec![
                        ("env".into(), "prod".into()),
                        ("team".into(), format!("team-{}", i % 5)),
                    ],
                    node_names: vec![format!("asset_{i}")],
                    priority: 0,
                    partition_key: None,
                    block_reason: None,
                    launched_by: LaunchedBy::Manual { user: None },
                    action: None,
                    config: None,
                })
                .await
                .unwrap();
        }

        // Create queued runs
        for i in 0..n_queued {
            storage
                .create_run(&RunRecord {
                    run_id: format!("q-{i}"),
                    code_location_id: DEFAULT_CODE_LOCATION_ID.to_string(),
                    job_name: Some("bench".into()),
                    status: RunStatus::Queued,
                    start_time: now,
                    end_time: None,
                    tags: vec![("env".into(), "staging".into())],
                    node_names: vec![format!("asset_{i}")],
                    priority: i as i32 % 10,
                    partition_key: None,
                    block_reason: None,
                    launched_by: LaunchedBy::Manual { user: None },
                    action: None,
                    config: None,
                })
                .await
                .unwrap();
        }

        // Create active pool slots
        for i in 0..n_slots {
            let pool = ["db", "api", "gpu", "cpu", "net"][i % 5];
            storage
                .db
                .query(
                    "CREATE concurrency_slots SET \
                     pool_key = $pool, run_id = $rid, step_key = $sk, \
                     slots_consumed = 1, claimed_at = $now, \
                     lease_expires_at = $exp, last_heartbeat = $now",
                )
                .bind(("pool", pool))
                .bind(("rid", format!("ip-{}", i % n_in_progress.max(1))))
                .bind(("sk", format!("step_{i}")))
                .bind(("now", now))
                .bind(("exp", lease_exp))
                .await
                .unwrap();
        }

        // Warm up
        let _ = storage
            .coordinator_tick_query(DEFAULT_CODE_LOCATION_ID)
            .await;

        let n_ticks: usize = if n_queued >= 2000 { 10 } else { 50 };
        let start = Instant::now();
        for _ in 0..n_ticks {
            let _ = storage
                .coordinator_tick_query(DEFAULT_CODE_LOCATION_ID)
                .await
                .unwrap();
        }
        let elapsed = start.elapsed();
        let per_tick = elapsed / n_ticks as u32;

        eprintln!(
            "  in_progress={n_in_progress:>3}, queued={n_queued:>5}, slots={n_slots:>4} → \
                 {per_tick:>8.3?}/tick ({n_ticks} ticks in {elapsed:.3?})"
        );
    }
}
