use super::*;

// ── Claim/Release protocol tests ──

// This test runs against the RocksDB backend rather than `make_storage()`
// (kv-mem). The kv-mem implementation (surrealmx) has a known race in its
// commit-queue conflict check that causes occasional lost updates under
// concurrent writers — production uses RocksDB, so the test exercises the
// path that actually has to hold up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sentinel_write_write_conflict() {
    // Prove that concurrent transactions writing the same key are detected:
    // 100 tasks all increment claim_version inside BEGIN/COMMIT.
    // With conflict detection, some will fail. Without retry, the final
    // counter value equals the number of successful commits.
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = std::sync::Arc::new(
        SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
            .await
            .expect("failed to create rocksdb storage"),
    );
    storage
        .set_pool_limit(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "conflict_test",
            10,
            300,
        )
        .await
        .unwrap();

    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(100));
    let conflicts = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let successes = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));

    let mut handles = Vec::new();
    for _ in 0..100 {
        let storage = storage.clone();
        let barrier = barrier.clone();
        let conflicts = conflicts.clone();
        let successes = successes.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let result = storage
                .db
                .query(
                    "BEGIN TRANSACTION; \
                         UPDATE concurrency_pools \
                             SET claim_version = claim_version + 1 \
                             WHERE pool_key = 'conflict_test'; \
                         COMMIT TRANSACTION;",
                )
                .await;
            match result {
                Ok(resp) => match resp.check() {
                    Ok(_) => {
                        successes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    Err(_) => {
                        conflicts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                },
                Err(_) => {
                    conflicts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let total_conflicts = conflicts.load(std::sync::atomic::Ordering::Relaxed);
    let total_successes = successes.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(total_conflicts + total_successes, 100);

    // Final claim_version must equal successful commits (no lost updates).
    let mut result = storage
        .db
        .query(
            "SELECT VALUE claim_version FROM concurrency_pools \
                 WHERE pool_key = 'conflict_test' LIMIT 1",
        )
        .await
        .unwrap();
    let versions: Vec<u32> = result.take(0).unwrap();
    let final_version = versions[0];

    assert_eq!(
        final_version, total_successes,
        "claim_version ({final_version}) must equal successful commits ({total_successes}), \
             conflicts={total_conflicts}"
    );

    // Log the outcome for visibility.
    eprintln!(
        "sentinel test: successes={total_successes}, conflicts={total_conflicts}, \
             claim_version={final_version}"
    );
}

#[tokio::test]
async fn test_claim_check_statement_index() {
    let storage = make_storage().await;
    let pool_names = ["a", "b", "c", "d", "e"];
    for name in &pool_names {
        storage
            .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, name, 10, 300)
            .await
            .unwrap();
    }

    for n in 1..=5 {
        let pools: Vec<(String, u32)> =
            pool_names[..n].iter().map(|k| (k.to_string(), 1)).collect();
        let query = SurrealStorage::build_claim_transaction(&pools, &[], None);
        let now_ns = now_nanos();
        let lease_exp = now_ns + 300_000_000_000i64;

        let mut q = storage.db.query(&query);
        for (i, (pk, _)) in pools.iter().enumerate() {
            q = q.bind((format!("p{i}"), pk.clone()));
        }
        q = q
            .bind(("cl", crate::storage::DEFAULT_CODE_LOCATION_ID.to_string()))
            .bind(("run_id", format!("run_{n}")))
            .bind(("step_key", format!("step_{n}")))
            .bind(("now", now_ns))
            .bind(("lease_exp", lease_exp));

        let mut response = q.await.unwrap().check().unwrap();
        let idx = SurrealStorage::claim_check_statement_index(n, 0);
        let count: Option<u32> = response.take((idx, "total")).unwrap();
        assert_eq!(
            count,
            Some(n as u32),
            "pools={n}: expected {n} slots at statement index {idx}"
        );
    }

    // Mixed counted + asset pools: asset pools emit one LET (the overlap
    // predicate) instead of two ($lim/$used), so the formula's asset term
    // is only exercised when both kinds ride one transaction.
    for n_counted in 1..=2 {
        let mut pools: Vec<(String, u32)> = pool_names[..n_counted]
            .iter()
            .map(|k| (k.to_string(), 1))
            .collect();
        // Distinct per iteration — the exclusive overlap claim from one
        // iteration would contend the next on a shared asset pool.
        pools.push((format!("__asset__:orders_{n_counted}"), 1));
        let asset_idx = [pools.len() - 1];
        let query = SurrealStorage::build_claim_transaction(&pools, &asset_idx, None);
        let now_ns = now_nanos();
        let lease_exp = now_ns + 300_000_000_000i64;

        let mut q = storage.db.query(&query);
        for (i, (pk, _)) in pools.iter().enumerate() {
            q = q.bind((format!("p{i}"), pk.clone()));
        }
        q = q
            .bind(("cl", crate::storage::DEFAULT_CODE_LOCATION_ID.to_string()))
            .bind(("run_id", format!("mixed_run_{n_counted}")))
            .bind(("step_key", format!("mixed_step_{n_counted}")))
            .bind(("now", now_ns))
            .bind(("lease_exp", lease_exp));

        let mut response = q.await.unwrap().check().unwrap();
        let idx = SurrealStorage::claim_check_statement_index(pools.len(), 1);
        let count: Option<u32> = response.take((idx, "total")).unwrap();
        assert_eq!(
            count,
            Some(pools.len() as u32),
            "mixed pools={} + 1 asset: expected slots at statement index {idx}",
            n_counted
        );
    }
}

#[tokio::test]
async fn test_claim_single_pool_success() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 3, 300)
        .await
        .unwrap();

    let status = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
            "run1",
            "step_a",
            0,
            300,
            None,
        )
        .await
        .unwrap();

    assert_eq!(status, ConcurrencyClaimStatus::Claimed);

    // Verify slot was created
    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(info.claimed_count, 1);
    assert_eq!(info.pending_count, 0);
}

#[tokio::test]
async fn test_claim_single_pool_full() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 1, 300)
        .await
        .unwrap();

    // Claim the only slot
    let s1 = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
            "run1",
            "step_a",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert_eq!(s1, ConcurrencyClaimStatus::Claimed);

    // Second claim should be pending
    let s2 = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
            "run2",
            "step_b",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    match &s2 {
        ConcurrencyClaimStatus::Pending { reason, .. } => {
            assert!(
                matches!(reason, BlockReason::PoolFull { pool_key, claimed: 1, limit: 1 } if pool_key == "db")
            );
        }
        ConcurrencyClaimStatus::Claimed => panic!("expected Pending, got Claimed"),
    }

    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(info.claimed_count, 1);
    assert_eq!(info.pending_count, 1);
}

#[tokio::test]
async fn test_claim_and_release_then_reclaim() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 1, 300)
        .await
        .unwrap();

    // Claim
    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
            "run1",
            "step_a",
            0,
            300,
            None,
        )
        .await
        .unwrap();

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

    // Reclaim should succeed
    let s = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
            "run2",
            "step_b",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert_eq!(s, ConcurrencyClaimStatus::Claimed);
}

/// Claim `pool` for `(run, step)` with the given scope, asserting the
/// storage call itself succeeded, and return whether it got in.
async fn claim_scoped(
    storage: &SurrealStorage,
    pool: &str,
    run: &str,
    step: &str,
    partitions: Option<&[&str]>,
    exclusive: bool,
) -> bool {
    let scope = AssetScope {
        partitions: partitions.map(|p| p.iter().map(|s| s.to_string()).collect()),
        exclusive,
    };
    let status = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[(pool.to_string(), 1)],
            run,
            step,
            0,
            300,
            Some(&scope),
        )
        .await
        .unwrap();
    matches!(status, ConcurrencyClaimStatus::Claimed)
}

/// The whole point of scoping the implicit pool: an action on one partition
/// must not block work on a different one.
#[tokio::test]
async fn test_asset_scope_disjoint_partitions_do_not_conflict() {
    let storage = make_storage().await;
    let pool = "__asset__:orders";
    storage
        .set_pool_limit(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            pool,
            1_000_000,
            300,
        )
        .await
        .unwrap();

    // delete(p1) — exclusive on p1 only.
    assert!(
        claim_scoped(&storage, pool, "r1", "delete", Some(&["p1"]), true).await,
        "first claim on an empty pool always lands"
    );
    // materialize(p2) — different partition, must not be blocked.
    assert!(
        claim_scoped(&storage, pool, "r2", "mat_p2", Some(&["p2"]), false).await,
        "materialize of p2 must run beside delete(p1)"
    );
    // delete(p3) — a second exclusive action, disjoint from both holders.
    assert!(
        claim_scoped(&storage, pool, "r3", "delete_p3", Some(&["p3"]), true).await,
        "two actions on disjoint partitions must run together"
    );
    // delete(p2) overlaps the running materialize of p2, so it must wait.
    assert!(
        !claim_scoped(&storage, pool, "r5", "delete_p2", Some(&["p2"]), true).await,
        "an action must wait for a materialize of the same partition"
    );
    // materialize(p1) — same partition as the running delete, must wait.
    assert!(
        !claim_scoped(&storage, pool, "r4", "mat_p1", Some(&["p1"]), false).await,
        "materialize of p1 must block on delete(p1)"
    );
}

/// The safety-critical cell: two exclusive actions on the SAME partition
/// must serialize, and two whole-asset actions must serialize too — the old
/// slot-count rule gave both for free, the overlap rule has to re-earn them.
#[tokio::test]
async fn test_asset_scope_exclusive_conflicts_with_exclusive() {
    let storage = make_storage().await;
    let pool = "__asset__:orders";
    storage
        .set_pool_limit(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            pool,
            1_000_000,
            300,
        )
        .await
        .unwrap();

    assert!(claim_scoped(&storage, pool, "r1", "del_a", Some(&["p1"]), true).await);
    assert!(
        !claim_scoped(&storage, pool, "r2", "del_b", Some(&["p1"]), true).await,
        "two exclusive actions on the same partition must serialize"
    );

    let storage2 = make_storage().await;
    storage2
        .set_pool_limit(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            pool,
            1_000_000,
            300,
        )
        .await
        .unwrap();
    assert!(claim_scoped(&storage2, pool, "r1", "opt_a", None, true).await);
    assert!(
        !claim_scoped(&storage2, pool, "r2", "opt_b", None, true).await,
        "two whole-asset actions must serialize"
    );
}

/// `CONTAINSANY` must mean *any* member, on both sides. Every other test
/// uses a single-element set, which a wrong all-vs-any operator would pass.
#[tokio::test]
async fn test_asset_scope_batched_sets_overlap_by_any_member() {
    let storage = make_storage().await;
    let pool = "__asset__:orders";
    storage
        .set_pool_limit(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            pool,
            1_000_000,
            300,
        )
        .await
        .unwrap();

    // A batched *exclusive* action — the only place a multi-member set is
    // written with exclusive = true.
    assert!(
        claim_scoped(
            &storage,
            pool,
            "r1",
            "del_batch",
            Some(&["p1", "p2", "p3"]),
            true
        )
        .await
    );
    // Overlap on the tail member only.
    assert!(
        !claim_scoped(&storage, pool, "r2", "mat_b", Some(&["p3", "p9"]), false).await,
        "one shared member is enough to conflict"
    );
    // Fully disjoint batch.
    assert!(
        claim_scoped(&storage, pool, "r3", "mat_c", Some(&["p9", "p10"]), false).await,
        "disjoint batches must not conflict"
    );
    // A second exclusive batch, disjoint from the first.
    assert!(
        claim_scoped(&storage, pool, "r4", "del_d", Some(&["p4", "p5"]), true).await,
        "disjoint exclusive batches run together"
    );
}

/// Whole-asset scope on the *claimant* side with `exclusive: false` — an
/// unpartitioned materialize while a partition action runs.
#[tokio::test]
async fn test_asset_scope_whole_asset_shared_blocks_on_partition_action() {
    let storage = make_storage().await;
    let pool = "__asset__:orders";
    storage
        .set_pool_limit(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            pool,
            1_000_000,
            300,
        )
        .await
        .unwrap();

    assert!(claim_scoped(&storage, pool, "r1", "del_p1", Some(&["p1"]), true).await);
    assert!(
        !claim_scoped(&storage, pool, "r2", "mat_all", None, false).await,
        "a whole-asset materialize covers p1, so it must wait for delete(p1)"
    );
}

/// The pre-check and the transaction both read other rows' scopes before
/// writing their own. Concurrency is fenced by the shared `claim_version`
/// bump on the pool row; without it both would see "no conflict" and commit.
#[tokio::test]
async fn test_asset_scope_concurrent_exclusive_claims_admit_one() {
    let storage = std::sync::Arc::new(make_storage().await);
    let pool = "__asset__:orders";
    storage
        .set_pool_limit(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            pool,
            1_000_000,
            300,
        )
        .await
        .unwrap();

    let mut tasks = Vec::new();
    for i in 0..8 {
        let s = storage.clone();
        tasks.push(tokio::spawn(async move {
            claim_scoped(&s, pool, &format!("r{i}"), "del", Some(&["p1"]), true).await
        }));
    }
    let mut claimed = 0;
    for t in tasks {
        if t.await.unwrap() {
            claimed += 1;
        }
    }
    assert_eq!(
        claimed, 1,
        "exactly one exclusive claim on a partition may be admitted"
    );
    let holders = storage
        .get_pool_slot_holders(crate::storage::DEFAULT_CODE_LOCATION_ID, pool)
        .await
        .unwrap();
    assert_eq!(holders.len(), 1, "and only one slot row exists");
}

#[tokio::test]
async fn test_asset_scope_shared_never_conflicts() {
    let storage = make_storage().await;
    let pool = "__asset__:orders";
    storage
        .set_pool_limit(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            pool,
            1_000_000,
            300,
        )
        .await
        .unwrap();

    // Fan-out: many materialize instances of the SAME partition coexist.
    for i in 0..5 {
        assert!(
            claim_scoped(
                &storage,
                pool,
                "r1",
                &format!("inst_{i}"),
                Some(&["p1"]),
                false
            )
            .await,
            "non-exclusive holders never conflict, even on one partition"
        );
    }
    // An action on that partition now has to wait for all of them.
    assert!(
        !claim_scoped(&storage, pool, "r2", "delete", Some(&["p1"]), true).await,
        "an action must wait for the materializes it overlaps"
    );
}

#[tokio::test]
async fn test_asset_scope_batch_overlap_blocks() {
    let storage = make_storage().await;
    let pool = "__asset__:orders";
    storage
        .set_pool_limit(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            pool,
            1_000_000,
            300,
        )
        .await
        .unwrap();

    // A batched materialize over p1..p3 — one row carrying the whole set.
    assert!(
        claim_scoped(
            &storage,
            pool,
            "r1",
            "batch",
            Some(&["p1", "p2", "p3"]),
            false
        )
        .await
    );
    // An action inside the batch's range overlaps it.
    assert!(
        !claim_scoped(&storage, pool, "r2", "del_p2", Some(&["p2"]), true).await,
        "delete(p2) overlaps the batch and must wait"
    );
    // An action outside it does not — no threshold, no over-blocking.
    assert!(
        claim_scoped(&storage, pool, "r3", "del_p9", Some(&["p9"]), true).await,
        "delete(p9) is disjoint from the batch and must run"
    );
}

#[tokio::test]
async fn test_asset_scope_whole_asset_conflicts_with_everything() {
    let storage = make_storage().await;
    let pool = "__asset__:orders";
    storage
        .set_pool_limit(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            pool,
            1_000_000,
            300,
        )
        .await
        .unwrap();

    // An unpartitioned action: `partitions: None` = the whole asset.
    assert!(claim_scoped(&storage, pool, "r1", "optimize", None, true).await);
    assert!(
        !claim_scoped(&storage, pool, "r2", "mat_p1", Some(&["p1"]), false).await,
        "a whole-asset action blocks every partition"
    );

    // And the reverse order: a partition holder blocks a whole-asset action.
    let storage2 = make_storage().await;
    storage2
        .set_pool_limit(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            pool,
            1_000_000,
            300,
        )
        .await
        .unwrap();
    assert!(claim_scoped(&storage2, pool, "r1", "mat_p1", Some(&["p1"]), false).await);
    assert!(
        !claim_scoped(&storage2, pool, "r2", "optimize", None, true).await,
        "a whole-asset action waits for any partition holder"
    );
}

#[tokio::test]
async fn test_asset_scope_released_slot_unblocks() {
    let storage = make_storage().await;
    let pool = "__asset__:orders";
    storage
        .set_pool_limit(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            pool,
            1_000_000,
            300,
        )
        .await
        .unwrap();

    assert!(claim_scoped(&storage, pool, "r1", "delete", Some(&["p1"]), true).await);
    assert!(!claim_scoped(&storage, pool, "r2", "mat_p1", Some(&["p1"]), false).await);

    storage
        .free_concurrency_slots("r1", "delete")
        .await
        .unwrap();
    assert!(
        claim_scoped(&storage, pool, "r2", "mat_p1", Some(&["p1"]), false).await,
        "releasing the action's slot must unblock the partition"
    );
}

/// One `concurrency_slots` row, read back to check what a claim wrote.
#[derive(Debug, PartialEq, SurrealValue)]
struct HeldSlot {
    pool_key: String,
    slots_consumed: u32,
    claimed_at: i64,
    lease_expires_at: i64,
    partitions: Option<Vec<String>>,
    exclusive: bool,
}

/// Every slot row `(run, step)` holds, by pool. `partitions` is a set in
/// storage and comes back as a sorted array.
async fn held_slots(storage: &SurrealStorage, run: &str, step: &str) -> Vec<HeldSlot> {
    storage
        .db
        .query(
            "SELECT pool_key, slots_consumed, claimed_at, lease_expires_at, exclusive, \
                     IF partitions IS NONE THEN NONE ELSE array::sort(<array> partitions) END \
                         AS partitions \
                 FROM concurrency_slots \
                 WHERE run_id = $run_id AND step_key = $step_key ORDER BY pool_key",
        )
        .bind(("run_id", run.to_string()))
        .bind(("step_key", step.to_string()))
        .await
        .unwrap()
        .check()
        .unwrap()
        .take(0)
        .unwrap()
}

/// A step that runs again under its run and step key (a Kubernetes retry
/// pod after an OOM kill, or a `--resume`) claims while the row of its
/// killed attempt still holds the pool. The claim must take that row over:
/// a second row for the step collided with `idx_slot_unique`, the claim
/// failed after its whole retry budget, and the step never ran.
#[tokio::test]
async fn test_asset_pool_reclaim_by_same_step_takes_over_its_slot() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
        .await
        .expect("failed to create rocksdb storage");
    let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
    let scope = |partitions: Option<&[&str]>, exclusive| AssetScope {
        partitions: partitions.map(|p| p.iter().map(|s| s.to_string()).collect()),
        exclusive,
    };
    // (killed attempt's scope, re-claim's scope)
    let cases = [
        // A materialize, of the whole asset and of one partition.
        (scope(None, false), scope(None, false)),
        (scope(Some(&["p1"]), false), scope(Some(&["p1"]), false)),
        // An exclusive action, likewise.
        (scope(None, true), scope(None, true)),
        (scope(Some(&["p1"]), true), scope(Some(&["p1"]), true)),
        // The re-claim's scope replaces the killed attempt's.
        (scope(None, true), scope(Some(&["p1"]), false)),
    ];
    for (i, (killed_scope, scope)) in cases.iter().enumerate() {
        let pool = format!("__asset__:orders_{i}");
        let pools = [(pool.clone(), 1)];
        let run = format!("run_{i}");
        storage.set_pool_limit(cl, &pool, -1, 300).await.unwrap();

        let status = storage
            .claim_concurrency_slots(cl, &pools, &run, "orders", 0, 60, Some(killed_scope))
            .await
            .unwrap();
        assert_eq!(status, ConcurrencyClaimStatus::Claimed, "case {i}");
        let killed = held_slots(&storage, &run, "orders").await;

        // The killed attempt never released its slot.
        let status = storage
            .claim_concurrency_slots(cl, &pools, &run, "orders", 0, 300, Some(scope))
            .await
            .unwrap_or_else(|e| panic!("case {i}: re-claim failed: {e:#}"));
        assert_eq!(status, ConcurrencyClaimStatus::Claimed, "case {i}");

        let held = held_slots(&storage, &run, "orders").await;
        assert_eq!(
            held.len(),
            1,
            "case {i}: one row per pool and step: {held:?}"
        );
        let row = &held[0];
        assert_eq!(row.pool_key, pool);
        assert_eq!(row.slots_consumed, 1);
        assert!(
            row.claimed_at > killed[0].claimed_at
                && row.lease_expires_at > killed[0].lease_expires_at,
            "case {i}: the row must carry the re-claim's lease: {row:?} vs {killed:?}"
        );
        assert_eq!(row.partitions, scope.partitions, "case {i}");
        assert_eq!(row.exclusive, scope.exclusive, "case {i}");

        let parts: Option<Vec<&str>> = scope
            .partitions
            .as_ref()
            .map(|p| p.iter().map(String::as_str).collect());
        assert!(
            !claim_scoped(
                &storage,
                &pool,
                "other_run",
                "optimize",
                parts.as_deref(),
                true
            )
            .await,
            "case {i}: the taken-over slot must still hold off another run's action"
        );
    }
    // The killed attempt held the whole asset exclusively; the re-claim
    // holds p1, shared. Another run's materialize of p2 must get in.
    assert!(
        claim_scoped(
            &storage,
            "__asset__:orders_4",
            "other_run",
            "mat_p2",
            Some(&["p2"]),
            false
        )
        .await
    );
}

/// The same take-over on a counted pool: the step holds one slot, not two.
#[tokio::test]
async fn test_user_pool_reclaim_by_same_step_takes_over_its_slot() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
        .await
        .expect("failed to create rocksdb storage");
    let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
    storage.set_pool_limit(cl, "db", 2, 300).await.unwrap();
    let pools = [("db".to_string(), 1)];
    let claim = async |run: &str, lease: u32| {
        storage
            .claim_concurrency_slots(cl, &pools, run, "load", 0, lease, None)
            .await
            .unwrap_or_else(|e| panic!("claim by {run} failed: {e:#}"))
    };

    assert_eq!(claim("run_r", 60).await, ConcurrencyClaimStatus::Claimed);
    let killed = held_slots(&storage, "run_r", "load").await;
    assert_eq!(claim("run_r", 300).await, ConcurrencyClaimStatus::Claimed);

    let held = held_slots(&storage, "run_r", "load").await;
    assert_eq!(held.len(), 1, "one row per pool and step: {held:?}");
    let row = &held[0];
    assert_eq!(
        (row.pool_key.as_str(), row.slots_consumed, row.exclusive),
        ("db", 1, false)
    );
    assert_eq!(row.partitions, None);
    assert!(
        row.claimed_at > killed[0].claimed_at && row.lease_expires_at > killed[0].lease_expires_at,
        "the row must carry the re-claim's lease: {row:?} vs {killed:?}"
    );
    assert_eq!(
        storage.get_pool_info(cl, "db").await.unwrap().claimed_count,
        1
    );
    assert_eq!(claim("run_b", 300).await, ConcurrencyClaimStatus::Claimed);
    assert!(
        matches!(
            claim("run_c", 300).await,
            ConcurrencyClaimStatus::Pending { .. }
        ),
        "the pool of two is full with run_r and run_b"
    );
}

/// A step on a counted pool and its asset's pool takes over both rows.
#[tokio::test]
async fn test_multi_pool_reclaim_by_same_step_takes_over_every_slot() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
        .await
        .expect("failed to create rocksdb storage");
    let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
    storage.set_pool_limit(cl, "db", 2, 300).await.unwrap();
    storage
        .set_pool_limit(cl, "__asset__:orders", -1, 300)
        .await
        .unwrap();
    let pools = [("db".to_string(), 1), ("__asset__:orders".to_string(), 1)];
    let scope = AssetScope {
        partitions: Some(vec!["p1".to_string()]),
        exclusive: false,
    };

    for lease in [60, 300] {
        let status = storage
            .claim_concurrency_slots(cl, &pools, "run_r", "orders", 0, lease, Some(&scope))
            .await
            .unwrap_or_else(|e| panic!("claim with a {lease}s lease failed: {e:#}"));
        assert_eq!(status, ConcurrencyClaimStatus::Claimed);
    }

    let held = held_slots(&storage, "run_r", "orders").await;
    let pools_held: Vec<(&str, Option<&[String]>)> = held
        .iter()
        .map(|r| (r.pool_key.as_str(), r.partitions.as_deref()))
        .collect();
    assert_eq!(
        pools_held,
        [
            ("__asset__:orders", Some(&["p1".to_string()][..])),
            ("db", None)
        ]
    );
    let now = now_nanos();
    assert!(
        held.iter()
            .all(|r| r.lease_expires_at > now + 200 * 1_000_000_000),
        "both rows must carry the re-claim's 300s lease: {held:?}"
    );
    assert_eq!(
        storage.get_pool_info(cl, "db").await.unwrap().claimed_count,
        1
    );
}

/// The count a claim reads from its claim-result statement, for the claim
/// transaction run the way a claim runs it once its pre-check has passed.
/// A claim by another step that commits between the two leaves the
/// transaction's `IF` false.
async fn claim_transaction_count(
    storage: &SurrealStorage,
    pools: &[(String, u32)],
    asset_pools: &[usize],
    run: &str,
    step: &str,
    scope: Option<&AssetScope>,
) -> u32 {
    let now_ns = now_nanos();
    let query = SurrealStorage::build_claim_transaction(pools, asset_pools, scope);
    let mut q = storage.db.query(&query);
    for (i, (pool_key, _)) in pools.iter().enumerate() {
        q = q.bind((format!("p{i}"), pool_key.clone()));
    }
    if let Some(parts) = scope.and_then(|s| s.partitions.as_ref()) {
        for &i in asset_pools {
            q = q.bind((format!("parts{i}"), parts.clone()));
        }
    }
    let mut response = q
        .bind(("cl", crate::storage::DEFAULT_CODE_LOCATION_ID.to_string()))
        .bind(("run_id", run.to_string()))
        .bind(("step_key", step.to_string()))
        .bind(("now", now_ns))
        .bind(("lease_exp", now_ns + 300_000_000_000))
        .await
        .unwrap()
        .check()
        .unwrap();
    let idx = SurrealStorage::claim_check_statement_index(pools.len(), asset_pools.len());
    let count: Option<u32> = response.take((idx, "total")).unwrap();
    count.unwrap_or(0)
}

/// A step run again under its run and step key after its killed attempt's
/// lease ran out, while another run holds the asset in a way that blocks
/// it, must wait. That run can claim between the re-claim's pre-check and
/// its transaction: the step's leftover row then still exists, but the
/// transaction did not write it, so the claim must not report Claimed.
#[tokio::test]
async fn test_asset_pool_reclaim_by_same_step_waits_for_another_runs_claim() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
        .await
        .expect("failed to create rocksdb storage");
    let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
    let scope = |partitions: Option<&[&str]>, exclusive| AssetScope {
        partitions: partitions.map(|p| p.iter().map(|s| s.to_string()).collect()),
        exclusive,
    };
    // (the re-run step's scope, the other run's scope)
    let cases = [
        // A materialize, blocked by a delete of the whole asset.
        (scope(None, false), scope(None, true)),
        // Both on one partition.
        (scope(Some(&["p1"]), false), scope(Some(&["p1"]), true)),
        // The re-run step is the action; the other run materializes.
        (scope(Some(&["p1"]), true), scope(Some(&["p1"]), false)),
    ];
    for (i, (mine, theirs)) in cases.iter().enumerate() {
        let pool = format!("__asset__:orders_{i}");
        let pools = [(pool.clone(), 1)];
        let run = format!("run_{i}");
        storage.set_pool_limit(cl, &pool, -1, 300).await.unwrap();
        let claim = async |run: &str, lease: u32, scope: &AssetScope| {
            storage
                .claim_concurrency_slots(cl, &pools, run, "orders", 0, lease, Some(scope))
                .await
                .unwrap_or_else(|e| panic!("case {i}: claim by {run} failed: {e:#}"))
        };

        // The killed attempt never released its slot, and its lease ran out.
        assert_eq!(
            claim(&run, 0, mine).await,
            ConcurrencyClaimStatus::Claimed,
            "case {i}"
        );
        let killed = held_slots(&storage, &run, "orders").await;
        assert_eq!(
            claim("other_run", 300, theirs).await,
            ConcurrencyClaimStatus::Claimed,
            "case {i}"
        );

        let status = claim(&run, 300, mine).await;
        assert!(
            matches!(status, ConcurrencyClaimStatus::Pending { .. }),
            "case {i}: {status:?}"
        );
        assert_eq!(
            claim_transaction_count(&storage, &pools, &[0], &run, "orders", Some(mine)).await,
            0,
            "case {i}: the transaction claimed nothing; the leftover row is not its claim"
        );
        assert_eq!(
            held_slots(&storage, &run, "orders").await,
            killed,
            "case {i}"
        );
        let holders: Vec<(String, String)> = storage
            .get_pool_slot_holders(cl, &pool)
            .await
            .unwrap()
            .into_iter()
            .map(|h| (h.run_id, h.step_key))
            .collect();
        assert_eq!(
            holders,
            [("other_run".to_string(), "orders".to_string())],
            "case {i}"
        );

        storage
            .free_concurrency_slots("other_run", "orders")
            .await
            .unwrap();
        assert_eq!(
            claim(&run, 300, mine).await,
            ConcurrencyClaimStatus::Claimed,
            "case {i}"
        );
        let held = held_slots(&storage, &run, "orders").await;
        assert_eq!(held.len(), 1, "case {i}: {held:?}");
        assert!(
            held[0].claimed_at > killed[0].claimed_at
                && held[0].lease_expires_at > killed[0].lease_expires_at,
            "case {i}: the re-claim must take the row over: {held:?} vs {killed:?}"
        );
    }
}

/// A re-run step whose own leftover row is all that fills a counted pool
/// takes that row over at once: the row does not count toward the limit
/// its re-claim is checked against, so the step does not wait out its
/// killed attempt's lease.
#[tokio::test]
async fn test_user_pool_reclaim_by_same_step_does_not_wait_for_its_old_lease() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
        .await
        .expect("failed to create rocksdb storage");
    let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
    let scope = AssetScope {
        partitions: Some(vec!["p1".to_string()]),
        exclusive: false,
    };
    // (the counted pool's limit, the pools the step claims)
    let cases = [
        (1, vec![("db_0".to_string(), 1)]),
        // Two slots of a pool of two.
        (2, vec![("db_1".to_string(), 2)]),
        // Beside its asset's pool.
        (
            1,
            vec![("db_2".to_string(), 1), ("__asset__:orders".to_string(), 1)],
        ),
    ];
    for (i, (limit, pools)) in cases.iter().enumerate() {
        let run = format!("run_{i}");
        let (db, slots) = &pools[0];
        storage.set_pool_limit(cl, db, *limit, 300).await.unwrap();
        let scope = if pools.len() > 1 {
            storage
                .set_pool_limit(cl, "__asset__:orders", -1, 300)
                .await
                .unwrap();
            Some(&scope)
        } else {
            None
        };
        let claim = async |run: &str| {
            storage
                .claim_concurrency_slots(cl, pools, run, "load", 0, 300, scope)
                .await
                .unwrap_or_else(|e| panic!("case {i}: claim by {run} failed: {e:#}"))
        };

        assert_eq!(
            claim(&run).await,
            ConcurrencyClaimStatus::Claimed,
            "case {i}"
        );
        let killed = held_slots(&storage, &run, "load").await;
        // The killed attempt never released its slot; its lease is live.
        assert_eq!(
            claim(&run).await,
            ConcurrencyClaimStatus::Claimed,
            "case {i}: the re-claim must not wait for its own old lease"
        );

        let held = held_slots(&storage, &run, "load").await;
        assert_eq!(held.len(), pools.len(), "case {i}: {held:?}");
        for (row, old) in held.iter().zip(&killed) {
            assert_eq!(
                (&row.pool_key, row.slots_consumed),
                (&old.pool_key, old.slots_consumed),
                "case {i}"
            );
            assert!(
                row.claimed_at > old.claimed_at && row.lease_expires_at > old.lease_expires_at,
                "case {i}: the re-claim must take the row over: {row:?} vs {old:?}"
            );
        }
        assert_eq!(
            storage.get_pool_info(cl, db).await.unwrap().claimed_count,
            *slots,
            "case {i}"
        );
        match claim("other_run").await {
            ConcurrencyClaimStatus::Pending {
                reason:
                    BlockReason::PoolFull {
                        pool_key,
                        claimed,
                        limit: pool_limit,
                    },
                ..
            } => assert_eq!(
                (pool_key.as_str(), claimed, pool_limit),
                (db.as_str(), *slots, *limit),
                "case {i}"
            ),
            other => panic!("case {i}: the taken-over row must fill the pool: {other:?}"),
        }
    }
}

/// A re-run step must still wait for a counted pool's slot that another
/// step holds, whether its own leftover row's lease ran out or not, also
/// when that step claims between the re-claim's pre-check and its
/// transaction. Only the step's own row, by run and step key, is left out
/// of the count.
#[tokio::test]
async fn test_user_pool_reclaim_by_same_step_waits_for_another_steps_slot() {
    let temp_dir = test_temp_dir::test_temp_dir!();
    let storage = SurrealStorage::new_embedded(temp_dir.as_path_untracked().to_str().unwrap())
        .await
        .expect("failed to create rocksdb storage");
    let cl = crate::storage::DEFAULT_CODE_LOCATION_ID;
    // (the killed attempt's lease, the re-run step, the other holder)
    let cases = [
        // The lease ran out, and another run's step of the same name took
        // the slot.
        (0, ("run_0", "load"), ("other_run", "load")),
        // The lease is live, and a sibling step of the same run holds the
        // only slot left after the limit was lowered.
        (300, ("run_1", "load"), ("run_1", "sibling")),
    ];
    for (i, (lease, (run, step), (holder_run, holder_step))) in cases.into_iter().enumerate() {
        let pool = format!("db_{i}");
        let pools = [(pool.clone(), 1)];
        let claim = async |run: &str, step: &str, lease: u32| {
            storage
                .claim_concurrency_slots(cl, &pools, run, step, 0, lease, None)
                .await
                .unwrap_or_else(|e| panic!("case {i}: claim by {run}/{step} failed: {e:#}"))
        };
        storage.set_pool_limit(cl, &pool, 2, 300).await.unwrap();
        assert_eq!(
            claim(run, step, lease).await,
            ConcurrencyClaimStatus::Claimed,
            "case {i}"
        );
        let killed = held_slots(&storage, run, step).await;
        assert_eq!(
            claim(holder_run, holder_step, 300).await,
            ConcurrencyClaimStatus::Claimed,
            "case {i}"
        );
        storage.set_pool_limit(cl, &pool, 1, 300).await.unwrap();

        match claim(run, step, 300).await {
            ConcurrencyClaimStatus::Pending {
                reason:
                    BlockReason::PoolFull {
                        pool_key,
                        claimed,
                        limit,
                    },
                ..
            } => assert_eq!(
                (pool_key.as_str(), claimed, limit),
                (pool.as_str(), 1, 1),
                "case {i}"
            ),
            other => panic!("case {i}: expected Pending on the full pool, got {other:?}"),
        }
        assert_eq!(
            claim_transaction_count(&storage, &pools, &[], run, step, None).await,
            0,
            "case {i}: the transaction claimed nothing; the leftover row is not its claim"
        );
        assert_eq!(held_slots(&storage, run, step).await, killed, "case {i}");

        storage
            .free_concurrency_slots(holder_run, holder_step)
            .await
            .unwrap();
        assert_eq!(
            claim(run, step, 300).await,
            ConcurrencyClaimStatus::Claimed,
            "case {i}"
        );
        let held = held_slots(&storage, run, step).await;
        assert_eq!(held.len(), 1, "case {i}: {held:?}");
        assert!(
            held[0].claimed_at > killed[0].claimed_at
                && held[0].lease_expires_at > killed[0].lease_expires_at,
            "case {i}: the re-claim must take the row over: {held:?} vs {killed:?}"
        );
    }
}

#[tokio::test]
async fn test_claim_mixing_unlimited_and_limited_pools() {
    // Unlimited pools are dropped from the transaction, so the post-COMMIT
    // check has to be indexed by the pools actually in it. An asset that
    // declares a pool with no explicit limit (auto-registered as -1) and an
    // exclusive action (its implicit pool) hits exactly this shape.
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", -1, 300)
        .await
        .unwrap();
    storage
        .set_pool_limit(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "__asset__:orders",
            1_000_000,
            300,
        )
        .await
        .unwrap();

    let status = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[
                ("db".to_string(), 1),
                ("__asset__:orders".to_string(), 1_000_000),
            ],
            "run1",
            "step_a",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        status,
        ConcurrencyClaimStatus::Claimed,
        "an unlimited pool alongside a limited one must not be read as contention"
    );

    // And the limited pool really was claimed, so exclusion still holds.
    let blocked = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("__asset__:orders".to_string(), 1)],
            "run2",
            "step_b",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(blocked, ConcurrencyClaimStatus::Pending { .. }));
}

#[tokio::test]
async fn test_claim_weighted_slots() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "gpu", 4, 300)
        .await
        .unwrap();

    // Claim 3 of 4 slots
    let s1 = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("gpu".to_string(), 3)],
            "run1",
            "step_a",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert_eq!(s1, ConcurrencyClaimStatus::Claimed);

    // Claim 2 more → exceeds (3 + 2 = 5 > 4)
    let s2 = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("gpu".to_string(), 2)],
            "run2",
            "step_b",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(s2, ConcurrencyClaimStatus::Pending { .. }));

    // Claim 1 more → fits (3 + 1 = 4 <= 4)
    let s3 = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("gpu".to_string(), 1)],
            "run3",
            "step_c",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert_eq!(s3, ConcurrencyClaimStatus::Claimed);

    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "gpu")
        .await
        .unwrap();
    assert_eq!(info.claimed_count, 4);
    assert_eq!(info.pending_count, 1);
}

#[tokio::test]
async fn test_claim_multi_pool_all_or_none() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 2, 300)
        .await
        .unwrap();
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "api", 1, 300)
        .await
        .unwrap();

    // Claim 1 slot in each — should succeed
    let s1 = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1), ("api".to_string(), 1)],
            "run1",
            "step_a",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert_eq!(s1, ConcurrencyClaimStatus::Claimed);

    // api is now full (1/1). Claim db+api again — api blocks, so NEITHER should be claimed.
    let s2 = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1), ("api".to_string(), 1)],
            "run2",
            "step_b",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(s2, ConcurrencyClaimStatus::Pending { .. }));

    // db should still have only 1 claimed (all-or-none: step_b got nothing)
    let db_info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(db_info.claimed_count, 1);
    let api_info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "api")
        .await
        .unwrap();
    assert_eq!(api_info.claimed_count, 1);
}

#[tokio::test]
async fn test_claim_multi_pool_block_reason_pools_full() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 1, 300)
        .await
        .unwrap();
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "api", 1, 300)
        .await
        .unwrap();

    // Fill both pools
    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
            "run1",
            "s1",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("api".to_string(), 1)],
            "run2",
            "s2",
            0,
            300,
            None,
        )
        .await
        .unwrap();

    // Multi-pool claim against both full pools
    let s = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1), ("api".to_string(), 1)],
            "run3",
            "s3",
            0,
            300,
            None,
        )
        .await
        .unwrap();

    match s {
        ConcurrencyClaimStatus::Pending { reason, .. } => match reason {
            BlockReason::PoolsFull { pools } => {
                assert_eq!(pools.len(), 2);
                assert!(pools.iter().any(|p| p.pool_key == "db"));
                assert!(pools.iter().any(|p| p.pool_key == "api"));
            }
            other => panic!("expected PoolsFull, got {:?}", other),
        },
        ConcurrencyClaimStatus::Claimed => panic!("expected Pending"),
    }
}

#[tokio::test]
async fn test_free_concurrency_slots_for_run() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 10, 300)
        .await
        .unwrap();

    // Claim multiple steps for the same run
    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
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
            &[("db".to_string(), 2)],
            "run1",
            "step_b",
            0,
            300,
            None,
        )
        .await
        .unwrap();

    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(info.claimed_count, 3);

    // Free all for the run
    storage
        .free_concurrency_slots_for_run("run1")
        .await
        .unwrap();

    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(info.claimed_count, 0);
}

#[tokio::test]
async fn test_free_concurrency_slots_for_run_clears_pending() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 1, 300)
        .await
        .unwrap();

    // Fill pool then enqueue a step
    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
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
            &[("db".to_string(), 1)],
            "run1",
            "step_b",
            0,
            300,
            None,
        )
        .await
        .unwrap();

    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(info.pending_count, 1);

    // Free all for run1
    storage
        .free_concurrency_slots_for_run("run1")
        .await
        .unwrap();

    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(info.claimed_count, 0);
    assert_eq!(info.pending_count, 0);
}

#[tokio::test]
async fn test_claim_removes_pending_on_success() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 1, 300)
        .await
        .unwrap();

    // Fill pool, making step_b pending
    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
            "run1",
            "step_a",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    let s = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
            "run2",
            "step_b",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(s, ConcurrencyClaimStatus::Pending { .. }));

    // Free step_a
    storage
        .free_concurrency_slots("run1", "step_a")
        .await
        .unwrap();

    // Now step_b retries and should succeed, removing its pending entry
    let s = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
            "run2",
            "step_b",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert_eq!(s, ConcurrencyClaimStatus::Claimed);

    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(info.claimed_count, 1);
    assert_eq!(info.pending_count, 0);
}

#[tokio::test]
async fn test_claim_unconfigured_pool_errors() {
    let storage = make_storage().await;
    let result = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("nonexistent".to_string(), 1)],
            "run1",
            "s1",
            0,
            300,
            None,
        )
        .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not configured"));
}

#[tokio::test]
async fn test_claim_empty_pools_errors() {
    let storage = make_storage().await;
    let result = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[],
            "run1",
            "s1",
            0,
            300,
            None,
        )
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_concurrent_claims_limit_one() {
    let storage = std::sync::Arc::new(make_storage().await);
    storage
        .set_pool_limit(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            "exclusive",
            1,
            300,
        )
        .await
        .unwrap();

    let mut handles = Vec::new();
    for i in 0..50 {
        let storage = storage.clone();
        handles.push(tokio::spawn(async move {
            storage
                .claim_concurrency_slots(
                    crate::storage::DEFAULT_CODE_LOCATION_ID,
                    &[("exclusive".to_string(), 1)],
                    &format!("run_{i}"),
                    &format!("step_{i}"),
                    0,
                    300,
                    None,
                )
                .await
                .unwrap()
        }));
    }

    let mut claimed = 0;
    let mut pending = 0;
    for h in handles {
        match h.await.unwrap() {
            ConcurrencyClaimStatus::Claimed => claimed += 1,
            ConcurrencyClaimStatus::Pending { .. } => pending += 1,
        }
    }

    assert_eq!(claimed, 1, "exactly 1 should be claimed");
    assert_eq!(pending, 49, "exactly 49 should be pending");

    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "exclusive")
        .await
        .unwrap();
    assert_eq!(info.claimed_count, 1);
    assert_eq!(info.pending_count, 49);
}

#[tokio::test]
async fn test_free_step_only_affects_that_step() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 10, 300)
        .await
        .unwrap();

    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 2)],
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
            &[("db".to_string(), 3)],
            "run1",
            "step_b",
            0,
            300,
            None,
        )
        .await
        .unwrap();

    // Free only step_a
    storage
        .free_concurrency_slots("run1", "step_a")
        .await
        .unwrap();

    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(info.claimed_count, 3); // only step_b's 3 remain
}

#[tokio::test]
async fn test_claim_multi_pool_weighted() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 5, 300)
        .await
        .unwrap();
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "api", 3, 300)
        .await
        .unwrap();

    // Claim 2 db slots + 2 api slots
    let s1 = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 2), ("api".to_string(), 2)],
            "run1",
            "step_a",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert_eq!(s1, ConcurrencyClaimStatus::Claimed);

    // Claim 2 db + 2 api again: api would be 4 > 3
    let s2 = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 2), ("api".to_string(), 2)],
            "run2",
            "step_b",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(s2, ConcurrencyClaimStatus::Pending { .. }));

    // Verify all-or-none: db still at 2, not 4
    let db_info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(db_info.claimed_count, 2);
}

// ── Lease renewal and expiry ──

#[tokio::test]
async fn test_expired_slots_not_counted() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 2, 300)
        .await
        .unwrap();

    // Claim with 1-second lease
    let s = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
            "run1",
            "step_a",
            0,
            1,
            None,
        )
        .await
        .unwrap();
    assert_eq!(s, ConcurrencyClaimStatus::Claimed);

    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(info.claimed_count, 1);

    // Wait for the lease to expire
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // The expired slot should no longer be counted
    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(info.claimed_count, 0, "expired slot should not be counted");
}

#[tokio::test]
async fn test_expired_slot_frees_capacity() {
    // With limit=1 and an expired lease, a new claim should succeed.
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 1, 300)
        .await
        .unwrap();

    // Fill the pool with a 1-second lease
    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
            "run1",
            "step_a",
            0,
            1,
            None,
        )
        .await
        .unwrap();

    // Immediately, a second claim should be Pending
    let s = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
            "run2",
            "step_b",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(s, ConcurrencyClaimStatus::Pending { .. }));

    // Wait for the first lease to expire
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Now step_b should be able to claim
    let s = storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
            "run2",
            "step_b",
            0,
            300,
            None,
        )
        .await
        .unwrap();
    assert_eq!(s, ConcurrencyClaimStatus::Claimed);
}

#[tokio::test]
async fn test_renew_slot_lease() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 2, 300)
        .await
        .unwrap();

    // Claim with 1-second lease
    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
            "run1",
            "step_a",
            0,
            1,
            None,
        )
        .await
        .unwrap();

    // Renew with a long lease before it expires
    let renewed = storage
        .renew_slot_lease("run1", "step_a", 300)
        .await
        .unwrap();
    assert_eq!(renewed, 1, "should renew 1 slot row");

    // Wait past original 1-second lease
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Slot should still be active because we renewed
    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(
        info.claimed_count, 1,
        "renewed slot should still be counted"
    );
}

#[tokio::test]
async fn test_renew_multi_pool_lease() {
    // Claim across two pools, renew, verify both renewed.
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 5, 300)
        .await
        .unwrap();
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "api", 5, 300)
        .await
        .unwrap();

    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1), ("api".to_string(), 1)],
            "run1",
            "step_a",
            0,
            1,
            None,
        )
        .await
        .unwrap();

    // Renew both
    let renewed = storage
        .renew_slot_lease("run1", "step_a", 300)
        .await
        .unwrap();
    assert_eq!(renewed, 2, "should renew 2 slot rows (one per pool)");

    // Wait past original lease
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let db_info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    let api_info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "api")
        .await
        .unwrap();
    assert_eq!(db_info.claimed_count, 1);
    assert_eq!(api_info.claimed_count, 1);
}

#[tokio::test]
async fn test_renew_nonexistent_step_returns_zero() {
    let storage = make_storage().await;
    let renewed = storage
        .renew_slot_lease("no_run", "no_step", 300)
        .await
        .unwrap();
    assert_eq!(renewed, 0);
}

#[tokio::test]
async fn test_free_expired_leases() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 10, 300)
        .await
        .unwrap();

    // Claim 3 slots: 2 with 1-second lease, 1 with long lease
    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
            "run1",
            "step_a",
            0,
            1,
            None,
        )
        .await
        .unwrap();
    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
            "run2",
            "step_b",
            0,
            1,
            None,
        )
        .await
        .unwrap();
    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
            "run3",
            "step_c",
            0,
            300,
            None,
        )
        .await
        .unwrap();

    // Wait for the short leases to expire
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // GC expired leases
    let freed = storage.free_expired_leases().await.unwrap();
    assert_eq!(freed, 2, "should free 2 expired slot rows");

    // Only step_c's slot remains
    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(info.claimed_count, 1);
}

#[tokio::test]
async fn test_free_expired_leases_none_expired() {
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 10, 300)
        .await
        .unwrap();

    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
            "run1",
            "step_a",
            0,
            300,
            None,
        )
        .await
        .unwrap();

    let freed = storage.free_expired_leases().await.unwrap();
    assert_eq!(freed, 0, "no expired leases to free");

    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(info.claimed_count, 1);
}

#[tokio::test]
async fn test_free_expired_leases_empty() {
    let storage = make_storage().await;
    let freed = storage.free_expired_leases().await.unwrap();
    assert_eq!(freed, 0);
}

#[tokio::test]
async fn test_renewal_prevents_expiry_gc() {
    // Claim with short lease, renew it, run GC — slot should survive.
    let storage = make_storage().await;
    storage
        .set_pool_limit(crate::storage::DEFAULT_CODE_LOCATION_ID, "db", 5, 300)
        .await
        .unwrap();

    storage
        .claim_concurrency_slots(
            crate::storage::DEFAULT_CODE_LOCATION_ID,
            &[("db".to_string(), 1)],
            "run1",
            "step_a",
            0,
            1,
            None,
        )
        .await
        .unwrap();

    // Renew with long lease
    storage
        .renew_slot_lease("run1", "step_a", 300)
        .await
        .unwrap();

    // Wait past original lease
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // GC should find nothing expired
    let freed = storage.free_expired_leases().await.unwrap();
    assert_eq!(freed, 0, "renewed slot should not be freed by GC");

    let info = storage
        .get_pool_info(crate::storage::DEFAULT_CODE_LOCATION_ID, "db")
        .await
        .unwrap();
    assert_eq!(info.claimed_count, 1);
}
