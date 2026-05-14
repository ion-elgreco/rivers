"""Concurrency Pools — Claim/Release Protocol tests.

Tests slot claiming, releasing, multi-pool atomicity, weighted slots,
block reasons, and concurrent claim stress tests.
"""

import threading

import pytest


# ── Single-pool claim/release ──


def test_claim_single_pool_success(storage):
    """Claiming a slot in an uncongested pool succeeds."""
    storage.set_pool_limit("db", 3)
    status = storage._claim_concurrency_slots([("db", 1)], "run1", "step_a")

    assert status.is_claimed
    assert status.status == "claimed"
    assert status.reason is None

    info = storage.get_pool_info("db")
    assert info.claimed_count == 1
    assert info.pending_count == 0


def test_claim_single_pool_full(storage):
    """When a pool is full, the claim returns Pending with PoolFull reason."""
    storage.set_pool_limit("db", 1)
    storage._claim_concurrency_slots([("db", 1)], "run1", "step_a")

    status = storage._claim_concurrency_slots([("db", 1)], "run2", "step_b")

    assert not status.is_claimed
    assert status.status == "pending"
    assert status.reason is not None
    assert status.reason.kind == "pool_full"
    assert status.reason.pool_key == "db"
    assert status.reason.claimed == 1
    assert status.reason.limit == 1

    info = storage.get_pool_info("db")
    assert info.claimed_count == 1
    assert info.pending_count == 1


def test_claim_and_release_then_reclaim(storage):
    """After releasing a slot, a new claim succeeds."""
    storage.set_pool_limit("db", 1)

    storage._claim_concurrency_slots([("db", 1)], "run1", "step_a")
    storage._free_concurrency_slots("run1", "step_a")

    info = storage.get_pool_info("db")
    assert info.claimed_count == 0

    status = storage._claim_concurrency_slots([("db", 1)], "run2", "step_b")
    assert status.is_claimed


def test_claim_removes_pending_on_success(storage):
    """When a previously-pending step retries and succeeds, its pending entry is removed."""
    storage.set_pool_limit("db", 1)

    storage._claim_concurrency_slots([("db", 1)], "run1", "step_a")
    status = storage._claim_concurrency_slots([("db", 1)], "run2", "step_b")
    assert not status.is_claimed

    # Free and retry
    storage._free_concurrency_slots("run1", "step_a")
    status = storage._claim_concurrency_slots([("db", 1)], "run2", "step_b")
    assert status.is_claimed

    info = storage.get_pool_info("db")
    assert info.claimed_count == 1
    assert info.pending_count == 0


# ── Weighted slots ──


def test_claim_weighted_slots(storage):
    """Weighted slots (pool_slots > 1) consume multiple slots per claim."""
    storage.set_pool_limit("gpu", 4)

    # Claim 3 of 4
    status = storage._claim_concurrency_slots([("gpu", 3)], "run1", "step_a")
    assert status.is_claimed

    # 3 + 2 = 5 > 4 → blocked
    status = storage._claim_concurrency_slots([("gpu", 2)], "run2", "step_b")
    assert not status.is_claimed

    # 3 + 1 = 4 <= 4 → fits
    status = storage._claim_concurrency_slots([("gpu", 1)], "run3", "step_c")
    assert status.is_claimed

    info = storage.get_pool_info("gpu")
    assert info.claimed_count == 4
    assert info.pending_count == 1


# ── Multi-pool all-or-none ──


def test_multi_pool_all_or_none(storage):
    """Multi-pool claims are atomic: if any pool is full, neither gets claimed."""
    storage.set_pool_limit("db", 2)
    storage.set_pool_limit("api", 1)

    # Claim 1 each → succeeds
    status = storage._claim_concurrency_slots([("db", 1), ("api", 1)], "run1", "step_a")
    assert status.is_claimed

    # api is full (1/1). db has room, but all-or-none → pending
    status = storage._claim_concurrency_slots([("db", 1), ("api", 1)], "run2", "step_b")
    assert not status.is_claimed

    # Verify db still at 1 (step_b got nothing)
    db_info = storage.get_pool_info("db")
    assert db_info.claimed_count == 1
    api_info = storage.get_pool_info("api")
    assert api_info.claimed_count == 1


def test_multi_pool_block_reason_pools_full(storage):
    """When multiple pools are full, BlockReason is pools_full with details."""
    storage.set_pool_limit("db", 1)
    storage.set_pool_limit("api", 1)

    storage._claim_concurrency_slots([("db", 1)], "run1", "s1")
    storage._claim_concurrency_slots([("api", 1)], "run2", "s2")

    status = storage._claim_concurrency_slots([("db", 1), ("api", 1)], "run3", "s3")
    assert not status.is_claimed
    assert status.reason.kind == "pools_full"
    assert len(status.reason.pools) == 2
    pool_keys = {p.pool_key for p in status.reason.pools}
    assert pool_keys == {"db", "api"}


def test_multi_pool_weighted(storage):
    """Multi-pool with weighted slots checks each pool's capacity."""
    storage.set_pool_limit("db", 5)
    storage.set_pool_limit("api", 3)

    # Claim 2 db + 2 api
    status = storage._claim_concurrency_slots([("db", 2), ("api", 2)], "run1", "step_a")
    assert status.is_claimed

    # 2+2=4 db ok, 2+2=4 api exceeds 3 → blocked, all-or-none
    status = storage._claim_concurrency_slots([("db", 2), ("api", 2)], "run2", "step_b")
    assert not status.is_claimed

    # db still at 2 (not 4)
    db_info = storage.get_pool_info("db")
    assert db_info.claimed_count == 2


# ── Run-level cleanup ──


def test_free_concurrency_slots_for_run(storage):
    """Freeing all slots for a run clears all step entries."""
    storage.set_pool_limit("db", 10)

    storage._claim_concurrency_slots([("db", 1)], "run1", "step_a")
    storage._claim_concurrency_slots([("db", 2)], "run1", "step_b")

    info = storage.get_pool_info("db")
    assert info.claimed_count == 3

    storage._free_concurrency_slots_for_run("run1")

    info = storage.get_pool_info("db")
    assert info.claimed_count == 0


def test_free_for_run_clears_pending(storage):
    """Freeing a run also removes pending entries."""
    storage.set_pool_limit("db", 1)

    storage._claim_concurrency_slots([("db", 1)], "run1", "step_a")
    storage._claim_concurrency_slots([("db", 1)], "run1", "step_b")  # pending

    info = storage.get_pool_info("db")
    assert info.pending_count == 1

    storage._free_concurrency_slots_for_run("run1")

    info = storage.get_pool_info("db")
    assert info.claimed_count == 0
    assert info.pending_count == 0


def test_free_step_only_affects_that_step(storage):
    """Freeing one step doesn't affect other steps in the same run."""
    storage.set_pool_limit("db", 10)

    storage._claim_concurrency_slots([("db", 2)], "run1", "step_a")
    storage._claim_concurrency_slots([("db", 3)], "run1", "step_b")

    storage._free_concurrency_slots("run1", "step_a")

    info = storage.get_pool_info("db")
    assert info.claimed_count == 3  # only step_b's 3 remain


# ── Error cases ──


def test_claim_unconfigured_pool_errors(storage):
    """Claiming against an unconfigured pool raises."""
    with pytest.raises(Exception, match="not configured"):
        storage._claim_concurrency_slots([("ghost", 1)], "run1", "s1")


# ── Concurrency stress test ──


def test_concurrent_claims_limit_one(embedded_storage):
    """50 threads claim same pool (limit=1): exactly 1 Claimed, 49 Pending.

    Uses ``embedded_storage`` because kv-mem misses some write-write conflicts
    in the commit-queue check, which would let multiple threads observe a
    Claimed result here.
    """
    storage = embedded_storage
    storage.set_pool_limit("exclusive", 1)

    results = [None] * 50

    def claim(i):
        results[i] = storage._claim_concurrency_slots(
            [("exclusive", 1)], f"run_{i}", f"step_{i}"
        )

    threads = [threading.Thread(target=claim, args=(i,)) for i in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    claimed = sum(1 for r in results if r.is_claimed)
    pending = sum(1 for r in results if not r.is_claimed)

    assert claimed == 1, f"expected 1 claimed, got {claimed}"
    assert pending == 49, f"expected 49 pending, got {pending}"

    info = storage.get_pool_info("exclusive")
    assert info.claimed_count == 1
    assert info.pending_count == 49
