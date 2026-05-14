"""Concurrency Pools — Lease Renewal and Expiry tests.

Tests lease expiry, renewal, free_expired_leases GC, and that renewal
prevents GC from reclaiming active slots.
"""

import time

# ── Lease expiry ──


def test_expired_slots_not_counted(storage):
    """Slots with expired leases are not counted as claimed."""
    storage.set_pool_limit("db", 2)

    # Claim with 1-second lease
    status = storage._claim_concurrency_slots(
        [("db", 1)], "run1", "step_a", lease_duration="1s"
    )
    assert status.is_claimed

    info = storage.get_pool_info("db")
    assert info.claimed_count == 1

    # Wait for lease to expire
    time.sleep(2)

    info = storage.get_pool_info("db")
    assert info.claimed_count == 0, "expired slot should not be counted"


def test_expired_slot_frees_capacity(storage):
    """A new claim succeeds after a previous slot's lease expires."""
    storage.set_pool_limit("db", 1)

    # Fill pool with 1-second lease
    storage._claim_concurrency_slots([("db", 1)], "run1", "step_a", lease_duration="1s")

    # Immediately, a second claim is pending
    status = storage._claim_concurrency_slots([("db", 1)], "run2", "step_b")
    assert not status.is_claimed

    # Wait for expiry
    time.sleep(2)

    # Now step_b can claim
    status = storage._claim_concurrency_slots([("db", 1)], "run2", "step_b")
    assert status.is_claimed


# ── Lease renewal ──


def test_renew_slot_lease(storage):
    """Renewing a slot's lease extends it past the original expiry."""
    storage.set_pool_limit("db", 2)

    # Claim with 1-second lease
    storage._claim_concurrency_slots([("db", 1)], "run1", "step_a", lease_duration="1s")

    # Renew with long lease
    renewed = storage._renew_slot_lease("run1", "step_a", lease_duration="5m")
    assert renewed == 1

    # Wait past original lease
    time.sleep(2)

    # Slot should still be active
    info = storage.get_pool_info("db")
    assert info.claimed_count == 1, "renewed slot should still be counted"


def test_renew_multi_pool_lease(storage):
    """Renewal updates slots across multiple pools for a step."""
    storage.set_pool_limit("db", 5)
    storage.set_pool_limit("api", 5)

    storage._claim_concurrency_slots(
        [("db", 1), ("api", 1)], "run1", "step_a", lease_duration="1s"
    )

    renewed = storage._renew_slot_lease("run1", "step_a", lease_duration="5m")
    assert renewed == 2, "should renew 2 slot rows (one per pool)"

    time.sleep(2)

    assert storage.get_pool_info("db").claimed_count == 1
    assert storage.get_pool_info("api").claimed_count == 1


def test_renew_nonexistent_step(storage):
    """Renewing a non-existent step returns 0."""
    renewed = storage._renew_slot_lease("no_run", "no_step")
    assert renewed == 0


# ── free_expired_leases GC ──


def test_free_expired_leases(storage):
    """GC removes expired slot rows but keeps active ones."""
    storage.set_pool_limit("db", 10)

    # 2 short-lived, 1 long-lived
    storage._claim_concurrency_slots([("db", 1)], "run1", "step_a", lease_duration="1s")
    storage._claim_concurrency_slots([("db", 1)], "run2", "step_b", lease_duration="1s")
    storage._claim_concurrency_slots([("db", 1)], "run3", "step_c", lease_duration="5m")

    time.sleep(2)

    freed = storage._free_expired_leases()
    assert freed == 2, "should free 2 expired slot rows"

    info = storage.get_pool_info("db")
    assert info.claimed_count == 1


def test_free_expired_leases_none_expired(storage):
    """GC returns 0 when no leases are expired."""
    storage.set_pool_limit("db", 10)
    storage._claim_concurrency_slots([("db", 1)], "run1", "step_a")

    freed = storage._free_expired_leases()
    assert freed == 0

    info = storage.get_pool_info("db")
    assert info.claimed_count == 1


def test_free_expired_leases_empty(storage):
    """GC returns 0 on an empty table."""
    freed = storage._free_expired_leases()
    assert freed == 0
