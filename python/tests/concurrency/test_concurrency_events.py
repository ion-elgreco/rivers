"""Concurrency Observability — Event emission tests.

Tests that concurrency lifecycle events (RunQueued, RunDequeued, StepSlotClaimed,
StepSlotWaiting, StepSlotRenewed, StepSlotReleased) are emitted at the correct
points. Also tests that block_reason is persisted on RunRecord.
"""

import pytest
import rivers as rs

from _helpers import make_repo


# ═══════════════════════════════════════════════════════════════════════════
# Pool slot events: StepSlotClaimed, StepSlotReleased
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "executor_factory,is_async",
    [
        pytest.param(rs.Executor.in_process, False, id="in_process_sync"),
        pytest.param(rs.Executor.in_process, True, id="in_process_async"),
        pytest.param(rs.Executor.parallel, False, id="parallel_sync"),
    ],
)
def test_pool_step_emits_claimed_and_released(storage, executor_factory, is_async):
    """A pool step emits StepSlotClaimed (with pools metadata) and StepSlotReleased."""

    if is_async:

        @rs.Asset(pool="db", pool_slots=1, io_handler=rs.InMemoryIOHandler())
        async def pooled_asset():
            return 42
    else:

        @rs.Asset(pool="db", pool_slots=1, io_handler=rs.InMemoryIOHandler())
        def pooled_asset():
            return 42

    storage.set_pool_limit("db", 5)
    repo = make_repo([pooled_asset], storage, executor=executor_factory())
    result = repo.materialize()
    assert result.success

    events = storage.get_events_for_run(result.run_id)
    event_types = [e.event_type for e in events]
    assert "StepSlotClaimed" in event_types
    assert "StepSlotReleased" in event_types

    claimed = [e for e in events if e.event_type == "StepSlotClaimed"]
    assert len(claimed) == 1
    pools_meta = dict(claimed[0].metadata).get("pools")
    assert pools_meta == "db"


def test_no_pool_step_no_slot_events(storage):
    """A step without pool config should NOT emit any slot events."""

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def regular_asset():
        return 1

    repo = make_repo([regular_asset], storage)
    result = repo.materialize()
    assert result.success

    events = storage.get_events_for_run(result.run_id)
    slot_events = [e for e in events if "Slot" in e.event_type]
    assert len(slot_events) == 0


def test_multi_pool_step_events(storage):
    """A step claiming multiple pools emits correct metadata."""

    @rs.Asset(pool=["db", "api"], pool_slots=1, io_handler=rs.InMemoryIOHandler())
    def multi_pool_asset():
        return 100

    storage.set_pool_limit("db", 5)
    storage.set_pool_limit("api", 5)
    repo = make_repo([multi_pool_asset], storage)
    result = repo.materialize()
    assert result.success

    events = storage.get_events_for_run(result.run_id)
    claimed = [e for e in events if e.event_type == "StepSlotClaimed"]
    assert len(claimed) == 1
    pools_meta = dict(claimed[0].metadata).get("pools", "")
    assert "db" in pools_meta
    assert "api" in pools_meta


# ═══════════════════════════════════════════════════════════════════════════
# RunRecord.block_reason
# ═══════════════════════════════════════════════════════════════════════════


def test_run_record_block_reason_field(storage):
    """RunRecord exposes block_reason field (initially None after materialize)."""

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def asset_br():
        return 1

    repo = make_repo([asset_br], storage)
    result = repo.materialize()
    assert result.success

    run = storage.get_run(result.run_id)
    assert run.block_reason is None


# ═══════════════════════════════════════════════════════════════════════════
# Event ordering
# ═══════════════════════════════════════════════════════════════════════════


def test_slot_events_ordered_within_step_lifecycle(storage):
    """StepSlotClaimed comes before StepSlotReleased for same step."""

    @rs.Asset(pool="db", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    def ordered_asset():
        return 1

    storage.set_pool_limit("db", 5)
    repo = make_repo([ordered_asset], storage)
    result = repo.materialize()
    assert result.success

    events = storage.get_events_for_run(result.run_id)
    claimed_ts = next(
        (e.timestamp for e in events if e.event_type == "StepSlotClaimed"), None
    )
    released_ts = next(
        (e.timestamp for e in events if e.event_type == "StepSlotReleased"), None
    )

    assert claimed_ts is not None
    assert released_ts is not None
    assert claimed_ts < released_ts, "StepSlotClaimed must precede StepSlotReleased"


# ═══════════════════════════════════════════════════════════════════════════
# StepStart vs pool-claim ordering (review_in_process.md bug #7)
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "executor_factory,is_async",
    [
        pytest.param(rs.Executor.in_process, False, id="in_process_sync"),
        pytest.param(rs.Executor.in_process, True, id="in_process_async"),
        pytest.param(rs.Executor.parallel, False, id="parallel_sync"),
    ],
)
def test_step_start_emitted_after_pool_claim(storage, executor_factory, is_async):
    """Regression for review_in_process.md bug #7.

    `StepStart` must be emitted *after* the pool claim has succeeded — not
    before. Pre-fix, every executor emitted `StepStart` immediately when the
    step was scheduled, so a step queued behind a saturated pool appeared
    "Started" in the UI while it was actually waiting for a slot.

    Asserts that the `StepSlotClaimed` event's timestamp is `<=` the
    `StepStart` event's timestamp for the pooled step.
    """
    if is_async:

        @rs.Asset(pool="db", pool_slots=1, io_handler=rs.InMemoryIOHandler())
        async def ordered_pooled():
            return 1
    else:

        @rs.Asset(pool="db", pool_slots=1, io_handler=rs.InMemoryIOHandler())
        def ordered_pooled():
            return 1

    storage.set_pool_limit("db", 5)
    repo = make_repo([ordered_pooled], storage, executor=executor_factory())
    result = repo.materialize()
    assert result.success

    events = storage.get_events_for_run(result.run_id)
    claimed = next((e for e in events if e.event_type == "StepSlotClaimed"), None)
    start = next(
        (
            e
            for e in events
            if e.event_type == "StepStart" and e.asset_key == "ordered_pooled"
        ),
        None,
    )

    assert claimed is not None, (
        f"expected StepSlotClaimed for ordered_pooled, "
        f"got types: {[e.event_type for e in events]}"
    )
    assert start is not None, (
        f"expected StepStart for ordered_pooled, "
        f"got types: {[e.event_type for e in events]}"
    )
    assert claimed.timestamp <= start.timestamp, (
        f"StepSlotClaimed must precede or equal StepStart, got "
        f"claimed.timestamp={claimed.timestamp}, start.timestamp={start.timestamp}. "
        f"Pre-fix, StepStart fired before pool claim — UI would show 'Started' "
        f"while the step was actually queued."
    )


# Note on mapped instance coverage: the mapped fan-out backends
# (`run_mapped_sequential`, `run_mapped_step_parallel_pooled`,
# `AsyncBackend::run_mapped_to_completion`) had the same pre-fix bug
# (`emit_start(&instance_name, ...)` before `PoolGuard::acquire`), but they
# can't be exercised by an integration test today: mapped instances run on
# `Task` nodes (`.map(task, ...)`), and `Task` has no `pool` argument — so
# `node.pool()` always returns `vec![]` for mapped instances and the pool
# branch of `run_mapped_*` is unreachable from the public API. The fix is a
# mechanical reorder, identical in shape to the single-step fix exercised
# above. If `Task` ever grows pool support, add a parametrized version of
# `test_step_start_emitted_after_pool_claim` over a fan-out pipeline.
