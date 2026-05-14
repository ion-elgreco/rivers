"""Storage calls from inside asset execution.

Each storage method on `rs.Storage` releases the GIL and `block_on`s a
storage future internally. The risk is that asset execution might run on a
tokio worker thread, in which case `block_on` would panic with "Cannot start
a runtime from within a runtime". These tests exercise that combination
across the executors we ship.
"""

from __future__ import annotations

import asyncio

import pytest

import rivers as rs
from rivers.testing import embedded_storage as _embedded_storage_factory


@pytest.fixture
def storage(tmp_path):
    return _embedded_storage_factory(str(tmp_path / "db"))


# ── In-process executor ──


def test_sync_asset_kv_roundtrip(storage):
    """A sync asset calls kv_set / kv_get during execution."""

    @rs.Asset
    def producer():
        storage.kv_set("from_asset", b"hello")
        return storage.kv_get("from_asset")

    repo = rs.CodeRepository(
        assets=[producer], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    repo.materialize(raise_on_error=True)
    assert storage.kv_get("from_asset") == b"hello"


def test_sync_asset_lists_runs(storage):
    """A sync asset calls storage.get_runs() during its body."""

    @rs.Asset
    def lister():
        runs = storage.get_runs(limit=50)
        return len(runs)

    repo = rs.CodeRepository(assets=[lister], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=True)
    assert result.success


def test_sync_asset_reads_events_for_self(storage):
    """An asset queries its own (in-progress) event stream."""

    @rs.Asset
    def self_reader():
        # No materialization event yet at this point — but the call itself
        # should not panic with a nested-runtime error.
        events = storage.get_events_for_asset("self_reader", limit=10)
        return len(events)

    repo = rs.CodeRepository(
        assets=[self_reader], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=True)
    assert result.success


def test_sync_asset_storage_calls_in_loop(storage):
    """Many storage calls in a single asset to surface intermittent races."""

    @rs.Asset
    def churn():
        for i in range(50):
            storage.kv_set(f"k{i}", str(i).encode())
        for i in range(50):
            assert storage.kv_get(f"k{i}") == str(i).encode()
        return 50

    repo = rs.CodeRepository(assets=[churn], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=True)
    assert result.success


# ── Async (asyncio) assets ──


def test_async_asset_kv_roundtrip(storage):
    """An async asset awaits asyncio.sleep and then hits storage."""

    @rs.Asset
    async def async_producer():
        await asyncio.sleep(0)
        storage.kv_set("async_key", b"world")
        return storage.kv_get("async_key")

    repo = rs.CodeRepository(
        assets=[async_producer], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=True)
    assert result.success
    assert storage.kv_get("async_key") == b"world"


def test_async_asset_storage_between_awaits(storage):
    """Async asset alternates between asyncio.sleep and storage calls."""

    @rs.Asset
    async def alternator():
        await asyncio.sleep(0)
        storage.kv_set("a", b"1")
        await asyncio.sleep(0)
        storage.kv_set("b", b"2")
        await asyncio.sleep(0)
        return [storage.kv_get("a"), storage.kv_get("b")]

    repo = rs.CodeRepository(
        assets=[alternator], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=True)
    assert result.success


# ── Parallel executor (mixed sync + async assets) ──


def test_parallel_async_asset_storage_calls(storage):
    """The parallel executor routes async assets through the async backend.

    Async assets in this path run via the AsyncBridge — driven on the tokio
    runtime — so storage calls inside them are exactly the case where a
    `block_on` from a tokio worker would panic.
    """

    @rs.Asset
    async def async_writer():
        await asyncio.sleep(0)
        storage.kv_set("parallel_async", b"ok")
        await asyncio.sleep(0)
        return 1

    @rs.Asset
    async def async_reader(async_writer: int):
        await asyncio.sleep(0)
        return storage.kv_get("parallel_async")

    repo = rs.CodeRepository(
        assets=[async_writer, async_reader],
        default_executor=rs.Executor.parallel(),
    )
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=True)
    assert result.success


# ── Recursive materialize from inside an asset ──


def test_asset_calls_repo_materialize_for_other_repo(tmp_path):
    """Recursive materialize: outer asset calls repo.materialize() on a
    second repo with its own storage. The inner materialize spins its own
    storage I/O and event-writer flush — exercising every nested block_on
    path while the outer call is still on the stack.
    """
    outer_storage = _embedded_storage_factory(str(tmp_path / "outer"))
    inner_storage = _embedded_storage_factory(str(tmp_path / "inner"))

    @rs.Asset
    def inner_leaf():
        return 42

    inner_repo = rs.CodeRepository(
        assets=[inner_leaf], default_executor=rs.Executor.in_process()
    )
    inner_repo.resolve(storage=inner_storage)

    @rs.Asset
    def outer_caller():
        result = inner_repo.materialize(raise_on_error=True)
        assert result.success
        return result.run_id

    outer_repo = rs.CodeRepository(
        assets=[outer_caller], default_executor=rs.Executor.in_process()
    )
    outer_repo.resolve(storage=outer_storage)
    result = outer_repo.materialize(raise_on_error=True)
    assert result.success
