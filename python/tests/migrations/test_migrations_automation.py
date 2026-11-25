"""Automation leaves alone an asset that migrates and the assets that read
it: their conditions fire, but no run starts until the migration has
ended."""

import contextlib
import threading

import pytest

import rivers as rs
from _polling import wait_for, wait_for_asset_materialized
from rivers._core import AutomationDaemon


def _repo(storage, io, version, convert=None, queue=False):
    """``orders`` at ``version``, with ``convert`` to version 2, and
    ``summary``, which reads ``orders`` and is materialized when it is
    missing. Both store their data in ``io``. With ``queue``, in run-queue
    mode."""
    migrations = [rs.AssetMigration(to=2)(convert)] if convert else []

    @rs.Asset(io_handler=io, schema_version=version, migrations=migrations)
    def orders() -> int:
        return 1

    @rs.Asset(io_handler=io, automation_condition=rs.AutomationCondition.on_missing())
    def summary(orders: int) -> int:
        return orders

    extra = {"run_queue": rs.RunQueueConfig(dequeue_interval="50ms")} if queue else {}
    repo = rs.CodeRepository(
        assets=[orders, summary], default_executor=rs.Executor.in_process(), **extra
    )
    repo.resolve(storage=storage)
    return repo


@contextlib.contextmanager
def _daemon(repo, storage):
    daemon = AutomationDaemon(repo=repo, storage=storage, condition_eval_interval="1s")
    daemon.start()
    try:
        yield
    finally:
        daemon.stop()


def _skipped(storage, name):
    return [tick for tick in storage.get_ticks(name) if tick.status == "Skipped"]


@pytest.mark.parametrize("queue", [False, True], ids=["direct", "queued"])
def test_automation_waits_for_the_migration_of_an_asset_that_it_reads(storage, queue):
    converting = threading.Event()
    release = threading.Event()

    def add_currency(ctx: rs.MigrationContext) -> None:
        converting.set()
        release.wait(30)

    io = rs.InMemoryIOHandler()
    assert _repo(storage, io, 1).materialize(["orders"]).success
    repo = _repo(storage, io, 2, add_currency, queue)

    with _daemon(repo, storage):
        try:
            assert converting.wait(30)
            wait_for(
                lambda: _skipped(storage, "summary"), 15, "a skipped tick of summary"
            )
            assert {tick.skip_reason for tick in _skipped(storage, "summary")} == {
                "orders is migrating to version 2"
            }
            assert not [
                run for run in storage.get_runs() if "summary" in run.node_names
            ]
        finally:
            release.set()
        # The condition stays armed, and fires again after the migration.
        record = wait_for_asset_materialized(storage, "summary", timeout=30)

    assert record is not None
    assert record.last_data_version is not None
    assert storage.get_asset_version("orders").state == "ready"
