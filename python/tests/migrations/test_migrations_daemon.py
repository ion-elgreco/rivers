"""The daemon migrates the stored data of an asset when new code declares a
higher version: once no started run touches the asset or a reader, it
converts or rebuilds the data, one version at a time."""

import contextlib
import threading
import time

import pytest

import rivers as rs
from _polling import wait_for
from rivers._core import AutomationDaemon
from rivers.exceptions import AssetMigrationPendingError

PD = rs.PartitionsDefinition.static_(["eu", "us", "ap"])


def _repo(storage, assets, queue=False):
    """A resolved repository of ``assets``; with ``queue``, in run-queue
    mode."""
    extra = {"run_queue": rs.RunQueueConfig(dequeue_interval="50ms")} if queue else {}
    repo = rs.CodeRepository(
        assets=assets, default_executor=rs.Executor.in_process(), **extra
    )
    repo.resolve(storage=storage)
    return repo


def _orders(**declared):
    @rs.Asset(io_handler=rs.InMemoryIOHandler(), **declared)
    def orders() -> int:
        return 1

    return orders


@contextlib.contextmanager
def _daemon(repo, storage):
    daemon = AutomationDaemon(repo=repo, storage=storage, condition_eval_interval="1s")
    daemon.start()
    try:
        yield
    finally:
        daemon.stop()


def _ended(storage, key="orders", timeout=30.0):
    """The version record of ``key`` once its migration has ended."""
    wait_for(
        lambda: storage.get_asset_version(key).state in ("ready", "failed"),
        timeout,
        f"the migration of {key} to end",
    )
    return storage.get_asset_version(key)


@pytest.mark.parametrize("queue", [False, True], ids=["direct", "queued"])
def test_the_daemon_converts_the_stored_data_once(storage, queue):
    calls = []

    def add_currency(ctx: rs.MigrationContext) -> None:
        calls.append(ctx.run_id)

    assert _repo(storage, [_orders()]).materialize().success
    migration = rs.AssetMigration(to=2)(add_currency)
    repo = _repo(storage, [_orders(schema_version=2, migrations=[migration])], queue)
    assert storage.get_asset_version("orders").state == "pending"

    with _daemon(repo, storage):
        row = _ended(storage)

    assert (row.state, row.applied_version, row.owner) == ("ready", 2, None)
    (run_id,) = calls
    run = storage.get_run(run_id)
    assert (run.status, run.action) == ("Success", "migrate")
    assert run.launched_by == rs.LaunchedBy.migration("orders", 2)
    assert storage._asset_version_history("orders") == [
        (2, "convert", run_id, None, None, None)
    ]


def test_steps_that_only_record_the_version_run_nothing(storage):
    assert _repo(storage, [_orders()]).materialize().success
    steps = [rs.AssetMigration(to=2), rs.AssetMigration(to=3)]
    repo = _repo(storage, [_orders(schema_version=3, migrations=steps)])

    with _daemon(repo, storage):
        row = _ended(storage)

    assert (row.state, row.applied_version) == ("ready", 3)
    history = storage._asset_version_history("orders")
    assert [entry[:2] for entry in history] == [
        (2, "version_only"),
        (3, "version_only"),
    ]
    assert len(storage.get_runs()) == 1


def _partitioned(version, seen, fail=()):
    """``orders`` on ``PD`` at ``version``, with a rebuild to version 2. It
    records each partition that it materializes in ``seen``, and fails for
    the keys in ``fail``."""
    migrations = [rs.AssetMigration(to=2, rebuild=True)] if version > 1 else []

    @rs.Asset(
        partitions_def=PD,
        io_handler=rs.InMemoryIOHandler(),
        schema_version=version,
        migrations=migrations,
    )
    def orders(context: rs.AssetExecutionContext) -> int:
        seen.append((version, context.partition_key))
        if context.partition_key in fail:
            raise RuntimeError(f"no orders for {context.partition_key}")
        return version

    return orders


@pytest.mark.parametrize("fail", [(), ("us",)], ids=["each-succeeds", "one-fails"])
def test_a_rebuild_materializes_each_stored_partition_again(storage, fail):
    seen = []
    old = _repo(storage, [_partitioned(1, seen)])
    for key in ("eu", "us"):
        assert old.materialize(partition_key=rs.PartitionKey.single(key)).success
    seen.clear()
    repo = _repo(storage, [_partitioned(2, seen, fail)])

    with _daemon(repo, storage):
        row = _ended(storage, timeout=60)

    assert (row.state, row.applied_version) == ("ready", 2)
    assert sorted(seen) == [(2, "eu"), (2, "us")]
    ((_, kind, run_id, backfill_id, filled, failed),) = storage._asset_version_history(
        "orders"
    )
    assert (kind, run_id, filled, failed) == ("rebuild", None, 2 - len(fail), len(fail))
    assert repo.get_backfill(backfill_id).launched_by == rs.LaunchedBy.migration(
        "orders", 2
    )
    # A partition that the rebuild did not write no longer holds data.
    materialized = sorted(
        str(key) for key in storage.get_materialized_partitions("orders")
    )
    assert materialized == sorted({"eu", "us"} - set(fail))


def test_a_rebuild_of_an_asset_without_partitions_runs_it_once(storage):
    versions = []

    def make(version):
        migrations = [rs.AssetMigration(to=2, rebuild=True)] if version > 1 else []

        @rs.Asset(
            io_handler=rs.InMemoryIOHandler(),
            schema_version=version,
            migrations=migrations,
        )
        def orders() -> int:
            versions.append(version)
            return version

        return orders

    assert _repo(storage, [make(1)]).materialize().success
    repo = _repo(storage, [make(2)])

    with _daemon(repo, storage):
        row = _ended(storage)

    assert (row.state, versions) == ("ready", [1, 2])
    ((_, kind, run_id, _, filled, failed),) = storage._asset_version_history("orders")
    assert (kind, filled, failed) == ("rebuild", 1, 0)
    assert storage.get_run(run_id).launched_by == rs.LaunchedBy.migration("orders", 2)


def test_the_migration_waits_for_a_started_run_of_a_reader(storage):
    calls = []
    reading = threading.Event()
    release = threading.Event()

    def assets(**declared):
        @rs.Asset(io_handler=rs.InMemoryIOHandler())
        def summary(orders: int) -> int:
            reading.set()
            release.wait(30)
            return orders

        return [_orders(**declared), summary]

    old = _repo(storage, assets())
    assert old.materialize(["orders"]).success
    reader = threading.Thread(target=old.materialize, args=(["summary"],))
    reader.start()
    try:
        assert reading.wait(10)
        migration = rs.AssetMigration(to=2)(lambda ctx: calls.append(ctx.run_id))
        repo = _repo(storage, assets(schema_version=2, migrations=[migration]))

        with _daemon(repo, storage):
            wait_for(
                lambda: storage.get_asset_version("orders").state == "draining",
                10,
                "the drain",
            )
            time.sleep(1.5)
            assert (storage.get_asset_version("orders").state, calls) == (
                "draining",
                [],
            )
            release.set()
            row = _ended(storage)
    finally:
        release.set()
        reader.join(30)

    assert (row.state, len(calls)) == ("ready", 1)


def test_a_failed_convert_holds_the_asset(storage):
    def add_currency(ctx) -> None:
        raise RuntimeError("the warehouse is down")

    assert _repo(storage, [_orders()]).materialize().success
    migration = rs.AssetMigration(to=2)(add_currency)
    repo = _repo(storage, [_orders(schema_version=2, migrations=[migration])])

    with _daemon(repo, storage):
        row = _ended(storage)

    assert (row.state, row.applied_version) == ("failed", 1)
    assert row.reason.startswith(
        f"the convert to version 2 failed in run {row.run_id}: "
    )
    assert "the warehouse is down" in row.reason
    with pytest.raises(
        AssetMigrationPendingError,
        match="^the migration of orders to version 2 failed: ",
    ):
        repo.materialize()


def test_a_version_without_a_migration_fails(storage):
    assert _repo(storage, [_orders()]).materialize().success
    steps = [rs.AssetMigration(to=3)]
    repo = _repo(storage, [_orders(schema_version=3, migrations=steps)])

    with _daemon(repo, storage):
        row = _ended(storage)

    assert (row.state, row.reason) == ("failed", "no migration to version 2")


def test_the_migration_of_an_asset_that_a_reader_reads_runs_first(storage):
    order = []

    def assets(version):
        def migrations(name):
            if version == 1:
                return []
            return [rs.AssetMigration(to=2)(lambda ctx: order.append(name))]

        @rs.Asset(
            io_handler=rs.InMemoryIOHandler(),
            schema_version=version,
            migrations=migrations("orders"),
        )
        def orders() -> int:
            return 1

        @rs.Asset(
            io_handler=rs.InMemoryIOHandler(),
            schema_version=version,
            migrations=migrations("summary"),
        )
        def summary(orders: int) -> int:
            return orders

        return [orders, summary]

    assert _repo(storage, assets(1)).materialize().success
    repo = _repo(storage, assets(2))

    with _daemon(repo, storage):
        _ended(storage, "orders")
        row = _ended(storage, "summary")

    assert (row.state, order) == ("ready", ["orders", "summary"])


def test_launched_by_a_migration():
    launched_by = rs.LaunchedBy.migration("orders", 3)

    assert (launched_by.kind, launched_by.asset_key, launched_by.to_version) == (
        "migration",
        "orders",
        3,
    )
    assert (launched_by.name, launched_by.backfill_id, launched_by.user) == (
        None,
        None,
        None,
    )
    assert repr(launched_by) == 'LaunchedBy.migration("orders", 3)'
    assert launched_by == rs.LaunchedBy.migration("orders", 3)
    assert hash(launched_by) != hash(rs.LaunchedBy.migration("orders", 4))
    assert rs.LaunchedBy.condition().asset_key is None
