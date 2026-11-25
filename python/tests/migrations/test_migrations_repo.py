"""``repo.migrate()`` runs the asset migrations of its code in this process,
without a daemon."""

import _thread
import threading
import time

import pytest

import rivers as rs
from rivers.exceptions import AssetMigrationPendingError, AssetNotFoundError

PD = rs.PartitionsDefinition.static_(["eu", "us", "ap"])


def _repo(storage, assets):
    repo = rs.CodeRepository(assets=assets, default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    return repo


def _orders(io, **declared):
    @rs.Asset(io_handler=io, **declared)
    def orders() -> int:
        return 1

    return orders


def test_migrate_converts_the_stored_data_in_this_process(storage):
    io = rs.InMemoryIOHandler()
    calls = []
    assert _repo(storage, [_orders(io)]).materialize().success
    migration = rs.AssetMigration(to=2)(lambda ctx: calls.append(ctx.run_id))
    repo = _repo(storage, [_orders(io, schema_version=2, migrations=[migration])])

    (record,) = repo.migrate()

    assert (record.asset_key, record.state, record.applied_version) == (
        "orders",
        "ready",
        2,
    )
    (run_id,) = calls
    run = storage.get_run(run_id)
    assert (run.status, run.action) == ("Success", "migrate")
    assert run.launched_by == rs.LaunchedBy.migration("orders", 2)
    assert repo.materialize().success
    ready = storage.get_asset_version("orders")
    (again,) = repo.migrate()
    assert (again.state, again.revision) == ("ready", ready.revision)
    assert len(calls) == 1


def _partitioned(io, version, seen):
    migrations = [rs.AssetMigration(to=2, rebuild=True)] if version > 1 else []

    @rs.Asset(
        partitions_def=PD,
        io_handler=io,
        schema_version=version,
        migrations=migrations,
    )
    def orders(context: rs.AssetExecutionContext) -> int:
        seen.append((version, context.partition_key))
        return version

    return orders


def test_migrate_rebuilds_each_stored_partition_in_this_process(storage):
    io = rs.InMemoryIOHandler()
    seen = []
    old = _repo(storage, [_partitioned(io, 1, seen)])
    for key in ("eu", "us"):
        assert old.materialize(partition_key=rs.PartitionKey.single(key)).success
    seen.clear()
    repo = _repo(storage, [_partitioned(io, 2, seen)])

    (record,) = repo.migrate(["orders"])

    assert (record.state, record.applied_version) == ("ready", 2)
    assert sorted(seen) == [(2, "eu"), (2, "us")]
    ((_, kind, _, backfill_id, filled, failed),) = storage._asset_version_history(
        "orders"
    )
    assert (kind, filled, failed) == ("rebuild", 2, 0)
    assert repo.get_backfill(backfill_id).launched_by == rs.LaunchedBy.migration(
        "orders", 2
    )


def test_migrate_retries_a_failed_migration(storage):
    io = rs.InMemoryIOHandler()
    down = [True]

    def add_currency(ctx: rs.MigrationContext) -> None:
        if down[0]:
            raise RuntimeError("the warehouse is down")

    assert _repo(storage, [_orders(io)]).materialize().success
    migration = rs.AssetMigration(to=2)(add_currency)
    repo = _repo(storage, [_orders(io, schema_version=2, migrations=[migration])])

    with pytest.raises(
        AssetMigrationPendingError,
        match="^the migration of orders to version 2 failed: ",
    ):
        repo.migrate()
    (record,) = repo.migrate(raise_on_error=False)
    assert (record.state, record.applied_version) == ("failed", 1)
    assert "the warehouse is down" in record.reason

    down[0] = False
    (record,) = repo.migrate()

    assert (record.state, record.applied_version, record.reason) == ("ready", 2, None)


def test_migrate_runs_the_migrations_of_the_assets_that_the_selection_reads_first(
    storage,
):
    io = rs.InMemoryIOHandler()
    order = []

    def assets(version):
        def migrations(name):
            if version == 1:
                return []
            return [rs.AssetMigration(to=2)(lambda ctx: order.append(name))]

        @rs.Asset(
            io_handler=io, schema_version=version, migrations=migrations("orders")
        )
        def orders() -> int:
            return 1

        @rs.Asset(
            io_handler=io, schema_version=version, migrations=migrations("summary")
        )
        def summary(orders: int) -> int:
            return orders

        return [orders, summary]

    assert _repo(storage, assets(1)).materialize().success
    repo = _repo(storage, assets(2))

    records = repo.migrate(["summary"])

    assert [(r.asset_key, r.state) for r in records] == [("summary", "ready")]
    assert order == ["orders", "summary"]
    assert storage.get_asset_version("orders").state == "ready"


def test_migrate_waits_for_a_started_run_of_a_reader(storage):
    io = rs.InMemoryIOHandler()
    reading = threading.Event()
    release = threading.Event()

    def assets(**declared):
        @rs.Asset(io_handler=io)
        def summary(orders: int) -> int:
            reading.set()
            release.wait(30)
            return orders

        return [_orders(io, **declared), summary]

    old = _repo(storage, assets())
    assert old.materialize(["orders"]).success
    reader = threading.Thread(target=old.materialize, args=(["summary"],))
    reader.start()
    try:
        assert reading.wait(10)
        migration = rs.AssetMigration(to=2)(lambda ctx: None)
        repo = _repo(storage, assets(schema_version=2, migrations=[migration]))
        records = []
        migrating = threading.Thread(
            target=lambda: records.extend(repo.migrate(["orders"]))
        )
        migrating.start()

        time.sleep(1.0)
        assert migrating.is_alive()
        assert storage.get_asset_version("orders").state == "draining"
        release.set()
        migrating.join(30)
    finally:
        release.set()
        reader.join(30)

    assert [(r.state, r.applied_version) for r in records] == [("ready", 2)]


def test_migrate_that_stops_lets_go_of_its_migrations(storage):
    io = rs.InMemoryIOHandler()
    reading = threading.Event()
    release = threading.Event()

    def assets(**declared):
        @rs.Asset(io_handler=io)
        def summary(orders: int) -> int:
            reading.set()
            release.wait(30)
            return orders

        return [_orders(io, **declared), summary]

    old = _repo(storage, assets())
    assert old.materialize(["orders"]).success
    reader = threading.Thread(target=old.materialize, args=(["summary"],))
    reader.start()
    try:
        assert reading.wait(10)
        migration = rs.AssetMigration(to=2)(lambda ctx: None)
        repo = _repo(storage, assets(schema_version=2, migrations=[migration]))
        # Ctrl-C while the migration waits for the reader.
        threading.Timer(0.5, _thread.interrupt_main).start()
        with pytest.raises(KeyboardInterrupt):
            repo.migrate(["orders"])
    finally:
        release.set()
        reader.join(30)

    row = storage.get_asset_version("orders")
    assert (row.state, row.owner, row.lease_expires_at) == ("draining", None, None)
    (record,) = repo.migrate(["orders"])
    assert (record.state, record.applied_version) == ("ready", 2)


def test_migrate_rejects_an_unknown_asset(storage):
    repo = _repo(storage, [_orders(rs.InMemoryIOHandler())])

    with pytest.raises(
        AssetNotFoundError, match="^Selection contains unknown asset: 'nope'$"
    ):
        repo.migrate(["nope"])
