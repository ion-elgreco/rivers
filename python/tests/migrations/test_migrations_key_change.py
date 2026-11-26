"""A change of an asset's partition keys. Without a migration, an asset with
stored partitions waits, because its stored partitions do not name partitions
of the new definition. The migration to a new version fills the partitions of
the new definition that the stored partitions cover."""

import pickle
import re
from datetime import datetime

import pytest

import rivers as rs
from _polling import wait_for
from rivers._core import AutomationDaemon
from rivers.exceptions import AssetDefinitionError, AssetMigrationPendingError

DAILY = rs.PartitionsDefinition.daily(
    start=datetime(2024, 1, 1), end=datetime(2024, 2, 1)
)
HOURLY = rs.PartitionsDefinition.hourly(
    start=datetime(2024, 1, 1), end=datetime(2024, 2, 1)
)

BLOCKED = (
    "the partition keys of orders changed (before: daily from 2024-01-01 to 2024-02-01; "
    "now: hourly from 2024-01-01T00:00 to 2024-02-01T00:00); raise schema_version to 2 and "
    "add AssetMigration(to=2): a convert, a rebuild of 48 partitions (rebuild=True), or "
    "rebuild=True, partitions=[] to start empty"
)


def _repo(storage, io, partitions_def, **declared):
    @rs.Asset(io_handler=io, partitions_def=partitions_def, **declared)
    def orders() -> int:
        return 1

    repo = rs.CodeRepository(assets=[orders], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    return repo


def _materialize(repo, *keys):
    for key in keys:
        assert repo.materialize(partition_key=rs.PartitionKey.single(key)).success


def _state(storage):
    record = storage.get_asset_version("orders")
    return None if record is None else (record.state, record.applied_version)


def test_new_keys_hold_an_asset_with_stored_partitions(storage):
    io = rs.InMemoryIOHandler()
    _materialize(_repo(storage, io, DAILY), "2024-01-01", "2024-01-02")

    hourly = _repo(storage, io, HOURLY)

    assert _state(storage) == ("blocked", 1)
    assert storage.get_asset_version("orders").reason == BLOCKED
    with pytest.raises(AssetMigrationPendingError, match=re.escape(BLOCKED)):
        _materialize(hourly, "2024-01-03T00:00")


def test_the_old_keys_again_end_the_hold(storage):
    io = rs.InMemoryIOHandler()
    _materialize(_repo(storage, io, DAILY), "2024-01-01")
    _repo(storage, io, HOURLY)

    daily = _repo(storage, io, DAILY)

    assert _state(storage) == ("ready", 1)
    _materialize(daily, "2024-01-02")


def test_a_new_version_lets_a_migration_change_the_keys(storage):
    io = rs.InMemoryIOHandler()
    _materialize(_repo(storage, io, DAILY), "2024-01-01")
    _repo(storage, io, HOURLY)

    _repo(
        storage,
        io,
        HOURLY,
        schema_version=2,
        migrations=[rs.AssetMigration(to=2, rebuild=True)],
    )

    record = storage.get_asset_version("orders")
    assert (record.state, record.applied_version, record.target_version) == (
        "pending",
        1,
        2,
    )
    assert '"0 0 * * *"' in record.partitions


@pytest.mark.parametrize(
    ("before", "after", "key"),
    [
        (DAILY, HOURLY, "2024-01-05T00:00"),
        (rs.PartitionsDefinition.static_(["eu", "us"]), DAILY, "2024-01-05"),
    ],
    ids=["daily-to-hourly", "static-to-daily"],
)
def test_new_keys_without_stored_partitions_need_no_migration(
    storage, before, after, key
):
    io = rs.InMemoryIOHandler()
    _repo(storage, io, before)

    repo = _repo(storage, io, after)

    assert _state(storage) is None
    _materialize(repo, key)


@pytest.mark.parametrize(
    ("before", "after"),
    [
        (DAILY, rs.PartitionsDefinition.daily(start=datetime(2023, 1, 1))),
        (
            rs.PartitionsDefinition.static_(["eu", "us"]),
            rs.PartitionsDefinition.static_(["us", "ap"]),
        ),
    ],
    ids=["daily-range", "static-keys"],
)
def test_a_new_range_or_new_static_keys_keep_the_keys(storage, before, after):
    io = rs.InMemoryIOHandler()
    key = "2024-01-02" if before is DAILY else "us"
    _materialize(_repo(storage, io, before), key)

    repo = _repo(storage, io, after)

    assert _state(storage) is None
    _materialize(repo, key)


def _hours(day: str) -> list[str]:
    return [f"{day}T{hour:02}:00" for hour in range(24)]


def _materialized(storage) -> list[str]:
    return sorted(str(key) for key in storage.get_materialized_partitions("orders"))


def _history(storage):
    ((version, kind, run_id, backfill_id, filled, failed),) = (
        storage._asset_version_history("orders")
    )
    return version, kind, run_id, backfill_id, filled, failed


def test_a_convert_fills_the_new_partitions_in_one_run(storage):
    io = rs.InMemoryIOHandler()
    _materialize(_repo(storage, io, DAILY), "2024-01-01", "2024-01-02")
    seen = []

    def to_hours(ctx: rs.MigrationContext) -> None:
        seen.append(sorted(str(key) for key in ctx.partition.keys))
        ctx.mark_partition_failed(rs.PartitionKey.single("2024-01-02T05:00"), "no rows")

    migration = rs.AssetMigration(to=2)(to_hours)
    repo = _repo(storage, io, HOURLY, schema_version=2, migrations=[migration])

    (record,) = repo.migrate()

    assert (record.state, record.applied_version) == ("ready", 2)
    hours = _hours("2024-01-01") + _hours("2024-01-02")
    assert seen == [hours]
    _, kind, run_id, _, filled, failed = _history(storage)
    assert (kind, filled, failed) == ("convert", 47, 1)
    run_key, _ = storage._stored_keys(run_id)
    assert run_key == "2024-01-01T00:00..2024-01-02T23:00"
    assert _materialized(storage) == [
        hour for hour in hours if hour != "2024-01-02T05:00"
    ]
    _materialize(repo, "2024-01-03T00:00")


def test_a_rebuild_backfills_the_new_partitions(storage):
    io = rs.InMemoryIOHandler()
    _materialize(_repo(storage, io, DAILY), "2024-01-01")
    seen = []

    @rs.Asset(
        io_handler=io,
        partitions_def=HOURLY,
        schema_version=2,
        migrations=[rs.AssetMigration(to=2, rebuild=True)],
    )
    def orders(context: rs.AssetExecutionContext) -> int:
        seen.extend(str(key) for key in context.partition.keys)
        return 2

    repo = rs.CodeRepository(assets=[orders], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)

    (record,) = repo.migrate()

    assert (record.state, record.applied_version) == ("ready", 2)
    assert sorted(seen) == _hours("2024-01-01")
    _, kind, _, backfill_id, filled, failed = _history(storage)
    assert (kind, filled, failed) == ("rebuild", 24, 0)
    assert backfill_id is not None
    assert _materialized(storage) == _hours("2024-01-01")


def test_a_migration_that_only_records_the_version_starts_the_asset_empty(storage):
    io = rs.InMemoryIOHandler()
    _materialize(_repo(storage, io, DAILY), "2024-01-01")
    repo = _repo(
        storage, io, HOURLY, schema_version=2, migrations=[rs.AssetMigration(to=2)]
    )

    (record,) = repo.migrate()

    assert (record.state, record.applied_version) == ("ready", 2)
    assert _history(storage) == (2, "version_only", None, None, 0, 0)
    assert _materialized(storage) == []


def test_partitions_name_the_partitions_that_rivers_cannot_find(storage):
    io = rs.InMemoryIOHandler()
    _materialize(
        _repo(storage, io, rs.PartitionsDefinition.static_(["eu", "us"])), "eu"
    )
    seen = []

    def to_days(ctx: rs.MigrationContext) -> None:
        seen.append([str(key) for key in ctx.partition.keys])

    found = _repo(
        storage,
        io,
        DAILY,
        schema_version=2,
        migrations=[rs.AssetMigration(to=2)(to_days)],
    )
    with pytest.raises(AssetMigrationPendingError, match="give them with partitions="):
        found.migrate()

    named = rs.AssetMigration(to=2, partitions=[rs.PartitionKey.single("2024-01-01")])(
        to_days
    )
    (record,) = _repo(
        storage, io, DAILY, schema_version=2, migrations=[named]
    ).migrate()

    assert (record.state, record.applied_version) == ("ready", 2)
    assert seen == [["2024-01-01"]]
    assert _materialized(storage) == ["2024-01-01"]


def test_no_partitions_to_fill_start_the_asset_empty_without_a_run(storage):
    io = rs.InMemoryIOHandler()
    _materialize(_repo(storage, io, DAILY), "2024-01-01")
    calls = []

    class Orders(rs.Asset):
        io_handler = io
        partitions_def = HOURLY
        schema_version = 2

        @classmethod
        def materialize(cls) -> int:
            return 2

        @rs.migration(to=2, partitions=[])
        @classmethod
        def to_hours(cls, ctx) -> None:
            calls.append(ctx.run_id)

    repo = rs.CodeRepository(assets=[Orders], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)

    (record,) = repo.migrate()

    assert (record.state, record.applied_version) == ("ready", 2)
    assert calls == []
    assert _materialized(storage) == []


def test_a_convert_to_an_asset_without_partitions_fills_it_whole(storage):
    io = rs.InMemoryIOHandler()
    _materialize(_repo(storage, io, DAILY), "2024-01-01")
    seen = []
    migration = rs.AssetMigration(to=2)(lambda ctx: seen.append(ctx.partition))
    repo = _repo(storage, io, None, schema_version=2, migrations=[migration])

    (record,) = repo.migrate()

    assert (record.state, record.applied_version) == ("ready", 2)
    assert seen == [None]
    _, _, run_id, _, filled, failed = _history(storage)
    assert (filled, failed) == (1, 0)
    materializations = [
        event
        for event in storage.get_events_for_run(run_id)
        if event.event_type == "Materialization"
    ]
    assert [event.partition_key for event in materializations] == [None]
    assert _materialized(storage) == []
    assert repo.materialize().success


def test_multi_partitions_fill_the_new_time_keys_under_each_other_value(storage):
    io = rs.InMemoryIOHandler()
    regions = rs.PartitionsDefinition.static_(["eu", "us"])
    by_day = rs.PartitionsDefinition.multi({"date": DAILY, "region": regions})
    by_hour = rs.PartitionsDefinition.multi({"date": HOURLY, "region": regions})
    old = _repo(storage, io, by_day)
    stored = rs.PartitionKey.multi({"date": "2024-01-01", "region": "eu"})
    assert old.materialize(partition_key=stored).success
    seen = []

    def to_hours(ctx: rs.MigrationContext) -> None:
        seen.extend(ctx.partition.keys)

    repo = _repo(
        storage,
        io,
        by_hour,
        schema_version=2,
        migrations=[rs.AssetMigration(to=2)(to_hours)],
    )

    (record,) = repo.migrate()

    assert (record.state, record.applied_version) == ("ready", 2)
    assert len(seen) == 24
    assert len(storage.get_materialized_partitions("orders")) == 24


def test_partitions_on_a_migration_that_only_records_the_version_is_an_error():
    with pytest.raises(AssetDefinitionError, match="only records the version"):

        @rs.Asset(
            partitions_def=HOURLY,
            schema_version=2,
            migrations=[rs.AssetMigration(to=2, partitions=[])],
        )
        def orders() -> int:
            return 1


def _convert(ctx: rs.MigrationContext) -> None:
    pass


def test_an_asset_migration_pickles_with_its_partitions():
    key = rs.PartitionKey.single("2024-01-01T00:00")
    migration = rs.AssetMigration(to=2, partitions=[key])(_convert)

    copy = pickle.loads(pickle.dumps(migration))

    assert copy.partitions == [key]


@pytest.mark.parametrize("queue", [False, True], ids=["direct", "queued"])
def test_the_daemon_fills_the_new_partitions(storage, queue):
    io = rs.InMemoryIOHandler()
    _materialize(_repo(storage, io, DAILY), "2024-01-01")
    seen = []

    @rs.Asset(
        io_handler=io,
        partitions_def=HOURLY,
        schema_version=2,
        migrations=[
            rs.AssetMigration(to=2)(lambda ctx: seen.append(len(ctx.partition.keys)))
        ],
    )
    def orders() -> int:
        return 2

    extra = {"run_queue": rs.RunQueueConfig(dequeue_interval="50ms")} if queue else {}
    repo = rs.CodeRepository(
        assets=[orders], default_executor=rs.Executor.in_process(), **extra
    )
    repo.resolve(storage=storage)
    daemon = AutomationDaemon(repo=repo, storage=storage, condition_eval_interval="1s")
    daemon.start()
    try:
        wait_for(
            lambda: storage.get_asset_version("orders").state in ("ready", "failed"),
            30.0,
            "the migration of orders to end",
        )
    finally:
        daemon.stop()

    record = storage.get_asset_version("orders")
    assert (record.state, record.applied_version) == ("ready", 2)
    assert seen == [24]
    assert _materialized(storage) == _hours("2024-01-01")
