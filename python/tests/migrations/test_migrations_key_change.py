"""A change of an asset's partition keys. Without a migration, an asset with
stored partitions waits, because its stored partitions do not name partitions
of the new definition."""

import re
from datetime import datetime

import pytest

import rivers as rs
from rivers.exceptions import AssetMigrationPendingError

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
