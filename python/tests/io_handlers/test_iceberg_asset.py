"""IcebergAsset actions: delete and expire_snapshots."""

import pytest

pytest.importorskip("pyiceberg")

import pyarrow as pa  # noqa: E402

import rivers as rs  # noqa: E402
from rivers.exceptions import ExecutionError  # noqa: E402
from rivers.io_handlers.iceberg import IcebergAsset  # noqa: E402

from .helpers import iceberg_handler  # noqa: E402

_DAYS = rs.PartitionsDefinition.static_(["a", "b"])


def _repo(storage, *assets) -> rs.CodeRepository:
    repo = rs.CodeRepository(
        assets=list(assets), default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    return repo


def _events(handler, meta=None):
    class Events(IcebergAsset):
        name = "events"
        io_handler = handler
        partitions_def = _DAYS
        metadata = {"iceberg/partition_expr": "day"} if meta is None else meta

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext) -> pa.Table:
            return pa.table({"day": [context.partition_key] * 2, "v": [1, 2]})

    return Events


def _materialized_events(tmp_path, storage, meta=None, **handler_kwargs):
    handler = iceberg_handler(tmp_path, **handler_kwargs)
    repo = _repo(storage, _events(handler, meta))
    for day in ("a", "b"):
        repo.materialize(partition_key=rs.PartitionKey.single(day))
    return handler, repo


def _days(handler) -> list[str]:
    return sorted(
        handler.load_table("events").scan().to_arrow().column("day").to_pylist()
    )


def _deletions(storage, name="events") -> int:
    return sum(e.event_type == "Deletion" for e in storage.get_events_for_asset(name))


def _action_metadata(storage, result) -> dict[str, str]:
    event = next(
        e
        for e in storage.get_events_for_run(result.run_id)
        if str(e.event_type) == "ActionCompleted"
    )
    return dict(event.metadata)


def test_kinds():
    assert IcebergAsset.kinds == "iceberg"


def test_keyed_delete_removes_one_partition(tmp_path, storage):
    handler, repo = _materialized_events(
        tmp_path, storage, snapshot_properties={"producer": "rivers"}
    )

    result = repo.run_action("delete", partition_key=rs.PartitionKey.single("a"))

    assert result.success
    assert _days(handler) == ["b", "b"]
    assert _deletions(storage) == 1
    summary = handler.load_table("events").current_snapshot().summary
    assert summary.operation.value == "delete"
    assert summary["producer"] == "rivers"


def test_keyless_delete_removes_every_row(tmp_path, storage):
    handler, repo = _materialized_events(tmp_path, storage)

    assert repo.run_action("delete").success

    assert _days(handler) == []
    assert storage.get_asset_record("events").last_timestamp is None
    assert storage.get_materialized_partitions("events") == []


def test_delete_of_a_missing_table_clears_state(tmp_path, storage):
    handler, repo = _materialized_events(tmp_path, storage)
    handler.catalog().drop_table(("ns", "events"))

    assert repo.run_action("delete").success
    assert storage.get_asset_record("events").last_timestamp is None


def test_keyed_delete_needs_partition_expr(tmp_path, storage):
    handler = iceberg_handler(tmp_path, mode="append")
    repo = _repo(storage, _events(handler, meta={}))
    repo.materialize(partition_key=rs.PartitionKey.single("a"))

    result = repo.run_action(
        "delete", partition_key=rs.PartitionKey.single("a"), raise_on_error=False
    )

    assert not result.success
    assert (
        "needs the 'iceberg/partition_expr' metadata key" in result.failed_assets[0][1]
    )


def test_expire_snapshots_keeps_the_newest(tmp_path, storage):
    handler, repo = _materialized_events(tmp_path, storage)
    repo.materialize(partition_key=rs.PartitionKey.single("a"))
    table = handler.load_table("events")
    before = len(table.snapshots())
    current = table.current_snapshot().snapshot_id

    result = repo.run_action(
        "expire_snapshots",
        config={"assets": {"events": {"config": {"retention_hours": 0}}}},
    )

    assert result.success
    table = handler.load_table("events")
    assert [s.snapshot_id for s in table.snapshots()] == [current]
    assert str(before - 1) in _action_metadata(storage, result)["snapshots_expired"]
    assert _days(handler) == ["a", "a", "b", "b"]


def test_expire_snapshots_retain_last_and_tags(tmp_path, storage):
    handler, repo = _materialized_events(tmp_path, storage)
    table = handler.load_table("events")
    first = table.snapshots()[0].snapshot_id
    table.manage_snapshots().create_tag(first, "audited").commit()
    repo.materialize(partition_key=rs.PartitionKey.single("a"))

    assert repo.run_action(
        "expire_snapshots",
        config={
            "assets": {"events": {"config": {"retention_hours": 0, "retain_last": 2}}}
        },
    ).success

    table = handler.load_table("events")
    kept = {s.snapshot_id for s in table.snapshots()}
    assert first in kept
    assert table.current_snapshot().snapshot_id in kept
    assert table.current_snapshot().parent_snapshot_id in kept
    assert len(kept) == 3


def test_expire_snapshots_defaults_keep_recent_snapshots(tmp_path, storage):
    handler, repo = _materialized_events(tmp_path, storage)
    before = len(handler.load_table("events").snapshots())

    result = repo.run_action("expire_snapshots")

    assert result.success
    assert len(handler.load_table("events").snapshots()) == before
    assert "0" in _action_metadata(storage, result)["snapshots_expired"]


def test_expire_snapshots_reads_the_table_property(tmp_path, storage):
    handler, repo = _materialized_events(
        tmp_path,
        storage,
        table_properties={"history.expire.max-snapshot-age-ms": "0"},
    )

    assert repo.run_action("expire_snapshots").success
    assert len(handler.load_table("events").snapshots()) == 1


def test_expire_snapshots_of_a_missing_table_is_unchanged(tmp_path, storage):
    handler = iceberg_handler(tmp_path)
    repo = _repo(storage, _events(handler))

    result = repo.run_action("expire_snapshots")

    assert result.success
    assert "0" in _action_metadata(storage, result)["snapshots_expired"]


def test_expire_snapshots_rejects_a_partition_key(tmp_path, storage):
    _, repo = _materialized_events(tmp_path, storage)
    with pytest.raises(ExecutionError, match="whole-asset"):
        repo.run_action("expire_snapshots", partition_key=rs.PartitionKey.single("a"))


def test_verb_config_rejects_unknown_keys(tmp_path, storage):
    _, repo = _materialized_events(tmp_path, storage)
    result = repo.run_action(
        "expire_snapshots",
        config={"assets": {"events": {"config": {"retention_hrs": 1}}}},
        raise_on_error=False,
    )
    assert not result.success
    assert "retention_hrs" in result.failed_assets[0][1]


def test_verbs_need_an_iceberg_handler(storage):
    class NotIceberg(IcebergAsset):
        name = "not_iceberg"
        io_handler = rs.InMemoryIOHandler()

        @classmethod
        def materialize(cls) -> dict:
            return {"v": 1}

    repo = _repo(storage, NotIceberg)
    repo.materialize()

    result = repo.run_action("expire_snapshots", raise_on_error=False)
    assert not result.success
    assert "IcebergIOHandler" in result.failed_assets[0][1]


def test_subclass_override_wins(tmp_path, storage):
    calls = []
    handler = iceberg_handler(tmp_path)

    class Events(IcebergAsset):
        name = "events"
        io_handler = handler

        @classmethod
        def materialize(cls) -> pa.Table:
            return pa.table({"v": [1]})

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def expire_snapshots(cls, ctx: rs.ActionContext) -> None:
            calls.append(ctx.asset_name)

    repo = _repo(storage, Events)
    repo.materialize()

    assert repo.run_action("expire_snapshots").success
    assert calls == ["events"]
