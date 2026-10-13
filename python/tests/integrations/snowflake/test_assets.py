"""SnowflakeAsset's delete action against a fakesnow server."""

import json
from datetime import date, datetime

import pytest

snowflake_connector = pytest.importorskip("snowflake.connector")
pytest.importorskip("fakesnow")

import pyarrow as pa  # noqa: E402

import rivers as rs  # noqa: E402
from _helpers import make_repo  # noqa: E402
from rivers.integrations.snowflake import (  # noqa: E402
    SnowflakeAsset,
    SnowflakeIOHandler,
    SnowflakeResource,
)

_REGIONS = rs.PartitionsDefinition.static_(["us", "eu"])


def _rows(warehouse: SnowflakeResource, sql: str) -> list[tuple]:
    with warehouse.get_connection() as con:
        cur = con.cursor()
        cur.execute(sql)
        return cur.fetchall()


def _deletions(storage, name: str) -> int:
    return sum(e.event_type == "Deletion" for e in storage.get_events_for_asset(name))


def _users(handler):
    class Users(SnowflakeAsset):
        name = "users"
        io_handler = handler

        @classmethod
        def materialize(cls) -> pa.Table:
            return pa.table({"id": [1, 2], "name": ["a", "b"]})

    return Users


def _action_context(handler, partition=None, meta=None) -> rs.ActionContext:
    return rs.ActionContext(
        asset_name="events",
        action="delete",
        io_handler=handler,
        asset_metadata=meta,
        partition=partition,
    )


def test_keyed_delete_removes_one_partition(storage, executor_env, warehouse):
    executor, _ = executor_env

    class Events(SnowflakeAsset):
        name = "events"
        io_handler = SnowflakeIOHandler(resource=warehouse)
        partitions_def = _REGIONS
        metadata = {"snowflake/partition_expr": "region"}

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext) -> pa.Table:
            return pa.table({"region": [context.partition_key] * 2, "v": [1, 2]})

    repo = make_repo([Events], storage, executor=executor)
    for region in ("us", "eu"):
        repo.materialize(partition_key=rs.PartitionKey.single(region))

    assert repo.run_action("delete", partition_key=rs.PartitionKey.single("us")).success

    assert _rows(
        warehouse, 'SELECT "REGION", "V" FROM "PUBLIC"."EVENTS" ORDER BY "V"'
    ) == [("eu", 1), ("eu", 2)]
    assert _deletions(storage, "events") == 1
    assert storage.get_materialized_partitions("events") == [
        rs.PartitionKey.single("eu")
    ]


def test_backfill_delete_removes_each_day(storage, warehouse):
    """One run with several daily keys deletes each day's range."""

    class Daily(SnowflakeAsset):
        name = "daily"
        io_handler = SnowflakeIOHandler(resource=warehouse)
        partitions_def = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))
        metadata = {"snowflake/partition_expr": "day"}

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext) -> pa.Table:
            return pa.table({"day": [date.fromisoformat(context.partition_key)]})

    repo = make_repo([Daily], storage)
    keys = [rs.PartitionKey.single(f"2024-01-0{day}") for day in (1, 2, 3)]
    for key in keys:
        repo.materialize(partition_key=key)

    result = repo.backfill(
        selection=["daily"],
        partition_keys=[keys[0], keys[2]],
        strategy=rs.BackfillStrategy.single_run(),
        action="delete",
    )

    assert (result.status, result.num_runs) == ("CompletedSuccess", 1)
    assert _rows(warehouse, 'SELECT "DAY" FROM "PUBLIC"."DAILY"') == [
        (date(2024, 1, 2),)
    ]
    assert storage.get_materialized_partitions("daily") == [keys[1]]


def test_backfill_over_days_of_a_timestamp_column(storage, warehouse):
    """One run over several days selects each day's range of a timestamp column:
    a rerun replaces the rows, and a read and the delete find them."""
    days = ["2024-01-01", "2024-01-02", "2024-01-03"]

    class Events(SnowflakeAsset):
        name = "events"
        io_handler = SnowflakeIOHandler(resource=warehouse)
        partitions_def = rs.PartitionsDefinition.multi(
            {
                "region": _REGIONS,
                "date": rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1)),
            }
        )
        metadata = {
            "snowflake/partition_expr": json.dumps({"region": "region", "date": "ts"})
        }

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext) -> pa.Table:
            keys = [key.keys for key in context.partition.keys]
            return pa.table(
                {
                    "region": [key["region"][0] for key in keys],
                    "ts": [
                        datetime.fromisoformat(key["date"][0]).replace(hour=10)
                        for key in keys
                    ],
                }
            )

    repo = make_repo([Events], storage)
    count = 'SELECT "REGION", COUNT(*) FROM "PUBLIC"."EVENTS" GROUP BY 1 ORDER BY 1'

    def keys(region: str) -> list[rs.PartitionKey]:
        return [rs.PartitionKey.multi({"region": region, "date": day}) for day in days]

    def backfill(region: str, action: str | None = None) -> None:
        result = repo.backfill(
            selection=["events"],
            partition_keys=keys(region),
            strategy=rs.BackfillStrategy.single_run(),
            action=action,
        )
        assert (result.status, result.num_runs) == ("CompletedSuccess", 1)

    backfill("eu")
    backfill("us")
    backfill("us")
    assert _rows(warehouse, count) == [("eu", 3), ("us", 3)]

    us = repo.load_node(
        "events",
        partition_key=rs.PartitionKey.multi({"region": "us", "date": days}),
        type_hint=pa.Table,
    )
    assert us.sort_by("ts").to_pylist() == [
        {"region": "us", "ts": datetime(2024, 1, day, 10)} for day in (1, 2, 3)
    ]

    backfill("us", action="delete")
    assert _rows(warehouse, count) == [("eu", 3)]
    assert set(storage.get_materialized_partitions("events")) == set(keys("eu"))


def test_keyless_delete_removes_every_row_and_keeps_the_table(storage, warehouse):
    repo = make_repo([_users(SnowflakeIOHandler(resource=warehouse))], storage)
    repo.materialize()

    assert repo.run_action("delete").success

    assert _rows(warehouse, 'SELECT COUNT(*) FROM "PUBLIC"."USERS"') == [(0,)]
    assert _rows(
        warehouse,
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'USERS' ORDER BY column_name",
    ) == [("ID",), ("NAME",)]
    assert _deletions(storage, "users") == 1
    assert storage.get_asset_record("users").last_run_id is None


def test_delete_on_a_missing_table_clears_state(storage, warehouse):
    repo = make_repo([_users(SnowflakeIOHandler(resource=warehouse))], storage)
    repo.materialize()
    _rows(warehouse, 'DROP TABLE "PUBLIC"."USERS"')

    assert repo.run_action("delete").success

    assert _deletions(storage, "users") == 1
    assert storage.get_asset_record("users").last_run_id is None


def test_delete_runs_in_the_handler_session(warehouse, monkeypatch):
    """Partition bounds are UTC times. fakesnow always runs in UTC, so the
    session that the delete opens is checked instead."""
    handler = SnowflakeIOHandler(resource=warehouse)
    handler.handle_output(
        rs.OutputContext(asset_name="events"), pa.table({"region": ["us"]})
    )

    opened = []
    connect = snowflake_connector.connect

    def recording(**kwargs):
        opened.append(kwargs)
        return connect(**kwargs)

    monkeypatch.setattr(snowflake_connector, "connect", recording)
    partition = rs.PartitionContext(
        keys=[rs.PartitionKey.single("us")], definition=_REGIONS
    )
    meta = {"snowflake/partition_expr": "region"}
    SnowflakeAsset.delete(_action_context(handler, partition, meta))

    assert [kwargs["timezone"] for kwargs in opened] == ["UTC"]
    assert _rows(warehouse, 'SELECT COUNT(*) FROM "PUBLIC"."EVENTS"') == [(0,)]


@pytest.mark.parametrize("process_wide", [False, True], ids=["resource", "process"])
def test_delete_with_qmark_paramstyle(warehouse, monkeypatch, process_wide):
    """The delete binds its table lookup, whatever paramstyle the resource or process sets."""
    handler = SnowflakeIOHandler(resource=warehouse)
    handler.handle_output(
        rs.OutputContext(asset_name="events"), pa.table({"region": ["us", "eu"]})
    )

    if process_wide:
        monkeypatch.setattr(snowflake_connector, "paramstyle", "qmark")
    else:
        args = {**warehouse.connection_args, "paramstyle": "qmark"}
        resource = SnowflakeResource(database=warehouse.database, connection_args=args)
        handler = SnowflakeIOHandler(resource=resource)

    partition = rs.PartitionContext(
        keys=[rs.PartitionKey.single("us")], definition=_REGIONS
    )
    meta = {"snowflake/partition_expr": "region"}
    SnowflakeAsset.delete(_action_context(handler, partition, meta))

    assert _rows(warehouse, 'SELECT "REGION" FROM "PUBLIC"."EVENTS"') == [("eu",)]


def test_keyed_delete_with_a_backslash_in_the_key(warehouse):
    """Snowflake reads backslash escapes in string literals."""
    keys = ["us", "a\\b", "us\\", "x\\' OR 1=1 --"]
    handler = SnowflakeIOHandler(resource=warehouse)
    handler.handle_output(
        rs.OutputContext(asset_name="events"),
        pa.table({"region": keys, "v": list(range(len(keys)))}),
    )

    partition = rs.PartitionContext(
        keys=[rs.PartitionKey.single("x\\' OR 1=1 --")],
        definition=rs.PartitionsDefinition.static_(keys),
    )
    meta = {"snowflake/partition_expr": "region"}
    SnowflakeAsset.delete(_action_context(handler, partition, meta))

    assert _rows(
        warehouse, 'SELECT "REGION", "V" FROM "PUBLIC"."EVENTS" ORDER BY "V"'
    ) == [("us", 0), ("a\\b", 1), ("us\\", 2)]


def test_keyed_delete_without_partition_expr_names_the_key():
    resource = SnowflakeResource(account="unreachable", user="u", password="p")
    partition = rs.PartitionContext(
        keys=[rs.PartitionKey.single("us")], definition=_REGIONS
    )
    ctx = _action_context(SnowflakeIOHandler(resource=resource), partition)
    with pytest.raises(ValueError, match="'snowflake/partition_expr'"):
        SnowflakeAsset.delete(ctx)


def test_delete_needs_a_snowflake_handler():
    ctx = _action_context(rs.InMemoryIOHandler())
    with pytest.raises(
        TypeError, match="SnowflakeAsset actions need a SnowflakeIOHandler"
    ):
        SnowflakeAsset.delete(ctx)
