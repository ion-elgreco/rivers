"""IcebergIOHandler in repository runs: executors, async and multi-asset steps, backfills."""

from datetime import date, datetime

import pytest

pytest.importorskip("pyiceberg")
duckdb = pytest.importorskip("duckdb")

import pandas as pd  # noqa: E402
import polars as pl  # noqa: E402
import pyarrow as pa  # noqa: E402

import rivers as rs  # noqa: E402

from .helpers import iceberg_handler  # noqa: E402

_USERS = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
_SCORES = pl.DataFrame({"id": [1, 3], "score": [10, 30]})


def _table(handler, name: str) -> pa.Table:
    return handler.load_table(name).scan().to_arrow()


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("shape", ["single", "multi"])
def test_executors(storage, executor_env, tmp_path, is_async, shape):
    executor, _ = executor_env
    handler = iceberg_handler(tmp_path)

    if shape == "single":
        if is_async:

            @rs.Asset(io_handler=handler)
            async def users() -> pa.Table:
                return _USERS

            @rs.Asset(io_handler=handler)
            async def scores() -> pl.DataFrame:
                return _SCORES

        else:

            @rs.Asset(io_handler=handler)
            def users() -> pa.Table:
                return _USERS

            @rs.Asset(io_handler=handler)
            def scores() -> pl.DataFrame:
                return _SCORES

        sources = [users, scores]
    else:
        outputs = [
            rs.AssetDef("users", io_handler=handler),
            rs.AssetDef("scores", io_handler=handler),
        ]
        if is_async:

            @rs.Asset.from_multi(output_defs=outputs)
            async def both():
                return {"users": _USERS, "scores": _SCORES}

        else:

            @rs.Asset.from_multi(output_defs=outputs)
            def both():
                return {"users": _USERS, "scores": _SCORES}

        sources = [both]

    if is_async:

        @rs.Asset(io_handler=handler)
        async def ranked(
            users: duckdb.DuckDBPyRelation, scores: duckdb.DuckDBPyRelation
        ) -> duckdb.DuckDBPyRelation:
            return users.join(scores, "id").select("name, score").order("score DESC")

    else:

        @rs.Asset(io_handler=handler)
        def ranked(
            users: duckdb.DuckDBPyRelation, scores: duckdb.DuckDBPyRelation
        ) -> duckdb.DuckDBPyRelation:
            return users.join(scores, "id").select("name, score").order("score DESC")

    @rs.Asset(io_handler=handler)
    def total(scores: pd.DataFrame, users: pl.LazyFrame) -> pa.Table:
        named = users.filter(pl.col("id").is_in(scores["id"].tolist())).collect()
        return pa.table(
            {"total": [int(scores["score"].sum())], "users": [named.height]}
        )

    repo = rs.CodeRepository(
        assets=[*sources, ranked, total], default_executor=executor
    )
    repo.resolve(storage=storage)
    repo.materialize()

    assert sorted(_table(handler, "ranked").to_pylist(), key=lambda r: r["name"]) == [
        {"name": "a", "score": 10},
        {"name": "c", "score": 30},
    ]
    assert _table(handler, "total").to_pylist() == [{"total": 40, "users": 2}]


def test_materialization_records_the_snapshot_as_data_version(storage, tmp_path):
    handler = iceberg_handler(tmp_path)

    @rs.Asset(io_handler=handler)
    def users() -> pa.Table:
        return _USERS

    repo = rs.CodeRepository(assets=[users], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    repo.materialize()

    [event] = [
        e
        for e in storage.get_events_for_asset("users")
        if e.event_type == "Materialization"
    ]
    snapshot_id = handler.load_table("users").current_snapshot().snapshot_id
    assert event.data_version == str(snapshot_id)
    assert "ns.users" in dict(event.metadata)["iceberg/table"]


def test_backfill_writes_every_key_in_one_run(storage, tmp_path):
    handler = iceberg_handler(tmp_path)
    definition = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))

    @rs.Asset(
        io_handler=handler,
        partitions_def=definition,
        metadata={"iceberg/partition_expr": "day"},
    )
    def events(context: rs.AssetExecutionContext) -> pa.Table:
        days = [date.fromisoformat(k.key[0]) for k in context.partition.keys]
        return pa.table({"day": days, "n": list(range(len(days)))})

    repo = rs.CodeRepository(assets=[events], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    keys = [rs.PartitionKey.single(f"2024-01-0{i}") for i in (1, 2, 3)]

    result = repo.backfill(
        selection=["events"],
        partition_keys=keys,
        strategy=rs.BackfillStrategy.single_run(),
    )
    assert result.num_runs == 1
    assert _table(handler, "events").sort_by("day").column("n").to_pylist() == [0, 1, 2]

    result = repo.backfill(selection=["events"], partition_keys=keys[1:])
    assert result.completed == 2
    assert _table(handler, "events").sort_by("day").column("n").to_pylist() == [0, 0, 0]


def test_partitioned_downstream_reads_its_partition(storage, tmp_path):
    handler = iceberg_handler(tmp_path)
    definition = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))

    @rs.Asset(
        io_handler=handler,
        partitions_def=definition,
        metadata={"iceberg/partition_expr": "ts"},
    )
    def events(context: rs.AssetExecutionContext) -> pl.DataFrame:
        start = datetime.fromisoformat(context.partition_key)
        return pl.DataFrame({"ts": [start.replace(hour=h) for h in (1, 2, 3)]})

    @rs.Asset(
        io_handler=handler,
        partitions_def=definition,
        metadata={"iceberg/partition_expr": "day"},
    )
    def daily(context: rs.AssetExecutionContext, events: pl.DataFrame) -> pa.Table:
        day = date.fromisoformat(context.partition_key)
        return pa.table({"day": [day], "events": [events.height]})

    repo = rs.CodeRepository(
        assets=[events, daily], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    for day in ("2024-01-01", "2024-01-02"):
        repo.materialize(partition_key=rs.PartitionKey.single(day))

    assert _table(handler, "daily").sort_by("day").to_pylist() == [
        {"day": date(2024, 1, 1), "events": 3},
        {"day": date(2024, 1, 2), "events": 3},
    ]


def test_input_metadata_selects_columns_and_snapshot(storage, tmp_path):
    handler = iceberg_handler(tmp_path)
    users_v1 = pa.table({"id": [1], "name": ["a"], "email": ["a@x"]})

    @rs.Asset(io_handler=handler)
    def users() -> pa.Table:
        return users_v1

    repo = rs.CodeRepository(assets=[users], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    repo.materialize()
    first = handler.load_table("users").current_snapshot().snapshot_id
    users_v1 = pa.table({"id": [2], "name": ["b"], "email": ["b@x"]})
    repo.materialize()

    @rs.Asset(
        io_handler=rs.InMemoryIOHandler(),
        deps=[
            rs.AssetDef.input(
                "users",
                metadata={
                    "iceberg/columns": '["id", "name"]',
                    "iceberg/snapshot_id": str(first),
                },
            )
        ],
    )
    def names(users: pa.Table) -> list:
        return users.to_pylist()

    repo = rs.CodeRepository(
        assets=[users, names], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    repo.materialize(selection=["names"])

    assert repo.load_node("names") == [{"id": 1, "name": "a"}]
