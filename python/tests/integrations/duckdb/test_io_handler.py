"""DuckDBIOHandler on DuckDB database files: types, partitions, retries, executors."""

import json
import sys
import threading
from datetime import datetime

import pytest

duckdb = pytest.importorskip("duckdb")

import pandas as pd  # noqa: E402
import polars as pl  # noqa: E402
import pyarrow as pa  # noqa: E402
from arro3.core import RecordBatchReader as Arro3Reader  # noqa: E402
from polars.testing import assert_frame_equal  # noqa: E402

import rivers as rs  # noqa: E402
from rivers.integrations.duckdb import (  # noqa: E402
    DuckDBIOHandler,
    DuckDBResource,
)
from rivers.integrations.duckdb import io_handler as io_module  # noqa: E402

_TABLE = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})


def _handler(tmp_path, **kwargs) -> DuckDBIOHandler:
    return DuckDBIOHandler(
        resource=DuckDBResource(database=str(tmp_path / "w.duckdb")), **kwargs
    )


def _rows(handler, sql: str) -> list[tuple]:
    with handler.resource.get_connection() as con:
        return con.sql(sql).fetchall()


def _load(handler, name, type_hint, **kwargs):
    return handler.load_input(
        rs.InputContext(
            asset_name=name, downstream_asset="x", type_hint=type_hint, **kwargs
        )
    )


def _partition(definition, *keys) -> rs.PartitionContext:
    return rs.PartitionContext(keys=list(keys), definition=definition)


@pytest.mark.parametrize(
    "make",
    [
        pytest.param(lambda: _TABLE, id="pyarrow-table"),
        pytest.param(
            lambda: pa.RecordBatchReader.from_batches(
                _TABLE.schema, _TABLE.to_batches()
            ),
            id="pyarrow-reader",
        ),
        pytest.param(lambda: Arro3Reader.from_arrow(_TABLE), id="arro3-reader"),
        pytest.param(lambda: pl.DataFrame(_TABLE), id="polars"),
        pytest.param(lambda: pl.LazyFrame(_TABLE), id="polars-lazy"),
        pytest.param(lambda: _TABLE.to_pandas(), id="pandas"),
        pytest.param(
            lambda: duckdb.sql(
                "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) v(id, name)"
            ),
            id="relation",
        ),
    ],
)
def test_write_each_type(tmp_path, make):
    handler = _handler(tmp_path)
    handler.handle_output(rs.OutputContext(asset_name="t"), make())
    assert _rows(handler, "FROM main.t ORDER BY id") == [(1, "a"), (2, "b"), (3, "c")]


def test_read_each_type(tmp_path):
    handler = _handler(tmp_path)
    handler.handle_output(rs.OutputContext(asset_name="t"), _TABLE)

    rel = _load(handler, "t", duckdb.DuckDBPyRelation)
    assert rel.order("id").fetchall() == [(1, "a"), (2, "b"), (3, "c")]
    assert _load(handler, "t", pa.Table).sort_by("id").equals(_TABLE)
    reader = _load(handler, "t", pa.RecordBatchReader)
    assert reader.read_all().sort_by("id").equals(_TABLE)
    assert_frame_equal(
        _load(handler, "t", pl.DataFrame).sort("id"), pl.DataFrame(_TABLE)
    )
    lazy = _load(handler, "t", pl.LazyFrame)
    assert isinstance(lazy, pl.LazyFrame)
    assert_frame_equal(lazy.collect().sort("id"), pl.DataFrame(_TABLE))
    pd.testing.assert_frame_equal(
        _load(handler, "t", pd.DataFrame).sort_values("id", ignore_index=True),
        _TABLE.to_pandas(),
    )


def test_unread_lazy_inputs_do_not_block_the_database(tmp_path):
    handler = _handler(tmp_path)
    handler.handle_output(rs.OutputContext(asset_name="t"), _TABLE)

    reader = _load(handler, "t", pa.RecordBatchReader)
    rel = _load(handler, "t", duckdb.DuckDBPyRelation)
    handler.handle_output(rs.OutputContext(asset_name="u"), _TABLE)
    assert _load(handler, "u", pa.Table).num_rows == 3
    assert reader.read_all().num_rows == 3
    assert rel.count("*").fetchone() == (3,)


def test_read_errors(tmp_path):
    handler = _handler(tmp_path)
    with pytest.raises(TypeError, match="No type_hint"):
        _load(handler, "t", None)
    with pytest.raises(TypeError, match="cannot read <class 'dict'>"):
        _load(handler, "t", dict)
    with pytest.raises(duckdb.CatalogException, match="no DuckDB table main.missing"):
        _load(handler, "missing", pa.Table)
    with pytest.raises(
        ValueError, match="duckdb/version needs a DuckDBResource with ducklake"
    ):
        _load(handler, "t", pa.Table, asset_metadata={"duckdb/version": "1"})


def test_write_errors(tmp_path):
    handler = _handler(tmp_path)
    with pytest.raises(TypeError, match="cannot write dict"):
        handler.handle_output(rs.OutputContext(asset_name="t"), {"a": 1})
    with pytest.raises(ValueError, match="duckdb/mode must be"):
        handler.handle_output(
            rs.OutputContext(asset_name="t", asset_metadata={"duckdb/mode": "merge"}),
            _TABLE,
        )
    with pytest.raises(ValueError, match="ducklake/options needs"):
        handler.handle_output(
            rs.OutputContext(
                asset_name="t", asset_metadata={"ducklake/options": '{"a": 1}'}
            ),
            _TABLE,
        )


def test_relation_in_relation_out_on_one_file(tmp_path):
    handler = _handler(tmp_path)
    handler.handle_output(
        rs.OutputContext(asset_name="events"),
        pa.table({"day": ["d1", "d1", "d2"], "n": [1, 2, 3]}),
    )
    events = _load(handler, "events", duckdb.DuckDBPyRelation)
    daily = events.aggregate("day, sum(n) AS n").order("day")
    handler.handle_output(rs.OutputContext(asset_name="daily"), daily)

    assert _rows(handler, "FROM daily ORDER BY day") == [("d1", 3), ("d2", 3)]


def test_overwrite_replaces_the_table(tmp_path):
    handler = _handler(tmp_path)
    ctx = rs.OutputContext(asset_name="t")
    handler.handle_output(ctx, _TABLE)
    handler.handle_output(ctx, pa.table({"other": [True]}))
    assert _rows(handler, "FROM t") == [(True,)]


def test_static_partition_overwrite(tmp_path):
    handler = _handler(tmp_path)
    definition = rs.PartitionsDefinition.static_(["us", "eu"])
    meta = {"duckdb/partition_expr": "region"}

    def write(region, values):
        handler.handle_output(
            rs.OutputContext(
                asset_name="t",
                asset_metadata=meta,
                partition=_partition(definition, rs.PartitionKey.single(region)),
            ),
            pa.table({"region": [region] * len(values), "v": values}),
        )

    write("us", [1, 2])
    write("eu", [3])
    write("us", [4])

    assert _rows(handler, "FROM t ORDER BY v") == [("eu", 3), ("us", 4)]
    eu = _load(
        handler,
        "t",
        pa.Table,
        asset_metadata=meta,
        partition=_partition(definition, rs.PartitionKey.single("eu")),
    )
    assert eu.to_pylist() == [{"region": "eu", "v": 3}]


def test_daily_partition_overwrite_on_a_date_column(tmp_path):
    handler = _handler(tmp_path)
    definition = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))
    meta = {"duckdb/partition_expr": "day"}

    def write(day, value):
        handler.handle_output(
            rs.OutputContext(
                asset_name="t",
                asset_metadata=meta,
                partition=_partition(definition, rs.PartitionKey.single(day)),
            ),
            duckdb.sql(f"SELECT DATE '{day}' AS day, {value} AS v"),
        )

    write("2024-01-01", 1)
    write("2024-01-02", 2)
    write("2024-01-01", 3)

    assert _rows(
        handler, "SELECT strftime(day, '%Y-%m-%d'), v FROM t ORDER BY day"
    ) == [
        ("2024-01-01", 3),
        ("2024-01-02", 2),
    ]


def test_hourly_partition_overwrite_on_a_timestamp_column(tmp_path):
    handler = _handler(tmp_path)
    definition = rs.PartitionsDefinition.hourly(start=datetime(2024, 1, 1))
    meta = {"duckdb/partition_expr": "ts"}

    def write(key, minutes):
        rows = ", ".join(
            f"(TIMESTAMP '{key.replace('T', ' ')}' + INTERVAL {m} MINUTE, {m})"
            for m in minutes
        )
        handler.handle_output(
            rs.OutputContext(
                asset_name="t",
                asset_metadata=meta,
                partition=_partition(definition, rs.PartitionKey.single(key)),
            ),
            duckdb.sql(f"SELECT * FROM (VALUES {rows}) v(ts, m)"),
        )

    write("2024-01-01T05:00", [0, 30, 59])
    write("2024-01-01T06:00", [0])
    write("2024-01-01T05:00", [15])

    assert _rows(handler, "SELECT strftime(ts, '%H:%M'), m FROM t ORDER BY ts") == [
        ("05:15", 15),
        ("06:00", 0),
    ]


def test_multi_partition_overwrite(tmp_path):
    handler = _handler(tmp_path)
    definition = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
        }
    )
    meta = {"duckdb/partition_expr": json.dumps({"region": "region", "tier": "tier"})}

    def write(region, tier, v):
        handler.handle_output(
            rs.OutputContext(
                asset_name="t",
                asset_metadata=meta,
                partition=_partition(
                    definition, rs.PartitionKey.multi({"region": region, "tier": tier})
                ),
            ),
            pa.table({"region": [region], "tier": [tier], "v": [v]}),
        )

    write("us", "free", 1)
    write("us", "pro", 2)
    write("eu", "free", 3)
    write("us", "pro", 4)

    assert _rows(handler, "FROM t ORDER BY v") == [
        ("us", "free", 1),
        ("eu", "free", 3),
        ("us", "pro", 4),
    ]


def test_partitioned_write_needs_partition_expr(tmp_path):
    handler = _handler(tmp_path)
    definition = rs.PartitionsDefinition.static_(["us"])
    with pytest.raises(ValueError, match="'duckdb/partition_expr' metadata key"):
        handler.handle_output(
            rs.OutputContext(
                asset_name="t",
                partition=_partition(definition, rs.PartitionKey.single("us")),
            ),
            _TABLE,
        )


def test_append_mode_and_name_overrides(tmp_path):
    handler = _handler(tmp_path, mode="append", schema_name="raw")
    meta = {"duckdb/schema": "staging", "duckdb/table": "people"}
    ctx = rs.OutputContext(asset_name="asset", asset_metadata=meta)
    handler.handle_output(ctx, _TABLE)
    handler.handle_output(ctx, pa.table({"name": ["d"], "id": [4]}))

    assert _rows(handler, "SELECT id, name FROM staging.people ORDER BY id") == [
        (1, "a"),
        (2, "b"),
        (3, "c"),
        (4, "d"),
    ]
    assert ctx.output_metadata["duckdb/table"].raw_value() == "staging.people"
    columns = _load(
        handler,
        "asset",
        pa.Table,
        asset_metadata={**meta, "duckdb/columns": json.dumps(["name"])},
    )
    assert sorted(columns.column("name").to_pylist()) == ["a", "b", "c", "d"]
    assert columns.column_names == ["name"]

    handler.handle_output(rs.OutputContext(asset_name="plain"), _TABLE)
    assert _rows(handler, "SELECT count(*) FROM raw.plain") == [(3,)]


@pytest.mark.parametrize("name", ["graph/task", "mapped__0", 'odd "name"'])
def test_asset_names_are_quoted(tmp_path, name):
    handler = _handler(tmp_path)
    handler.handle_output(rs.OutputContext(asset_name=name), _TABLE)
    assert _load(handler, name, pa.Table).num_rows == 3
    with handler.resource.get_connection() as con:
        assert io_module.table_exists(con, "main", name)


def test_output_metadata(tmp_path):
    handler = _handler(tmp_path)
    ctx = rs.OutputContext(asset_name="t")
    handler.handle_output(ctx, _TABLE)

    meta = ctx.output_metadata
    assert meta["duckdb/table"].raw_value() == "main.t"
    assert meta["duckdb/num_rows"].raw_value() == 3
    assert meta["duckdb/write_duration_s"].raw_value() >= 0
    assert pa.schema(meta["rivers/schema"].raw_value()) == pa.schema(
        [("id", pa.int64()), ("name", pa.string())]
    )
    assert "ducklake/snapshot_id" not in meta


@pytest.mark.parametrize(
    ("blocked", "library"),
    [((), "pyarrow"), (("pyarrow",), "arro3"), (("pyarrow", "arro3.core"), None)],
    ids=["pyarrow", "arro3", "neither"],
)
def test_schema_metadata_without_pyarrow(tmp_path, monkeypatch, blocked, library):
    handler = _handler(tmp_path)
    handler.handle_output(rs.OutputContext(asset_name="t"), _TABLE)
    for name in blocked:
        monkeypatch.setitem(sys.modules, name, None)

    with handler.resource.get_connection() as con:
        schema = io_module._arrow_schema(con, '"main"."t"')

    if library is None:
        assert schema is None
    else:
        assert type(schema).__module__.startswith(library)
        assert pa.schema(schema) == _TABLE.schema


class _Stream:
    """An object that only exports an Arrow stream."""

    def __init__(self, table: pa.Table):
        self.reader = pa.RecordBatchReader.from_batches(
            table.schema, table.to_batches()
        )

    def __arrow_c_stream__(self, requested_schema=None):
        return self.reader.__arrow_c_stream__(requested_schema)


def _ipc_stream_reader(table: pa.Table) -> pa.RecordBatchStreamReader:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return pa.ipc.open_stream(sink.getvalue())


@pytest.mark.parametrize(
    "make",
    [
        pytest.param(
            lambda: pa.RecordBatchReader.from_batches(
                _TABLE.schema, _TABLE.to_batches()
            ),
            id="pyarrow-reader",
        ),
        pytest.param(lambda: _ipc_stream_reader(_TABLE), id="pyarrow-ipc-stream"),
        pytest.param(lambda: Arro3Reader.from_arrow(_TABLE), id="arro3-reader"),
        pytest.param(lambda: _Stream(_TABLE), id="arrow-c-stream"),
    ],
)
def test_stream_output_is_retried_after_a_conflict(tmp_path, monkeypatch, make):
    handler = _handler(tmp_path)
    attempts = []
    real_write = io_module._write

    def conflict_once(*args):
        real_write(*args)
        attempts.append(1)
        if len(attempts) == 1:
            raise duckdb.TransactionException("Transaction conflict: injected")

    monkeypatch.setattr(io_module, "_write", conflict_once)
    monkeypatch.setattr(io_module, "sleep", lambda s: None)
    handler.handle_output(rs.OutputContext(asset_name="t"), make())

    assert len(attempts) == 2
    assert _rows(handler, "FROM t ORDER BY id") == [(1, "a"), (2, "b"), (3, "c")]


def test_other_transaction_errors_are_not_retried(tmp_path, monkeypatch):
    handler = _handler(tmp_path)
    calls = []

    def fail(*args):
        calls.append(1)
        raise duckdb.TransactionException("something else")

    monkeypatch.setattr(io_module, "_write", fail)
    with pytest.raises(duckdb.TransactionException, match="something else"):
        handler.handle_output(rs.OutputContext(asset_name="t"), _TABLE)
    assert calls == [1]


def test_threads_overwrite_different_partitions(tmp_path):
    handler = _handler(tmp_path)
    definition = rs.PartitionsDefinition.static_([f"p{i}" for i in range(6)])
    meta = {"duckdb/partition_expr": "p"}
    handler.handle_output(
        rs.OutputContext(asset_name="t"), pa.table({"p": ["seed"], "v": [0]})
    )
    errors: list[BaseException] = []

    def write(i: int) -> None:
        try:
            handler.handle_output(
                rs.OutputContext(
                    asset_name="t",
                    asset_metadata=meta,
                    partition=_partition(definition, rs.PartitionKey.single(f"p{i}")),
                ),
                pa.table({"p": [f"p{i}"] * 1000, "v": list(range(1000))}),
            )
        except BaseException as e:
            errors.append(e)

    threads = [threading.Thread(target=write, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert _rows(handler, "SELECT p, count(*) FROM t GROUP BY p ORDER BY p") == [
        *[(f"p{i}", 1000) for i in range(6)],
        ("seed", 1),
    ]


def test_backfill_writes_every_key_in_one_run(storage, tmp_path):
    handler = _handler(tmp_path)
    definition = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))

    @rs.Asset(
        io_handler=handler,
        partitions_def=definition,
        metadata={"duckdb/partition_expr": "day"},
    )
    def events(context: rs.AssetExecutionContext) -> pa.Table:
        days = [k.key[0] for k in context.partition.keys]
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
    assert _rows(handler, "SELECT day FROM events ORDER BY day") == [
        ("2024-01-01",),
        ("2024-01-02",),
        ("2024-01-03",),
    ]

    result = repo.backfill(selection=["events"], partition_keys=keys[1:])
    assert result.completed == 2
    assert _rows(handler, "SELECT day, n FROM events ORDER BY day") == [
        ("2024-01-01", 0),
        ("2024-01-02", 0),
        ("2024-01-03", 0),
    ]


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("shape", ["single", "multi"])
def test_executors(storage, executor_env, tmp_path, is_async, shape):
    executor, _ = executor_env
    handler = _handler(tmp_path)

    if shape == "single":
        if is_async:

            @rs.Asset(io_handler=handler)
            async def users() -> pa.Table:
                return _TABLE

            @rs.Asset(io_handler=handler)
            async def scores() -> pl.DataFrame:
                return pl.DataFrame({"id": [1, 3], "score": [10, 30]})

        else:

            @rs.Asset(io_handler=handler)
            def users() -> pa.Table:
                return _TABLE

            @rs.Asset(io_handler=handler)
            def scores() -> pl.DataFrame:
                return pl.DataFrame({"id": [1, 3], "score": [10, 30]})

        sources = [users, scores]
    else:
        outputs = [
            rs.AssetDef("users", io_handler=handler),
            rs.AssetDef("scores", io_handler=handler),
        ]
        if is_async:

            @rs.Asset.from_multi(output_defs=outputs)
            async def both():
                return {
                    "users": _TABLE,
                    "scores": pl.DataFrame({"id": [1, 3], "score": [10, 30]}),
                }

        else:

            @rs.Asset.from_multi(output_defs=outputs)
            def both():
                return {
                    "users": _TABLE,
                    "scores": pl.DataFrame({"id": [1, 3], "score": [10, 30]}),
                }

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
    def total(scores: pa.Table) -> pa.Table:
        return pa.table({"total": [sum(scores.column("score").to_pylist())]})

    repo = rs.CodeRepository(
        assets=[*sources, ranked, total], default_executor=executor
    )
    repo.resolve(storage=storage)
    repo.materialize()

    assert _rows(handler, "FROM ranked") == [("c", 30), ("a", 10)]
    assert _rows(handler, "FROM total") == [(40,)]


def test_parallel_assets_share_one_file(storage, tmp_path):
    handler = _handler(tmp_path)

    def make(name):
        @rs.Asset(name=name, io_handler=handler)
        def asset() -> pa.Table:
            return pa.table({"n": list(range(50_000))})

        return asset

    assets = [make(f"a{i}") for i in range(4)]
    repo = rs.CodeRepository(
        assets=assets, default_executor=rs.Executor.parallel(max_workers=4)
    )
    repo.resolve(storage=storage)
    repo.materialize()

    for i in range(4):
        assert _rows(handler, f"SELECT count(*), sum(n) FROM a{i}") == [
            (50_000, 50_000 * 49_999 // 2)
        ]
