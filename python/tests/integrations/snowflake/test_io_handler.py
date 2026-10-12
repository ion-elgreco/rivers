"""SnowflakeIOHandler against a fakesnow server: types, partitions, transactions, executors.

fakesnow runs Snowflake SQL on DuckDB. Its server mode takes the real
connector's requests, PUT uploads included, so loky workers reach it too.
"""

import json
import os
from datetime import date, datetime, time, timezone
from decimal import Decimal

import pytest

pytest.importorskip("snowflake.connector")
pytest.importorskip("fakesnow")

from snowflake.connector.errors import ProgrammingError  # noqa: E402

import pandas as pd  # noqa: E402
import polars as pl  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from arro3.core import RecordBatchReader as Arro3Reader  # noqa: E402
from polars.testing import assert_frame_equal  # noqa: E402

import rivers as rs  # noqa: E402
from _helpers import make_repo  # noqa: E402
from rivers.integrations.snowflake import (  # noqa: E402
    SnowflakeIOHandler,
    SnowflakeResource,
)
from rivers.integrations.snowflake import io_handler as io_module  # noqa: E402

_TABLE = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})


def _rows(warehouse: SnowflakeResource, sql: str) -> list[tuple]:
    with warehouse.get_connection() as con:
        cur = con.cursor()
        cur.execute(sql)
        return cur.fetchall()


def _load(handler, name, type_hint, **kwargs):
    return handler.load_input(
        rs.InputContext(
            asset_name=name, downstream_asset="x", type_hint=type_hint, **kwargs
        )
    )


def _partition(definition, *keys) -> rs.PartitionContext:
    return rs.PartitionContext(keys=list(keys), definition=definition)


_REGIONS = rs.PartitionsDefinition.static_(["us", "eu"])
_BY_REGION = {"snowflake/partition_expr": "region"}


def _write_region(handler, region: str, values: list) -> None:
    handler.handle_output(
        rs.OutputContext(
            asset_name="t",
            asset_metadata=_BY_REGION,
            partition=_partition(_REGIONS, rs.PartitionKey.single(region)),
        ),
        pa.table({"region": [region] * len(values), "v": values}),
    )


@pytest.mark.parametrize(
    "make",
    [
        pytest.param(lambda: _TABLE, id="pyarrow-table"),
        pytest.param(lambda: _TABLE.to_reader(), id="pyarrow-reader"),
        pytest.param(lambda: Arro3Reader.from_arrow(_TABLE), id="arro3-reader"),
        pytest.param(lambda: pl.DataFrame(_TABLE), id="polars"),
        pytest.param(lambda: pl.LazyFrame(_TABLE), id="polars-lazy"),
        pytest.param(lambda: _TABLE.to_pandas(), id="pandas"),
    ],
)
def test_write_each_type(warehouse, make):
    handler = SnowflakeIOHandler(resource=warehouse)
    handler.handle_output(rs.OutputContext(asset_name="t"), make())
    assert _rows(warehouse, 'SELECT "ID", "NAME" FROM "PUBLIC"."T" ORDER BY "ID"') == [
        (1, "a"),
        (2, "b"),
        (3, "c"),
    ]


def test_read_each_type(warehouse):
    handler = SnowflakeIOHandler(resource=warehouse)
    handler.handle_output(rs.OutputContext(asset_name="t"), _TABLE)

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


def test_values_round_trip(warehouse):
    handler = SnowflakeIOHandler(resource=warehouse)
    utc = datetime(2024, 1, 1, 5, 30, tzinfo=timezone.utc)
    rows = pa.table(
        {
            "Mixed_Case": pa.array([1, None], pa.int32()),
            "price": pa.array([Decimal("1.25"), None], pa.decimal128(10, 2)),
            "ratio": [0.5, None],
            "ok": [True, None],
            "label": ["x", None],
            "day": pa.array([date(2024, 1, 1), None], pa.date32()),
            "at": pa.array([time(5, 30), None], pa.time64("us")),
            "local": pa.array([datetime(2024, 1, 1, 5, 30)] * 2, pa.timestamp("us")),
            "instant": pa.array([utc] * 2, pa.timestamp("us", tz="UTC")),
            "raw": pa.array([b"\x00\xff", None]),
            "tags": pa.array([["a", "b"], None]),
        }
    )
    handler.handle_output(rs.OutputContext(asset_name="t"), rows)

    read = _load(handler, "t", pa.Table).sort_by("price")
    assert read.column_names == [name.lower() for name in rows.column_names]
    assert [json.loads(tags or "null") for tags in read.column("tags").to_pylist()] == [
        ["a", "b"],
        None,
    ]
    assert read.drop_columns("tags").to_pylist() == [
        {
            "mixed_case": 1,
            "price": Decimal("1.25"),
            "ratio": 0.5,
            "ok": True,
            "label": "x",
            "day": date(2024, 1, 1),
            "at": time(5, 30),
            "local": datetime(2024, 1, 1, 5, 30),
            "instant": utc,
            "raw": b"\x00\xff",
        },
        {
            "mixed_case": None,
            "price": None,
            "ratio": None,
            "ok": None,
            "label": None,
            "day": None,
            "at": None,
            "local": datetime(2024, 1, 1, 5, 30),
            "instant": utc,
            "raw": None,
        },
    ]
    assert read.schema.field("instant").type.tz in ("UTC", "Etc/UTC")


@pytest.mark.parametrize(
    ("arrow_type", "snowflake_type"),
    [
        (pa.int8(), "NUMBER(38,0)"),
        (pa.uint64(), "NUMBER(38,0)"),
        (pa.float32(), "FLOAT"),
        (pa.decimal128(10, 2), "NUMBER(10,2)"),
        (pa.decimal32(5, 1), "NUMBER(5,1)"),
        (pa.bool_(), "BOOLEAN"),
        (pa.string(), "VARCHAR"),
        (pa.large_string(), "VARCHAR"),
        (pa.string_view(), "VARCHAR"),
        (pa.null(), "VARCHAR"),
        (pa.dictionary(pa.int8(), pa.string()), "VARCHAR"),
        (pa.binary(), "BINARY"),
        (pa.binary(16), "BINARY"),
        (pa.date32(), "DATE"),
        (pa.time32("s"), "TIME(0)"),
        (pa.time64("ns"), "TIME(9)"),
        (pa.timestamp("ms"), "TIMESTAMP_NTZ(3)"),
        (pa.timestamp("us", tz="Europe/Amsterdam"), "TIMESTAMP_LTZ(6)"),
        (pa.list_(pa.int64()), "ARRAY"),
        (pa.struct({"a": pa.int64()}), "OBJECT"),
        (pa.map_(pa.string(), pa.int64()), "OBJECT"),
        (pa.uuid(), "BINARY"),
    ],
)
def test_snowflake_column_types(arrow_type, snowflake_type):
    assert io_module._snowflake_type(arrow_type) == snowflake_type


def test_reads_lowercase_names_and_widen_integers():
    """Snowflake sends small NUMBER values as int8, with its own field metadata."""
    sent = pa.table(
        [pa.array([1, 2], pa.int8()), pa.array(["a", "b"]), pa.array([1.5, None])],
        schema=pa.schema(
            [
                pa.field("ID", pa.int8(), metadata={"logicalType": "FIXED"}),
                pa.field("NAME", pa.string(), metadata={"logicalType": "TEXT"}),
                pa.field("RATIO", pa.float64()),
            ]
        ),
    )
    read = io_module._from_snowflake(sent)
    assert read.schema == pa.schema(
        [("id", pa.int64()), ("name", pa.string()), ("ratio", pa.float64())]
    )
    assert read.schema.field("id").metadata is None
    assert read.to_pylist() == [
        {"id": 1, "name": "a", "ratio": 1.5},
        {"id": 2, "name": "b", "ratio": None},
    ]


class RecordingCursor:
    def __init__(self, *results: list[tuple]):
        self.results = list(results)
        self.sql: list[str] = []

    def execute(self, sql, params=None):
        self.sql.append(sql)

    def fetchall(self):
        return self.results.pop(0)


@pytest.mark.parametrize(
    ("results", "exists", "creates"),
    [
        ([[("ID",)]], True, []),
        ([[], [(1,)]], False, []),
        ([[], [(0,)]], False, ['CREATE SCHEMA IF NOT EXISTS "RAW"']),
    ],
    ids=["table", "schema", "neither"],
)
def test_create_statements_only_run_for_missing_objects(results, exists, creates):
    """A role without the create privilege can still write to existing objects."""
    cur = RecordingCursor(*results)
    assert io_module._prepare_table(cur, "RAW", "T", ["id"]) is exists
    assert [sql for sql in cur.sql if sql.startswith("CREATE")] == creates


def test_unsupported_column_type_fails_before_connecting():
    resource = SnowflakeResource(account="unreachable", user="u", password="p")
    handler = SnowflakeIOHandler(resource=resource)
    with pytest.raises(TypeError, match="no column type for Arrow type duration"):
        handler.handle_output(
            rs.OutputContext(asset_name="t"),
            pa.table({"took": pa.array([1], pa.duration("s"))}),
        )


def test_read_errors(warehouse):
    handler = SnowflakeIOHandler(resource=warehouse)
    with pytest.raises(TypeError, match="No type_hint"):
        _load(handler, "t", None)
    with pytest.raises(TypeError, match="cannot read <class 'dict'>"):
        _load(handler, "t", dict)
    with pytest.raises(ProgrammingError, match="MISSING"):
        _load(handler, "missing", pa.Table)


def test_write_errors(warehouse):
    handler = SnowflakeIOHandler(resource=warehouse)
    with pytest.raises(TypeError, match="cannot write dict"):
        handler.handle_output(rs.OutputContext(asset_name="t"), {"a": 1})
    with pytest.raises(ValueError, match="snowflake/mode must be"):
        handler.handle_output(
            rs.OutputContext(
                asset_name="t", asset_metadata={"snowflake/mode": "merge"}
            ),
            _TABLE,
        )


def test_overwrite_replaces_the_rows(warehouse):
    handler = SnowflakeIOHandler(resource=warehouse)
    ctx = rs.OutputContext(asset_name="t")
    handler.handle_output(ctx, _TABLE)
    handler.handle_output(ctx, pa.table({"id": [4], "name": ["d"]}))
    assert _rows(warehouse, 'SELECT "ID", "NAME" FROM "PUBLIC"."T"') == [(4, "d")]


def test_new_column_fails_and_keeps_the_rows(warehouse):
    handler = SnowflakeIOHandler(resource=warehouse)
    ctx = rs.OutputContext(asset_name="t")
    handler.handle_output(ctx, _TABLE)
    with pytest.raises(ValueError, match="table PUBLIC.T has no column score"):
        handler.handle_output(ctx, pa.table({"id": [4], "name": ["d"], "score": [1]}))
    assert _rows(warehouse, 'SELECT COUNT(*) FROM "PUBLIC"."T"') == [(3,)]

    handler.handle_output(ctx, pa.table({"id": [5]}))
    assert _rows(warehouse, 'SELECT "ID", "NAME" FROM "PUBLIC"."T"') == [(5, None)]


def test_failed_load_rolls_back_the_delete(warehouse):
    handler = SnowflakeIOHandler(resource=warehouse)
    _write_region(handler, "us", [1, 2])
    with pytest.raises(ProgrammingError, match="not-a-number"):
        _write_region(handler, "us", ["not-a-number"])
    assert _rows(warehouse, 'SELECT "REGION", "V" FROM "PUBLIC"."T" ORDER BY "V"') == [
        ("us", 1),
        ("us", 2),
    ]


def test_static_partition_overwrite(warehouse):
    handler = SnowflakeIOHandler(resource=warehouse)
    _write_region(handler, "us", [1, 2])
    _write_region(handler, "eu", [3])
    _write_region(handler, "us", [4])

    assert _rows(warehouse, 'SELECT "REGION", "V" FROM "PUBLIC"."T" ORDER BY "V"') == [
        ("eu", 3),
        ("us", 4),
    ]
    eu = _load(
        handler,
        "t",
        pa.Table,
        asset_metadata=_BY_REGION,
        partition=_partition(_REGIONS, rs.PartitionKey.single("eu")),
    )
    assert eu.to_pylist() == [{"region": "eu", "v": 3}]


def test_daily_partition_overwrite_on_a_date_column(warehouse):
    handler = SnowflakeIOHandler(resource=warehouse)
    definition = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))
    meta = {"snowflake/partition_expr": "day"}

    def write(day, value):
        handler.handle_output(
            rs.OutputContext(
                asset_name="t",
                asset_metadata=meta,
                partition=_partition(definition, rs.PartitionKey.single(day)),
            ),
            pa.table({"day": [date.fromisoformat(day)], "v": [value]}),
        )

    write("2024-01-01", 1)
    write("2024-01-02", 2)
    write("2024-01-01", 3)

    assert _rows(warehouse, 'SELECT "DAY", "V" FROM "PUBLIC"."T" ORDER BY "DAY"') == [
        (date(2024, 1, 1), 3),
        (date(2024, 1, 2), 2),
    ]


@pytest.mark.parametrize("tz", [None, "UTC"], ids=["timestamp_ntz", "timestamp_ltz"])
def test_hourly_partition_overwrite_on_a_timestamp_column(warehouse, tz):
    handler = SnowflakeIOHandler(resource=warehouse)
    definition = rs.PartitionsDefinition.hourly(start=datetime(2024, 1, 1))
    meta = {"snowflake/partition_expr": "ts"}

    def write(key, minutes):
        hour = datetime.fromisoformat(key)
        handler.handle_output(
            rs.OutputContext(
                asset_name="t",
                asset_metadata=meta,
                partition=_partition(definition, rs.PartitionKey.single(key)),
            ),
            pa.table(
                {
                    "ts": pa.array(
                        [hour.replace(minute=m) for m in minutes],
                        pa.timestamp("us", tz=tz),
                    ),
                    "m": minutes,
                }
            ),
        )

    write("2024-01-01T05:00", [0, 30, 59])
    write("2024-01-01T06:00", [0])
    write("2024-01-01T05:00", [15])

    read = _load(handler, "t", pl.DataFrame).sort("ts")
    assert read.select(pl.col("ts").dt.strftime("%H:%M"), "m").rows() == [
        ("05:15", 15),
        ("06:00", 0),
    ]


def test_multi_partition_overwrite(warehouse):
    handler = SnowflakeIOHandler(resource=warehouse)
    definition = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
        }
    )
    meta = {
        "snowflake/partition_expr": json.dumps({"region": "region", "tier": "tier"})
    }

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

    assert _rows(warehouse, 'SELECT * FROM "PUBLIC"."T" ORDER BY "V"') == [
        ("us", "free", 1),
        ("eu", "free", 3),
        ("us", "pro", 4),
    ]


def test_partitioned_write_needs_partition_expr(warehouse):
    handler = SnowflakeIOHandler(resource=warehouse)
    definition = rs.PartitionsDefinition.static_(["us"])
    with pytest.raises(ValueError, match="'snowflake/partition_expr' metadata key"):
        handler.handle_output(
            rs.OutputContext(
                asset_name="t",
                partition=_partition(definition, rs.PartitionKey.single("us")),
            ),
            _TABLE,
        )


def test_append_mode_and_name_overrides(warehouse):
    handler = SnowflakeIOHandler(resource=warehouse, mode="append", schema_name="raw")
    meta = {"snowflake/schema": "staging", "snowflake/table": "people"}
    ctx = rs.OutputContext(asset_name="asset", asset_metadata=meta)
    handler.handle_output(ctx, _TABLE)
    handler.handle_output(ctx, pa.table({"name": ["d"], "id": [4]}))

    assert _rows(
        warehouse, 'SELECT "ID", "NAME" FROM "STAGING"."PEOPLE" ORDER BY "ID"'
    ) == [(1, "a"), (2, "b"), (3, "c"), (4, "d")]
    assert ctx.output_metadata["snowflake/table"].raw_value() == (
        f"{warehouse.database}.STAGING.PEOPLE"
    )
    names = _load(
        handler,
        "asset",
        pa.Table,
        asset_metadata={**meta, "snowflake/columns": json.dumps(["name"])},
    )
    assert names.column_names == ["name"]
    assert sorted(names.column("name").to_pylist()) == ["a", "b", "c", "d"]

    handler.handle_output(rs.OutputContext(asset_name="plain"), _TABLE)
    assert _rows(warehouse, 'SELECT COUNT(*) FROM "RAW"."PLAIN"') == [(3,)]


def test_names_are_uppercased_and_quoted():
    """Checked as text: fakesnow's COPY cannot load a table whose name needs
    quotes, such as ``graph/task``. Snowflake can."""
    assert io_module.quote_table("graph", 'task/"odd" name') == (
        '"GRAPH"."TASK/""ODD"" NAME"'
    )


def test_empty_output_creates_the_table(warehouse):
    handler = SnowflakeIOHandler(resource=warehouse)
    handler.handle_output(rs.OutputContext(asset_name="t"), _TABLE.slice(0, 0))

    read = _load(handler, "t", pa.Table)
    assert read.num_rows == 0
    assert read.column_names == ["id", "name"]


def _parquet_file(file) -> pq.ParquetFile:
    # a BufferReader, not the BytesIO: a read from a Python file can hang the
    # interpreter at exit
    return pq.ParquetFile(pa.BufferReader(file.getvalue()))


def test_large_batches_are_sliced_into_files(monkeypatch):
    monkeypatch.setattr(io_module, "_FILE_SIZE", 64 * 1024)
    table = pa.table({"id": pa.array(range(100_000), pa.int64())})

    files = list(io_module._parquet_files(table.to_reader()))

    assert len(files) == -(-table.nbytes // io_module._FILE_SIZE)
    read = pa.concat_tables(_parquet_file(file).read() for file in files)
    assert read.equals(table)


def test_small_batches_share_a_row_group():
    batches = [pa.record_batch({"id": [i] * 10}) for i in range(100)]
    reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)

    [file] = io_module._parquet_files(reader)

    metadata = _parquet_file(file).metadata
    assert (metadata.num_rows, metadata.num_row_groups) == (1000, 1)


@pytest.mark.parametrize(
    "make",
    [
        pytest.param(lambda: _TABLE.to_reader(), id="pyarrow-reader"),
        pytest.param(lambda: pl.LazyFrame(_TABLE), id="polars-lazy"),
    ],
)
def test_write_in_several_files(warehouse, monkeypatch, make):
    """Each row is its own file; the overwrite loads them all in one transaction."""
    handler = SnowflakeIOHandler(resource=warehouse)
    ctx = rs.OutputContext(asset_name="t")
    handler.handle_output(ctx, pa.table({"id": [9], "name": ["old"]}))

    monkeypatch.setattr(io_module, "_FILE_SIZE", 1)
    staged = []
    parquet_files = io_module._parquet_files

    def recording(reader):
        for file in parquet_files(reader):
            staged.append(_parquet_file(file).metadata.num_rows)
            yield file

    monkeypatch.setattr(io_module, "_parquet_files", recording)
    handler.handle_output(ctx, make())

    assert staged == [1, 1, 1]
    assert _rows(warehouse, 'SELECT "ID", "NAME" FROM "PUBLIC"."T" ORDER BY "ID"') == [
        (1, "a"),
        (2, "b"),
        (3, "c"),
    ]


def test_lazy_frame_is_not_collected(warehouse, monkeypatch):
    def collect(self, *args, **kwargs):
        raise AssertionError("LazyFrame.collect")

    monkeypatch.setattr(pl.LazyFrame, "collect", collect)
    handler = SnowflakeIOHandler(resource=warehouse)
    lazy = pl.LazyFrame({"id": range(1, 1001)}).with_columns(double=pl.col("id") * 2)

    handler.handle_output(rs.OutputContext(asset_name="t"), lazy)

    assert _rows(warehouse, 'SELECT COUNT(*), SUM("DOUBLE") FROM "PUBLIC"."T"') == [
        (1000, 1001000)
    ]


def _failing_reader() -> pa.RecordBatchReader:
    def batches():
        yield from _TABLE.to_batches()
        raise RuntimeError("source failed")

    return pa.RecordBatchReader.from_batches(_TABLE.schema, batches())


@pytest.mark.parametrize(
    ("make", "match"),
    [
        pytest.param(_failing_reader, "source failed", id="pyarrow-reader"),
        pytest.param(
            lambda: pl.LazyFrame({"id": ["1", "x"]}).select(pl.col("id").cast(int)),
            "conversion from `str` to `i64` failed",
            id="polars-lazy",
        ),
    ],
)
def test_failed_read_changes_nothing(warehouse, monkeypatch, make, match):
    """Files staged before the failure are not loaded, and a new asset gets no table."""
    monkeypatch.setattr(io_module, "_FILE_SIZE", 1)
    handler = SnowflakeIOHandler(resource=warehouse)
    handler.handle_output(rs.OutputContext(asset_name="t"), _TABLE)

    with pytest.raises(Exception, match=match):
        handler.handle_output(rs.OutputContext(asset_name="t"), make())
    with pytest.raises(Exception, match=match):
        handler.handle_output(rs.OutputContext(asset_name="new"), make())

    assert _rows(warehouse, 'SELECT "ID" FROM "PUBLIC"."T" ORDER BY "ID"') == [
        (1,),
        (2,),
        (3,),
    ]
    assert _rows(
        warehouse,
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'NEW'",
    ) == [(0,)]


def test_output_metadata(warehouse):
    handler = SnowflakeIOHandler(resource=warehouse)
    ctx = rs.OutputContext(asset_name="t")
    handler.handle_output(ctx, _TABLE)
    handler.handle_output(
        rs.OutputContext(asset_name="t", asset_metadata={"snowflake/mode": "append"}),
        _TABLE,
    )

    meta = ctx.output_metadata
    assert meta["snowflake/table"].raw_value() == f"{warehouse.database}.PUBLIC.T"
    assert meta["snowflake/num_rows"].raw_value() == 3
    assert meta["snowflake/write_duration_s"].raw_value() >= 0
    assert pa.schema(meta["rivers/schema"].raw_value()) == _TABLE.schema
    assert _rows(warehouse, 'SELECT COUNT(*) FROM "PUBLIC"."T"') == [(6,)]


def test_backfill_writes_every_key_in_one_run(storage, warehouse):
    handler = SnowflakeIOHandler(resource=warehouse)
    definition = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))

    @rs.Asset(
        io_handler=handler,
        partitions_def=definition,
        metadata={"snowflake/partition_expr": "day"},
    )
    def events(context: rs.AssetExecutionContext) -> pa.Table:
        days = [k.key[0] for k in context.partition.keys]
        return pa.table({"day": days, "n": list(range(len(days)))})

    repo = make_repo([events], storage)
    keys = [rs.PartitionKey.single(f"2024-01-0{i}") for i in (1, 2, 3)]
    result = repo.backfill(
        selection=["events"],
        partition_keys=keys,
        strategy=rs.BackfillStrategy.single_run(),
    )
    assert result.num_runs == 1
    assert _rows(
        warehouse, 'SELECT "DAY", "N" FROM "PUBLIC"."EVENTS" ORDER BY "DAY"'
    ) == [
        ("2024-01-01", 0),
        ("2024-01-02", 1),
        ("2024-01-03", 2),
    ]

    result = repo.backfill(selection=["events"], partition_keys=keys[1:])
    assert result.completed == 2
    assert _rows(
        warehouse, 'SELECT "DAY", "N" FROM "PUBLIC"."EVENTS" ORDER BY "DAY"'
    ) == [
        ("2024-01-01", 0),
        ("2024-01-02", 0),
        ("2024-01-03", 0),
    ]


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("shape", ["single", "multi"])
def test_executors(storage, executor_env, warehouse, is_async, shape):
    """Steps in loky workers rebuild the handler and the injected resource."""
    executor, _ = executor_env
    handler = SnowflakeIOHandler(resource=warehouse)
    scores_frame = pl.DataFrame({"id": [1, 3], "score": [10, 30]})

    if shape == "single":
        if is_async:

            @rs.Asset(io_handler=handler)
            async def users() -> pa.Table:
                return _TABLE

            @rs.Asset(io_handler=handler)
            async def scores() -> pl.DataFrame:
                return scores_frame

        else:

            @rs.Asset(io_handler=handler)
            def users() -> pa.Table:
                return _TABLE

            @rs.Asset(io_handler=handler)
            def scores() -> pl.DataFrame:
                return scores_frame

        sources = [users, scores]
    else:
        outputs = [
            rs.AssetDef("users", io_handler=handler),
            rs.AssetDef("scores", io_handler=handler),
        ]
        if is_async:

            @rs.Asset.from_multi(output_defs=outputs)
            async def both():
                return {"users": _TABLE, "scores": scores_frame}

        else:

            @rs.Asset.from_multi(output_defs=outputs)
            def both():
                return {"users": _TABLE, "scores": scores_frame}

        sources = [both]

    if is_async:

        @rs.Asset(io_handler=handler)
        async def ranked(users: pl.DataFrame, scores: pl.LazyFrame) -> pl.LazyFrame:
            return scores.join(users.lazy(), on="id").select("name", "score")

    else:

        @rs.Asset(io_handler=handler)
        def ranked(users: pl.DataFrame, scores: pl.LazyFrame) -> pl.LazyFrame:
            return scores.join(users.lazy(), on="id").select("name", "score")

    @rs.Asset(io_handler=handler)
    def total(scores: pd.DataFrame, warehouse: SnowflakeResource) -> pa.Table:
        with warehouse.get_connection() as con:
            cur = con.cursor()
            cur.execute('SELECT COUNT(*) FROM "PUBLIC"."USERS"')
            [(users,)] = cur.fetchall()
        return pa.table(
            {
                "users": [users],
                "total": [int(scores["score"].sum())],
                "pid": [os.getpid()],
            }
        )

    repo = make_repo(
        [*sources, ranked, total],
        storage,
        executor=executor,
        resources={"warehouse": warehouse},
    )
    repo.materialize()

    assert _rows(
        warehouse, 'SELECT "NAME", "SCORE" FROM "PUBLIC"."RANKED" ORDER BY "SCORE"'
    ) == [
        ("a", 10),
        ("c", 30),
    ]
    [(users, total_score, pid)] = _rows(warehouse, 'SELECT * FROM "PUBLIC"."TOTAL"')
    assert (users, total_score) == (3, 40)
    # async steps run in this process, and so does a lone sync step beside them
    in_worker = isinstance(executor, rs.Executor.Parallel) and not is_async
    assert (pid != os.getpid()) == in_worker
