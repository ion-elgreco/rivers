"""SnowflakeIOHandler against a fakesnow server: types, partitions, transactions, executors.

fakesnow runs Snowflake SQL on DuckDB. Its server mode takes the real
connector's requests, PUT uploads included, so loky workers reach it too.
"""

import json
import os
import re
from contextlib import nullcontext
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


def _write_region(
    handler, region: str, values: list | pa.Array, definition=_REGIONS
) -> None:
    handler.handle_output(
        rs.OutputContext(
            asset_name="t",
            asset_metadata=_BY_REGION,
            partition=_partition(definition, rs.PartitionKey.single(region)),
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
    ("results", "columns", "creates"),
    [
        ([[("ID", "NUMBER", 38, 0)]], {"ID": io_module.Column("NUMBER", 38, 0)}, []),
        ([[], [(1,)]], {}, []),
        ([[], [(0,)]], {}, ['CREATE SCHEMA IF NOT EXISTS "RAW"']),
    ],
    ids=["table", "schema", "neither"],
)
def test_create_statements_only_run_for_missing_objects(results, columns, creates):
    """A role without the create privilege can still write to existing objects."""
    cur = RecordingCursor(*results)
    output = pa.schema([("id", pa.int64())])
    assert io_module._prepare_table(cur, "RAW", "T", output) == columns
    assert [sql for sql in cur.sql if sql.startswith("CREATE")] == creates


@pytest.mark.parametrize(
    ("column", "arrow_type", "holds"),
    [
        (("NUMBER", 38, 0), pa.int8(), True),
        (("NUMBER", 10, 2), pa.decimal128(12, 1), True),
        (("NUMBER", 10, 2), pa.decimal128(10, 3), False),
        (("NUMBER", 38, 0), pa.float64(), False),
        (("NUMBER", 38, 0), pa.bool_(), False),
        (("FLOAT", None, None), pa.int64(), False),
        (("FLOAT", None, None), pa.null(), True),
        (("TEXT", None, None), pa.large_string(), True),
        (("TEXT", None, None), pa.float64(), False),
        (("BINARY", None, None), pa.string(), False),
        (("DATE", None, None), pa.timestamp("us"), False),
        (("TIME", None, None), pa.time32("s"), True),
        (("TIMESTAMP_NTZ", None, None), pa.timestamp("ns"), True),
        (("TIMESTAMP_NTZ", None, None), pa.timestamp("us", tz="UTC"), False),
        (("TIMESTAMP_LTZ", None, None), pa.timestamp("ms", tz="UTC"), True),
        (("TIMESTAMP_LTZ", None, None), pa.timestamp("us"), False),
        (("TIMESTAMP_TZ", None, None), pa.timestamp("us", tz="UTC"), True),
        (("ARRAY", None, None), pa.list_(pa.int64()), True),
        (("OBJECT", None, None), pa.list_(pa.int64()), False),
        (("OBJECT", None, None), pa.map_(pa.string(), pa.int64()), True),
        (("VARIANT", None, None), pa.float64(), True),
    ],
)
def test_column_types_that_hold_the_output(column, arrow_type, holds):
    """With the types that Snowflake reports: fakesnow reports TIMESTAMP_LTZ
    columns as TIMESTAMP_TZ, and ARRAY and OBJECT columns as VARIANT."""
    cur = RecordingCursor([("V", *column)])
    output = pa.schema([("v", arrow_type)])
    check = (
        nullcontext()
        if holds
        else pytest.raises(ValueError, match="table RAW.T has column v ")
    )
    with check:
        assert io_module._prepare_table(cur, "RAW", "T", output)


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
    """A value too large for the column fails the COPY, after the delete."""
    handler = SnowflakeIOHandler(resource=warehouse)
    _write_region(handler, "us", pa.array([1, 2], pa.decimal128(3, 0)))
    with pytest.raises(ProgrammingError, match="out of range"):
        _write_region(handler, "us", pa.array([1234], pa.decimal128(4, 0)))
    assert _rows(warehouse, 'SELECT "REGION", "V" FROM "PUBLIC"."T" ORDER BY "V"') == [
        ("us", 1),
        ("us", 2),
    ]


@pytest.mark.parametrize(
    ("first", "then", "types"),
    [
        pytest.param(
            pa.array([10, 20]),
            pa.array([10.5, 20.25]),
            "NUMBER(38,0) (output FLOAT)",
            id="floats-into-integers",
        ),
        pytest.param(
            pa.array([None, None]),
            pa.array([0.5, 0.25]),
            "TEXT (output FLOAT)",
            id="floats-after-nulls",
        ),
        pytest.param(
            pa.array([Decimal("1.25")], pa.decimal128(10, 2)),
            pa.array([Decimal("1.125")], pa.decimal128(10, 3)),
            "NUMBER(10,2) (output NUMBER(10,3))",
            id="larger-scale",
        ),
    ],
)
def test_type_that_changes_values_fails_and_keeps_the_rows(
    warehouse, first, then, types
):
    """COPY would round the floats and decimals, or store the floats as text."""
    handler = SnowflakeIOHandler(resource=warehouse)
    ctx = rs.OutputContext(asset_name="t")
    handler.handle_output(ctx, pa.table({"price": first}))

    with pytest.raises(
        ValueError, match=re.escape(f"table PUBLIC.T has column price {types}")
    ):
        handler.handle_output(ctx, pa.table({"price": then}))

    assert _rows(warehouse, 'SELECT "PRICE" FROM "PUBLIC"."T" ORDER BY 1') == [
        (value,) for value in first.to_pylist()
    ]


def test_types_that_keep_the_values_load(warehouse):
    """Wider integers and decimals load, and so do nulls into a column of any type."""
    handler = SnowflakeIOHandler(resource=warehouse)
    ctx = rs.OutputContext(asset_name="t")
    handler.handle_output(
        ctx,
        pa.table(
            {
                "n": pa.array([1], pa.int32()),
                "price": pa.array([Decimal("1.25")], pa.decimal128(10, 2)),
                "ratio": [0.5],
            }
        ),
    )

    handler.handle_output(
        ctx,
        pa.table(
            {
                "n": pa.array([2**40], pa.int64()),
                "price": pa.array([Decimal("2.5")], pa.decimal128(10, 1)),
                "ratio": [None],
            }
        ),
    )

    assert _rows(warehouse, 'SELECT "N", "PRICE", "RATIO" FROM "PUBLIC"."T"') == [
        (2**40, Decimal("2.50"), None)
    ]


@pytest.mark.parametrize(
    ("output", "match"),
    [
        pytest.param(
            pa.table({"region": ["eu"], "v": [2], "extra": ["x"]}),
            "table PUBLIC.T has no column extra",
            id="missing-column",
        ),
        pytest.param(
            pa.table({"region": ["eu"], "v": [2.5]}),
            re.escape("table PUBLIC.T has column v NUMBER(38,0) (output FLOAT)"),
            id="type-change",
        ),
    ],
)
def test_table_created_during_the_upload_is_checked(
    warehouse, monkeypatch, output, match
):
    """Two first writes: the other one creates the table while this one uploads."""
    handler = SnowflakeIOHandler(resource=warehouse)
    others = [lambda: _write_region(handler, "us", [1])]
    parquet_files = io_module._parquet_files

    def racing(reader):
        if others:
            others.pop()()
        yield from parquet_files(reader)

    monkeypatch.setattr(io_module, "_parquet_files", racing)
    with pytest.raises(ValueError, match=match):
        handler.handle_output(
            rs.OutputContext(
                asset_name="t",
                asset_metadata=_BY_REGION,
                partition=_partition(_REGIONS, rs.PartitionKey.single("eu")),
            ),
            output,
        )

    assert _rows(warehouse, 'SELECT * FROM "PUBLIC"."T"') == [("us", 1)]


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


_ODD_KEYS = ["us", "a\\b", "us\\", "x\\' OR 1=1 --"]


@pytest.mark.parametrize(
    "key", _ODD_KEYS[1:], ids=["backslash", "trailing-backslash", "injection"]
)
def test_keys_with_backslashes_select_only_their_rows(warehouse, key):
    """Snowflake reads backslash escapes in string literals."""
    handler = SnowflakeIOHandler(resource=warehouse)
    definition = rs.PartitionsDefinition.static_(_ODD_KEYS)
    handler.handle_output(
        rs.OutputContext(asset_name="t"),
        pa.table({"region": _ODD_KEYS, "v": list(range(len(_ODD_KEYS)))}),
    )

    _write_region(handler, key, [9], definition)

    expected = {region: v for v, region in enumerate(_ODD_KEYS)} | {key: 9}
    assert sorted(_rows(warehouse, 'SELECT "REGION", "V" FROM "PUBLIC"."T"')) == sorted(
        expected.items()
    )
    for region, v in expected.items():
        read = _load(
            handler,
            "t",
            pa.Table,
            asset_metadata=_BY_REGION,
            partition=_partition(definition, rs.PartitionKey.single(region)),
        )
        assert read.to_pylist() == [{"region": region, "v": v}]


@pytest.mark.parametrize("process_wide", [False, True], ids=["resource", "process"])
def test_writes_with_qmark_paramstyle(warehouse, monkeypatch, process_wide):
    """The handler binds its own lookups, whatever paramstyle the resource or process sets."""
    if process_wide:
        monkeypatch.setattr("snowflake.connector.paramstyle", "qmark")
    else:
        args = {**warehouse.connection_args, "paramstyle": "qmark"}
        warehouse = SnowflakeResource(database=warehouse.database, connection_args=args)
    handler = SnowflakeIOHandler(resource=warehouse)

    _write_region(handler, "us", [1, 2])
    _write_region(handler, "eu", [3])
    _write_region(handler, "us", [4])

    assert _load(handler, "t", pa.Table).sort_by("v").to_pylist() == [
        {"region": "eu", "v": 3},
        {"region": "us", "v": 4},
    ]


@pytest.mark.parametrize("fmt", ["%Y-%m-%d", "%Y%m%d"])
def test_daily_partition_overwrite_on_a_date_column(warehouse, fmt):
    """As text, Snowflake would read the key 20240101 as seconds since 1970."""
    handler = SnowflakeIOHandler(resource=warehouse)
    definition = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1), fmt=fmt)
    meta = {"snowflake/partition_expr": "day"}

    def partition(day: date) -> rs.PartitionContext:
        return _partition(definition, rs.PartitionKey.single(day.strftime(fmt)))

    def write(day: date, value: int) -> None:
        handler.handle_output(
            rs.OutputContext(
                asset_name="t", asset_metadata=meta, partition=partition(day)
            ),
            pa.table({"day": [day], "v": [value]}),
        )

    jan1, jan2, jan3 = (date(2024, 1, day) for day in (1, 2, 3))
    write(jan1, 1)
    write(jan2, 2)
    write(jan3, 3)
    write(jan2, 4)

    assert _rows(warehouse, 'SELECT "DAY", "V" FROM "PUBLIC"."T" ORDER BY "DAY"') == [
        (jan1, 1),
        (jan2, 4),
        (jan3, 3),
    ]
    read = _load(handler, "t", pa.Table, asset_metadata=meta, partition=partition(jan2))
    assert read.to_pylist() == [{"day": jan2, "v": 4}]


@pytest.mark.parametrize("tz", [None, "UTC"], ids=["timestamp_ntz", "timestamp_ltz"])
def test_day_first_keys_on_a_timestamp_column(warehouse, tz):
    """As text, Snowflake would read the key 05/01/2024 (Jan 5) as May 1."""
    handler = SnowflakeIOHandler(resource=warehouse)
    definition = rs.PartitionsDefinition.daily(
        start=datetime(2024, 1, 1), fmt="%d/%m/%Y"
    )
    meta = {"snowflake/partition_expr": "ts"}
    jan5 = _partition(definition, rs.PartitionKey.single("05/01/2024"))

    def rows(*stamps: datetime) -> pa.Table:
        return pa.table(
            {
                "ts": pa.array(stamps, pa.timestamp("us", tz=tz)),
                "v": [stamp.month * 100 + stamp.day for stamp in stamps],
            }
        )

    may = [datetime(2024, 5, 1, 10), datetime(2024, 5, 2, 10)]
    handler.handle_output(
        rs.OutputContext(asset_name="t"), rows(datetime(2024, 1, 5, 10), *may)
    )
    handler.handle_output(
        rs.OutputContext(asset_name="t", asset_metadata=meta, partition=jan5),
        rows(datetime(2024, 1, 5, 12)),
    )

    read = _load(handler, "t", pl.DataFrame).sort("ts")
    assert read.select(pl.col("ts").dt.strftime("%m-%d %H:%M"), "v").rows() == [
        ("01-05 12:00", 105),
        ("05-01 10:00", 501),
        ("05-02 10:00", 502),
    ]
    read = _load(handler, "t", pl.DataFrame, asset_metadata=meta, partition=jan5)
    assert read["v"].to_list() == [105]


def test_first_write_types_the_bounds_of_the_table_it_creates(warehouse, monkeypatch):
    """The statement itself: fakesnow casts no bounds on an empty table, where
    Snowflake fails on the text 13/01/2024 (month 13)."""
    deletes = []
    write = io_module._write

    def recording(cur, target, stage, delete):
        deletes.append(delete)
        write(cur, target, stage, delete)

    monkeypatch.setattr(io_module, "_write", recording)
    handler = SnowflakeIOHandler(resource=warehouse)
    definition = rs.PartitionsDefinition.daily(
        start=datetime(2024, 1, 1), fmt="%d/%m/%Y"
    )

    handler.handle_output(
        rs.OutputContext(
            asset_name="t",
            asset_metadata={"snowflake/partition_expr": "day"},
            partition=_partition(definition, rs.PartitionKey.single("13/01/2024")),
        ),
        pa.table({"day": [date(2024, 1, 13)]}),
    )

    fmt = "'YYYY-MM-DD HH24:MI:SS.FF6'"
    assert deletes == [
        'DELETE FROM "PUBLIC"."T" WHERE '
        f"day >= TO_TIMESTAMP_NTZ('2024-01-13 00:00:00.000000', {fmt}) "
        f"AND day < TO_TIMESTAMP_NTZ('2024-01-14 00:00:00.000000', {fmt})"
    ]
    assert _rows(warehouse, 'SELECT "DAY" FROM "PUBLIC"."T"') == [(date(2024, 1, 13),)]


@pytest.mark.parametrize(
    ("expr", "days"),
    [
        pytest.param("day", ["20240104", "20240105", "20240106"], id="varchar-column"),
        pytest.param(
            "TO_VARCHAR(TO_DATE(day), 'YYYYMMDD')",
            [date(2024, 1, day) for day in (4, 5, 6)],
            id="expression",
        ),
    ],
)
def test_text_partitions_compare_keys_in_their_format(warehouse, expr, days):
    """A text column or an expression gets the keys as text, in the partition's format."""
    handler = SnowflakeIOHandler(resource=warehouse)
    definition = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1), fmt="%Y%m%d")
    meta = {"snowflake/partition_expr": expr}
    jan5 = _partition(definition, rs.PartitionKey.single("20240105"))
    handler.handle_output(
        rs.OutputContext(asset_name="t"), pa.table({"day": days, "v": [4, 5, 6]})
    )

    handler.handle_output(
        rs.OutputContext(asset_name="t", asset_metadata=meta, partition=jan5),
        pa.table({"day": days[1:2], "v": [50]}),
    )

    assert _rows(warehouse, 'SELECT "DAY", "V" FROM "PUBLIC"."T" ORDER BY "DAY"') == [
        (days[0], 4),
        (days[1], 50),
        (days[2], 6),
    ]
    read = _load(handler, "t", pa.Table, asset_metadata=meta, partition=jan5)
    assert read.to_pylist() == [{"day": days[1], "v": 50}]


def test_partition_bounds_on_date_and_timestamp_columns_are_typed():
    """The SQL itself: fakesnow fails on a key such as 20240105 as a date, where
    Snowflake reads seconds since 1970."""
    resource = SnowflakeResource(account="unreachable", user="u", password="p")
    handler = SnowflakeIOHandler(resource=resource)
    definition = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1), fmt="%Y%m%d")
    partition = _partition(definition, rs.PartitionKey.single("20240105"))
    columns = {
        "DAY": io_module.Column("DATE", None, None),
        "TS": io_module.Column("TIMESTAMP_LTZ", None, None),
        "KEY": io_module.Column("TEXT", None, None),
    }

    def predicate(expr: str, columns=columns) -> str:
        meta = {"snowflake/partition_expr": expr}
        return handler.partition_predicate(meta, partition, columns)

    def stamp(day: int) -> str:
        return (
            f"TO_TIMESTAMP_NTZ('2024-01-{day:02} 00:00:00.000000', "
            "'YYYY-MM-DD HH24:MI:SS.FF6')"
        )

    for expr in ("day", "Ts"):
        assert predicate(expr) == f"{expr} >= {stamp(5)} AND {expr} < {stamp(6)}"
    assert predicate("key") == "key >= '20240105' AND key < '20240106'"
    assert predicate("TO_VARCHAR(day, 'YYYYMMDD')") == (
        "TO_VARCHAR(day, 'YYYYMMDD') >= '20240105' "
        "AND TO_VARCHAR(day, 'YYYYMMDD') < '20240106'"
    )
    assert predicate("day", columns=None) == "day >= '20240105' AND day < '20240106'"


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


def test_a_key_with_several_days_reads_each_day(warehouse):
    """Mapped reads (specific_partitions, multi_to_single) pass one key with several days."""
    handler = SnowflakeIOHandler(resource=warehouse)
    at_ten = [datetime(2024, 1, day, 10) for day in (1, 2, 3)]
    handler.handle_output(
        rs.OutputContext(asset_name="t"),
        pa.table({"region": ["us", "eu", "us"], "ts": at_ten}),
    )
    daily = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))
    days = ["2024-01-01", "2024-01-02"]

    by_day = _load(
        handler,
        "t",
        pa.Table,
        asset_metadata={"snowflake/partition_expr": "ts"},
        partition=_partition(daily, rs.PartitionKey.single(days)),
    )
    us = _load(
        handler,
        "t",
        pa.Table,
        asset_metadata={
            "snowflake/partition_expr": json.dumps({"region": "region", "date": "ts"})
        },
        partition=_partition(
            rs.PartitionsDefinition.multi({"region": _REGIONS, "date": daily}),
            rs.PartitionKey.multi({"region": "us", "date": days}),
        ),
    )

    assert by_day.sort_by("ts").to_pylist() == [
        {"region": "us", "ts": at_ten[0]},
        {"region": "eu", "ts": at_ten[1]},
    ]
    assert us.to_pylist() == [{"region": "us", "ts": at_ten[0]}]


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


def test_partitioned_write_and_read_need_partition_expr():
    """Both fail before connecting."""
    resource = SnowflakeResource(account="unreachable", user="u", password="p")
    handler = SnowflakeIOHandler(resource=resource)
    partition = _partition(_REGIONS, rs.PartitionKey.single("us"))

    with pytest.raises(ValueError, match="'snowflake/partition_expr' metadata key"):
        handler.handle_output(
            rs.OutputContext(asset_name="t", partition=partition), _TABLE
        )
    with pytest.raises(ValueError, match="'snowflake/partition_expr' metadata key"):
        _load(handler, "t", pa.Table, partition=partition)


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
