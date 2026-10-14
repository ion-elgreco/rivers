"""IcebergIOHandler partitions: row filters, partition specs, partition overwrites."""

import json
import threading
from datetime import date, datetime, timedelta, timezone

import pytest

pytest.importorskip("pyiceberg")

import polars as pl  # noqa: E402
import pyarrow as pa  # noqa: E402
from pyiceberg.expressions import (  # noqa: E402
    AlwaysTrue,
    And,
    EqualTo,
    GreaterThanOrEqual,
    In,
    LessThan,
    Or,
)
from pyiceberg.expressions.visitors import bind  # noqa: E402
from pyiceberg.schema import Schema  # noqa: E402
from pyiceberg.transforms import (  # noqa: E402
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    YearTransform,
)
from pyiceberg.types import (  # noqa: E402
    DateType,
    NestedField,
    StringType,
    TimestampType,
)

import rivers as rs  # noqa: E402
from rivers.io_handlers._partition_sql import PartitionExpr  # noqa: E402
from rivers.io_handlers.iceberg._partitions import (  # noqa: E402
    partition_fields,
    partition_filter,
)

from .helpers import iceberg_handler  # noqa: E402

_DAILY = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))
_REGIONS = rs.PartitionsDefinition.static_(["us", "eu", "apac"])


def _ctx(definition, *keys) -> rs.PartitionContext:
    return rs.PartitionContext(
        keys=[
            k if isinstance(k, rs.PartitionKey) else rs.PartitionKey.single(k)
            for k in keys
        ],
        definition=definition,
    )


def _write(handler, obj, partition, meta, name="t") -> rs.OutputContext:
    ctx = rs.OutputContext(asset_name=name, partition=partition, asset_metadata=meta)
    handler.handle_output(ctx, obj)
    return ctx


def _read(handler, partition=None, meta=None, type_hint=pa.Table, name="t"):
    value = handler.load_input(
        rs.InputContext(
            asset_name=name,
            downstream_asset="d",
            partition=partition,
            asset_metadata=meta,
            type_hint=type_hint,
        )
    )
    return value.to_arrow() if isinstance(value, pl.DataFrame) else value


def _values(table: pa.Table) -> list:
    return sorted(table.column("v").to_pylist())


def _spec(handler, name="t") -> list[tuple[str, object]]:
    table = handler.load_table(name)
    schema = table.schema()
    return [
        (schema.find_column_name(f.source_id), f.transform) for f in table.spec().fields
    ]


# --- filters -----------------------------------------------------------------

_SCHEMA = Schema(
    NestedField(1, "region", StringType()),
    NestedField(2, "day", StringType()),
    NestedField(3, "d", DateType()),
    NestedField(4, "ts", TimestampType()),
)


def _range(column, start, end):
    return And(GreaterThanOrEqual(column, start), LessThan(column, end))


def test_filter_without_a_partition_is_every_row():
    assert partition_filter(None, None, _SCHEMA) == AlwaysTrue()


def test_filter_static_keys():
    expr = PartitionExpr(expr="region")
    assert partition_filter(_ctx(_REGIONS, "us"), expr, _SCHEMA) == EqualTo(
        "region", "us"
    )
    assert partition_filter(_ctx(_REGIONS, "us", "eu"), expr, _SCHEMA) == In(
        "region", ["us", "eu"]
    )


def test_filter_time_window_on_a_string_column_compares_strings():
    expr = PartitionExpr(expr="day")
    assert partition_filter(_ctx(_DAILY, "2024-01-05"), expr, _SCHEMA) == _range(
        "day", "2024-01-05", "2024-01-06"
    )


@pytest.mark.parametrize("column", ["d", "ts"])
def test_filter_time_window_on_a_temporal_column_uses_datetimes(column):
    expr = PartitionExpr(expr=column)
    assert partition_filter(_ctx(_DAILY, "2024-01-05"), expr, _SCHEMA) == _range(
        column, datetime(2024, 1, 5), datetime(2024, 1, 6)
    )


def test_filter_merges_adjacent_time_windows():
    days = _ctx(_DAILY, "2024-01-03", "2024-01-01", "2024-01-02", "2024-01-06")
    assert partition_filter(days, PartitionExpr(expr="ts"), _SCHEMA) == Or(
        _range("ts", datetime(2024, 1, 1), datetime(2024, 1, 4)),
        _range("ts", datetime(2024, 1, 6), datetime(2024, 1, 7)),
    )


def test_filter_multi_dimension_keys():
    definition = rs.PartitionsDefinition.multi({"region": _REGIONS, "day": _DAILY})
    expr = PartitionExpr(expr={"region": "region", "day": "ts"})

    def key(region, day):
        return rs.PartitionKey.multi({"region": region, "day": day})

    every_combination = _ctx(
        definition,
        *(key(r, d) for r in ("us", "eu") for d in ("2024-01-01", "2024-01-02")),
    )
    assert partition_filter(every_combination, expr, _SCHEMA) == And(
        _range("ts", datetime(2024, 1, 1), datetime(2024, 1, 3)),
        In("region", ["us", "eu"]),
    )

    some = _ctx(definition, key("us", "2024-01-01"), key("eu", "2024-01-02"))
    assert partition_filter(some, expr, _SCHEMA) == Or(
        And(
            _range("ts", datetime(2024, 1, 1), datetime(2024, 1, 2)),
            EqualTo("region", "us"),
        ),
        And(
            _range("ts", datetime(2024, 1, 2), datetime(2024, 1, 3)),
            EqualTo("region", "eu"),
        ),
    )


def test_filter_of_many_separate_windows_binds():
    hourly = rs.PartitionsDefinition.hourly(start=datetime(2024, 1, 1))
    every_other_hour = [
        (datetime(2024, 1, 1) + timedelta(hours=2 * i)).strftime("%Y-%m-%dT%H:00")
        for i in range(1200)
    ]
    expr = partition_filter(
        _ctx(hourly, *every_other_hour), PartitionExpr(expr="ts"), _SCHEMA
    )
    bind(_SCHEMA, expr, case_sensitive=True)


def test_filter_errors():
    with pytest.raises(ValueError, match="'iceberg/partition_expr'"):
        partition_filter(_ctx(_REGIONS, "us"), None, _SCHEMA)
    with pytest.raises(ValueError, match="string column name is required"):
        partition_filter(
            _ctx(_REGIONS, "us"), PartitionExpr(expr={"r": "region"}), _SCHEMA
        )


# --- partition specs for new tables -------------------------------------------


@pytest.mark.parametrize(
    "definition, key, column_type, transform",
    [
        (_REGIONS, "us", pa.string(), IdentityTransform()),
        (_DAILY, "2024-01-01", pa.string(), IdentityTransform()),
        (_DAILY, "2024-01-01", pa.date32(), DayTransform()),
        (_DAILY, "2024-01-01", pa.timestamp("us"), DayTransform()),
        (_DAILY, "2024-01-01", pa.timestamp("us", tz="UTC"), DayTransform()),
        (
            rs.PartitionsDefinition.hourly(start=datetime(2024, 1, 1)),
            "2024-01-01T05:00",
            pa.timestamp("us"),
            HourTransform(),
        ),
        (
            rs.PartitionsDefinition.hourly(start=datetime(2024, 1, 1)),
            "2024-01-01T05:00",
            pa.date32(),
            DayTransform(),
        ),
        (
            rs.PartitionsDefinition.time_window(
                start=datetime(2024, 1, 1), cron_schedule="0 0 * * 1"
            ),
            "2024-01-01T00:00:00",
            pa.timestamp("us"),
            DayTransform(),
        ),
        (
            rs.PartitionsDefinition.time_window(
                start=datetime(2024, 1, 1), cron_schedule="0 0 1 * *"
            ),
            "2024-02-01T00:00:00",
            pa.date32(),
            MonthTransform(),
        ),
        (
            rs.PartitionsDefinition.time_window(
                start=datetime(2024, 1, 1), cron_schedule="0 0 1 1 *"
            ),
            "2025-01-01T00:00:00",
            pa.timestamp("us"),
            YearTransform(),
        ),
        (
            rs.PartitionsDefinition.time_window(
                start=datetime(2024, 1, 1), interval_seconds=900
            ),
            "2024-01-01T00:15:00",
            pa.timestamp("us"),
            HourTransform(),
        ),
    ],
    ids=[
        "static",
        "daily-string",
        "daily-date",
        "daily-timestamp",
        "daily-timestamptz",
        "hourly-timestamp",
        "hourly-date",
        "weekly",
        "monthly",
        "yearly",
        "15-minutes",
    ],
)
def test_partition_fields(definition, key, column_type, transform):
    schema = pa.schema([("c", column_type), ("v", pa.int64())])
    fields = partition_fields(_ctx(definition, key), PartitionExpr(expr="c"), schema)
    assert fields == [("c", transform)]


def test_partition_fields_need_the_column_in_the_data():
    schema = pa.schema([("v", pa.int64())])
    with pytest.raises(ValueError, match="'region' is not in the data"):
        partition_fields(_ctx(_REGIONS, "us"), PartitionExpr(expr="region"), schema)


# --- partitioned writes and reads ---------------------------------------------


def test_static_partition_overwrite_and_spec(tmp_path):
    handler = iceberg_handler(tmp_path)
    meta = {"iceberg/partition_expr": "region"}

    def write(region, v):
        _write(
            handler,
            pa.table({"region": [region] * 2, "v": [v, v]}),
            _ctx(_REGIONS, region),
            meta,
        )

    write("us", 1)
    write("eu", 2)
    write("us", 3)

    assert _spec(handler) == [("region", IdentityTransform())]
    assert _values(_read(handler, _ctx(_REGIONS, "us"), meta)) == [3, 3]
    assert _values(_read(handler, _ctx(_REGIONS, "eu"), meta)) == [2, 2]
    assert _values(_read(handler)) == [2, 2, 3, 3]


@pytest.mark.parametrize(
    "column_type, value, transform",
    [
        (pa.string(), lambda day: day.isoformat(), IdentityTransform()),
        (pa.date32(), lambda day: day, DayTransform()),
        (
            pa.timestamp("us"),
            lambda day: datetime(day.year, day.month, day.day, 23, 30),
            DayTransform(),
        ),
        (
            pa.timestamp("us", tz="UTC"),
            lambda day: datetime(
                day.year, day.month, day.day, 23, 30, tzinfo=timezone.utc
            ),
            DayTransform(),
        ),
    ],
    ids=["string", "date", "timestamp", "timestamptz"],
)
@pytest.mark.parametrize("type_hint", [pa.Table, pl.DataFrame])
def test_daily_partition_overwrite(tmp_path, column_type, value, transform, type_hint):
    handler = iceberg_handler(tmp_path)
    meta = {"iceberg/partition_expr": "day"}

    def write(day, v):
        data = pa.table({"day": pa.array([value(day)], column_type), "v": [v]})
        _write(handler, data, _ctx(_DAILY, day.isoformat()), meta)

    write(date(2024, 1, 1), 1)
    write(date(2024, 1, 2), 2)
    write(date(2024, 1, 1), 3)

    assert _spec(handler) == [("day", transform)]
    first = _read(handler, _ctx(_DAILY, "2024-01-01"), meta, type_hint)
    assert _values(first) == [3]
    second = _read(handler, _ctx(_DAILY, "2024-01-02"), meta, type_hint)
    assert _values(second) == [2]
    assert _values(_read(handler)) == [2, 3]


def test_hourly_partition_overwrite(tmp_path):
    handler = iceberg_handler(tmp_path)
    hourly = rs.PartitionsDefinition.hourly(start=datetime(2024, 1, 1))
    meta = {"iceberg/partition_expr": "ts"}

    def write(hour, minutes, v):
        start = datetime(2024, 1, 1, hour)
        data = pa.table(
            {
                "ts": [start + timedelta(minutes=m) for m in minutes],
                "v": [v] * len(minutes),
            }
        )
        _write(handler, data, _ctx(hourly, f"2024-01-01T{hour:02}:00"), meta)

    write(5, [0, 30, 59], 1)
    write(6, [0], 2)
    write(5, [15], 3)

    assert _spec(handler) == [("ts", HourTransform())]
    assert _values(_read(handler)) == [2, 3]


def test_multi_dimension_partition_overwrite(tmp_path):
    handler = iceberg_handler(tmp_path)
    definition = rs.PartitionsDefinition.multi({"region": _REGIONS, "day": _DAILY})
    meta = {"iceberg/partition_expr": json.dumps({"region": "region", "day": "d"})}

    def key(region, day):
        return _ctx(definition, rs.PartitionKey.multi({"region": region, "day": day}))

    def write(region, day, v):
        data = pa.table({"region": [region], "d": [date.fromisoformat(day)], "v": [v]})
        _write(handler, data, key(region, day), meta)

    write("us", "2024-01-01", 1)
    write("eu", "2024-01-01", 2)
    write("us", "2024-01-02", 3)
    write("us", "2024-01-01", 4)

    assert sorted(_spec(handler), key=lambda f: f[0]) == [
        ("d", DayTransform()),
        ("region", IdentityTransform()),
    ]
    assert _values(_read(handler, key("us", "2024-01-01"), meta)) == [4]
    assert _values(_read(handler, key("eu", "2024-01-01"), meta)) == [2]
    assert _values(_read(handler)) == [2, 3, 4]


def test_backfill_overwrites_only_its_keys(tmp_path):
    handler = iceberg_handler(tmp_path)
    meta = {"iceberg/partition_expr": "day"}
    days = [f"2024-01-0{i}" for i in range(1, 6)]
    data = pa.table(
        {"day": pa.array([date.fromisoformat(d) for d in days]), "v": [0] * 5}
    )
    _write(handler, data, _ctx(_DAILY, *days), meta)

    backfill = _ctx(_DAILY, "2024-01-02", "2024-01-04")
    _write(
        handler,
        pa.table({"day": [date(2024, 1, 2), date(2024, 1, 4)], "v": [1, 1]}),
        backfill,
        meta,
    )

    rows = _read(handler).sort_by("day").to_pylist()
    assert [row["v"] for row in rows] == [0, 1, 0, 1, 0]
    assert _values(_read(handler, backfill, meta)) == [1, 1]


def test_partitioned_overwrite_needs_partition_expr(tmp_path):
    handler = iceberg_handler(tmp_path)
    with pytest.raises(ValueError, match="'iceberg/partition_expr'"):
        _write(
            handler, pa.table({"region": ["us"], "v": [1]}), _ctx(_REGIONS, "us"), {}
        )
    assert not handler.catalog().table_exists(("ns", "t"))


def test_partitioned_append_without_partition_expr(tmp_path):
    handler = iceberg_handler(tmp_path, mode="append")
    for region in ("us", "eu"):
        _write(
            handler,
            pa.table({"region": [region], "v": [1]}),
            _ctx(_REGIONS, region),
            {},
        )

    assert _spec(handler) == []
    assert _read(handler).num_rows == 2


def test_an_existing_table_keeps_its_spec(tmp_path):
    handler = iceberg_handler(tmp_path)
    handler.handle_output(
        rs.OutputContext(asset_name="t"),
        pa.table({"region": ["us", "eu"], "v": [0, 0]}),
    )
    meta = {"iceberg/partition_expr": "region"}
    _write(handler, pa.table({"region": ["us"], "v": [1]}), _ctx(_REGIONS, "us"), meta)

    assert _spec(handler) == []
    assert sorted(_read(handler).to_pylist(), key=lambda r: r["region"]) == [
        {"region": "eu", "v": 0},
        {"region": "us", "v": 1},
    ]


def test_reader_into_a_partitioned_table(tmp_path):
    handler = iceberg_handler(tmp_path)
    meta = {"iceberg/partition_expr": "region"}
    data = pa.table({"region": ["us", "us"], "v": [1, 2]})
    reader = pa.RecordBatchReader.from_batches(
        data.schema, data.to_batches(max_chunksize=1)
    )
    _write(handler, reader, _ctx(_REGIONS, "us"), meta)

    assert _values(_read(handler, _ctx(_REGIONS, "us"), meta)) == [1, 2]


def test_concurrent_writers_create_one_table_and_keep_every_partition(tmp_path):
    handler = iceberg_handler(
        tmp_path, table_properties={"commit.retry.num-retries": "20"}
    )
    definition = rs.PartitionsDefinition.static_([f"p{i}" for i in range(6)])
    meta = {"iceberg/partition_expr": "p"}
    barrier = threading.Barrier(6)
    errors: list[BaseException] = []

    def write(i: int) -> None:
        barrier.wait()
        try:
            data = pa.table({"p": [f"p{i}"] * 100, "v": list(range(100))})
            _write(handler, data, _ctx(definition, f"p{i}"), meta)
        except BaseException as e:
            errors.append(e)

    threads = [threading.Thread(target=write, args=(i,)) for i in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    counts = _read(handler).group_by("p").aggregate([("v", "count")]).to_pylist()
    assert sorted(counts, key=lambda r: r["p"]) == [
        {"p": f"p{i}", "v_count": 100} for i in range(6)
    ]
