"""IcebergIOHandler: types, output metadata, write modes, schema evolution, names, reads."""

import sys
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyiceberg")

import pandas as pd  # noqa: E402
import polars as pl  # noqa: E402
import pyarrow as pa  # noqa: E402
from pyiceberg.exceptions import NoSuchTableError  # noqa: E402
from pyiceberg.table import Transaction  # noqa: E402

import rivers as rs  # noqa: E402
from rivers.io_handlers.iceberg import UpsertConfig  # noqa: E402

from .helpers import iceberg_handler, read_input, write_output  # noqa: E402

_TABLE = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
_ROWS = _TABLE.to_pylist()


def _rows(value) -> list[dict]:
    """``value`` as a list of rows, ordered by ``id``."""
    if isinstance(value, pa.RecordBatchReader):
        value = value.read_all()
    if isinstance(value, pl.LazyFrame):
        value = value.collect()
    if isinstance(value, pd.DataFrame):
        rows = value.to_dict("records")
    elif isinstance(value, pa.Table):
        rows = value.to_pylist()
    else:
        rows = value.to_dicts()
    return sorted(rows, key=lambda row: row["id"])


@pytest.mark.parametrize(
    "make",
    [
        lambda: _TABLE,
        lambda: pa.RecordBatchReader.from_batches(
            _TABLE.schema, _TABLE.to_batches(max_chunksize=1)
        ),
        lambda: pl.from_arrow(_TABLE),
        lambda: pl.from_arrow(_TABLE).lazy(),
        lambda: _TABLE.to_pandas(),
    ],
    ids=[
        "pa.Table",
        "pa.RecordBatchReader",
        "pl.DataFrame",
        "pl.LazyFrame",
        "pd.DataFrame",
    ],
)
def test_write_each_type(tmp_path, make):
    handler = iceberg_handler(tmp_path)
    write_output(handler, make())
    assert _rows(read_input(handler)) == _ROWS


@pytest.mark.parametrize(
    "type_hint",
    [pa.Table, pa.RecordBatchReader, pl.DataFrame, pl.LazyFrame, pd.DataFrame],
)
def test_read_each_type(tmp_path, type_hint):
    handler = iceberg_handler(tmp_path)
    write_output(handler, _TABLE)
    value = read_input(handler, type_hint)
    assert isinstance(value, type_hint)
    assert _rows(value) == _ROWS


def test_pandas_index_is_not_written(tmp_path):
    handler = iceberg_handler(tmp_path)
    df = pd.DataFrame({"v": [10, 20]}, index=pd.Index(["r1", "r2"], name="label"))
    write_output(handler, df)
    assert read_input(handler).column_names == ["v"]


def test_reader_streams_into_an_unpartitioned_table(tmp_path, monkeypatch):
    handler = iceberg_handler(tmp_path)
    written: list[type] = []
    append = Transaction.append

    def spy(self, df, *args, **kwargs):
        written.append(type(df))
        return append(self, df, *args, **kwargs)

    monkeypatch.setattr(Transaction, "append", spy)
    batches = (batch for batch in _TABLE.to_batches(max_chunksize=1))
    write_output(handler, pa.RecordBatchReader.from_batches(_TABLE.schema, batches))

    assert written == [pa.RecordBatchReader]
    assert _rows(read_input(handler)) == _ROWS


def test_output_metadata_and_data_version(tmp_path):
    handler = iceberg_handler(tmp_path)
    ctx = write_output(handler, _TABLE)
    table = handler.load_table("t")
    snapshot = table.current_snapshot()
    meta = ctx.output_metadata

    assert meta["iceberg/table"].raw_value() == "ns.t"
    assert meta["iceberg/mode"].raw_value() == "overwrite"
    assert meta["iceberg/snapshot_id"].raw_value() == snapshot.snapshot_id
    assert meta["iceberg/num_rows"].raw_value() == 3
    assert isinstance(meta["iceberg/size_bytes"], rs.MetadataValue.Int)
    assert meta["iceberg/size_bytes"].raw_value() > 0
    assert isinstance(meta["iceberg/write_duration_s"], rs.MetadataValue.Float)
    assert meta["iceberg/metadata_location"].raw_value() == table.metadata_location
    assert isinstance(meta["rivers/schema"], rs.MetadataValue.Schema)
    assert ctx.drain_data_version() == str(snapshot.snapshot_id)

    appended = write_output(
        handler, _TABLE.slice(0, 1), asset_metadata={"iceberg/mode": "append"}
    )
    assert appended.output_metadata["iceberg/num_rows"].raw_value() == 4
    assert appended.drain_data_version() != str(snapshot.snapshot_id)


def test_namespace_and_table_overrides(tmp_path):
    handler = iceberg_handler(tmp_path)
    meta = {"iceberg/namespace": "lake", "iceberg/table": "events_v2"}
    write_output(handler, _TABLE, name="events", asset_metadata=meta)

    catalog = handler.pyiceberg_catalog()
    assert catalog.table_exists(("lake", "events_v2"))
    assert not catalog.table_exists(("ns", "events"))
    assert _rows(read_input(handler, name="events", asset_metadata=meta)) == _ROWS


def test_dots_in_the_namespace_nest_it(tmp_path):
    handler = iceberg_handler(tmp_path, namespace="lake.raw")
    assert handler.table_identifier("events") == ("lake", "raw", "events")
    assert handler.table_identifier("events", {"iceberg/namespace": "ops"}) == (
        "ops",
        "events",
    )


@pytest.mark.parametrize(
    "name",
    [
        "raw.events",
        "graph/node",
        pytest.param(
            "a:b",
            marks=pytest.mark.skipif(
                sys.platform == "win32", reason="Windows paths cannot contain ':'"
            ),
        ),
    ],
)
def test_asset_names_are_kept_as_table_names(tmp_path, name):
    handler = iceberg_handler(tmp_path)
    write_output(handler, _TABLE, name=name)
    assert handler.load_table(name).name() == ("ns", name)
    assert _rows(read_input(handler, name=name)) == _ROWS


def test_overwrite_replaces_the_table(tmp_path):
    handler = iceberg_handler(tmp_path)
    write_output(handler, _TABLE)
    write_output(handler, pa.table({"id": [9], "name": ["z"]}))
    assert _rows(read_input(handler)) == [{"id": 9, "name": "z"}]


def test_append_mode_and_per_asset_override(tmp_path):
    handler = iceberg_handler(tmp_path, mode="append")
    write_output(handler, _TABLE)
    write_output(handler, _TABLE)
    assert read_input(handler).num_rows == 6

    write_output(handler, _TABLE, asset_metadata={"iceberg/mode": "overwrite"})
    assert _rows(read_input(handler)) == _ROWS


@pytest.mark.parametrize(
    "key, value",
    [("iceberg/mode", "merge"), ("iceberg/schema_mode", "union")],
)
def test_unknown_modes_are_rejected(tmp_path, key, value):
    handler = iceberg_handler(tmp_path)
    with pytest.raises(ValueError, match=f"{key} must be one of"):
        write_output(handler, _TABLE, asset_metadata={key: value})


def test_upsert_updates_and_inserts(tmp_path):
    handler = iceberg_handler(
        tmp_path, mode="upsert", upsert_config=UpsertConfig(join_cols=["id"])
    )
    write_output(handler, _TABLE)
    ctx = write_output(handler, pa.table({"id": [2, 4], "name": ["B", "d"]}))

    assert _rows(read_input(handler)) == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "B"},
        {"id": 3, "name": "c"},
        {"id": 4, "name": "d"},
    ]
    meta = ctx.output_metadata
    assert meta["iceberg/mode"].raw_value() == "upsert"
    assert meta["iceberg/rows_updated"].raw_value() == 1
    assert meta["iceberg/rows_inserted"].raw_value() == 1


def test_upsert_join_cols_from_metadata_and_insert_only(tmp_path):
    handler = iceberg_handler(
        tmp_path,
        mode="upsert",
        upsert_config=UpsertConfig(when_matched_update_all=False),
    )
    meta = {"iceberg/join_cols": '["id"]'}
    write_output(handler, _TABLE, asset_metadata=meta)
    write_output(
        handler, pa.table({"id": [2, 4], "name": ["B", "d"]}), asset_metadata=meta
    )

    assert _rows(read_input(handler)) == [*_ROWS, {"id": 4, "name": "d"}]


def test_upsert_without_changes_keeps_the_data_version(tmp_path):
    handler = iceberg_handler(
        tmp_path, mode="upsert", upsert_config=UpsertConfig(join_cols=["id"])
    )
    version = write_output(handler, _TABLE).drain_data_version()

    again = write_output(handler, _TABLE)
    assert again.drain_data_version() == version
    assert again.output_metadata["iceberg/rows_updated"].raw_value() == 0
    assert again.output_metadata["iceberg/rows_inserted"].raw_value() == 0


def test_upsert_needs_join_columns(tmp_path):
    handler = iceberg_handler(tmp_path, mode="upsert")
    with pytest.raises(ValueError, match="Join columns could not be found"):
        write_output(handler, _TABLE)


def test_schema_mode_merge_adds_columns(tmp_path):
    handler = iceberg_handler(tmp_path, mode="append", schema_mode="merge")
    write_output(handler, _TABLE)
    write_output(handler, pa.table({"id": [4], "name": ["d"], "score": [1.5]}))

    rows = _rows(read_input(handler))
    assert rows[0] == {"id": 1, "name": "a", "score": None}
    assert rows[-1] == {"id": 4, "name": "d", "score": 1.5}


def test_schema_mode_overwrite_drops_missing_columns(tmp_path):
    handler = iceberg_handler(tmp_path, schema_mode="overwrite")
    write_output(handler, _TABLE)
    write_output(handler, pa.table({"id": [7], "score": [0.5]}))

    assert handler.load_table("t").schema().column_names == ["id", "score"]
    assert _rows(read_input(handler)) == [{"id": 7, "score": 0.5}]


def test_a_new_column_without_schema_mode_fails(tmp_path):
    handler = iceberg_handler(tmp_path)
    write_output(handler, _TABLE)
    with pytest.raises(ValueError, match="more columns"):
        write_output(handler, pa.table({"id": [4], "name": ["d"], "score": [1.5]}))


def test_schema_mode_overwrite_rejects_append(tmp_path):
    handler = iceberg_handler(tmp_path, mode="append", schema_mode="overwrite")
    with pytest.raises(ValueError, match="needs mode 'overwrite'"):
        write_output(handler, _TABLE)


def test_nanosecond_and_zoned_timestamps_become_utc_microseconds(tmp_path):
    handler = iceberg_handler(tmp_path)
    df = pd.DataFrame(
        {
            "zoned": [
                pd.Timestamp("2024-01-01 12:00:00.123456789", tz="Europe/Amsterdam")
            ],
            "naive": [pd.Timestamp("2024-01-01 12:00:00.123456789")],
        }
    )
    write_output(handler, df)

    table = read_input(handler)
    assert table.schema.field("zoned").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("naive").type == pa.timestamp("us")
    assert table.to_pylist() == [
        {
            "zoned": datetime(2024, 1, 1, 11, 0, 0, 123456, tzinfo=timezone.utc),
            "naive": datetime(2024, 1, 1, 12, 0, 0, 123456),
        }
    ]


def test_reader_with_nanosecond_timestamps(tmp_path):
    handler = iceberg_handler(tmp_path)
    data = pa.table({"ts": pa.array([1_000, 2_000], pa.timestamp("ns"))})
    write_output(
        handler, pa.RecordBatchReader.from_batches(data.schema, data.to_batches())
    )

    ts = read_input(handler).column("ts")
    assert ts.type == pa.timestamp("us")
    assert ts.to_pylist() == [
        datetime(1970, 1, 1, 0, 0, 0, 1),
        datetime(1970, 1, 1, 0, 0, 0, 2),
    ]


@pytest.mark.parametrize("type_hint", [pl.DataFrame, pl.LazyFrame])
def test_polars_reads_with_the_storage_properties(tmp_path, monkeypatch, type_hint):
    handler = iceberg_handler(tmp_path)
    storage = {"s3.endpoint": "http://localhost:9000", "s3.region": "eu-west-1"}
    handler = handler.model_copy(
        update={"catalog_properties": {**handler.catalog_properties, **storage}}
    )
    write_output(handler, _TABLE)
    seen: dict[str, str] = {}
    scan_iceberg = pl.scan_iceberg

    def spy(table, **kwargs):
        seen.update(kwargs["storage_options"])
        return scan_iceberg(table, **kwargs)

    monkeypatch.setattr(pl, "scan_iceberg", spy)

    assert _rows(read_input(handler, type_hint)) == _ROWS
    assert seen == storage


@pytest.mark.parametrize(
    "type_hint", [pa.Table, pl.DataFrame, pl.LazyFrame, pd.DataFrame]
)
def test_column_projection(tmp_path, type_hint):
    handler = iceberg_handler(tmp_path)
    write_output(handler, _TABLE)
    value = read_input(
        handler, type_hint, asset_metadata={"iceberg/columns": '["name"]'}
    )
    if isinstance(value, pl.LazyFrame):
        value = value.collect()
    names = value.column_names if isinstance(value, pa.Table) else list(value.columns)
    assert names == ["name"]


@pytest.mark.parametrize("type_hint", [pa.Table, pl.DataFrame, pd.DataFrame])
def test_snapshot_id_reads_an_older_snapshot(tmp_path, type_hint):
    handler = iceberg_handler(tmp_path)
    first = (
        write_output(handler, _TABLE).output_metadata["iceberg/snapshot_id"].raw_value()
    )
    write_output(handler, pa.table({"id": [9], "name": ["z"]}))

    old = read_input(
        handler, type_hint, asset_metadata={"iceberg/snapshot_id": str(first)}
    )
    assert _rows(old) == _ROWS
    assert _rows(read_input(handler, type_hint)) == [{"id": 9, "name": "z"}]


def test_table_properties_on_create_and_on_change(tmp_path):
    handler = iceberg_handler(
        tmp_path, table_properties={"write.parquet.compression-codec": "zstd"}
    )
    write_output(
        handler,
        _TABLE,
        asset_metadata={"iceberg/table_properties": '{"owner": "data"}'},
    )
    properties = handler.load_table("t").properties
    assert properties["write.parquet.compression-codec"] == "zstd"
    assert properties["owner"] == "data"

    write_output(
        handler, _TABLE, asset_metadata={"iceberg/table_properties": '{"owner": "ml"}'}
    )
    assert handler.load_table("t").properties["owner"] == "ml"


def test_snapshot_properties_are_in_the_summary(tmp_path):
    handler = iceberg_handler(tmp_path, snapshot_properties={"producer": "rivers"})
    write_output(
        handler,
        _TABLE,
        asset_metadata={"iceberg/snapshot_properties": '{"batch": "b1"}'},
    )
    summary = handler.load_table("t").current_snapshot().summary
    assert summary["producer"] == "rivers"
    assert summary["batch"] == "b1"


def test_load_input_needs_a_type_hint(tmp_path):
    handler = iceberg_handler(tmp_path)
    with pytest.raises(TypeError, match="No type_hint"):
        handler.load_input(rs.InputContext(asset_name="t", downstream_asset="d"))


def test_unsupported_types_are_rejected(tmp_path):
    handler = iceberg_handler(tmp_path)
    with pytest.raises(TypeError, match="No type handler found"):
        write_output(handler, {"id": [1]})


def test_reading_a_missing_table_raises(tmp_path):
    handler = iceberg_handler(tmp_path)
    with pytest.raises(NoSuchTableError):
        read_input(handler)
