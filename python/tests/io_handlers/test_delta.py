import json

import polars as pl
import pyarrow as pa
import rivers as rs
from deltalake import DeltaTable
from polars.testing import assert_frame_equal

from .helpers import (
    make_daily_partition,
    make_multi_partition,
    make_multi_partition_with_daily,
    make_partition,
)


def _make_handler(tmp_path, **kwargs):
    uri = str(tmp_path)
    return rs.DeltaIOHandler(table_uri=uri, **kwargs), uri


def test_round_trip_pyarrow(tmp_path):
    """Write a pa.Table, read back as pa.Table (default when no type_hint)."""
    handler, _ = _make_handler(tmp_path)
    table = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    ctx_out = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx_out, table)

    ctx_in = rs.InputContext(
        asset_name="tbl", downstream_asset="consumer", type_hint=pa.Table
    )
    result = handler.load_input(ctx_in)
    assert isinstance(result, pa.Table)
    assert result.cast(table.schema).equals(table)


def test_round_trip_polars(tmp_path):
    """Write a pa.Table, read back as pl.DataFrame when type_hint is pl.DataFrame."""
    handler, _ = _make_handler(tmp_path)
    table = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    ctx_out = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx_out, table)

    ctx_in = rs.InputContext(
        asset_name="tbl", downstream_asset="consumer", type_hint=pl.DataFrame
    )
    result = handler.load_input(ctx_in)
    assert isinstance(result, pl.DataFrame)
    expected = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    assert_frame_equal(result, expected)


def test_round_trip_pandas(tmp_path):
    """Write a pd.DataFrame, read back as pd.DataFrame when type_hint is pd.DataFrame."""
    import pandas as pd
    from pandas.testing import assert_frame_equal

    handler, _ = _make_handler(tmp_path)
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    ctx_out = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx_out, df)

    ctx_in = rs.InputContext(
        asset_name="tbl", downstream_asset="consumer", type_hint=pd.DataFrame
    )
    result = handler.load_input(ctx_in)
    assert isinstance(result, pd.DataFrame)
    assert_frame_equal(result, df)


def test_write_pandas_read_polars(tmp_path):
    """A pd.DataFrame output is readable back through another type handler."""
    import pandas as pd

    handler, _ = _make_handler(tmp_path)
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    handler.handle_output(rs.OutputContext(asset_name="tbl"), df)

    ctx_in = rs.InputContext(
        asset_name="tbl", downstream_asset="consumer", type_hint=pl.DataFrame
    )
    result = handler.load_input(ctx_in)
    assert isinstance(result, pl.DataFrame)
    assert_frame_equal(result, pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}))


def test_round_trip_pandas_index_dropped(tmp_path):
    """A non-trivial pandas index is not persisted as a phantom column."""
    import pandas as pd

    handler, _ = _make_handler(tmp_path)
    df = pd.DataFrame({"v": [10, 20]}, index=pd.Index(["r1", "r2"], name="label"))

    handler.handle_output(rs.OutputContext(asset_name="tbl"), df)

    ctx_in = rs.InputContext(
        asset_name="tbl", downstream_asset="consumer", type_hint=pa.Table
    )
    result = handler.load_input(ctx_in)
    assert result.column_names == ["v"]


def test_root_name_override(tmp_path):
    """root_name metadata overrides asset_name for table path."""
    handler, uri = _make_handler(tmp_path)
    table = pa.table({"a": [1, 2, 3]})

    ctx_out = rs.OutputContext(
        asset_name="my_asset", asset_metadata={"delta/root_name": "raw_events"}
    )
    handler.handle_output(ctx_out, table)

    # Table written to raw_events, not my_asset
    assert (tmp_path / "raw_events" / "_delta_log").is_dir()
    assert not (tmp_path / "my_asset").exists()

    # Read back using same root_name override
    ctx_in = rs.InputContext(
        asset_name="my_asset",
        downstream_asset="consumer",
        asset_metadata={"delta/root_name": "raw_events"},
        type_hint=pa.Table,
    )
    result = handler.load_input(ctx_in)
    assert result.column("a").to_pylist() == [1, 2, 3]


def test_creates_delta_log(tmp_path):
    """Delta log directory exists after write."""
    handler, _ = _make_handler(tmp_path)
    table = pa.table({"x": [1]})

    ctx_out = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx_out, table)

    assert (tmp_path / "tbl" / "_delta_log").is_dir()


def test_output_metadata(tmp_path):
    """Output metadata is populated after write."""
    handler, uri = _make_handler(tmp_path)
    table = pa.table({"a": [1, 2]})

    ctx = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx, table)

    meta = ctx.output_metadata
    assert meta is not None
    assert meta["delta/table_uri"].raw_value() == f"{uri}/tbl"
    assert meta["delta/mode"].raw_value() == "overwrite"
    assert isinstance(meta["delta/num_rows"], rs.MetadataValue.Int)
    assert meta["delta/num_rows"].raw_value() == 2
    assert isinstance(meta["delta/size_bytes"], rs.MetadataValue.Int)
    assert isinstance(meta["delta/write_duration_s"], rs.MetadataValue.Float)


def test_single_partition_isolation(tmp_path):
    """Two partitions of the same asset don't cross-contaminate."""
    handler, _ = _make_handler(tmp_path)
    p_a = make_partition("2024-01-01")
    p_b = make_partition("2024-01-02")

    table_a = pa.table({"date": ["2024-01-01"], "val": [10]})
    table_b = pa.table({"date": ["2024-01-02"], "val": [20]})

    ctx_out_a = rs.OutputContext(
        asset_name="daily",
        partition=p_a,
        asset_metadata={"delta/partition_expr": "date"},
    )
    handler.handle_output(ctx_out_a, table_a)

    ctx_out_b = rs.OutputContext(
        asset_name="daily",
        partition=p_b,
        asset_metadata={"delta/partition_expr": "date"},
    )
    handler.handle_output(ctx_out_b, table_b)

    dt = DeltaTable(str(tmp_path / "daily"))
    assert dt.metadata().partition_columns == ["date"]

    ctx_in_a = rs.InputContext(
        asset_name="daily",
        downstream_asset="x",
        partition=p_a,
        asset_metadata={"delta/partition_expr": "date"},
        type_hint=pa.Table,
    )
    ctx_in_b = rs.InputContext(
        asset_name="daily",
        downstream_asset="x",
        partition=p_b,
        asset_metadata={"delta/partition_expr": "date"},
        type_hint=pa.Table,
    )
    result_a = handler.load_input(ctx_in_a)
    result_b = handler.load_input(ctx_in_b)
    assert result_a.column("val").to_pylist() == [10]
    assert result_b.column("val").to_pylist() == [20]


def test_single_partition_overwrite(tmp_path):
    """Re-writing the same partition replaces data."""
    handler, _ = _make_handler(tmp_path)
    p = make_partition("2024-01-01")
    meta = {"delta/partition_expr": "date"}

    table_v1 = pa.table({"date": ["2024-01-01"], "val": [1]})
    table_v2 = pa.table({"date": ["2024-01-01"], "val": [2]})

    ctx1 = rs.OutputContext(asset_name="daily", partition=p, asset_metadata=meta)
    handler.handle_output(ctx1, table_v1)

    ctx2 = rs.OutputContext(asset_name="daily", partition=p, asset_metadata=meta)
    handler.handle_output(ctx2, table_v2)

    dt = DeltaTable(str(tmp_path / "daily"))
    assert dt.metadata().partition_columns == ["date"]

    ctx_in = rs.InputContext(
        asset_name="daily",
        downstream_asset="x",
        partition=p,
        asset_metadata=meta,
        type_hint=pa.Table,
    )
    result = handler.load_input(ctx_in)
    assert result.column("val").to_pylist() == [2]


def test_multi_partition(tmp_path):
    """Multi-dimension partition read/write."""
    import json

    handler, _ = _make_handler(tmp_path)
    p = make_multi_partition({"region": "us", "env": "prod"})
    meta = {"delta/partition_expr": json.dumps({"region": "region", "env": "env"})}

    table = pa.table({"region": ["us"], "env": ["prod"], "val": [42]})

    ctx_out = rs.OutputContext(asset_name="sales", partition=p, asset_metadata=meta)
    handler.handle_output(ctx_out, table)

    dt = DeltaTable(str(tmp_path / "sales"))
    assert sorted(dt.metadata().partition_columns) == ["env", "region"]

    ctx_in = rs.InputContext(
        asset_name="sales",
        downstream_asset="x",
        partition=p,
        asset_metadata=meta,
        type_hint=pa.Table,
    )
    result = handler.load_input(ctx_in)
    assert result.column("val").to_pylist() == [42]


def test_append_mode(tmp_path):
    """Append mode doubles rows on second write."""
    handler, _ = _make_handler(tmp_path, mode="append")
    table = pa.table({"a": [1, 2]})

    ctx1 = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx1, table)

    ctx2 = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx2, table)

    ctx_in = rs.InputContext(asset_name="tbl", downstream_asset="x", type_hint=pa.Table)
    result = handler.load_input(ctx_in)
    assert len(result) == 4


def test_per_asset_mode_override(tmp_path):
    """asset_metadata mode overrides handler default."""
    handler, _ = _make_handler(tmp_path, mode="overwrite")
    table = pa.table({"a": [1]})

    # First write
    ctx1 = rs.OutputContext(asset_name="tbl", asset_metadata={"delta/mode": "append"})
    handler.handle_output(ctx1, table)

    # Second write with append override
    ctx2 = rs.OutputContext(asset_name="tbl", asset_metadata={"delta/mode": "append"})
    handler.handle_output(ctx2, table)

    ctx_in = rs.InputContext(asset_name="tbl", downstream_asset="x", type_hint=pa.Table)
    result = handler.load_input(ctx_in)
    assert len(result) == 2  # appended, not overwritten


def test_partition_expr(tmp_path):
    """partition_expr maps dimension names to Delta column names."""
    import json

    handler, _ = _make_handler(tmp_path)
    expr = {"date_dim": "dt", "region_dim": "region"}
    meta = {"delta/partition_expr": json.dumps(expr)}

    p = make_multi_partition({"date_dim": "2024-01-01", "region_dim": "us"})
    table = pa.table({"dt": ["2024-01-01"], "region": ["us"], "val": [42]})

    ctx_out = rs.OutputContext(asset_name="events", partition=p, asset_metadata=meta)
    handler.handle_output(ctx_out, table)

    dt = DeltaTable(str(tmp_path / "events"))
    assert sorted(dt.metadata().partition_columns) == ["dt", "region"]

    # Write another partition to verify isolation
    p2 = make_multi_partition({"date_dim": "2024-01-02", "region_dim": "eu"})
    table2 = pa.table({"dt": ["2024-01-02"], "region": ["eu"], "val": [99]})
    ctx_out2 = rs.OutputContext(asset_name="events", partition=p2, asset_metadata=meta)
    handler.handle_output(ctx_out2, table2)

    ctx_in = rs.InputContext(
        asset_name="events",
        downstream_asset="x",
        partition=p,
        asset_metadata=meta,
        type_hint=pa.Table,
    )
    result = handler.load_input(ctx_in)
    assert result.column("val").to_pylist() == [42]


def test_polars_partition_read(tmp_path):
    """Polars reader via type_hint applies SQL partition filter."""
    handler, _ = _make_handler(tmp_path)
    p_a = make_partition("2024-01-01")
    p_b = make_partition("2024-01-02")
    meta = {"delta/partition_expr": "date"}

    table_a = pa.table({"date": ["2024-01-01"], "val": [10]})
    table_b = pa.table({"date": ["2024-01-02"], "val": [20]})

    ctx_out_a = rs.OutputContext(asset_name="daily", partition=p_a, asset_metadata=meta)
    handler.handle_output(ctx_out_a, table_a)

    ctx_out_b = rs.OutputContext(asset_name="daily", partition=p_b, asset_metadata=meta)
    handler.handle_output(ctx_out_b, table_b)

    dt = DeltaTable(str(tmp_path / "daily"))
    assert dt.metadata().partition_columns == ["date"]

    ctx_in = rs.InputContext(
        asset_name="daily",
        downstream_asset="x",
        partition=p_a,
        asset_metadata=meta,
        type_hint=pl.DataFrame,
    )
    result = handler.load_input(ctx_in)
    assert isinstance(result, pl.DataFrame)
    assert result["val"].to_list() == [10]


def test_daily_partition_isolation(tmp_path):
    """Daily time window partitions use range predicates for isolation."""
    handler, _ = _make_handler(tmp_path)
    p_a = make_daily_partition("2024-01-01")
    p_b = make_daily_partition("2024-01-02")
    meta = {"delta/partition_expr": "date"}

    table_a = pa.table({"date": ["2024-01-01"], "val": [10]})
    table_b = pa.table({"date": ["2024-01-02"], "val": [20]})

    ctx_out_a = rs.OutputContext(asset_name="daily", partition=p_a, asset_metadata=meta)
    handler.handle_output(ctx_out_a, table_a)

    ctx_out_b = rs.OutputContext(asset_name="daily", partition=p_b, asset_metadata=meta)
    handler.handle_output(ctx_out_b, table_b)

    dt = DeltaTable(str(tmp_path / "daily"))
    assert dt.metadata().partition_columns == ["date"]

    ctx_in_a = rs.InputContext(
        asset_name="daily",
        downstream_asset="x",
        partition=p_a,
        asset_metadata=meta,
        type_hint=pa.Table,
    )
    ctx_in_b = rs.InputContext(
        asset_name="daily",
        downstream_asset="x",
        partition=p_b,
        asset_metadata=meta,
        type_hint=pa.Table,
    )
    result_a = handler.load_input(ctx_in_a)
    result_b = handler.load_input(ctx_in_b)
    assert result_a.column("val").to_pylist() == [10]
    assert result_b.column("val").to_pylist() == [20]


def test_daily_partition_overwrite(tmp_path):
    """Re-writing a daily time window partition replaces data."""
    handler, _ = _make_handler(tmp_path)
    p = make_daily_partition("2024-01-01")
    meta = {"delta/partition_expr": "date"}

    table_v1 = pa.table({"date": ["2024-01-01"], "val": [1]})
    table_v2 = pa.table({"date": ["2024-01-01"], "val": [2]})

    ctx1 = rs.OutputContext(asset_name="daily", partition=p, asset_metadata=meta)
    handler.handle_output(ctx1, table_v1)

    ctx2 = rs.OutputContext(asset_name="daily", partition=p, asset_metadata=meta)
    handler.handle_output(ctx2, table_v2)

    dt = DeltaTable(str(tmp_path / "daily"))
    assert dt.metadata().partition_columns == ["date"]

    ctx_in = rs.InputContext(
        asset_name="daily",
        downstream_asset="x",
        partition=p,
        asset_metadata=meta,
        type_hint=pa.Table,
    )
    result = handler.load_input(ctx_in)
    assert result.column("val").to_pylist() == [2]


def test_column_selection_pyarrow(tmp_path):
    """Loading with columns metadata returns only selected columns."""
    handler, _ = _make_handler(tmp_path)
    table = pa.table({"a": [1, 2], "b": ["x", "y"], "c": [3.0, 4.0]})

    ctx_out = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx_out, table)

    ctx_in = rs.InputContext(
        asset_name="tbl",
        downstream_asset="x",
        asset_metadata={"delta/columns": '["a", "c"]'},
        type_hint=pa.Table,
    )
    result = handler.load_input(ctx_in)
    expected = pa.table({"a": [1, 2], "c": [3.0, 4.0]})
    assert result.cast(expected.schema).equals(expected)


def test_column_selection_polars(tmp_path):
    """Column selection works with polars type hint."""
    handler, _ = _make_handler(tmp_path)
    table = pa.table({"a": [1, 2], "b": ["x", "y"], "c": [3.0, 4.0]})

    ctx_out = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx_out, table)

    ctx_in = rs.InputContext(
        asset_name="tbl",
        downstream_asset="x",
        asset_metadata={"delta/columns": '["a", "b"]'},
        type_hint=pl.DataFrame,
    )
    result = handler.load_input(ctx_in)
    expected = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    assert_frame_equal(result, expected)


def test_table_versioning(tmp_path):
    """Loading a specific table version returns historical data."""
    handler, _ = _make_handler(tmp_path)

    table_v0 = pa.table({"a": [1]})
    ctx0 = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx0, table_v0)

    table_v1 = pa.table({"a": [2]})
    ctx1 = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx1, table_v1)

    # Load version 0 (first write)
    ctx_in = rs.InputContext(
        asset_name="tbl",
        downstream_asset="x",
        asset_metadata={"delta/version": "0"},
        type_hint=pa.Table,
    )
    result = handler.load_input(ctx_in)
    assert result.column("a").to_pylist() == [1]

    # Load latest (version 1)
    ctx_latest = rs.InputContext(
        asset_name="tbl", downstream_asset="x", type_hint=pa.Table
    )
    result_latest = handler.load_input(ctx_latest)
    assert result_latest.column("a").to_pylist() == [2]


def test_writer_properties(tmp_path):
    """Writer properties are passed through to write_deltalake."""
    from deltalake import WriterProperties

    wp = WriterProperties(compression="SNAPPY", max_row_group_size=100)
    handler, _ = _make_handler(tmp_path, writer_properties=wp)
    table = pa.table({"a": [1, 2, 3]})

    ctx_out = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx_out, table)

    ctx_in = rs.InputContext(asset_name="tbl", downstream_asset="x", type_hint=pa.Table)
    result = handler.load_input(ctx_in)
    assert result.column("a").to_pylist() == [1, 2, 3]


def test_commit_properties(tmp_path):
    """Commit properties with custom metadata are stored in Delta log."""
    from deltalake import CommitProperties

    cp = CommitProperties(custom_metadata={"author": "test"})
    handler, _ = _make_handler(tmp_path, commit_properties=cp)
    table = pa.table({"a": [1]})

    ctx_out = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx_out, table)

    dt = DeltaTable(str(tmp_path / "tbl"))
    assert dt.version() == 0
    history = dt.history()
    assert history[0]["author"] == "test"


def test_table_config(tmp_path):
    """Table config is applied to the Delta table."""
    handler, _ = _make_handler(tmp_path, table_config={"delta.appendOnly": "true"})
    table = pa.table({"a": [1]})

    ctx_out = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx_out, table)

    dt = DeltaTable(str(tmp_path / "tbl"))
    assert dt.metadata().configuration.get("delta.appendOnly") == "true"


def test_schema_metadata_output(tmp_path):
    """Output metadata includes schema and version."""
    handler, _ = _make_handler(tmp_path)
    table = pa.table({"a": [1], "b": ["x"]})

    ctx = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx, table)

    meta = ctx.output_metadata
    assert meta is not None
    assert isinstance(meta["delta/version"], rs.MetadataValue.Int)
    assert meta["delta/version"].raw_value() == 0
    schema = meta["rivers/schema"].raw_value()
    assert isinstance(schema, rs.Schema)
    assert schema.names == ["a", "b"]


def test_multi_partition_with_time_window(tmp_path):
    """Multi-partition with one time window dim uses range predicate for that dim."""
    handler, _ = _make_handler(tmp_path)
    meta = {"delta/partition_expr": json.dumps({"date": "date", "region": "region"})}

    p1 = make_multi_partition_with_daily(
        {"date": "2024-01-01", "region": "us"}, daily_dims={"date"}
    )
    p2 = make_multi_partition_with_daily(
        {"date": "2024-01-02", "region": "us"}, daily_dims={"date"}
    )
    table1 = pa.table({"date": ["2024-01-01"], "region": ["us"], "val": [10]})
    table2 = pa.table({"date": ["2024-01-02"], "region": ["us"], "val": [20]})

    ctx1 = rs.OutputContext(asset_name="events", partition=p1, asset_metadata=meta)
    handler.handle_output(ctx1, table1)

    ctx2 = rs.OutputContext(asset_name="events", partition=p2, asset_metadata=meta)
    handler.handle_output(ctx2, table2)

    dt = DeltaTable(str(tmp_path / "events"))
    assert sorted(dt.metadata().partition_columns) == ["date", "region"]

    ctx_in = rs.InputContext(
        asset_name="events",
        downstream_asset="x",
        partition=p1,
        asset_metadata=meta,
        type_hint=pa.Table,
    )
    result = handler.load_input(ctx_in)
    assert result.column("val").to_pylist() == [10]


def test_merge_upsert(tmp_path):
    """Merge mode with upsert updates existing rows and inserts new ones."""
    from rivers.io_handlers.delta import MergeConfig

    # First, create the table with initial data using overwrite mode
    init_handler, _ = _make_handler(tmp_path)
    initial = pa.table({"id": [1, 2], "val": [10, 20]})
    ctx_init = rs.OutputContext(asset_name="tbl")
    init_handler.handle_output(ctx_init, initial)

    # Now use merge handler for upsert
    mc = MergeConfig(merge_type="upsert", predicate="s.id = t.id")
    handler = rs.DeltaIOHandler(table_uri=str(tmp_path), mode="merge", merge_config=mc)

    # id=2 updated, id=3 inserted
    new_data = pa.table({"id": [2, 3], "val": [99, 30]})
    ctx_out = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx_out, new_data)

    ctx_in = rs.InputContext(asset_name="tbl", downstream_asset="x", type_hint=pa.Table)
    result = init_handler.load_input(ctx_in)
    expected = pa.table({"id": [1, 2, 3], "val": [10, 99, 30]})
    assert result.sort_by("id").cast(expected.schema).equals(expected)


def test_merge_deduplicate_insert(tmp_path):
    """Merge with deduplicate_insert only inserts non-matching rows."""
    from rivers.io_handlers.delta import MergeConfig

    init_handler, _ = _make_handler(tmp_path)
    initial = pa.table({"id": [1, 2], "val": [10, 20]})
    ctx_init = rs.OutputContext(asset_name="tbl")
    init_handler.handle_output(ctx_init, initial)

    mc = MergeConfig(merge_type="deduplicate_insert", predicate="s.id = t.id")
    handler = rs.DeltaIOHandler(table_uri=str(tmp_path), mode="merge", merge_config=mc)

    # id=2 exists (skip), id=3 new (insert)
    new_data = pa.table({"id": [2, 3], "val": [99, 30]})
    ctx_out = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx_out, new_data)

    ctx_in = rs.InputContext(asset_name="tbl", downstream_asset="x", type_hint=pa.Table)
    result = init_handler.load_input(ctx_in)
    expected = pa.table({"id": [1, 2, 3], "val": [10, 20, 30]})
    assert result.sort_by("id").cast(expected.schema).equals(expected)


def test_merge_update_only(tmp_path):
    """Merge with update_only only updates matched rows, no inserts."""
    from rivers.io_handlers.delta import MergeConfig

    init_handler, _ = _make_handler(tmp_path)
    initial = pa.table({"id": [1, 2], "val": [10, 20]})
    ctx_init = rs.OutputContext(asset_name="tbl")
    init_handler.handle_output(ctx_init, initial)

    mc = MergeConfig(merge_type="update_only", predicate="s.id = t.id")
    handler = rs.DeltaIOHandler(table_uri=str(tmp_path), mode="merge", merge_config=mc)

    new_data = pa.table({"id": [2, 3], "val": [99, 30]})
    ctx_out = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx_out, new_data)

    ctx_in = rs.InputContext(asset_name="tbl", downstream_asset="x", type_hint=pa.Table)
    result = init_handler.load_input(ctx_in)
    # id=2 updated, id=3 NOT inserted, id=1 unchanged
    expected = pa.table({"id": [1, 2], "val": [10, 99]})
    assert result.sort_by("id").cast(expected.schema).equals(expected)


def test_lazyframe_type_hint(tmp_path):
    """type_hint=pl.LazyFrame returns a LazyFrame without collecting."""
    handler, _ = _make_handler(tmp_path)
    table = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    ctx_out = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx_out, table)

    ctx_in = rs.InputContext(
        asset_name="tbl", downstream_asset="consumer", type_hint=pl.LazyFrame
    )
    result = handler.load_input(ctx_in)
    assert isinstance(result, pl.LazyFrame)
    expected = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    assert_frame_equal(result.collect(), expected)


def test_error_on_type_mismatch(tmp_path):
    """error_on_type_mismatch=False allows merging with mismatched types."""
    from rivers.io_handlers.delta import MergeConfig

    init_handler, _ = _make_handler(tmp_path)
    initial = pa.table({"id": pa.array([1, 2], type=pa.int64()), "val": [10, 20]})
    ctx_init = rs.OutputContext(asset_name="tbl")
    init_handler.handle_output(ctx_init, initial)

    # Source has int32 id, target has int64 — should succeed with mismatch disabled
    mc = MergeConfig(
        merge_type="upsert",
        predicate="s.id = t.id",
        error_on_type_mismatch=False,
    )
    handler = rs.DeltaIOHandler(table_uri=str(tmp_path), mode="merge", merge_config=mc)
    new_data = pa.table({"id": pa.array([2, 3], type=pa.int32()), "val": [99, 30]})
    ctx_out = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx_out, new_data)

    ctx_in = rs.InputContext(asset_name="tbl", downstream_asset="x", type_hint=pa.Table)
    result = init_handler.load_input(ctx_in)
    expected = pa.table(
        {"id": pa.array([1, 2, 3], type=pa.int64()), "val": [10, 99, 30]}
    )
    assert result.sort_by("id").equals(expected)


def test_merge_stats_metadata(tmp_path):
    """Merge operations report merge_stats and num_output_rows in output metadata."""
    from rivers.io_handlers.delta import MergeConfig

    init_handler, _ = _make_handler(tmp_path)
    initial = pa.table({"id": [1, 2], "val": [10, 20]})
    ctx_init = rs.OutputContext(asset_name="tbl")
    init_handler.handle_output(ctx_init, initial)

    mc = MergeConfig(merge_type="upsert", predicate="s.id = t.id")
    handler = rs.DeltaIOHandler(table_uri=str(tmp_path), mode="merge", merge_config=mc)
    new_data = pa.table({"id": [2, 3], "val": [99, 30]})
    ctx_out = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx_out, new_data)

    meta = ctx_out.output_metadata
    assert meta is not None
    assert "delta/merge_stats" in meta
    assert isinstance(meta["delta/num_output_rows"], rs.MetadataValue.Int)
    stats = json.loads(meta["delta/merge_stats"].raw_value())  # type: ignore[arg-type]
    assert "num_output_rows" in stats


def test_per_asset_merge_predicate(tmp_path):
    """Per-asset merge_predicate overrides MergeConfig.predicate."""
    from rivers.io_handlers.delta import MergeConfig

    init_handler, _ = _make_handler(tmp_path)
    initial = pa.table({"key": [1, 2], "val": [10, 20]})
    ctx_init = rs.OutputContext(asset_name="tbl")
    init_handler.handle_output(ctx_init, initial)

    # Handler predicate uses "id" column (wrong), but per-asset overrides to "key"
    mc = MergeConfig(merge_type="upsert", predicate="s.id = t.id")
    handler = rs.DeltaIOHandler(table_uri=str(tmp_path), mode="merge", merge_config=mc)
    new_data = pa.table({"key": [2, 3], "val": [99, 30]})
    ctx_out = rs.OutputContext(
        asset_name="tbl", asset_metadata={"delta/merge_predicate": "s.key = t.key"}
    )
    handler.handle_output(ctx_out, new_data)

    ctx_in = rs.InputContext(asset_name="tbl", downstream_asset="x", type_hint=pa.Table)
    result = init_handler.load_input(ctx_in)
    expected = pa.table({"key": [1, 2, 3], "val": [10, 99, 30]})
    assert result.sort_by("key").cast(expected.schema).equals(expected)


def test_per_asset_commit_properties_override(tmp_path):
    """Per-asset commit_properties override handler defaults."""
    handler, _ = _make_handler(tmp_path)
    table = pa.table({"a": [1]})

    ctx_out = rs.OutputContext(
        asset_name="tbl",
        asset_metadata={
            "delta/commit_properties": json.dumps(
                {"custom_metadata": {"source": "override"}}
            )
        },
    )
    handler.handle_output(ctx_out, table)

    dt = DeltaTable(str(tmp_path / "tbl"))
    history = dt.history()
    assert history[0]["source"] == "override"


def test_create_or_replace(tmp_path):
    """create_or_replace mode replaces the table schema."""
    handler, _ = _make_handler(tmp_path)

    # First write with schema {a: int, b: str}
    table_v1 = pa.table({"a": [1], "b": ["x"]})
    ctx1 = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx1, table_v1)

    dt = DeltaTable(str(tmp_path / "tbl"))
    assert set(f.name for f in pa.schema(dt.schema().to_arrow())) == {"a", "b"}

    # create_or_replace with different schema {a: int, c: float}
    cr_handler = rs.DeltaIOHandler(table_uri=str(tmp_path), mode="create_or_replace")
    table_v2 = pa.table({"a": [2], "c": [3.14]})
    ctx2 = rs.OutputContext(asset_name="tbl")
    cr_handler.handle_output(ctx2, table_v2)

    dt2 = DeltaTable(str(tmp_path / "tbl"))
    col_names = {f.name for f in pa.schema(dt2.schema().to_arrow())}
    assert col_names == {"a", "c"}

    ctx_in = rs.InputContext(asset_name="tbl", downstream_asset="x", type_hint=pa.Table)
    result = cr_handler.load_input(ctx_in)
    expected = pa.table({"a": [2], "c": [3.14]})
    assert result.cast(expected.schema).equals(expected)


def test_custom_merge(tmp_path):
    """Custom merge type with explicit operations."""
    from rivers.io_handlers.delta import (
        MergeConfig,
        MergeOperationsConfig,
        WhenMatchedUpdateAll,
        WhenNotMatchedInsertAll,
    )

    init_handler, _ = _make_handler(tmp_path)
    initial = pa.table({"id": [1, 2], "val": [10, 20]})
    ctx_init = rs.OutputContext(asset_name="tbl")
    init_handler.handle_output(ctx_init, initial)

    ops = MergeOperationsConfig(
        when_matched_update_all=[WhenMatchedUpdateAll()],
        when_not_matched_insert_all=[WhenNotMatchedInsertAll()],
    )
    mc = MergeConfig(merge_type="custom", predicate="s.id = t.id", operations=ops)
    handler = rs.DeltaIOHandler(table_uri=str(tmp_path), mode="merge", merge_config=mc)

    new_data = pa.table({"id": [2, 3], "val": [99, 30]})
    ctx_out = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx_out, new_data)

    ctx_in = rs.InputContext(asset_name="tbl", downstream_asset="x", type_hint=pa.Table)
    result = init_handler.load_input(ctx_in)
    expected = pa.table({"id": [1, 2, 3], "val": [10, 99, 30]})
    assert result.sort_by("id").cast(expected.schema).equals(expected)


def test_in_clause_partition(tmp_path):
    """Multi-value partition key generates IN clause predicate."""
    handler, _ = _make_handler(tmp_path)
    meta = {"delta/partition_expr": "region"}

    # Write three partitions
    for region in ["us", "eu", "ap"]:
        p = make_partition(region)
        table = pa.table(
            {"region": [region], "val": [{"us": 1, "eu": 2, "ap": 3}[region]]}
        )
        ctx = rs.OutputContext(asset_name="data", partition=p, asset_metadata=meta)
        handler.handle_output(ctx, table)

    dt = DeltaTable(str(tmp_path / "data"))
    assert dt.metadata().partition_columns == ["region"]

    # Load with multi-value key (list)
    p_multi = make_partition(["us", "eu"])
    ctx_in = rs.InputContext(
        asset_name="data",
        downstream_asset="x",
        partition=p_multi,
        asset_metadata=meta,
        type_hint=pa.Table,
    )
    result = handler.load_input(ctx_in)
    expected = pa.table({"region": ["eu", "us"], "val": [2, 1]})
    assert result.cast(expected.schema).sort_by("region").equals(expected)


def test_multi_value_partition_write(tmp_path):
    """Writing with a multi-value partition key writes all values at once."""
    handler, _ = _make_handler(tmp_path)
    meta = {"delta/partition_expr": "region"}

    # Write multiple partitions in a single call
    p = make_partition(["us", "eu"])
    table = pa.table({"region": ["us", "eu"], "val": [1, 2]})
    ctx = rs.OutputContext(asset_name="data", partition=p, asset_metadata=meta)
    handler.handle_output(ctx, table)

    dt = DeltaTable(str(tmp_path / "data"))
    assert dt.metadata().partition_columns == ["region"]

    # Read back all data
    result = dt.to_pyarrow_table()
    expected = pa.table({"region": ["eu", "us"], "val": [2, 1]})
    assert result.cast(expected.schema).sort_by("region").equals(expected)

    # Read back with the same multi-value key
    ctx_in = rs.InputContext(
        asset_name="data",
        downstream_asset="x",
        partition=p,
        asset_metadata=meta,
        type_hint=pa.Table,
    )
    result = handler.load_input(ctx_in)
    assert result.cast(expected.schema).sort_by("region").equals(expected)


def test_date_type_time_window_partition(tmp_path):
    """Date-typed partition column works with daily time window partitioning."""
    import datetime

    handler, _ = _make_handler(tmp_path)
    meta = {"delta/partition_expr": "date"}

    # Write two daily partitions with date-typed column
    for day in ["2024-01-01", "2024-01-02"]:
        p = make_daily_partition(day)
        d = datetime.date.fromisoformat(day)
        table = pa.table(
            {
                "date": pa.array([d], type=pa.date32()),
                "value": [10 if day == "2024-01-01" else 20],
            }
        )
        ctx = rs.OutputContext(asset_name="dated", partition=p, asset_metadata=meta)
        handler.handle_output(ctx, table)

    dt = DeltaTable(str(tmp_path / "dated"))
    assert dt.metadata().partition_columns == ["date"]

    # Read back first partition
    p1 = make_daily_partition("2024-01-01")
    ctx_in = rs.InputContext(
        asset_name="dated",
        downstream_asset="x",
        partition=p1,
        asset_metadata=meta,
        type_hint=pa.Table,
    )
    result = handler.load_input(ctx_in)
    expected = pa.table(
        {
            "date": pa.array([datetime.date(2024, 1, 1)], type=pa.date32()),
            "value": [10],
        }
    )
    assert result.equals(expected)

    # Read back all data
    all_data = dt.to_pyarrow_table()
    assert len(all_data) == 2


def test_int_partition_column(tmp_path):
    """Integer-typed static partition column is written and read correctly."""
    handler, _ = _make_handler(tmp_path)
    meta = {"delta/partition_expr": "year"}

    for year in [2023, 2024]:
        p = make_partition(str(year))
        table = pa.table(
            {"year": pa.array([year], type=pa.int32()), "val": [year * 10]}
        )
        ctx = rs.OutputContext(asset_name="by_year", partition=p, asset_metadata=meta)
        handler.handle_output(ctx, table)

    dt = DeltaTable(str(tmp_path / "by_year"))
    assert dt.metadata().partition_columns == ["year"]

    # Read back one partition
    p1 = make_partition("2024")
    ctx_in = rs.InputContext(
        asset_name="by_year",
        downstream_asset="x",
        partition=p1,
        asset_metadata=meta,
        type_hint=pa.Table,
    )
    result = handler.load_input(ctx_in)
    expected = pa.table({"year": pa.array([2024], type=pa.int32()), "val": [20240]})
    assert result.cast(expected.schema).equals(expected)

    # Verify all data round-trips
    all_data = dt.to_pyarrow_table()
    assert len(all_data) == 2
    all_expected = pa.table(
        {
            "year": pa.array([2023, 2024], type=pa.int32()),
            "val": [20230, 20240],
        }
    )
    assert all_data.cast(all_expected.schema).sort_by("year").equals(all_expected)


# ---------------------------------------------------------------------------
# Backfill predicate tests
# ---------------------------------------------------------------------------


def test_backfill_predicate_single_static_multi_keys():
    """Multiple static partition keys produce an IN predicate."""
    from rivers.io_handlers.delta.config import PartitionExpr
    from rivers.io_handlers.delta.predicate import _build_predicate

    pd = rs.PartitionsDefinition.static_(["a", "b", "c", "d"])
    ctx = rs.PartitionContext(
        keys=[
            rs.PartitionKey.single("a"),
            rs.PartitionKey.single("b"),
            rs.PartitionKey.single("c"),
        ],
        definition=pd,
    )
    pred = _build_predicate(ctx, PartitionExpr(expr="region"))
    assert pred == "region IN ('a', 'b', 'c')"


def test_backfill_predicate_single_key_equals():
    """Single key still produces simple equality."""
    from rivers.io_handlers.delta.config import PartitionExpr
    from rivers.io_handlers.delta.predicate import _build_predicate

    pd = rs.PartitionsDefinition.static_(["x"])
    ctx = rs.PartitionContext(
        keys=[rs.PartitionKey.single("x")],
        definition=pd,
    )
    pred = _build_predicate(ctx, PartitionExpr(expr="col"))
    assert pred == "col = 'x'"


def test_backfill_predicate_time_window_multi_keys():
    """Multiple time window keys produce OR of range predicates."""
    from datetime import datetime

    from rivers.io_handlers.delta.config import PartitionExpr
    from rivers.io_handlers.delta.predicate import _build_predicate

    pd = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))
    ctx = rs.PartitionContext(
        keys=[
            rs.PartitionKey.single("2024-01-15"),
            rs.PartitionKey.single("2024-01-16"),
        ],
        definition=pd,
    )
    pred = _build_predicate(ctx, PartitionExpr(expr="date"))
    assert pred == (
        "(date >= '2024-01-15' AND date < '2024-01-16') OR "
        "(date >= '2024-01-16' AND date < '2024-01-17')"
    )


def test_backfill_predicate_multi_partition_keys_non_cartesian():
    """Non-cartesian multi-dimension keys produce OR of AND predicates."""
    from rivers.io_handlers.delta.config import PartitionExpr
    from rivers.io_handlers.delta.predicate import _build_predicate

    pd = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
        }
    )
    # (us,free) and (eu,pro) is NOT a cartesian product of {us,eu}×{free,pro}
    # because (us,pro) and (eu,free) are missing → must use OR
    ctx = rs.PartitionContext(
        keys=[
            rs.PartitionKey.multi({"region": "us", "tier": "free"}),
            rs.PartitionKey.multi({"region": "eu", "tier": "pro"}),
        ],
        definition=pd,
    )
    pred = _build_predicate(
        ctx, PartitionExpr(expr={"region": "region", "tier": "tier"})
    )
    assert pred == (
        "(region = 'us' AND tier = 'free') OR (region = 'eu' AND tier = 'pro')"
    )


def test_backfill_predicate_multi_partition_keys_cartesian():
    """Cartesian product of multi-dimension keys produces factored AND + IN."""
    from rivers.io_handlers.delta.config import PartitionExpr
    from rivers.io_handlers.delta.predicate import _build_predicate

    pd = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
        }
    )
    # Full cartesian product: {us,eu} × {free,pro} = 4 keys
    # → factored: region IN ('us', 'eu') AND tier IN ('free', 'pro')
    ctx = rs.PartitionContext(
        keys=[
            rs.PartitionKey.multi({"region": "us", "tier": "free"}),
            rs.PartitionKey.multi({"region": "us", "tier": "pro"}),
            rs.PartitionKey.multi({"region": "eu", "tier": "free"}),
            rs.PartitionKey.multi({"region": "eu", "tier": "pro"}),
        ],
        definition=pd,
    )
    pred = _build_predicate(
        ctx, PartitionExpr(expr={"region": "region", "tier": "tier"})
    )
    assert pred == "region IN ('us', 'eu') AND tier IN ('free', 'pro')"


def test_backfill_predicate_multi_fixed_dimension():
    """PerDimension-style: one fixed dim + varying dim → equality AND IN."""
    from rivers.io_handlers.delta.config import PartitionExpr
    from rivers.io_handlers.delta.predicate import _build_predicate

    pd = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu", "apac"]),
            "date": rs.PartitionsDefinition.static_(["d1", "d2", "d3"]),
        }
    )
    # PerDimension(multi_run=["region"], single_run=["date"]) for region=us:
    # 3 keys all sharing region=us, different dates
    ctx = rs.PartitionContext(
        keys=[
            rs.PartitionKey.multi({"region": "us", "date": "d1"}),
            rs.PartitionKey.multi({"region": "us", "date": "d2"}),
            rs.PartitionKey.multi({"region": "us", "date": "d3"}),
        ],
        definition=pd,
    )
    pred = _build_predicate(
        ctx, PartitionExpr(expr={"region": "region_col", "date": "date_col"})
    )
    assert pred == "date_col IN ('d1', 'd2', 'd3') AND region_col = 'us'"


def test_backfill_predicate_multi_three_dims_partial_cartesian():
    """3 dimensions, full cartesian on 2 dims + 1 fixed → factored."""
    from rivers.io_handlers.delta.config import PartitionExpr
    from rivers.io_handlers.delta.predicate import _build_predicate

    pd = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us"]),
            "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
            "env": rs.PartitionsDefinition.static_(["staging", "prod"]),
        }
    )
    # region fixed=us, tier×env full product = 4 keys
    ctx = rs.PartitionContext(
        keys=[
            rs.PartitionKey.multi({"region": "us", "tier": "free", "env": "staging"}),
            rs.PartitionKey.multi({"region": "us", "tier": "free", "env": "prod"}),
            rs.PartitionKey.multi({"region": "us", "tier": "pro", "env": "staging"}),
            rs.PartitionKey.multi({"region": "us", "tier": "pro", "env": "prod"}),
        ],
        definition=pd,
    )
    pred = _build_predicate(
        ctx,
        PartitionExpr(expr={"region": "region", "tier": "tier", "env": "env"}),
    )
    assert (
        pred
        == "env IN ('staging', 'prod') AND region = 'us' AND tier IN ('free', 'pro')"
    )


def test_backfill_multi_key_write_single_key_read(tmp_path):
    """Write 3 partitions individually, read back one at a time."""
    handler, _ = _make_handler(tmp_path)
    pd = rs.PartitionsDefinition.static_(["a", "b", "c"])

    for pk_val, val in [("a", 10), ("b", 20), ("c", 30)]:
        ctx_out = rs.OutputContext(
            asset_name="scores",
            partition=rs.PartitionContext(
                keys=[rs.PartitionKey.single(pk_val)], definition=pd
            ),
            asset_metadata={"delta/partition_expr": "category"},
        )
        handler.handle_output(ctx_out, pa.table({"category": [pk_val], "score": [val]}))

    # Read single partition "b"
    ctx_in = rs.InputContext(
        asset_name="scores",
        downstream_asset="x",
        partition=rs.PartitionContext(
            keys=[rs.PartitionKey.single("b")], definition=pd
        ),
        asset_metadata={"delta/partition_expr": "category"},
        type_hint=pa.Table,
    )
    result = handler.load_input(ctx_in)
    expected = pa.table({"category": ["b"], "score": [20]})
    assert result.cast(expected.schema).equals(expected)


def test_backfill_multi_key_write_multi_key_read(tmp_path):
    """Write 3 partitions individually, read back 2 with multi-key context."""
    handler, _ = _make_handler(tmp_path)
    pd = rs.PartitionsDefinition.static_(["a", "b", "c"])

    for pk_val, val in [("a", 10), ("b", 20), ("c", 30)]:
        ctx_out = rs.OutputContext(
            asset_name="scores",
            partition=rs.PartitionContext(
                keys=[rs.PartitionKey.single(pk_val)], definition=pd
            ),
            asset_metadata={"delta/partition_expr": "category"},
        )
        handler.handle_output(ctx_out, pa.table({"category": [pk_val], "score": [val]}))

    # Read partitions "a" and "c" together (simulating SingleRun backfill read)
    ctx_in = rs.InputContext(
        asset_name="scores",
        downstream_asset="x",
        partition=rs.PartitionContext(
            keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("c"),
            ],
            definition=pd,
        ),
        asset_metadata={"delta/partition_expr": "category"},
        type_hint=pa.Table,
    )
    result = handler.load_input(ctx_in)
    expected = pa.table({"category": ["a", "c"], "score": [10, 30]})
    result_casted = result.cast(expected.schema)
    assert result_casted.sort_by("category").equals(expected)


def test_backfill_multi_dim_write_and_read(tmp_path):
    """Write multi-dimension partitions, read back a subset with multi-key context."""
    handler, _ = _make_handler(tmp_path)
    pd = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
        }
    )
    expr = {"region": "region", "tier": "tier"}

    # Write all 4 combos
    for region, tier, val in [
        ("us", "free", 1),
        ("us", "pro", 2),
        ("eu", "free", 3),
        ("eu", "pro", 4),
    ]:
        ctx_out = rs.OutputContext(
            asset_name="metrics",
            partition=rs.PartitionContext(
                keys=[rs.PartitionKey.multi({"region": region, "tier": tier})],
                definition=pd,
            ),
            asset_metadata={"delta/partition_expr": json.dumps(expr)},
        )
        handler.handle_output(
            ctx_out,
            pa.table({"region": [region], "tier": [tier], "val": [val]}),
        )

    # Read back us×{free,pro} (PerDimension style: fixed region, all tiers)
    ctx_in = rs.InputContext(
        asset_name="metrics",
        downstream_asset="x",
        partition=rs.PartitionContext(
            keys=[
                rs.PartitionKey.multi({"region": "us", "tier": "free"}),
                rs.PartitionKey.multi({"region": "us", "tier": "pro"}),
            ],
            definition=pd,
        ),
        asset_metadata={"delta/partition_expr": json.dumps(expr)},
        type_hint=pa.Table,
    )
    result = handler.load_input(ctx_in)
    expected = pa.table(
        {
            "region": ["us", "us"],
            "tier": ["free", "pro"],
            "val": [1, 2],
        }
    )
    result_casted = result.cast(expected.schema)
    assert result_casted.sort_by("tier").equals(expected.sort_by("tier"))


def test_backfill_overwrite_only_targeted_partitions(tmp_path):
    """Writing with multi-key context overwrites only those partitions, not others."""
    handler, _ = _make_handler(tmp_path)
    pd = rs.PartitionsDefinition.static_(["a", "b", "c"])

    # Initial write: all 3 partitions with value=1
    for pk_val in ["a", "b", "c"]:
        ctx_out = rs.OutputContext(
            asset_name="data",
            partition=rs.PartitionContext(
                keys=[rs.PartitionKey.single(pk_val)], definition=pd
            ),
            asset_metadata={"delta/partition_expr": "key"},
        )
        handler.handle_output(ctx_out, pa.table({"key": [pk_val], "val": [1]}))

    # Overwrite partitions "a" and "b" with value=99 (simulating backfill re-run)
    for pk_val in ["a", "b"]:
        ctx_out = rs.OutputContext(
            asset_name="data",
            partition=rs.PartitionContext(
                keys=[rs.PartitionKey.single(pk_val)], definition=pd
            ),
            asset_metadata={"delta/partition_expr": "key"},
        )
        handler.handle_output(ctx_out, pa.table({"key": [pk_val], "val": [99]}))

    # Read all 3 partitions: "a" and "b" should be 99, "c" should still be 1
    results = {}
    for pk_val in ["a", "b", "c"]:
        ctx_in = rs.InputContext(
            asset_name="data",
            downstream_asset="x",
            partition=rs.PartitionContext(
                keys=[rs.PartitionKey.single(pk_val)], definition=pd
            ),
            asset_metadata={"delta/partition_expr": "key"},
            type_hint=pa.Table,
        )
        tbl = handler.load_input(ctx_in)
        results[pk_val] = tbl.column("val").to_pylist()[0]

    assert results["a"] == 99, "partition 'a' should have been overwritten"
    assert results["b"] == 99, "partition 'b' should have been overwritten"
    assert results["c"] == 1, "partition 'c' should NOT have been overwritten"


def test_backfill_merge_scoped_to_partition(tmp_path):
    """Merge with partition predicate only affects the targeted partition.

    Setup: table with (region, id, val) partitioned by region.
    Initial: us=[1:10, 2:20], eu=[3:30]
    Merge upsert on us partition: id=2→99, id=4→40 (new)
    Result: us=[1:10, 2:99, 4:40], eu=[3:30] (untouched)
    """
    from rivers.io_handlers.delta import MergeConfig

    pd = rs.PartitionsDefinition.static_(["us", "eu"])

    # Initial write: overwrite mode
    init_handler, _ = _make_handler(tmp_path)
    ctx_us = rs.OutputContext(
        asset_name="sales",
        partition=rs.PartitionContext(
            keys=[rs.PartitionKey.single("us")], definition=pd
        ),
        asset_metadata={"delta/partition_expr": "region"},
    )
    init_handler.handle_output(
        ctx_us, pa.table({"region": ["us", "us"], "id": [1, 2], "val": [10, 20]})
    )
    ctx_eu = rs.OutputContext(
        asset_name="sales",
        partition=rs.PartitionContext(
            keys=[rs.PartitionKey.single("eu")], definition=pd
        ),
        asset_metadata={"delta/partition_expr": "region"},
    )
    init_handler.handle_output(
        ctx_eu, pa.table({"region": ["eu"], "id": [3], "val": [30]})
    )

    # Merge upsert scoped to "us" partition only
    mc = MergeConfig(merge_type="upsert", predicate="s.id = t.id")
    merge_handler = rs.DeltaIOHandler(
        table_uri=str(tmp_path), mode="merge", merge_config=mc
    )
    ctx_merge = rs.OutputContext(
        asset_name="sales",
        partition=rs.PartitionContext(
            keys=[rs.PartitionKey.single("us")], definition=pd
        ),
        asset_metadata={"delta/partition_expr": "region"},
    )
    merge_handler.handle_output(
        ctx_merge, pa.table({"region": ["us", "us"], "id": [2, 4], "val": [99, 40]})
    )

    # Read "us": should have id=1 (unchanged), id=2 (updated), id=4 (inserted)
    ctx_in_us = rs.InputContext(
        asset_name="sales",
        downstream_asset="x",
        partition=rs.PartitionContext(
            keys=[rs.PartitionKey.single("us")], definition=pd
        ),
        asset_metadata={"delta/partition_expr": "region"},
        type_hint=pa.Table,
    )
    result_us = init_handler.load_input(ctx_in_us)
    expected_us = pa.table(
        {"region": ["us", "us", "us"], "id": [1, 2, 4], "val": [10, 99, 40]}
    )
    assert result_us.cast(expected_us.schema).sort_by("id").equals(expected_us)

    # Read "eu": should be completely untouched
    ctx_in_eu = rs.InputContext(
        asset_name="sales",
        downstream_asset="x",
        partition=rs.PartitionContext(
            keys=[rs.PartitionKey.single("eu")], definition=pd
        ),
        asset_metadata={"delta/partition_expr": "region"},
        type_hint=pa.Table,
    )
    result_eu = init_handler.load_input(ctx_in_eu)
    expected_eu = pa.table({"region": ["eu"], "id": [3], "val": [30]})
    assert result_eu.cast(expected_eu.schema).equals(expected_eu)


def test_backfill_merge_multi_key_partition_scope(tmp_path):
    """Merge with multi-key context scopes to those partitions only.

    Initial: us=[1:10], eu=[2:20], apac=[3:30]
    Merge upsert on [us, eu]: id=1→99, id=2→88, id=5→50 (new)
    Result: us=[1:99, 5:50], eu=[2:88, 5:50], apac=[3:30] (untouched)

    Note: the merge inserts id=5 into both us and eu since the merge
    source covers both partitions.
    """
    from rivers.io_handlers.delta import MergeConfig

    pd = rs.PartitionsDefinition.static_(["us", "eu", "apac"])

    init_handler, _ = _make_handler(tmp_path)
    for region, id_val, val in [("us", 1, 10), ("eu", 2, 20), ("apac", 3, 30)]:
        ctx_out = rs.OutputContext(
            asset_name="metrics",
            partition=rs.PartitionContext(
                keys=[rs.PartitionKey.single(region)], definition=pd
            ),
            asset_metadata={"delta/partition_expr": "region"},
        )
        init_handler.handle_output(
            ctx_out, pa.table({"region": [region], "id": [id_val], "val": [val]})
        )

    # Merge upsert scoped to [us, eu] via multi-key partition context
    mc = MergeConfig(merge_type="upsert", predicate="s.id = t.id")
    merge_handler = rs.DeltaIOHandler(
        table_uri=str(tmp_path), mode="merge", merge_config=mc
    )
    ctx_merge = rs.OutputContext(
        asset_name="metrics",
        partition=rs.PartitionContext(
            keys=[
                rs.PartitionKey.single("us"),
                rs.PartitionKey.single("eu"),
            ],
            definition=pd,
        ),
        asset_metadata={"delta/partition_expr": "region"},
    )
    merge_handler.handle_output(
        ctx_merge,
        pa.table({"region": ["us", "eu", "us"], "id": [1, 2, 5], "val": [99, 88, 50]}),
    )

    # Read apac: should be completely untouched
    ctx_in_apac = rs.InputContext(
        asset_name="metrics",
        downstream_asset="x",
        partition=rs.PartitionContext(
            keys=[rs.PartitionKey.single("apac")], definition=pd
        ),
        asset_metadata={"delta/partition_expr": "region"},
        type_hint=pa.Table,
    )
    result_apac = init_handler.load_input(ctx_in_apac)
    expected_apac = pa.table({"region": ["apac"], "id": [3], "val": [30]})
    assert result_apac.cast(expected_apac.schema).equals(expected_apac)
