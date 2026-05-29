"""DataFusion type handler for the Delta Lake IO handler."""

import gc

import pyarrow as pa
from datafusion import DataFrame, SessionContext

import rivers as rs

from .helpers import make_partition


def _make_handler(tmp_path, **kwargs):
    return rs.DeltaIOHandler(table_uri=str(tmp_path), **kwargs), str(tmp_path)


def _datafusion_df(data: dict) -> DataFrame:
    """Build a datafusion DataFrame from column data."""
    return SessionContext().from_arrow(pa.table(data))


def test_round_trip_datafusion(tmp_path):
    """Write a datafusion DataFrame, read back as datafusion DataFrame."""
    handler, _ = _make_handler(tmp_path)
    df = _datafusion_df({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    ctx_out = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx_out, df)

    ctx_in = rs.InputContext(
        asset_name="tbl", downstream_asset="consumer", type_hint=DataFrame
    )
    result = handler.load_input(ctx_in)
    assert isinstance(result, DataFrame)
    # DataFusion emits Utf8View; cast to the source schema before comparing.
    expected = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    assert pa.table(result).cast(expected.schema).equals(expected)


def test_write_datafusion_read_pyarrow(tmp_path):
    """A datafusion DataFrame output is readable back through another handler."""
    handler, _ = _make_handler(tmp_path)
    df = _datafusion_df({"a": [1, 2], "b": ["x", "y"]})

    handler.handle_output(rs.OutputContext(asset_name="tbl"), df)

    ctx_in = rs.InputContext(asset_name="tbl", downstream_asset="x", type_hint=pa.Table)
    result = handler.load_input(ctx_in)
    assert isinstance(result, pa.Table)
    expected = pa.table({"a": [1, 2], "b": ["x", "y"]})
    assert result.cast(expected.schema).equals(expected)


def test_lazy_frame_survives_context_gc(tmp_path):
    """The returned lazy frame outlives the local SessionContext (FFI keep-alive)."""
    handler, _ = _make_handler(tmp_path)
    handler.handle_output(
        rs.OutputContext(asset_name="tbl"), _datafusion_df({"a": [1, 2, 3]})
    )

    result = handler.load_input(
        rs.InputContext(asset_name="tbl", downstream_asset="x", type_hint=DataFrame)
    )
    gc.collect()  # force-drop any unreferenced SessionContext
    # Without the keep-alive this raises "TaskContextProvider went out of scope".
    assert pa.table(result).sort_by("a").to_pydict() == {"a": [1, 2, 3]}


def test_session_context_reusable(tmp_path):
    """The frame exposes its SessionContext as ``rivers_ctx`` for reuse."""
    handler, _ = _make_handler(tmp_path)
    handler.handle_output(
        rs.OutputContext(asset_name="tbl"),
        _datafusion_df({"id": [1, 2], "v": [10, 20]}),
    )

    result = handler.load_input(
        rs.InputContext(asset_name="tbl", downstream_asset="x", type_hint=DataFrame)
    )
    ctx = result.rivers_ctx
    assert isinstance(ctx, SessionContext)

    # The loaded table is registered under its asset name ("tbl"); register
    # another table and join them by name in the same session.
    ctx.register_table("extra", _datafusion_df({"id": [1, 2], "w": [100, 200]}))
    joined = ctx.sql("SELECT tbl.id, v, w FROM tbl JOIN extra USING (id)")
    assert pa.table(joined).sort_by("id").to_pydict() == {
        "id": [1, 2],
        "v": [10, 20],
        "w": [100, 200],
    }


def test_two_inputs_join_distinct_qualifiers(tmp_path):
    """Two DataFusion-backed inputs join without table-qualifier collisions."""
    h1 = rs.DeltaIOHandler(table_uri=str(tmp_path / "h1"))
    h2 = rs.DeltaIOHandler(table_uri=str(tmp_path / "h2"))
    h1.handle_output(
        rs.OutputContext(asset_name="users"),
        _datafusion_df({"id": [1, 2], "v": [10, 20]}),
    )
    h2.handle_output(
        rs.OutputContext(asset_name="orders"),
        _datafusion_df({"id": [1, 2], "total": [99, 88]}),
    )

    users = h1.load_input(
        rs.InputContext(asset_name="users", downstream_asset="d", type_hint=DataFrame)
    )
    orders = h2.load_input(
        rs.InputContext(asset_name="orders", downstream_asset="d", type_hint=DataFrame)
    )

    # Independent contexts join fine; distinct asset-name qualifiers avoid the
    # "duplicate qualified field name" collision a shared table name would cause.
    joined = users.join(orders, on="id", how="inner")
    assert sorted(joined.to_pylist(), key=lambda r: r["id"]) == [
        {"id": 1, "v": 10, "total": 99},
        {"id": 2, "v": 20, "total": 88},
    ]


def test_column_selection_datafusion(tmp_path):
    """delta/columns projects only the selected columns into the scan."""
    handler, _ = _make_handler(tmp_path)
    df = _datafusion_df({"a": [1, 2], "b": ["x", "y"], "c": [3.0, 4.0]})

    handler.handle_output(rs.OutputContext(asset_name="tbl"), df)

    ctx_in = rs.InputContext(
        asset_name="tbl",
        downstream_asset="x",
        asset_metadata={"delta/columns": '["a", "c"]'},
        type_hint=DataFrame,
    )
    result = handler.load_input(ctx_in)
    out = pa.table(result).sort_by("a")
    assert out.column_names == ["a", "c"]
    assert out.to_pydict() == {"a": [1, 2], "c": [3.0, 4.0]}


def test_partition_read_datafusion(tmp_path):
    """The datafusion reader applies the partition predicate."""
    handler, _ = _make_handler(tmp_path)
    p_a = make_partition("2024-01-01")
    p_b = make_partition("2024-01-02")
    meta = {"delta/partition_expr": "date"}

    handler.handle_output(
        rs.OutputContext(asset_name="daily", partition=p_a, asset_metadata=meta),
        _datafusion_df({"date": ["2024-01-01"], "val": [10]}),
    )
    handler.handle_output(
        rs.OutputContext(asset_name="daily", partition=p_b, asset_metadata=meta),
        _datafusion_df({"date": ["2024-01-02"], "val": [20]}),
    )

    ctx_in = rs.InputContext(
        asset_name="daily",
        downstream_asset="x",
        partition=p_a,
        asset_metadata=meta,
        type_hint=DataFrame,
    )
    result = handler.load_input(ctx_in)
    assert pa.table(result).to_pydict()["val"] == [10]


def test_table_versioning_datafusion(tmp_path):
    """delta/version time-travels the datafusion reader."""
    handler, _ = _make_handler(tmp_path)
    handler.handle_output(
        rs.OutputContext(asset_name="tbl"), _datafusion_df({"a": [1]})
    )
    handler.handle_output(
        rs.OutputContext(asset_name="tbl"), _datafusion_df({"a": [2]})
    )

    ctx_in = rs.InputContext(
        asset_name="tbl",
        downstream_asset="x",
        asset_metadata={"delta/version": "0"},
        type_hint=DataFrame,
    )
    result = handler.load_input(ctx_in)
    assert pa.table(result).to_pydict() == {"a": [1]}


def test_append_mode_datafusion(tmp_path):
    """Append mode accumulates rows across datafusion writes."""
    handler, _ = _make_handler(tmp_path, mode="append")
    df = _datafusion_df({"a": [1, 2]})

    handler.handle_output(rs.OutputContext(asset_name="tbl"), df)
    handler.handle_output(rs.OutputContext(asset_name="tbl"), df)

    result = handler.load_input(
        rs.InputContext(asset_name="tbl", downstream_asset="x", type_hint=DataFrame)
    )
    assert pa.table(result).num_rows == 4


def test_write_streams_multiple_batches(tmp_path):
    """A multi-batch DataFusion frame round-trips all rows via execute_stream."""
    handler, _ = _make_handler(tmp_path)
    ctx = SessionContext()
    df = ctx.sql("SELECT value AS i FROM range(0, 100000)")  # spans several batches

    handler.handle_output(rs.OutputContext(asset_name="big"), df)

    result = handler.load_input(
        rs.InputContext(asset_name="big", downstream_asset="x", type_hint=DataFrame)
    )
    assert pa.table(result).num_rows == 100000
