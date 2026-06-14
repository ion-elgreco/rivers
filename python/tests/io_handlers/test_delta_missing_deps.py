"""Tests that helpful error messages are raised when optional deps are missing."""

from unittest.mock import patch

import pytest

import rivers as rs


def _handler_without_extras(tmp_path):
    """Create a DeltaIOHandler with no type handlers loaded."""
    handler = rs.DeltaIOHandler(table_uri=str(tmp_path))
    return handler


def test_missing_pyarrow_output_error(tmp_path):
    """Writing a pyarrow Table without pyarrow suggests rivers[pyarrow]."""
    import pyarrow as pa

    handler = _handler_without_extras(tmp_path)
    table = pa.table({"a": [1]})
    ctx = rs.OutputContext(asset_name="tbl")

    with patch.object(
        type(handler), "type_handlers", staticmethod(lambda *args, **kwargs: {})
    ):
        with pytest.raises(TypeError, match=r"pip install rivers\[pyarrow\]"):
            handler.handle_output(ctx, table)


def test_missing_pyarrow_input_error(tmp_path):
    """Loading as pyarrow Table without pyarrow suggests rivers[pyarrow]."""
    import pyarrow as pa

    handler = _handler_without_extras(tmp_path)
    ctx = rs.InputContext(asset_name="tbl", downstream_asset="x", type_hint=pa.Table)

    with patch.object(
        type(handler), "type_handlers", staticmethod(lambda *args, **kwargs: {})
    ):
        with pytest.raises(TypeError, match=r"pip install rivers\[pyarrow\]"):
            handler.load_input(ctx)


def test_missing_polars_output_error(tmp_path):
    """Writing a polars DataFrame without polars suggests rivers[polars]."""
    import polars as pl

    handler = _handler_without_extras(tmp_path)
    df = pl.DataFrame({"a": [1]})
    ctx = rs.OutputContext(asset_name="tbl")

    with patch.object(
        type(handler), "type_handlers", staticmethod(lambda *args, **kwargs: {})
    ):
        with pytest.raises(TypeError, match=r"pip install rivers\[polars\]"):
            handler.handle_output(ctx, df)


def test_missing_polars_input_error(tmp_path):
    """Loading as polars DataFrame without polars suggests rivers[polars]."""
    import polars as pl

    handler = _handler_without_extras(tmp_path)
    ctx = rs.InputContext(
        asset_name="tbl", downstream_asset="x", type_hint=pl.DataFrame
    )

    with patch.object(
        type(handler), "type_handlers", staticmethod(lambda *args, **kwargs: {})
    ):
        with pytest.raises(TypeError, match=r"pip install rivers\[polars\]"):
            handler.load_input(ctx)


def test_missing_polars_lazyframe_error(tmp_path):
    """Loading as polars LazyFrame without polars suggests rivers[polars]."""
    import polars as pl

    handler = _handler_without_extras(tmp_path)
    ctx = rs.InputContext(
        asset_name="tbl", downstream_asset="x", type_hint=pl.LazyFrame
    )

    with patch.object(
        type(handler), "type_handlers", staticmethod(lambda *args, **kwargs: {})
    ):
        with pytest.raises(TypeError, match=r"pip install rivers\[polars\]"):
            handler.load_input(ctx)


def test_missing_pandas_output_error(tmp_path):
    """Writing a pandas DataFrame without the handler suggests rivers[pandas]."""
    import pandas as pd

    handler = _handler_without_extras(tmp_path)
    df = pd.DataFrame({"a": [1]})
    ctx = rs.OutputContext(asset_name="tbl")

    with patch.object(
        type(handler), "type_handlers", staticmethod(lambda *args, **kwargs: {})
    ):
        with pytest.raises(TypeError, match=r"pip install rivers\[pandas\]"):
            handler.handle_output(ctx, df)


def test_missing_pandas_input_error(tmp_path):
    """Loading as pandas DataFrame without the handler suggests rivers[pandas]."""
    import pandas as pd

    handler = _handler_without_extras(tmp_path)
    ctx = rs.InputContext(
        asset_name="tbl", downstream_asset="x", type_hint=pd.DataFrame
    )

    with patch.object(
        type(handler), "type_handlers", staticmethod(lambda *args, **kwargs: {})
    ):
        with pytest.raises(TypeError, match=r"pip install rivers\[pandas\]"):
            handler.load_input(ctx)


def test_missing_pyarrow_recordbatchreader_error(tmp_path):
    """Loading as RecordBatchReader without pyarrow suggests rivers[pyarrow]."""
    import pyarrow as pa

    handler = _handler_without_extras(tmp_path)
    ctx = rs.InputContext(
        asset_name="tbl", downstream_asset="x", type_hint=pa.RecordBatchReader
    )

    with patch.object(
        type(handler), "type_handlers", staticmethod(lambda *args, **kwargs: {})
    ):
        with pytest.raises(TypeError, match=r"pip install rivers\[pyarrow\]"):
            handler.load_input(ctx)


def test_unknown_type_generic_error(tmp_path):
    """Unknown types get a generic error listing supported types."""
    handler = _handler_without_extras(tmp_path)
    ctx = rs.OutputContext(asset_name="tbl")

    with patch.object(
        type(handler), "type_handlers", staticmethod(lambda *args, **kwargs: {})
    ):
        with pytest.raises(TypeError, match="No type handler found"):
            handler.handle_output(ctx, {"not": "a table"})
