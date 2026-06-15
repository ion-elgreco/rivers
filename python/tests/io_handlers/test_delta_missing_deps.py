"""Tests that helpful error messages are raised when optional deps are missing."""

import contextlib
import sys
from unittest.mock import patch

import pytest

import rivers as rs


@contextlib.contextmanager
def _block_imports(*names):
    """Make ``import name`` raise ImportError for each name, then restore.

    Only the named keys are touched (unlike ``patch.dict(sys.modules, ...)``,
    which clears and restores the whole table and so evicts modules imported
    inside the block — corrupting C extensions like numpy).
    """
    missing = object()
    saved = {name: sys.modules.get(name, missing) for name in names}
    for name in names:
        sys.modules[name] = None  # type: ignore[assignment]
    try:
        yield
    finally:
        for name, prev in saved.items():
            if prev is missing:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = prev


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


_DATA = {"a": [1, 2, 3], "b": ["x", "y", "z"]}

# Heavy optional deps shared by every backend: the shared write path (and its
# ``merge`` import) must never require the Spark stack.
_SPARK = ("pyspark", "delta")


def _polars_case():
    """polars frame, native read-back; needs none of the other Arrow backends."""
    import polars as pl
    from polars.testing import assert_frame_equal

    frame = pl.DataFrame(_DATA)

    def check(result):
        assert isinstance(result, pl.DataFrame)
        assert_frame_equal(result, frame)

    return frame, pl.DataFrame, ("pyarrow", "pandas", "datafusion", *_SPARK), check


def _datafusion_case():
    """datafusion frame, native read-back; keeps pyarrow (genuinely required)."""
    import pyarrow as pa
    from datafusion import DataFrame, SessionContext

    frame = SessionContext().from_arrow(pa.table(_DATA))
    expected = pa.table(_DATA)

    def check(result):
        assert isinstance(result, DataFrame)
        # DataFusion may emit Utf8View; cast to the source schema before comparing.
        assert pa.table(result).cast(expected.schema).equals(expected)

    return frame, DataFrame, ("polars", "pandas", *_SPARK), check


def _pandas_case():
    """pandas frame, native read-back; keeps pyarrow (genuinely required)."""
    import pandas as pd
    from pandas.testing import assert_frame_equal

    frame = pd.DataFrame(_DATA)

    def check(result):
        assert isinstance(result, pd.DataFrame)
        assert_frame_equal(result, frame)

    return frame, pd.DataFrame, ("polars", "datafusion", *_SPARK), check


@pytest.mark.parametrize(
    "make_case",
    [
        pytest.param(_polars_case, id="polars"),
        pytest.param(_datafusion_case, id="datafusion"),
        pytest.param(_pandas_case, id="pandas"),
    ],
)
def test_delta_round_trip_without_unneeded_deps(tmp_path, make_case):
    """Each Arrow backend writes + reads through Delta with every backend it does
    not use uninstalled.

    Regression test for #83: ``base.handle_output`` builds schema metadata via
    arro3 (``dt.schema().to_arrow()``) rather than pyarrow and imports ``merge``
    on every write, so a write pulls in only the chosen backend's libraries — a
    polars install needs no pyarrow, and no backend needs pyspark / delta-spark.
    """
    frame, type_hint, unneeded, check = make_case()
    handler = rs.DeltaIOHandler(table_uri=str(tmp_path))

    with _block_imports(*unneeded):
        handler.handle_output(rs.OutputContext(asset_name="tbl"), frame)
        result = handler.load_input(
            rs.InputContext(asset_name="tbl", downstream_asset="c", type_hint=type_hint)
        )
        check(result)


def test_merge_helpers_import_without_spark():
    """The shared merge helpers import without pyspark / delta-spark.

    Regression test for #83: ``base`` imports ``merge`` on every Delta write, so
    ``merge`` must not pull in pyspark / delta-spark — only the lazily loaded
    pyspark handler may, or a polars/pyarrow-only install cannot even import
    :class:`DeltaIOHandler`.
    """
    import importlib

    from rivers.io_handlers.delta import merge

    # reload re-runs the module's top-level imports; if it needs a blocked
    # dependency the ImportError fails the test.
    with _block_imports("pyspark", "delta"):
        importlib.reload(merge)
