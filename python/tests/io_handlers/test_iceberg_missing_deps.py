"""Error messages that name the extra to install when an Iceberg dependency is missing."""

import subprocess
import sys
import textwrap
from unittest.mock import patch

import pytest

pytest.importorskip("pyiceberg")

import duckdb  # noqa: E402
import pandas as pd  # noqa: E402
import polars as pl  # noqa: E402
import pyarrow as pa  # noqa: E402

import rivers as rs  # noqa: E402
from rivers.io_handlers.iceberg import IcebergIOHandler  # noqa: E402

from .helpers import iceberg_handler  # noqa: E402

_NO_TYPE_HANDLERS = patch.object(
    IcebergIOHandler, "type_handlers", staticmethod(lambda *args, **kwargs: {})
)


@pytest.mark.parametrize(
    "obj, extra",
    [
        (pa.table({"a": [1]}), "iceberg"),
        (pl.DataFrame({"a": [1]}), "iceberg-polars"),
        (pd.DataFrame({"a": [1]}), "iceberg-pandas"),
        (duckdb.sql("SELECT 1 AS a"), "iceberg-duckdb"),
    ],
    ids=["pyarrow", "polars", "pandas", "duckdb"],
)
def test_missing_output_handler_names_the_extra(tmp_path, obj, extra):
    handler = iceberg_handler(tmp_path)
    with _NO_TYPE_HANDLERS, pytest.raises(TypeError, match=rf"rivers\[{extra}\]"):
        handler.handle_output(rs.OutputContext(asset_name="t"), obj)


@pytest.mark.parametrize(
    "type_hint, extra",
    [(pl.LazyFrame, "iceberg-polars"), (pd.DataFrame, "iceberg-pandas")],
)
def test_missing_input_handler_names_the_extra(tmp_path, type_hint, extra):
    handler = iceberg_handler(tmp_path)
    ctx = rs.InputContext(asset_name="t", downstream_asset="d", type_hint=type_hint)
    with _NO_TYPE_HANDLERS, pytest.raises(TypeError, match=rf"rivers\[{extra}\]"):
        handler.load_input(ctx)


def _run(code: str) -> str:
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )
    return result.stdout


@pytest.mark.parametrize(
    "library, name, message",
    [
        (
            "pyiceberg",
            "IcebergIOHandler",
            "IcebergIOHandler requires the Iceberg extra — install 'rivers[iceberg]'",
        ),
        (
            "pyiceberg",
            "IcebergAsset",
            "IcebergAsset requires the Iceberg extra — install 'rivers[iceberg]'",
        ),
        (
            "deltalake",
            "DeltaIOHandler",
            "DeltaIOHandler requires the Delta extras — install 'rivers[delta-pyarrow]'",
        ),
    ],
)
def test_top_level_name_without_its_library(library, name, message):
    output = _run(
        f"""
        import sys
        sys.modules[{library!r}] = None
        import rivers
        try:
            rivers.{name}
        except ImportError as e:
            print(e)
        try:
            rivers.not_a_name
        except AttributeError as e:
            print(e)
        """
    )
    assert message in output
    assert "module 'rivers' has no attribute 'not_a_name'" in output


def test_package_import_without_pyiceberg():
    output = _run(
        """
        import sys
        sys.modules["pyiceberg"] = None
        import rivers.io_handlers
        print("IcebergIOHandler" in rivers.io_handlers.__all__)
        try:
            import rivers.io_handlers.iceberg
        except ImportError as e:
            print(e)
        """
    )
    assert output.splitlines() == [
        "False",
        "rivers.io_handlers.iceberg needs pyiceberg: pip install rivers[iceberg]",
    ]
