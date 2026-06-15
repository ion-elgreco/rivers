"""PySpark type handler tests for the Delta Lake IO handler.

Tests to check exercise code paths in PySparkDeltaTypeHandler and verify that
PySpark DataFrames integrate correctly with the DeltaIOHandler pipeline.

The spark fixture is module-scoped to create a single local SparkSession with
the Delta only for this test file.
"""

from __future__ import annotations

import logging
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pytest
import rivers as rs
from deltalake import CommitProperties, WriterProperties
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from rivers.io_handlers.delta.config import (
    MergeConfig,
    MergeOperationsConfig,
    WhenMatchedDelete,
    WhenMatchedUpdate,
    WhenMatchedUpdateAll,
    WhenNotMatchedBySourceDelete,
    WhenNotMatchedBySourceUpdate,
    WhenNotMatchedInsert,
    WhenNotMatchedInsertAll,
)
from rivers.io_handlers.delta.pyspark import PySparkDeltaTypeHandler

from .helpers import make_daily_partition, make_multi_partition, make_partition


@pytest.fixture(scope="module")
def spark() -> SparkSession:  # type: ignore[return]
    """Module scoped SparkSession with the Delta Lake extensions enabled.

    Uses ``delta.configure_spark_with_delta_pip`` to ensure the
    ``DeltaSparkSessionExtension`` and ``DeltaCatalog`` are registered before
    any test reads or writes Delta tables.  Arrow-optimised ``toPandas()`` is
    also enabled so that ``PySparkDeltaTypeHandler.to_arrow`` exercises the fast
    path during tests.
    """
    pytest.importorskip(
        "delta",
        reason="delta-spark not installed — skipping PySpark Delta tests",
    )
    from delta import configure_spark_with_delta_pip  # type: ignore[import-untyped]

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("rivers-pyspark-tests")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.network.timeout", "300s")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
        .config(
            "spark.driver.extraJavaOptions",
            "-Divy.connection.timeout=300000 -Divy.read.timeout=300000",
        )
    )
    session: SparkSession = configure_spark_with_delta_pip(builder).getOrCreate()

    # Only show error logs.
    session.sparkContext.setLogLevel("ERROR")
    logging.getLogger("py4j").setLevel(logging.ERROR)

    yield session
    session.stop()


def _make_handler(tmp_path, spark=None, **kwargs) -> tuple[rs.DeltaIOHandler, str]:
    uri = str(tmp_path)
    return rs.DeltaIOHandler(
        table_uri=uri, handler_config={"spark_session": spark}, **kwargs
    ), uri


def _pyspark_df(spark: SparkSession, data: dict) -> SparkDataFrame:  # type: ignore[return]
    """Construct a PySpark DataFrame from a column-oriented dict via pandas."""

    return spark.createDataFrame(pd.DataFrame(data))


def _collect_sorted(df: SparkDataFrame, sort_col: str) -> dict:
    """Collect *df* to a row-sorted python dict for deterministic comparison."""
    return df.sort(sort_col).toPandas().to_dict(orient="list")


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_round_trip_pyspark(tmp_path, spark):
    """Write a PySpark DataFrame, read back as PySpark DataFrame."""
    handler, _ = _make_handler(tmp_path, spark)
    df = _pyspark_df(spark, {"a": [1, 2, 3], "b": ["x", "y", "z"]})

    handler.handle_output(rs.OutputContext(asset_name="tbl"), df)

    ctx_in = rs.InputContext(
        asset_name="tbl", downstream_asset="consumer", type_hint=SparkDataFrame
    )
    result = handler.load_input(ctx_in)
    assert isinstance(result, SparkDataFrame)

    result_pa = result.sort("a").toArrow()
    expected = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    assert result_pa.cast(expected.schema).equals(expected)


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_write_pyspark_read_pyarrow(tmp_path, spark):
    """A PySpark output is readable back via the PyArrow type handler."""
    handler, _ = _make_handler(tmp_path, spark)
    df = _pyspark_df(spark, {"a": [1, 2], "b": ["x", "y"]})

    handler.handle_output(rs.OutputContext(asset_name="tbl"), df)

    ctx_in = rs.InputContext(
        asset_name="tbl", downstream_asset="consumer", type_hint=pa.Table
    )
    result = handler.load_input(ctx_in)
    assert isinstance(result, pa.Table)

    expected = pa.table({"a": [1, 2], "b": ["x", "y"]})
    assert result.cast(expected.schema).sort_by("a").equals(expected)


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_write_pyarrow_read_pyspark(tmp_path, spark):
    """A PyArrow Table output is readable back via the PySpark type handler."""
    handler, _ = _make_handler(tmp_path, spark)
    table = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    handler.handle_output(rs.OutputContext(asset_name="tbl"), table)

    ctx_in = rs.InputContext(
        asset_name="tbl", downstream_asset="consumer", type_hint=SparkDataFrame
    )
    result = handler.load_input(ctx_in)
    assert isinstance(result, SparkDataFrame)

    result_dict = _collect_sorted(result, "a")
    assert result_dict["a"] == [1, 2, 3]
    assert result_dict["b"] == ["x", "y", "z"]


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_write_pyspark_read_polars(tmp_path, spark):
    """A PySpark output is readable back via the Polars type handler."""
    import polars as pl

    handler, _ = _make_handler(tmp_path, spark)
    df = _pyspark_df(spark, {"a": [10, 20, 30], "b": ["p", "q", "r"]})

    handler.handle_output(rs.OutputContext(asset_name="tbl"), df)

    ctx_in = rs.InputContext(
        asset_name="tbl", downstream_asset="consumer", type_hint=pl.DataFrame
    )
    result = handler.load_input(ctx_in)
    assert isinstance(result, pl.DataFrame)

    from polars.testing import assert_frame_equal

    assert_frame_equal(
        result.sort("a"),
        pl.DataFrame({"a": [10, 20, 30], "b": ["p", "q", "r"]}),
    )


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_append_mode_pyspark(tmp_path, spark):
    """Append mode accumulates rows across PySpark writes."""
    handler, _ = _make_handler(tmp_path, spark, mode="append")
    df = _pyspark_df(spark, {"a": [1, 2]})

    handler.handle_output(rs.OutputContext(asset_name="tbl"), df)
    handler.handle_output(rs.OutputContext(asset_name="tbl"), df)

    ctx_in = rs.InputContext(
        asset_name="tbl", downstream_asset="x", type_hint=SparkDataFrame
    )
    result = handler.load_input(ctx_in)
    assert result.count() == 4


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_single_partition_isolation_pyspark(tmp_path, spark):
    """Two partitions written via PySpark do not cross-contaminate on read."""
    handler, _ = _make_handler(tmp_path, spark)
    p_a = make_partition("2024-01-01")
    p_b = make_partition("2024-01-02")
    meta = {"delta/partition_expr": "date"}

    df_a = _pyspark_df(spark, {"date": ["2024-01-01"], "val": [10]})
    df_b = _pyspark_df(spark, {"date": ["2024-01-02"], "val": [20]})

    handler.handle_output(
        rs.OutputContext(asset_name="daily", partition=p_a, asset_metadata=meta), df_a
    )
    handler.handle_output(
        rs.OutputContext(asset_name="daily", partition=p_b, asset_metadata=meta), df_b
    )

    ctx_in_a = rs.InputContext(
        asset_name="daily",
        downstream_asset="x",
        partition=p_a,
        asset_metadata=meta,
        type_hint=SparkDataFrame,
    )
    ctx_in_b = rs.InputContext(
        asset_name="daily",
        downstream_asset="x",
        partition=p_b,
        asset_metadata=meta,
        type_hint=SparkDataFrame,
    )
    result_a = handler.load_input(ctx_in_a)
    result_b = handler.load_input(ctx_in_b)

    assert result_a.toPandas()["val"].tolist() == [10]
    assert result_b.toPandas()["val"].tolist() == [20]


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_partition_overwrite_pyspark(tmp_path, spark):
    """Re-writing a partition via PySpark replaces its data."""
    handler, _ = _make_handler(tmp_path, spark)
    p = make_partition("2024-01-01")
    meta = {"delta/partition_expr": "date"}

    handler.handle_output(
        rs.OutputContext(asset_name="daily", partition=p, asset_metadata=meta),
        _pyspark_df(spark, {"date": ["2024-01-01"], "val": [1]}),
    )
    handler.handle_output(
        rs.OutputContext(asset_name="daily", partition=p, asset_metadata=meta),
        _pyspark_df(spark, {"date": ["2024-01-01"], "val": [99]}),
    )

    ctx_in = rs.InputContext(
        asset_name="daily",
        downstream_asset="x",
        partition=p,
        asset_metadata=meta,
        type_hint=SparkDataFrame,
    )
    result = handler.load_input(ctx_in)
    assert result.toPandas()["val"].tolist() == [99]


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_daily_partition_isolation_pyspark(tmp_path, spark):
    """Daily time-window partitions written via PySpark read back correctly."""
    handler, _ = _make_handler(tmp_path, spark)
    p_a = make_daily_partition("2024-01-01")
    p_b = make_daily_partition("2024-01-02")
    meta = {"delta/partition_expr": "date"}

    handler.handle_output(
        rs.OutputContext(asset_name="daily", partition=p_a, asset_metadata=meta),
        _pyspark_df(spark, {"date": ["2024-01-01"], "val": [10]}),
    )
    handler.handle_output(
        rs.OutputContext(asset_name="daily", partition=p_b, asset_metadata=meta),
        _pyspark_df(spark, {"date": ["2024-01-02"], "val": [20]}),
    )

    ctx_in_a = rs.InputContext(
        asset_name="daily",
        downstream_asset="x",
        partition=p_a,
        asset_metadata=meta,
        type_hint=SparkDataFrame,
    )
    result_a = handler.load_input(ctx_in_a)
    assert result_a.toPandas()["val"].tolist() == [10]


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_column_selection_pyspark(tmp_path, spark):
    """``delta/columns`` metadata projects only the selected columns."""
    handler, _ = _make_handler(tmp_path, spark)
    df = _pyspark_df(spark, {"a": [1, 2], "b": ["x", "y"], "c": [3.0, 4.0]})

    handler.handle_output(rs.OutputContext(asset_name="tbl"), df)

    ctx_in = rs.InputContext(
        asset_name="tbl",
        downstream_asset="x",
        asset_metadata={"delta/columns": '["a", "c"]'},
        type_hint=SparkDataFrame,
    )
    result = handler.load_input(ctx_in)

    assert sorted(result.columns) == ["a", "c"]
    collected = result.sort("a").toPandas()
    assert collected["a"].tolist() == [1, 2]
    assert collected["c"].tolist() == [3.0, 4.0]


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_table_versioning_pyspark(tmp_path, spark):
    """``delta/version`` time-travels the PySpark reader to a prior commit."""
    handler, _ = _make_handler(tmp_path, spark)

    handler.handle_output(
        rs.OutputContext(asset_name="tbl"),
        _pyspark_df(spark, {"a": [1]}),
    )
    handler.handle_output(
        rs.OutputContext(asset_name="tbl"),
        _pyspark_df(spark, {"a": [2]}),
    )

    # Read version 0 — should contain [1]
    ctx_0 = rs.InputContext(
        asset_name="tbl",
        downstream_asset="x",
        asset_metadata={"delta/version": "0"},
        type_hint=SparkDataFrame,
    )
    result_0 = handler.load_input(ctx_0)
    assert result_0.toPandas()["a"].tolist() == [1]

    # Read latest — should contain [2]
    ctx_latest = rs.InputContext(
        asset_name="tbl", downstream_asset="x", type_hint=SparkDataFrame
    )
    result_latest = handler.load_input(ctx_latest)
    assert result_latest.toPandas()["a"].tolist() == [2]


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_null_values_preserved_pyspark(tmp_path, spark):
    """Null values in a PySpark DataFrame round-trip through Delta correctly."""
    import pandas as pd

    handler, _ = _make_handler(tmp_path, spark)
    df = spark.createDataFrame(
        pd.DataFrame({"id": [1, 2, 3], "val": [10.0, None, 30.0]})
    )

    handler.handle_output(rs.OutputContext(asset_name="tbl"), df)

    ctx_in = rs.InputContext(
        asset_name="tbl", downstream_asset="x", type_hint=SparkDataFrame
    )
    result = handler.load_input(ctx_in)

    collected = result.sort("id").toPandas()
    assert collected["id"].tolist() == [1, 2, 3]
    import math

    assert math.isnan(collected["val"].iloc[1]) or collected["val"].isna().iloc[1]


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_output_metadata_pyspark(tmp_path, spark):
    """Output metadata is populated correctly after a PySpark write."""
    handler, uri = _make_handler(tmp_path, spark)
    df = _pyspark_df(spark, {"a": [1, 2]})

    ctx = rs.OutputContext(asset_name="tbl")
    handler.handle_output(ctx, df)

    meta = ctx.output_metadata
    assert meta is not None
    assert meta["delta/table_uri"].raw_value() == f"{uri}/tbl"
    assert meta["delta/mode"].raw_value() == "overwrite"
    assert isinstance(meta["delta/num_rows"], rs.MetadataValue.Int)
    assert meta["delta/num_rows"].raw_value() == 2
    assert isinstance(meta["delta/size_bytes"], rs.MetadataValue.Int)
    assert isinstance(meta["delta/write_duration_s"], rs.MetadataValue.Float)
    assert isinstance(meta["delta/version"], rs.MetadataValue.Int)
    assert meta["delta/version"].raw_value() == 0
    schema = meta["rivers/schema"].raw_value()
    assert isinstance(schema, rs.Schema)
    assert "a" in schema.names


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_creates_delta_log_pyspark(tmp_path, spark):
    """Delta log directory exists on disk after a PySpark write."""
    handler, _ = _make_handler(tmp_path, spark)
    df = _pyspark_df(spark, {"x": [1]})

    handler.handle_output(rs.OutputContext(asset_name="tbl"), df)

    assert (tmp_path / "tbl" / "_delta_log").is_dir()


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_root_name_override_pyspark(tmp_path, spark):
    """``delta/root_name`` metadata overrides the asset name in the table path."""
    handler, _ = _make_handler(tmp_path, spark)
    df = _pyspark_df(spark, {"a": [1, 2, 3]})

    ctx_out = rs.OutputContext(
        asset_name="my_asset", asset_metadata={"delta/root_name": "raw_events"}
    )
    handler.handle_output(ctx_out, df)

    # Table should be at raw_events/, not my_asset/
    assert (tmp_path / "raw_events" / "_delta_log").is_dir()
    assert not (tmp_path / "my_asset").exists()

    # Read back using the same root_name override
    ctx_in = rs.InputContext(
        asset_name="my_asset",
        downstream_asset="consumer",
        asset_metadata={"delta/root_name": "raw_events"},
        type_hint=SparkDataFrame,
    )
    result = handler.load_input(ctx_in)
    assert result.toPandas()["a"].tolist() == [1, 2, 3]


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_multi_partition_pyspark(tmp_path, spark):
    """Multi-dimension partition read/write with PySpark DataFrames."""
    import json

    handler, _ = _make_handler(tmp_path, spark)
    p = make_multi_partition({"region": "us", "env": "prod"})
    meta = {"delta/partition_expr": json.dumps({"region": "region", "env": "env"})}

    df = _pyspark_df(spark, {"region": ["us"], "env": ["prod"], "val": [42]})

    ctx_out = rs.OutputContext(asset_name="sales", partition=p, asset_metadata=meta)
    handler.handle_output(ctx_out, df)

    ctx_in = rs.InputContext(
        asset_name="sales",
        downstream_asset="x",
        partition=p,
        asset_metadata=meta,
        type_hint=SparkDataFrame,
    )
    result = handler.load_input(ctx_in)
    assert result.toPandas()["val"].tolist() == [42]


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_pyspark_handler_with_explicit_spark(tmp_path, spark):
    """Explicit PySparkDeltaTypeHandler initialized by user."""

    handler, _ = _make_handler(tmp_path)

    # Write via the shared handler (uses getActiveSession internally)
    df = _pyspark_df(spark, {"a": [7, 8, 9]})
    handler.handle_output(rs.OutputContext(asset_name="tbl"), df)

    # Read via a handler whose type_handler map uses an explicit session
    explicit_handler = PySparkDeltaTypeHandler(spark_session=spark)
    result = explicit_handler.load_input(
        table_uri=str(tmp_path / "tbl"),
        table_name="tbl",
        storage_options=None,
        predicate=None,
        target_type=SparkDataFrame,
    )
    assert isinstance(result, SparkDataFrame)
    assert result.count() == 3


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_handler_fallback_to_active_session_with_warn(tmp_path, spark):
    """handler should fallback to active spark session when
    no spark session is supplied by user in DeltaIOHandler."""

    handler, _ = _make_handler(tmp_path)
    df = _pyspark_df(spark, {"a": [7, 8, 9]})
    handler.handle_output(rs.OutputContext(asset_name="tbl"), df)

    # With _user_spark_session being None, load_input raises a warning.
    ctx_in = rs.InputContext(
        asset_name="tbl", downstream_asset="x", type_hint=SparkDataFrame
    )
    with pytest.warns(UserWarning, match="No spark session set by user"):
        result = handler.load_input(ctx_in)
    assert isinstance(result, SparkDataFrame)

    # Spark session must match the active spark session even though its not passed,
    # since _get_or_create_spark uses the active session when available.
    assert (
        result.sparkSession.sparkContext.applicationId
        == spark.sparkContext.applicationId
    )


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_handler_using_user_spark_correctly(tmp_path, spark, recwarn):
    """handler should should correctly use user spark session
    (supplied by user in DeltaIOHandler)."""

    handler, _ = _make_handler(tmp_path, spark)
    df = _pyspark_df(spark, {"a": [7, 8, 9]})
    handler.handle_output(rs.OutputContext(asset_name="tbl"), df)

    ctx_in = rs.InputContext(
        asset_name="tbl", downstream_asset="x", type_hint=SparkDataFrame
    )

    # No user warnings from load_input since the user spark session
    # is set in handler_config already.
    result = handler.load_input(ctx_in)
    assert isinstance(result, SparkDataFrame)
    assert not any(issubclass(w.category, UserWarning) for w in recwarn)
    assert (
        result.sparkSession.sparkContext.applicationId
        == spark.sparkContext.applicationId
    )


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_get_or_create_spark_no_active_no_delta_raises(monkeypatch):
    """_get_or_create_spark raises ImportError when no active session and no delta-spark."""
    from rivers.io_handlers.delta import pyspark as pyspark_module

    # Case where there's no active SparkSession
    monkeypatch.setattr(
        SparkSession,
        "getActiveSession",
        staticmethod(lambda: None),
    )
    # Case where delta-spark not being importable
    import builtins

    real_import = builtins.__import__

    def _block_delta(name, *args, **kwargs):
        if name == "delta":
            raise ImportError("delta-spark not installed")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _block_delta)

    with pytest.raises(ImportError, match=r"pip install rivers\[delta-pyspark\]"):
        pyspark_module.PySparkDeltaTypeHandler(
            spark_session=None
        )._get_or_create_spark_session()


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_schema_mode_merge_pyspark(tmp_path, spark):
    """schema_mode='merge' adds new columns on subsequent PySpark writes."""
    handler, _ = _make_handler(tmp_path, spark)

    # First write — schema {a: int}
    df_1 = _pyspark_df(spark, {"a": [1, 2]})
    handler.handle_output(rs.OutputContext(asset_name="tbl"), df_1)

    # Second write — schema {a: int, b: str} with schema_mode="merge" and mode="append"
    merge_handler, _ = _make_handler(tmp_path, schema_mode="merge", mode="append")
    df_2 = _pyspark_df(spark, {"a": [3], "b": ["new"]})
    merge_handler.handle_output(rs.OutputContext(asset_name="tbl"), df_2)

    ctx_in = rs.InputContext(
        asset_name="tbl", downstream_asset="x", type_hint=SparkDataFrame
    )
    result = handler.load_input(ctx_in)
    # All three rows should be present; first two rows have null in "b"
    assert result.count() == 3
    assert "b" in result.columns


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_custom_merge_when_matched_update(tmp_path, spark):
    """schema_mode='merge' with merge_type='custom' for PySpark writes."""
    handler, _ = _make_handler(tmp_path, spark)
    df_base = _pyspark_df(
        spark,
        {
            "id": [1, 2, 3, 4],
            "val": ["a", "b", "c", "d"],
            "flag": ["keep", "keep", "keep", "keep"],
            "status": ["old", "old", "old", "old"],
        },
    )
    handler.handle_output(rs.OutputContext(asset_name="tbl"), df_base)

    # when_matched_update
    handler, _ = _make_handler(
        tmp_path=tmp_path,
        spark=spark,
        mode="merge",
        merge_config=MergeConfig(
            predicate="target.id = source.id",
            merge_type="custom",
            source_alias="source",
            target_alias="target",
            operations=MergeOperationsConfig(
                when_matched_update=[
                    WhenMatchedUpdate(
                        predicate="target.id = 1",
                        updates={"status": "'updated_partial'"},
                    )
                ],
            ),
        ),
    )
    handler.handle_output(
        rs.OutputContext(asset_name="tbl"),
        _pyspark_df(
            spark, {"id": [1], "val": ["a_new"], "flag": ["keep"], "status": ["s1"]}
        ),
    )

    ctx = rs.InputContext(
        asset_name="tbl",
        downstream_asset="x",
        type_hint=SparkDataFrame,
    )
    df = handler.load_input(ctx)
    rows = {r["id"]: r for r in df.collect()}

    assert rows[1]["status"] == "updated_partial"


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_custom_merge_when_matched_update_all(tmp_path, spark):
    """schema_mode='merge' with merge_type='custom' for PySpark writes."""
    handler, _ = _make_handler(tmp_path, spark)
    df_base = _pyspark_df(
        spark,
        {
            "id": [1, 2, 3, 4],
            "val": ["a", "b", "c", "d"],
            "flag": ["keep", "keep", "keep", "keep"],
            "status": ["old", "old", "old", "old"],
        },
    )
    handler.handle_output(rs.OutputContext(asset_name="tbl"), df_base)

    # when_matched_update_all with except_cols
    handler, _ = _make_handler(
        tmp_path=tmp_path,
        spark=spark,
        mode="merge",
        merge_config=MergeConfig(
            predicate="target.id = source.id",
            merge_type="custom",
            source_alias="source",
            target_alias="target",
            operations=MergeOperationsConfig(
                when_matched_update_all=[
                    WhenMatchedUpdateAll(
                        predicate="target.id = 2",
                        except_cols=["flag"],
                    )
                ],
            ),
        ),
    )
    handler.handle_output(
        rs.OutputContext(asset_name="tbl"),
        _pyspark_df(
            spark,
            {"id": [2], "val": ["b_new"], "flag": ["SHOULD_STAY"], "status": ["s2"]},
        ),
    )

    ctx = rs.InputContext(
        asset_name="tbl",
        downstream_asset="x",
        type_hint=SparkDataFrame,
    )
    df = handler.load_input(ctx)
    rows = {r["id"]: r for r in df.collect()}

    assert rows[2]["flag"] == "keep"
    assert rows[2]["val"] == "b_new"


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_custom_merge_when_matched_delete(tmp_path, spark):
    """schema_mode='merge' with merge_type='custom' for PySpark writes."""
    handler, _ = _make_handler(tmp_path, spark)
    df_base = _pyspark_df(
        spark,
        {
            "id": [1, 2, 3, 4],
            "val": ["a", "b", "c", "d"],
            "flag": ["keep", "keep", "keep", "keep"],
            "status": ["old", "old", "old", "old"],
        },
    )
    handler.handle_output(rs.OutputContext(asset_name="tbl"), df_base)

    # when_matched_delete
    handler, _ = _make_handler(
        tmp_path=tmp_path,
        spark=spark,
        mode="merge",
        merge_config=MergeConfig(
            predicate="target.id = source.id",
            merge_type="custom",
            source_alias="source",
            target_alias="target",
            operations=MergeOperationsConfig(
                when_matched_delete=[
                    WhenMatchedDelete(
                        predicate="target.id = 3",
                    )
                ],
            ),
        ),
    )
    handler.handle_output(
        rs.OutputContext(asset_name="tbl"),
        _pyspark_df(
            spark, {"id": [3], "val": ["c_new"], "flag": ["keep"], "status": ["s3"]}
        ),
    )

    ctx = rs.InputContext(
        asset_name="tbl",
        downstream_asset="x",
        type_hint=SparkDataFrame,
    )
    df = handler.load_input(ctx)
    rows = {r["id"]: r for r in df.collect()}

    assert 3 not in rows


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_custom_merge_when_not_matched_insert(tmp_path, spark):
    """schema_mode='merge' with merge_type='custom' for PySpark writes."""
    handler, _ = _make_handler(tmp_path, spark)
    df_base = _pyspark_df(
        spark,
        {
            "id": [1, 2, 3, 4],
            "val": ["a", "b", "c", "d"],
            "flag": ["keep", "keep", "keep", "keep"],
            "status": ["old", "old", "old", "old"],
        },
    )
    handler.handle_output(rs.OutputContext(asset_name="tbl"), df_base)

    # when_not_matched_insert
    handler, _ = _make_handler(
        tmp_path=tmp_path,
        spark=spark,
        mode="merge",
        merge_config=MergeConfig(
            predicate="target.id = source.id",
            merge_type="custom",
            source_alias="source",
            target_alias="target",
            operations=MergeOperationsConfig(
                when_not_matched_insert=[
                    WhenNotMatchedInsert(
                        predicate="source.id = 99",
                        updates={
                            "id": "'99'",
                            "val": "'insert_single'",
                            "flag": "'x'",
                            "status": "'s99'",
                        },
                    )
                ],
            ),
        ),
    )
    handler.handle_output(
        rs.OutputContext(asset_name="tbl"),
        _pyspark_df(spark, {"id": [99], "val": ["x"], "flag": ["x"], "status": ["x"]}),
    )

    ctx = rs.InputContext(
        asset_name="tbl",
        downstream_asset="x",
        type_hint=SparkDataFrame,
    )
    df = handler.load_input(ctx)
    rows = {r["id"]: r for r in df.collect()}

    assert 99 in rows
    assert 100 not in rows


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_custom_merge_when_not_matched_insert_all(tmp_path, spark):
    """schema_mode='merge' with merge_type='custom' for PySpark writes."""
    handler, _ = _make_handler(tmp_path, spark)
    df_base = _pyspark_df(
        spark,
        {
            "id": [1, 2, 3, 4],
            "val": ["a", "b", "c", "d"],
            "flag": ["keep", "keep", "keep", "keep"],
            "status": ["old", "old", "old", "old"],
        },
    )
    handler.handle_output(rs.OutputContext(asset_name="tbl"), df_base)

    # when_not_matched_insert_all
    handler, _ = _make_handler(
        tmp_path=tmp_path,
        spark=spark,
        mode="merge",
        merge_config=MergeConfig(
            predicate="target.id = source.id",
            merge_type="custom",
            source_alias="source",
            target_alias="target",
            operations=MergeOperationsConfig(
                when_not_matched_insert_all=[
                    WhenNotMatchedInsertAll(
                        predicate="source.id = 10",
                    )
                ],
            ),
        ),
    )
    handler.handle_output(
        rs.OutputContext(asset_name="tbl"),
        _pyspark_df(
            spark, {"id": [10], "val": ["z_new"], "flag": ["keep"], "status": ["s10"]}
        ),
    )

    ctx = rs.InputContext(
        asset_name="tbl",
        downstream_asset="x",
        type_hint=SparkDataFrame,
    )
    df = handler.load_input(ctx)
    rows = {r["id"]: r for r in df.collect()}

    assert 10 in rows
    assert rows[10]["val"] == "z_new"
    assert rows[10]["status"] == "s10"


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_custom_merge_when_not_matched_by_source_delete(tmp_path, spark):
    """schema_mode='merge' with merge_type='custom' for PySpark writes."""
    handler, _ = _make_handler(tmp_path, spark)
    df_base = _pyspark_df(
        spark,
        {
            "id": [1, 2, 3, 4],
            "val": ["a", "b", "c", "d"],
            "flag": ["keep", "keep", "keep", "keep"],
            "status": ["old", "old", "old", "old"],
        },
    )
    handler.handle_output(rs.OutputContext(asset_name="tbl"), df_base)

    # when_not_matched_by_source_delete
    handler, _ = _make_handler(
        tmp_path=tmp_path,
        spark=spark,
        mode="merge",
        merge_config=MergeConfig(
            predicate="target.id = source.id",
            merge_type="custom",
            source_alias="source",
            target_alias="target",
            operations=MergeOperationsConfig(
                when_not_matched_by_source_delete=[
                    WhenNotMatchedBySourceDelete(
                        predicate="target.id = 4",
                    )
                ],
            ),
        ),
    )
    handler.handle_output(
        rs.OutputContext(asset_name="tbl"),
        _pyspark_df(spark, {"id": [1, 2, 3, 99, 10], "val": ["x"] * 5}),
    )

    ctx = rs.InputContext(
        asset_name="tbl",
        downstream_asset="x",
        type_hint=SparkDataFrame,
    )
    df = handler.load_input(ctx)
    rows = {r["id"]: r for r in df.collect()}

    assert 4 not in rows
    assert list(rows.keys()) == [1, 2, 3]


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_custom_merge_when_not_matched_by_source_update(tmp_path, spark):
    """schema_mode='merge' with merge_type='custom' for PySpark writes."""
    handler, _ = _make_handler(tmp_path, spark)
    df_base = _pyspark_df(
        spark,
        {
            "id": [1, 2, 3, 4],
            "val": ["a", "b", "c", "d"],
            "flag": ["keep", "keep", "keep", "keep"],
            "status": ["old", "old", "old", "old"],
        },
    )
    handler.handle_output(rs.OutputContext(asset_name="tbl"), df_base)

    # when_not_matched_by_source_update
    handler, _ = _make_handler(
        tmp_path=tmp_path,
        spark=spark,
        mode="merge",
        merge_config=MergeConfig(
            predicate="target.id = source.id",
            merge_type="custom",
            source_alias="source",
            target_alias="target",
            operations=MergeOperationsConfig(
                when_not_matched_by_source_update=[
                    WhenNotMatchedBySourceUpdate(
                        predicate="target.id = 4",
                        updates={"status": "'new'"},
                    )
                ],
            ),
        ),
    )
    handler.handle_output(
        rs.OutputContext(asset_name="tbl"),
        _pyspark_df(spark, {"id": [1, 2, 3, 99, 10]}),
    )

    ctx = rs.InputContext(
        asset_name="tbl",
        downstream_asset="x",
        type_hint=SparkDataFrame,
    )
    df = handler.load_input(ctx)
    rows = {r["id"]: r for r in df.collect()}

    assert rows[4]["status"] == "new"
    assert rows[4]["val"] == "d"


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_missing_pyspark_output_error(tmp_path, spark):
    """Writing a PySpark DataFrame without the handler suggests rivers[pyspark]."""
    handler, _ = _make_handler(tmp_path)
    df = _pyspark_df(spark, {"a": [1]})
    ctx = rs.OutputContext(asset_name="tbl")

    with patch.object(
        type(handler), "type_handlers", staticmethod(lambda *args, **kwargs: {})
    ):
        with pytest.raises(TypeError, match=r"pip install rivers\[pyspark\]"):
            handler.handle_output(ctx, df)


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_missing_pyspark_input_error(tmp_path, spark):
    """Loading as PySpark DataFrame without the handler suggests rivers[pyspark]."""
    # Write first so the table exists
    handler, _ = _make_handler(tmp_path)
    handler.handle_output(
        rs.OutputContext(asset_name="tbl"),
        _pyspark_df(spark, {"a": [1]}),
    )

    ctx_in = rs.InputContext(
        asset_name="tbl", downstream_asset="x", type_hint=SparkDataFrame
    )

    with patch.object(
        type(handler), "type_handlers", staticmethod(lambda *args, **kwargs: {})
    ):
        with pytest.raises(TypeError, match=r"pip install rivers\[pyspark\]"):
            handler.load_input(ctx_in)


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_merge_upsert_pyspark(tmp_path, spark):
    """Merge mode upsert via PySpark source updates existing rows and inserts new ones."""
    from rivers.io_handlers.delta import MergeConfig

    init_handler, _ = _make_handler(tmp_path, spark)
    initial = pa.table({"id": [1, 2], "val": [10, 20]})
    init_handler.handle_output(rs.OutputContext(asset_name="tbl"), initial)

    # Merge source is a PySpark DataFrame
    mc = MergeConfig(merge_type="upsert", predicate="s.id = t.id")
    merge_handler, _ = _make_handler(tmp_path, mode="merge", merge_config=mc)
    source_df = _pyspark_df(spark, {"id": [2, 3], "val": [99, 30]})
    merge_handler.handle_output(rs.OutputContext(asset_name="tbl"), source_df)

    ctx_in = rs.InputContext(asset_name="tbl", downstream_asset="x", type_hint=pa.Table)
    result = init_handler.load_input(ctx_in)
    expected = pa.table({"id": [1, 2, 3], "val": [10, 99, 30]})
    assert result.sort_by("id").cast(expected.schema).equals(expected)


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_merge_deduplicate_insert_pyspark(tmp_path, spark):
    """Merge deduplicate_insert via PySpark only inserts non-matching rows."""
    from rivers.io_handlers.delta import MergeConfig

    init_handler, _ = _make_handler(tmp_path, spark)
    init_handler.handle_output(
        rs.OutputContext(asset_name="tbl"),
        pa.table({"id": [1, 2], "val": [10, 20]}),
    )

    mc = MergeConfig(merge_type="deduplicate_insert", predicate="s.id = t.id")
    merge_handler, _ = _make_handler(tmp_path, spark, mode="merge", merge_config=mc)
    source_df = _pyspark_df(spark, {"id": [2, 3], "val": [99, 30]})
    merge_handler.handle_output(rs.OutputContext(asset_name="tbl"), source_df)

    ctx_in = rs.InputContext(asset_name="tbl", downstream_asset="x", type_hint=pa.Table)
    result = init_handler.load_input(ctx_in)
    # id=2 should NOT be updated (deduplicate_insert skips matches)
    expected = pa.table({"id": [1, 2, 3], "val": [10, 20, 30]})
    assert result.sort_by("id").cast(expected.schema).equals(expected)


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_backfill_overwrite_pyspark(tmp_path, spark):
    """Overwriting a subset of partitions via PySpark leaves untouched ones intact."""
    handler, _ = _make_handler(tmp_path, spark)
    meta = {"delta/partition_expr": "key"}
    pd_def = rs.PartitionsDefinition.static_(["a", "b", "c"])

    # Initial write — all three partitions, value=1
    for pk_val in ["a", "b", "c"]:
        ctx_out = rs.OutputContext(
            asset_name="data",
            partition=rs.PartitionContext(
                keys=[rs.PartitionKey.single(pk_val)], definition=pd_def
            ),
            asset_metadata=meta,
        )
        handler.handle_output(
            ctx_out,
            _pyspark_df(spark, {"key": [pk_val], "val": [1]}),
        )

    # Overwrite only "a" and "b" with value=99
    for pk_val in ["a", "b"]:
        ctx_out = rs.OutputContext(
            asset_name="data",
            partition=rs.PartitionContext(
                keys=[rs.PartitionKey.single(pk_val)], definition=pd_def
            ),
            asset_metadata=meta,
        )
        handler.handle_output(
            ctx_out,
            _pyspark_df(spark, {"key": [pk_val], "val": [99]}),
        )

    # Read back all three — "a","b" → 99, "c" → 1
    results = {}
    for pk_val in ["a", "b", "c"]:
        ctx_in = rs.InputContext(
            asset_name="data",
            downstream_asset="x",
            partition=rs.PartitionContext(
                keys=[rs.PartitionKey.single(pk_val)], definition=pd_def
            ),
            asset_metadata=meta,
            type_hint=SparkDataFrame,
        )
        tbl = handler.load_input(ctx_in)
        results[pk_val] = tbl.toPandas()["val"].tolist()[0]

    assert results["a"] == 99, "partition 'a' should have been overwritten"
    assert results["b"] == 99, "partition 'b' should have been overwritten"
    assert results["c"] == 1, "partition 'c' should NOT have been overwritten"


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_large_pyspark_df_round_trips(tmp_path, spark):
    """PySpark DataFrame with many rows round-trips all rows through Arrow."""
    import pandas as pd

    n = 10_000
    handler, _ = _make_handler(tmp_path, spark)
    df = spark.createDataFrame(pd.DataFrame({"i": range(n)}))

    handler.handle_output(rs.OutputContext(asset_name="big"), df)

    ctx_in = rs.InputContext(
        asset_name="big", downstream_asset="x", type_hint=SparkDataFrame
    )
    result = handler.load_input(ctx_in)
    assert result.count() == n


@pytest.mark.spark_test
@pytest.mark.filterwarnings("ignore::pandas.errors.Pandas4Warning")
def test_warns_when_user_sets_unsupported_properties(tmp_path, spark):
    wp = WriterProperties(compression="SNAPPY", max_row_group_size=100)
    cp = CommitProperties(custom_metadata={"author": "test"})
    storage_options = {"access_key": "test"}
    handler, _ = _make_handler(
        tmp_path,
        spark,
        storage_options=storage_options,
        commit_properties=cp,
        writer_properties=wp,
    )
    writer_warning_msg = (
        "Values set in commit_properties, writer_properties, storage_options "
        "will be ignored, these are not supported by the PySpark handler."
    )
    with pytest.warns(UserWarning, match=writer_warning_msg):
        df = _pyspark_df(spark, {"a": [1, 2, 3], "b": ["x", "y", "z"]})

        handler.handle_output(rs.OutputContext(asset_name="tbl"), df)

    reader_warning_msg = (
        "Values set in storage_options will be ignored, "
        "these are not supported by the PySpark handler."
    )
    with pytest.warns(UserWarning, match=reader_warning_msg):
        ctx_in = rs.InputContext(
            asset_name="tbl",
            downstream_asset="consumer",
            type_hint=SparkDataFrame,
        )
        result = handler.load_input(ctx_in)
        assert isinstance(result, SparkDataFrame)

        result_pa = result.sort("a").toArrow()
        expected = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        assert result_pa.cast(expected.schema).equals(expected)
