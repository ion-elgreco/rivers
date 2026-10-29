"""PySpark Iceberg IO integration tests.

Every behavior of :class:`rivers.io_handlers.iceberg.pyspark.PySparkIcebergTypeHandler`
is exercised against a real Spark session with the Iceberg Spark runtime. Spark
and PyIceberg address one shared catalog: Spark's JDBC catalog and PyIceberg's
SQL catalog both use the same SQLite database and warehouse.

The Iceberg Spark runtime is a JVM dependency resolved from Maven. Set
``RIVERS_TEST_ICEBERG_SPARK_RUNTIME`` to a Maven coordinate if the default does
not match the installed PySpark line.
"""

from __future__ import annotations

from dataclasses import replace
import json
import logging
import os
from datetime import datetime
from unittest.mock import patch
from uuid import uuid4

import pytest

pytest.importorskip("pyiceberg")
pytest.importorskip("pyspark")

import rivers as rs  # noqa: E402
from pyiceberg.schema import Schema  # noqa: E402
from pyiceberg.transforms import DayTransform, IdentityTransform  # noqa: E402
from pyiceberg.types import LongType, NestedField, StringType  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql.dataframe import DataFrame as SparkDataFrame  # noqa: E402
from rivers.io_handlers.iceberg import IcebergIOHandler  # noqa: E402
from rivers.io_handlers.iceberg.config import UpsertConfig  # noqa: E402
from rivers.io_handlers.iceberg.pyspark import PySparkIcebergTypeHandler  # noqa: E402

from .helpers import (  # noqa: E402
    make_daily_partition,
    make_multi_partition,
    make_partition,
)

pytestmark = [pytest.mark.spark_test, pytest.mark.xdist_group("spark")]

_CATALOG = "rivers_spark_test"
_SQLITE_JDBC = "org.xerial:sqlite-jdbc:3.46.1.3"


def _iceberg_runtime_coordinate() -> str:
    """The Iceberg Spark runtime for the installed PySpark line."""
    import pyspark

    major, minor = (int(part) for part in pyspark.__version__.split(".")[:2])
    runtime_line = f"{major}.{minor}" if major < 4 else "4.0"
    scala_line = "2.12" if major < 4 else "2.13"
    return os.environ.get(
        "RIVERS_TEST_ICEBERG_SPARK_RUNTIME",
        f"org.apache.iceberg:iceberg-spark-runtime-{runtime_line}_{scala_line}:1.10.0",
    )


@pytest.fixture(scope="module")
def catalog_db(tmp_path_factory) -> str:
    """The SQLite file both Spark (JDBC catalog) and PyIceberg (SQL catalog) use."""
    return str(tmp_path_factory.mktemp("iceberg-spark-catalog") / "catalog.db")


@pytest.fixture(scope="module")
def spark(tmp_path_factory, catalog_db: str) -> SparkSession:  # type: ignore[return]
    """Spark with an Iceberg JDBC catalog that PyIceberg's SQL catalog shares."""
    warehouse = str(tmp_path_factory.mktemp("iceberg-spark-warehouse"))
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("rivers-pyspark-iceberg-tests")
        .config(
            "spark.jars.packages", f"{_iceberg_runtime_coordinate()},{_SQLITE_JDBC}"
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{_CATALOG}", "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            f"spark.sql.catalog.{_CATALOG}.catalog-impl",
            "org.apache.iceberg.jdbc.JdbcCatalog",
        )
        .config(
            f"spark.sql.catalog.{_CATALOG}.uri",
            f"jdbc:sqlite:{catalog_db}",
        )
        .config(f"spark.sql.catalog.{_CATALOG}.jdbc.busy_timeout", "60000")
        .config(f"spark.sql.catalog.{_CATALOG}.jdbc.schema-version", "V1")
        .config(f"spark.sql.catalog.{_CATALOG}.warehouse", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.storeAssignmentPolicy", "ANSI")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    session = builder.getOrCreate()
    assert session.conf.get(f"spark.sql.catalog.{_CATALOG}.warehouse") == warehouse
    session.sparkContext.setLogLevel("ERROR")
    logging.getLogger("py4j").setLevel(logging.ERROR)
    yield session
    session.stop()


@pytest.fixture(scope="module")
def warehouse(spark: SparkSession) -> str:
    """The warehouse path Spark's catalog writes to; PyIceberg reads the same one."""
    return spark.conf.get(f"spark.sql.catalog.{_CATALOG}.warehouse")


def _handler(
    spark: SparkSession,
    warehouse: str,
    catalog_db: str,
    namespace: str = "ns",
    **kwargs,
) -> IcebergIOHandler:
    return IcebergIOHandler(
        namespace=namespace,
        catalog_name=_CATALOG,
        catalog_properties={
            "type": "sql",
            "uri": f"sqlite:///{catalog_db}",
            "warehouse": warehouse,
            "connection.connect_args": '{"timeout": 60}',
            "poolclass": "nullpool",
        },
        handler_config={"spark_session": spark, "spark_catalog": _CATALOG},
        **kwargs,
    )


def _name(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:12]}"


def _write(handler: IcebergIOHandler, data: SparkDataFrame, name: str, **context):
    ctx = rs.OutputContext(asset_name=name, **context)
    handler.handle_output(ctx, data)
    return ctx


def _read(handler: IcebergIOHandler, name: str, **context) -> SparkDataFrame:
    return handler.load_input(
        rs.InputContext(
            asset_name=name,
            downstream_asset="consumer",
            type_hint=SparkDataFrame,
            **context,
        )
    )


def _rows(data: SparkDataFrame) -> list[tuple]:
    return sorted(tuple(row) for row in data.collect())


def _create_identified_table(handler, name):
    with handler.pyiceberg_catalog() as catalog:
        catalog.create_namespace_if_not_exists(("ns",))
        catalog.create_table(
            ("ns", name),
            schema=Schema(
                NestedField(1, "id", LongType(), required=True),
                NestedField(2, "name", StringType(), required=False),
                identifier_field_ids=[1],
            ),
        )


# ---------------------------------------------------------------------------
# Round trips, write modes, and output metadata
# ---------------------------------------------------------------------------


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_spark_catalog_name_is_passed_through_handler_config(
    spark, warehouse, catalog_db
):
    handlers = IcebergIOHandler.type_handlers(
        {"spark_session": spark, "spark_catalog": _CATALOG}
    )
    assert isinstance(handlers[DataFrame], PySparkIcebergTypeHandler)
    assert handlers[DataFrame]._spark_catalog == _CATALOG


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_spark_dataframe_round_trip_and_metadata(spark, warehouse, catalog_db):
    handler = _handler(spark, warehouse, catalog_db)
    name = _name("round_trip")
    data = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])

    ctx = _write(handler, data, name)
    result = _read(handler, name)

    assert isinstance(result, SparkDataFrame)
    assert _rows(result) == [(1, "a"), (2, "b")]
    assert ctx.output_metadata["iceberg/table"].raw_value() == f"ns.{name}"
    assert ctx.output_metadata["iceberg/mode"].raw_value() == "overwrite"
    assert ctx.output_metadata["iceberg/num_rows"].raw_value() == 2
    assert ctx.output_metadata["iceberg/size_bytes"].raw_value() > 0
    assert ctx.output_metadata["iceberg/metadata_location"].raw_value()
    assert "rivers/schema" in ctx.output_metadata
    snapshot_id = ctx.output_metadata["iceberg/snapshot_id"].raw_value()
    assert isinstance(snapshot_id, int)
    assert ctx.drain_data_version() == str(snapshot_id)


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_append_mode_accumulates_rows(spark, warehouse, catalog_db):
    name = _name("append")
    handler = _handler(spark, warehouse, catalog_db, mode="append")
    _write(handler, spark.createDataFrame([(1,), (2,)], ["id"]), name)
    ctx = _write(handler, spark.createDataFrame([(3,)], ["id"]), name)

    assert _rows(_read(handler, name)) == [(1,), (2,), (3,)]
    assert ctx.output_metadata["iceberg/mode"].raw_value() == "append"


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_overwrite_mode_replaces_whole_table(spark, warehouse, catalog_db):
    name = _name("full_overwrite")
    handler = _handler(spark, warehouse, catalog_db, mode="overwrite")
    _write(handler, spark.createDataFrame([(1,), (2,)], ["id"]), name)
    _write(handler, spark.createDataFrame([(7,)], ["id"]), name)

    assert _rows(_read(handler, name)) == [(7,)]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_mode_can_be_overridden_per_asset(spark, warehouse, catalog_db):
    name = _name("mode_override")
    handler = _handler(spark, warehouse, catalog_db, mode="overwrite")
    _write(handler, spark.createDataFrame([(1,)], ["id"]), name)
    _write(
        handler,
        spark.createDataFrame([(2,)], ["id"]),
        name,
        asset_metadata={"iceberg/mode": "append"},
    )

    assert _rows(_read(handler, name)) == [(1,), (2,)]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_create_table_on_first_write(spark, warehouse, catalog_db):
    name = _name("create")
    handler = _handler(spark, warehouse, catalog_db)
    _write(handler, spark.createDataFrame([(1,)], ["id"]), name)

    table = handler.load_table(name)
    assert table.current_snapshot() is not None
    assert table.spec().is_unpartitioned()


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_snapshot_properties_recorded_on_snapshot_summary(spark, warehouse, catalog_db):
    name = _name("snapshot_props")
    handler = _handler(
        spark,
        warehouse,
        catalog_db,
        mode="append",
        snapshot_properties={"pipeline": "nightly"},
    )
    ctx = _write(handler, spark.createDataFrame([(1,)], ["id"]), name)

    snapshot = handler.load_table(name).snapshot_by_id(
        ctx.output_metadata["iceberg/snapshot_id"].raw_value()
    )
    assert snapshot is not None
    assert snapshot.summary["pipeline"] == "nightly"


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_table_properties_applied_on_create_and_updated_later(
    spark, warehouse, catalog_db
):
    name = _name("table_props")
    first = _handler(
        spark, warehouse, catalog_db, table_properties={"owner.team": "data"}
    )
    _write(first, spark.createDataFrame([(1,)], ["id"]), name)
    assert first.load_table(name).properties["owner.team"] == "data"

    second = _handler(
        spark,
        warehouse,
        catalog_db,
        mode="append",
        table_properties={"owner.team": "platform"},
    )
    _write(second, spark.createDataFrame([(2,)], ["id"]), name)
    assert second.load_table(name).properties["owner.team"] == "platform"


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_nested_namespace_round_trip(spark, warehouse, catalog_db):
    name = _name("nested")
    handler = _handler(spark, warehouse, catalog_db, namespace="team.sales")
    ctx = _write(handler, spark.createDataFrame([(1,)], ["id"]), name)

    assert ctx.output_metadata["iceberg/table"].raw_value() == f"team.sales.{name}"
    assert _rows(_read(handler, name)) == [(1,)]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_column_names_needing_quotes_round_trip(spark, warehouse, catalog_db):
    name = _name("quoted_cols")
    handler = _handler(spark, warehouse, catalog_db)
    _write(handler, spark.createDataFrame([(1, "x")], ["order id", "a`b"]), name)

    assert _rows(_read(handler, name)) == [(1, "x")]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_null_values_preserved(spark, warehouse, catalog_db):
    name = _name("nulls")
    handler = _handler(spark, warehouse, catalog_db)
    _write(
        handler, spark.createDataFrame([(1, None), (2, 3.0)], "id long, v double"), name
    )

    rows = sorted(_read(handler, name).collect(), key=lambda row: row["id"])
    assert [row["v"] for row in rows] == [None, 3.0]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_large_dataframe_row_count_preserved(spark, warehouse, catalog_db):
    name = _name("large")
    handler = _handler(spark, warehouse, catalog_db)
    ctx = _write(handler, spark.range(5_000).toDF("i"), name)

    assert ctx.output_metadata["iceberg/num_rows"].raw_value() == 5_000
    assert _read(handler, name).count() == 5_000


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_empty_dataframe_creates_table_with_schema(spark, warehouse, catalog_db):
    name = _name("empty_create")
    handler = _handler(spark, warehouse, catalog_db)

    empty = spark.createDataFrame([], "id long, name string")
    ctx = _write(handler, empty, name)

    assert _read(handler, name).count() == 0
    assert _read(handler, name).columns == ["id", "name"]
    assert ctx.output_metadata["iceberg/num_rows"].raw_value() == 0


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_empty_append_preserves_existing_rows(spark, warehouse, catalog_db):
    name = _name("empty_append")
    handler = _handler(spark, warehouse, catalog_db, mode="append")

    _write(handler, spark.createDataFrame([(1,), (2,)], ["id"]), name)

    empty = spark.createDataFrame([], "id long")
    _write(handler, empty, name)

    assert _rows(_read(handler, name)) == [(1,), (2,)]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_empty_overwrite_clears_existing_rows(spark, warehouse, catalog_db):
    name = _name("empty_overwrite")
    handler = _handler(spark, warehouse, catalog_db, mode="overwrite")

    _write(handler, spark.createDataFrame([(1,), (2,)], ["id"]), name)

    empty = spark.createDataFrame([], "id long")
    _write(handler, empty, name)

    assert _read(handler, name).count() == 0


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_successive_writes_register_new_snapshots(spark, warehouse, catalog_db):
    name = _name("snapshot_sequence")
    handler = _handler(spark, warehouse, catalog_db, mode="append")

    first = _write(
        handler,
        spark.createDataFrame([(1,)], ["id"]),
        name,
    )
    first_id = first.output_metadata["iceberg/snapshot_id"].raw_value()

    second = _write(
        handler,
        spark.createDataFrame([(2,)], ["id"]),
        name,
    )
    second_id = second.output_metadata["iceberg/snapshot_id"].raw_value()

    assert first_id != second_id
    assert second.drain_data_version() == str(second_id)
    assert _rows(_read(handler, name)) == [(1,), (2,)]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_validation_failure_does_not_register_data_version(
    spark, warehouse, catalog_db
):
    name = _name("failed_write_version")
    handler = _handler(spark, warehouse, catalog_db)

    _write(handler, spark.createDataFrame([(1,)], ["id"]), name)

    invalid_handler = _handler(
        spark,
        warehouse,
        catalog_db,
        mode="upsert",
        schema_mode="merge",
        upsert_config=UpsertConfig(join_cols=["id"]),
    )

    ctx = rs.OutputContext(asset_name=name)
    request = invalid_handler._write_request(ctx, invalid_handler.pyiceberg_catalog())
    data = spark.createDataFrame([(2,)], ["id"])

    with pytest.raises(ValueError, match="does not support schema_mode"):
        PySparkIcebergTypeHandler(
            spark_session=spark,
            spark_catalog=_CATALOG,
        ).handle_output(ctx, data, request)

    assert ctx.drain_data_version() is None
    assert _rows(_read(handler, name)) == [(1,)]


# ---------------------------------------------------------------------------
# Reads: projection, time travel, partition filters
# ---------------------------------------------------------------------------


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_column_projection_and_snapshot_time_travel(spark, warehouse, catalog_db):
    name = _name("time_travel")
    handler = _handler(spark, warehouse, catalog_db)
    first = _write(handler, spark.createDataFrame([(1, "a")], ["id", "name"]), name)
    snapshot_id = first.output_metadata["iceberg/snapshot_id"].raw_value()
    _write(
        handler,
        spark.createDataFrame([(2, "b")], ["id", "name"]),
        name,
        asset_metadata={"iceberg/mode": "append"},
    )

    old = _read(
        handler,
        name,
        asset_metadata={
            "iceberg/snapshot_id": str(snapshot_id),
            "iceberg/columns": '["id"]',
        },
    )
    assert old.columns == ["id"]
    assert _rows(old) == [(1,)]
    assert _rows(_read(handler, name)) == [(1, "a"), (2, "b")]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_read_filters_to_requested_partition(spark, warehouse, catalog_db):
    name = _name("partition_read")
    handler = _handler(spark, warehouse, catalog_db)
    metadata = {"iceberg/partition_expr": "day"}
    for day, value in [("2024-01-01", 10), ("2024-01-02", 20)]:
        _write(
            handler,
            spark.createDataFrame([(day, value)], ["day", "value"]),
            name,
            partition=make_partition(day),
            asset_metadata=metadata,
        )

    result = _read(
        handler, name, partition=make_partition("2024-01-02"), asset_metadata=metadata
    )
    assert _rows(result) == [("2024-01-02", 20)]


# ---------------------------------------------------------------------------
# Partitioned writes
# ---------------------------------------------------------------------------


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_identity_partition_isolation(spark, warehouse, catalog_db):
    name = _name("identity_iso")
    handler = _handler(spark, warehouse, catalog_db)
    metadata = {"iceberg/partition_expr": "day"}
    _write(
        handler,
        spark.createDataFrame([("2024-01-01", 10)], ["day", "value"]),
        name,
        partition=make_partition("2024-01-01"),
        asset_metadata=metadata,
    )
    _write(
        handler,
        spark.createDataFrame([("2024-01-02", 20)], ["day", "value"]),
        name,
        partition=make_partition("2024-01-02"),
        asset_metadata=metadata,
    )

    assert _rows(_read(handler, name)) == [("2024-01-01", 10), ("2024-01-02", 20)]
    spec = handler.load_table(name).spec()
    assert isinstance(spec.fields[0].transform, IdentityTransform)


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_partition_overwrite_replaces_only_that_partition(spark, warehouse, catalog_db):
    name = _name("partition_overwrite")
    handler = _handler(spark, warehouse, catalog_db)
    metadata = {"iceberg/partition_expr": "day"}
    first = make_partition("2024-01-01")
    second = make_partition("2024-01-02")
    _write(
        handler,
        spark.createDataFrame([("2024-01-01", 10)], ["day", "value"]),
        name,
        partition=first,
        asset_metadata=metadata,
    )
    _write(
        handler,
        spark.createDataFrame([("2024-01-02", 20)], ["day", "value"]),
        name,
        partition=second,
        asset_metadata=metadata,
    )
    _write(
        handler,
        spark.createDataFrame([("2024-01-01", 11)], ["day", "value"]),
        name,
        partition=first,
        asset_metadata=metadata,
    )

    assert _rows(_read(handler, name)) == [("2024-01-01", 11), ("2024-01-02", 20)]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_backfill_overwrite_replaces_only_listed_keys(spark, warehouse, catalog_db):
    name = _name("backfill")
    handler = _handler(spark, warehouse, catalog_db)
    metadata = {"iceberg/partition_expr": "key"}
    for key, value in [("a", 1), ("b", 1), ("c", 1)]:
        _write(
            handler,
            spark.createDataFrame([(key, value)], ["key", "val"]),
            name,
            partition=make_partition(key),
            asset_metadata=metadata,
        )

    _write(
        handler,
        spark.createDataFrame([("a", 99), ("b", 99)], ["key", "val"]),
        name,
        partition=make_partition(["a", "b"]),
        asset_metadata=metadata,
    )

    assert _rows(_read(handler, name)) == [("a", 99), ("b", 99), ("c", 1)]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_multi_dimension_partition(spark, warehouse, catalog_db):
    name = _name("multi_dim")
    handler = _handler(spark, warehouse, catalog_db)
    metadata = {
        "iceberg/partition_expr": json.dumps({"region": "region", "env": "env"})
    }
    us_prod = make_multi_partition({"region": "us", "env": "prod"})
    for region, value in [("us", 1), ("eu", 2)]:
        partition = make_multi_partition({"region": region, "env": "prod"})
        _write(
            handler,
            spark.createDataFrame([(region, "prod", value)], ["region", "env", "val"]),
            name,
            partition=partition,
            asset_metadata=metadata,
        )
    _write(
        handler,
        spark.createDataFrame([("us", "prod", 99)], ["region", "env", "val"]),
        name,
        partition=us_prod,
        asset_metadata=metadata,
    )

    assert _rows(_read(handler, name)) == [("eu", "prod", 2), ("us", "prod", 99)]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_daily_time_window_partition_uses_day_transform(spark, warehouse, catalog_db):
    name = _name("daily")
    handler = _handler(spark, warehouse, catalog_db)
    metadata = {"iceberg/partition_expr": "ts"}
    day_one = make_daily_partition("2024-01-01")
    day_two = make_daily_partition("2024-01-02")
    _write(
        handler,
        spark.createDataFrame([(datetime(2024, 1, 1, 10), 1)], ["ts", "val"]),
        name,
        partition=day_one,
        asset_metadata=metadata,
    )
    _write(
        handler,
        spark.createDataFrame([(datetime(2024, 1, 2, 10), 2)], ["ts", "val"]),
        name,
        partition=day_two,
        asset_metadata=metadata,
    )
    _write(
        handler,
        spark.createDataFrame([(datetime(2024, 1, 1, 15), 9)], ["ts", "val"]),
        name,
        partition=day_one,
        asset_metadata=metadata,
    )

    spec = handler.load_table(name).spec()
    assert isinstance(spec.fields[0].transform, DayTransform)
    values = sorted(row["val"] for row in _read(handler, name).collect())
    assert values == [2, 9]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_partitioned_write_requires_partition_expr(spark, warehouse, catalog_db):
    handler = _handler(spark, warehouse, catalog_db)

    # Case 1: A new table cannot be created from a partitioned write
    # without the partition expression.
    new_name = _name("missing_expr_new_table")

    with pytest.raises(ValueError, match="partition_expr"):
        _write(
            handler,
            spark.createDataFrame([("2024-01-01", 1)], ["day", "v"]),
            new_name,
            partition=make_partition("2024-01-01"),
        )

    # Verify the failed write did not create the table.
    with pytest.raises(Exception):
        handler.load_table(new_name)

    # Case 2: An existing partitioned table also requires the expression
    # on subsequent partitioned writes.
    existing_name = _name("missing_expr_existing_table")
    metadata = {"iceberg/partition_expr": "day"}

    _write(
        handler,
        spark.createDataFrame([("2024-01-01", 1)], ["day", "v"]),
        existing_name,
        partition=make_partition("2024-01-01"),
        asset_metadata=metadata,
    )

    with pytest.raises(ValueError, match="partition_expr"):
        _write(
            handler,
            spark.createDataFrame([("2024-01-02", 2)], ["day", "v"]),
            existing_name,
            partition=make_partition("2024-01-02"),
        )

    # The failed write must not change the existing table's data.
    assert _rows(_read(handler, existing_name)) == [("2024-01-01", 1)]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_partition_overwrite_rejects_schema_overwrite(spark, warehouse, catalog_db):
    name = _name("partition_schema_overwrite")
    metadata = {"iceberg/partition_expr": "day"}

    # Create the table using a normal partitioned write.
    initial_handler = _handler(spark, warehouse, catalog_db)
    _write(
        initial_handler,
        spark.createDataFrame([("2024-01-01", 1)], ["day", "v"]),
        name,
        partition=make_partition("2024-01-01"),
        asset_metadata=metadata,
    )

    # Configure schema overwrite only for the operation under test.
    handler = _handler(
        spark,
        warehouse,
        catalog_db,
        mode="overwrite",
        schema_mode="overwrite",
    )

    with pytest.raises(
        ValueError,
        match="cannot be combined with a partition overwrite",
    ):
        _write(
            handler,
            spark.createDataFrame([("2024-01-01", 2)], ["day", "v"]),
            name,
            partition=make_partition("2024-01-01"),
            asset_metadata=metadata,
        )


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_daily_partition_boundary_uses_utc_day(spark, warehouse, catalog_db):
    name = _name("daily_boundary")
    handler = _handler(spark, warehouse, catalog_db)
    metadata = {"iceberg/partition_expr": "ts"}

    data = spark.createDataFrame(
        [
            (datetime(2024, 1, 1, 23, 59, 59), 1),
            (datetime(2024, 1, 2, 0, 0, 0), 2),
        ],
        "ts timestamp, value long",
    )

    # Both rows are written together, using the daily partition definition.
    # This verifies that Spark and Iceberg can partition timestamps across
    # the UTC midnight boundary.
    _write(
        handler,
        data,
        name,
        partition=make_daily_partition("2024-01-01"),
        asset_metadata=metadata,
    )

    result = _read(handler, name)
    assert sorted(row["value"] for row in result.collect()) == [1, 2]


# ---------------------------------------------------------------------------
# Schema evolution
# ---------------------------------------------------------------------------


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_schema_merge_adds_columns(spark, warehouse, catalog_db):
    name = _name("schema_merge")
    handler = _handler(spark, warehouse, catalog_db, mode="append", schema_mode="merge")
    _write(handler, spark.createDataFrame([(1,)], ["id"]), name)
    _write(handler, spark.createDataFrame([(2, "b")], ["id", "name"]), name)

    result = _read(handler, name)
    assert result.columns == ["id", "name"]
    assert _rows(result) == [(1, None), (2, "b")]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_schema_overwrite_drops_columns_without_losing_table(
    spark, warehouse, catalog_db
):
    name = _name("schema_overwrite")
    handler = _handler(
        spark, warehouse, catalog_db, mode="overwrite", schema_mode="overwrite"
    )
    _write(handler, spark.createDataFrame([(1, "old")], ["id", "obsolete"]), name)
    _write(handler, spark.createDataFrame([(2, "new")], ["id", "name"]), name)

    result = _read(handler, name)
    assert result.columns == ["id", "name"]
    assert _rows(result) == [(2, "new")]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_schema_overwrite_rejects_dropping_partition_column(
    spark, warehouse, catalog_db
):
    name = _name("drop_partition_column")
    metadata = {"iceberg/partition_expr": "day"}

    initial_handler = _handler(spark, warehouse, catalog_db)

    _write(
        initial_handler,
        spark.createDataFrame(
            [("2024-01-01", 1)],
            ["day", "value"],
        ),
        name,
        partition=make_partition("2024-01-01"),
        asset_metadata=metadata,
    )

    overwrite_handler = _handler(
        spark,
        warehouse,
        catalog_db,
        mode="overwrite",
        schema_mode="overwrite",
    )

    with pytest.raises(ValueError, match="partition"):
        _write(
            overwrite_handler,
            spark.createDataFrame([(2,)], ["value"]),
            name,
        )

    assert _rows(_read(initial_handler, name)) == [("2024-01-01", 1)]


# ---------------------------------------------------------------------------
# Upserts
# ---------------------------------------------------------------------------


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_upsert_updates_matches_and_inserts_new_rows(spark, warehouse, catalog_db):
    name = _name("upsert")
    handler = _handler(
        spark,
        warehouse,
        catalog_db,
        mode="upsert",
        upsert_config=UpsertConfig(join_cols=["id"]),
    )
    _write(handler, spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]), name)
    ctx = _write(
        handler, spark.createDataFrame([(2, "B"), (3, "c")], ["id", "name"]), name
    )

    assert _rows(_read(handler, name)) == [(1, "a"), (2, "B"), (3, "c")]
    assert ctx.output_metadata["iceberg/mode"].raw_value() == "upsert"


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_upsert_insert_only_leaves_matches_untouched(spark, warehouse, catalog_db):
    name = _name("upsert_insert_only")
    handler = _handler(
        spark,
        warehouse,
        catalog_db,
        mode="upsert",
        upsert_config=UpsertConfig(
            join_cols=["id"],
            when_matched_update_all=False,
            when_not_matched_insert_all=True,
        ),
    )
    _write(handler, spark.createDataFrame([(1, "a")], ["id", "name"]), name)
    _write(handler, spark.createDataFrame([(1, "X"), (2, "b")], ["id", "name"]), name)

    assert _rows(_read(handler, name)) == [(1, "a"), (2, "b")]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_upsert_update_only_leaves_unmatched_rows_untouched(
    spark, warehouse, catalog_db
):
    name = _name("upsert_update_only")
    handler = _handler(
        spark,
        warehouse,
        catalog_db,
        mode="upsert",
        upsert_config=UpsertConfig(
            join_cols=["id"],
            when_matched_update_all=True,
            when_not_matched_insert_all=False,
        ),
    )
    _write(handler, spark.createDataFrame([(1, "a")], ["id", "name"]), name)
    _write(handler, spark.createDataFrame([(1, "X"), (2, "b")], ["id", "name"]), name)

    assert _rows(_read(handler, name)) == [(1, "X")]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_upsert_into_missing_table_creates_it(spark, warehouse, catalog_db):
    name = _name("upsert_bootstrap")
    handler = _handler(
        spark,
        warehouse,
        catalog_db,
        mode="upsert",
        upsert_config=UpsertConfig(join_cols=["id"]),
    )
    ctx = _write(
        handler, spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]), name
    )

    assert _rows(_read(handler, name)) == [(1, "a"), (2, "b")]
    assert ctx.output_metadata["iceberg/mode"].raw_value() == "upsert"


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_upsert_rejects_duplicate_source_join_keys(spark, warehouse, catalog_db):
    name = _name("upsert_duplicate_source_keys")

    handler = _handler(
        spark,
        warehouse,
        catalog_db,
        mode="upsert",
        upsert_config=UpsertConfig(join_cols=["id"]),
    )

    # Create a target with a single row for id=1.
    _write(
        handler,
        spark.createDataFrame([(1, "original")], ["id", "name"]),
        name,
    )

    # Two source rows share the same join key.
    source = spark.createDataFrame(
        [(1, "first"), (1, "second")],
        ["id", "name"],
    )

    with pytest.raises(
        ValueError,
        match="source contains duplicate join keys",
    ):
        _write(handler, source, name)

    # The failed upsert must not modify the target.
    assert _rows(_read(handler, name)) == [(1, "original")]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_upsert_defaults_join_cols_to_identifier_fields(spark, warehouse, catalog_db):
    name = _name("upsert_identifier")
    handler = _handler(spark, warehouse, catalog_db, mode="upsert")

    _create_identified_table(handler, name)
    spark.sql(f"REFRESH TABLE {_CATALOG}.ns.{name}")

    table = handler.load_table(name)
    assert table.schema().identifier_field_ids == [1]

    _write(handler, spark.createDataFrame([(1, "a")], ["id", "name"]), name)
    _write(handler, spark.createDataFrame([(1, "A"), (2, "b")], ["id", "name"]), name)

    assert _rows(_read(handler, name)) == [(1, "A"), (2, "b")]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_upsert_without_join_cols_or_identifier_fields_fails(
    spark, warehouse, catalog_db
):
    name = _name("upsert_no_keys")
    base = _handler(spark, warehouse, catalog_db)
    _write(base, spark.createDataFrame([(1, "a")], ["id", "name"]), name)

    upsert = _handler(spark, warehouse, catalog_db, mode="upsert")
    with pytest.raises(
        ValueError, match="iceberg/join_cols or Iceberg identifier fields"
    ):
        _write(upsert, spark.createDataFrame([(1, "A")], ["id", "name"]), name)


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_upsert_join_column_must_exist_in_source_and_target(
    spark, warehouse, catalog_db
):
    name = _name("upsert_missing_join")
    base = _handler(spark, warehouse, catalog_db)
    _write(base, spark.createDataFrame([(1, "a")], ["id", "name"]), name)

    upsert = _handler(
        spark,
        warehouse,
        catalog_db,
        mode="upsert",
        upsert_config=UpsertConfig(join_cols=["nope"]),
    )
    with pytest.raises(ValueError, match="must exist in both source and target"):
        _write(upsert, spark.createDataFrame([(1, "A")], ["id", "name"]), name)


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_upsert_needs_at_least_one_clause(spark, warehouse, catalog_db):
    name = _name("upsert_no_clause")
    base = _handler(spark, warehouse, catalog_db)
    _write(base, spark.createDataFrame([(1, "a")], ["id", "name"]), name)

    upsert = _handler(
        spark,
        warehouse,
        catalog_db,
        mode="upsert",
        upsert_config=UpsertConfig(
            join_cols=["id"],
            when_matched_update_all=False,
            when_not_matched_insert_all=False,
        ),
    )
    with pytest.raises(ValueError, match="at least one matched/insert clause"):
        _write(upsert, spark.createDataFrame([(1, "A")], ["id", "name"]), name)


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_upsert_join_on_column_with_space(spark, warehouse, catalog_db):
    name = _name("upsert_quoted")
    handler = _handler(
        spark,
        warehouse,
        catalog_db,
        mode="upsert",
        upsert_config=UpsertConfig(join_cols=["order id"]),
    )
    _write(handler, spark.createDataFrame([(1, "a")], ["order id", "name"]), name)
    _write(
        handler, spark.createDataFrame([(1, "A"), (2, "b")], ["order id", "name"]), name
    )

    assert _rows(_read(handler, name)) == [(1, "A"), (2, "b")]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_upsert_rejects_schema_mode(spark, warehouse, catalog_db):
    name = _name("upsert_schema")
    base = _handler(spark, warehouse, catalog_db)
    _write(base, spark.createDataFrame([(1, "a")], ["id", "name"]), name)

    upsert = _handler(
        spark,
        warehouse,
        catalog_db,
        mode="upsert",
        schema_mode="merge",
        upsert_config=UpsertConfig(join_cols=["id"]),
    )
    with pytest.raises(ValueError, match="does not support schema_mode"):
        _write(
            upsert,
            spark.createDataFrame([(2, "b", 3.0)], ["id", "name", "extra"]),
            name,
        )


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_upsert_rejects_snapshot_properties(spark, warehouse, catalog_db):
    name = _name("upsert_snapshot")
    base = _handler(spark, warehouse, catalog_db)
    _write(base, spark.createDataFrame([(1, "a")], ["id", "name"]), name)

    upsert = _handler(
        spark,
        warehouse,
        catalog_db,
        mode="upsert",
        snapshot_properties={"run": "1"},
        upsert_config=UpsertConfig(join_cols=["id"]),
    )
    with pytest.raises(ValueError, match="cannot attach custom snapshot properties"):
        _write(upsert, spark.createDataFrame([(1, "A")], ["id", "name"]), name)


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_upsert_defaults_join_cols_to_composite_identifier_fields(
    spark, warehouse, catalog_db
):
    name = _name("upsert_composite_identifier")
    handler = _handler(spark, warehouse, catalog_db, mode="upsert")

    with handler.pyiceberg_catalog() as catalog:
        catalog.create_namespace_if_not_exists(("ns",))
        catalog.create_table(
            ("ns", name),
            schema=Schema(
                NestedField(1, "tenant_id", LongType(), required=True),
                NestedField(2, "record_id", LongType(), required=True),
                NestedField(3, "value", StringType(), required=False),
                identifier_field_ids=[1, 2],
            ),
        )

    spark.sql(f"REFRESH TABLE {_CATALOG}.ns.{name}")

    _write(
        handler,
        spark.createDataFrame(
            [(1, 10, "old"), (1, 11, "keep")],
            ["tenant_id", "record_id", "value"],
        ),
        name,
    )

    _write(
        handler,
        spark.createDataFrame(
            [(1, 10, "updated"), (2, 10, "new")],
            ["tenant_id", "record_id", "value"],
        ),
        name,
    )

    assert _rows(_read(handler, name)) == [
        (1, 10, "updated"),
        (1, 11, "keep"),
        (2, 10, "new"),
    ]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_upsert_rejects_null_source_join_keys(spark, warehouse, catalog_db):
    name = _name("upsert_null_keys")
    handler = _handler(
        spark,
        warehouse,
        catalog_db,
        mode="upsert",
        upsert_config=UpsertConfig(join_cols=["id"]),
    )

    _write(
        handler,
        spark.createDataFrame([(1, "original")], ["id", "name"]),
        name,
    )

    source = spark.createDataFrame(
        [(None, "invalid")],
        "id long, name string",
    )

    with pytest.raises(ValueError, match="null.*join keys"):
        _write(handler, source, name)

    assert _rows(_read(handler, name)) == [(1, "original")]


# ---------------------------------------------------------------------------
# Handler-level behavior: sessions, input validation, and type routing
# ---------------------------------------------------------------------------


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_explicit_session_is_used(spark):
    handler = PySparkIcebergTypeHandler(spark_session=spark, spark_catalog=_CATALOG)
    assert handler._get_spark_session() is spark


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_missing_session_raises_setup_guidance(monkeypatch):
    monkeypatch.setattr(SparkSession, "getActiveSession", staticmethod(lambda: None))
    with pytest.raises(
        RuntimeError, match="active, Iceberg-configured SparkSession"
    ) as info:
        PySparkIcebergTypeHandler()._get_spark_session()
    assert "handler_config" in str(info.value)
    assert "rivers[iceberg-pyspark]" in str(info.value)


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_active_session_is_used_when_none_is_configured(spark):
    handler = PySparkIcebergTypeHandler()
    assert handler._get_spark_session() is spark


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_handle_output_rejects_non_dataframe(spark, warehouse, catalog_db):
    io = _handler(spark, warehouse, catalog_db)
    ctx = rs.OutputContext(asset_name=_name("not_df"))
    request = io._write_request(ctx, io.pyiceberg_catalog())
    with pytest.raises(TypeError, match="must be a PySpark DataFrame"):
        PySparkIcebergTypeHandler(spark_session=spark).handle_output(
            ctx, [1, 2], request
        )


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_supported_types_are_pyspark_dataframes(spark):
    assert list(PySparkIcebergTypeHandler().supported_types) == [DataFrame]


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_missing_pyspark_handler_names_the_extra(spark, warehouse, catalog_db):
    io = _handler(spark, warehouse, catalog_db)
    with patch.object(
        IcebergIOHandler, "type_handlers", staticmethod(lambda *args, **kwargs: {})
    ):
        with pytest.raises(TypeError, match=r"rivers\[iceberg-pyspark\]"):
            io._resolve_handler(DataFrame)


@pytest.mark.parametrize("mode", ["invalid", "", "merge"])
@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_handle_output_rejects_unsupported_write_mode(
    spark, warehouse, catalog_db, mode
):
    handler = _handler(spark, warehouse, catalog_db)
    ctx = rs.OutputContext(asset_name=_name("invalid_mode"))
    request = handler._write_request(ctx, handler.pyiceberg_catalog())
    request = replace(request, mode=mode)

    data = spark.createDataFrame([(1,)], ["id"])

    with pytest.raises(ValueError, match="Unsupported Iceberg write mode"):
        PySparkIcebergTypeHandler(
            spark_session=spark,
            spark_catalog=_CATALOG,
        ).handle_output(ctx, data, request)


@pytest.mark.parametrize("schema_mode", ["invalid", "merge_all"])
@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_handle_output_rejects_unsupported_schema_mode(
    spark, warehouse, catalog_db, schema_mode
):
    handler = _handler(spark, warehouse, catalog_db)
    ctx = rs.OutputContext(asset_name=_name("invalid_schema_mode"))
    request = handler._write_request(ctx, handler.pyiceberg_catalog())
    request = replace(request, schema_mode=schema_mode)

    data = spark.createDataFrame([(1,)], ["id"])

    with pytest.raises(ValueError, match="Unsupported Iceberg schema mode"):
        PySparkIcebergTypeHandler(
            spark_session=spark,
            spark_catalog=_CATALOG,
        ).handle_output(ctx, data, request)


@pytest.mark.iceberg_spark
@pytest.mark.spark_test
def test_handle_output_rejects_schema_overwrite_with_append(
    spark, warehouse, catalog_db
):
    handler = _handler(
        spark,
        warehouse,
        catalog_db,
        mode="append",
        schema_mode="overwrite",
    )
    ctx = rs.OutputContext(asset_name=_name("schema_overwrite_append"))
    with pytest.raises(
        ValueError,
        match=r"schema_mode 'overwrite' drops columns, so it needs mode 'overwrite', not 'append'",
    ):
        request = handler._write_request(ctx, handler.pyiceberg_catalog())
        data = spark.createDataFrame([(1,)], ["id"])
        PySparkIcebergTypeHandler(
            spark_session=spark,
            spark_catalog=_CATALOG,
        ).handle_output(ctx, data, request)
