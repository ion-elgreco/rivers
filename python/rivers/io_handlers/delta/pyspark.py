"""PySpark to Delta Lake bridge."""

from __future__ import annotations

import json
import time
from collections.abc import Sequence
from typing import Any, Literal, get_args

import pyarrow as pa
from arro3.core import RecordBatchReader
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampType,
)

from rivers._core import MetadataValue, OutputContext
from rivers.io_handlers.delta.base import DeltaTypeHandler
from rivers.io_handlers.delta.config import MergeConfig
from rivers.io_handlers.delta.merge import _merge_execute_spark

# Mapping between Arrow and Spark types.
_SPARK_TO_ARROW_MAP = {
    LongType: lambda _: pa.int64(),
    IntegerType: lambda _: pa.int32(),
    ShortType: lambda _: pa.int16(),
    ByteType: lambda _: pa.int8(),
    DoubleType: lambda _: pa.float64(),
    FloatType: lambda _: pa.float32(),
    BooleanType: lambda _: pa.bool_(),
    StringType: lambda _: pa.string(),
    BinaryType: lambda _: pa.binary(),
    DateType: lambda _: pa.date32(),
}

SparkWriteMode = Literal["overwrite", "append", "error", "ignore"]


def _to_spark_write_mode(mode: str) -> str:
    """Returns Spark writer compatible mode from DeltaWriteMode."""

    if mode not in get_args(SparkWriteMode):
        raise ValueError(f"Spark does not support the write mode {mode}.")
    return mode


def _spark_to_arrow_type(dt):
    """Returns Arrow-equivalent of Spark type ``dt``."""

    if isinstance(dt, DecimalType):
        return pa.decimal128(dt.precision, dt.scale)

    if isinstance(dt, TimestampType):
        return pa.timestamp("us")

    for spark_type, fn in _SPARK_TO_ARROW_MAP.items():
        if isinstance(dt, spark_type):
            return fn(dt)

    raise TypeError(f"Unsupported Spark type: {dt}")


def _get_or_create_spark(spark: SparkSession | None = None) -> SparkSession:
    """Return spark if provided or create a new one.

    Resolution order:
    1. Caller-supplied session is returned as-is; no configuration is
       validated. So pass a fully configured session when you need specific
       cluster settings, credentials, or catalog options.
    2. The currently active session via
       :func:`~pyspark.sql.SparkSession.getActiveSession`.
    3. A new local ``local[*]`` session bootstrapped with
       ``delta.configure_spark_with_delta_pip`` from the ``delta-spark`` package.

    Raises:
        ImportError: When no active session exists and ``delta-spark`` is not
            installed.  Install the extra ``pip install rivers[delta-pyspark]``
            and start a session before calling the handler, or pass one
            explicitly via ``PySparkTypeHandler(spark=...)``.
    """
    if spark is not None:
        return spark

    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    try:
        from delta import configure_spark_with_delta_pip  # type: ignore[import-untyped]

        builder = (
            SparkSession.builder.appName("rivers-delta")
            .master("local[*]")
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError as exc:
        raise ImportError(
            "No active SparkSession found and delta-spark is not installed. "
            "Either start and configure a SparkSession before using this handler, "
            "or install the required extra: pip install rivers[delta-pyspark]. "
            "To pass a pre-built session explicitly, construct the handler as "
            "PySparkTypeHandler(spark=my_session)."
        ) from exc


def write_delta_spark(
    context: OutputContext,
    spark_df: DataFrame,
    delta_write_mode: str,
    meta: dict[str, str],
    table_uri: str,
    predicate: str | None,
    partition_by: list[str] | None,
    schema_mode: str | None,
    merge_config: MergeConfig | None,
    table_config: dict[str, str] | None,
) -> None:
    """Handles writes via spark session with delta-spark."""

    start_time = time.monotonic()
    spark = spark_df.sparkSession
    merge_stats: dict[str, Any] | None = None

    if delta_write_mode == "merge":
        merge_predicate_override = meta.get("delta/merge_predicate")
        merge_stats = _merge_execute_spark(
            uri=table_uri,
            sdf=spark_df,
            merge_config=merge_config,
            partition_predicate=predicate,
            merge_predicate_override=merge_predicate_override,
        )
        num_rows = merge_stats.get("numOutputRows")
        size_bytes = merge_stats.get("numOutputBytes")
        version = merge_stats.get("version")
    else:
        spark_writer = spark_df.write.format("delta").mode(
            _to_spark_write_mode(delta_write_mode)
        )

        if partition_by:
            spark_writer = spark_writer.partitionBy(*partition_by)
        if predicate and delta_write_mode == "overwrite":
            spark_writer = spark_writer.option("replaceWhere", predicate)

        if schema_mode == "merge":
            spark_writer = spark_writer.option("mergeSchema", "true")
        elif schema_mode == "overwrite":
            spark_writer = spark_writer.option("overwriteSchema", "true")

        if table_config:
            for k, v in table_config.items():
                spark_writer = spark_writer.option(k, v)

        spark_writer.save(table_uri)

        hist = DeltaTable.forPath(spark, table_uri).history(1).collect()[0]
        metrics = hist.operationMetrics or {}
        num_rows = metrics.get("numOutputRows")
        size_bytes = metrics.get("numOutputBytes")
        version = hist.version

    duration = time.monotonic() - start_time
    arrow_schema = pa.schema(
        [pa.field(f.name, _spark_to_arrow_type(f.dataType)) for f in spark_df.schema]
    )
    output_meta: dict[str, Any] = {
        "delta/table_uri": table_uri,
        "delta/mode": delta_write_mode,
        "delta/num_rows": int(num_rows) if num_rows is not None else None,
        "delta/size_bytes": int(size_bytes) if size_bytes is not None else None,
        "delta/write_duration_s": round(duration, 6),
        "delta/version": version,
        "rivers/schema": MetadataValue.schema(arrow_schema),
    }
    if merge_stats is not None:
        output_meta["delta/num_output_rows"] = merge_stats.get("numOutputRows", 0)
        output_meta["delta/merge_stats"] = json.dumps(merge_stats)

    context.add_output_metadata(output_meta)


class PySparkTypeHandler(DeltaTypeHandler[DataFrame]):
    """Handles PySpark ``DataFrame`` for Delta Lake IO.

    Some notes for Production use:
    1. Session should be created and configured before
       materialising assets so that cluster settings, credentials, and Delta
       options are applied to the session rather than the auto-created fallback.

    2. Cloud storage credentials should be configured via SparkSession
       Hadoop conf (e.g. spark.conf.set("fs.s3a.access.key", "...")).
       The storage_options kwarg on DeltaIOHandler is not forwarded
       to Spark on read and write paths.

    3. The to_arrow materialises the full DataFrame on the driver. For
       very large DataFrames, consider repartitioning before reading
       through it.
    """

    def __init__(self, spark: SparkSession | None = None) -> None:
        """
        Args:
            spark: Optional pre-configured :class:`~pyspark.sql.SparkSession`.
                When ``None`` the handler resolves a session at call-time using
                :func:`_get_or_create_spark`.
        """
        self._spark = spark

    def _get_spark(self) -> SparkSession:
        """Resolve the :class:`~pyspark.sql.SparkSession` for this invocation."""
        return _get_or_create_spark(self._spark)

    @property
    def supported_types(self) -> Sequence[type[DataFrame]]:
        """PySpark types this handler accepts as asset outputs / inputs."""
        return [DataFrame]

    def to_arrow(self, obj: DataFrame) -> RecordBatchReader:
        """Convert a PySpark ``DataFrame`` to an arro3 ``RecordBatchReader``.

        Enables Arrow-optimised ``toPandas()`` via
        ``spark.sql.execution.arrow.pyspark.enabled`` (or silently
        falls back to non-Arrow path when the flag cannot be set, e.g.
        when the SparkSession configuration is read only).  Resulting pandas
        frame is converted to :class:`~pyarrow.Table` with
        ``preserve_index=False`` and pandas ``RangeIndex`` is not written.

        Args:
            obj: A :class:`~pyspark.sql.DataFrame` to serialise.

        Returns:
            An arro3 :class:`~arro3.core.RecordBatchReader` wrapping the
            Arrow representation of obj, ready for ``write_deltalake``.
        """
        spark = obj.sparkSession
        try:
            spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        except Exception:  # noqa: BLE001
            # Note: Some SparkSession configurations are immutable
            # after session creation (e.g. remote or Spark Connect sessions).
            # Fall back to the non-Arrow toPandas path - semantically identical,
            # just slower for wide DataFrames.
            pass

        pandas_df = obj.toPandas()
        table = pa.Table.from_pandas(pandas_df, preserve_index=False)
        return RecordBatchReader.from_arrow(table)  # type: ignore[arg-type]

    def load_input(
        self,
        table_uri: str,
        table_name: str,
        storage_options: dict[str, str] | None,
        predicate: str | None,
        target_type: type[DataFrame],
        columns: list[str] | None = None,
        version: int | None = None,
    ) -> DataFrame:
        """Read a Delta table into a PySpark ``DataFrame``.

        Uses ``spark.read.format("delta")`` so the returned ``DataFrame`` is
        backed by a lazy Spark execution plan.  Spark's Delta connector handles
        predicate pushdown and column pruning internally.

        The SparkSession must have the Delta Lake extension registered::

            SparkSession.builder
                .config("spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate()

        Args:
            table_uri: Absolute filesystem path (or cloud URI) to the Delta
                table written by ``DeltaIOHandler``.
            table_name: Unused on the Spark read path; present for interface
                parity with other :class:`DeltaTypeHandler` implementations.
            storage_options: Ignored for Spark reads.  Configure cloud
                credentials via ``SparkSession`` Hadoop conf instead.
            predicate: Optional SQL ``WHERE`` expression forwarded to
                :meth:`~pyspark.sql.DataFrame.filter`.
            target_type: Must be :class:`pyspark.sql.DataFrame`.
            columns: Optional column-name list forwarded to
                :meth:`~pyspark.sql.DataFrame.select`.
            version: Optional Delta table version for time-travel reads
                (translated to ``versionAsOf`` reader option).

        Returns:
            A lazy :class:`~pyspark.sql.DataFrame`.  Spark execution is
            deferred until an action (``.collect()``, ``.toPandas()``,
            ``.count()``, …) is triggered.
        """
        spark = self._get_spark()
        reader = spark.read.format("delta")

        if version is not None:
            reader = reader.option("versionAsOf", str(version))

        df: DataFrame = reader.load(table_uri)

        if columns:
            df = df.select(columns)

        if predicate is not None:
            df = df.filter(predicate)

        return df
