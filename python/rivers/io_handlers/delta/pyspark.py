"""PySpark to Delta Lake bridge."""

from __future__ import annotations

import json
import time
import warnings
from collections.abc import Sequence
from typing import Any, Literal, get_args

from delta import DeltaTable
from delta._typing import ColumnMapping
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.pandas.types import to_arrow_schema

from rivers._core import MetadataValue, OutputContext
from rivers.io_handlers.delta import DeltaWriteRequest
from rivers.io_handlers.delta.base import DeltaTypeHandler
from rivers.io_handlers.delta.config import MergeConfig

SparkWriteMode = Literal["overwrite", "append", "error", "ignore"]


def _to_spark_write_mode(mode: str) -> str:
    """Returns Spark writer compatible mode from DeltaWriteMode."""

    if mode not in get_args(SparkWriteMode):
        raise ValueError(f"Spark does not support the write mode {mode}.")
    return mode


def _to_spark_column_mapping(mapping: dict[str, str]) -> ColumnMapping:
    """Returns a ColumnMapping representation of specified dict."""

    return {col: F.expr(expr) for col, expr in mapping.items()}


def _build_updates_mapping(
    *,
    sdf: DataFrame,
    source_alias: str,
    except_cols: list[str],
) -> ColumnMapping:
    """Builds updates mapping to exclude columns specified by exclude_cols."""
    return _to_spark_column_mapping(
        {
            col.name: f"{source_alias}.`{col.name}`"
            for col in sdf.schema
            if col.name not in except_cols
        }
    )


def _merge_execute_spark(
    *,
    uri: str,
    sdf: DataFrame,
    merge_config: MergeConfig | None,
    partition_predicate: str | None,
    merge_predicate_override: str | None = None,
) -> dict[str, Any]:
    """Execute a Delta Lake merge using delta-spark, returns a metrics dict."""

    if merge_config is None:
        raise ValueError("merge_config is required when mode='merge'")

    predicate = merge_predicate_override or merge_config.predicate

    if partition_predicate is not None:
        predicate = f"({predicate}) AND ({partition_predicate})"

    if not predicate.strip():
        raise ValueError("Merge predicate cannot be empty.")

    spark_session = sdf.sparkSession

    if not DeltaTable.isDeltaTable(spark_session, uri):
        sdf_count = sdf.count()
        sdf.write.format("delta").mode("overwrite").save(uri)
        return {
            "numOutputRows": sdf_count,
            "numTargetRowsInserted": sdf_count,
            "numTargetRowsUpdated": 0,
            "numTargetRowsDeleted": 0,
            "version": 0,
        }

    delta_table = DeltaTable.forPath(spark_session, uri)

    merge_builder = delta_table.alias(merge_config.target_alias).merge(
        sdf.alias(merge_config.source_alias),
        predicate,
    )

    mt = merge_config.merge_type

    if mt == "update_only":
        merge_builder = merge_builder.whenMatchedUpdateAll()
    elif mt == "deduplicate_insert":
        merge_builder = merge_builder.whenNotMatchedInsertAll()
    elif mt == "upsert":
        merge_builder = merge_builder.whenMatchedUpdateAll().whenNotMatchedInsertAll()
    elif mt == "replace_delete_unmatched":
        merge_builder = (
            merge_builder.whenMatchedUpdateAll().whenNotMatchedBySourceDelete()
        )
    elif mt == "custom":
        ops = merge_config.operations

        if ops is None:
            raise ValueError("operations is required when merge_type='custom'")

        # when matched update
        if ops.when_matched_update is not None:
            for op in ops.when_matched_update:
                merge_builder = merge_builder.whenMatchedUpdate(
                    condition=op.predicate,
                    set=_to_spark_column_mapping(op.updates),
                )

        # when matched update all
        if ops.when_matched_update_all is not None:
            for op in ops.when_matched_update_all:
                kwargs = {}
                if op.predicate:
                    kwargs["condition"] = op.predicate
                if op.except_cols:
                    kwargs["set"] = _build_updates_mapping(
                        sdf=sdf,
                        source_alias=merge_config.source_alias,
                        except_cols=op.except_cols,
                    )
                    merge_builder = merge_builder.whenMatchedUpdate(**kwargs)
                else:
                    merge_builder = merge_builder.whenMatchedUpdateAll(**kwargs)

        # when matched delete
        if ops.when_matched_delete is not None:
            for op in ops.when_matched_delete:
                kwargs = {}
                if op.predicate:
                    kwargs["condition"] = op.predicate
                merge_builder = merge_builder.whenMatchedDelete(**kwargs)

        # when not matched insert
        if ops.when_not_matched_insert is not None:
            for op in ops.when_not_matched_insert:
                kwargs = {}
                values = _to_spark_column_mapping(op.updates)
                if op.predicate:
                    kwargs["condition"] = op.predicate
                if values:
                    kwargs["values"] = values
                merge_builder = merge_builder.whenNotMatchedInsert(**kwargs)

        # when not matched insert all
        if ops.when_not_matched_insert_all is not None:
            for op in ops.when_not_matched_insert_all:
                kwargs = {}
                if op.predicate:
                    kwargs["condition"] = op.predicate
                if op.except_cols:
                    kwargs["set"] = _build_updates_mapping(
                        sdf=sdf,
                        source_alias=merge_config.source_alias,
                        except_cols=op.except_cols,
                    )
                    merge_builder = merge_builder.whenNotMatchedInsert(**kwargs)
                else:
                    merge_builder = merge_builder.whenNotMatchedInsertAll(**kwargs)

        # when not matched by source delete
        if ops.when_not_matched_by_source_delete is not None:
            for op in ops.when_not_matched_by_source_delete:
                kwargs = {}
                if op.predicate:
                    kwargs["condition"] = op.predicate
                merge_builder = merge_builder.whenNotMatchedBySourceDelete(**kwargs)

        # when not matched by source update
        if ops.when_not_matched_by_source_update is not None:
            for op in ops.when_not_matched_by_source_update:
                kwargs = {}
                set = _to_spark_column_mapping(op.updates)
                if op.predicate:
                    kwargs["condition"] = op.predicate
                if set:
                    kwargs["set"] = set
                merge_builder = merge_builder.whenNotMatchedBySourceUpdate(**kwargs)

    else:
        raise ValueError(f"Unknown merge_type: {mt!r}")

    merge_builder.execute()

    hist = delta_table.history(1).collect()[0]
    metrics = dict(hist.operationMetrics or {})
    metrics["version"] = hist.version
    return metrics


class PySparkDeltaTypeHandler(DeltaTypeHandler[DataFrame]):
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

    def __init__(self, spark_session: SparkSession | None = None) -> None:
        """
        Args:
            spark_session: Optional user-configured spark session. When
                ``None``, ``load_input()`` fall backs to an active or a
                new default spark session by default at call-time.
        """
        self._user_spark_session = spark_session
        self._reader_ignored_fields = ["storage_options"]
        self._writer_ignored_fields = [
            "commit_properties",
            "writer_properties",
            "storage_options",
        ]

    def _warn_ignored_fields(
        self, ignored_fields: list[str], request: dict[str, Any] | DeltaWriteRequest
    ):
        """Logs a warning about ignored fields by reader or writer."""

        if isinstance(request, DeltaWriteRequest):
            request = request.__dict__

        warn_fields = []
        for field_name in ignored_fields:
            if request.get(field_name):
                warn_fields.append(field_name)

        if warn_fields:
            warnings.warn(
                f"Values set in {', '.join(warn_fields)} will be ignored, "
                "these are not supported by the PySpark handler.",
                UserWarning,
            )

    def _get_or_create_spark_session(self) -> SparkSession:
        """Return an active spark session or create a new spark session with default config.

        Resolution order:
        1. The currently active session via
        :func:`~pyspark.sql.SparkSession.getActiveSession`.
        2. A new local ``local[*]`` session bootstrapped with
        ``delta.configure_spark_with_delta_pip`` from the ``delta-spark`` package.

        Raises:
            ImportError: When no active session exists and ``delta-spark`` is not
                installed.  Install the extra ``pip install rivers[delta-pyspark]``
                and start a session before calling the handler, or pass one
                explicitly via ``PySparkDeltaTypeHandler(spark=...)``.
        """

        active = SparkSession.getActiveSession()
        if active is not None:
            return active

        try:
            from delta import (
                configure_spark_with_delta_pip,  # type: ignore[import-untyped]
            )

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
                "PySparkDeltaTypeHandler(spark=my_session)."
            ) from exc

    @property
    def supported_types(self) -> Sequence[type[DataFrame]]:
        """PySpark types this handler accepts as asset outputs / inputs."""
        return [DataFrame]

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

        Uses ``spark_session.read.format("delta")`` so the returned ``DataFrame``
        is backed by a lazy Spark execution plan.  Spark's Delta connector handles
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
            target_type: Must be :class:`pyspark.sql.DataFrame`. Unused on the
                Spark read path; present for interface parity with other
                :class:`DeltaTypeHandler` implementations.
            columns: Optional column-name list forwarded to
                :meth:`~pyspark.sql.DataFrame.select`.
            version: Optional Delta table version for time-travel reads
                (translated to ``versionAsOf`` reader option).

        Returns:
            A lazy :class:`~pyspark.sql.DataFrame`.  Spark execution is
            deferred until an action (``.collect()``, ``.toPandas()``,
            ``.count()``, …) is triggered.
        """

        self._warn_ignored_fields(
            self._reader_ignored_fields,
            {
                "table_name": table_name,
                "storage_options": storage_options,
                "target_type": target_type,
            },
        )

        spark_session: SparkSession | None = None
        if not self._user_spark_session:
            warnings.warn(
                "No spark session set by user, so will use an active "
                "session or create a default one. For production, setting "
                "your own spark session in DeltaIOHandler is highly recommended."
            )
            spark_session = self._get_or_create_spark_session()
        else:
            spark_session = self._user_spark_session

        reader = spark_session.read.format("delta")

        if version is not None:
            reader = reader.option("versionAsOf", str(version))

        df: DataFrame = reader.load(table_uri)

        if columns:
            df = df.select(columns)

        if predicate is not None:
            df = df.filter(predicate)

        return df

    def handle_output(
        self,
        context: OutputContext,
        obj: object,
        request: DeltaWriteRequest,
    ) -> None:
        """Handles writes via spark session with delta-spark."""

        self._warn_ignored_fields(self._writer_ignored_fields, request)

        if not isinstance(obj, DataFrame):
            raise TypeError("obj must be a Spark DataFrame.")

        start_time = time.monotonic()
        spark = obj.sparkSession
        merge_stats: dict[str, Any] | None = None

        if request.delta_write_mode == "merge":
            merge_stats = _merge_execute_spark(
                uri=request.table_uri,
                sdf=obj,
                merge_config=request.merge_config,
                partition_predicate=request.predicate,
                merge_predicate_override=request.merge_predicate_override,
            )
            num_rows = merge_stats.get("numOutputRows")
            size_bytes = merge_stats.get("numOutputBytes")
            version = merge_stats.get("version")
        else:
            spark_writer = obj.write.format("delta").mode(
                _to_spark_write_mode(request.delta_write_mode)
            )

            if request.partition_by:
                spark_writer = spark_writer.partitionBy(*request.partition_by)
            if (
                request.predicate
                and request.delta_write_mode == "overwrite"
                and DeltaTable.isDeltaTable(spark, request.table_uri)
            ):
                spark_writer = spark_writer.option("replaceWhere", request.predicate)

            if request.schema_mode == "merge":
                spark_writer = spark_writer.option("mergeSchema", "true")
            elif request.schema_mode == "overwrite":
                spark_writer = spark_writer.option("overwriteSchema", "true")

            if request.table_configuration:
                for k, v in request.table_configuration.items():
                    spark_writer = spark_writer.option(k, v)

            spark_writer.save(request.table_uri)

            hist = DeltaTable.forPath(spark, request.table_uri).history(1).collect()[0]
            metrics = hist.operationMetrics or {}
            num_rows = metrics.get("numOutputRows")
            size_bytes = metrics.get("numOutputBytes")
            version = hist.version

        duration = time.monotonic() - start_time
        arrow_schema = to_arrow_schema(obj.schema)
        output_meta: dict[str, Any] = {
            "delta/table_uri": request.table_uri,
            "delta/mode": request.delta_write_mode,
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
