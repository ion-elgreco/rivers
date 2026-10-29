"""PySpark to Apache Iceberg bridge."""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import Any
from uuid import uuid4

import pyarrow as pa
from pyiceberg.transforms import (
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    YearTransform,
)
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.pandas.types import to_arrow_schema

try:
    from pyspark.errors import AnalysisException
except ImportError:  # pyspark < 3.4
    from pyspark.sql.utils import AnalysisException  # type: ignore[no-redef]

from rivers._core import MetadataValue, OutputContext
from rivers.io_handlers._partition_sql import _build_predicate
from rivers.io_handlers.iceberg._partitions import partition_fields
from rivers.io_handlers.iceberg.base import (
    IcebergReadRequest,
    IcebergTypeHandler,
    IcebergWriteRequest,
)


_NOT_FOUND_ERROR_CLASSES = {
    "TABLE_OR_VIEW_NOT_FOUND",
    "NO_SUCH_TABLE_OR_VIEW",
    "SCHEMA_NOT_FOUND",
    "SCHEMA_NOT_FOUND_OR_NOT_VISIBLE",
    "NAMESPACE_NOT_FOUND",
}


def _quote_part(part: str) -> str:
    """Quote a single Spark SQL identifier component."""
    if not part:
        raise ValueError("Spark catalog, namespace, and table names must not be empty")
    return f"`{part.replace('`', '``')}`"


def _qualified_table_name(catalog: str, identifier: tuple[str, ...]) -> str:
    """Quote each identifier component, preserving dots inside individual names."""
    if not catalog or "." in catalog:
        raise ValueError("spark_catalog must be a non-empty, single identifier")
    if not identifier:
        raise ValueError("Iceberg table identifier must not be empty")
    return ".".join(_quote_part(part) for part in (catalog, *identifier))


def _sql_literal(value: str) -> str:
    """Escape a string literal for Spark SQL DDL."""
    return "'" + value.replace("'", "''") + "'"


def _table_exists(spark: SparkSession, table_name: str) -> bool:
    """Check for a missing table without masking unrelated Spark errors."""
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}").collect()
    except AnalysisException as exc:
        get_error_class = getattr(exc, "getErrorClass", None)
        error_class = get_error_class() if callable(get_error_class) else None

        if error_class in _NOT_FOUND_ERROR_CLASSES:
            return False

        message = str(exc).lower()
        if any(
            marker in message
            for marker in (
                "table_or_view_not_found",
                "table or view not found",
                "does not exist",
            )
        ):
            return False

        if ("schema" in message or "namespace" in message) and any(
            marker in message for marker in ("not found", "cannot be found")
        ):
            return False

        raise

    return True


def _ensure_namespace(
    spark: SparkSession, catalog: str, namespace: tuple[str, ...]
) -> None:
    """Create missing namespace levels before creating a table."""
    for depth in range(1, len(namespace) + 1):
        name = _qualified_table_name(catalog, namespace[:depth])
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {name}")


def _spark_partition_columns(
    obj: DataFrame,
    request: IcebergWriteRequest,
) -> list[Any]:
    """Translate Rivers partition definitions to Spark/Iceberg transforms."""
    if request.partition is None or request.partition_expr is None:
        return []

    schema: pa.Schema = to_arrow_schema(obj.schema)
    fields = partition_fields(request.partition, request.partition_expr, schema)

    transforms: list[Any] = []
    supported_transforms = (
        (IdentityTransform, None),
        (YearTransform, "years"),
        (MonthTransform, "months"),
        (DayTransform, "days"),
        (HourTransform, "hours"),
    )

    for column, transform in fields:
        transform_name = next(
            (
                name
                for transform_type, name in supported_transforms
                if isinstance(transform, transform_type)
            ),
            ...,
        )

        if transform_name is ...:
            raise NotImplementedError(
                f"Unsupported Iceberg partition transform: {type(transform).__name__}"
            )

        quoted_column = _quote_part(column)

        if transform_name is None:
            transforms.append(F.col(quoted_column))
            continue

        # Use spark native column transform when available.
        transform_function = getattr(F, transform_name, None)

        if callable(transform_function):
            transforms.append(transform_function(F.col(quoted_column)))
        else:
            # Fallback for Spark versions exposing transforms through the SQL function registry.
            transforms.append(F.expr(f"{transform_name}({quoted_column})"))

    return transforms


def _writer(
    obj: DataFrame,
    request: IcebergWriteRequest,
    table_name: str,
):
    """Build a DataFrameWriterV2 with schema and snapshot options."""
    writer = obj.writeTo(table_name)

    if request.schema_mode is not None:
        writer = writer.option("mergeSchema", "true")

    for key, value in request.snapshot_properties.items():
        writer = writer.option(f"snapshot-property.{key}", value)

    return writer


def _table_properties(spark: SparkSession, table_name: str) -> dict[str, str]:
    """Read table properties through Spark SQL."""
    rows = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
    return {str(row[0]): str(row[1]) for row in rows}


def _snapshot_summary(spark: SparkSession, table_name: str) -> dict[str, Any]:
    """Retrieve the current snapshot ID and summary using Iceberg metadata tables."""
    history = spark.sql(
        f"SELECT * FROM {table_name}.history ORDER BY made_current_at DESC LIMIT 1"
    ).collect()

    if not history:
        return {}

    snapshot_id = int(history[0]["snapshot_id"])
    snapshots = spark.sql(
        f"SELECT * FROM {table_name}.snapshots WHERE snapshot_id = {snapshot_id}"
    ).collect()

    if not snapshots:
        return {"snapshot_id": snapshot_id}

    summary = snapshots[0].asDict(recursive=True).get("summary") or {}
    return {
        "snapshot_id": snapshot_id,
        "summary": dict(summary),
    }


def _set_table_properties(
    spark: SparkSession,
    request: IcebergWriteRequest,
    table_name: str,
) -> None:
    """Apply changed table properties through Spark SQL."""
    properties = dict(request.table_properties)

    if request.schema_mode is not None:
        properties.setdefault("write.spark.accept-any-schema", "true")

    current = _table_properties(spark, table_name)
    changed = {
        key: value for key, value in properties.items() if current.get(key) != value
    }

    if not changed:
        return

    assignments = ", ".join(
        f"{_sql_literal(key)} = {_sql_literal(value)}" for key, value in changed.items()
    )
    spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ({assignments})")


def _load_iceberg_table(spark, table_name):
    jvm = getattr(spark, "_jvm", None)
    jspark_session = getattr(spark, "_jsparkSession", None)

    if jvm is None or jspark_session is None:
        raise RuntimeError("Spark JVM gateway is unavailable")

    org = getattr(jvm, "org")
    apache = getattr(org, "apache")
    iceberg = getattr(apache, "iceberg")
    iceberg_spark = getattr(iceberg, "spark")
    spark3_util = getattr(iceberg_spark, "Spark3Util")

    return spark3_util.loadIcebergTable(
        jspark_session,
        table_name,
    )


def _validate_unique_source_join_keys(
    obj: DataFrame,
    join_columns: list[str],
) -> None:
    """Reject duplicate source keys before executing an Iceberg upsert."""
    duplicates = (
        obj.groupBy(*join_columns).count().filter(F.col("count") > 1).limit(1).collect()
    )
    if duplicates:
        duplicate_key = duplicates[0].asDict()
        duplicate_key.pop("count", None)
        raise ValueError(
            f"Iceberg upsert source contains duplicate join keys: {duplicate_key}"
        )


def _validate_non_null_source_join_keys(
    obj: DataFrame,
    join_columns: list[str],
) -> None:
    """Reject source rows containing null upsert join keys."""
    null_condition = None

    for column in join_columns:
        condition = F.col(_quote_part(column)).isNull()
        null_condition = (
            condition if null_condition is None else null_condition | condition
        )

    if null_condition is None:
        return

    if obj.filter(null_condition).limit(1).count() > 0:
        raise ValueError("Iceberg upsert source contains null values in join keys")


def _validate_schema_overwrite_partition_columns(
    spark: SparkSession,
    table_name: str,
    source_columns: set[str],
) -> None:
    """Reject schema overwrites that remove columns used by partition fields."""
    if not _table_exists(spark, table_name):
        return

    table = _load_iceberg_table(spark, table_name)
    partition_columns = {field.name() for field in table.spec().fields()}
    missing_partition_columns = partition_columns - source_columns
    if missing_partition_columns:
        raise ValueError(
            "Schema overwrite cannot drop Iceberg partition columns: "
            f"{sorted(missing_partition_columns)}"
        )


def _validate_join_keys(
    obj: DataFrame,
    join_columns: list[str],
) -> None:

    _validate_non_null_source_join_keys(obj, join_columns)
    _validate_unique_source_join_keys(obj, join_columns)


def _resolve_join_columns(
    request: IcebergWriteRequest,
    spark: SparkSession,
    table_name: str,
) -> list[str]:
    """Resolve explicit upsert keys or fall back to Iceberg identifier fields."""
    columns = list(request.upsert_config.join_cols or [])

    if columns:
        if len(set(columns)) != len(columns):
            raise ValueError("Iceberg upsert join columns must be unique")
        return columns

    try:
        table = _load_iceberg_table(spark, table_name)
        schema = table.schema()

        identifier_field_ids = list(schema.identifierFieldIds())
        columns = [
            str(schema.findField(field_id).name()) for field_id in identifier_field_ids
        ]

    except Exception as exc:
        raise ValueError(
            "Could not resolve Iceberg identifier fields for upsert. "
            "Specify iceberg/join_cols explicitly."
        ) from exc

    if not columns:
        raise ValueError(
            "PySpark Iceberg upsert requires iceberg/join_cols or "
            "Iceberg identifier fields."
        )

    if len(set(columns)) != len(columns):
        raise ValueError("Iceberg upsert join columns must be unique")

    return columns


def _create_table(
    obj: DataFrame,
    request: IcebergWriteRequest,
    spark: SparkSession,
    table_name: str,
    spark_catalog: str,
) -> None:
    """Create a missing Iceberg table and write its initial data."""
    _ensure_namespace(spark, spark_catalog, request.identifier[:-1])

    writer = _writer(obj, request, table_name)
    writer = writer.using("iceberg")

    properties = dict(request.table_properties)
    if request.schema_mode is not None:
        properties.setdefault("write.spark.accept-any-schema", "true")

    for key, value in properties.items():
        writer = writer.tableProperty(key, value)

    partition_columns = _spark_partition_columns(obj, request)
    if partition_columns:
        writer = writer.partitionedBy(*partition_columns)

    writer.create()


def _merge(
    obj: DataFrame,
    request: IcebergWriteRequest,
    spark: SparkSession,
    table_name: str,
    join_columns: list[str],
) -> None:
    """Execute an Iceberg MERGE INTO for configured upsert behavior."""
    if request.schema_mode is not None:
        raise ValueError("PySpark Iceberg upsert does not support schema_mode")

    if request.snapshot_properties:
        raise ValueError(
            "PySpark Iceberg MERGE INTO cannot attach custom snapshot properties; "
            "use mode='append' or mode='overwrite' for those writes"
        )

    config = request.upsert_config
    clauses: list[str] = []

    if config.when_matched_update_all:
        clauses.append("WHEN MATCHED THEN UPDATE SET *")

    if config.when_not_matched_insert_all:
        clauses.append("WHEN NOT MATCHED THEN INSERT *")

    if not clauses:
        raise ValueError("Iceberg upsert needs at least one matched/insert clause")

    source_columns = set(obj.columns)
    target_columns = set(spark.table(table_name).columns)

    missing = [
        column
        for column in join_columns
        if column not in source_columns or column not in target_columns
    ]
    if missing:
        raise ValueError(
            "Iceberg upsert join columns must exist in both source and target: "
            f"{missing}"
        )

    condition = " AND ".join(
        f"target.{_quote_part(column)} = source.{_quote_part(column)}"
        for column in join_columns
    )

    view_name = f"__rivers_iceberg_upsert_{uuid4().hex}"
    obj.createOrReplaceTempView(view_name)

    try:
        spark.sql(
            f"MERGE INTO {table_name} AS target "
            f"USING {_quote_part(view_name)} AS source ON {condition} "
            + " ".join(clauses)
        )
    finally:
        spark.catalog.dropTempView(view_name)


class PySparkIcebergTypeHandler(IcebergTypeHandler[DataFrame]):
    """Read and write Spark DataFrames through Spark's Iceberg connector.

    ``spark_session`` is optional at the handler level. If omitted, an active
    SparkSession must already exist. ``IcebergIOHandler`` should register this
    handler only when a Spark session is explicitly configured, so ordinary
    PyIceberg use remains unchanged when Spark is not configured.

    ``spark_catalog`` is the configured Spark catalog name, for example
    ``local`` for ``spark.sql.catalog.local``.
    """

    def __init__(
        self,
        spark_session: SparkSession | None = None,
        spark_catalog: str | None = None,
    ) -> None:
        self._user_spark_session = spark_session
        self._spark_catalog = (
            "spark_catalog" if spark_catalog is None else spark_catalog
        )

    @property
    def supported_types(self) -> Sequence[type[DataFrame]]:
        """PySpark DataFrame types accepted for asset inputs and outputs."""
        return [DataFrame]

    def _get_spark_session(self) -> SparkSession:
        """Return the configured or active session, with setup guidance."""
        if self._user_spark_session is not None:
            return self._user_spark_session

        active = SparkSession.getActiveSession()
        if active is not None:
            return active

        raise RuntimeError(
            "PySpark Iceberg IO requires an active, Iceberg-configured SparkSession. "
            "Create one with the Iceberg Spark runtime JAR and catalog settings, or "
            "pass it as handler_config={'spark_session': spark, "
            "'spark_catalog': 'your_catalog'}; install 'rivers[iceberg-pyspark]'."
        )

    def load_input(
        self, request: IcebergReadRequest, target_type: type[DataFrame]
    ) -> DataFrame:
        """Load a lazy Spark DataFrame with projection, partition, and snapshot filters."""

        if target_type is not DataFrame:
            raise TypeError("target_type must be PySpark DataFrame")

        spark = self._get_spark_session()
        table_name = _qualified_table_name(self._spark_catalog, request.identifier)

        if request.snapshot_id is None:
            data = spark.table(table_name)
        else:
            data = spark.sql(
                f"SELECT * FROM {table_name} VERSION AS OF {int(request.snapshot_id)}"
            )

        if request.columns:
            # Spark treats select strings as column expressions. Quoting each
            # name allows columns containing spaces, dots, or backticks.
            data = data.select(
                *[F.col(_quote_part(column)) for column in request.columns]
            )

        if request.predicate is not None:
            data = data.filter(request.predicate)

        return data

    def _validate_handle_output_params(
        self, obj: DataFrame, request: IcebergWriteRequest
    ):
        if not isinstance(obj, DataFrame):
            raise TypeError("obj must be a PySpark DataFrame")

        if request.mode not in ("append", "overwrite", "upsert"):
            raise ValueError(f"Unsupported Iceberg write mode: {request.mode!r}")

        if request.schema_mode not in (None, "merge", "overwrite"):
            raise ValueError(
                f"Unsupported Iceberg schema mode: {request.schema_mode!r}"
            )

        if request.schema_mode == "overwrite" and request.mode != "overwrite":
            raise ValueError("schema_mode='overwrite' requires mode='overwrite'")

    def _validate_upsert_request(
        self,
        request: IcebergWriteRequest,
        obj: DataFrame,
        spark: SparkSession,
        table_name: str,
        table_exists: bool,
    ) -> list[str]:
        if request.schema_mode is not None:
            raise ValueError("PySpark Iceberg upsert does not support schema_mode")

        if request.snapshot_properties:
            raise ValueError(
                "PySpark Iceberg MERGE INTO cannot attach custom snapshot "
                "properties; use mode='append' or mode='overwrite' "
                "for those writes"
            )

        config = request.upsert_config
        if not (config.when_matched_update_all or config.when_not_matched_insert_all):
            raise ValueError("Iceberg upsert needs at least one matched/insert clause")

        # Explicit join columns take precedence. For an existing table,
        # fall back to its Iceberg identifier fields when available.
        join_columns = list(config.join_cols or [])

        if not join_columns and table_exists:
            join_columns = _resolve_join_columns(
                request,
                spark,
                table_name,
            )

        if not join_columns:
            raise ValueError(
                "PySpark Iceberg upsert requires iceberg/join_cols when "
                "creating a table or when identifier fields are unavailable."
            )

        if len(set(join_columns)) != len(join_columns):
            raise ValueError("Iceberg upsert join columns must be unique")

        missing_source = [
            column for column in join_columns if column not in obj.columns
        ]
        if missing_source:
            raise ValueError(
                "Iceberg upsert join columns must exist in both source "
                f"and target: {missing_source}"
            )

        _validate_join_keys(obj, join_columns)

        if table_exists:
            target_columns = set(spark.table(table_name).columns)
            missing_target = [
                column for column in join_columns if column not in target_columns
            ]
            if missing_target:
                raise ValueError(
                    "Iceberg upsert join columns must exist in both source "
                    f"and target: {missing_target}"
                )

        return join_columns

    def _validate_overwrite_request(self, request: IcebergWriteRequest) -> str | None:
        if request.partition is not None:
            predicate_sql = _build_predicate(
                request.partition,
                request.partition_expr,
            )

            if request.schema_mode == "overwrite":
                raise ValueError(
                    "schema_mode='overwrite' cannot be combined with a partition overwrite"
                )

            return predicate_sql

        return None

    def _write_table_metadata(
        self,
        spark: SparkSession,
        table_name: str,
        context: OutputContext,
        request: IcebergWriteRequest,
        duration: float,
    ):
        try:
            snapshot_info = _snapshot_summary(spark, table_name)
        except Exception as exc:
            raise RuntimeError(
                "PySpark wrote the Iceberg table, but Spark could not retrieve "
                "its snapshot metadata. Verify that the configured Spark "
                "catalog supports the Iceberg metadata tables."
            ) from exc

        snapshot_id = snapshot_info.get("snapshot_id")
        summary = snapshot_info.get("summary") or {}
        raw_rows = summary.get("total-records")
        num_rows = (
            int(raw_rows)
            if raw_rows is not None
            else int(spark.table(table_name).count())
        )
        raw_size = summary.get("total-files-size")
        if raw_size is None:
            raw_size = summary.get("added-files-size")
        size_bytes = int(raw_size) if raw_size is not None else 0
        arrow_schema = to_arrow_schema(spark.table(table_name).schema)
        metadata_location = None
        try:
            metadata_rows = spark.sql(
                f"""
                SELECT file
                FROM {table_name}.metadata_log_entries
                ORDER BY timestamp DESC
                LIMIT 1
                """
            ).collect()
            if metadata_rows:
                metadata_location = metadata_rows[0]["file"]
        except Exception:
            raise ValueError("Unable to determine metadata_location.")

        metadata: dict[str, Any] = {
            "iceberg/table": ".".join(request.identifier),
            "iceberg/mode": request.mode,
            "iceberg/num_rows": num_rows,
            "iceberg/size_bytes": size_bytes,
            "iceberg/metadata_location": metadata_location,
            "iceberg/write_duration_s": round(duration, 6),
            "rivers/schema": MetadataValue.schema(arrow_schema),
        }

        if snapshot_id is not None:
            metadata["iceberg/snapshot_id"] = int(snapshot_id)

        context.add_output_metadata(metadata)

        if snapshot_id is not None:
            context.register_data_version(str(snapshot_id))

    def handle_output(
        self,
        context: OutputContext,
        obj: DataFrame,
        request: IcebergWriteRequest,
    ) -> None:
        """Write through Spark and attach Rivers output metadata."""

        self._validate_handle_output_params(obj, request)

        started = time.monotonic()
        spark = obj.sparkSession
        table_name = _qualified_table_name(self._spark_catalog, request.identifier)
        table_exists = _table_exists(spark, table_name)
        join_columns: list[str] = []
        predicate_sql = None

        # Validations
        match request.mode:
            case "upsert":
                join_columns = self._validate_upsert_request(
                    request=request,
                    obj=obj,
                    spark=spark,
                    table_name=table_name,
                    table_exists=table_exists,
                )

            case "overwrite":
                predicate_sql = self._validate_overwrite_request(request=request)

        # Writing
        if not table_exists:
            # Very first write creates the table and commits its initial rows.
            _create_table(obj, request, spark, table_name, self._spark_catalog)
        else:
            _set_table_properties(spark, request, table_name)

            match request.mode:
                case "append":
                    _writer(obj, request, table_name).append()

                case "overwrite":
                    writer = _writer(obj, request, table_name)
                    if request.schema_mode == "overwrite":
                        # Preserve existing Iceberg table and partition spec
                        # and make its schema match the incoming DataFrame.
                        source_columns = set(obj.columns)
                        _validate_schema_overwrite_partition_columns(
                            spark,
                            table_name,
                            source_columns,
                        )
                        target_columns = spark.table(table_name).columns
                        for column in target_columns:
                            if column not in source_columns:
                                spark.sql(
                                    f"ALTER TABLE {table_name} DROP COLUMN "
                                    f"{_quote_part(column)}"
                                )
                        writer.overwrite(F.lit(True))
                    else:
                        predicate = (
                            F.expr(predicate_sql)
                            if predicate_sql is not None
                            else F.lit(True)
                        )
                        writer.overwrite(predicate)

                case "upsert":
                    _merge(obj, request, spark, table_name, join_columns)

        duration = time.monotonic() - started
        spark.catalog.refreshTable(table_name)
        self._write_table_metadata(
            spark=spark,
            table_name=table_name,
            context=context,
            request=request,
            duration=duration,
        )
