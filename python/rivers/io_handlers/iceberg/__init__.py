"""Apache Iceberg IO handler — read/write asset outputs as Iceberg tables with pyiceberg.

Install with ``pip install rivers[iceberg]``. Type-specific conversion is
delegated to :class:`IcebergTypeHandler` implementations registered in
:func:`IcebergIOHandler.type_handlers`.

When ``spark_session`` is supplied in ``handler_config``, Spark DataFrame
operations use Spark's Iceberg Datasource API without opening a PyIceberg
catalog. Other supported Python data types continue to use their registered
PyIceberg-backed handlers. Without a Spark session, the standard PyIceberg
handlers are used.
"""

from __future__ import annotations

try:
    import pyiceberg  # noqa: F401
except ImportError as e:
    raise ImportError(
        "rivers.io_handlers.iceberg needs pyiceberg: pip install rivers[iceberg]"
    ) from e

import json
from typing import Any, get_args

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.expressions import BooleanExpression
from pyiceberg.schema import Schema
from pyiceberg.table import Table

from rivers._core import InputContext, OutputContext
from rivers._core.partitions import PartitionContext
from rivers.io_handlers._partition_sql import PartitionExpr, _resolve_partition_expr
from rivers.io_handlers._type_handlers import Candidate, discover, resolve
from rivers.io_handlers.base import BaseIOHandler
from rivers.io_handlers.iceberg import _partitions
from rivers.io_handlers.iceberg.asset import ExpireSnapshotsConfig, IcebergAsset
from rivers.io_handlers.iceberg.base import (
    ArrowIcebergTypeHandler,
    IcebergReadRequest,
    IcebergSchemaMode,
    IcebergTypeHandler,
    IcebergWriteMode,
    IcebergWriteRequest,
)
from rivers.io_handlers.iceberg.config import UpsertConfig

__all__: list[str] = [
    "ArrowIcebergTypeHandler",
    "ExpireSnapshotsConfig",
    "IcebergAsset",
    "IcebergIOHandler",
    "IcebergReadRequest",
    "IcebergTypeHandler",
    "IcebergWriteRequest",
    "PartitionExpr",
    "UpsertConfig",
]

_TYPE_HANDLERS: list[Candidate] = [
    ("rivers.io_handlers.iceberg.pyarrow", "PyArrowTypeHandler", []),
    ("rivers.io_handlers.iceberg.polars", "PolarsTypeHandler", []),
    ("rivers.io_handlers.iceberg.pandas", "PandasTypeHandler", []),
    ("rivers.io_handlers.iceberg.duckdb", "DuckDBTypeHandler", ["duckdb"]),
    (
        "rivers.io_handlers.iceberg.pyspark",
        "PySparkIcebergTypeHandler",
        ["spark_session", "spark_catalog"],
    ),
]

# Keyed by the type's top-level module: ``DataFrame`` is shared by pandas and
# polars, so a name-based map can't tell them apart.
_MODULE_TO_EXTRA: dict[str, str] = {
    "pyarrow": "iceberg",
    "polars": "iceberg-polars",
    "pandas": "iceberg-pandas",
    "_duckdb": "iceberg-duckdb",
    "pyspark": "iceberg-pyspark",
}


class IcebergIOHandler(BaseIOHandler):
    """Persist asset outputs as Apache Iceberg tables, through a pyiceberg or spark catalog.

    Each asset is the table ``{namespace}.{asset_name}``; the
    ``iceberg/namespace`` and ``iceberg/table`` asset metadata keys override
    the two parts. Per-asset overrides through ``asset_metadata`` may also
    set ``iceberg/mode``, ``iceberg/schema_mode``, ``iceberg/partition_expr``,
    ``iceberg/join_cols``, ``iceberg/table_properties``,
    ``iceberg/snapshot_properties``, ``iceberg/columns`` (read), and
    ``iceberg/snapshot_id`` (read).

    Args:
        namespace: The namespace of the asset tables. Dots separate nested
            namespaces.
        catalog_name: The pyiceberg catalog name.
        catalog_properties: The catalog's properties, such as ``type``,
            ``uri``, and ``warehouse``, and the storage properties of
            pyiceberg's FileIO (``s3.*``, ``adls.*``, ``gcs.*``). pyiceberg adds
            the ones set for ``catalog_name`` in ``.pyiceberg.yaml`` and in
            ``PYICEBERG_CATALOG__<NAME>__<KEY>`` environment variables.
        mode: ``"overwrite"`` replaces the table, or the partition's rows;
            ``"append"`` adds rows; ``"upsert"`` updates and inserts rows
            matched on the join columns.
        schema_mode: ``"merge"`` adds new columns and widens types before a
            write; ``"overwrite"`` also drops the columns the data does not
            have (overwrite mode only). ``None`` fails on a different schema.
            Upserts do not change the schema.
        upsert_config: Join columns and clauses for ``mode="upsert"``.
        table_properties: Iceberg table properties, set on create and when
            they change.
        snapshot_properties: Properties added to the summary of each snapshot
            the handler writes.
        handler_config: Custom type handler config, e.g. ``{"duckdb":
            DuckDBResource(...)}``.
    """

    namespace: str
    catalog_name: str = "default"
    catalog_properties: dict[str, str] | None = None
    mode: IcebergWriteMode = "overwrite"
    schema_mode: IcebergSchemaMode | None = None
    upsert_config: UpsertConfig | None = None
    table_properties: dict[str, str] | None = None
    snapshot_properties: dict[str, str] | None = None
    handler_config: dict[str, Any] | None = None

    @staticmethod
    def type_handlers(
        handler_config: dict[str, Any] | None = None,
    ) -> dict[type, IcebergTypeHandler]:
        """Return the currently registered ``{type: IcebergTypeHandler}`` map.

        The PySpark handler is registered only when a Spark session is supplied.
        Its catalog name is optional because the handler has a default.
        """
        return discover(_TYPE_HANDLERS, handler_config)

    def _resolve_handler(self, target_type: type) -> IcebergTypeHandler:
        """Look up the type handler for ``target_type`` or raise ``TypeError``.

        Suggests the right install extra (e.g. ``rivers[iceberg-polars]``)
        when a known type is requested but its handler library is not
        installed.
        """
        try:
            return resolve(
                self.type_handlers(self.handler_config),
                target_type,
                _MODULE_TO_EXTRA,
            )
        except TypeError as exc:
            # A PySpark DataFrame cannot be read or written by the Arrow
            # handlers implicitly. Explain the missing runtime configuration
            # instead of suggesting that installing an extra alone is enough.
            if getattr(target_type, "__module__", "").startswith("pyspark.") and not (
                self.handler_config or {}
            ).get("spark_session"):
                raise TypeError(
                    "PySpark DataFrame IO requires "
                    "handler_config={'spark_session': spark}. "
                    "Without a configured Spark session, use a supported "
                    "PyIceberg-backed input/output type."
                ) from exc
            raise

    def pyiceberg_catalog(self) -> Catalog:
        """Load the pyiceberg catalog: ``load_catalog(catalog_name, **catalog_properties)``.

        This method is used only by PyIceberg-backed handlers. Spark DataFrame
        operations use the configured Spark catalog directly.
        """
        return load_catalog(self.catalog_name, **(self.catalog_properties or {}))

    def table_identifier(
        self, asset_name: str, asset_metadata: dict[str, str] | None = None
    ) -> tuple[str, ...]:
        """``(*namespace, table)``, honoring ``iceberg/namespace`` and ``iceberg/table``.

        Args:
            asset_name: The asset name (``ctx.asset_name`` in an action).
            asset_metadata: Per-asset metadata (``ctx.asset_metadata``).
        """
        meta = asset_metadata or {}
        namespace = meta.get("iceberg/namespace", self.namespace)
        return (*namespace.split("."), meta.get("iceberg/table", asset_name))

    def load_table(
        self, asset_name: str, asset_metadata: dict[str, str] | None = None
    ) -> Table:
        """The asset's pyiceberg ``Table``, for action bodies.

        Raises:
            NoSuchTableError: If the table does not exist yet.
        """
        return self.pyiceberg_catalog().load_table(
            self.table_identifier(asset_name, asset_metadata)
        )

    def partition_filter(
        self,
        asset_metadata: dict[str, str] | None,
        partition: PartitionContext | None,
        schema: Schema,
    ) -> BooleanExpression:
        """The partition's rows as a pyiceberg expression, honoring ``iceberg/partition_expr``.

        The filter the overwrite path uses, for action bodies
        (``table.delete(...)``, ``table.scan(row_filter=...)``).

        Args:
            asset_metadata: Per-asset metadata (``ctx.asset_metadata``).
            partition: The partition context (``ctx.partition``).
            schema: The table's schema (``table.schema()``); it types the
                time-window bounds.

        Raises:
            ValueError: If ``partition`` is ``None``, or the metadata has no
                ``iceberg/partition_expr``.
        """
        if partition is None:
            raise ValueError(
                "partition_filter needs a partition context; this action run "
                "has no partition key — operate on the whole table instead"
            )
        partition_expr = _resolve_partition_expr(
            asset_metadata or {}, "iceberg/partition_expr"
        )
        return _partitions.partition_filter(partition, partition_expr, schema)

    def resolved_snapshot_properties(
        self, asset_metadata: dict[str, str] | None
    ) -> dict[str, str]:
        """The snapshot properties of the asset's commits.

        The handler's ``snapshot_properties``, updated with the asset's
        ``iceberg/snapshot_properties`` (a JSON object).
        """
        meta = asset_metadata or {}
        properties = dict(self.snapshot_properties or {})
        properties.update(json.loads(meta.get("iceberg/snapshot_properties", "{}")))
        return properties

    def _write_request(
        self, context: OutputContext, catalog: Catalog | None
    ) -> IcebergWriteRequest:
        """Resolve the write parameters from the handler and the asset metadata."""
        meta = context.asset_metadata or {}
        mode: IcebergWriteMode = meta.get("iceberg/mode", self.mode)  # type: ignore[assignment]
        if mode not in get_args(IcebergWriteMode):
            raise ValueError(
                f"iceberg/mode must be one of {list(get_args(IcebergWriteMode))}, "
                f"got {mode!r}"
            )
        schema_mode: IcebergSchemaMode | None = meta.get(
            "iceberg/schema_mode", self.schema_mode
        )  # type: ignore[assignment]
        if schema_mode is not None and schema_mode not in get_args(IcebergSchemaMode):
            raise ValueError(
                "iceberg/schema_mode must be one of "
                f"{list(get_args(IcebergSchemaMode))}, got {schema_mode!r}"
            )
        if schema_mode == "overwrite" and mode == "append":
            raise ValueError(
                "schema_mode 'overwrite' drops columns, so it needs mode "
                "'overwrite', not 'append'"
            )

        partition_expr = _resolve_partition_expr(meta, "iceberg/partition_expr")
        if context.partition is not None and mode == "overwrite":
            _partitions.require_partition_expr(partition_expr)

        upsert_config = self.upsert_config or UpsertConfig()
        if "iceberg/join_cols" in meta:
            upsert_config = upsert_config.model_copy(
                update={"join_cols": json.loads(meta["iceberg/join_cols"])}
            )

        table_properties = dict(self.table_properties or {})
        table_properties.update(json.loads(meta.get("iceberg/table_properties", "{}")))

        return IcebergWriteRequest(
            catalog=catalog,
            identifier=self.table_identifier(context.asset_name, meta),
            mode=mode,
            schema_mode=schema_mode,
            partition=context.partition,
            partition_expr=partition_expr,
            table_properties=table_properties,
            snapshot_properties=self.resolved_snapshot_properties(meta),
            upsert_config=upsert_config,
        )

    def handle_output(self, context: OutputContext, obj: object) -> None:
        """Write ``obj`` to the asset's Iceberg table and record table-level metadata.

        Honors per-asset metadata overrides for the table name, write mode,
        schema evolution, partition expression, upsert join columns, and table
        and snapshot properties. A partitioned overwrite replaces only the
        partition's rows. Records ``iceberg/table``, ``iceberg/mode``,
        ``iceberg/snapshot_id``, ``iceberg/num_rows``, ``iceberg/size_bytes``,
        ``iceberg/write_duration_s``, ``iceberg/metadata_location``, and the
        Arrow schema.
        """
        # Resolve the handler first: a Spark DataFrame must not trigger
        # PyIceberg catalog construction.
        handler = self._resolve_handler(type(obj))
        is_spark = handler.__class__.__name__ == "PySparkIcebergTypeHandler"

        if is_spark:
            catalog = None
        else:
            catalog = self.pyiceberg_catalog()

        if catalog is None:
            request = self._write_request(context, None)
            handler.handle_output(context=context, obj=obj, request=request)
            return

        with catalog:
            request = self._write_request(context, catalog)
            handler.handle_output(context=context, obj=obj, request=request)

    def load_input(self, context: InputContext) -> Any:
        """Read an upstream Iceberg table in the requested Python type."""
        if context.type_hint is None:
            raise TypeError(
                "No type_hint provided on InputContext. "
                f"Supported types: {list(self.type_handlers(self.handler_config))}"
            )

        handler = self._resolve_handler(context.type_hint)
        is_spark = handler.__class__.__name__ == "PySparkIcebergTypeHandler"

        if is_spark:
            catalog = None
        else:
            catalog = self.pyiceberg_catalog()

        meta = context.asset_metadata or {}
        columns = (
            json.loads(meta["iceberg/columns"]) if "iceberg/columns" in meta else None
        )
        snapshot_id = meta.get("iceberg/snapshot_id")

        request = IcebergReadRequest(
            catalog=catalog,
            identifier=self.table_identifier(context.asset_name, meta),
            partition=context.partition,
            partition_expr=_resolve_partition_expr(meta, "iceberg/partition_expr"),
            columns=columns,
            snapshot_id=int(snapshot_id) if snapshot_id is not None else None,
        )

        if catalog is None:
            return handler.load_input(request, context.type_hint)

        with catalog:
            return handler.load_input(request, context.type_hint)
