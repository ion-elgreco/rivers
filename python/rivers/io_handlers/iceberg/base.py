"""Plug-in interface for type-specific Iceberg reads and writes, and the shared Arrow write."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Generic, Literal, TypeVar

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import (
    CommitFailedException,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.table import DataScan, Table, Transaction, UpsertResult

from rivers._core import MetadataValue, OutputContext
from rivers._core.partitions import PartitionContext
from rivers.io_handlers._partition_sql import PartitionExpr, _build_predicate
from rivers.io_handlers.iceberg._partitions import (
    partition_fields,
    partition_filter,
    require_partition_expr,
)
from rivers.io_handlers.iceberg.config import UpsertConfig

T = TypeVar("T")

IcebergWriteMode = Literal["overwrite", "append", "upsert"]

IcebergSchemaMode = Literal["merge", "overwrite"]


@dataclass(frozen=True)
class IcebergWriteRequest:
    """Fully-resolved write parameters."""

    catalog: Catalog
    identifier: tuple[str, ...]
    mode: IcebergWriteMode
    schema_mode: IcebergSchemaMode | None
    partition: PartitionContext | None
    partition_expr: PartitionExpr | None
    table_properties: dict[str, str]
    snapshot_properties: dict[str, str]
    upsert_config: UpsertConfig


@dataclass(frozen=True)
class IcebergReadRequest:
    """Fully-resolved read parameters."""

    catalog: Catalog
    identifier: tuple[str, ...]
    partition: PartitionContext | None
    partition_expr: PartitionExpr | None
    columns: list[str] | None
    snapshot_id: int | None

    @property
    def predicate(self) -> str | None:
        """The partition's rows as a SQL ``WHERE`` clause, for SQL engines."""
        if self.partition is None:
            return None
        return _build_predicate(
            self.partition, require_partition_expr(self.partition_expr)
        )

    def load_table(self) -> Table:
        """The table to read."""
        return self.catalog.load_table(self.identifier)

    def scan(self, table: Table) -> DataScan:
        """A scan of ``table`` with the partition filter, the columns, and the snapshot."""
        return table.scan(
            row_filter=partition_filter(
                self.partition, self.partition_expr, table.schema()
            ),
            selected_fields=tuple(self.columns) if self.columns else ("*",),
            snapshot_id=self.snapshot_id,
        )


class IcebergTypeHandler(ABC, Generic[T]):
    """Base class for type-specific Iceberg read/write handlers.

    Implementations register the Python types they convert;
    :class:`IcebergIOHandler` picks one by the output's type or the input's
    type hint.
    """

    @property
    @abstractmethod
    def supported_types(self) -> Sequence[type[T]]:
        """Return the Python types this handler can serialize / deserialize."""
        ...

    @abstractmethod
    def load_input(self, request: IcebergReadRequest, target_type: type[T]) -> T:
        """Read the table as ``target_type``.

        Args:
            request: The table, partition, column projection, and snapshot.
            target_type: The exact type the caller expects back.
        """
        ...

    @abstractmethod
    def handle_output(
        self, context: OutputContext, obj: T, request: IcebergWriteRequest
    ) -> None:
        """Write ``obj`` to the table and record output metadata on ``context``."""
        ...


class ArrowIcebergTypeHandler(IcebergTypeHandler[T], ABC):
    """Base class for handlers that write through Arrow with pyiceberg.

    Subclasses convert their type with :meth:`to_arrow`; the write and its
    output metadata are shared.
    """

    @abstractmethod
    def to_arrow(self, obj: T) -> pa.Table | pa.RecordBatchReader:
        """``obj`` as Arrow data. A reader streams into unpartitioned tables."""
        ...

    def handle_output(
        self, context: OutputContext, obj: T, request: IcebergWriteRequest
    ) -> None:
        """Write ``obj`` in one commit and record table-level metadata.

        Nanosecond timestamps are written as microseconds and zoned
        timestamps as UTC, the timestamps Iceberg v2 stores. A new table gets
        the partition spec of the asset's partitions; an existing table keeps
        its spec. The snapshot id is registered as the data version.
        """
        start = time.monotonic()
        data = _iceberg_types(self.to_arrow(obj))
        table = _load_or_create(request, data.schema)
        if isinstance(data, pa.RecordBatchReader) and (
            request.mode == "upsert" or not table.spec().is_unpartitioned()
        ):
            data = data.read_all()

        upserted = _write(table, data, request)
        duration = time.monotonic() - start

        context.add_output_metadata(
            _output_metadata(table, request, duration, upserted)
        )
        if (snapshot := table.current_snapshot()) is not None:
            context.register_data_version(str(snapshot.snapshot_id))


def _iceberg_types(
    data: pa.Table | pa.RecordBatchReader,
) -> pa.Table | pa.RecordBatchReader:
    schema = pa.schema(
        [field.with_type(_iceberg_type(field.type)) for field in data.schema],
        metadata=data.schema.metadata,
    )
    if schema.equals(data.schema):
        return data

    if isinstance(data, pa.Table):
        return data.cast(schema, safe=False)
    return pa.RecordBatchReader.from_batches(
        schema, (batch.cast(schema, safe=False) for batch in data)
    )


def _iceberg_type(data_type: pa.DataType) -> pa.DataType:
    if not pa.types.is_timestamp(data_type):
        return data_type

    unit = "us" if data_type.unit == "ns" else data_type.unit
    return pa.timestamp(unit, None if data_type.tz is None else "UTC")


def _load_or_create(request: IcebergWriteRequest, schema: pa.Schema) -> Table:
    catalog, identifier = request.catalog, request.identifier
    try:
        return catalog.load_table(identifier)
    except NoSuchTableError:
        pass

    fields = []
    if request.partition is not None and request.partition_expr is not None:
        fields = partition_fields(request.partition, request.partition_expr, schema)

    namespace = identifier[:-1]
    try:
        catalog.create_namespace_if_not_exists(namespace)
    except Exception:
        # the SQL catalog reports a concurrent create as a database error
        if not catalog.namespace_exists(namespace):
            raise

    try:
        with catalog.create_table_transaction(
            identifier, schema=schema, properties=request.table_properties
        ) as txn:
            if fields:
                with txn.update_spec() as spec:
                    for column, transform in fields:
                        spec.add_field(column, transform)
    except (CommitFailedException, TableAlreadyExistsError):
        # another writer created it first; depending on timing the create
        # fails with either error
        if not catalog.table_exists(identifier):
            raise

    return catalog.load_table(identifier)


def _write(
    table: Table, data: pa.Table | pa.RecordBatchReader, request: IcebergWriteRequest
) -> UpsertResult | None:
    upserted = None
    with table.transaction() as txn:
        # pyiceberg's upsert reads matches with the last snapshot's schema, so
        # it cannot write a column that the same transaction adds
        if request.schema_mode is not None and request.mode != "upsert":
            _evolve_schema(txn, data.schema, request.schema_mode)

        properties = request.table_properties
        if changed := {
            k: v for k, v in properties.items() if table.properties.get(k) != v
        }:
            txn.set_properties(changed)

        snapshot_properties = request.snapshot_properties
        if request.mode == "upsert":
            assert isinstance(data, pa.Table)
            upsert = request.upsert_config
            upserted = txn.upsert(
                data,
                join_cols=upsert.join_cols,
                when_matched_update_all=upsert.when_matched_update_all,
                when_not_matched_insert_all=upsert.when_not_matched_insert_all,
                snapshot_properties=snapshot_properties,
            )
        elif request.mode == "append" or table.current_snapshot() is None:
            txn.append(data, snapshot_properties=snapshot_properties)
        else:
            row_filter = partition_filter(
                request.partition, request.partition_expr, txn.table_metadata.schema()
            )
            txn.overwrite(
                data,
                overwrite_filter=row_filter,
                snapshot_properties=snapshot_properties,
            )

    return upserted


def _evolve_schema(
    txn: Transaction, data_schema: pa.Schema, schema_mode: IcebergSchemaMode
) -> None:
    with txn.update_schema() as update:
        if schema_mode == "overwrite":
            for name in txn.table_metadata.schema().column_names:
                if name not in data_schema.names:
                    update.delete_column(name)
        update.union_by_name(data_schema)


def _output_metadata(
    table: Table,
    request: IcebergWriteRequest,
    duration: float,
    upserted: UpsertResult | None,
) -> dict[str, Any]:
    snapshot = table.current_snapshot()
    summary = snapshot.summary if snapshot is not None else None
    meta: dict[str, Any] = {
        "iceberg/table": ".".join(request.identifier),
        "iceberg/mode": request.mode,
        "iceberg/num_rows": int(summary.get("total-records", 0)) if summary else 0,
        "iceberg/size_bytes": int(summary.get("total-files-size", 0)) if summary else 0,
        "iceberg/write_duration_s": round(duration, 6),
        "iceberg/metadata_location": table.metadata_location,
        "rivers/schema": MetadataValue.schema(table.schema().as_arrow()),
    }
    if snapshot is not None:
        meta["iceberg/snapshot_id"] = snapshot.snapshot_id
    if upserted is not None:
        meta["iceberg/rows_updated"] = upserted.rows_updated
        meta["iceberg/rows_inserted"] = upserted.rows_inserted

    return meta
