"""Plug-in interface for type-specific Delta Lake read/write conversions."""

from __future__ import annotations

import json
import time
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Generic, Literal, TypeVar

from arro3.core import RecordBatchReader
from deltalake import CommitProperties, DeltaTable, WriterProperties, write_deltalake

from rivers._core import OutputContext
from rivers.io_handlers.delta.config import MergeConfig
from rivers.io_handlers.delta.merge import _merge_execute

T = TypeVar("T")

DeltaWriteMode = Literal[
    "overwrite", "append", "error", "ignore", "merge", "create_or_replace"
]

DeltaSchemaMode = Literal["overwrite", "merge"]


class DeltaTypeHandler(ABC, Generic[T]):
    """Base class for type-specific Delta Lake read/write handlers.

    Concrete implementations register the Python types they can convert
    to/from Arrow (e.g. ``pyarrow.Table``, ``polars.DataFrame``) or PySpark
    (e.g. ``pyspark.sql.DataFrame``) and provide the encode/decode bridge
    used by :class:`DeltaIOHandler`.
    """

    @property
    @abstractmethod
    def supported_types(self) -> Sequence[type[T]]:
        """Return the Python types this handler can serialize / deserialize."""
        ...

    @abstractmethod
    def load_input(
        self,
        table_uri: str,
        table_name: str,
        storage_options: dict[str, str] | None,
        predicate: str | None,
        target_type: type[T],
        columns: list[str] | None = None,
        version: int | None = None,
    ) -> T:
        """Read a Delta table back into ``target_type``.

        Args:
            table_uri: Location of the Delta table.
            table_name: The asset / root name, usable as a SQL table identifier.
            storage_options: Filesystem credentials / options forwarded to ``deltalake``.
            predicate: Optional SQL ``WHERE`` clause for partition / row filtering.
            target_type: The exact type the caller expects back.
            columns: Optional column projection.
            version: Optional time-travel version.
        """
        ...

    @abstractmethod
    def handle_output(self, context: OutputContext, obj: T, request: DeltaWriteRequest):
        """Write ``obj`` as a Delta commit and record table-level metadata."""
        ...


class ArrowDeltaTypeHandler(DeltaTypeHandler, ABC, Generic[T]):
    """Base class for Arrow-based type-specific Delta Lake read/write handlers.

    Concrete implementations register the Python types they can convert
    to/from Arrow (e.g. ``pyarrow.Table``, ``polars.DataFrame``) and provide
    the encode/decode bridge used by :class:`DeltaIOHandler`.
    """

    @abstractmethod
    def to_arrow(self, obj: T) -> RecordBatchReader:
        """Convert the object to an arro3 RecordBatchReader for write_deltalake."""
        ...

    def handle_output(self, context: OutputContext, obj: T, request: DeltaWriteRequest):
        """Write ``obj`` as a Delta commit and record table-level metadata."""

        merge_stats: dict[str, Any] | None = None
        start = time.monotonic()
        data = self.to_arrow(obj)
        if request.delta_write_mode == "merge":
            merge_stats = _merge_execute(
                uri=request.table_uri,
                data=data,
                merge_config=request.merge_config,
                partition_predicate=request.predicate,
                merge_predicate_override=request.merge_predicate_override,
                storage_options=request.storage_options,
                writer_properties=request.writer_properties,
                commit_properties=request.commit_properties,
            )
        elif request.delta_write_mode == "create_or_replace":
            DeltaTable.create(
                table_uri=request.table_uri,
                schema=data.schema,
                mode="overwrite",
                partition_by=request.partition_by,
                configuration=request.table_configuration,
                storage_options=request.storage_options,
                commit_properties=request.commit_properties,
            )
            write_deltalake(
                request.table_uri,
                data,
                mode="append",
                storage_options=request.storage_options,
                writer_properties=request.writer_properties,  # type: ignore[arg-type]
                commit_properties=request.commit_properties,
            )
        else:
            write_deltalake(
                request.table_uri,
                data,
                mode=request.delta_write_mode,  # type: ignore[arg-type]
                schema_mode=request.schema_mode,
                partition_by=request.partition_by,
                predicate=request.predicate,
                storage_options=request.storage_options,
                configuration=request.table_configuration,
                writer_properties=request.writer_properties,  # type: ignore[arg-type]
                commit_properties=request.commit_properties,
            )
        duration = time.monotonic() - start

        dt = DeltaTable(request.table_uri, storage_options=request.storage_options)
        actions = dt.get_add_actions(flatten=True)
        num_rows = sum(actions.column("num_records").to_pylist())
        size_bytes = sum(actions.column("size_bytes").to_pylist())

        from rivers import MetadataValue

        arrow_schema = dt.schema().to_arrow()

        output_meta: dict[str, Any] = {
            "delta/table_uri": request.table_uri,
            "delta/mode": request.delta_write_mode,
            "delta/num_rows": num_rows,
            "delta/size_bytes": size_bytes,
            "delta/write_duration_s": round(duration, 6),
            "delta/version": dt.version(),
            "rivers/schema": MetadataValue.schema(arrow_schema),
        }
        if merge_stats is not None:
            output_meta["delta/num_output_rows"] = merge_stats.get("num_output_rows", 0)
            output_meta["delta/merge_stats"] = json.dumps(merge_stats)

        context.add_output_metadata(output_meta)


@dataclass(frozen=True)
class DeltaWriteRequest:
    """Fully-resolved write parameters."""

    table_uri: str
    table_name: str
    delta_write_mode: DeltaWriteMode
    schema_mode: DeltaSchemaMode | None
    predicate: str | None
    partition_by: list[str] | None
    table_configuration: dict[str, str] | None
    writer_properties: WriterProperties | None
    commit_properties: CommitProperties | None
    merge_config: MergeConfig | None
    merge_predicate_override: str | None
    storage_options: dict[str, str] | None
