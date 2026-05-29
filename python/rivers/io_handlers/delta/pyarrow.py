"""PyArrow ↔ Delta Lake bridge — handles ``pa.Table`` and ``pa.RecordBatchReader``."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

import pyarrow as pa
from arro3.core import RecordBatchReader
from deltalake import DeltaTable, QueryBuilder

from rivers.io_handlers.delta.base import DeltaTypeHandler

ArrowTypes = Union[pa.Table, pa.RecordBatchReader]


class PyArrowTypeHandler(DeltaTypeHandler[ArrowTypes]):
    """Handles pyarrow types for Delta Lake IO."""

    @property
    def supported_types(self) -> Sequence[type[ArrowTypes]]:
        """PyArrow types this handler accepts as asset outputs / inputs."""
        return [pa.Table, pa.RecordBatchReader]

    def to_arrow(self, obj: ArrowTypes) -> RecordBatchReader:
        """Wrap ``obj`` in an arro3 ``RecordBatchReader`` accepted by ``write_deltalake``."""
        return RecordBatchReader.from_arrow(obj)  # type: ignore[arg-type]

    def load_input(
        self,
        table_uri: str,
        table_name: str,
        storage_options: dict[str, str] | None,
        predicate: str | None,
        target_type: type[ArrowTypes],
        columns: list[str] | None = None,
        version: int | None = None,
    ) -> ArrowTypes:
        """Read the Delta table via DataFusion and return the requested PyArrow type.

        When ``target_type`` is ``pa.RecordBatchReader`` the stream is returned
        without buffering; otherwise the reader is fully drained into a
        ``pa.Table``.
        """
        dt = DeltaTable(table_uri, storage_options=storage_options, version=version)
        select = ", ".join(columns) if columns else "*"
        sql_query = f'SELECT {select} FROM "{table_name}"'
        if predicate is not None:
            sql_query += f" WHERE {predicate}"

        reader = QueryBuilder().register(table_name, dt).execute(sql_query)
        data = pa.RecordBatchReader.from_stream(reader)
        if target_type is pa.RecordBatchReader:
            return data
        return data.read_all()
