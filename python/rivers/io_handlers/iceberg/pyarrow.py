"""PyArrow ↔ Iceberg bridge — handles ``pa.Table`` and ``pa.RecordBatchReader``."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

import pyarrow as pa

from rivers.io_handlers.iceberg.base import ArrowIcebergTypeHandler, IcebergReadRequest

ArrowTypes = Union[pa.Table, pa.RecordBatchReader]


class PyArrowTypeHandler(ArrowIcebergTypeHandler[ArrowTypes]):
    """Handles pyarrow types for Iceberg IO."""

    @property
    def supported_types(self) -> Sequence[type[ArrowTypes]]:
        """PyArrow types this handler accepts as asset outputs / inputs."""
        return [pa.Table, pa.RecordBatchReader]

    def to_arrow(self, obj: ArrowTypes) -> pa.Table | pa.RecordBatchReader:
        """``obj`` as is; a reader streams into unpartitioned tables."""
        return obj

    def load_input(
        self, request: IcebergReadRequest, target_type: type[ArrowTypes]
    ) -> ArrowTypes:
        """Scan the table with pyiceberg.

        A ``pa.RecordBatchReader`` streams the scan; a ``pa.Table`` reads it
        into memory.
        """
        scan = request.scan(request.load_table())
        if target_type is pa.RecordBatchReader:
            return scan.to_arrow_batch_reader()
        return scan.to_arrow()
