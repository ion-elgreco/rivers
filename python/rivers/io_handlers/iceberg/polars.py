"""Polars ↔ Iceberg bridge — handles ``pl.DataFrame`` and ``pl.LazyFrame``."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

import polars as pl
import pyarrow as pa

from rivers.io_handlers.iceberg.base import ArrowIcebergTypeHandler, IcebergReadRequest

PolarsTypes = Union[pl.DataFrame, pl.LazyFrame]


class PolarsTypeHandler(ArrowIcebergTypeHandler[PolarsTypes]):
    """Handles polars types for Iceberg IO."""

    @property
    def supported_types(self) -> Sequence[type[PolarsTypes]]:
        """Polars types this handler accepts as asset outputs / inputs."""
        return [pl.DataFrame, pl.LazyFrame]

    def to_arrow(self, obj: PolarsTypes) -> pa.Table:
        """``obj`` as a ``pa.Table``; a ``LazyFrame`` is collected with the streaming engine."""
        if isinstance(obj, pl.LazyFrame):
            obj = obj.collect(engine="streaming")
        return obj.to_arrow()

    def load_input(
        self, request: IcebergReadRequest, target_type: type[PolarsTypes]
    ) -> PolarsTypes:
        """Scan the table with ``pl.scan_iceberg``.

        The partition predicate and the column projection are pushed into the
        polars query; a ``pl.LazyFrame`` is returned as is, a
        ``pl.DataFrame`` is collected with the streaming engine. Polars reads
        the files with the table's storage properties, as pyiceberg does.
        """
        table = request.table
        lf = pl.scan_iceberg(
            table,
            snapshot_id=request.snapshot_id,
            storage_options=storage_options(table.io.properties),
        )
        if (predicate := request.predicate) is not None:
            lf = lf.sql(f"SELECT * FROM self WHERE {predicate}")
        if request.columns:
            lf = lf.select(request.columns)

        if target_type is pl.LazyFrame:
            return lf
        return lf.collect(engine="streaming")


def storage_options(properties: dict[str, str]) -> dict[str, str]:
    """The storage keys of pyiceberg FileIO ``properties``, for ``pl.scan_iceberg``.

    Polars converts the keys it knows (``s3.*``, ``adls.*``, ...) for its own
    reader, which does not use pyiceberg's FileIO. Catalog keys such as
    ``uri`` have no dot; polars would pass them to its reader unchanged.
    """
    return {key: value for key, value in properties.items() if "." in key}
