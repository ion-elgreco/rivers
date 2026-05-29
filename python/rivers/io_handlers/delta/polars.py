"""Polars ↔ Delta Lake bridge — handles ``pl.DataFrame`` and ``pl.LazyFrame``."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

import polars as pl
from arro3.core import RecordBatchReader
from polars_deltalake import scan_delta

from rivers.io_handlers.delta.base import DeltaTypeHandler

PolarsTypes = Union[pl.DataFrame, pl.LazyFrame]


class PolarsTypeHandler(DeltaTypeHandler[PolarsTypes]):
    """Handles polars types for Delta Lake IO."""

    @property
    def supported_types(self) -> Sequence[type[PolarsTypes]]:
        """Polars types this handler accepts as asset outputs / inputs."""
        return [pl.DataFrame, pl.LazyFrame]

    def to_arrow(self, obj: PolarsTypes) -> RecordBatchReader:
        """Materialize ``obj`` into an Arrow ``RecordBatchReader`` for ``write_deltalake``.

        ``LazyFrame`` is collected lazily as streaming batches; ``DataFrame`` is
        wrapped directly. Other types raise ``TypeError``.
        """
        if isinstance(obj, pl.LazyFrame):
            return RecordBatchReader.from_arrow(obj.collect_batches(lazy=True))  # type: ignore[arg-type]
        elif isinstance(obj, pl.DataFrame):
            return RecordBatchReader.from_arrow(obj)
        else:
            raise TypeError(f"Expected polars type, got {type(obj)}")

    def load_input(
        self,
        table_uri: str,
        table_name: str,
        storage_options: dict[str, str] | None,
        predicate: str | None,
        target_type: type[PolarsTypes],
        columns: list[str] | None = None,
        version: int | None = None,
    ) -> PolarsTypes:
        """Scan the Delta table and return a ``LazyFrame`` or collected ``DataFrame``.

        Predicate and column projection are pushed into the polars query; when
        ``target_type`` is :class:`pl.LazyFrame` the lazy frame is returned
        as-is, otherwise it is collected with the streaming engine.
        """
        lf = scan_delta(table_uri, storage_options=storage_options, version=version)
        if columns:
            lf = lf.select(columns)
        if predicate is not None:
            sql_query = "SELECT * FROM self"
            sql_query += f" WHERE {predicate}"
            lf = lf.sql(sql_query)

        if target_type is pl.LazyFrame:
            return lf
        return lf.collect(engine="streaming")
