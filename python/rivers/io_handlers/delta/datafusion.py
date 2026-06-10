"""DataFusion ↔ Delta Lake bridge — handles ``datafusion.DataFrame``."""

from __future__ import annotations

from collections.abc import Iterator, Sequence

import pyarrow as pa
from arro3.core import RecordBatchReader
from datafusion import DataFrame, SessionContext
from deltalake import DeltaTable

from rivers.io_handlers.delta.base import ArrowDeltaTypeHandler


def _stream_pyarrow_batches(df: DataFrame) -> Iterator[pa.RecordBatch]:
    """Yield ``df`` as pyarrow batches via DataFusion's bounded ``execute_stream``."""
    for batch in df.execute_stream():
        yield batch.to_pyarrow()


class DataFusionTypeHandler(ArrowDeltaTypeHandler[DataFrame]):
    """Handles datafusion ``DataFrame`` for Delta Lake IO."""

    @property
    def supported_types(self) -> Sequence[type[DataFrame]]:
        """DataFusion types this handler accepts as asset outputs / inputs."""
        return [DataFrame]

    def to_arrow(self, obj: DataFrame) -> RecordBatchReader:
        """Stream ``obj`` into an arro3 ``RecordBatchReader`` for ``write_deltalake``.

        Uses DataFusion's ``execute_stream`` rather than the ``DataFrame`` Arrow C
        stream (which coalesces partitions and buffers ~half the result) so the
        plan executes with bounded memory as ``write_deltalake`` pulls batches.
        """
        schema = pa.schema(obj.schema())
        reader = pa.RecordBatchReader.from_batches(schema, _stream_pyarrow_batches(obj))
        return RecordBatchReader.from_arrow(reader)  # type: ignore[arg-type]

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
        """Register the Delta table with a ``SessionContext`` and return a lazy query.

        Column projection and the partition predicate are pushed through SQL into
        the Delta scan. The returned ``DataFrame`` is lazy; it is executed by the
        caller (``.collect()`` or the Arrow C stream interface).

        The backing ``SessionContext`` is attached to the frame as
        ``DataFrame.rivers_ctx``, with the table registered under ``table_name``.
        """
        dt = DeltaTable(table_uri, storage_options=storage_options, version=version)
        ctx = SessionContext()
        ctx.register_table(table_name, dt)
        select = ", ".join(columns) if columns else "*"
        sql_query = f'SELECT {select} FROM "{table_name}"'
        if predicate is not None:
            sql_query += f" WHERE {predicate}"
        df = ctx.sql(sql_query)

        # Expose the SessionContext as ``rivers_ctx`` so callers can reuse the
        # session (register more tables, compose further queries). It doubles as
        # the keep-alive the Delta FFI table provider requires: dropping it early
        # fails with "TaskContextProvider went out of scope over FFI boundary".
        df.rivers_ctx = ctx  # type: ignore[attr-defined]
        return df
