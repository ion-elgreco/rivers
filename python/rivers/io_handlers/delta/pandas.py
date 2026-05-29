"""Pandas ↔ Delta Lake bridge — handles ``pd.DataFrame``."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd
import pyarrow as pa
from arro3.core import RecordBatchReader
from deltalake import DeltaTable, QueryBuilder

from rivers.io_handlers.delta.base import DeltaTypeHandler


class PandasTypeHandler(DeltaTypeHandler[pd.DataFrame]):
    """Handles pandas ``DataFrame`` for Delta Lake IO."""

    @property
    def supported_types(self) -> Sequence[type[pd.DataFrame]]:
        """Pandas types this handler accepts as asset outputs / inputs."""
        return [pd.DataFrame]

    def to_arrow(self, obj: pd.DataFrame) -> RecordBatchReader:
        """Convert ``obj`` to an arro3 ``RecordBatchReader`` for ``write_deltalake``.

        The pandas index is dropped (``preserve_index=False``) so only the frame's
        columns are persisted; reading back yields a fresh ``RangeIndex``.
        """
        table = pa.Table.from_pandas(obj, preserve_index=False)
        return RecordBatchReader.from_arrow(table)  # type: ignore[arg-type]

    def load_input(
        self,
        table_uri: str,
        table_name: str,
        storage_options: dict[str, str] | None,
        predicate: str | None,
        target_type: type[pd.DataFrame],
        columns: list[str] | None = None,
        version: int | None = None,
    ) -> pd.DataFrame:
        """Read the Delta table via DataFusion and return a ``pd.DataFrame``.

        Column projection and the partition predicate are pushed into the query;
        the Arrow result is fully materialized before conversion to pandas.
        """
        dt = DeltaTable(table_uri, storage_options=storage_options, version=version)
        select = ", ".join(columns) if columns else "*"
        sql_query = f'SELECT {select} FROM "{table_name}"'
        if predicate is not None:
            sql_query += f" WHERE {predicate}"

        reader = QueryBuilder().register(table_name, dt).execute(sql_query)
        return pa.RecordBatchReader.from_stream(reader).read_all().to_pandas()
