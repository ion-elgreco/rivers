"""Pandas ↔ Iceberg bridge — handles ``pd.DataFrame``."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd
import pyarrow as pa

from rivers.io_handlers.iceberg.base import ArrowIcebergTypeHandler, IcebergReadRequest


class PandasTypeHandler(ArrowIcebergTypeHandler[pd.DataFrame]):
    """Handles pandas ``DataFrame`` for Iceberg IO."""

    @property
    def supported_types(self) -> Sequence[type[pd.DataFrame]]:
        """Pandas types this handler accepts as asset outputs / inputs."""
        return [pd.DataFrame]

    def to_arrow(self, obj: pd.DataFrame) -> pa.Table:
        """``obj`` as a ``pa.Table``, without the index.

        Reading back yields a fresh ``RangeIndex``.
        """
        return pa.Table.from_pandas(obj, preserve_index=False)

    def load_input(
        self, request: IcebergReadRequest, target_type: type[pd.DataFrame]
    ) -> pd.DataFrame:
        """Scan the table with pyiceberg into a ``pd.DataFrame``."""
        return request.scan(request.load_table()).to_pandas()
