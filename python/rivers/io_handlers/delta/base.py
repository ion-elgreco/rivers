"""Plug-in interface for type-specific Delta Lake read/write conversions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Generic, TypeVar

from arro3.core import RecordBatchReader

T = TypeVar("T")


class DeltaTypeHandler(ABC, Generic[T]):
    """Base class for type-specific Delta Lake read/write handlers.

    Concrete implementations register the Python types they can convert
    to/from Arrow (e.g. ``pyarrow.Table``, ``polars.DataFrame``) and provide
    the encode/decode bridge used by :class:`DeltaIOHandler`.
    """

    @property
    @abstractmethod
    def supported_types(self) -> Sequence[type[T]]:
        """Return the Python types this handler can serialize / deserialize."""
        ...

    @abstractmethod
    def to_arrow(self, obj: T) -> RecordBatchReader:
        """Convert the object to an arro3 RecordBatchReader for write_deltalake."""
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
