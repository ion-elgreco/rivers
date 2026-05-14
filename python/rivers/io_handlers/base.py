from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pydantic_settings import BaseSettings

from rivers._core import InputContext, OutputContext


class BaseIOHandler(BaseSettings, ABC):
    """Abstract base for IO handlers.

    Extends ``pydantic_settings.BaseSettings`` so handler configuration can be
    resolved from environment variables, ``.env`` files, or explicit kwargs.

    Attach to an asset via ``@Asset(io_handler=MyHandler(...))``.

    ``handle_output`` is called after the asset function returns to persist the result.
    Use ``context.add_output_metadata(key, value)`` inside ``handle_output`` to attach
    metadata about the write (e.g. path, byte size, duration).

    ``load_input`` is called to load a previously persisted asset output when the
    in-memory result is not available.
    """

    @abstractmethod
    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Persist ``obj`` produced by an asset materialization.

        Args:
            context: Output metadata (asset name, partition, configured asset
                metadata, optional type hint).
            obj: The value returned (or yielded) by the asset function.
        """
        ...

    @abstractmethod
    def load_input(self, context: InputContext) -> Any:
        """Load a previously persisted asset output.

        Args:
            context: Input metadata describing the upstream asset, the partition
                to read, and the type hint declared on the consumer side.

        Returns:
            The deserialized value to inject into the downstream asset/task.
        """
        ...
