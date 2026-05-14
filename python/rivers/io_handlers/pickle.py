from __future__ import annotations

import pickle
import time
from typing import Any

import obstore
from obstore.store import ObjectStore
from pydantic_settings import SettingsConfigDict

from rivers._core import InputContext, OutputContext, PartitionContext
from rivers.io_handlers.base import BaseIOHandler
from rivers.io_handlers.memory import _partition_suffix


class PickleIOHandler(BaseIOHandler):
    """Persist asset outputs as pickle files via any object store backend.

    Configured with an ``obstore`` :class:`ObjectStore` (local FS, S3, GCS, …) and
    an optional path ``prefix``. Partitioned outputs are stored under
    ``<prefix>/<asset>/<partition_suffix>/data.pkl``; non-partitioned assets use
    ``<prefix>/<asset>.pkl``.
    """

    model_config = SettingsConfigDict(arbitrary_types_allowed=True)

    store: ObjectStore
    prefix: str = ""

    def _key_for(
        self, asset_name: str, partition: PartitionContext | None = None
    ) -> str:
        """Build the object-store key for ``asset_name`` at ``partition``."""
        suffix = _partition_suffix(partition)
        if suffix is not None:
            base = f"{asset_name}/{suffix}/data.pkl"
        else:
            base = f"{asset_name}.pkl"
        if self.prefix:
            return f"{self.prefix}/{base}"
        return base

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Pickle ``obj`` and put it in the object store, recording size and timing."""
        key = self._key_for(context.asset_name, context.partition)
        data = pickle.dumps(obj)

        start = time.monotonic()
        obstore.put(self.store, key, data)
        duration = time.monotonic() - start

        context.add_output_metadata(
            {
                "path": key,
                "serializer": "pickle",
                "size_bytes": len(data),
                "write_duration_s": round(duration, 6),
            }
        )

    def load_input(self, context: InputContext) -> Any:
        """Get the pickled object from the store and unpickle it."""
        result = obstore.get(
            self.store, self._key_for(context.asset_name, context.partition)
        )
        data = bytes(result.bytes())
        return pickle.loads(data)  # noqa: S301
