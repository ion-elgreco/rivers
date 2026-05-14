from __future__ import annotations

import sys
from typing import Any

from pydantic import PrivateAttr

from rivers._core import InputContext, OutputContext, PartitionContext, PartitionKey
from rivers.io_handlers.base import BaseIOHandler


def _partition_suffix(partition: PartitionContext | None) -> str | None:
    """Render a partition key into a stable filesystem-style suffix.

    Returns ``None`` when there is no partition. Single-dimension keys join their
    component values with ``/``; multi-dimension keys produce ``dim=v1|v2`` segments
    in alphabetical order so the same logical partition always maps to the same path.
    """
    if partition is None:
        return None
    key = partition.key
    if isinstance(key, PartitionKey.Single):
        return "/".join(key.key)
    if isinstance(key, PartitionKey.Multi):
        return "/".join(f"{k}={'|'.join(v)}" for k, v in sorted(key.keys.items()))
    return None


class InMemoryIOHandler(BaseIOHandler):
    """Store asset outputs in an in-memory dictionary.

    Useful for testing and short-lived pipelines where persistence is not needed.
    Partitioned outputs are keyed by ``"<asset>/<partition_suffix>"`` so distinct
    partitions of the same asset coexist.
    """

    _storage: dict[str, Any] = PrivateAttr(default_factory=dict)

    @staticmethod
    def _storage_key(asset_name: str, partition: PartitionContext | None) -> str:
        """Compose the dictionary key for ``asset_name`` at ``partition``."""
        suffix = _partition_suffix(partition)
        if suffix is not None:
            return f"{asset_name}/{suffix}"
        return asset_name

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Store ``obj`` under the asset/partition key and record byte size metadata."""
        key = self._storage_key(context.asset_name, context.partition)
        self._storage[key] = obj
        context.add_output_metadata(
            {"storage": "memory", "size_bytes": sys.getsizeof(obj)}
        )

    def load_input(self, context: InputContext) -> Any:
        """Return the value stored for the upstream asset and partition."""
        key = self._storage_key(context.asset_name, context.partition)
        return self._storage[key]
