"""Reusable helpers for the rivers test suite.

Imported directly (no relative-import needed) because pytest's rootpath
mechanism adds `python/tests/` to `sys.path`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import rivers as rs


def static_pd(keys):
    return rs.PartitionsDefinition.static_(keys)


def daily_pd():
    return rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))


def multi_pd():
    return rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "date": rs.PartitionsDefinition.static_(["d1", "d2"]),
        }
    )


class DictIOHandler(rs.BaseIOHandler):
    """Dict-backed IO handler — raises FileNotFoundError on missing key."""

    store: dict[str, Any] = {}

    def handle_output(self, context: rs.OutputContext, obj):
        self.store[context.asset_name] = obj

    def load_input(self, context: rs.InputContext):
        key = context.asset_name
        if key not in self.store:
            raise FileNotFoundError(f"No data for '{key}'")
        return self.store[key]


class TrackingHandler(rs.BaseIOHandler):
    """IO handler that records partition contexts seen during execution.

    Always returns ``default_load_value`` from load_input so downstream
    functions can execute. Tests inspect ``output_partitions`` and
    ``load_input_partitions`` to assert routing.
    """

    default_load_value: Any = 0
    output_partitions: list = []
    load_input_partitions: list = []

    def handle_output(self, context, obj):
        self.output_partitions.append(context.partition)

    def load_input(self, context):
        self.load_input_partitions.append(context.partition)
        return self.default_load_value


class CapturingHandler(rs.BaseIOHandler):
    """IO handler that captures contexts and stores values flat + by partition.

    Stores values keyed by ``(asset_name, partition_key)`` when partitioned,
    plus a flat ``store`` dict that always holds the latest value per asset
    for simple assertions.
    """

    output_contexts: list = []
    input_contexts: list = []
    store: dict[str, Any] = {}
    partitioned_store: dict[tuple[str, str], Any] = {}

    def handle_output(self, context: rs.OutputContext, obj):
        self.output_contexts.append(context)
        self.store[context.asset_name] = obj
        if context.partition is not None:
            pk = str(context.partition.key)
            self.partitioned_store[(context.asset_name, pk)] = obj

    def load_input(self, context: rs.InputContext):
        self.input_contexts.append(context)
        if context.partition is not None:
            pk = str(context.partition.key)
            val = self.partitioned_store.get((context.asset_name, pk))
            if val is not None:
                return val
        return self.store.get(context.asset_name)


def make_repo(assets, storage=None, *, executor=None, **kwargs) -> rs.CodeRepository:
    """Build a CodeRepository (defaulting to in-process executor) and resolve it."""
    if executor is None:
        executor = rs.Executor.in_process()
    repo = rs.CodeRepository(assets=assets, default_executor=executor, **kwargs)
    repo.resolve(storage=storage)
    return repo
