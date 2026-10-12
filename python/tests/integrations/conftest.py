"""Shared setup for the frame validator integrations: pandera and dataframely."""

import json
import os
from dataclasses import dataclass
from typing import Any

import pytest
from _helpers import materialize_beside_sibling

import rivers as rs


@dataclass
class Env:
    """An executor from ``executor_env``, with a storage and an IO handler."""

    storage: rs.Storage
    executor: rs.Executor
    handler: rs.BaseIOHandler

    @property
    def parallel(self) -> bool:
        return isinstance(self.executor, rs.Executor.Parallel)

    def asset(self, hint, value, *, is_async=False, pid_file=None):
        """An ``orders`` asset that returns ``value`` under the return hint ``hint``."""
        if is_async:

            @rs.Asset(name="orders", io_handler=self.handler)
            async def orders_async() -> hint:
                return value

            return orders_async

        @rs.Asset(name="orders", io_handler=self.handler)
        def orders() -> hint:
            if pid_file is not None:
                pid_file.write_text(str(os.getpid()))
            return value

        return orders

    def materialize(self, *assets) -> tuple[rs.CodeRepository, rs.RunResult]:
        return materialize_beside_sibling(
            self.storage, self.executor, self.handler, *assets
        )

    def stored_json(self, asset: str, key: str) -> Any:
        """The JSON metadata entry ``key`` of ``asset``'s materialization, decoded."""
        materialization = self.storage.get_latest_materialization(asset)
        [(variant, payload)] = json.loads(dict(materialization.metadata)[key]).items()
        # Metadata that a parallel worker sends back loses its type, so only the
        # value is compared there.
        if not self.parallel:
            assert variant == "Json"
        return json.loads(payload["value"])


@pytest.fixture
def env(storage, executor_env) -> Env:
    executor, io_factory = executor_env
    return Env(storage, executor, io_factory())
