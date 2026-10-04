"""Metadata an IO handler adds in ``handle_output`` lands on the materialization."""

import json
import os
from pathlib import Path
from typing import Any

import pytest

import rivers as rs

IN_PROCESS = rs.Executor.in_process()
PARALLEL = rs.Executor.parallel(max_workers=2)


class JsonHandler(rs.BaseIOHandler):
    """Writes each output as JSON under ``root`` and records what it wrote,
    and in which process."""

    root: str

    def _path(self, name: str) -> Path:
        return Path(self.root) / f"{name}.json"

    def handle_output(self, context: rs.OutputContext, obj: Any) -> None:
        path = self._path(context.asset_name)
        path.write_text(json.dumps(obj))
        context.add_output_metadata(
            {"rows": len(obj), "path": str(path), "pid": os.getpid()}
        )

    def load_input(self, context: rs.InputContext) -> Any:
        return json.loads(self._path(context.asset_name).read_text())


@pytest.fixture(params=[IN_PROCESS, PARALLEL], ids=["in_process", "parallel"])
def executor(request):
    return request.param


def materialization_metadata(storage, asset_key: str) -> dict[str, Any]:
    """Raw values of the latest materialization's metadata, without ``pid``."""
    event = storage.get_latest_materialization(asset_key)
    assert event is not None
    meta = {
        key: next(iter(json.loads(value).values()))["value"]
        for key, value in event.metadata
    }
    meta.pop("pid", None)
    return meta


def written_in_worker(storage, asset_key: str) -> bool:
    """Whether the handler wrote ``asset_key`` in a process other than this one."""
    event = storage.get_latest_materialization(asset_key)
    assert event is not None
    (pid,) = [value for key, value in event.metadata if key == "pid"]
    return json.loads(pid)["Int"]["value"] != os.getpid()


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_handler_metadata_reaches_the_materialization(
    executor, is_async, storage, tmp_path
):
    """Entries the handler adds are stored on the materialization, next to the
    asset's own ``add_output_metadata`` entries."""
    io = JsonHandler(root=str(tmp_path))

    if is_async:

        @rs.Asset(io_handler=io)
        async def orders(context: rs.AssetExecutionContext) -> list:
            context.add_output_metadata({"source": "api"})
            return [1, 2, 3]
    else:

        @rs.Asset(io_handler=io)
        def orders(context: rs.AssetExecutionContext) -> list:
            context.add_output_metadata({"source": "api"})
            return [1, 2, 3]

    @rs.Asset(io_handler=io)
    def customers() -> list:
        return ["ann", "bob"]

    @rs.Asset(io_handler=io)
    def regions() -> list:
        return ["eu"]

    repo = rs.CodeRepository(
        assets=[orders, customers, regions], default_executor=executor
    )
    repo.resolve(storage=storage)
    assert repo.materialize().success

    assert materialization_metadata(storage, "orders") == {
        "source": "api",
        "rows": 3,
        "path": str(tmp_path / "orders.json"),
    }
    assert materialization_metadata(storage, "customers") == {
        "rows": 2,
        "path": str(tmp_path / "customers.json"),
    }
    assert materialization_metadata(storage, "regions") == {
        "rows": 1,
        "path": str(tmp_path / "regions.json"),
    }
    in_worker = executor is PARALLEL
    assert written_in_worker(storage, "orders") is (in_worker and not is_async)
    assert written_in_worker(storage, "customers") is in_worker
    assert written_in_worker(storage, "regions") is in_worker


@pytest.mark.parametrize("shape", ["dict", "generator"])
def test_handler_metadata_per_output_of_a_multi_asset(
    executor, shape, storage, tmp_path
):
    """Each output of a multi-asset keeps the metadata of its own write."""
    io = JsonHandler(root=str(tmp_path))
    outputs = [
        rs.AssetDef("users", io_handler=io),
        rs.AssetDef("events", io_handler=io),
    ]

    if shape == "dict":

        @rs.Asset.from_multi(output_defs=outputs)
        def load():
            return {"users": ["ann", "bob"], "events": [1, 2, 3]}
    else:

        @rs.Asset.from_multi(output_defs=outputs)
        def load():
            yield rs.Output(value=["ann", "bob"], output_name="users")
            yield rs.Output(value=[1, 2, 3], output_name="events")

    @rs.Asset(io_handler=io)
    def regions() -> list:
        return ["eu"]

    repo = rs.CodeRepository(assets=[load, regions], default_executor=executor)
    repo.resolve(storage=storage)
    assert repo.materialize().success

    assert materialization_metadata(storage, "users") == {
        "rows": 2,
        "path": str(tmp_path / "users.json"),
    }
    assert materialization_metadata(storage, "events") == {
        "rows": 3,
        "path": str(tmp_path / "events.json"),
    }
    assert written_in_worker(storage, "users") is (executor is PARALLEL)


def test_handler_metadata_wins_on_a_key_conflict(executor, storage, tmp_path):
    """On a key the asset and its handler both set, the handler's entry is
    stored: it describes what was written."""
    io = JsonHandler(root=str(tmp_path))

    @rs.Asset(io_handler=io)
    def orders(context: rs.AssetExecutionContext) -> rs.Output:
        context.add_output_metadata({"rows": 100, "note": "from context"})
        return rs.Output(
            value=[1, 2, 3], metadata={"path": "s3://guess", "owner": "ops"}
        )

    @rs.Asset(io_handler=io)
    def regions() -> list:
        return ["eu"]

    repo = rs.CodeRepository(assets=[orders, regions], default_executor=executor)
    repo.resolve(storage=storage)
    assert repo.materialize().success

    assert materialization_metadata(storage, "orders") == {
        "rows": 3,
        "path": str(tmp_path / "orders.json"),
        "note": "from context",
        "owner": "ops",
    }
