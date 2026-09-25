"""IO handler resolution matrix — exercises the ``IOHandlerRegistry`` chain
through the public ``CodeRepository.io_handler_for_output`` accessor.

Checks the same chain the executor walks at materialize time, but without
running any steps. The corresponding parallel-mode regression
(per-input override honored under loky) lives at
``tests/executor/test_parallel_worker.py::test_mp_input_io_handler_override_honored``.
The task section at the end does materialize: tasks that name their
io_handler by resource key, on both executors.
"""

import pickle
import re
from pathlib import Path

import obstore.store
import pytest

import rivers as rs
from rivers.exceptions import AssetDefinitionError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class _NamedHandler(rs.BaseIOHandler):
    """IOHandler with a stable identity for assertion via attribute equality."""

    name: str

    def handle_output(self, context, obj):
        pass

    def load_input(self, context):
        return None


# ---------------------------------------------------------------------------
# for_output — node.io_handler() → default
# ---------------------------------------------------------------------------


def test_for_output_uses_node_handler_when_set():
    """Asset with explicit io_handler resolves to that handler."""
    handler = _NamedHandler(name="explicit")

    @rs.Asset(io_handler=handler)
    def configured() -> int:
        return 1

    repo = rs.CodeRepository(assets=[configured])

    resolved = repo.io_handler_for_output("configured")
    assert isinstance(resolved, _NamedHandler)
    assert resolved.name == "explicit"


def test_for_output_falls_back_to_default_in_memory():
    """Asset without io_handler resolves to the shared InMemoryIOHandler."""

    @rs.Asset
    def unconfigured() -> int:
        return 1

    repo = rs.CodeRepository(assets=[unconfigured])

    resolved = repo.io_handler_for_output("unconfigured")
    assert isinstance(resolved, rs.InMemoryIOHandler)


def test_for_output_resolves_resource_ref():
    """``io_handler="key"`` resolves through resources to the registered handler."""
    handler = _NamedHandler(name="from_resource")

    @rs.Asset(io_handler="my_handler")
    def via_ref() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[via_ref],
        resources={"my_handler": handler},
    )

    resolved = repo.io_handler_for_output("via_ref")
    assert isinstance(resolved, _NamedHandler)
    assert resolved.name == "from_resource"


def test_for_output_unknown_asset_raises():
    """Looking up a non-existent asset raises NodeNotFoundError."""
    from rivers.exceptions import NodeNotFoundError

    @rs.Asset
    def only_one() -> int:
        return 1

    repo = rs.CodeRepository(assets=[only_one])

    with pytest.raises(NodeNotFoundError, match="not_a_real_asset"):
        repo.io_handler_for_output("not_a_real_asset")


# ---------------------------------------------------------------------------
# Resolve-time errors — broken ResourceRefs caught before execution
# ---------------------------------------------------------------------------


def test_missing_resource_ref_errors_at_resolve():
    """``io_handler="missing"`` errors at resolve time, not at execution."""

    @rs.Asset(io_handler="missing_resource")
    def asset_with_bad_ref() -> int:
        return 1

    repo = rs.CodeRepository(assets=[asset_with_bad_ref])

    with pytest.raises(BaseException, match="missing_resource"):
        repo.io_handler_for_output("asset_with_bad_ref")


def test_resource_ref_to_wrong_protocol_errors_at_resolve():
    """``io_handler="key"`` pointing at a non-IOHandler resource errors at resolve."""

    class NotAnIOHandler(rs.Resource):
        value: str = "oops"

    @rs.Asset(io_handler="bad_proto")
    def asset_with_wrong_ref() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[asset_with_wrong_ref],
        resources={"bad_proto": NotAnIOHandler()},
    )

    with pytest.raises(BaseException, match="does not implement"):
        repo.io_handler_for_output("asset_with_wrong_ref")


# ---------------------------------------------------------------------------
# Graph asset propagation — node_io_handler becomes task io_handler_override
# ---------------------------------------------------------------------------


def test_graph_node_io_handler_propagates_to_internal_tasks():
    """Graph asset's ``node_io_handler`` is the resolved handler for its tasks."""
    node_handler = _NamedHandler(name="graph_internal")
    output_handler = _NamedHandler(name="graph_output")

    @rs.Task
    def step_a() -> int:
        return 1

    @rs.Task
    def step_b(value: int) -> int:
        return value + 1

    @rs.Asset.from_graph(
        name="pipe",
        node_io_handler=node_handler,
        io_handler=output_handler,
    )
    def pipe():
        return step_b(value=step_a())

    repo = rs.CodeRepository(assets=[pipe], tasks=[step_a, step_b])

    # Internal tasks pick up node_io_handler via io_handler_override.
    a_handler = repo.io_handler_for_output("pipe/step_a")
    assert isinstance(a_handler, _NamedHandler)
    assert a_handler.name == "graph_internal"

    # Graph asset itself uses io_handler.
    pipe_handler = repo.io_handler_for_output("pipe")
    assert isinstance(pipe_handler, _NamedHandler)
    assert pipe_handler.name == "graph_output"


# ---------------------------------------------------------------------------
# Tasks — io_handler named by resource key
# ---------------------------------------------------------------------------

IN_PROCESS = rs.Executor.in_process()
PARALLEL = rs.Executor.parallel(max_workers=2)

KINDS_AND_EXECUTORS = pytest.mark.parametrize(
    ("kind", "executor"),
    [
        pytest.param("sync", IN_PROCESS, id="sync-in_process"),
        pytest.param("sync", PARALLEL, id="sync-parallel"),
        pytest.param("async", IN_PROCESS, id="async-in_process"),
        pytest.param("async", PARALLEL, id="async-parallel"),
        pytest.param("bash", IN_PROCESS, id="bash-in_process"),
        pytest.param(
            "bash",
            PARALLEL,
            id="bash-parallel",
            marks=pytest.mark.xfail(
                raises=AttributeError,
                strict=True,
                reason="a BashTask that shares a level fails on the parallel "
                "executor before any IO: it has no __annotations__",
            ),
        ),
    ],
)


def _pickle_handler(root: Path) -> rs.PickleIOHandler:
    return rs.PickleIOHandler(store=obstore.store.LocalStore(str(root), mkdir=True))


def _stored(root: Path) -> dict:
    """What a PickleIOHandler holds under ``root``, by node path."""
    return {
        p.relative_to(root).with_suffix("").as_posix(): pickle.loads(p.read_bytes())
        for p in sorted(root.rglob("*.pkl"))
    }


def _warehouse_task(kind: str, name: str, value: str):
    """A task of ``kind`` that returns ``value`` and names its io_handler by the
    resource key "warehouse"."""
    if kind == "bash":
        return rs.BashTask(name=name, command=f"echo {value}", io_handler="warehouse")
    if kind == "async":

        async def run() -> str:
            return value

    else:

        def run() -> str:
            return value

    return rs.Task(run, name=name, io_handler="warehouse")


@KINDS_AND_EXECUTORS
def test_task_resource_key_io_handler_writes_and_reads_back(kind, executor, tmp_path):
    """A task's ``io_handler="key"`` writes through the resource handler, and
    downstream assets read the values back through it."""
    warehouse = tmp_path / "warehouse"

    @rs.Asset(io_handler="warehouse")
    def order_total(orders: str) -> int:
        return int(orders) + 1

    @rs.Asset(io_handler="warehouse")
    def greeting(customers: str) -> str:
        return f"hi {customers}"

    repo = rs.CodeRepository(
        assets=[order_total, greeting],
        tasks=[
            _warehouse_task(kind, "orders", "7"),
            _warehouse_task(kind, "customers", "ada"),
        ],
        resources={"warehouse": _pickle_handler(warehouse)},
        default_executor=executor,
    )
    repo.materialize()
    assert _stored(warehouse) == {
        "customers": "ada",
        "greeting": "hi ada",
        "order_total": 8,
        "orders": "7",
    }


@KINDS_AND_EXECUTORS
def test_graph_inner_task_resource_key_io_handler(kind, executor, tmp_path):
    """Inner tasks of a graph without ``node_io_handler`` write through their
    own ``io_handler="key"``, and the next inner task reads them back."""
    warehouse = tmp_path / "warehouse"

    @rs.Task(io_handler="warehouse")
    def left() -> int:
        return 3

    right = _warehouse_task(kind, "right", "4")

    @rs.Task(io_handler="warehouse")
    def add(a: int, b: str) -> int:
        return a + int(b)

    @rs.Asset.from_graph(io_handler="warehouse")
    def total():
        return add(left(), right())

    repo = rs.CodeRepository(
        assets=[total],
        tasks=[left, right, add],
        resources={"warehouse": _pickle_handler(warehouse)},
        default_executor=executor,
    )
    repo.materialize()
    assert _stored(warehouse) == {
        "total": 7,
        "total/add": 7,
        "total/left": 3,
        "total/right": "4",
    }


class _Settings(rs.Resource):
    """A resource that is not an IOHandler."""

    value: str = "oops"


@pytest.mark.parametrize("kind", ["sync", "bash"])
@pytest.mark.parametrize(
    ("resources", "reason"),
    [
        ({}, "which is not in resources"),
        (
            {"warehouse": _Settings()},
            "which does not implement the IOHandler protocol "
            "(handle_output + load_input)",
        ),
    ],
    ids=["unknown-key", "not-an-io-handler"],
)
def test_task_resource_key_io_handler_errors_at_resolve(kind, resources, reason):
    """A task's ``io_handler="key"`` that names no IOHandler resource fails
    resolve, as on an asset, instead of panicking when the task runs."""
    repo = rs.CodeRepository(
        assets=[],
        tasks=[_warehouse_task(kind, "orders", "7")],
        resources=resources,
    )
    message = f"Task 'orders': io_handler references resource 'warehouse' {reason}"
    with pytest.raises(AssetDefinitionError, match=re.escape(message)):
        repo.resolve()
