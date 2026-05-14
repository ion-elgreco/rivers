"""IO handler resolution matrix — exercises the ``IOHandlerRegistry`` chain
through the public ``CodeRepository.io_handler_for_output`` accessor.

Checks the same chain the executor walks at materialize time, but without
running any steps. The corresponding parallel-mode regression
(per-input override honored under loky) lives at
``tests/executor/test_parallel_worker.py::test_mp_input_io_handler_override_honored``.
"""

import pytest

import rivers as rs


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
