"""Tests verifying that InMemoryIOHandler is assigned as the default IO handler
for all Assets, Tasks, and BashTasks that don't have an explicit handler."""

import rivers as rs
from rivers.exceptions import ExecutionError


def test_single_asset_gets_default_io_handler():
    """A single asset without io_handler gets InMemoryIOHandler after resolve."""

    @rs.Asset
    def my_asset() -> int:
        return 42

    repo = rs.CodeRepository(
        assets=[my_asset], default_executor=rs.Executor.in_process()
    )
    repo.materialize()
    assert repo.load_node("my_asset") == 42


def test_multiple_assets_share_same_in_memory_handler():
    """All assets without explicit handlers share the same InMemoryIOHandler instance,
    allowing upstream outputs to be read by downstream nodes."""

    @rs.Asset
    def upstream() -> int:
        return 10

    @rs.Asset
    def downstream(upstream: int) -> int:
        return upstream * 2

    repo = rs.CodeRepository(
        assets=[upstream, downstream], default_executor=rs.Executor.in_process()
    )
    repo.materialize()
    assert repo.load_node("upstream") == 10
    assert repo.load_node("downstream") == 20


def test_task_gets_default_io_handler():
    """A Task without io_handler gets InMemoryIOHandler, and its output
    is available to downstream assets."""

    @rs.Asset
    def source() -> int:
        return 5

    @rs.Task
    def process(source: int) -> str:
        return f"processed: {source}"

    repo = rs.CodeRepository(
        assets=[source],
        tasks=[process],
        jobs=[
            rs.Job(
                name="pipeline",
                assets=[source, process],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("pipeline").execute()
    assert repo.load_node("source") == 5
    assert repo.load_node("process") == "processed: 5"


def test_bash_task_gets_default_io_handler():
    """A BashTask gets InMemoryIOHandler, and its output is passed downstream."""
    greet = rs.BashTask(name="greet", command="echo hello")

    @rs.Asset
    def use_greeting(greet: str) -> str:
        return f"got: {greet}"

    repo = rs.CodeRepository(
        assets=[use_greeting],
        tasks=[greet],
        jobs=[
            rs.Job(
                name="pipeline",
                assets=[greet, use_greeting],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("pipeline").execute()
    assert "hello" in repo.load_node("greet")
    assert "got:" in repo.load_node("use_greeting")


def test_explicit_io_handler_not_overridden():
    """An asset with an explicit IO handler keeps it (not replaced by default)."""
    explicit_handler = rs.InMemoryIOHandler()

    @rs.Asset(io_handler=explicit_handler)
    def my_asset() -> int:
        return 99

    @rs.Asset
    def downstream(my_asset: int) -> int:
        return my_asset + 1

    repo = rs.CodeRepository(
        assets=[my_asset, downstream], default_executor=rs.Executor.in_process()
    )
    repo.materialize()
    assert repo.load_node("my_asset") == 99
    assert repo.load_node("downstream") == 100
    # The explicit handler should have the value stored
    ctx = rs.InputContext(asset_name="my_asset", downstream_asset="test")
    assert explicit_handler.load_input(ctx) == 99


def test_multi_asset_outputs_get_default_handler():
    """Multi-asset outputs without explicit handlers get InMemoryIOHandler."""

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef(name="left"), rs.AssetDef(name="right")]
    )
    def split() -> dict:
        return {"left": 10, "right": 20}

    @rs.Asset
    def consumer(left: int) -> int:
        return left * 2

    repo = rs.CodeRepository(
        assets=[split, consumer], default_executor=rs.Executor.in_process()
    )
    repo.materialize()
    assert repo.load_node("left") == 10
    assert repo.load_node("right") == 20
    assert repo.load_node("consumer") == 20


def test_multi_asset_both_outputs_consumed_by_separate_downstreams():
    """Each multi-asset output is consumed by a different downstream asset."""

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef(name="left"), rs.AssetDef(name="right")]
    )
    def split() -> dict:
        return {"left": 10, "right": 20}

    @rs.Asset
    def use_left(left: int) -> int:
        return left + 1

    @rs.Asset
    def use_right(right: int) -> int:
        return right + 2

    repo = rs.CodeRepository(
        assets=[split, use_left, use_right],
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize()
    assert repo.load_node("use_left") == 11
    assert repo.load_node("use_right") == 22


def test_multi_asset_per_output_pickle_io_handler(tmp_path):
    """Multi-asset with PickleIOHandler on individual outputs persists each output."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef(name="alpha", io_handler=handler),
            rs.AssetDef(name="beta", io_handler=handler),
        ]
    )
    def produce() -> dict:
        return {"alpha": [1, 2, 3], "beta": "hello"}

    @rs.Asset
    def consume_alpha(alpha: list) -> int:
        return sum(alpha)

    repo = rs.CodeRepository(
        assets=[produce, consume_alpha],
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize()
    assert repo.load_node("consume_alpha") == 6

    # Verify persistence via handler — each output stored individually (sliced)
    alpha_ctx = rs.InputContext(asset_name="alpha", downstream_asset="test")
    beta_ctx = rs.InputContext(asset_name="beta", downstream_asset="test")
    assert handler.load_input(alpha_ctx) == [1, 2, 3]
    assert handler.load_input(beta_ctx) == "hello"


def test_chain_of_three_via_default_handler():
    """A chain of three assets passes values via shared InMemoryIOHandler."""

    @rs.Asset
    def a() -> int:
        return 1

    @rs.Asset
    def b(a: int) -> int:
        return a + 1

    @rs.Asset
    def c(b: int) -> int:
        return b + 1

    repo = rs.CodeRepository(
        assets=[a, b, c], default_executor=rs.Executor.in_process()
    )
    repo.materialize()
    assert repo.load_node("a") == 1
    assert repo.load_node("b") == 2
    assert repo.load_node("c") == 3


def test_diamond_dag_via_default_handler():
    """Diamond DAG (fan-out + fan-in) works via shared InMemoryIOHandler."""

    @rs.Asset
    def root() -> int:
        return 10

    @rs.Asset
    def left(root: int) -> int:
        return root + 1

    @rs.Asset
    def right(root: int) -> int:
        return root + 2

    @rs.Asset
    def merge(left: int, right: int) -> int:
        return left + right

    repo = rs.CodeRepository(
        assets=[root, left, right, merge],
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize()
    assert repo.load_node("root") == 10
    assert repo.load_node("left") == 11
    assert repo.load_node("right") == 12
    assert repo.load_node("merge") == 23


def test_parallel_rejects_default_in_memory_handler():
    """parallel executor rejects InMemoryIOHandler when steps run in parallel."""
    import pytest

    @rs.Asset
    def a() -> int:
        return 1

    @rs.Asset
    def b() -> int:
        return 2

    repo = rs.CodeRepository(
        assets=[a, b],
        jobs=[
            rs.Job(
                name="parallel",
                assets=[a, b],
                executor=rs.Executor.parallel(max_workers=2),
            )
        ],
    )
    with pytest.raises(ExecutionError, match="InMemoryIOHandler"):
        repo.get_job("parallel").execute()


def test_parallel_rejects_multi_asset_without_io_handler():
    """parallel executor rejects multi-asset outputs that lack explicit IO handlers."""
    import pytest

    @rs.Asset.from_multi(output_defs=[rs.AssetDef(name="m1"), rs.AssetDef(name="m2")])
    def multi_no_handler() -> dict:
        return {"m1": 1, "m2": 2}

    @rs.Asset
    def other() -> int:
        return 3

    repo = rs.CodeRepository(
        assets=[multi_no_handler, other],
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    with pytest.raises(ExecutionError, match="InMemoryIOHandler"):
        repo.materialize()
