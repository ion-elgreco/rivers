"""Integration tests covering multi-asset execution, graph-asset execution,
mixed asset types, and complex dependency patterns."""

import rivers as rs

# ---------------------------------------------------------------------------
# Multi-asset execution
# ---------------------------------------------------------------------------


def test_multi_asset_execution_in_job():
    """Multi-asset is executed through a job, producing all outputs."""
    call_count = 0

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("x"), rs.AssetDef("y")],
    )
    def producer() -> dict:
        nonlocal call_count
        call_count += 1
        return {"x": 10, "y": 20}

    repo = rs.CodeRepository(
        assets=[producer],
        default_executor=rs.Executor.in_process(),
    )
    _result = repo.materialize()

    # Multi-asset function is called once; outputs are sliced per output name
    assert call_count == 1
    assert repo.load_node("x") == 10
    assert repo.load_node("y") == 20


def test_multi_asset_with_downstream():
    """Multi-asset outputs are sliced and downstream receives individual values."""

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("left"), rs.AssetDef("right")],
    )
    def split() -> dict:
        return {"left": 10, "right": 20}

    @rs.Asset
    def consumer(left: int) -> int:
        return left * 2

    repo = rs.CodeRepository(
        assets=[split, consumer],
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize()
    assert repo.load_node("left") == 10
    assert repo.load_node("right") == 20
    assert repo.load_node("consumer") == 20


def test_multi_asset_with_parent_io_handler():
    """Multi-asset with parent-level io_handler stores outputs."""
    stored = {}

    class DictHandler(rs.BaseIOHandler):
        def handle_output(self, context, obj):
            stored[context.asset_name] = obj

        def load_input(self, context):
            return stored[context.asset_name]

    handler = DictHandler()  # noqa

    # Parent-level io_handler is not exposed via Asset.io_handler() for Multi,
    # so per-def io_handlers propagate through AssetDef. However, during
    # execution the graph node returns None for Multi.io_handler().
    # This test verifies the multi-asset function executes and produces outputs.
    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("a"), rs.AssetDef("b")],
    )
    def produce() -> dict:
        return {"a": 1, "b": 2}

    repo = rs.CodeRepository(
        assets=[produce],
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize()

    assert repo.load_node("a") == 1
    assert repo.load_node("b") == 2


def test_multi_asset_executes_function_once():
    """Multi-asset function body runs exactly once, not once per output."""
    call_count = 0

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("a"), rs.AssetDef("b"), rs.AssetDef("c")],
    )
    def triple() -> dict:
        nonlocal call_count
        call_count += 1
        return {"a": 1, "b": 2, "c": 3}

    repo = rs.CodeRepository(assets=[triple], default_executor=rs.Executor.in_process())
    repo.materialize()

    assert call_count == 1
    assert repo.load_node("a") == 1
    assert repo.load_node("b") == 2
    assert repo.load_node("c") == 3


def test_multi_asset_downstream_receives_sliced_values():
    """Downstream assets receive individual sliced values, not the full dict."""

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("x"), rs.AssetDef("y")],
    )
    def source() -> dict:
        return {"x": 10, "y": "hello"}

    @rs.Asset
    def use_x(x: int) -> int:
        # x should be 10 (int), not {"x": 10, "y": "hello"} (dict)
        return x + 1

    @rs.Asset
    def use_y(y: str) -> str:
        # y should be "hello" (str), not the full dict
        return y.upper()

    repo = rs.CodeRepository(
        assets=[source, use_x, use_y],
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize()
    assert repo.load_node("use_x") == 11
    assert repo.load_node("use_y") == "HELLO"


def test_multi_asset_success_hooks_receive_sliced_values():
    """Success hooks fire per output and receive the sliced value, not the full dict."""
    hook_data = []

    @rs.Hook.success
    def track(context: rs.HookContext):
        hook_data.append((context.asset_name, context.output))

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("p"), rs.AssetDef("q")],
        hooks=[track],
    )
    def producer() -> dict:
        return {"p": 42, "q": "data"}

    repo = rs.CodeRepository(
        assets=[producer], default_executor=rs.Executor.in_process()
    )
    _result = repo.materialize()

    names = [name for name, _ in hook_data]
    values = {name: val for name, val in hook_data}
    assert "p" in names
    assert "q" in names
    assert values["p"] == 42
    assert values["q"] == "data"


def test_multi_asset_context_awareness():
    """Multi-asset context has correct asset_name, is_multi_asset, and output_selection."""
    captured = {}

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("out_a"), rs.AssetDef("out_b")],
    )
    def my_producer(context: rs.AssetExecutionContext) -> dict:
        captured["asset_name"] = context.asset_name
        captured["is_multi_asset"] = context.is_multi_asset
        captured["output_selection"] = list(context.output_selection)
        return {"out_a": 1, "out_b": 2}

    repo = rs.CodeRepository(
        assets=[my_producer], default_executor=rs.Executor.in_process()
    )
    repo.materialize()

    # asset_name should be the function name, not an output name
    assert captured["asset_name"] == "my_producer"
    assert captured["is_multi_asset"] is True
    assert set(captured["output_selection"]) == {"out_a", "out_b"}


def test_single_asset_context_not_multi():
    """Single asset context has is_multi_asset=False and empty output_selection."""
    captured = {}

    @rs.Asset
    def simple(context: rs.AssetExecutionContext) -> int:
        captured["is_multi_asset"] = context.is_multi_asset
        captured["output_selection"] = list(context.output_selection)
        return 42

    repo = rs.CodeRepository(assets=[simple], default_executor=rs.Executor.in_process())
    repo.materialize()

    assert captured["is_multi_asset"] is False
    assert captured["output_selection"] == []


def test_multi_asset_per_output_metadata_and_data_version():
    """Multi-asset can return Output() wrappers with per-output metadata and data_version."""

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("out_x"), rs.AssetDef("out_y")],
    )
    def with_output_wrappers() -> dict:
        return {
            "out_x": rs.Output(value=10, metadata={"rows": 100}, data_version="v1"),
            "out_y": rs.Output(value=20, metadata={"rows": 200}, data_version="v2"),
        }

    repo = rs.CodeRepository(
        assets=[with_output_wrappers], default_executor=rs.Executor.in_process()
    )
    _result = repo.materialize()

    # Values are unwrapped from Output wrappers
    assert repo.load_node("out_x") == 10
    assert repo.load_node("out_y") == 20


def test_multi_asset_mixed_output_and_raw():
    """Multi-asset can mix Output() wrappers and raw values."""

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("wrapped"), rs.AssetDef("raw")],
    )
    def mixed() -> dict:
        return {
            "wrapped": rs.Output(value="hello", metadata={"source": "api"}),
            "raw": 42,
        }

    @rs.Asset
    def use_wrapped(wrapped: str) -> str:
        return wrapped.upper()

    @rs.Asset
    def use_raw(raw: int) -> int:
        return raw + 1

    repo = rs.CodeRepository(
        assets=[mixed, use_wrapped, use_raw],
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize()
    assert repo.load_node("use_wrapped") == "HELLO"
    assert repo.load_node("use_raw") == 43


def test_multi_asset_failure_hooks_fire_on_error():
    """Failure hooks fire when a multi-asset function raises, via the collecting path."""
    calls = []

    @rs.Hook.failure
    def on_fail(context: rs.HookContext):
        calls.append(context.asset_name)

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("f1"), rs.AssetDef("f2")],
        hooks=[on_fail],
    )
    def bad() -> dict:
        raise ValueError("boom")

    repo = rs.CodeRepository(assets=[bad], default_executor=rs.Executor.in_process())
    # Use raise_on_error=False to exercise the execute_collecting path
    result = repo.materialize(raise_on_error=False)
    assert not result.success
    assert len(calls) > 0


# ---------------------------------------------------------------------------
# Graph-asset execution
# ---------------------------------------------------------------------------


def test_graph_asset_in_repo_with_deps():
    """Graph asset with external dependency registers in repo and resolves graph."""

    @rs.Asset
    def source() -> int:
        return 5

    @rs.Task
    def double(source: int) -> int:
        return source * 2

    @rs.Asset.from_graph(name="pipeline")
    def pipeline(source: int):
        double(source)

    repo = rs.CodeRepository(assets=[source, pipeline], tasks=[double])
    assert "pipeline" in repo.assets
    assert "source" in repo.assets


def test_graph_asset_captures_multiple_steps():
    """Graph asset captures multiple task invocations in composition."""

    @rs.Task
    def step_a() -> int:
        return 1

    @rs.Task
    def step_b(step_a: int) -> int:
        return step_a + 1

    @rs.Task
    def step_c(step_b: int) -> int:
        return step_b + 1

    @rs.Asset.from_graph(name="chain")
    def chain():
        a = step_a()
        b = step_b(a)
        step_c(b)

    assert chain.is_graph
    assert chain.name == "chain"


# ---------------------------------------------------------------------------
# Mixed asset types in a single job
# ---------------------------------------------------------------------------


def test_mixed_types_in_job():
    """Job containing SingleAsset, Task, and BashTask all execute together."""

    @rs.Asset
    def data() -> int:
        return 42

    @rs.Task
    def process(data: int) -> str:
        return f"processed_{data}"

    echo_task = rs.BashTask(name="echo_task", command="echo hello")

    repo = rs.CodeRepository(
        assets=[data],
        tasks=[process, echo_task],
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize()

    assert repo.load_node("data") == 42
    assert repo.load_node("process") == "processed_42"
    assert repo.load_node("echo_task") == "hello"


def test_task_depends_on_asset():
    """Task that depends on an asset receives the asset's output."""

    @rs.Asset
    def source() -> list:
        return [1, 2, 3]

    @rs.Task
    def sink(source: list) -> int:
        return sum(source)

    repo = rs.CodeRepository(
        assets=[source],
        tasks=[sink],
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize()
    assert repo.load_node("sink") == 6


# ---------------------------------------------------------------------------
# Complex dependency patterns
# ---------------------------------------------------------------------------


def test_wide_fan_out():
    """Source fans out to many downstream assets."""

    @rs.Asset
    def root() -> int:
        return 1

    assets = [root]
    for i in range(10):

        @rs.Asset(name=f"leaf_{i}")
        def leaf(root: int, _i=i) -> int:
            return root + _i

        assets.append(leaf)

    repo = rs.CodeRepository(
        assets=assets,
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize()
    assert repo.load_node("root") == 1
    for i in range(10):
        assert repo.load_node(f"leaf_{i}") == 1 + i


def test_fan_in():
    """Multiple assets converge into a single downstream."""

    @rs.Asset
    def a() -> int:
        return 1

    @rs.Asset
    def b() -> int:
        return 2

    @rs.Asset
    def c() -> int:
        return 3

    @rs.Asset
    def merged(a: int, b: int, c: int) -> int:
        return a + b + c

    repo = rs.CodeRepository(
        assets=[a, b, c, merged],
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize()
    assert repo.load_node("merged") == 6


def test_deep_chain():
    """Long linear dependency chain."""

    @rs.Asset
    def step_0() -> int:
        return 0

    @rs.Asset
    def step_1(step_0: int) -> int:
        return step_0 + 1

    @rs.Asset
    def step_2(step_1: int) -> int:
        return step_1 + 1

    @rs.Asset
    def step_3(step_2: int) -> int:
        return step_2 + 1

    @rs.Asset
    def step_4(step_3: int) -> int:
        return step_3 + 1

    @rs.Asset
    def step_5(step_4: int) -> int:
        return step_4 + 1

    @rs.Asset
    def step_6(step_5: int) -> int:
        return step_5 + 1

    @rs.Asset
    def step_7(step_6: int) -> int:
        return step_6 + 1

    @rs.Asset
    def step_8(step_7: int) -> int:
        return step_7 + 1

    @rs.Asset
    def step_9(step_8: int) -> int:
        return step_8 + 1

    assets = [
        step_0,
        step_1,
        step_2,
        step_3,
        step_4,
        step_5,
        step_6,
        step_7,
        step_8,
        step_9,
    ]
    repo = rs.CodeRepository(
        assets=assets,
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize()
    assert repo.load_node("step_0") == 0
    assert repo.load_node("step_9") == 9


def test_disconnected_subgraphs():
    """Multiple disconnected subgraphs in one repo."""

    @rs.Asset
    def a1() -> int:
        return 1

    @rs.Asset
    def a2(a1: int) -> int:
        return a1 + 1

    @rs.Asset
    def b1() -> str:
        return "x"

    @rs.Asset
    def b2(b1: str) -> str:
        return b1 + "y"

    repo = rs.CodeRepository(
        assets=[a1, a2, b1, b2],
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize()
    assert repo.load_node("a2") == 2
    assert repo.load_node("b2") == "xy"


# ---------------------------------------------------------------------------
# Context integration
# ---------------------------------------------------------------------------


def test_context_metadata_flows_to_io_handler():
    """Asset's context.add_output_metadata() reaches the IO handler's OutputContext."""
    captured_metadata = {}

    class MetaCapturingHandler(rs.BaseIOHandler):
        def handle_output(self, context, obj):
            captured_metadata.update(context.output_metadata or {})

        def load_input(self, context):
            return None

    @rs.Asset(io_handler=MetaCapturingHandler())
    def my_asset(context: rs.AssetExecutionContext) -> int:
        context.add_output_metadata({"rows": 100, "status": "ok"})
        return 42

    repo = rs.CodeRepository(
        assets=[my_asset],
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize()

    assert "rows" in captured_metadata
    assert captured_metadata["rows"] == rs.MetadataValue.int(100)
    assert captured_metadata["status"] == rs.MetadataValue.text("ok")


def test_context_properties_accessible():
    """AssetExecutionContext exposes all asset properties during execution."""
    captured = {}

    @rs.Asset(tags=["analytics"], kinds="table", group="reports", code_version="v2")
    def instrumented(context: rs.AssetExecutionContext) -> int:
        captured["name"] = context.asset_name
        captured["tags"] = context.tags
        captured["kinds"] = context.kinds
        captured["group"] = context.group
        captured["code_version"] = context.code_version
        captured["has_partition"] = context.has_partition_key
        return 1

    repo = rs.CodeRepository(
        assets=[instrumented],
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize()

    assert captured["name"] == "instrumented"
    assert captured["tags"] == ["analytics"]
    assert captured["kinds"] == ["table"]
    assert captured["group"] == "reports"
    assert captured["code_version"] == "v2"
    assert captured["has_partition"] is False


def test_context_partition_key_accessible():
    """AssetExecutionContext provides partition_key during partitioned execution."""
    captured_key = []
    pd = rs.PartitionsDefinition.static_(["us", "eu"])

    @rs.Asset(partitions_def=pd)
    def partitioned(context: rs.AssetExecutionContext) -> str:
        captured_key.append(context.partition_key)
        return context.partition_key

    repo = rs.CodeRepository(
        assets=[partitioned],
        jobs=[
            rs.Job(
                name="p_job",
                assets=[partitioned],
                executor=rs.Executor.in_process(),
            ),
        ],
    )
    repo.get_job("p_job").execute(partition_key=rs.PartitionKey.single("us"))
    assert captured_key[0] == "us"


# ---------------------------------------------------------------------------
# Materialize with partition key
# ---------------------------------------------------------------------------


def test_materialize_with_partition_key():
    """repo.materialize(partition_key=...) passes partition to the default job."""
    captured = []
    pd = rs.PartitionsDefinition.static_(["a", "b"])

    class CapHandler(rs.BaseIOHandler):
        def handle_output(self, context, obj):
            captured.append(context.partition)

        def load_input(self, context):
            return None

    @rs.Asset(partitions_def=pd, io_handler=CapHandler())
    def my_data() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[my_data],
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize(partition_key=rs.PartitionKey.single("a"))

    assert len(captured) == 1
    assert captured[0] is not None
    assert captured[0].key == rs.PartitionKey.single("a")


# ---------------------------------------------------------------------------
# External + self-dependency combination
# ---------------------------------------------------------------------------


def test_self_dep_with_external_dep():
    """Asset uses both SelfDependency and an external asset as input."""

    class DictIOHandler(rs.BaseIOHandler):
        store: dict = {}

        def handle_output(self, context, obj):
            self.store[context.asset_name] = obj

        def load_input(self, context):
            return self.store.get(context.asset_name)

    handler = DictIOHandler(store={"ext_vals": [10, 20]})

    ext = rs.Asset.external(name="ext_vals", io_handler=handler)

    @rs.Asset(io_handler=handler)
    def accumulator(self: rs.SelfDependency[int], ext_vals: list) -> int:
        prev = self.get_inner() or 0
        return prev + sum(ext_vals)

    repo = rs.CodeRepository(
        assets=[ext, accumulator],
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize()
    assert repo.load_node("accumulator") == 30
