"""Tests for native async step execution."""

import asyncio
import os
import time

import pytest

import rivers as rs

# ── Async detection at decoration time ──


def test_sync_asset_is_not_async():
    """A regular def asset should have is_async=False."""

    @rs.Asset
    def sync_asset():
        return 1

    assert sync_asset.is_async is False


def test_async_asset_is_async():
    """An async def asset should have is_async=True."""

    @rs.Asset
    async def async_asset():
        return 1

    assert async_asset.is_async is True


def test_sync_asset_with_params_is_not_async():
    """A sync asset created with decorator params should have is_async=False."""

    @rs.Asset(name="my_sync")
    def sync_asset():
        return 1

    assert sync_asset.is_async is False


def test_async_asset_with_params_is_async():
    """An async asset created with decorator params should have is_async=True."""

    @rs.Asset(name="my_async")
    async def async_asset():
        return 1

    assert async_asset.is_async is True


def test_sync_task_is_not_async():
    """A regular def task should have is_async=False."""

    @rs.Task
    def sync_task():
        return 1

    assert sync_task.is_async is False


def test_async_task_is_async():
    """An async def task should have is_async=True."""

    @rs.Task
    async def async_task():
        return 1

    assert async_task.is_async is True


def test_sync_multi_asset_is_not_async():
    """A sync multi-asset should have is_async=False."""

    @rs.Asset.from_multi(output_defs=[rs.AssetDef("a"), rs.AssetDef("b")])
    def sync_multi():
        return {"a": 1, "b": 2}

    assert sync_multi.is_async is False


def test_async_multi_asset_is_async():
    """An async multi-asset should have is_async=True."""

    @rs.Asset.from_multi(output_defs=[rs.AssetDef("a"), rs.AssetDef("b")])
    async def async_multi():
        return {"a": 1, "b": 2}

    assert async_multi.is_async is True


def test_external_asset_sync_observe_is_not_async():
    """External asset with sync observe_fn should have is_async=False."""

    class DummyIO(rs.BaseIOHandler):
        def handle_output(self, context, obj):
            pass

        def load_input(self, context):
            return None

    @rs.Asset.external(io_handler=DummyIO(), name="ext_sync")
    def ext_sync():
        return rs.Observation(data_version="v1")

    assert ext_sync.is_async is False


def test_external_asset_async_observe_is_async():
    """External asset with async observe_fn should have is_async=True."""

    class DummyIO(rs.BaseIOHandler):
        def handle_output(self, context, obj):
            pass

        def load_input(self, context):
            return None

    @rs.Asset.external(io_handler=DummyIO(), name="ext_async")
    async def ext_async():
        return rs.Observation(data_version="v1")

    assert ext_async.is_async is True


def test_external_asset_no_observe_is_not_async():
    """External asset with no observe_fn should have is_async=False."""

    class DummyIO(rs.BaseIOHandler):
        def handle_output(self, context, obj):
            pass

        def load_input(self, context):
            return None

    ext = rs.Asset.external(name="ext", io_handler=DummyIO())
    assert ext.is_async is False


# ── Basic async asset execution ──


def test_async_asset_executes_and_returns_value():
    """An async def asset should execute and produce the correct result."""

    @rs.Asset
    async def async_value():
        await asyncio.sleep(0.01)
        return 42

    repo = rs.CodeRepository(assets=[async_value])
    repo.materialize()
    assert repo.load_node("async_value") == 42


def test_async_task_executes_and_returns_value():
    """An async def task should execute and produce the correct result."""

    @rs.Asset
    async def async_source() -> int:
        await asyncio.sleep(0.01)
        return 10

    @rs.Asset
    def consumer(async_source: int) -> int:
        return async_source + 1

    repo = rs.CodeRepository(assets=[async_source, consumer])
    repo.materialize()
    assert repo.load_node("consumer") == 11


def test_async_asset_with_context():
    """An async asset receiving AssetExecutionContext should work."""

    @rs.Asset
    async def ctx_asset(context: rs.AssetExecutionContext) -> str:
        await asyncio.sleep(0.01)
        return f"name={context.asset_name}"

    repo = rs.CodeRepository(assets=[ctx_asset])
    repo.materialize()
    assert repo.load_node("ctx_asset") == "name=ctx_asset"


def test_async_asset_with_config():
    """An async asset using config should receive it correctly."""
    from pydantic import BaseModel

    class MyConfig(BaseModel):
        multiplier: int = 3

    @rs.Asset
    async def with_config(context: rs.AssetExecutionContext[MyConfig]) -> int:
        await asyncio.sleep(0.01)
        return context.config.multiplier * 10

    repo = rs.CodeRepository(assets=[with_config])
    repo.materialize()
    assert repo.load_node("with_config") == 30


def test_async_asset_failure_propagates():
    """An async asset that raises should fail and skip downstream."""

    @rs.Asset
    async def failing() -> int:
        await asyncio.sleep(0.01)
        raise ValueError("boom")

    repo = rs.CodeRepository(assets=[failing])
    result = repo.materialize(raise_on_error=False)
    assert not result.success
    failure_names = [name for name, _ in result.failed_assets]
    assert "failing" in failure_names


def test_async_asset_with_output_wrapper():
    """An async asset returning Output() should have metadata extracted."""

    @rs.Asset
    async def with_output():
        await asyncio.sleep(0.01)
        return rs.Output(value=99, metadata={"source": "test"})

    repo = rs.CodeRepository(assets=[with_output])
    repo.materialize()
    assert repo.load_node("with_output") == 99


# ── In-process concurrent async execution ──


def test_async_assets_same_level_run_concurrently():
    """Two async assets at the same dependency level should overlap execution."""

    @rs.Asset
    async def slow_a():
        await asyncio.sleep(0.5)
        return "a"

    @rs.Asset
    async def slow_b():
        await asyncio.sleep(0.5)
        return "b"

    @rs.Asset
    def merge(slow_a: str, slow_b: str) -> str:
        return f"{slow_a}+{slow_b}"

    repo = rs.CodeRepository(
        assets=[slow_a, slow_b, merge],
        default_executor=rs.Executor.in_process(),
    )
    start = time.monotonic()
    repo.materialize()
    elapsed = time.monotonic() - start

    assert repo.load_node("merge") == "a+b"
    # If truly concurrent, should take ~0.5s, not ~1.0s
    assert elapsed < 0.9, f"Expected concurrent execution, but took {elapsed:.2f}s"


def test_mixed_sync_async_same_level():
    """Mix of sync and async assets at the same level should all execute correctly."""

    @rs.Asset
    def sync_a():
        return 1

    @rs.Asset
    async def async_b():
        await asyncio.sleep(0.01)
        return 2

    @rs.Asset
    def merge(sync_a: int, async_b: int) -> int:
        return sync_a + async_b

    repo = rs.CodeRepository(
        assets=[sync_a, async_b, merge],
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize()
    assert repo.load_node("merge") == 3


def test_async_chain_executes_in_order():
    """Async assets with dependencies should respect dependency ordering."""

    @rs.Asset
    async def first():
        await asyncio.sleep(0.01)
        return 1

    @rs.Asset
    async def second(first: int):
        await asyncio.sleep(0.01)
        return first + 1

    @rs.Asset
    async def third(second: int):
        await asyncio.sleep(0.01)
        return second + 1

    repo = rs.CodeRepository(assets=[first, second, third])
    repo.materialize()
    assert repo.load_node("third") == 3


# ── Async assets under InProcess executor ──


def test_in_process_with_async_assets():
    """InProcess executor should handle async assets correctly."""

    @rs.Asset
    async def async_a():
        await asyncio.sleep(0.01)
        return "a"

    @rs.Asset
    async def async_b():
        await asyncio.sleep(0.01)
        return "b"

    @rs.Asset
    def sync_c(async_a: str, async_b: str) -> str:
        return f"{async_a}+{async_b}"

    assets = [async_a, async_b, sync_c]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[
            rs.Job(
                name="mixed",
                assets=assets,
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("mixed").execute()
    assert repo.load_node("async_a") == "a"
    assert repo.load_node("async_b") == "b"


# ── Parallel async support ──


def test_parallel_async_asset_single_step(tmp_path):
    """Single async asset delegates to in-process, should work."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    async def mp_async_src():
        await asyncio.sleep(0.01)
        return 42

    @rs.Asset(io_handler=handler)
    def mp_sync_consumer(mp_async_src: int):
        return mp_async_src + 1

    assets = [mp_async_src, mp_sync_consumer]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[
            rs.Job(
                name="mp_async",
                assets=assets,
                executor=rs.Executor.parallel(max_workers=2),
            )
        ],
    )
    repo.get_job("mp_async").execute()

    def load(name):
        return handler.load_input(
            rs.InputContext(asset_name=name, downstream_asset="t")
        )

    assert load("mp_async_src") == 42
    assert load("mp_sync_consumer") == 43


def test_parallel_async_parallel_workers(tmp_path):
    """Multiple async assets at the same level should run in parallel workers."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    async def mp_left():
        await asyncio.sleep(0.01)
        return 10

    @rs.Asset(io_handler=handler)
    async def mp_right():
        await asyncio.sleep(0.01)
        return 20

    @rs.Asset(io_handler=handler)
    def mp_merge(mp_left: int, mp_right: int) -> int:
        return mp_left + mp_right

    assets = [mp_left, mp_right, mp_merge]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[
            rs.Job(
                name="mp_parallel",
                assets=assets,
                executor=rs.Executor.parallel(max_workers=2),
            )
        ],
    )
    repo.get_job("mp_parallel").execute()

    def load(name):
        return handler.load_input(
            rs.InputContext(asset_name=name, downstream_asset="t")
        )

    assert load("mp_left") == 10
    assert load("mp_right") == 20
    assert load("mp_merge") == 30


# ── Async multi-assets across executors ──


@pytest.mark.parametrize("executor_kind", ["default", "in_process"])
def test_async_generator_multi_asset(executor_kind):
    """An async generator multi-asset yields outputs under default and in_process executors."""

    @rs.Asset.from_multi(output_defs=[rs.AssetDef("x"), rs.AssetDef("y")])
    async def async_gen_multi():
        await asyncio.sleep(0.01)
        yield rs.Output(value=10, output_name="x")
        await asyncio.sleep(0.01)
        yield rs.Output(value=20, output_name="y")

    executor = rs.Executor.in_process() if executor_kind == "in_process" else None
    repo = rs.CodeRepository(assets=[async_gen_multi], default_executor=executor)
    repo.materialize()
    assert repo.load_node("x") == 10
    assert repo.load_node("y") == 20


@pytest.mark.parametrize("executor_kind", ["default", "in_process"])
def test_async_multi_asset_dict_return(executor_kind):
    """An async (non-generator) multi-asset returning a dict works under both executors."""

    @rs.Asset.from_multi(output_defs=[rs.AssetDef("p"), rs.AssetDef("q")])
    async def async_dict_multi():
        await asyncio.sleep(0.01)
        return {"p": 100, "q": 200}

    executor = rs.Executor.in_process() if executor_kind == "in_process" else None
    repo = rs.CodeRepository(assets=[async_dict_multi], default_executor=executor)
    repo.materialize()
    assert repo.load_node("p") == 100
    assert repo.load_node("q") == 200


# ── Cross-executor coverage ──


def test_in_process_async_concurrency_timing():
    """InProcess executor should run async assets concurrently."""

    @rs.Asset
    async def fast_a():
        await asyncio.sleep(0.5)
        return "a"

    @rs.Asset
    async def fast_b():
        await asyncio.sleep(0.5)
        return "b"

    @rs.Asset
    def join(fast_a: str, fast_b: str) -> str:
        return f"{fast_a}+{fast_b}"

    assets = [fast_a, fast_b, join]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[
            rs.Job(
                name="ae_timing",
                assets=assets,
                executor=rs.Executor.in_process(),
            )
        ],
    )
    start = time.monotonic()
    repo.get_job("ae_timing").execute()
    elapsed = time.monotonic() - start

    assert repo.load_node("join") == "a+b"
    assert elapsed < 0.9, f"Expected concurrent execution, but took {elapsed:.2f}s"


def test_parallel_async_runs_in_orchestrator(tmp_path):
    """parallel routes async steps to orchestrator — verify concurrency."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    async def mp_a():
        await asyncio.sleep(0.5)
        return "a"

    @rs.Asset(io_handler=handler)
    async def mp_b():
        await asyncio.sleep(0.5)
        return "b"

    @rs.Asset(io_handler=handler)
    def mp_join(mp_a: str, mp_b: str) -> str:
        return f"{mp_a}+{mp_b}"

    assets = [mp_a, mp_b, mp_join]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[
            rs.Job(
                name="mp_orch",
                assets=assets,
                executor=rs.Executor.parallel(max_workers=2),
            )
        ],
    )
    start = time.monotonic()
    repo.get_job("mp_orch").execute()
    elapsed = time.monotonic() - start

    def load(name):
        return handler.load_input(
            rs.InputContext(asset_name=name, downstream_asset="t")
        )

    assert load("mp_a") == "a"
    assert load("mp_b") == "b"
    assert load("mp_join") == "a+b"
    # Async steps run concurrently in orchestrator, not in separate workers
    assert elapsed < 0.9, f"Expected concurrent execution, but took {elapsed:.2f}s"


# ── Graph assets with async tasks ──


def test_graph_asset_with_async_tasks():
    """Graph asset whose internal tasks are async should execute correctly."""

    @rs.Task
    async def async_step_one() -> int:
        await asyncio.sleep(0.01)
        return 10

    @rs.Task
    async def async_step_two(async_step_one: int) -> int:
        await asyncio.sleep(0.01)
        return async_step_one * 3

    @rs.Asset.from_graph(name="async_pipeline")
    def async_pipeline():
        a = async_step_one()
        async_step_two(a)

    repo = rs.CodeRepository(
        assets=[async_pipeline],
        tasks=[async_step_one, async_step_two],
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize()
    assert result.success
    assert repo.load_node("async_pipeline/async_step_one") == 10
    assert repo.load_node("async_pipeline/async_step_two") == 30


def test_graph_asset_mixed_sync_async_tasks():
    """Graph asset with a mix of sync and async internal tasks."""

    @rs.Task
    def sync_source() -> int:
        return 5

    @rs.Task
    async def async_double(sync_source: int) -> int:
        await asyncio.sleep(0.01)
        return sync_source * 2

    @rs.Asset.from_graph(name="mixed_pipeline")
    def mixed_pipeline():
        s = sync_source()
        return async_double(s)

    repo = rs.CodeRepository(
        assets=[mixed_pipeline],
        tasks=[sync_source, async_double],
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize()
    assert result.success
    assert repo.load_node("mixed_pipeline/sync_source") == 5
    assert repo.load_node("mixed_pipeline/async_double") == 10
    assert repo.load_node("mixed_pipeline") == 10


def test_graph_asset_with_async_tasks_in_process():
    """Graph asset with async tasks under InProcess executor."""

    @rs.Task
    async def ae_step() -> int:
        await asyncio.sleep(0.01)
        return 42

    @rs.Asset.from_graph(name="ae_graph")
    def ae_graph():
        ae_step()

    repo = rs.CodeRepository(
        assets=[ae_graph],
        tasks=[ae_step],
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize()
    assert result.success
    assert repo.load_node("ae_graph/ae_step") == 42


def test_graph_asset_concurrent_sub_steps_in_process():
    """Independent graph sub-steps should run concurrently under InProcess executor."""

    @rs.Task
    async def slow_left() -> int:
        await asyncio.sleep(0.5)
        return 10

    @rs.Task
    async def slow_right() -> int:
        await asyncio.sleep(0.5)
        return 20

    @rs.Task
    def combine(slow_left: int, slow_right: int) -> int:
        return slow_left + slow_right

    @rs.Asset.from_graph(name="concurrent_graph")
    def concurrent_graph():
        left = slow_left()
        right = slow_right()
        return combine(left, right)

    repo = rs.CodeRepository(
        assets=[concurrent_graph],
        tasks=[slow_left, slow_right, combine],
        default_executor=rs.Executor.in_process(),
    )
    start = time.monotonic()
    result = repo.materialize()
    elapsed = time.monotonic() - start

    assert result.success
    assert repo.load_node("concurrent_graph/slow_left") == 10
    assert repo.load_node("concurrent_graph/slow_right") == 20
    assert repo.load_node("concurrent_graph/combine") == 30
    # slow_left and slow_right are independent sub-steps — should overlap
    assert elapsed < 0.9, (
        f"Expected concurrent graph sub-steps, but took {elapsed:.2f}s"
    )


def test_graph_asset_with_external_dep_and_async_task():
    """Graph asset receives upstream async asset output, passes to async task."""

    @rs.Asset
    async def async_source() -> int:
        await asyncio.sleep(0.01)
        return 7

    @rs.Task
    async def async_triple(async_source: int) -> int:
        await asyncio.sleep(0.01)
        return async_source * 3

    @rs.Asset.from_graph(name="dep_pipeline")
    def dep_pipeline(async_source: int):
        return async_triple(async_source)

    repo = rs.CodeRepository(
        assets=[async_source, dep_pipeline],
        tasks=[async_triple],
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize()
    assert result.success
    assert repo.load_node("async_source") == 7
    assert repo.load_node("dep_pipeline/async_triple") == 21
    assert repo.load_node("dep_pipeline") == 21


# ── Fan-out with async tasks ──


# Module-level tasks for fan-out (must be picklable for parallel)
@rs.Task
async def async_double_item(x: int) -> int:
    await asyncio.sleep(0.01)
    return x * 2


@rs.Task
def sum_collected(values: list) -> int:
    return sum(values)


@pytest.mark.parametrize(
    "input_values,expected_sum",
    [
        ([1, 2, 3], 12),  # (1+2+3)*2
        ([10, 20], 60),  # (10+20)*2
    ],
)
def test_fan_out_async_task_in_process(input_values, expected_sum):
    """Fan-out .map() with async task, collected and summed under InProcess executor."""

    @rs.Asset
    def numbers() -> list:
        return input_values

    @rs.Asset.from_graph()
    def fan_out_async():
        n = numbers()
        mapped = n.map(async_double_item)
        return sum_collected(mapped.collect())

    assets = [numbers, fan_out_async]
    repo = rs.CodeRepository(
        assets=assets,
        tasks=[async_double_item, sum_collected],
        jobs=[rs.Job(name="j", assets=assets, executor=rs.Executor.in_process())],
    )
    repo.get_job("j").execute()
    assert repo.load_node("fan_out_async") == expected_sum


def test_fan_out_async_task_parallel(tmp_path):
    """Fan-out .map() with async task under parallel executor.
    Async mapped instances run in orchestrator, not workers."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def mp_nums() -> list:
        return [1, 2, 3, 4]

    @rs.Asset.from_graph(io_handler=handler, node_io_handler=handler)
    def mp_fan_out():
        n = mp_nums()
        mapped = n.map(async_double_item)
        return sum_collected(mapped.collect())

    repo = rs.CodeRepository(
        assets=[mp_nums, mp_fan_out],
        tasks=[async_double_item, sum_collected],
        jobs=[
            rs.Job(
                name="mp_fan",
                assets=[mp_nums, mp_fan_out],
                executor=rs.Executor.parallel(max_workers=2),
            )
        ],
    )
    repo.get_job("mp_fan").execute()

    def load(name):
        return handler.load_input(
            rs.InputContext(asset_name=name, downstream_asset="t")
        )

    assert load("mp_fan_out") == 20  # (1+2+3+4)*2


# ── Async external asset observation ──


def test_async_external_observe_with_metadata():
    """Async observe function should execute and collect metadata."""

    class DictIO(rs.BaseIOHandler):
        store: dict = {}

        def handle_output(self, context, obj):
            self.store[context.asset_name] = obj

        def load_input(self, context):
            return self.store.get(context.asset_name)

    handler = DictIO()

    @rs.Asset.external(io_handler=handler, name="async_feed")
    async def async_feed(context: rs.AssetExecutionContext):
        await asyncio.sleep(0.01)
        context.add_output_metadata({"freshness": rs.MetadataValue.text("live")})

    repo = rs.CodeRepository(assets=[async_feed])
    result = repo.observe()
    assert "async_feed" in result
    assert result["async_feed"]["freshness"].raw_value() == "live"


def test_async_external_observe_with_observation_return():
    """Async observe function returning Observation should extract data_version."""

    class DictIO(rs.BaseIOHandler):
        store: dict = {}

        def handle_output(self, context, obj):
            self.store[context.asset_name] = obj

        def load_input(self, context):
            return self.store.get(context.asset_name)

    handler = DictIO()

    @rs.Asset.external(io_handler=handler, name="versioned_feed")
    async def versioned_feed():
        await asyncio.sleep(0.01)
        return rs.Observation(data_version="v42")

    repo = rs.CodeRepository(assets=[versioned_feed])
    result = repo.observe()
    assert "versioned_feed" in result


def test_async_external_as_dependency():
    """Async external asset can be observed, then used as upstream dependency."""

    class DictIO(rs.BaseIOHandler):
        store: dict = {}

        def handle_output(self, context, obj):
            self.store[context.asset_name] = obj

        def load_input(self, context):
            return self.store[context.asset_name]

    handler = DictIO()

    @rs.Asset.external(io_handler=handler, name="ext_source")
    async def ext_source(context: rs.AssetExecutionContext):
        await asyncio.sleep(0.01)
        context.add_output_metadata({"status": rs.MetadataValue.text("ok")})

    # Pre-populate the external asset's data
    handler.store["ext_source"] = 100

    @rs.Asset
    def consumer(ext_source: int) -> int:
        return ext_source + 1

    repo = rs.CodeRepository(
        assets=[ext_source, consumer],
        default_executor=rs.Executor.in_process(),
    )
    # Observe the external asset
    obs_result = repo.observe(asset_names=["ext_source"])
    assert "ext_source" in obs_result

    # Materialize downstream
    repo.materialize(selection=["consumer"])
    assert repo.load_node("consumer") == 101


# ── Error propagation ──


def test_async_failure_skips_downstream():
    """Async asset failure should skip dependent downstream assets."""

    @rs.Asset
    async def upstream() -> int:
        await asyncio.sleep(0.01)
        raise ValueError("upstream failed")

    @rs.Asset
    def downstream(upstream: int) -> int:
        return upstream + 1

    repo = rs.CodeRepository(
        assets=[upstream, downstream],
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize(raise_on_error=False)
    assert not result.success
    failure_names = [name for name, _ in result.failed_assets]
    assert "upstream" in failure_names
    assert "downstream" in failure_names


# ── max_async_concurrent on Parallel executor ──


def test_parallel_max_async_concurrent_limits_parallelism(tmp_path):
    """max_async_concurrent=1 serializes async steps under Parallel executor."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    async def slow_a():
        await asyncio.sleep(0.3)
        return "a"

    @rs.Asset(io_handler=handler)
    async def slow_b():
        await asyncio.sleep(0.3)
        return "b"

    @rs.Asset(io_handler=handler)
    def merge(slow_a: str, slow_b: str) -> str:
        return f"{slow_a}+{slow_b}"

    assets = [slow_a, slow_b, merge]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[
            rs.Job(
                name="j",
                assets=assets,
                executor=rs.Executor.parallel(max_async_concurrent=1),
            )
        ],
    )
    start = time.monotonic()
    repo.get_job("j").execute()
    elapsed = time.monotonic() - start

    def load(name):
        return handler.load_input(
            rs.InputContext(asset_name=name, downstream_asset="t")
        )

    assert load("merge") == "a+b"
    # max_async_concurrent=1 → sequential: ~0.6s, not ~0.3s
    assert elapsed >= 0.5, f"Expected serial async execution, but took {elapsed:.2f}s"


def test_parallel_max_async_concurrent_none_is_unbounded(tmp_path):
    """max_async_concurrent=None (default) allows full concurrency."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    async def conc_a():
        await asyncio.sleep(0.5)
        return "a"

    @rs.Asset(io_handler=handler)
    async def conc_b():
        await asyncio.sleep(0.5)
        return "b"

    @rs.Asset(io_handler=handler)
    def conc_merge(conc_a: str, conc_b: str) -> str:
        return f"{conc_a}+{conc_b}"

    assets = [conc_a, conc_b, conc_merge]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[
            rs.Job(
                name="j",
                assets=assets,
                executor=rs.Executor.parallel(),
            )
        ],
    )
    start = time.monotonic()
    repo.get_job("j").execute()
    elapsed = time.monotonic() - start

    def load(name):
        return handler.load_input(
            rs.InputContext(asset_name=name, downstream_asset="t")
        )

    assert load("conc_merge") == "a+b"
    # Unbounded → concurrent: ~0.5s, not ~1.0s
    assert elapsed < 0.9, (
        f"Expected concurrent async execution, but took {elapsed:.2f}s"
    )


@pytest.mark.skipif(
    os.environ.get("CI") == "true",
    reason="Timing-sensitive: CI runners have limited vCPUs",
)
def test_parallel_overlaps_sync_workers_with_async(tmp_path):
    """Parallel executor should overlap loky workers (sync) with async event loop.
    2 sync steps (time.sleep 0.5s) + 2 async steps (asyncio.sleep 0.5s) at the same
    level should all overlap, completing in ~0.5s total, not ~1.0s."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def sync_a():
        time.sleep(0.5)
        return "sa"

    @rs.Asset(io_handler=handler)
    def sync_b():
        time.sleep(0.5)
        return "sb"

    @rs.Asset(io_handler=handler)
    async def async_a():
        await asyncio.sleep(0.5)
        return "aa"

    @rs.Asset(io_handler=handler)
    async def async_b():
        await asyncio.sleep(0.5)
        return "ab"

    @rs.Asset(io_handler=handler)
    def merge(sync_a: str, sync_b: str, async_a: str, async_b: str) -> str:
        return f"{sync_a}+{sync_b}+{async_a}+{async_b}"

    assets = [sync_a, sync_b, async_a, async_b, merge]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[
            rs.Job(
                name="j",
                assets=assets,
                executor=rs.Executor.parallel(max_workers=2),
            )
        ],
    )
    start = time.monotonic()
    repo.get_job("j").execute()
    elapsed = time.monotonic() - start

    def load(name):
        return handler.load_input(
            rs.InputContext(asset_name=name, downstream_asset="t")
        )

    assert load("merge") == "sa+sb+aa+ab"
    # Sequential would be 2.0s (4 × 0.5s). Overlap → ~0.5s.
    assert elapsed < 0.9, f"Expected overlapped execution, but took {elapsed:.2f}s"


def test_parallel_max_async_concurrent_in_repr():
    """max_async_concurrent appears in repr when set."""
    e1 = rs.Executor.parallel(max_workers=2, max_async_concurrent=8)
    assert "max_async_concurrent=8" in repr(e1)

    e2 = rs.Executor.parallel(max_workers=2)
    assert "max_async_concurrent" not in repr(e2)


# ── Parallel executor async coverage ──


def test_async_chain_under_parallel(tmp_path):
    """Async chain (async→async→async) under Parallel executor."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    async def par_first():
        await asyncio.sleep(0.01)
        return 1

    @rs.Asset(io_handler=handler)
    async def par_second(par_first: int):
        await asyncio.sleep(0.01)
        return par_first + 1

    @rs.Asset(io_handler=handler)
    async def par_third(par_second: int):
        await asyncio.sleep(0.01)
        return par_second + 1

    assets = [par_first, par_second, par_third]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[
            rs.Job(
                name="j", assets=assets, executor=rs.Executor.parallel(max_workers=2)
            )
        ],
    )
    repo.get_job("j").execute()

    def load(name):
        return handler.load_input(
            rs.InputContext(asset_name=name, downstream_asset="t")
        )

    assert load("par_first") == 1
    assert load("par_second") == 2
    assert load("par_third") == 3


def test_async_asset_with_context_parallel(tmp_path):
    """Async asset receiving AssetExecutionContext under Parallel executor."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    async def par_ctx_asset(context: rs.AssetExecutionContext) -> str:
        await asyncio.sleep(0.01)
        return f"name={context.asset_name}"

    assets = [par_ctx_asset]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[
            rs.Job(
                name="j", assets=assets, executor=rs.Executor.parallel(max_workers=2)
            )
        ],
    )
    repo.get_job("j").execute()

    result = handler.load_input(
        rs.InputContext(asset_name="par_ctx_asset", downstream_asset="t")
    )
    assert result == "name=par_ctx_asset"


def test_async_asset_with_config_parallel(tmp_path):
    """Async asset with config under Parallel executor."""
    import obstore.store
    from pydantic import BaseModel

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    class Cfg(BaseModel):
        factor: int = 5

    @rs.Asset(io_handler=handler)
    async def par_with_config(context: rs.AssetExecutionContext[Cfg]) -> int:
        await asyncio.sleep(0.01)
        return context.config.factor * 10

    assets = [par_with_config]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[
            rs.Job(
                name="j", assets=assets, executor=rs.Executor.parallel(max_workers=2)
            )
        ],
    )
    repo.get_job("j").execute()

    result = handler.load_input(
        rs.InputContext(asset_name="par_with_config", downstream_asset="t")
    )
    assert result == 50


def test_graph_asset_with_async_tasks_parallel(tmp_path):
    """Graph asset with async internal tasks under Parallel executor."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Task
    async def par_async_step() -> int:
        await asyncio.sleep(0.01)
        return 42

    @rs.Asset.from_graph(
        name="par_async_pipe", io_handler=handler, node_io_handler=handler
    )
    def par_async_pipe():
        par_async_step()

    assets = [par_async_pipe]
    repo = rs.CodeRepository(
        assets=assets,
        tasks=[par_async_step],
        jobs=[
            rs.Job(
                name="j", assets=assets, executor=rs.Executor.parallel(max_workers=2)
            )
        ],
    )
    repo.get_job("j").execute()

    result = handler.load_input(
        rs.InputContext(
            asset_name="par_async_pipe/par_async_step", downstream_asset="t"
        )
    )
    assert result == 42


def test_async_failure_propagation_parallel(tmp_path):
    """Async asset failure under Parallel executor skips downstream."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    async def par_failing() -> int:
        await asyncio.sleep(0.01)
        raise ValueError("async boom")

    @rs.Asset(io_handler=handler)
    def par_downstream(par_failing: int) -> int:
        return par_failing + 1

    assets = [par_failing, par_downstream]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[
            rs.Job(
                name="j", assets=assets, executor=rs.Executor.parallel(max_workers=2)
            )
        ],
    )
    with pytest.raises(Exception):
        repo.get_job("j").execute()

    with pytest.raises(Exception):
        handler.load_input(
            rs.InputContext(asset_name="par_downstream", downstream_asset="t")
        )


def test_async_failure_events_parallel(tmp_path, storage):
    """Async asset failure under Parallel emits StepFailure events."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    async def par_fail_ev() -> int:
        await asyncio.sleep(0.01)
        raise ValueError("parallel async failure")

    assets = [par_fail_ev]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[
            rs.Job(
                name="j", assets=assets, executor=rs.Executor.parallel(max_workers=2)
            )
        ],
    )
    repo.resolve(storage=storage)
    with pytest.raises(Exception):
        repo.get_job("j").execute()

    events = storage.get_events_for_asset("par_fail_ev")
    event_types = [e.event_type for e in events]
    assert "StepFailure" in event_types
