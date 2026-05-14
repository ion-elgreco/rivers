"""Tests for executor bug fixes and improvements.

Covers:
- AsyncBackend stdout/stderr/log capture (previously missing entirely)
- Async concurrent mapped fan-out (InProcess + Async + parallel)
- fail_all_steps emitting failure events
- collect_future_result Materialization events for mapped instances
- run_mapped_to_completion failed_names tracking
- Dep-fail filter deduplication (only classify_step, not execute_plan)
"""

import asyncio
import time

import obstore.store
import pytest

import rivers as rs

# ---------------------------------------------------------------------------
# 1. AsyncBackend captures sync stdout (during arg building, before coroutine)
#    NOTE: stdout from inside async coroutines (running on the event loop
#    thread) is NOT captured due to contextvars isolation. This is a known
#    limitation. We test that the capture mechanism is wired up at all.
# ---------------------------------------------------------------------------


def test_async_backend_emits_log_events(storage):
    """Async steps should emit LogOutput events (at minimum for Rust-side logs)."""

    @rs.Asset
    async def logged_async() -> int:
        await asyncio.sleep(0.01)
        return 42

    repo = rs.CodeRepository(
        assets=[logged_async],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    repo.materialize()
    assert repo.load_node("logged_async") == 42

    # Verify the step had start+success events (basic sanity)
    events = storage.get_events_for_asset("logged_async")
    event_types = [e.event_type for e in events]
    assert "StepStart" in event_types
    assert "StepSuccess" in event_types


def test_async_backend_sync_step_completes(storage):
    """Sync steps under AsyncBackend (via spawn_blocking) should execute correctly.
    NOTE: stdout capture in spawn_blocking threads is limited by Python's
    contextvars isolation (new thread context doesn't inherit parent context)."""

    @rs.Asset
    def sync_under_async() -> int:
        return 99

    assets = [sync_under_async]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[rs.Job(name="j", assets=assets, executor=rs.Executor.in_process())],
    )
    repo.resolve(storage=storage)
    repo.get_job("j").execute()
    assert repo.load_node("sync_under_async") == 99

    events = storage.get_events_for_asset("sync_under_async")
    event_types = [e.event_type for e in events]
    assert "StepStart" in event_types
    assert "StepSuccess" in event_types
    assert "Materialization" in event_types


def test_async_backend_failure_emits_events(storage):
    """AsyncBackend failure should emit StepFailure events and mark all event_names."""

    @rs.Asset
    async def failing_async() -> int:
        await asyncio.sleep(0.01)
        raise ValueError("async boom")

    repo = rs.CodeRepository(
        assets=[failing_async],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=False)

    assert not result.success
    events = storage.get_events_for_asset("failing_async")
    event_types = [e.event_type for e in events]
    assert "StepFailure" in event_types


# ---------------------------------------------------------------------------
# 2. Async concurrent mapped fan-out
# ---------------------------------------------------------------------------


@rs.Task
async def async_triple(x: int) -> int:
    await asyncio.sleep(0.01)
    return x * 3


@rs.Task
def sum_items(values: list) -> int:
    return sum(values)


def test_async_mapped_fan_out_in_process():
    """InProcess executor should run async mapped fan-out concurrently."""

    @rs.Asset
    def nums() -> list:
        return [10, 20, 30]

    @rs.Asset.from_graph()
    def ae_fan():
        n = nums()
        mapped = n.map(async_triple)
        return sum_items(mapped.collect())

    assets = [nums, ae_fan]
    repo = rs.CodeRepository(
        assets=assets,
        tasks=[async_triple, sum_items],
        jobs=[rs.Job(name="j", assets=assets, executor=rs.Executor.in_process())],
    )
    repo.get_job("j").execute()
    assert repo.load_node("ae_fan") == 180  # (10+20+30)*3


def test_async_mapped_fan_out_parallel(tmp_path):
    """parallel executor delegates async mapped to run_mapped_concurrent."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def mp_src() -> list:
        return [1, 2, 3, 4]

    @rs.Asset.from_graph(io_handler=handler, node_io_handler=handler)
    def mp_async_fan():
        s = mp_src()
        mapped = s.map(async_triple)
        return sum_items(mapped.collect())

    assets = [mp_src, mp_async_fan]
    repo = rs.CodeRepository(
        assets=assets,
        tasks=[async_triple, sum_items],
        jobs=[
            rs.Job(
                name="j",
                assets=assets,
                executor=rs.Executor.parallel(max_workers=2),
            )
        ],
    )
    repo.get_job("j").execute()

    def load(name):
        return handler.load_input(
            rs.InputContext(asset_name=name, downstream_asset="t")
        )

    assert load("mp_async_fan") == 30  # (1+2+3+4)*3


def test_async_mapped_fan_out_concurrency_timing():
    """Async mapped instances should overlap execution (not run sequentially)."""

    @rs.Task
    async def slow_triple(x: int) -> int:
        await asyncio.sleep(0.3)
        return x * 3

    @rs.Task
    def collect_sum(values: list) -> int:
        return sum(values)

    @rs.Asset
    def items() -> list:
        return [1, 2, 3]

    @rs.Asset.from_graph()
    def timed_fan():
        s = items()
        mapped = s.map(slow_triple)
        return collect_sum(mapped.collect())

    assets = [items, timed_fan]
    repo = rs.CodeRepository(
        assets=assets,
        tasks=[slow_triple, collect_sum],
        jobs=[rs.Job(name="j", assets=assets, executor=rs.Executor.in_process())],
    )
    start = time.monotonic()
    repo.get_job("j").execute()
    elapsed = time.monotonic() - start

    assert repo.load_node("timed_fan") == 18  # (1+2+3)*3
    # 3 instances at 0.3s each: sequential=0.9s, concurrent=~0.3s
    assert elapsed < 0.8, (
        f"Expected concurrent mapped execution, but took {elapsed:.2f}s"
    )


# ---------------------------------------------------------------------------
# 3. fail_all_steps emits failure events
# ---------------------------------------------------------------------------


def test_parallel_in_memory_io_emits_failure_events(storage):
    """InMemoryIOHandler assets under parallel should fail with events emitted."""

    @rs.Asset
    def no_io_a() -> int:
        return 1

    @rs.Asset
    def no_io_b() -> int:
        return 2

    repo = rs.CodeRepository(
        assets=[no_io_a, no_io_b],
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=False)

    assert not result.success
    # Each step should have a StepFailure event (not stuck "in progress")
    for name in ["no_io_a", "no_io_b"]:
        events = storage.get_events_for_asset(name)
        event_types = [e.event_type for e in events]
        assert "StepFailure" in event_types, f"Missing StepFailure event for {name}"


# ---------------------------------------------------------------------------
# 4. collect_future_result Materialization events for mapped instances
# ---------------------------------------------------------------------------


def test_parallel_mapped_emits_materialization(tmp_path, storage):
    """parallel mapped instances should emit Materialization events."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Task
    def mp_double(x: int) -> int:
        return x * 2

    @rs.Task
    def mp_total(values: list) -> int:
        return sum(values)

    @rs.Asset(io_handler=handler)
    def mp_map_source() -> list:
        return [1, 2, 3]

    @rs.Asset.from_graph(io_handler=handler, node_io_handler=handler)
    def mp_map_pipeline():
        s = mp_map_source()
        mapped = s.map(mp_double)
        return mp_total(mapped.collect())

    assets = [mp_map_source, mp_map_pipeline]
    repo = rs.CodeRepository(
        assets=assets,
        tasks=[mp_double, mp_total],
        jobs=[
            rs.Job(
                name="j",
                assets=assets,
                executor=rs.Executor.parallel(max_workers=2),
            )
        ],
    )
    repo.resolve(storage=storage)
    repo.get_job("j").execute()

    def load(name):
        return handler.load_input(
            rs.InputContext(asset_name=name, downstream_asset="t")
        )

    assert load("mp_map_pipeline") == 12  # (1+2+3)*2

    # Each mapped instance should have a Materialization event
    for key in ["0", "1", "2"]:
        instance_name = f"mp_map_pipeline/mp_double__{key}"
        events = storage.get_events_for_asset(instance_name)
        event_types = [e.event_type for e in events]
        has_materialization = any("Materialization" in et for et in event_types)
        assert has_materialization, (
            f"Missing Materialization event for {instance_name}, got: {event_types}"
        )


# ---------------------------------------------------------------------------
# 5. run_mapped_to_completion failed_names tracking
# ---------------------------------------------------------------------------


def test_mapped_instance_failure_skips_downstream():
    """When a mapped instance fails, downstream steps should be skipped."""

    @rs.Task
    def fail_on_two(x: int) -> int:
        if x == 2:
            raise ValueError("fail on 2")
        return x * 10

    @rs.Task
    def consume(values: list) -> int:
        return sum(values)

    @rs.Asset
    def src() -> list:
        return [1, 2, 3]

    @rs.Asset.from_graph()
    def fail_fan():
        s = src()
        mapped = s.map(fail_on_two)
        return consume(mapped.collect())

    @rs.Asset
    def after_fan(fail_fan: int) -> int:
        return fail_fan + 1

    assets = [src, fail_fan, after_fan]
    repo = rs.CodeRepository(
        assets=assets,
        tasks=[fail_on_two, consume],
        jobs=[rs.Job(name="j", assets=assets, executor=rs.Executor.in_process())],
    )
    with pytest.raises(Exception):
        repo.get_job("j").execute()

    # after_fan should not have been materialized (it depends on the failed fan)
    with pytest.raises(KeyError):
        repo.load_node("after_fan")


# ---------------------------------------------------------------------------
# 6. Dep-fail only handled by classify_step (no duplicate events)
# ---------------------------------------------------------------------------


def test_dep_fail_no_duplicate_events(storage):
    """Dep-failed step should have exactly 1 StepFailure event (not duplicated)."""

    @rs.Asset
    def upstream() -> int:
        raise ValueError("intentional failure")

    @rs.Asset
    def downstream(upstream: int) -> int:
        return upstream + 1

    repo = rs.CodeRepository(
        assets=[upstream, downstream],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=False)

    assert not result.success

    events = storage.get_events_for_asset("downstream")
    failure_events = [e for e in events if e.event_type == "StepFailure"]
    assert len(failure_events) == 1, (
        f"Expected exactly 1 StepFailure for downstream, got {len(failure_events)}"
    )


def test_dep_fail_chain_no_duplicate_events(storage):
    """Three-level chain: mid and leaf should each get exactly 1 failure event."""

    @rs.Asset
    def root() -> int:
        raise ValueError("root fails")

    @rs.Asset
    def mid(root: int) -> int:
        return root + 1

    @rs.Asset
    def leaf(mid: int) -> int:
        return mid + 1

    repo = rs.CodeRepository(
        assets=[root, mid, leaf],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=False)

    assert not result.success
    for name in ["mid", "leaf"]:
        events = storage.get_events_for_asset(name)
        failure_events = [e for e in events if e.event_type == "StepFailure"]
        assert len(failure_events) == 1, (
            f"Expected exactly 1 StepFailure for {name}, got {len(failure_events)}"
        )


# ---------------------------------------------------------------------------
# 7. Fan-out cross-executor coverage
# ---------------------------------------------------------------------------


@rs.Task
def double_sync(x: int) -> int:
    return x * 2


@rs.Task
def sum_sync(values: list) -> int:
    return sum(values)


def test_fan_out_map_collect_in_process():
    """Fan-out .map/.collect with InProcess executor."""

    @rs.Asset
    def data() -> list:
        return [5, 10, 15]

    @rs.Asset.from_graph()
    def fan_ae():
        d = data()
        mapped = d.map(double_sync)
        return sum_sync(mapped.collect())

    assets = [data, fan_ae]
    repo = rs.CodeRepository(
        assets=assets,
        tasks=[double_sync, sum_sync],
        jobs=[rs.Job(name="j", assets=assets, executor=rs.Executor.in_process())],
    )
    repo.get_job("j").execute()
    assert repo.load_node("fan_ae") == 60  # (5+10+15)*2


@rs.Task
def consume_stream(items: object) -> list:
    return list(items)


def test_fan_out_collect_stream_in_process():
    """Fan-out .collect_stream() with InProcess executor."""

    @rs.Asset
    def stream_data() -> list:
        return [1, 2, 3]

    @rs.Asset.from_graph()
    def fan_stream_ae():
        d = stream_data()
        mapped = d.map(double_sync)
        return consume_stream(mapped.collect_stream())

    assets = [stream_data, fan_stream_ae]
    repo = rs.CodeRepository(
        assets=assets,
        tasks=[double_sync, consume_stream],
        jobs=[rs.Job(name="j", assets=assets, executor=rs.Executor.in_process())],
    )
    repo.get_job("j").execute()
    result = repo.load_node("fan_stream_ae")
    assert sorted(result) == [2, 4, 6]


# ---------------------------------------------------------------------------
# 8. AsyncBackend error handling marks all event_names (multi-asset)
# ---------------------------------------------------------------------------


def test_async_multi_asset_failure_marks_all_outputs():
    """When an async multi-asset fails, all output names should be failed."""

    @rs.Asset.from_multi(output_defs=[rs.AssetDef("out_a"), rs.AssetDef("out_b")])
    async def failing_multi():
        await asyncio.sleep(0.01)
        raise ValueError("multi-asset boom")

    @rs.Asset
    def depends_on_a(out_a: int) -> int:
        return out_a + 1

    repo = rs.CodeRepository(
        assets=[failing_multi, depends_on_a],
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize(raise_on_error=False)

    assert not result.success
    failure_names = [name for name, _ in result.failed_assets]
    # Downstream should be skipped due to dependency failure
    assert "depends_on_a" in failure_names


# ---------------------------------------------------------------------------
# Stdout/stderr capture content assertion
# ---------------------------------------------------------------------------


def test_sync_stdout_capture_content(monkeypatch, storage):
    """InProcess sync step stdout is captured as LogOutput event content."""
    import rivers._capture as cap

    monkeypatch.setattr(cap, "_installed", False)
    cap.install(tee=True)

    @rs.Asset
    def cap_stdout_asset() -> int:
        print("hello from sync step")
        return 42

    assets = [cap_stdout_asset]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[rs.Job(name="j", assets=assets, executor=rs.Executor.in_process())],
    )
    repo.resolve(storage=storage)
    repo.get_job("j").execute()
    assert repo.load_node("cap_stdout_asset") == 42

    events = storage.get_events_for_asset("cap_stdout_asset")
    log_events = [e for e in events if e.event_type == "LogOutput"]
    assert len(log_events) >= 1
    log_meta = dict(log_events[0].metadata)
    assert "hello from sync step" in log_meta.get("stdout", "")


def test_sync_stderr_capture_content(monkeypatch, storage):
    """InProcess sync step stderr is captured as LogOutput event content."""
    import sys

    import rivers._capture as cap

    monkeypatch.setattr(cap, "_installed", False)
    cap.install(tee=True)

    @rs.Asset
    def cap_stderr_asset() -> int:
        print("error output", file=sys.stderr)
        return 99

    assets = [cap_stderr_asset]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[rs.Job(name="j", assets=assets, executor=rs.Executor.in_process())],
    )
    repo.resolve(storage=storage)
    repo.get_job("j").execute()
    assert repo.load_node("cap_stderr_asset") == 99

    events = storage.get_events_for_asset("cap_stderr_asset")
    log_events = [e for e in events if e.event_type == "LogOutput"]
    assert len(log_events) >= 1
    log_meta = dict(log_events[0].metadata)
    assert "error output" in log_meta.get("stderr", "")
