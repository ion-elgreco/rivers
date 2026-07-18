"""Comprehensive tests for the parallel executor worker.

Tests cover: context injection, output metadata, data versions, resources,
DAG shapes, error handling, collecting mode, IO handlers (Pickle, Delta),
mixed configurations, and per-asset executor overrides.
"""

import obstore
import obstore.store
import polars as pl
import pyarrow as pa
import pytest
from pydantic import BaseModel

import rivers as rs
from rivers.exceptions import ConfigurationError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MP = rs.Executor.parallel(max_workers=2)


def _job_repo(assets, executor=MP, resources=None):
    """Build a CodeRepository with a single job wrapping the given assets."""
    return rs.CodeRepository(
        assets=assets,
        resources=resources or {},
        jobs=[rs.Job(name="j", assets=assets, executor=executor)],
    )


def _run(assets, executor=MP, resources=None):
    """Execute a single-job repo and return the repo (use repo.load_node to read outputs)."""
    repo = _job_repo(assets, executor, resources)
    repo.get_job("j").execute()
    return repo


# ---------------------------------------------------------------------------
# 1. Context injection in parallel parallel steps
# ---------------------------------------------------------------------------


def test_mp_context_asset_name():
    """Context.asset_name is correct in parallel subprocess."""

    @rs.Asset
    def ctx_name(context: rs.AssetExecutionContext) -> str:
        return context.asset_name

    repo = _run([ctx_name])
    assert repo.load_node("ctx_name") == "ctx_name"


def test_mp_context_tags():
    """Context.tags are passed through to parallel subprocess."""

    @rs.Asset(tags=["prod", "data"])
    def tagged(context: rs.AssetExecutionContext) -> list:
        return list(context.tags)  # type: ignore

    repo = _run([tagged])
    assert sorted(repo.load_node("tagged")) == ["data", "prod"]


def test_mp_context_kinds():
    """Context.kinds are passed through to parallel subprocess."""

    @rs.Asset(kinds="table")
    def kinded(context: rs.AssetExecutionContext) -> list:
        return list(context.kinds)

    repo = _run([kinded])
    assert "table" in repo.load_node("kinded")


def test_mp_context_group():
    """Context.group is passed through to parallel subprocess."""

    @rs.Asset(group="analytics")
    def grouped(context: rs.AssetExecutionContext) -> str:
        return context.group  # type: ignore

    repo = _run([grouped])
    assert repo.load_node("grouped") == "analytics"


def test_mp_context_code_version():
    """Context.code_version is passed through to parallel subprocess."""

    @rs.Asset(code_version="v2.1")
    def versioned(context: rs.AssetExecutionContext) -> str:
        return context.code_version  # type: ignore

    repo = _run([versioned])
    assert repo.load_node("versioned") == "v2.1"


def test_mp_context_metadata_access():
    """Context can access asset metadata in parallel subprocess."""

    @rs.Asset(metadata={"priority": "high"})
    def with_meta(context: rs.AssetExecutionContext) -> dict:
        return context.asset_metadata  # type: ignore

    repo = _run([with_meta])
    assert repo.load_node("with_meta")["priority"] == "high"


def test_mp_context_add_output_metadata():
    """Output metadata added via context is returned from subprocess."""

    @rs.Asset
    def meta_setter(context: rs.AssetExecutionContext) -> int:
        context.add_output_metadata({"rows": 42})
        return 1

    repo = _run([meta_setter])
    assert repo.load_node("meta_setter") == 1


def test_mp_context_data_version_registration():
    """data_version registered via context propagates from subprocess."""

    @rs.Asset
    def dv_asset(context: rs.AssetExecutionContext) -> int:
        context.register_data_version("abc123")
        return 99

    repo = _run([dv_asset])
    assert repo.load_node("dv_asset") == 99


def test_mp_two_parallel_contexts(tmp_path):
    """Two independent assets with context run in parallel subprocess."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def p1(context: rs.AssetExecutionContext) -> str:
        return f"p1:{context.asset_name}"

    @rs.Asset(io_handler=handler)
    def p2(context: rs.AssetExecutionContext) -> str:
        return f"p2:{context.asset_name}"

    _run([p1, p2])
    ctx_p1 = rs.InputContext(asset_name="p1", downstream_asset="test")
    ctx_p2 = rs.InputContext(asset_name="p2", downstream_asset="test")
    assert handler.load_input(ctx_p1) == "p1:p1"
    assert handler.load_input(ctx_p2) == "p2:p2"


def test_mp_context_with_downstream(tmp_path):
    """Context assets feed into a downstream merge."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def ca(context: rs.AssetExecutionContext) -> str:
        return context.asset_name

    @rs.Asset(io_handler=handler)
    def cb(context: rs.AssetExecutionContext) -> str:
        return context.asset_name

    @rs.Asset(io_handler=handler)
    def merged(ca: str, cb: str) -> str:
        return f"{ca}+{cb}"

    repo = _run([ca, cb, merged])
    assert repo.load_node("merged") == "ca+cb"


# ---------------------------------------------------------------------------
# 2. Config injection in parallel
# ---------------------------------------------------------------------------


class MPConfig(BaseModel):
    threshold: float = 0.5
    label: str = "default"


def test_mp_config_injection():
    """Pydantic config is available via context.config in subprocess."""

    @rs.Asset
    def cfg_asset(context: rs.AssetExecutionContext[MPConfig]) -> dict:
        return {"threshold": context.config.threshold, "label": context.config.label}

    repo = _run([cfg_asset])
    output = repo.load_node("cfg_asset")
    assert output["threshold"] == 0.5
    assert output["label"] == "default"


def test_mp_captures_stdout_in_log_event(tmp_path, storage):
    """Two sync siblings forced through loky should each ship their stdout
    back as a run_logs row, mirroring the in-process StepCapture path
    (`test_materialize_captures_stdout_to_run_logs` in test_materialize_parity).
    Currently the worker doesn't run StepCapture, so no log row is ever
    emitted from a loky child — `WorkOutcome::Error/WorkerSummary` always
    arrives with `captured_logs: None`."""

    import rivers._capture as cap

    cap._installed = False  # force install path to run

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def chatty_left() -> int:
        print("hello from left")
        return 1

    @rs.Asset(io_handler=handler)
    def chatty_right() -> int:
        print("hello from right")
        return 2

    assets = [chatty_left, chatty_right]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[rs.Job(name="j", assets=assets, executor=MP)],
    )
    repo.resolve(storage=storage)
    result = repo.get_job("j").execute()

    logs = storage.get_run_logs(result.run_id)
    log_left = [log for log in logs if log.step_key == "chatty_left"]
    log_right = [log for log in logs if log.step_key == "chatty_right"]
    assert log_left, "no run_logs row for chatty_left under parallel executor"
    assert log_right, "no run_logs row for chatty_right under parallel executor"
    assert "hello from left" in (log_left[0].stdout or "")
    assert "hello from right" in (log_right[0].stdout or "")


def test_mp_captures_stdout_when_worker_raises(tmp_path, storage):
    """Stdout printed by the worker before it raises should still arrive on
    the parent as a run_logs row. The worker stashes
    `_rivers_captured_logs` on the exception before re-raising; the
    orchestrator extracts it at the `WorkOutcome::Error` site."""

    import rivers._capture as cap

    cap._installed = False  # force install path to run

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def chatty_then_boom() -> int:
        print("about to fail")
        raise RuntimeError("intentional failure for test")

    @rs.Asset(io_handler=handler)
    def chatty_sibling() -> int:
        print("sibling completed")
        return 1

    assets = [chatty_then_boom, chatty_sibling]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[rs.Job(name="j", assets=assets, executor=MP)],
    )
    repo.resolve(storage=storage)
    with pytest.raises(Exception, match="intentional failure"):
        repo.get_job("j").execute()

    runs = storage.get_runs()
    assert len(runs) == 1
    boom_logs = [
        log
        for log in storage.get_run_logs(runs[0].run_id)
        if log.step_key == "chatty_then_boom"
    ]
    boom_events = storage.get_events_for_asset("chatty_then_boom")
    failure_events = [e for e in boom_events if e.event_type == "StepFailure"]
    assert boom_logs, "no run_logs row emitted for failed parallel asset"
    assert failure_events, "no StepFailure emitted for failed parallel asset"
    assert "about to fail" in (boom_logs[0].stdout or "")


def test_mp_success_hook_receives_config(tmp_path):
    """When a parallel asset succeeds, the success hook should fire with
    `context.config` populated — same as the in-process path. Today
    `process_one_worker_item` (results.rs) hardcodes `config_instance: None`
    when invoking `run_success_hooks` for the worker-summary path."""

    received: dict = {}

    def on_success(context: rs.HookContext[MPConfig]):
        received["config"] = context.config
        received["asset_name"] = context.asset_name
        received["hook_type"] = context.hook_type

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler, hooks=[rs.Hook.success(on_success)])
    def hooked_succeeding(context: rs.AssetExecutionContext[MPConfig]) -> int:
        return int(context.config.threshold * 100)

    @rs.Asset(io_handler=handler)
    def hooked_sibling() -> int:
        return 1

    assets = [hooked_succeeding, hooked_sibling]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[rs.Job(name="j", assets=assets, executor=MP)],
    )
    repo.get_job("j").execute()

    assert received.get("asset_name") == "hooked_succeeding"
    assert received.get("hook_type") == "success"
    assert isinstance(received.get("config"), MPConfig), (
        f"expected MPConfig, got {type(received.get('config'))!r}"
    )
    assert received["config"].threshold == 0.5


def test_mp_failure_hook_receives_config(tmp_path):
    """When a parallel asset fails, the failure hook should fire with
    `context.config` populated — same as the in-process path. Today the
    parallel orchestrator hardcodes `failure_config: None` at every
    `WorkOutcome::Error` site (execute.rs)."""

    received: dict = {}

    def on_failure(context: rs.HookContext[MPConfig]):
        received["config"] = context.config
        received["error"] = context.error
        received["asset_name"] = context.asset_name

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler, hooks=[rs.Hook.failure(on_failure)])
    def hooked_failing(context: rs.AssetExecutionContext[MPConfig]) -> int:
        raise ValueError(f"boom (threshold={context.config.threshold})")

    @rs.Asset(io_handler=handler)
    def hooked_sibling() -> int:
        return 1

    assets = [hooked_failing, hooked_sibling]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[rs.Job(name="j", assets=assets, executor=MP)],
    )
    with pytest.raises(Exception, match="boom"):
        repo.get_job("j").execute()

    assert received.get("asset_name") == "hooked_failing"
    assert received.get("error") is not None and "boom" in received["error"]
    assert isinstance(received.get("config"), MPConfig), (
        f"expected MPConfig, got {type(received.get('config'))!r}"
    )
    assert received["config"].threshold == 0.5


def test_graph_asset_success_hook_fires_once(tmp_path):
    """Asset.from_graph(hooks=[…]) success hook fires exactly once when the
    graph asset materializes — both under parallel and in-process. Today
    the graph-asset coordinator step emits Materialization+Success events
    (classify.rs:74-86) but never invokes run_success_hooks."""

    received: list[str] = []

    def on_success(context: rs.HookContext):
        received.append(context.asset_name)

    @rs.Task
    def double_local(x: int) -> int:
        return x * 2

    @rs.Task
    def sum_all_local(values: list) -> int:
        return sum(values)

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    io = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=io)
    def numbers() -> list:
        return [1, 2, 3]

    @rs.Asset.from_graph(
        io_handler=io,
        node_io_handler=io,
        hooks=[rs.Hook.success(on_success)],
    )
    def doubled_sum():
        nums = numbers()
        mapped = nums.map(double_local)
        return sum_all_local(mapped.collect())

    # In-process baseline first — establishes that exactly-one is the contract.
    ip_repo = rs.CodeRepository(
        assets=[numbers, doubled_sum],
        tasks=[double_local, sum_all_local],
        default_executor=rs.Executor.in_process(),
    )
    ip_repo.materialize()
    assert received.count("doubled_sum") == 1, (
        f"in-process baseline expected exactly one hook firing; got {received}"
    )

    received.clear()
    repo = rs.CodeRepository(
        assets=[numbers, doubled_sum],
        tasks=[double_local, sum_all_local],
        jobs=[rs.Job(name="j", assets=[numbers, doubled_sum], executor=MP)],
    )
    repo.get_job("j").execute()

    assert received.count("doubled_sum") == 1, (
        f"expected exactly one parent-asset hook firing under parallel; got {received}"
    )


def test_graph_asset_failure_hook_fires_when_internal_task_fails(tmp_path):
    """When an internal task of `Asset.from_graph(hooks=[…])` fails, the
    graph asset's failure hook should fire — across both in-process and
    parallel. Today classify.rs's dep-fail path uses
    `record_failure_no_hooks`, so graph-asset failure hooks never fire."""

    received: list[str] = []

    def on_failure(context: rs.HookContext):
        received.append(context.asset_name)

    @rs.Task
    def boom_task() -> int:
        raise ValueError("graph internal task failure")

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    io = rs.PickleIOHandler(store=store)

    @rs.Asset.from_graph(
        io_handler=io,
        node_io_handler=io,
        hooks=[rs.Hook.failure(on_failure)],
    )
    def graph_pipe():
        return boom_task()

    # In-process baseline.
    ip_repo = rs.CodeRepository(
        assets=[graph_pipe],
        tasks=[boom_task],
        default_executor=rs.Executor.in_process(),
    )
    with pytest.raises(Exception, match="graph internal task failure"):
        ip_repo.materialize()
    assert received.count("graph_pipe") == 1, (
        f"in-process: expected exactly one graph-asset failure hook firing; got {received}"
    )

    received.clear()
    repo = rs.CodeRepository(
        assets=[graph_pipe],
        tasks=[boom_task],
        jobs=[rs.Job(name="j", assets=[graph_pipe], executor=MP)],
    )
    with pytest.raises(Exception, match="graph internal task failure"):
        repo.get_job("j").execute()
    assert received.count("graph_pipe") == 1, (
        f"parallel: expected exactly one graph-asset failure hook firing; got {received}"
    )


def test_mp_failure_hook_fires_when_pool_unavailable(monkeypatch, tmp_path):
    """When the loky pool can't be acquired the whole batch fails before any
    worker runs. Failure hooks should still fire — same as the in-process
    path. Today `fail_all_instances` (dispatch/context.rs) only emits the
    StepFailure event and pushes to `failures`; it doesn't call
    `run_failure_hooks`."""

    import loky

    received: list[str] = []

    def on_failure(context: rs.HookContext):
        received.append(context.asset_name)

    def boom_get_executor(*args, **kwargs):
        raise RuntimeError("simulated loky pool unavailable")

    monkeypatch.setattr(loky, "get_reusable_executor", boom_get_executor)

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler, hooks=[rs.Hook.failure(on_failure)])
    def hooked_a() -> int:
        return 1

    @rs.Asset(io_handler=handler, hooks=[rs.Hook.failure(on_failure)])
    def hooked_b() -> int:
        return 2

    assets = [hooked_a, hooked_b]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[rs.Job(name="j", assets=assets, executor=MP)],
    )
    with pytest.raises(Exception):
        repo.get_job("j").execute()

    assert "hooked_a" in received, (
        f"expected failure hook fired for hooked_a; got {received}"
    )
    assert "hooked_b" in received, (
        f"expected failure hook fired for hooked_b; got {received}"
    )


def test_mp_config_two_siblings_crosses_loky_boundary(tmp_path):
    """Two sync siblings in the same batch defeat the single-asset
    InProcess fast-path (parallel/execute.rs:52-61) and force execution
    through the loky worker. Today `WorkerArgs` doesn't serialize config,
    so `context.config` is None inside the worker and accessing a field
    raises AttributeError."""

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def cfg_left(context: rs.AssetExecutionContext[MPConfig]) -> dict:
        return {"side": "left", "threshold": context.config.threshold}

    @rs.Asset(io_handler=handler)
    def cfg_right(context: rs.AssetExecutionContext[MPConfig]) -> dict:
        return {"side": "right", "label": context.config.label}

    _run([cfg_left, cfg_right])

    left = handler.load_input(
        rs.InputContext(asset_name="cfg_left", downstream_asset="t")
    )
    right = handler.load_input(
        rs.InputContext(asset_name="cfg_right", downstream_asset="t")
    )
    assert left == {"side": "left", "threshold": 0.5}
    assert right == {"side": "right", "label": "default"}


# ---------------------------------------------------------------------------
# 3. Output metadata propagation from subprocess
# ---------------------------------------------------------------------------


def test_mp_output_metadata_multiple_keys():
    """Multiple metadata keys are all propagated from subprocess."""

    @rs.Asset
    def rich_meta(context: rs.AssetExecutionContext) -> str:
        context.add_output_metadata(
            {
                "rows": 100,
                "source": "api",
                "latency_ms": 42.5,
            }
        )
        return "done"

    repo = _run([rich_meta])
    assert repo.load_node("rich_meta") == "done"


# ---------------------------------------------------------------------------
# 4. Data version registration
# ---------------------------------------------------------------------------


def test_mp_data_version_parallel(tmp_path):
    """Both parallel assets register distinct data versions."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def dv_x(context: rs.AssetExecutionContext) -> int:
        context.register_data_version("hash-x")
        return 1

    @rs.Asset(io_handler=handler)
    def dv_y(context: rs.AssetExecutionContext) -> int:
        context.register_data_version("hash-y")
        return 2

    _run([dv_x, dv_y])
    ctx_x = rs.InputContext(asset_name="dv_x", downstream_asset="test")
    ctx_y = rs.InputContext(asset_name="dv_y", downstream_asset="test")
    assert handler.load_input(ctx_x) == 1
    assert handler.load_input(ctx_y) == 2


# ---------------------------------------------------------------------------
# 5. Resource injection with parallel
# ---------------------------------------------------------------------------


class SimpleResource(rs.Resource):
    prefix: str = "res"


class SetupResource(rs.Resource):
    label: str = "worker"

    def setup(self):
        self.__dict__["_ready"] = True  # type: ignore

    def teardown(self):
        self.__dict__["_ready"] = False  # type: ignore


def test_mp_resource_injection():
    """Resource is deserialized and available in subprocess."""

    @rs.Asset
    def res_user(res: SimpleResource) -> str:
        return res.prefix

    repo = _run([res_user], resources={"res": SimpleResource(prefix="hello")})
    assert repo.load_node("res_user") == "hello"


def test_mp_resource_setup_called():
    """Resource setup() is called in subprocess after deserialization."""

    @rs.Asset
    def setup_check(res: SetupResource) -> bool:
        return res.__dict__.get("_ready", False)

    repo = _run([setup_check], resources={"res": SetupResource()})
    assert repo.load_node("setup_check") is True


def test_mp_resource_parallel(tmp_path):
    """Two parallel assets both get resource injected."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def r_a(res: SimpleResource) -> str:
        return f"a:{res.prefix}"

    @rs.Asset(io_handler=handler)
    def r_b(res: SimpleResource) -> str:
        return f"b:{res.prefix}"

    _run([r_a, r_b], resources={"res": SimpleResource(prefix="mp")})
    ctx_a = rs.InputContext(asset_name="r_a", downstream_asset="test")
    ctx_b = rs.InputContext(asset_name="r_b", downstream_asset="test")
    assert handler.load_input(ctx_a) == "a:mp"
    assert handler.load_input(ctx_b) == "b:mp"


def test_mp_resource_with_context():
    """Resource + context both work together in subprocess."""

    @rs.Asset
    def both(context: rs.AssetExecutionContext, res: SimpleResource) -> str:
        return f"{context.asset_name}:{res.prefix}"

    repo = _run([both], resources={"res": SimpleResource(prefix="dual")})
    assert repo.load_node("both") == "both:dual"


def test_mp_resource_with_upstream():
    """Resource + upstream dependency both resolve in subprocess."""

    @rs.Asset
    def source() -> int:
        return 10

    @rs.Asset
    def consumer(source: int, res: SimpleResource) -> str:
        return f"{source}:{res.prefix}"

    repo = _run([source, consumer], resources={"res": SimpleResource(prefix="up")})
    assert repo.load_node("consumer") == "10:up"


# ---------------------------------------------------------------------------
# 6. DAG shapes
# ---------------------------------------------------------------------------


def test_mp_single_node():
    """Single-node DAG runs in-process (optimization)."""

    @rs.Asset
    def solo() -> int:
        return 42

    repo = _run([solo])
    assert repo.load_node("solo") == 42


def test_mp_chain_3():
    """Linear chain of 3 assets."""

    @rs.Asset
    def c1() -> int:
        return 1

    @rs.Asset
    def c2(c1: int) -> int:
        return c1 + 1

    @rs.Asset
    def c3(c2: int) -> int:
        return c2 + 1

    repo = _run([c1, c2, c3])
    assert repo.load_node("c3") == 3


def test_mp_chain_5():
    """Deeper chain of 5 assets."""

    @rs.Asset
    def d1() -> int:
        return 1

    @rs.Asset
    def d2(d1: int) -> int:
        return d1 * 2

    @rs.Asset
    def d3(d2: int) -> int:
        return d2 * 2

    @rs.Asset
    def d4(d3: int) -> int:
        return d3 * 2

    @rs.Asset
    def d5(d4: int) -> int:
        return d4 * 2

    repo = _run([d1, d2, d3, d4, d5])
    assert repo.load_node("d5") == 16


def test_mp_wide_fan_out(tmp_path):
    """One root fans out to 4 independent leaves."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def root() -> int:
        return 10

    @rs.Asset(io_handler=handler)
    def leaf_a(root: int) -> int:
        return root + 1

    @rs.Asset(io_handler=handler)
    def leaf_b(root: int) -> int:
        return root + 2

    @rs.Asset(io_handler=handler)
    def leaf_c(root: int) -> int:
        return root + 3

    @rs.Asset(io_handler=handler)
    def leaf_d(root: int) -> int:
        return root + 4

    _run([root, leaf_a, leaf_b, leaf_c, leaf_d])
    assert (
        handler.load_input(rs.InputContext(asset_name="leaf_a", downstream_asset="t"))
        == 11
    )
    assert (
        handler.load_input(rs.InputContext(asset_name="leaf_b", downstream_asset="t"))
        == 12
    )
    assert (
        handler.load_input(rs.InputContext(asset_name="leaf_c", downstream_asset="t"))
        == 13
    )
    assert (
        handler.load_input(rs.InputContext(asset_name="leaf_d", downstream_asset="t"))
        == 14
    )


def test_mp_fan_in(tmp_path):
    """Multiple independent sources fan into one merge."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def s1() -> int:
        return 1

    @rs.Asset(io_handler=handler)
    def s2() -> int:
        return 2

    @rs.Asset(io_handler=handler)
    def s3() -> int:
        return 3

    @rs.Asset(io_handler=handler)
    def fan_merge(s1: int, s2: int, s3: int) -> int:
        return s1 + s2 + s3

    repo = _run([s1, s2, s3, fan_merge])
    assert repo.load_node("fan_merge") == 6


def test_mp_multi_level_parallel(tmp_path):
    """Two levels of parallelism: (a,b) → (c,d) → merge."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def ma() -> int:
        return 1

    @rs.Asset(io_handler=handler)
    def mb() -> int:
        return 2

    @rs.Asset(io_handler=handler)
    def mc(ma: int) -> int:
        return ma * 10

    @rs.Asset(io_handler=handler)
    def md(mb: int) -> int:
        return mb * 10

    @rs.Asset(io_handler=handler)
    def mm(mc: int, md: int) -> int:
        return mc + md

    repo = _run([ma, mb, mc, md, mm])
    assert repo.load_node("mm") == 30


# ---------------------------------------------------------------------------
# 7. Error handling
# ---------------------------------------------------------------------------


def test_mp_error_in_subprocess(tmp_path):
    """Error in a parallel subprocess propagates to parent."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def ok_asset() -> int:
        return 1

    @rs.Asset(io_handler=handler)
    def bad_asset() -> int:
        raise ValueError("intentional failure")

    with pytest.raises(Exception, match="intentional failure"):
        _run([ok_asset, bad_asset])


def test_mp_collecting_continues_on_failure(tmp_path):
    """Collecting mode continues past failures."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def good() -> int:
        return 1

    @rs.Asset(io_handler=handler)
    def fail_asset() -> int:
        raise ValueError("boom")

    @rs.Asset(io_handler=handler)
    def after_good(good: int) -> int:
        return good + 1

    @rs.Asset(io_handler=handler)
    def after_fail(fail_asset: int) -> int:
        return fail_asset + 1

    assets = [good, fail_asset, after_good, after_fail]
    repo = rs.CodeRepository(assets=assets, default_executor=MP)
    result = repo.materialize(raise_on_error=False)
    assert (
        handler.load_input(rs.InputContext(asset_name="good", downstream_asset="t"))
        == 1
    )
    assert (
        handler.load_input(
            rs.InputContext(asset_name="after_good", downstream_asset="t")
        )
        == 2
    )
    assert "fail_asset" in [name for name, _ in result.failed_assets]


def test_mp_collecting_skips_downstream_of_failure():
    """Downstream of a failed asset is skipped in collecting mode."""

    @rs.Asset
    def fail_root() -> int:
        raise RuntimeError("root fail")

    @rs.Asset
    def dep_of_fail(fail_root: int) -> int:
        return fail_root + 1

    assets = [fail_root, dep_of_fail]
    repo = rs.CodeRepository(assets=assets, default_executor=MP)
    result = repo.materialize(raise_on_error=False)
    failure_names = [name for name, _ in result.failed_assets]
    assert "fail_root" in failure_names
    assert "dep_of_fail" in failure_names


# ---------------------------------------------------------------------------
# 8. PickleIOHandler + parallel
# ---------------------------------------------------------------------------


def test_mp_pickle_io_chain(tmp_path):
    """PickleIOHandler writes/reads in parallel chain."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def pk_src() -> list:
        return [1, 2, 3]

    @rs.Asset(io_handler=handler)
    def pk_dst(pk_src: list) -> int:
        return sum(pk_src)

    repo = _run([pk_src, pk_dst])
    assert repo.load_node("pk_src") == [1, 2, 3]
    assert repo.load_node("pk_dst") == 6
    assert (tmp_path / "pk_src.pkl").exists()
    assert (tmp_path / "pk_dst.pkl").exists()


def test_mp_pickle_io_parallel_write(tmp_path):
    """Two parallel assets both write via PickleIOHandler."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def pw_a() -> int:
        return 10

    @rs.Asset(io_handler=handler)
    def pw_b() -> int:
        return 20

    _run([pw_a, pw_b])
    # Parallel IO-handler assets write to store, verify via load
    assert (tmp_path / "pw_a.pkl").exists()
    assert (tmp_path / "pw_b.pkl").exists()
    ctx_a = rs.InputContext(asset_name="pw_a", downstream_asset="test")
    ctx_b = rs.InputContext(asset_name="pw_b", downstream_asset="test")
    assert handler.load_input(ctx_a) == 10
    assert handler.load_input(ctx_b) == 20


def test_mp_pickle_io_with_context(tmp_path):
    """PickleIOHandler + context in parallel."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def pic_ctx(context: rs.AssetExecutionContext) -> str:
        return f"name={context.asset_name}"

    repo = _run([pic_ctx])
    assert repo.load_node("pic_ctx") == "name=pic_ctx"


def test_mp_pickle_io_downstream_loads_from_store(tmp_path):
    """Downstream loads from IO handler store when upstream ran in parallel subprocess."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def io_src() -> list:
        return [10, 20]

    @rs.Asset(io_handler=handler)
    def io_parallel() -> int:
        return 99

    @rs.Asset(io_handler=handler)
    def io_sink(io_src: list, io_parallel: int) -> int:
        return sum(io_src) + io_parallel

    _run([io_src, io_parallel, io_sink])
    # Verify final result via direct load
    ctx = rs.InputContext(asset_name="io_sink", downstream_asset="test")
    assert handler.load_input(ctx) == 129


class _PayloadHandler(rs.BaseIOHandler):
    """Test handler whose ``load_input`` always returns ``payload``.

    Writes are discarded — used to distinguish *which* handler loaded a value
    in the parallel pickle path tests below.
    """

    payload: int

    def handle_output(self, context, obj):
        # Writes discarded; this handler exists to assert load-path attribution.
        pass

    def load_input(self, context):
        return self.payload


_OVERRIDE_TEST_UPSTREAM_HANDLER = _PayloadHandler(payload=100)


@rs.Asset(io_handler=_OVERRIDE_TEST_UPSTREAM_HANDLER)
def _override_test_upstream() -> int:
    # Value irrelevant — _OVERRIDE_TEST_UPSTREAM_HANDLER.handle_output is a no-op.
    # load_input always returns 100 (the upstream's handler payload).
    return 0


def test_mp_input_io_handler_override_honored(tmp_path):
    """Per-input ``io_handler`` override is honored under the parallel executor.

    The upstream is defined module-level so its callable has a stable
    ``module.qualname`` (``IOHandlerRef`` only succeeds for non-``<locals>``
    callables — without that the bug doesn't surface). A sibling sync asset at
    the downstream's level defeats the size-1 ``InProcess`` fallback in
    ``parallel/execute.rs`` so loky actually runs.

    The downstream's ``AssetDef.input(io_handler=override)`` should take
    precedence over upstream's own handler. With the bug, the parallel pickle
    path wraps via ``upstream_func`` and the worker reconstructs
    ``upstream.io_handler`` (payload=100), discarding the override
    (payload=999).
    """
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    shared_handler = rs.PickleIOHandler(store=store)
    override_handler = _PayloadHandler(payload=999)

    @rs.Asset(
        io_handler=shared_handler,
        deps=[
            rs.AssetDef.input("_override_test_upstream", io_handler=override_handler)
        ],
    )
    def downstream_with_override(_override_test_upstream: int) -> int:
        # Echoes whichever handler loaded the upstream value.
        return _override_test_upstream

    @rs.Asset(io_handler=shared_handler)
    def downstream_sibling(_override_test_upstream: int) -> int:
        # Same level as downstream_with_override → 2 sync instances → loky
        # fires (defeats parallel/execute.rs size-1 InProcess fallback).
        return _override_test_upstream

    repo = _run([_override_test_upstream, downstream_with_override, downstream_sibling])
    assert repo.load_node("downstream_with_override") == 999, (
        "Per-input io_handler override should be honored under parallel; "
        "got upstream's handler payload (100) instead, indicating the override "
        "was discarded by the parallel pickle wrapper."
    )


def test_mp_mixed_io_and_no_io(tmp_path):
    """All parallel assets need a non-InMemory IO handler for parallel."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def persisted() -> int:
        return 42

    @rs.Asset(io_handler=handler)
    def not_persisted() -> int:
        return 7

    @rs.Asset(io_handler=handler)
    def combined(persisted: int, not_persisted: int) -> int:
        return persisted + not_persisted

    _run([persisted, not_persisted, combined])
    assert (
        handler.load_input(
            rs.InputContext(asset_name="persisted", downstream_asset="t")
        )
        == 42
    )
    assert (
        handler.load_input(
            rs.InputContext(asset_name="not_persisted", downstream_asset="t")
        )
        == 7
    )
    assert (
        handler.load_input(rs.InputContext(asset_name="combined", downstream_asset="t"))
        == 49
    )


# ---------------------------------------------------------------------------
# 9. Per-asset executor override
# ---------------------------------------------------------------------------


def test_mp_override_in_process_with_context():
    """Asset with rivers/executor=in_process runs in-process, gets context."""

    @rs.Asset(metadata={"rivers/executor": "in_process"})
    def ip_ctx(context: rs.AssetExecutionContext) -> str:
        return f"ip:{context.asset_name}"

    @rs.Asset
    def parallel_step() -> int:
        return 42

    @rs.Asset
    def merge(ip_ctx: str, parallel_step: int) -> str:
        return f"{ip_ctx},{parallel_step}"

    repo = _run([ip_ctx, parallel_step, merge])
    assert repo.load_node("ip_ctx") == "ip:ip_ctx"
    assert repo.load_node("merge") == "ip:ip_ctx,42"


def test_mp_override_invalid_raises():
    """Invalid executor override raises ConfigurationError."""

    @rs.Asset(metadata={"rivers/executor": "bogus"})
    def bad() -> int:
        return 1

    repo = _job_repo([bad], executor=rs.Executor.in_process())  # type: ignore
    with pytest.raises(ConfigurationError, match="Unknown executor"):
        repo.get_job("j").execute()


# ---------------------------------------------------------------------------
# 10. Delta IO handler + parallel
# ---------------------------------------------------------------------------


def _delta_handler(tmp_path, **kwargs):
    return rs.DeltaIOHandler(table_uri=str(tmp_path), **kwargs)


def test_mp_delta_pyarrow_round_trip(tmp_path):
    """Delta IO handler writes/reads PyArrow table in parallel."""
    handler = _delta_handler(tmp_path)

    @rs.Asset(io_handler=handler)
    def delta_pa() -> pa.Table:
        return pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    @rs.Asset
    def delta_reader(delta_pa: pa.Table) -> list:
        return delta_pa.column("a").to_pylist()

    repo = _run([delta_pa, delta_reader])
    assert sorted(repo.load_node("delta_reader")) == [1, 2, 3]


def test_mp_delta_polars_round_trip(tmp_path):
    """Delta IO handler writes PyArrow, downstream reads as Polars DataFrame via IO load."""
    handler = _delta_handler(tmp_path)

    @rs.Asset(io_handler=handler)
    def delta_src() -> pa.Table:
        return pa.table({"x": [10, 20], "y": [1.1, 2.2]})

    _run([delta_src])
    # Load as Polars via IO handler directly
    ctx_in = rs.InputContext(
        asset_name="delta_src", downstream_asset="test", type_hint=pl.DataFrame
    )
    result = handler.load_input(ctx_in)
    assert isinstance(result, pl.DataFrame)
    assert result["x"].to_list() == [10, 20]


def test_mp_delta_parallel_writes(tmp_path):
    """Two parallel assets both write to Delta (different table subdirs)."""
    handler = _delta_handler(tmp_path)

    @rs.Asset(io_handler=handler)
    def delta_a() -> pa.Table:
        return pa.table({"id": [1], "val": [10]})

    @rs.Asset(io_handler=handler)
    def delta_b() -> pa.Table:
        return pa.table({"id": [2], "val": [20]})

    _run([delta_a, delta_b])
    # Parallel IO-handler assets don't store in result dict; verify via DeltaTable
    from deltalake import DeltaTable

    dt_a = DeltaTable(str(tmp_path / "delta_a"))
    dt_b = DeltaTable(str(tmp_path / "delta_b"))
    assert dt_a.to_pyarrow_table().column("val").to_pylist() == [10]
    assert dt_b.to_pyarrow_table().column("val").to_pylist() == [20]


def test_mp_delta_chain(tmp_path):
    """Chain: delta_src → delta_transform → delta_sink."""
    handler = _delta_handler(tmp_path)

    @rs.Asset(io_handler=handler)
    def dt_src() -> pa.Table:
        return pa.table({"val": [1, 2, 3]})

    @rs.Asset(io_handler=handler)
    def dt_transform(dt_src: pa.Table) -> pa.Table:
        df = pl.DataFrame(dt_src)
        doubled = df.with_columns((pl.col("val") * 2).alias("val"))
        return doubled.to_arrow()

    @rs.Asset
    def dt_sink(dt_transform: pa.Table) -> int:
        return sum(dt_transform.column("val").to_pylist())

    repo = _run([dt_src, dt_transform, dt_sink])
    assert repo.load_node("dt_sink") == 12  # (1+2+3)*2


def test_mp_delta_with_context(tmp_path):
    """Delta IO + context injection in parallel."""
    handler = _delta_handler(tmp_path)

    @rs.Asset(io_handler=handler)
    def delta_ctx(context: rs.AssetExecutionContext) -> pa.Table:
        context.add_output_metadata({"source": "test"})
        return pa.table({"name": [context.asset_name], "val": [42]})

    repo = _run([delta_ctx])
    output = repo.load_node("delta_ctx", type_hint=pa.Table)

    assert output.column("val").to_pylist() == [42]


def test_mp_delta_with_resource(tmp_path):
    """Delta IO + resource injection in parallel."""
    handler = _delta_handler(tmp_path)

    @rs.Asset(io_handler=handler)
    def delta_res(res: SimpleResource) -> pa.Table:
        return pa.table({"prefix": [res.prefix], "val": [1]})

    repo = _run([delta_res], resources={"res": SimpleResource(prefix="delta")})
    assert repo.load_node("delta_res", type_hint=pa.Table).column(
        "prefix"
    ).to_pylist() == ["delta"]


def test_mp_delta_append_mode(tmp_path):
    """Delta append mode accumulates rows across parallel executions."""
    handler = _delta_handler(tmp_path, mode="append")

    @rs.Asset(io_handler=handler)
    def append_tbl() -> pa.Table:
        return pa.table({"a": [1, 2]})

    # First execution
    _run([append_tbl])

    # Second execution
    _run([append_tbl])

    from deltalake import DeltaTable

    dt = DeltaTable(str(tmp_path / "append_tbl"))
    assert len(dt.to_pyarrow_table()) == 4


def test_mp_delta_overwrite_mode(tmp_path):
    """Delta overwrite mode replaces data."""
    handler = _delta_handler(tmp_path)

    @rs.Asset(io_handler=handler)
    def ow_tbl() -> pa.Table:
        return pa.table({"a": [1]})

    _run([ow_tbl])
    _run([ow_tbl])

    from deltalake import DeltaTable

    dt = DeltaTable(str(tmp_path / "ow_tbl"))
    assert len(dt.to_pyarrow_table()) == 1


def test_mp_delta_polars_write(tmp_path):
    """Writing a Polars DataFrame through Delta IO in parallel."""
    handler = _delta_handler(tmp_path)

    @rs.Asset(io_handler=handler)
    def pl_write() -> pl.DataFrame:
        return pl.DataFrame({"x": [10, 20, 30]})

    @rs.Asset
    def pl_read(pl_write: pl.DataFrame) -> int:
        return pl_write["x"].sum()  # type: ignore

    repo = _run([pl_write, pl_read])
    assert repo.load_node("pl_read") == 60


def test_mp_delta_diamond_dag(tmp_path):
    """Diamond DAG with Delta IO handler on all assets."""
    handler = _delta_handler(tmp_path)

    @rs.Asset(io_handler=handler)
    def d_top() -> pa.Table:
        return pa.table({"val": [10]})

    @rs.Asset(io_handler=handler)
    def d_left(d_top: pa.Table) -> pa.Table:
        val = d_top.column("val").to_pylist()[0]
        return pa.table({"val": [val + 1]})

    @rs.Asset(io_handler=handler)
    def d_right(d_top: pa.Table) -> pa.Table:
        val = d_top.column("val").to_pylist()[0]
        return pa.table({"val": [val + 2]})

    @rs.Asset(io_handler=handler)
    def d_bottom(d_left: pa.Table, d_right: pa.Table) -> pa.Table:
        l_val = d_left.column("val").to_pylist()[0]
        r_val = d_right.column("val").to_pylist()[0]
        return pa.table({"val": [l_val + r_val]})

    _run([d_top, d_left, d_right, d_bottom])
    from deltalake import DeltaTable

    dt = DeltaTable(str(tmp_path / "d_bottom"))
    assert dt.to_pyarrow_table().column("val").to_pylist() == [23]


def test_mp_delta_merge_upsert(tmp_path):
    """Delta merge upsert in parallel context."""
    from rivers.io_handlers.delta import MergeConfig

    # First create the table
    init_handler = _delta_handler(tmp_path)

    @rs.Asset(io_handler=init_handler)
    def merge_tbl() -> pa.Table:  # type: ignore
        return pa.table({"id": [1, 2], "val": [10, 20]})

    _run([merge_tbl])

    # Now upsert
    mc = MergeConfig(merge_type="upsert", predicate="s.id = t.id")
    merge_handler = rs.DeltaIOHandler(
        table_uri=str(tmp_path), mode="merge", merge_config=mc
    )

    @rs.Asset(io_handler=merge_handler)
    def merge_tbl() -> pa.Table:  # noqa: F811
        return pa.table({"id": [2, 3], "val": [99, 30]})

    _run([merge_tbl])

    from deltalake import DeltaTable

    dt = DeltaTable(str(tmp_path / "merge_tbl"))
    result = dt.to_pyarrow_table().sort_by("id")
    assert result.column("id").to_pylist() == [1, 2, 3]
    assert result.column("val").to_pylist() == [10, 99, 30]


# ---------------------------------------------------------------------------
# 11. Mixed configurations
# ---------------------------------------------------------------------------


def test_mp_mixed_context_and_no_context(tmp_path):
    """Some assets have context, others don't — all work in parallel."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def with_ctx(context: rs.AssetExecutionContext) -> str:
        return context.asset_name

    @rs.Asset(io_handler=handler)
    def without_ctx() -> int:
        return 42

    @rs.Asset(io_handler=handler)
    def combine(with_ctx: str, without_ctx: int) -> str:
        return f"{with_ctx}:{without_ctx}"

    repo = _run([with_ctx, without_ctx, combine])
    assert repo.load_node("combine") == "with_ctx:42"


def test_mp_context_resource_upstream_all():
    """Asset with context + resource + upstream dependency in parallel."""

    @rs.Asset
    def up() -> int:
        return 5

    @rs.Asset
    def full(context: rs.AssetExecutionContext, up: int, res: SimpleResource) -> str:
        return f"{context.asset_name}:{up}:{res.prefix}"

    repo = _run([up, full], resources={"res": SimpleResource(prefix="all")})
    assert repo.load_node("full") == "full:5:all"


def test_mp_all_types_of_return(tmp_path):
    """Various return types work through parallel pickle."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def ret_str() -> str:
        return "hello"

    @rs.Asset(io_handler=handler)
    def ret_list() -> list:
        return [1, 2, 3]

    @rs.Asset(io_handler=handler)
    def ret_dict() -> dict:
        return {"key": "val"}

    @rs.Asset(io_handler=handler)
    def ret_float() -> float:
        return 3.14

    @rs.Asset(io_handler=handler)
    def ret_none() -> None:
        return None

    @rs.Asset(io_handler=handler)
    def ret_bool() -> bool:
        return True

    _run([ret_str, ret_list, ret_dict, ret_float, ret_none, ret_bool])

    def load(name):
        return handler.load_input(
            rs.InputContext(asset_name=name, downstream_asset="t")
        )

    assert load("ret_str") == "hello"
    assert load("ret_list") == [1, 2, 3]
    assert load("ret_dict") == {"key": "val"}
    assert abs(load("ret_float") - 3.14) < 0.001
    assert load("ret_none") is None
    assert load("ret_bool") is True


def test_mp_nested_data_structures():
    """Complex nested data structures survive parallel pickle."""

    @rs.Asset
    def nested() -> dict:
        return {
            "users": [
                {"name": "Alice", "scores": [90, 85, 92]},
                {"name": "Bob", "scores": [75, 88, 91]},
            ],
            "metadata": {"version": 2, "tags": {"env": "test"}},
        }

    repo = _run([nested])
    output = repo.load_node("nested")
    assert output["users"][0]["name"] == "Alice"
    assert output["metadata"]["tags"]["env"] == "test"


# ---------------------------------------------------------------------------
# 12. Execute steps and collecting with parallel
# ---------------------------------------------------------------------------


def test_mp_execute_steps_subset(tmp_path):
    """execute_steps runs a subset of the DAG with parallel."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def es_a() -> int:
        return 1

    @rs.Asset(io_handler=handler)
    def es_b() -> int:
        return 2

    @rs.Asset(io_handler=handler)
    def es_c(es_a: int, es_b: int) -> int:
        return es_a + es_b

    assets = [es_a, es_b, es_c]
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
    repo.get_job("j").execute()
    assert repo.load_node("es_c") == 3


def test_mp_collecting_all_succeed(tmp_path):
    """Collecting mode with all successes returns complete results."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def ok_a() -> int:
        return 1

    @rs.Asset(io_handler=handler)
    def ok_b() -> int:
        return 2

    @rs.Asset(io_handler=handler)
    def ok_c(ok_a: int, ok_b: int) -> int:
        return ok_a + ok_b

    assets = [ok_a, ok_b, ok_c]
    repo = rs.CodeRepository(assets=assets, default_executor=MP)
    result = repo.materialize(raise_on_error=False)

    def load(name):
        return handler.load_input(
            rs.InputContext(asset_name=name, downstream_asset="t")
        )

    assert load("ok_a") == 1
    assert load("ok_b") == 2
    assert load("ok_c") == 3
    assert len(result.failed_assets) == 0


def test_mp_collecting_partial_parallel_failure(tmp_path):
    """In parallel level, one failure doesn't block independent siblings."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def par_ok() -> int:
        return 42

    @rs.Asset(io_handler=handler)
    def par_fail() -> int:
        raise ValueError("nope")

    assets = [par_ok, par_fail]
    repo = rs.CodeRepository(assets=assets, default_executor=MP)
    result = repo.materialize(raise_on_error=False)
    assert (
        handler.load_input(rs.InputContext(asset_name="par_ok", downstream_asset="t"))
        == 42
    )
    failure_names = [name for name, _ in result.failed_assets]
    assert "par_fail" in failure_names


# ---------------------------------------------------------------------------
# 13. Edge cases
# ---------------------------------------------------------------------------


def test_mp_asset_returns_empty_list():
    """Asset returning empty list works through parallel."""

    @rs.Asset
    def empty() -> list:
        return []

    @rs.Asset
    def consumer(empty: list) -> int:
        return len(empty)

    repo = _run([empty, consumer])
    assert repo.load_node("consumer") == 0


def test_mp_asset_returns_large_string():
    """Large string result survives parallel pickle."""

    @rs.Asset
    def big() -> str:
        return "x" * 100_000

    repo = _run([big])
    assert len(repo.load_node("big")) == 100_000


def test_mp_six_parallel_assets(tmp_path):
    """Six independent assets run in parallel."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def p_1() -> int:
        return 1

    @rs.Asset(io_handler=handler)
    def p_2() -> int:
        return 2

    @rs.Asset(io_handler=handler)
    def p_3() -> int:
        return 3

    @rs.Asset(io_handler=handler)
    def p_4() -> int:
        return 4

    @rs.Asset(io_handler=handler)
    def p_5() -> int:
        return 5

    @rs.Asset(io_handler=handler)
    def p_6() -> int:
        return 6

    _run([p_1, p_2, p_3, p_4, p_5, p_6])

    def load(name):
        return handler.load_input(
            rs.InputContext(asset_name=name, downstream_asset="t")
        )

    assert sum(load(f"p_{i}") for i in range(1, 7)) == 21


# ---------------------------------------------------------------------------
# 14. InMemoryIOHandler validation
# ---------------------------------------------------------------------------


def test_mp_in_memory_io_rejected_for_parallel():
    """InMemoryIOHandler raises error when used with parallel parallel steps."""
    handler = rs.InMemoryIOHandler()

    @rs.Asset(io_handler=handler)
    def mem_a() -> int:
        return 1

    @rs.Asset(io_handler=handler)
    def mem_b() -> int:
        return 2

    with pytest.raises(Exception, match="InMemoryIOHandler"):
        _run([mem_a, mem_b])


def test_mp_in_memory_io_single_step_ok():
    """InMemoryIOHandler is fine for single-step levels (run in-process)."""
    handler = rs.InMemoryIOHandler()

    @rs.Asset(io_handler=handler)
    def solo_mem() -> int:
        return 42

    repo = _run([solo_mem])
    assert repo.load_node("solo_mem") == 42


def test_mp_in_memory_io_rejected_in_collecting():
    """InMemoryIOHandler is rejected even in collecting mode."""
    handler = rs.InMemoryIOHandler()

    @rs.Asset(io_handler=handler)
    def cm_a() -> int:
        return 1

    @rs.Asset(io_handler=handler)
    def cm_b() -> int:
        return 2

    assets = [cm_a, cm_b]
    repo = rs.CodeRepository(assets=assets, default_executor=MP)
    result = repo.materialize(raise_on_error=False)
    assert len(result.failed_assets) > 0
    assert any("InMemoryIOHandler" in msg for _, msg in result.failed_assets)


# ---------------------------------------------------------------------------
# 15. IO load spec — upstream loaded in subprocess via IO handler
# ---------------------------------------------------------------------------


def test_mp_io_load_spec_pickle_handler(tmp_path):
    """Upstream with PickleIOHandler is loaded via _IOLoadSpec in subprocess."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def io_up() -> list:
        return [100, 200, 300]

    @rs.Asset(io_handler=handler)
    def io_down(io_up: list) -> int:
        return sum(io_up)

    _run([io_up, io_down])
    ctx = rs.InputContext(asset_name="io_down", downstream_asset="test")
    assert handler.load_input(ctx) == 600


def test_mp_io_load_spec_delta_handler(tmp_path):
    """Upstream with DeltaIOHandler is loaded via _IOLoadSpec in subprocess."""
    handler = _delta_handler(tmp_path)

    @rs.Asset(io_handler=handler)
    def delta_up() -> pa.Table:
        return pa.table({"val": [10, 20, 30]})

    @rs.Asset
    def delta_down(delta_up: pa.Table) -> int:
        return sum(delta_up.column("val").to_pylist())

    repo = _run([delta_up, delta_down])
    assert repo.load_node("delta_down") == 60


def test_mp_io_load_spec_mixed_upstream(tmp_path):
    """All parallel assets need a non-InMemory IO handler for parallel."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def with_io() -> int:
        return 100

    @rs.Asset(io_handler=handler)
    def without_io() -> int:
        return 5

    @rs.Asset(io_handler=handler)
    def merger(with_io: int, without_io: int) -> int:
        return with_io + without_io

    repo = _run([with_io, without_io, merger])
    assert repo.load_node("merger") == 105


def test_mp_io_load_spec_parallel_both_io(tmp_path):
    """Two parallel IO-handler assets feed into a downstream that loads both via spec."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def par_a() -> int:
        return 10

    @rs.Asset(io_handler=handler)
    def par_b() -> int:
        return 20

    @rs.Asset
    def par_merge(par_a: int, par_b: int) -> int:
        return par_a + par_b

    repo = _run([par_a, par_b, par_merge])
    assert repo.load_node("par_merge") == 30


def test_mp_dynamic_keys_round_trip_through_kv(tmp_path):
    """Fan-out source emits DynamicOutputs in a loky worker subprocess.
    The worker doesn't write KV (embedded RocksDB is single-process); it
    surfaces the keys back on `WorkerResult.dynamic_keys`. The orchestrator
    persists them to KV scoped by the materialization's data_version, and
    the downstream fan-out resolver reads them back from KV — so mapped
    instances are written under the user-supplied DynamicOutput keys
    instead of synthetic numeric indices."""
    import os

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Task
    def mp_double(x: int) -> int:
        return x * 2

    @rs.Task
    def mp_total(values: list) -> int:
        return sum(values)

    @rs.Asset(io_handler=handler)
    def mp_keysrc() -> list:
        return [
            rs.DynamicOutput(key="alpha", value=10),
            rs.DynamicOutput(key="beta", value=20),
        ]

    @rs.Asset.from_graph(io_handler=handler, node_io_handler=handler)
    def mp_fan_pipeline():
        s = mp_keysrc()
        mapped = s.map(mp_double)
        return mp_total(mapped.collect())

    assets = [mp_keysrc, mp_fan_pipeline]
    repo = rs.CodeRepository(
        assets=assets,
        tasks=[mp_double, mp_total],
        jobs=[rs.Job(name="j", assets=assets, executor=MP)],
    )
    repo.get_job("j").execute()

    # (10 + 20) × 2
    assert (
        handler.load_input(
            rs.InputContext(asset_name="mp_fan_pipeline", downstream_asset="t")
        )
        == 60
    )

    # Mapped instance files live under the user-supplied DynamicOutput keys —
    # confirmation that the downstream fan-out used the keys from KV (not
    # synthetic numeric indices). Without the KV round-trip, these files
    # would be `mp_double__0.pkl` / `mp_double__1.pkl`.
    store_root = handler.store.prefix
    assert os.path.exists(
        os.path.join(store_root, "mp_fan_pipeline", "mp_double__alpha.pkl")
    )
    assert os.path.exists(
        os.path.join(store_root, "mp_fan_pipeline", "mp_double__beta.pkl")
    )

    # And the user IO handler must NOT see a `__keys` write — that's now
    # rivers-internal KV, not user IO.
    with pytest.raises(Exception):
        handler.load_input(
            rs.InputContext(asset_name="mp_keysrc__keys", downstream_asset="t")
        )
