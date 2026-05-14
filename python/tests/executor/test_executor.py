import rivers as rs
from rivers.exceptions import ConfigurationError


def test_executor_in_process():
    executor = rs.Executor.in_process()
    assert repr(executor) == "Executor.InProcess()"


def test_executor_parallel():
    executor = rs.Executor.parallel(max_workers=2)
    assert repr(executor) == "Executor.Parallel(max_workers=2)"


def test_executor_parallel_default_workers():
    executor = rs.Executor.parallel()
    r = repr(executor)
    assert r.startswith("Executor.Parallel(max_workers=")
    assert r.endswith(")")


@rs.Asset
def mp_source() -> list:
    return [1, 2, 3]


@rs.Asset
def mp_doubled(mp_source: list) -> list:
    return [x * 2 for x in mp_source]


@rs.Asset
def mp_summed(mp_doubled: list) -> int:
    return sum(mp_doubled)


def test_job_execute_parallel_chain():
    """Execute a chain of assets with Parallel executor."""
    assets = [mp_source, mp_doubled, mp_summed]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[
            rs.Job(
                name="mp_chain",
                assets=assets,
                executor=rs.Executor.parallel(max_workers=2),
            )
        ],
    )
    repo.get_job("mp_chain").execute()

    assert repo.load_node("mp_source") == [1, 2, 3]
    assert repo.load_node("mp_doubled") == [2, 4, 6]
    assert repo.load_node("mp_summed") == 12


def test_per_asset_executor_override_in_process():
    """Asset with rivers/executor=in_process runs in-process in a parallel job,
    allowing AssetExecutionContext which isn't supported in subprocess."""

    @rs.Asset(metadata={"rivers/executor": "in_process"})
    def with_context(context: rs.AssetExecutionContext) -> str:
        return f"name={context.asset_name}"

    @rs.Asset
    def no_override() -> int:
        return 42

    @rs.Asset
    def downstream(with_context: str, no_override: int) -> str:
        return f"{with_context},{no_override}"

    assets = [with_context, no_override, downstream]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[
            rs.Job(
                name="test",
                assets=assets,
                executor=rs.Executor.parallel(max_workers=2),
            )
        ],
    )
    repo.get_job("test").execute()

    assert repo.load_node("with_context") == "name=with_context"
    assert repo.load_node("no_override") == 42
    assert repo.load_node("downstream") == "name=with_context,42"


def test_mixed_overrides_preserve_parallelism(tmp_path):
    """Assets with rivers/executor overrides coexist with regular assets
    under parallel execution."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def a() -> int:
        return 1

    @rs.Asset(io_handler=handler)
    def b() -> int:
        return 2

    @rs.Asset(metadata={"rivers/executor": "in_process"}, io_handler=handler)
    def c() -> int:
        return 3

    @rs.Asset(io_handler=handler)
    def merge(a: int, b: int, c: int) -> int:
        return a + b + c

    assets = [a, b, c, merge]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[
            rs.Job(
                name="test",
                assets=assets,
                executor=rs.Executor.parallel(max_workers=2),
            )
        ],
    )
    repo.get_job("test").execute()

    def load(name):
        return handler.load_input(
            rs.InputContext(asset_name=name, downstream_asset="t")
        )

    assert load("a") == 1
    assert load("b") == 2
    assert load("c") == 3
    assert load("merge") == 6


def test_invalid_executor_override_raises():
    """Unknown executor string in rivers/executor raises ValueError."""
    import pytest

    @rs.Asset(metadata={"rivers/executor": "unknown_executor"})
    def bad() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[bad],
        jobs=[rs.Job(name="test", assets=[bad], executor=rs.Executor.in_process())],
    )
    with pytest.raises(ConfigurationError, match="Unknown executor"):
        repo.get_job("test").execute()


def test_no_overrides_uses_fast_path():
    """Jobs without rivers/executor overrides still work via the standard executor path."""

    @rs.Asset
    def x() -> int:
        return 10

    @rs.Asset
    def y(x: int) -> int:
        return x * 2

    assets = [x, y]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[
            rs.Job(
                name="test",
                assets=assets,
                executor=rs.Executor.parallel(max_workers=2),
            )
        ],
    )
    repo.get_job("test").execute()

    assert repo.load_node("x") == 10
    assert repo.load_node("y") == 20


def test_parallel_context_injection(tmp_path):
    """AssetExecutionContext is supported in parallel execution."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def ctx_a(context: rs.AssetExecutionContext) -> str:
        context.add_output_metadata({"source": "ctx_a"})
        return f"name={context.asset_name}"

    @rs.Asset(io_handler=handler)
    def ctx_b(context: rs.AssetExecutionContext) -> str:
        return f"name={context.asset_name}"

    @rs.Asset(io_handler=handler)
    def ctx_merge(ctx_a: str, ctx_b: str) -> str:
        return f"{ctx_a},{ctx_b}"

    assets = [ctx_a, ctx_b, ctx_merge]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[
            rs.Job(
                name="test_ctx",
                assets=assets,
                executor=rs.Executor.parallel(max_workers=2),
            )
        ],
    )
    repo.get_job("test_ctx").execute()

    def load(name):
        return handler.load_input(
            rs.InputContext(asset_name=name, downstream_asset="t")
        )

    assert load("ctx_a") == "name=ctx_a"
    assert load("ctx_b") == "name=ctx_b"
    assert load("ctx_merge") == "name=ctx_a,name=ctx_b"


def test_parallel_data_version(tmp_path):
    """data_version registered in context propagates correctly under parallel."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def dv_a(context: rs.AssetExecutionContext) -> int:
        context.register_data_version("v1.0")
        return 42

    @rs.Asset(io_handler=handler)
    def dv_b(context: rs.AssetExecutionContext) -> int:
        context.register_data_version("v2.0")
        return 99

    assets = [dv_a, dv_b]
    repo = rs.CodeRepository(
        assets=assets,
        jobs=[
            rs.Job(
                name="test_dv",
                assets=assets,
                executor=rs.Executor.parallel(max_workers=2),
            )
        ],
    )
    repo.get_job("test_dv").execute()

    def load(name):
        return handler.load_input(
            rs.InputContext(asset_name=name, downstream_asset="t")
        )

    assert load("dv_a") == 42
    assert load("dv_b") == 99


# ---------------------------------------------------------------------------
# Regression: executor overrides work via materialize (not only Job)
# ---------------------------------------------------------------------------


def test_executor_override_via_materialize():
    """rivers/executor=in_process override is respected through materialize().
    Proof: the overridden asset uses InMemoryIOHandler (no io_handler set),
    which parallel rejects. If materialize succeeds, the override routed
    it to in_process. Previously, materialize() ignored per-asset overrides."""

    @rs.Asset(metadata={"rivers/executor": "in_process"})
    def in_process_only() -> int:
        return 42

    # This asset has no io_handler (defaults to InMemoryIOHandler).
    # parallel would reject it with "uses InMemoryIOHandler which cannot
    # work with parallel". The in_process override must be respected.
    repo = rs.CodeRepository(
        assets=[in_process_only],
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    result = repo.materialize()

    assert repo.load_node("in_process_only") == 42
    assert result.success


def test_invalid_executor_override_via_materialize():
    """Invalid rivers/executor metadata raises ConfigurationError through materialize()."""
    import pytest

    @rs.Asset(metadata={"rivers/executor": "nonexistent"})
    def bad() -> int:
        return 1

    repo = rs.CodeRepository(assets=[bad])
    with pytest.raises(ConfigurationError, match="Unknown executor"):
        repo.materialize()


# ---------------------------------------------------------------------------
# Regression: multi-asset with parallel siblings under parallel
# ---------------------------------------------------------------------------


def test_multi_asset_parallel_with_siblings_parallel(tmp_path):
    """Multi-asset step runs correctly alongside parallel single-output siblings
    under parallel executor. Previously the parallel path didn't handle
    multi-asset in the parallel batch (only in the single-step fallback)."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    # All at level 0 (no inter-dependencies) → parallel batches them.
    # run_batch routes: multi-asset → in_process (can't subprocess a
    # single function with multiple outputs), siblings → loky futures.
    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("x", io_handler=handler),
            rs.AssetDef("y", io_handler=handler),
        ],
    )
    def producer() -> dict:
        return {"x": 10, "y": 20}

    @rs.Asset(io_handler=handler)
    def sibling_a() -> int:
        return 100

    @rs.Asset(io_handler=handler)
    def sibling_b() -> int:
        return 200

    repo = rs.CodeRepository(
        assets=[producer, sibling_a, sibling_b],
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    result = repo.materialize(raise_on_error=False)

    def load(name):
        return handler.load_input(
            rs.InputContext(asset_name=name, downstream_asset="t")
        )

    assert load("x") == 10
    assert load("y") == 20
    assert load("sibling_a") == 100
    assert load("sibling_b") == 200
    assert len(result.failed_assets) == 0


def test_multi_asset_failure_with_parallel_siblings_parallel(tmp_path):
    """Multi-asset failure in a parallel batch doesn't crash and independent
    siblings still succeed."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("a", io_handler=handler),
            rs.AssetDef("b", io_handler=handler),
        ],
    )
    def failing_multi() -> dict:
        raise ValueError("multi-fail")

    @rs.Asset(io_handler=handler)
    def independent() -> int:
        return 99

    repo = rs.CodeRepository(
        assets=[failing_multi, independent],
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    result = repo.materialize(raise_on_error=False)

    def load(name):
        return handler.load_input(
            rs.InputContext(asset_name=name, downstream_asset="t")
        )

    assert load("independent") == 99
    failure_names = [name for name, _ in result.failed_assets]
    assert "a" in failure_names or "failing_multi" in failure_names


# ---------------------------------------------------------------------------
# Regression: cross-group data_versions with executor overrides
# ---------------------------------------------------------------------------


def test_data_versions_shared_across_executor_groups(storage):
    """When assets at the same level use different executors (via overrides),
    data_versions must be shared across groups so the downstream's materialization
    records input_data_versions from both groups. Previously each executor group
    had its own data_versions map, so the downstream would miss versions from
    the other group."""

    @rs.Asset(
        metadata={"rivers/executor": "in_process"}, io_handler=rs.InMemoryIOHandler()
    )
    def from_in_process() -> int:
        return 10

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def from_default() -> int:
        return 20

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def downstream(from_in_process: int, from_default: int) -> int:
        return from_in_process + from_default

    repo = rs.CodeRepository(
        assets=[from_in_process, from_default, downstream],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    _result = repo.materialize()

    assert repo.load_node("downstream") == 30

    # Verify the downstream's materialization recorded input_data_versions
    # from BOTH upstream assets (which ran in different executor groups).
    ds_record = storage.get_asset_record("downstream")
    assert ds_record is not None
    input_names = {name for name, _ in ds_record.last_input_data_versions}
    assert input_names == {"from_in_process", "from_default"}

    # Each input version should match the upstream's recorded data_version.
    for upstream_name, version in ds_record.last_input_data_versions:
        upstream_record = storage.get_asset_record(upstream_name)
        assert upstream_record is not None
        assert version == upstream_record.last_data_version


# ---------------------------------------------------------------------------
# Graph assets under Parallel executor
# ---------------------------------------------------------------------------


def test_basic_graph_asset_parallel(tmp_path):
    """Basic graph asset (non-fan-out) under Parallel executor."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Task
    def gp_step_one() -> int:
        return 10

    @rs.Task
    def gp_step_two(gp_step_one: int) -> int:
        return gp_step_one * 3

    @rs.Asset.from_graph(name="gp_pipe", io_handler=handler, node_io_handler=handler)
    def gp_pipe():
        a = gp_step_one()
        gp_step_two(a)

    assets = [gp_pipe]
    repo = rs.CodeRepository(
        assets=assets,
        tasks=[gp_step_one, gp_step_two],
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

    assert load("gp_pipe/gp_step_one") == 10
    assert load("gp_pipe/gp_step_two") == 30


# ---------------------------------------------------------------------------
# Cross-executor parity: diamond DAG
# ---------------------------------------------------------------------------


def test_cross_executor_parity_diamond(tmp_path):
    """Diamond DAG produces identical results under InProcess and Parallel."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    def build_and_run(executor, io):
        @rs.Asset(io_handler=io)
        def par_root() -> int:
            return 10

        @rs.Asset(io_handler=io)
        def par_left(par_root: int) -> int:
            return par_root + 1

        @rs.Asset(io_handler=io)
        def par_right(par_root: int) -> int:
            return par_root + 2

        @rs.Asset(io_handler=io)
        def par_merge(par_left: int, par_right: int) -> int:
            return par_left + par_right

        repo = rs.CodeRepository(
            assets=[par_root, par_left, par_right, par_merge],
            default_executor=executor,
        )
        repo.materialize()
        return repo.load_node("par_merge")

    ip_result = build_and_run(rs.Executor.in_process(), None)
    par_result = build_and_run(rs.Executor.parallel(max_workers=2), handler)
    assert ip_result == par_result == 23
