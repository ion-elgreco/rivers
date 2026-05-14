"""Concurrency Pools — Executor Integration tests.

Tests that pool-aware execution claims slots before step execution, releases
after completion, respects pool limits across concurrent steps, and performs
run-level cleanup. Covers InProcess, Parallel, and Async executor paths.
"""

import asyncio
import time

import obstore.store
import pytest
import rivers as rs

from _helpers import make_repo


# ═══════════════════════════════════════════════════════════════════════════
# Executor-parametrized: behaviors that must hold across executor paths
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    "executor_factory,is_async",
    [
        pytest.param(rs.Executor.in_process, False, id="in_process_sync"),
        pytest.param(rs.Executor.in_process, True, id="in_process_async"),
        pytest.param(rs.Executor.parallel, False, id="parallel_sync"),
    ],
)
def test_pool_step_failure_releases_slots(storage, executor_factory, is_async):
    """A failed pool-limited step still releases its slot."""

    if is_async:

        @rs.Asset(pool="api", pool_slots=1, io_handler=rs.InMemoryIOHandler())
        async def fail():
            await asyncio.sleep(0.01)
            raise ValueError("boom")
    else:

        @rs.Asset(pool="api", pool_slots=1, io_handler=rs.InMemoryIOHandler())
        def fail():
            raise ValueError("boom")

    storage.set_pool_limit("api", 1)
    repo = make_repo([fail], storage, executor=executor_factory())
    result = repo.materialize(raise_on_error=False)

    assert not result.success
    assert storage.get_pool_info("api").claimed_count == 0


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_multi_pool_step_atomic(storage, is_async):
    """Steps requiring multiple pools claim all-or-none and release on completion."""

    if is_async:

        @rs.Asset(
            pool=["db", "api"],
            pool_slots={"db": 1, "api": 1},
            io_handler=rs.InMemoryIOHandler(),
        )
        async def multi_pool_step():
            await asyncio.sleep(0.01)
            return 42
    else:

        @rs.Asset(
            pool=["db", "api"],
            pool_slots={"db": 1, "api": 1},
            io_handler=rs.InMemoryIOHandler(),
        )
        def multi_pool_step():
            return 42

    storage.set_pool_limit("db", 5)
    storage.set_pool_limit("api", 5)
    repo = make_repo([multi_pool_step], storage)
    repo.materialize()

    assert storage.get_pool_info("db").claimed_count == 0
    assert storage.get_pool_info("api").claimed_count == 0


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_no_pool_step_unaffected(storage, is_async):
    """A step without pool config runs to completion with no slot bookkeeping."""

    if is_async:

        @rs.Asset(io_handler=rs.InMemoryIOHandler())
        async def free():
            await asyncio.sleep(0.01)
            return 7
    else:

        @rs.Asset(io_handler=rs.InMemoryIOHandler())
        def free():
            return 7

    repo = make_repo([free], storage)
    repo.materialize()
    assert repo.load_node("free") == 7


@pytest.mark.parametrize(
    "executor_factory",
    [
        pytest.param(rs.Executor.in_process, id="in_process"),
        pytest.param(rs.Executor.parallel, id="parallel"),
    ],
)
def test_mixed_sync_async_on_shared_pool(storage, tmp_path, executor_factory):
    """Sync + async steps sharing one pool both claim and release correctly."""
    # Sync uses Pickle so the same body works under both executors — the
    # parallel path rejects InMemoryIOHandler on sync assets.
    sync_store = obstore.store.LocalStore(str(tmp_path / "io"), mkdir=True)

    @rs.Asset(pool="db", pool_slots=1, io_handler=rs.PickleIOHandler(store=sync_store))
    def sync_pool():
        return 1

    @rs.Asset(pool="db", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    async def async_pool():
        await asyncio.sleep(0.01)
        return 2

    storage.set_pool_limit("db", 2)
    repo = make_repo([sync_pool, async_pool], storage, executor=executor_factory())
    result = repo.materialize()

    assert result.success
    assert storage.get_pool_info("db").claimed_count == 0


# ═══════════════════════════════════════════════════════════════════════════
# InProcess executor
# ═══════════════════════════════════════════════════════════════════════════


def test_inprocess_unconfigured_pool_auto_registers(storage):
    """Pools referenced by assets are auto-registered during resolve as unlimited (-1)."""

    @rs.Asset(pool="auto_created", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    def auto_pool():
        return 1

    repo = make_repo([auto_pool], storage)
    result = repo.materialize(raise_on_error=False)

    assert result.success
    pool_info = storage.get_pool_info("auto_created")
    assert pool_info.slot_limit == -1  # unlimited
    assert pool_info.lease_duration_secs == 300


def test_pool_limits_on_repo_overrides_default(storage):
    """Explicit pool_limits on CodeRepository override the -1 default."""

    @rs.Asset(pool="gpu", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    def gpu_step():
        return 1

    repo = rs.CodeRepository(
        assets=[gpu_step],
        default_executor=rs.Executor.in_process(),
        pool_limits={"gpu": 4},
    )
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=False)

    assert result.success
    pool_info = storage.get_pool_info("gpu")
    assert pool_info.slot_limit == 4


def test_pool_limits_mixed_explicit_and_auto(storage):
    """Explicit limits apply to declared pools; undeclared asset pools get -1."""

    @rs.Asset(pool="db", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    def db_step():
        return 1

    @rs.Asset(pool="api", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    def api_step():
        return 2

    repo = rs.CodeRepository(
        assets=[db_step, api_step],
        default_executor=rs.Executor.in_process(),
        pool_limits={"db": 5},
    )
    repo.resolve(storage=storage)
    repo.materialize(raise_on_error=False)

    db_info = storage.get_pool_info("db")
    assert db_info.slot_limit == 5

    api_info = storage.get_pool_info("api")
    assert api_info.slot_limit == -1  # auto-registered as unlimited


def test_pool_limits_enforced_during_execution(storage):
    """Pool with explicit limit=1 blocks concurrent claims."""
    execution_order = []

    @rs.Asset(pool="serial", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    def step_a():
        execution_order.append("a")
        return 1

    @rs.Asset(pool="serial", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    def step_b():
        execution_order.append("b")
        return 2

    repo = rs.CodeRepository(
        assets=[step_a, step_b],
        default_executor=rs.Executor.in_process(),
        pool_limits={"serial": 1},
    )
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=False)

    assert result.success
    assert set(execution_order) == {"a", "b"}

    info = storage.get_pool_info("serial")
    assert info.slot_limit == 1
    assert info.claimed_count == 0  # all released after execution


def test_unlimited_pool_skips_claims(storage):
    """Assets in unlimited pools (-1) execute without creating slot records."""

    @rs.Asset(pool="unlimited_pool", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    def free_step():
        return 42

    repo = make_repo([free_step], storage)
    result = repo.materialize(raise_on_error=False)

    assert result.success
    info = storage.get_pool_info("unlimited_pool")
    assert info.slot_limit == -1
    assert info.claimed_count == 0  # no slots claimed for unlimited pools


# ═══════════════════════════════════════════════════════════════════════════
# Async assets (routed through AsyncBackend JoinSet)
# ═══════════════════════════════════════════════════════════════════════════


def test_async_pool_claims_and_releases(storage):
    """Async assets with pool config claim/release via AsyncBackend."""

    @rs.Asset(pool="api", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    async def async_step_a():
        await asyncio.sleep(0.01)
        return 10

    @rs.Asset(pool="api", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    async def async_step_b():
        await asyncio.sleep(0.01)
        return 20

    storage.set_pool_limit("api", 2)
    repo = make_repo([async_step_a, async_step_b], storage)
    repo.materialize()

    assert repo.load_node("async_step_a") == 10
    assert repo.load_node("async_step_b") == 20

    info = storage.get_pool_info("api")
    assert info.claimed_count == 0


# ═══════════════════════════════════════════════════════════════════════════
# Parallel executor
# ═══════════════════════════════════════════════════════════════════════════


def test_parallel_pool_steps_execute_and_release(storage, tmp_path):
    """Pool-limited steps under Parallel executor claim/release correctly.
    Pool steps are routed to InProcess (sequential) to avoid deadlocks."""
    store = obstore.store.LocalStore(str(tmp_path / "io"), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(pool="db", pool_slots=1, io_handler=handler)
    def par_pool_a():
        return 100

    @rs.Asset(pool="db", pool_slots=1, io_handler=handler)
    def par_pool_b():
        return 200

    storage.set_pool_limit("db", 1)
    repo = make_repo(
        [par_pool_a, par_pool_b],
        storage,
        executor=rs.Executor.parallel(max_workers=2),
    )
    repo.materialize()

    info = storage.get_pool_info("db")
    assert info.claimed_count == 0


def test_parallel_mixed_pool_and_nonpool(storage, tmp_path):
    """Parallel executor: non-pool steps go to loky, pool steps to InProcess."""
    store = obstore.store.LocalStore(str(tmp_path / "io"), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def par_free_a():
        return 1

    @rs.Asset(io_handler=handler)
    def par_free_b():
        return 2

    @rs.Asset(pool="db", pool_slots=1, io_handler=handler)
    def par_pool():
        return 3

    storage.set_pool_limit("db", 1)
    repo = make_repo(
        [par_free_a, par_free_b, par_pool],
        storage,
        executor=rs.Executor.parallel(max_workers=2),
    )
    repo.materialize()

    info = storage.get_pool_info("db")
    assert info.claimed_count == 0


def test_parallel_async_pool_steps(storage, tmp_path):
    """Async steps with pools under Parallel executor claim/release correctly.
    Parallel routes async steps to AsyncBackend."""

    @rs.Asset(pool="api", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    async def par_async_a():
        await asyncio.sleep(0.01)
        return 10

    @rs.Asset(pool="api", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    async def par_async_b():
        await asyncio.sleep(0.01)
        return 20

    storage.set_pool_limit("api", 2)
    repo = make_repo(
        [par_async_a, par_async_b],
        storage,
        executor=rs.Executor.parallel(max_workers=2),
    )
    repo.materialize()

    assert repo.load_node("par_async_a") == 10
    assert repo.load_node("par_async_b") == 20

    info = storage.get_pool_info("api")
    assert info.claimed_count == 0


# ═══════════════════════════════════════════════════════════════════════════
# Dependency chains with pools
# ═══════════════════════════════════════════════════════════════════════════


def test_chain_with_pool_limits(storage):
    """A→B→C chain where all steps share a pool(limit=1). Each claims/releases
    in sequence so downstream can claim after upstream releases."""

    @rs.Asset(pool="serial", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    def chain_a():
        return 10

    @rs.Asset(pool="serial", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    def chain_b(chain_a: int):
        return chain_a + 5

    @rs.Asset(pool="serial", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    def chain_c(chain_b: int):
        return chain_b * 2

    storage.set_pool_limit("serial", 1)
    repo = make_repo([chain_a, chain_b, chain_c], storage)
    repo.materialize()

    assert repo.load_node("chain_a") == 10
    assert repo.load_node("chain_b") == 15
    assert repo.load_node("chain_c") == 30

    info = storage.get_pool_info("serial")
    assert info.claimed_count == 0


def test_diamond_with_pools(storage):
    """Diamond DAG: root→(left, right)→merge. Left and right share pool(limit=1),
    so they execute one at a time."""

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def d_root():
        return 10

    @rs.Asset(pool="narrow", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    def d_left(d_root: int):
        return d_root + 1

    @rs.Asset(pool="narrow", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    def d_right(d_root: int):
        return d_root + 2

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def d_merge(d_left: int, d_right: int):
        return d_left + d_right

    storage.set_pool_limit("narrow", 1)
    repo = make_repo([d_root, d_left, d_right, d_merge], storage)
    repo.materialize()

    assert repo.load_node("d_merge") == 23

    info = storage.get_pool_info("narrow")
    assert info.claimed_count == 0


# ═══════════════════════════════════════════════════════════════════════════
# Run-level cleanup and mixed scenarios
# ═══════════════════════════════════════════════════════════════════════════


def test_run_level_cleanup(storage):
    """After execution, free_concurrency_slots_for_run cleans up all slots."""

    @rs.Asset(pool="db", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    def step_a():
        return 1

    @rs.Asset(pool="db", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    def step_b():
        return 2

    storage.set_pool_limit("db", 2)
    repo = make_repo([step_a, step_b], storage)
    repo.materialize()

    info = storage.get_pool_info("db")
    assert info.claimed_count == 0


def test_mixed_pool_and_nonpool_steps(storage):
    """Steps with and without pools execute correctly in the same job."""
    results = {}

    @rs.Asset(pool="db", pool_slots=1, io_handler=rs.InMemoryIOHandler())
    def pool_step():
        results["pool"] = True
        return 1

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def free_step():
        results["free"] = True
        return 2

    storage.set_pool_limit("db", 1)
    repo = make_repo([pool_step, free_step], storage)
    repo.materialize()

    assert results == {"pool": True, "free": True}


# ═══════════════════════════════════════════════════════════════════════════
# Coordinator GC and lease renewal
# ═══════════════════════════════════════════════════════════════════════════


def test_coordinator_gc_frees_expired(storage):
    """The expired lease GC removes stale rows."""
    storage.set_pool_limit("db", 2)

    storage._claim_concurrency_slots([("db", 1)], "run1", "step_a", lease_duration="1s")

    time.sleep(1.5)

    freed = storage._free_expired_leases()
    assert freed == 1

    info = storage.get_pool_info("db")
    assert info.claimed_count == 0


def test_lease_renewal_keeps_slot_alive(storage):
    """Renewing a lease extends it, keeping the slot active."""
    storage.set_pool_limit("db", 1)
    storage._claim_concurrency_slots([("db", 1)], "run1", "step_a", lease_duration="2s")

    renewed = storage._renew_slot_lease("run1", "step_a", lease_duration="2s")
    assert renewed == 1

    info = storage.get_pool_info("db")
    assert info.claimed_count == 1
