"""rivers side of the comparison benchmarks. Runs in the project `.venv`.

This module deliberately omits ``from __future__ import annotations``. Under
PEP 563 every annotation becomes a string, and rivers then rejects a correctly
typed asset with "returned value of type 'int' but expected 'int'".
"""

import argparse
import contextlib
import os
import shutil
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from bench.harness import (
    LOG_LINE,
    PARALLEL_SLEEP_S,
    PARALLEL_WORKERS,
    QUEUE_SLOTS,
    Budget,
    claim_verdict,
    cold_start_benchmark,
    counted_window,
    main,
    log_verdict,
    parallel_verdict,
    pass_verdict,
    query_summary,
    quiet_env,
    rss_mb,
    sample,
    sensor_interval,
    summarize,
    throughput,
)

# Also handed to the cold-start child, which does not inherit this process.
QUIET_ENV = {"RUST_LOG": "off"}
quiet_env(QUIET_ENV)

# Import the framework only after quiet_env(), so its logging setup reads
# the silenced configuration and cannot distort the measurement.
import rivers as rs  # noqa: E402
from obstore.store import LocalStore  # noqa: E402
from rivers._core import AutomationDaemon, RunQueueConfig  # noqa: E402

# Upstream chain behind the asset the selection benchmark picks. Fixed, so the
# executed work does not grow with the graph the selection resolves against.
SELECTION_DEPTH = 10
# The pool the queue benchmark contends for, and how long a winner holds a slot.
POOL_KEY = "bench_pool"
SLOT_HOLD_S = 0.001
# Prefect queues a claimant it cannot serve, so the other two retry until the
# slot frees rather than reporting a refusal the moment the pool is full. All
# three then measure the same thing: how fast real claims are granted under
# contention. Without this, `claims_per_s` compared a grant rate against a
# refusal rate, and refusing is nearly free.
CLAIM_TIMEOUT_S = 30.0
CLAIM_RETRY_S = 0.001
# Partition runs a backfill may have in flight at once.
BACKFILL_CONCURRENCY = 4
# The interval both condition evaluators are held to, and how often the graph is
# polled while it settles before the window opens.
CONDITION_INTERVAL_S = 1
CONDITION_SETTLE_POLL_S = 0.1
# The one asset that never materializes, so its ticks count the passes.
CANARY_ASSET = "canary"
# The dynamic partitions definition `partition_ops` registers keys against.
DYNAMIC_DEF = "bench_dynamic"
DYNAMIC_KEYS = 1_000
# Every schedule carries the same cron. It is never waited on — schedules are
# evaluated directly — so this only has to be valid.
SCHEDULE_CRON = "*/5 * * * *"
# The cancelled run sleeps far longer than the measurement, so the cancellation
# always lands mid-step rather than racing the step's own end.
CANCEL_SLEEP_S = 60
CANCEL_POLL_S = 0.01
RUN_POLL_S = 0.02
TERMINAL_STATUSES = ("Success", "Failure", "Canceled", "Cancelled")
# Objects a benchmark must keep alive past its own return. `main` hard-exits
# after emitting, so nothing here is ever dropped.
KEEP_ALIVE: list = []


@contextlib.contextmanager
def workspace():
    """A temporary embedded-storage workspace, removed on exit."""
    directory = tempfile.mkdtemp(prefix="rivers_cmp_")
    try:
        yield rs.Storage.embedded(os.path.join(directory, "db"))
    finally:
        shutil.rmtree(directory, ignore_errors=True)


@contextlib.contextmanager
def parallel_workspace():
    """A workspace that also carries an IO handler the parallel executor can use.

    rivers defaults to the parallel executor, which runs each step in a
    subprocess. The default `InMemoryIOHandler` cannot reach across that
    boundary, so any benchmark that keeps the default executor has to name a
    picklable handler on every asset.
    """
    directory = tempfile.mkdtemp(prefix="rivers_cmp_")
    io_dir = os.path.join(directory, "io")
    os.makedirs(io_dir, exist_ok=True)
    try:
        yield (
            rs.Storage.embedded(os.path.join(directory, "db")),
            rs.PickleIOHandler(store=LocalStore(io_dir)),
        )
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def wait_for_condition_steady_state(storage, budget) -> None:
    """Block until the daemon has absorbed the first pass's fan-out.

    Two waits, and the second is the one that matters. The runs going terminal
    is not the end of the fan-out: every asset is still in the daemon's
    in-progress set, and the next refresh is what clears it. That refresh is
    startup, not the steady state this benchmark reports, so the window opens
    after it.
    """
    while True:
        budget.check()
        runs = storage.get_runs(limit=1_000_000)
        if runs and all(r.status in TERMINAL_STATUSES for r in runs):
            break
        time.sleep(CONDITION_SETTLE_POLL_S)

    absorbed = len(storage.get_ticks(CANARY_ASSET, limit=1_000_000)) + 1
    while len(storage.get_ticks(CANARY_ASSET, limit=1_000_000)) < absorbed:
        budget.check()
        time.sleep(CONDITION_SETTLE_POLL_S)


def make_assets(n: int) -> list:
    """Build ``n`` independent no-op assets."""

    def build(i: int):
        def fn() -> int:
            return i

        fn.__name__ = f"asset_{i}"
        return rs.Asset(name=f"asset_{i}")(fn)

    return [build(i) for i in range(n)]


def make_sensors(n: int, interval_s: int) -> list:
    """Build ``n`` no-op sensors, all held to ``interval_s``."""

    def build(i: int):
        def evaluate(context: rs.SensorEvaluationContext) -> rs.SkipReason:
            return rs.SkipReason("noop")

        evaluate.__name__ = f"sensor_{i}"
        return rs.Sensor(
            evaluate,
            name=f"sensor_{i}",
            minimum_interval=f"{interval_s}s",
            default_status=rs.SensorStatus.Running,
            asset_selection=["asset_0"],
            eval_mode=rs.EvalMode.InProcess,
        )

    return [build(i) for i in range(n)]


def bench_graph_load(args: argparse.Namespace) -> dict:
    """Time defining ``n`` assets and loading them into a servable code location."""
    budget = Budget(args.budget)
    t0 = time.perf_counter()
    assets = make_assets(args.n)
    define_s = time.perf_counter() - t0

    with workspace() as storage:
        t0 = time.perf_counter()
        repo = rs.CodeRepository(assets=assets)
        construct_s = time.perf_counter() - t0

        # `validate` is the pure in-memory graph build — the only part Dagster
        # and Prefect also do at load time.
        t0 = time.perf_counter()
        repo.validate()
        validate_s = time.perf_counter() - t0

        # `resolve` additionally persists topology to storage. Neither Dagster
        # nor Prefect writes anything at load, so this has no counterpart.
        t0 = time.perf_counter()
        repo.resolve(storage=storage)
        resolve_s = time.perf_counter() - t0
        budget.check()

        peak_mb = rss_mb()
        repo.shutdown()

    return {
        "define_ms": define_s * 1000,
        "construct_ms": construct_s * 1000,
        "build_ms": validate_s * 1000,
        "load_ms": resolve_s * 1000,
        "persists_topology": True,
        "total_ms": (define_s + construct_s + resolve_s) * 1000,
        "rss_mb": peak_mb,
    }


def bench_run_latency(args: argparse.Namespace) -> dict:
    """Time end-to-end execution of a single no-op asset, repeated."""
    budget = Budget(args.budget)
    with workspace() as storage:
        repo = rs.CodeRepository(assets=make_assets(max(args.n, 1)))
        repo.resolve(storage=storage)

        def run() -> None:
            repo.materialize(selection=["asset_0"])

        run()  # One untimed run absorbs lazy executor and runtime initialisation.
        samples = sample(run, args.iterations, budget)
        used_mb = rss_mb()
        repo.shutdown()
    return {"latency": summarize(samples), "rss_mb": used_mb}


def bench_run_throughput(args: argparse.Namespace) -> dict:
    """Execute ``n`` no-op runs back to back and report runs per second."""
    budget = Budget(args.budget)
    with workspace() as storage:
        repo = rs.CodeRepository(assets=make_assets(1))
        repo.resolve(storage=storage)

        def run() -> None:
            repo.materialize(selection=["asset_0"])

        run()  # warm-up, discarded
        result = throughput(run, args.n, budget)
        result["rss_mb"] = rss_mb()
        repo.shutdown()
    return result


def bench_sensor_pass(args: argparse.Namespace) -> dict:
    """Measure whether the daemon can tick every one of ``n`` sensors each interval.

    Both daemons run at the same interval, and both are judged on durable tick
    records. A *pass* is complete only when every sensor produced a new tick, so
    the slowest sensor sets the pass time. ``starved`` means the daemon missed
    more than 10% of the passes its own interval promised.
    """
    budget = Budget(args.budget)
    interval_s = sensor_interval(args.tuned)

    with workspace() as storage:
        sensors = make_sensors(args.n, interval_s)
        names = [s.name for s in sensors]
        repo = rs.CodeRepository(assets=make_assets(1), sensors=sensors)
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage, max_ticks_retained=None)
        daemon.start()
        time.sleep(min(interval_s, 2.0))  # warm-up, discarded
        budget.check()

        def tick_counts() -> list[int]:
            return [len(storage.get_ticks(n, limit=1_000_000)) for n in names]

        deltas, window_s = counted_window(tick_counts, args.duration)
        used_mb = rss_mb()

    result = pass_verdict(deltas, window_s, interval_s, args.n)
    result["rss_mb"] = used_mb
    # rivers settles deferred per-tick work during `stop()`, at a cost
    # proportional to ticks produced. `daemon_shutdown` measures that; here the
    # process hard-exits instead, so it does not distort the pass timing.
    return result


def bench_daemon_shutdown(args: argparse.Namespace) -> dict:
    """Measure what a clean daemon shutdown costs after ``n`` seconds of ticking.

    rivers defers some per-tick work and settles it in ``stop()``. This reports
    how long that settle takes relative to the ticks produced. It has no
    counterpart in the other two frameworks, so it is run by hand rather than
    swept — see `bench/README.md`.
    """
    budget = Budget(args.budget)
    with workspace() as storage:
        sensors = make_sensors(10, 0)
        names = [s.name for s in sensors]
        repo = rs.CodeRepository(assets=make_assets(1), sensors=sensors)
        repo.resolve(storage=storage)
        daemon = AutomationDaemon(repo=repo, storage=storage, max_ticks_retained=None)
        daemon.start()
        time.sleep(args.duration)
        ticks = sum(len(storage.get_ticks(n, limit=1_000_000)) for n in names)
        budget.check()
        t0 = time.perf_counter()
        daemon.stop()
        stop_s = time.perf_counter() - t0
        used_mb = rss_mb()
        repo.shutdown()
    return {
        "ticking_s": args.duration,
        "ticks": ticks,
        "ticks_per_s": ticks / args.duration,
        "shutdown_s": stop_s,
        "shutdown_ms_per_tick": stop_s * 1000 / ticks if ticks else 0.0,
        "rss_mb": used_mb,
    }


def bench_partitions(args: argparse.Namespace) -> dict:
    """Time defining and loading one asset with ``n`` partitions.

    Static keys, not a date range. Dagster's time-window partitions clamp to the
    current date, so a daily definition asked for a million partitions quietly
    returns only the days that have actually elapsed.
    """
    budget = Budget(args.budget)
    with workspace() as storage:
        keys = [f"key_{i:08d}" for i in range(args.n)]
        t0 = time.perf_counter()
        parts = rs.PartitionsDefinition.static_(keys)
        define_s = time.perf_counter() - t0

        def fn(context: rs.AssetExecutionContext) -> int:
            return 1

        fn.__name__ = "partitioned"
        asset = rs.Asset(name="partitioned", partitions_def="static")(fn)
        repo = rs.CodeRepository(assets=[asset], partition_defs={"static": parts})

        t0 = time.perf_counter()
        repo.resolve(storage=storage)
        resolve_s = time.perf_counter() - t0
        budget.check()

        t0 = time.perf_counter()
        listed = parts.get_partition_keys()
        query_s = time.perf_counter() - t0
        used_mb = rss_mb()
        repo.shutdown()

    return {
        "partitions": len(listed),
        "define_ms": define_s * 1000,
        "load_ms": resolve_s * 1000,
        "query_ms": query_s * 1000,
        "total_ms": (define_s + resolve_s) * 1000,
        "rss_mb": used_mb,
    }


def make_layered_assets(n: int, fan_in: int = 2) -> list:
    """Build ``n`` assets in layers, each depending on ``fan_in`` of the layer above.

    ``graph_load`` measures ``n`` assets with no edges at all, which is the
    easiest graph a resolver can be handed. Real graphs have edges, and that is
    where a graph engine either scales or does not.
    """
    width = max(int(n**0.5), 1)

    def build(i: int):
        upstream = [j for j in range(max(i - width, 0), i)][:fan_in]

        def fn() -> int:
            return i

        fn.__name__ = f"asset_{i}"
        return rs.Asset(
            name=f"asset_{i}",
            deps=[rs.AssetDef.dep(f"asset_{j}") for j in upstream],
        )(fn)

    return [build(i) for i in range(n)]


def make_sleep_assets(n: int, seconds: float, handler) -> list:
    """Build ``n`` independent assets that each sleep ``seconds``."""

    def build(i: int):
        def fn() -> int:
            time.sleep(seconds)
            return i

        fn.__name__ = f"sleep_{i}"
        return rs.Asset(name=f"sleep_{i}", io_handler=handler)(fn)

    return [build(i) for i in range(n)]


def bench_graph_deps(args: argparse.Namespace) -> dict:
    """Time defining and loading ``n`` assets wired into a layered graph."""
    budget = Budget(args.budget)
    t0 = time.perf_counter()
    assets = make_layered_assets(args.n)
    define_s = time.perf_counter() - t0

    with workspace() as storage:
        t0 = time.perf_counter()
        repo = rs.CodeRepository(assets=assets)
        construct_s = time.perf_counter() - t0

        t0 = time.perf_counter()
        repo.validate()
        validate_s = time.perf_counter() - t0

        t0 = time.perf_counter()
        repo.resolve(storage=storage)
        resolve_s = time.perf_counter() - t0
        budget.check()

        peak_mb = rss_mb()
        repo.shutdown()

    return {
        "define_ms": define_s * 1000,
        "construct_ms": construct_s * 1000,
        "build_ms": validate_s * 1000,
        "load_ms": resolve_s * 1000,
        "total_ms": (define_s + construct_s + resolve_s) * 1000,
        "rss_mb": peak_mb,
    }


def bench_selection(args: argparse.Namespace) -> dict:
    """Time resolving one asset plus its upstream inside an ``n``-asset graph.

    The chain behind the selected asset is fixed at ``SELECTION_DEPTH``, so the
    executed work does not grow with ``n`` and the number reflects resolving a
    selection against a graph of that size.
    """
    budget = Budget(args.budget)
    depth = SELECTION_DEPTH

    def build(i: int, upstream: list[str]):
        def fn() -> int:
            return i

        fn.__name__ = f"asset_{i}"
        return rs.Asset(name=f"asset_{i}", deps=[rs.AssetDef.dep(u) for u in upstream])(
            fn
        )

    chain = [build(i, [f"asset_{i - 1}"] if i else []) for i in range(depth)]
    rest = [build(i, []) for i in range(depth, max(args.n, depth))]

    with workspace() as storage:
        repo = rs.CodeRepository(assets=chain + rest)
        repo.resolve(storage=storage)
        budget.check()

        leaf = f"asset_{depth - 1}"
        repo.materialize(selection=[leaf], include_upstream=True)  # warm-up
        samples = sample(
            lambda: repo.materialize(selection=[leaf], include_upstream=True),
            args.iterations,
            budget,
        )
        used_mb = rss_mb()
        repo.shutdown()

    return {
        "graph_assets": max(args.n, depth),
        "selected": depth,
        "latency": summarize(samples),
        "total_ms": summarize(samples)["median_ms"],
        "rss_mb": used_mb,
    }


def bench_run_steps(args: argparse.Namespace) -> dict:
    """Time one run that materializes ``n`` assets, and report the per-step cost.

    Every other run benchmark executes a single asset, which measures launching
    a run rather than executing one. This is what a real pipeline pays.
    """
    budget = Budget(args.budget)
    with workspace() as storage:
        # rivers defaults to the parallel executor, which runs each step in a
        # subprocess. The other two execute a multi-step run in the calling
        # process, so this is the like-for-like configuration.
        repo = rs.CodeRepository(
            assets=make_assets(args.n),
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        repo.materialize()  # warm-up, discarded
        t0 = time.perf_counter()
        result = repo.materialize()
        elapsed_s = time.perf_counter() - t0
        budget.check()
        used_mb = rss_mb()
        repo.shutdown()

    return {
        "steps": args.n,
        "success": result.success,
        "total_ms": elapsed_s * 1000,
        "ms_per_step": elapsed_s * 1000 / args.n,
        "rss_mb": used_mb,
    }


def bench_parallel_scaling(args: argparse.Namespace) -> dict:
    """Time ``n`` sleeping assets executed across a fixed worker pool.

    Each asset sleeps, so the work itself is identical everywhere and whatever
    the wall clock holds above the ideal is the framework's scheduling cost.
    """
    budget = Budget(args.budget)
    with parallel_workspace() as (storage, handler):
        repo = rs.CodeRepository(
            assets=make_sleep_assets(args.n, PARALLEL_SLEEP_S, handler),
            default_executor=rs.Executor.parallel(max_workers=PARALLEL_WORKERS),
        )
        repo.resolve(storage=storage)

        repo.materialize()  # warm-up: the worker pool starts here, not in the measurement
        budget.check()
        t0 = time.perf_counter()
        result = repo.materialize()
        elapsed_s = time.perf_counter() - t0
        budget.check()

        verdict = parallel_verdict(elapsed_s, args.n)
        verdict["success"] = result.success
        verdict["rss_mb"] = rss_mb()
        repo.shutdown()
    return verdict


def bench_read_path(args: argparse.Namespace) -> dict:
    """Time the queries a UI page makes against a store holding ``n`` runs.

    Seeding is not timed and not compared: each framework fills its own store
    by whatever path is cheapest for it. All three end with ``n`` runs and
    ``n`` per-unit rows, so the queries read at the same volume.

    One difference is left standing, and it counts against rivers: rivers seeds
    by executing the runs, so its event table also holds a full event stream
    per run, where the other two write one record each. Its `asset_events`
    query therefore reads the largest table of the three.
    """
    budget = Budget(args.budget)
    with workspace() as storage:
        repo = rs.CodeRepository(assets=make_assets(1))
        repo.resolve(storage=storage)
        for _ in range(args.n):
            repo.materialize(selection=["asset_0"])
            budget.check()

        def timed(call) -> float:
            started = time.perf_counter()
            call()
            return time.perf_counter() - started

        timings = {
            "recent_runs": timed(lambda: storage.get_runs(limit=100)),
            "latest_materialization": timed(
                lambda: storage.get_latest_materialization("asset_0")
            ),
            "asset_records": timed(storage.get_asset_records),
            "asset_events": timed(
                lambda: storage.get_events_for_asset("asset_0", limit=100)
            ),
        }
        used_mb = rss_mb()
        repo.shutdown()

    return {"seeded_runs": args.n, **query_summary(timings), "rss_mb": used_mb}


def bench_queue_limits(args: argparse.Namespace) -> dict:
    """Contend ``n`` claims against a pool of ``QUEUE_SLOTS`` slots.

    Speed is only half the question. A limiter that hands out more slots than
    its limit is wrong however fast it is, so the number of slots held at once
    is tracked and a breach fails the measurement.

    Every claimant waits for a slot rather than giving up on the first refusal,
    because Prefect queues and the other two do not. A claim that never lands
    inside ``CLAIM_TIMEOUT_S`` is the refusal.
    """
    budget = Budget(args.budget)
    with workspace() as storage:
        storage.set_pool_limit(POOL_KEY, QUEUE_SLOTS)
        held = 0
        max_held = 0
        granted = 0
        refused = 0
        lock = threading.Lock()

        def claimant(index: int) -> None:
            nonlocal held, max_held, granted, refused
            run_id = f"run_{index}"
            deadline = time.monotonic() + CLAIM_TIMEOUT_S
            while True:
                status = storage._claim_concurrency_slots(
                    [(POOL_KEY, 1)], run_id, "step", 0, "5m"
                )
                if status.is_claimed:
                    break
                if time.monotonic() >= deadline:
                    with lock:
                        refused += 1
                    return
                time.sleep(CLAIM_RETRY_S)
            with lock:
                held += 1
                granted += 1
                max_held = max(max_held, held)
            time.sleep(SLOT_HOLD_S)
            # Drop the count before releasing the slot. The other order lets a
            # waiting thread claim the freed slot and count itself in while
            # this one is still counted, which reads as a breach.
            with lock:
                held -= 1
            storage._free_concurrency_slots(run_id, "step")

        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=QUEUE_SLOTS * 2) as pool:
            list(pool.map(claimant, range(args.n)))
        elapsed_s = time.perf_counter() - t0
        budget.check()

        verdict = claim_verdict(elapsed_s, granted, refused, QUEUE_SLOTS, max_held)
        verdict["rss_mb"] = rss_mb()
    return verdict


def bench_log_capture(args: argparse.Namespace) -> dict:
    """Time one run whose step writes ``n`` log lines, then read them back.

    The run is launched through the queue rather than executed inline, because
    Dagster persists step output only for a launched run and both sides have to
    do the same job. Writing the lines and retrieving them are separate costs,
    and a framework can be fast at one and slow at the other.
    """
    budget = Budget(args.budget)
    lines = args.n
    to_write = 0

    def fn() -> int:
        for _ in range(to_write):
            print(LOG_LINE)
        return 1

    fn.__name__ = "logged"

    with workspace() as storage:
        repo = rs.CodeRepository(
            assets=[rs.Asset(name="logged")(fn)], run_queue=RunQueueConfig()
        )
        repo.resolve(storage=storage)
        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()

        def timed_run() -> tuple[float, str, str]:
            started = time.perf_counter()
            handle = repo._submit_run(selection=["logged"])
            while handle.status not in TERMINAL_STATUSES:
                budget.check()
                time.sleep(RUN_POLL_S)
            return time.perf_counter() - started, handle.run_id, handle.status

        # The same run writing nothing. Two of them: the daemon dequeues its
        # first run more slowly than the rest, which would inflate the baseline
        # and hide real per-line cost.
        timed_run()
        launch_s, _, _ = timed_run()

        to_write = lines
        write_s, run_id, run_status = timed_run()

        t0 = time.perf_counter()
        stored = storage.get_run_logs(run_id)
        read_s = time.perf_counter() - t0
        captured = sum(len((row.stdout or "").splitlines()) for row in stored)
        used_mb = rss_mb()

    return log_verdict(
        lines,
        captured,
        write_s,
        read_s,
        launch_s,
        run_status=run_status,
        rss_mb=used_mb,
    )


def bench_backfill_drain(args: argparse.Namespace) -> dict:
    """Time a backfill over ``n`` partitions until every partition is terminal."""
    budget = Budget(args.budget)
    with workspace() as storage:
        keys = [f"key_{i:08d}" for i in range(args.n)]
        parts = rs.PartitionsDefinition.static_(keys)

        def fn(context: rs.AssetExecutionContext) -> int:
            return 1

        fn.__name__ = "partitioned"
        asset = rs.Asset(name="partitioned", partitions_def="static")(fn)
        repo = rs.CodeRepository(assets=[asset], partition_defs={"static": parts})
        repo.resolve(storage=storage)
        budget.check()

        t0 = time.perf_counter()
        result = repo.backfill(
            selection=["partitioned"],
            partition_keys=parts.get_partition_keys(),
            max_concurrency=BACKFILL_CONCURRENCY,
            block=True,
        )
        elapsed_s = time.perf_counter() - t0
        budget.check()
        used_mb = rss_mb()
        repo.shutdown()

    return {
        "partitions": result.num_partitions,
        "runs": result.num_runs,
        "completed": result.completed,
        "failed": result.failed,
        "backfill_status": result.status,
        "total_ms": elapsed_s * 1000,
        "ms_per_partition": elapsed_s * 1000 / args.n,
        "partitions_per_s": args.n / elapsed_s if elapsed_s else 0.0,
        "rss_mb": used_mb,
    }


def bench_condition_pass(args: argparse.Namespace) -> dict:
    """Measure whether the daemon can evaluate every one of ``n`` conditions each interval.

    Every asset carries ``AutomationCondition.missing()``. All but one
    materialize on the first pass and then stop being requested, so the steady
    state is: evaluate ``n`` conditions, dispatch exactly one run. The one that
    stays missing is a step that always fails, and its ticks are what a pass is
    counted by. ``starved`` means the daemon missed more than 10% of the passes
    its own interval promised.

    This reports the steady state only. `wait_for_condition_steady_state` waits
    out the first pass's fan-out and the refresh that absorbs it.
    """
    budget = Budget(args.budget)

    def build(i: int, handler):
        def fn() -> int:
            return i

        fn.__name__ = f"asset_{i}"
        return rs.Asset(
            name=f"asset_{i}",
            io_handler=handler,
            automation_condition=rs.AutomationCondition.missing(),
        )(fn)

    def build_canary(handler):
        def fn() -> int:
            raise RuntimeError("stays missing on purpose")

        fn.__name__ = CANARY_ASSET
        return rs.Asset(
            name=CANARY_ASSET,
            io_handler=handler,
            automation_condition=rs.AutomationCondition.missing(),
        )(fn)

    with parallel_workspace() as (storage, handler):
        assets = [build(i, handler) for i in range(max(args.n - 1, 1))]
        assets.append(build_canary(handler))
        repo = rs.CodeRepository(assets=assets)
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(
            repo=repo,
            storage=storage,
            max_ticks_retained=None,
            condition_eval_interval=f"{CONDITION_INTERVAL_S}s",
        )
        daemon.start()
        wait_for_condition_steady_state(storage, budget)

        def tick_counts() -> list:
            return [len(storage.get_ticks(CANARY_ASSET, limit=1_000_000))]

        deltas, window_s = counted_window(tick_counts, args.duration)
        used_mb = rss_mb()

    result = pass_verdict(deltas, window_s, CONDITION_INTERVAL_S, args.n)
    result["assets"] = args.n
    result["dispatch_per_pass"] = 1
    result["rss_mb"] = used_mb
    return result


def bench_partition_ops(args: argparse.Namespace) -> dict:
    """Time the operations a partitioned asset pays for once it holds ``n`` keys.

    `partitions` measures defining and loading them. This measures using them:
    materializing one key, counting what is materialized, listing it, and
    registering a block of dynamic keys.
    """
    budget = Budget(args.budget)
    with workspace() as storage:
        keys = [f"key_{i:08d}" for i in range(args.n)]
        parts = rs.PartitionsDefinition.static_(keys)

        def fn(context: rs.AssetExecutionContext) -> int:
            return 1

        fn.__name__ = "partitioned"
        asset = rs.Asset(name="partitioned", partitions_def="static")(fn)
        repo = rs.CodeRepository(assets=[asset], partition_defs={"static": parts})
        repo.resolve(storage=storage)
        budget.check()

        target = rs.PartitionKey.single(keys[args.n // 2])
        repo.materialize(selection=["partitioned"], partition_key=target)  # warm-up

        def timed(call) -> float:
            started = time.perf_counter()
            call()
            return time.perf_counter() - started

        materialize_s = timed(
            lambda: repo.materialize(selection=["partitioned"], partition_key=target)
        )
        count_s = timed(lambda: storage.count_materialized_partitions("partitioned"))
        list_s = timed(lambda: storage.get_materialized_partitions("partitioned"))
        dynamic_s = timed(
            lambda: storage.add_dynamic_partitions(
                DYNAMIC_DEF, [f"dyn_{i:08d}" for i in range(DYNAMIC_KEYS)]
            )
        )
        budget.check()
        used_mb = rss_mb()
        repo.shutdown()

    return {
        "partitions": args.n,
        "materialize_one_ms": materialize_s * 1000,
        "count_ms": count_s * 1000,
        "list_ms": list_s * 1000,
        "add_dynamic_ms": dynamic_s * 1000,
        "dynamic_keys": DYNAMIC_KEYS,
        # Registering dynamic keys is a fixed block of work, so it is reported
        # but kept out of the headline, which is about the ops that scale.
        "total_ms": (materialize_s + count_s + list_s) * 1000,
        "rss_mb": used_mb,
    }


def bench_schedule_tick(args: argparse.Namespace) -> dict:
    """Time evaluating every one of ``n`` schedules once.

    The cron interval is deliberately not part of this. Dagster accepts no cron
    finer than one minute, so a windowed measurement would report that floor
    rather than what a tick costs. Evaluating each schedule directly measures
    the work instead.
    """
    budget = Budget(args.budget)
    with workspace() as storage:

        def build(i: int):
            def evaluate(context: rs.ScheduleEvaluationContext) -> rs.SkipReason:
                return rs.SkipReason("noop")

            evaluate.__name__ = f"schedule_{i}"
            return rs.Schedule(
                evaluate,
                name=f"schedule_{i}",
                cron_schedule=SCHEDULE_CRON,
                job_name="noop_job",
                eval_mode=rs.EvalMode.InProcess,
            )

        schedules = [build(i) for i in range(args.n)]
        assets = make_assets(1)
        repo = rs.CodeRepository(
            assets=assets,
            jobs=[rs.Job(name="noop_job", assets=assets)],
            schedules=schedules,
        )
        repo.resolve(storage=storage)
        budget.check()

        repo.evaluate_schedule(schedules[0].name)  # warm-up, discarded
        t0 = time.perf_counter()
        for schedule in schedules:
            repo.evaluate_schedule(schedule.name)
        elapsed_s = time.perf_counter() - t0
        budget.check()
        used_mb = rss_mb()
        repo.shutdown()

    return {
        "schedules": args.n,
        "pass_ms": elapsed_s * 1000,
        "total_ms": elapsed_s * 1000,
        "ms_per_schedule": elapsed_s * 1000 / args.n,
        "rss_mb": used_mb,
    }


def bench_cancel_latency(args: argparse.Namespace) -> dict:
    """Time a launched run from cancellation request to a terminal state.

    The run goes through the queue, which is the path `rivers serve` uses and
    the only one that watches for a cancellation. A direct ``materialize`` call
    runs the step inline and would never see the request.
    """
    budget = Budget(args.budget)
    directory = tempfile.mkdtemp(prefix="rivers_cancel_")
    storage = rs.Storage.embedded(os.path.join(directory, "db"))

    def fn() -> int:
        time.sleep(CANCEL_SLEEP_S)
        return 1

    fn.__name__ = "sleeper"
    repo = rs.CodeRepository(
        assets=[rs.Asset(name="sleeper")(fn)], run_queue=RunQueueConfig()
    )
    repo.resolve(storage=storage)
    daemon = AutomationDaemon(repo=repo, storage=storage)
    daemon.start()
    # The cancelled step keeps sleeping in its worker, and dropping the storage
    # or the repository waits for it. Held here so nothing is collected before
    # `main` hard-exits; the temporary directory is removed below instead.
    KEEP_ALIVE.extend([storage, repo, daemon])

    handle = repo._submit_run(selection=["sleeper"])
    while handle.status in ("NotStarted", "Queued"):
        budget.check()
        time.sleep(CANCEL_POLL_S)
    started_status = handle.status

    t0 = time.perf_counter()
    handle.cancel()
    terminal = None
    while terminal is None:
        budget.check()
        status = handle.status
        if status in TERMINAL_STATUSES:
            terminal = status
            break
        time.sleep(CANCEL_POLL_S)
    elapsed_s = time.perf_counter() - t0
    used_mb = rss_mb()
    shutil.rmtree(directory, ignore_errors=True)

    return {
        "status_at_request": started_status,
        "terminal_status": terminal,
        "cancel_ms": elapsed_s * 1000,
        "rss_mb": used_mb,
    }


def bench_reload(args: argparse.Namespace) -> dict:
    """Time loading ``n`` assets into a store that already holds them.

    A code location is loaded once when it starts and again on every change, so
    a reload rebuilds the definitions — all three drivers do that here, inside
    the timer. The second load also writes over topology that is already there,
    which is not the same work as the first, and ``reload_ratio`` is where that
    shows.
    """
    budget = Budget(args.budget)
    with workspace() as storage:

        def load():
            repo = rs.CodeRepository(assets=make_assets(args.n))
            repo.resolve(storage=storage)
            return repo

        t0 = time.perf_counter()
        first = load()
        first_s = time.perf_counter() - t0
        first.shutdown()
        budget.check()

        t0 = time.perf_counter()
        second = load()
        reload_s = time.perf_counter() - t0
        budget.check()
        loaded = len(second.assets)
        used_mb = rss_mb()
        second.shutdown()

    return {
        "assets": args.n,
        "loaded_assets": loaded,
        "first_load_ms": first_s * 1000,
        "total_ms": reload_s * 1000,
        "reload_ratio": reload_s / first_s if first_s else 0.0,
        "rss_mb": used_mb,
    }


COLD_START_SCRIPT = """
import os, tempfile
import rivers as rs

n = int(os.environ["BENCH_N_ASSETS"])


def build(i):
    def fn() -> int:
        return i

    fn.__name__ = "asset_{}".format(i)
    return rs.Asset(name="asset_{}".format(i))(fn)


assets = [build(i) for i in range(n)]
storage = rs.Storage.embedded(os.path.join(tempfile.mkdtemp(), "db"))
repo = rs.CodeRepository(assets=assets)
repo.resolve(storage=storage)
# Servable: the code location can now answer what assets it holds.
assert len(repo.assets) == n
"""


if __name__ == "__main__":
    main(
        "rivers",
        {
            "graph_load": bench_graph_load,
            "run_latency": bench_run_latency,
            "run_throughput": bench_run_throughput,
            "sensor_pass": bench_sensor_pass,
            "partitions": bench_partitions,
            "cold_start": cold_start_benchmark(COLD_START_SCRIPT, QUIET_ENV),
            "daemon_shutdown": bench_daemon_shutdown,
            "graph_deps": bench_graph_deps,
            "selection": bench_selection,
            "run_steps": bench_run_steps,
            "parallel_scaling": bench_parallel_scaling,
            "read_path": bench_read_path,
            "queue_limits": bench_queue_limits,
            "log_capture": bench_log_capture,
            "backfill_drain": bench_backfill_drain,
            "condition_pass": bench_condition_pass,
            "partition_ops": bench_partition_ops,
            "schedule_tick": bench_schedule_tick,
            "cancel_latency": bench_cancel_latency,
            "reload": bench_reload,
        },
    )
