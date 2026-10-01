"""Dagster side of the comparison benchmarks. Runs in `.venv-bench`."""

from __future__ import annotations

import argparse
import contextlib
import logging
import os
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
QUIET_ENV = {"DAGSTER_HOME_QUIET": "1"}
quiet_env(QUIET_ENV)

# Import the framework only after quiet_env(), so its logging setup reads
# the silenced configuration and cannot distort the measurement.
import dagster as dg  # noqa: E402
from dagster._core.instance import DagsterInstance  # noqa: E402
from dagster._core.remote_representation.external_data import RepositorySnap  # noqa: E402

# Thread pool a tuned Dagster deployment gives its sensor daemon.
SENSOR_DAEMON_WORKERS = 4
TMP_PREFIX = "dagster_cmp_"

# These match `rivers_bench.py` exactly. A shape that differs by framework
# invalidates the comparison, so changing one means changing both.
SELECTION_DEPTH = 10
POOL_KEY = "bench_pool"
SLOT_HOLD_S = 0.001
# Prefect queues a claimant it cannot serve, so the other two retry until the
# slot frees rather than reporting a refusal the moment the pool is full. All
# three then measure the same thing: how fast real claims are granted under
# contention. Without this, `claims_per_s` compared a grant rate against a
# refusal rate, and refusing is nearly free.
CLAIM_TIMEOUT_S = 30.0
CLAIM_RETRY_S = 0.001
DYNAMIC_DEF = "bench_dynamic"
DYNAMIC_KEYS = 1_000
SCHEDULE_CRON = "*/5 * * * *"
CANCEL_SLEEP_S = 60
CANCEL_POLL_S = 0.01
CONDITION_INTERVAL_S = 1
# The one asset that never materializes, so exactly one run is requested each
# pass. Same name and same role as rivers'.
CANARY_ASSET = "canary"
RUN_POLL_S = 0.02


def make_assets(n: int) -> list:
    """Build ``n`` independent no-op assets."""

    def build(i: int):
        @dg.asset(name=f"asset_{i}")
        def _asset() -> int:
            return i

        return _asset

    return [build(i) for i in range(n)]


def _local_instance(tmpdir: str) -> DagsterInstance:
    """A SQLite-backed instance, comparable to rivers' embedded storage."""
    return DagsterInstance.local_temp(tempdir=tmpdir)


def bench_graph_load(args: argparse.Namespace) -> dict:
    """Time defining ``n`` assets and building the code-location snapshot.

    ``RepositorySnap.from_def`` is the serialisable snapshot a Dagster gRPC code
    location hands to the daemon and webserver. It is the closest equivalent of
    the work rivers does in ``CodeRepository.resolve``.
    """
    budget = Budget(args.budget)
    t0 = time.perf_counter()
    assets = make_assets(args.n)
    define_s = time.perf_counter() - t0

    t0 = time.perf_counter()
    defs = dg.Definitions(assets=assets)
    repo = defs.get_repository_def()
    construct_s = time.perf_counter() - t0
    budget.check()

    t0 = time.perf_counter()
    RepositorySnap.from_def(repo)
    snapshot_s = time.perf_counter() - t0
    budget.check()

    return {
        "define_ms": define_s * 1000,
        "construct_ms": construct_s * 1000,
        "load_ms": snapshot_s * 1000,
        "total_ms": (define_s + construct_s + snapshot_s) * 1000,
        "rss_mb": rss_mb(),
    }


def bench_run_latency(args: argparse.Namespace) -> dict:
    """Time end-to-end execution of a single no-op asset, repeated."""
    budget = Budget(args.budget)
    with tempfile.TemporaryDirectory(
        prefix=TMP_PREFIX, ignore_cleanup_errors=True
    ) as tmpdir:
        assets = make_assets(max(args.n, 1))
        t0 = time.perf_counter()
        instance = _local_instance(tmpdir)
        instance_s = time.perf_counter() - t0

        def run() -> None:
            dg.materialize([assets[0]], instance=instance)

        run()  # warm-up, discarded
        samples = sample(run, args.iterations, budget)
        used_mb = rss_mb()
        instance.dispose()
    return {
        "latency": summarize(samples),
        "instance_startup_ms": instance_s * 1000,
        "rss_mb": used_mb,
    }


def bench_run_throughput(args: argparse.Namespace) -> dict:
    """Execute ``n`` no-op runs back to back and report runs per second."""
    budget = Budget(args.budget)
    with tempfile.TemporaryDirectory(
        prefix=TMP_PREFIX, ignore_cleanup_errors=True
    ) as tmpdir:
        assets = make_assets(1)
        instance = _local_instance(tmpdir)

        def run() -> None:
            dg.materialize([assets[0]], instance=instance)

        run()  # warm-up, discarded
        result = throughput(run, args.n, budget)
        result["rss_mb"] = rss_mb()
        instance.dispose()
    return result


def bench_sensor_pass(args: argparse.Namespace) -> dict:
    """Measure whether Dagster's sensor daemon ticks every one of ``n`` sensors each interval.

    ``execute_sensor_iteration`` is driven in a tight loop rather than on the
    sensor daemon's own 30-second wake cycle, so the result reflects what
    Dagster can do rather than how often it chooses to wake. The tuned run also
    gets the thread pool a tuned deployment would configure.
    """
    from concurrent.futures import ThreadPoolExecutor

    from dagster._core.test_utils import create_test_daemon_workspace_context
    from dagster._core.workspace.load_target import ModuleTarget
    from dagster._daemon.sensor import execute_sensor_iteration

    budget = Budget(args.budget)
    interval = sensor_interval(args.tuned)
    os.environ["BENCH_N_SENSORS"] = str(args.n)
    os.environ["BENCH_N_ASSETS"] = "1"
    os.environ["BENCH_SENSOR_INTERVAL"] = str(interval)

    logger = logging.getLogger("SensorDaemon")
    logger.setLevel(logging.CRITICAL)

    pool = ThreadPoolExecutor(max_workers=SENSOR_DAEMON_WORKERS) if args.tuned else None
    # Dagster requires a futures dict alongside a thread pool; it tracks the
    # ticks still in flight between iterations.
    futures: dict | None = {} if pool is not None else None
    try:
        with tempfile.TemporaryDirectory(
            prefix=TMP_PREFIX, ignore_cleanup_errors=True
        ) as tmpdir:
            instance = _local_instance(tmpdir)
            with create_test_daemon_workspace_context(
                workspace_load_target=ModuleTarget(
                    module_name="dagster_defs",
                    attribute="defs",
                    working_directory=os.path.dirname(os.path.abspath(__file__)),
                    location_name="bench",
                ),
                instance=instance,
            ) as workspace_ctx:
                # The first iteration registers every sensor; it is not
                # representative.
                list(
                    execute_sensor_iteration(workspace_ctx, logger, pool, None, futures)
                )
                budget.check()

                request_ctx = workspace_ctx.create_request_context()
                location = request_ctx.get_code_location("bench")
                repository = next(iter(location.get_repositories().values()))
                remote_sensors = repository.get_sensors()

                def tick_counts() -> list[int]:
                    return [
                        len(
                            instance.get_ticks(
                                sensor.get_remote_origin_id(),
                                sensor.selector_id,
                                limit=1_000_000,
                            )
                        )
                        for sensor in remote_sensors
                    ]

                # The iteration loop runs on a thread so the counting window can
                # be timed the same way rivers' is, against a daemon that never
                # stops.
                stop = threading.Event()
                state = {"iterations": 0}

                def drive() -> None:
                    while not stop.is_set():
                        list(
                            execute_sensor_iteration(
                                workspace_ctx, logger, pool, None, futures
                            )
                        )
                        state["iterations"] += 1
                        if pool is not None:
                            # Threaded iterations return immediately; pace the
                            # loop so it does not spin instead of letting ticks
                            # run.
                            time.sleep(0.05)

                driver = threading.Thread(target=drive, daemon=True)
                driver.start()
                deltas, window_s = counted_window(tick_counts, args.duration)
                stop.set()
                driver.join(timeout=30)
            used_mb = rss_mb()
            instance.dispose()
    finally:
        if pool is not None:
            pool.shutdown(wait=False)

    result = pass_verdict(deltas, window_s, interval, args.n)
    result["iterations"] = state["iterations"]
    result["rss_mb"] = used_mb
    return result


def bench_partitions(args: argparse.Namespace) -> dict:
    """Time defining and loading one asset with ``n`` partitions.

    Static keys, not a date range. Dagster's time-window partitions clamp to the
    current date, so a daily definition asked for a million partitions quietly
    returns only the days that have actually elapsed.
    """
    budget = Budget(args.budget)
    keys = [f"key_{i:08d}" for i in range(args.n)]
    t0 = time.perf_counter()
    parts = dg.StaticPartitionsDefinition(keys)
    define_s = time.perf_counter() - t0

    @dg.asset(name="partitioned", partitions_def=parts)
    def partitioned() -> int:
        return 1

    t0 = time.perf_counter()
    defs = dg.Definitions(assets=[partitioned])
    RepositorySnap.from_def(defs.get_repository_def())
    load_s = time.perf_counter() - t0
    budget.check()

    t0 = time.perf_counter()
    listed = parts.get_partition_keys()
    query_s = time.perf_counter() - t0

    return {
        "partitions": len(listed),
        "define_ms": define_s * 1000,
        "load_ms": load_s * 1000,
        "query_ms": query_s * 1000,
        "total_ms": (define_s + load_s) * 1000,
        "rss_mb": rss_mb(),
    }


@contextlib.contextmanager
def _workspace(instance, launcher: bool = False):
    """A code location backed by ``dagster_defs``, served over gRPC.

    The daemon benchmarks and the cancellation benchmark all need a real code
    location: the backfill daemon launches runs through it, and a run can only
    be terminated if something launched it in the first place.
    """
    from dagster._core.test_utils import create_test_daemon_workspace_context
    from dagster._core.workspace.load_target import ModuleTarget

    with create_test_daemon_workspace_context(
        workspace_load_target=ModuleTarget(
            module_name="dagster_defs",
            attribute="defs",
            working_directory=os.path.dirname(os.path.abspath(__file__)),
            location_name="bench",
        ),
        instance=instance,
    ) as context:
        yield context


def _launcher_instance(tmpdir: str):
    """An instance whose runs are launched as real processes.

    `execute_job` runs in this process and only notices a cancellation if it is
    interrupted, so the in-process path cannot answer the cancellation
    question. The default launcher starts the run where a deployment would.
    """
    from dagster._core.test_utils import instance_for_test

    return instance_for_test(
        temp_dir=tmpdir,
        overrides={
            "run_launcher": {
                "module": "dagster._core.launcher.default_run_launcher",
                "class": "DefaultRunLauncher",
            }
        },
    )


def make_layered_assets(n: int, fan_in: int = 2) -> list:
    """Build ``n`` assets in layers, each depending on ``fan_in`` of the layer above."""
    width = max(int(n**0.5), 1)

    def build(i: int):
        upstream = [f"asset_{j}" for j in range(max(i - width, 0), i)][:fan_in]

        @dg.asset(name=f"asset_{i}", deps=upstream)
        def _asset() -> int:
            return i

        return _asset

    return [build(i) for i in range(n)]


def bench_graph_deps(args: argparse.Namespace) -> dict:
    """Time defining and loading ``n`` assets wired into a layered graph."""
    budget = Budget(args.budget)
    t0 = time.perf_counter()
    assets = make_layered_assets(args.n)
    define_s = time.perf_counter() - t0

    t0 = time.perf_counter()
    defs = dg.Definitions(assets=assets)
    repo = defs.get_repository_def()
    construct_s = time.perf_counter() - t0
    budget.check()

    t0 = time.perf_counter()
    RepositorySnap.from_def(repo)
    snapshot_s = time.perf_counter() - t0
    budget.check()

    return {
        "define_ms": define_s * 1000,
        "construct_ms": construct_s * 1000,
        "load_ms": snapshot_s * 1000,
        "total_ms": (define_s + construct_s + snapshot_s) * 1000,
        "rss_mb": rss_mb(),
    }


def bench_selection(args: argparse.Namespace) -> dict:
    """Time resolving one asset plus its upstream inside an ``n``-asset graph."""
    budget = Budget(args.budget)
    depth = SELECTION_DEPTH

    def build(i: int, upstream: list):
        @dg.asset(name=f"asset_{i}", deps=upstream)
        def _asset() -> int:
            return i

        return _asset

    chain = [build(i, [f"asset_{i - 1}"] if i else []) for i in range(depth)]
    rest = [build(i, []) for i in range(depth, max(args.n, depth))]
    assets = chain + rest
    leaf = dg.AssetKey(f"asset_{depth - 1}")
    selection = dg.AssetSelection.keys(leaf).upstream()

    with tempfile.TemporaryDirectory(
        prefix=TMP_PREFIX, ignore_cleanup_errors=True
    ) as tmpdir:
        instance = _local_instance(tmpdir)

        def run() -> None:
            dg.materialize(assets, selection=selection, instance=instance)

        run()  # warm-up, discarded
        samples = sample(run, args.iterations, budget)
        used_mb = rss_mb()
        instance.dispose()

    return {
        "graph_assets": max(args.n, depth),
        "selected": depth,
        "latency": summarize(samples),
        "total_ms": summarize(samples)["median_ms"],
        "rss_mb": used_mb,
    }


def bench_run_steps(args: argparse.Namespace) -> dict:
    """Time one run that materializes ``n`` assets, and report the per-step cost."""
    budget = Budget(args.budget)
    with tempfile.TemporaryDirectory(
        prefix=TMP_PREFIX, ignore_cleanup_errors=True
    ) as tmpdir:
        assets = make_assets(args.n)
        instance = _local_instance(tmpdir)

        dg.materialize(assets, instance=instance)  # warm-up, discarded
        budget.check()
        t0 = time.perf_counter()
        result = dg.materialize(assets, instance=instance)
        elapsed_s = time.perf_counter() - t0
        budget.check()
        used_mb = rss_mb()
        instance.dispose()

    return {
        "steps": args.n,
        "success": result.success,
        "total_ms": elapsed_s * 1000,
        "ms_per_step": elapsed_s * 1000 / args.n,
        "rss_mb": used_mb,
    }


def bench_parallel_scaling(args: argparse.Namespace) -> dict:
    """Time ``n`` sleeping assets executed across a fixed worker pool."""
    budget = Budget(args.budget)
    os.environ["BENCH_N_SLEEP"] = str(args.n)
    os.environ["BENCH_SLEEP_MS"] = str(PARALLEL_SLEEP_S * 1000)

    from bench.drivers import dagster_defs

    job = dg.reconstructable(dagster_defs.parallel_job)
    config = {"execution": {"config": {"max_concurrent": PARALLEL_WORKERS}}}

    with tempfile.TemporaryDirectory(
        prefix=TMP_PREFIX, ignore_cleanup_errors=True
    ) as tmpdir:
        instance = _local_instance(tmpdir)
        dg.execute_job(job, instance=instance, run_config=config)  # warm-up
        budget.check()
        t0 = time.perf_counter()
        result = dg.execute_job(job, instance=instance, run_config=config)
        elapsed_s = time.perf_counter() - t0
        budget.check()

        verdict = parallel_verdict(elapsed_s, args.n)
        verdict["success"] = result.success
        verdict["rss_mb"] = rss_mb()
        instance.dispose()
    return verdict


def bench_read_path(args: argparse.Namespace) -> dict:
    """Time the queries a UI page makes against a store holding ``n`` runs.

    Seeding is not timed and not compared: runs are written straight into the
    store rather than executed, which is far cheaper than running them. What is
    compared is answering the same questions afterwards, at the same volume.
    """
    from dagster._core.test_utils import create_run_for_test

    budget = Budget(args.budget)
    with tempfile.TemporaryDirectory(
        prefix=TMP_PREFIX, ignore_cleanup_errors=True
    ) as tmpdir:
        instance = _local_instance(tmpdir)
        key = dg.AssetKey("asset_0")
        for _ in range(args.n):
            create_run_for_test(instance, job_name="noop_job")
            instance.report_runless_asset_event(dg.AssetMaterialization(asset_key=key))
            budget.check()

        def timed(call) -> float:
            started = time.perf_counter()
            call()
            return time.perf_counter() - started

        timings = {
            "recent_runs": timed(lambda: instance.get_run_records(limit=100)),
            "latest_materialization": timed(
                lambda: instance.get_latest_materialization_events([key])
            ),
            "asset_records": timed(lambda: instance.get_asset_records([key])),
            "asset_events": timed(
                lambda: instance.fetch_materializations(key, limit=100)
            ),
        }
        used_mb = rss_mb()
        instance.dispose()

    return {"seeded_runs": args.n, **query_summary(timings), "rss_mb": used_mb}


def bench_queue_limits(args: argparse.Namespace) -> dict:
    """Contend ``n`` claims against a pool of ``QUEUE_SLOTS`` slots.

    Every claimant waits for a slot rather than giving up on the first refusal,
    because Prefect queues and the other two do not. A claim that never lands
    inside ``CLAIM_TIMEOUT_S`` is the refusal.
    """
    budget = Budget(args.budget)
    with tempfile.TemporaryDirectory(
        prefix=TMP_PREFIX, ignore_cleanup_errors=True
    ) as tmpdir:
        instance = _local_instance(tmpdir)
        storage = instance.event_log_storage
        storage.set_concurrency_slots(POOL_KEY, QUEUE_SLOTS)

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
                status = storage.claim_concurrency_slot(POOL_KEY, run_id, "step")
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
            storage.free_concurrency_slot_for_step(run_id, "step")

        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=QUEUE_SLOTS * 2) as pool:
            list(pool.map(claimant, range(args.n)))
        elapsed_s = time.perf_counter() - t0
        budget.check()

        verdict = claim_verdict(elapsed_s, granted, refused, QUEUE_SLOTS, max_held)
        verdict["rss_mb"] = rss_mb()
        instance.dispose()
    return verdict


def _launch_and_wait(instance, request_ctx, job_name: str, budget: Budget) -> str:
    """Launch ``job_name`` through the run launcher and wait for it to finish."""
    location = request_ctx.get_code_location("bench")
    repository = next(iter(location.get_repositories().values()))
    remote_job = repository.get_full_job(job_name)

    run = instance.create_run(
        job_name=job_name,
        run_id=None,
        run_config={},
        resolved_op_selection=None,
        step_keys_to_execute=None,
        status=None,
        tags={},
        root_run_id=None,
        parent_run_id=None,
        job_snapshot=remote_job.job_snapshot,
        execution_plan_snapshot=None,
        parent_job_snapshot=remote_job.parent_job_snapshot,
        remote_job_origin=remote_job.get_remote_origin(),
        job_code_origin=remote_job.get_python_origin(),
        asset_selection=None,
        op_selection=None,
        asset_check_selection=None,
        asset_graph=request_ctx.asset_graph,
    )
    instance.launch_run(run.run_id, request_ctx)
    while not instance.get_run_by_id(run.run_id).is_finished:
        budget.check()
        time.sleep(RUN_POLL_S)
    return run.run_id


def bench_log_capture(args: argparse.Namespace) -> dict:
    """Time one run whose step writes ``n`` log lines, then read them back.

    The run is launched rather than executed in this process, because Dagster
    persists step output only for a launched run. rivers' driver launches its
    run through its own queue for the same reason, so both pay a launch.
    """
    from dagster._core.storage.compute_log_manager import ComputeIOType

    budget = Budget(args.budget)
    os.environ["BENCH_N_LINES"] = str(args.n)
    os.environ["BENCH_LOG_LINE"] = LOG_LINE
    os.environ["BENCH_N_ASSETS"] = "0"

    with tempfile.TemporaryDirectory(
        prefix=TMP_PREFIX, ignore_cleanup_errors=True
    ) as tmpdir:
        with _launcher_instance(tmpdir) as instance:
            with _workspace(instance) as workspace_ctx:
                request_ctx = workspace_ctx.create_request_context()

                def timed_run(job: str) -> tuple[float, str]:
                    started = time.perf_counter()
                    run = _launch_and_wait(instance, request_ctx, job, budget)
                    return time.perf_counter() - started, run

                # `noop_job` is one op writing nothing, launched exactly as
                # `log_job` is, so subtracting it leaves the per-line cost. It
                # stands in for `log_job` at zero lines because the line count
                # is read from the environment when the code location loads,
                # which has already happened by here.
                timed_run("noop_job")  # warm-up, discarded
                launch_s, _ = timed_run("noop_job")

                write_s, run_id = timed_run("log_job")

                # The capture is filed under a generated key, not the step
                # name, so it has to be discovered rather than constructed.
                manager = instance.compute_log_manager
                t0 = time.perf_counter()
                keys = manager.get_log_keys_for_log_key_prefix(
                    [run_id, "compute_logs"], ComputeIOType.STDOUT
                )
                captured = 0
                for key in keys:
                    data = manager.get_log_data(key)
                    captured += len((data.stdout or b"").decode().splitlines())
                read_s = time.perf_counter() - t0
                used_mb = rss_mb()

    return log_verdict(args.n, captured, write_s, read_s, launch_s, rss_mb=used_mb)


def bench_backfill_drain(args: argparse.Namespace) -> dict:
    """Time a backfill over ``n`` partitions until every partition is terminal.

    The backfill daemon is driven in a tight loop rather than on its own wake
    cycle, so the number reflects what Dagster can do rather than how often it
    chooses to look.
    """
    from dagster._core.execution.backfill import BulkActionStatus, PartitionBackfill
    from dagster._daemon.backfill import execute_backfill_iteration
    from dagster._daemon.run_coordinator.queued_run_coordinator_daemon import (
        QueuedRunCoordinatorDaemon,
    )

    terminal_statuses = (
        BulkActionStatus.COMPLETED,
        BulkActionStatus.COMPLETED_SUCCESS,
        BulkActionStatus.COMPLETED_FAILED,
        BulkActionStatus.FAILED,
        BulkActionStatus.CANCELED,
    )
    budget = Budget(args.budget)
    os.environ["BENCH_N_PARTITIONS"] = str(args.n)
    os.environ["BENCH_N_ASSETS"] = "0"
    logger = logging.getLogger("BackfillDaemon")
    logger.setLevel(logging.CRITICAL)

    keys = [f"key_{i:08d}" for i in range(args.n)]
    with tempfile.TemporaryDirectory(
        prefix=TMP_PREFIX, ignore_cleanup_errors=True
    ) as tmpdir:
        with _launcher_instance(tmpdir) as instance:
            with _workspace(instance) as workspace_ctx:
                request_ctx = workspace_ctx.create_request_context()
                asset_graph = request_ctx.asset_graph
                backfill = PartitionBackfill.from_asset_partitions(
                    backfill_id="bench",
                    asset_graph=asset_graph,
                    partition_names=keys,
                    asset_selection=[dg.AssetKey("partitioned")],
                    backfill_timestamp=time.time(),
                    tags={},
                    dynamic_partitions_store=instance,
                    all_partitions=False,
                    title=None,
                    description=None,
                    run_config=None,
                )
                instance.add_backfill(backfill)
                budget.check()

                # Two daemons, not one. The backfill daemon only submits runs;
                # a submitted run sits queued until the run-queue daemon
                # dequeues and launches it, so both have to be driven or the
                # backfill never finishes.
                queue_daemon = QueuedRunCoordinatorDaemon(interval_seconds=0)
                t0 = time.perf_counter()
                status = None
                while status not in terminal_statuses:
                    budget.check()
                    list(execute_backfill_iteration(workspace_ctx, logger))
                    list(queue_daemon.run_iteration(workspace_ctx))
                    current = instance.get_backfill("bench")
                    status = current.status if current else None
                elapsed_s = time.perf_counter() - t0

                runs = instance.get_runs(
                    filters=dg.RunsFilter(tags={"dagster/backfill": "bench"})
                )
                used_mb = rss_mb()

    return {
        "partitions": args.n,
        "runs": len(runs),
        "completed": sum(1 for r in runs if r.status == dg.DagsterRunStatus.SUCCESS),
        "failed": sum(1 for r in runs if r.status == dg.DagsterRunStatus.FAILURE),
        "backfill_status": str(status),
        "total_ms": elapsed_s * 1000,
        "ms_per_partition": elapsed_s * 1000 / args.n,
        "partitions_per_s": args.n / elapsed_s if elapsed_s else 0.0,
        "rss_mb": used_mb,
    }


def bench_condition_pass(args: argparse.Namespace) -> dict:
    """Measure whether Dagster can evaluate every one of ``n`` conditions each interval.

    Every asset carries ``AutomationCondition.missing()``, the same condition
    rivers is given. `AutomationTickEvaluationContext.evaluate` is Dagster's own
    pass over the whole graph; it is driven in a tight loop held to the same
    interval, so the result reflects what Dagster can do rather than how often
    its daemon chooses to wake.

    Each pass then persists its evaluations, which is what Dagster's asset
    daemon does with them, and a pass is counted from those durable records.
    rivers is counted on tick records it wrote to storage, so counting Dagster
    on an in-memory integer would judge one side on a write and the other on
    arithmetic.

    Both evaluators are also put in the same steady state. rivers materializes
    every asset but the canary on its first pass, so ``missing()`` then
    requests exactly one run per pass. Dagster launches nothing here, so its
    assets would stay missing forever and it would build ``n`` run requests
    every pass against rivers' one. Reporting the materializations up front
    fixes that, and the canary keeps one asset missing on both sides.

    One difference remains and cannot be closed: ``evaluate`` returns its one
    run request instead of launching it, because Dagster exposes no way to
    include the dispatch without running its whole asset daemon.
    """
    from dagster._core.definitions.asset_daemon_cursor import AssetDaemonCursor
    from dagster._daemon.asset_daemon import AutomationTickEvaluationContext

    budget = Budget(args.budget)
    logger = logging.getLogger("AssetDaemon")
    logger.setLevel(logging.CRITICAL)

    def build(i: int):
        @dg.asset(
            name=f"asset_{i}",
            automation_condition=dg.AutomationCondition.missing(),
        )
        def _asset() -> int:
            return i

        return _asset

    def build_canary():
        @dg.asset(
            name=CANARY_ASSET,
            automation_condition=dg.AutomationCondition.missing(),
        )
        def _canary() -> int:
            raise RuntimeError("stays missing on purpose")

        return _canary

    materialized = max(args.n - 1, 1)
    assets = [build(i) for i in range(materialized)]
    assets.append(build_canary())
    asset_graph = dg.Definitions(assets=assets).resolve_asset_graph()

    with tempfile.TemporaryDirectory(
        prefix=TMP_PREFIX, ignore_cleanup_errors=True
    ) as tmpdir:
        instance = _local_instance(tmpdir)
        # A runless event, because nothing is launched here. It is the same
        # state rivers reaches by actually materializing them.
        for i in range(materialized):
            instance.report_runless_asset_event(
                dg.AssetMaterialization(asset_key=dg.AssetKey(f"asset_{i}"))
            )
        budget.check()
        state = {"passes": 0, "cursor": AssetDaemonCursor.empty(), "evaluation": 1}

        def one_pass() -> None:
            context = AutomationTickEvaluationContext(
                evaluation_id=state["evaluation"],
                instance=instance,
                asset_graph=asset_graph,
                cursor=state["cursor"],
                materialize_run_tags={},
                observe_run_tags={},
                auto_observe_asset_keys=set(),
                asset_selection=dg.AssetSelection.all(),
                logger=logger,
                emit_backfills=False,
            )
            _requests, cursor, evaluations = context.evaluate()
            # `evaluate` returns evaluations without run ids; storage wants
            # them attached, which is what the daemon does once it has
            # launched. Nothing is launched here, so the set is empty.
            instance.schedule_storage.add_auto_materialize_asset_evaluations(
                state["evaluation"],
                [e.with_run_ids(frozenset()) for e in evaluations],
            )
            state["cursor"] = cursor
            state["evaluation"] += 1
            state["passes"] += 1

        one_pass()  # warm-up, discarded
        budget.check()

        stop = threading.Event()

        def drive() -> None:
            while not stop.is_set():
                started = time.monotonic()
                one_pass()
                # Hold the same interval rivers' daemon is held to, so the
                # comparison is about the cost of a pass and not about how
                # often each side is willing to run one.
                remaining = CONDITION_INTERVAL_S - (time.monotonic() - started)
                if remaining > 0:
                    stop.wait(remaining)

        # The canary is what ticks every pass, on both sides.
        probe = dg.AssetKey(CANARY_ASSET)

        def evaluation_counts() -> list[int]:
            return [
                len(
                    instance.schedule_storage.get_auto_materialize_asset_evaluations(
                        key=probe, limit=1_000_000
                    )
                )
            ]

        driver = threading.Thread(target=drive, daemon=True)
        driver.start()
        deltas, window_s = counted_window(evaluation_counts, args.duration)
        stop.set()
        driver.join(timeout=30)
        used_mb = rss_mb()
        instance.dispose()

    result = pass_verdict(deltas, window_s, CONDITION_INTERVAL_S, args.n)
    result["assets"] = args.n
    result["dispatch_per_pass"] = 0
    result["rss_mb"] = used_mb
    return result


def bench_partition_ops(args: argparse.Namespace) -> dict:
    """Time the operations a partitioned asset pays for once it holds ``n`` keys."""
    budget = Budget(args.budget)
    keys = [f"key_{i:08d}" for i in range(args.n)]
    parts = dg.StaticPartitionsDefinition(keys)

    @dg.asset(name="partitioned", partitions_def=parts)
    def partitioned() -> int:
        return 1

    key = dg.AssetKey("partitioned")
    target = keys[args.n // 2]

    with tempfile.TemporaryDirectory(
        prefix=TMP_PREFIX, ignore_cleanup_errors=True
    ) as tmpdir:
        instance = _local_instance(tmpdir)
        dg.materialize(
            [partitioned], instance=instance, partition_key=target
        )  # warm-up
        budget.check()

        def timed(call) -> float:
            started = time.perf_counter()
            call()
            return time.perf_counter() - started

        materialize_s = timed(
            lambda: dg.materialize(
                [partitioned], instance=instance, partition_key=target
            )
        )
        materialized = instance.get_materialized_partitions(key)
        count_s = timed(lambda: len(instance.get_materialized_partitions(key)))
        list_s = timed(lambda: instance.get_materialized_partitions(key))
        dynamic_s = timed(
            lambda: instance.add_dynamic_partitions(
                DYNAMIC_DEF, [f"dyn_{i:08d}" for i in range(DYNAMIC_KEYS)]
            )
        )
        budget.check()
        used_mb = rss_mb()
        instance.dispose()

    return {
        "partitions": args.n,
        "materialized": len(materialized),
        "materialize_one_ms": materialize_s * 1000,
        "count_ms": count_s * 1000,
        "list_ms": list_s * 1000,
        "add_dynamic_ms": dynamic_s * 1000,
        "dynamic_keys": DYNAMIC_KEYS,
        "total_ms": (materialize_s + count_s + list_s) * 1000,
        "rss_mb": used_mb,
    }


def bench_schedule_tick(args: argparse.Namespace) -> dict:
    """Time evaluating every one of ``n`` schedules once."""
    budget = Budget(args.budget)

    def build(i: int):
        return dg.ScheduleDefinition(
            name=f"schedule_{i}",
            cron_schedule=SCHEDULE_CRON,
            job_name="noop_job",
            execution_fn=lambda context: dg.SkipReason("noop"),
        )

    schedules = [build(i) for i in range(args.n)]

    with tempfile.TemporaryDirectory(
        prefix=TMP_PREFIX, ignore_cleanup_errors=True
    ) as tmpdir:
        instance = _local_instance(tmpdir)
        context = dg.build_schedule_context(instance=instance)
        budget.check()

        schedules[0].evaluate_tick(context)  # warm-up, discarded
        t0 = time.perf_counter()
        for schedule in schedules:
            schedule.evaluate_tick(context)
        elapsed_s = time.perf_counter() - t0
        budget.check()
        used_mb = rss_mb()
        instance.dispose()

    return {
        "schedules": args.n,
        "pass_ms": elapsed_s * 1000,
        "total_ms": elapsed_s * 1000,
        "ms_per_schedule": elapsed_s * 1000 / args.n,
        "rss_mb": used_mb,
    }


def bench_cancel_latency(args: argparse.Namespace) -> dict:
    """Time a launched run from cancellation request to a terminal state."""
    budget = Budget(args.budget)
    os.environ["BENCH_CANCEL_SLEEP_S"] = str(CANCEL_SLEEP_S)
    os.environ["BENCH_N_ASSETS"] = "0"

    with tempfile.TemporaryDirectory(
        prefix=TMP_PREFIX, ignore_cleanup_errors=True
    ) as tmpdir:
        with _launcher_instance(tmpdir) as instance:
            with _workspace(instance) as workspace_ctx:
                request_ctx = workspace_ctx.create_request_context()
                location = request_ctx.get_code_location("bench")
                repository = next(iter(location.get_repositories().values()))
                remote_job = repository.get_full_job("cancel_job")

                run = instance.create_run(
                    job_name="cancel_job",
                    run_id=None,
                    run_config={},
                    resolved_op_selection=None,
                    step_keys_to_execute=None,
                    status=None,
                    tags={},
                    root_run_id=None,
                    parent_run_id=None,
                    job_snapshot=remote_job.job_snapshot,
                    execution_plan_snapshot=None,
                    parent_job_snapshot=remote_job.parent_job_snapshot,
                    remote_job_origin=remote_job.get_remote_origin(),
                    job_code_origin=remote_job.get_python_origin(),
                    asset_selection=None,
                    op_selection=None,
                    asset_check_selection=None,
                    asset_graph=request_ctx.asset_graph,
                )
                instance.launch_run(run.run_id, request_ctx)

                while (
                    instance.get_run_by_id(run.run_id).status
                    != dg.DagsterRunStatus.STARTED
                ):
                    budget.check()
                    time.sleep(CANCEL_POLL_S)
                started_status = str(instance.get_run_by_id(run.run_id).status)

                t0 = time.perf_counter()
                instance.run_launcher.terminate(run.run_id)
                terminal = None
                while terminal is None:
                    budget.check()
                    current = instance.get_run_by_id(run.run_id)
                    if current.is_finished:
                        terminal = str(current.status)
                        break
                    time.sleep(CANCEL_POLL_S)
                elapsed_s = time.perf_counter() - t0
                used_mb = rss_mb()

    return {
        "status_at_request": started_status,
        "terminal_status": terminal,
        "cancel_ms": elapsed_s * 1000,
        "rss_mb": used_mb,
    }


def bench_reload(args: argparse.Namespace) -> dict:
    """Time building the code-location snapshot a second time, in a warm process.

    A reload rebuilds the definitions, so the assets are rebuilt inside the
    timer here, the same way the other two drivers do it. Dagster keeps nothing
    between loads, so its second load repeats the first.
    """
    budget = Budget(args.budget)

    def load() -> int:
        snap = RepositorySnap.from_def(
            dg.Definitions(assets=make_assets(args.n)).get_repository_def()
        )
        return len(snap.asset_nodes)

    t0 = time.perf_counter()
    load()
    first_s = time.perf_counter() - t0
    budget.check()

    t0 = time.perf_counter()
    loaded = load()
    reload_s = time.perf_counter() - t0
    budget.check()

    return {
        "assets": args.n,
        "loaded_assets": loaded,
        "first_load_ms": first_s * 1000,
        "total_ms": reload_s * 1000,
        "reload_ratio": reload_s / first_s if first_s else 0.0,
        "rss_mb": rss_mb(),
    }


COLD_START_SCRIPT = """
import os
import dagster as dg
from dagster._core.remote_representation.external_data import RepositorySnap

n = int(os.environ["BENCH_N_ASSETS"])


def build(i):
    @dg.asset(name="asset_{}".format(i))
    def _asset() -> int:
        return i

    return _asset


assets = [build(i) for i in range(n)]
defs = dg.Definitions(assets=assets)
snap = RepositorySnap.from_def(defs.get_repository_def())
# Servable: the code location can now answer what assets it holds.
assert len(snap.asset_nodes) == n
"""


if __name__ == "__main__":
    main(
        "dagster",
        {
            "graph_load": bench_graph_load,
            "run_latency": bench_run_latency,
            "run_throughput": bench_run_throughput,
            "sensor_pass": bench_sensor_pass,
            "partitions": bench_partitions,
            "cold_start": cold_start_benchmark(COLD_START_SCRIPT, QUIET_ENV),
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
