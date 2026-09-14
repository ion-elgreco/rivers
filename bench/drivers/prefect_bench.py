"""Prefect side of the comparison benchmarks. Runs in `.venv-bench`.

Prefect has no sensor or partition concept, so those benchmarks report
``unsupported`` rather than a number. Its asset model (``@materialize``) is
task-scoped, which the asset benchmarks use.
"""

from __future__ import annotations

import argparse
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
    main,
    log_verdict,
    parallel_verdict,
    query_summary,
    quiet_env,
    rss_mb,
    sample,
    summarize,
    throughput,
    unsupported,
)

# Also handed to the cold-start child, which does not inherit this process.
QUIET_ENV = {
    "PREFECT_SERVER_ALLOW_EPHEMERAL_MODE": "true",
    "PREFECT_LOGGING_LEVEL": "ERROR",
    "PREFECT_LOGGING_SERVER_LEVEL": "ERROR",
}
# Prefect keeps its database in ~/.prefect by default, so every measurement
# would inherit the state left by the previous one. rivers and Dagster each get
# a fresh temporary store, so Prefect gets one too.
if "PREFECT_HOME" not in os.environ:
    QUIET_ENV["PREFECT_HOME"] = tempfile.mkdtemp(prefix="prefect_cmp_")
quiet_env(QUIET_ENV)

# Import the framework only after quiet_env(), so its logging setup reads
# the silenced configuration and cannot distort the measurement.
from prefect import flow, get_run_logger, task  # noqa: E402
from prefect.assets import materialize  # noqa: E402

# These match the other two drivers exactly. A shape that differs by framework
# invalidates the comparison, so changing one means changing all three.
POOL_KEY = "bench_pool"
SLOT_HOLD_S = 0.001
# Prefect queues a claimant rather than refusing it, so a wait this long is
# what counts as a refusal here.
CLAIM_TIMEOUT_S = 30.0
# How often the log benchmark asks whether the shipped lines have landed.
LOG_POLL_S = 0.05
# Prefect's API rejects a page larger than its own default limit, so reading a
# run's logs back means paging rather than asking for all of them at once.
LOG_PAGE_SIZE = 200
# How often the step benchmark asks whether the run has been recorded yet.
# Slower than the log poll because each check is a server-side count over a
# growing table, and polling it hard competes with the writer being waited on.
STEP_POLL_S = 0.25


def recorded_counter(client, flow_run_ids: list[str]):
    """A callable counting the task runs Prefect has recorded for these runs.

    Prefect records task-run state from a background worker, so a flow call
    returns before any of it exists. Every benchmark that times a run has to
    wait for this, or it times the part Prefect does in the foreground and
    stops the clock on the rest. rivers and Dagster have finished writing
    before their run returns.

    The count endpoint rather than reading the rows back: this is asked
    repeatedly while the writer is still working, and paging the rows would
    compete with it for the same SQLite file.
    """
    query = {"flow_runs": {"id": {"any_": flow_run_ids}}}
    return lambda: int(client._client.post("/task_runs/count", json=query).text)


def wait_for_recorded(count, expected: int, budget: Budget) -> int:
    """Block until Prefect has recorded ``expected`` task runs."""
    written = count()
    while written < expected:
        budget.check()
        time.sleep(STEP_POLL_S)
        written = count()
    return written


def make_asset_tasks(n: int) -> list:
    """Build ``n`` no-op materializing tasks, one asset each."""

    def build(i: int):
        @materialize(f"s3://bench/asset_{i}", name=f"asset_{i}")
        def _asset() -> int:
            return i

        return _asset

    return [build(i) for i in range(n)]


def _single_task_flow():
    """A flow wrapping one no-op materializing task."""
    single = make_asset_tasks(1)[0]

    @flow(name="bench_single", log_prints=False)
    def bench_single():
        return single()

    return bench_single


def bench_graph_load(args: argparse.Namespace) -> dict:
    """Time defining ``n`` asset tasks and building a servable flow.

    Prefect resolves nothing ahead of run time — there is no graph to compile,
    so ``load_ms`` covers building the flow object the worker executes.
    """
    budget = Budget(args.budget)
    t0 = time.perf_counter()
    tasks = make_asset_tasks(args.n)
    define_s = time.perf_counter() - t0

    t0 = time.perf_counter()

    @flow(name="bench_flow")
    def bench_flow():
        return [t() for t in tasks]

    construct_s = time.perf_counter() - t0

    t0 = time.perf_counter()
    bench_flow.to_deployment(name="bench")
    load_s = time.perf_counter() - t0
    budget.check()

    return {
        "define_ms": define_s * 1000,
        "construct_ms": construct_s * 1000,
        "load_ms": load_s * 1000,
        "total_ms": (define_s + construct_s + load_s) * 1000,
        "rss_mb": rss_mb(),
    }


def bench_run_latency(args: argparse.Namespace) -> dict:
    """Time end-to-end execution of a flow with a single no-op task, repeated.

    A sample ends when the run is recorded, not when the call returns. See
    :func:`recorded_counter` for why those are not the same moment.
    """
    from prefect.client.orchestration import get_client

    budget = Budget(args.budget)
    run = _single_task_flow()
    client = get_client(sync_client=True)

    def run_and_settle() -> None:
        state = run(return_state=True)
        flow_run_id = str(state.state_details.flow_run_id)
        wait_for_recorded(recorded_counter(client, [flow_run_id]), 1, budget)

    # The first call pays for API/server startup; report it, do not fold it in.
    t0 = time.perf_counter()
    run_and_settle()
    startup_s = time.perf_counter() - t0

    return {
        "latency": summarize(sample(run_and_settle, args.iterations, budget)),
        "first_run_ms": startup_s * 1000,
        "api_url": os.environ.get("PREFECT_API_URL", "<ephemeral>"),
        "rss_mb": rss_mb(),
    }


def bench_run_throughput(args: argparse.Namespace) -> dict:
    """Execute ``n`` flow runs back to back and report runs per second.

    Each run waits to be recorded before the next starts, which is the state
    rivers and Dagster reach before their own call returns.
    """
    from prefect.client.orchestration import get_client

    budget = Budget(args.budget)
    run = _single_task_flow()
    client = get_client(sync_client=True)

    def run_and_settle() -> None:
        state = run(return_state=True)
        flow_run_id = str(state.state_details.flow_run_id)
        wait_for_recorded(recorded_counter(client, [flow_run_id]), 1, budget)

    run_and_settle()  # warm-up, discarded
    return {**throughput(run_and_settle, args.n, budget), "rss_mb": rss_mb()}


def make_dep_tasks(n: int, fan_in: int = 2) -> list:
    """Build ``n`` materializing tasks in layers, each naming ``fan_in`` upstream assets.

    Prefect builds no graph before a run, so the dependencies are declared as
    upstream asset keys rather than resolved into edges. Its figure is the cost
    of defining the tasks and building the flow, same as ``graph_load``.
    """
    width = max(int(n**0.5), 1)

    def build(i: int):
        upstream = [f"s3://bench/asset_{j}" for j in range(max(i - width, 0), i)][
            :fan_in
        ]

        @materialize(f"s3://bench/asset_{i}", name=f"asset_{i}", asset_deps=upstream)
        def _asset() -> int:
            return i

        return _asset

    return [build(i) for i in range(n)]


def bench_graph_deps(args: argparse.Namespace) -> dict:
    """Time defining ``n`` asset tasks that declare dependencies, and building the flow."""
    budget = Budget(args.budget)
    t0 = time.perf_counter()
    tasks = make_dep_tasks(args.n)
    define_s = time.perf_counter() - t0

    t0 = time.perf_counter()

    @flow(name="bench_deps_flow")
    def bench_deps_flow():
        return [t() for t in tasks]

    construct_s = time.perf_counter() - t0

    t0 = time.perf_counter()
    bench_deps_flow.to_deployment(name="bench")
    load_s = time.perf_counter() - t0
    budget.check()

    return {
        "define_ms": define_s * 1000,
        "construct_ms": construct_s * 1000,
        "load_ms": load_s * 1000,
        "total_ms": (define_s + construct_s + load_s) * 1000,
        "rss_mb": rss_mb(),
    }


def bench_run_steps(args: argparse.Namespace) -> dict:
    """Time one flow run that executes ``n`` tasks, and report the per-step cost.

    The wait for Prefect to record the run is part of the cost. At 5,000 tasks
    none of the 5,000 runs had reached the API when the call returned, so
    without it Prefect's per-step figure *falls* as the run grows. See
    :func:`recorded_counter`.
    """
    from prefect.client.orchestration import get_client

    budget = Budget(args.budget)
    tasks = make_asset_tasks(args.n)

    @flow(name="bench_steps", log_prints=False)
    def bench_steps():
        return [t() for t in tasks]

    bench_steps()  # warm-up, discarded
    budget.check()

    t0 = time.perf_counter()
    state = bench_steps(return_state=True)
    execute_s = time.perf_counter() - t0
    budget.check()

    client = get_client(sync_client=True)
    flow_run_id = str(state.state_details.flow_run_id)
    count = recorded_counter(client, [flow_run_id])

    t0 = time.perf_counter()
    written = wait_for_recorded(count, args.n, budget)
    persist_s = time.perf_counter() - t0

    elapsed_s = execute_s + persist_s
    return {
        "steps": args.n,
        "success": written == args.n,
        "recorded_runs": written,
        "execute_ms": execute_s * 1000,
        "persist_ms": persist_s * 1000,
        "total_ms": elapsed_s * 1000,
        "ms_per_step": elapsed_s * 1000 / args.n,
        "rss_mb": rss_mb(),
    }


@task(name="sleeper")
def sleeper(i: int) -> int:
    """One unit of the parallel benchmark's work, shared by every framework.

    Module level because `ProcessPoolTaskRunner` ships the task to a worker
    process, which cannot receive a closure. Dagster's driver moves its job
    into `dagster_defs` for the same reason.
    """
    time.sleep(PARALLEL_SLEEP_S)
    return i


def bench_parallel_scaling(args: argparse.Namespace) -> dict:
    """Time ``n`` sleeping tasks executed across a fixed worker pool.

    The pool runs processes, not threads. Threads would skip the pickling and
    the process start that rivers and Dagster both pay, and the column would
    compare an isolation model against none.
    """
    from prefect.task_runners import ProcessPoolTaskRunner

    budget = Budget(args.budget)

    @flow(
        name="bench_parallel",
        log_prints=False,
        task_runner=ProcessPoolTaskRunner(max_workers=PARALLEL_WORKERS),
    )
    def bench_parallel(count: int):
        futures = [sleeper.submit(i) for i in range(count)]
        return [f.result() for f in futures]

    bench_parallel(args.n)  # warm-up: the worker pool starts here
    budget.check()
    t0 = time.perf_counter()
    results = bench_parallel(args.n)
    elapsed_s = time.perf_counter() - t0
    budget.check()

    verdict = parallel_verdict(elapsed_s, args.n)
    # Every other driver reports what its run returned. Asserting success here
    # would hide a task that never ran.
    verdict["success"] = len(results) == args.n
    verdict["rss_mb"] = rss_mb()
    return verdict


def bench_read_path(args: argparse.Namespace) -> dict:
    """Time the queries a UI page makes against a store holding ``n`` runs.

    Seeding is not timed and not compared: flow runs are written straight
    through the client rather than executed. What is compared is answering the
    same questions afterwards, at the same data volume — so a task run is
    seeded per flow run too. Without it, two of these four queries read a
    single-row table while the other two frameworks read ``n`` rows.
    """
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.filters import FlowRunFilter
    from prefect.states import Completed

    budget = Budget(args.budget)
    run = _single_task_flow()
    run()  # one real run, so the flow exists server-side

    client = get_client(sync_client=True)
    flow_id = client.create_flow(run)
    task = make_asset_tasks(1)[0]
    for i in range(args.n):
        flow_run = client.create_flow_run(run, state=Completed())
        client.create_task_run(
            task=task,
            flow_run_id=flow_run.id,
            dynamic_key=str(i),
            state=Completed(),
        )
        budget.check()

    def timed(call) -> float:
        started = time.perf_counter()
        call()
        return time.perf_counter() - started

    timings = {
        "recent_runs": timed(lambda: client.read_flow_runs(limit=100)),
        "latest_materialization": timed(
            lambda: client.read_flow_runs(
                flow_run_filter=FlowRunFilter(), limit=1, sort="START_TIME_DESC"
            )
        ),
        # Prefect has no asset record to read. Its task runs are the closest
        # per-unit rows it keeps, and there are now ``n`` of them, so this
        # reads at the volume the other two do.
        "asset_records": timed(lambda: client.read_task_runs(limit=100)),
        "asset_events": timed(lambda: client.read_task_runs(limit=100)),
    }
    return {
        "seeded_runs": args.n,
        "flow_id": str(flow_id),
        **query_summary(timings),
        "rss_mb": rss_mb(),
    }


def bench_queue_limits(args: argparse.Namespace) -> dict:
    """Contend ``n`` claims against a pool of ``QUEUE_SLOTS`` slots.

    Prefect queues a claimant that cannot be served instead of refusing it, so
    its ``refused`` count stays at zero while the other two report refusals.
    The rate and the held-slot check still compare.
    """
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
    from prefect.concurrency.sync import concurrency

    budget = Budget(args.budget)
    client = get_client(sync_client=True)
    client.create_global_concurrency_limit(
        GlobalConcurrencyLimitCreate(name=POOL_KEY, limit=QUEUE_SLOTS)
    )
    # The first acquisition starts the ephemeral API server; that cost belongs
    # to startup, not to the claim rate.
    with concurrency(POOL_KEY, occupy=1, timeout_seconds=CLAIM_TIMEOUT_S):
        pass
    budget.check()

    held = 0
    max_held = 0
    granted = 0
    refused = 0
    lock = threading.Lock()

    def claimant(_index: int) -> None:
        nonlocal held, max_held, granted, refused
        try:
            with concurrency(POOL_KEY, occupy=1, timeout_seconds=CLAIM_TIMEOUT_S):
                with lock:
                    held += 1
                    granted += 1
                    max_held = max(max_held, held)
                time.sleep(SLOT_HOLD_S)
                # Drop the count before the slot is released, so a waiting
                # thread cannot be counted in while this one still is.
                with lock:
                    held -= 1
        except Exception:  # noqa: BLE001 - a timed-out wait is a refusal here
            with lock:
                refused += 1

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=QUEUE_SLOTS * 2) as pool:
        list(pool.map(claimant, range(args.n)))
    elapsed_s = time.perf_counter() - t0
    budget.check()

    verdict = claim_verdict(elapsed_s, granted, refused, QUEUE_SLOTS, max_held)
    verdict["rss_mb"] = rss_mb()
    return verdict


def bench_log_capture(args: argparse.Namespace) -> dict:
    """Time one flow run that writes ``n`` log lines, then read them back.

    Prefect ships logs to its API from a background worker, so a line is not
    readable the moment the run ends. ``read_ms`` therefore covers waiting for
    every line to become readable, which is the state the other two reach
    before their run returns.
    """
    import logging

    from prefect.client.orchestration import get_client
    from prefect.client.schemas.filters import LogFilter, LogFilterFlowRunId
    from prefect.logging.configuration import setup_logging
    from prefect.settings import PREFECT_LOGGING_LEVEL, temporary_settings

    budget = Budget(args.budget)
    lines = args.n
    to_write = 0

    @flow(name="bench_logs", log_prints=False)
    def bench_logs():
        logger = get_run_logger()
        for _ in range(to_write):
            logger.info(LOG_LINE)
        return 1

    # Prefect's log capture runs through the logging module, which the harness
    # silences everywhere else so framework chatter cannot distort a
    # measurement. Here the logging is the measurement, so it is turned back on
    # for this benchmark only.
    logging.disable(logging.NOTSET)
    with temporary_settings({PREFECT_LOGGING_LEVEL: "INFO"}):
        setup_logging()

        # The first flow run in a process starts the ephemeral API server.
        # `run_latency` reports that cost on its own; it does not belong here.
        _single_task_flow()()
        budget.check()

        # The same flow writing nothing, so the launch can be subtracted from
        # the figure that carries the column.
        t0 = time.perf_counter()
        bench_logs(return_state=True)
        launch_s = time.perf_counter() - t0
        budget.check()

        to_write = lines
        t0 = time.perf_counter()
        state = bench_logs(return_state=True)
        write_s = time.perf_counter() - t0
        budget.check()

        client = get_client(sync_client=True)
        flow_run_id = state.state_details.flow_run_id
        log_filter = LogFilter(flow_run_id=LogFilterFlowRunId(any_=[flow_run_id]))

        def count_captured() -> int:
            """Page through this run's logs and count the lines it wrote."""
            found = 0
            offset = 0
            while True:
                page = client.read_logs(
                    log_filter=log_filter, limit=LOG_PAGE_SIZE, offset=offset
                )
                if not page:
                    return found
                found += sum(1 for entry in page if entry.message == LOG_LINE)
                offset += len(page)

        t0 = time.perf_counter()
        captured = 0
        while captured < lines:
            budget.check()
            captured = count_captured()
            if captured >= lines:
                break
            time.sleep(LOG_POLL_S)
        read_s = time.perf_counter() - t0
    logging.disable(logging.INFO)

    return log_verdict(lines, captured, write_s, read_s, launch_s, rss_mb=rss_mb())


def bench_reload(args: argparse.Namespace) -> dict:
    """Time loading ``n`` assets a second time, in a warm process.

    A code location is loaded once at startup and again on every change, so a
    reload rebuilds the definitions — all three drivers do that here. Calling
    ``to_deployment`` twice on an already-built flow does not: it is the same
    work whatever ``n`` is, which is why this used to report 0.2 ms at every
    size. Prefect keeps nothing between loads, so its second load repeats the
    first and its ratio sits near 1.
    """
    budget = Budget(args.budget)

    def load() -> int:
        tasks = make_asset_tasks(args.n)

        @flow(name="bench_reload_flow")
        def bench_reload_flow():
            return [t() for t in tasks]

        bench_reload_flow.to_deployment(name="bench")
        return len(tasks)

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
from prefect import flow
from prefect.assets import materialize

n = int(os.environ["BENCH_N_ASSETS"])


def build(i):
    @materialize("s3://bench/asset_{}".format(i), name="asset_{}".format(i))
    def _asset() -> int:
        return i

    return _asset


tasks = [build(i) for i in range(n)]


@flow(name="bench_flow")
def bench_flow():
    return [t() for t in tasks]


deployment = bench_flow.to_deployment(name="bench")
# Servable: the flow is built and a deployment can be handed to a worker.
assert len(tasks) == n
assert deployment is not None
"""


if __name__ == "__main__":
    main(
        "prefect",
        {
            "graph_load": bench_graph_load,
            "run_latency": bench_run_latency,
            "run_throughput": bench_run_throughput,
            "sensor_pass": unsupported(
                "Prefect has no sensor; automations are server-side event triggers"
            ),
            "cold_start": cold_start_benchmark(COLD_START_SCRIPT, QUIET_ENV),
            "partitions": unsupported("Prefect has no partitioned asset concept"),
            "graph_deps": bench_graph_deps,
            "run_steps": bench_run_steps,
            "parallel_scaling": bench_parallel_scaling,
            "read_path": bench_read_path,
            "queue_limits": bench_queue_limits,
            "log_capture": bench_log_capture,
            "reload": bench_reload,
            "selection": unsupported(
                "Prefect builds no graph before a run, so there is no selection to resolve"
            ),
            "partition_ops": unsupported("Prefect has no partitioned asset concept"),
            "backfill_drain": unsupported(
                "Prefect has no backfill; a partitioned rerun has no equivalent"
            ),
            "condition_pass": unsupported(
                "Prefect has no declarative automation condition on an asset"
            ),
            "schedule_tick": unsupported(
                "Prefect schedules are server-side cron on a deployment, with no "
                "per-tick evaluation function to run"
            ),
            "cancel_latency": unsupported(
                "cancelling a Prefect run needs a deployment and a worker; there is "
                "no in-process equivalent"
            ),
        },
    )
