"""Thread-safety of execution contexts used from many threads in one step.

The deadlocks these tests catch happen on both builds, and on free-threaded
CPython (3.14t) a garbage-collection pause makes them much more likely.
"""

import gc
import json
import logging
import threading
import time

import pytest
from _threads import N_THREADS, run_in_child, run_threads

import rivers as rs


def materialize_asset(use_context):
    @rs.Asset
    def a(context: rs.AssetExecutionContext) -> int:
        use_context(context)
        return 1

    repo = rs.CodeRepository(assets=[a], default_executor=rs.Executor.in_process())
    repo.materialize()


def execute_task(use_context):
    @rs.Task
    def t(context: rs.TaskExecutionContext) -> int:
        use_context(context)
        return 1

    repo = rs.CodeRepository(
        assets=[],
        tasks=[t],
        jobs=[rs.Job(name="j", assets=[t])],
        default_executor=rs.Executor.in_process(),
    )
    repo.get_job("j").execute()


def run_asset_action(use_context):
    def touch(context: rs.ActionContext) -> None:
        use_context(context)

    @rs.Asset(
        actions=[rs.AssetAction(name="touch", outcome=rs.Outcome.Unchanged)(touch)]
    )
    def a() -> int:
        return 1

    repo = rs.CodeRepository(assets=[a], default_executor=rs.Executor.in_process())
    repo.run_action("touch")


def evaluate_sensor(use_context):
    @rs.Sensor
    def s(context: rs.SensorEvaluationContext):
        use_context(context)
        return rs.SkipReason("done")

    rs.CodeRepository(assets=[], sensors=[s]).evaluate_sensor("s")


def evaluate_schedule(use_context):
    @rs.Schedule(cron_schedule="* * * * *", job_name="j")
    def s(context: rs.ScheduleEvaluationContext):
        use_context(context)
        return rs.SkipReason("done")

    rs.CodeRepository(assets=[], schedules=[s]).evaluate_schedule("s")


CONTEXT_LOGGERS = {
    "asset": ("code-repo.assets.a", materialize_asset),
    "task": ("code-repo.tasks.t", execute_task),
    "action": ("code-repo.actions.a", run_asset_action),
    "sensor": ("code-repo.sensors.s", evaluate_sensor),
    "schedule": ("code-repo.schedules.s", evaluate_schedule),
}


def read_context_log_in_threads(kind):
    """Child process of ``test_context_log_first_read_in_threads``: all threads
    read ``context.log`` while the first read sleeps and collects garbage in
    ``logging.getLogger``."""
    get_logger = logging.getLogger

    def slow_get_logger(name=None):
        time.sleep(0.05)
        gc.collect()
        return get_logger(name)

    logging.getLogger = slow_get_logger
    reads = []

    def read_log(context):
        loggers, errors = run_threads(lambda i: context.log)
        reads.append(
            {
                "errors": errors,
                "names": [getattr(logger, "name", None) for logger in loggers],
                "objects": len({id(logger) for logger in loggers}),
            }
        )

    CONTEXT_LOGGERS[kind][1](read_log)
    print(json.dumps(reads))


@pytest.mark.parametrize("kind", CONTEXT_LOGGERS)
def test_context_log_first_read_in_threads(kind, tmp_path):
    """Threads that wait for the first ``context.log`` read must not block it."""
    proc = run_in_child(
        "concurrency.test_thread_safety_contexts:read_context_log_in_threads",
        kind,
        cwd=tmp_path,
    )

    assert proc.returncode == 0, proc.stderr
    name = CONTEXT_LOGGERS[kind][0]
    assert json.loads(proc.stdout.splitlines()[-1]) == [
        {"errors": [], "names": [name] * N_THREADS, "objects": 1}
    ]


def write_asset_output(use_context):
    class Handler(rs.BaseIOHandler):
        def handle_output(self, context, obj):
            use_context(context)

        def load_input(self, context):
            return None

    @rs.Asset(io_handler=Handler())
    def a() -> int:
        return 1

    repo = rs.CodeRepository(assets=[a], default_executor=rs.Executor.in_process())
    repo.materialize()


METADATA_CONTEXTS = {"asset": materialize_asset, "io": write_asset_output}


class SlowInt:
    """An int whose conversion sleeps and collects garbage."""

    def __init__(self, value):
        self.value = value

    def __index__(self):
        time.sleep(0.01)
        gc.collect()
        return self.value


def raw_output_metadata(context):
    return {k: v.raw_value() for k, v in (context.output_metadata or {}).items()}


def run_metadata_scenario(kind, scenario):
    """Run ``scenario(context)`` in a step and print its result as JSON."""
    out = {}
    METADATA_CONTEXTS[kind](lambda context: out.update(scenario(context)))
    print(json.dumps(out))


def add_metadata_in_threads(kind):
    """Child process of ``test_add_output_metadata_in_threads``."""

    def scenario(context):
        _, errors = run_threads(
            lambda i: context.add_output_metadata({f"k{i}": SlowInt(i)})
        )
        return {"errors": errors, "metadata": raw_output_metadata(context)}

    run_metadata_scenario(kind, scenario)


@pytest.mark.parametrize("kind", METADATA_CONTEXTS)
def test_add_output_metadata_in_threads(kind, tmp_path):
    """A value that is slow to convert must not block other threads that add
    metadata to the same context."""
    proc = run_in_child(
        "concurrency.test_thread_safety_contexts:add_metadata_in_threads",
        kind,
        cwd=tmp_path,
    )

    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout.splitlines()[-1]) == {
        "errors": [],
        "metadata": {f"k{i}": i for i in range(N_THREADS)},
    }


def add_and_read_metadata_in_threads(kind):
    """Child process of ``test_read_output_metadata_while_adding``."""

    def scenario(context):
        def add_then_read(i):
            for j in range(3):
                context.add_output_metadata({f"k{i}.{j}": SlowInt(j)})
                seen = raw_output_metadata(context)
                assert all(seen.get(f"k{i}.{m}") == m for m in range(j + 1)), seen

        _, errors = run_threads(add_then_read)
        return {"errors": errors, "metadata": raw_output_metadata(context)}

    run_metadata_scenario(kind, scenario)


@pytest.mark.parametrize("kind", METADATA_CONTEXTS)
def test_read_output_metadata_while_adding(kind, tmp_path):
    """``output_metadata`` reads and slow adds from many threads do not block
    each other, and each thread reads back the keys it added."""
    proc = run_in_child(
        "concurrency.test_thread_safety_contexts:add_and_read_metadata_in_threads",
        kind,
        cwd=tmp_path,
    )

    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout.splitlines()[-1]) == {
        "errors": [],
        "metadata": {f"k{i}.{j}": j for i in range(N_THREADS) for j in range(3)},
    }


def add_changing_dict_in_threads(kind):
    """Child process of ``test_add_output_metadata_from_a_changing_dict``."""

    def scenario(context):
        shared = {f"k{i}": i for i in range(100)}
        stop = threading.Event()

        def change_or_add(i):
            if i == 0:
                for n in range(100_000):
                    shared[f"x{n}"] = n
                    shared.pop(f"x{n - 1}", None)
                stop.set()
                return
            while True:
                context.add_output_metadata(shared)
                if stop.is_set():
                    return

        _, errors = run_threads(change_or_add)
        return {"errors": errors, "metadata": raw_output_metadata(context)}

    run_metadata_scenario(kind, scenario)


@pytest.mark.parametrize("kind", METADATA_CONTEXTS)
def test_add_output_metadata_from_a_changing_dict(kind, tmp_path):
    """Adding metadata from a dict that another thread changes adds a
    consistent copy of it and never panics."""
    proc = run_in_child(
        "concurrency.test_thread_safety_contexts:add_changing_dict_in_threads",
        kind,
        cwd=tmp_path,
    )

    assert proc.returncode == 0, proc.stderr
    result = json.loads(proc.stdout.splitlines()[-1])
    assert result["errors"] == []
    metadata = result["metadata"]
    assert {k: v for k, v in metadata.items() if k.startswith("k")} == {
        f"k{i}": i for i in range(100)
    }
    assert all(v == int(k[1:]) for k, v in metadata.items())
