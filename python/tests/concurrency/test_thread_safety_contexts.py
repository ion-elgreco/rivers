"""Thread-safety of execution contexts read from many threads in one step.

The deadlocks these tests catch happen on both builds, and on free-threaded
CPython (3.14t) a garbage-collection pause makes them much more likely.
"""

import gc
import json
import logging
import time

import pytest
from _threads import N_THREADS, run_in_child, run_threads

import rivers as rs


def materialize_asset(read_log):
    @rs.Asset
    def a(context: rs.AssetExecutionContext) -> int:
        read_log(context)
        return 1

    repo = rs.CodeRepository(assets=[a], default_executor=rs.Executor.in_process())
    repo.materialize()


def execute_task(read_log):
    @rs.Task
    def t(context: rs.TaskExecutionContext) -> int:
        read_log(context)
        return 1

    repo = rs.CodeRepository(
        assets=[],
        tasks=[t],
        jobs=[rs.Job(name="j", assets=[t])],
        default_executor=rs.Executor.in_process(),
    )
    repo.get_job("j").execute()


def run_asset_action(read_log):
    def touch(context: rs.ActionContext) -> None:
        read_log(context)

    @rs.Asset(
        actions=[rs.AssetAction(name="touch", outcome=rs.Outcome.Unchanged)(touch)]
    )
    def a() -> int:
        return 1

    repo = rs.CodeRepository(assets=[a], default_executor=rs.Executor.in_process())
    repo.run_action("touch")


def evaluate_sensor(read_log):
    @rs.Sensor
    def s(context: rs.SensorEvaluationContext):
        read_log(context)
        return rs.SkipReason("done")

    rs.CodeRepository(assets=[], sensors=[s]).evaluate_sensor("s")


def evaluate_schedule(read_log):
    @rs.Schedule(cron_schedule="* * * * *", job_name="j")
    def s(context: rs.ScheduleEvaluationContext):
        read_log(context)
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
