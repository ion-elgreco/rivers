"""Thread-safety of AutomationDaemon.start() and stop().

A daemon runs one loop. Any other start() warns and does nothing, from any
thread, also while stop() waits for the loop and after it.
"""

import gc
import json
import sys
import threading
import time
import warnings

import pydantic
import pytest
from _polling import wait_until
from _threads import N_THREADS, run_in_child, run_threads

import rivers as rs
from rivers._core import AutomationDaemon
from rivers.testing import memory_storage

ALREADY_STARTED = "AutomationDaemon is already started; start() did nothing"
STOPPED = "AutomationDaemon is stopped; start() did nothing"


class PausingConfig(pydantic.BaseModel):
    """Sensor config that start() builds; building it pauses every thread."""

    @pydantic.model_validator(mode="after")
    def pause(self):
        gc.collect()
        time.sleep(0.05)
        return self


def counting_daemon(storage, on_eval=lambda n: None):
    """A daemon whose sensor evaluates without pause, each time with the
    previous evaluation's cursor + 1. Returns it and the cursors in order:
    one loop counts 0, 1, 2, ...; a second loop repeats a cursor."""
    cursors = []

    @rs.Asset
    def a() -> int:
        return 1

    @rs.Sensor(
        asset_selection=["a"],
        minimum_interval="0s",
        default_status=rs.SensorStatus.Running,
    )
    def counter(context: rs.SensorEvaluationContext[PausingConfig]):
        n = int(context.cursor or 0)
        cursors.append(n)
        on_eval(n)
        time.sleep(0.01)
        return rs.SensorResult(skip_reason="counted", cursor=str(n + 1))

    repo = rs.CodeRepository(assets=[a], sensors=[counter])
    repo.resolve(storage=storage)
    return AutomationDaemon(repo=repo, storage=storage), cursors


def keeps_counting(cursors):
    n = len(cursors)
    return wait_until(lambda: len(cursors) >= n + 3, timeout=5)


def counts_up(cursors):
    return cursors == list(range(len(cursors)))


def test_second_start_warns_and_keeps_one_loop(storage):
    """start() on a running daemon warns and does nothing: the loop keeps
    counting and no second loop starts."""
    daemon, cursors = counting_daemon(storage)
    daemon.start()
    try:
        assert wait_until(lambda: cursors)
        with pytest.warns(UserWarning, match="already started"):
            daemon.start()
        assert keeps_counting(cursors)
    finally:
        daemon.stop()
    assert counts_up(cursors)


def test_threads_that_start_one_daemon_run_one_loop(storage):
    """Threads that call start() together, while the first one builds the
    sensor config, start one loop; every other call warns."""
    daemon, cursors = counting_daemon(storage)
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _, errors = run_threads(lambda i: daemon.start())
        counting = keeps_counting(cursors)
    finally:
        daemon.stop()
    assert {
        "errors": errors,
        "warnings": [str(w.message) for w in caught],
        "counting": counting,
        "counts up": counts_up(cursors),
    } == {
        "errors": [],
        "warnings": [ALREADY_STARTED] * (N_THREADS - 1),
        "counting": True,
        "counts up": True,
    }


def test_start_while_stop_waits_warns(storage):
    """start() while stop() waits for the loop warns and starts nothing. The
    first evaluation keeps stop() waiting and calls start() meanwhile."""
    stopping = threading.Event()
    start_result = {}

    def start_during_stop(n):
        if n:
            return
        stopping.wait(5)
        time.sleep(0.1)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            try:
                daemon.start()
            except BaseException as e:
                start_result["error"] = f"{type(e).__name__}: {e}"
        start_result["warnings"] = [str(w.message) for w in caught]

    daemon, cursors = counting_daemon(storage, on_eval=start_during_stop)
    daemon.start()
    try:
        assert wait_until(lambda: cursors)
        stopping.set()
    finally:
        daemon.stop()
    assert {"start": start_result, "cursors": cursors} == {
        "start": {"warnings": [STOPPED]},
        "cursors": [0],
    }


def stop_without_a_loop():
    """Child process of ``test_stop_without_a_loop_returns``."""
    sys.modules["loky"] = None

    @rs.Asset
    def a() -> int:
        return 1

    @rs.Sensor(
        asset_selection=["a"],
        default_status=rs.SensorStatus.Running,
        eval_mode=rs.EvalMode.Subprocess,
    )
    def remote(context: rs.SensorEvaluationContext):
        return rs.SkipReason("never evaluated")

    storage = memory_storage()
    repo = rs.CodeRepository(assets=[a], sensors=[remote])
    repo.resolve(storage=storage)
    daemon = AutomationDaemon(repo=repo, storage=storage)
    errors = []
    for _ in range(2):
        try:
            daemon.start()
        except ImportError as e:
            errors.append(f"{type(e).__name__}: {e}")
    daemon.stop()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        daemon.start()
    print(json.dumps({"errors": errors, "warnings": [str(w.message) for w in caught]}))


def test_stop_without_a_loop_returns(tmp_path):
    """A start() that fails leaves the daemon able to start again. stop()
    without a loop returns at once, and a stopped daemon does not start."""
    proc = run_in_child(
        "concurrency.test_thread_safety_daemon:stop_without_a_loop", cwd=tmp_path
    )

    assert proc.returncode == 0, proc.stderr
    loky_missing = (
        "ImportError: loky is required for subprocess eval mode. "
        "Install it: uv pip install rivers[loky]"
    )
    assert json.loads(proc.stdout.splitlines()[-1]) == {
        "errors": [loky_missing] * 2,
        "warnings": [STOPPED],
    }
