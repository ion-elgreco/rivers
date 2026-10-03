"""Dropping the last handle to a storage closes it, and closing waits for the
datastore to shut down. That wait must not pause other threads: they cannot
run Python on a GIL build, nor collect garbage on a free-threaded build. Each
scenario runs in a child process, so its timings are not shared with the rest
of the test run.
"""

import gc
import json
import threading
import time

import pytest
from _threads import run_in_child, run_threads

import rivers as rs
from rivers._core import AutomationDaemon
from rivers.testing import memory_storage

N_STORAGES = 32


@rs.Asset
def a() -> int:
    return 1


def _repository(storage, **kwargs):
    repo = rs.CodeRepository(
        assets=[a], default_executor=rs.Executor.in_process(), **kwargs
    )
    repo.resolve(storage=storage)
    return repo


def _run_handle(storage):
    return _repository(storage, run_queue=rs.RunQueueConfig())._submit_run(["a"])


def _job(storage):
    return _repository(storage, jobs=[rs.Job(name="j", assets=[a])]).get_job("j")


def _daemon(storage):
    return AutomationDaemon(repo=rs.CodeRepository(assets=[a]), storage=storage)


HOLDERS = {
    "storage": lambda storage: storage,
    "repository": _repository,
    "run_handle": _run_handle,
    "job": _job,
    "daemon": _daemon,
}


def drop_last_handles(holder):
    """Child process of ``test_dropping_the_last_storage_handle_does_not_pause_other_threads``."""
    storages, errors = run_threads(lambda i: memory_storage(), n=N_STORAGES)
    assert not errors, errors
    holders = [HOLDERS[holder](storages.pop()) for _ in range(N_STORAGES)]

    stop = threading.Event()
    pauses = []

    def collect():
        last, longest = time.perf_counter(), 0.0
        while not stop.is_set():
            gc.collect()
            now = time.perf_counter()
            longest, last = max(longest, now - last), now
            time.sleep(0.001)
        pauses.append(longest)

    collector = threading.Thread(target=collect)
    collector.start()
    time.sleep(0.1)
    begin = time.perf_counter()
    holders.clear()
    dropped = time.perf_counter() - begin
    time.sleep(0.1)
    stop.set()
    collector.join()
    print(json.dumps({"dropped": dropped, "longest_pause": pauses[0]}))


@pytest.mark.parametrize("holder", HOLDERS)
def test_dropping_the_last_storage_handle_does_not_pause_other_threads(
    tmp_path, holder
):
    """A thread that drops the last handles to many storages waits for them to
    close; a thread collecting garbage meanwhile keeps running."""
    proc = run_in_child(
        "concurrency.test_thread_safety_storage:drop_last_handles",
        holder,
        cwd=tmp_path,
    )

    assert proc.returncode == 0, proc.stderr
    result = json.loads(proc.stdout.splitlines()[-1])
    assert result["dropped"] > 0.1
    assert result["longest_pause"] < result["dropped"] / 4
