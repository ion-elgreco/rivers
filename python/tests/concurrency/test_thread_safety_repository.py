"""Thread-safety of a CodeRepository used from many threads.

A run must not keep the repository's resolved state locked: a thread that
waits for that lock while attached to the interpreter blocks the run, which
needs the interpreter to finish. Each scenario runs in a child process, so a
deadlock fails its test instead of the whole test run.
"""

import gc
import json
import threading
import time

import obstore.store
import pytest
from _threads import run_in_child, run_threads

import rivers as rs

N_RUNS = 8


def materialize_unresolved_in_threads():
    """Child process of ``test_first_materialize_in_threads_resolves_once``."""
    setups = []

    class Slow(rs.Resource):
        def setup(self):
            setups.append(threading.get_ident())
            time.sleep(0.2)
            gc.collect()

    @rs.Asset
    def a(res: Slow) -> int:
        gc.collect()
        return 7

    repo = rs.CodeRepository(
        assets=[a], resources={"res": Slow()}, default_executor=rs.Executor.in_process()
    )
    results, errors = run_threads(lambda i: repo.materialize(["a"]), n=N_RUNS)
    stored = {r.run_id: r.status for r in repo.storage.get_runs()}
    print(
        json.dumps(
            {
                "errors": errors,
                "setups": len(setups),
                "statuses": [stored.get(r.run_id) for r in results if r],
                "success": [r.success for r in results if r],
                "value": repo.load_node("a"),
            }
        )
    )


def test_first_materialize_in_threads_resolves_once(tmp_path):
    """Threads that materialize an unresolved repository resolve it once
    (one ``setup()``, one storage that records every run) and all finish."""
    proc = run_in_child(
        "concurrency.test_thread_safety_repository:materialize_unresolved_in_threads",
        cwd=tmp_path,
    )

    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout.splitlines()[-1]) == {
        "errors": [],
        "setups": 1,
        "statuses": ["Success"] * N_RUNS,
        "success": [True] * N_RUNS,
        "value": 7,
    }


REPOSITORY_CALLS = {
    "storage": lambda repo: repo.storage,
    "load_node": lambda repo: repo.load_node("a"),
    "materialize": lambda repo: repo.materialize(["a"]),
    "resolve": lambda repo: repo.resolve(),
}


def use_repository_in_setup(call):
    """Child process of ``test_setup_that_uses_its_repository_raises``."""
    setups = []

    class UsesRepository(rs.Resource):
        def setup(self):
            setups.append(call)
            if len(setups) == 1:
                REPOSITORY_CALLS[call](repo)

    @rs.Asset
    def a(res: UsesRepository) -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[a],
        resources={"res": UsesRepository()},
        default_executor=rs.Executor.in_process(),
    )
    error = None
    try:
        repo.materialize(["a"])
    except Exception as e:
        error = f"{type(e).__name__}: {e}"
    again = repo.materialize(["a"])
    print(
        json.dumps(
            {
                "error": error,
                "again": [again.success, repo.storage.get_run(again.run_id).status],
                "setups": len(setups),
            }
        )
    )


@pytest.mark.parametrize("call", REPOSITORY_CALLS)
def test_setup_that_uses_its_repository_raises(tmp_path, call):
    """A resource's ``setup()`` that uses its repository during the first
    resolve gets an error instead of waiting for the resolve it runs in; the
    next call resolves again."""
    proc = run_in_child(
        "concurrency.test_thread_safety_repository:use_repository_in_setup",
        call,
        cwd=tmp_path,
    )

    assert proc.returncode == 0, proc.stderr
    result = json.loads(proc.stdout.splitlines()[-1])
    assert result.pop("error").startswith(
        "ExecutionError: CodeRepository is still resolving"
    )
    assert result == {"again": [True, "Success"], "setups": 2}


def release_storage_during_run():
    """Child process of ``test_release_storage_during_a_run``."""
    started, released = threading.Event(), threading.Event()

    @rs.Asset
    def a() -> int:
        started.set()
        released.wait(10)
        gc.collect()
        return 1

    repo = rs.CodeRepository(assets=[a], default_executor=rs.Executor.in_process())
    repo.resolve()
    first_storage = repo.storage
    out = {}
    runner = threading.Thread(target=lambda: out.update(run=repo.materialize(["a"])))
    runner.start()
    started.wait(10)
    begin = time.monotonic()
    repo._release_storage()
    release_seconds = time.monotonic() - begin
    released.set()
    runner.join()

    run, again = out["run"], repo.materialize(["a"])
    print(
        json.dumps(
            {
                "release_seconds": release_seconds,
                "run": [run.success, first_storage.get_run(run.run_id).status],
                "again": [again.success, repo.storage.get_run(again.run_id).status],
                "run_in_new_storage": repo.storage.get_run(run.run_id) is not None,
            }
        )
    )


def test_release_storage_during_a_run(tmp_path):
    """Releasing the storage while a run is in flight returns at once; the run
    finishes on the storage it started with, and the next call resolves the
    repository again with a new storage."""
    proc = run_in_child(
        "concurrency.test_thread_safety_repository:release_storage_during_run",
        cwd=tmp_path,
    )

    assert proc.returncode == 0, proc.stderr
    result = json.loads(proc.stdout.splitlines()[-1])
    assert result.pop("release_seconds") < 2
    assert result == {
        "run": [True, "Success"],
        "again": [True, "Success"],
        "run_in_new_storage": False,
    }


def load_node_inside_run_during_release():
    """Child process of ``test_load_node_inside_a_run_during_release``."""
    started, releasing = threading.Event(), threading.Event()
    handler = rs.PickleIOHandler(store=obstore.store.MemoryStore())

    @rs.Asset(io_handler=handler)
    def a() -> int:
        return 3

    @rs.Asset(io_handler=handler)
    def b(a: int) -> int:
        started.set()
        releasing.wait(10)
        time.sleep(0.2)
        return repo.load_node("a") + a

    repo = rs.CodeRepository(assets=[a, b], default_executor=rs.Executor.in_process())
    repo.materialize(["a"])
    out = {}
    runner = threading.Thread(target=lambda: out.update(run=repo.materialize(["b"])))
    runner.start()
    started.wait(10)
    releasing.set()
    repo._release_storage()
    runner.join()
    print(json.dumps({"success": out["run"].success, "b": repo.load_node("b")}))


def test_load_node_inside_a_run_during_release(tmp_path):
    """An asset that reads the repository while another thread releases its
    storage gets an answer: the read resolves the repository again."""
    proc = run_in_child(
        "concurrency.test_thread_safety_repository:load_node_inside_run_during_release",
        cwd=tmp_path,
    )

    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout.splitlines()[-1]) == {"success": True, "b": 6}
