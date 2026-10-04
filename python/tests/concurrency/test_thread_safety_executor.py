"""Thread-safety of steps: inputs a step shares between its own threads, and
the other threads of the process while a step waits for a command."""

import gc
import json
import threading
import time
from pathlib import Path

import obstore.store
import pytest
from _threads import N_THREADS, run_in_child, run_threads

import rivers as rs

N_ITEMS = 2 * N_THREADS


class SlowLoadIOHandler(rs.PickleIOHandler):
    """Loads slowly, so other threads ask for the next item meanwhile."""

    def load_input(self, context):
        time.sleep(0.01)
        gc.collect()
        return super().load_input(context)


@rs.Task
def scale(x: int) -> int:
    return x * 10


def drain_in_threads(items):
    chunks, errors = run_threads(lambda i: list(items))
    return {
        "errors": errors,
        "items": sorted(x for chunk in chunks if chunk for x in chunk),
        "rest": list(items),
        "iterator": type(items).__name__,
    }


@rs.Task
def drain(items: object) -> dict:
    return drain_in_threads(items)


@rs.Task
async def drain_async(items: object) -> dict:
    return drain_in_threads(items)


CONSUMERS = {"sync": drain, "async": drain_async}
EXECUTORS = {
    "in_process": rs.Executor.in_process,
    "parallel": lambda: rs.Executor.parallel(max_workers=2),
}


@pytest.mark.parametrize("consumer", CONSUMERS)
@pytest.mark.parametrize("executor", EXECUTORS)
def test_collect_stream_shared_between_threads(executor, consumer, tmp_path):
    """Threads that share one ``collect_stream()`` iterator get every item
    exactly once between them, then all see the end of the stream."""
    io = SlowLoadIOHandler(store=obstore.store.LocalStore(str(tmp_path), mkdir=True))
    task = CONSUMERS[consumer]

    @rs.Asset(io_handler=io)
    def numbers() -> list:
        return list(range(N_ITEMS))

    # Two consumers in one batch: a parallel executor sends both to workers.
    @rs.Asset.from_graph(io_handler=io, node_io_handler=io)
    def stream_a():
        return task(numbers().map(scale).collect_stream())

    @rs.Asset.from_graph(io_handler=io, node_io_handler=io)
    def stream_b():
        return task(numbers().map(scale).collect_stream())

    assets = [numbers, stream_a, stream_b]
    repo = rs.CodeRepository(
        assets=assets,
        tasks=[scale, task],
        jobs=[rs.Job(name="j", assets=assets, executor=EXECUTORS[executor]())],
    )
    repo.get_job("j").execute()

    in_worker = (executor, consumer) == ("parallel", "sync")
    expected = {
        "errors": [],
        "items": [x * 10 for x in range(N_ITEMS)],
        "rest": [],
        "iterator": "WorkerCollectStreamIter" if in_worker else "MappedResultsIter",
    }
    assert [repo.load_node("stream_a"), repo.load_node("stream_b")] == [
        expected,
        expected,
    ]


WAIT_FOR_FLAG = (
    "touch started; for i in $(seq 100); do"
    " [ -f flag ] && echo seen && exit 0; sleep 0.05; done;"
    " echo 'no flag' >&2; exit 1"
)


def bash_command_beside_python_thread(how):
    """Child process of ``test_bash_command_lets_other_threads_run``."""
    task = rs.BashTask(name="wait_for_flag", command=WAIT_FOR_FLAG)
    done = threading.Event()

    def run_command():
        try:
            if how == "call":
                return task()
            repo = rs.CodeRepository(
                assets=[], tasks=[task], default_executor=EXECUTORS[how]()
            )
            repo.materialize()
            return repo.load_node("wait_for_flag")
        finally:
            done.set()

    def raise_flag():
        while not Path("started").exists():
            if done.wait(0.01):
                return
        gc.collect()
        Path("flag").touch()

    results, errors = run_threads(lambda i: (run_command, raise_flag)[i](), n=2)
    print(json.dumps({"errors": errors, "output": results[0]}))


@pytest.mark.parametrize("how", ["call", *EXECUTORS])
def test_bash_command_lets_other_threads_run(how, tmp_path):
    """Another Python thread runs, and collects garbage, while a ``BashTask``
    waits for its command: the command ends only when that thread writes the
    flag file."""
    proc = run_in_child(
        "concurrency.test_thread_safety_executor:bash_command_beside_python_thread",
        how,
        cwd=tmp_path,
    )

    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout.splitlines()[-1]) == {
        "errors": [],
        "output": "seen",
    }
