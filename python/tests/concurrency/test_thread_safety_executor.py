"""Thread-safety of step inputs that a step shares between its own threads."""

import gc
import time

import obstore.store
import pytest
from _threads import N_THREADS, run_threads

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
