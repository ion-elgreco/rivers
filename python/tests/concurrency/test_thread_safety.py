"""Thread-safety of rivers objects shared between Python threads.

The failures these tests catch show up on free-threaded CPython (3.14t), where
threads run Python and rivers code at the same time.
"""

import threading

import rivers as rs

N_THREADS = 16
CALLS_PER_THREAD = 2000


def run_threads(fn, n=N_THREADS):
    """Run ``fn(i)`` on ``n`` threads released together; return (results, errors)."""
    barrier = threading.Barrier(n)
    results = [None] * n
    errors = []

    def body(i):
        barrier.wait()
        try:
            results[i] = fn(i)
        except BaseException as e:  # PyO3 panics are BaseExceptions
            errors.append(f"{type(e).__name__}: {e}")

    threads = [threading.Thread(target=body, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results, errors


def test_task_concurrent_direct_calls():
    @rs.Task
    def add_one(x):
        return x + 1

    results, errors = run_threads(
        lambda i: sum(add_one(i) for _ in range(CALLS_PER_THREAD))
    )

    assert errors == []
    assert results == [CALLS_PER_THREAD * (i + 1) for i in range(N_THREADS)]


def test_task_factory_decorates_many_functions():
    etl = rs.Task(tags=["etl"])

    @etl
    def extract():
        return "e"

    @etl
    def load():
        return "l"

    assert [(t.name, t.tags, t()) for t in (extract, load)] == [
        ("extract", ["etl"], "e"),
        ("load", ["etl"], "l"),
    ]
    assert etl.name is None


def test_task_factory_shared_across_threads():
    etl = rs.Task(tags=["etl"])

    def decorate(i):
        def body():
            return i

        body.__name__ = f"task_{i}"
        task = etl(body)
        return task.name, task.tags, task()

    results, errors = run_threads(decorate)

    assert errors == []
    assert results == [(f"task_{i}", ["etl"], i) for i in range(N_THREADS)]
