"""Helpers for thread-safety tests: start threads together, and run a
scenario that can deadlock in a child process."""

import subprocess
import sys
import threading
from pathlib import Path

N_THREADS = 16
TESTS_DIR = Path(__file__).resolve().parent


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


def run_in_child(target, *args, cwd, timeout=30):
    """Run ``"module:function"`` with ``args`` (as strings) in a new interpreter.

    A deadlock then fails the calling test with ``TimeoutExpired`` instead of
    hanging the test run. The timeout stays under pytest-timeout's 60 s, whose
    ``thread`` method stops the whole run.
    """
    module, func = target.split(":")
    code = (
        "import sys; sys.path.insert(0, sys.argv[1]); "
        f"from {module} import {func}; {func}(*sys.argv[2:])"
    )
    return subprocess.run(
        [sys.executable, "-c", code, str(TESTS_DIR), *map(str, args)],
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
