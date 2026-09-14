"""The entry point every driver shares.

A driver defines its benchmarks as named functions and hands them to
:func:`main`. Everything else — argument parsing, the failure taxonomy, the
memory watchdog, emitting the result and reaping children — happens here, once,
the same way for all three frameworks.
"""

from __future__ import annotations

import argparse
import os
import time
import traceback
from typing import Any
from collections.abc import Callable

from .machine import environment, package_version
from .measure import rss_mb
from .process import kill_children, start_memory_watchdog
from .result import emit

Benchmark = Callable[[argparse.Namespace], dict]


def unsupported(reason: str) -> Benchmark:
    """Register a benchmark the framework has no equivalent for.

    A missing feature is marked, never scored. Inventing a number here would
    imply a comparison that does not exist.
    """

    def run(_args: argparse.Namespace) -> dict:
        return {"status": "unsupported", "detail": reason}

    return run


def main(
    framework: str,
    benchmarks: dict[str, Benchmark],
    version: str | None = None,
) -> None:
    """Run one named benchmark and print its result line."""
    parser = argparse.ArgumentParser(description=f"{framework} comparison benchmarks")
    parser.add_argument("benchmark", choices=sorted(benchmarks))
    parser.add_argument("--n", type=int, default=100, help="workload size")
    parser.add_argument("--iterations", type=int, default=10, help="repeat count")
    parser.add_argument(
        "--duration", type=float, default=10.0, help="measurement window, seconds"
    )
    parser.add_argument(
        "--budget", type=float, default=300.0, help="wall-clock budget, seconds"
    )
    parser.add_argument(
        "--tuned",
        action="store_true",
        help="use the tuned config, not the shipped default",
    )
    args = parser.parse_args()

    record: dict[str, Any] = {
        "framework": framework,
        "benchmark": args.benchmark,
        "n": args.n,
        "tuned": args.tuned,
        "status": "ok",
        "version": version or package_version(framework),
        "environment": environment(),
    }
    start_memory_watchdog(record)
    started = time.monotonic()
    try:
        record.update(benchmarks[args.benchmark](args))
    except TimeoutError as exc:
        record["status"] = "timeout"
        record["detail"] = str(exc)
    except MemoryError as exc:
        record["status"] = "oom"
        record["detail"] = str(exc)
    except BaseException as exc:  # noqa: BLE001 - the taxonomy needs every failure
        record["status"] = "error"
        record["detail"] = f"{type(exc).__name__}: {exc}"
        record["traceback"] = traceback.format_exc()[-2000:]
    record["wall_s"] = time.monotonic() - started
    if "rss_mb" not in record:
        # Sampling walks the whole process table, so only do it when the
        # benchmark did not already report a figure of its own.
        record["rss_mb"] = rss_mb()
    emit(record)
    # Prefect starts an ephemeral API server in a child process. Left alive, it
    # holds this process's stdout pipe open and the sweep blocks reading it long
    # after the measurement finished. Reap children before exiting.
    kill_children()
    # The result is already flushed. Hard-exit so no framework's teardown cost
    # leaks into the next measurement — teardown is measured on its own.
    os._exit(0)
