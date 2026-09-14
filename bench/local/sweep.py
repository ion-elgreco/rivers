"""Drive the local rivers / Dagster / Prefect comparison sweeps.

Each measurement runs as a fresh subprocess in its framework's own virtualenv,
so a crash or an out-of-memory kill at one size is recorded as a data point
instead of taking the sweep down.

Results are written after every point, so a long sweep can be stopped and
whatever it reached is kept. The output file is written under a temporary name
first, so an interrupted sweep never truncates the previous run's results.

Run from the repository root:
    python -m bench.local.sweep --quick
    python -m bench.local.sweep --full
    python -m bench.local.sweep --only sensor_pass --framework rivers
"""

from __future__ import annotations

import argparse
import contextlib
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from bench.benchmarks import BENCHMARKS, LIVE_KEYS
from bench.harness import parse_result
from bench.paths import (
    DRIVER_MODULES,
    FRAMEWORKS,
    INTERPRETERS,
    LOCAL_FULL,
    LOCAL_QUICK,
    ROOT,
    resolve,
    write_json,
)

# Slack between the sweep's own kill timer and the budget the driver enforces,
# so a driver gets the chance to report its own timeout first.
BUDGET_SLACK = 20


def _kill_group(proc: subprocess.Popen) -> None:
    """Kill the measurement process and anything it left behind."""
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)


def run_point(
    framework: str,
    benchmark: str,
    n: int,
    *,
    tuned: bool,
    duration: float,
    iterations: int,
) -> dict:
    """Run one measurement in its own process and return the parsed result."""

    def failure(status: str, **extra) -> dict:
        # These four fields are the measurement's identity, and `report.data`
        # indexes on exactly them, so every path has to carry all four.
        return {
            "framework": framework,
            "benchmark": benchmark,
            "n": n,
            "tuned": tuned,
            "status": status,
            **extra,
        }

    interpreter = INTERPRETERS[framework]
    if not interpreter.exists():
        return failure("missing_env", detail=f"{interpreter} not found — run setup.sh")

    timeout = BENCHMARKS[benchmark].budget(duration)
    # `-m` from ROOT is what lets each framework's virtualenv import the
    # `bench` package without anything being installed into it.
    cmd = [
        str(interpreter),
        "-m",
        DRIVER_MODULES[framework],
        benchmark,
        "--n",
        str(n),
        "--duration",
        str(duration),
        "--iterations",
        str(iterations),
        "--budget",
        str(timeout - BUDGET_SLACK),
    ]
    if tuned:
        cmd.append("--tuned")

    started = time.monotonic()
    # Each point gets its own process group. Prefect leaves an orphaned server
    # process holding the output pipes open, which would otherwise hang the
    # sweep long after the measurement finished.
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(ROOT),
        start_new_session=True,
    )
    timed_out = False
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        stdout, stderr = "", ""
    finally:
        _kill_group(proc)
        if timed_out:
            try:
                stdout, stderr = proc.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                stdout, stderr = "", ""

    result = parse_result(stdout)
    if result is not None:
        return result
    if timed_out:
        return failure(
            "timeout",
            detail=f"killed after {timeout}s",
            wall_s=time.monotonic() - started,
        )
    # No result line means the process died before it could report — the usual
    # cause is the OOM killer at large sizes.
    return failure(
        "oom" if proc.returncode in (-9, 137) else "error",
        detail=(stderr or "")[-1500:],
        returncode=proc.returncode,
        wall_s=time.monotonic() - started,
    )


def describe(result: dict) -> str:
    """One-line human summary of a measurement."""
    status = result.get("status", "?")
    parts = [
        f"{result['framework']:<8} {result['benchmark']:<16} n={result['n']:<8} {status:<11}"
    ]
    for key in LIVE_KEYS:
        if key in result and isinstance(result[key], (int, float)):
            parts.append(f"{key}={result[key]:,.1f}")
    if "coverage" in result:
        parts.append(f"coverage={result['coverage']:.0%}")
    if "latency" in result and result["latency"]:
        parts.append(f"median_ms={result['latency']['median_ms']:.1f}")
    if "rss_mb" in result:
        parts.append(f"rss={result['rss_mb']:,.0f}MB")
    if status not in ("ok", "unsupported") and result.get("detail"):
        parts.append(f"| {str(result['detail'])[:120]}")
    return "  ".join(parts)


def _partial(path: Path) -> Path:
    """Where results go until the sweep finishes."""
    return path.with_suffix(".partial.json")


def save(records: list[dict], path: Path) -> None:
    """Write the results so far, without destroying the previous run's file.

    A sweep takes the better part of an hour and is often stopped part way. The
    partial results go to a sibling `.partial` file and only replace the real
    one when the sweep finishes, so stopping early costs nothing already
    published.
    """
    write_json(records, _partial(path))


def finish(records: list[dict], path: Path) -> None:
    """Promote the partial file to the real output path."""
    write_json(records, path)
    _partial(path).unlink(missing_ok=True)


def points(plan: str, names: list[str], optional_modes: bool, window: float):
    """Yield every (benchmark, size, tuned, window) the sweep will measure.

    Every rule here comes from the benchmark's own declaration, so no benchmark
    is named in this function.
    """
    for name in names:
        benchmark = BENCHMARKS.get(name)
        if benchmark is None:
            print(f"skipping unknown benchmark {name}", file=sys.stderr)
            continue
        for n in benchmark.sizes(plan):
            for mode in benchmark.modes:
                if mode.optional and not optional_modes:
                    continue
                if mode.sizes and n not in mode.sizes:
                    continue
                yield name, n, mode.tuned, max(window, mode.window)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full", action="store_true", help="run the full sweep")
    parser.add_argument(
        "--quick", action="store_true", help="run the smoke sweep (default)"
    )
    parser.add_argument("--only", action="append", help="restrict to these benchmarks")
    parser.add_argument(
        "--framework", action="append", help="restrict to these frameworks"
    )
    parser.add_argument(
        "--duration", type=float, default=20.0, help="sensor window, seconds"
    )
    parser.add_argument(
        "--iterations", type=int, default=20, help="run-latency repeats"
    )
    parser.add_argument(
        "--out", default=None, help="output JSON path, relative to bench/"
    )
    parser.add_argument(
        "--default-interval",
        action="store_true",
        help="also sweep each benchmark's optional modes, such as sensors at "
        "the shipped 30s interval (slow)",
    )
    args = parser.parse_args()

    plan = "full" if args.full else "quick"
    benchmarks = args.only or list(BENCHMARKS)
    frameworks = args.framework or FRAMEWORKS
    out_path = (
        resolve(args.out) if args.out else (LOCAL_FULL if args.full else LOCAL_QUICK)
    )

    records: list[dict] = []
    for benchmark, n, tuned, duration in points(
        plan, benchmarks, args.default_interval, args.duration
    ):
        for framework in frameworks:
            result = run_point(
                framework,
                benchmark,
                n,
                tuned=tuned,
                duration=duration,
                iterations=args.iterations,
            )
            records.append(result)
            print(describe(result), flush=True)
            save(records, out_path)

    finish(records, out_path)
    print(f"\nwrote {len(records)} measurements to {out_path}")


if __name__ == "__main__":
    main()
