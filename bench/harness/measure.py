"""Timing, memory and the statistics every benchmark reports."""

from __future__ import annotations

import contextlib
import statistics
import time
from collections.abc import Callable

# Both daemons are held to the same interval, so the sensor benchmark measures
# what a pass costs rather than how often a framework chooses to wake. The
# shipped default happens to be 30 seconds in rivers and in Dagster alike; one
# second is the floor Dagster accepts. These live here, not in the drivers,
# because an interval that differs by framework invalidates the comparison.
DEFAULT_SENSOR_INTERVAL = 30
TUNED_SENSOR_INTERVAL = 1


def sensor_interval(tuned: bool) -> int:
    """The interval every framework's sensor daemon is held to."""
    return TUNED_SENSOR_INTERVAL if tuned else DEFAULT_SENSOR_INTERVAL


class Budget:
    """Wall-clock budget for one measured step."""

    def __init__(self, seconds: float) -> None:
        self.seconds = seconds
        self._start = time.monotonic()

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self._start

    @property
    def expired(self) -> bool:
        return self.elapsed > self.seconds

    def check(self) -> None:
        if self.expired:
            raise TimeoutError(f"exceeded {self.seconds}s budget")


def rss_bytes() -> int:
    """Resident memory of this process and its children, in bytes."""
    import psutil

    proc = psutil.Process()
    total = proc.memory_info().rss
    for child in proc.children(recursive=True):
        with contextlib.suppress(psutil.Error):
            total += child.memory_info().rss
    return total


def rss_mb() -> float:
    """Resident memory of this process and its children, in megabytes."""
    return rss_bytes() / 1024**2


def summarize(samples: list[float]) -> dict[str, float]:
    """Reduce a list of per-iteration seconds to reportable statistics."""
    if not samples:
        return {}
    ordered = sorted(samples)
    return {
        "n": len(ordered),
        "min_ms": ordered[0] * 1000,
        "median_ms": statistics.median(ordered) * 1000,
        "mean_ms": statistics.fmean(ordered) * 1000,
        "p95_ms": ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))] * 1000,
        "max_ms": ordered[-1] * 1000,
    }


def sample(call: Callable[[], object], iterations: int, budget: Budget) -> list[float]:
    """Time ``call`` ``iterations`` times and return the per-iteration seconds.

    How a sample is timed must not vary by framework, so every driver's latency
    benchmark goes through here and supplies only the call inside the loop.
    """
    samples = []
    for _ in range(iterations):
        budget.check()
        started = time.perf_counter()
        call()
        samples.append(time.perf_counter() - started)
    return samples


def throughput(call: Callable[[], object], runs: int, budget: Budget) -> dict:
    """Run ``call`` ``runs`` times back to back and report the rate.

    Like :func:`sample`, this owns the shape of the reported numbers so the
    three drivers cannot define runs per second differently.
    """
    started = time.perf_counter()
    for _ in range(runs):
        call()
        budget.check()
    elapsed = time.perf_counter() - started
    return {
        "runs": runs,
        "elapsed_s": elapsed,
        "runs_per_s": runs / elapsed,
        "ms_per_run": elapsed * 1000 / runs,
    }


def counted_window(count_fn, duration: float) -> tuple[list[int], float]:
    """Count ticks before and after ``duration``, returning deltas and the window.

    At large sensor counts the counting pass itself takes seconds, and ticks
    keep landing while it runs. Timing between the midpoints of the two counts
    removes that bias instead of charging it to one side.
    """
    start_a = time.monotonic()
    before = count_fn()
    mid_a = (start_a + time.monotonic()) / 2

    time.sleep(duration)

    start_b = time.monotonic()
    after = count_fn()
    mid_b = (start_b + time.monotonic()) / 2

    return [a - b for a, b in zip(after, before, strict=True)], mid_b - mid_a


def pass_verdict(deltas: list[int], window_s: float, interval_s: float, n: int) -> dict:
    """Judge one sensor-daemon measurement window.

    ``deltas`` holds the new tick count for each sensor over ``window_s``. The
    interval promises ``window_s / interval_s`` passes. A daemon that delivers
    fewer than 90% of them is starved: some sensor did not fire on time.
    """
    if not deltas:
        return {"status": "error", "detail": "no sensors measured"}
    expected = window_s / interval_s if interval_s else float("inf")
    complete = min(deltas)
    total = sum(deltas)
    coverage = complete / expected if expected else 0.0
    # With the interval throttling, a pass cannot be faster than the interval.
    # Once the daemon saturates, this is the true cost of one pass.
    pass_ms = (window_s * 1000 / complete) if complete else float("inf")
    return {
        "status": "ok" if coverage >= 0.9 else "starved",
        "sensors": n,
        "interval_s": interval_s,
        "expected_passes": expected,
        "complete_passes": complete,
        "coverage": coverage,
        "total_ticks": total,
        "ticks_per_s": total / window_s,
        "pass_ms": pass_ms,
        "min_ticks": min(deltas),
        "max_ticks": max(deltas),
    }


# --- shapes every framework is held to ---------------------------------------
#
# A benchmark that lets each driver pick its own workload shape is not a
# comparison. These live here for the same reason the sensor interval does:
# a shape that differs by framework invalidates the number.

# The unit of work the parallel benchmark schedules. Long enough that
# scheduling overhead cannot hide inside it, short enough that a large size
# still finishes inside the budget.
PARALLEL_SLEEP_S = 0.05
# Workers every framework's parallel executor is given.
PARALLEL_WORKERS = 4
# Slots in the pool the queue benchmark contends for.
QUEUE_SLOTS = 4
# The exact line every framework writes, so the log benchmark compares the
# same bytes rather than each driver's idea of a log record.
LOG_LINE = "bench log line with enough text to look like a real record"


def parallel_verdict(
    elapsed_s: float,
    n: int,
    workers: int = PARALLEL_WORKERS,
    sleep_s: float = PARALLEL_SLEEP_S,
) -> dict:
    """Judge one parallel-execution measurement.

    ``n`` assets each sleep ``sleep_s``. With ``workers`` of them running at
    once the work itself needs ``sleep_s * n / workers`` seconds, so anything
    above that is the framework's scheduling cost. Efficiency states it as a
    fraction: 1.0 is perfect, 0.5 means half the wall clock went to overhead.
    """
    ideal_s = sleep_s * n / workers
    return {
        "assets": n,
        "workers": workers,
        "sleep_ms": sleep_s * 1000,
        "total_ms": elapsed_s * 1000,
        "ideal_ms": ideal_s * 1000,
        "overhead_ms": (elapsed_s - ideal_s) * 1000,
        "ms_per_asset": elapsed_s * 1000 / n if n else 0.0,
        "efficiency": ideal_s / elapsed_s if elapsed_s else 0.0,
    }


def log_verdict(
    lines: int,
    captured: int,
    write_s: float,
    read_s: float,
    launch_s: float,
    **extra,
) -> dict:
    """Judge one log-capture measurement.

    Speed is only half of it, the same way it is for a concurrency limit. A
    framework that drops lines is wrong however fast it was, so a short capture
    is recorded as a failed measurement rather than a fast one. rivers silently
    truncated stdout at 4 MiB and published 71,090 of 100,000 lines as its
    quickest result; nothing here compared the two numbers.

    ``launch_s`` times the same run writing no lines at all. Subtracting it
    gives ``lines_ms``, the part that actually scales with the column. Without
    it the figure is dominated by launching a run: rivers and Dagster were both
    flat across a thousandfold increase in lines, which says nothing about log
    capture.

    ``lines_ms`` is floored at zero. A zero means the per-line cost is smaller
    than the run-to-run variance of the launch itself, which is a real answer
    rather than a missing one.
    """
    lost = lines - captured
    return {
        "status": "error" if lost else "ok",
        "detail": f"captured {captured:,} of {lines:,} lines" if lost else "",
        "lines": lines,
        "captured_lines": captured,
        "lines_lost": lost,
        "launch_ms": launch_s * 1000,
        "write_ms": write_s * 1000,
        "read_ms": read_s * 1000,
        "lines_ms": max((write_s + read_s - launch_s) * 1000, 0.0),
        "us_per_line": max((write_s + read_s - launch_s) * 1e6 / lines, 0.0)
        if lines
        else 0.0,
        "total_ms": (write_s + read_s) * 1000,
        **extra,
    }


def claim_verdict(
    elapsed_s: float, granted: int, refused: int, slots: int, max_held: int
) -> dict:
    """Judge one concurrency-limit measurement.

    Speed is only half of it. A limiter that hands out more slots than the
    limit is wrong however fast it is, so ``limit_respected`` is reported
    beside the rate and a breach is recorded as a failed measurement.
    """
    attempts = granted + refused
    breached = max_held > slots
    return {
        "status": "error" if breached else "ok",
        "detail": f"held {max_held} slots against a limit of {slots}"
        if breached
        else "",
        "slots": slots,
        "attempts": attempts,
        "granted": granted,
        "refused": refused,
        "max_held": max_held,
        "limit_respected": not breached,
        "elapsed_s": elapsed_s,
        "claims_per_s": attempts / elapsed_s if elapsed_s else 0.0,
        "ms_per_claim": elapsed_s * 1000 / attempts if attempts else 0.0,
    }


def query_summary(timings: dict[str, float]) -> dict:
    """Reduce named query timings to per-query milliseconds plus their total.

    ``query_ms`` is the headline: answering every question the UI asks on one
    page, against a store of the measured size.
    """
    result = {f"{name}_ms": seconds * 1000 for name, seconds in timings.items()}
    result["query_ms"] = sum(timings.values()) * 1000
    return result
