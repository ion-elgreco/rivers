"""Shared harness for the rivers / Dagster / Prefect comparison benchmarks.

Every driver is a standalone script that runs inside its own virtualenv. It
imports this package for the parts that must be identical across all three
frameworks, so a difference in a number comes from the framework and never from
how it was measured. `bench/README.md` lists the modules and the failure
taxonomy; each module's own docstring says what it owns.
"""

from .coldstart import READY_MARKER, cold_start_benchmark, time_cold_start
from .measure import (
    LOG_LINE,
    PARALLEL_SLEEP_S,
    PARALLEL_WORKERS,
    QUEUE_SLOTS,
    Budget,
    claim_verdict,
    counted_window,
    log_verdict,
    parallel_verdict,
    pass_verdict,
    query_summary,
    rss_mb,
    sample,
    sensor_interval,
    summarize,
    throughput,
)
from .process import quiet_env
from .result import parse_result
from .runner import main, unsupported

__all__ = [
    "LOG_LINE",
    "PARALLEL_SLEEP_S",
    "PARALLEL_WORKERS",
    "QUEUE_SLOTS",
    "READY_MARKER",
    "Budget",
    "claim_verdict",
    "cold_start_benchmark",
    "counted_window",
    "main",
    "log_verdict",
    "parallel_verdict",
    "parse_result",
    "pass_verdict",
    "query_summary",
    "quiet_env",
    "rss_mb",
    "sample",
    "sensor_interval",
    "summarize",
    "throughput",
    "time_cold_start",
    "unsupported",
]
