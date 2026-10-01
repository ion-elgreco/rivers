"""The comparison's headline rows: one topic, every framework, a winner.

Both `RESULTS.md` and the landing page render this list, so a benchmark added
here appears in both without either renderer naming a benchmark itself. The
rows are grouped by topic and keep this order everywhere they are published.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import NamedTuple

from bench.paths import FRAMEWORKS
from bench.report.data import (
    GRAPH_BUILD_KEYS,
    by_orchestrator,
    field_of,
    index,
    k8s_value,
    largest_size,
    pass_ceiling,
    pass_status,
)


class Figure(NamedTuple):
    """One framework's answer for one row.

    `status` says why there is no number, so a renderer picks its own wording
    instead of reading the records again to find out. `at_limit` says the sweep
    ran out of sizes before the framework ran out of capacity, which makes the
    number a floor rather than a ceiling.
    """

    value: float | None = None
    status: str = ""
    at_limit: bool = False


class Row(NamedTuple):
    """One measurement, answered by every framework, with the winner marked.

    `unit` formats the number and `quantity` says what the number is, because a
    renderer may write the same quantity a different way. The landing page
    writes a long duration in seconds; the results document keeps milliseconds.
    """

    topic: str
    label: str
    unit: str
    quantity: str
    higher_is_better: bool
    figures: dict[str, Figure]
    best: str | None

    def value(self, framework: str) -> float | None:
        """This framework's number, or None when it has none."""
        return self.figures.get(framework, Figure()).value

    def text(self, framework: str) -> str | None:
        """This framework's number, formatted, or None when it has none.

        A duration under a millisecond keeps two significant digits. The row's
        own format would round it to `0 ms`, which reads as a broken
        measurement rather than as the real one it is.
        """
        value = self.value(framework)
        if value is None:
            return None
        if self.quantity == "ms" and value < 1:
            return f"{value:.2g} ms"
        return self.unit.format(value)

    def status(self, framework: str) -> str:
        """Why this framework has no number. Empty when it was not measured."""
        return self.figures.get(framework, Figure()).status

    def at_limit(self, framework: str) -> bool:
        """True when the sweep, not the framework, decided this number."""
        return self.figures.get(framework, Figure()).at_limit


Reader = Callable[[str], Figure]


def _best(figures: dict[str, Figure], higher_is_better: bool) -> str | None:
    """The framework with the winning number, or None when nobody answered."""
    answered = {f: g.value for f, g in figures.items() if g.value is not None}
    if not answered:
        return None
    return (max if higher_is_better else min)(answered, key=lambda f: answered[f])


def rows(local: list[dict], k8s: list[dict]) -> list[Row]:
    """Every topic the comparison covers, in publication order.

    A row nobody answered is dropped rather than published as a line of
    dashes.
    """
    idx = index(local)
    by_orch = by_orchestrator(k8s)

    def point(benchmark: str, key: str | dict, n: int) -> Reader:
        """One field of one measurement, at the biggest size it was swept at."""
        keys = key if isinstance(key, dict) else {}
        default = "total_ms" if isinstance(key, dict) else key

        def read(framework: str) -> Figure:
            record = idx.get((benchmark, framework, n, False))
            if record is None:
                return Figure()
            status = record.get("status", "ok")
            if status != "ok":
                # A starved run measured something, but not the thing this row
                # claims, so it is reported as a status rather than a number.
                return Figure(status=status)
            value = field_of(record, keys.get(framework, default))
            return Figure(value if isinstance(value, (int, float)) else None)

        return read

    def k8s_field(field: str) -> Reader:
        return lambda framework: Figure(k8s_value(by_orch.get(framework), field))

    def ceiling(benchmark: str, tuned: bool) -> Reader:
        """The largest size a windowed benchmark still kept up at."""
        top = largest_size(local, benchmark)

        def read(framework: str) -> Figure:
            largest = pass_ceiling(local, benchmark, framework, tuned=tuned)
            if largest is None:
                return Figure(status=pass_status(local, benchmark, framework))
            return Figure(float(largest), at_limit=largest >= top)

        return read

    big = {
        name: largest_size(local, name)
        for name in (
            "cold_start",
            "reload",
            "graph_load",
            "graph_deps",
            "selection",
            "run_throughput",
            "run_steps",
            "parallel_scaling",
            "partitions",
            "partition_ops",
            "schedule_tick",
            "backfill_drain",
            "queue_limits",
            "read_path",
            "log_capture",
        )
    }

    # (topic, measurement, reader, unit, quantity, higher_is_better)
    spec: list[tuple[str, str, Reader, str, str, bool]] = [
        (
            "Kubernetes control plane",
            "install to ready for work",
            k8s_field("ready"),
            "{:,.0f} s",
            "seconds",
            False,
        ),
        (
            "Kubernetes control plane",
            "memory at idle",
            k8s_field("memory"),
            "{:,.0f} MB",
            "mb",
            False,
        ),
        (
            "Kubernetes control plane",
            "images on disk",
            k8s_field("image_mb"),
            "{:,.0f} MB",
            "mb",
            False,
        ),
        (
            "Code location load",
            f"cold start, {big['cold_start']:,} assets",
            point("cold_start", "cold_start_ms", big["cold_start"]),
            "{:,.0f} ms",
            "ms",
            False,
        ),
        (
            "Code location load",
            f"reload {big['reload']:,} assets into a loaded store",
            point("reload", "total_ms", big["reload"]),
            "{:,.0f} ms",
            "ms",
            False,
        ),
        (
            "Graph engine",
            f"build a {big['graph_load']:,}-asset graph",
            point("graph_load", GRAPH_BUILD_KEYS, big["graph_load"]),
            "{:,.0f} ms",
            "ms",
            False,
        ),
        (
            "Graph engine",
            f"build a {big['graph_deps']:,}-asset graph with dependencies",
            point("graph_deps", GRAPH_BUILD_KEYS, big["graph_deps"]),
            "{:,.0f} ms",
            "ms",
            False,
        ),
        (
            "Graph engine",
            f"resolve a selection over {big['selection']:,} assets",
            point("selection", "total_ms", big["selection"]),
            "{:,.0f} ms",
            "ms",
            False,
        ),
        (
            "Run execution",
            "one no-op asset, end to end",
            point("run_latency", "latency", 1),
            "{:,.0f} ms",
            "ms",
            False,
        ),
        (
            "Run execution",
            "no-op runs per second",
            point("run_throughput", "runs_per_s", big["run_throughput"]),
            "{:,.0f}/s",
            "rate",
            True,
        ),
        (
            "Run execution",
            f"per step in one run of {big['run_steps']:,}",
            point("run_steps", "ms_per_step", big["run_steps"]),
            "{:,.1f} ms",
            "ms",
            False,
        ),
        (
            "Run execution",
            f"parallel efficiency, {big['parallel_scaling']:,} sleeping assets",
            point("parallel_scaling", "efficiency", big["parallel_scaling"]),
            "{:.0%}",
            "percent",
            True,
        ),
        (
            "Run execution",
            "cancel a running run",
            point("cancel_latency", "cancel_ms", 1),
            "{:,.0f} ms",
            "ms",
            False,
        ),
        (
            "Partitions",
            f"define and load {big['partitions']:,}",
            point("partitions", "total_ms", big["partitions"]),
            "{:,.0f} ms",
            "ms",
            False,
        ),
        (
            "Partitions",
            f"materialize, count and list over {big['partition_ops']:,} keys",
            point("partition_ops", "total_ms", big["partition_ops"]),
            "{:,.0f} ms",
            "ms",
            False,
        ),
        (
            "Automation daemon",
            "sensors held at a 1-second interval",
            ceiling("sensor_pass", tuned=True),
            "{:,.0f}",
            "count",
            True,
        ),
        (
            "Automation daemon",
            "conditions held at a 1-second interval",
            ceiling("condition_pass", tuned=False),
            "{:,.0f}",
            "count",
            True,
        ),
        (
            "Automation daemon",
            f"evaluate {big['schedule_tick']:,} schedules once",
            point("schedule_tick", "total_ms", big["schedule_tick"]),
            "{:,.1f} ms",
            "ms",
            False,
        ),
        (
            "Backfill",
            f"partitions per second, {big['backfill_drain']:,} partitions",
            point("backfill_drain", "partitions_per_s", big["backfill_drain"]),
            "{:,.1f}/s",
            "rate",
            True,
        ),
        (
            "Concurrency",
            f"claims per second, {big['queue_limits']:,} against four slots",
            point("queue_limits", "claims_per_s", big["queue_limits"]),
            "{:,.0f}/s",
            "rate",
            True,
        ),
        (
            "Read path",
            f"answer a UI page over {big['read_path']:,} runs",
            point("read_path", "query_ms", big["read_path"]),
            "{:,.1f} ms",
            "ms",
            False,
        ),
        (
            "Logs",
            f"write and read {big['log_capture']:,} lines",
            point("log_capture", "total_ms", big["log_capture"]),
            "{:,.0f} ms",
            "ms",
            False,
        ),
    ]

    published = []
    for topic, measurement, read, unit, quantity, higher_is_better in spec:
        figures = {framework: read(framework) for framework in FRAMEWORKS}
        if all(figure.value is None for figure in figures.values()):
            continue
        published.append(
            Row(
                topic=topic,
                label=measurement,
                unit=unit,
                quantity=quantity,
                higher_is_better=higher_is_better,
                figures=figures,
                best=_best(figures, higher_is_better),
            )
        )
    return published
