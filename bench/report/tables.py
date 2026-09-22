"""Markdown tables, one function per topic in the results document.

Every table goes through :func:`markdown_table`, which owns the header, the
alignment row and the empty case. A column can then be added or removed in one
place instead of counting pipes by eye.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from bench.benchmarks import BENCHMARKS
from bench.paths import FRAMEWORKS
from bench.report.data import (
    RIVALS,
    STATUS_TEXT,
    SUBJECT,
    cell,
    index,
    k8s_value,
    label,
    sizes_for,
    value_of,
)
from bench.report.summary import rows as summary_rows

EMPTY = "_No measurements._\n"

LEFT, RIGHT = "---", "---:"


def markdown_table(
    headers: Sequence[str],
    align: Sequence[str],
    rows: Iterable[Sequence[str]],
    empty: str = EMPTY,
) -> str:
    """Render one markdown table, or ``empty`` when it has no data rows."""
    body = [f"| {' | '.join(row)} |" for row in rows]
    if not body:
        return empty
    head = [f"| {' | '.join(headers)} |", "|" + "|".join(align) + "|"]
    return "\n".join(head + body) + "\n"


def _frameworks_in(records: list[dict]) -> list[str]:
    return [f for f in FRAMEWORKS if any(r["framework"] == f for r in records)]


def table(
    records: list[dict],
    benchmark: str,
    key: str | dict[str, str] | None = None,
    *,
    tuned: bool = False,
    fmt: str = ",.1f",
) -> str:
    """One measurement field, compared across frameworks at every size.

    The field and the size column come from the benchmark's own declaration.
    Pass ``key`` to read something else — a dict where the comparable quantity
    has a different name per framework, as rivers' pure graph build does
    against the other two's whole load, which never touches storage.
    """
    declared = BENCHMARKS[benchmark]
    keys = key if isinstance(key, dict) else {}
    default = declared.metric if key is None or isinstance(key, dict) else key
    size_header = declared.size_header
    idx = index(records)
    frameworks = _frameworks_in(records)

    def row(n: int) -> list[str] | None:
        cells = [
            cell(idx.get((benchmark, f, n, tuned)), keys.get(f, default), fmt)
            for f in frameworks
        ]
        # A size swept in one configuration but not another leaves an all-empty
        # row; drop it rather than showing a line of dashes.
        return None if all(c == "—" for c in cells) else [f"{n:,}", *cells]

    rows = (row(n) for n in sizes_for(records, benchmark))
    return markdown_table(
        [size_header, *(label(f) for f in frameworks)],
        [RIGHT] * (len(frameworks) + 1),
        [r for r in rows if r is not None],
    )


def summary_table(local: list[dict], k8s: list[dict]) -> str:
    """One row per topic, so the whole comparison is legible at a glance.

    Every row names the winner explicitly, including the rows rivers loses.
    """
    rows = []
    for row in summary_rows(local, k8s):
        cells = []
        for framework in FRAMEWORKS:
            text = row.text(framework)
            if text is None:
                cells.append("n/a")
                continue
            cells.append(f"**{text}**" if framework == row.best else text)
        rows.append([row.topic, row.label, *cells, label(row.best)])

    return markdown_table(
        ["Topic", "Measurement", *(label(f) for f in FRAMEWORKS), "Best"],
        [LEFT, LEFT, *([RIGHT] * len(FRAMEWORKS)), LEFT],
        rows,
    )


def _phase_text(phases: dict[str, dict], name: str) -> str:
    """One startup phase as a duration, or why there is no duration."""
    phase = phases.get(name)
    if phase is None:
        return "—"
    if phase["status"] != "ok":
        return STATUS_TEXT.get(phase["status"], phase["status"])
    return f"{phase['seconds']:,.1f} s"


def k8s_table(measurements: list[dict]) -> str:
    """Control-plane startup, pod count and footprint per orchestrator."""
    rows = []
    for m in measurements:
        phases = {p["name"]: p for p in m["phases"]}
        memory = k8s_value(m, "memory")
        rows.append(
            [
                label(m["orchestrator"]),
                _phase_text(phases, "control_plane_ready"),
                _phase_text(phases, "code_location_ready"),
                f"{m['pod_count']}",
                f"{m['image_mb']:,.0f} MB",
                f"{memory:,.0f} MB" if memory is not None else "—",
                f"{m.get('runs', 1)}",
            ]
        )
    text = markdown_table(
        [
            "Orchestrator",
            "Control plane ready",
            "Code location ready",
            "Pods",
            "Images on disk",
            "Memory at idle",
            "Runs",
        ],
        [LEFT, *([RIGHT] * 6)],
        rows,
    )
    if not rows:
        return text

    # The three stacks are not the same shape, so say what each one deployed.
    notes = [
        f"- **{label(m['orchestrator'])}:** {note}"
        for m in measurements
        for note in m.get("notes", [])
    ]
    return f"{text}\n" + "\n".join(notes) + "\n" if notes else text


def regression_table(records: list[dict], factor: float = 1.5) -> str:
    """Every measurement where rivers is more than ``factor`` times slower.

    A benchmark is only useful if it reports the losses too. These are the rows
    a reader will check first, so they get their own table.
    """
    idx = index(records)
    rows = []
    for benchmark, declared in BENCHMARKS.items():
        if not declared.regression:
            continue
        key = declared.metric
        for n in sizes_for(records, benchmark):
            mine = value_of(idx.get((benchmark, SUBJECT, n, False)), key)
            if mine is None:
                continue
            rivals = [
                (value, other)
                for other in RIVALS
                if (value := value_of(idx.get((benchmark, other, n, False)), key))
                is not None
            ]
            if not rivals:
                continue
            best, who = min(rivals)
            if best > 0 and mine / best >= factor:
                rows.append(
                    [
                        benchmark,
                        f"{n:,}",
                        f"{mine:,.1f} ms",
                        f"{label(who)} {best:,.1f} ms",
                        f"{mine / best:,.1f}x",
                    ]
                )
    return markdown_table(
        ["Benchmark", "Size", label(SUBJECT), "Best rival", "Ratio"],
        [LEFT, RIGHT, RIGHT, LEFT, RIGHT],
        rows,
        empty=(
            f"_{label(SUBJECT)} is within {factor}x of the best rival "
            "on every measurement._\n"
        ),
    )


def failure_table(records: list[dict]) -> str:
    """Where each framework stops working, by benchmark."""
    rows = []
    seen = set()
    for record in sorted(
        records, key=lambda r: (r["benchmark"], r["framework"], r["n"])
    ):
        status = record.get("status", "ok")
        if status in ("ok", "unsupported"):
            continue
        key = (record["benchmark"], record["framework"])
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            [
                record["benchmark"],
                label(record["framework"]),
                f"{record['n']:,}",
                STATUS_TEXT.get(status, status),
            ]
        )
    return markdown_table(
        ["Benchmark", "Framework", "First failing size", "How it failed"],
        [LEFT, LEFT, RIGHT, LEFT],
        rows,
        empty="_Nothing failed within the sizes measured._\n",
    )
