"""Load measurement files and look measurements up.

Everything downstream reads results through here, so the rules for "which
record answers this question" live in one place: later files win, a starved run
still counts as a measurement, and a missing one is a dash.
"""

from __future__ import annotations

import argparse
import json
import statistics

from bench.paths import FRAMEWORKS, K8S_INPUTS, LOCAL_INPUTS, resolve

LABELS = {"rivers": "rivers", "dagster": "Dagster", "prefect": "Prefect"}

# The framework the comparison is about. Every other one is a rival.
SUBJECT = "rivers"
RIVALS = [f for f in FRAMEWORKS if f != SUBJECT]

# rivers persists topology at load and the other two do not, so the field that
# compares like for like is not the same for every framework.
GRAPH_BUILD_KEYS = {SUBJECT: "build_ms"}

# How a non-ok status is shown in a results cell.
STATUS_TEXT = {
    "timeout": "timed out",
    "oom": "out of memory",
    "error": "failed",
    "starved": "starved",
    "unsupported": "not supported",
    "missing_env": "not installed",
}


def add_result_args(parser: argparse.ArgumentParser) -> None:
    """Declare the `--local` / `--k8s` inputs every renderer accepts."""
    parser.add_argument(
        "--local",
        action="append",
        default=None,
        help="local results; repeat to merge, later files win per measurement",
    )
    parser.add_argument(
        "--k8s",
        action="append",
        default=None,
        help="Kubernetes results; repeat to merge, later files win per orchestrator",
    )


def load_results(args: argparse.Namespace) -> tuple[list[dict], list[dict]]:
    """Read the local and Kubernetes results a renderer was pointed at."""
    return load_local(args.local), merge_k8s(args.k8s)


def load(path) -> list[dict]:
    """Read one results file. A missing file is empty, not an error."""
    if not path:
        return []
    file = resolve(path)
    return json.loads(file.read_text()) if file.exists() else []


def key_of(record: dict) -> tuple:
    """The identity of a measurement: what was measured, by whom, at what size."""
    return (
        record["benchmark"],
        record["framework"],
        record["n"],
        record.get("tuned", False),
    )


def index(records: list[dict]) -> dict:
    """Index measurements by :func:`key_of`."""
    return {key_of(record): record for record in records}


def load_local(paths: list[str] | None) -> list[dict]:
    """Merge local results files, later files winning per measurement.

    This is what makes a single point re-measurable: rerun one benchmark into
    its own file, pass it last, and it replaces that point without redoing the
    sweep it came from.
    """
    merged: dict[tuple, dict] = {}
    for path in paths or LOCAL_INPUTS:
        for record in load(path):
            merged[key_of(record)] = record
    return list(merged.values())


def sizes_for(records: list[dict], benchmark: str) -> list[int]:
    """Every size measured for ``benchmark``, ascending."""
    return sorted({r["n"] for r in records if r["benchmark"] == benchmark})


def largest_size(records: list[dict], benchmark: str) -> int:
    """The biggest size ``benchmark`` was measured at, or 0."""
    return (sizes_for(records, benchmark) or [0])[-1]


def field_of(record: dict, key: str):
    """Read one field, taking the median when it holds a statistics block.

    Latency is recorded as a block rather than a scalar, so a request for it
    means the median. This is the only place that rule is written down.
    """
    value = record.get(key)
    return value.get("median_ms") if isinstance(value, dict) else value


def value_of(record: dict | None, key: str) -> float | None:
    """Read one number from a successful measurement, or None."""
    if record is None or record.get("status") != "ok":
        return None
    value = field_of(record, key)
    return value if isinstance(value, (int, float)) else None


def cell(record: dict | None, key: str, fmt: str = ",.1f") -> str:
    """Format one measurement, or say why there is no number.

    A starved run still produced a real measurement — the daemon ran, it just
    missed passes — so the number is shown with a `*` rather than replaced.
    """
    if record is None:
        return "—"
    status = record.get("status", "ok")
    if status not in ("ok", "starved"):
        return STATUS_TEXT.get(status, status)
    value = field_of(record, key)
    if value is None:
        return "—"
    mark = "*" if status == "starved" else ""
    if isinstance(value, float) and value == float("inf"):
        # No sensor completed a full pass inside the measurement window.
        return "no pass"
    if isinstance(value, (int, float)):
        return f"{value:{fmt}}{mark}"
    return f"{value}{mark}"


def label(framework: str) -> str:
    """How a framework is written in a published table."""
    return LABELS.get(framework, framework)


def merge_k8s(paths: list[str] | None) -> list[dict]:
    """Combine Kubernetes result files into one measurement per orchestrator.

    Repeat runs are reduced to the median of each phase, because control-plane
    startup on a laptop cluster varies by tens of seconds between runs. A later
    file's runs replace an earlier file's for the same orchestrator, so one
    system can be re-measured without redoing the other two.
    """
    by_orch: dict[str, list[dict]] = {}
    for path in paths or K8S_INPUTS:
        from_file: dict[str, list[dict]] = {}
        for measurement in load(path):
            from_file.setdefault(measurement["orchestrator"], []).append(measurement)
        by_orch.update(from_file)

    merged = []
    for name in FRAMEWORKS:
        runs = by_orch.get(name)
        if not runs:
            continue
        base = dict(runs[-1])
        # A phase with no successful run has no time to take a median of, so it
        # is left out rather than reported as zero seconds.
        phase_times: dict[str, list[float]] = {}
        for run in runs:
            for phase in run["phases"]:
                if phase["status"] == "ok":
                    phase_times.setdefault(phase["name"], []).append(phase["seconds"])
        base["phases"] = [
            {
                "name": phase_name,
                "seconds": statistics.median(times),
                "status": "ok",
                "detail": "",
                "runs": len(times),
            }
            for phase_name, times in phase_times.items()
        ]
        base["runs"] = len(runs)
        merged.append(base)
    return merged


def by_orchestrator(k8s: list[dict]) -> dict[str, dict]:
    """Index Kubernetes measurements by the orchestrator they describe."""
    return {m["orchestrator"]: m for m in k8s}


def k8s_ready_seconds(measurement: dict | None) -> float | None:
    """Total seconds from install to ready for work, or None."""
    if not measurement:
        return None
    total = sum(
        p["seconds"]
        for p in measurement["phases"]
        if p["name"] in ("control_plane_ready", "code_location_ready")
        and p["status"] == "ok"
    )
    return total or None


def _memory_mb(measurement: dict) -> float | None:
    memory = measurement.get("memory") or {}
    return memory.get("total_mb") if memory.get("status") == "ok" else None


# The footprint figures both renderers quote, so neither has to decode the
# shape `k8s/measure.py` writes.
K8S_FIELDS = {
    "ready": k8s_ready_seconds,
    "image_mb": lambda m: m["image_mb"],
    "memory": _memory_mb,
}


def k8s_value(measurement: dict | None, field: str) -> float | None:
    """Read one Kubernetes figure, or None when it was not recorded."""
    return K8S_FIELDS[field](measurement) if measurement else None


def pass_ceiling(
    records: list[dict], benchmark: str, framework: str, tuned: bool = True
) -> int | None:
    """Largest size this framework still kept up with on a windowed benchmark.

    A windowed benchmark reports `starved` once the daemon stops delivering the
    passes its interval promised, so the biggest `ok` size is where it still
    kept up.
    """
    ok = [
        r["n"]
        for r in records
        if r["benchmark"] == benchmark
        and r["framework"] == framework
        and bool(r.get("tuned")) == tuned
        and r.get("status") == "ok"
    ]
    return max(ok) if ok else None


def pass_status(records: list[dict], benchmark: str, framework: str) -> str:
    """Why a framework has no ceiling, taken from what it reported.

    A framework without sensors or conditions says so in its own records, so no
    renderer has to know which framework that is.
    """
    for record in records:
        if record["benchmark"] == benchmark and record["framework"] == framework:
            status = record.get("status", "ok")
            if status != "ok":
                return status
    return ""
