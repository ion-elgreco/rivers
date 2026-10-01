"""Where every part of the benchmark lives.

Scripts resolve their paths from here rather than counting parent directories
themselves. Moving a file then costs one line, not a hunt for the `parents[N]`
that silently became wrong.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

BENCH = Path(__file__).resolve().parent
ROOT = BENCH.parent

K8S = BENCH / "k8s"
RESULTS = BENCH / "results"
LANDING_PAGE = ROOT / "www" / "index.html"

# The files a sweep writes and the report reads. Naming them here keeps the
# producer and the consumer from drifting apart under a rename.
LOCAL_FULL = RESULTS / "local_full.json"
LOCAL_QUICK = RESULTS / "local_quick.json"
K8S_RESULTS = RESULTS / "k8s.json"

# What `render` and `landing` read when no `--local` / `--k8s` is given. Later
# files win per measurement, so a re-measured point can be folded in by adding
# its file here.
#
# The files after `local_full` re-measure what changed rather than the whole
# matrix: every rivers benchmark, because the published run had been built
# without optimisation, and the Dagster and Prefect benchmarks whose drivers
# were corrected. `prefect_readpath` then replaces one point that had timed out
# on seeding, and `rivers_queue_limits` the three that a claim losing 20 races
# in a row had failed. `condition_pass_paced` re-measures all three, because
# that benchmark gained a 20,000-asset size. `parallel_and_log_capture`
# re-measures all three on two counts: rivers stopped truncating captured
# stdout at 4 MiB, whose old figures were fast partly because they dropped
# 29% of the lines at 100,000, and Prefect's parallel benchmark moved from
# threads to processes so that all three pay the same isolation cost.
# A fresh `bench/run.sh` writes
# `local_full` and passes it alone, which is correct — it measures everything
# again.
LOCAL_INPUTS = [
    LOCAL_FULL,
    RESULTS / "rivers_release.json",
    RESULTS / "dagster_fixed.json",
    RESULTS / "prefect_fixed.json",
    RESULTS / "prefect_readpath.json",
    RESULTS / "rivers_queue_limits.json",
    RESULTS / "condition_pass_paced.json",
    RESULTS / "parallel_and_log_capture.json",
]
K8S_INPUTS = [K8S_RESULTS]

FRAMEWORKS = ["rivers", "dagster", "prefect"]

# rivers is measured in the repository's own virtualenv, because maturin
# builds it there. Dagster and Prefect share one, which `setup.sh` builds from
# `envs/`. Every measurement still runs in a fresh process of its own.
INTERPRETERS = {
    "rivers": ROOT / ".venv" / "bin" / "python",
    "dagster": ROOT / ".venv-bench" / "bin" / "python",
    "prefect": ROOT / ".venv-bench" / "bin" / "python",
}
# Drivers are launched with `python -m` from ROOT, so each virtualenv finds the
# `bench` package through the working directory and needs nothing installed.
DRIVER_MODULES = {name: f"bench.drivers.{name}_bench" for name in FRAMEWORKS}


def resolve(path: str | Path) -> Path:
    """Make a path absolute, treating a relative one as relative to `bench/`."""
    file = Path(path)
    return file if file.is_absolute() else BENCH / file


def write_json(data: Any, path: Path) -> None:
    """Write results JSON, creating the directory if it is not there yet."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))
