#!/usr/bin/env bash
# One-time setup for the orchestrator comparison benchmark.
#
# Nothing is installed ad hoc. Every dependency is declared and locked:
#
#   bench/envs/pyproject.toml + uv.lock   -> .venv-bench   (Dagster, Prefect)
#   pyproject.toml dev group (psutil)     -> .venv         (rivers)
#
# `uv sync --frozen` installs exactly what the lockfile says, so a rerun months
# from now reproduces the published numbers.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PYTHON_VERSION="3.13"

if [ ! -x .venv/bin/python ]; then
    echo "No .venv found. Run 'just develop-fast' to build rivers first." >&2
    exit 1
fi

if ! .venv/bin/python -c "import psutil" 2>/dev/null; then
    echo "psutil missing from .venv. Run 'just venv' to pick up the dev group." >&2
    exit 1
fi

echo "==> Dagster and Prefect"
uv venv --python "$PYTHON_VERSION" .venv-bench
VIRTUAL_ENV="$ROOT/.venv-bench" uv sync --project bench/envs --active --frozen

echo
echo "Ready. Next:"
echo "  bench/run.sh --quick     # a few minutes, checks everything works"
echo "  bench/run.sh             # the full sweep, about 50 minutes"
