#!/usr/bin/env bash
# Run the orchestrator comparison, then regenerate bench/RESULTS.md and the
# benchmark section of the landing page in www/index.html.
#
#   bench/run.sh --quick      Small sizes only. Use this first. About 15 minutes.
#   bench/run.sh              Full sweep, no Kubernetes. About two hours.
#   bench/run.sh --with-k8s   Full sweep plus Kubernetes. About three hours.
#
# The Kubernetes part needs docker, k3d, helm and kubectl, and builds release
# rivers images first, which adds about 25 minutes on a cold cargo cache.
#
# To run one piece on its own, call it as a module from the repository root:
#
#   .venv/bin/python -m bench.local.sweep --only sensor_pass --framework rivers
#   .venv/bin/python -m bench.k8s.bench --orchestrator rivers --repeat 3
#   .venv/bin/python -m bench.report.render --out RESULTS.md
set -euo pipefail

# Every entry point is a module under `bench`, so it resolves through the
# working directory. Stay in the repository root for the whole run.
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="$ROOT/.venv/bin/python"
cd "$ROOT"

MODE="full"
case "${1:-}" in
    --quick) MODE="quick" ;;
    --with-k8s) MODE="k8s" ;;
    "") ;;
    *) echo "unknown option: $1" >&2; exit 1 ;;
esac

if [ ! -x .venv-bench/bin/python ]; then
    echo "Environment missing. Run bench/setup.sh first." >&2
    exit 1
fi

# These numbers get published, so they have to come from an optimized rivers.
# `just develop-fast` defaults to the dev profile, which is opt-level 0 — it
# measured 4.5x slower on `resolve()` — and nothing about the installed
# extension says which profile built it. Comparing it against the release
# artifact does, and costs a second.
EXTENSION="python/rivers/_core.abi3.so"
RELEASE_LIB=""
for candidate in target/release/librivers.dylib target/release/librivers.so; do
    [ -f "$candidate" ] && RELEASE_LIB="$candidate"
done
if [ -z "$RELEASE_LIB" ] || ! cmp -s "$EXTENSION" "$RELEASE_LIB"; then
    echo "rivers is not built in release; its figures would be wrong." >&2
    echo "Build it first:  PROFILE=release just develop-fast" >&2
    exit 1
fi

if [ "$MODE" = "quick" ]; then
    echo "==> Quick sweep"
    "$PYTHON" -u -m bench.local.sweep --quick
    echo
    echo "Quick sweep done. Results in bench/results/local_quick.json"
    exit 0
fi

echo "==> Full local sweep"
"$PYTHON" -u -m bench.local.sweep --full --default-interval

INPUTS=(--local results/local_full.json)
if [ "$MODE" = "k8s" ]; then
    echo
    echo "==> Building Dagster and Prefect images"
    bench/k8s/build_images.sh

    echo
    echo "==> Building release rivers images"
    bench/k8s/build_rivers_images.sh

    echo
    echo "==> Kubernetes control-plane startup, three installs each"
    "$PYTHON" -u -m bench.k8s.bench --repeat 3
    INPUTS+=(--k8s results/k8s.json)
fi

echo
echo "==> Rendering results"
"$PYTHON" -m bench.report.render "${INPUTS[@]}" --out RESULTS.md

# The landing page publishes the same rows, so it is regenerated from the same
# inputs here. Its section in the page says to rerun this script.
"$PYTHON" -m bench.report.landing "${INPUTS[@]}"

echo
echo "Results:      bench/RESULTS.md"
echo "Raw data:     bench/results/*.json"
echo "Landing page: www/index.html"
