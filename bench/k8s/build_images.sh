#!/usr/bin/env bash
# Build the Dagster and Prefect images the Kubernetes comparison needs.
#
# Dagster publishes amd64-only images, so they are rebuilt natively here to
# keep emulation out of the measurement. Prefect publishes native arm64, so its
# image only adds the flows that get registered as deployments.
#
# rivers' images are a separate script because they need a 25-minute cargo
# release build: bench/k8s/build_rivers_images.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

IMAGES="bench/k8s/images"
# Keep in sync with bench/envs/pyproject.toml and bench/k8s/deploy.py.
DAGSTER_VERSION="1.13.22"
DAGSTER_LIB_VERSION="0.29.22"
PREFECT_VERSION="3.8.5"

echo "==> Dagster ${DAGSTER_VERSION}"
docker build -f "$IMAGES/Dockerfile.dagster" \
    --build-arg "DAGSTER_VERSION=${DAGSTER_VERSION}" \
    --build-arg "DAGSTER_LIB_VERSION=${DAGSTER_LIB_VERSION}" \
    -t "dagster-bench:${DAGSTER_VERSION}" "$IMAGES"

echo "==> Dagster code location"
docker build -f "$IMAGES/Dockerfile.dagster-usercode" \
    --build-arg "BASE=dagster-bench:${DAGSTER_VERSION}" \
    -t "dagster-bench-usercode:${DAGSTER_VERSION}" "$IMAGES"

echo "==> Prefect flows ${PREFECT_VERSION}"
docker build -f "$IMAGES/Dockerfile.prefect-flows" \
    --build-arg "PREFECT_VERSION=${PREFECT_VERSION}" \
    -t "prefect-bench-flows:${PREFECT_VERSION}" "$IMAGES"

docker images --format "{{.Repository}}:{{.Tag}} {{.Size}}" | grep -E "^(dagster|prefect)-bench"
