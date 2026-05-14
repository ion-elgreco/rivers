#!/usr/bin/env bash
# Bring up the local k3d test cluster: create cluster + registry, push
# code-location images into the registry, import the rest directly into
# the cluster, then `helmfile sync` (rivers-crds → rivers → rivers-dev).
# The operator runs with allowInsecureRegistry=true so the digest-resolve
# probe can talk to the HTTP-only k3d registry — no manual pinning needed.

set -euo pipefail

CLUSTER_NAME="${RIVERS_K3D_CLUSTER:-rivers-test}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Local OCI registry created alongside the cluster. Code-location images
# get pushed here so the operator can resolve a real manifest digest from
# them (executor pod pulls require `image@sha256:...`). Operator/UI/external
# images stay on the `k3d image import` path — pulled by tag, no digest.
REGISTRY_NAME="k3d-rivers-registry"
REGISTRY_HOST_PORT="5111"
REGISTRY_HOST="localhost:${REGISTRY_HOST_PORT}"

echo "==> Creating k3d cluster: ${CLUSTER_NAME}"
if k3d cluster get "${CLUSTER_NAME}" &>/dev/null; then
    echo "    Cluster already exists, skipping creation"
else
    k3d cluster create "${CLUSTER_NAME}" \
        --agents 1 \
        --registry-create "${REGISTRY_NAME}:0.0.0.0:${REGISTRY_HOST_PORT}" \
        --wait
fi

echo "==> Pushing code-location images to local registry"
for img in rivers-code-location rivers-demo; do
    docker tag "${img}:latest" "${REGISTRY_HOST}/${img}:latest"
    docker push "${REGISTRY_HOST}/${img}:latest"
done

echo "==> Importing operator + UI images directly into k3d (pulled by tag)"
k3d image import rivers-operator:latest rivers-ui:latest -c "${CLUSTER_NAME}"

echo "==> Importing external images"
for img in minio/mc:latest minio/minio:latest surrealdb/surrealdb:v3; do
    docker pull --platform linux/arm64 "$img"
    k3d image import "$img" -c "${CLUSTER_NAME}"
done

echo "==> helmfile sync (rivers-crds → rivers → rivers-dev)"
cd "${SCRIPT_DIR}"
# helmfile uses a tight timeout for fast-fail dev iteration; the
# rivers chart's `wait-for-surreal` init container does the actual
# readiness gating, so a `--wait` timeout doesn't mean the install
# failed — resources keep converging in the background. Tolerate
# non-zero so the rest of setup.sh (status banner) still runs.
helmfile sync || echo "==> helmfile timed out; resources still converging — check 'kubectl -n rivers get pods'"

echo ""
echo "Cluster '${CLUSTER_NAME}' is ready."
echo "  kubectl -n rivers get pods"
echo "  kubectl -n rivers get codelocations"
echo "  kubectl -n rivers port-forward svc/rivers-ui 3000:3000"
