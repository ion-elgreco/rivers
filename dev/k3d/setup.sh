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
# RIVERS_K8S_SKIP_DEMO=1 (CI) drops the demo code-location — the integration
# suite only uses rivers-code-location.
code_location_images=(rivers-code-location)
[ "${RIVERS_K8S_SKIP_DEMO:-}" = "1" ] || code_location_images+=(rivers-demo)
for img in "${code_location_images[@]}"; do
    docker tag "${img}:latest" "${REGISTRY_HOST}/${img}:latest"
    docker push "${REGISTRY_HOST}/${img}:latest"
done

echo "==> Importing operator + UI images directly into k3d (pulled by tag)"
# RIVERS_K8S_SKIP_UI=1 (CI) drops the UI image — it isn't built or deployed.
import_images=(rivers-operator:latest)
[ "${RIVERS_K8S_SKIP_UI:-}" = "1" ] || import_images+=(rivers-ui:latest)
k3d image import "${import_images[@]}" -c "${CLUSTER_NAME}"

echo "==> Importing external images"
for img in minio/mc:latest minio/minio:latest surrealdb/surrealdb:v3; do
    # Skip the Docker Hub round-trip when a correct-arch copy is already
    # local (CI restores these from cache; locally they persist across runs).
    if [ "$(docker image inspect --format '{{.Architecture}}' "$img" 2>/dev/null)" != "arm64" ]; then
        docker pull --platform linux/arm64 "$img"
    fi
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
