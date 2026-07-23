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

# Opt-in browsable OIDC login (RIVERS_K8S_AUTH=1). The ferriskey release is
# already in the helmfile; here we wait for it, add the split-horizon DNS,
# bootstrap the `rivers` client + demo user, and flip rivers-ui to oidc. Needs
# the UI, so it's skipped under RIVERS_K8S_SKIP_UI=1.
if [ "${RIVERS_K8S_AUTH:-}" = "1" ] && [ "${RIVERS_K8S_SKIP_UI:-}" != "1" ]; then
    KCTX="k3d-${CLUSTER_NAME}"
    kc=(kubectl --context "${KCTX}" -n rivers)
    UI_CALLBACK="http://localhost:3000/auth/callback"
    DEMO_USER="john.doe"
    DEMO_PASS="Password123!"   # FerrisKey enforces a 12+ char policy

    echo "==> [auth] Waiting for ferriskey-api (image pull + DB migrations)"
    # The helmfile sync above may have been tolerated mid-timeout, so wait for
    # the Deployment object to exist before gating on its rollout.
    for _ in $(seq 1 30); do
        "${kc[@]}" get deploy/ferriskey-api >/dev/null 2>&1 && break
        sleep 2
    done
    if ! "${kc[@]}" rollout status deploy/ferriskey-api --timeout=480s; then
        echo "!! [auth] ferriskey-api never became ready — check 'kubectl -n rivers get pods'" >&2
        exit 1
    fi

    echo "==> [auth] Installing split-horizon DNS (coredns-custom)"
    kubectl --context "${KCTX}" apply -f "${SCRIPT_DIR}/coredns-ferriskey.yaml"
    if ! kubectl --context "${KCTX}" -n kube-system get cm coredns -o yaml \
        | grep -q 'import /etc/coredns/custom'; then
        echo "!! [auth] this k3s coredns Corefile does not import custom configs;"
        echo "   the rewrite won't apply and rivers-ui will fail OIDC discovery."
        echo "   Fallback: add the rewrite line into the 'coredns' ConfigMap directly."
    fi
    kubectl --context "${KCTX}" -n kube-system rollout restart deploy/coredns
    kubectl --context "${KCTX}" -n kube-system rollout status deploy/coredns --timeout=60s

    # Idempotent: the server-minted client secret is kept in a Secret so a re-run
    # (or a helmfile sync that reset the UI env) reuses it instead of failing on a
    # duplicate client. Delete rivers-ui-oidc-dev to force a re-bootstrap (e.g.
    # after the ephemeral ferriskey Postgres is wiped by a pod restart).
    SECRET_NAME="rivers-ui-oidc-dev"
    if client_secret="$("${kc[@]}" get secret "${SECRET_NAME}" \
        -o go-template='{{ index .data "client-secret" | base64decode }}' 2>/dev/null)"; then
        echo "==> [auth] Reusing the 'rivers' client secret from ${SECRET_NAME}"
    else
        echo "==> [auth] Bootstrapping the 'rivers' client + ${DEMO_USER} via the admin API"
        admin_pw="$("${kc[@]}" get secret ferriskey-api-admin \
            -o go-template='{{ .data.password | base64decode }}')"
        "${kc[@]}" port-forward svc/ferriskey-api 13333:3333 >/dev/null 2>&1 &
        pf_pid=$!
        trap 'kill "${pf_pid}" 2>/dev/null || true' EXIT
        base="http://127.0.0.1:13333/api"
        for _ in $(seq 1 60); do
            curl -fsS "${base}/realms/master/.well-known/openid-configuration" \
                >/dev/null 2>&1 && break
            sleep 2
        done

        tok="$(curl -fsS "${base}/realms/master/protocol/openid-connect/token" \
            -d grant_type=password -d client_id=admin-cli -d username=admin \
            --data-urlencode "password=${admin_pw}" | jq -r .access_token)"
        auth=(-H "authorization: Bearer ${tok}" -H 'content-type: application/json')

        client="$(curl -fsS "${auth[@]}" "${base}/realms/master/clients" -d '{
            "client_id":"rivers","name":"rivers","enabled":true,
            "protocol":"openid-connect","public_client":false,
            "service_account_enabled":false,"client_type":"confidential"}')"
        client_id="$(echo "${client}" | jq -r .id)"
        client_secret="$(echo "${client}" | jq -r .secret)"

        redirect="$(curl -fsS "${auth[@]}" \
            "${base}/realms/master/clients/${client_id}/redirects" \
            -d "{\"value\":\"${UI_CALLBACK}\"}")"
        redirect_id="$(echo "${redirect}" | jq -r .id)"
        curl -fsS -X PUT "${auth[@]}" \
            "${base}/realms/master/clients/${client_id}/redirects/${redirect_id}" \
            -d "{\"enabled\":true,\"value\":\"${UI_CALLBACK}\"}" >/dev/null

        user="$(curl -fsS "${auth[@]}" "${base}/realms/master/users" -d '{
            "username":"john.doe","email":"john.doe@example.com",
            "firstname":"John","lastname":"Doe","email_verified":true}')"
        user_id="$(echo "${user}" | jq -r .data.id)"
        curl -fsS -X PUT "${auth[@]}" \
            "${base}/realms/master/users/${user_id}/reset-password" \
            -d "{\"credential_type\":\"password\",\"value\":\"${DEMO_PASS}\",\"temporary\":false}" \
            >/dev/null

        kill "${pf_pid}" 2>/dev/null || true
        trap - EXIT
        "${kc[@]}" create secret generic "${SECRET_NAME}" \
            --from-literal=client-secret="${client_secret}"
    fi

    echo "==> [auth] Switching rivers-ui to OIDC mode"
    "${kc[@]}" set env deploy/rivers-ui -c ui \
        RIVERS_AUTH_MODE=oidc \
        RIVERS_AUTH_OIDC_ISSUER="http://ferriskey.localtest.me:3333/api/realms/master" \
        RIVERS_AUTH_OIDC_CLIENT_ID=rivers \
        RIVERS_AUTH_OIDC_CLIENT_SECRET="${client_secret}" \
        RIVERS_AUTH_PUBLIC_URL="http://localhost:3000"
    "${kc[@]}" rollout status deploy/rivers-ui --timeout=180s
fi

echo ""
echo "Cluster '${CLUSTER_NAME}' is ready."
echo "  kubectl -n rivers get pods"
echo "  kubectl -n rivers get codelocations"
if [ "${RIVERS_K8S_AUTH:-}" = "1" ] && [ "${RIVERS_K8S_SKIP_UI:-}" != "1" ]; then
    echo ""
    echo "Auth is ON (FerrisKey OIDC). Start the port-forwards, then sign in:"
    echo "  just k8s-auth-forward"
    echo "  open http://localhost:3000  →  sign in as  john.doe / Password123!"
    echo "  (login page served from http://ferriskey.localtest.me:8080; localtest.me"
    echo "   resolves to 127.0.0.1 — if your resolver blocks it, add"
    echo "   '127.0.0.1 ferriskey.localtest.me' to /etc/hosts)"
else
    echo "  kubectl -n rivers port-forward svc/rivers-ui 3000:3000"
fi
