# Kubernetes

Install rivers on a Kubernetes cluster using Helm. For a tour of how the
pieces interact, see [Overview](overview.md).

## Before you begin

- A Kubernetes cluster (1.28+).
- `kubectl` configured against it.
- `helm` 3.8+ (OCI registry support).

## Install with Helm

The CRDs are cluster-scoped and ship as a separate chart so multiple
`rivers` namespaces can share them. Install them once per cluster, then
the main `rivers` chart per namespace.

```sh
helm install rivers-crds \
  oci://ghcr.io/ion-elgreco/charts/rivers-crds \
  --version 0.0.0-dev \
  --namespace kube-system

helm install rivers \
  oci://ghcr.io/ion-elgreco/charts/rivers \
  --version 0.0.0-dev \
  --namespace rivers \
  --create-namespace
```

Wait for the operator and UI to come up:

```sh
kubectl -n rivers rollout status deploy/rivers-operator
kubectl -n rivers rollout status deploy/rivers-ui
```

!!! note
    The `rivers` chart bundles a SurrealDB subchart by default. To point
    rivers at an external SurrealDB, set `surrealdb.enabled=false` and
    `surrealdb.endpoint=wss://surreal.example.com:8000`.

## Reach the UI

By default the UI service is `ClusterIP`. For local access:

```sh
kubectl -n rivers port-forward svc/rivers-ui 3000:3000
# open http://localhost:3000
```

For public exposure, set `ui.serviceType=LoadBalancer` (cloud) or front
the service with your usual `Ingress` / `Gateway`.

## Deploy a CodeLocation

A `CodeLocation` is a deployment unit — one image containing a Python
module that exposes a `CodeRepository`. The operator pulls the image,
resolves a tag to a digest, and runs replicas of `rivers serve`.

```yaml title="analytics.yaml"
apiVersion: rivers.io/v1alpha1
kind: CodeLocation
metadata:
  name: analytics
  namespace: rivers
spec:
  image: ghcr.io/acme/pipelines
  tag: v0.2.0
  module: pipelines.analytics
```

```sh
kubectl apply -f analytics.yaml
kubectl -n rivers get codelocations
# NAME        PHASE   IMAGE                                    REPLICAS   AGE
# analytics   Ready   ghcr.io/acme/pipelines@sha256:abc12...   1/1        30s
```

Once `PHASE=Ready` the UI lists the location's assets, jobs, and run
history. Materializations triggered from the UI dispatch back to the
code-location's gRPC endpoint (`spec.grpcPort`, default `3001`).

A fuller spec wires in resources, env, and secrets:

```yaml
apiVersion: rivers.io/v1alpha1
kind: CodeLocation
metadata:
  name: analytics
  namespace: rivers
spec:
  image: ghcr.io/acme/pipelines
  tag: v0.2.0
  module: pipelines.analytics
  replicas: 3
  digestRefreshInterval: 5m
  resources:
    requests:
      cpu: 500m
      memory: 512Mi
    limits:
      cpu: "2"
      memory: 2Gi
  env:
    - name: SNOWFLAKE_ACCOUNT
      value: "acme-prod"
    - name: SNOWFLAKE_PASSWORD
      valueFrom:
        secretKeyRef:
          name: snowflake-creds
          key: password
  imagePullSecrets:
    - name: ghcr-pull-secret
  serviceAccountName: rivers-pipelines
```

!!! tip "Pinning to a digest"
    Set `spec.digest: sha256:...` directly to skip the registry probe
    (required for HTTP-only registries unless `operator.allowInsecureRegistry`
    is enabled). `spec.tag` is ignored when `digest` is set.

!!! warning "Identity is immutable"
    `spec.identity` is a UUID the mutating webhook stamps on creation
    and the validating webhook rejects changes to. It's the storage
    key for everything the operator writes about this CodeLocation in
    SurrealDB. To migrate a CodeLocation between namespaces:

    ```sh
    kubectl get codelocation analytics -n old -o yaml \
      | yq '.metadata.namespace = "new"' \
      | kubectl apply -f -
    kubectl delete codelocation analytics -n old
    ```

## Helm chart customizations

Drop these into a `values.yaml` and pass `-f values.yaml` on
`helm install` / `upgrade`:

```yaml
operator:
  replicas: 2
  # In production, leave false. Enable only for in-cluster HTTP-only
  # registries (e.g. k3d's local registry); CodeLocations against an
  # HTTP registry must otherwise pre-set spec.digest.
  allowInsecureRegistry: false

  webhook:
    # selfSigned (default): chart generates a self-signed CA + serving
    #   cert at install time. No external dependency.
    # certManager: emit a cert-manager Certificate; requires a working
    #   cert-manager and an issuerRef.
    certProvider: certManager
    certManager:
      issuerRef:
        name: letsencrypt-prod
        kind: ClusterIssuer

ui:
  enabled: true
  # ClusterIP (default), NodePort, or LoadBalancer
  serviceType: ClusterIP

surrealdb:
  enabled: true
  persistence:
    size: 10Gi
```

The full set of values is documented in
[`deploy/helm/rivers/values.yaml`](https://github.com/ion-elgreco/rivers/blob/main/deploy/helm/rivers/values.yaml).

## Authenticated SurrealDB connections

The bundled SurrealDB always runs authenticated. Every rivers pod
(operator, UI, code-location, run, step) signs in as a database-scoped
user (`DEFINE USER ... ON DATABASE`) on every connection — credentials
flow via `valueFrom.secretKeyRef` so the password value never lands in a
pod spec or CR.

### With the bundled SurrealDB (default)

Out of the box, `helm install` creates three Secrets and a Job without
any extra values:

- **`rivers-surrealdb-bootstrap`** — root creds for the bundled DB pod.
  Username defaults to `rivers-root`, password is `randAlphaNum 32` on
  first install and preserved across `helm upgrade` via Helm's `lookup`.
- **`rivers-surrealdb-auth`** — database-scoped rivers user. Username
  defaults to `rivers`, password auto-generated and preserved the same
  way. This is the Secret every rivers pod mounts.
- **`rivers-surrealdb-setup`** — pre-rendered SurrealQL file with the
  rivers user definition (password baked in from the same helper as the
  auth Secret). Mounted by the user-init Job.
- **`<release>-surrealdb-user-init-r<revision>` Job**: applies the setup
  file via `surreal import` against the bundled DB using the bootstrap
  root creds. `DEFINE USER ... OVERWRITE` makes the auth Secret the
  source of truth — rotate the Secret then `helm upgrade` to rotate the
  in-DB user. The Job runs as a regular resource (not a Helm hook); a
  `wait-for-surreal` init container on operator/UI pods gates them on
  SurrealDB readiness so they don't crashloop while the bundled pod is
  still starting.

To set the rivers user creds explicitly (instead of auto-generating):

```yaml
surrealdb:
  auth:
    username: rivers-prod
    password: change-me   # dev convenience; for prod use `existingSecret` instead
```

To bring your own Secret (production path):

```bash
kubectl -n rivers create secret generic rivers-prod-creds \
  --from-literal=username=rivers-prod \
  --from-literal=password='...'
```

```yaml
surrealdb:
  auth:
    existingSecret: rivers-prod-creds
    secretKeys:
      username: username
      password: password
```

The user-init Job then defines that user inside the bundled DB.

### With external SurrealDB

Define the user yourself and point the chart at the Secret:

```sql
-- Once, against your external SurrealDB:
DEFINE NAMESPACE IF NOT EXISTS rivers;
USE NS rivers;
DEFINE DATABASE IF NOT EXISTS main;
USE NS rivers DB main;
DEFINE USER `rivers-prod` ON DATABASE PASSWORD '...' ROLES OWNER;
```

```bash
kubectl -n rivers create secret generic rivers-surrealdb \
  --from-literal=username=rivers-prod \
  --from-literal=password='...'
```

```yaml
surrealdb:
  enabled: false
  endpoint: wss://surreal.example.com:443
  auth:
    namespace: rivers
    database: main
    existingSecret: rivers-surrealdb
```

No bootstrap Secret or init Job is created — those only exist for the
bundled DB. Rotate the user externally and update the Secret; the next
`helm upgrade` (or pod restart) picks up the new value via the
`secretKeyRef` mount.

### External SurrealDB without auth

For an unauthenticated external SurrealDB (e.g. a closed dev cluster), just
omit `auth.existingSecret` and `auth.username`/`password`:

```yaml
surrealdb:
  enabled: false
  endpoint: ws://surreal.dev:8000
  # auth.existingSecret / auth.username / auth.password all empty → no signin
```

Pods connect without `signin`, matching `surreal start --unauthenticated`.

### Why the bundled DB is always authenticated

There's no useful "unauthenticated bundled DB" mode — once SurrealDB is
sharing a cluster with rivers, anything else in the namespace can dial
`ws://surrealdb:8000` and read/write the orchestration state. The chart
removes the footgun by always requiring auth for the bundled path.

### Local dev (`rivers dev`)

`rivers dev` reads the same env vars (`RIVERS_SURREAL_USERNAME` /
`RIVERS_SURREAL_PASSWORD` / `RIVERS_SURREAL_NAMESPACE` /
`RIVERS_SURREAL_DATABASE`) for `--surreal-endpoint` connections, or none
of them when using embedded storage. Set them in your shell when pointing
`rivers dev` at an authenticated remote SurrealDB.

## UI authentication

The UI ships unauthenticated (`ui.auth.mode: none`). Before exposing it via
Ingress or HTTPRoute, enable one of the two auth modes:

```yaml
ui:
  auth:
    mode: oidc                        # or "forward" behind an auth proxy
    publicUrl: https://rivers.example.com
    oidc:
      issuer: https://keycloak.example.com/realms/main
      clientId: rivers
      existingSecret: rivers-oidc-client   # key: client-secret
```

`oidc` speaks OpenID Connect (code flow + PKCE) directly to your IdP;
`forward` trusts identity headers injected by an authenticating reverse
proxy (Authelia, oauth2-proxy, Envoy Gateway, …) from an explicit
`trustedProxies` CIDR list. Invalid combinations fail `helm install`
loudly. See the [authentication guide](../guides/authentication.md) for the
full option set, proxy header mappings, allowlists, and the launched-by
audit trail.

## Open ports

| Port    | Component         | Purpose                                       |
| ------- | ----------------- | --------------------------------------------- |
| `3000`  | `rivers-ui`       | Web UI (HTTP + Server-Sent Events)            |
| `3001`  | CodeLocation Pod  | gRPC — UI write paths (materialize, trigger)  |
| `8000`  | SurrealDB         | Storage backend (WebSocket protocol)          |
| `9443`  | `rivers-operator` | Admission webhook (HTTPS)                     |
| `50052` | `rivers-operator` | `CodeLocationRegistry` gRPC — UI discovery    |

## Upgrade

The chart, operator/UI images, and CRDs all release on the same `vX.Y.Z`
tag. To upgrade in place:

```sh
helm upgrade rivers-crds \
  oci://ghcr.io/ion-elgreco/charts/rivers-crds \
  --version 0.2.0 -n kube-system

helm upgrade rivers \
  oci://ghcr.io/ion-elgreco/charts/rivers \
  --version 0.2.0 -n rivers
```

Existing `CodeLocation` resources are re-reconciled against the new
operator without re-creation.

## Uninstall

```sh
kubectl delete codelocations -n rivers --all  # operator cleans up child resources first
helm uninstall rivers -n rivers
helm uninstall rivers-crds -n kube-system     # only when no other namespaces use rivers
```
