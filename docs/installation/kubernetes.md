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

## Git-sourced CodeLocations

Instead of baking your code into an image, point a `CodeLocation` at a
**git repository**. The operator resolves the ref to a pinned commit, an
init container fetches the code and installs dependencies with `uv` into a
mounted workspace, and every run pins that exact commit — the release
pipeline shrinks to `git push`.

```yaml title="analytics-git.yaml"
apiVersion: rivers.io/v1alpha1
kind: CodeLocation
metadata:
  name: analytics
  namespace: rivers
spec:
  git:
    url: https://github.com/acme/pipelines.git
    ref:
      branch: main            # exactly one of branch | tag | commit
    path: analytics           # subdirectory holding the project
    secretRef:
      name: git-creds
  module: analytics.pipeline
```

```sh
kubectl -n rivers get rcl
# NAME        PHASE   SOURCE         REPLICAS   AGE
# analytics   Ready   main@9f3c1ab   1/1        45s
```

**Pinning a commit**: `ref.commit` takes the full 40-character SHA in
lowercase, as `git rev-parse` prints it. The same applies to
`spec.source.git.commit` of a `Run` that you create with your own
`spec.source`. The API server and the webhook reject a short or uppercase
SHA. A CodeLocation that has one all the same (for example, one created
before these checks) fails with reason `InvalidRef`, and the operator
does not deploy it.

How it composes with `image`: `spec.image` is always *the container the
pods run*. Omit it in git mode to run the chart's default runtime image
(`codeLocation.runtime.*`, by default
`ghcr.io/ion-elgreco/rivers-runtime:<chart version>-py3.12`). Each release
publishes this image for Python 3.11, 3.12 and 3.13 (tags
`<version>-py3.11`, `-py3.12`, `-py3.13`), for `linux/amd64` and
`linux/arm64`. `spec.tag` or `spec.digest` on its own replaces that
image's tag or digest, e.g. `tag: 0.5.0-py3.11` for another interpreter.
Set `spec.image` to use a private mirror
(`image: harbor.internal/rivers/rivers-runtime`, `tag: 0.5.0-py3.11`); as
in image mode, it then resolves `spec.tag` (default `latest`) or
`spec.digest`.

**Dependencies** come from the repo itself. With `dependencies.mode: auto`
(the default), the init container looks in the project directory (`path`):

1. `uv.lock` ⇒ `uv sync --locked`.
2. Else `requirements.txt` ⇒ `uv pip install`.
3. Else `pyproject.toml` in a uv workspace ⇒ `uv sync --locked` with the
   workspace's `uv.lock`. uv writes one `uv.lock`, at the workspace root,
   so a member has none of its own. The init container uses the nearest
   `uv.lock` above the project, up to the repository root, if the
   `pyproject.toml` next to it has `[tool.uv.workspace]`. uv installs the
   project and the workspace members it depends on. If the workspace does
   not include the project (`members`, `exclude`), the install fails with
   uv's error.
4. Else nothing is installed.

Set `dependencies.mode` to `uvSync`, `requirements` or `none` to choose
the mode yourself, for example `uvSync` for a workspace member that also
has a `requirements.txt`. The lockfile should include `rivers` — the
venv's `rivers` is what runs.

**Credentials Secret keys** (Flux-compatible): the url's scheme selects
the keys. `https://` (and `http://`) urls use `username`/`password` (a
forge token is a password). Without these two keys the fetch is
anonymous; a Secret with only one of them is an error. Newlines at the
end of these two values are ignored, so a file that ends in a newline
works with `kubectl create secret --from-file`. `ssh://` urls need
`identity` + `known_hosts`, and the url must name the user that the key
logs in as: `ssh://git@github.com/acme/pipelines.git` (`git` on most git
hosts). Without a user, ssh in the pods would log in as the pod's own
account: the webhook rejects such a url, and the operator fails a
CodeLocation that has one with reason `GitAuthFailed`. The keys of the
other scheme are ignored, so one Secret with all four keys can serve
both kinds of url. `identity`
must be a private key without a passphrase. `known_hosts` supports exact
and hashed (`|1|`) entries only — no `*` wildcards or `@cert-authority`
lines; list each host explicitly. Each line must be an entry or a
comment: the operator refuses a `known_hosts` with any other line and
does not connect to the host. Ed25519, ECDSA and RSA host keys work, not
DSA. One key per host is enough: the operator asks each host only for the
key types that `known_hosts` lists for it. If `secretRef` names a Secret
that does not exist, or that the operator may not read, the CodeLocation
fails with reason `GitAuthFailed`.

**Shared workspace** (recommended where you have RWX storage): set
`codeLocation.workspace.shared.enabled=true` and the code+venv is built
once on a per-CL PVC, then mounted read-only by every run and step pod —
a wide fan-out starts as fast as image mode. Each tree is for one commit,
one runtime image, one `path` and one `dependencies` setting
(`timeoutSeconds` excluded). If you change one of these, the
code-location pod builds a new tree. Without RWX storage every pod builds
its own tree; that's fine for `executor: parallel` but slow for wide
`executor: kubernetes` fan-outs.

**uv cache**: in shared mode, uv's cache is on the PVC (`cache/`). After
each successful build, the code-location pod runs `uv cache prune --ci`:
only the wheels that uv built from source stay, and pre-built wheels
download again on the next build. In fallback mode, pods install with
`UV_NO_CACHE=1` and keep no cache. The `emptyDir` limit
(`codeLocation.workspace.sizeLimit` or `spec.git.workspaceSize`) must hold
the checkout and the venv. During an install, uv's temporary copy of the
wheels is in the container's `/tmp`, outside this limit.

**Old trees**: in shared mode, each time a code-location pod starts, it
deletes old trees from the PVC. It keeps the trees that the CodeLocation
and its unfinished runs use, the `codeLocation.workspace.keepRevisions`
newest trees (default 3), and all trees younger than
`codeLocation.workspace.minTreeAge` (default `1h`). Write `minTreeAge` as
a whole number with `s`, `m` or `h`, for example `90m` or `24h`. The
operator does not start if it cannot read `minTreeAge` (for example `1d`,
`1h30m` or `1.5h`) or `keepRevisions`. If the pod cannot read these
limits, for example because `spec.env` sets
`RIVERS_WORKSPACE_MIN_AGE_SECONDS` or `RIVERS_WORKSPACE_KEEP_REVISIONS` to
a value that is not a whole number, it deletes no tree, and its init
container log gives the reason.

**Which commit a run uses**: runs get the last *fully rolled-out* commit.
After a push, the operator rolls the code-location pods to the new
commit, but `status.resolvedCommit` moves to it only when every pod runs
it and is ready. `status.runSource` records that tree's full source (url,
commit, path, Secret, dependencies, and the runtime image that built it,
which is also `status.resolvedImage`); it moves with `resolvedCommit`.
Until then runs keep the previous commit, and if the new commit fails to
build they keep it until a later commit rolls out. Meanwhile the
CodeLocation stays `Ready`: the git
Deployment never takes an old pod down before its replacement is ready, so
`spec.replicas` pods stay ready throughout (a rollout needs room for one
extra pod). A `Run` you create yourself without `image` gets this tree:
the webhook copies `status.runSource` into the run's `spec.source`, and
its runtime image into `spec.image`. A change of the url, `path`, Secret,
`dependencies` or runtime image also reaches runs only when its rollout
finishes. Just after you add `spec.git` to an image-mode CodeLocation,
`status.runSource` is empty and the webhook rejects such runs; try again
when the first tree has rolled out.
A run launched from a code-location pod (UI launches, schedules, sensors,
backfills) pins the commit that pod serves, so during a rollout each run
uses the same tree as the pod that launched it. The webhook accepts the
url and Secret of such a run if they are those of `spec.git`, or, while a
change of them rolls out, those of `status.runSource`. To wait until a
push is live:
`kubectl -n rivers wait rcl/analytics --for=jsonpath='{.status.resolvedCommit}'=<sha>`.

**Other images for run and step pods**: in git mode,
`RunBackendConfig.kubernetes(image=...)` and
`Executor.kubernetes(worker_image=...)` replace only the image of the
main container of run and step pods. These pods still run the tree that
the runtime image built: the checkout and the venv. The run records that
image in `spec.source.runtimeImage`, and in fallback mode the init
container that builds the pod's tree runs it. The main container starts
`/workspace/venv/bin/rivers`, so its image must have the same Python
version at the same path as the runtime image: build it `FROM` the
runtime image. A `Run` you create with a digest `image` and your own
`spec.source` must set `spec.source.runtimeImage` to a digest in the
CodeLocation's runtime image repository: that of `spec.image` (or of the
chart's default runtime image), or, while a change of `spec.image` rolls
out, that of `status.resolvedImage`. The webhook rejects other images.

**Egress**: in shared mode only the code-location pod needs outbound
access to the git host and the package index (run/step pods fetch
nothing); in fallback mode every pod does. Adjust NetworkPolicies
accordingly. The operator's requests to git hosts and registries carry
`User-Agent: rivers-operator/<version>`, for firewalls that filter on it.

**When things fail**, `kubectl describe rcl analytics` carries the
answer: `RefNotFound` / `InvalidRef` / `GitAuthFailed` / `GitHostKeyRejected` /
`GitMalformedResponse` on the `SourceResolved` condition for resolution
problems, and a failed install surfaces `uv`'s error tail in
`status.message`. A resolution problem sets the phase to `Failed`, and
new runs are rejected until you fix it. A git host that does not answer
(connection error, HTTP 5xx, or no complete answer within
`operator.git.timeoutSeconds`, 30 seconds by default) does not take the
CodeLocation down: it stays `Ready` on the commit it serves, with
`SourceResolved` `False`, reason `GitUnreachable`, and the error, also
in `status.message`. In shared mode, runs keep using that commit's tree. In
fallback mode, each run pod fetches the commit itself, so a run that
starts while the host is down fails. The operator asks the host again
after 1, 2 and 4 minutes, then every 5 minutes, and picks up new commits
when the host answers. A host that rate-limits the operator (HTTP 429,
or 503 with `Retry-After`) has the same result, with reason
`GitRateLimited`. The operator then sends no request to that host, for
any CodeLocation, until the host's `Retry-After` time is over: at most
one hour, and not sooner than the steps above. The message gives the
time of the next request. Before the first commit has rolled out there
is nothing to serve, and the CodeLocation is `Failed` until the host
answers.

A rollout shows on the `DeploymentAvailable` condition: reason
`RollingOut` while it runs, or `ProgressDeadlineExceeded` when it is
stuck (for example, the new commit fails to install), with a message that
names the commit being rolled out and the commit runs still use.

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

## OpenTelemetry export

Every pod that runs rivers Python code (code-location, run, step) can
export traces over OTLP/gRPC. The operator and UI do not export traces. Set
the endpoint once; the operator stamps it on every pod it creates, and the
run pod stamps it on its step pods. The chart gives the endpoint to the
operator as `RIVERS_OTEL_ENDPOINT`, so an `OTEL_EXPORTER_OTLP_ENDPOINT` that
something else injects into the operator pod does not reach your pods:

```yaml
otel:
  endpoint: https://otlp.example.com:4317
```

Most hosted backends need an API key or bearer token. Put it in a Secret in
OTLP header form and reference the Secret. The value reaches pods via
`valueFrom.secretKeyRef` and never lands in a pod spec or CR:

```bash
kubectl -n rivers create secret generic otel-headers \
  --from-literal=headers='authorization=Bearer ...'
```

```yaml
otel:
  endpoint: https://otlp.example.com:4317
  headers:
    existingSecret: otel-headers   # key: headers
```

`https://` endpoints use TLS with the image's system roots plus bundled
Mozilla roots, so images without a CA bundle work too. To override the
endpoint or headers for one code location, set the same
`OTEL_EXPORTER_OTLP_*` variables in `CodeLocation.spec.env`; each entry there
replaces the chart value of the same name. A node-local collector endpoint
such as `http://$(HOST_IP):4317` cannot go in `otel.endpoint`. Leave
`otel.endpoint` unset and put two entries in `CodeLocation.spec.env`: first
`HOST_IP` from a `status.hostIP` `fieldRef`, then the endpoint. Kubernetes
expands `$(HOST_IP)` only from entries earlier in the list.

Spans from every pod have the service name `rivers`. To tell code locations
apart in your backend, set `OTEL_SERVICE_NAME` in each `CodeLocation.spec.env`,
for example `analytics`. Its run and step pods get the same name. The
[environment variable reference](../api-reference/environment-variables.md#observability)
lists every variable rivers reads.

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

Then migrate the storage schema once, before the upgraded UI and code
locations open the database — a newer build refuses an older database until
it is migrated. Run it from any image that has the new rivers:

```sh
rivers db migrate --surreal-endpoint ws://surrealdb:8000
```

A migration can also raise the oldest build allowed to *write* (see
[Schema versioning](../api-reference/storage.md#schema-versioning-migration)).
When it does, rebuild every `CodeLocation` image on the new rivers version
right after migrating: an image built on an older rivers is refused as a
writer and its code location stops serving. Schema versions 5 and 6 (asset
actions) both raise the write floor.

## Uninstall

```sh
kubectl delete codelocations -n rivers --all  # operator cleans up child resources first
helm uninstall rivers -n rivers
helm uninstall rivers-crds -n kube-system     # only when no other namespaces use rivers
```
