# Environment Variables

rivers reads a number of environment variables to configure the daemon, the operator, and the standalone UI binary. Most have sensible defaults.

This page lists user-settable env vars that rivers itself reads. Variables consumed by user code (e.g. `pydantic_settings.BaseSettings`) are out of scope — see [Configuration](../concepts/configuration.md) for that pattern. Operator- and CLI-injected variables (run context, step-pod plumbing) are also omitted — they are managed by rivers itself.

## Deployment

| Variable | Default | Description |
|----------|---------|-------------|
| `RIVERS_DEPLOYMENT` | unset (treated as `dev`) | Either `dev` or `cloud`. `cloud` activates strict checks — most importantly, code-location identity becomes mandatory and rivers panics rather than silently writing under a default identity. Set automatically by `rivers serve` / `execute` / `execute-step`. |
| `RIVERS_CODE_LOCATION_ID` | unset (code location `default`) | The code location that runs, asset state and pool claims are recorded under. The operator sets it on rivers pods to the `spec.identity` of the `CodeLocation`. Outside those pods, set it (or pass `--code-location-id`) when a [CLI](cli.md#storage-flags) command acts on a deployed code location. |

## Daemon and automation

| Variable | Default | Description |
|----------|---------|-------------|
| `RIVERS_TICK_BATCH_SIZE` | `32` (in-memory storage), `256` (SurrealDB) | Maximum number of automation tick records to accumulate before flushing. The tick writer flushes on a 500 ms timer or when this batch fills. |
| `RIVERS_MAX_CONDITION_EVALS` | `100` | Number of condition evaluations to retain per automation before pruning. Bounds growth of the eval-history table. Falls back to default on parse failure. |

## Concurrency-pool claim loop

Tunes how step workers wait for available slots in a concurrency pool. Durations are parsed with `humantime` (e.g. `"500ms"`, `"30s"`, `"5m"`).

| Variable | Default | Description |
|----------|---------|-------------|
| `RIVERS_CLAIM_POLL_INTERVAL` | `1s` | How often to re-check storage for an available slot. Shorter = faster pickup, more storage load. |
| `RIVERS_CLAIM_POLL_JITTER` | `500ms` | Maximum random jitter added to the poll interval to break up correlated retries. |
| `RIVERS_CLAIM_TIMEOUT` | `600s` (~10 min) | Total time a step waits for a slot before failing with a claim timeout. Time spent waiting on an asset's own pool (behind an exclusive [action](../concepts/actions.md)) does not count. |

## Operator

Read by the `rivers-operator` binary at startup.

| Variable | Default | Description |
|----------|---------|-------------|
| `RIVERS_METRICS_ADDR` | `0.0.0.0:9090` | Bind address for the operator's Prometheus `/metrics` and health endpoints. |
| `RIVERS_CODE_LOCATION_SERVICE_ACCOUNT` | `rivers-code-location` | ServiceAccount the operator stamps onto code-location pods (governs their RBAC). |
| `RIVERS_REGISTRY_ADDR` | `0.0.0.0:50052` | Bind address for the operator's `CodeLocationRegistry` gRPC service. |
| `RIVERS_REGISTRY_TOKEN` | unset | Bearer token clients must present to the registry. Leave unset to disable auth. |
| `RIVERS_WEBHOOK_ADDR` | `0.0.0.0:9443` | Bind address for the mutating-admission webhook (HTTPS). |
| `RIVERS_WEBHOOK_CERT_DIR` | `/etc/webhook-cert` | Directory holding `tls.crt` / `tls.key` (the conventional `kubernetes.io/tls` Secret layout — works with cert-manager out of the box). |
| `RIVERS_WEBHOOK_DISABLED` | unset (`"1"` to disable) | Disables the webhook entirely. Useful for local operator dev where you don't want to set up cert-manager. |

## Kubernetes step pods

Stamped by the executor onto every step pod (read them from asset/task code).

| Variable | Default | Description |
|----------|---------|-------------|
| `RIVERS_STEP_ATTEMPT` | `1` | 1-indexed attempt number of this step pod — increments on each [retry](retries.md). |

## Standalone UI binary

Read by `rivers-ui` (the standalone UI server, distinct from the in-process UI started by `rivers dev`). Both flag and env-var forms are accepted.

| Variable | Default | Description |
|----------|---------|-------------|
| `RIVERS_REGISTRY_URL` | unset | Operator's `CodeLocationRegistry` gRPC URL (e.g. `http://rivers-operator-registry.rivers.svc:50052`). When unset, the UI starts with no known code locations. |
| `RIVERS_REGISTRY_TOKEN` | unset | Bearer token for the registry. Required when `RIVERS_REGISTRY_URL` is set. Passed via env so it doesn't show up in process listings. |

## Observability

| Variable | Default | Description |
|----------|---------|-------------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | OTLP/gRPC endpoint. When set, rivers installs an OpenTelemetry tracing layer that exports to it. `https://` endpoints use TLS and verify the server against the system roots plus bundled Mozilla roots. Leave unset to disable OTel export entirely. |
| `OTEL_EXPORTER_OTLP_HEADERS` | unset | Headers sent with every export, as comma-separated `key=value` pairs. This is where API keys and bearer tokens go, e.g. `authorization=Bearer <token>` or `x-honeycomb-team=<key>`. |
| `OTEL_EXPORTER_OTLP_CERTIFICATE` | unset | Path to a PEM file holding the CA that signs the collector's certificate. Replaces the system and bundled roots. Requires an `https://` endpoint. |
| `OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE` / `OTEL_EXPORTER_OTLP_CLIENT_KEY` | unset | Paths to a PEM client certificate and private key for mutual TLS. Set both or neither. Requires an `https://` endpoint. |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | `10000` | Export timeout in milliseconds. |
| `OTEL_EXPORTER_OTLP_TRACES_*` | unset | Traces-specific form of each variable above (`..._TRACES_ENDPOINT`, `..._TRACES_HEADERS`, ...). Takes precedence over the generic one. An empty `..._TRACES_ENDPOINT` or certificate variable counts as unset. |
| `OTEL_SERVICE_NAME` | `rivers` | `service.name` on exported spans. A `service.name` entry in `OTEL_RESOURCE_ATTRIBUTES` also sets it. Set one or the other, not both. |
| `OTEL_RESOURCE_ATTRIBUTES` | unset | Extra resource attributes on exported spans, as comma-separated `key=value` pairs. |
| `RUST_LOG` | `info` | Standard `tracing-subscriber` env filter. Honoured by the operator and the UI binary. |

If the exporter cannot be built (for example an unreadable certificate file, or a certificate variable with an `http://` endpoint), rivers logs one error at startup naming the variable and runs with export disabled. Spans are sent in batches; when the process exits normally, rivers sends the spans still queued and waits at most five seconds for the collector. On Kubernetes the [Helm chart](../installation/kubernetes.md#opentelemetry-export) sets the endpoint and headers on every pod for you. The operator reads the endpoint from `RIVERS_OTEL_ENDPOINT`, not from `OTEL_EXPORTER_OTLP_ENDPOINT`.
