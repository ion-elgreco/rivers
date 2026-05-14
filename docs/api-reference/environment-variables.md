# Environment Variables

rivers reads a number of environment variables to configure the daemon, the operator, and the standalone UI binary. Most have sensible defaults.

This page lists user-settable env vars that rivers itself reads. Variables consumed by user code (e.g. `pydantic_settings.BaseSettings`) are out of scope — see [Configuration](../concepts/configuration.md) for that pattern. Operator- and CLI-injected variables (code-location identity, run context, step-pod plumbing) are also omitted — they are managed by rivers itself.

## Deployment

| Variable | Default | Description |
|----------|---------|-------------|
| `RIVERS_DEPLOYMENT` | unset (treated as `dev`) | Either `dev` or `cloud`. `cloud` activates strict checks — most importantly, code-location identity becomes mandatory and rivers panics rather than silently writing under a default identity. Set automatically by `rivers serve` / `execute` / `execute-step`. |

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
| `RIVERS_CLAIM_TIMEOUT` | `600s` (~10 min) | Total time a step waits for a slot before failing with a claim timeout. |

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

## Standalone UI binary

Read by `rivers-ui` (the standalone UI server, distinct from the in-process UI started by `rivers dev`). Both flag and env-var forms are accepted.

| Variable | Default | Description |
|----------|---------|-------------|
| `RIVERS_REGISTRY_URL` | unset | Operator's `CodeLocationRegistry` gRPC URL (e.g. `http://rivers-operator-registry.rivers.svc:50052`). When unset, the UI starts with no known code locations. |
| `RIVERS_REGISTRY_TOKEN` | unset | Bearer token for the registry. Required when `RIVERS_REGISTRY_URL` is set. Passed via env so it doesn't show up in process listings. |

## Observability

| Variable | Default | Description |
|----------|---------|-------------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | When set, rivers installs an OpenTelemetry tracing layer that exports to this OTLP endpoint. Leave unset to disable OTel export entirely. |
| `RUST_LOG` | `info` | Standard `tracing-subscriber` env filter. Honoured by the operator and the UI binary. |
