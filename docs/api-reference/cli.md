# CLI

The `rivers` console script is installed by `pip install rivers`. It exposes development, deployment, materialization, backfill, pool, and queue subcommands. Run any command with `--help` to see all flags.

```bash
rivers --help
```

## Loading a repository

Most commands take a Python module path as the first argument and a `--repo-var` flag (default `repo`) naming the `CodeRepository` instance:

```bash
rivers dev my_pipeline                        # loads my_pipeline.repo
rivers materialize my_pipeline --repo-var pipeline_repo
```

Storage flags follow the same pattern — `--memory` for in-memory or `--storage-path .rivers/storage` (default) for embedded SurrealDB+RocksDB.

---

## `dev` — local development server

```bash
rivers dev my_pipeline \
  --host 127.0.0.1 \
  --port 3000 \
  --grpc-port 3001 \
  --storage-path .rivers/storage/
```

Resolves the repository, then starts the gRPC backend, the web UI, and (unless `--no-daemon`) the automation daemon. Tears down storage on exit.

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `127.0.0.1` | UI/gRPC bind host. |
| `--port` | `3000` | Web UI port. |
| `--grpc-port` | `3001` | gRPC backend port. |
| `--storage-path` | `.rivers/storage/` | Embedded SurrealDB+RocksDB path. |
| `--surreal-endpoint` | unset | Connect to a remote SurrealDB instead of using embedded storage. |
| `--no-daemon` | `False` | Disable the automation daemon. |
| `--synthetic` | unset | Override the graph with a synthetic DAG (`100`, `1k`, `10k`, `50k`) for benchmarking. |

---

## `serve` — Kubernetes code-location server

```bash
rivers serve my_pipeline \
  --host 0.0.0.0 \
  --grpc-port 3001 \
  --surreal-endpoint $RIVERS_SURREAL_ENDPOINT
```

Connects to a remote SurrealDB instance, starts the gRPC backend and web UI, and runs the automation daemon. Designed to run inside a code-location pod.

`--surreal-endpoint` may also be set via the `RIVERS_SURREAL_ENDPOINT` env var.

---

## `materialize` — synchronous materialization

```bash
rivers materialize my_pipeline --partition-key 2024-01-15
```

Resolves the repository and runs `repo.materialize()` synchronously. Useful for batch runs from cron, CI, or one-off jobs.

| Flag | Description |
|------|-------------|
| `--partition-key` | Partition key (string). |
| `--memory` / `--storage-path` | Backend selection. |

---

## `backfill` — partition-range execution

```bash
rivers backfill my_pipeline \
  --assets daily_events \
  --from 2024-01-01 --to 2024-01-31 \
  --strategy multi_run \
  --concurrency 4
```

Launches `repo.backfill()` against either:

- `--partitions a,b,c` — explicit list, or
- `--from K --to K` — single-dimension range, or
- `--range dim=from..to` (repeatable) — multi-dimension range.

| Flag | Default | Description |
|------|---------|-------------|
| `--assets`, `-a` | all | Comma-separated asset names. |
| `--strategy` | none | `multi_run`, `single_run`, or `dim=mode,dim=mode` for `per_dimension`. |
| `--concurrency`, `-c` | `4` | Max concurrent partition runs. |
| `--on-failure` | `continue` | `continue` or `stop_on_failure`. |
| `--dry-run` | `False` | Preview without executing. |

---

## `backfill-status` / `backfill-cancel`

```bash
rivers backfill-status BACKFILL_ID my_pipeline
rivers backfill-cancel BACKFILL_ID my_pipeline
```

---

## `execute` / `execute-step` (Kubernetes-internal)

```bash
rivers execute my_pipeline --run-id RID --surreal-endpoint ws://surreal:8000
rivers execute-step my_pipeline --run-id RID --step-key my_asset
```

Designed for K8s execution pods. `execute` runs an entire run with a pre-assigned `run-id`; `execute-step` runs one step (used by step worker pods).

---

## `pools` — concurrency-pool management

```bash
rivers pools list                                 # show all configured pools
rivers pools info warehouse                       # claimed/pending + active holders
rivers pools set warehouse 8 --lease-duration 5m  # upsert slot limit and lease
```

All `pools` commands accept `--storage-path` to point at an alternate embedded backend.

---

## `queue` — run-queue inspection

```bash
rivers queue list                # queued runs sorted by priority + start time
rivers queue cancel RUN_ID       # cancel a not-yet-started run
rivers queue why RUN_ID          # explain why a queued run is blocked
```

## `db migrate` — storage schema migration

Brings a database up to the running rivers build's schema version, applying any pending migrations under a cross-process lease. Idempotent. Run it after upgrading rivers when a code location or the UI reports that the database needs migration; see [Storage › Schema versioning & migration](storage.md#schema-versioning-migration).

```bash
rivers db migrate                                          # embedded (default .rivers/storage/)
rivers db migrate --storage-path /data/rivers              # embedded, explicit path
rivers db migrate --surreal-endpoint ws://surrealdb:8000   # remote (or RIVERS_SURREAL_ENDPOINT)
```

In Kubernetes, run this as an init/job step before rolling out upgraded code locations. `rivers dev` offers to run it interactively when it finds the database behind the build.
