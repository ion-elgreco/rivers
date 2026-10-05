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

### Storage flags

`materialize`, `run-action`, `backfill`, `backfill-status` and `backfill-cancel` choose their storage with the same flags:

| Flag | Description |
|------|-------------|
| `--surreal-endpoint` | Remote SurrealDB endpoint, e.g. `ws://surrealdb:8000`. Also read from `RIVERS_SURREAL_ENDPOINT`. Overrides `--storage-path` and `--memory`. |
| `--code-location-id` | The code location to record runs, asset state and pool claims under. Also read from `RIVERS_CODE_LOCATION_ID`. Default: `default`. |
| `--storage-path` | Embedded SurrealDB+RocksDB path. Kept at exit. |
| `--memory` | In-memory storage, lost at exit. |

When `RIVERS_SURREAL_ENDPOINT` and `RIVERS_CODE_LOCATION_ID` are set (the operator sets both on rivers pods), these commands use them without the flags. The endpoint also overrides `--storage-path` and `--memory`.

A deployed code location reads only what is recorded under its id: the `spec.identity` UUID of its `CodeLocation`.

```bash
kubectl get codelocation analytics -o jsonpath='{.spec.identity}'
```

Outside a rivers pod, pass that id with `--code-location-id`. Without it, the command records under code location `default`. So a `delete` removes the real data, but the code location still sees the partition as materialized, and the verb's pool claims do not block the code location's runs. The commands write a warning to stderr when they use `--surreal-endpoint` or `RIVERS_SURREAL_ENDPOINT` without a code location id. The command still runs.

Without `--surreal-endpoint`, `RIVERS_SURREAL_ENDPOINT` or `--storage-path`, the run's state and its pool claims go to a scratch store in a new temporary directory for each command. The CLI removes that store at exit, and no other process reads it. So a `delete` removes the real data, but the code location never sees the deletion, and the verb's pool claims do not block the code location's runs. To act on a code location's data, point the command at the storage that the code location uses, and give the code location's id.

`run-action` and `backfill --action` write a warning to stderr when a verb with outcome `Unmaterialize` (such as `delete`) runs on the scratch store or with `--memory`, which is also removed at exit. The command still runs.

---

## `dev` — local development server

```bash
rivers dev my_pipeline \
  --host 127.0.0.1 \
  --port 3000 \
  --grpc-port 3001 \
  --storage-path .rivers/storage/
```

Serves the embedded storage and the web UI from this process, and runs the code location (the gRPC backend and, unless `--no-daemon`, the automation daemon) in a child process. Tears down storage on exit.

### Reloading code

A reload replaces the code location with a fresh interpreter, so every edit is picked up: new or renamed assets, changed dependencies, new imports, edited helper modules. Storage, the UI server and open browser tabs stay up; the tabs refetch definitions once the new code location is serving.

The previous code location retires first: it stops scheduling and releases its gRPC port, and the new one starts as soon as the port is free, so two daemons never run at the same time. Runs that were in flight finish in the background with no time limit, and the retired process exits on its own; the console prints `Generation N finished.` Queued runs wait in storage for the new code location. Stopping `rivers dev` with Ctrl-C stops every generation, including those still finishing runs, with the usual 30 s cap.

Press **Reload code location** in the sidebar of the UI.

A code location that does not come back (a syntax error, a failed import, a failed resolve) does not end the session: the error shows in the console and under the reload button, and the next reload tries again. Only a code location that fails on the first start ends `rivers dev`.

The embedded storage is served on an ephemeral loopback port. With `--surreal-endpoint`, the UI and the code location connect to that server instead and nothing is served locally.

Every flag (and the module argument) can also come from configuration, with CLI flags taking precedence: `RIVERS_<GROUP>_<KEY>` environment variables (e.g. `RIVERS_SERVER_PORT`, `RIVERS_MODULE_PATH`), then the `[module]`/`[server]`/`[storage]`/`[daemon]` tables in `rivers.toml`, then `[tool.rivers.*]` in `pyproject.toml` — both files found by walking up from the current directory.

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

Connects to a remote SurrealDB instance, starts the gRPC backend, and runs the automation daemon. Designed to run inside a code-location pod.

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
| `--surreal-endpoint` / `--code-location-id` / `--storage-path` / `--memory` | Storage. See [Storage flags](#storage-flags). |

---

## `run-action` — run an asset action

```bash
rivers run-action my_pipeline optimize \
  --surreal-endpoint ws://surrealdb:8000 --code-location-id CODE_LOCATION_ID
rivers run-action my_pipeline delete --select events --partition-key 2024-01-15 \
  --surreal-endpoint ws://surrealdb:8000 --code-location-id CODE_LOCATION_ID
```

Resolves the repository and runs `repo.run_action(VERB, ...)` synchronously — the
[action](../concepts/actions.md) counterpart of `materialize`.

| Flag | Description |
|------|-------------|
| `--select`, `-s` | Comma-separated asset names. Default: every asset that defines the verb. |
| `--partition-key` | Partition key (string), as the verb's `partitioning` allows. |
| `--surreal-endpoint` / `--code-location-id` / `--storage-path` / `--memory` | Storage. See [Storage flags](#storage-flags). A verb that changes data needs the code location's storage and id. |

---

## `backfill` — partition-range execution

```bash
rivers backfill my_pipeline \
  --assets daily_events \
  --from 2024-01-01 --to 2024-01-31 \
  --strategy multi_run \
  --concurrency 4 \
  --surreal-endpoint ws://surrealdb:8000 \
  --code-location-id CODE_LOCATION_ID
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
| `--action` | none | Run this verb in every child run instead of materializing. |
| `--surreal-endpoint` / `--code-location-id` / `--storage-path` / `--memory` | scratch store | Storage. See [Storage flags](#storage-flags). |

---

## `backfill-status` / `backfill-cancel`

```bash
rivers backfill-status BACKFILL_ID my_pipeline \
  --surreal-endpoint ws://surrealdb:8000 --code-location-id CODE_LOCATION_ID
rivers backfill-cancel BACKFILL_ID my_pipeline \
  --surreal-endpoint ws://surrealdb:8000 --code-location-id CODE_LOCATION_ID
```

Both take the [storage flags](#storage-flags). Point them at the storage that the backfill runs against: a scratch store never holds an earlier backfill.

---

## `execute` / `execute-step` (Kubernetes-internal)

```bash
rivers execute my_pipeline --run-id RID --surreal-endpoint ws://surreal:8000
rivers execute-step my_pipeline --run-id RID --step-key my_asset
```

Designed for K8s execution pods. `execute` runs an entire run with a pre-assigned `run-id`; `execute-step` runs one step (used by step worker pods). Both read the run record for the verb and the per-asset config overrides the run was launched with, and exit 1 without one. With `--job`, `execute` fails the run, without starting it, when the job's verb in the pod's code differs from the verb on the run record.

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

`queue list` and `queue why` show each run's verb (`materialize` for a plain run),
and `backfill-status` shows the backfill's verb when it runs one.

## `db migrate` — storage schema migration

Brings a database up to the running rivers build's schema version, applying any pending migrations under a cross-process lease. Idempotent. Run it after upgrading rivers when a code location or the UI reports that the database needs migration; see [Storage › Schema versioning & migration](storage.md#schema-versioning-migration).

```bash
rivers db migrate                                          # embedded (default .rivers/storage/)
rivers db migrate --storage-path /data/rivers              # embedded, explicit path
rivers db migrate --surreal-endpoint ws://surrealdb:8000   # remote (or RIVERS_SURREAL_ENDPOINT)
```

In Kubernetes, run this as an init/job step before rolling out upgraded code locations. `rivers dev` offers to run it interactively when it finds the database behind the build.
