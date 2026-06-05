# Comparison with Dagster

rivers takes the asset-graph orchestration model that Dagster pioneered and brings it to the compiled world.

This page is intentionally **not** a feature-parity matrix. It only lists what rivers does *differently or better*; for everything else (assets, partitions, IO handlers, schedules, sensors, automation conditions, backfills, jobs) assume the surface area is comparable.

## Native Rust control plane

Dagster's daemon, schedulers, sensors, partition logic, and execution planning run as Python processes. rivers compiles every one of those into a single Rust binary.

| Concern | Dagster | rivers |
|---------|---------|--------|
| Graph resolution | Python | Compiled Rust (`petgraph`) |
| Partition mapping & enumeration | Python | Compiled Rust |
| Execution planner | Python | Compiled Rust |
| Daemon (schedules, sensors, automation conditions, run queue, backfills) | Python | Compiled Rust |
| Web UI server | Python + JavaScript bundle (Dagit / Webserver) | Rust (Axum + Leptos SSR + WASM hydration) |
| Kubernetes integration | `dagster-k8s` launcher | Native Rust operator (`kube-rs`) with CRDs |
| Storage backend | Python ORM over PostgreSQL/MySQL/SQLite | SurrealDB v3 (embedded RocksDB locally; SurrealDB server + TiKV for HA) |
| Python interpreter on the control plane | Yes (everywhere) | **No** — Python only runs inside user code |

Plan times stay sub-millisecond on graphs with thousands of nodes; the daemon ticks evaluate automation conditions in Rust without holding the GIL.

## Single-binary developer experience

`rivers dev <module>` boots the storage layer (embedded SurrealDB + RocksDB), the scheduler, the gRPC code-location backend, and the web UI on `:3000` — all in one process.

```bash
rivers dev pipelines.analytics
```

No `dagster-webserver` + `dagster-daemon` + Postgres juggling, no `workspace.yaml`, no docker-compose. The same binary is what ships to production via Helm.

## Native Kubernetes operator

rivers ships a Rust Kubernetes operator with first-class CRDs (`CodeLocation`, `Run`) instead of the Python-driven `dagster-k8s` launcher and `dagster-cloud` deployment glue. Both CRDs are reconciled by the operator — `CodeLocation`s become Deployments + Services running `rivers serve`, `Run`s become run pods that drive the execution. The control plane *is* the Kubernetes API.

### Registering a code location is one `kubectl apply`

```yaml
apiVersion: rivers.io/v1alpha1
kind: CodeLocation
metadata:
  name: analytics
spec:
  image: ghcr.io/acme/pipelines
  tag: v0.2.0
  module: pipelines.analytics
```

```bash
kubectl apply -f codelocation.yaml
```

That's the whole registration step. The operator resolves `image:tag` to a digest, reconciles a `Deployment` + `Service` running `rivers serve`, and registers the resulting gRPC endpoint in its in-process `CodeLocationRegistry`. The UI queries that registry on every page load — no `workspace.yaml` to maintain, no UI-side "add code location" flow, no agent process to register against.

To add a second pipeline, write another `CodeLocation` and apply it; to remove one, `kubectl delete`. Multi-tenancy is namespace-scoped like any other K8s resource.

### Operator capabilities

- **`CodeLocation` reconciler** — resolves `image:tag` to an immutable digest, reconciles Deployment + Service running `rivers serve`, re-polls on `digestRefreshInterval`. Cross-code-location asset deps (RFC 035) are wired up via the discovery registry.
- **`Run` reconciler** — each `Run` CR becomes a run pod the operator schedules and watches; when the run uses `Executor.kubernetes(...)`, that pod fans out step pods with native concurrency caps (`max_concurrent_steps`), image-pull credentials resolved from cluster secrets, and resource requests/limits per asset.
- **Admission webhooks** — a mutating webhook stamps `spec.identity` (UUID) on create; a validating webhook rejects identity changes.
- **Live discovery registry** — the UI dials the operator's gRPC registry; no static workspace file, no restart to pick up new locations.
- **HA operator + Prometheus metrics** — leader election via `coordination.k8s.io/Lease`; exports `rivers_registry_request_total{registry,outcome}`, `rivers_digest_cache_hits_total`, `rivers_codelocation_digest_resolution_seconds`, `rivers_operator_leader`.
- **Storage isolated by stamped identity** — every storage row is scoped by the immutable `spec.identity` UUID, so renaming a `CodeLocation` (or its namespace) doesn't orphan its run history.
- **Structural env propagation** — `CodeLocation.spec.env` flows end-to-end into run pods *and* step pods, preserving `secretKeyRef` / `configMapKeyRef` / `fieldRef` references.
- **Two-phase graceful shutdown** — SIGTERM flips `/readyz` to fail (drain), a drain token quiesces new work, then a shutdown token finalises in-flight steps.
- **Multi-arch images + SLSA provenance** — `ghcr.io/ion-elgreco/rivers-{operator,ui}` ship `linux/amd64` and `linux/arm64` with build-provenance attestations on every release.

## Daemon per code location

Dagster ships **one global daemon process** (`dagster-daemon`) that owns schedules, sensors, automation, and the run queue for every code location in the workspace. A slow eval in one location, a memory leak in one user's sensor, or a crash in any handler takes the whole daemon down — and with it, every other team's triggers.

rivers runs **one daemon per code location**, in-process inside the `rivers serve` pod that already hosts that location's code:

- **Isolation** — a misbehaving sensor in `team-a/analytics` can't stall ticks for `team-b/billing`. Each location's daemon sees only its own definitions.
- **Scales with code locations** — daemon capacity grows linearly as you add `CodeLocation` CRs; there's no shared "daemon node" to provision.
- **Parallel within a tick** — schedules and sensors in the same location evaluate concurrently inside one tick (async on the event loop, sync via a GIL-aware loky pool), rather than serialised one-by-one like the global Dagster daemon.

The run queue, tag concurrency limits, and pool slot tracking are global (they live in SurrealDB), so cross-location fairness is preserved. Only the *evaluation* — the part that runs user Python — is partitioned.

## Web UI

The UI is a Rust binary (Leptos SSR + WASM hydration over Axum), not a Python service serving a webpack bundle. State is read directly from SurrealDB and pushed to the browser via Server-Sent Events on `/api/events`. The UI never proxies asset compute — it talks to each code-location pod's gRPC server directly.

UI features that go beyond Dagit:

- **Live UI** — run state, asset materializations, and automation ticks stream straight to the browser over SSE with a `Live` / `Reconnecting` / `Stale` indicator; no manual refresh.

## Storage backend

rivers uses **SurrealDB v3** as its storage backend, with three modes:

- **In-memory** — `Storage.memory()`. Zero setup, ephemeral; ideal for tests and short-lived scripts.
- **Local / single-node** — `Storage.embedded(path)`. Embedded RocksDB, same binary, no external process.
- **HA / production** — `Storage.connect("ws://...")`. SurrealDB server backed by TiKV. Horizontal scale-out for reads and writes.

Dagster's storage layer is a Python ORM over relational databases (SQLite for dev, PostgreSQL/MySQL for prod). rivers' Rust storage layer is async and provides typed access to runs, events, asset records, dynamic partitions, ticks, pools, and KV state from one transactional store.

## API ergonomics: one class, one entry point

Every concept in rivers lives behind **one class with static-method factories** — so `rs.Executor.` in your IDE shows every executor, `rs.PartitionsDefinition.` shows every partition kind, and so on. There's one symbol to import per concept.

```python
# Assets — one decorator, every variant
@rs.Asset
@rs.Asset.from_multi(...)
@rs.Asset.from_graph(...)
rs.Asset.external(...)

# Executors
rs.Executor.in_process()
rs.Executor.parallel(max_workers=8)
rs.Executor.kubernetes(worker_image="...", max_concurrent_steps=20)

# Partition definitions
rs.PartitionsDefinition.static_(["us", "eu"])
rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))
rs.PartitionsDefinition.hourly(start=datetime(2024, 1, 1))
rs.PartitionsDefinition.time_window(start=..., cron_schedule="*/30 * * * * *")
rs.PartitionsDefinition.dynamic("customers")
rs.PartitionsDefinition.multi({...})

# Partition keys & ranges
rs.PartitionKey.single("2024-01-15")
rs.PartitionKey.multi({"date": "2024-01-15", "region": ["us", "eu"]})
rs.PartitionKeyRange.single("2024-01-01", "2024-01-31")
rs.PartitionKeyRange.multi({"date": ("2024-01-01", "2024-01-07"), "region": ["us", "eu"]})

# Partition mappings
rs.PartitionMapping.identity()
rs.PartitionMapping.time_window(offset=-1)
rs.PartitionMapping.for_keys([rs.PartitionKey.single("a")])
rs.PartitionMapping.subset()

# Backfill strategies
rs.BackfillStrategy.multi_run()
rs.BackfillStrategy.single_run()
rs.BackfillStrategy.per_dimension(multi_run=["date"], single_run=["region"])

# Storage
rs.Storage.memory()
rs.Storage.embedded(".rivers/storage")
rs.Storage.connect("ws://surrealdb:8000")
```

### Typed generic context

`AssetExecutionContext[ConfigT]` is parameterised by the config type itself — no separate `config=` argument on the decorator, no `RunConfig` dictionary. The type is read from the annotation, so IDE auto-complete on `context.config.<field>` Just Works:

```python
class ThresholdConfig(BaseModel):
    min_value: float = 0.0

@rs.Asset
def filtered(context: rs.AssetExecutionContext[ThresholdConfig]):
    return [x for x in range(100) if x >= context.config.min_value]
```

## Asset system features rivers adds

### `Materialization` return type

Terminal side-effecting assets — pushing to an API, writing directly to an external system — return a `Materialization` instead of an `Output(value)`:

```python
@rs.Asset
def push_to_api(rows: list[dict]) -> rs.Materialization:
    response = requests.post(API_URL, json=rows)
    return rs.Materialization(
        metadata={"status_code": rs.MetadataValue.int(response.status_code)},
        data_version=response.headers["ETag"],
    )
```

The framework records a `Materialization` event with metadata and data version but **never invokes the IO handler** — the discriminator lives at the return type, so every executor (in-process, parallel, Kubernetes) takes the same code path. In Dagster, opting out of IO managers is per-executor and requires plumbing.

### `SelfDependency` for incremental assets

Read an asset's own previous value through a typed parameter, without creating a graph cycle:

```python
@rs.Asset
def running_total(self: rs.SelfDependency[int], new_events: list) -> int:
    prev = self.get_inner()         # int | None on first run
    return (prev or 0) + len(new_events)
```

The dependency is satisfied through `io_handler.load_input()` — no graph edge, no DAG-cycle workaround. `T` is forwarded to the IO handler as the type hint.

### `mark_partition_failed` for `SingleRun` backfills

In a `SingleRun` backfill the step receives every partition key in `context.partition.keys`. rivers lets the function record per-key failures while preserving the rest as successes:

```python
@rs.Asset(backfill_strategy=rs.BackfillStrategy.single_run())
def daily_events(context: rs.AssetExecutionContext):
    for key in context.partition.keys:
        try:
            process_day(key)
        except Exception as exc:
            context.mark_partition_failed(key, str(exc))
```

Failures roll up into the same `BackfillStatus.failed_partitions` counter as multi-run failures.

### `PerDimension` backfill strategy

For multi-dimensional partitions, rivers exposes per-dimension control: dimensions in `multi_run` are iterated across runs, dimensions in `single_run` are batched within each run.

```python
strategy=rs.BackfillStrategy.per_dimension(
    multi_run=["date"],
    single_run=["region"],
)
```

This gives you "one run per date, all regions inside each run" without writing custom backfill orchestration.

### Multi-asset subsetting via `output_selection`

Multi-assets are subsettable without redefining the asset. Branch on `context.output_selection` to skip work for outputs that aren't being persisted:

```python
@rs.Asset.from_multi(output_defs=[rs.AssetDef("users"), rs.AssetDef("orders")])
def extract(context: rs.AssetExecutionContext):
    if "users" in context.output_selection:
        yield rs.Output(value=load_users(), output_name="users")
    if "orders" in context.output_selection:
        yield rs.Output(value=load_orders(), output_name="orders")
```

### Mixed partition dimensions

A `Multi` partition definition can combine any partition kinds in a single dict — static, time-window, *and* dynamic dimensions side by side. There is no upper limit on dimension count; the only restriction is no nesting.

```python
rs.PartitionsDefinition.multi({
    "date": rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1)),
    "region": rs.PartitionsDefinition.static_(["us", "eu"]),
    "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
    "tenant": rs.PartitionsDefinition.dynamic("tenants"),
})
```

### Sub-minute cron and time-window partitions

Cron expressions accept **5 fields or 6 fields with leading seconds**, so partitions and schedules can tick at sub-minute cadence:

```python
rs.PartitionsDefinition.time_window(
    start=datetime(2024, 1, 1),
    cron_schedule="*/30 * * * * *",   # every 30 seconds
)
```

### Dynamic fan-out with `.map()` and `.collect_stream()`

Inside a graph asset, the output of a task or upstream asset is a handle with `.map(task)` for fan-out and `.collect()` / `.collect_stream()` for fan-in. Streaming collect means downstream tasks can start consuming as soon as the first mapped result lands — no barrier required.

```python
@rs.Task
def double(x: int) -> int:
    return x * 2

@rs.Asset
def numbers() -> list:
    return [1, 2, 3, 4, 5]

@rs.Asset.from_graph()
def doubled():
    mapped = numbers().map(double, max_concurrency=4)
    return sum_all(mapped.collect_stream())
```

Dagster's `DynamicOutput` is static-list fan-out with a barrier collect; rivers supports streaming producers, streaming collects, and async map bodies on the same surface.

### Native `async def` assets and tasks

`@rs.Asset` and `@rs.Task` accept `async def` directly. The runtime detects the coroutine at decoration time and awaits it on the tokio event loop — no `asyncio.run()` wrapper, no separate `async_*` decorator. `Executor.parallel()` runs sync steps in a subprocess pool *and* async steps concurrently as event-loop tasks, with `max_async_concurrent=N` to cap the async fan-out separately from `max_workers`.

```python
@rs.Asset
async def prices():
    return await client.fetch("/prices")

@rs.Asset
async def inventory():
    return await client.fetch("/inventory")

@rs.Asset
def report(prices, inventory):
    return {**prices, **inventory}
```

Async also works inside `@rs.Asset.from_graph()` task bodies and as `.map()` templates.

### `AssetDef.input` vs `AssetDef.dep`

rivers splits "upstream that produces data I want to load" from "upstream that just creates an ordering edge":

```python
deps=[
    rs.AssetDef.input("source", partition_mapping=...),   # loaded via io_handler, name-matched to a param
    rs.AssetDef.dep("trigger"),                            # lineage-only edge, never loaded
]
```

`AssetDef.input(...)` also takes per-dependency `io_handler=` and `metadata=` overrides — you can give *one* upstream a different load path without touching the others. Dagster collapses these into a single `deps=[AssetKey(...)]` list with no per-dependency knobs.

### Native `@BashTask`

```python
@rs.BashTask(command=["python", "-m", "my.script", "--day", "2024-01-15"], env={...})
def run_script(): ...
```

A first-class shell-command task — pickleable, transportable across `parallel` and `kubernetes` executors, with `env` and `cwd` kwargs. Dagster requires the `dagster-shell` package and the `execute_shell_command` op pattern.

### Per-asset executor override

A single asset can override the run's executor through the `rivers/executor` metadata key:

```python
@rs.Asset(metadata={"rivers/executor": "in_process"})
def needs_in_process() -> int:
    return 42
```

This works for both `Job.execute()` and `CodeRepository.materialize()`. For graph assets, `rivers/node/executor` overrides individual internal tasks.

### Pydantic-everywhere config

Config classes are plain `pydantic.BaseModel` (static) or `pydantic_settings.BaseSettings` (env-aware). No `Config` schema DSL, no Dagster-specific `RunConfig` dictionary, no separate validation step:

```python
class ThresholdConfig(BaseModel):
    min_value: float = 0.0
    max_value: float = 1.0

@rs.Asset
def filtered(context: rs.AssetExecutionContext[ThresholdConfig]):
    return [x for x in range(100) if x >= context.config.min_value]
```

The config type is inferred from the `context` annotation — no separate `config=` argument on the decorator.

## Concurrency model

rivers exposes **four composable concurrency layers** — the same four that Dagster offers (run queue, tag concurrency, executor step parallelism, and pools / "concurrency keys"). The differences are in the implementation:

| Layer | Scope | What it gates |
|-------|-------|---------------|
| Run queue | All runs in flight | `max_concurrent_runs` |
| Tag concurrency limits | Runs sharing a tag | per-value or per-distinct-value caps |
| Executor step parallelism | Steps inside one run | `max_workers` / `max_concurrent_steps` |
| Concurrency pools | Steps across all runs | Per-asset slot claims against a shared resource |

Where rivers differs from Dagster on pools specifically:

- **Multiple pools per asset** — pass a list to `pool=`; the step waits for *all* its pools to have space before running.
- **Lease-based slot tracking** — if a step crashes (pod OOM, node failure), slot leases are reclaimed by GC after `lease_duration`. A dead worker can't permanently hold capacity.
- **Works uniformly across `in_process`, `parallel`, and `kubernetes` executors** — gating is enforced by the Rust executor core, not per-executor plumbing.
- **Runtime CLI + storage introspection** — `rivers pools list/info/set`, plus `storage.get_all_pool_infos()` and `storage.get_pool_slot_holders(...)`.

Pools are declared on the asset and the slot limit is set on the repository:

```python
@rs.Asset(pool="warehouse", pool_slots=2)
def heavy_query(...): ...

repo = rs.CodeRepository(
    assets=[heavy_query, ...],
    pool_limits={"warehouse": 4},
)
```

## Graph asset features

Graph assets compose `Task` operations into a sub-DAG that is treated as a single asset. Internal tasks are independent execution-plan steps and can:

- Use a **different IO handler** than the graph's terminal output (`node_io_handler` for cheap in-memory hops, persistent IO only for the terminal node).
- Use a **different executor** per task (`rivers/node/executor` metadata).
- **Resolve parameters by name** from the outer dependency graph — a task inside a graph asset can take a parameter named after another asset and rivers wires it in.

## Run-launch path

| Trigger | Goes through |
|---------|--------------|
| `repo.materialize()`, `repo.backfill()`, `Job.execute()` | Run **inline**, bypass the queue (synchronous calls) |
| Schedules, sensors, automation conditions, UI-triggered runs | Run queue + tag limits + executor gates + pools |

The Python pymethods intentionally skip the queue — they are the "I want to run this now" interface. Daemon-launched runs flow through the full gate stack.

## Run lifecycle controls

- **Sensors can yield `BackfillRequest`** — one `SensorResult` can carry `run_requests=[...]` (mixed `RunRequest` / `BackfillRequest`), a `cursor`, and a `skip_reason` from the same tick.

## Observability

- **OTLP tracing built-in** — set `OTEL_EXPORTER_OTLP_ENDPOINT` and rivers wires an OpenTelemetry span exporter to the Rust `tracing` subscriber. No `dagster-opentelemetry` extension.
- **Python `logging` bridge** — `logging.getLogger("rivers")` and `context.log` flow through the same `tracing` subscriber via `pyo3-pylogger`, so per-step log capture works without user setup.
