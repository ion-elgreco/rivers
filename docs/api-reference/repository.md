# CodeRepository

## `CodeRepository`

Central registry of assets, tasks, jobs, schedules, and sensors. Resolves the full dependency graph and validates each user-defined `Job`. Calls to `materialize()` build an ephemeral execution plan over the selection — there is no auto-generated job, and ad-hoc runs have `RunRecord.job_name = None`.

```python
import rivers as rs

repo = rs.CodeRepository(
    assets=[asset_a, asset_b, asset_c],
    tasks=[my_task],
    jobs=[
        rs.Job(name="pipeline", assets=[asset_a, asset_b]),
    ],
    schedules=[nightly_schedule],
    sensors=[file_sensor],
)
```

**Constructor:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `assets` | `Sequence[SingleAsset \| MultiAsset \| GraphAsset \| ExternalAsset]` | required | All assets to register. |
| `tasks` | `Sequence[Task \| BashTask] \| None` | `None` | Standalone tasks (not asset producers) to register. |
| `jobs` | `Sequence[Job] \| None` | `None` | Jobs to validate and register. |
| `schedules` | `Sequence[Schedule] \| None` | `None` | Schedules to register. |
| `sensors` | `Sequence[Sensor] \| None` | `None` | Sensors to register. |
| `default_executor` | `Executor \| None` | `None` | Default executor for jobs without one. Defaults to `Executor.parallel()` when not set. |
| `resources` | `dict[str, Any] \| None` | `None` | Shared resources injected by parameter name into asset/task/schedule/sensor functions. |
| `run_queue` | `RunQueueConfig \| None` | `None` | Run-queue limits applied to daemon-submitted runs. See [Concurrency](concurrency.md). |
| `run_backend` | `RunBackendConfig \| None` | `None` | Where runs are launched — local subprocess (default) or Kubernetes pods. |
| `pool_limits` | `dict[str, int] \| None` | `None` | Initial concurrency-pool slot caps (`{pool_key: limit}`). |

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `assets` | `dict[str, SingleAsset \| MultiAsset \| GraphAsset]` | Dict mapping asset names to asset objects. |
| `storage` | `Storage` | The storage backend bound by `resolve()`. |
| `schedules` | `list[Schedule]` | Registered schedules. |
| `sensors` | `list[Sensor]` | Registered sensors. |

---

## Methods

### `resolve()`

```python
def resolve(self, storage: Storage | None = None) -> None
```

Validate the graph and persist topology / metadata into `storage`. Must be called before `materialize()`, `backfill()`, or daemon start. Without an explicit `storage`, an embedded SurrealDB+RocksDB backend is created at `.rivers/storage/`.

### `validate()`

```python
def validate(self) -> None
```

Run the storage-independent validation pipeline: graph composition, partition / external / resource-reference checks, and per-job plan building. Raises the same errors `resolve()` would for graph problems, but **does not** initialize storage, invoke `Resource.setup()`, resolve IO handler `ResourceRef`s, register assets/pools, or persist topology.

Intended for CLI / IDE / UI tools that want fast feedback on whether a repository is well-formed without the side effects of a full `resolve()`. Always re-runs (no idempotency guard) so it can be called repeatedly while the user edits code.

### `get_job()`

```python
def get_job(self, name: str) -> Job
```

Retrieve a validated job by name. Raises `ValueError` if no job with that name exists.

### `materialize()`

```python
def materialize(
    self,
    selection: list[str] | None = None,
    partition_key: PartitionKey | None = None,
    tags: list[tuple[str, str]] | None = None,
    raise_on_error: bool = True,
    config: dict[str, dict[str, Any]] | None = None,
    run_id_override: str | None = None,
    include_upstream: bool = False,
    resume: bool = False,
) -> RunResult
```

Materialize assets synchronously over an ephemeral execution plan. When `selection` is provided, only the selected assets are materialized; pass `include_upstream=True` to also materialize their transitive deps. The resulting `RunRecord.job_name` is `None`.

When a `RunQueueConfig` is configured, the run queue applies to daemon-submitted runs (sensors, schedules, automation conditions, UI). `materialize()` always executes directly regardless of queue configuration.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `selection` | `list[str] \| None` | `None` | Asset names to materialize. `None` for all. |
| `partition_key` | `PartitionKey \| None` | `None` | Partition to materialize. Required for partitioned assets. |
| `tags` | `list[tuple[str, str]] \| None` | `None` | Tags applied to the run for queue / observability filtering. |
| `raise_on_error` | `bool` | `True` | Raise on first failure rather than returning a failed result. |
| `config` | `dict[str, dict[str, Any]] \| None` | `None` | Per-asset config overrides keyed by asset name. |
| `run_id_override` | `str \| None` | `None` | Use a pre-assigned run ID (used by K8s execution pods). |
| `include_upstream` | `bool` | `False` | Also materialize transitive deps of `selection`. |
| `resume` | `bool` | `False` | Skip already-completed steps from a crashed prior run with the same `run_id_override`. |

**Returns:** [`RunResult`](#runresult).

### `backfill()`

```python
def backfill(
    self,
    selection: list[str] | None = None,
    partition_keys: list[PartitionKey] | None = None,
    partition_range: PartitionKeyRange | None = None,
    strategy: BackfillStrategy | None = None,
    failure_policy: str = "continue",
    max_concurrency: int = 4,
    tags: list[tuple[str, str]] | None = None,
    config: dict[str, dict[str, Any]] | None = None,
    block: bool = True,
    dry_run: bool = False,
) -> BackfillResult
```

Backfill partitions for the selected assets. See [Backfills](backfills.md) for the full reference.

### `cancel_backfill()`

```python
def cancel_backfill(self, backfill_id: str) -> bool
```

Cancel a running backfill. Returns `True` if the in-process coordinator was signalled (the backfill has live state in this process); returns `False` and falls back to a storage-level cancel marker otherwise.

### `get_backfill()`

```python
def get_backfill(self, backfill_id: str) -> BackfillStatus | None
```

Look up a backfill's current status by ID, or `None` if not found.

### `rerun_backfill()`

```python
def rerun_backfill(
    self, backfill_id: str, block: bool = True, dry_run: bool = False
) -> BackfillResult
```

Re-launch the failed and canceled partitions of a previous backfill.

### `observe()`

```python
def observe(self, asset_names: list[str] | None = None) -> dict[str, Any]
```

Run observation functions on external assets. Only external assets with an `observe_fn` (set via the `@Asset.external()` decorator) are observed.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_names` | `list[str] \| None` | `None` | Filter to specific asset names. Observes all observable external assets when `None`. |

**Returns:** `dict[str, dict[str, MetadataValue]]` mapping asset names to their observation metadata.

```python
@rs.Asset.external(io_handler=handler)
def source(context: rs.AssetExecutionContext):
    context.add_output_metadata({"row_count": rs.MetadataValue.int(1000)})

repo = rs.CodeRepository(assets=[source])
result = repo.observe()
print(result["source"]["row_count"].raw_value())  # 1000
```

### `load_node()`

```python
def load_node(
    self,
    name: str,
    *,
    partition_key: PartitionKey | None = None,
    type_hint: type[T] | None = None,
) -> T | Any
```

Load a node's persisted output via its IO handler. Returns the value stored by the most recent materialization.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Node name (e.g. `"my_asset"` or `"pipeline/step_a"` for graph-asset internal tasks). |
| `partition_key` | `PartitionKey \| None` | `None` | Partition to load. Required for partitioned assets. |
| `type_hint` | `type \| None` | `None` | Target type passed to the IO handler (e.g. `pyarrow.Table` for Delta). |

```python
repo.materialize()
value = repo.load_node("my_asset")
df = repo.load_node("delta_asset", type_hint=pa.Table)  # typed as pa.Table
```

### `io_handler_for_output()`

```python
def io_handler_for_output(self, name: str) -> BaseIOHandler
```

Resolve the IO handler this repository would use to write `name`'s output. Walks the registry chain `node.io_handler() → default` — the same chain the executor uses at materialize time. Returns the resolved handler instance (the configured one, an instance derived from a `ResourceRef`, or the shared default `InMemoryIOHandler`).

Useful for debugging "which handler does this asset actually use?" without running execution.

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `str` | Asset name. |

**Raises:** `NodeNotFoundError` if `name` is not in the resolved repository.

### `get_schedule()` / `get_sensor()`

```python
def get_schedule(self, name: str) -> Schedule
def get_sensor(self, name: str) -> Sensor
```

Look up a registered schedule or sensor by name.

### `evaluate_schedule()`

```python
def evaluate_schedule(
    self, name: str, execution_time: str | None = None
) -> ScheduleTickResult
```

Run a schedule's evaluation function once and return the tick result.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Schedule name. |
| `execution_time` | `str \| None` | now | Override the scheduled execution timestamp (RFC 3339). |

### `evaluate_sensor()`

```python
def evaluate_sensor(
    self, name: str, cursor: str | None = None, last_tick_time: float | None = None
) -> SensorTickResult
```

Run a sensor's evaluation function once and return the tick result.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Sensor name. |
| `cursor` | `str \| None` | `None` | Override the cursor passed to the sensor. |
| `last_tick_time` | `float \| None` | `None` | Override the last-tick timestamp (seconds since epoch). |

### `shutdown()`

```python
def shutdown(self) -> None
```

Tear down resources, daemons, and gRPC/UI servers started by this repository. Calls `teardown()` on every registered resource.

### Context manager

`CodeRepository` supports the context manager protocol; `__exit__` calls `shutdown()`:

```python
with rs.CodeRepository(assets=[a, b]) as repo:
    repo.materialize()
# storage and resources are cleaned up automatically
```

---

## `RunResult`

Outcome of a synchronous `materialize()` call.

| Property | Type | Description |
|----------|------|-------------|
| `success` | `bool` | `True` when every requested asset materialized. |
| `run_id` | `str` | ID of the underlying run record. |
| `materialized_assets` | `list[str]` | Asset keys attempted in this run. |
| `failed_assets` | `list[tuple[str, str]]` | `(asset_key, error_message)` pairs for steps that failed. |

To read output values, use `repo.load_node(name)`.

---

## `RunHandle`

Async handle to a submitted run. Returned by internal submission paths (daemon, gRPC/UI) when a `RunQueueConfig` is configured. Can be used to poll status, wait for completion, or cancel queued runs.

| Property | Type | Description |
|----------|------|-------------|
| `run_id` | `str` | ID of the underlying run record. |
| `status` | `str` | Current run status (`"Queued"`, `"NotStarted"`, `"Running"`, `"Success"`, `"Failure"`, `"Canceled"`). |

### `wait()`

```python
def wait(self, timeout: float | None = None) -> RunResult
```

Block until the run reaches a terminal state (or `timeout` seconds elapse). Raises on timeout.

### `cancel()`

```python
def cancel(self) -> None
```

Request cancellation of the run. No-op if already in a terminal state.

---

## Ad-hoc materialization

`repo.materialize()` runs over all non-external assets without requiring a `Job`. Each call builds an ephemeral execution plan over the selection — the resulting `RunRecord` has `job_name = None`. Use a named `Job` when you want to surface the run target in the UI, share it across schedules, or pin executor/partition behavior.

```python
repo = rs.CodeRepository(assets=[a, b, c])
result = repo.materialize()  # all non-external assets, ad-hoc
result = repo.materialize(selection=["b"], include_upstream=True)
```
