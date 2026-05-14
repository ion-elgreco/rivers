# Jobs & Executors

## Jobs

A `Job` defines a named subset of assets and tasks to execute together. Jobs must be added to a `CodeRepository`, which resolves the dependency graph and validates the job's contents before execution.

```python
import rivers as rs

@rs.Asset
def asset_a() -> int:
    return 1

@rs.Asset
def asset_b(asset_a: int) -> int:
    return asset_a + 1

repo = rs.CodeRepository(
    assets=[asset_a, asset_b],
    jobs=[
        rs.Job(name="my_pipeline", assets=[asset_a, asset_b], executor=rs.Executor.in_process()),
    ],
)
repo.get_job("my_pipeline").execute()
```

`Job.execute()` returns a [`RunResult`](../api-reference/repository.md#runresult) — check `result.success`, `result.run_id`, `result.materialized_assets`, `result.failed_assets`. Read materialized values back via `repo.load_node("asset_b")` (which goes through the asset's IO handler).

### Subgraph validation

When a `CodeRepository` is constructed, every job is validated:

- **All assets must exist** in the repository.
- **No broken chains**: every upstream dependency of a job's assets must also be in the job (or marked as incomplete — see below).
- **Independent assets are fine**: assets with no dependency relationship can coexist in the same job.

```python
# Valid — a and c are independent
rs.Job(name="ok", assets=[a, c])

# Invalid — b depends on a, but a is not in the job
rs.Job(name="broken", assets=[b, c])
```

### Incomplete dependencies

Set `allow_incomplete_deps=True` to allow missing upstream dependencies, provided they have an `io_handler` that can load data externally:

```python
rs.Job(name="partial", assets=[b], allow_incomplete_deps=True)
```

Use this when upstream assets are materialized separately and their data can be loaded via IO handlers.

### Partitioned execution

Pass a `PartitionKey` to materialize a specific partition:

```python
repo.get_job("my_pipeline").execute(
    partition_key=rs.PartitionKey.single("2024-01-15"),
)
```

## Executors

An `Executor` picks how a job's steps are dispatched. Construct one through the static factories on `Executor`:

### InProcess

Runs every step serially in the calling Python process. Best for debugging and small pipelines:

```python
executor = rs.Executor.in_process()
```

### Parallel (subprocess pool)

Runs sync steps concurrently in subprocesses (loky pool) and async steps concurrently as tasks:

```python
executor = rs.Executor.parallel(max_workers=4)
```

| Argument | Default | Notes |
|----------|---------|-------|
| `max_workers` | `os.cpu_count()` | Subprocess pool size for sync steps. |
| `max_async_concurrent` | unbounded | Cap on concurrent async tasks. |

!!! note
    `Executor.parallel()` does **not** cloudpickle your asset bodies. Functions and IO handlers are sent to workers as `(module, qualname)` refs and re-imported in the subprocess — they need to be importable on the worker's Python path, not pickle-friendly. Upstream inputs are loaded inside the worker via the IO handler (so input values never cross the boundary), outputs are written from the worker, and `Resource` instances ride across as JSON via Pydantic. Closures and locally-defined functions (`<locals>`) fall back to direct pickling — keep production assets at module scope.

### Kubernetes

Each step runs as its own Kubernetes worker pod:

```python
executor = rs.Executor.kubernetes(
    worker_image="my-registry/rivers:1.2.3",
    max_concurrent_steps=20,
    namespace="rivers",
    service_account="rivers-executor",
    worker_cpu="500m",
    worker_memory="512Mi",
)
```

`worker_image` defaults to the controlling pod's image when omitted. See the [Executors API reference](../api-reference/executors.md) for every parameter.

### Default executor

When no `default_executor` is specified on `CodeRepository`, the default is `Executor.parallel()`. Jobs without an explicit executor inherit this default.

### Per-asset executor override

Individual assets can override the executor via the `rivers/executor` metadata key. This works in both `Job.execute()` and `CodeRepository.materialize()`:

```python
@rs.Asset(metadata={"rivers/executor": "in_process"})
def needs_in_process() -> int:
    return 42
```

Valid values: `"in_process"`, `"parallel"`. See [Executors API](../api-reference/executors.md) for details.
