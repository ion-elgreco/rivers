# Executors

## `Executor`

Base class for execution strategies. Construct via the static factories and pass to `Job(executor=...)` or `CodeRepository(default_executor=...)`.

The variants — `Executor.InProcess`, `Executor.Parallel`, `Executor.Kubernetes` — are exposed as nested classes for `isinstance` checks.

### `Executor.in_process()`

Runs every step serially in the calling Python process.

```python
executor = rs.Executor.in_process()
```

No parameters.

### `Executor.parallel()`

Runs sync steps concurrently in subprocesses (loky pool) and async steps concurrently as async tasks.

```python
executor = rs.Executor.parallel(max_workers=4, max_async_concurrent=8)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_workers` | `int \| None` | `os.cpu_count()` | Worker subprocesses for sync steps. |
| `max_async_concurrent` | `int \| None` | unbounded | Cap on concurrent async tasks. |

!!! note
    `Executor.parallel()` does **not** cloudpickle your asset bodies. Functions and IO handlers are sent to workers as `(module, qualname)` refs and re-imported in the subprocess — they need to be importable on the worker's Python path, not pickle-friendly. Upstream inputs are loaded inside the worker via the IO handler (so input values never cross the boundary), outputs are written from the worker, and `Resource` instances ride across as JSON via Pydantic. Closures and locally-defined functions (`<locals>`) fall back to direct pickling — keep production assets at module scope.

### `Executor.kubernetes()`

Runs each step as its own Kubernetes worker pod. Designed for production deployments where each step needs its own image / resources.

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

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `worker_image` | `str \| None` | controlling-pod image | Container image for worker pods. |
| `max_concurrent_steps` | `int \| None` | unbounded | Cap on concurrent step pods. |
| `namespace` | `str \| None` | current namespace | Namespace pods are launched in. |
| `service_account` | `str` | `"rivers-executor"` | Service account bound to worker pods. |
| `worker_cpu` | `str` | `"500m"` | CPU request/limit for worker pods. |
| `worker_memory` | `str` | `"512Mi"` | Memory request/limit for worker pods. |

## Per-asset executor override

Individual assets can override the default executor via the `rivers/executor` metadata key. This works in both `Job.execute()` and `CodeRepository.materialize()`:

```python
@rs.Asset(metadata={"rivers/executor": "in_process"})
def needs_in_process(context: rs.AssetExecutionContext) -> int:
    return 42

repo = rs.CodeRepository(
    assets=[needs_in_process, other_asset],
    default_executor=rs.Executor.parallel(),
)
repo.materialize()  # needs_in_process runs in-process; other_asset via loky
```

Valid values: `"in_process"`, `"parallel"`.

When overrides are present, the executor groups independent steps by level and partitions each level by executor. Steps sharing the same executor within a level still run in parallel; steps with different executors in the same level run as separate batches.

For graph-asset internal tasks, use `rivers/node/executor` metadata on the inner `Task` (it falls back to `rivers/executor`, then to the default). See the [Graph Assets guide](../guides/graph-assets.md) for the full resolution hierarchy.
