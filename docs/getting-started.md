# Getting Started

## Installation

```bash
pip install rivers
```

Optional extras:

```bash
pip install rivers[delta]     # Delta Lake IO handler
pip install rivers[pyarrow]   # PyArrow Tables / RecordBatchReaders for Delta
pip install rivers[polars]    # Polars DataFrames / LazyFrames for Delta
pip install rivers[otel]      # OpenTelemetry instrumentation
```

## Your first asset

An asset is a function that produces a data artifact. Decorate it with `@Asset`:

```python
import rivers as rs

@rs.Asset
def users():
    return [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]
```

## Adding dependencies

Assets declare dependencies through their function parameters. rivers resolves the DAG automatically — the parameter name must match an upstream asset's name:

```python
@rs.Asset
def active_users(users: list):
    return [u for u in users if u["name"] != "Bob"]

@rs.Asset
def user_count(active_users: list):
    return len(active_users)
```

## Running assets

A `CodeRepository` registers your assets, resolves the dependency graph, and lets you materialize everything:

```python
repo = rs.CodeRepository(assets=[users, active_users, user_count])
result = repo.materialize()
assert result.success
print(repo.load_node("user_count"))  # 1
```

`materialize()` returns a [`RunResult`](api-reference/repository.md#runresult) — `result.success`, `result.run_id`, `result.materialized_assets`, `result.failed_assets`. Asset values are read back through `repo.load_node(name)`, which goes through the asset's IO handler.

For named subsets, use a `Job`:

```python
job = rs.Job(
    name="user_pipeline",
    assets=[users, active_users, user_count],
    executor=rs.Executor.in_process(),
)
repo = rs.CodeRepository(assets=[users, active_users, user_count], jobs=[job])
result = repo.get_job("user_pipeline").execute()  # returns RunResult; read values via load_node
```

## Adding an IO handler

By default, asset outputs are passed through a shared in-memory handler. To persist outputs across runs, attach an IO handler:

```python
from obstore.store import LocalStore

io = rs.PickleIOHandler(store=LocalStore(prefix="/tmp/rivers"))

@rs.Asset(io_handler=io)
def users():
    return [{"id": 1, "name": "Alice"}]
```

The `PickleIOHandler` works with any `obstore`-compatible backend (local filesystem, S3, GCS, Azure). For typed Arrow data, use the [Delta Lake handler](api-reference/delta.md).

## Parallel execution

By default, `CodeRepository` uses `Executor.parallel()` (subprocess pool via loky) to run independent assets concurrently. Configure it explicitly:

```python
repo = rs.CodeRepository(
    assets=[users, active_users, user_count],
    default_executor=rs.Executor.parallel(max_workers=4),
)
```

For sequential execution (best for debugging), use `Executor.in_process()`:

```python
repo = rs.CodeRepository(
    assets=[users, active_users, user_count],
    default_executor=rs.Executor.in_process(),
)
```

For Kubernetes step pods, use `Executor.kubernetes()` — see the [Executors API](api-reference/executors.md).

## Cleanup with the context manager

`CodeRepository` is a context manager. Exiting the `with` block calls `shutdown()`, which closes storage and runs `teardown()` on every registered resource:

```python
with rs.CodeRepository(assets=[users, active_users, user_count]) as repo:
    repo.materialize()
```

## Next steps

- [Assets](concepts/assets.md) — single, multi, graph, and external assets
- [Jobs & Executors](concepts/jobs.md) — bundle assets into jobs and pick an execution strategy
- [Partitions](concepts/partitions.md) — partition assets by time, category, or dimension product
- [IO Handlers](concepts/io-handlers.md) — persist data with Delta Lake, pickle, or custom handlers
- [Schedules & Sensors](api-reference/schedules.md) — drive runs from cron and external events
