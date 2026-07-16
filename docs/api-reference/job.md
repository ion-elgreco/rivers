# Job

## `Job`

A named bundle of assets, multi-assets, graph assets, tasks, and bash tasks executed together as one run. Must be added to a `CodeRepository` for validation and execution.

```python
import rivers as rs

repo = rs.CodeRepository(
    assets=[asset_a, asset_b],
    jobs=[
        rs.Job(name="pipeline", assets=[asset_a, asset_b], executor=rs.Executor.in_process()),
    ],
)
repo.get_job("pipeline").execute()
```

**Constructor:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Unique name for the job within the repository. |
| `assets` | `Sequence[SingleAsset \| MultiAsset \| GraphAsset \| Task \| BashTask]` | required | Nodes the job will materialize. |
| `executor` | `Executor \| None` | `None` | Override the repository default executor for this job. |
| `allow_incomplete_deps` | `bool` | `False` | Tolerate missing upstream deps (debug / partial graphs). Production jobs should leave this `False`. |
| `retry` | `RetryPolicy \| str \| None` | `None` | Job-level retry default — a [`RetryPolicy`](retries.md) or a `retries` registry name. Assets with their own policy keep it. |

**Methods:**

### `execute()`

```python
def execute(
    self,
    partition_key: PartitionKey | None = None,
    tags: list[tuple[str, str]] | None = None,
    config: dict[str, dict[str, Any]] | None = None,
    raise_on_error: bool = True,
) -> RunResult
```

Run the job synchronously, optionally targeting a single partition. Returns a [`RunResult`](repository.md#runresult). Read materialized values back via `repo.load_node(name)`.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `partition_key` | `PartitionKey \| None` | `None` | Partition to materialize. Required for partitioned assets. |
| `tags` | `list[tuple[str, str]] \| None` | `None` | Run tags applied for queue / observability filtering. `rivers/priority` is honored for run-queue priority. |
| `config` | `dict[str, dict[str, Any]] \| None` | `None` | Per-asset config, keyed by asset name. |
| `raise_on_error` | `bool` | `True` | Raise on first failure instead of returning a failed result. |

**Raises:** `ValueError` if the job has not been added to a `CodeRepository`.

---

## Per-asset executor override

Override the executor for individual assets via the `rivers/executor` metadata key:

```python
@rs.Asset(metadata={"rivers/executor": "in_process"})
def needs_context(context: rs.AssetExecutionContext) -> int:
    return context.asset_name
```

| Value | Executor |
|-------|----------|
| `"in_process"` | `Executor.in_process()` |
| `"parallel"` | `Executor.parallel()` (default subprocess pool size) |

When overrides are present, the executor groups independent steps by level and partitions each level by executor. Steps sharing the same executor within a level still run in parallel (for `parallel`); steps with different executors in the same level run as separate batches.

For graph asset internal tasks, use `rivers/node/executor` (it falls back to `rivers/executor`, then to the default).
