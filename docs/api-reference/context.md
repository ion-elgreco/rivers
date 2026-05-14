# Execution Contexts

## `AssetExecutionContext`

Execution context injected into asset functions. Provides access to asset properties, partitioning info, logging, and output metadata.

### Usage

Add `context: AssetExecutionContext` as the **first parameter** of your asset function:

```python
from rivers import Asset, AssetExecutionContext

@Asset(tags=["analytics"], kinds="table", group="reports")
def my_asset(context: AssetExecutionContext):
    context.log.info(f"Running {context.asset_name}")
    context.add_output_metadata({"rows": 1000})
    return compute_data()

@Asset
def downstream(context: AssetExecutionContext, my_asset: list):
    context.log.info(f"Processing {len(my_asset)} items")
    return transform(my_asset)
```

Context is optional — assets without it continue to work as before.

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `asset_name` | `str` | Name of the current asset. |
| `tags` | `list[str] \| None` | Tags from the asset definition. |
| `kinds` | `list[str]` | Kinds/types from the asset definition. |
| `group` | `str \| None` | Group from the asset definition. |
| `code_version` | `str \| None` | Code version from the asset definition. |
| `asset_metadata` | `dict[str, str] \| None` | Metadata from the asset definition. |
| `is_multi_asset` | `bool` | True if this asset is a multi-asset. |
| `output_selection` | `list[str]` | Output names being materialized (multi-asset only). |
| `partition` | `PartitionContext \| None` | Partition context (if partitioned). |
| `has_partition_key` | `bool` | Whether a partition key is available. |
| `partition_key` | `str` | Single partition key string (raises `ValueError` if not partitioned). |
| `partition_time_window` | `tuple[datetime, datetime] \| None` | Time window for time-partitioned assets. |
| `config` | `ConfigT` | Config instance (if the asset function uses a config type hint). |
| `log` | `logging.Logger` | Logger named `rivers.assets.<asset_name>`. |

### Methods

#### `add_output_metadata(metadata)`

Add metadata that will flow to the IO handler's `OutputContext`:

```python
@Asset(io_handler=MyHandler())
def my_asset(context: AssetExecutionContext):
    result = compute()
    context.add_output_metadata({
        "rows": len(result),
        "status": "success",
    })
    return result
```

Values are auto-coerced to `MetadataValue` (str, int, float, bool, None supported); pass an explicit `MetadataValue` for typed variants.

#### `register_data_version(version)`

Register a custom data version for this materialization, overriding the auto-generated UUID:

```python
@Asset
def my_asset(context: AssetExecutionContext):
    data = fetch_data()
    context.register_data_version(compute_hash(data))
    return data
```

#### `mark_partition_failed(partition_key, error)`

For multi-partition steps (single-run backfills, multi-key materializations), record that one specific partition inside the step failed without aborting the rest:

```python
for key in keys:
    try:
        process(key)
    except Exception as exc:
        context.mark_partition_failed(rs.PartitionKey.single(key), str(exc))
```

### Property: `output_metadata`

Returns accumulated metadata as `dict[str, MetadataValue]`, or `None` if empty.

---

## `TaskExecutionContext`

A lightweight execution context for tasks. Contains only task-relevant fields — no asset-specific fields like `kinds`, `group`, `code_version`, `asset_metadata`, or output metadata.

### Usage

Add `context: TaskExecutionContext` as the **first parameter** of your task function:

```python
from rivers import Task, TaskExecutionContext

@Task(tags=["etl"])
def my_task(context: TaskExecutionContext, source: int) -> str:
    context.log.info(f"Running task {context.task_name}")
    return f"processed: {source}"
```

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `task_name` | `str` | Name of the current task. |
| `tags` | `list[str] \| None` | Tags from the task definition. |
| `partition` | `PartitionContext \| None` | Partition context (if partitioned). |
| `has_partition_key` | `bool` | Whether a partition key is available. |
| `partition_key` | `str` | Single partition key string (raises `ValueError` if not partitioned). |
| `partition_time_window` | `tuple[datetime, datetime] \| None` | Time window for time-partitioned tasks. |
| `config` | `ConfigT` | Config instance (if the task function uses a config type hint). |
| `log` | `logging.Logger` | Logger named `rivers.tasks.<task_name>`. |

```python
parts = rs.PartitionsDefinition.static_(["a", "b"])

@rs.Task(partitions_def=parts)
def partitioned_task(context: rs.TaskExecutionContext) -> str:
    return f"key={context.partition_key}"
```

---

## Detection rules

Both context types follow the same injection rules:

1. If the first parameter's type annotation is `AssetExecutionContext` or `TaskExecutionContext`, the appropriate context is injected
2. If the first parameter is named `context` and no asset/task with that name exists, `AssetExecutionContext` is injected
3. Context as a non-first parameter raises `ExecutionError` (`"Context must be the first parameter of '<step>'"`)

Tasks can use either `TaskExecutionContext` (recommended) or `AssetExecutionContext` (backward compatible).

## Executor support

| Executor | Support |
|----------|---------|
| `Executor.in_process()` | Full support |
| `Executor.parallel()` | Full support (context is serialized to the subprocess) |
| `Executor.kubernetes()` | Full support (context is reconstructed in the step pod) |
