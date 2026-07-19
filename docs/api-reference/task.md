# Task

## `Task`

A unit of computation used inside graph assets. Can be used as a bare decorator or with parameters.

```python
import rivers as rs

@rs.Task
def my_task(data):
    return transform(data)

@rs.Task(name="custom_name", tags=["slow"])
def another_task(data):
    return heavy_transform(data)
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str \| None` | `None` | Task name. Defaults to function name. |
| `tags` | `list[str] \| None` | `None` | Tags for categorization. |
| `partitions_def` | `PartitionsDefinition \| str \| None` | `None` | Partition definition for partitioned tasks, or the name of one registered in `CodeRepository(partition_defs={...})`. |
| `partition_mapping` | `dict[str, PartitionMapping] \| None` | `None` | Mapping from dependency name to partition mapping strategy. |
| `io_handler` | `BaseIOHandler \| str \| None` | `None` | IO handler for persisting the task's output. |
| `retry` | `RetryPolicy \| str \| None` | `None` | Retry policy for this task's step, or a `retries` registry name. See [Retries & Compute](retries.md). |

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `name` | `str \| None` | Task name (defaults to the wrapped function name). |
| `tags` | `list[str] \| None` | Tags propagated to runs that include this task. |
| `is_async` | `bool` | `True` when the wrapped function is a coroutine function. |

When called inside an `Asset.from_graph()` body, a `Task` records itself into the composition graph and returns an `InvokedNodeOutput` instead of executing.

**Partitioning:**

Tasks support the same partitioning features as assets:

```python
parts = rs.PartitionsDefinition.static_(["a", "b", "c"])

# Partitioned task — receives PartitionContext during execution
@rs.Task(partitions_def=parts)
def process(context: rs.AssetExecutionContext, source: str) -> str:
    return f"processed-{context.partition_key}"

# Unpartitioned task depending on partitioned asset via AllPartitions
@rs.Task(partition_mapping={"source": rs.PartitionMapping.all_partitions()})
def aggregate(source: int) -> int:
    return source + 1
```

---

## `BashTask`

A task that runs a shell command natively in Rust via `std::process::Command`. No Python subprocess overhead.

```python
import rivers as rs

# Shell command (interpreted by sh -c)
greet = rs.BashTask(name="greet", command="echo hello")

# Command as list (no shell interpretation)
build = rs.BashTask(name="build", command=["make", "build"])

# With environment and working directory
deploy = rs.BashTask(
    name="deploy",
    command="./deploy.sh",
    env={"STAGE": "prod"},
    cwd="/app",
)

result = greet()  # "hello"
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Task name. |
| `command` | `str \| list[str]` | required | Shell command string or argv list. |
| `env` | `dict[str, str] \| None` | `None` | Additional environment variables. |
| `cwd` | `str \| None` | `None` | Working directory. |
| `tags` | `list[str] \| None` | `None` | Tags for categorization. |
| `partition_mapping` | `dict[str, PartitionMapping] \| None` | `None` | Mapping from dependency name to partition mapping strategy. |
| `io_handler` | `BaseIOHandler \| str \| None` | `None` | IO handler for persisting the task's output. |
| `retry` | `RetryPolicy \| str \| None` | `None` | Retry policy for this task's step, or a `retries` registry name. See [Retries & Compute](retries.md). |

**Properties:**

| Property | Type |
|----------|------|
| `name` | `str` |
| `command` | `str \| list[str]` |
| `env` | `dict[str, str] \| None` |
| `cwd` | `str \| None` |
| `tags` | `list[str] \| None` |

**Behavior:**

- `command: str` runs through `sh -c` (shell interpretation, pipes, etc.)
- `command: list[str]` runs directly via exec (no shell)
- Returns stdout as a string (trailing newline stripped)
- Raises `OSError` on non-zero exit code with stderr in the message
- Works inside `Asset.from_graph()` composition context

---

## `TaskExecutionContext`

A lightweight execution context for tasks. Similar to `AssetExecutionContext` but without asset-specific fields (kinds, group, code_version, metadata, output metadata).

```python
import rivers as rs

@rs.Task(tags=["etl"])
def my_task(context: rs.TaskExecutionContext, source: int) -> str:
    context.log.info(f"Running task {context.task_name}")
    return f"processed: {source}"

# With partitions
parts = rs.PartitionsDefinition.static_(["a", "b"])

@rs.Task(partitions_def=parts)
def partitioned_task(context: rs.TaskExecutionContext) -> str:
    return f"key={context.partition_key}"
```

**Parameters (constructor):**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `task_name` | `str` | required | Name of the task. |
| `tags` | `list[str] \| None` | `None` | Tags for categorization. |
| `partition` | `PartitionContext \| None` | `None` | Partition context if task is partitioned. |

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `task_name` | `str` | Name of the task. |
| `tags` | `list[str] \| None` | Tags assigned to the task. |
| `partition` | `PartitionContext \| None` | Partition context. |
| `has_partition_key` | `bool` | Whether a partition key is available. |
| `partition_key` | `str` | The current partition key (raises if not partitioned). |
| `partition_time_window` | `tuple[datetime, datetime] \| None` | Time window for time-based partitions. |
| `config` | `ConfigT` | Config instance (if the task function uses a config type hint). |
| `log` | `logging.Logger` | Logger named `rivers.tasks.<task_name>`. |

---

## `InvokedNodeOutput`

Returned when calling a `Task` or `Asset` inside a graph asset body. Represents a dependency edge in the composition graph.

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `node_name` | `str` | Name of the invoked task or asset. |
| `output_name` | `str` | Name of the output (typically `"result"`). |

### `map()`

```python
def map(self, task: Task, *, max_concurrency: int | None = None) -> MappedOutput
```

Fan `task` out across each value of this output. The producer must yield an iterable (or a list of `DynamicOutput`); `task` is invoked once per element.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `task` | `Task` | required | Task to invoke once per element. |
| `max_concurrency` | `int \| None` | unbounded | Cap on simultaneously fanned-out instances. |

```python
@rs.Task
def chunk_ids() -> list[int]:
    return [1, 2, 3, 4]

@rs.Task
def process_id(chunk_id: int) -> int:
    return chunk_id * 10

@rs.Asset.from_graph()
def fanout():
    ids = chunk_ids()
    mapped = ids.map(process_id, max_concurrency=2)
    return mapped.collect()
```

---

## `MappedOutput`

Handle returned by `InvokedNodeOutput.map()`. Use `.collect()` to wait for every mapped instance, or `.collect_stream()` to consume them as they finish.

| Property | Type | Description |
|----------|------|-------------|
| `node_name` | `str` | Producer node whose output is being mapped. |
| `output_name` | `str` | Output identifier on the producer node. |

### `collect()`

```python
def collect(self) -> InvokedNodeOutput
```

Wait for all mapped instances to finish; return a single aggregated output for downstream wiring.

### `collect_stream()`

```python
def collect_stream(self, *, ordered: bool = False) -> InvokedNodeOutput
```

Stream mapped instance results as they complete. With `ordered=True`, emit results in their original mapping-key order; otherwise emit in completion order.

---

## `DynamicOutput`

Wrap a value with an explicit mapping key for dynamic fan-out. When a producer asset returns a list of `DynamicOutput`, the executor uses `.key` as the mapping key (instance name) instead of a numeric index.

```python
@rs.Task
def fan_out_with_keys() -> list[rs.DynamicOutput]:
    return [
        rs.DynamicOutput(key="alpha", value=1),
        rs.DynamicOutput(key="beta", value=2),
    ]
```

| Property | Type | Description |
|----------|------|-------------|
| `key` | `str` | Mapping key (used as the fan-out instance name). |
| `value` | `Any` | The wrapped output value. |
