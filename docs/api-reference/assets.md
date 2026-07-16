# Assets

## `Asset`

Decorator for defining assets. Can be used bare or with parameters.

```python
# Bare decorator
@rs.Asset
def my_asset():
    return data

# With parameters
@rs.Asset(name="custom_name", tags=["etl"], kinds="table", group="pipeline")
def my_asset():
    return data
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str \| None` | `None` | Asset name. Defaults to function name. |
| `tags` | `list[str] \| None` | `None` | Tags for categorization. |
| `kinds` | `str \| list[str] \| None` | `None` | Asset kind(s) (e.g. `"table"`, `["table", "delta"]`). |
| `group` | `str \| None` | `None` | Group name for organization. |
| `code_version` | `str \| None` | `None` | Version string for change detection. |
| `io_handler` | `BaseIOHandler \| None` | `None` | IO handler for persistence. |
| `metadata` | `dict[str, str] \| None` | `None` | Static metadata passed to IO handlers. |
| `partitions_def` | `PartitionsDefinition \| None` | `None` | Partition definition. |
| `deps` | `list[DepDef] \| None` | `None` | Input and lineage-only dependencies. Created via `AssetDef.input()` and `AssetDef.dep()`. |
| `hooks` | `list[Hook] \| None` | `None` | Success/failure hooks for this asset. |
| `automation_condition` | `AutomationCondition \| None` | `None` | Declarative automation condition. |
| `backfill_strategy` | `BackfillStrategy \| None` | `None` | Default strategy when this asset is included in a backfill. |
| `pool` | `str \| list[str] \| None` | `None` | Concurrency pool(s) this asset belongs to. |
| `pool_slots` | `int \| dict[str, int] \| None` | `None` | Slots consumed per pool (default 1). |
| `retry` | `RetryPolicy \| str \| None` | `None` | Retry policy for this asset's step, or the name of a policy registered in `CodeRepository(retries=...)`. See [Retries & Compute](retries.md). |
| `compute` | `Compute \| None` | `None` | Per-asset compute (Kubernetes executor). See [Retries & Compute](retries.md#compute). |

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `name` | `str` | Asset name (function name or explicit `name=`). |
| `tags` | `list[str] \| None` | Tags. |
| `kinds` | `list[str]` | Kind(s), always a list. |
| `group` | `str \| None` | Group name. |
| `code_version` | `str \| None` | Code version string. |
| `metadata` | `dict[str, str] \| None` | Static metadata. |
| `partitions_def` | `PartitionsDefinition \| None` | Partition definition. |
| `partition_mapping` | `dict[str, PartitionMapping] \| None` | Per-dep partition mappings (derived from `deps`). |
| `hooks` | `list[Hook] \| None` | Attached hooks. |
| `automation_condition` | `AutomationCondition \| None` | Automation condition. |
| `pool` | `list[tuple[str, int]]` | Normalized pool membership: `(pool_key, slots)` pairs. |
| `observe_fn` | `Callable \| None` | Observation function (external assets only). |
| `is_async` | `bool` | True when the wrapped function is a coroutine function. |
| `is_single` | `bool` | True for `SingleAsset`. |
| `is_multi` | `bool` | True for `MultiAsset`. |
| `is_graph` | `bool` | True for `GraphAsset`. |
| `is_external` | `bool` | True for `ExternalAsset`. |

### `Asset.from_multi()`

Create a multi-output asset:

```python
asset = Asset.from_multi(
    output_defs=[AssetDef(name="a"), AssetDef(name="b")],
    name="extractor",
)
```

**Parameters:** Same as `Asset` plus:

| Parameter | Type | Description |
|-----------|------|-------------|
| `wraps` | `Callable \| None` | The function to wrap. |
| `output_defs` | `list[AssetDef]` | Output definitions for each output. |
| `partitions_def` | `PartitionsDefinition \| None` | Top-level partition definition applied to all outputs. Takes precedence over per-output `AssetDef.partitions_def`. |
| `deps` | `list[DepDef]` | Input and lineage-only dependencies. Created via `AssetDef.input()` and `AssetDef.dep()`. |
| `compute` | `Compute \| None` | Compute for the whole step — a multi-asset runs as one step (one pod), so this is declared here, not per output. |

#### Top-level `partitions_def`

When set on `from_multi`, the partition definition applies to every output, overriding any `partitions_def` set on individual `AssetDef` entries:

```python
pd = rs.PartitionsDefinition.static_(["a", "b", "c"])

@rs.Asset.from_multi(
    partitions_def=pd,
    output_defs=[
        rs.AssetDef("x", io_handler=handler),
        rs.AssetDef("y", io_handler=handler),
    ],
)
def multi(context: rs.AssetExecutionContext):
    yield rs.Output(value=10, output_name="x")
    yield rs.Output(value=20, output_name="y")
```

Without a top-level `partitions_def`, per-output definitions are allowed if they share the same variant type (e.g. all Static) and have at least one overlapping key. Mixed variant types (e.g. Static + Daily) are rejected.

#### `deps` parameter

The `deps` parameter declares upstream dependencies with fine-grained control over partition mapping, IO handler, and metadata. Dependencies are created via `AssetDef.input()` (data dependency) or `AssetDef.dep()` (lineage-only graph edge):

```python
@rs.Asset.from_multi(
    partitions_def=pd_multi,
    output_defs=[rs.AssetDef("out", io_handler=handler)],
    deps=[
        rs.AssetDef.input("data_source", partition_mapping=rs.PartitionMapping.static_({"a": "1"})),
        rs.AssetDef.dep("trigger"),  # ordering edge, no data loaded
    ],
)
def multi(data_source: int):
    yield rs.Output(value=data_source * 2, output_name="out")
```

See [`AssetDef.input()`](#assetdefinput), [`AssetDef.dep()`](#assetdefdep), and [`DepDef`](#depdef) below.

### `Asset.from_graph()`

Create a graph asset that composes tasks into a sub-DAG. Internal tasks are namespaced as `{graph_name}/{task_name}` and execute as independent plan steps. The `return` value determines the final node whose output becomes the graph asset's output.

```python
@Asset.from_graph(name="pipeline")
def my_pipeline():
    x = step_a()
    return step_b(x)
```

**Parameters:** Same as `Asset` plus:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `node_io_handler` | `BaseIOHandler \| str \| None` | `None` | IO handler for internal tasks. Falls back to `io_handler`, then default. |

`deps` is inherited from `Asset` — partition mappings, IO handler overrides, and metadata overrides are propagated to internal tasks.

Internal task executor is controlled via `rivers/node/executor` metadata (falls back to `rivers/executor`, then default). See [Graph Assets guide](../guides/graph-assets.md) for full details.

### `Asset.external()`

Create an external asset — a data source managed outside rivers. It participates in the dependency graph but is never materialized. Downstream assets load its data via `io_handler.load_input()`.

```python
# Direct call — no observation function
source = Asset.external(
    name="source_table",
    io_handler=my_handler,
    kinds="table",
    metadata={"path": "s3://bucket/table"},
)

# As decorator — the function becomes the observation function
@Asset.external(io_handler=my_handler)
def source_table(context: rs.AssetExecutionContext):
    context.add_output_metadata({
        "row_count": rs.MetadataValue.int(42_000),
    })
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str \| None` | `None` | Asset name. Required unless used as decorator. |
| `io_handler` | `BaseIOHandler` | required | IO handler for loading data. |
| `tags` | `list[str] \| None` | `None` | Tags for categorization. |
| `kinds` | `str \| list[str] \| None` | `None` | Asset kind(s) (e.g. `"table"`). |
| `group` | `str \| None` | `None` | Group name for organization. |
| `metadata` | `dict[str, str] \| None` | `None` | Static metadata passed to IO handlers. |
| `partitions_def` | `PartitionsDefinition \| None` | `None` | Partition definition. |
| `automation_condition` | `AutomationCondition \| None` | `None` | Declarative automation condition. |

The optional observation function receives an `AssetExecutionContext` and can call `context.add_output_metadata(...)` to record metadata about the external data source (e.g., row count, last modified timestamp). Trigger observations via `CodeRepository.observe()`.

---

## `SingleAsset`

Concrete type returned when `@Asset` decorates a function. Inherits every property from `Asset` (no extra attributes).

---

## `MultiAsset`

Returned by `Asset.from_multi()`. Inherits every property from `Asset` plus:

| Property | Type | Description |
|----------|------|-------------|
| `output_defs` | `list[AssetDef]` | Per-output asset definitions declared on this multi-asset. |

---

## `GraphAsset`

Returned by `Asset.from_graph()`. Inherits every property from `Asset` (no extra attributes).

---

## `ExternalAsset`

Returned by `Asset.external()`. Inherits every property from `Asset`. The `observe_fn` property exposes the callable attached when `Asset.external` is used as a decorator.

---

## `AssetDef`

Output definition for multi-assets.

```python
rs.AssetDef(
    name="output_name",
    tags=["tag"],
    kinds="table",
    io_handler=my_handler,
)
```

**Parameters:**

| Parameter | Type | Default |
|-----------|------|---------|
| `name` | `str` | required |
| `tags` | `list[str] \| None` | `None` |
| `kinds` | `str \| list[str] \| None` | `None` |
| `group` | `str \| None` | `None` |
| `code_version` | `str \| None` | `None` |
| `io_handler` | `BaseIOHandler \| str \| None` | `None` |
| `metadata` | `dict[str, str] \| None` | `None` |
| `partitions_def` | `PartitionsDefinition \| None` | `None` |
| `partition_mapping` | `dict[str \| AssetDef, PartitionMapping] \| None` | `None` |
| `pool` | `str \| list[str] \| None` | `None` |
| `pool_slots` | `int \| dict[str, int] \| None` | `None` |
| `retry` | `RetryPolicy \| str \| None` | `None` |
| `deps` | `list[DepDef]` | `[]` |

A multi-asset retries as one unit: every output that sets `retry` must set the same policy (checked at `resolve()`). Step compute is declared on `Asset.from_multi(compute=...)`, not per output.

### `AssetDef.input()`

Create a data dependency for use in the `deps` parameter of `@Asset(...)`, `Asset.from_multi()`, or `Asset.from_graph()`. The upstream asset's data is loaded and passed as a function parameter (matched by name).

```python
AssetDef.input(
    name: str,
    partition_mapping: PartitionMapping | None = None,
    io_handler: BaseIOHandler | str | None = None,
    metadata: dict[str, str] | None = None,
) -> DepDef
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Upstream asset name. Must match a parameter on the decorated function. |
| `partition_mapping` | `PartitionMapping \| None` | `None` | Partition mapping for this dependency. |
| `io_handler` | `BaseIOHandler \| str \| None` | `None` | Override the upstream's IO handler when loading this input. |
| `metadata` | `dict[str, str] \| None` | `None` | Override the upstream's metadata in `InputContext.asset_metadata`. Does not mutate the upstream asset. |

```python
deps=[
    # Load "source" with a custom partition mapping and IO handler override
    rs.AssetDef.input(
        "source",
        partition_mapping=rs.PartitionMapping.static_({"a": "1", "b": "2"}),
        io_handler=CustomLoader(),
    ),
    # Load "config" with metadata override (controls how the IO handler reads)
    rs.AssetDef.input("config", metadata={"columns": "a,b,c"}),
]
```

The `io_handler` override only affects how the downstream multi-asset loads this input — it does not change the upstream asset's output handler. Similarly, `metadata` replaces (not merges) the upstream's metadata for this load context only.

### `AssetDef.dep()`

Create a lineage-only dependency for use in the `deps` parameter of `@Asset(...)`, `Asset.from_multi()`, or `Asset.from_graph()`. Adds a graph edge (ordering guarantee) without loading any data. The name does not need to match a function parameter.

```python
AssetDef.dep(
    name: str,
    partition_mapping: PartitionMapping | None = None,
) -> DepDef
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Upstream asset name. |
| `partition_mapping` | `PartitionMapping \| None` | `None` | Partition mapping for this dependency. |

```python
deps=[
    rs.AssetDef.dep("trigger"),  # trigger runs before multi, but no data is passed
]
```

---

## `DepDef`

Dependency definition returned by `AssetDef.input()` and `AssetDef.dep()`. Used in the `deps` parameter of `Asset.from_multi()`.

**Attributes:**

| Attribute | Type | Description |
|-----------|------|-------------|
| `name` | `str` | Upstream asset name. |
| `partition_mapping` | `PartitionMapping \| None` | Partition mapping for this dependency. |
| `metadata` | `dict[str, str] \| None` | Metadata override (input deps only). |
| `is_input` | `bool` | `True` for `AssetDef.input()`, `False` for `AssetDef.dep()`. |

---

## `SelfDependency[T]`

Generic wrapper for self-referencing assets. When an asset parameter is named `self` with a `SelfDependency[T]` type hint, the executor loads the asset's own previous output via its `io_handler` before execution.

```python
@rs.Asset(io_handler=my_handler)
def incremental(self: rs.SelfDependency[list]) -> list:
    prev = self.get_inner()  # list | None
    if prev is None:
        return [1, 2, 3]
    return prev + [4, 5, 6]
```

- No graph edge is created for `self` — no cycle
- On first run (no persisted data), `get_inner()` returns `None`
- On subsequent runs, `get_inner()` returns the previously stored output of type `T`
- An IO handler is required — the asset's own `io_handler` is preferred, otherwise the default `InMemoryIOHandler` is used. Raises `ConfigurationError` only if neither is available.

**Methods:**

| Method | Returns | Description |
|--------|---------|-------------|
| `get_inner()` | `T \| None` | The previously stored output, or `None` on first run. |

---

## `Output`

Per-asset result type for materializations. Return from an `@Asset` function to declaratively carry a value alongside metadata, data version, and tags.

```python
@rs.Asset(io_handler=handler)
def my_asset() -> rs.Output:
    df = compute_data()
    return rs.Output(
        value=df,
        metadata={"row_count": rs.MetadataValue.int(len(df))},
        data_version="v2.0",
        tags=["validated"],
    )
```

The IO handler receives the **unwrapped value**, not the `Output` object. If combined with `context.add_output_metadata()`, both are merged — `Output` metadata takes precedence on key conflicts.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `value` | `Any` | `None` | The output data (passed to IO handler and downstream assets). |
| `metadata` | `dict[str, MetadataValue] \| None` | `None` | Metadata to record with the materialization event. |
| `data_version` | `str \| None` | `None` | Explicit data version (overrides auto UUID). |
| `tags` | `list[str] \| None` | `None` | Tags to record with the event. |

---

## `Observation`

Per-asset result type for external asset observations. Return from an observation function to declaratively carry metadata and data version.

```python
@rs.Asset.external(io_handler=handler)
def source_table(context: rs.AssetExecutionContext):
    return rs.Observation(
        metadata={"row_count": rs.MetadataValue.int(150_000)},
        data_version="2024-03-16",
    )
```

When returned, an `Observation` event is emitted to storage. Can be combined with `context.add_output_metadata()` — `Observation` metadata takes precedence on conflict.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `metadata` | `dict[str, MetadataValue] \| None` | `None` | Metadata to record with the observation event. |
| `data_version` | `str \| None` | `None` | The observed data version. |

---

## `Materialization`

Per-asset result type for **assets that manage their own persistence**. Return from an `@Asset` function when the body has already written its output to its destination — e.g. an HTTP push, a message-queue emit, or a direct write to an external table — and there is nothing meaningful to round-trip through an IO handler.

```python
@rs.Asset
def push_to_api(rows: list[dict]) -> rs.Materialization:
    response = requests.post(API_URL, json=rows)
    return rs.Materialization(
        metadata={"status_code": rs.MetadataValue.int(response.status_code)},
        data_version=response.headers["ETag"],
    )
```

When returned, a `Materialization` event is emitted to storage with the supplied metadata, data version, and tags — but `handle_output` is **never called**. Use this in place of `Output(value)` when:

- The asset is a terminal side-effecting node (push, emit, external write).
- You want provenance and observability (data version, metadata) without persisting a value rivers can later `load_input`.

This is the recommended pattern for opting an asset out of the IO handler framework. It works uniformly across `Executor.in_process()`, `Executor.parallel()`, and `Executor.kubernetes()` — the discriminator lives at the return type, so every executor takes the same code path.

**Downstream consumers cannot `load_input`** an output produced via `Materialization` — by design. Treat such assets as terminal in the graph.

Can be combined with `context.add_output_metadata()` and `context.register_data_version()` — `Materialization` metadata and data version take precedence on conflict.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `metadata` | `dict[str, MetadataValue] \| None` | `None` | Metadata to record with the materialization event. |
| `data_version` | `str \| None` | `None` | Explicit data version (overrides auto UUID). |
| `tags` | `list[str] \| None` | `None` | Tags to record with the event. |
| `output_name` | `str \| None` | `None` | For multi-asset generator yields, identifies which output this belongs to. |
