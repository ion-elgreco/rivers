# Assets

An **asset** is a function that produces a data artifact. Assets form a directed acyclic graph (DAG) where dependencies are declared through function parameters.

rivers supports four asset types:

## Single assets

The most common type. A function decorated with `@Asset` that produces one output:

```python
import rivers as rs

@rs.Asset
def raw_data():
    return {"key": "value"}

@rs.Asset(name="clean_data", tags=["etl"], kinds="table", group="pipeline")
def clean(raw_data: dict):
    return {k: v.upper() for k, v in raw_data.items()}
```

Parameters like `name`, `tags`, `kinds`, and `group` are optional. The asset name defaults to the function name.

Single assets support the `deps` parameter for declaring upstream dependencies with custom partition mappings, IO handler overrides, or metadata overrides — see [Dependencies with `deps`](#dependencies-with-deps) below.

## Multi assets

A single function that produces multiple named outputs:

```python
@rs.Asset.from_multi(
    output_defs=[
        rs.AssetDef(name="users"),
        rs.AssetDef(name="orders"),
    ],
)
def extract():
    return {
        "users": load_users(),
        "orders": load_orders(),
    }
```

Each output becomes its own asset in the DAG. Downstream assets depend on individual outputs by name.

### Subsetting

Multi-assets are implicitly subsettable — you can materialize a subset of the outputs without redefining the asset. rivers runs the function as usual, then only persists yields / dict entries whose `output_name` is in the requested selection:

```python
@rs.Asset.from_multi(
    output_defs=[rs.AssetDef("users"), rs.AssetDef("orders")],
)
def extract():
    yield rs.Output(value=load_users(), output_name="users")
    yield rs.Output(value=load_orders(), output_name="orders")

repo = rs.CodeRepository(assets=[extract])
repo.materialize(selection=["users"])  # only "users" is persisted
```

For expensive computations, branch on `context.output_selection` to skip work for outputs that won't be persisted:

```python
@rs.Asset.from_multi(
    output_defs=[rs.AssetDef("users"), rs.AssetDef("orders")],
)
def extract(context: rs.AssetExecutionContext):
    if "users" in context.output_selection:
        yield rs.Output(value=load_users(), output_name="users")
    if "orders" in context.output_selection:
        yield rs.Output(value=load_orders(), output_name="orders")
```

When no subsetting is requested (i.e. the whole multi-asset is materialized), `context.output_selection` contains all output names.

### Partitions on multi-assets

Apply `partitions_def` at the top level to partition all outputs uniformly:

```python
pd = rs.PartitionsDefinition.static_(["us", "eu"])

@rs.Asset.from_multi(
    partitions_def=pd,
    output_defs=[rs.AssetDef("users"), rs.AssetDef("orders")],
)
def extract(context: rs.AssetExecutionContext):
    region = context.partition_key
    yield rs.Output(value=load_users(region), output_name="users")
    yield rs.Output(value=load_orders(region), output_name="orders")
```

Alternatively, set `partitions_def` on individual `AssetDef` entries when outputs have different partition spaces. Per-output definitions must share the same variant type and have overlapping keys.

### Dependencies with `deps`

Use the `deps` parameter to declare upstream dependencies with custom partition mappings, IO handler overrides, or metadata overrides. There are two kinds:

- **`AssetDef.input(name)`** — a data dependency. The upstream's output is loaded and passed as a function parameter (matched by `name`).
- **`AssetDef.dep(name)`** — a lineage-only edge. Ensures ordering in the DAG without loading data. The name does not need to match a function parameter.

```python
@rs.Asset.from_multi(
    partitions_def=pd,
    output_defs=[rs.AssetDef("report", io_handler=handler)],
    deps=[
        rs.AssetDef.input("source", partition_mapping=rs.PartitionMapping.static_({"us": "1"})),
        rs.AssetDef.dep("trigger"),
    ],
)
def report(source: int):
    yield rs.Output(value=source * 2, output_name="report")
```

`deps` can also be declared per-output on `AssetDef`. Input deps merge into the multi-asset's function-level input set (the function fires once for all outputs); lineage-only deps become edges to that specific output only:

```python
@rs.Asset.from_multi(
    output_defs=[
        rs.AssetDef("users", deps=[rs.AssetDef.dep("users_source")]),
        rs.AssetDef("orders", deps=[rs.AssetDef.dep("orders_source")]),
    ],
)
def extract():
    yield rs.Output(value=..., output_name="users")
    yield rs.Output(value=..., output_name="orders")
```

See the [API reference](../api-reference/assets.md#assetdefinput) for the full `AssetDef.input()` and `AssetDef.dep()` signatures.

## Graph assets

Compose `Task` operations into a sub-DAG treated as a single asset. Internal tasks are namespaced as `{graph_name}/{task_name}` and execute as independent steps:

```python
@rs.Task
def fetch(url: str):
    return requests.get(url).json()

@rs.Task
def transform(data: dict):
    return [row for row in data if row["active"]]

@rs.Asset.from_graph()
def pipeline():
    data = fetch("https://api.example.com/users")
    return transform(data)
```

The `return` value determines the final node — its output becomes the graph asset's output. Use `node_io_handler` to give internal tasks a different IO handler than the graph output, and `rivers/node/executor` metadata to control which executor internal tasks use. See the [Graph Assets guide](../guides/graph-assets.md) for details.

## External assets

An external asset represents a data source managed outside rivers — for example, a table produced by another pipeline, an S3 bucket populated by an external system, or a shared dataset. External assets participate in the dependency graph so downstream assets can depend on them, but rivers never materializes them.

```python
# Declare an external data source
source_table = rs.Asset.external(
    name="source_table",
    io_handler=delta_handler,
    kinds="table",
    metadata={"path": "s3://bucket/source"},
)

# Downstream assets depend on it like any other asset
@rs.Asset
def enriched(source_table: pl.DataFrame) -> pl.DataFrame:
    return source_table.with_columns(pl.lit("enriched").alias("status"))
```

When `enriched` is materialized, rivers loads `source_table` via `delta_handler.load_input()` and passes the result as the function argument — but `source_table` itself is never executed.

### Observations

External assets can have an optional **observation function** that inspects the external data source and records metadata. This is useful for tracking freshness, row counts, or schema changes without materializing anything.

Use `Asset.external()` as a decorator to attach an observation function:

```python
@rs.Asset.external(io_handler=delta_handler)
def source_table(context: rs.AssetExecutionContext):
    # Query the external system for metadata
    context.add_output_metadata({
        "row_count": rs.MetadataValue.int(150_000),
        "last_modified": rs.MetadataValue.timestamp(1709913600.0),
    })
```

Trigger observations manually via `CodeRepository.observe()`:

```python
repo = rs.CodeRepository(assets=[source_table, enriched])

# Observe all external assets (or filter with asset_names=["source_table"])
observations = repo.observe()
print(observations["source_table"]["row_count"].raw_value())  # 150000
```

In production, attach an `AutomationCondition.on_cron()` to run observations on a schedule. When the observation records a new `data_version`, downstream assets with `AutomationCondition.eager()` will automatically materialize:

```python
source_table = rs.Asset.external(
    name="source_table",
    io_handler=handler,
    automation_condition=rs.AutomationCondition.on_cron("0 */6 * * *"),
)
```

### Key properties

- **No compute function** — external assets are never materialized by rivers
- **`io_handler` is required** — downstream assets need it to load data via `load_input()`
- **Source-only nodes** — external assets are leaf nodes in the DAG; they provide data to downstream assets but are never materialized themselves
- **Excluded from materialization** — `materialize()` automatically filters out external assets from the execution selection, but they remain in the dependency graph so downstream assets can load their data via `io_handler.load_input()`

## Self-dependent assets

An asset can read its own previously persisted value to support incremental patterns (running counters, accumulating windows, append-only state). Declare a parameter named `self` typed as `SelfDependency[T]` — the executor loads the asset's last stored value through its IO handler before invoking the function:

```python
@rs.Asset
def running_total(self: rs.SelfDependency[int], new_events: list) -> int:
    prev = self.get_inner()         # int | None
    if prev is None:
        return len(new_events)      # first run
    return prev + len(new_events)   # subsequent runs
```

How it works:

- **`get_inner()` returns the previously stored value, or `None` on first run** (when nothing has been persisted yet).
- **No graph edge is created for `self`** — there's no cycle. The dependency is satisfied through `io_handler.load_input()`, not the DAG.
- **An IO handler is required.** The asset's own `io_handler` is preferred; if unset, the default `InMemoryIOHandler` is used. Materialization raises `ConfigurationError` if neither is available.
- **`T` is forwarded as the IO handler's `type_hint`** (e.g. `SelfDependency[pa.Table]` makes `DeltaIOHandler.load_input` return a PyArrow Table).

This composes with normal `deps`: `self: SelfDependency[T]` only handles the self-load, leaving every other parameter free to bind to upstream assets / resources / config as usual.

## Asset metadata

Attach static metadata to an asset via the `metadata` parameter. Metadata serves three purposes:

1. **IO handler configuration** — IO handlers receive metadata as `context.asset_metadata` and use prefixed keys to control behavior (e.g. write mode, partition columns)
2. **Engine behavior** — rivers itself reads `rivers/`-prefixed keys to change execution behavior
3. **UI display** — all metadata is visible in the rivers web UI for debugging and documentation

```python
@rs.Asset(metadata={
    "delta/mode": "append",              # DeltaIOHandler: use append mode
    "delta/partition_expr": "date",       # DeltaIOHandler: partition by date column
    "rivers/executor": "in_process",      # Engine: force in-process execution
    "owner": "data-team",                 # UI: informational, displayed in asset detail
})
def events():
    ...
```

### Reserved metadata prefixes

| Prefix | Read by | Examples |
|--------|---------|----------|
| `rivers/` | rivers engine | `rivers/executor` — override the executor for this asset (`"in_process"` or `"parallel"`); `rivers/node/executor` — override for a graph asset's internal task; `rivers/schema` — Arrow schema attached as `MetadataValue.Schema` |
| `delta/` | `DeltaIOHandler` | `delta/mode`, `delta/schema_mode`, `delta/partition_expr`, `delta/root_name`, `delta/columns`, `delta/version`, `delta/merge_predicate`, `delta/writer_properties`, `delta/commit_properties`, `delta/table_configuration` |

Custom IO handlers can define their own prefixed keys following the same convention.

Metadata is a `dict[str, str]` — all values are strings. IO handlers parse them as needed (e.g. `delta/partition_expr` can be a JSON dict for multi-dimensional partitions).

## Output metadata

IO handlers and asset functions can attach runtime metadata via `context.add_output_metadata()`. This metadata is available in the job result and can include `MetadataValue` instances for typed values:

```python
context.add_output_metadata({
    "num_rows": 1000,
    "schema": rs.MetadataValue.json('{"id": "int64"}'),
})
```

### Using `Output` for declarative metadata

Instead of using the context imperatively, you can return an `Output` object from your asset function. This carries the value, metadata, data version, and tags together:

```python
@rs.Asset(io_handler=handler)
def processed_data() -> rs.Output:
    df = compute_data()
    return rs.Output(
        value=df,
        metadata={
            "row_count": rs.MetadataValue.int(len(df)),
            "schema": rs.MetadataValue.json(df.schema.to_json()),
        },
        data_version="v2.0",
    )
```

When you return `Output`, the IO handler receives the **unwrapped value** (not the `Output` wrapper). You can also combine `Output` with `context.add_output_metadata()` — both are merged, with `Output` metadata taking precedence on key conflicts.

### Using `Observation` for external assets

Similarly, observation functions on external assets can return an `Observation` object:

```python
@rs.Asset.external(io_handler=handler)
def source_table(context: rs.AssetExecutionContext):
    last_mod = query_table_last_modified()
    return rs.Observation(
        metadata={"last_modified": rs.MetadataValue.timestamp(last_mod)},
        data_version=str(last_mod),
    )
```

`Observation` carries metadata and an optional data version, but no value (observations don't produce data). When returned, an `Observation` event is emitted to storage.

### Using `Materialization` for self-managed persistence

For terminal side-effecting assets — pushing to an API, emitting a message, writing directly to an external system — return a `Materialization` instead of an `Output`:

```python
@rs.Asset
def push_to_api(rows: list[dict]) -> rs.Materialization:
    response = requests.post(API_URL, json=rows)
    return rs.Materialization(
        metadata={"status_code": rs.MetadataValue.int(response.status_code)},
        data_version=response.headers["ETag"],
    )
```

`Materialization` is the explicit way to opt an asset out of the IO handler framework: the asset has already persisted its output, so the framework records a `Materialization` event with the supplied metadata and data version but never invokes `handle_output`. Downstream consumers cannot `load_input` the result — by design. See the [IO handlers concept](./io-handlers.md#opting-an-asset-out-of-the-io-handler) for details.
