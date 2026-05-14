# Partitioned Assets

This guide walks through practical partition patterns. See [Partitions concept](../concepts/partitions.md) for API details on `PartitionsDefinition`, `PartitionKey`, and `PartitionMapping`.

## Static partitions with Delta Lake

Partition by a fixed set of categories and map each to a Delta Lake column:

```python
import rivers as rs

partitions = rs.PartitionsDefinition.static_(["us", "eu", "asia"])

@rs.Asset(
    partitions_def=partitions,
    io_handler=io,
    metadata={"delta/partition_expr": "region"},
)
def regional_data():
    ...
```

The `delta/partition_expr` metadata tells the IO handler which column maps to the partition dimension. On read, the handler generates a predicate (`region = 'us'`).

Execute a specific partition:

```python
repo = rs.CodeRepository(assets=[regional_data])
repo.materialize(partition_key=rs.PartitionKey.single("us"))

# Multiple partitions at once
repo.materialize(partition_key=rs.PartitionKey.single(["us", "eu"]))
```

## Time-window partitions with Delta Lake

Time-window partitions generate range predicates on read (`date >= '2024-01-15' AND date < '2024-01-16'`):

```python
from datetime import datetime

partitions = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))

@rs.Asset(
    partitions_def=partitions,
    io_handler=io,
    metadata={"delta/partition_expr": "date"},
)
def daily_events():
    ...

repo.materialize(partition_key=rs.PartitionKey.single("2024-01-15"))
```

## Multi-dimensional partitions

For multi-dimensional partitions, `delta/partition_expr` is a JSON dict mapping dimension names to column names:

```python
partitions = rs.PartitionsDefinition.multi({
    "date": rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1)),
    "region": rs.PartitionsDefinition.static_(["us", "eu"]),
})

@rs.Asset(
    partitions_def=partitions,
    io_handler=io,
    metadata={
        "delta/partition_expr": '{"date": "event_date", "region": "region_code"}',
    },
)
def events():
    ...
```

Execute with a multi key:

```python
repo.materialize(partition_key=rs.PartitionKey.multi({
    "date": "2024-01-15",
    "region": "us",
}))

# Multiple values per dimension
repo.materialize(partition_key=rs.PartitionKey.multi({
    "date": "2024-01-15",
    "region": ["us", "eu"],
}))
```

## Dependencies with partition mappings

Use `deps` on any asset type to declare upstream dependencies with custom partition mappings:

```python
pd_down = rs.PartitionsDefinition.static_(["a", "b"])
pd_source = rs.PartitionsDefinition.static_(["1", "2", "3"])

@rs.Asset(io_handler=io, partitions_def=pd_source)
def source(context: rs.AssetExecutionContext) -> int:
    return {"1": 10, "2": 20, "3": 30}[context.partition_key]

@rs.Asset(
    io_handler=io,
    partitions_def=pd_down,
    deps=[
        rs.AssetDef.input(
            "source",
            partition_mapping=rs.PartitionMapping.static_({"a": "1", "b": "2"}),
        ),
    ],
)
def consumer(source: int) -> int:
    return source + 1
```

When partition `"a"` of `consumer` is materialized, partition `"1"` of `source` is loaded.

## Listing and validating keys

```python
partitions = rs.PartitionsDefinition.daily(
    start=datetime(2024, 1, 1),
    end=datetime(2024, 1, 5),
)
keys = partitions.get_partition_keys()
# [PartitionKey("2024-01-01"), PartitionKey("2024-01-02"), ...]

partitions.validate_partition_key(rs.PartitionKey.single("2024-01-03"))  # True
```

## Partitioned multi-assets

Apply `partitions_def` at the top level of `Asset.from_multi()` to partition all outputs uniformly. Each output receives the same partition context:

```python
pd = rs.PartitionsDefinition.static_(["us", "eu", "asia"])

@rs.Asset.from_multi(
    partitions_def=pd,
    output_defs=[
        rs.AssetDef("users", io_handler=io),
        rs.AssetDef("orders", io_handler=io),
    ],
)
def extract(context: rs.AssetExecutionContext):
    region = context.partition_key
    yield rs.Output(value=load_users(region), output_name="users")
    yield rs.Output(value=load_orders(region), output_name="orders")

repo = rs.CodeRepository(assets=[extract])
repo.materialize(partition_key=rs.PartitionKey.single("us"))
```

Downstream assets depend on individual outputs as usual:

```python
@rs.Asset(partitions_def=pd, io_handler=io)
def user_report(users: list) -> dict:
    return {"count": len(users)}
```

### Per-output partition definitions

When outputs have different partition spaces, set `partitions_def` on individual `AssetDef` entries instead. The definitions must share the same variant type (e.g. all Static) and have at least one overlapping key:

```python
pd_a = rs.PartitionsDefinition.static_(["a", "b", "c"])
pd_b = rs.PartitionsDefinition.static_(["b", "c", "d"])

@rs.Asset.from_multi(
    output_defs=[
        rs.AssetDef("x", partitions_def=pd_a, io_handler=io),
        rs.AssetDef("y", partitions_def=pd_b, io_handler=io),
    ],
)
def multi():
    yield rs.Output(value=1, output_name="x")
    yield rs.Output(value=2, output_name="y")

# "b" is in both partition spaces — both outputs execute
repo.materialize(partition_key=rs.PartitionKey.single("b"))

# "a" is only in x's space — materialize x individually
repo.materialize(["x"], partition_key=rs.PartitionKey.single("a"))
```

### Multi-asset dependencies with `deps`

Use `deps` to declare upstream dependencies with custom partition mappings, IO handler overrides, or metadata overrides.

**Data dependency with partition mapping:**

```python
pd_multi = rs.PartitionsDefinition.static_(["a", "b"])
pd_source = rs.PartitionsDefinition.static_(["1", "2", "3"])

@rs.Asset(io_handler=io, partitions_def=pd_source)
def source(context: rs.AssetExecutionContext) -> int:
    return {"1": 10, "2": 20, "3": 30}[context.partition_key]

@rs.Asset.from_multi(
    partitions_def=pd_multi,
    output_defs=[rs.AssetDef("out", io_handler=io)],
    deps=[
        rs.AssetDef.input(
            "source",
            partition_mapping=rs.PartitionMapping.static_({"a": "1", "b": "2"}),
        ),
    ],
)
def transform(source: int):
    yield rs.Output(value=source * 2, output_name="out")
```

When partition `"a"` is materialized, `source` partition `"1"` is loaded (value 10), so `out` receives 20.

**IO handler override:**

```python
deps=[
    rs.AssetDef.input("source", io_handler=CustomLoader()),
]
```

The override only affects how the multi-asset loads this input — the upstream's output handler is unchanged.

**Metadata override:**

```python
deps=[
    rs.AssetDef.input("source", metadata={"columns": "a,b,c"}),
]
```

Replaces (not merges) the upstream's metadata in `InputContext.asset_metadata` for this load only.

**Lineage-only dependency:**

```python
deps=[
    rs.AssetDef.dep("trigger"),  # ensures trigger runs first, no data loaded
]
```

## Partitioned graph assets

Graph assets support `partitions_def` — the partition context propagates through all internal tasks:

```python
pd = rs.PartitionsDefinition.static_(["a", "b", "c"])

@rs.Task
def step_one() -> int:
    return 10

@rs.Task
def step_two(step_one: int) -> int:
    return step_one * 2

@rs.Asset.from_graph(name="pipeline", partitions_def=pd)
def pipeline():
    a = step_one()
    return step_two(a)

repo = rs.CodeRepository(
    assets=[pipeline],
    tasks=[step_one, step_two],
)

pk = rs.PartitionKey.single("a")
repo.materialize(partition_key=pk)

# Load outputs by partition key
repo.load_node("pipeline", partition_key=pk)          # 20
repo.load_node("pipeline/step_one", partition_key=pk)  # 10
```

### Partition mappings on graph assets

When a graph asset depends on an upstream asset with a different partition space, use `deps` to define the key mapping:

```python
pd_graph = rs.PartitionsDefinition.static_(["x", "y"])
pd_source = rs.PartitionsDefinition.static_(["1", "2", "3"])

@rs.Asset(io_handler=io, partitions_def=pd_source)
def source(context: rs.AssetExecutionContext) -> int:
    return {"1": 10, "2": 20, "3": 30}[context.partition_key]

@rs.Task
def transform(source: int) -> int:
    return source + 1

@rs.Asset.from_graph(
    name="pipeline",
    partitions_def=pd_graph,
    io_handler=io,
    deps=[
        rs.AssetDef.input(
            "source",
            partition_mapping=rs.PartitionMapping.static_(mapping={"x": "1", "y": "2"}),
        ),
    ],
)
def pipeline(source: int):
    return transform(source)
```

The mappings are propagated to internal tasks that consume the upstream dependency. When graph partition `"x"` is materialized, `source` partition `"1"` is loaded.

## Unpartitioned to partitioned (ForKeys)

Use `PartitionMapping.for_keys()` to map an unpartitioned upstream to specific downstream partition keys. Non-matching partitions receive `None`:

```python
@rs.Asset
def source_a() -> dict:
    return {"origin": "system_a", "data": [1, 2, 3]}

@rs.Asset
def source_b() -> dict:
    return {"origin": "system_b", "data": [4, 5, 6]}

@rs.Asset(
    partitions_def=rs.PartitionsDefinition.static_(["a", "b"]),
    deps=[
        rs.AssetDef.input(
            "source_a",
            partition_mapping=rs.PartitionMapping.for_keys([rs.PartitionKey.single("a")]),
        ),
        rs.AssetDef.input(
            "source_b",
            partition_mapping=rs.PartitionMapping.for_keys([rs.PartitionKey.single("b")]),
        ),
    ],
)
def merged(context: rs.AssetExecutionContext, source_a, source_b) -> dict:
    # partition "a": source_a=<data>, source_b=None
    # partition "b": source_a=None, source_b=<data>
    return source_a or source_b
```

Selectors can also be ranges for time-window partitions:

```python
rs.PartitionMapping.for_keys([
    rs.PartitionKeyRange.single(from_key="2024-01-01", to_key="2024-06-30"),
])
```

## Partition union (Subset)

Use `PartitionMapping.subset()` when multiple partitioned upstreams with disjoint key sets feed into a downstream whose partitions are the union:

```python
@rs.Asset(partitions_def=rs.PartitionsDefinition.static_(["a", "b"]))
def region_ab() -> int:
    return 1

@rs.Asset(partitions_def=rs.PartitionsDefinition.static_(["c"]))
def region_c() -> int:
    return 2

@rs.Asset(
    partitions_def=rs.PartitionsDefinition.static_(["a", "b", "c"]),
    deps=[
        rs.AssetDef.input("region_ab", partition_mapping=rs.PartitionMapping.subset()),
        rs.AssetDef.input("region_c", partition_mapping=rs.PartitionMapping.subset()),
    ],
)
def all_regions(context: rs.AssetExecutionContext, region_ab, region_c) -> int:
    # partition "a": region_ab=1, region_c=None
    # partition "c": region_ab=None, region_c=2
    return region_ab if region_ab is not None else region_c
```

## Mixed ForKeys + Subset

Combine both mappings when a downstream is fed by a mix of unpartitioned and partitioned upstreams:

```python
@rs.Asset
def legacy() -> dict:
    return {"source": "legacy", "data": [1, 2, 3]}

@rs.Asset(partitions_def=rs.PartitionsDefinition.static_(["b", "c"]))
def new_source() -> dict:
    return {"source": "new", "data": [4, 5, 6]}

@rs.Asset(
    partitions_def=rs.PartitionsDefinition.static_(["a", "b", "c"]),
    deps=[
        rs.AssetDef.input(
            "legacy",
            partition_mapping=rs.PartitionMapping.for_keys([rs.PartitionKey.single("a")]),
        ),
        rs.AssetDef.input(
            "new_source", partition_mapping=rs.PartitionMapping.subset()
        ),
    ],
)
def unified(context: rs.AssetExecutionContext, legacy, new_source) -> dict:
    # partition "a": legacy=<data>, new_source=None
    # partition "b": legacy=None, new_source=<data>
    return legacy if legacy is not None else new_source
```

## Custom format strings

Control the partition key format with `fmt`:

```python
# Partition keys like "20240115" instead of "2024-01-15"
rs.PartitionsDefinition.daily(
    start=datetime(2024, 1, 1),
    fmt="%Y%m%d",
)
```
