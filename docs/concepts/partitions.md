# Partitions

Partitions let you divide an asset's data into logical slices. This enables incremental processing, backfills, and per-slice IO operations.

## Partition definitions

Attach a `PartitionsDefinition` to an asset:

```python
import rivers as rs
from datetime import datetime

@rs.Asset(
    partitions_def=rs.PartitionsDefinition.daily(
        start=datetime(2024, 1, 1),
    ),
    metadata={"delta/partition_expr": "date"},
)
def daily_events():
    ...
```

### Static partitions

A fixed set of string keys:

```python
rs.PartitionsDefinition.static_(["us", "eu", "asia"])
```

### Time-window partitions

Daily or hourly with a start date:

```python
# Daily partitions
rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))

# Hourly partitions
rs.PartitionsDefinition.hourly(start=datetime(2024, 1, 1))

# Custom schedule (every 6 hours)
rs.PartitionsDefinition.time_window(
    start=datetime(2024, 1, 1),
    cron_schedule="0 */6 * * *",
)

# 6-field cron (seconds optional) — every 30 seconds
rs.PartitionsDefinition.time_window(
    start=datetime(2024, 1, 1),
    cron_schedule="*/30 * * * * *",
)

# Custom interval (every 30 minutes)
rs.PartitionsDefinition.time_window(
    start=datetime(2024, 1, 1),
    interval_seconds=1800,
)
```

`cron_schedule` accepts 5 fields (`min hour dom mon dow`) or 6 fields with leading seconds (`sec min hour dom mon dow`). The `fmt` parameter controls the partition key format string (default: `"%Y-%m-%d"` for daily, `"%Y-%m-%dT%H:00"` for hourly).

`start`/`end` are tz-naive: keys are wall-clock labels on the naive timeline,
so the grid is identical on every host timezone and DST transitions neither
drop nor duplicate keys.

### Multi-dimensional partitions

Cartesian product of any number of named dimensions. Each dimension can be `static_`, `daily`/`hourly`/`time_window`, or `dynamic` — mix and match freely:

```python
rs.PartitionsDefinition.multi({
    "date": rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1)),
    "region": rs.PartitionsDefinition.static_(["us", "eu"]),
    "tier": rs.PartitionsDefinition.static_(["free", "pro", "enterprise"]),
    "tenant": rs.PartitionsDefinition.dynamic("tenants"),
})
```

There is no upper limit on the dimension count. The only restriction is that `Multi` cannot be nested inside another `Multi` — flatten into a single dict instead.

### Dynamic partitions

Partition keys that are added at runtime via [`Storage`](../api-reference/storage.md):

```python
pd = rs.PartitionsDefinition.dynamic("customers")

@rs.Asset(partitions_def=pd)
def per_customer(context: rs.AssetExecutionContext):
    customer_id = context.partition_key
    return load_for(customer_id)

# Add new keys from outside the asset (e.g. a sensor):
repo.storage.add_dynamic_partitions("customers", ["acme", "globex"])
```

Dynamic partitions are useful when the key set is discovered at runtime (new tenants, new files, new symbols).

Keys must be registered **before** they can be materialized or backfilled —
submitting a key that isn't in storage (a typo, or one deleted via
`delete_dynamic_partition`) is rejected at the boundary with a precise error
instead of recording a materialization for a partition that doesn't exist.
To purge a key's data, run the [delete action](actions.md#delete) before you
remove the key.

## Named partition definitions

Register definitions once on the repository and reference them by name — the
same pattern as named retry policies and IO handlers:

```python
repo = rs.CodeRepository(
    assets=[daily_events, daily_summary],
    partition_defs={
        "daily": rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1)),
        "regions": rs.PartitionsDefinition.static_(["us", "eu", "asia"]),
    },
)

@rs.Asset(partitions_def="daily")
def daily_events(): ...

@rs.Asset(partitions_def="daily")
def daily_summary(daily_events): ...
```

Every `partitions_def=` site accepts a name: `@Asset`, `Asset.from_multi`
(top-level and per-output `AssetDef`), `Asset.from_graph`, `Asset.external`,
and `Task`. An unknown name fails at `resolve()`/`validate()` with the
registered names listed — never silently at execution time.

## Partition keys

When executing a partitioned job, provide a key:

```python
job = repo.get_job("daily")

# Single partition
job.execute(partition_key=rs.PartitionKey.single("2024-01-15"))

# Multiple values at once
job.execute(partition_key=rs.PartitionKey.single(["2024-01-15", "2024-01-16"]))

# Multi-dimensional
job.execute(partition_key=rs.PartitionKey.multi({
    "date": "2024-01-15",
    "region": ["us", "eu"],
}))
```

## Partition context in IO handlers

IO handlers receive a `PartitionContext` on the context object with:

- `key` -- the `PartitionKey` being materialized
- `definition` -- the `PartitionsDefinition` for the asset
- `time_window()` -- returns `(start, end)` datetimes for time-window partitions

## Partitions on multi-assets and graph assets

Both multi-assets and graph assets support `partitions_def`. On multi-assets, a top-level `partitions_def` applies to all outputs. On graph assets, the partition context propagates through all internal tasks.

All asset types accept `deps` with `AssetDef.input()` to specify per-dependency partition mappings, IO handler overrides, and metadata overrides.

See the [Partitioned Assets guide](../guides/partitioned-assets.md) for detailed examples.

## Partition mappings

Control how upstream partition keys map to downstream keys:

```python
rs.PartitionMapping.identity()        # same key
rs.PartitionMapping.all_partitions()  # load all upstream partitions
rs.PartitionMapping.static_({"a": "b"})  # explicit mapping
rs.PartitionMapping.time_window(offset=-1)  # offset by N periods
rs.PartitionMapping.specific_partitions(["a", "b"])  # always depend on specific keys
rs.PartitionMapping.for_keys([rs.PartitionKey.single("a")])  # unpartitioned upstream → specific partitions
rs.PartitionMapping.subset()          # partitioned upstream with subset of downstream keys
```

### ForKeys and Subset mappings

`ForKeys` maps an unpartitioned upstream to specific downstream partition keys. When materializing a partition that matches a selector, the upstream is loaded; for non-matching partitions, the parameter receives `None`:

```python
@rs.Asset
def source_a() -> dict:
    return {"data": [1, 2, 3]}

@rs.Asset(
    partitions_def=rs.PartitionsDefinition.static_(["a", "b"]),
    deps=[
        rs.AssetDef.input(
            "source_a",
            partition_mapping=rs.PartitionMapping.for_keys(
                [rs.PartitionKey.single("a")]
            ),
        ),
    ],
)
def merged(source_a) -> dict:
    # partition "a": source_a=<data>
    # partition "b": source_a=None
    return source_a or {"data": []}
```

`Subset` maps a partitioned upstream whose keys are a subset of the downstream. Keys present in the upstream are loaded normally; missing keys receive `None`:

```python
@rs.Asset(partitions_def=rs.PartitionsDefinition.static_(["a", "b"]))
def region_ab() -> int:
    return 1

@rs.Asset(
    partitions_def=rs.PartitionsDefinition.static_(["a", "b", "c"]),
    deps=[
        rs.AssetDef.input("region_ab", partition_mapping=rs.PartitionMapping.subset()),
    ],
)
def all_regions(region_ab) -> int:
    # partition "a": region_ab=1, partition "c": region_ab=None
    return region_ab if region_ab is not None else 0
```

## Partitions in the UI

The **Partitions** tab of an asset shows the state of each partition. A partition is *materialized*, *failed* (it failed and was not materialized again), or *missing*.

- **Tiles** show the number of partitions and the number in each state. The tile of the missing partitions has the **Materialize missing** button.
- **The strip** has one bar for each run of keys of the first dimension, for example one bar per day. Each bar is stacked by state, and a state that has a partition is always visible. Point at a bar to read its numbers.
- **The outline** has one row for each run of keys, with the partitions in each state. Open a row to see the rows below it: a month opens to its days, a day to its hours, and a key opens to the keys of the next dimension. The lowest level lists the partitions with their state, the time of the last materialization, and the run.

**Show** limits the outline to the rows that hold failed or missing partitions. The find field lists the keys of the first dimension that hold the text.

The dimensions of a multi-dimensional definition are in the order of their names: the first dimension by name leads the strip and the outline. The rows of time windows are in the order newest first, and the bars of the strip are in the order oldest first. Keys that do not sort by time (for example with the format `%d/%m/%Y`) are in the order of the definition, and so are the keys that the find field lists.

The browser gets only the rows on the screen. The code location splits the keys into rows, and storage counts the partitions of each row from an index: the size of the page does not grow with the number of partitions. The find field does not read each key of a time-window dimension: the code location looks at each day and at each time of day of the grid once, so a search of millions of keys takes some milliseconds. A key format that has a field with no fixed width (for example `%b`, the name of the month) makes the find read every key. When the code location does not answer, the tab lists the materialized partitions only, with no filter and no find field.
