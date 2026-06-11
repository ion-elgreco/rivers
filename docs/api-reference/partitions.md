# Partitions

## `PartitionKey`

Identifies which partition(s) to materialize.

### `PartitionKey.single()`

```python
PartitionKey.single(key: str | list[str]) -> PartitionKey.Single
```

Create a single-dimension partition key.

```python
# Single value
rs.PartitionKey.single("2024-01-15")

# Multiple values
rs.PartitionKey.single(["2024-01-15", "2024-01-16"])
```

**`PartitionKey.Single` attributes:**

| Attribute | Type |
|-----------|------|
| `key` | `list[str]` |

### `PartitionKey.multi()`

```python
PartitionKey.multi(keys: dict[str, str | list[str]]) -> PartitionKey.Multi
```

Create a multi-dimension partition key.

```python
rs.PartitionKey.multi({"date": "2024-01-15", "region": ["us", "eu"]})
```

**`PartitionKey.Multi` attributes:**

| Attribute | Type |
|-----------|------|
| `keys` | `dict[str, list[str]]` |

---

## `PartitionsDefinition`

Defines how an asset is partitioned.

Partition key values may not contain `|` or `,` — those characters are reserved by the canonical display form (`dim=value|dim=value`, values joined with `,`) used everywhere keys appear as strings. Dimension names in `multi()` additionally may not contain `=`. Constructors, `fmt` rendering, and `storage.add_dynamic_partitions()` reject violations.

### `PartitionsDefinition.static_()`

```python
PartitionsDefinition.static_(keys: list[str]) -> PartitionsDefinition.Static
```

Fixed set of partition keys.

```python
rs.PartitionsDefinition.static_(["us", "eu", "asia"])
```

**`Static` attributes:** `keys: list[str]`

### `PartitionsDefinition.daily()`

```python
PartitionsDefinition.daily(
    start: datetime,
    end: datetime | None = None,
    fmt: str | None = None,
) -> PartitionsDefinition.TimeWindow
```

Daily partitions. Default format: `"%Y-%m-%d"`.

Raises `PartitionDefinitionError` if a `fmt` override cannot round-trip the daily grid — e.g. `daily(fmt="%Y-%m")` collapses ~30 windows into one key (see [`time_window()`](#partitionsdefinitiontime_window) for the round-trip rule).

### `PartitionsDefinition.hourly()`

```python
PartitionsDefinition.hourly(
    start: datetime,
    end: datetime | None = None,
    fmt: str | None = None,
) -> PartitionsDefinition.TimeWindow
```

Hourly partitions. Default format: `"%Y-%m-%dT%H:00"`.

Raises `PartitionDefinitionError` if a `fmt` override cannot round-trip the hourly grid, e.g. `hourly(fmt="%Y-%m-%d")`.

### `PartitionsDefinition.time_window()`

```python
PartitionsDefinition.time_window(
    start: datetime,
    cron_schedule: str | None = None,
    interval_seconds: float | None = None,
    end: datetime | None = None,
    fmt: str | None = None,
) -> PartitionsDefinition.TimeWindow
```

Custom time-window partitions. Exactly one of `cron_schedule` or `interval_seconds` is required. `cron_schedule` accepts 5 fields (`min hour dom mon dow`) or 6 fields (`sec min hour dom mon dow`) — seconds are optional.

`fmt` must be at least as fine as the grid: every window start has to format to a key that parses back to exactly that start (e.g. an hourly grid with `fmt="%Y-%m-%d"` is rejected, because 24 windows would collapse into one key). Coarse fmts like `%Y-%m` or `%Y` are fine on equally coarse grids — missing calendar fields parse back as the window start. The round-trip is checked across the grid's first four years (capped at 1024 windows), so calendar drift that spares the earliest ticks — e.g. a 31-day interval under `fmt="%Y-%m"`, whose first ticks happen to land on month starts — is still rejected at construction. This applies to `daily(fmt=...)` and `hourly(fmt=...)` overrides too.

**`TimeWindow` attributes:**

| Attribute | Type |
|-----------|------|
| `cron_schedule` | `str \| None` |
| `interval_seconds` | `float \| None` |
| `start` | `datetime` |
| `end` | `datetime \| None` |
| `fmt` | `str` |

### `PartitionsDefinition.multi()`

```python
PartitionsDefinition.multi(
    dimensions: dict[str, PartitionsDefinition],
) -> PartitionsDefinition.Multi
```

Cartesian product of named child dimensions. Any number of dimensions is supported (`Static`, `TimeWindow`, `Dynamic`); the only restrictions are that there must be at least one dimension and no `Multi` may be nested inside another `Multi`.

**`Multi` attributes:** `dimensions: list[tuple[str, PartitionsDefinition]]`

### `PartitionsDefinition.dynamic()`

```python
PartitionsDefinition.dynamic(name: str) -> PartitionsDefinition.Dynamic
```

Runtime-extensible partitions. Keys are stored in [`Storage`](storage.md) and managed via:

- `storage.add_dynamic_partitions(name, keys)`
- `storage.delete_dynamic_partition(name, key)`
- `storage.get_dynamic_partitions(name)`
- `storage.has_dynamic_partition(name, key)`

```python
pd = rs.PartitionsDefinition.dynamic("customers")

@rs.Asset(partitions_def=pd)
def per_customer(context: rs.AssetExecutionContext):
    return load(context.partition_key)

repo.storage.add_dynamic_partitions("customers", ["acme", "globex"])
```

**`Dynamic` attributes:** `name: str`

### Methods

```python
def get_partition_keys(self) -> list[PartitionKey]
```

Enumerate all partition keys.

```python
def validate_partition_key(self, key: PartitionKey) -> bool
```

Check if a key is valid for this definition.

---

## `PartitionContext`

Runtime partition information available on IO context objects.

```python
PartitionContext(keys: list[PartitionKey], definition: PartitionsDefinition)
```

**Attributes:**

| Attribute | Type | Description |
|-----------|------|-------------|
| `keys` | `list[PartitionKey]` | All partition keys this step is responsible for. |
| `key` | `PartitionKey` | Convenience accessor for `keys[0]` — the canonical / first key. |
| `definition` | `PartitionsDefinition` | The definition the keys belong to. |

**Methods:**

```python
def time_window(self) -> tuple[datetime, datetime] | None
```

Returns the half-open `(start, end)` time window for the current key, or `None` if not a time-window partition.

---

## `PartitionKeyRange`

An inclusive range of partition keys for backfills and lookups.

Range endpoints follow the **partition definition's ordering**, resolved when
the range is used: positional order for static keys (which need not be
alphabetical) and chronological order for time windows (so custom formats
like `%m/%d/%Y` work). Unknown endpoints and inverted ranges are rejected
with a precise error at backfill submission.

### `PartitionKeyRange.single()`

```python
PartitionKeyRange.single(from_key: str, to_key: str) -> PartitionKeyRange
```

Single-dimension range from `from_key` to `to_key` (inclusive).

### `PartitionKeyRange.multi()`

```python
PartitionKeyRange.multi(
    dimensions: dict[str, tuple[str, str] | list[str]],
) -> PartitionKeyRange
```

Multi-dimension range. Each dimension is either a `(from, to)` tuple or an explicit list of keys.

---

## `PartitionMapping`

Controls how upstream partition keys map to downstream partition keys.

### `PartitionMapping.identity()`

Same partition key passes through.

### `PartitionMapping.all_partitions()`

All upstream partitions are loaded.

### `PartitionMapping.static_(mapping)`

```python
PartitionMapping.static_(mapping: dict[str, str]) -> PartitionMapping.Static
```

Explicit key-to-key mapping.

### `PartitionMapping.time_window(offset)`

```python
PartitionMapping.time_window(offset: int) -> PartitionMapping.TimeWindow
```

Offset by N time periods (e.g., `-1` for previous day): materializing
`2024-01-05` with `offset=-1` loads the upstream's `2024-01-04` partition.
Works for cron and interval windows alike; a key that shifts outside the
upstream's `[start, end)` range fails the run with a precise error.

### `PartitionMapping.specific_partitions(partition_keys)`

```python
PartitionMapping.specific_partitions(partition_keys: list[str]) -> PartitionMapping.SpecificPartitions
```

Maps all downstream partitions to a specific set of upstream partition keys. Every downstream partition depends on the same named upstream partitions regardless of its own key.

```python
# Downstream always depends on upstream partitions "a" and "b"
rs.PartitionMapping.specific_partitions(["a", "b"])
```

### `PartitionMapping.for_keys(selectors)`

```python
PartitionMapping.for_keys(
    selectors: list[PartitionKey | PartitionKeyRange],
) -> PartitionMapping.ForKeys
```

Maps an unpartitioned upstream to specific downstream partition keys. When the downstream partition key matches a selector, the upstream is loaded; otherwise the parameter receives `None`.

Only valid on edges where the downstream is partitioned and the upstream is unpartitioned.

```python
# Load this upstream only for partition "a"
rs.PartitionMapping.for_keys([rs.PartitionKey.single("a")])

# Load for a range of time-window partitions
rs.PartitionMapping.for_keys([
    rs.PartitionKeyRange.single(from_key="2024-01-01", to_key="2024-06-30"),
])
```

**`ForKeys` attributes:**

| Attribute | Type |
|-----------|------|
| `selectors` | `list[PartitionKey \| PartitionKeyRange]` |

### `PartitionMapping.subset()`

```python
PartitionMapping.subset() -> PartitionMapping.Subset
```

Maps a partitioned upstream whose keys are a subset of the downstream's keys. When the downstream partition key exists in the upstream, the upstream is loaded with that key; otherwise the parameter receives `None`.

Only valid on edges where both sides are partitioned with the same partition type, and the upstream keys are a subset of the downstream keys.

```python
rs.PartitionMapping.subset()
```

### `PartitionMapping.multi(dimension_mappings)`

```python
PartitionMapping.multi(
    dimension_mappings: dict[str, PartitionMapping | tuple[str, PartitionMapping]]
) -> PartitionMapping.Multi
```

Maps individual dimensions of a multi-dimensional partition. Each key is a dimension name, and the value is either a `PartitionMapping` (same-name dimension) or a `(target_dimension_name, mapping)` tuple (cross-dimension mapping).

### `PartitionMapping.multi_to_single(dimension_name)`

```python
PartitionMapping.multi_to_single(
    dimension_name: str,
    partition_mapping: PartitionMapping | None = None,
) -> PartitionMapping.MultiToSingle
```

Maps from a multi-dimensional upstream to a single-dimensional downstream by extracting one dimension. The optional `partition_mapping` is applied within that dimension.
