# Backfills

## `BackfillResult`

Returned by `CodeRepository.backfill()`.

| Attribute | Type | Description |
|-----------|------|-------------|
| `backfill_id` | `str` | Unique identifier for the backfill |
| `num_partitions` | `int` | Total number of partitions in the backfill |
| `num_runs` | `int` | Number of runs created |
| `status` | `str` | Current status (`"in_progress"`, `"completed"`, `"failed"`, `"canceled"`) |
| `completed` | `int` | Number of completed partitions |
| `failed` | `int` | Number of failed partitions |
| `canceled` | `int` | Number of canceled partitions |
| `run_ids` | `list[str]` | IDs of runs created by the backfill |
| `is_dry_run` | `bool` | Whether this was a dry-run preview |
| `partition_keys` | `list[PartitionKey]` | Partition keys included in the backfill |

---

## `BackfillStatus`

Returned by `CodeRepository.get_backfill()`.

| Attribute | Type | Description |
|-----------|------|-------------|
| `backfill_id` | `str` | Unique identifier for the backfill |
| `status` | `str` | Current status (`"in_progress"`, `"completed"`, `"failed"`, `"canceled"`) |
| `total_partitions` | `int` | Total number of partitions |
| `completed_partitions` | `int` | Number of completed partitions |
| `failed_partitions` | `int` | Number of failed partitions |
| `canceled_partitions` | `int` | Number of canceled partitions |
| `run_ids` | `list[str]` | IDs of runs created by the backfill |
| `error` | `str \| None` | Error message if the backfill failed |
| `tags` | `list[tuple[str, str]]` | Tags attached to every run launched by the backfill |

---

## `BackfillStrategy`

Controls how partition keys are grouped into runs during a backfill.

### `BackfillStrategy.multi_run()`

```python
BackfillStrategy.multi_run() -> BackfillStrategy.MultiRun
```

One run per partition key (default).

### `BackfillStrategy.single_run()`

```python
BackfillStrategy.single_run() -> BackfillStrategy.SingleRun
```

All partition keys in a single run.

### `BackfillStrategy.per_dimension()`

```python
BackfillStrategy.per_dimension(
    multi_run: list[str],
    single_run: list[str],
) -> BackfillStrategy.PerDimension
```

Per-dimension control for multi-dimensional partitions.

| Parameter | Type | Description |
|-----------|------|-------------|
| `multi_run` | `list[str]` | Dimensions iterated across runs (at least one required) |
| `single_run` | `list[str]` | Dimensions batched within each run (at least one required) |

**`BackfillStrategy.PerDimension` attributes:**

| Attribute | Type |
|-----------|------|
| `multi_run` | `list[str]` |
| `single_run` | `list[str]` |

!!! warning
    A dimension cannot appear in both `multi_run` and `single_run`.

---

## `PartitionKeyRange`

Specifies a range of partition keys for a backfill.

### `PartitionKeyRange.single()`

```python
PartitionKeyRange.single(from_key: str, to_key: str) -> PartitionKeyRange
```

Range of single-dimension partition keys from `from_key` to `to_key` (inclusive).

```python
rs.PartitionKeyRange.single("2024-01-01", "2024-01-31")
```

### `PartitionKeyRange.multi()`

```python
PartitionKeyRange.multi(
    dimensions: dict[str, tuple[str, str] | list[str]],
) -> PartitionKeyRange
```

Multi-dimension partition key range. Each dimension value can be:

- A `(from, to)` tuple for a range
- A `list[str]` for explicit keys

```python
rs.PartitionKeyRange.multi({
    "date": ("2024-01-01", "2024-01-07"),
    "region": ["us", "eu", "asia"],
})
```

---

## `CodeRepository.backfill()`

```python
CodeRepository.backfill(
    selection: list[str] | None = None,
    partition_keys: list[PartitionKey] | None = None,
    partition_range: PartitionKeyRange | None = None,
    strategy: BackfillStrategy | None = None,
    failure_policy: str = "continue",
    max_concurrency: int = 4,
    tags: list[tuple[str, str]] | None = None,
    config: dict[str, dict[str, Any]] | None = None,
    block: bool = True,
    dry_run: bool = False,
) -> BackfillResult
```

Launch a backfill to reprocess partitions.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `selection` | `list[str] \| None` | `None` | Asset keys to backfill. `None` selects all partitioned assets. |
| `partition_keys` | `list[PartitionKey] \| None` | `None` | Explicit list of partition keys to process. |
| `partition_range` | `PartitionKeyRange \| None` | `None` | Range of partition keys. Mutually exclusive with `partition_keys`. |
| `strategy` | `BackfillStrategy \| None` | `None` | How to group partitions into runs. Falls back to asset-level strategy, then `MultiRun`. |
| `failure_policy` | `str` | `"continue"` | `"continue"` to keep processing on failure, `"stop_on_failure"` to halt. |
| `max_concurrency` | `int` | `4` | Maximum number of concurrent runs. |
| `tags` | `list[tuple[str, str]] \| None` | `None` | Tags attached to the backfill and its runs. Use `("rivers/priority", "N")` to override default priority (-10). |
| `config` | `dict[str, dict[str, Any]] \| None` | `None` | Per-asset config overrides (keyed by asset name). |
| `block` | `bool` | `True` | If `True`, wait for the backfill to complete before returning. |
| `dry_run` | `bool` | `False` | If `True`, compute the plan without executing. |

!!! note
    Provide either `partition_keys` or `partition_range`, not both.

!!! info "Priority"
    Backfill runs default to priority **-10** (lower than regular runs at priority 0), ensuring scheduled and manually triggered runs are dequeued first when a run queue is configured. Override with `tags=[("rivers/priority", "5")]`.

---

## `CodeRepository.get_backfill()`

```python
CodeRepository.get_backfill(backfill_id: str) -> BackfillStatus | None
```

Get the current status of a backfill by ID. Returns `None` if not found.

---

## `CodeRepository.cancel_backfill()`

```python
CodeRepository.cancel_backfill(backfill_id: str) -> bool
```

Cancel a running or requested backfill. Returns `True` if the in-process coordinator was signalled (the backfill has live state in this process); returns `False` and falls back to a storage-level cancel marker otherwise.

---

## `CodeRepository.rerun_backfill()`

```python
CodeRepository.rerun_backfill(
    backfill_id: str,
    block: bool = True,
    dry_run: bool = False,
) -> BackfillResult
```

Re-launch the failed and canceled partitions of a previous backfill.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `backfill_id` | `str` | required | ID of the backfill to retry. |
| `block` | `bool` | `True` | If `True`, wait for the rerun to complete before returning. |
| `dry_run` | `bool` | `False` | If `True`, compute the plan without executing. |
