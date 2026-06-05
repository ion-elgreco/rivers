# Backfills

Backfills let you reprocess a range of partitions for one or more assets. This is useful when you need to recompute historical data after a bug fix, schema change, or when onboarding a new asset that depends on existing partitioned data.

## When to use backfills

- Reprocess partitions after fixing a bug in asset logic
- Fill in data for a newly added partitioned asset
- Recompute downstream assets after upstream schema changes
- Re-run failed partitions from a previous execution

## Backfill strategies

The `BackfillStrategy` controls how partition keys are grouped into runs.

### MultiRun (default)

Creates one run per partition key. This gives maximum granularity -- if one partition fails, the others are unaffected.

```python
import rivers as rs

repo.backfill(
    selection=["daily_events"],
    partition_range=rs.PartitionKeyRange.single("2024-01-01", "2024-01-31"),
    strategy=rs.BackfillStrategy.multi_run(),
)
```

With 31 daily partitions, this creates 31 separate runs.

### SingleRun

Batches all partition keys into a single run. Useful when you want to minimize run overhead or when the asset logic handles multiple partitions efficiently.

```python
repo.backfill(
    selection=["daily_events"],
    partition_range=rs.PartitionKeyRange.single("2024-01-01", "2024-01-31"),
    strategy=rs.BackfillStrategy.single_run(),
)
```

With 31 daily partitions, this creates 1 run that processes all 31 partitions.

#### Reporting per-partition failures with `mark_partition_failed`

A `SingleRun` step is invoked once with **all** the backfill's keys in `context.partition.keys`. By default the run treats every key as succeeded if the function returns, and as failed if it raises — there's no granularity below the step boundary.

`context.mark_partition_failed(partition_key, error)` lets the function record failures for individual partitions while keeping the rest as successes. Use it when you can isolate per-key errors inside a vectorized run:

```python
@rs.Asset(
    partitions_def=rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1)),
    backfill_strategy=rs.BackfillStrategy.single_run(),
)
def daily_events(context: rs.AssetExecutionContext):
    for key in context.partition.keys:
        try:
            process_day(key)
        except Exception as exc:
            context.mark_partition_failed(key, str(exc))
            context.log.warning("partition %s failed: %s", key, exc)
    # Returning normally — partitions not marked failed are recorded as succeeded.
```

Semantics:

- Only valid in batched runs (`SingleRun` and the single-run dimensions of `PerDimension`). Calling it on a non-batched run is a no-op since each key already has its own run.
- The key must be in `context.partition.keys`; passing an unrelated key raises `ExecutionError("partition key … is not in this context's partition keys")`.
- Failures recorded this way roll up into the same `BackfillStatus.failed_partitions` / `BackfillResult.failed` counters as full-run failures — the UI and `repo.get_backfill()` see them identically.
- The function may still raise to signal a whole-run failure; `mark_partition_failed` is for the partial-failure case where you want some keys preserved as successes.
- Automation leaves a marked-failed partition alone: `eager()` / `on_missing()` treat it as *failed* (not *missing*), so they won't auto-re-request it. The deliberate failure sticks until you re-run that partition.

### PerDimension

For multi-dimensional partitions, gives per-dimension control. Dimensions in `multi_run` are iterated across runs; dimensions in `single_run` are batched within each run.

```python
repo.backfill(
    selection=["regional_events"],
    partition_range=rs.PartitionKeyRange.multi({
        "date": ("2024-01-01", "2024-01-07"),
        "region": ["us", "eu", "asia"],
    }),
    strategy=rs.BackfillStrategy.per_dimension(
        multi_run=["date"],
        single_run=["region"],
    ),
)
```

This creates 7 runs (one per date), each processing all 3 regions within a single run.

## Partition key ranges

Specify which partitions to backfill using `PartitionKeyRange`.

### Single-dimension range

```python
# Range of daily partitions
rs.PartitionKeyRange.single("2024-01-01", "2024-01-31")
```

### Multi-dimension range

```python
rs.PartitionKeyRange.multi({
    "date": ("2024-01-01", "2024-01-07"),
    "region": ["us", "eu"],
})
```

Dimension values can be a `(from, to)` tuple for ranges or a list of explicit keys.

## Running a backfill

Use `repo.backfill()` to launch a backfill:

```python
import rivers as rs
from datetime import datetime

@rs.Asset(
    partitions_def=rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1)),
)
def daily_events():
    ...

repo = rs.CodeRepository(assets=[daily_events])
repo.resolve()

result = repo.backfill(
    selection=["daily_events"],
    partition_range=rs.PartitionKeyRange.single("2024-01-01", "2024-01-15"),
    strategy=rs.BackfillStrategy.multi_run(),
    failure_policy="continue",
    max_concurrency=4,
    tags=[("team", "data-eng")],
    block=True,
)

print(f"Backfill {result.backfill_id}: {result.status}")
print(f"Completed: {result.completed}/{result.num_partitions}")
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `selection` | `list[str]` | All assets | Asset keys to backfill |
| `partition_keys` | `list[PartitionKey]` | None | Explicit partition keys |
| `partition_range` | `PartitionKeyRange` | None | Range of partition keys |
| `strategy` | `BackfillStrategy` | None | How to group partitions into runs |
| `failure_policy` | `str` | `"continue"` | `"continue"` or `"stop_on_failure"` |
| `max_concurrency` | `int` | `4` | Max concurrent runs |
| `tags` | `list[tuple[str, str]]` | None | Tags to attach to the backfill and its runs |
| `config` | `dict` | None | Per-asset config overrides |
| `block` | `bool` | `True` | Wait for completion |
| `dry_run` | `bool` | `False` | Preview without executing |

## Dry-run preview

Set `dry_run=True` to see what a backfill would do without actually executing:

```python
result = repo.backfill(
    selection=["daily_events"],
    partition_range=rs.PartitionKeyRange.single("2024-01-01", "2024-01-31"),
    dry_run=True,
)

print(f"Would create {result.num_runs} runs for {result.num_partitions} partitions")
print(f"Partition keys: {result.partition_keys}")
```

## Failure policies

Control what happens when a partition fails:

- **`continue`** (default) -- other partitions keep running. Failed partitions are recorded and can be retried.
- **`stop_on_failure`** -- stop the backfill immediately when any partition fails. Remaining partitions are marked as canceled.

```python
repo.backfill(
    selection=["daily_events"],
    partition_range=rs.PartitionKeyRange.single("2024-01-01", "2024-01-31"),
    failure_policy="stop_on_failure",
)
```

## Config overrides

Pass per-asset configuration overrides to the backfill:

```python
repo.backfill(
    selection=["daily_events"],
    partition_range=rs.PartitionKeyRange.single("2024-01-01", "2024-01-31"),
    config={"daily_events": {"batch_size": 5000}},
)
```

## Asset-level backfill strategy

You can set a default backfill strategy on an asset using the `backfill_strategy` parameter on `@Asset`. This strategy is used when no explicit strategy is passed to `repo.backfill()`.

```python
@rs.Asset(
    partitions_def=rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1)),
    backfill_strategy=rs.BackfillStrategy.single_run(),
)
def daily_events():
    ...
```

### Strategy precedence

1. Explicit `strategy` passed to `repo.backfill()` (highest priority)
2. `backfill_strategy` on the `@Asset` decorator
3. Default (`MultiRun`)

## Monitoring backfills

### Programmatic

```python
# Check status of a running backfill
status = repo.get_backfill(result.backfill_id)
print(f"Status: {status.status}")
print(f"Progress: {status.completed_partitions}/{status.total_partitions}")
print(f"Failed: {status.failed_partitions}, canceled: {status.canceled_partitions}")

# Cancel a running backfill
repo.cancel_backfill(result.backfill_id)

# Re-launch failed/canceled partitions of a previous backfill
result = repo.rerun_backfill(result.backfill_id, block=True)
```

### Web UI

The rivers web UI provides a dedicated **Backfills** page at `/backfills` that shows all backfills with their status, progress, and associated runs.
