# Schedules

## `@Schedule` decorator

Creates a schedule from a function. The function receives a `ScheduleEvaluationContext` and returns a `RunRequest`, `SkipReason`, or a list of `RunRequest`.

```python
import rivers as rs

@rs.Schedule(cron_schedule="0 0 * * *", job_name="my_job")
def nightly(context: rs.ScheduleEvaluationContext):
    return rs.RunRequest(tags={"date": context.scheduled_execution_time})
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `cron_schedule` | `str` | required | Cron expression. 5 fields (`min hour dom mon dow`, e.g. `"0 0 * * *"`) or 6 fields (`sec min hour dom mon dow`, e.g. `"*/30 0 0 * * *"`) — seconds are optional. |
| `job_name` | `str` | required | Name of the job to trigger. |
| `name` | `str \| None` | `None` | Schedule name. Defaults to the function name. |
| `default_status` | `ScheduleStatus` | `Stopped` | Whether the schedule starts running or stopped. |
| `timezone` | `str \| None` | `None` | Timezone for cron evaluation (e.g. `"US/Eastern"`). |
| `tags` | `dict[str, str] \| None` | `None` | Tags for categorization. |
| `description` | `str \| None` | `None` | Human-readable description. |
| `eval_mode` | `EvalMode` | `EvalMode.Auto` | Execution mode for the evaluation function. |
| `eval_timeout` | `str \| None` | `None` | Timeout for evaluation as a human-readable duration (e.g. `"5m"`). |

**Returns:** `Schedule`

---

## `Schedule`

A schedule that can trigger a job on a cron expression. Can be created directly or via the `@Schedule` decorator.

```python
# Direct construction (no evaluation function)
sched = rs.Schedule(cron_schedule="0 0 * * *", job_name="my_job", name="nightly")

# With evaluation function
sched = rs.Schedule(
    cron_schedule="0 12 * * *",
    job_name="daily_job",
    name="noon_schedule",
    evaluation_fn=my_eval_fn,
    default_status=rs.ScheduleStatus.Running,
    timezone="US/Eastern",
    tags={"env": "prod"},
    description="Runs at noon",
)
```

**Constructor:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `cron_schedule` | `str` | required | Cron expression. Cannot be empty. |
| `job_name` | `str` | required | Job to trigger. Cannot be empty. |
| `name` | `str \| None` | `None` | Schedule name. Defaults to `{job_name}_schedule`. |
| `evaluation_fn` | `Callable \| None` | `None` | Function called on each tick. |
| `default_status` | `ScheduleStatus` | `Stopped` | Initial status. |
| `timezone` | `str \| None` | `None` | Timezone for cron evaluation. |
| `tags` | `dict[str, str] \| None` | `None` | Tags for categorization. |
| `description` | `str \| None` | `None` | Human-readable description. |
| `eval_mode` | `EvalMode` | `EvalMode.Auto` | Execution mode for the evaluation function. |
| `eval_timeout` | `str \| None` | `None` | Timeout for evaluation as a human-readable duration (e.g. `"5m"`). |

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `name` | `str` | Schedule name. |
| `cron_schedule` | `str` | Cron expression. |
| `job_name` | `str` | Target job name. |
| `default_status` | `ScheduleStatus` | Initial status. |
| `timezone` | `str \| None` | Timezone. |
| `tags` | `dict[str, str] \| None` | Tags. |
| `description` | `str \| None` | Description. |
| `eval_mode` | `EvalMode` | Execution mode. |
| `eval_timeout` | `str \| None` | Evaluation timeout. |

---

## `ScheduleEvaluationContext`

Context passed to a schedule's evaluation function on each tick.

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `scheduled_execution_time` | `str` | ISO 8601 timestamp of the tick. |
| `schedule_name` | `str` | Name of the schedule being evaluated. |
| `config` | `ConfigT` | Config instance (if the evaluation function uses a config type hint). |
| `log` | `logging.Logger` | Logger named `rivers.schedules.<schedule_name>`. |

---

## `ScheduleStatus`

Enum controlling whether a schedule is active.

| Value | Description |
|-------|-------------|
| `ScheduleStatus.Running` | Schedule is active and will be evaluated. |
| `ScheduleStatus.Stopped` | Schedule is paused. |

---

## `EvalMode`

Controls how a schedule or sensor evaluation function is executed by the daemon.

| Value | Description |
|-------|-------------|
| `EvalMode.Auto` | Inferred at daemon start — async functions run automatically on the Python event loop managed from Rust (GIL released during `await`); sync functions run on a dedicated thread holding the GIL. |
| `EvalMode.InProcess` | Same as `Auto` today — keep the function in the daemon process. |
| `EvalMode.Subprocess` | Always run in a loky subprocess. Resources and config injection are not available. |

---

## `RunRequest`

Returned from a schedule or sensor evaluation to request a job run.

```python
req = rs.RunRequest(
    run_key="2025-03-10",
    tags={"env": "prod"},
    partition_key="2025-03-10",
    job_name="my_job",
)
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `run_key` | `str \| None` | `None` | Idempotency key to prevent duplicate runs. |
| `tags` | `dict[str, str] \| None` | `None` | Tags for the triggered run. |
| `partition_key` | `str \| None` | `None` | Partition key to materialize. |
| `job_name` | `str \| None` | `None` | Override the default job name. |

---

## `SkipReason`

Returned from a schedule or sensor evaluation to skip the tick.

```python
skip = rs.SkipReason("Data not ready yet")
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `message` | `str` | `""` | Reason for skipping. |

---

## `BackfillRequest`

A schedule (or sensor) can also yield a `BackfillRequest` to ask the daemon to launch a backfill instead of a single run. Mirrors the arguments of `CodeRepository.backfill()`.

```python
@rs.Schedule(cron_schedule="0 0 * * 0", job_name="weekly_etl")
def weekly_recompute(context: rs.ScheduleEvaluationContext):
    return rs.BackfillRequest(
        selection=["daily_events"],
        partition_range=rs.PartitionKeyRange.single("2024-01-01", "2024-12-31"),
        strategy=rs.BackfillStrategy.multi_run(),
        max_concurrency=4,
    )
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `selection` | `list[str]` | required | Asset names to backfill. |
| `partition_keys` | `list[PartitionKey] \| None` | `None` | Explicit partition keys (alternative to `partition_range`). |
| `partition_range` | `PartitionKeyRange \| None` | `None` | Range / cartesian-product spec. |
| `strategy` | `BackfillStrategy \| None` | `None` | How partitions are grouped into runs. |
| `failure_policy` | `str \| None` | `"continue"` | `"continue"` or `"stop_on_failure"`. |
| `max_concurrency` | `int` | `4` | Max concurrent partition runs. |
| `tags` | `dict[str, str] \| None` | `None` | Tags applied to every spawned run. |

---

## `ScheduleTickResult`

Result of evaluating a schedule tick via `CodeRepository.evaluate_schedule()`.

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `schedule_name` | `str` | Name of the evaluated schedule. |
| `run_requests` | `list[RunRequest \| BackfillRequest]` | Run/backfill requests generated. |
| `skip_reason` | `SkipReason \| None` | Skip reason if the tick was skipped. |

---

## Registration and evaluation

Register schedules with `CodeRepository` and evaluate them programmatically:

```python
@rs.Schedule(cron_schedule="0 0 * * *", job_name="my_job")
def nightly(context: rs.ScheduleEvaluationContext):
    return rs.RunRequest(tags={"date": context.scheduled_execution_time})

@rs.Asset
def my_asset() -> int:
    return 42

repo = rs.CodeRepository(assets=[my_asset], schedules=[nightly])

# Evaluate a schedule tick
result = repo.evaluate_schedule("nightly", execution_time="2025-03-10T00:00:00")
print(result.run_requests[0].tags)  # {"date": "2025-03-10T00:00:00"}

# Access registered schedules
print(repo.schedules)        # [Schedule(...)]
sched = repo.get_schedule("nightly")
```

**Evaluation return types:**

The evaluation function can return:

- `RunRequest` — triggers a single run
- `list[RunRequest]` — triggers multiple runs
- `SkipReason` — skips the tick with a reason
- `None` — skips the tick silently
