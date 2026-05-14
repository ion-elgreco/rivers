# Sensors

## `@Sensor` decorator

Creates a sensor from a function. The function receives a `SensorEvaluationContext` and returns a `SensorResult`, `RunRequest`, `SkipReason`, or `None`.

```python
import rivers as rs

@rs.Sensor(job_name="my_job")
def file_sensor(context: rs.SensorEvaluationContext):
    # Check for new data
    if has_new_files():
        return rs.SensorResult(
            run_requests=[rs.RunRequest()],
            cursor=str(latest_timestamp()),
        )
    return rs.SkipReason("No new files")
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `job_name` | `str \| None` | `None` | Name of the job to trigger. |
| `name` | `str \| None` | `None` | Sensor name. Defaults to the function name. |
| `minimum_interval` | `str \| None` | `None` | Minimum interval between evaluations as a human-readable duration (e.g. `"30s"`, `"1m"`). |
| `default_status` | `SensorStatus` | `Running` | Whether the sensor starts running or stopped. |
| `description` | `str \| None` | `None` | Human-readable description. |
| `tags` | `dict[str, str] \| None` | `None` | Tags for categorization. |
| `asset_selection` | `list[str] \| None` | `None` | Assets this sensor monitors. |
| `eval_mode` | `EvalMode` | `EvalMode.Auto` | Execution mode for the evaluation function. |
| `eval_timeout` | `str \| None` | `None` | Timeout for evaluation as a human-readable duration (e.g. `"5m"`). |

**Returns:** `Sensor`

---

## `Sensor`

A sensor that polls for external conditions and triggers runs. Can be created directly or via the `@Sensor` decorator.

```python
sensor = rs.Sensor(
    name="my_sensor",
    job_name="my_job",
    minimum_interval="1m",
    default_status=rs.SensorStatus.Running,
    description="Checks for new data",
    tags={"team": "data"},
    asset_selection=["raw_data"],
)
```

**Constructor:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | Sensor name. |
| `job_name` | `str \| None` | `None` | Job to trigger. |
| `evaluation_fn` | `Callable \| None` | `None` | Function called on each tick. |
| `minimum_interval` | `str \| None` | `None` | Minimum interval between ticks (e.g. `"30s"`). |
| `default_status` | `SensorStatus` | `Running` | Initial status. |
| `description` | `str \| None` | `None` | Description. |
| `tags` | `dict[str, str] \| None` | `None` | Tags. |
| `asset_selection` | `list[str] \| None` | `None` | Assets this sensor monitors. |
| `eval_mode` | `EvalMode` | `EvalMode.Auto` | Execution mode for the evaluation function. |
| `eval_timeout` | `str \| None` | `None` | Timeout for evaluation as a human-readable duration (e.g. `"5m"`). |

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `name` | `str` | Sensor name. |
| `job_name` | `str \| None` | Target job name. |
| `minimum_interval` | `str \| None` | Min interval between ticks. |
| `default_status` | `SensorStatus` | Initial status. |
| `description` | `str \| None` | Description. |
| `tags` | `dict[str, str] \| None` | Tags. |
| `asset_selection` | `list[str] \| None` | Monitored assets. |
| `eval_mode` | `EvalMode` | Execution mode. |
| `eval_timeout` | `str \| None` | Evaluation timeout. |

---

## `SensorEvaluationContext`

Context passed to a sensor's evaluation function on each tick.

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `sensor_name` | `str` | Name of the sensor being evaluated. |
| `cursor` | `str \| None` | Cursor from the previous tick (for stateful sensors). |
| `last_tick_time` | `float \| None` | Unix timestamp of the last tick. |
| `config` | `ConfigT` | Config instance (if the evaluation function uses a config type hint). |
| `log` | `logging.Logger` | Logger named `rivers.sensors.<sensor_name>`. |

---

## `SensorResult`

Structured return type from a sensor evaluation function.

```python
result = rs.SensorResult(
    run_requests=[rs.RunRequest(tags={"batch": "123"})],
    cursor="offset_42",
)
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `run_requests` | `Sequence[RunRequest \| BackfillRequest] \| None` | `None` | Runs / backfills to trigger. |
| `skip_reason` | `str \| SkipReason \| None` | `None` | Reason to skip (mutually exclusive with `run_requests`). |
| `cursor` | `str \| None` | `None` | Cursor to persist for the next tick. |

Providing both `run_requests` and `skip_reason` raises `ValueError`.

---

## `SensorStatus`

Enum controlling whether a sensor is active.

| Value | Description |
|-------|-------------|
| `SensorStatus.Running` | Sensor is active and will be evaluated. |
| `SensorStatus.Stopped` | Sensor is paused. |

---

## `SensorTickResult`

Result of evaluating a sensor tick via `CodeRepository.evaluate_sensor()`.

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `sensor_name` | `str` | Name of the evaluated sensor. |
| `run_requests` | `list[RunRequest \| BackfillRequest]` | Run/backfill requests generated. |
| `skip_reason` | `SkipReason \| None` | Skip reason if the tick was skipped. |
| `cursor` | `str \| None` | Updated cursor value. |

---

## Registration and evaluation

```python
@rs.Sensor(job_name="my_job")
def my_sensor(context: rs.SensorEvaluationContext):
    return rs.SensorResult(
        run_requests=[rs.RunRequest()],
        cursor="new_cursor",
    )

@rs.Asset
def my_asset() -> int:
    return 42

repo = rs.CodeRepository(assets=[my_asset], sensors=[my_sensor])

# Evaluate with optional cursor from previous tick
result = repo.evaluate_sensor("my_sensor", cursor="old_cursor")
print(result.run_requests)  # [RunRequest(...)]
print(result.cursor)        # "new_cursor"
```

## Cursor-based sensors

Sensors can track state across ticks using cursors. The cursor from the previous tick is passed into the next evaluation:

```python
@rs.Sensor(job_name="process_events")
def event_sensor(context: rs.SensorEvaluationContext):
    last_id = int(context.cursor) if context.cursor else 0
    new_events = fetch_events_after(last_id)
    if new_events:
        return rs.SensorResult(
            run_requests=[rs.RunRequest(tags={"event": e.id}) for e in new_events],
            cursor=str(new_events[-1].id),
        )
    return rs.SkipReason("No new events")
```

## Evaluation return types

The evaluation function can return:

- `SensorResult` — full result with run requests, skip reason, and cursor
- `RunRequest` — shorthand for a single run request
- `SkipReason` — skip with a reason
- `None` — skip silently
