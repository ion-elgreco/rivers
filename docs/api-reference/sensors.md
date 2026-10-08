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
| `default_status` | `SensorStatus` | `Stopped` | Whether the sensor starts running or stopped. |
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
| `default_status` | `SensorStatus` | `Stopped` | Initial status. |
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
| `monitored_status` | `RunStatus \| None` | Status a run-status sensor watches; `None` for a plain sensor. |
| `monitored_jobs` | `list[str] \| None` | Jobs a run-status sensor watches. |

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

---

## Run-status sensors

A run-status sensor calls its function once for each run of its code location that reaches a given status. Use it to alert on failed runs, or to act when a run succeeds.

```python
import rivers as rs

@rs.Sensor.run_failure(default_status=rs.SensorStatus.Running)
def on_failure(context: rs.RunStatusSensorContext) -> None:
    for failure in context.step_failures:
        context.log.error("%s failed in run %s: %s", failure.asset_name, context.run.run_id, failure.error)

@rs.Sensor.run_status(
    rs.RunStatus.Success,
    monitored_jobs=["nightly"],
    default_status=rs.SensorStatus.Running,
)
def on_nightly_done(context: rs.RunStatusSensorContext) -> None:
    ...

repo = rs.CodeRepository(assets=[...], jobs=[...], sensors=[on_failure, on_nightly_done])
```

`Sensor.run_failure(...)` is `Sensor.run_status(RunStatus.Failure, ...)`, and also works as a bare decorator: `@rs.Sensor.run_failure`.

The daemon evaluates only sensors with `default_status=rs.SensorStatus.Running`.

### `Sensor.run_status`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `status` | `RunStatus` | required | `RunStatus.Success`, `RunStatus.Failure` or `RunStatus.Canceled`. Other statuses raise `SensorDefinitionError`. |
| `name` | `str \| None` | `None` | Sensor name. Defaults to the function name. |
| `monitored_jobs` | `list[str] \| None` | `None` | Only watch runs of these jobs. `None` watches every run of the code location. Unknown jobs fail `resolve()`. |
| `minimum_interval` | `str \| None` | `None` | Minimum interval between ticks (default `"30s"`). |
| `default_status` | `SensorStatus` | `Stopped` | Whether the daemon evaluates the sensor. |
| `description` | `str \| None` | `None` | Description. |
| `tags` | `dict[str, str] \| None` | `None` | Tags. |
| `eval_mode` | `EvalMode` | `EvalMode.Auto` | Run the function in the daemon process or in a subprocess. An async function runs to completion with `asyncio.run`. |
| `eval_timeout` | `str \| None` | `None` | Timeout for one tick (default `"5m"`). |

The function takes a `RunStatusSensorContext` as its first parameter, and resources by parameter name. It must return `None`.

### How runs are picked

- **First tick:** the sensor starts watching at that time. Runs that ended earlier are not reported. The tick is `Skipped` with "Watching for … runs from now on".
- **Each later tick:** the sensor calls the function once per new run, oldest first, for at most 5 runs. Further runs wait for the next tick.
- **Errors:** if the function raises for a run, the tick is `Failed` with `run <run_id>: <error>`, and the sensor moves on. That run is not tried again. If the whole tick times out, the tick fails and its runs are tried again on the next tick.
- **Restarts:** the cursor is stored on the tick, so a restarted daemon reports each run once.
- **Late writes:** each read starts 60 s back, so a run is reported when its record is written within 60 s of the end time it carries.
- **Scope:** only runs of the sensor's own code location. With `monitored_jobs` set, runs without a job, such as `repo.materialize()` runs, are skipped.

A run counts as finished when it gets its end time. The status on that run record decides whether the sensor reports it.

`CodeRepository.evaluate_sensor()` raises `SensorDefinitionError` for a run-status sensor: only the daemon evaluates it.

### `RunStatusSensorContext`

| Property | Type | Description |
|----------|------|-------------|
| `sensor_name` | `str` | Name of the sensor. |
| `run` | `RunRecord` | The run that reached the watched status. |
| `step_failures` | `list[StepFailure]` | Failed steps, oldest first. Empty unless the run failed. |
| `launch_error` | `str \| None` | Why the run could not start, if it failed before any step ran. |
| `config` | `ConfigT` | Config instance, from `RunStatusSensorContext[MyConfig]`. |
| `log` | `logging.Logger` | Logger named after the sensor. |

### `StepFailure`

| Property | Type | Description |
|----------|------|-------------|
| `asset_name` | `str` | Asset (or task) whose step failed. A step skipped because an upstream step failed also appears, with a `Skipped: …` error. |
| `partition_key` | `PartitionKey \| None` | Partition the step ran for, if any; a `PartitionKey.Set` when one run covered several partitions. |
| `error` | `str` | Error message recorded for the failure. |

### `RunStatus`

| Value | Description |
|-------|-------------|
| `RunStatus.Queued` | Waiting in the run queue. |
| `RunStatus.NotStarted` | Dequeued; the executor has not started it yet. |
| `RunStatus.Started` | Running. |
| `RunStatus.Success` | Finished; every step succeeded. |
| `RunStatus.Failure` | Finished; a step failed, or the run failed to launch. |
| `RunStatus.Canceled` | Canceled before or while it ran. |

`RunRecord.status` holds the same names as strings.
