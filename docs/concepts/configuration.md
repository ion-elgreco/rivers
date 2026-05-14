# Configuration

Config gives each asset or task structured, validated settings via Pydantic models.

## BaseModel (static config)

Use `pydantic.BaseModel` when values are known at definition time:

```python
from pydantic import BaseModel
import rivers as rs

class ThresholdConfig(BaseModel):
    min_value: float = 0.0
    max_value: float = 1.0

@rs.Asset
def filtered_data(context: rs.AssetExecutionContext[ThresholdConfig]):
    config = context.config
    return [x for x in range(100) if config.min_value <= x <= config.max_value]
```

## BaseSettings (env-aware config)

Use `pydantic_settings.BaseSettings` when values come from environment variables:

```python
from pydantic_settings import BaseSettings
import rivers as rs

class PipelineConfig(BaseSettings):
    api_key: str        # resolved from API_KEY env var
    batch_size: int = 100

@rs.Asset
def api_data(context: rs.AssetExecutionContext[PipelineConfig]):
    config = context.config
    return fetch_data(config.api_key, batch_size=config.batch_size)
```

## Materialize-time overrides

Override config values when calling `materialize()`:

```python
repo.materialize(
    selection=["filtered_data"],
    config={"filtered_data": {"min_value": 10, "max_value": 50}},
)
```

Overrides are merged with defaults at instantiation time. For `BaseSettings`, env vars are resolved first, then overrides take precedence.

## Tasks

Tasks support config the same way as assets:

```python
@rs.Task
def my_task(context: rs.TaskExecutionContext[ThresholdConfig]):
    config = context.config
    ...
```

## Schedules and sensors

`Schedule` and `Sensor` evaluation functions also accept a generic config type. The same `BaseModel` / `BaseSettings` pattern works on `ScheduleEvaluationContext[ConfigT]` and `SensorEvaluationContext[ConfigT]`:

```python
class CronConfig(BaseModel):
    selection: list[str] = ["nightly_etl"]

@rs.Schedule(cron_schedule="0 2 * * *", job_name="nightly")
def nightly(context: rs.ScheduleEvaluationContext[CronConfig]):
    return rs.RunRequest(tags={"selection": ",".join(context.config.selection)})
```

```python
class SensorConfig(BaseSettings):
    inbox_url: str        # resolved from INBOX_URL env var
    poll_batch_size: int = 50

@rs.Sensor(job_name="ingest", minimum_interval="30s")
def inbox_sensor(context: rs.SensorEvaluationContext[SensorConfig]):
    config = context.config
    new_files = list_new_files(config.inbox_url, limit=config.poll_batch_size)
    if not new_files:
        return rs.SkipReason("inbox empty")
    return rs.SensorResult(
        run_requests=[rs.RunRequest(tags={"file": f}) for f in new_files],
        cursor=new_files[-1],
    )
```

## Typed context generics

`AssetExecutionContext[ConfigT]` and `TaskExecutionContext[ConfigT]` accept a generic type parameter that serves two purposes: it gives IDE auto-completion on `context.config`, and it tells rivers which config class to instantiate at runtime. The config type is derived from the annotation on the `context` parameter — no separate `config=` argument is needed.

## When to use which

| | `BaseModel` | `BaseSettings` |
|---|---|---|
| **Values from** | Explicit overrides + defaults only | Env vars, `.env` files, secrets, overrides, defaults |
| **Use when** | Config is static/known at definition time | Config varies by environment (dev/staging/prod) |
| **Dependencies** | `pydantic` (bundled with rivers) | `pydantic-settings` (bundled with rivers) |
