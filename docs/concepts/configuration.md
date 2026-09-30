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

The run record keeps the overrides (`RunRecord.config`, as long as the values are JSON-serializable), so a rerun replays them and the run page shows them. Every launch path applies them: the run queue, Kubernetes run pods, backfill child runs and reruns.

## Overrides from the UI

The Materialize and Execute job dialogs show a **Config** editor when a selected asset (or the chosen action) takes config. It holds the same JSON object `materialize()` takes, keyed by asset name, and opens pre-filled with each field's default:

```json
{
  "api_data": {
    "batch_size": 100
  }
}
```

The editor checks the text against each config class's JSON schema as you type. A syntax error, an unknown field, a value of the wrong type or outside its bounds, or a value that is not one of a `Literal`'s choices is underlined and listed under the editor with its line and column; clicking the line moves the caret there. Any of these disables the submit button: pydantic would otherwise ignore an unknown field silently, and a wrong value would fail the step when the run starts. Once the schema is satisfied, the config class itself checks the overrides in the code location, the way a run builds it, so its validators and `pattern` or `format` constraints show in the editor a moment after typing. A launch whose values the class rejects is refused before a run exists.

A field without a default — a required field, or a `BaseSettings` field the environment resolves — is never pre-filled, so nothing is sent for it unless you set it. The editor lists such fields as "Required, not set"; **Insert missing fields** adds them with an empty value of the right type. They do not block a submit.

Typing a field name or a value opens a completion list, and `Ctrl+Space` opens it anywhere. It offers the fields the object does not have yet, with their type, default and description, and for a value the choices of a `Literal` or `Enum`, `true`/`false`, `null` where the field allows it, and the default. `Enter` or `Tab` accepts, `Escape` closes. `Tab` indents by two spaces (`Escape` then `Tab` leaves the editor), and `{`, `[` and `"` close themselves.

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
