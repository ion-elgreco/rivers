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

## The launch document

A run can depart from the definitions in one document, passed as `config=` to `materialize()`, `run_action()`, `backfill()` and `Job.execute()`, and edited in the UI's launch dialogs. Per asset it holds `config`, values for the asset's config class, and `metadata`, keys added to or replacing the asset's [metadata](assets.md#asset-metadata) for this run; per [resource](resources.md#per-run-overrides), field values the resource is rebuilt with; and under `execution`, the run's [executor](../api-reference/executors.md#per-run-executor):

```python
repo.materialize(
    selection=["filtered_data"],
    config={
        "assets": {
            "filtered_data": {
                "config": {"min_value": 10, "max_value": 50},
                "metadata": {"delta/mode": "overwrite", "rivers/executor": "in_process"},
            }
        },
        "resources": {"db": {"pool_size": 2}},
        "execution": {"executor": "parallel", "max_workers": 4},
    },
)
```

Every part is optional; what is absent stays as defined. `config` values are merged with the class's defaults when the class is instantiated (for `BaseSettings`, env vars are resolved first, then these take precedence). `metadata` values are strings, like the asset's own; the merged metadata is what `context.asset_metadata`, the IO handlers and the engine's `rivers/` keys see for that run, so `rivers/executor` picks the executor for that asset's step. Only assets carry metadata: naming a task under `metadata` is an error. A resource named under `resources` is rebuilt for the run with those values, set up before the first step and torn down after the last; the repository's instance is untouched. `execution.executor` (`in_process` or `parallel`, with `max_workers` and `max_async_concurrent` for parallel) is the run's executor in place of the repository's or the job's; an asset's own `rivers/executor` metadata still wins for its step.

The document must be JSON-serializable (pydantic's encoder is used, so dates, paths and enums are fine). The run record keeps it (`RunRecord.config`), so a rerun replays it and the run page shows it, and every launch path applies it: the run queue, Kubernetes run and step pods, backfill child runs and reruns. A document that names an unknown asset or an unknown section is refused before a run exists. So is a launch from the UI or the gRPC API whose config classes reject it: a value a class refuses, or a `BaseModel` field without a default that the document leaves unset, whether or not it mentions the asset. A `BaseSettings` field left unset is not refused, since the run's environment may resolve it and the launch cannot see that environment; if it does not, the step fails. `materialize()` and the other Python entry points build the classes at the step, so they raise the same error from the run.

## The document in the UI

The Materialize and Execute job dialogs hold a **Config** editor with the launch document. It opens pre-filled with each config field's default, each asset's current metadata and each resource's current values, plus an empty `execution` section to fill:

```json
{
  "assets": {
    "api_data": {
      "config": {
        "batch_size": 100
      },
      "metadata": {
        "delta/mode": "append"
      }
    }
  },
  "execution": {},
  "resources": {
    "db": {
      "pool_size": 10
    }
  }
}
```

Only what differs from these values is sent. An untouched dialog launches the run as defined, a resource is rebuilt only when one of its values changed, and a `BaseSettings` field left at its class default keeps the value the run's environment gives it. A value typed equal to its default is not sent either. A secret is never pre-filled; completion shows every other field's current value.

The editor checks the text against the document's JSON schema as you type. A syntax error, an unknown section, asset or field, a value of the wrong type or outside its bounds, a metadata value that is not a string, or a value that is not one of a `Literal`'s choices is underlined and listed under the editor with its line and column; clicking the line moves the caret there. Any of these disables the submit button: pydantic would otherwise ignore an unknown field silently, and a wrong value would fail the step when the run starts. Once the schema is satisfied, the code location checks the document as a run would, building each config class, so validators and `pattern` or `format` constraints show in the editor a moment after typing. A launch the code location refuses never becomes a run.

A field without a default is never pre-filled, so nothing is sent for it unless you set it. The editor lists such fields as "Required, not set" under their document path, and **Insert missing fields** adds them with an empty value of the right type. For a `BaseModel` field nothing else can set it, so it is also an issue at the object that lacks it and blocks the submit until you set it. For a `BaseSettings` field the environment may resolve it when the run starts, so it only appears in the list.

Typing a key or a value opens a completion list, and `Ctrl+Space` opens it anywhere. It offers the sections, assets and fields the object does not have yet, with their type, default and description — an asset's current metadata keys and values included — and for a value the choices of a `Literal` or `Enum`, `true`/`false`, `null` where the field allows it, and the default. `Enter` or `Tab` accepts, `Escape` closes. `Tab` indents by two spaces (`Escape` then `Tab` leaves the editor), and `{`, `[` and `"` close themselves.

An asset with neither a config class nor metadata launches on one click, as does a job whose assets have neither and that needs no partition; the dialog opens on its own when one has either. Next to such a one-click button, **Materialize…** opens the dialog anyway, for the document's other sections; for a job, **Execute with config…** in the Execute button's menu does.

The run page shows the run's document under **Config**, collapsed, with a **Copy** button. **Re-execute** replays the run with the same document. Its menu holds **Re-execute with config…**, which opens the editor on the run's document over the current defaults; the new run gets the edited document, checked as a launch is, and the original run keeps its own. Over gRPC, `RerunRunRequest.config` does the same: unset reuses the stored document, an empty string runs the definitions as they are.

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
