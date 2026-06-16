# Scheduling & Automation

rivers runs assets in three ways outside of a manual `repo.materialize()` call: **schedules** (time-based), **sensors** (event-based), and **automation conditions** (graph-state-based). All three are evaluated by the rivers daemon, share the same run-launch path, and can coexist on the same repository.

## Three trigger models

| Trigger | Drives runs based on | Defined on | Best for |
|---------|----------------------|------------|----------|
| `Schedule` | A cron expression | A job | "Run job X every day at 2am." |
| `Sensor` | A polling function that returns run requests | A job | "Run job X when a new file appears in this S3 prefix." |
| `AutomationCondition` | The state of an asset and its deps | An asset | "Materialize this asset whenever any of its deps update, but not while it's in progress." |

The three are not mutually exclusive — a repository can ship all of them at once. They differ in *what causes a run to fire*, not in how the run is executed.

## Schedules

A `Schedule` is a cron expression bound to a job. The decorated function — the *evaluation function* — runs on each tick and returns a `RunRequest` (or `SkipReason`, or a list of either).

```python
import rivers as rs

@rs.Schedule(cron_schedule="0 2 * * *", job_name="nightly")
def nightly_etl(context: rs.ScheduleEvaluationContext):
    return rs.RunRequest(
        run_key=context.scheduled_execution_time,   # idempotent across daemon restarts
        tags={"trigger": "scheduled"},
    )
```

A schedule's evaluation function may also yield a `BackfillRequest` to launch a multi-partition backfill instead of a single run, or a `SkipReason("…")` to skip the tick. See the [Schedules API reference](../api-reference/schedules.md) for the full signature.

Cron expressions accept 5 fields (`min hour dom mon dow`) or 6 fields (`sec min hour dom mon dow`); seconds are optional.

## Sensors

A `Sensor` is an evaluation function that the daemon re-runs on a fixed cadence (`minimum_interval`) — typically polling an external system. The function returns run requests when it detects work to do.

```python
@rs.Sensor(job_name="ingest", minimum_interval="30s")
def s3_inbox(context: rs.SensorEvaluationContext):
    last_seen = context.cursor or "1970-01-01"
    new_files = list_s3_files_after(last_seen)
    if not new_files:
        return rs.SkipReason("inbox empty")
    return rs.SensorResult(
        run_requests=[rs.RunRequest(tags={"file": f}) for f in new_files],
        cursor=new_files[-1],   # persisted for the next tick
    )
```

Sensors maintain a **cursor** — an opaque string the daemon persists between ticks. Use it to track "what's the last thing I've already processed"; the next tick receives it back via `context.cursor`.

For idempotency, set `run_key=` on the `RunRequest` — the run-queue de-dupes across runs that share a key.

## Automation conditions

`AutomationCondition` is a declarative DSL that fires on **asset graph state**, not on time or external events. You attach a condition to an asset; the daemon evaluates it on every tick and materializes the asset when the condition is true.

```python
@rs.Asset(automation_condition=rs.AutomationCondition.eager())
def hourly_aggregate(raw_events: list) -> int:
    return len(raw_events)
```

`eager()` is the most common preset — "materialize when any dep updates, skip while a dep is in progress, and don't refire while already in flight (a run *or* an active backfill)." Other presets:

- `on_cron("0 * * * *")` — fire on a cron schedule, but only after every dep has updated since the last cron tick. Won't overlap a run that's still in flight.
- `on_missing()` — fire once when the asset becomes missing, then stop.

You can also build conditions from leaf primitives (`missing()`, `code_version_changed()`, `any_deps_updated()`, etc.) using `&` / `|` / `~`. See the [Automation API reference](../api-reference/automation.md) for the full operator catalog.

Unlike schedules and sensors, automation conditions don't go through `RunRequest`s — the daemon launches matching assets directly via the run coordinator.

## Picking between them

The three triggers overlap. A rough decision rule:

- **You know the time the run should happen** → `Schedule` with a cron.
- **An external system knows when there's work** → `Sensor` polling that system.
- **The graph itself knows when there's work** (deps just updated, code changed, partition is missing) → `AutomationCondition` on the asset.

`AutomationCondition.on_cron()` looks like a schedule but isn't — the difference is who decides the run *should* happen. A `Schedule` always fires on the cron tick; `on_cron()` waits for both the tick *and* every dep to be ready, so it's the right choice for cron-driven downstreams in a partitioned graph.

## Daemon execution

All three triggers are evaluated by the same background process: `AutomationDaemon`. It is started in-process by `rivers dev` and `rivers serve`, and tested directly via the `AutomationDaemon` constructor. The daemon:

1. Loads each schedule, sensor, and automation condition tree from the repository.
2. For schedules, computes the next cron tick; for sensors, polls every `minimum_interval`; for automation conditions, re-evaluates every `condition_eval_interval` (default `30s`).
3. Calls the evaluation function (in-process, in a thread, or in a subprocess — see below).
4. Persists a tick record (run requests, skip reason, cursor, error) to storage.
5. Submits any resulting runs through the run queue.

### Eval modes

`Schedule` and `Sensor` accept an `eval_mode` argument:

| Mode | Behavior |
|------|----------|
| `EvalMode.Auto` (default) | Auto-detected at daemon start: async functions run automatically on the Python event loop managed from Rust; sync functions run on a dedicated thread holding the GIL. |
| `EvalMode.InProcess` | Same as `Auto` today — keep the function in the daemon process. |
| `EvalMode.Subprocess` | Run in a loky subprocess. Resources and config injection are not available. Useful when the eval function needs CPU isolation or is unsafe to run alongside the daemon. |

Automation conditions don't have an eval mode — they are evaluated entirely in Rust (no user code runs unless an asset matches and gets launched).

### Cursor and tick history

The daemon persists every tick to storage:

- `repo.storage.get_ticks(name, limit=...)` returns the recent tick history for a schedule, sensor, or automation condition (capped at `max_ticks_retained`, default 100).
- A sensor's last cursor is read back on the next tick, surviving daemon restarts.

## Disabling triggers

Both `Schedule` and `Sensor` accept `default_status=ScheduleStatus.Stopped` / `SensorStatus.Stopped`. Stopped triggers are registered (their definitions are validated, and the UI shows them) but the daemon doesn't tick them. This is the recommended default during development — `rivers dev` doesn't kick off every job the moment you save the file.

Triggers can also be paused/resumed at runtime via the UI.

## Resource and config injection

Schedule and sensor evaluation functions receive the same resource/config injection as asset functions: any parameter name matching a registered resource is injected, and the typed `ScheduleEvaluationContext[ConfigT]` / `SensorEvaluationContext[ConfigT]` makes a Pydantic config available on `context.config`. See [Configuration](configuration.md) and [Resources](resources.md) for details.

## Programmatic evaluation (for tests)

You don't need to start the daemon to test a schedule or sensor. `CodeRepository` exposes:

```python
result = repo.evaluate_schedule("nightly_etl")
result = repo.evaluate_sensor("s3_inbox", cursor="2024-01-01", last_tick_time=1234.5)
```

Both return a tick result with the run requests / skip reason / cursor your function produced — without dispatching anything. Use these in unit tests to assert your evaluation logic without standing up the daemon.

For automation conditions, use the daemon directly (`AutomationDaemon(repo, storage).start()` in a test fixture), since condition evaluation is owned by the Rust daemon path.
