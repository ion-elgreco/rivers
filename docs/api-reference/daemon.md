# AutomationDaemon

The `AutomationDaemon` continuously evaluates schedules and sensors in the background. It is implemented in Rust for performance — only crossing into Python to call evaluation functions.

## Usage

The daemon starts automatically with `rivers dev`:

```bash
rivers dev my_module
```

Disable it with `--no-daemon`:

```bash
rivers dev my_module --no-daemon
```

### Programmatic usage

```python
import rivers as rs
from rivers._core import AutomationDaemon

storage = rs.Storage.embedded(".rivers/storage/")
repo = rs.CodeRepository(assets=[...], schedules=[...], sensors=[...])
repo.resolve(storage=storage)

daemon = AutomationDaemon(
    repo=repo,
    storage=storage,
    max_ticks_retained=100,
    condition_eval_interval="30s",
)
daemon.start()

# ... later
daemon.stop()
```

`AutomationDaemon` is intentionally exposed under `rivers._core` rather than the top-level package — only the `rivers dev` / `rivers serve` CLI commands and tests typically construct it directly.

## Eval Modes

Each sensor and schedule can specify an `eval_mode` that controls how the evaluation function is dispatched:

| Mode | Description |
|------|-------------|
| `EvalMode.Auto` | **(default)** Auto-detects async vs sync. Async functions run automatically on the Python event loop managed from Rust (GIL released during I/O). Sync functions run in-process via a dedicated thread with GIL. |
| `EvalMode.InProcess` | Always run in the daemon process. Async functions run on the Python event loop managed from Rust; sync functions run on a dedicated thread holding the GIL. |
| `EvalMode.Subprocess` | Run in a loky subprocess for true parallelism. Requires `pip install loky`. Config injection and resources are not available in subprocess mode. |

### Example

```python
import rivers as rs

# Auto (default): both sync and async run in-process
@rs.Sensor(job_name="my_job")
def my_sync_sensor(context: rs.SensorEvaluationContext):
    # Runs in-process via dedicated thread with GIL
    return rs.RunRequest()

@rs.Sensor(job_name="my_job")
async def my_async_sensor(context: rs.SensorEvaluationContext):
    # Runs in-process, GIL released during await
    import httpx
    async with httpx.AsyncClient() as client:
        resp = await client.get("https://api.example.com/status")
    if resp.json()["updated"]:
        return rs.RunRequest()
    return rs.SkipReason("no updates")

# Force in-process for a sync function (e.g. needs resources)
@rs.Sensor(
    job_name="my_job",
    default_status=rs.SensorStatus.Running,
    eval_mode=rs.EvalMode.InProcess,
)
def my_resource_sensor(context: rs.SensorEvaluationContext):
    return rs.RunRequest()

# Force subprocess for a CPU-heavy async function
@rs.Sensor(
    job_name="my_job",
    default_status=rs.SensorStatus.Running,
    eval_mode=rs.EvalMode.Subprocess,
)
async def heavy_sensor(context: rs.SensorEvaluationContext):
    # Runs in subprocess via asyncio.run()
    return rs.RunRequest()
```

## How it works

### Schedules

For each schedule with `default_status=ScheduleStatus.Running`:

1. **Rust** parses the cron expression and computes the next tick time
2. **Rust** sleeps until the next tick (with periodic shutdown checks)
3. **Python** evaluates the schedule function → `RunRequest` or `SkipReason`
4. **Rust** persists the tick result to storage
5. **Python** executes any `RunRequest`s via `repo.materialize()`
6. **Rust** prunes old tick records (keeps last 100)

### Sensors

For each sensor with `default_status=SensorStatus.Running`:

1. **Rust** loads the cursor from the last tick in storage
2. **Python** evaluates the sensor function with cursor + last tick time
3. **Rust** persists the tick result (including updated cursor) to storage
4. **Python** executes any `RunRequest`s via `repo.materialize()`
5. **Rust** prunes old tick records (keeps last 100)
6. **Rust** sleeps for `minimum_interval` (default `"30s"`)

### Dispatch strategy

At daemon startup, each eval function is classified:

- **`inspect.iscoroutinefunction`** detects async functions
- Combined with the user's `eval_mode` setting, this determines the dispatch:
  - **Async in-process**: the coroutine runs automatically on the Python event loop managed from Rust. The GIL is released during `await` points, allowing true I/O concurrency.
  - **Sync in-process**: the function runs on a dedicated tokio blocking thread, holding the GIL for the duration of the call.
  - **Subprocess**: the eval function and context data are submitted to a loky process pool. The subprocess reconstructs the context from primitives and calls the function (no resources / config injection available across the boundary).

## API

### `AutomationDaemon`

| Method | Description |
|--------|-------------|
| `AutomationDaemon(repo, storage, *, max_ticks_retained=100, condition_eval_interval="30s")` | Create a daemon. `max_ticks_retained` limits stored tick history per automation. `condition_eval_interval` is the interval between automation condition evaluations (human-readable duration, e.g. `"30s"`, `"1m"`). |
| `start()` | Start evaluation loops for all running schedules and sensors. |
| `stop()` | Signal all loops to stop and wait for cleanup. |

### `EvalMode`

| Value | Description |
|-------|-------------|
| `EvalMode.Auto` | Auto-detect async/sync, both run in-process. |
| `EvalMode.InProcess` | Always run in the daemon process. |
| `EvalMode.Subprocess` | Always run in a loky subprocess. |
