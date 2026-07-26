# rivers API reference

Signature-exact surface, taken from the type stubs (`python/rivers/_core/**/__init__.pyi`). Everything below is importable from the top-level `rivers` namespace:

```python
import rivers as rs
```

If a name is not in this file, check the stubs before using it — do not guess.

## Assets

### `@rs.Asset`

Bare or configured. Function parameters that match another asset's name are **loaded inputs**; a `context` parameter receives `AssetExecutionContext`; a parameter matching a registered resource name receives that resource.

```python
@rs.Asset
def users() -> list[dict]:
    return [{"id": 1}]

@rs.Asset(group="analytics", kinds="python", code_version="v2")
def active_users(users: list[dict]) -> list[dict]:
    return [u for u in users if u["id"]]
```

Full kwarg list (all keyword-only, all optional):

| kwarg | type | notes |
|---|---|---|
| `name` | `str` | defaults to the function name |
| `tags` | `list[str]` | |
| `kinds` | `str \| list[str]` | |
| `group` | `str` | Dagster's `group_name` |
| `code_version` | `str` | drives staleness |
| `io_handler` | `IOHandler \| str` | instance, or name registered in `resources=` |
| `metadata` | `dict[str, str]` | static, string values only |
| `partitions_def` | `PartitionsDefinition \| str` | str = name registered in `partition_defs=` |
| `deps` | `list[DepDef]` | lineage-only edges; see below |
| `hooks` | `list[Hook]` | |
| `automation_condition` | `AutomationCondition` | |
| `backfill_strategy` | `BackfillStrategy` | |
| `pool` | `str \| list[str]` | concurrency pool(s) |
| `pool_slots` | `int \| dict[str, int]` | slots claimed per pool |
| `retry` | `RetryPolicy \| str` | str = name registered in `retries=` |
| `compute` | `Compute` | per-asset CPU/memory/GPU |

There is **no `description` kwarg** — use the docstring or `metadata`.

### Dependency edges

Two kinds:

```python
# Loaded input — value is passed to the function (this is the default, via params)
@rs.Asset
def report(active_users: list[dict]): ...

# Lineage-only — establishes ordering, injects nothing
@rs.Asset(deps=[rs.AssetDef.dep("raw_dump")])
def report2(): ...

# Loaded input declared explicitly (e.g. to attach a partition mapping)
rs.AssetDef.input("events", partition_mapping=rs.PartitionMapping.time_window(offset=-1))
```

`rs.AssetDef.input(name, partition_mapping=None, io_handler=None, metadata=None) -> DepDef`
`rs.AssetDef.dep(name, partition_mapping=None) -> DepDef`

### Multi-assets

```python
@rs.Asset.from_multi(
    output_defs=[rs.AssetDef("customers"), rs.AssetDef("orders")],
)
def ingest():
    yield rs.Output([{"id": 1}], output_name="customers")
    yield rs.Output([{"id": 9}], output_name="orders")
```

`rs.AssetDef(name, tags=, kinds=, group=, code_version=, io_handler=, metadata=, partitions_def=, partition_mapping=, pool=, pool_slots=, deps=)`.

`compute` and `retry` live on `from_multi` itself, not per output — a multi-asset is one step.

### Graph assets

Compose `@rs.Task`s into one asset. **Every task used in a graph must also be registered on the repository via `tasks=[...]`** — registering only the graph asset fails validation with `unresolved input`.

```python
@rs.Task
def download() -> bytes: ...

@rs.Task
def parse(raw: bytes) -> list[dict]: ...

@rs.Asset.from_graph()
def documents():
    raw = download()
    return parse(raw)

repo = rs.CodeRepository(assets=[documents], tasks=[download, parse])
```

Internal tasks are namespaced `{graph_name}/{task_name}` and run as independent steps. The returned invocation is the graph's output.

Task inputs bind two ways: parameters passed positionally or by keyword in the graph body take that value; **any parameter left unbound resolves by name** against the outer graph (assets, other tasks, resources). So `def process(value, config_data)` called as `process(x)` gets `value` from the composition and `config_data` from an asset of that name.

`from_graph` kwargs: `name`, `tags`, `kinds`, `group`, `code_version`, `io_handler`, `node_io_handler`, `metadata`, `partitions_def`, `deps`, `hooks`, `automation_condition`, `retry`.

### External assets

Materialized outside rivers; the decorated function is an optional observe callable. **`io_handler` is required.**

```python
@rs.Asset.external(io_handler=handler, partitions_def=daily)
def vendor_feed() -> rs.Observation:
    return rs.Observation(metadata={"rows": 1_000}, data_version="abc123")
```

### `AssetExecutionContext`

Declared via a `context` parameter. Generic over the config type: `rs.AssetExecutionContext[MyConfig]`.

| member | notes |
|---|---|
| `.asset_name`, `.tags`, `.kinds`, `.group`, `.code_version`, `.asset_metadata` | static identity |
| `.log` | `logging.Logger`, records ship to the run event log |
| `.config` | resolved config instance (typed by the generic parameter) |
| `.has_partition_key` / `.partition_key` | `partition_key` raises when unpartitioned |
| `.partition_time_window` | `tuple[datetime, datetime] | None` |
| `.partition` | `PartitionContext` — `.keys`, `.key`, `.definition`, `.time_window()` |
| `.add_output_metadata({...})` | values: primitives or `MetadataValue` |
| `.register_data_version(str)` | |
| `.is_multi_asset`, `.output_selection` | multi-asset only |
| `.mark_partition_failed(key, error)` | single-run backfills: fail one partition, continue |

### Return types

| return | effect |
|---|---|
| a plain value | passed to the IO handler's `handle_output` |
| `rs.Output(value, output_name=, metadata=, data_version=, tags=)` | value + metadata; `output_name` selects the output in multi-assets |
| `rs.Materialization(metadata=, data_version=, tags=, output_name=)` | records the event, **skips the IO handler** — for assets that persist themselves. Downstream assets cannot load it; treat as terminal |
| `rs.Observation(metadata=, data_version=, output_name=)` | external-asset observe result |
| `list[rs.DynamicOutput(key, value)]` | dynamic fan-out with explicit mapping keys |

## Tasks

`@rs.Task` wraps a callable for use inside `Asset.from_graph`. Tasks are not assets — they produce no materialization of their own.

```python
@rs.Task(name="fetch", retry="flaky")
def fetch(url: str) -> bytes: ...
```

Kwargs: `wraps`, `name`, `tags`, `partitions_def`, `partition_mapping`, `io_handler`, `retry`.

`rs.BashTask(name, command, env=None, cwd=None, tags=None, partition_mapping=None, io_handler=None, retry=None)` — output is the command's stdout as `str`. `command` is a shell string or an argv list.

`rs.TaskExecutionContext[ConfigT]`: `.task_name`, `.tags`, `.partition`, `.has_partition_key`, `.partition_key`, `.partition_time_window`, `.log`, `.config`.

Inside a graph, `InvokedNodeOutput.map(task, max_concurrency=None)` fans out; `.collect()` / `.collect_stream(ordered=False)` gather. A collected result must be consumed by a downstream task — it cannot be the graph's return value directly:

```python
@rs.Asset.from_graph()
def processed():
    mapped = produce_items().map(process)
    return summarize(mapped.collect())
```

## Partitions

### Definitions

```python
from datetime import datetime

rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1), end=None, fmt=None)
rs.PartitionsDefinition.hourly(start=datetime(2024, 1, 1), end=None, fmt=None)
rs.PartitionsDefinition.time_window(start, cron_schedule=None, interval_seconds=None, end=None, fmt=None)
rs.PartitionsDefinition.static_(["us", "eu", "apac"])        # note the trailing underscore
rs.PartitionsDefinition.multi({"date": daily, "region": regions})
rs.PartitionsDefinition.dynamic("regions")                    # keys added at runtime
```

`start`/`end` are **`datetime` objects**, not strings. `time_window` takes exactly one of `cron_schedule` or `interval_seconds`. Weekly/monthly grids come from `time_window(cron_schedule="0 0 * * 0")` — there are no `weekly()`/`monthly()` helpers.

Methods: `.get_partition_keys() -> list[PartitionKey]`, `.validate_partition_key(key) -> bool`.

### Keys

```python
rs.PartitionKey.single("2024-01-15")
rs.PartitionKey.multi({"date": "2024-01-15", "region": "eu"})
rs.PartitionKeyRange.single("2024-01-01", "2024-01-31")
rs.PartitionKeyRange.multi({"date": ("2024-01-01", "2024-01-31"), "region": ["eu", "us"]})
```

Multi-dimensional keys display as `dim=value|dim=value`. Variants for matching: `PartitionKey.Single` (`.key`), `PartitionKey.Multi` (`.keys`), `PartitionKey.Set` (`.keys`). `.to_json()` / `.from_json()` for transport.

### Mappings

Attach per dependency edge via `AssetDef.input(...)` / `AssetDef.dep(...)`.

| mapping | meaning |
|---|---|
| `PartitionMapping.identity()` | same key both sides (default) |
| `PartitionMapping.time_window(offset)` | shift N windows; negative lags. Out-of-range shifts fail the run |
| `PartitionMapping.all_partitions()` | every downstream key depends on every upstream key |
| `PartitionMapping.static_({"down": "up"})` | fixed key→key map |
| `PartitionMapping.multi({...})` | per-dimension mappings |
| `PartitionMapping.multi_to_single(dimension_name, partition_mapping=None)` | project multi-dim onto one dimension |
| `PartitionMapping.specific_partitions([...])` | always depend on a fixed key set |
| `PartitionMapping.for_keys([...])` | depend on listed keys/ranges |
| `PartitionMapping.subset()` | depend only on upstream keys that exist |

### Backfill strategy

`rs.BackfillStrategy.multi_run()` (one run per key, default), `.single_run()` (all keys in one run), `.per_dimension(multi_run=[...], single_run=[...])`.

## Automation conditions

`rs.AutomationCondition`, attached via `automation_condition=` on an asset. Compose with `&`, `|`, `~`.

**Presets:** `.eager()`, `.on_cron(cron_schedule, timezone=None)`, `.on_missing()`

**Leaves:** `.missing()`, `.in_progress()`, `.execution_failed()`, `.newly_updated()`, `.newly_requested()`, `.code_version_changed()`, `.cron_tick_passed(cron, timezone=None)`, `.in_latest_time_window(lookback_delta=None)`, `.initial_evaluation()`, `.data_version_changed()`, `.backfill_in_progress()`, `.in_flight()`, `.will_be_requested()`, `.last_run_includes_target()`, `.last_executed_with_tags(tag_keys=, tag_values=)`, `.has_run_with_tags(...)`, `.all_runs_have_tags(...)`

**Dep aggregates:** `.any_deps_missing()`, `.any_deps_in_progress()`, `.any_deps_updated()`, `.any_deps_match(cond)`, `.all_deps_match(cond)`, `.all_deps_updated_since_cron(cron, timezone=None)`

**Composition methods:** `.newly_true()`, `.since(reset_cond)`, `.since_last_handled()`, `.replace(old, new)`, `.without(cond_or_description)`, `.with_label(str)`, `.on_selected(keys)`

```python
# eager, but fire even while a dep is in progress
rs.AutomationCondition.eager().without(~rs.AutomationCondition.any_deps_in_progress())

# watch a specific asset
rs.AutomationCondition.newly_updated().on_selected("upstream_feed")
```

`.eager()` expands to `(missing().newly_true() | any_deps_updated()).since_last_handled() & ~any_deps_missing() & ~any_deps_in_progress() & ~in_flight() & ~execution_failed()`.

`in_flight()` covers both runs and backfills — negate it in custom conditions to avoid re-dispatch. The presets already include it.

## Schedules and sensors

```python
@rs.Schedule(cron_schedule="0 2 * * *", job_name="nightly", timezone="Europe/Amsterdam")
def nightly(context: rs.ScheduleEvaluationContext):
    return rs.RunRequest(partition_key="2024-01-15", tags={"team": "data"})
```

`Schedule(func=, *, cron_schedule, job_name, name=None, default_status=, timezone=None, tags=None, description=None, eval_mode=EvalMode.Auto, eval_timeout=None)`. **`cron_schedule` and `job_name` are required** — a schedule always targets a job.

```python
@rs.Sensor(job_name="ingest", minimum_interval="30s")
def inbox(context: rs.SensorEvaluationContext):
    new = list_files(after=context.cursor)
    if not new:
        return rs.SkipReason("nothing new")
    return rs.SensorResult(
        run_requests=[rs.RunRequest(run_key=f, tags={"file": f}) for f in new],
        cursor=new[-1],          # returned, not set imperatively
    )
```

`Sensor(func=, *, name=None, job_name=None, minimum_interval=None, default_status=, description=None, tags=None, asset_selection=None, eval_mode=EvalMode.Auto, eval_timeout=None)`. `minimum_interval` is a **humantime string** (`"30s"`, `"5m"`), not seconds.

- `rs.RunRequest(run_key=None, tags=None, partition_key=None, job_name=None)` — `partition_key` is a `str`
- `rs.BackfillRequest(selection, partition_keys=, partition_range=, strategy=, failure_policy=, max_concurrency=4, tags=)`
- `rs.SensorResult(run_requests=None, skip_reason=None, cursor=None)`
- `rs.SkipReason(message)`
- Context: `.cursor`, `.last_tick_time`, `.sensor_name` / `.scheduled_execution_time`, `.schedule_name`, `.log`, `.config`
- `EvalMode.Auto | InProcess | Subprocess` — `Subprocess` isolates blocking user code
- Statuses: `rs.ScheduleStatus.Running/.Stopped`, `rs.SensorStatus.Running/.Stopped`

## IO handlers

Subclass `rs.BaseIOHandler` (a `pydantic_settings.BaseSettings`, so fields resolve from env vars):

```python
class MyHandler(rs.BaseIOHandler):
    root: str = "/tmp/data"

    def handle_output(self, context: rs.OutputContext, obj) -> None:
        path = f"{self.root}/{context.asset_name}.json"
        ...
        context.add_output_metadata({"path": rs.MetadataValue.path(path)})

    def load_input(self, context: rs.InputContext):
        ...
```

- `OutputContext`: `.asset_name`, `.asset_metadata`, `.partition`, `.type_hint`, `.output_metadata`, `.add_output_metadata()`, `.register_data_version()`
- `InputContext`: `.asset_name`, `.downstream_asset`, `.asset_metadata`, `.partition`, `.type_hint`

Built-ins: `rs.InMemoryIOHandler` (shared default), `rs.PickleIOHandler(store=...)` (any `obstore` store — local, S3, GCS, Azure), `rs.DeltaIOHandler` (needs `pip install rivers[delta]`).

## Resources

Subclass `rs.Resource` (also `BaseSettings`), register by name, inject by parameter name:

```python
class Warehouse(rs.Resource):
    dsn: str                      # from WAREHOUSE_DSN etc. per BaseSettings rules

    def setup(self) -> None: ...      # once at resolve time
    def teardown(self) -> None: ...   # at repository shutdown

@rs.Asset
def rows(warehouse: Warehouse) -> list[dict]:
    return warehouse.query("select 1")

repo = rs.CodeRepository(assets=[rows], resources={"warehouse": Warehouse(dsn="...")})
```

A parameter is a resource if its name matches a `resources=` key; otherwise it is treated as an upstream asset input.

## Config

Config is declared through the **context generic**, not a separate parameter:

```python
from pydantic import BaseModel

class Thresholds(BaseModel):
    minimum: float = 0.0

@rs.Asset
def filtered(context: rs.AssetExecutionContext[Thresholds]):
    return [x for x in range(100) if x >= context.config.minimum]

repo.materialize(selection=["filtered"], config={"filtered": {"minimum": 10}})
```

`BaseModel` for static config; `pydantic_settings.BaseSettings` when values come from the environment. The same pattern works on `TaskExecutionContext[C]`, `ScheduleEvaluationContext[C]`, `SensorEvaluationContext[C]`, `HookContext[C]`.

## Jobs and executors

```python
job = rs.Job(name="nightly", assets=[users, active_users], executor=rs.Executor.in_process())
result = repo.get_job("nightly").execute(partition_key=None, tags=None, config=None, raise_on_error=True)
```

`rs.Job(name, assets, executor=None, allow_incomplete_deps=False, retry=None)` — `assets` may include tasks.

| executor | kwargs |
|---|---|
| `rs.Executor.in_process()` | — |
| `rs.Executor.parallel(max_workers=None, max_async_concurrent=None)` | loky subprocess pool; the repository default |
| `rs.Executor.kubernetes(worker_image=None, *, max_concurrent_steps=None, namespace=None, service_account="rivers-executor", worker_cpu="500m", worker_memory="512Mi")` | one pod per step |

## Retries and compute

```python
rs.RetryPolicy(
    max_retries=3,
    backoff=rs.Backoff.exponential(1.0, factor=2.0, jitter=0.1, max_delay=60.0),
    retry_on=rs.RetryOn.TRANSIENT,          # or [TimeoutError, rs.FailureReason.OUT_OF_MEMORY]
    escalate=rs.ComputeEscalation(max_memory="16Gi", factor=2.0),
)
```

- `rs.Backoff.constant(delay, jitter=0.0, max_delay=None)`, `.linear(step, initial=0.0, ...)`, `.exponential(initial, factor=2.0, ...)`, `.fixed([1.0, 5.0, 30.0], jitter=0.0)` — jitter is a fraction (0..1) of the computed wait
- `rs.RetryOn.ALL` (default) | `rs.RetryOn.TRANSIENT` (OOM/timeout/infrastructure only), or an explicit list mixing exception types and `FailureReason` members
- `rs.FailureReason.ERROR | OUT_OF_MEMORY | TIMEOUT | INFRASTRUCTURE | CANCELLED` — `CANCELLED` is never retried
- Precedence: asset `retry=` > job `retry=` > `CodeRepository(default_retry_policy=)`
- `rs.Compute(cpu=None, memory=None, gpu=None)` — K8s quantity strings, per asset

## Concurrency

- Per-asset pools: `@rs.Asset(pool="warehouse", pool_slots=2)`, caps via `CodeRepository(pool_limits={"warehouse": 4})`
- Run queue: `rs.RunQueueConfig(max_concurrent_runs=10, tag_concurrency_limits=[...], dequeue_interval="250ms", start_timeout="180s")`
- `rs.TagConcurrencyLimit(key, limit, value=None, per_unique_value=False)`
- Step concurrency: executor `max_workers` / `max_concurrent_steps`

## `CodeRepository`

```python
repo = rs.CodeRepository(
    assets=[...],
    tasks=None,                  # required for tasks used inside graph assets
    jobs=None, schedules=None, sensors=None,
    default_executor=None,
    resources=None,              # {name: Resource | IOHandler}
    partition_defs=None,         # {name: PartitionsDefinition} — referenced by string
    retries=None,                # {name: RetryPolicy} — referenced by string
    default_retry_policy=None,
    run_queue=None,              # RunQueueConfig
    run_backend=None,            # RunBackendConfig.local() | .kubernetes(...)
    pool_limits=None,
)
```

Key methods:

| method | notes |
|---|---|
| `.validate()` | graph/partition/resource validation, no storage, no side effects — the fast gate |
| `.resolve(storage=None)` | validates **and** persists topology; required before materialize/backfill/daemon |
| `.materialize(selection=None, partition_key=None, tags=None, raise_on_error=True, config=None, run_id_override=None, include_upstream=False, resume=False, retry=None)` | synchronous; returns `RunResult` |
| `.backfill(selection=None, partition_keys=None, partition_range=None, strategy=None, failure_policy="continue", max_concurrency=4, tags=None, config=None, block=True, dry_run=False)` | returns `BackfillResult` |
| `.load_node(name, *, partition_key=None, type_hint=None)` | read a materialized value back through its IO handler |
| `.get_job(name)` / `.get_schedule(name)` / `.get_sensor(name)` | lookup |
| `.evaluate_schedule(name, execution_time=None)` / `.evaluate_sensor(name, cursor=None, last_tick_time=None)` | tick once — use in tests |
| `.observe(asset_names=None)` | run external assets' observe functions |
| `.cancel_backfill(id)` / `.get_backfill(id)` / `.rerun_backfill(id, ...)` | backfill control |
| `.shutdown()` | also runs on `__exit__`; `CodeRepository` is a context manager |

`selection=` does **not** include upstream assets unless `include_upstream=True`.

`RunResult`: `.success`, `.run_id`, `.materialized_assets`, `.failed_assets` (list of `(asset, error)`).

## Hooks

```python
@rs.Hook.failure
def alert(context: rs.HookContext):
    page(f"{context.asset_name} failed in {context.run_id}: {context.error}")

@rs.Asset(hooks=[alert])
def critical(): ...
```

`rs.Hook.success` / `rs.Hook.failure`, bare or with `(name=...)`. `HookContext`: `.asset_name`, `.run_id`, `.hook_type`, `.output`, `.error`, `.metadata`, `.config`.

## Metadata

`rs.MetadataValue` constructors — note the trailing underscores where the plain name is a Python builtin:

`.text()`, `.int()`, `.float_()`, `.bool_()`, `.url()`, `.path()`, `.json()`, `.md()`, `.timestamp()`, `.null()`, `.bytes()`, `.duration()`, `.sql(query, dialect=None)`, `.code_block(code, language=None)`, `.image()`, `.percentage()`, `.list_([...])`, `.date_range(start, end)`, `.schema(arrow_schema)`, `.data_version()`

`rs.Schema(arrow_schema)` wraps anything exposing `__arrow_c_schema__` (e.g. `pyarrow.Schema`).

## Exceptions

From `rivers.exceptions`: `AssetDefinitionError`, `AssetNotFoundError`, `AssetOutputValidationError`, `ConfigurationError`, `ExecutionError`, `GraphValidationError`, `InvalidMetadataError`, `NodeNotFoundError`, `PartitionDefinitionError`, `PartitionValidationError`, `ResultDefinitionError`, `ScheduleDefinitionError`, `SensorDefinitionError`, `StorageError`, `TaskDefinitionError`.

## CLI

```bash
rivers dev my_pipeline [--host 127.0.0.1] [--port 3000] [--grpc-port 3001] [--storage-path .rivers/storage/] [--no-daemon]
rivers serve my_pipeline --surreal-endpoint $RIVERS_SURREAL_ENDPOINT   # code-location pod
rivers materialize my_pipeline [--partition-key 2024-01-15] [--memory]
rivers backfill my_pipeline --assets daily_events --from 2024-01-01 --to 2024-01-31 --strategy multi_run
rivers db migrate my_pipeline
```

The module argument is a Python module path; `--repo-var` (default `repo`) names the `CodeRepository` instance. Flags also come from `RIVERS_<GROUP>_<KEY>` env vars, `rivers.toml`, or `[tool.rivers.*]` in `pyproject.toml`.
