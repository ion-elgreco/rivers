# Dagster → rivers

rivers uses Dagster's model: assets wired by dependencies, partitions, IO indirection, declarative automation, schedules and sensors. Most Dagster code has a direct rivers counterpart, and function-parameter dependencies work identically.

What differs is spelling and a handful of semantics. The spellings are close enough to be dangerous — read the gotchas at the bottom before porting, and check every symbol against `rivers-api.md`.

Dagster snippets below assume `import dagster as dg`; rivers snippets assume `import rivers as rs`.

## Mapping table

| Dagster | rivers |
|---|---|
| `@dg.asset` | `@rs.Asset` |
| upstream via function parameter | same — parameter name matches the upstream asset |
| `deps=[upstream]` (no value loaded) | `deps=[rs.AssetDef.dep("upstream")]` |
| `group_name=` | `group=` |
| `key_prefix=` | *(no equivalent — names are flat)* |
| `kinds=`, `code_version=`, `tags=` | same names |
| `description=` | docstring, or `metadata=` |
| `owners=` | `metadata={"owners": "..."}` |
| `@dg.multi_asset(specs=[dg.AssetSpec("a")])` | `@rs.Asset.from_multi(output_defs=[rs.AssetDef("a")])` |
| `@dg.graph_asset` + `@dg.op` | `@rs.Asset.from_graph` + `@rs.Task` |
| `dg.SourceAsset` / external `AssetSpec` | `@rs.Asset.external(io_handler=...)` |
| `dg.Definitions(...)` | `rs.CodeRepository(...)` |
| `dg.MaterializeResult(metadata=)` | `rs.Output(value, metadata=)` or `rs.Materialization(metadata=)` |
| `dg.MetadataValue.float/int/…` | `rs.MetadataValue.float_/int/…` (see gotcha 2) |
| `context.log`, `context.partition_key`, `context.partition_time_window` | same |
| `context.add_output_metadata()` | same |
| **Partitions** | |
| `dg.DailyPartitionsDefinition(start_date="2024-01-01")` | `rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))` |
| `dg.HourlyPartitionsDefinition(...)` | `rs.PartitionsDefinition.hourly(...)` |
| `dg.WeeklyPartitionsDefinition` / `Monthly…` | `rs.PartitionsDefinition.time_window(start, cron_schedule="0 0 * * 0")` |
| `dg.StaticPartitionsDefinition([...])` | `rs.PartitionsDefinition.static_([...])` |
| `dg.MultiPartitionsDefinition({...})` | `rs.PartitionsDefinition.multi({...})` |
| `dg.DynamicPartitionsDefinition(name=)` | `rs.PartitionsDefinition.dynamic(name)` |
| `dg.MultiPartitionKey({...})` | `rs.PartitionKey.multi({...})` |
| `dg.IdentityPartitionMapping()` | `rs.PartitionMapping.identity()` |
| `dg.TimeWindowPartitionMapping(start_offset=-1, end_offset=-1)` | `rs.PartitionMapping.time_window(offset=-1)` (see gotcha 6) |
| `dg.AllPartitionMapping()` | `rs.PartitionMapping.all_partitions()` |
| `dg.StaticPartitionMapping({...})` | `rs.PartitionMapping.static_({...})` **inverted** (gotcha 7) |
| `dg.MultiPartitionMapping({...})` | `rs.PartitionMapping.multi({...})` |
| `dg.LastPartitionMapping()` | *(no direct equivalent — use `specific_partitions([...])`)* |
| `backfill_policy=dg.BackfillPolicy.single_run()` | `backfill_strategy=rs.BackfillStrategy.single_run()` |
| **Automation** | |
| `automation_condition=` | same kwarg |
| `AutomationCondition.eager()/.on_cron()/.on_missing()` | same names |
| `.missing()`, `.in_progress()`, `.newly_updated()`, `.any_deps_updated()`, … | same names |
| `.since()`, `.newly_true()`, `.replace()`, `.since_last_handled()` | same names |
| `.allow(...)` / `.ignore(...)` | `.without(...)` / `.on_selected(...)` (gotcha 8) |
| `dg.AutoMaterializePolicy` (legacy) | `rs.AutomationCondition` — port to conditions |
| **Orchestration** | |
| `dg.define_asset_job(name, selection=)` | `rs.Job(name=, assets=[...])` |
| `dg.ScheduleDefinition(cron_schedule=, target=)` | `rs.Schedule(cron_schedule=, job_name=)` (gotcha 4) |
| `execution_timezone=` | `timezone=` |
| `@dg.sensor(job=, minimum_interval_seconds=30)` | `@rs.Sensor(job_name=, minimum_interval="30s")` |
| `context.update_cursor(x)` | return `rs.SensorResult(cursor=x)` (gotcha 3) |
| `dg.RunRequest(run_key=, partition_key=, tags=)` | same (no `run_config=`) |
| `dg.SkipReason("…")` | `rs.SkipReason("…")` |
| `dg.DefaultSensorStatus.RUNNING` | `rs.SensorStatus.Running` |
| **Infrastructure** | |
| `dg.ConfigurableIOManager` | `rs.BaseIOHandler` (same `handle_output`/`load_input`) |
| `io_manager_key="x"` + `resources={"x": …}` | `io_handler=handler_instance` or `io_handler="x"` + `resources={"x": …}` |
| `dg.FilesystemIOManager()` | `rs.PickleIOHandler(store=LocalStore(prefix=...))` |
| `dg.InMemoryIOManager()` | `rs.InMemoryIOHandler()` (the default) |
| `dg.ConfigurableResource` | `rs.Resource` |
| `setup_for_execution` / `teardown_after_execution` | `setup()` / `teardown()` |
| `dg.Config` parameter | `context: rs.AssetExecutionContext[MyConfig]` (gotcha 1) |
| `dg.RetryPolicy(max_retries=, delay=, backoff=, jitter=)` | `rs.RetryPolicy(max_retries=, backoff=rs.Backoff.exponential(...))` (gotcha 5) |
| `retry_policy=` | `retry=` |
| `in_process_executor` / `multiprocess_executor` | `rs.Executor.in_process()` / `rs.Executor.parallel()` |
| `dagster-k8s` `k8s_job_executor` | `rs.Executor.kubernetes()` |
| concurrency pools (`dagster.yaml`) | `pool=` / `pool_slots=` + `CodeRepository(pool_limits={...})` |
| run queue `max_concurrent_runs` | `rs.RunQueueConfig(max_concurrent_runs=…)` |
| run tag limits | `rs.TagConcurrencyLimit(key, limit, …)` |
| `@dg.success_hook` / `@dg.failure_hook` | `@rs.Hook.success` / `@rs.Hook.failure`, attached via `hooks=[...]` |
| `workspace.yaml` | *(none — `rivers dev <module>`, or a `CodeLocation` CRD on K8s)* |
| `dagster dev` | `rivers dev my_pipeline` |

## Assets

```python
# Dagster
@dg.asset(group_name="analytics", code_version="v2")
def active_users(users: list[dict]) -> list[dict]:
    return [u for u in users if u["active"]]

# rivers
@rs.Asset(group="analytics", code_version="v2")
def active_users(users: list[dict]) -> list[dict]:
    return [u for u in users if u["active"]]
```

Lineage-only deps:

```python
# Dagster
@dg.asset(deps=[raw_dump])
def report(): ...

# rivers
@rs.Asset(deps=[rs.AssetDef.dep("raw_dump")])
def report(): ...
```

Multi-assets — Dagster's `specs=` becomes `output_defs=`, and outputs are selected by `output_name` on `rs.Output` rather than `asset_key` on `MaterializeResult`:

```python
# Dagster
@dg.multi_asset(specs=[dg.AssetSpec("customers"), dg.AssetSpec("orders")])
def ingest():
    yield dg.MaterializeResult(asset_key="customers", metadata={"rows": 10})
    yield dg.MaterializeResult(asset_key="orders", metadata={"rows": 20})

# rivers
@rs.Asset.from_multi(output_defs=[rs.AssetDef("customers"), rs.AssetDef("orders")])
def ingest():
    yield rs.Output(customers_data, output_name="customers", metadata={"rows": 10})
    yield rs.Output(orders_data, output_name="orders", metadata={"rows": 20})
```

Note that rivers `Output` carries the **value**; Dagster's `MaterializeResult` does not. If the Dagster asset wrote its own data and returned only metadata, use `rs.Materialization(...)` instead — it records the event and skips the IO handler.

Graph assets — `@dg.op` becomes `@rs.Task`. The bodies port directly; the `tasks=` registration is the part that is easy to miss (gotcha 13):

```python
# Dagster
@dg.op
def download(): ...
@dg.op
def parse(raw): ...

@dg.graph_asset
def documents():
    return parse(download())

defs = dg.Definitions(assets=[documents])

# rivers
@rs.Task
def download(): ...
@rs.Task
def parse(raw): ...

@rs.Asset.from_graph()
def documents():
    raw = download()
    return parse(raw)

repo = rs.CodeRepository(assets=[documents], tasks=[download, parse])
```

## Definitions → CodeRepository

```python
# Dagster
defs = dg.Definitions(
    assets=[users, active_users],
    jobs=[nightly_job],
    schedules=[nightly_schedule],
    sensors=[inbox_sensor],
    resources={"io_manager": MyIOManager(), "warehouse": Warehouse(dsn="...")},
)

# rivers
repo = rs.CodeRepository(
    assets=[users, active_users],
    jobs=[nightly_job],
    schedules=[nightly_schedule],
    sensors=[inbox_sensor],
    resources={"warehouse": Warehouse(dsn="...")},
    default_executor=rs.Executor.parallel(),
)
```

The variable must be named `repo` (or passed as `--repo-var`). rivers has no `defs/` autoloading and no `workspace.yaml` — one module, one `CodeRepository`.

Modern Dagster projects using `dg` scaffolding (`src/<pkg>/defs/` with autoloading) need their definitions collected explicitly into a single `CodeRepository` during the port.

## Resources and config

```python
# Dagster
class Warehouse(dg.ConfigurableResource):
    dsn: str
    def setup_for_execution(self, context): ...

class Thresholds(dg.Config):
    minimum: float = 0.0

@dg.asset
def filtered(context: dg.AssetExecutionContext, config: Thresholds, warehouse: Warehouse):
    return warehouse.query(config.minimum)

# rivers
class Warehouse(rs.Resource):
    dsn: str
    def setup(self) -> None: ...

class Thresholds(BaseModel):          # plain pydantic
    minimum: float = 0.0

@rs.Asset
def filtered(context: rs.AssetExecutionContext[Thresholds], warehouse: Warehouse):
    return warehouse.query(context.config.minimum)
```

Config is not a parameter in rivers — it is the context's generic parameter. See gotcha 1.

## IO managers → IO handlers

Method names and roles are identical; the base class and attachment differ.

```python
# Dagster
class MyIOManager(dg.ConfigurableIOManager):
    root: str
    def handle_output(self, context: dg.OutputContext, obj): ...
    def load_input(self, context: dg.InputContext): ...

@dg.asset(io_manager_key="my_io")
def data(): ...

defs = dg.Definitions(assets=[data], resources={"my_io": MyIOManager(root="/tmp")})

# rivers
class MyIOHandler(rs.BaseIOHandler):
    root: str
    def handle_output(self, context: rs.OutputContext, obj) -> None: ...
    def load_input(self, context: rs.InputContext): ...

handler = MyIOHandler(root="/tmp")

@rs.Asset(io_handler=handler)          # or io_handler="my_io" with resources={"my_io": handler}
def data(): ...
```

## Schedules and sensors

```python
# Dagster
@dg.sensor(job=ingest_job, minimum_interval_seconds=30)
def inbox_sensor(context):
    new = list_files(after=context.cursor)
    if not new:
        return dg.SkipReason("nothing new")
    context.update_cursor(new[-1])
    return [dg.RunRequest(run_key=f) for f in new]

# rivers
@rs.Sensor(job_name="ingest", minimum_interval="30s")
def inbox_sensor(context: rs.SensorEvaluationContext):
    new = list_files(after=context.cursor)
    if not new:
        return rs.SkipReason("nothing new")
    return rs.SensorResult(
        run_requests=[rs.RunRequest(run_key=f) for f in new],
        cursor=new[-1],
    )
```

## Gotchas

Each of these produces code that looks correct and is not.

**1. Config is a context generic, not a parameter.** Dagster's `config: MyConfig` parameter has no rivers equivalent — rivers would treat a parameter named `config` as an upstream asset or resource and fail resolution. Declare it as `context: rs.AssetExecutionContext[MyConfig]` and read `context.config`. rivers uses plain `pydantic.BaseModel` / `BaseSettings`; there is no `dg.Config` base class.

**2. Trailing underscores on builtin-shadowing names.** `PartitionsDefinition.static_`, `PartitionMapping.static_`, `MetadataValue.float_`, `MetadataValue.bool_`, `MetadataValue.list_`. But `MetadataValue.int` and `.bytes` have **no** underscore, and markdown is `.md()`. Check the list in `rivers-api.md` rather than pattern-matching.

**3. Sensor cursors are returned, not set.** `context.update_cursor(x)` does not exist. Return the cursor on `rs.SensorResult(cursor=x)`. A port that keeps the imperative call raises `AttributeError`; a port that drops the cursor silently re-processes every item on every tick — check for this specifically.

**4. Schedules require a job.** `rs.Schedule` takes a mandatory `job_name`; there is no asset-targeting form like Dagster's `target=[asset1, asset2]`. Create an `rs.Job` wrapping those assets and point the schedule at it by name.

**5. Retry backoff is an object, not enums.** Dagster's `delay=`/`backoff=Backoff.EXPONENTIAL`/`jitter=Jitter.PLUS_MINUS` collapse into one `rs.Backoff` factory: `rs.Backoff.exponential(initial, factor=2.0, jitter=0.1, max_delay=60.0)`. Jitter is a **fraction of the computed wait** (0..1), not an enum. The kwarg is `retry=`, not `retry_policy=`.

**6. Time-window mappings take one offset, not two.** Dagster's `TimeWindowPartitionMapping(start_offset=-2, end_offset=0)` describes a *window* of upstream partitions (here: three days). rivers' `PartitionMapping.time_window(offset)` shifts by N windows, 1:1. A multi-window dependency has no direct translation — use `PartitionMapping.for_keys([...])` or reshape the asset, and flag it for the user. Do not silently port `start_offset=-2` as `offset=-2`; that changes what the asset reads.

**7. `StaticPartitionMapping` direction is inverted.** Dagster keys the dict by **upstream** partition key (`downstream_partition_keys_by_upstream_partition_key`); rivers' `PartitionMapping.static_({...})` maps **downstream → upstream**. Invert the dict when porting.

**8. `.allow()` / `.ignore()` do not exist.** rivers' equivalents are `.without(condition)` (remove an operand from an `And` — pass `~cond` to drop a negated guard) and `.on_selected(keys)` (evaluate against named assets). rivers also adds conditions Dagster lacks: `.in_flight()`, `.data_version_changed()`, `.backfill_in_progress()`, `.last_executed_with_tags(...)`.

**9. Partition start dates are `datetime` objects.** `daily(start=datetime(2024, 1, 1))`, not `start_date="2024-01-01"`. Passing a string raises. There is no `timezone=` kwarg on the partition definition — cron-gridded `time_window(cron_schedule=...)` handles non-UTC grids.

**10. `materialize(selection=[...])` excludes upstreams.** Pass `include_upstream=True` to match the behavior of Dagster's "materialize with upstream" flows.

**11. Asset names are flat.** No `key_prefix`, no `AssetKey` tuples, no slash-separated paths. Fold any prefix into the name (`analytics/users` → `analytics_users`) and keep it consistent across every dep reference.

**12. Multi-dimensional partition keys display as `dim=value|dim=value`.** When constructing keys for run requests or backfills, build them with `rs.PartitionKey.multi({...})` — never format the string yourself and never `str()`/`repr()` a key into a request.

**13. Graph-asset tasks must be registered separately.** `dg.Definitions(assets=[documents])` carries a graph asset's ops with it; `rs.CodeRepository(assets=[documents])` does not. Every `@rs.Task` used inside a graph asset must also be passed as `tasks=[...]`, or validation fails with `unresolved input '<graph>/<task>'`. Task parameters bound positionally in the graph body work exactly like Dagster's ops; only parameters left unbound resolve by name against the outer graph.

## No rivers equivalent

Report these to the user rather than approximating:

- **Asset checks** (`@dg.asset_check`, `AssetCheckResult`) — no rivers counterpart. Options: fold the assertion into the asset body (fails the materialization), or drop it and note the lost signal.
- **Integrations** — `dagster-dbt`, `dagster-dlt`, `dagster-aws`, and the rest of the integration library have no rivers ports. dbt models must be driven by a `BashTask` or a custom asset.
- **Dagster+ features** — Insights, alerts, branch deployments, the Dagster+ agent.
- **`RunConfig` at launch** — rivers takes per-asset config dicts (`config={"asset": {...}}`), not a structured `RunConfig` object.
- **`@dg.run_status_sensor` / `@dg.asset_sensor`** — write an ordinary `rs.Sensor` that reads run/materialization state from `repo.storage`, or flag it as unported.
- **`dg.DynamicOut` / `DynamicOutput` in ops** — rivers has `rs.DynamicOutput` for fan-out inside graph assets, but the mapping/collect API differs (`InvokedNodeOutput.map(...).collect()`); re-derive rather than transliterate.
