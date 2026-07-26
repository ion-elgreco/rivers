# Prefect → rivers

Prefect and rivers do not share a model. Prefect orchestrates **work**: flows call tasks, control flow is ordinary Python, and the graph exists only as a side effect of what ran. rivers orchestrates **data**: assets are declared artifacts wired by dependency, and the graph is resolved before anything executes.

So this is a port with design decisions in it, not a transliteration. Expect to make judgment calls, and surface them to the user rather than deciding silently.

Verify Prefect-side specifics against [docs.prefect.io](https://docs.prefect.io) as you go — its surface moves faster than this file. The rivers side is pinned by `rivers-api.md`.

## The core shift

| Prefect | rivers |
|---|---|
| `@task` = a unit of work | `@rs.Asset` = a unit of **data** |
| Graph emerges from what the flow called | Graph is declared and validated up front |
| `.submit()` / `.map()` request concurrency | Executor derives concurrency from the DAG |
| Results are a side effect (`persist_result`) | Every asset materializes through an IO handler |
| Caching by input hash (`cache_policy`) | Materialization state + `code_version` / `data_version` |
| Deployment = a flow + infrastructure | Code location = a `CodeRepository` module |

The porting question for each Prefect task is **"what artifact does this produce?"** If the answer is "a table, a file, a model" it becomes an asset. If the answer is "a step in producing one artifact" it becomes an `rs.Task` inside a graph asset. If the answer is "nothing, it sends a Slack message" it becomes a hook or a terminal asset returning `rs.Materialization`.

## Choosing a shape for each flow

| The flow looks like | Port it as |
|---|---|
| Tasks each produce a persisted, separately-useful artifact | One `@rs.Asset` per artifact; drop the flow wrapper |
| A linear procedure producing one artifact | One `@rs.Asset.from_graph` with `@rs.Task` steps |
| Fan-out over a runtime-computed list | Graph asset using `InvokedNodeOutput.map(task).collect()`, or a partitioned asset if the fan-out keys are stable |
| A flow calling subflows | `rs.Job` bundling the resulting assets, or plain asset dependencies |
| Loops/conditionals over runtime data | Usually one asset doing the work internally — the DAG cannot express data-dependent branching |
| A schedule wrapper with no real logic | Delete it; use `rs.Schedule` or an automation condition |

If the flow's structure is genuinely dynamic (branch counts unknown until runtime, recursive fan-out), say so plainly. Forcing it into a static DAG changes behavior.

## Mapping table

| Prefect | rivers |
|---|---|
| `from prefect import flow, task` | `import rivers as rs` |
| `@flow` | `rs.Job` (a named bundle) — or nothing, if the assets stand alone |
| `@task` producing an artifact | `@rs.Asset` |
| `@task` as a step | `@rs.Task` inside `@rs.Asset.from_graph` |
| `from prefect.assets import materialize` / `@materialize("s3://…")` | `@rs.Asset` (closest analog; rivers assets are first-class, not a task variant) |
| `asset_deps=[...]` | `deps=[rs.AssetDef.dep("name")]` |
| `add_asset_metadata(...)` | `context.add_output_metadata({...})` |
| `task.submit()` | *(nothing — parallelism comes from the DAG + executor)* |
| `task.map(items)` | `InvokedNodeOutput.map(task).collect()` inside a graph asset, or partitions |
| `wait_for=[other]` | `deps=[rs.AssetDef.dep("other")]` |
| `retries=3, retry_delay_seconds=10` | `retry=rs.RetryPolicy(max_retries=3, backoff=rs.Backoff.constant(10.0))` |
| `timeout_seconds=` | *(no per-asset timeout — use `rs.FailureReason.TIMEOUT` handling at the infra layer)* |
| `cache_policy=INPUTS` / `cache_key_fn` | *(no memoization cache — see "Caching" below)* |
| `persist_result=True`, `result_storage=` | IO handler on the asset (`rs.PickleIOHandler(store=...)`) |
| pydantic model as flow parameter | `context: rs.AssetExecutionContext[MyConfig]` + `config={...}` at launch |
| `prefect.runtime.flow_run.id` | `context` attributes (`.asset_name`, `.partition_key`, …); run id via `HookContext.run_id` |
| `get_run_logger()` | `context.log` |
| `on_completion=` / `on_failure=` hooks | `@rs.Hook.success` / `@rs.Hook.failure` + `hooks=[...]` |
| Blocks (`Secret`, credentials) | `rs.Resource` — a `BaseSettings`, so env vars/`.env` resolve automatically |
| Storage blocks (S3/GCS filesystems) | `obstore` store passed to an IO handler |
| `prefect.variables` | `rs.Resource` fields, or plain env vars |
| **Deployment & scheduling** | |
| `flow.serve()` | `rivers dev my_pipeline` (local) |
| `flow.deploy(work_pool_name=, image=)` | `CodeLocation` CRD on K8s + `rivers serve` |
| `prefect.yaml` deployments | the `CodeRepository` module itself |
| deployment `cron` / `interval` schedule | `rs.Schedule(cron_schedule=..., job_name=...)` |
| Automations with event triggers | `rs.Sensor` (polling) or `rs.AutomationCondition` (declarative) |
| `emit_event(...)` + event trigger | usually an `rs.AutomationCondition` on the downstream asset |
| Work pools / workers | `rs.Executor` + `rs.RunBackendConfig.kubernetes(...)` |
| Work queue concurrency limit | `rs.RunQueueConfig(max_concurrent_runs=...)` |
| Global concurrency limits / `concurrency()` | `pool=` / `pool_slots=` + `CodeRepository(pool_limits={...})` |
| Tag-based concurrency limits | `rs.TagConcurrencyLimit(key, limit, ...)` |
| Transactions (`prefect.transactions`) | *(no equivalent)* |

## Examples

A flow whose tasks each produce an artifact — the flow wrapper disappears:

```python
# Prefect
@task
def extract() -> list[dict]:
    return fetch_rows()

@task
def transform(rows: list[dict]) -> list[dict]:
    return [clean(r) for r in rows]

@flow
def etl():
    rows = extract()
    return transform(rows)

# rivers
@rs.Asset
def raw_rows() -> list[dict]:
    return fetch_rows()

@rs.Asset
def clean_rows(raw_rows: list[dict]) -> list[dict]:
    return [clean(r) for r in raw_rows]

repo = rs.CodeRepository(assets=[raw_rows, clean_rows])
```

The dependency that was a local variable becomes a parameter name. Nothing "calls" the assets — rivers resolves and schedules them.

A linear procedure producing one artifact stays one asset:

```python
# Prefect
@task
def download() -> bytes: ...
@task
def parse(raw: bytes) -> list[dict]: ...

@flow
def documents_flow():
    return parse(download())

# rivers — note that the tasks must be registered too
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

Concurrency stops being explicit:

```python
# Prefect — concurrency is requested
@flow
def fan_out():
    futures = [process.submit(i) for i in range(10)]
    return [f.result() for f in futures]

# rivers — independent assets run concurrently under Executor.parallel();
# within one asset, fan out with map/collect in a graph asset
@rs.Asset.from_graph()
def processed():
    mapped = produce_items().map(process)
    return summarize(mapped.collect())     # a collected result must feed a task
```

Scheduling moves from the deployment to the repository:

```python
# Prefect: prefect.yaml
# deployments:
#   - name: nightly
#     entrypoint: flows/etl.py:etl
#     schedules:
#       - cron: "0 2 * * *"

# rivers
job = rs.Job(name="nightly", assets=[raw_rows, clean_rows])

@rs.Schedule(cron_schedule="0 2 * * *", job_name="nightly")
def nightly(context: rs.ScheduleEvaluationContext):
    return rs.RunRequest()

repo = rs.CodeRepository(assets=[raw_rows, clean_rows], jobs=[job], schedules=[nightly])
```

Often the schedule is unnecessary: if the point is "run this when its inputs change", use `automation_condition=rs.AutomationCondition.eager()` on the downstream asset instead. Point this out to the user — it is the main leverage they gain from the move.

## Caching does not port

Prefect's `cache_policy` / `cache_key_fn` memoizes **task results** by input hash: a re-run with identical inputs skips the body and reuses the cached result. rivers has nothing equivalent. What it has instead:

- **Materialization state** — rivers knows an asset has been materialized for a partition; automation conditions (`.missing()`, `.eager()`) use that to avoid redundant work at the *scheduling* layer, not inside a run
- **`code_version`** — marks downstream work stale when the code changes
- **`data_version`** — `context.register_data_version(...)` lets downstream conditions react to content changes

A Prefect task relying on caching for correctness (idempotency, expensive API calls) needs one of: partitioning so each unit materializes once, an automation condition that skips already-materialized work, or explicit caching inside the asset body. Do not assume the behavior carries over — call it out.

## Gotchas

**1. A `@flow` is not an `rs.Job`.** A Job is a named *selection* of assets, not a callable procedure. Any logic in the flow body — branching, error handling, parameter munging — must land somewhere else (inside an asset, in a graph asset, or in a sensor).

**2. Return values are not passed around.** In Prefect, a task's return value flows to the next call in Python. In rivers it goes through the asset's IO handler and comes back to the downstream asset by parameter name. An asset returning something unpicklable needs an IO handler that can persist it (or must not be split across assets at all).

**3. Graph-asset tasks must be registered separately.** Passing the graph asset to `CodeRepository(assets=[...])` is not enough — every `@rs.Task` it calls must also appear in `tasks=[...]`, or validation fails with `unresolved input '<graph>/<task>'`. Task parameters bound positionally in the graph body behave like ordinary Python calls; only unbound parameters resolve by name against the outer graph.

**4. `.submit()` / futures have no counterpart.** Delete them. Parallelism comes from `rs.Executor.parallel()` plus the DAG's shape. A port that keeps a future-collecting loop inside an asset body is running the work serially in one step.

**5. Task retries move to the asset step.** `retries=3, retry_delay_seconds=10` becomes `retry=rs.RetryPolicy(max_retries=3, backoff=rs.Backoff.constant(10.0))`. Note rivers' `retry_on` can filter by exception type or `FailureReason`, which Prefect expresses with `retry_condition_fn`.

**6. Blocks are not a registry.** Prefect Blocks are stored server-side and loaded by name at runtime (`Secret.load("x")`). `rs.Resource` is a `BaseSettings` resolved from the environment at process start and injected by parameter name. Secrets come from env vars / mounted secrets, not from rivers storage. This changes the deployment story — flag it.

**7. Prefect Cloud features.** `send-email-notification` and `declare-incident` automation actions are Cloud-only; a self-hosted port has no equivalent regardless of orchestrator. rivers' analog for alerting is a failure hook.

**8. rivers assets are not `@materialize` tasks.** If the source project already uses `prefect.assets`, the concepts align better — but Prefect's assets are declared by URI string and attached to tasks, while rivers assets are the primary unit with flat names. The URIs become asset names plus IO handler configuration, not identifiers rivers tracks.
