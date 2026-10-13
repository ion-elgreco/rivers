# dbt

`rivers.integrations.dbt` makes the models, seeds, and snapshots of a [dbt](https://www.getdbt.com) project into rivers assets, and runs dbt to build them:

- `DbtProject` reads the project's manifest and defines the assets.
- `DbtResource` runs dbt commands and reports each output that dbt builds.
- `DbtNode` is one model, seed, snapshot, or source of the manifest. The functions that change the outputs get it.

## Setup

```bash
pip install rivers[dbt] dbt-duckdb   # dbt-core, and the adapter for your warehouse
```

rivers supports dbt-core 1.7.10 and later 1.x releases.

## Define the assets

```python
from pathlib import Path

import rivers as rs
from rivers.integrations.dbt import DbtProject, DbtResource

project = DbtProject(Path(__file__).parent / "analytics")

@project.assets()
def analytics(context: rs.AssetExecutionContext, dbt: DbtResource):
    yield from dbt.cli(["build"], context)

repo = rs.CodeRepository(
    assets=[analytics],
    resources={"dbt": DbtResource(project_dir=project)},
)
```

`project.assets()` returns a decorator that builds one [multi-asset](../concepts/assets.md#multi-assets). Each model, seed, and snapshot is an output. By default, [`node.output_def()`](#change-the-outputs) builds it:

| Setting | Value |
|---------|-------|
| Name | The resource name, e.g. `orders`. Version 2 of a versioned model is `orders_v2`. |
| Code version | The checksum dbt records for the resource's file. When the file changes, the asset is stale. |
| Group | The resource's dbt group. |
| Tags | The resource's dbt tags. |
| Kinds | `dbt`. |
| Metadata | `dbt/unique_id`, `dbt/relation`, `dbt/materialized`, and `dbt/description`. |

rivers then adds to each output:

- Deps: the resources that it refs, as [lineage-only deps](../api-reference/assets.md#assetdefdep). An ephemeral model builds no table, so the deps go through it to the resources it reads.
- With dbt-duckdb, the `duckdb/schema` and `duckdb/table` metadata.

Tests, sources, and ephemeral models are not assets.

Other keyword arguments go to `rs.Asset.from_multi`, e.g. `name`, `group` (for resources without a dbt group), `partitions_def`, `hooks`, `retry`, or `automation_condition`.

### Write the manifest

`DbtProject` reads `manifest.json` from the target directory. Write it before the code location loads, e.g. when you build its image:

```bash
dbt parse --project-dir analytics --profiles-dir analytics
```

`project.prepare()` does the same: it runs `dbt parse` and replaces the manifest in one step. During development, call it in the module, so that a reload picks up your dbt changes:

```python
project = DbtProject(Path(__file__).parent / "analytics")
project.prepare()
```

`prepare()` uses the profile, like other dbt commands. A [selection](#select-resources) also uses it when the definitions load. When the project uses packages, run `dbt deps` first. Parse with the target that the runs use: the manifest records the database and schema of each table, which go into `dbt/relation`, so use a placeholder only for a secret.

| `DbtProject` field | Default | Description |
|--------------------|---------|-------------|
| `project_dir` | required | The directory with `dbt_project.yml`. |
| `target_path` | `"target"` | The directory with `manifest.json`, relative to `project_dir`. |
| `profiles_dir` | `None` | The directory with `profiles.yml`, for `prepare()`, selections, and a `DbtResource` made from the project. `None` uses dbt's search. |
| `profile` | `None` | The profile to use instead of the project's. |
| `target` | `None` | The target to use instead of the profile's default. |
| `asset_def_fn` | `None` | Builds the output of each model, seed, and snapshot. `None` uses `DbtNode.output_def`. See [Change the outputs](#change-the-outputs). |
| `source_asset_fn` | `None` | Names the rivers asset that loads a dbt source. `None` adds no dep for sources. See [Sources](#sources). |

## Run dbt

The body runs dbt with `dbt.cli(args, context)`, and `yield from` reports each output when dbt finishes it:

- dbt builds the outputs that the run selected. A run of every output uses the definition's selection. A run of some outputs selects each one by its fqn and keeps the definition's `exclude`: `repo.materialize(["customers"])` runs `dbt build --select fqn:analytics.marts.customers`.
- dbt's log goes to the run log.
- When dbt fails, the outputs that it built keep their materializations. The other outputs fail with dbt's error, which names each failed or skipped resource.

The body must take the resource as a parameter, as in the [example](#define-the-assets): a body that makes its own `DbtResource` instead raises `TypeError`.

Each materialization records:

| Key | Type | Description |
|-----|------|-------------|
| `dbt/invocation_id` | text | dbt's id for the command. |
| `dbt/execution_time` | duration | How long dbt took to build the resource. |
| `dbt/message` | text | The adapter's message, e.g. `INSERT 3`. |
| `dbt/rows_affected` | int | Rows the adapter reports, if it does. |
| `dbt/tests` | JSON | The status of each test on the output, e.g. `{"unique_orders_id": "pass"}`. |

### Tests

rivers has no asset checks. In a `dbt build`, dbt tests each resource after it builds it, and skips the resources downstream of a failed test. rivers follows dbt:

- An output with tests is reported when its tests finish. `dbt/tests` lists their status.
- A failed test with severity `error` fails the output that it tests, with the test's message. dbt skips the downstream resources, so they fail too. dbt has already rebuilt the table of the tested output.
- A test with severity `warn` does not fail the output. Its status is `warn`.
- A generic test belongs to the model it is defined on. A singular test belongs to each model that it refs.
- A failed source test or unit test fails the run. Its message is in the error.

`dbt run` runs no tests: each output is reported when dbt builds it.

### DbtResource

| Field | Default | Description |
|-------|---------|-------------|
| `project_dir` | required | The directory with `dbt_project.yml`, or a `DbtProject`. |
| `profiles_dir` | `None` | The directory with `profiles.yml`. `None` uses dbt's search: the project directory, then `~/.dbt`. |
| `profile` | `None` | The profile to use instead of the project's. |
| `target` | `None` | The target to use instead of the profile's default. |
| `executable` | `None` | The dbt executable, e.g. `"dbt"` or a path. `None` runs the dbt-core installed with rivers. |

Give the resource the `DbtProject`, as in the [example](#define-the-assets). It then takes the project's `profiles_dir`, `profile`, and `target`, so dbt runs with the settings that `prepare()` and the selections use. Each field takes the first value that it finds:

1. The value that you give the resource.
2. The project's value, if it is not `None`.
3. The environment variable that dbt reads: `DBT_PROJECT_DIR`, `DBT_PROFILES_DIR`, `DBT_PROFILE`, or `DBT_TARGET`.
4. The default.

Each command runs in a new process with its own target directory, so commands can run at the same time. The directory is removed when the command ends. The command starts from the project's `partial_parse.msgpack`, so dbt parses only the files that changed. For `prepare()`, selections, and the commands in a `project.assets()` body, the file is in the project's `target_path`. Elsewhere, it is in `target` in the project directory, or in `DBT_TARGET_PATH` when you set it. rivers sets `DBT_SEND_ANONYMOUS_USAGE_STATS=false` unless you set it.

rivers reads dbt's JSON debug log. Do not pass `--log-format`, `--log-level`, or `--quiet`.

### Other commands

Without a context, `cli` runs the command as given and yields nothing. `wait()` runs it to the end and raises `DbtError` when dbt fails:

```python
@rs.Asset(deps=[rs.AssetDef.dep("orders")])
def export_orders(dbt: DbtResource) -> None:
    dbt.cli(["run-operation", "export_orders"]).wait()
```

Pass `target_path` to keep `run_results.json` and the other artifacts. In a `project.assets()` body, your own `--select`, `--exclude`, or `--selector` replaces the selection of the run.

### Async

The body can be an async generator. `async for` yields the same materializations as `yield from`, and dbt's errors fail the outputs in the same way:

```python
@project.assets()
async def analytics(context: rs.AssetExecutionContext, dbt: DbtResource):
    async for materialization in dbt.cli(["build"], context):
        yield materialization
```

dbt runs in its own process, so the step only waits for dbt. An async body waits on an event loop, so its step does not hold a worker of the executor:

- With the parallel executor, a sync body holds a worker process until dbt ends. An async body leaves the workers to the other steps, and all the async steps of a level wait at the same time. `max_async_concurrent` limits them.
- With the in-process executor, the sync steps of a level run one after the other. Up to four async steps of a level wait at the same time.

A waiting step keeps its [pool](../concepts/concurrency.md#concurrency-pools) slots until dbt ends.

In an `async def` asset, `await dbt.cli([...]).async_wait()` runs a command without blocking the event loop. It raises `DbtError` as `wait()` does:

```python
@rs.Asset(deps=[rs.AssetDef.dep("orders")])
async def export_orders(dbt: DbtResource) -> None:
    await dbt.cli(["run-operation", "export_orders"]).async_wait()
```

When rivers cancels an async step, e.g. on Ctrl-C, dbt stops as it does for a sync body. It gets an interrupt, so that it cancels its queries, and rivers kills it when it still runs 25 seconds later. On Windows, rivers ends dbt at once.

## Select resources

`select`, `exclude`, and `selector` take dbt's [node selection syntax](https://docs.getdbt.com/reference/node-selection/syntax). Use them to split a project into definitions, e.g. to partition the incremental models:

```python
@project.assets(select="config.materialized:incremental", partitions_def=daily, name="incremental")
def incremental(context: rs.AssetExecutionContext, dbt: DbtResource): ...

@project.assets(exclude="config.materialized:incremental", name="analytics")
def analytics(context: rs.AssetExecutionContext, dbt: DbtResource): ...
```

A selection other than the default runs `dbt ls` when the definitions load. `dbt ls` needs what `dbt parse` needs, e.g. `profiles.yml` and each env var that the profile or the project reads. dbt does not connect to the warehouse, so a placeholder for a secret, such as a password, is enough.

rivers caches the result in the target directory. The load runs `dbt ls` again for a new selection or a new manifest: each `dbt parse` or `prepare()` writes a new manifest, also when no file changed. To put the cache in the image, load the definitions once when you build it, after `dbt parse`:

```bash
dbt parse --project-dir analytics --profiles-dir analytics
python -c "import my_module"   # the code location's module
```

The code location then loads without `dbt ls`, so the load does not need the profile.

A dep on a resource outside the selection refers to that resource's output by name. Names are project-wide: the project's `asset_def_fn` builds the output of every model, seed, and snapshot, also of the ones that a definition does not select. So a dep has the same name in every definition. Define every resource that the selected resources ref, e.g. with a second definition as above: a dep on an asset that no definition builds fails the repository's validation.

## Sources

A dbt source adds no dep by default. To make the models that read a source run after the rivers asset that loads it, give the project a `source_asset_fn`. It gets each source that a selected output reads, also through ephemeral models, and returns the name of the asset that loads the source, or `None` for no dep:

```python
project = DbtProject(
    Path(__file__).parent / "analytics",
    source_asset_fn=lambda source: f"{source.props['source_name']}_{source.name}",  # e.g. raw_payments
)
```

The asset must be in the repository.

## Partitions

Give dbt the partition as vars, and filter on them in the model:

```python
import json
from datetime import datetime

daily = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))

@project.assets(select="config.materialized:incremental", partitions_def=daily)
def incremental(context: rs.AssetExecutionContext, dbt: DbtResource):
    start, end = context.partition_time_window
    dbt_vars = {"min_date": start.isoformat(), "max_date": end.isoformat()}
    yield from dbt.cli(["build", "--vars", json.dumps(dbt_vars)], context)
```

```sql
{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='order_date') }}

select * from {{ ref('stg_orders') }}
where order_date >= '{{ var("min_date") }}' and order_date < '{{ var("max_date") }}'
```

With `delete+insert`, a second run of a partition replaces its rows.

## Read the tables downstream

A downstream asset can depend on a model with `rs.AssetDef.dep("customers")` and query the table. Or give the definition an `io_handler` that reads tables. With dbt-duckdb, each output has `duckdb/schema` and `duckdb/table` metadata, so [`DuckDBIOHandler`](duckdb.md) loads it:

```python
import pyarrow as pa
from rivers.integrations.duckdb import DuckDBIOHandler, DuckDBResource

warehouse = DuckDBResource(database="warehouse.duckdb")  # the path in profiles.yml

@project.assets(io_handler=DuckDBIOHandler(resource=warehouse))
def analytics(context: rs.AssetExecutionContext, dbt: DbtResource):
    yield from dbt.cli(["build"], context)

@rs.Asset
def customer_count(customers: pa.Table) -> int:
    return customers.num_rows
```

The handler only reads: dbt writes the tables.

## Change the outputs

The project's `asset_def_fn` builds the output of each model, seed, and snapshot. It takes a `DbtNode` and returns an [`AssetDef`](../api-reference/assets.md). `node.output_def(...)` returns the default output with the fields that you give:

```python
from rivers.integrations.dbt import DbtNode

def asset_def(node: DbtNode) -> rs.AssetDef:
    return node.output_def(
        name=f"analytics_{node.asset_name}",
        group="raw" if node.resource_type == "seed" else "analytics",
    )

project = DbtProject(Path(__file__).parent / "analytics", asset_def_fn=asset_def)
```

| `DbtNode` | Description |
|-----------|-------------|
| `unique_id` | dbt's id, e.g. `model.analytics.orders`. |
| `name` | The resource name, e.g. `orders`. |
| `resource_type` | `model`, `seed`, `snapshot`, or `source`. |
| `props` | The resource's entry in `manifest.json`, e.g. `node.props["config"]`, `node.props["meta"]`, or `node.props["source_name"]`. |
| `asset_name` | The default name: `name`, with `_v<version>` for a versioned model. |

`metadata=` replaces the default metadata. To keep the `dbt/` keys, add to them: `node.output_def(metadata=node.output_def().metadata | {"team": "finance"})`.

rivers adds the [deps and the `duckdb/` keys](#define-the-assets) to the output that the function returns. The deps and the `duckdb/` keys that the function returns stay, e.g. `deps=[rs.AssetDef.dep("crm_export")]`.

The function runs for every model, seed, and snapshot of the project, so a dep on a resource of another definition gets the same name. An output without a name raises `ValueError`, and so do two resources with the same output name.

## Limits

- A run of some outputs lists each one in `--select`. On Windows, the command line can be too long for a run of more than about 1,000 models.
- dbt-core 2, the dbt Fusion engine, is not supported.
- A microbatch model with a `partial success` fails its output.
