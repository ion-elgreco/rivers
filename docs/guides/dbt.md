# dbt

`rivers.integrations.dbt` makes the models, seeds, and snapshots of a [dbt](https://www.getdbt.com) project into rivers assets, and runs dbt to build them:

- `DbtProject` reads the project's manifest and defines the assets.
- `DbtResource` runs dbt commands and reports each output that dbt builds.
- `DbtTranslator` sets the asset names, groups, metadata, and deps.

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

`project.assets()` returns a decorator that builds one [multi-asset](../concepts/assets.md#multi-assets). Each model, seed, and snapshot is an output:

| Setting | Value |
|---------|-------|
| Name | The resource name, e.g. `orders`. Version 2 of a versioned model is `orders_v2`. |
| Deps | The resources it refs, as [lineage-only deps](../api-reference/assets.md#assetdefdep). An ephemeral model builds no table, so the deps go through it to the resources it reads. |
| Code version | The checksum dbt records for the resource's file. When the file changes, the asset is stale. |
| Group | The resource's dbt group. |
| Tags | The resource's dbt tags. |
| Kinds | `dbt`. |
| Metadata | `dbt/unique_id`, `dbt/relation`, `dbt/materialized`, and `dbt/description`. With dbt-duckdb, also `duckdb/schema` and `duckdb/table`. |

Tests, sources, and ephemeral models are not assets.

Other keyword arguments go to `rs.Asset.from_multi`, e.g. `name`, `group` (for resources without a dbt group), `partitions_def`, `hooks`, `retry`, or `automation_condition`.

### Write the manifest

`DbtProject` reads `manifest.json` from the target directory. Write it before the code location loads, e.g. when you build its image:

```bash
dbt parse --project-dir analytics
```

`project.prepare()` does the same: it runs `dbt parse` and replaces the manifest in one step. During development, call it in the module, so that a reload picks up your dbt changes:

```python
project = DbtProject(Path(__file__).parent / "analytics")
project.prepare()
```

`prepare()` uses the profile, like other dbt commands. When the project uses packages, run `dbt deps` first.

| `DbtProject` field | Default | Description |
|--------------------|---------|-------------|
| `project_dir` | required | The directory with `dbt_project.yml`. |
| `target_path` | `"target"` | The directory with `manifest.json`, relative to `project_dir`. |
| `profiles_dir` | `None` | The directory with `profiles.yml`, for `prepare()`, selections, and a `DbtResource` made from the project. `None` uses dbt's search. |
| `profile` | `None` | The profile to use instead of the project's. |
| `target` | `None` | The target to use instead of the profile's default. |

## Run dbt

The body runs dbt with `dbt.cli(args, context)`, and `yield from` reports each output when dbt finishes it:

- dbt builds the outputs that the run selected. A run of every output uses the definition's selection. A run of some outputs selects each one by its fqn and keeps the definition's `exclude`: `repo.materialize(["customers"])` runs `dbt build --select fqn:analytics.marts.customers`.
- dbt's log goes to the run log.
- When dbt fails, the outputs that it built keep their materializations. The other outputs fail with dbt's error, which names each failed or skipped resource.

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

Each command runs in a new process with its own target directory, so commands can run at the same time. The command starts from the project's `partial_parse.msgpack`, so dbt parses only the files that changed. The directory is removed when the command ends. rivers sets `DBT_SEND_ANONYMOUS_USAGE_STATS=false` unless you set it.

rivers reads dbt's JSON debug log. Do not pass `--log-format`, `--log-level`, or `--quiet`.

### Other commands

Without a context, `cli` runs the command as given and yields nothing. `wait()` runs it to the end and raises `DbtError` when dbt fails:

```python
@rs.Asset(deps=[rs.AssetDef.dep("orders")])
def export_orders(dbt: DbtResource) -> None:
    dbt.cli(["run-operation", "export_orders"]).wait()
```

Pass `target_path` to keep `run_results.json` and the other artifacts. In a `project.assets()` body, your own `--select`, `--exclude`, or `--selector` replaces the selection of the run.

## Select resources

`select`, `exclude`, and `selector` take dbt's [node selection syntax](https://docs.getdbt.com/reference/node-selection/syntax). Use them to split a project into definitions, e.g. to partition the incremental models:

```python
@project.assets(select="config.materialized:incremental", partitions_def=daily, name="incremental")
def incremental(context: rs.AssetExecutionContext, dbt: DbtResource): ...

@project.assets(exclude="config.materialized:incremental", name="analytics")
def analytics(context: rs.AssetExecutionContext, dbt: DbtResource): ...
```

A selection other than the default runs `dbt ls` when the definitions load. The result is cached in the target directory, for that manifest.

A dep on a resource outside the selection refers to that resource's asset by name. Define every resource that the selected resources ref, e.g. with a second definition as above, or drop the dep with [`upstream_asset`](#translator).

## Sources

A dbt source adds no dep by default. To make the models that read a source run after the rivers asset that loads it, map the source to that asset:

```python
from rivers.integrations.dbt import DbtTranslator

class Translator(DbtTranslator):
    def upstream_asset(self, node):
        if node["resource_type"] == "source":
            return f"{node['source_name']}_{node['name']}"  # e.g. raw_payments
        return super().upstream_asset(node)

@project.assets(translator=Translator())
def analytics(context: rs.AssetExecutionContext, dbt: DbtResource): ...
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

## Translator

Subclass `DbtTranslator` and override a method to change one setting. Each method gets the resource's entry in `manifest.json`.

| Method | Default |
|--------|---------|
| `asset_name(node)` | `node["name"]`, with `_v<version>` for a versioned model. |
| `group(node)` | The dbt group. `None` keeps the group of the definition. |
| `tags(node)` | The dbt tags. |
| `metadata(node)` | `dbt/unique_id`, `dbt/relation`, `dbt/materialized`, and `dbt/description`. |
| `code_version(node)` | The checksum of the resource's file. |
| `upstream_asset(node)` | For a source: `None`, no dep. For a resource outside the selection: `asset_name(node)`. |

## Limits

- A run of some outputs lists each one in `--select`. On Windows, the command line can be too long for a run of more than about 1,000 models.
- dbt-core 2, the dbt Fusion engine, is not supported.
- A microbatch model with a `partial success` fails its output.
