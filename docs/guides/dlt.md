# dlt

`rivers.integrations.dlt` loads [dlt](https://dlthub.com/docs) sources as rivers assets. Each resource of a source becomes one output of a multi-asset:

- `DltSourceAssets` reads a dlt source and gives each resource an output.
- Its `load` method runs a dlt pipeline on the source.

## Setup

```bash
pip install rivers[dlt]
```

Also install the dlt extra for your destination, e.g. `pip install "dlt[snowflake]"`.

## Assets from a source

```python
import dlt
import rivers as rs
from rivers.integrations.dlt import DltSourceAssets


@dlt.source(name="github")
def github_source(repo: str = "dlt-hub/dlt"):
    @dlt.resource(primary_key="id", write_disposition="merge")
    def issues(): ...

    @dlt.resource(primary_key="id", write_disposition="merge")
    def pull_requests(): ...

    return issues, pull_requests


github = DltSourceAssets(github_source())


@github.assets(name="github", group="github")
def github_assets(context: rs.AssetExecutionContext):
    pipeline = dlt.pipeline("github", destination="snowflake", dataset_name="github")
    yield from github.load(context, pipeline)


repo = rs.CodeRepository(assets=[github_assets])
```

This defines the assets `github_issues` and `github_pull_requests`, materialized together by the step `github`. `github.assets(...)` builds the [multi-asset](../concepts/assets.md#multi-assets); its keyword arguments go to `Asset.from_multi`, e.g. `name`, `group`, `partitions_def`, `io_handler`, `hooks`, or `retry`. The function makes the dlt pipeline and loads the source: `github.load` yields one materialization per resource.

`DltSourceAssets` takes the dlt source, e.g. `github_source()`. Use `github_source().with_resources("issues")` for some of its resources.

The function runs in the process that runs the step, and a dlt source cannot be sent to another process. With the parallel executor, define the `DltSourceAssets` and the function at module level in an importable module: the worker imports them.

### Asset names

An output is named after the source and the resource: `<source>_<resource>`, e.g. `github_issues`. Use an [`asset_def_fn`](#change-the-outputs) to choose other names. Two resources with the same output name raise `ValueError`.

## Run part of a source

Materialize some of the outputs, and the pipeline loads only their resources:

```python
repo.materialize(selection=["github_issues"])
```

A dlt transformer still reads from its parent resource, but the parent's table does not load.

## The pipeline and the source

The function makes the pipeline in each run, so it can use [resources](../concepts/resources.md) and config like any asset. For example, keep the destination's password in a resource:

```python
from pydantic import SecretStr


class Warehouse(rs.Resource):
    host: str
    password: SecretStr

    def pipeline(self, name: str) -> dlt.Pipeline:
        credentials = f"postgresql://loader:{self.password.get_secret_value()}@{self.host}/analytics"
        destination = dlt.destinations.postgres(credentials=credentials)
        return dlt.pipeline(name, destination=destination, dataset_name=name)


@github.assets(name="github")
def github_assets(context: rs.AssetExecutionContext, warehouse: Warehouse):
    yield from github.load(context, warehouse.pipeline("github"))
```

`load` loads the source of `DltSourceAssets` in every run. To build the source in the run, for example from the [partition](#partitions), give it to `load`: `github.load(context, pipeline, github_source(repo=...))`. It must have the resources of the selected outputs. A resource made from a generator object, as in `dlt.resource(rows())`, runs only once: give such a source to `load` in each run.

## What a run records

dlt's load summary goes to the step log. Each materialization has this metadata:

| Key | Type | Description |
|-----|------|-------------|
| `dlt/load_id` | `str` | The load package of the run, as in the `_dlt_load_id` column. Not set when the run extracted nothing. |
| `dlt/dataset` | `str` | The dataset that the tables are in. |
| `dlt/rows_loaded` | `int` | Rows loaded into the resource's table in this run, without its nested tables. |
| `dlt/row_counts` | `MetadataValue.json` | Rows loaded per table, with the nested tables, e.g. `{"issues": 30, "issues__labels": 41}`. |
| `dlt/columns` | `MetadataValue.json` | The columns and dlt data types of each table. |

A resource that raises, or a failed normalize step, fails every selected output: dlt extracts a source into one load package, and loads nothing when the extract fails.

A load job that fails fails every selected output, with dlt's default `load.raise_on_failed_jobs = true`. With `false`, dlt finishes the package. rivers then records the resources whose tables loaded, and the step fails for the others with `DestinationHasFailedJobs`.

Before each run, the pipeline drops the load packages that a failed run left, so that a retry loads new data. With `restore_from_destination` on (dlt's default), the pipeline also drops its local state and restores it from the destination. With it off, the pipeline only drops the packages, and keeps its incremental state.

To try a failed load again, give the assets a [retry policy](../concepts/retries.md):

```python
from dlt.common.destination.exceptions import DestinationHasFailedJobs
from dlt.pipeline.exceptions import PipelineStepFailed


@github.assets(
    name="github",
    retry=rs.RetryPolicy(max_retries=2, retry_on=[PipelineStepFailed, DestinationHasFailedJobs]),
)
def github_assets(context: rs.AssetExecutionContext): ...
```

## Partitions

Build the source from the partition, and give it to `load`:

```python
from datetime import datetime

from rivers.integrations.dlt import output_def


@dlt.source(name="events")
def events_source(since: datetime | None = None, until: datetime | None = None):
    @dlt.resource
    def page_views():
        yield from read_page_views(since, until)

    return page_views


events = DltSourceAssets(
    events_source(),
    asset_def_fn=lambda resource: output_def(resource, pool="events_pipeline"),
)


@events.assets(
    name="events",
    partitions_def=rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1)),
)
def events_assets(context: rs.AssetExecutionContext):
    since, until = context.partition_time_window
    pipeline = dlt.pipeline("events", destination="snowflake", dataset_name="events")
    yield from events.load(context, pipeline, events_source(since=since, until=until))


repo = rs.CodeRepository(assets=[events_assets], pool_limits={"events_pipeline": 1})
```

A dlt pipeline keeps its state and load packages in one working directory, so two runs of one pipeline must not overlap. Give the outputs a pool with a limit of 1, as above, or give each concurrent run its own pipeline name.

For an incremental resource, pass the window as `initial_value` and `end_value` of `dlt.sources.incremental`. With an `end_value`, dlt loads that range and does not change the incremental state.

## Change the outputs

`asset_def_fn` builds the output of each resource. It takes the dlt resource and returns an [`AssetDef`](../api-reference/assets.md). `output_def(resource, ...)` returns the default output with the fields that you give:

```python
from rivers.integrations.dlt import output_def


def asset_def(resource) -> rs.AssetDef:
    return output_def(
        resource,
        name=f"raw_{resource.name}",
        kinds=["dlt", "snowflake"],
        deps=[rs.AssetDef.dep("github_app")] if resource.name == "issues" else [],
    )


github = DltSourceAssets(github_source(), asset_def_fn=asset_def)
```

| Field | Default |
|-------|---------|
| `name` | `<source>_<resource>`, e.g. `github_issues`. |
| `kinds` | `["dlt"]`. |
| `metadata` | `dlt/resource` (the resource name) and `dlt/table` (the table name, or the resource name when the resource sets the table name in the run). |

The default output has no deps, because dlt reads data from outside rivers. To link a resource to the rivers asset that holds its source data, return `deps=[rs.AssetDef.dep(...)]`. That asset must be in the repository.

## Downstream assets

dlt writes the tables to its destination, not to an IO handler. A downstream asset can read the destination, and take the resource's output as a lineage dep:

```python
@rs.Asset(deps=[rs.AssetDef.dep("github_issues")])
def issue_report() -> None: ...
```

Or it takes the output as an input, through an IO handler that reads the destination. For a DuckDB destination, give the assets a [`DuckDBIOHandler`](duckdb.md#io-handler), and point it at the tables with the `duckdb/schema` and `duckdb/table` metadata keys:

```python
import pyarrow as pa
import pyarrow.compute as pc
from rivers.integrations.duckdb import DuckDBIOHandler, DuckDBResource

github = DltSourceAssets(
    github_source(),
    asset_def_fn=lambda resource: output_def(
        resource,
        metadata={"duckdb/schema": "github", "duckdb/table": resource.table_name},
    ),
)


@github.assets(
    name="github",
    io_handler=DuckDBIOHandler(resource=DuckDBResource(database="warehouse.duckdb")),
)
def github_assets(context: rs.AssetExecutionContext):
    destination = dlt.destinations.duckdb("warehouse.duckdb")
    pipeline = dlt.pipeline("github", destination=destination, dataset_name="github")
    yield from github.load(context, pipeline)


@rs.Asset
def open_issues(github_issues: pa.Table) -> pa.Table:
    return github_issues.filter(pc.field("state") == "open")
```

The IO handler only reads: dlt writes the tables.

## Limits

- A step runs one `pipeline.run`, so one failing resource fails the step. For loads that fail on their own, make one `DltSourceAssets` per group of resources, e.g. with `github_source().with_resources("issues")`.
- dlt supports Python up to 3.14, and the extra does not install on free-threaded Python.

See the [API reference](../api-reference/dlt.md) for every parameter.
