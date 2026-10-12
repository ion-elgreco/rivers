# Sling

`rivers.integrations.sling` runs [Sling](https://slingdata.io) replications as rivers assets. Each stream of a replication becomes one output of a multi-asset:

- `SlingReplication` reads a replication and gives each stream an output.
- `SlingResource` holds the connections and runs the replication.

## Setup

```bash
pip install rivers[sling]
```

The `sling` package downloads the sling binary from GitHub when a replication first runs (about 120 MB, to `~/.sling/bin`). Set `SLING_BINARY` to the path of a sling binary to use that one instead, for example in an image without network access. Some targets need one more tool, which sling also downloads on first use: the DuckDB CLI for DuckDB targets and Parquet files, and the `sqlite3` CLI for bulk loads into SQLite.

## Assets from a replication

A [replication](https://docs.slingdata.io/concepts/replication) copies streams (tables, queries, or files) from a source connection to a target connection:

```yaml
# replication.yaml
source: PG
target: WAREHOUSE

defaults:
  mode: full-refresh
  object: raw.{stream_table}

streams:
  public.orders:
  public.users:
    object: raw.app_users
```

```python
import rivers as rs
from rivers.integrations.sling import SlingConnection, SlingReplication, SlingResource

replication = SlingReplication("replication.yaml")

@replication.assets(name="raw")
def raw(context: rs.AssetExecutionContext, sling: SlingResource):
    yield from sling.replicate(context, replication)

repo = rs.CodeRepository(
    assets=[raw],
    resources={
        "sling": SlingResource(
            connections=[
                SlingConnection(name="PG", url="${PG_URL}"),
                SlingConnection(name="WAREHOUSE", type="duckdb", instance="warehouse.duckdb"),
            ]
        )
    },
)
```

This defines the assets `public.orders` and `raw.app_users`, materialized together by the step `raw`. `replication.assets(...)` builds the [multi-asset](../concepts/assets.md#multi-assets); its keyword arguments go to `Asset.from_multi`, e.g. `name`, `group`, `partitions_def`, `hooks`, or `retry`. The function runs the replication: `sling.replicate` yields one materialization per stream.

`SlingReplication` takes a YAML or JSON file, or the replication as a dict. A stream with `disabled: true` gets no output.

### Asset names

An output is named after the stream's `object` when the stream sets one without runtime variables, and after the stream otherwise. The name is lowercased, quotes are removed, and each run of characters other than letters, digits, `_`, and `.` becomes `_`:

| Stream | `object` | Asset name |
|--------|----------|------------|
| `public.orders` | from `defaults`: `raw.{stream_table}` | `public.orders` |
| `public.users` | `raw.app_users` | `raw.app_users` |
| `"Public"."Users"` | none | `public.users` |
| `file://data/orders.csv` | none | `file_data_orders.csv` |

Two streams with the same name raise `ValueError`. Use a [translator](#translator) to choose other names.

## Run part of a replication

Materialize some of the outputs, and sling runs only their streams:

```python
repo.materialize(selection=["raw.app_users"])
```

The other streams are disabled for the run, not removed. So a wildcard stream such as `public.*` still leaves out the tables that other streams name.

## What a run records

Sling's log goes to the step log. Each stream is materialized as soon as sling reports that it succeeded, with this metadata:

| Key | Type | Description |
|-----|------|-------------|
| `sling/stream` | `str` | The stream. |
| `sling/object` | `str` | The table or file that sling wrote, e.g. `raw.app_users`. |
| `sling/rows_written` | `int` | Rows that sling wrote for the stream in this run. |
| `sling/duration_s` | `float` | Seconds the stream took. |

`sling/object` and `sling/rows_written` are there when sling's log shows them. A stream that sling's log does not name, such as a wildcard stream that becomes many tables, is materialized when the whole replication succeeds, with `sling/stream` only.

When a stream fails, sling goes on with the next streams and then exits with an error. The streams that succeeded keep their materialization. The step fails with `SlingError`, whose message holds the errors that sling reported, and the selected streams that did not succeed get no materialization.

## Connections

`SlingConnection` defines a connection by the name that the replication's `source` and `target` use. Give the connection's [properties](https://docs.slingdata.io/connections/database-connections) as keyword arguments, or a `url`:

```python
from pydantic import SecretStr

SlingConnection(name="PG", type="postgres", host="db", user="etl", password="${PG_PASSWORD}", database="app")
SlingConnection(name="PG", url="postgresql://etl:${PG_PASSWORD}@db:5432/app")
SlingConnection(name="LAKE", type="s3", bucket="lake", secret_access_key=SecretStr("..."))
```

`url` and `password` are `SecretStr` fields. Give other secret properties as `SecretStr` too. A string written as `"${VAR}"` is read from the environment variable `VAR` when the replication runs, in the process that runs it, so the resource holds the reference and not the secret. A variable that is not set raises `ValueError` before sling starts. The values of `SecretStr` properties and of `"${VAR}"` references are masked as `***` in the step log and in `SlingError`.

`SlingResource` gives each connection to sling as JSON in an environment variable named after the connection. Sling also finds connections in environment variables and in `~/.sling/env.yaml` by itself; those need no `SlingConnection`.

When `connections` is not given, it is read from the `SLING_CONNECTIONS` environment variable, as a JSON list.

## Translator

A translator builds the output of each stream. It takes a `SlingStream`, with the stream's `name` and its own `config`, and returns an [`AssetDef`](../api-reference/assets.md). `stream.output_def(...)` returns the default output with the fields that you give:

```python
from rivers.integrations.sling import SlingStream

def translate(stream: SlingStream) -> rs.AssetDef:
    table = stream.name.split(".")[-1]
    return stream.output_def(
        name=f"raw_{table}",
        group="raw",
        deps=[rs.AssetDef.dep(f"postgres_{table}")],
    )

replication = SlingReplication("replication.yaml", translator=translate)
```

The default output has no deps, so a replication works without other assets. To link a stream to the rivers asset that holds its source data, return `deps=[rs.AssetDef.dep(...)]`. That asset must be in the repository.

When the translator sets no `code_version`, the output gets a hash of the stream's config and of the replication's other keys. So `code_version_changed()` sees a change to the config.

## Downstream assets

Sling writes the data to its target, not to an IO handler. A downstream asset reads the target and takes the stream's output as a lineage dep:

```python
from rivers.integrations.duckdb import DuckDBResource

@rs.Asset(deps=[rs.AssetDef.dep("raw.app_users")])
def active_users(warehouse: DuckDBResource) -> int:
    with warehouse.get_connection() as con:
        return con.sql("SELECT count(*) FROM raw.app_users WHERE active").fetchone()[0]
```

See the [API reference](../api-reference/sling.md) for every parameter.
