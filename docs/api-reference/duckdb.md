# DuckDB

## `DuckDBIOHandler`

Persists asset outputs as tables in a DuckDB database or a DuckLake catalog.

```python
from rivers.integrations.duckdb import DuckDBIOHandler, DuckDBResource

io = DuckDBIOHandler(
    resource=DuckDBResource(database="warehouse.duckdb"),
    mode="overwrite",
)
```

**Constructor:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `resource` | `DuckDBResource` | required | Opens the connections. Each asset becomes the table `{schema_name}.{asset_name}`. |
| `schema_name` | `str` | `"main"` | Schema for the tables. |
| `mode` | `DuckDBWriteMode` | `OVERWRITE` | `OVERWRITE` or `APPEND`. The strings `"overwrite"` and `"append"` work too. |

**Supported types:**

| Type | Write | Read |
|------|-------|------|
| `duckdb.DuckDBPyRelation` | yes | lazy |
| `pyarrow.Table` | yes | eager |
| `pyarrow.RecordBatchReader` | yes | lazy |
| arro3 `RecordBatchReader` | yes | no |
| `polars.DataFrame` | yes | eager |
| `polars.LazyFrame` | yes | lazy |
| `pandas.DataFrame` | yes | eager |

Lazy inputs read in one thread share `DuckDBResource.shared_connection()`, so a step can join them.

**Asset metadata overrides:**

These metadata keys override handler defaults per-asset:

| Key | Type | Description |
|-----|------|-------------|
| `duckdb/schema` | `str` | Schema override. |
| `duckdb/table` | `str` | Table name override (default: the asset name). |
| `duckdb/mode` | `str` | Write mode override. |
| `duckdb/partition_expr` | `str \| JSON dict` | Partition column mapping. |
| `duckdb/columns` | `JSON list` | Column selection for reads. |
| `duckdb/version` | `str` | DuckLake snapshot for time travel reads. Raises `ValueError` on a DuckDB database. |
| `ducklake/options` | `JSON dict` | DuckLake table options, set after each write when they differ from the catalog. |

**Output metadata:**

| Key | Type | Description |
|-----|------|-------------|
| `duckdb/table` | `str` | `schema.table`. |
| `duckdb/num_rows` | `int` | Total rows in table after write. |
| `duckdb/write_duration_s` | `float` | Write duration in seconds. |
| `rivers/schema` | `Schema` | Arrow schema of the written table (needs pyarrow or arro3). |
| `ducklake/snapshot_id` | `int` | DuckLake only: snapshot of the write, also registered as the data version. |

### Methods for action bodies

An [action](../concepts/actions.md) receives the asset's resolved handler as
`ctx.io_handler`. These methods expose the same table and partition resolution
the write path uses, so a custom action targets exactly the rows the
materialize path would have written.

#### `table_name(asset_name, asset_metadata)`

Returns the `(schema, table)` pair, honoring `duckdb/schema` and `duckdb/table`
overrides in `asset_metadata`. Pass `ctx.asset_name` and `ctx.asset_metadata`.

```python
@rs.action(outcome=rs.Outcome.Unchanged)
@classmethod
def analyze(cls, ctx: rs.ActionContext) -> None:
    schema, table = ctx.io_handler.table_name(ctx.asset_name, ctx.asset_metadata)
    with ctx.io_handler.resource.get_connection() as con:
        con.execute(f'ANALYZE "{schema}"."{table}"')
```

#### `partition_predicate(asset_metadata, partition)`

Returns a SQL predicate covering the partition(s), honoring
`duckdb/partition_expr`. Pass `ctx.asset_metadata` and `ctx.partition`.

```python
@rs.action(outcome=rs.Outcome.Unmaterialize)
@classmethod
def delete(cls, ctx: rs.ActionContext) -> None:
    schema, table = ctx.io_handler.table_name(ctx.asset_name, ctx.asset_metadata)
    predicate = ctx.io_handler.partition_predicate(ctx.asset_metadata, ctx.partition)
    with ctx.io_handler.resource.get_connection() as con:
        con.execute(f'DELETE FROM "{schema}"."{table}" WHERE {predicate}')
```

`partition` is required: reach this only from a keyed action run. On a
non-partitioned asset, delete the whole table instead of building a predicate.

---

## `DuckDBAsset`

Asset base class with the `delete` verb built in. Subclass it, define
`materialize`, and `delete` appears as an [action](../concepts/actions.md),
resolved against the asset's `DuckDBIOHandler` (its own `io_handler` or the
repository default). Sets `kinds = "duckdb"`.

```python
from rivers.integrations.duckdb import DuckDBAsset

class Orders(DuckDBAsset):
    io_handler = WAREHOUSE

    @classmethod
    def materialize(cls) -> pl.DataFrame: ...
```

| Verb | Declaration | Behavior |
|------|-------------|----------|
| `delete` | `Unmaterialize` + `Exclusive` + `DownstreamFirst` + `Optional` key | Deletes the keyed partition's rows via `partition_predicate`, or every row without a key — on partitioned assets both forms are valid. From the UI or gRPC, the keyless form needs an explicit whole-asset choice. |

A verb requires the asset to resolve a `DuckDBIOHandler` — anything else fails the
step with a `TypeError` naming the requirement. On a table that does not exist,
`delete` still applies its `Unmaterialize` outcome, so dangling state clears. A keyed
`delete` on an asset without `duckdb/partition_expr` fails with a `ValueError` naming
the key. A subclass redefining a verb replaces the built-in.

---

## `DuckLakeAsset`

`DuckDBAsset` subclass for DuckLake tables, with `optimize` and `vacuum` added.
Sets `kinds = "ducklake"`.

```python
from rivers.integrations.duckdb import DuckLakeAsset

class Events(DuckLakeAsset):
    io_handler = LAKE
    metadata = {"ducklake/options": '{"rewrite_delete_threshold": 0.5}'}

    @classmethod
    def materialize(cls) -> pl.DataFrame: ...
```

| Verb | Declaration | Behavior |
|------|-------------|----------|
| `optimize` | `Unchanged` + `Exclusive` + `Keyless` | `ducklake_flush_inlined_data`, `ducklake_rewrite_data_files`, and `ducklake_merge_adjacent_files` for the table, with its `rewrite_delete_threshold` and `target_file_size` options. Reports `rows_flushed`, `files_rewritten`, and `files_merged`. |
| `vacuum` | `Unchanged` + `Exclusive` + `Keyless` | `ducklake_expire_snapshots` and `ducklake_cleanup_old_files` for the whole lake, with its `expire_older_than` and `delete_older_than` options. Does nothing when neither is set. Reports `snapshots_expired` and `files_deleted`. |
| `delete` | as `DuckDBAsset` | |

The verbs take no config: set table options with the `ducklake/options` metadata key,
and lake-wide options with `DuckLake(options=...)`. A verb requires the handler's
resource to have `ducklake` — otherwise it fails the step with a `TypeError`. On a
table that does not exist yet, `optimize` reports `ActionResult.unchanged()`.

---

## `DuckDBResource`

Opens DuckDB connections to a database file, `:memory:`, MotherDuck, or a DuckLake
catalog. Fields are read from `DUCKDB_*` environment variables when not given.

```python
from rivers.integrations.duckdb import DuckDBResource

warehouse = DuckDBResource(database="warehouse.duckdb", extensions=["spatial"])
```

**Constructor:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `database` | `str` | `":memory:"` | A file path, `":memory:"`, or `"md:<database>"` for MotherDuck. |
| `connection_config` | `dict[str, str \| bool \| int \| float \| list[str]]` | `{}` | DuckDB settings passed to `duckdb.connect(config=...)`. |
| `extensions` | `list[str]` | `[]` | Extensions to install and load on each connection. |
| `secrets` | `dict[str, dict[str, str \| int \| bool]]` | `{}` | Secret name to `CREATE SECRET` parameters, created on each connection. |
| `ducklake` | `DuckLake \| None` | `None` | A DuckLake catalog to attach. Needs `database=":memory:"`. |

Opening a file that another process holds is retried 10 times, with a delay of 0.1 s
that doubles each time. After the last try the `duckdb.IOException` is raised; for a
DuckLake catalog in a DuckDB file, the error says that the catalog allows one process.

### Methods

#### `get_connection()`

Context manager: a connection that is closed when the block exits. Relations made
from it stop working after the block.

```python
with warehouse.get_connection() as con:
    con.execute("CREATE TABLE users AS FROM 'users.parquet'")
```

#### `connect()`

Returns a new connection. The caller closes it, or a relation keeps it open. With
`ducklake`, it is a cursor whose default database is the catalog.

#### `shared_connection()`

Returns the calling thread's connection for relations that must be combined: DuckDB
joins relations only when they come from one connection. The connection closes when
the last relation made from it is gone.

```python
@rs.Asset(io_handler=io)
def enriched(orders: duckdb.DuckDBPyRelation, warehouse: DuckDBResource) -> duckdb.DuckDBPyRelation:
    regions = warehouse.shared_connection().sql("FROM 'regions.csv'")
    return orders.join(regions, "region_id")
```

#### `teardown()`

Releases the DuckLake root connection. The catalog closes when the last cursor or
relation made from it is gone.

---

## `DuckLake`

A DuckLake catalog, attached once per process to every connection of a
`DuckDBResource`.

```python
from rivers.integrations.duckdb import DuckDBResource, DuckLake

lake = DuckDBResource(
    ducklake=DuckLake(
        metadata="postgres:dbname=lake host=pg",
        data_path="s3://bucket/lake/",
        options={"expire_older_than": "7 days"},
    )
)
```

**Attributes:**

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `metadata` | `str` | required | The catalog database: a DuckDB file path (one process), `"postgres:dbname=lake host=pg"`, or `"sqlite:meta.sqlite"`. |
| `data_path` | `str \| None` | `None` | Where DuckLake writes Parquet files. |
| `name` | `str` | `"lake"` | The catalog name in SQL. |
| `attach_options` | `dict[str, str \| int \| bool]` | `{}` | Other `ATTACH` options, e.g. `{"METADATA_SCHEMA": "lake"}`. |
| `options` | `dict[str, str \| int \| float \| bool]` | `{}` | Lake-wide DuckLake options. Each one that differs from the catalog is set when the catalog is attached. |
