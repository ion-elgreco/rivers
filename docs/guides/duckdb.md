# DuckDB

`rivers.integrations.duckdb` connects rivers to [DuckDB](https://duckdb.org):

- `DuckDBResource` opens connections to a database file, MotherDuck, or a [DuckLake](https://ducklake.select) catalog.
- `DuckDBIOHandler` stores each asset as a table, in DuckDB or DuckLake.
- `DuckDBAsset` and `DuckLakeAsset` add maintenance [actions](../concepts/actions.md).

The Delta Lake handler can also [read Delta tables with DuckDB](delta-lake.md#read-with-duckdb).

## Setup

```bash
pip install rivers[duckdb]

# Also read Delta tables as DuckDB relations
pip install rivers[delta-duckdb]
```

## Resource

```python
import pyarrow as pa
import rivers as rs
from rivers.integrations.duckdb import DuckDBResource

warehouse = DuckDBResource(database="warehouse.duckdb")

@rs.Asset
def top_users(warehouse: DuckDBResource) -> pa.Table:
    with warehouse.get_connection() as con:
        return con.sql("FROM users ORDER BY score DESC LIMIT 10").to_arrow_table()

repo = rs.CodeRepository(assets=[top_users], resources={"warehouse": warehouse})
```

`get_connection()` closes the connection when the block exits. `connect()` returns a connection that you close, or that a relation keeps open.

Each call opens a new connection. In one process, DuckDB shares one database instance per file, and releases the file when the last connection closes. A DuckDB file allows one read-write process at a time. When another process holds the file, the resource tries again 10 times, with a delay of 0.1 s that doubles each time (about 100 s in total).

Fields that you do not give are read from `DUCKDB_*` environment variables, for example `DUCKDB_DATABASE`.

| Field | Default | Description |
|-------|---------|-------------|
| `database` | `":memory:"` | A file path, `":memory:"`, or `"md:<database>"` for MotherDuck. |
| `connection_config` | `{}` | [DuckDB settings](https://duckdb.org/docs/current/configuration/overview) for `duckdb.connect(config=...)`, e.g. `{"threads": 4}`. |
| `extensions` | `[]` | Extensions to install and load on each connection. |
| `secrets` | `{}` | [Secrets](https://duckdb.org/docs/current/configuration/secrets_manager) to create on each connection: name to `CREATE SECRET` parameters. |
| `ducklake` | `None` | A [DuckLake catalog](#ducklake) to attach. |

### MotherDuck

Set `database="md:<database>"` and the `motherduck_token` environment variable:

```python
warehouse = DuckDBResource(database="md:analytics")
```

### Extensions and secrets

```python
lake_files = DuckDBResource(
    extensions=["spatial"],
    secrets={"s3": {"type": "s3", "provider": "credential_chain", "region": "eu-west-1"}},
)
```

DuckDB loads core extensions such as `httpfs`, `delta`, and `ducklake` when a query needs them.

To run without network access, put the extension files in a directory and point DuckDB at it:

```python
DuckDBResource(
    extensions=["spatial"],
    connection_config={"extension_directory": "/opt/duckdb/extensions"},
)
```

## IO handler

`DuckDBIOHandler` stores each asset as the table `<schema>.<asset name>`:

```python
import duckdb
import polars as pl
from rivers.integrations.duckdb import DuckDBIOHandler

io = DuckDBIOHandler(resource=warehouse)

@rs.Asset(io_handler=io)
def orders() -> pl.DataFrame:
    return pl.DataFrame({"day": ["d1", "d1", "d2"], "amount": [5, 7, 3]})

@rs.Asset(io_handler=io)
def daily(orders: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    return orders.aggregate("day, sum(amount) AS total")
```

A `DuckDBPyRelation` input is a lazy query. DuckDB runs it when the output is written, and the rows stream from one table to the other.

| Type | Write | Read |
|------|-------|------|
| `duckdb.DuckDBPyRelation` | yes | lazy |
| `pyarrow.Table` | yes | eager |
| `pyarrow.RecordBatchReader` | yes | lazy |
| arro3 `RecordBatchReader` | yes | no |
| `polars.DataFrame` | yes | eager |
| `polars.LazyFrame` | yes | lazy |
| `pandas.DataFrame` | yes | eager |

A read needs a type hint on the downstream parameter. Lazy inputs that one step reads share one connection per thread, so the step can join them. A relation that you make with `duckdb.sql(...)` uses another connection and cannot be joined with the inputs. To query next to the inputs, use `warehouse.shared_connection()`.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `resource` | required | The `DuckDBResource`. |
| `schema_name` | `"main"` | The schema for the tables. |
| `mode` | `DuckDBWriteMode.OVERWRITE` | `OVERWRITE` replaces the table, or the partition's rows. `APPEND` inserts rows. The strings `"overwrite"` and `"append"` work too. |

### Partitioned assets

Map the partition key to columns with `duckdb/partition_expr`, as with [Delta](delta-lake.md#partitioned-writes):

```python
from datetime import datetime

@rs.Asset(
    io_handler=io,
    partitions_def=rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1)),
    metadata={"duckdb/partition_expr": "day"},
)
def events(context: rs.AssetExecutionContext) -> pl.DataFrame:
    ...
```

An overwrite deletes the partition's rows and inserts the new rows in one transaction. A backfill run with several keys replaces all of them. Daily and hourly keys become ranges, so the column can be text, `DATE`, or `TIMESTAMP`.

When two writes conflict, the handler runs the transaction again. It uses DuckLake's retry settings: `ducklake_max_retry_count` (10), `ducklake_retry_wait_ms` (100), and `ducklake_retry_backoff` (1.5). For a DuckLake catalog, change them in `connection_config`.

### Metadata keys

| Key | Description |
|-----|-------------|
| `duckdb/schema` | Schema for this asset's table. |
| `duckdb/table` | Table name, instead of the asset name. |
| `duckdb/mode` | `"overwrite"` or `"append"`. |
| `duckdb/partition_expr` | Partition column, or a JSON object from dimension to column. |
| `duckdb/columns` | JSON list of columns to read. |
| `duckdb/version` | DuckLake snapshot to read. |
| `ducklake/options` | JSON object of DuckLake table options. |

Each write records `duckdb/table`, `duckdb/num_rows` (rows in the whole table), `duckdb/write_duration_s`, and `rivers/schema` (with pyarrow or arro3 installed). DuckLake writes also record `ducklake/snapshot_id`.

### One writer per file

A DuckDB file allows one read-write process. With the parallel executor or Kubernetes, steps in other processes wait for the file. A lazy input keeps the file open while its step runs. For many writers, use DuckLake with a PostgreSQL catalog.

## DuckLake

Give the resource a `DuckLake` catalog. The IO handler then writes DuckLake tables:

```python
from rivers.integrations.duckdb import DuckDBIOHandler, DuckDBResource, DuckLake

lake = DuckDBResource(
    ducklake=DuckLake(
        metadata="postgres:dbname=lake host=pg",
        data_path="s3://bucket/lake/",
        options={"expire_older_than": "7 days", "delete_older_than": "7 days"},
    ),
    secrets={"s3": {"type": "s3", "provider": "credential_chain"}},
)
io = DuckDBIOHandler(resource=lake)
```

The catalog is attached once per process. Each call gets a cursor on that connection. The connection stays open until `teardown()`, and until the last cursor or relation made from it is gone. Resources with equal fields share it.

| `DuckLake` field | Default | Description |
|------------------|---------|-------------|
| `metadata` | required | The catalog database: a DuckDB file path, `"postgres:..."`, or `"sqlite:..."`. |
| `data_path` | `None` | Where DuckLake writes Parquet files. Give credentials for it in `secrets`. |
| `name` | `"lake"` | The catalog name in SQL. |
| `attach_options` | `{}` | Other `ATTACH` options, e.g. `{"METADATA_SCHEMA": "lake"}`. |
| `options` | `{}` | Lake-wide DuckLake options. |

### Choose a catalog

From [Choosing a Catalog Database](https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database):

| Catalog | Clients | Use with |
|---------|---------|----------|
| DuckDB file | one process | the in-process executor |
| SQLite | several local processes, with retries | one machine |
| PostgreSQL | many, also remote | the parallel executor and Kubernetes |

A second process that attaches a DuckDB-file catalog fails after the lock retries, with an error that names the limit.

### Tables, snapshots, and data versions

- A partitioned asset gets a table that is partitioned by its partition columns. A table that exists is never changed.
- A partition overwrite is one snapshot.
- Each write registers the snapshot id as the asset's [data version](../api-reference/context.md), so downstream assets go stale after a new write.
- `duckdb/version` reads an older snapshot. An overwrite of an unpartitioned table keeps the old snapshots readable.
- DuckLake keeps writes of up to 10 rows in the catalog instead of a Parquet file ([data inlining](https://ducklake.select/docs/stable/duckdb/advanced_features/data_inlining)). `optimize` moves them to files. Change the limit with `attach_options={"DATA_INLINING_ROW_LIMIT": n}`.

### Conflicts

A partition overwrite conflicts with any other write to the same table that commits at the same time ([conflict resolution](https://ducklake.select/docs/stable/duckdb/advanced_features/conflict_resolution)). The handler runs the transaction again with the retry settings above. When many partitions of one table are written at once, limit them with a [concurrency pool](../api-reference/concurrency.md#concurrency-pools):

```python
@rs.Asset(io_handler=io, pool="events_table", metadata={"duckdb/partition_expr": "day"})
def events(...): ...

repo = rs.CodeRepository(assets=[events], pool_limits={"events_table": 2})
```

### Options

DuckLake stores [options](https://ducklake.select/docs/stable/duckdb/usage/configuration) in the catalog. rivers sets an option only when it differs from the stored value. Setting an option makes no snapshot.

- Lake-wide: `DuckLake(options={...})`, set when the catalog is attached.
- Per table: the `ducklake/options` metadata key, set after each write.

```python
@rs.Asset(
    io_handler=io,
    metadata={"ducklake/options": '{"rewrite_delete_threshold": 0.5, "target_file_size": "128MB"}'},
)
def orders() -> pl.DataFrame: ...
```

## Maintenance actions

```python
from rivers.integrations.duckdb import DuckLakeAsset

class Orders(DuckLakeAsset):
    io_handler = io

    @classmethod
    def materialize(cls) -> pl.DataFrame: ...

repo.run_action("optimize", selection=["orders"])
```

| Class | Action | What it does |
|-------|--------|--------------|
| `DuckDBAsset` | `delete` | Deletes the partition's rows (with a key) or all rows, and clears the state. A missing table only clears the state. |
| `DuckLakeAsset` | `delete` | The same. |
| `DuckLakeAsset` | `optimize` | Moves inlined rows to files, rewrites files with many deleted rows, and merges small files. Uses the table's `rewrite_delete_threshold` and `target_file_size`. |
| `DuckLakeAsset` | `vacuum` | Expires old snapshots and deletes their files, for the whole lake. Uses the lake's `expire_older_than` and `delete_older_than`; without them it does nothing. |

The actions take no config: the DuckLake options are the settings. A keyed `delete` needs `duckdb/partition_expr`.

## Limits

- The handler writes with overwrite or append. It has no MERGE.
- DuckDB has no wheels for free-threaded Python or for Linux musl.
- The Delta reader cannot run on MotherDuck, and reads GCS only with HMAC keys.
