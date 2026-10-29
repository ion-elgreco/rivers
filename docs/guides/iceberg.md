# Apache Iceberg

The `IcebergIOHandler` stores asset outputs as [Apache Iceberg](https://iceberg.apache.org/) tables through a [pyiceberg](https://py.iceberg.apache.org/) catalog. It writes PyArrow, Polars, pandas, and DuckDB data, partitions tables by the asset's partitions, and supports overwrite, append, and upsert writes, schema evolution, and time travel.

## Setup

```bash
pip install rivers[iceberg]

# Plus, for these data types:
pip install rivers[iceberg-polars]
pip install rivers[iceberg-pandas]
pip install rivers[iceberg-duckdb]
```

`rivers[iceberg]` installs pyiceberg with PyArrow, so PyArrow tables need no other extra. It also installs `pyiceberg-core`, which pyiceberg needs to write tables that are partitioned by year, month, day, or hour.

The REST catalog needs nothing more. For other catalogs, install the pyiceberg extra of the catalog, for example `pyiceberg[sql-postgres]`, `pyiceberg[sql-sqlite]`, `pyiceberg[glue]`, or `pyiceberg[hive]`.

## Basic usage

```python
import polars as pl
import rivers as rs
from rivers.io_handlers.iceberg import IcebergIOHandler

io = IcebergIOHandler(
    catalog_name="lake",
    catalog_properties={"type": "rest", "uri": "https://catalog.example.com"},
    namespace="analytics",
)

@rs.Asset(io_handler=io)
def users() -> pl.DataFrame:
    return pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Carol"],
    })
```

The first write creates the namespace and the table `analytics.users`.

### Catalogs

The handler loads the catalog with `pyiceberg.catalog.load_catalog(catalog_name, **catalog_properties)`. pyiceberg also reads the properties of the catalog from `.pyiceberg.yaml` and from environment variables (see [Storage](#storage)), so the handler can use a catalog that you configure outside your code:

```python
io = IcebergIOHandler(catalog_name="lake", namespace="analytics")
```

A local catalog for development and tests needs `pip install "pyiceberg[sql-sqlite]"`:

```python
io = IcebergIOHandler(
    catalog_name="local",
    catalog_properties={
        "type": "sql",
        "uri": "sqlite:////data/iceberg/catalog.db",
        "warehouse": "file:///data/iceberg/warehouse",
    },
    namespace="dev",
)
```

The handler loads the catalog for each read and write, and keeps no catalog object, so it also works in the worker processes of `Executor.parallel()`.

### Table names

Each asset writes the table `{namespace}.{asset name}`. Two metadata keys change the name for one asset:

```python
@rs.Asset(io_handler=io, metadata={"iceberg/namespace": "raw", "iceberg/table": "events_v2"})
def events() -> pa.Table:
    ...
```

Dots in a namespace make a nested namespace: `namespace="lake.raw"` is the namespace `raw` in `lake`. REST catalogs can create tables in nested namespaces. pyiceberg's SQL catalog cannot.

## Storage

The storage settings and credentials of the table files are pyiceberg [FileIO properties](https://py.iceberg.apache.org/configuration/#fileio). pyiceberg reads them from the catalog properties, so put them in `catalog_properties`. They apply to every table of the catalog:

```python
io = IcebergIOHandler(
    catalog_name="lake",
    catalog_properties={
        "type": "sql",
        "uri": "postgresql+psycopg2://user:password@db/catalog",
        "warehouse": "s3://bucket/warehouse",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "...",
        "s3.secret-access-key": "...",
        "s3.region": "us-east-1",
    },
    namespace="analytics",
)
```

The keys are pyiceberg's, not the `storage_options` keys of the Delta handler: `s3.access-key-id`, not `AWS_ACCESS_KEY_ID`. Frequently used keys:

| Store | Keys |
|-------|------|
| S3 and S3-compatible stores | `s3.endpoint`, `s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`, `s3.region`, `s3.force-virtual-addressing` |
| Azure (ADLS) | `adls.account-name`, `adls.account-key`, `adls.sas-token`, `adls.tenant-id`, `adls.client-id`, `adls.client-secret` |
| Google Cloud Storage | `gcs.project-id`, `gcs.oauth2.token` |

Without credentials, pyiceberg uses the default credential chain of the store, for example the AWS environment variables, profile, or instance role.

To keep the credentials out of your code, set them in `.pyiceberg.yaml`, or in environment variables named `PYICEBERG_CATALOG__<CATALOG>__<KEY>`. In the key, write `__` for each `.` and `_` for each `-`:

```bash
export PYICEBERG_CATALOG__LAKE__S3__ACCESS_KEY_ID=...
export PYICEBERG_CATALOG__LAKE__S3__SECRET_ACCESS_KEY=...
```

pyiceberg reads a `_` in the catalog name as `-` too, so these variables cannot configure a catalog named `my_lake`. Use a name without `_`, such as `my-lake`.

A REST catalog can give out short-lived credentials for each table, and pyiceberg asks for them by default. pyiceberg uses these credentials before the catalog properties, so with such a catalog you often set no storage keys.

All readers and writers use the same properties:

- pyiceberg reads and writes the files for writes, and for PyArrow and pandas inputs.
- Polars inputs: polars converts the S3 keys, the Azure keys, and `gcs.oauth2.token` for its own reader.
- DuckDB inputs: the handler turns the S3 and Azure keys into a DuckDB secret (see [Read with DuckDB](#read-with-duckdb)).

## Supported types

| Type | Extra | Read with |
|------|-------|-----------|
| `pyarrow.Table` | `rivers[iceberg]` | a pyiceberg scan, into memory |
| `pyarrow.RecordBatchReader` | `rivers[iceberg]` | a pyiceberg scan, in batches |
| `polars.DataFrame` | `rivers[iceberg-polars]` | `pl.scan_iceberg`, collected |
| `polars.LazyFrame` | `rivers[iceberg-polars]` | `pl.scan_iceberg`, lazy |
| `pandas.DataFrame` | `rivers[iceberg-pandas]` | a pyiceberg scan, into memory |
| `duckdb.DuckDBPyRelation` | `rivers[iceberg-duckdb]` | DuckDB's `iceberg_scan`, lazy |
| `pyspark.sql.DataFrame` | `rivers[iceberg-pyspark]` | Spark's Iceberg reader, lazy |

The type of the returned value selects the writer. The type annotation of the downstream parameter selects the reader.

About writes:

- A write needs memory for all of its data. pyiceberg 0.12 also reads a `RecordBatchReader` or a DuckDB relation into memory.
- Iceberg v2 stores timestamps in microseconds, and pyiceberg accepts only UTC time zones. The handler writes nanosecond timestamps as microseconds, and zoned timestamps as UTC.
- The pandas index is not written.

### Write and read with PySpark

A `pyspark.sql.DataFrame` is written and read through Spark's Iceberg connector, so Spark must have the Iceberg runtime and a catalog configured. Spark's catalog and the handler's `catalog_properties` must point at the same tables. Pass the session and the Spark catalog name in `handler_config={"spark_session": spark, "spark_catalog": "<name>"}`; without a session the handler uses the active one.

### Read with DuckDB

A `duckdb.DuckDBPyRelation` input reads the table's current metadata file with DuckDB's [`iceberg_scan`](https://duckdb.org/docs/current/core_extensions/iceberg/overview). The relation is lazy. `iceberg/columns`, the partition predicate, and file statistics prune the files that DuckDB reads. `iceberg/snapshot_id` reads an older snapshot.

```python
import duckdb
import rivers as rs

@rs.Asset(io_handler=io)
def summary(orders: duckdb.DuckDBPyRelation, users: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    return orders.join(users, "user_id").aggregate("country, sum(amount) AS total")
```

Inputs that one step reads share one DuckDB connection per thread, so the step can join them. To set extensions, secrets, or settings, pass a [`DuckDBResource`](duckdb.md#resource) as `handler_config={"duckdb": DuckDBResource(...)}`.

The handler turns the table's storage properties into a DuckDB secret for the table's bucket or container. These properties include the credentials that a REST catalog gives out for the table.

- S3 (`s3.*`, with `client.*` as fallback): the access key, secret, session token, region, endpoint, and `s3.force-virtual-addressing`. Without a key, DuckDB uses the AWS credential chain.
- Azure (`adls.*`, for `abfs://` and `abfss://` tables): an account key, a SAS token, or a service principal. Without one, DuckDB uses the Azure credential chain.
- GCS: not supported. DuckDB reads GCS only with HMAC keys; set them with `DuckDBResource(secrets=...)`.

## Write modes

Set the default mode on the handler, or change it for one asset with metadata:

```python
io = IcebergIOHandler(catalog_name="lake", namespace="analytics", mode="append")

@rs.Asset(io_handler=io, metadata={"iceberg/mode": "overwrite"})
def events() -> pl.DataFrame:
    ...
```

| Mode | Behavior |
|------|----------|
| `overwrite` | Replace the table, or the rows of the partition (the default) |
| `append` | Add rows |
| `upsert` | Update the rows that match on the join columns, and insert the others |

Each write is one commit: schema changes, table properties, and data. When another writer commits first, pyiceberg tries the commit again, 4 times by default. Set the number with the `commit.retry.num-retries` table property, for example when many partitions of one table are written at the same time.

### Upserts

```python
from rivers.io_handlers.iceberg import IcebergIOHandler, UpsertConfig

io = IcebergIOHandler(
    catalog_name="lake",
    namespace="analytics",
    mode="upsert",
    upsert_config=UpsertConfig(join_cols=["id"]),
)

@rs.Asset(io_handler=io, metadata={"iceberg/join_cols": '["order_id"]'})
def orders() -> pa.Table:
    ...
```

- `join_cols` are the columns that identify a row. Without them, pyiceberg uses the table's identifier fields.
- `UpsertConfig(when_matched_update_all=False)` only inserts new rows. `UpsertConfig(when_not_matched_insert_all=False)` only updates rows.
- The data must not have two rows with the same join columns.
- Rows that did not change are not written. When no row changes, the write makes no snapshot and the data version stays the same, so downstream assets do not become stale.
- An upsert matches rows in the whole table. For a partitioned asset, put the partition column in the join columns.
- An upsert does not change the schema. Add new columns with an `overwrite` or `append` write and `schema_mode` first.

## Schema evolution

| `schema_mode` | Behavior |
|---------------|----------|
| `None` (default) | A write with a new column fails. A column that the data does not have is written as nulls. |
| `"merge"` | Add new columns and widen types before the write |
| `"overwrite"` | Also delete the columns that the data does not have. Needs `mode="overwrite"`. |

Set it on the handler or with the `iceberg/schema_mode` metadata key. A type change that Iceberg cannot widen, for example from `long` to `string`, fails.

## Partitioned writes

Combine with `PartitionsDefinition` and `iceberg/partition_expr`:

```python
@rs.Asset(
    io_handler=io,
    partitions_def=rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1)),
    metadata={"iceberg/partition_expr": "ts"},
)
def events() -> pl.DataFrame:
    ...
```

`iceberg/partition_expr` names the column of the partition. For multi-partitions, it is a JSON object from dimension name to column:

```python
metadata={"iceberg/partition_expr": json.dumps({"region": "region", "day": "event_date"})}
```

- An overwrite replaces only the rows of the partition. A backfill run with many partition keys replaces the rows of all its keys.
- When the table is partitioned on the same columns, pyiceberg drops the data files of the partition. Otherwise it rewrites the files without the partition's rows.
- A partitioned input reads only the rows of its partition.
- On date and timestamp columns, the partition is a time range. On other columns, such as strings, the keys are compared as text in the partition key format.

The first write creates the table with a partition spec from the asset's partitions:

| Partition dimension | Column type | Iceberg transform |
|---------------------|-------------|-------------------|
| Static or dynamic | any | `identity` |
| Time window | timestamp | the coarsest of `year`, `month`, `day`, `hour` that all windows align to |
| Time window | date | the coarsest of `year`, `month`, `day` that all windows align to |
| Time window | other | `identity` |

For example, daily partitions on a timestamp column give `day(ts)`, hourly partitions give `hour(ts)`, weekly partitions give `day(ts)`, and monthly partitions give `month(ts)`. The handler never changes the partition spec of a table that exists.

## Table and snapshot properties

```python
io = IcebergIOHandler(
    catalog_name="lake",
    namespace="analytics",
    table_properties={"write.parquet.compression-codec": "zstd"},
    snapshot_properties={"producer": "rivers"},
)

@rs.Asset(
    io_handler=io,
    metadata={
        "iceberg/table_properties": '{"commit.retry.num-retries": "10"}',
        "iceberg/snapshot_properties": '{"team": "growth"}',
    },
)
def events() -> pl.DataFrame:
    ...
```

- Table properties are set when the table is created, and set again when they change. Properties that you remove from the config stay on the table.
- Snapshot properties are added to the summary of each snapshot that the handler writes.

## Reading data

Downstream assets read the data by type annotation:

```python
@rs.Asset(io_handler=io)
def summary(users: pl.DataFrame) -> pl.DataFrame:
    return users.group_by("region").len()
```

The metadata of the upstream asset selects the table and its partitions. An input with its own metadata, from `AssetDef.input(..., metadata=...)`, uses that metadata instead. So repeat the upstream keys that the read needs, such as `iceberg/partition_expr`.

### Column selection

```python
@rs.Asset(
    io_handler=io,
    deps=[rs.AssetDef.input("users", metadata={"iceberg/columns": '["id", "name"]'})],
)
def user_names(users: pl.DataFrame) -> pl.DataFrame:
    ...
```

### Time travel

Each write records its snapshot id as `iceberg/snapshot_id` in the materialization's metadata. `iceberg/snapshot_id` on an input reads that snapshot:

```python
@rs.Asset(
    io_handler=io,
    deps=[rs.AssetDef.input("users", metadata={"iceberg/snapshot_id": "4720155209217613612"})],
)
def users_at_release(users: pa.Table) -> pa.Table:
    ...
```

## Output metadata and data versions

Each write records `iceberg/table`, `iceberg/mode`, `iceberg/snapshot_id`, `iceberg/num_rows` and `iceberg/size_bytes` (of the whole table), `iceberg/write_duration_s`, `iceberg/metadata_location`, and `rivers/schema`. An upsert also records `iceberg/rows_updated` and `iceberg/rows_inserted`.

The snapshot id is the asset's [data version](../concepts/io-handlers.md#register-data-version-during-handle_output). A write that changes the table makes a new snapshot, so downstream assets become stale.

## Maintenance actions

Subclass [`IcebergAsset`](../api-reference/iceberg.md#icebergasset) to get the `delete` and `expire_snapshots` actions. They use the asset's `IcebergIOHandler`: the same catalog, table, and snapshot properties as the writes.

```python
from rivers.io_handlers.iceberg import IcebergAsset  # also exported as rs.IcebergAsset

class Orders(IcebergAsset):
    io_handler = LAKE

    @classmethod
    def materialize(cls) -> pl.DataFrame:
        return pl.DataFrame({"id": [1, 2, 3]})


repo = rs.CodeRepository(assets=[Orders])
repo.run_action("expire_snapshots")
repo.run_action("expire_snapshots", config={"assets": {"orders": {"config": {"retention_hours": 24}}}})
repo.run_action("delete", partition_key=pk)
```

| Verb | Declaration | Behavior |
|------|-------------|----------|
| `delete` | `Unmaterialize` + `Exclusive` + `DownstreamFirst` + `Optional` key | Deletes the rows of the partition, or every row without a key. A keyed delete needs `iceberg/partition_expr`. |
| `expire_snapshots` | `Unchanged` + `Exclusive` + `Keyless` | Removes old snapshots from the table's metadata. |

`expire_snapshots` takes two config fields:

- `retention_hours`: expire the snapshots that are older. The default is the table's `history.expire.max-snapshot-age-ms` property, 5 days by default.
- `retain_last`: always keep this many of the newest snapshots of the table's history. The default is the table's `history.expire.min-snapshots-to-keep` property, 1 by default.

Branch and tag heads are never expired. pyiceberg removes only the snapshots: their data files stay in storage. pyiceberg cannot compact files or remove orphan files yet, so `IcebergAsset` has no `optimize` or `vacuum`. Use an engine such as Spark for those.

On a table that does not exist yet, `expire_snapshots` reports `unchanged`, and `delete` still applies `Unmaterialize`, so the asset's state clears. A subclass that defines a verb replaces the built-in one.

### Custom verbs

For other verbs, the handler gives action bodies the table and the partition filter that the writes use:

```python
class Events(IcebergAsset):
    io_handler = LAKE

    @classmethod
    def materialize(cls) -> pl.DataFrame: ...

    @rs.action(outcome=rs.Outcome.Unchanged)
    @classmethod
    def tag_release(cls, ctx: rs.ActionContext) -> None:
        table = ctx.io_handler.load_table(ctx.asset_name, ctx.asset_metadata)
        table.manage_snapshots().create_tag(table.current_snapshot().snapshot_id, "release").commit()
```

- `load_table(asset_name, asset_metadata)`: the pyiceberg `Table`, with the `iceberg/namespace` and `iceberg/table` overrides.
- `partition_filter(asset_metadata, partition, schema)`: the pyiceberg expression for the rows of the partition, with `iceberg/partition_expr`.
- `resolved_snapshot_properties(asset_metadata)`: the snapshot properties of the asset's commits.
