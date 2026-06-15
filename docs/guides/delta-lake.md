# Delta Lake

The `DeltaIOHandler` persists asset outputs as Delta Lake tables. It supports PyArrow, Polars, pandas, and DataFusion data types, partitioned writes, and full MERGE INTO operations.

## Setup

```bash
pip install rivers[delta]

# Plus at least one of:
pip install rivers[pyarrow]
pip install rivers[polars]
pip install rivers[pandas]
pip install rivers[datafusion]
pip install rivers[pyspark]
```

## Basic usage

```python
import polars as pl
import rivers as rs
from rivers.io_handlers.delta import DeltaIOHandler

io = DeltaIOHandler(table_uri="/data/delta")

@rs.Asset(io_handler=io)
def users() -> pl.DataFrame:
    return pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Carol"],
    })
```

### Usage with Spark

```python
import pandas as pd
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
import rivers as rs
from rivers.io_handlers.delta import DeltaIOHandler

spark_session = ... # your spark session initialization
io = DeltaIOHandler(
    table_uri="/data/delta",
    handler_config={"spark_session": spark_session}
)

@rs.Asset(io_handler=io)
def users() -> SparkDataFrame:
    return spark.createDataFrame(
        pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
        })
    )
```

In above cases, the handler creates a Delta table at `/data/delta/users/`.

## Supported types

| Type | Extra | IO Type |
|------|-------|---------|
| `pyarrow.Table` | `rivers[pyarrow]` | Arrow |
| `pyarrow.RecordBatchReader` | `rivers[pyarrow]` | Arrow |
| `polars.DataFrame` | `rivers[polars]` | Arrow |
| `polars.LazyFrame` | `rivers[polars]` | Arrow |
| `pandas.DataFrame` | `rivers[pandas]` | Arrow |
| `datafusion.DataFrame` | `rivers[datafusion]` | Arrow |
| `pyspark.sql.DataFrame` | `rivers[pyspark]` | Spark |

The type is detected automatically from the object passed to `handle_output`, and `load_input` uses the `type_hint` from the downstream parameter annotation.

When read back as a `datafusion.DataFrame`, the Delta table is registered with a DataFusion `SessionContext` and returned as a lazy query — column projection and the partition predicate are pushed into the scan, and execution happens when the consumer collects or streams it (like a `polars.LazyFrame`).

```python
import datafusion
import rivers as rs

@rs.Asset
def enriched(users: datafusion.DataFrame, orders: datafusion.DataFrame) -> datafusion.DataFrame:
    return users.join(orders, on="id", how="inner")
```

The backing `SessionContext` is attached to the returned frame as `rivers_ctx`. Reach for it only when you want the session handle itself — for example, to register an extra table and query it by name with `ctx.sql(...)`.

## Write modes

Set the default mode on the handler, or override per-asset via metadata:

```python
# Handler-level default
io = DeltaIOHandler(table_uri="/data/delta", mode="append")

# Per-asset override
@rs.Asset(io_handler=io, metadata={"delta/mode": "overwrite"})
def events() -> pl.DataFrame:
    ...
```

### Arrow type handlers

| Mode | Behavior |
|------|----------|
| `overwrite` | Replace the table (or partition) |
| `append` | Add rows to existing table |
| `error` | Fail if table exists |
| `ignore` | Skip write if table exists |
| `merge` | MERGE INTO (see below) |
| `create_or_replace` | Drop and recreate schema, then append |

### Spark type handlers

#### Supported table write modes

| Mode | Behavior |
|------|----------|
| `overwrite` | Replace the table (or partition) |
| `append` | Add rows to existing table |
| `error` | Fail if table exists |
| `ignore` | Skip write if table exists |

#### Support schema modes

| Mode | Behavior |
|------|----------|
| `overwrite` | Replace the table (or partition) |
| `merge` | MERGE INTO (see below) |

## Partitioned writes

Combine with `PartitionsDefinition` and `partition_expr`:

```python
@rs.Asset(
    io_handler=io,
    partitions_def=rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1)),
    metadata={"delta/partition_expr": "date"},
)
def daily_events() -> pl.DataFrame:
    ...
```

When writing in `overwrite` mode with a partition key, the handler generates a predicate to overwrite only the target partition.

## Merge operations

For upserts, deduplication, and other MERGE INTO patterns:

```python
from rivers.io_handlers.delta import DeltaIOHandler, MergeConfig

io = DeltaIOHandler(
    table_uri="/data/delta",
    mode="merge",
    merge_config=MergeConfig(
        merge_type="upsert",
        predicate="s.id = t.id",
    ),
)
```

### Merge types

| Type | Behavior |
|------|----------|
| `upsert` | Update matched rows, insert unmatched |
| `deduplicate_insert` | Insert only unmatched rows |
| `update_only` | Update matched rows only |
| `replace_delete_unmatched` | Update matched, delete unmatched in target |
| `custom` | Full control via `MergeOperationsConfig` |

### Custom merge operations

```python
from rivers.io_handlers.delta import (
    MergeConfig,
    MergeOperationsConfig,
    WhenMatchedUpdateAll,
    WhenNotMatchedInsertAll,
)

io = DeltaIOHandler(
    table_uri="/data/delta",
    mode="merge",
    merge_config=MergeConfig(
        merge_type="custom",
        predicate="s.id = t.id",
        operations=MergeOperationsConfig(
            when_matched_update_all=[
                WhenMatchedUpdateAll(predicate="s.updated_at > t.updated_at"),
            ],
            when_not_matched_insert_all=[
                WhenNotMatchedInsertAll(),
            ],
        ),
    ),
)
```

## Storage options

Pass storage credentials for remote backends:

```python
io = DeltaIOHandler(
    table_uri="s3://my-bucket/delta",
    storage_options={
        "aws_region": "us-east-1",
        "aws_access_key_id": "...",
        "aws_secret_access_key": "...",
    },
)
```

## Handler configuration

Handler configuration allows you to pass custom handler properties
like a `SparkSession` object when using a Spark type handler.

### Example reading `my_table` in a cloud blob storage with Spark

```python
spark_session = ... # your spark session initialized with cloud credentials
io_spark = DeltaIOHandler(
    table_uri="/cloud/path/to/my_table",
    handler_config={"spark_session": spark_session}
)
ctx = rs.InputContext(
    asset_name="tbl",
    downstream_asset="consumer",
    type_hint=pyspark.sql.DataFrame,
)
spark_df = handler.load_input(ctx)
```

## Table configuration

Set Delta table properties:

```python
io = DeltaIOHandler(
    table_uri="/data/delta",
    table_config={
        "delta.deletedFileRetentionDuration": "interval 30 days",
    },
)
```

Override per-asset via metadata:

```python
@rs.Asset(
    io_handler=io,
    metadata={
        "delta/table_configuration": '{"delta.enableChangeDataFeed": "true"}',
    },
)
def events() -> pl.DataFrame:
    ...
```

## Reading data

Downstream assets read data by type annotation:

```python
@rs.Asset(io_handler=io)
def summary(users: pl.DataFrame) -> pl.DataFrame:
    return users.group_by("region").len()
```

### Column selection

Load only specific columns:

```python
@rs.Asset(
    io_handler=io,
    metadata={"delta/columns": '["id", "name"]'},
)
def user_names(users: pl.DataFrame) -> pl.DataFrame:
    ...
```

### Time travel

Read a specific table version:

```python
@rs.Asset(io_handler=io, metadata={"delta/version": "3"})
def historical(users: pl.DataFrame) -> pl.DataFrame:
    ...
```
