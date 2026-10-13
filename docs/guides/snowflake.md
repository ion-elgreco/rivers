# Snowflake

`rivers.integrations.snowflake` connects rivers to [Snowflake](https://www.snowflake.com):

- `SnowflakeResource` opens connections with a password, a key pair, or another authenticator.
- `SnowflakeIOHandler` stores each asset as a Snowflake table.
- `SnowflakeAsset` adds a `delete` [action](../concepts/actions.md).

## Setup

```bash
pip install rivers[snowflake]
```

The extra installs [`snowflake-connector-python`](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector), pyarrow, and pandas: the connector needs pandas to fetch rows as Arrow. To read or write polars data frames, also install `rivers[polars]`.

## Resource

```python
import pyarrow as pa
import rivers as rs
from rivers.integrations.snowflake import SnowflakeResource

warehouse = SnowflakeResource(database="ANALYTICS", warehouse="TRANSFORM_WH")

@rs.Asset
def top_users(warehouse: SnowflakeResource) -> pa.Table:
    with warehouse.get_connection() as con:
        cur = con.cursor()
        cur.execute("SELECT * FROM users ORDER BY score DESC LIMIT 10")
        return cur.fetch_arrow_all(force_return_table=True)

repo = rs.CodeRepository(assets=[top_users], resources={"warehouse": warehouse})
```

`get_connection()` opens a `snowflake.connector` connection and closes it when the block exits. Keyword arguments replace the resource's connection arguments for that connection, for example `get_connection(warehouse="LARGE_WH")`.

Fields that you do not give are read from `SNOWFLAKE_*` environment variables, for example `SNOWFLAKE_ACCOUNT` and `SNOWFLAKE_PASSWORD`.

| Field | Default | Description |
|-------|---------|-------------|
| `account` | `None` | Account identifier, e.g. `"myorg-myaccount"`. |
| `user` | `None` | Login name. |
| `password` | `None` | Password, or a [programmatic access token](https://docs.snowflake.com/en/user-guide/programmatic-access-tokens). |
| `private_key` | `None` | PEM private key for [key-pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth). Base64 of the PEM text works too. |
| `private_key_path` | `None` | Path to a PEM private key file. |
| `private_key_password` | `None` | Passphrase of an encrypted private key. |
| `authenticator` | `None` | Authentication method, e.g. `"externalbrowser"` or `"oauth"`. |
| `database` | `None` | Default database. |
| `schema_name` | `None` | Default schema. |
| `warehouse` | `None` | Warehouse that runs the queries. |
| `role` | `None` | Role of the session. |
| `connection_args` | `{}` | Other keyword arguments for [`snowflake.connector.connect`](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api#connect), e.g. `{"login_timeout": 30}`. |

Give one of `password`, `private_key`, and `private_key_path`.

### Key-pair authentication

```python
warehouse = SnowflakeResource(
    account="myorg-myaccount",
    user="RIVERS",
    private_key_path="/secrets/rsa_key.p8",
    database="ANALYTICS",
)
```

In an environment variable, a PEM key keeps its newlines, or you can give its base64 form: `SNOWFLAKE_PRIVATE_KEY=$(base64 < rsa_key.p8)`. Set `SNOWFLAKE_PRIVATE_KEY_PASSWORD` for an encrypted key.

### Secrets

`password`, `private_key`, and `private_key_password` are pydantic `SecretStr` values: the resource's `repr` and the UI's launch form show them masked. Other connection secrets, such as an OAuth token in `connection_args`, have no such protection.

## IO handler

`SnowflakeIOHandler` stores each asset as the table `<schema>.<asset name>` in the resource's `database`:

```python
import polars as pl
from rivers.integrations.snowflake import SnowflakeIOHandler

io = SnowflakeIOHandler(resource=warehouse)

@rs.Asset(io_handler=io)
def orders() -> pl.DataFrame:
    return pl.DataFrame({"day": ["d1", "d1", "d2"], "amount": [5, 7, 3]})

@rs.Asset(io_handler=io)
def daily(orders: pl.DataFrame) -> pl.DataFrame:
    return orders.group_by("day").agg(pl.col("amount").sum())
```

Schema, table, and column names are uppercased, because Snowflake stores names without quotes in uppercase. The asset `orders` becomes `PUBLIC.ORDERS` with the columns `DAY` and `AMOUNT`, and your SQL can use them without quotes. A read lowercases the column names again: a column `userId` comes back as `userid`.

A write:

1. Creates the schema, if it does not exist.
2. Writes the rows as Parquet files, each from about 100 MB of Arrow data, and uploads each file with `PUT` to a temporary stage when it is complete. Only one file is in memory at a time.
3. Creates the table from the output's Arrow types, if it does not exist.
4. Deletes the old rows (overwrite) and loads all the files with `COPY INTO`, in one transaction.

If reading the output fails, the write stops before step 3: the table and its rows do not change.

The table keeps its columns, grants, and policies between writes. An output with a column that the table does not have fails. An output column also fails if the table's column type would change its values, for example floats in a `NUMBER(38,0)` column or numbers in a `VARCHAR` column. A `NUMBER` column takes integers and decimals with the same or a smaller scale, a `VARIANT` column takes all types, and a `TIME` or timestamp column takes all precisions but stores its own. Add or alter the column with `ALTER TABLE`, or drop the table to create it again on the next write. A column that the output does not have gets `NULL`.

The role needs `USAGE` on the database and the schema, `CREATE STAGE` on the schema, and `SELECT`, `INSERT`, and `DELETE` on the table. To create a missing table, it needs `CREATE TABLE` on the schema. To create a missing schema, it needs `CREATE SCHEMA` on the database.

| Type | Write | Read |
|------|-------|------|
| `pyarrow.Table` | yes | yes |
| `pyarrow.RecordBatchReader` | yes | yes |
| arro3 `RecordBatchReader`, or other `__arrow_c_stream__` objects | yes | no |
| `polars.DataFrame` | yes | yes |
| `polars.LazyFrame` | yes | yes |
| `pandas.DataFrame` | yes | yes |

A `LazyFrame` output runs in batches with `collect_batches`, so its whole result is never in memory. A read needs a type hint on the downstream parameter. It fetches the rows as Arrow. A `RecordBatchReader` or `LazyFrame` input holds rows that are already fetched. A pandas index is not written.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `resource` | required | The `SnowflakeResource`. Its `database` holds the tables. |
| `schema_name` | `"PUBLIC"` | The schema for the tables. |
| `mode` | `SnowflakeWriteMode.OVERWRITE` | `OVERWRITE` replaces the table's rows, or the partition's rows. `APPEND` adds rows. The strings `"overwrite"` and `"append"` work too. |

### Column types

The first write creates the columns from the Arrow types, as Snowflake's [ADBC driver](https://arrow.apache.org/adbc/current/driver/snowflake.html) does:

| Arrow type | Snowflake type |
|------------|----------------|
| integers | `NUMBER(38,0)` |
| floating point | `FLOAT` |
| `decimal(p, s)` | `NUMBER(p,s)` |
| `bool` | `BOOLEAN` |
| strings, `null` | `VARCHAR` |
| binary | `BINARY` |
| `date32`, `date64` | `DATE` |
| `time32`, `time64` | `TIME(0)` to `TIME(9)`, from the unit |
| `timestamp` without a time zone | `TIMESTAMP_NTZ(0)` to `TIMESTAMP_NTZ(9)` |
| `timestamp` with a time zone | `TIMESTAMP_LTZ(0)` to `TIMESTAMP_LTZ(9)` |
| lists | `ARRAY` |
| `struct`, `map` | `OBJECT` |
| dictionary | its value type |

Other Arrow types, such as `duration`, fail before the write starts.

A read returns `NUMBER` columns without a scale as `int64` (or `decimal128` when Snowflake cannot tell the largest value), `NUMBER(p,s)` as `decimal128(p,s)`, timestamps in the unit of the column's precision (`TIMESTAMP_NTZ(6)` in microseconds), and `ARRAY` and `OBJECT` as JSON text.

### Time zones

The handler's sessions use the UTC time zone. A timestamp with a time zone is stored as an instant in a `TIMESTAMP_LTZ` column, and a read returns it in UTC. A timestamp without a time zone is stored as is in a `TIMESTAMP_NTZ` column.

### Partitioned assets

Map the partition key to columns with `snowflake/partition_expr`, as with [Delta](delta-lake.md#partitioned-writes):

```python
from datetime import datetime

@rs.Asset(
    io_handler=io,
    partitions_def=rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1)),
    metadata={"snowflake/partition_expr": "day"},
)
def events(context: rs.AssetExecutionContext) -> pl.DataFrame:
    ...
```

An overwrite deletes the partition's rows and loads the new rows in one transaction. A backfill run with several keys replaces all of them. Daily and hourly keys become ranges, so the column can be text, `DATE`, or a timestamp. The range bounds are in UTC. A `DATE` or timestamp column gets timestamp bounds, so the partition can use any `fmt`. A text column or an expression gets the bounds as text in the partition's `fmt`. The expression is SQL, for example `TO_DATE(created_at)`. A name without quotes in it matches the uppercased column: `day` finds `DAY`.

A delete locks the table until its transaction ends, so overwrites of one table run one after the other.

### Metadata keys

| Key | Description |
|-----|-------------|
| `snowflake/schema` | Schema for this asset's table. |
| `snowflake/table` | Table name, instead of the asset name. |
| `snowflake/mode` | `"overwrite"` or `"append"`. |
| `snowflake/partition_expr` | Partition column, or a JSON object from dimension to column. |
| `snowflake/columns` | JSON list of columns to read. |

Each write records `snowflake/table` (`DATABASE.SCHEMA.TABLE`), `snowflake/num_rows` (rows in the whole table), `snowflake/write_duration_s`, and `rivers/schema` (the Arrow schema of the written rows).

## Maintenance actions

```python
from rivers.integrations.snowflake import SnowflakeAsset

class Orders(SnowflakeAsset):
    io_handler = io

    @classmethod
    def materialize(cls) -> pl.DataFrame: ...

repo.run_action("delete", selection=["orders"])
```

`SnowflakeAsset` has one action, `delete`. With a partition key, it deletes the partition's rows; this needs `snowflake/partition_expr`. Without a key, it deletes all rows. The table keeps its columns, grants, and policies. A missing table only clears the state.

There is no `optimize` or `vacuum`: Snowflake merges small micro-partitions itself, and it frees old storage when Time Travel and Fail-safe end.

## Limits

- The handler writes with overwrite or append. It has no MERGE.
- A read fetches all rows before the step gets them.
- The handler has no PySpark type. Convert a Spark DataFrame to pandas or Arrow first.
- A `TIMESTAMP_*(9)` column with dates outside the years 1677 to 2262 can fail to read: the connector reads such dates in microseconds, which can differ between result batches.
