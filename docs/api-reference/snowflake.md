# Snowflake

## `SnowflakeIOHandler`

Persists asset outputs as tables in a Snowflake database. See the [Snowflake guide](../guides/snowflake.md) for how a write works.

```python
from rivers.integrations.snowflake import SnowflakeIOHandler, SnowflakeResource

io = SnowflakeIOHandler(
    resource=SnowflakeResource(database="ANALYTICS", warehouse="TRANSFORM_WH"),
    mode="overwrite",
)
```

**Constructor:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `resource` | `SnowflakeResource` | required | Opens the connections. Each asset becomes the table `{schema_name}.{asset_name}` in the resource's `database`, uppercased. |
| `schema_name` | `str` | `"PUBLIC"` | Schema for the tables. |
| `mode` | `SnowflakeWriteMode` | `OVERWRITE` | `OVERWRITE` or `APPEND`. The strings `"overwrite"` and `"append"` work too. |

**Supported types:**

| Type | Write | Read |
|------|-------|------|
| `pyarrow.Table` | yes | yes |
| `pyarrow.RecordBatchReader` | yes | yes |
| arro3 `RecordBatchReader`, or other `__arrow_c_stream__` objects | yes | no |
| `polars.DataFrame` | yes | yes |
| `polars.LazyFrame` | yes | yes |
| `pandas.DataFrame` | yes | yes |

Reads fetch all rows as Arrow and lowercase the column names.

**Asset metadata overrides:**

These metadata keys override handler defaults per-asset:

| Key | Type | Description |
|-----|------|-------------|
| `snowflake/schema` | `str` | Schema override. |
| `snowflake/table` | `str` | Table name override (default: the asset name). |
| `snowflake/mode` | `str` | Write mode override. |
| `snowflake/partition_expr` | `str \| JSON dict` | Partition column mapping. |
| `snowflake/columns` | `JSON list` | Column selection for reads. |

**Output metadata:**

| Key | Type | Description |
|-----|------|-------------|
| `snowflake/table` | `str` | `DATABASE.SCHEMA.TABLE`. |
| `snowflake/num_rows` | `int` | Total rows in the table after the write. |
| `snowflake/write_duration_s` | `float` | Write duration in seconds. |
| `rivers/schema` | `Schema` | Arrow schema of the written rows. |

**Raises:**

- `TypeError` from a write: an unsupported output type, or a column type with no Snowflake type (for example `duration`). Nothing is written.
- `ValueError` from a write: an output column that the table does not have, a partitioned asset without `snowflake/partition_expr`, or an unknown `snowflake/mode`.
- `TypeError` from a read: a missing or unsupported type hint.

### Methods for action bodies

An [action](../concepts/actions.md) receives the asset's resolved handler as `ctx.io_handler`. These methods expose the session, table, and partition resolution that the write path uses.

#### `get_connection()`

Context manager: a connection in the handler's session, closed when the block exits. The session uses UTC, the time zone of daily and hourly partition bounds, and fetches `NUMBER` columns with a scale as decimals, as reads do.

#### `table_name(asset_name, asset_metadata)`

Returns the uppercased `(schema, table)` pair, honoring `snowflake/schema` and `snowflake/table` overrides in `asset_metadata`. Pass `ctx.asset_name` and `ctx.asset_metadata`.

```python
@rs.action(outcome=rs.Outcome.Unmaterialize)
@classmethod
def truncate(cls, ctx: rs.ActionContext) -> None:
    schema, table = ctx.io_handler.table_name(ctx.asset_name, ctx.asset_metadata)
    with ctx.io_handler.get_connection() as con:
        con.cursor().execute(f'TRUNCATE TABLE IF EXISTS "{schema}"."{table}"')
```

#### `partition_predicate(asset_metadata, partition)`

Returns a SQL predicate covering the partition(s), honoring `snowflake/partition_expr`. Pass `ctx.asset_metadata` and `ctx.partition`. Raises `ValueError` without `snowflake/partition_expr`.

Daily and hourly bounds are UTC times: run the predicate on `ctx.io_handler.get_connection()`. In another time zone, it selects other rows of a `TIMESTAMP_LTZ` column.

```python
@rs.action(outcome=rs.Outcome.Unmaterialize)
@classmethod
def delete(cls, ctx: rs.ActionContext) -> None:
    schema, table = ctx.io_handler.table_name(ctx.asset_name, ctx.asset_metadata)
    predicate = ctx.io_handler.partition_predicate(ctx.asset_metadata, ctx.partition)
    with ctx.io_handler.get_connection() as con:
        con.cursor().execute(f'DELETE FROM "{schema}"."{table}" WHERE {predicate}')
```

`partition` is required: reach this only from a keyed action run. [`SnowflakeAsset`](#snowflakeasset) has this `delete` built in.

---

## `SnowflakeAsset`

Asset base class with the `delete` verb built in. Subclass it, define
`materialize`, and `delete` appears as an [action](../concepts/actions.md),
resolved against the asset's `SnowflakeIOHandler` (its own `io_handler` or the
repository default). Sets `kinds = "snowflake"`.

```python
from rivers.integrations.snowflake import SnowflakeAsset

class Orders(SnowflakeAsset):
    io_handler = WAREHOUSE

    @classmethod
    def materialize(cls) -> pl.DataFrame: ...
```

| Verb | Declaration | Behavior |
|------|-------------|----------|
| `delete` | `Unmaterialize` + `Exclusive` + `DownstreamFirst` + `Optional` key | Deletes the keyed partition's rows via `partition_predicate`, or every row without a key — on partitioned assets both forms are valid. The table keeps its columns, grants, and policies. From the UI or gRPC, the keyless form needs an explicit whole-asset choice. |

A verb requires the asset to resolve a `SnowflakeIOHandler` — anything else fails the
step with a `TypeError` naming the requirement. On a table that does not exist,
`delete` still applies its `Unmaterialize` outcome, so dangling state clears. A keyed
`delete` on an asset without `snowflake/partition_expr` fails with a `ValueError` naming
the key. A subclass redefining a verb replaces the built-in.

There is no `optimize` or `vacuum`: Snowflake merges small micro-partitions itself,
and it frees old storage when Time Travel and Fail-safe end.

---

## `SnowflakeWriteMode`

`StrEnum` with `OVERWRITE = "overwrite"` and `APPEND = "append"`.

---

## `SnowflakeResource`

Opens Snowflake connections with `snowflake.connector`. Fields are read from `SNOWFLAKE_*` environment variables when not given.

```python
from rivers.integrations.snowflake import SnowflakeResource

warehouse = SnowflakeResource(
    account="myorg-myaccount",
    user="RIVERS",
    private_key_path="/secrets/rsa_key.p8",
    database="ANALYTICS",
    warehouse="TRANSFORM_WH",
)
```

**Constructor:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `account` | `str \| None` | `None` | Account identifier, e.g. `"myorg-myaccount"`. |
| `user` | `str \| None` | `None` | Login name. |
| `password` | `SecretStr \| None` | `None` | Password, or a programmatic access token. |
| `private_key` | `SecretStr \| None` | `None` | PEM private key, or base64 of the PEM text. |
| `private_key_path` | `str \| None` | `None` | Path to a PEM private key file. |
| `private_key_password` | `SecretStr \| None` | `None` | Passphrase of an encrypted private key. |
| `authenticator` | `str \| None` | `None` | Authentication method, e.g. `"externalbrowser"` or `"oauth"`. |
| `database` | `str \| None` | `None` | Default database. |
| `schema_name` | `str \| None` | `None` | Default schema, passed as `schema`. |
| `warehouse` | `str \| None` | `None` | Warehouse that runs the queries. |
| `role` | `str \| None` | `None` | Role of the session. |
| `connection_args` | `dict[str, Any]` | `{}` | Other keyword arguments for `snowflake.connector.connect`. |

The `SecretStr` fields show masked in the resource's `repr` and in the UI's launch form. Giving more than one of `password`, `private_key`, and `private_key_path` raises `ValueError`. A private key is loaded once per process and passed to the connector as a key object, so the connector uses key-pair authentication.

### Methods

#### `get_connection(**overrides)`

Context manager: a `snowflake.connector.SnowflakeConnection` that is closed when the block exits. With autocommit off, the connector commits when the block exits normally and rolls back when it raises. Keyword arguments replace the resource's arguments for `snowflake.connector.connect`.

```python
with warehouse.get_connection(warehouse="LARGE_WH") as con:
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM orders")
```
