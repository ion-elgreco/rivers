# Delta Lake

## `DeltaIOHandler`

Persists asset outputs as Delta Lake tables.

```python
from rivers.io_handlers.delta import DeltaIOHandler

io = DeltaIOHandler(
    table_uri="/data/delta",
    mode="overwrite",
)
```

**Constructor:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `table_uri` | `str` | required | Base URI for Delta tables. Each asset creates a subdirectory. |
| `mode` | `str` | `"overwrite"` | Write mode: `"overwrite"`, `"append"`, `"error"`, `"ignore"`, `"merge"`, `"create_or_replace"`. |
| `schema_mode` | `str \| None` | `None` | Schema evolution: `"overwrite"` or `"merge"`. |
| `storage_options` | `dict[str, str] \| None` | `None` | Credentials for remote storage. |
| `writer_properties` | `WriterProperties \| None` | `None` | Parquet writer settings. |
| `commit_properties` | `CommitProperties \| None` | `None` | Delta commit settings. |
| `table_config` | `dict[str, str] \| None` | `None` | Delta table properties. |
| `merge_config` | `MergeConfig \| None` | `None` | Merge configuration (required when `mode="merge"`). |
| `handler_config` | `dict[str, Any] \| None` | `None` | Useful to pass handler-related custom config (e.g. a pre-initialized `SparkSession` object) |

**Asset metadata overrides:**

These metadata keys override handler defaults per-asset:

| Key | Type | Description |
|-----|------|-------------|
| `delta/mode` | `str` | Write mode override. |
| `delta/schema_mode` | `str` | Schema mode override. |
| `delta/partition_expr` | `str \| JSON dict` | Partition column mapping. |
| `delta/table_configuration` | `JSON str` | Table properties override. |
| `delta/writer_properties` | `JSON str` | Writer properties override. |
| `delta/commit_properties` | `JSON str` | Commit properties override. |
| `delta/merge_predicate` | `str` | Override merge predicate for this asset. |
| `delta/columns` | `JSON list` | Column selection for reads. |
| `delta/version` | `str` | Table version for time travel reads. |

**Output metadata:**

| Key | Type | Description |
|-----|------|-------------|
| `delta/table_uri` | `str` | Full table URI. |
| `delta/mode` | `str` | Write mode used. |
| `delta/num_rows` | `int` | Total rows in table after write. |
| `delta/size_bytes` | `int` | Total table size in bytes. |
| `delta/write_duration_s` | `float` | Write duration in seconds. |
| `delta/version` | `int` | Delta table version after write. |
| `rivers/schema` | `Schema` | Arrow schema of the written table. |

### Methods for action bodies

An [action](../concepts/actions.md) receives the asset's resolved handler as
`ctx.io_handler`. These two methods expose the same URI and partition resolution
the write path uses, so a `delete`/`optimize` body targets exactly the rows the
materialize path would have written.

#### `asset_table_uri(asset_name, asset_metadata=None)`

Returns `{table_uri}/{leaf}`, honoring a `delta/root_name` override in
`asset_metadata`. Pass `ctx.asset_name` and `ctx.asset_metadata`.

```python
@rs.action(outcome=rs.Outcome.Unchanged)
@classmethod
def optimize(cls, ctx: rs.ActionContext) -> None:
    uri = ctx.io_handler.asset_table_uri(ctx.asset_name, ctx.asset_metadata)
    DeltaTable(uri, storage_options=ctx.io_handler.storage_options).optimize.compact()
```

#### `partition_predicate(asset_metadata, partition)`

Returns a SQL predicate covering the partition(s), honoring
`delta/partition_expr`. Pass `ctx.asset_metadata` and `ctx.partition`.

```python
@rs.action(outcome=rs.Outcome.Unmaterialize)
@classmethod
def delete(cls, ctx: rs.ActionContext) -> None:
    uri = ctx.io_handler.asset_table_uri(ctx.asset_name, ctx.asset_metadata)
    predicate = ctx.io_handler.partition_predicate(ctx.asset_metadata, ctx.partition)
    DeltaTable(uri, storage_options=ctx.io_handler.storage_options).delete(predicate)
```

`partition` is required: reach these only from a keyed action run. On a
non-partitioned asset, delete the whole table instead of building a predicate.

---

## `DeltaAsset`

Asset base class with the standard Delta maintenance verbs built in. Subclass it,
define `materialize`, and `optimize`, `vacuum`, and `delete` appear as
[actions](../concepts/actions.md), resolved against the asset's `DeltaIOHandler`
(its own `io_handler` or the repository default). Sets `kinds = "delta"`.

```python
from rivers.io_handlers.delta import DeltaAsset  # also exported as rs.DeltaAsset

class Orders(DeltaAsset):
    io_handler = WAREHOUSE

    @classmethod
    def materialize(cls) -> pl.DataFrame: ...
```

| Verb | Declaration | Behavior |
|------|-------------|----------|
| `optimize` | `Unchanged` + `Exclusive` + `Keyless` | `optimize.compact()`, or `z_order` when `z_order_by` is configured. Table-wide: runs without a partition key on partitioned assets too; a supplied key is rejected. |
| `vacuum` | `Unchanged` + `Exclusive` + `Keyless` | Removes files no longer referenced by the table. Table-wide, same key rule as `optimize`. |
| `delete` | `Unmaterialize` + `Exclusive` + `DownstreamFirst` + `Optional` key | Deletes the keyed partition's rows via `partition_predicate`, or every row without a key — on partitioned assets both forms are valid. |

A verb requires the asset to resolve a `DeltaIOHandler` — anything else fails the
step with a `TypeError` naming the requirement. On a table that does not exist yet,
`optimize` and `vacuum` report `ActionResult.unchanged()` instead of failing, so a
fleet-wide `repo.run_action("optimize")` skips never-materialized assets; `delete`
still applies its `Unmaterialize` outcome, so dangling state clears even when the
physical table is already gone. A subclass redefining a verb replaces the built-in.

### `OptimizeConfig`

Per-asset overrides via `run_action("optimize", config={"<asset>": {...}})`:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `target_size` | `int \| None` | `None` | Desired file size in bytes (`None` uses the deltalake default). |
| `z_order_by` | `list[str] \| None` | `None` | Z-order by these columns instead of plain compaction. |

### `VacuumConfig`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `retention_hours` | `int \| None` | `None` | Files older than this are removed (`None` uses the table's retention, 168h by default). |
| `enforce_retention_duration` | `bool` | `True` | Refuse retention windows shorter than the table's configured minimum. |

---

## `DeltaTypeHandler`

Parent abstract base for adding type support to `DeltaIOHandler`.

```python
from rivers.io_handlers.delta.base import DeltaTypeHandler

class MyTypeHandler(DeltaTypeHandler[MyType]):
    @property
    def supported_types(self) -> Sequence[type[MyType]]:
        return [MyType]

    def load_input(self, table_uri, storage_options, predicate,
                   target_type, columns=None, version=None) -> MyType:
        ...

    def handle_output(self, context: OutputContext,
                    obj: T, request: DeltaWriteRequest):
        ...
```

**Abstract members:**

| Member | Description |
|--------|-------------|
| `supported_types` | Property returning list of types this handler supports. |
| `load_input(...)` | Load data from a Delta table. |
| `handle_output(...)` | Write data to a Delta table. |

**Built-in type handler subclasses:**

| Type Handler Class | Description |
|--------|-------------|
| `ArrowDeltaTypeHandler` | For Arrow-based type support. |
| `PySparkDeltaTypeHandler` | For Spark-based types support. |

## `ArrowDeltaTypeHandler`

`DeltaTypeHandler`-based abstract handler class for adding
Arrow-based type support to `DeltaIOHandler`. Writes with
`handle_output` are pre-implemented using `deltalake (delta-rs)`.

```python
from rivers.io_handlers.delta.base import ArrowDeltaTypeHandler

class MyTypeHandler(ArrowDeltaTypeHandler[MyType]):
    @property
    def supported_types(self) -> Sequence[type[MyType]]:
        return [MyType]

    def to_arrow(self, obj: T) -> RecordBatchReader:
        ...

    def load_input(self, table_uri, storage_options, predicate,
                   target_type, columns=None, version=None) -> MyType:
        ...

    def handle_output(self, context: OutputContext,
                    obj: T, request: DeltaWriteRequest):
        ...
```

**Abstract members:**

| Member | Description |
|--------|-------------|
| `supported_types` | Property returning list of types this handler supports. |
| `to_arrow(obj)` | Convert object to `arro3.core.RecordBatchReader`. |
| `load_input(...)` | Load data from a Delta table. |

**Built-in handlers** (auto-registered when their library is installed):

| Handler | Module | Types |
|---------|--------|-------|
| `PyArrowTypeHandler` | `rivers.io_handlers.delta.pyarrow` | `pyarrow.Table`, `pyarrow.RecordBatchReader` |
| `PolarsTypeHandler` | `rivers.io_handlers.delta.polars` | `polars.DataFrame`, `polars.LazyFrame` |
| `PandasTypeHandler` | `rivers.io_handlers.delta.pandas` | `pandas.DataFrame` |
| `DataFusionTypeHandler` | `rivers.io_handlers.delta.datafusion` | `datafusion.DataFrame` |

## `PySparkDeltaTypeHandler`

`DeltaTypeHandler`-based handler class for adding
Spark-based type support to `DeltaIOHandler`. Reads with `load_input`
and writes with `handle_output`, both are implemented with Spark.

```python
from rivers.io_handlers.delta.pyspark import PySparkDeltaTypeHandler

class MyTypeHandler(ArrowDeltaTypeHandler[MyType]):
    @property
    def supported_types(self) -> Sequence[type[MyType]]:
        return [MyType]

    def load_input(self, table_uri, storage_options, predicate,
                   target_type, columns=None, version=None) -> MyType:
        ...

    def handle_output(self, context: OutputContext,
                    obj: T, request: DeltaWriteRequest):
        ...
```

---

## `PartitionExpr`

Maps partition dimensions to Delta table column names.

```python
from rivers.io_handlers.delta import PartitionExpr

# Single dimension
expr = PartitionExpr(expr="date")

# Multi-dimensional
expr = PartitionExpr(expr={"date": "event_date", "region": "region_code"})
```

**Attributes:**

| Attribute | Type | Description |
|-----------|------|-------------|
| `expr` | `str \| dict[str, str]` | Column name or dimension-to-column mapping. |

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `partition_columns` | `list[str]` | List of Delta column names. |

---

## `MergeConfig`

Configuration for MERGE INTO operations.

```python
from rivers.io_handlers.delta import MergeConfig

config = MergeConfig(
    merge_type="upsert",
    predicate="s.id = t.id",
    source_alias="s",
    target_alias="t",
)
```

**Attributes:**

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `merge_type` | `str` | required | One of: `"deduplicate_insert"`, `"update_only"`, `"upsert"`, `"replace_delete_unmatched"`, `"custom"`. |
| `predicate` | `str` | required | SQL merge condition. |
| `source_alias` | `str` | `"s"` | Alias for the source table. |
| `target_alias` | `str` | `"t"` | Alias for the target table. |
| `error_on_type_mismatch` | `bool` | `True` | Fail if source/target schemas differ. |
| `operations` | `MergeOperationsConfig \| None` | `None` | Required when `merge_type="custom"`. |

---

## `MergeOperationsConfig`

Fine-grained control over MERGE clauses.

**Attributes:**

| Attribute | Type |
|-----------|------|
| `when_not_matched_insert` | `list[WhenNotMatchedInsert] \| None` |
| `when_not_matched_insert_all` | `list[WhenNotMatchedInsertAll] \| None` |
| `when_matched_update` | `list[WhenMatchedUpdate] \| None` |
| `when_matched_update_all` | `list[WhenMatchedUpdateAll] \| None` |
| `when_matched_delete` | `list[WhenMatchedDelete] \| None` |
| `when_not_matched_by_source_delete` | `list[WhenNotMatchedBySourceDelete] \| None` |
| `when_not_matched_by_source_update` | `list[WhenNotMatchedBySourceUpdate] \| None` |

---

## Merge operation classes

### `WhenNotMatchedInsert`

| Attribute | Type |
|-----------|------|
| `predicate` | `str \| None` |
| `updates` | `dict[str, str]` |

### `WhenNotMatchedInsertAll`

| Attribute | Type |
|-----------|------|
| `predicate` | `str \| None` |
| `except_cols` | `list[str] \| None` |

### `WhenMatchedUpdate`

| Attribute | Type |
|-----------|------|
| `predicate` | `str \| None` |
| `updates` | `dict[str, str]` |

### `WhenMatchedUpdateAll`

| Attribute | Type |
|-----------|------|
| `predicate` | `str \| None` |
| `except_cols` | `list[str] \| None` |

### `WhenMatchedDelete`

| Attribute | Type |
|-----------|------|
| `predicate` | `str \| None` |

### `WhenNotMatchedBySourceDelete`

| Attribute | Type |
|-----------|------|
| `predicate` | `str \| None` |

### `WhenNotMatchedBySourceUpdate`

| Attribute | Type |
|-----------|------|
| `predicate` | `str \| None` |
| `updates` | `dict[str, str]` |
