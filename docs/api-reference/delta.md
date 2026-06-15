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
