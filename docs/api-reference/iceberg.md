# Apache Iceberg

## `IcebergIOHandler`

Persists asset outputs as Apache Iceberg tables, through a pyiceberg catalog. See the [Iceberg guide](../guides/iceberg.md).

```python
from rivers.io_handlers.iceberg import IcebergIOHandler

io = IcebergIOHandler(
    catalog_name="lake",
    catalog_properties={"type": "rest", "uri": "https://catalog.example.com"},
    namespace="analytics",
)
```

**Constructor:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `namespace` | `str` | required | Namespace of the asset tables. Dots separate nested namespaces. |
| `catalog_name` | `str` | `"default"` | pyiceberg catalog name. |
| `catalog_properties` | `dict[str, str] \| None` | `None` | Catalog properties for `load_catalog`, such as `type`, `uri`, and `warehouse`, and the storage properties of pyiceberg's FileIO (`s3.*`, `adls.*`, `gcs.*`; see [Storage](../guides/iceberg.md#storage)). pyiceberg adds the ones from `.pyiceberg.yaml` and `PYICEBERG_CATALOG__<NAME>__<KEY>` environment variables. |
| `mode` | `str` | `"overwrite"` | Write mode: `"overwrite"`, `"append"`, or `"upsert"`. |
| `schema_mode` | `str \| None` | `None` | Schema evolution: `"merge"` or `"overwrite"`. |
| `upsert_config` | `UpsertConfig \| None` | `None` | Join columns and clauses for `mode="upsert"`. |
| `table_properties` | `dict[str, str] \| None` | `None` | Iceberg table properties, set on create and when they change. |
| `snapshot_properties` | `dict[str, str] \| None` | `None` | Properties added to the summary of each snapshot that the handler writes. |
| `handler_config` | `dict[str, Any] \| None` | `None` | Type handler config, such as a `DuckDBResource` as `{"duckdb": ...}`. |

**Asset metadata overrides:**

| Key | Type | Description |
|-----|------|-------------|
| `iceberg/namespace` | `str` | Namespace of the table. |
| `iceberg/table` | `str` | Table name, instead of the asset name. |
| `iceberg/mode` | `str` | Write mode. |
| `iceberg/schema_mode` | `str` | Schema mode. |
| `iceberg/partition_expr` | `str \| JSON dict` | Partition column, or a JSON object from dimension to column. |
| `iceberg/join_cols` | `JSON list` | Upsert join columns. |
| `iceberg/table_properties` | `JSON dict` | Table properties, on top of `table_properties`. |
| `iceberg/snapshot_properties` | `JSON dict` | Snapshot properties, on top of `snapshot_properties`. |
| `iceberg/columns` | `JSON list` | Columns to read. |
| `iceberg/snapshot_id` | `str` | Snapshot to read. |

**Output metadata:**

| Key | Type | Description |
|-----|------|-------------|
| `iceberg/table` | `str` | Table identifier, such as `analytics.users`. |
| `iceberg/mode` | `str` | Write mode used. |
| `iceberg/snapshot_id` | `int` | Current snapshot after the write. Also the data version. |
| `iceberg/num_rows` | `int` | Rows in the table after the write. |
| `iceberg/size_bytes` | `int` | Size of the table's data files. |
| `iceberg/write_duration_s` | `float` | Write duration in seconds. |
| `iceberg/metadata_location` | `str` | Location of the table's current metadata file. |
| `iceberg/rows_updated` | `int` | Upserts only: rows updated. |
| `iceberg/rows_inserted` | `int` | Upserts only: rows inserted. |
| `rivers/schema` | `Schema` | Arrow schema of the table. |

### Methods for action bodies

An [action](../concepts/actions.md) gets the asset's resolved handler as `ctx.io_handler`. These methods use the same table and partition resolution as the writes.

#### `catalog()`

Loads the pyiceberg catalog: `load_catalog(catalog_name, **catalog_properties)`.

#### `table_identifier(asset_name, asset_metadata=None)`

Returns `(*namespace, table)`, honoring `iceberg/namespace` and `iceberg/table`.

#### `load_table(asset_name, asset_metadata=None)`

Returns the asset's pyiceberg `Table`. Raises `NoSuchTableError` when the table does not exist yet.

#### `partition_filter(asset_metadata, partition, schema)`

Returns the pyiceberg expression for the rows of the partition(s), honoring `iceberg/partition_expr`. Pass `ctx.asset_metadata`, `ctx.partition`, and `table.schema()`; the schema types the time-window bounds.

```python
@rs.action(outcome=rs.Outcome.Unmaterialize)
@classmethod
def delete(cls, ctx: rs.ActionContext) -> None:
    table = ctx.io_handler.load_table(ctx.asset_name, ctx.asset_metadata)
    table.delete(ctx.io_handler.partition_filter(ctx.asset_metadata, ctx.partition, table.schema()))
```

`partition` is required: a keyless action run raises `ValueError`.

#### `resolved_snapshot_properties(asset_metadata)`

Returns `snapshot_properties` with the asset's `iceberg/snapshot_properties` on top.

---

## `UpsertConfig`

How `mode="upsert"` matches rows, with pyiceberg's `Table.upsert`.

```python
from rivers.io_handlers.iceberg import UpsertConfig

config = UpsertConfig(join_cols=["id"])
```

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `join_cols` | `list[str] \| None` | `None` | Columns that identify a row. `None` uses the table's identifier fields. `iceberg/join_cols` overrides it per asset. |
| `when_matched_update_all` | `bool` | `True` | Update matched rows whose values changed. |
| `when_not_matched_insert_all` | `bool` | `True` | Insert rows without a match. |

---

## `IcebergAsset`

Asset base class with Iceberg maintenance verbs. Subclass it and define `materialize`; `delete` and `expire_snapshots` appear as [actions](../concepts/actions.md), run against the asset's `IcebergIOHandler` (its own `io_handler` or the repository default). Sets `kinds = "iceberg"`.

```python
from rivers.io_handlers.iceberg import IcebergAsset  # also exported as rs.IcebergAsset

class Orders(IcebergAsset):
    io_handler = LAKE

    @classmethod
    def materialize(cls) -> pl.DataFrame: ...
```

| Verb | Declaration | Behavior |
|------|-------------|----------|
| `delete` | `Unmaterialize` + `Exclusive` + `DownstreamFirst` + `Optional` key | Deletes the keyed partition's rows with `partition_filter`, or every row without a key. A keyed delete needs `iceberg/partition_expr`. |
| `expire_snapshots` | `Unchanged` + `Exclusive` + `Keyless` | Removes old snapshots from the table's metadata; their data files stay. Branch and tag heads stay. Reports `snapshots_expired`. |

A verb needs the asset to resolve an `IcebergIOHandler`; another handler fails the step with a `TypeError`. On a table that does not exist yet, `expire_snapshots` reports `ActionResult.unchanged()`, and `delete` still applies its `Unmaterialize` outcome. A subclass that defines a verb replaces the built-in one.

### `ExpireSnapshotsConfig`

Per-asset overrides via `run_action("expire_snapshots", config={"assets": {"<asset>": {"config": {...}}}})`:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `retention_hours` | `float \| None` | `None` | Expire snapshots older than this. `None` uses the table's `history.expire.max-snapshot-age-ms` (5 days by default). |
| `retain_last` | `int \| None` | `None` | Keep this many of the newest snapshots of the table's history. `None` uses `history.expire.min-snapshots-to-keep` (1 by default). |

---

## `IcebergTypeHandler`

Abstract base for adding type support to `IcebergIOHandler`.

```python
from rivers.io_handlers.iceberg import IcebergReadRequest, IcebergTypeHandler, IcebergWriteRequest

class MyTypeHandler(IcebergTypeHandler[MyType]):
    @property
    def supported_types(self) -> Sequence[type[MyType]]:
        return [MyType]

    def load_input(self, request: IcebergReadRequest, target_type: type[MyType]) -> MyType:
        ...

    def handle_output(self, context: OutputContext, obj: MyType, request: IcebergWriteRequest) -> None:
        ...
```

**Abstract members:**

| Member | Description |
|--------|-------------|
| `supported_types` | Property returning the types this handler supports. |
| `load_input(request, target_type)` | Read the table. |
| `handle_output(context, obj, request)` | Write the table and record output metadata. |

`IcebergReadRequest` has the `catalog`, the table `identifier`, the `partition` and `partition_expr`, the `columns`, and the `snapshot_id`. Its helpers are `load_table()`, `scan(table)` (a pyiceberg scan with the partition filter, the columns, and the snapshot), and `predicate` (the partition as a SQL `WHERE` clause). `IcebergWriteRequest` has the resolved write settings.

## `ArrowIcebergTypeHandler`

`IcebergTypeHandler` for types that convert to Arrow. Writes with `handle_output` are implemented with pyiceberg; subclasses implement `to_arrow` and `load_input`.

```python
from rivers.io_handlers.iceberg import ArrowIcebergTypeHandler

class MyTypeHandler(ArrowIcebergTypeHandler[MyType]):
    @property
    def supported_types(self) -> Sequence[type[MyType]]:
        return [MyType]

    def to_arrow(self, obj: MyType) -> pa.Table | pa.RecordBatchReader:
        ...

    def load_input(self, request: IcebergReadRequest, target_type: type[MyType]) -> MyType:
        return convert(request.scan(request.load_table()).to_arrow())
```

**Built-in handlers** (registered when their library is installed):

| Handler | Module | Types |
|---------|--------|-------|
| `PyArrowTypeHandler` | `rivers.io_handlers.iceberg.pyarrow` | `pyarrow.Table`, `pyarrow.RecordBatchReader` |
| `PolarsTypeHandler` | `rivers.io_handlers.iceberg.polars` | `polars.DataFrame`, `polars.LazyFrame` |
| `PandasTypeHandler` | `rivers.io_handlers.iceberg.pandas` | `pandas.DataFrame` |
| `DuckDBTypeHandler` | `rivers.io_handlers.iceberg.duckdb` | `duckdb.DuckDBPyRelation` |
