# Schema

## `Schema`

A wrapper around an Arrow schema, backed by Rust's `arrow-schema` crate. Accepts any Python object implementing the Arrow PyCapsule interface (`__arrow_c_schema__`), including PyArrow, Polars, and arro3 schemas.

```python
import pyarrow as pa
import rivers as rs

pa_schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("name", pa.utf8()),
    pa.field("price", pa.float64()),
])

schema = rs.Schema(pa_schema)
schema.names    # ["id", "name", "price"]
len(schema)     # 3
```

**Constructor:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `schema` | `ArrowSchemaExportable` | Any object with `__arrow_c_schema__` (PyArrow, Polars, arro3). |

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `names` | `list[str]` | Column names. |

**Methods:**

| Method | Returns | Description |
|--------|---------|-------------|
| `__len__()` | `int` | Number of fields. |
| `__eq__(other)` | `bool` | Schema equality. |
| `__arrow_c_schema__()` | `PyCapsule` | Export via Arrow PyCapsule interface. |
| `to_ipc()` | `bytes` | Serialize to IPC bytes. |
| `Schema.from_ipc(data)` | `Schema` | Deserialize from IPC bytes. |

---

## Using with `MetadataValue`

```python
mv = rs.MetadataValue.schema(pa_schema)
mv.raw_value()  # returns rs.Schema
```

## Using with assets

Attach a schema to an asset via metadata:

```python
@rs.Asset(metadata={"rivers/schema": pa_schema})
def my_table():
    ...
```

Arrow schema objects are auto-coerced to `MetadataValue.Schema` when passed to `add_output_metadata`.

## IPC serialization

Schemas are stored internally as Arrow IPC bytes for compact, lossless serialization. Use `to_ipc()` and `Schema.from_ipc()` for manual serialization if needed.
