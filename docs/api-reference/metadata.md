# MetadataValue

Typed metadata values for asset outputs. Each variant has a static constructor and typed attributes.

```python
import rivers as rs

meta = rs.MetadataValue.text("hello")
meta = rs.MetadataValue.int(42)
meta = rs.MetadataValue.json('{"key": "value"}')
```

## Variants

| Variant | Constructor | Attributes |
|---------|-------------|------------|
| `Text` | `.text(value: str)` | `value: str` |
| `Int` | `.int(value: int)` | `value: int` |
| `Float` | `.float_(value: float)` | `value: float` |
| `Bool` | `.bool_(value: bool)` | `value: bool` |
| `Url` | `.url(value: str)` | `value: str` |
| `Path` | `.path(value: str)` | `value: str` |
| `Json` | `.json(value: str)` | `value: str` |
| `Markdown` | `.md(value: str)` | `value: str` |
| `Timestamp` | `.timestamp(value: float)` | `value: float` |
| `Null` | `.null()` | -- |
| `Bytes` | `.bytes(value: int)` | `value: int` |
| `Duration` | `.duration(value: float)` | `value: float` |
| `Sql` | `.sql(query, dialect=None)` | `query: str`, `dialect: str \| None` |
| `CodeBlock` | `.code_block(code, language=None)` | `code: str`, `language: str \| None` |
| `Image` | `.image(value: str)` | `value: str` |
| `Percentage` | `.percentage(value: float)` | `value: float` |
| `List` | `.list_(values: list[MetadataValue])` | `values: list[MetadataValue]` |
| `DateRange` | `.date_range(start, end)` | `start: datetime`, `end: datetime` |
| `Schema` | `.schema(value: ArrowSchemaExportable)` | `ipc_bytes: bytes` |
| `DataVersion` | `.data_version(value: str)` | `value: str` |

## Type checking

Use `isinstance` to check the variant:

```python
meta = rs.MetadataValue.int(42)
assert isinstance(meta, rs.MetadataValue.Int)
assert meta.value == 42
```

## `raw_value()`

Extract the underlying Python value:

```python
meta = rs.MetadataValue.text("hello")
meta.raw_value()  # "hello"

meta = rs.MetadataValue.date_range(start, end)
meta.raw_value()  # (start, end)

meta = rs.MetadataValue.null()
meta.raw_value()  # None
```

**Return type:** `str | int | float | bool | None | list[MetadataValue] | tuple[datetime, datetime] | Schema`

## Automatic conversion

When passing values to `context.add_output_metadata()`, primitive types are automatically converted:

| Python type | MetadataValue variant |
|-------------|----------------------|
| `str` | `Text` |
| `int` | `Int` |
| `float` | `Float` |
| `bool` | `Bool` |
| `None` | `Null` |
