# dataframely

`rivers.integrations.dataframely` validates asset outputs against dataframely schemas. Install with `pip install rivers[dataframely]`. See the [dataframely guide](../guides/dataframely.md).

## Return hints

rivers validates the return value when the return hint holds a dataframely schema. The integration needs no import for this.

| Return hint | Checked value | Validation |
|-------------|---------------|------------|
| `dy.DataFrame[Schema]` | `polars.DataFrame` | `Schema.validate(frame, cast=...)` |
| `dy.LazyFrame[Schema]` | `polars.LazyFrame` | Columns and types when the asset returns; rows when the frame is collected |
| `Annotated[frame type, Schema]` | The frame type | As for `dy.DataFrame[Schema]` or `dy.LazyFrame[Schema]` |

A union such as `dy.DataFrame[Schema] | None` passes when the value matches one member.

`cast` is the `cast` attribute of the schema's nested `Config` class, and `False` without one. rivers stores the frame that `validate` returns: columns outside the schema are dropped, and cast columns keep the schema's types.

**Errors:**

| Case | `AssetOutputValidationError` message |
|------|--------------------------------------|
| The frame fails the schema | `Asset '<name>' returned a value that does not match '<hint>': <dataframely report>` |
| The value is not the hint's frame type | `Asset '<name>' returned value of type '<type>' but expected '<hint>'` |

The dataframely report names each failed rule and its number of failed rows. In-process, the exception's `__cause__` is dataframely's `ValidationError` or `SchemaError`.

**Output metadata:**

| Key | Type | Description |
|-----|------|-------------|
| `dataframely/schema` | `Json` | The schema that validated the frame. A `dataframely/schema` entry that the asset sets replaces it. |

The JSON object has these fields:

| Field | Description |
|-------|-------------|
| `name` | The schema's class name. |
| `description` | The schema's docstring. |
| `columns` | One object per column: `name`, `type` (the polars dtype, e.g. `"Int64"`), `nullable`, `primary_key`, `unique`, `description`, `checks`. |
| `checks` | The names of the schema's `@dy.rule()` rules. |

A column check is shown by its rule name, with its bound when it has one, e.g. `"min=0"` or `"regex='^(open|closed)$'"`.

## `schema_metadata(schema)`

```python
def schema_metadata(schema: type[dy.Schema]) -> dict[str, MetadataValue]
```

Returns `{"dataframely/schema": MetadataValue.json(...)}` for a schema class. Use it for outputs that have no dataframely return hint, such as multi-asset outputs:

```python
from rivers.integrations.dataframely import schema_metadata

yield rs.Output(
    Orders.validate(frame),
    output_name="orders",
    metadata=schema_metadata(Orders),
)
```

## `validate_output(value, hint)`

```python
def validate_output(value: Any, hint: Any) -> Any
```

The function that rivers calls for a dataframely return hint, and for each dataframely schema in an `Annotated` return hint.

| Parameter | Description |
|-----------|-------------|
| `value` | The asset's return value. |
| `hint` | A `dy.DataFrame[Schema]` or `dy.LazyFrame[Schema]` hint, or a `dy.Schema` subclass. |

**Returns:** an `Output` with the validated frame and the `dataframely/schema` metadata, or `NotImplemented` when `hint` holds no schema or `value` is not the hint's frame type.

**Raises:** `dataframely.exc.SchemaError` when a column is missing or has the wrong type, and `dataframely.exc.ValidationError` when a row breaks a rule.
