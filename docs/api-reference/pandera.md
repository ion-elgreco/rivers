# pandera

`rivers.integrations.pandera` validates asset outputs against pandera schemas. Install with `pip install rivers[pandera]`. See the [pandera guide](../guides/pandera.md).

## Return hints

rivers validates the return value when the return hint holds a pandera schema. The integration needs no import for this. For a multi-asset output, put the same hints in `AssetDef(type_hint=...)`, and rivers validates the output's value.

| Return hint | Checked value | Validation |
|-------------|---------------|------------|
| `pandera.typing.DataFrame[Model]` | `pandas.DataFrame` | `Model.validate(frame, lazy=True)` |
| `pandera.typing.polars.DataFrame[Model]` | `polars.DataFrame` | `Model.validate(frame, lazy=True)` |
| `pandera.typing.polars.LazyFrame[Model]` | `polars.LazyFrame` | Columns and types only, unless `PANDERA_VALIDATION_DEPTH=SCHEMA_AND_DATA` |
| `Annotated[frame type, schema]` | The frame type | `schema.validate(frame, lazy=True)`, for a `DataFrameSchema` or a `DataFrameModel` subclass |

A union such as `DataFrame[Model] | None` passes when the value matches one member.

rivers stores the frame that `validate` returns, so coercion, added columns, dropped rows, defaults and parsers take effect in storage.

**Errors:**

| Case | `AssetOutputValidationError` message |
|------|--------------------------------------|
| The frame fails the schema | `Asset '<name>' returned a value that does not match '<hint>': <pandera report>` |
| The value is not the hint's frame type | `Asset '<name>' returned value of type '<type>' but expected '<hint>'` |

The pandera report lists every failed check. In-process, the exception's `__cause__` is pandera's `SchemaErrors`.

**Output metadata:**

| Key | Type | Description |
|-----|------|-------------|
| `pandera/schema` | `Json` | The schema that validated the frame. A `pandera/schema` entry that the asset sets replaces it. |

The JSON object has these fields:

| Field | Description |
|-------|-------------|
| `name` | Schema name: the model's class name or `Config.name`, or `DataFrameSchema(name=...)`. |
| `description` | Schema description: the model's docstring or `Config.description`. |
| `columns` | One object per column: `name`, `type` (pandera's dtype, e.g. `"int64"`, `"Int64"`), `nullable`, `unique`, `description`, `checks`. |
| `checks` | The frame-wide checks. |

A check is shown by its description, else its error text (e.g. `"greater_than_or_equal_to(0)"`), else its name.

## `validate_output(value, hint)`

```python
def validate_output(value: Any, hint: Any) -> Any
```

The function that rivers calls for a pandera return hint or `AssetDef(type_hint=...)`, and for each pandera schema in an `Annotated` hint.

| Parameter | Description |
|-----------|-------------|
| `value` | The asset's return value, or the value of a multi-asset output. |
| `hint` | A pandera `DataFrame[Model]` or `LazyFrame[Model]` hint, a `DataFrameModel` subclass, or a `DataFrameSchema`. |

**Returns:** an `Output` with the validated frame and the `pandera/schema` metadata, or `NotImplemented` when `hint` holds no schema or `value` is not the hint's frame type.

**Raises:** `pandera.errors.SchemaErrors` when the frame does not match the schema.
