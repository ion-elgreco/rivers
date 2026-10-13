# pandera

`rivers.integrations.pandera` validates asset outputs against [pandera](https://pandera.readthedocs.io) schemas. Give an asset a pandera return hint, and rivers checks each frame that the asset returns.

## Setup

```bash
pip install rivers[pandera,pandas]   # pandas frames
pip install rivers[pandera,polars]   # polars frames
```

You do not import the integration: rivers finds it from the return hint.

## Validate an asset's output

Annotate the return with `pandera.typing.DataFrame[Model]`:

```python
import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame, Series

import rivers as rs

class Orders(pa.DataFrameModel):
    """Orders placed in the shop."""

    id: Series[int] = pa.Field(ge=0, unique=True, description="Order id")
    amount: Series[float] = pa.Field(gt=0, nullable=True)
    status: Series[str] = pa.Field(isin=["open", "closed"])

@rs.Asset
def orders() -> DataFrame[Orders]:
    return pd.read_parquet("orders.parquet")
```

When the asset returns, rivers calls `schema.validate(frame, lazy=True)`. A valid frame is stored and recorded. An invalid frame fails the step with `AssetOutputValidationError`, and nothing is stored. Downstream assets do not run.

These return hints hold a schema:

| Return hint | Value |
|-------------|-------|
| `pandera.typing.DataFrame[Model]` | `pandas.DataFrame` |
| `pandera.typing.polars.DataFrame[Model]` | `polars.DataFrame` |
| `pandera.typing.polars.LazyFrame[Model]` | `polars.LazyFrame` |
| `Annotated[pd.DataFrame, schema]`, `Annotated[pl.DataFrame, schema]` | The frame, checked against `schema`: a `DataFrameSchema` object or a `DataFrameModel` class |
| `DataFrame[Model] \| None` | A frame, or `None` (not validated) |

A value of the wrong type, for example a dict for `DataFrame[Orders]`, fails like any other [return type check](../concepts/assets.md#return-type-checks).

### Polars

```python
import polars as pl
import pandera.polars as pa
from pandera.typing.polars import DataFrame, LazyFrame

class Orders(pa.DataFrameModel):
    id: int = pa.Field(ge=0)
    status: str = pa.Field(isin=["open", "closed"])

@rs.Asset
def orders() -> DataFrame[Orders]:
    return pl.read_parquet("orders.parquet")

@rs.Asset
def open_orders(orders: pl.DataFrame) -> LazyFrame[Orders]:
    return orders.lazy().filter(pl.col("status") == "open")
```

pandera checks a `LazyFrame` for its columns and types only, so that the frame is not collected. To also check its rows, set `PANDERA_VALIDATION_DEPTH=SCHEMA_AND_DATA`.

### `DataFrameSchema` objects

A `DataFrameSchema` cannot be a type argument. Put it in `Annotated`:

```python
from typing import Annotated

orders_schema = pa.DataFrameSchema(
    {"id": pa.Column(int, pa.Check.ge(0)), "status": pa.Column(str)},
    name="Orders",
)

@rs.Asset
def orders() -> Annotated[pd.DataFrame, orders_schema]:
    return pd.read_parquet("orders.parquet")
```

rivers first checks that the value is a `pd.DataFrame`, then validates it against the schema.

## The stored frame

rivers stores the frame that pandera returns, not the frame that the asset returned. This makes the schema options that change data take effect in storage: `coerce`, `add_missing_columns`, `drop_invalid_rows`, column defaults, and parsers.

```python
class Prices(pa.DataFrameModel):
    price: Series[float]

    class Config:
        coerce = True

@rs.Asset
def prices() -> DataFrame[Prices]:
    return pd.DataFrame({"price": [1, 2]})  # stored with price as float64
```

## Schema metadata

Each validated materialization records the schema as `pandera/schema`, a JSON `MetadataValue`. The UI shows it with the other metadata of the materialization:

```json
{
  "name": "Orders",
  "description": "Orders placed in the shop.",
  "columns": [
    {"name": "id", "type": "int64", "nullable": false, "unique": true, "description": "Order id", "checks": ["greater_than_or_equal_to(0)"]},
    {"name": "amount", "type": "float64", "nullable": true, "unique": false, "description": null, "checks": ["greater_than(0)"]},
    {"name": "status", "type": "string[pyarrow]", "nullable": false, "unique": false, "description": null, "checks": ["isin(['open', 'closed'])"]}
  ],
  "checks": []
}
```

A `pandera/schema` entry that the asset sets itself, with `context.add_output_metadata()` or `Output(metadata=...)`, replaces this one.

## Failures

Validation is lazy, so the error lists every failed check, not only the first. The message has pandera's report:

```text
AssetOutputValidationError: Asset 'orders' returned a value that does not match 'pandera.typing.pandas.DataFrame[pipeline.Orders]': {
    "DATA": {
        "DATAFRAME_CHECK": [
            {
                "schema": "Orders",
                "column": "id",
                "check": "greater_than_or_equal_to(0)",
                "error": "Column 'id' failed element-wise validator number 0: greater_than_or_equal_to(0) failure cases: -1"
            },
            ...
        ]
    }
}
```

Both executors validate the same way. The parallel executor validates a sync step in its worker process.

To turn validation off, set pandera's `PANDERA_VALIDATION_ENABLED=False`. rivers then stores the frame as the asset returned it.

## Multi-asset outputs

Multi-asset outputs have no return hint each. Validate in the body, and attach the metadata with `schema_metadata`:

```python
from rivers.integrations.pandera import schema_metadata

@rs.Asset.from_multi(output_defs=[rs.AssetDef("orders"), rs.AssetDef("refunds")])
def load():
    yield rs.Output(
        Orders.validate(read_orders(), lazy=True),
        output_name="orders",
        metadata=schema_metadata(Orders),
    )
    yield rs.Output(read_refunds(), output_name="refunds")
```

## Inputs

rivers validates return values, not inputs: a frame is validated once, when its asset writes it. Annotate a downstream parameter with the frame type, for example `orders: pd.DataFrame`. `DeltaIOHandler` and `DuckDBIOHandler` read by that type and do not accept a pandera hint there.

## From `dagster-pandera`

| Dagster | rivers |
|---------|--------|
| `pandera_schema_to_dagster_type(Model)` with `@asset(dagster_type=...)` or `Out(dagster_type=...)` | Return hint `DataFrame[Model]` |
| `pandera_schema_to_dagster_type(schema)` for a `DataFrameSchema` | Return hint `Annotated[pd.DataFrame, schema]` |
| Failed `TypeCheck` | `AssetOutputValidationError` with pandera's report |
| The type's `schema` metadata (a `TableSchema`) | `pandera/schema` metadata on each materialization |

Dagster checks the frame and keeps the frame that the asset returned. rivers keeps the frame that pandera returns.
