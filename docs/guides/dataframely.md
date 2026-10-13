# dataframely

`rivers.integrations.dataframely` validates asset outputs against [dataframely](https://dataframely.readthedocs.io) schemas. Give an asset a dataframely return hint, and rivers checks each polars frame that the asset returns.

## Setup

```bash
pip install rivers[dataframely]
```

You do not import the integration: rivers finds it from the return hint.

## Validate an asset's output

Annotate the return with `dy.DataFrame[Schema]`:

```python
import dataframely as dy
import polars as pl

import rivers as rs

class Orders(dy.Schema):
    """Orders placed in the shop."""

    id = dy.Int64(primary_key=True, min=0, description="Order id")
    amount = dy.Float64(nullable=True, min_exclusive=0)
    status = dy.String(regex="^(open|closed)$")

    @dy.rule()
    def open_orders_have_amount(cls) -> pl.Expr:
        return (pl.col("status") != "open") | pl.col("amount").is_not_null()

@rs.Asset
def orders() -> dy.DataFrame[Orders]:
    return pl.read_parquet("orders.parquet")
```

When the asset returns, rivers calls `Orders.validate(frame)`. A valid frame is stored and recorded. An invalid frame fails the step with `AssetOutputValidationError`, and nothing is stored. Downstream assets do not run.

These return hints hold a schema:

| Return hint | Value |
|-------------|-------|
| `dy.DataFrame[Schema]` | `polars.DataFrame` |
| `dy.LazyFrame[Schema]` | `polars.LazyFrame` |
| `Annotated[pl.DataFrame, Schema]`, `Annotated[pl.LazyFrame, Schema]` | The frame, checked against `Schema` |
| `dy.DataFrame[Schema] \| None` | A frame, or `None` (not validated) |

A value of the wrong type, for example a dict for `dy.DataFrame[Orders]`, fails like any other [return type check](../concepts/assets.md#return-type-checks).

### Lazy frames

```python
@rs.Asset
def open_orders(orders: pl.DataFrame) -> dy.LazyFrame[Orders]:
    return orders.lazy().filter(pl.col("status") == "open")
```

When the asset returns a `LazyFrame`, rivers checks its columns and their types, and does not collect the frame. dataframely adds its row checks to the frame, and runs them when the frame is collected:

- `DeltaIOHandler` and `DuckDBIOHandler` collect the frame when they write it. A bad row fails the asset's step with the write's error, which holds dataframely's report, and the table does not change.
- `PickleIOHandler` stores the frame without collecting it, so a bad row fails the step that reads and collects it.

## Casting

dataframely checks types strictly: a column with the wrong type fails the step. To cast the columns to the schema's types first, set `cast` in the schema's `Config`:

```python
class Prices(dy.Schema):
    price = dy.Float64()

    class Config:
        cast = True

@rs.Asset
def prices() -> dy.DataFrame[Prices]:
    return pl.DataFrame({"price": [1, 2]})  # stored with price as Float64
```

rivers then calls `Prices.validate(frame, cast=True)`. Only rivers reads `Config`. A direct call to `Prices.validate(frame)` does not cast.

## The stored frame

rivers stores the frame that dataframely returns, not the frame that the asset returned. Columns that the schema does not define are dropped. With `Config.cast`, the columns have the schema's types.

## Schema metadata

Each validated materialization records the schema as `dataframely/schema`, a JSON `MetadataValue`. The UI shows it with the other metadata of the materialization:

```json
{
  "name": "Orders",
  "description": "Orders placed in the shop.",
  "columns": [
    {"name": "id", "type": "Int64", "nullable": false, "primary_key": true, "unique": false, "description": "Order id", "checks": ["min=0"]},
    {"name": "amount", "type": "Float64", "nullable": true, "primary_key": false, "unique": false, "description": null, "checks": ["min_exclusive=0", "inf", "nan"]},
    {"name": "status", "type": "String", "nullable": false, "primary_key": false, "unique": false, "description": null, "checks": ["regex='^(open|closed)$'"]}
  ],
  "checks": ["open_orders_have_amount"]
}
```

A check has the name that dataframely's failure report uses, with its bound if it has one. A `dataframely/schema` entry that the asset sets itself, with `context.add_output_metadata()` or `Output(metadata=...)`, replaces this one.

## Failures

The message has dataframely's report: each rule that failed, and the number of rows that failed it.

```text
AssetOutputValidationError: Asset 'orders' returned a value that does not match 'dataframely._typing.DataFrame[pipeline.Orders]': 4 rules failed validation:
 - 'open_orders_have_amount' failed for 1 rows
 * Column 'id' failed validation for 1 rules:
   - 'min' failed for 1 rows
 * Column 'amount' failed validation for 1 rules:
   - 'min_exclusive' failed for 1 rows
 * Column 'status' failed validation for 1 rules:
   - 'regex' failed for 1 rows
```

A missing column, or a column with the wrong type, fails with dataframely's `SchemaError` text instead.

Both executors validate the same way. The parallel executor validates a sync step in its worker process.

## Multi-asset outputs

A multi-asset has one return hint for all its outputs. Give an output its own hint with `AssetDef(type_hint=...)`. rivers validates that output's frame the same way as a returned frame:

```python
@rs.Asset.from_multi(
    output_defs=[
        rs.AssetDef("orders", type_hint=dy.DataFrame[Orders]),
        rs.AssetDef("refunds"),
    ]
)
def load():
    yield rs.Output(read_orders(), output_name="orders")
    yield rs.Output(read_refunds(), output_name="refunds")
```

An invalid frame fails its output. See [Return type checks](../concepts/assets.md#return-type-checks) for the other outputs.

## Inputs

rivers validates return values, not inputs: a frame is validated once, when its asset writes it. Annotate a downstream parameter with the frame type, for example `orders: pl.DataFrame`. `DeltaIOHandler` and `DuckDBIOHandler` read by that type and do not accept a dataframely hint there.
