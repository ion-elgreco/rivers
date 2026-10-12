"""dataframely integration: validate asset outputs against their dataframely schema.

Install with ``pip install rivers[dataframely]``.

Example::

    import dataframely as dy
    import polars as pl

    import rivers as rs

    class Orders(dy.Schema):
        id = dy.Int64(min=0)

    @rs.Asset
    def orders() -> dy.DataFrame[Orders]:
        return pl.DataFrame({"id": [1, 2]})
"""

import inspect
import json
import typing
from typing import Any

from rivers._core import MetadataValue, Output

try:
    import dataframely as dy
    import polars as pl
except ImportError as e:
    raise ImportError(
        "rivers.integrations.dataframely needs dataframely: pip install rivers[dataframely]"
    ) from e

__all__ = ["schema_metadata", "validate_output"]

_FRAME_TYPES = {dy.DataFrame: pl.DataFrame, dy.LazyFrame: pl.LazyFrame}

# Column rules that the column's `nullable` and `unique` fields already show.
_FIELD_RULES = frozenset({"nullability", "unique"})


def validate_output(value: Any, hint: Any) -> Any:
    """Validate an asset's return value against the dataframely schema in ``hint``.

    rivers calls this for a return hint from dataframely, and for each
    dataframely schema in an ``Annotated`` return hint. When the schema's
    ``Config`` sets ``cast = True``, columns are cast to the schema's types
    before they are checked. A ``LazyFrame`` is checked for its columns and
    types now; dataframely checks its rows when the frame is collected.

    Args:
        value: The asset's return value.
        hint: A ``dy.DataFrame[Schema]`` or ``dy.LazyFrame[Schema]`` hint, or a
            ``dy.Schema`` subclass.

    Returns:
        An ``Output`` with the frame that dataframely returns and the
        ``dataframely/schema`` metadata, or ``NotImplemented`` when ``hint``
        holds no schema or ``value`` is not the hint's frame type.

    Raises:
        dataframely.exc.SchemaError: A column is missing or has the wrong type.
        dataframely.exc.ValidationError: A row breaks a rule of the schema.
    """
    frame_type = _FRAME_TYPES.get(typing.get_origin(hint))
    if frame_type is not None:
        if not isinstance(value, frame_type):
            return NotImplemented
        hint = typing.get_args(hint)[0]

    if not (isinstance(hint, type) and issubclass(hint, dy.Schema)):
        return NotImplemented

    cast = getattr(getattr(hint, "Config", None), "cast", False)
    if isinstance(value, pl.LazyFrame):
        hint.validate(pl.DataFrame(schema=value.collect_schema()), cast=cast)

    return Output(hint.validate(value, cast=cast), metadata=schema_metadata(hint))


def schema_metadata(schema: type[dy.Schema]) -> dict[str, Any]:
    """Describe a dataframely schema as the ``dataframely/schema`` metadata entry.

    rivers adds this entry to every output it validates. Use it for outputs
    that have no dataframely return hint, e.g. the outputs of a multi-asset.

    Args:
        schema: A ``dy.Schema`` subclass.

    Returns:
        ``{"dataframely/schema": MetadataValue.json(...)}``: the schema's name,
        description, and rules, and each column's name, type, nullability,
        primary key, uniqueness, description, and checks.

    Example::

        @rs.Asset.from_multi(output_defs=[rs.AssetDef("orders")])
        def load():
            orders = Orders.validate(read_orders())
            yield rs.Output(orders, output_name="orders", metadata=schema_metadata(Orders))
    """
    table = {
        "name": schema.__name__,
        "description": inspect.cleandoc(schema.__doc__) if schema.__doc__ else None,
        "columns": [
            {
                "name": name,
                "type": str(column.dtype),
                "nullable": column.nullable,
                "primary_key": column.primary_key,
                "unique": column.unique,
                "description": column.description,
                "checks": [
                    _check_text(column, rule)
                    for rule in column.validation_rules(pl.col(name))
                    if rule not in _FIELD_RULES
                ],
            }
            for name, column in schema.columns().items()
        ],
        "checks": list(schema._schema_validation_rules()),
    }
    return {"dataframely/schema": MetadataValue.json(json.dumps(table, default=str))}


def _check_text(column: dy.Column, rule: str) -> str:
    # dataframely keeps a rule's bound under the rule's name, e.g. `min`.
    value = getattr(column, rule, None)
    return rule if value is None or callable(value) else f"{rule}={value!r}"
