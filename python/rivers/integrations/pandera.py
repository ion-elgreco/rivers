"""pandera integration: validate asset outputs against their pandera schema.

Install with ``pip install rivers[pandera]``, next to the extra of your frame
library: ``rivers[pandas]`` or ``rivers[polars]``.

Example::

    import pandas as pd
    import pandera.pandas as pa
    from pandera.typing import DataFrame, Series

    import rivers as rs

    class Orders(pa.DataFrameModel):
        id: Series[int] = pa.Field(ge=0)

    @rs.Asset
    def orders() -> DataFrame[Orders]:
        return pd.DataFrame({"id": [1, 2]})
"""

import json
import typing
from typing import Any

from rivers._core import MetadataValue, Output

try:
    from pandera.api.dataframe.container import DataFrameSchema
    from pandera.api.dataframe.model import DataFrameModel
    from pandera.typing.common import DataFrameBase
except ImportError as e:
    raise ImportError(
        "rivers.integrations.pandera needs pandera: pip install rivers[pandera]"
    ) from e

__all__ = ["validate_output"]


def validate_output(value: Any, hint: Any) -> Any:
    """Validate an asset's value against the pandera schema in ``hint``.

    rivers calls this for a pandera type hint, which is a return hint or a
    multi-asset output's ``AssetDef(type_hint=...)``, and for each pandera
    schema in an ``Annotated`` hint.

    Args:
        value: The asset's return value, or the value of a multi-asset output.
        hint: A pandera ``DataFrame[Model]`` or ``LazyFrame[Model]`` hint, a
            ``DataFrameModel`` subclass, or a ``DataFrameSchema``.

    Returns:
        An ``Output`` with the frame that pandera returns and the
        ``pandera/schema`` metadata, or ``NotImplemented`` when ``hint`` holds
        no schema or ``value`` is not the hint's frame type.

    Raises:
        pandera.errors.SchemaErrors: The frame does not match the schema.
    """
    origin = typing.get_origin(hint)
    if isinstance(origin, type) and issubclass(origin, DataFrameBase):
        if not isinstance(value, _frame_types(origin)):
            return NotImplemented
        hint = typing.get_args(hint)[0]

    if isinstance(hint, type) and issubclass(hint, DataFrameModel):
        schema = hint.to_schema()
    elif isinstance(hint, DataFrameSchema):
        schema = hint
    else:
        return NotImplemented

    return Output(schema.validate(value, lazy=True), metadata=_schema_metadata(schema))


def _schema_metadata(schema: DataFrameSchema) -> dict[str, Any]:
    """The ``pandera/schema`` metadata entry that describes ``schema``."""
    table = {
        "name": schema.name,
        "description": schema.description,
        "columns": [
            {
                "name": column.name,
                "type": None if column.dtype is None else str(column.dtype),
                "nullable": column.nullable,
                "unique": column.unique,
                "description": column.description,
                "checks": [_check_text(check) for check in column.checks],
            }
            for column in schema.columns.values()
        ],
        "checks": [_check_text(check) for check in schema.checks or ()],
    }
    return {"pandera/schema": MetadataValue.json(json.dumps(table, default=str))}


def _frame_types(origin: type) -> tuple[type, ...]:
    # A pandera typing class subclasses the frame class it stands for, e.g.
    # `pandera.typing.DataFrame` subclasses `pd.DataFrame`.
    return tuple(
        base
        for base in origin.__bases__
        if base is not typing.Generic and not issubclass(base, DataFrameBase)
    )


def _check_text(check: Any) -> str:
    return check.description or check.error or check.name or str(check)
