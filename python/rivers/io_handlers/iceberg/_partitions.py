"""Partition keys as pyiceberg row filters, and the partition spec of a new table."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import datetime

import pyarrow as pa
from pyiceberg.expressions import (
    AlwaysTrue,
    And,
    BooleanExpression,
    GreaterThanOrEqual,
    In,
    LessThan,
    Or,
)
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    Transform,
    YearTransform,
)
from pyiceberg.types import DateType, TimestampType, TimestamptzType

from rivers._core.partitions import PartitionContext, PartitionsDefinition
from rivers.io_handlers._partition_sql import PartitionExpr, _key_dimensions, _window

# Coarsest first. Each truncates a datetime to the start of its unit.
_TIME_TRANSFORMS: list[tuple[Transform, Callable[[datetime], datetime]]] = [
    (
        YearTransform(),
        lambda t: t.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0),
    ),
    (
        MonthTransform(),
        lambda t: t.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
    ),
    (DayTransform(), lambda t: t.replace(hour=0, minute=0, second=0, microsecond=0)),
    (HourTransform(), lambda t: t.replace(minute=0, second=0, microsecond=0)),
]


def require_partition_expr(partition_expr: PartitionExpr | None) -> PartitionExpr:
    """``partition_expr``, or a ``ValueError`` that names the metadata key."""
    if partition_expr is None:
        raise ValueError(
            "a partitioned Iceberg asset needs the 'iceberg/partition_expr' "
            "metadata key to map its partitions to table columns"
        )
    return partition_expr


def partition_filter(
    partition: PartitionContext | None,
    partition_expr: PartitionExpr | None,
    schema: Schema,
) -> BooleanExpression:
    """The rows of the partition's keys as a pyiceberg expression; all rows without a partition.

    Time-window bounds on date and timestamp columns are ``datetime`` values:
    pyiceberg cannot convert a key such as ``2024-01-01`` to a timestamp.
    Other values stay strings, which pyiceberg converts to the column type.

    Raises:
        ValueError: If ``partition_expr`` is ``None`` or does not fit the
            partition definition, or a partition column is not in ``schema``.
    """
    if partition is None:
        return AlwaysTrue()

    partition_expr = require_partition_expr(partition_expr)
    return _any(
        _all(
            _dimension_filter(column, values, definition, schema)
            for column, values, definition in _key_dimensions(
                key, partition.definition, partition_expr
            )
        )
        for key in partition.keys
    )


def partition_fields(
    partition: PartitionContext, partition_expr: PartitionExpr, schema: pa.Schema
) -> list[tuple[str, Transform]]:
    """The ``(column, transform)`` partition fields for a new table.

    Static and dynamic keys, and time windows on other columns, partition by
    value. Time windows on date and timestamp columns use the coarsest of
    year, month, day, and hour that the window bounds align to; the finest
    one when none does.

    Raises:
        ValueError: If ``partition_expr`` does not fit the partition
            definition, or a partition column is not in ``schema``.
    """
    fields = []
    for column, values, definition in _key_dimensions(
        partition.keys[0], partition.definition, partition_expr
    ):
        if column not in schema.names:
            raise ValueError(
                f"partition column {column!r} is not in the data: {schema.names}"
            )
        fields.append(
            (column, _transform(definition, values[0], schema.field(column).type))
        )

    return fields


def _dimension_filter(
    column: str,
    values: list[str],
    definition: PartitionsDefinition | None,
    schema: Schema,
) -> BooleanExpression:
    """The rows of one dimension: a time range for a single time-window value."""
    if len(values) == 1 and isinstance(definition, PartitionsDefinition.TimeWindow):
        window = _window(values[0], definition)
        if window is not None:
            start, end = window
            temporal = DateType | TimestampType | TimestamptzType
            if not isinstance(schema.find_type(column), temporal):
                start, end = (
                    start.strftime(definition.fmt),
                    end.strftime(definition.fmt),
                )
            return And(
                GreaterThanOrEqual(column, start),  # pyright: ignore[reportCallIssue]
                LessThan(column, end),  # pyright: ignore[reportCallIssue]
            )

    return In(column, values)  # pyright: ignore[reportCallIssue]


def _all(expressions: Iterable[BooleanExpression]) -> BooleanExpression:
    expressions = list(expressions)
    return expressions[0] if len(expressions) == 1 else And(*expressions)


def _any(expressions: Iterable[BooleanExpression]) -> BooleanExpression:
    expressions = list(expressions)
    return expressions[0] if len(expressions) == 1 else Or(*expressions)


def _transform(
    definition: PartitionsDefinition | None, value: str, column_type: pa.DataType
) -> Transform:
    timestamp = pa.types.is_timestamp(column_type)
    if not isinstance(definition, PartitionsDefinition.TimeWindow) or not (
        timestamp or pa.types.is_date(column_type)
    ):
        return IdentityTransform()

    candidates = _TIME_TRANSFORMS if timestamp else _TIME_TRANSFORMS[:-1]
    window = _window(value, definition)
    for transform, truncate in candidates:
        if window is not None and all(truncate(bound) == bound for bound in window):
            return transform

    return candidates[-1][0]
