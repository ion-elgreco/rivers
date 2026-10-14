"""Partition keys as pyiceberg row filters, and the partition spec of a new table."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from functools import reduce

import pyarrow as pa
from pyiceberg.expressions import (
    And,
    BooleanExpression,
    EqualTo,
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

from rivers._core.partitions import PartitionContext, PartitionKey, PartitionsDefinition
from rivers.io_handlers._partition_sql import (
    Filter,
    PartitionExpr,
    _build_partition_filters,
)

_COMPARISONS = {"=": EqualTo, ">=": GreaterThanOrEqual, "<": LessThan}

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
    partition: PartitionContext, partition_expr: PartitionExpr | None, schema: Schema
) -> BooleanExpression:
    """The rows of the partition's keys, as a pyiceberg expression.

    Time-window bounds on date and timestamp columns are ``datetime`` values:
    pyiceberg cannot convert a key such as ``2024-01-01`` to a timestamp.
    Other values stay strings, which pyiceberg converts to the column type.

    Raises:
        ValueError: If ``partition_expr`` is ``None`` or does not fit the
            partition definition, or a partition column is not in ``schema``.
    """
    partition_expr = require_partition_expr(partition_expr)
    formats = {
        column: definition.fmt
        for column, definition, _ in _dimensions(partition, partition_expr)
        if isinstance(definition, PartitionsDefinition.TimeWindow)
        and isinstance(
            schema.find_type(column), DateType | TimestampType | TimestamptzType
        )
    }

    return reduce(
        Or,
        (
            reduce(And, (_expression(f, formats) for f in conjunction))
            for conjunction in _build_partition_filters(partition, partition_expr)
        ),
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
    for column, definition, value in _dimensions(partition, partition_expr):
        if column not in schema.names:
            raise ValueError(
                f"partition column {column!r} is not in the data: {schema.names}"
            )
        fields.append(
            (column, _transform(definition, value, schema.field(column).type))
        )

    return fields


def _dimensions(
    partition: PartitionContext, partition_expr: PartitionExpr
) -> list[tuple[str, PartitionsDefinition | None, str]]:
    """``(column, definition, first key value)`` for each partition dimension."""
    key, definition = partition.keys[0], partition.definition
    if isinstance(key, PartitionKey.Single):
        if not isinstance(partition_expr.expr, str):
            raise ValueError(
                "partition_expr with a string column name is required "
                "for single-key partitioned assets"
            )
        return [(partition_expr.expr, definition, key.key[0])]

    if isinstance(key, PartitionKey.Multi):
        if not isinstance(partition_expr.expr, dict):
            raise ValueError(
                "partition_expr with a dict mapping is required "
                "for multi-key partitioned assets"
            )
        if missing := sorted(key.keys.keys() - partition_expr.expr.keys()):
            raise ValueError(f"partition_expr has no column for dimensions {missing}")
        definitions: dict[str, PartitionsDefinition] = {}
        if isinstance(definition, PartitionsDefinition.Multi):
            definitions = dict(definition.dimensions)
        return [
            (partition_expr.expr[dim], definitions.get(dim), values[0])
            for dim, values in sorted(key.keys.items())
        ]

    raise NotImplementedError(type(key))


def _expression(f: Filter, formats: dict[str, str]) -> BooleanExpression:
    column, op, value = f
    if (fmt := formats.get(column)) is not None:
        if op == "in":
            value = [datetime.strptime(v, fmt) for v in value]
        else:
            value = datetime.strptime(value, fmt)

    if op == "in":
        return In(column, value)  # pyright: ignore[reportCallIssue]
    return _COMPARISONS[op](column, value)


def _transform(
    definition: PartitionsDefinition | None, value: str, column_type: pa.DataType
) -> Transform:
    timestamp = pa.types.is_timestamp(column_type)
    if not isinstance(definition, PartitionsDefinition.TimeWindow) or not (
        timestamp or pa.types.is_date(column_type)
    ):
        return IdentityTransform()

    candidates = _TIME_TRANSFORMS if timestamp else _TIME_TRANSFORMS[:-1]
    window = PartitionContext(
        keys=[PartitionKey.single(value)], definition=definition
    ).time_window()
    for transform, truncate in candidates:
        if window is not None and all(truncate(bound) == bound for bound in window):
            return transform

    return candidates[-1][0]
