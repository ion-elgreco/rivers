"""Partition keys as pyiceberg row filters, and the partition spec of a new table."""

from __future__ import annotations

import itertools
import math
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

_Dimension = tuple[str, list[str], PartitionsDefinition | None]

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

    pyiceberg checks the expression against every data file, so the keys of a
    backfill are merged: one ``In`` per dimension, and adjacent time windows
    in one range. Time-window bounds on date and timestamp columns are
    ``datetime`` values: pyiceberg cannot convert a key such as
    ``2024-01-01`` to a timestamp. Other values stay strings, which pyiceberg
    converts to the column type.

    Raises:
        ValueError: If ``partition_expr`` is ``None`` or does not fit the
            partition definition, or a partition column is not in ``schema``.
    """
    if partition is None:
        return AlwaysTrue()

    partition_expr = require_partition_expr(partition_expr)
    keys = [
        _key_dimensions(key, partition.definition, partition_expr)
        for key in partition.keys
    ]
    if (dimensions := _product(keys)) is not None:
        return _all(_dimension_filter(*dimension, schema) for dimension in dimensions)
    return _any(
        _all(_dimension_filter(*dimension, schema) for dimension in dimensions)
        for dimensions in keys
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


def _product(keys: list[list[_Dimension]]) -> list[_Dimension] | None:
    """The dimensions with all their values, if the keys are every combination of them."""
    combinations = {
        combination
        for dimensions in keys
        for combination in itertools.product(*(values for _, values, _ in dimensions))
    }
    merged = [
        (
            column,
            sorted({v for dimensions in keys for v in dimensions[i][1]}),
            definition,
        )
        for i, (column, _, definition) in enumerate(keys[0])
    ]
    if math.prod(len(values) for _, values, _ in merged) != len(combinations):
        return None
    return merged


def _dimension_filter(
    column: str,
    values: list[str],
    definition: PartitionsDefinition | None,
    schema: Schema,
) -> BooleanExpression:
    """The rows of one dimension's values: time ranges for time-window values."""
    if not isinstance(definition, PartitionsDefinition.TimeWindow):
        return In(column, values)  # pyright: ignore[reportCallIssue]

    windows = [_window(value, definition) for value in values]
    if None in windows:
        return In(column, values)  # pyright: ignore[reportCallIssue]

    ranges = _merge([window for window in windows if window is not None])
    temporal = DateType | TimestampType | TimestamptzType
    bounds = (
        ranges
        if isinstance(schema.find_type(column), temporal)
        else [
            (s.strftime(definition.fmt), e.strftime(definition.fmt)) for s, e in ranges
        ]
    )
    return _any(_range(column, start, end) for start, end in bounds)


def _merge(windows: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    """The windows as ranges, with adjacent and overlapping windows joined."""
    ranges: list[tuple[datetime, datetime]] = []
    for start, end in sorted(windows):
        if ranges and start <= ranges[-1][1]:
            ranges[-1] = (ranges[-1][0], max(ranges[-1][1], end))
        else:
            ranges.append((start, end))
    return ranges


def _range(column: str, start: object, end: object) -> BooleanExpression:
    """``start <= column < end``."""
    return And(
        GreaterThanOrEqual(column, start),  # pyright: ignore[reportCallIssue]
        LessThan(column, end),  # pyright: ignore[reportCallIssue]
    )


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
