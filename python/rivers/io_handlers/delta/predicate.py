"""SQL predicates and delta-rs partition filters from rivers partition keys."""

from __future__ import annotations

import json
from typing import Any

from rivers._core.partitions import PartitionContext, PartitionKey, PartitionsDefinition
from rivers.io_handlers.delta.config import PartitionExpr

DELTA_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def _resolve_partition_expr(meta: dict[str, Any]) -> PartitionExpr | None:
    """Decode a ``delta/partition_expr`` asset metadata entry into a :class:`PartitionExpr`.

    Accepts either a JSON-encoded mapping (multi-dim) or a plain string column
    name (single-dim). Returns ``None`` when the key is absent.
    """
    raw = meta.get("delta/partition_expr")
    if raw is None:
        return None
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        parsed = raw
    return PartitionExpr(expr=parsed)


def _sql_quote(value: str) -> str:
    """Escape a string value for use in a SQL predicate."""
    return "'" + value.replace("'", "''") + "'"


Filter = tuple[str, str, Any]
"""One delta-rs partition filter: ``(column, op, value)``."""


def _time_window_filters(col: str, tw: tuple, fmt: str) -> list[Filter]:
    """Half-open ``col >= start AND col < end`` filters for a time window."""
    return [(col, ">=", tw[0].strftime(fmt)), (col, "<", tw[1].strftime(fmt))]


def _col_filters(col: str, values: list[str]) -> list[Filter]:
    """An equality or IN filter on a single column."""
    if len(values) == 1:
        return [(col, "=", values[0])]
    return [(col, "in", list(values))]


def _dim_filters(
    col: str, vals: list[str], dim_def: PartitionsDefinition | None
) -> list[Filter]:
    """Filters for one dimension: a range for a single time-window value."""
    if len(vals) == 1 and isinstance(dim_def, PartitionsDefinition.TimeWindow):
        dim_ctx = PartitionContext(
            keys=[PartitionKey.single(vals[0])], definition=dim_def
        )
        tw = dim_ctx.time_window()
        if tw is not None:
            return _time_window_filters(col, tw, dim_def.fmt)
    return _col_filters(col, vals)


def _render(filters: list[Filter]) -> str:
    """The SQL predicate for a conjunction of filters."""
    return " AND ".join(
        f"{col} IN ({', '.join(_sql_quote(v) for v in value)})"
        if op == "in"
        else f"{col} {op} {_sql_quote(value)}"
        for col, op, value in filters
    )


def _filters_for_key(
    key: PartitionKey,
    definition: PartitionsDefinition,
    partition_expr: PartitionExpr,
) -> list[Filter]:
    """The conjunction of filters that selects a single partition key."""
    if isinstance(key, PartitionKey.Single):
        if not isinstance(partition_expr.expr, str):
            raise ValueError(
                "partition_expr with a string column name is required "
                "for single-key partitioned assets"
            )
        col = partition_expr.expr
        if isinstance(definition, PartitionsDefinition.TimeWindow):
            ctx = PartitionContext(keys=[key], definition=definition)
            tw = ctx.time_window()
            if tw is not None:
                return _time_window_filters(col, tw, definition.fmt)
        return _col_filters(col, key.key)
    elif isinstance(key, PartitionKey.Multi):
        if not isinstance(partition_expr.expr, dict):
            raise ValueError(
                "partition_expr with a dict mapping is required "
                "for multi-key partitioned assets"
            )
        dim_defs: dict[str, PartitionsDefinition] = {}
        if isinstance(definition, PartitionsDefinition.Multi):
            dim_defs = {name: defn for name, defn in definition.dimensions}
        return [
            f
            for dim, vals in sorted(key.keys.items())
            for f in _dim_filters(partition_expr.expr[dim], vals, dim_defs.get(dim))
        ]
    else:
        raise NotImplementedError(type(key))


def _build_predicate_for_key(
    key: PartitionKey,
    definition: PartitionsDefinition,
    partition_expr: PartitionExpr,
) -> str:
    """Build a SQL predicate for a single partition key."""
    return _render(_filters_for_key(key, definition, partition_expr))


def _build_partition_filters(
    partition: PartitionContext, partition_expr: PartitionExpr | None
) -> list[list[Filter]]:
    """delta-rs ``partition_filters`` covering the context, one conjunction per key.

    ``optimize`` takes a single conjunction (no OR), so a multi-key context
    yields one filter list per key.
    """
    if partition_expr is None:
        raise ValueError(
            "PartitionExpr is required if a partition definition is defined."
        )
    return [
        _filters_for_key(k, partition.definition, partition_expr)
        for k in partition.keys
    ]


def _build_predicate(
    partition: PartitionContext, partition_expr: PartitionExpr | None
) -> str:
    """Build a SQL predicate covering all partition keys in the context.

    For normal materialization (1 key), produces a simple predicate.
    For backfill with multiple keys, produces an OR of per-key predicates
    or an IN clause when possible.
    """
    if partition_expr is None:
        raise ValueError(
            "PartitionExpr is required if a partition definition is defined."
        )

    keys = partition.keys
    if len(keys) == 1:
        return _build_predicate_for_key(keys[0], partition.definition, partition_expr)

    # Multiple keys (backfill): try to build an efficient combined predicate
    # For Single keys with static partitions, merge into one IN clause
    if all(isinstance(k, PartitionKey.Single) for k in keys) and isinstance(
        partition_expr.expr, str
    ):
        col = partition_expr.expr
        # Check if time-windowed — need OR of range predicates
        if isinstance(partition.definition, PartitionsDefinition.TimeWindow):
            predicates = [
                _build_predicate_for_key(k, partition.definition, partition_expr)
                for k in keys
            ]
            return " OR ".join(f"({p})" for p in predicates)
        # Static: merge all key values into one IN clause
        all_vals = []
        for k in keys:
            assert isinstance(k, PartitionKey.Single)
            all_vals.extend(k.key)
        return _render(_col_filters(col, all_vals))

    # Multi-dimension keys: factor out shared dimension values into AND + IN
    # e.g. keys [(region=us, tier=free), (region=us, tier=pro)]
    #   → region = 'us' AND tier IN ('free', 'pro')
    # instead of (region = 'us' AND tier = 'free') OR (region = 'us' AND tier = 'pro')
    if all(isinstance(k, PartitionKey.Multi) for k in keys) and isinstance(
        partition_expr.expr, dict
    ):
        return _build_factored_multi_predicate(
            keys, partition.definition, partition_expr
        )

    # Fallback: OR of individual predicates
    predicates = [
        _build_predicate_for_key(k, partition.definition, partition_expr) for k in keys
    ]
    if len(predicates) == 1:
        return predicates[0]
    return " OR ".join(f"({p})" for p in predicates)


def _build_factored_multi_predicate(
    keys: list[PartitionKey],
    definition: PartitionsDefinition,
    partition_expr: PartitionExpr,
) -> str:
    """Build an optimized predicate for multiple multi-dimension partition keys.

    For each dimension, collects all unique values across all keys. If a
    dimension has a single unique value, it becomes an equality predicate.
    If it has multiple values, it becomes an IN predicate. All dimensions
    are AND'ed together.

    This produces predicates like:
        region = 'us' AND tier IN ('free', 'pro')
    instead of:
        (region = 'us' AND tier = 'free') OR (region = 'us' AND tier = 'pro')
    """
    assert isinstance(partition_expr.expr, dict)

    dim_defs: dict[str, PartitionsDefinition] = {}
    if isinstance(definition, PartitionsDefinition.Multi):
        dim_defs = {name: defn for name, defn in definition.dimensions}

    # Collect all unique values per dimension across all keys
    dim_values: dict[str, list[str]] = {}
    for key in keys:
        assert isinstance(key, PartitionKey.Multi)
        for dim, vals in key.keys.items():
            if dim not in dim_values:
                dim_values[dim] = []
            for v in vals:
                if v not in dim_values[dim]:
                    dim_values[dim].append(v)

    # Check if the factored form is valid: the keys must be the full cartesian
    # product of the per-dimension values. Otherwise we'd over-select rows.
    # e.g. keys [(us,free), (eu,pro)] is NOT the product of {us,eu} × {free,pro}
    # because (us,pro) and (eu,free) are missing.
    expected_count = 1
    for vals in dim_values.values():
        expected_count *= len(vals)

    if expected_count != len(keys):
        # Not a full cartesian product — fall back to OR of individual predicates
        predicates = [
            _build_predicate_for_key(k, definition, partition_expr) for k in keys
        ]
        return " OR ".join(f"({p})" for p in predicates)

    return _render(
        [
            f
            for dim in sorted(dim_values)
            for f in _dim_filters(
                partition_expr.expr[dim], dim_values[dim], dim_defs.get(dim)
            )
        ]
    )
