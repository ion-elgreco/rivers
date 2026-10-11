"""Partition-key SQL predicates shared by the Delta and DuckDB handlers."""

from datetime import datetime

import rivers as rs
from rivers.io_handlers._partition_sql import PartitionExpr, _build_predicate


def test_backfill_predicate_single_static_multi_keys():
    """Multiple static partition keys produce an IN predicate."""
    pd = rs.PartitionsDefinition.static_(["a", "b", "c", "d"])
    ctx = rs.PartitionContext(
        keys=[
            rs.PartitionKey.single("a"),
            rs.PartitionKey.single("b"),
            rs.PartitionKey.single("c"),
        ],
        definition=pd,
    )
    pred = _build_predicate(ctx, PartitionExpr(expr="region"))
    assert pred == "region IN ('a', 'b', 'c')"


def test_backfill_predicate_single_key_equals():
    """Single key still produces simple equality."""
    pd = rs.PartitionsDefinition.static_(["x"])
    ctx = rs.PartitionContext(
        keys=[rs.PartitionKey.single("x")],
        definition=pd,
    )
    pred = _build_predicate(ctx, PartitionExpr(expr="col"))
    assert pred == "col = 'x'"


def test_backfill_predicate_time_window_multi_keys():
    """Multiple time window keys produce OR of range predicates."""
    pd = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))
    ctx = rs.PartitionContext(
        keys=[
            rs.PartitionKey.single("2024-01-15"),
            rs.PartitionKey.single("2024-01-16"),
        ],
        definition=pd,
    )
    pred = _build_predicate(ctx, PartitionExpr(expr="date"))
    assert pred == (
        "(date >= '2024-01-15' AND date < '2024-01-16') OR "
        "(date >= '2024-01-16' AND date < '2024-01-17')"
    )


def test_backfill_predicate_multi_partition_keys_non_cartesian():
    """Non-cartesian multi-dimension keys produce OR of AND predicates."""
    pd = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
        }
    )
    # (us,free) and (eu,pro) is NOT a cartesian product of {us,eu}×{free,pro}
    # because (us,pro) and (eu,free) are missing → must use OR
    ctx = rs.PartitionContext(
        keys=[
            rs.PartitionKey.multi({"region": "us", "tier": "free"}),
            rs.PartitionKey.multi({"region": "eu", "tier": "pro"}),
        ],
        definition=pd,
    )
    pred = _build_predicate(
        ctx, PartitionExpr(expr={"region": "region", "tier": "tier"})
    )
    assert pred == (
        "(region = 'us' AND tier = 'free') OR (region = 'eu' AND tier = 'pro')"
    )


def test_backfill_predicate_multi_partition_keys_cartesian():
    """Cartesian product of multi-dimension keys produces factored AND + IN."""
    pd = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
        }
    )
    # Full cartesian product: {us,eu} × {free,pro} = 4 keys
    # → factored: region IN ('us', 'eu') AND tier IN ('free', 'pro')
    ctx = rs.PartitionContext(
        keys=[
            rs.PartitionKey.multi({"region": "us", "tier": "free"}),
            rs.PartitionKey.multi({"region": "us", "tier": "pro"}),
            rs.PartitionKey.multi({"region": "eu", "tier": "free"}),
            rs.PartitionKey.multi({"region": "eu", "tier": "pro"}),
        ],
        definition=pd,
    )
    pred = _build_predicate(
        ctx, PartitionExpr(expr={"region": "region", "tier": "tier"})
    )
    assert pred == "region IN ('us', 'eu') AND tier IN ('free', 'pro')"


def test_backfill_predicate_multi_fixed_dimension():
    """PerDimension-style: one fixed dim + varying dim → equality AND IN."""
    pd = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu", "apac"]),
            "date": rs.PartitionsDefinition.static_(["d1", "d2", "d3"]),
        }
    )
    # PerDimension(multi_run=["region"], single_run=["date"]) for region=us:
    # 3 keys all sharing region=us, different dates
    ctx = rs.PartitionContext(
        keys=[
            rs.PartitionKey.multi({"region": "us", "date": "d1"}),
            rs.PartitionKey.multi({"region": "us", "date": "d2"}),
            rs.PartitionKey.multi({"region": "us", "date": "d3"}),
        ],
        definition=pd,
    )
    pred = _build_predicate(
        ctx, PartitionExpr(expr={"region": "region_col", "date": "date_col"})
    )
    assert pred == "date_col IN ('d1', 'd2', 'd3') AND region_col = 'us'"


def test_backfill_predicate_multi_three_dims_partial_cartesian():
    """3 dimensions, full cartesian on 2 dims + 1 fixed → factored."""
    pd = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us"]),
            "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
            "env": rs.PartitionsDefinition.static_(["staging", "prod"]),
        }
    )
    # region fixed=us, tier×env full product = 4 keys
    ctx = rs.PartitionContext(
        keys=[
            rs.PartitionKey.multi({"region": "us", "tier": "free", "env": "staging"}),
            rs.PartitionKey.multi({"region": "us", "tier": "free", "env": "prod"}),
            rs.PartitionKey.multi({"region": "us", "tier": "pro", "env": "staging"}),
            rs.PartitionKey.multi({"region": "us", "tier": "pro", "env": "prod"}),
        ],
        definition=pd,
    )
    pred = _build_predicate(
        ctx,
        PartitionExpr(expr={"region": "region", "tier": "tier", "env": "env"}),
    )
    assert (
        pred
        == "env IN ('staging', 'prod') AND region = 'us' AND tier IN ('free', 'pro')"
    )
