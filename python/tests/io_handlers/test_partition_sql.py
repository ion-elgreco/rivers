"""Partition-key SQL predicates shared by the Delta, DuckDB and Snowflake handlers."""

from datetime import datetime

import rivers as rs
from rivers.io_handlers._partition_sql import (
    PartitionExpr,
    _build_partition_filters,
    _build_predicate,
)


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


def test_default_quote_keeps_backslashes():
    """DuckDB and DataFusion read a backslash as itself: only quotes are doubled."""
    ctx = rs.PartitionContext(
        keys=[rs.PartitionKey.single("C:\\it's")],
        definition=rs.PartitionsDefinition.static_(["C:\\it's"]),
    )
    assert _build_predicate(ctx, PartitionExpr(expr="col")) == "col = 'C:\\it''s'"


def test_quote_renders_every_value():
    """Keys, IN lists, time bounds, and factored dimensions all go through ``quote``."""

    def angle(value: str) -> str:
        return f"<{value}>"

    def predicate(definition, expr, *keys):
        ctx = rs.PartitionContext(keys=list(keys), definition=definition)
        return _build_predicate(ctx, PartitionExpr(expr=expr), quote=angle)

    single = rs.PartitionKey.single
    static = rs.PartitionsDefinition.static_(["a", "b"])
    assert predicate(static, "col", single("a")) == "col = <a>"
    assert predicate(static, "col", single("a"), single("b")) == "col IN (<a>, <b>)"

    daily = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))
    assert predicate(daily, "day", single("2024-01-15"), single("2024-01-16")) == (
        "(day >= <2024-01-15> AND day < <2024-01-16>) OR "
        "(day >= <2024-01-16> AND day < <2024-01-17>)"
    )

    multi = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
        }
    )
    dims = {"region": "region", "tier": "tier"}
    us_free, us_pro, eu_pro = (
        rs.PartitionKey.multi({"region": region, "tier": tier})
        for region, tier in (("us", "free"), ("us", "pro"), ("eu", "pro"))
    )
    assert predicate(multi, dims, us_free, us_pro) == (
        "region = <us> AND tier IN (<free>, <pro>)"
    )
    assert predicate(multi, dims, us_free, eu_pro) == (
        "(region = <us> AND tier = <free>) OR (region = <eu> AND tier = <pro>)"
    )


def test_factored_days_become_an_or_of_ranges():
    """Several days in a batch select each day's range, so the column can be a timestamp."""
    definition = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "date": rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1)),
        }
    )
    ctx = rs.PartitionContext(
        keys=[
            rs.PartitionKey.multi({"region": region, "date": day})
            for region in ("us", "eu")
            for day in ("2024-01-15", "2024-01-16")
        ],
        definition=definition,
    )

    pred = _build_predicate(
        ctx,
        PartitionExpr(expr={"region": "region", "date": "ts"}),
        quote=lambda value: f"<{value}>",
    )

    assert pred == (
        "((ts >= <2024-01-15> AND ts < <2024-01-16>) OR "
        "(ts >= <2024-01-16> AND ts < <2024-01-17>)) AND region IN (<us>, <eu>)"
    )


def test_a_key_with_several_days_selects_each_range():
    """Mapped reads (specific_partitions, multi_to_single) pass one key with several days."""
    daily = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))
    multi = rs.PartitionsDefinition.multi(
        {"region": rs.PartitionsDefinition.static_(["us", "eu"]), "date": daily}
    )
    days = ["2024-01-15", "2024-01-16"]
    ranges = (
        "((ts >= '2024-01-15' AND ts < '2024-01-16') OR "
        "(ts >= '2024-01-16' AND ts < '2024-01-17'))"
    )

    single = rs.PartitionContext(keys=[rs.PartitionKey.single(days)], definition=daily)
    assert _build_predicate(single, PartitionExpr(expr="ts")) == ranges

    us = rs.PartitionContext(
        keys=[rs.PartitionKey.multi({"region": "us", "date": days})], definition=multi
    )
    expr = PartitionExpr(expr={"region": "region", "date": "ts"})
    assert _build_predicate(us, expr) == f"{ranges} AND region = 'us'"


def test_bound_renders_the_time_bounds_of_its_columns():
    """``bound`` gets each range bound as a datetime; ``None`` keeps the key format's quoted text."""
    daily = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1), fmt="%Y%m%d")
    multi = rs.PartitionsDefinition.multi(
        {"region": rs.PartitionsDefinition.static_(["us", "eu"]), "date": daily}
    )

    def bound(col: str, at: datetime) -> str | None:
        return f"<{at:%Y-%m-%d %H:%M}>" if col == "ts" else None

    def predicate(definition, expr, *keys):
        ctx = rs.PartitionContext(keys=list(keys), definition=definition)
        return _build_predicate(ctx, PartitionExpr(expr=expr), bound=bound)

    day, next_day = (
        rs.PartitionKey.single("20240115"),
        rs.PartitionKey.single("20240116"),
    )
    assert predicate(daily, "ts", day) == (
        "ts >= <2024-01-15 00:00> AND ts < <2024-01-16 00:00>"
    )
    assert predicate(daily, "day_text", day) == (
        "day_text >= '20240115' AND day_text < '20240116'"
    )
    assert predicate(daily, "ts", day, next_day) == (
        "(ts >= <2024-01-15 00:00> AND ts < <2024-01-16 00:00>) OR "
        "(ts >= <2024-01-16 00:00> AND ts < <2024-01-17 00:00>)"
    )

    dims = {"region": "region", "date": "ts"}
    both_days = rs.PartitionKey.multi(
        {"region": "us", "date": ["20240115", "20240116"]}
    )
    assert predicate(multi, dims, both_days) == (
        "((ts >= <2024-01-15 00:00> AND ts < <2024-01-16 00:00>) OR "
        "(ts >= <2024-01-16 00:00> AND ts < <2024-01-17 00:00>)) AND region = 'us'"
    )
    us, eu = (
        rs.PartitionKey.multi({"region": region, "date": "20240115"})
        for region in ("us", "eu")
    )
    assert predicate(multi, dims, us, eu) == (
        "ts >= <2024-01-15 00:00> AND ts < <2024-01-16 00:00> AND region IN ('us', 'eu')"
    )


def test_partition_filters_keep_bounds_as_text():
    """delta-rs compares the partition values, which are text in the key format."""
    daily = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1), fmt="%Y%m%d")
    ctx = rs.PartitionContext(
        keys=[rs.PartitionKey.single("20240115")], definition=daily
    )

    assert _build_partition_filters(ctx, PartitionExpr(expr="day")) == [
        [("day", ">=", "20240115"), ("day", "<", "20240116")]
    ]
