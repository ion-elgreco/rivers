"""Stress coverage for high-dimensionality `PartitionsDefinition.multi`.

Across the rest of the test suite, multi() is almost always 2D (74 of 75
constructions). This file exercises 5+ dimensions end-to-end:

- key enumeration (cartesian across Static + TimeWindow dims)
- materialization with a multi-dim key threaded through `context.partition`
- partition mapping (identity per dim) between two 5-dim assets
- backfill key expansion via `PartitionKeyRange.multi`
- IO handler round-trip (handler sees the full multi-dim partition context)
- mixing Dynamic in (registration via storage; enumeration is unsupported by
  design — `get_partition_keys()` raises NotImplementedError when any
  dimension is Dynamic)
"""

from datetime import datetime

import pytest
import rivers as rs

from _helpers import TrackingHandler, make_repo

DAILY_START = datetime(2024, 1, 1)
DAILY_END = datetime(2024, 1, 3)  # 2 daily keys


def _five_dim_static_timewindow():
    """5-dim definition spanning Static + TimeWindow only (fully enumerable).

    Cartesian count: 2 * 2 * 2 * 2 * 3 = 48 keys.
    """
    return rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
            "env": rs.PartitionsDefinition.static_(["staging", "prod"]),
            "date": rs.PartitionsDefinition.daily(start=DAILY_START, end=DAILY_END),
            "size": rs.PartitionsDefinition.static_(["s", "m", "l"]),
        }
    )


def _five_dim_with_dynamic():
    """5-dim definition including a Dynamic dimension.

    Cartesian product is unbounded by the static schema (depends on the
    Dynamic register at runtime), so `get_partition_keys()` is not callable
    on this — used only for materialization / explicit-key tests.
    """
    return rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
            "env": rs.PartitionsDefinition.static_(["staging", "prod"]),
            "date": rs.PartitionsDefinition.daily(start=DAILY_START, end=DAILY_END),
            "user": rs.PartitionsDefinition.dynamic("users"),
        }
    )


# ---------------------------------------------------------------------------
# get_partition_keys() — cartesian enumeration across 5 dims
# ---------------------------------------------------------------------------


def test_five_dim_get_partition_keys_full_cartesian():
    pd = _five_dim_static_timewindow()
    keys = pd.get_partition_keys()
    assert len(keys) == 2 * 2 * 2 * 2 * 3  # 48

    # Spot-check both ends of the cartesian product. `.keys` on a Multi
    # PartitionKey is `dict[str, list[str]]` (single-valued list per dim).
    sample = {
        "region": "us",
        "tier": "free",
        "env": "staging",
        "date": "2024-01-01",
        "size": "s",
    }
    assert any(
        isinstance(k, rs.PartitionKey.Multi)
        and {dim: vals[0] for dim, vals in k.keys.items()} == sample
        for k in keys
    )

    far_corner = {
        "region": "eu",
        "tier": "pro",
        "env": "prod",
        "date": "2024-01-02",
        "size": "l",
    }
    assert any(
        isinstance(k, rs.PartitionKey.Multi)
        and {dim: vals[0] for dim, vals in k.keys.items()} == far_corner
        for k in keys
    )


def test_five_dim_with_dynamic_get_partition_keys_unsupported():
    """Documents the design choice: enumeration of Multi-with-Dynamic is
    explicitly unsupported because Dynamic keys live in storage. Tests that
    rely on the cartesian product must avoid Dynamic dims; tests that need
    Dynamic must construct the keys explicitly."""
    pd = _five_dim_with_dynamic()
    with pytest.raises(NotImplementedError):
        pd.get_partition_keys()


# ---------------------------------------------------------------------------
# Materialization — full 5-dim key threaded through context + IO handler
# ---------------------------------------------------------------------------


def test_five_dim_materialize_threads_full_partition_context(storage):
    """A specific 5-dim key (mixing Static / TimeWindow / Dynamic) is visible
    on `context.partition` AND on the IO handler's OutputContext, with every
    dimension preserved."""
    pd = _five_dim_with_dynamic()
    captured = {}
    handler = TrackingHandler()

    @rs.Asset(partitions_def=pd, io_handler=handler)
    def my_asset(context: rs.AssetExecutionContext) -> int:
        captured["partition"] = context.partition
        return 1

    repo = make_repo([my_asset], storage=storage)
    storage.add_dynamic_partitions("users", ["alice", "bob"])

    key = rs.PartitionKey.multi(
        {
            "region": "us",
            "tier": "pro",
            "env": "prod",
            "date": "2024-01-01",
            "user": "alice",
        }
    )
    result = repo.materialize(["my_asset"], partition_key=key)
    assert result.success

    assert captured["partition"].key == key
    assert isinstance(captured["partition"].definition, rs.PartitionsDefinition.Multi)

    # IO handler saw the same multi-dim key
    assert len(handler.output_partitions) == 1
    assert handler.output_partitions[0].key == key

    # And only this one key is in storage's materialization log
    mat = storage.get_materialized_partitions("my_asset")
    assert key in mat
    other = rs.PartitionKey.multi(
        {
            "region": "eu",
            "tier": "free",
            "env": "staging",
            "date": "2024-01-02",
            "user": "bob",
        }
    )
    assert other not in mat


# ---------------------------------------------------------------------------
# Partition mapping — 5-dim → 5-dim identity
# ---------------------------------------------------------------------------


def test_five_dim_to_five_dim_identity_mapping(storage):
    """Identity mapping on every dim of a 5-dim chain: downstream materializes
    against the same multi-dim key as upstream, and the upstream load fetches
    the value for that exact key."""
    pd = _five_dim_with_dynamic()
    handler = TrackingHandler()
    handler.default_load_value = 1

    @rs.Asset(partitions_def=pd, io_handler=handler)
    def upstream() -> int:
        return 1

    @rs.Asset(
        partitions_def=pd,
        io_handler=handler,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi(
                    {
                        "region": rs.PartitionMapping.identity(),
                        "tier": rs.PartitionMapping.identity(),
                        "env": rs.PartitionMapping.identity(),
                        "date": rs.PartitionMapping.identity(),
                        "user": rs.PartitionMapping.identity(),
                    }
                ),
            ),
        ],
    )
    def downstream(upstream: int) -> int:
        return upstream + 1

    repo = make_repo([upstream, downstream], storage=storage)
    storage.add_dynamic_partitions("users", ["alice"])

    key = rs.PartitionKey.multi(
        {
            "region": "eu",
            "tier": "free",
            "env": "staging",
            "date": "2024-01-02",
            "user": "alice",
        }
    )
    result = repo.materialize(["upstream", "downstream"], partition_key=key)
    assert result.success

    # Both writes land at the same 5-dim key; the downstream load reads
    # from the same key — identity mapping with all 5 dims aligned.
    assert all(p.key == key for p in handler.output_partitions)
    assert all(p.key == key for p in handler.load_input_partitions)


# ---------------------------------------------------------------------------
# Backfill expansion — partition_range across 5 enumerable dims
# ---------------------------------------------------------------------------


def test_five_dim_backfill_partition_range_full_cartesian():
    """`PartitionKeyRange.multi(...)` expands across all 5 dims to the full
    cartesian product. Dimensions omitted from the range default to all
    values for that dim."""
    pd = _five_dim_static_timewindow()

    @rs.Asset(partitions_def=pd)
    def asset(context: rs.AssetExecutionContext) -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[asset],
        default_executor=rs.Executor.in_process(),
    )
    result = repo.backfill(
        selection=["asset"],
        partition_range=rs.PartitionKeyRange.multi(
            {
                "region": ["us", "eu"],
                "tier": ["free", "pro"],
                "env": ["staging"],  # 1 value
                "date": ("2024-01-01", "2024-01-02"),  # range → 2 keys
                "size": ["s", "m"],
            }
        ),
        dry_run=True,
    )
    # 2 * 2 * 1 * 2 * 2 = 16
    assert result.num_partitions == 16
    assert result.is_dry_run


def test_five_dim_backfill_partition_range_omitted_dim_includes_all():
    """Dimensions omitted from `PartitionKeyRange.multi(...)` should default
    to every value of that dim — important for end-to-end coverage that
    high-dim assets default sensibly when the user only constrains a few."""
    pd = _five_dim_static_timewindow()

    @rs.Asset(partitions_def=pd)
    def asset(context: rs.AssetExecutionContext) -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[asset],
        default_executor=rs.Executor.in_process(),
    )
    # Only constrain `region` and `date`; tier/env/size should expand fully.
    result = repo.backfill(
        selection=["asset"],
        partition_range=rs.PartitionKeyRange.multi(
            {
                "region": ["us"],
                "date": ("2024-01-01", "2024-01-01"),  # 1 day
            }
        ),
        dry_run=True,
    )
    # 1 region * 2 tier * 2 env * 1 date * 3 size = 12
    assert result.num_partitions == 12


def test_five_dim_backfill_explicit_keys_with_dynamic_dim(storage):
    """Backfill via explicit `partition_keys=[...]` works when the multi
    contains a Dynamic dim — there's no enumeration involved, the keys cross
    intact."""
    pd = _five_dim_with_dynamic()

    @rs.Asset(partitions_def=pd)
    def asset(context: rs.AssetExecutionContext) -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[asset],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    storage.add_dynamic_partitions("users", ["alice", "bob"])

    keys = [
        rs.PartitionKey.multi(
            {
                "region": "us",
                "tier": "free",
                "env": "prod",
                "date": "2024-01-01",
                "user": "alice",
            }
        ),
        rs.PartitionKey.multi(
            {
                "region": "eu",
                "tier": "pro",
                "env": "staging",
                "date": "2024-01-02",
                "user": "bob",
            }
        ),
    ]
    result = repo.backfill(selection=["asset"], partition_keys=keys, dry_run=True)
    assert result.num_partitions == 2
    assert result.is_dry_run
