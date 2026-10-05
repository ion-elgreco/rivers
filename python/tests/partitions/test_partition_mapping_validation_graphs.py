from datetime import datetime
from typing import Any

import pytest

import rivers as rs
from rivers.exceptions import PartitionValidationError

from .mapping_validation_helpers import DAILY_START, STATIC_KEYS, make_repo


# ---------------------------------------------------------------------------
# Multi-hop chains
# ---------------------------------------------------------------------------


def test_three_asset_chain_all_partitioned():
    """A → B → C all with same static partitions should work with default Identity."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset(partitions_def=parts)
    def a() -> Any:
        return 1

    @rs.Asset(partitions_def=parts)
    def b(a: Any) -> Any:
        return a + 1

    @rs.Asset(partitions_def=parts)
    def c(b: Any) -> Any:
        return b + 1

    repo = make_repo([a, b, c])
    assert repo is not None


def test_chain_with_mixed_partitions():
    """A (static) → B (daily) with no explicit mapping should fail Identity check."""
    static_parts = rs.PartitionsDefinition.static_(STATIC_KEYS)
    daily_parts = rs.PartitionsDefinition.daily(start=DAILY_START)

    @rs.Asset(partitions_def=static_parts)
    def a() -> Any:
        return 1

    @rs.Asset(partitions_def=daily_parts)
    def b(a: Any) -> Any:
        return a + 1

    with pytest.raises(
        PartitionValidationError, match="Identity mapping requires same partition type"
    ):
        make_repo([a, b])


def test_chain_with_mixed_partitions_all_mapping():
    """A (static) → B (daily) with AllPartitions mapping should work."""
    static_parts = rs.PartitionsDefinition.static_(STATIC_KEYS)
    daily_parts = rs.PartitionsDefinition.daily(start=DAILY_START)

    @rs.Asset(partitions_def=static_parts)
    def a() -> Any:
        return 1

    @rs.Asset(
        partitions_def=daily_parts,
        deps=[
            rs.AssetDef.input(
                "a", partition_mapping=rs.PartitionMapping.all_partitions()
            )
        ],
    )
    def b(a: Any) -> Any:
        return a + 1

    repo = make_repo([a, b])
    assert repo is not None


# ---------------------------------------------------------------------------
# Diamond dependency
# ---------------------------------------------------------------------------


def test_diamond_all_same_partitions():
    """Diamond: A → B, A → C, B+C → D with same partitions should work."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset(partitions_def=parts)
    def a() -> Any:
        return 1

    @rs.Asset(partitions_def=parts)
    def b(a: Any) -> Any:
        return a + 1

    @rs.Asset(partitions_def=parts)
    def c(a: Any) -> Any:
        return a * 2

    @rs.Asset(partitions_def=parts)
    def d(b: Any, c: Any) -> Any:
        return b + c

    repo = make_repo([a, b, c, d])
    assert repo is not None


def test_diamond_mixed_partitions():
    """Diamond with mixed partitions should require explicit mappings."""
    static_parts = rs.PartitionsDefinition.static_(STATIC_KEYS)
    daily_parts = rs.PartitionsDefinition.daily(start=DAILY_START)

    @rs.Asset(partitions_def=static_parts)
    def a() -> Any:
        return 1

    @rs.Asset(partitions_def=static_parts)
    def b(a: Any) -> Any:
        return a + 1

    @rs.Asset(partitions_def=daily_parts)
    def c(a: Any) -> Any:
        return a * 2

    # c depends on a with mismatched types — should fail
    with pytest.raises(
        PartitionValidationError, match="Identity mapping requires same partition type"
    ):
        make_repo([a, b, c])


# ---------------------------------------------------------------------------
# Multiple dependencies with different mappings
# ---------------------------------------------------------------------------


def test_multiple_deps_different_mappings():
    """Asset with multiple deps, each with a different partition mapping."""
    daily_parts = rs.PartitionsDefinition.daily(start=DAILY_START)
    static_parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset(partitions_def=daily_parts)
    def time_source() -> Any:
        return 1

    @rs.Asset(partitions_def=static_parts)
    def category_source() -> Any:
        return 2

    @rs.Asset(
        partitions_def=daily_parts,
        deps=[
            rs.AssetDef.input(
                "time_source",
                partition_mapping=rs.PartitionMapping.time_window(offset=-1),
            ),
            rs.AssetDef.input(
                "category_source",
                partition_mapping=rs.PartitionMapping.all_partitions(),
            ),
        ],
    )
    def combined(time_source: Any, category_source: Any) -> Any:
        return time_source + category_source

    repo = make_repo([time_source, category_source, combined])
    assert repo is not None


# ---------------------------------------------------------------------------
# Hourly partitions
# ---------------------------------------------------------------------------


def test_hourly_partitions_time_window_mapping():
    """Hourly partitions with TimeWindow mapping should work."""
    parts = rs.PartitionsDefinition.hourly(start=DAILY_START)

    @rs.Asset(partitions_def=parts)
    def hourly_source() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "hourly_source",
                partition_mapping=rs.PartitionMapping.time_window(offset=-2),
            )
        ],
    )
    def hourly_derived(hourly_source: Any) -> Any:
        return hourly_source + 1

    repo = make_repo([hourly_source, hourly_derived])
    assert repo is not None


# ---------------------------------------------------------------------------
# External asset with partitions
# ---------------------------------------------------------------------------


def test_external_partitioned_upstream():
    """External partitioned asset as upstream should require mapping on unpartitioned downstream."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    from rivers import InMemoryIOHandler

    ext = rs.Asset.external(
        name="ext_source",
        io_handler=InMemoryIOHandler(),
        partitions_def=parts,
    )

    @rs.Asset(
        deps=[
            rs.AssetDef.input(
                "ext_source", partition_mapping=rs.PartitionMapping.all_partitions()
            )
        ],
    )
    def consumer(ext_source: Any) -> Any:
        return ext_source

    repo = make_repo([ext, consumer])
    assert repo is not None


def test_external_partitioned_upstream_no_mapping():
    """External partitioned asset without mapping on unpartitioned downstream should fail."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    from rivers import InMemoryIOHandler

    ext = rs.Asset.external(
        name="ext_source",
        io_handler=InMemoryIOHandler(),
        partitions_def=parts,
    )

    @rs.Asset
    def consumer(ext_source: Any) -> Any:
        return ext_source

    with pytest.raises(
        PartitionValidationError, match="partition_mapping.*is required"
    ):
        make_repo([ext, consumer])


# ---------------------------------------------------------------------------
# Tasks with partitions
# ---------------------------------------------------------------------------


def test_task_with_partitioned_upstream_no_mapping_fails():
    """An unpartitioned task depending on a partitioned asset requires explicit mapping."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset(partitions_def=parts)
    def source() -> Any:
        return 1

    @rs.Task
    def process(source: Any) -> Any:
        return source + 1

    with pytest.raises(
        PartitionValidationError, match="partition_mapping.*is required"
    ):
        make_repo([source], tasks=[process])


def test_task_with_partitioned_upstream_all_partitions():
    """An unpartitioned task can depend on a partitioned asset via AllPartitions mapping."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset(partitions_def=parts)
    def source() -> Any:
        return 1

    @rs.Task(partition_mapping={"source": rs.PartitionMapping.all_partitions()})
    def process(source: Any) -> Any:
        return source + 1

    repo = make_repo([source], tasks=[process])
    assert repo is not None


def test_task_with_partitions_def_identity():
    """A partitioned task depending on a partitioned asset with same def uses Identity."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset(partitions_def=parts)
    def source() -> Any:
        return 1

    @rs.Task(partitions_def=parts)
    def process(source: Any) -> Any:
        return source + 1

    repo = make_repo([source], tasks=[process])
    assert repo is not None


def test_task_with_partitions_def_mismatch():
    """A partitioned task with different partition type should fail with Identity."""
    static_parts = rs.PartitionsDefinition.static_(STATIC_KEYS)
    daily_parts = rs.PartitionsDefinition.daily(DAILY_START, end=datetime(2024, 1, 5))

    @rs.Asset(partitions_def=static_parts)
    def source() -> Any:
        return 1

    @rs.Task(partitions_def=daily_parts)
    def process(source: Any) -> Any:
        return source + 1

    with pytest.raises(
        PartitionValidationError, match="Identity mapping requires same partition type"
    ):
        make_repo([source], tasks=[process])


def test_task_partitioned_with_unpartitioned_upstream():
    """A partitioned task depending on an unpartitioned asset is fine (shared dep)."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset
    def source() -> Any:
        return 1

    @rs.Task(partitions_def=parts)
    def process(source: Any) -> Any:
        return source + 1

    repo = make_repo([source], tasks=[process])
    assert repo is not None


# ---------------------------------------------------------------------------
# Definitions are re-validated at resolve — the PyO3 per-variant constructors
# (PartitionsDefinition.TimeWindow(...) etc.) bypass the factory staticmethods
# and every construction-time guard with them.
# ---------------------------------------------------------------------------


def test_resolve_rejects_variant_constructed_def_bypassing_fmt_validation():
    bad = rs.PartitionsDefinition.TimeWindow(
        cron_schedule="0 * * * *",
        interval_seconds=None,
        start=datetime(2024, 1, 1),
        end=None,
        fmt="%Y-%m-%d",
    )

    @rs.Asset(partitions_def=bad)
    def standalone() -> Any:
        return 1

    with pytest.raises(
        PartitionValidationError, match="cannot represent the partition grid"
    ):
        make_repo([standalone])


def test_resolve_rejects_variant_constructed_static_with_reserved_key():
    bad = rs.PartitionsDefinition.Static(keys=["us|eu"])

    @rs.Asset(partitions_def=bad)
    def standalone() -> Any:
        return 1

    with pytest.raises(PartitionValidationError, match="reserved character"):
        make_repo([standalone])


def test_resolve_rejects_variant_constructed_multi_with_bad_dim_name():
    bad = rs.PartitionsDefinition.Multi(
        dimensions=[("a=b", rs.PartitionsDefinition.static_(["x"]))]
    )

    @rs.Asset(partitions_def=bad)
    def standalone() -> Any:
        return 1

    with pytest.raises(PartitionValidationError, match="reserved character"):
        make_repo([standalone])


def test_resolve_rejects_variant_constructed_multi_with_duplicate_dim_names():
    """The multi() factory's dict input makes duplicate names impossible, but
    the raw variant constructor takes a list of tuples: a duplicated name
    silently collapses the universe (the enumeration paths even disagree on
    which dimension survives) and the minted keys fail the definition's own
    validation."""
    bad = rs.PartitionsDefinition.Multi(
        dimensions=[
            ("d", rs.PartitionsDefinition.static_(["x1", "x2"])),
            ("d", rs.PartitionsDefinition.static_(["y1", "y2"])),
        ]
    )

    @rs.Asset(partitions_def=bad)
    def standalone() -> Any:
        return 1

    with pytest.raises(PartitionValidationError, match="duplicate dimension name"):
        make_repo([standalone])


def test_factory_constructed_defs_resolve_fine():
    """The re-validation must not reject anything the factories produce."""
    pd = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
        }
    )

    @rs.Asset(partitions_def=pd)
    def standalone() -> Any:
        return 1

    assert make_repo([standalone]) is not None
