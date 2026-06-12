"""Tests for partition mapping validation during graph resolution."""

import re
from datetime import datetime
from typing import Any

import pytest

import rivers as rs
from rivers.exceptions import PartitionValidationError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DAILY_START = datetime(2024, 1, 1)
STATIC_KEYS = ["a", "b", "c"]


def make_repo(assets, tasks=None, resolve=True):
    """Build and resolve a CodeRepository."""
    repo = rs.CodeRepository(assets=assets, tasks=tasks)
    if resolve:
        repo.resolve()
    return repo


# ---------------------------------------------------------------------------
# Both partitioned — Identity (default)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "make_parts",
    [
        pytest.param(lambda: rs.PartitionsDefinition.static_(STATIC_KEYS), id="static"),
        pytest.param(
            lambda: rs.PartitionsDefinition.daily(start=DAILY_START), id="daily"
        ),
    ],
)
def test_identity_mapping_same_partitions(make_parts):
    """Identity mapping resolves when both sides share the same partition definition."""
    parts = make_parts()

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(partitions_def=parts)
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


def test_identity_mapping_mismatched_partition_types():
    """Identity mapping with different partition types should fail."""
    static_parts = rs.PartitionsDefinition.static_(STATIC_KEYS)
    daily_parts = rs.PartitionsDefinition.daily(start=DAILY_START)

    @rs.Asset(partitions_def=daily_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(partitions_def=static_parts)
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError, match="Identity mapping requires same partition type"
    ):
        make_repo([upstream, downstream])


def test_explicit_identity_mapping_same_type():
    """Explicit Identity mapping with same types should work."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.identity()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


# ---------------------------------------------------------------------------
# Both partitioned — TimeWindow mapping
# ---------------------------------------------------------------------------


def test_time_window_mapping_both_daily():
    """TimeWindow mapping with both sides daily should work."""
    parts = rs.PartitionsDefinition.daily(start=DAILY_START)

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.time_window(offset=-1)
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


def test_time_window_mapping_downstream_not_time_window():
    """TimeWindow mapping on downstream with static partitions should fail."""
    static_parts = rs.PartitionsDefinition.static_(STATIC_KEYS)
    daily_parts = rs.PartitionsDefinition.daily(start=DAILY_START)

    @rs.Asset(partitions_def=daily_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=static_parts,
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.time_window(offset=-1)
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError,
        match="TimeWindow mapping requires TimeWindow partitions on downstream",
    ):
        make_repo([upstream, downstream])


def test_time_window_mapping_upstream_not_time_window():
    """TimeWindow mapping on upstream with static partitions should fail."""
    static_parts = rs.PartitionsDefinition.static_(STATIC_KEYS)
    daily_parts = rs.PartitionsDefinition.daily(start=DAILY_START)

    @rs.Asset(partitions_def=static_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=daily_parts,
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.time_window(offset=-1)
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError,
        match="TimeWindow mapping requires TimeWindow partitions on upstream",
    ):
        make_repo([upstream, downstream])


def _tw_edge(down_def, up_def):
    """Upstream→downstream pair joined by time_window(offset=-1)."""

    @rs.Asset(partitions_def=up_def)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=down_def,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.time_window(offset=-1),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    return [upstream, downstream]


def test_time_window_mapping_fmt_mismatch_rejected():
    """Differing key formats: downstream keys can't reliably parse under the
    upstream fmt — the eval path would silently drop them."""
    down = rs.PartitionsDefinition.daily(start=DAILY_START)
    up = rs.PartitionsDefinition.daily(start=DAILY_START, fmt="%d/%m/%Y")
    with pytest.raises(
        PartitionValidationError,
        match=re.escape(
            "Asset 'downstream' depends on 'upstream': time_window mapping "
            "requires matching key formats: downstream fmt '%Y-%m-%d' != "
            "upstream fmt '%d/%m/%Y'"
        ),
    ):
        make_repo(_tw_edge(down, up))


def test_time_window_mapping_phase_mismatch_rejected():
    """Same interval but offset starts: every downstream key is off the
    upstream grid even though the cadences match."""
    fmt = "%Y-%m-%dT%H:%M:%S"
    up = rs.PartitionsDefinition.time_window(
        start=datetime(2024, 1, 1), interval_seconds=3600, fmt=fmt
    )
    down = rs.PartitionsDefinition.time_window(
        start=datetime(2024, 1, 1, 0, 30), interval_seconds=3600, fmt=fmt
    )
    with pytest.raises(
        PartitionValidationError,
        match=re.escape(
            "Asset 'downstream' depends on 'upstream': time_window mapping "
            "requires the downstream grid to be a subgrid of the upstream "
            "grid: downstream start 2024-01-01 00:30:00 is not aligned to "
            "the upstream grid (start 2024-01-01 00:00:00, interval 3600s)"
        ),
    ):
        make_repo(_tw_edge(down, up))


def test_time_window_mapping_aligned_mixed_grid_kinds_allowed():
    """A daily cron grid and an 86400s interval grid with the same anchor mint
    identical keys — grid kind alone is no reason to reject the edge."""
    fmt = "%Y-%m-%d"
    up = rs.PartitionsDefinition.time_window(
        start=DAILY_START, cron_schedule="0 0 * * *", fmt=fmt
    )
    down = rs.PartitionsDefinition.time_window(
        start=DAILY_START, interval_seconds=86400, fmt=fmt
    )
    assert make_repo(_tw_edge(down, up)) is not None


def test_time_window_mapping_misaligned_mixed_grid_kinds_rejected():
    """An interval grid anchored off the cron grid's ticks mints keys that
    never exist upstream."""
    fmt = "%Y-%m-%dT%H:%M"
    up = rs.PartitionsDefinition.time_window(
        start=DAILY_START, cron_schedule="0 0 * * *", fmt=fmt
    )
    down = rs.PartitionsDefinition.time_window(
        start=datetime(2024, 1, 1, 0, 30), interval_seconds=86400, fmt=fmt
    )
    with pytest.raises(
        PartitionValidationError,
        match=re.escape("is not on the upstream grid (cron '0 0 * * *')"),
    ):
        make_repo(_tw_edge(down, up))


def test_fractional_interval_downstream_on_cron_upstream_rejected():
    """Cron grids are second-granular, so an interval grid whose ticks carry
    sub-second fractions mints keys that can never exist upstream. croner
    matches fractional probe times verbatim (it never inspects nanoseconds),
    so the subgrid probe must reject them explicitly."""
    fmt = "%Y-%m-%dT%H:%M:%S%.f"
    up = rs.PartitionsDefinition.time_window(
        start=DAILY_START, cron_schedule="* * * * * *", fmt=fmt
    )
    down = rs.PartitionsDefinition.time_window(
        start=DAILY_START, interval_seconds=1.5, fmt=fmt
    )
    with pytest.raises(PartitionValidationError, match="is not on the upstream grid"):
        make_repo(_identity_edge(down, up))


def test_subgrid_divergence_past_first_window_rejected():
    """An hourly grid against a weekday-only hourly upstream diverges at the
    first Saturday — downstream tick 121. A 32-tick probe sails past
    construction and mints 24 phantom Saturday keys a week; the probe must
    look far enough to see one full week."""
    fmt = "%Y-%m-%dT%H:00"
    # 2024-01-01 is a Monday.
    up = rs.PartitionsDefinition.time_window(
        start=DAILY_START, cron_schedule="0 * * * 1-5", fmt=fmt
    )
    down = rs.PartitionsDefinition.time_window(
        start=DAILY_START, cron_schedule="0 * * * *", fmt=fmt
    )
    with pytest.raises(PartitionValidationError, match="is not on the upstream grid"):
        make_repo(_identity_edge(down, up))


def test_equivalent_cron_spellings_allowed():
    """'0 0 * * *' and its 6-field spelling '0 0 0 * * *' are one schedule;
    textual comparison must not reject them."""
    fmt = "%Y-%m-%d"
    up = rs.PartitionsDefinition.daily(start=DAILY_START)
    down = rs.PartitionsDefinition.time_window(
        start=DAILY_START, cron_schedule="0 0 0 * * *", fmt=fmt
    )
    assert make_repo(_identity_edge(down, up)) is not None


def test_time_window_mapping_differing_cron_rejected():
    fmt = "%Y-%m-%dT%H:00"
    up = rs.PartitionsDefinition.time_window(
        start=DAILY_START, cron_schedule="0 0 * * *", fmt=fmt
    )
    down = rs.PartitionsDefinition.time_window(
        start=DAILY_START, cron_schedule="0 6 * * *", fmt=fmt
    )
    with pytest.raises(PartitionValidationError, match="is not on the upstream grid"):
        make_repo(_tw_edge(down, up))


def test_time_window_mapping_differing_ranges_allowed():
    """Same grid with different start/end ranges is fine — range edges are a
    per-key concern, not an edge-validity one."""
    up = rs.PartitionsDefinition.daily(
        start=datetime(2024, 1, 1), end=datetime(2024, 12, 31)
    )
    down = rs.PartitionsDefinition.daily(
        start=datetime(2024, 2, 1), end=datetime(2024, 6, 30)
    )
    assert make_repo(_tw_edge(down, up)) is not None


# ---------------------------------------------------------------------------
# Both partitioned — Identity (default) grid compatibility
# ---------------------------------------------------------------------------


def _identity_edge(down_def, up_def):
    """Upstream→downstream pair with no explicit mapping (Identity default)."""

    @rs.Asset(partitions_def=up_def)
    def upstream() -> Any:
        return 1

    @rs.Asset(partitions_def=down_def)
    def downstream(upstream: Any) -> Any:
        return upstream

    return [upstream, downstream]


def test_identity_mapping_cross_cadence_rejected():
    """Finer downstream over coarser upstream: most downstream keys don't
    exist upstream — the persist-then-fail hole time_window(offset) had."""
    fmt = "%Y-%m-%dT%H:00"
    down = rs.PartitionsDefinition.time_window(
        start=DAILY_START, interval_seconds=3600, fmt=fmt
    )
    up = rs.PartitionsDefinition.time_window(
        start=DAILY_START, interval_seconds=21600, fmt=fmt
    )
    with pytest.raises(
        PartitionValidationError,
        match=re.escape(
            "Asset 'downstream' depends on 'upstream': Identity mapping "
            "requires the downstream grid to be a subgrid of the upstream "
            "grid: downstream interval 3600s is not a multiple of upstream "
            "interval 21600s"
        ),
    ):
        make_repo(_identity_edge(down, up))


def test_identity_mapping_fmt_mismatch_rejected():
    down = rs.PartitionsDefinition.daily(start=DAILY_START)
    up = rs.PartitionsDefinition.daily(start=DAILY_START, fmt="%d/%m/%Y")
    with pytest.raises(
        PartitionValidationError,
        match=re.escape(
            "Asset 'downstream' depends on 'upstream': Identity mapping "
            "requires matching key formats: downstream fmt '%Y-%m-%d' != "
            "upstream fmt '%d/%m/%Y'"
        ),
    ):
        make_repo(_identity_edge(down, up))


def test_identity_mapping_coarser_downstream_allowed():
    """Coarser downstream over finer upstream is a valid subgrid: every
    downstream key exists upstream."""
    fmt = "%Y-%m-%dT%H:00"
    down = rs.PartitionsDefinition.time_window(
        start=DAILY_START, interval_seconds=21600, fmt=fmt
    )
    up = rs.PartitionsDefinition.time_window(
        start=DAILY_START, interval_seconds=3600, fmt=fmt
    )
    assert make_repo(_identity_edge(down, up)) is not None


def test_identity_mapping_static_disjoint_keys_rejected():
    """A downstream static key the upstream lacks can never load its dep."""
    down = rs.PartitionsDefinition.static_(["a", "x"])
    up = rs.PartitionsDefinition.static_(["a", "b"])
    with pytest.raises(
        PartitionValidationError,
        match=re.escape(
            "Asset 'downstream' depends on 'upstream': Identity mapping "
            "requires every downstream key to exist upstream; missing "
            "upstream: x"
        ),
    ):
        make_repo(_identity_edge(down, up))


def test_identity_mapping_static_subset_allowed():
    down = rs.PartitionsDefinition.static_(["a"])
    up = rs.PartitionsDefinition.static_(["a", "b"])
    assert make_repo(_identity_edge(down, up)) is not None


def test_identity_mapping_dynamic_namespace_mismatch_rejected():
    """Identity between different dynamic namespaces would look every
    downstream key up in the wrong namespace — silently never matching."""
    down = rs.PartitionsDefinition.dynamic("colors")
    up = rs.PartitionsDefinition.dynamic("shapes")
    with pytest.raises(
        PartitionValidationError,
        match=re.escape(
            "Asset 'downstream' depends on 'upstream': Identity mapping "
            "requires matching dynamic namespaces: downstream 'colors' != "
            "upstream 'shapes'"
        ),
    ):
        make_repo(_identity_edge(down, up))


def test_identity_mapping_same_dynamic_namespace_allowed():
    down = rs.PartitionsDefinition.dynamic("colors")
    up = rs.PartitionsDefinition.dynamic("colors")
    assert make_repo(_identity_edge(down, up)) is not None


# ---------------------------------------------------------------------------
# Both partitioned — Static mapping
# ---------------------------------------------------------------------------


def test_static_mapping_valid_keys():
    """Static mapping with valid keys on both sides should work."""
    down_parts = rs.PartitionsDefinition.static_(["x", "y"])
    up_parts = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(partitions_def=up_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=down_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.static_({"x": "a", "y": "b"}),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


def test_static_mapping_invalid_downstream_key():
    """Static mapping with invalid downstream key should fail."""
    down_parts = rs.PartitionsDefinition.static_(["x", "y"])
    up_parts = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(partitions_def=up_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=down_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.static_(
                    {"x": "a", "INVALID": "b"}
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError, match="Static partition mapping key 'INVALID'"
    ):
        make_repo([upstream, downstream])


def test_static_mapping_invalid_upstream_key():
    """Static mapping with invalid upstream target key should fail."""
    down_parts = rs.PartitionsDefinition.static_(["x", "y"])
    up_parts = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(partitions_def=up_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=down_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.static_(
                    {"x": "a", "y": "INVALID"}
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError, match="Static mapping target 'INVALID'"
    ):
        make_repo([upstream, downstream])


# ---------------------------------------------------------------------------
# Both partitioned — AllPartitions mapping
# ---------------------------------------------------------------------------


def test_all_partitions_mapping_both_partitioned():
    """AllPartitions mapping with both sides partitioned should work."""
    parts_a = rs.PartitionsDefinition.static_(["a", "b"])
    parts_b = rs.PartitionsDefinition.static_(["x", "y", "z"])

    @rs.Asset(partitions_def=parts_a)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts_b,
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.all_partitions()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


# ---------------------------------------------------------------------------
# Both partitioned — SpecificPartitions
# ---------------------------------------------------------------------------


def test_specific_partitions_mapping_both_partitioned_rejected():
    """SpecificPartitions mapping with both sides partitioned should be rejected."""
    parts_a = rs.PartitionsDefinition.static_(["a", "b", "c"])
    parts_b = rs.PartitionsDefinition.static_(["x", "y", "z"])

    @rs.Asset(partitions_def=parts_a)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts_b,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.specific_partitions(["a", "b"]),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError, match="SpecificPartitions.*only valid.*unpartitioned"
    ):
        make_repo([upstream, downstream])


def test_specific_partitions_mapping_different_partition_types_rejected():
    """SpecificPartitions mapping with both sides partitioned (different types) should be rejected."""
    parts_a = rs.PartitionsDefinition.static_(["a", "b", "c"])
    parts_b = rs.PartitionsDefinition.daily(start=DAILY_START)

    @rs.Asset(partitions_def=parts_a)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts_b,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.specific_partitions(["a", "c"]),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError, match="SpecificPartitions.*only valid.*unpartitioned"
    ):
        make_repo([upstream, downstream])


def test_specific_partitions_mapping_invalid_keys():
    """SpecificPartitions with keys not in upstream should fail validation."""
    parts_a = rs.PartitionsDefinition.static_(["a", "b", "c"])

    @rs.Asset(partitions_def=parts_a)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.specific_partitions(["a", "z"]),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError, match="SpecificPartitions key 'z' is not a valid"
    ):
        make_repo([upstream, downstream])


# ---------------------------------------------------------------------------
# Downstream partitioned, upstream NOT partitioned
# ---------------------------------------------------------------------------


def test_partitioned_downstream_unpartitioned_upstream_no_mapping():
    """Partitioned downstream with unpartitioned upstream and no mapping should work."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset
    def upstream() -> Any:
        return 1

    @rs.Asset(partitions_def=parts)
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    # No mapping = implicitly shared, which is fine
    repo = make_repo([upstream, downstream])
    assert repo is not None


def test_partitioned_downstream_unpartitioned_upstream_all_partitions():
    """Partitioned downstream with unpartitioned upstream and AllPartitions should work."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.all_partitions()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


def test_partitioned_downstream_unpartitioned_upstream_specific_partitions_rejected():
    """Partitioned downstream with unpartitioned upstream and SpecificPartitions should fail."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.specific_partitions(["a"]),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError, match="only AllPartitions, ForKeys, or no mapping"
    ):
        make_repo([upstream, downstream])


def test_partitioned_downstream_unpartitioned_upstream_identity_mapping():
    """Partitioned downstream with unpartitioned upstream and Identity mapping should fail."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.identity()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError, match="only AllPartitions, ForKeys, or no mapping"
    ):
        make_repo([upstream, downstream])


# ---------------------------------------------------------------------------
# Downstream NOT partitioned, upstream IS partitioned
# ---------------------------------------------------------------------------


def test_unpartitioned_downstream_partitioned_upstream_no_mapping():
    """Unpartitioned downstream with partitioned upstream and no mapping should fail."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError, match="partition_mapping.*is required"
    ):
        make_repo([upstream, downstream])


def test_unpartitioned_downstream_partitioned_upstream_all_partitions():
    """Unpartitioned downstream with partitioned upstream and AllPartitions should work."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.all_partitions()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


def test_unpartitioned_downstream_partitioned_upstream_specific_partitions():
    """Unpartitioned downstream with partitioned upstream and SpecificPartitions should work."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.specific_partitions(["a", "b"]),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


def test_unpartitioned_downstream_partitioned_upstream_identity():
    """Unpartitioned downstream with partitioned upstream and Identity should fail."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.identity()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError, match="only AllPartitions or SpecificPartitions"
    ):
        make_repo([upstream, downstream])


# ---------------------------------------------------------------------------
# Neither partitioned
# ---------------------------------------------------------------------------


def test_neither_partitioned_no_mapping():
    """Neither partitioned and no mapping should work."""

    @rs.Asset
    def upstream() -> Any:
        return 1

    @rs.Asset
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


def test_neither_partitioned_with_non_identity_mapping():
    """Neither partitioned with a non-identity mapping should fail."""

    @rs.Asset
    def upstream() -> Any:
        return 1

    @rs.Asset(
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.all_partitions()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(PartitionValidationError, match="neither asset has partitions"):
        make_repo([upstream, downstream])


def test_neither_partitioned_with_identity_mapping():
    """Neither partitioned with Identity mapping should be tolerated (no-op)."""

    @rs.Asset
    def upstream() -> Any:
        return 1

    @rs.Asset(
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.identity()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


# ---------------------------------------------------------------------------
# Invalid mapping key (references non-dependency)
# ---------------------------------------------------------------------------


def test_mapping_references_non_dependency():
    """AssetDef.input() with a name not matching any function param should fail at decoration time."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    with pytest.raises(ValueError, match="does not match any parameter"):

        @rs.Asset(
            partitions_def=parts,
            deps=[
                rs.AssetDef.input(
                    "nonexistent", partition_mapping=rs.PartitionMapping.identity()
                )
            ],
        )
        def downstream(upstream: Any) -> Any:
            return upstream + 1


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


# ---------------------------------------------------------------------------
# Multi-dimensional partitions
# ---------------------------------------------------------------------------


def test_multi_partitions_identity():
    """Multi-dimensional partitions with Identity mapping should work."""
    parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "date": rs.PartitionsDefinition.static_(["2024-01", "2024-02"]),
        }
    )

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(partitions_def=parts)
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


def test_identity_multi_mismatched_dim_names_rejected():
    """Identity between Multi defs with different dimension names must fail resolve."""
    down = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.static_(["2024-01"]),
            "country": rs.PartitionsDefinition.static_(["us", "eu"]),
        }
    )
    up = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.static_(["2024-01"]),
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
        }
    )
    with pytest.raises(
        PartitionValidationError,
        match=re.escape(
            "Asset 'downstream' depends on 'upstream': Identity mapping requires "
            "matching Multi dimensions: downstream [country, date] != upstream "
            "[date, region]"
        ),
    ):
        make_repo(_identity_edge(down, up))


def test_identity_multi_incompatible_dim_def_rejected():
    """Identity between Multi defs recurses into each dimension's pair."""
    down = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.static_(["2024-01"]),
            "region": rs.PartitionsDefinition.static_(["us", "mars"]),
        }
    )
    up = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.static_(["2024-01"]),
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
        }
    )
    with pytest.raises(
        PartitionValidationError,
        match=re.escape(
            "in dimension 'region': Identity mapping requires every downstream "
            "key to exist upstream; missing upstream: mars"
        ),
    ):
        make_repo(_identity_edge(down, up))


def test_identity_multi_cross_grid_dim_rejected():
    """Per-dimension recursion applies grid compatibility to time dims."""
    down = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
            "region": rs.PartitionsDefinition.static_(["us"]),
        }
    )
    up = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.hourly(start=DAILY_START),
            "region": rs.PartitionsDefinition.static_(["us"]),
        }
    )
    with pytest.raises(PartitionValidationError, match="in dimension 'date'"):
        make_repo(_identity_edge(down, up))


def test_multi_partitions_explicit_identity_mapping():
    """Multi-dimensional partitions with explicit Identity mapping should work."""
    parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "date": rs.PartitionsDefinition.static_(["2024-01", "2024-02"]),
        }
    )

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.identity()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


def test_multi_partitions_all_partitions_mapping():
    """Multi-dimensional partitions with AllPartitions mapping should work."""
    parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "date": rs.PartitionsDefinition.static_(["2024-01", "2024-02"]),
        }
    )

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.all_partitions()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


def test_multi_partitions_time_window_mapping_rejected():
    """TimeWindow mapping on Multi partitions should fail."""
    parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "date": rs.PartitionsDefinition.static_(["2024-01", "2024-02"]),
        }
    )

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.time_window(offset=-1)
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError, match="TimeWindow mapping requires TimeWindow"
    ):
        make_repo([upstream, downstream])


def test_multi_to_unpartitioned_requires_all_partitions():
    """Unpartitioned downstream depending on multi-partitioned upstream needs AllPartitions."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
        }
    )

    @rs.Asset(partitions_def=multi_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.all_partitions()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


def test_multi_to_unpartitioned_without_mapping_fails():
    """Unpartitioned downstream depending on multi-partitioned upstream fails without mapping."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
        }
    )

    @rs.Asset(partitions_def=multi_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(PartitionValidationError, match="partition_mapping"):
        make_repo([upstream, downstream])


def test_unpartitioned_to_multi_no_mapping_needed():
    """Multi-partitioned downstream depending on unpartitioned upstream needs no mapping."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
        }
    )

    @rs.Asset
    def upstream() -> Any:
        return 1

    @rs.Asset(partitions_def=multi_parts)
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


def test_multi_to_static_with_all_partitions():
    """Static downstream depending on multi-partitioned upstream via AllPartitions."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
        }
    )
    static_parts = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(partitions_def=multi_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=static_parts,
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.all_partitions()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


def test_static_to_multi_with_all_partitions():
    """Multi-partitioned downstream depending on static upstream via AllPartitions."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
        }
    )
    static_parts = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(partitions_def=static_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=multi_parts,
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.all_partitions()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


def test_multi_chain_three_assets_identity():
    """Chain of three multi-partitioned assets with Identity mapping."""
    parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "date": rs.PartitionsDefinition.static_(["2024-01", "2024-02"]),
        }
    )

    @rs.Asset(partitions_def=parts)
    def source() -> Any:
        return 1

    @rs.Asset(partitions_def=parts)
    def middle(source: Any) -> Any:
        return source + 1

    @rs.Asset(partitions_def=parts)
    def sink(middle: Any) -> Any:
        return middle + 1

    repo = make_repo([source, middle, sink])
    assert repo is not None


def test_multi_diamond_mixed_mappings():
    """Diamond dependency with multi-partitioned assets using mixed mappings."""
    parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
        }
    )

    @rs.Asset(partitions_def=parts)
    def source() -> Any:
        return 1

    @rs.Asset(partitions_def=parts)
    def left(source: Any) -> Any:
        return source + 1

    @rs.Asset(partitions_def=parts)
    def right(source: Any) -> Any:
        return source + 2

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input("left", partition_mapping=rs.PartitionMapping.identity()),
            rs.AssetDef.input(
                "right", partition_mapping=rs.PartitionMapping.all_partitions()
            ),
        ],
    )
    def sink(left: Any, right: Any) -> Any:
        return left + right

    repo = make_repo([source, left, right, sink])
    assert repo is not None


def test_multi_with_asset_def_key_in_mapping():
    """Multi-partitioned assets with AssetDef as partition_mapping key."""
    parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
        }
    )

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                upstream.name, partition_mapping=rs.PartitionMapping.all_partitions()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


def test_multi_partitions_static_mapping_rejected():
    """Static mapping with multi-partitioned assets is rejected (keys are multi, not single)."""
    parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
        }
    )

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.static_({"us": "eu"})
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(PartitionValidationError, match="Static partition mapping key"):
        make_repo([upstream, downstream])


def test_multi_vs_static_mismatch():
    """Multi-dimensional vs static partitions should fail with Identity mapping."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
        }
    )
    static_parts = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(partitions_def=static_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(partitions_def=multi_parts)
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError, match="Identity mapping requires same partition type"
    ):
        make_repo([upstream, downstream])


# ---------------------------------------------------------------------------
# PartitionMapping.multi() — per-dimension mapping for MultiPartitions
# ---------------------------------------------------------------------------


def test_multi_mapping_same_dims_identity():
    """Multi mapping with Identity on each dimension works."""
    up_parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "date": rs.PartitionsDefinition.static_(["2024-01", "2024-02"]),
        }
    )
    down_parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "date": rs.PartitionsDefinition.static_(["2024-01", "2024-02"]),
        }
    )

    @rs.Asset(partitions_def=up_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=down_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi(
                    {
                        "region": rs.PartitionMapping.identity(),
                        "date": rs.PartitionMapping.identity(),
                    }
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


def test_multi_mapping_dimension_rename():
    """Multi mapping that renames dimensions between upstream and downstream."""
    up_parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "date": rs.PartitionsDefinition.static_(["2024-01", "2024-02"]),
        }
    )
    down_parts = rs.PartitionsDefinition.multi(
        {
            "country": rs.PartitionsDefinition.static_(["us", "eu"]),
            "period": rs.PartitionsDefinition.static_(["2024-01", "2024-02"]),
        }
    )

    @rs.Asset(partitions_def=up_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=down_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi(
                    {
                        "region": ("country", rs.PartitionMapping.identity()),
                        "date": ("period", rs.PartitionMapping.identity()),
                    }
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


def test_multi_mapping_mixed_per_dim_strategies():
    """Multi mapping with different strategies per dimension."""
    up_parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "date": rs.PartitionsDefinition.static_(["2024-01", "2024-02"]),
        }
    )
    down_parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "date": rs.PartitionsDefinition.static_(["2024-01", "2024-02"]),
        }
    )

    @rs.Asset(partitions_def=up_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=down_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi(
                    {
                        "region": rs.PartitionMapping.identity(),
                        "date": rs.PartitionMapping.all_partitions(),
                    }
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


def test_multi_mapping_on_non_multi_downstream_fails():
    """Multi mapping on static downstream is rejected."""
    up_parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
        }
    )
    static_parts = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(partitions_def=up_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=static_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi(
                    {
                        "region": rs.PartitionMapping.identity(),
                    }
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError,
        match="Multi mapping requires Multi partitions on downstream",
    ):
        make_repo([upstream, downstream])


def test_multi_mapping_on_non_multi_upstream_fails():
    """Multi mapping with static upstream is rejected."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
        }
    )
    static_parts = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(partitions_def=static_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=multi_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi(
                    {
                        "region": rs.PartitionMapping.identity(),
                    }
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError,
        match="Multi mapping requires Multi partitions on upstream",
    ):
        make_repo([upstream, downstream])


def test_multi_mapping_missing_upstream_dim_fails():
    """Multi mapping that doesn't cover all upstream dimensions fails."""
    parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "date": rs.PartitionsDefinition.static_(["2024-01", "2024-02"]),
        }
    )

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi(
                    {
                        "region": rs.PartitionMapping.identity(),
                        # missing "date" dimension
                    }
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(PartitionValidationError, match="missing upstream dimension"):
        make_repo([upstream, downstream])


def test_multi_mapping_missing_downstream_dim_fails():
    """Multi mapping that doesn't cover all downstream dimensions fails."""
    up_parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
        }
    )
    down_parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "date": rs.PartitionsDefinition.static_(["2024-01", "2024-02"]),
        }
    )

    @rs.Asset(partitions_def=up_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=down_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi(
                    {
                        "region": rs.PartitionMapping.identity(),
                        # "date" downstream dim is not targeted
                    }
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError, match="does not cover downstream dimension"
    ):
        make_repo([upstream, downstream])


def test_multi_mapping_nonexistent_upstream_dim_fails():
    """Multi mapping referencing a non-existent upstream dimension fails."""
    parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
        }
    )

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi(
                    {
                        "region": rs.PartitionMapping.identity(),
                        "nonexistent": rs.PartitionMapping.identity(),
                    }
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError,
        match="upstream dimension 'nonexistent' which does not exist",
    ):
        make_repo([upstream, downstream])


def test_multi_mapping_nonexistent_downstream_dim_fails():
    """Multi mapping targeting a non-existent downstream dimension fails."""
    parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
        }
    )

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi(
                    {
                        "region": ("nonexistent", rs.PartitionMapping.identity()),
                    }
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError,
        match="downstream dimension 'nonexistent' which does not exist",
    ):
        make_repo([upstream, downstream])


def test_multi_mapping_invalid_per_dim_mapping_fails():
    """Per-dimension TimeWindow mapping on Static sub-partitions is rejected."""
    parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
        }
    )

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi(
                    {
                        "region": rs.PartitionMapping.time_window(offset=-1),
                    }
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError, match="TimeWindow mapping requires TimeWindow"
    ):
        make_repo([upstream, downstream])


def test_multi_mapping_with_time_window_sub_partitions():
    """Multi mapping with TimeWindow per-dimension mapping on TimeWindow sub-partitions."""
    up_parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
        }
    )
    down_parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
        }
    )

    @rs.Asset(partitions_def=up_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=down_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi(
                    {
                        "region": rs.PartitionMapping.identity(),
                        "date": rs.PartitionMapping.time_window(offset=-1),
                    }
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


def test_multi_mapping_with_static_sub_mapping():
    """Multi mapping with Static per-dimension mapping on Static sub-partitions."""
    up_parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "tier": rs.PartitionsDefinition.static_(["free", "pro"]),
        }
    )
    down_parts = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "tier": rs.PartitionsDefinition.static_(["basic", "premium"]),
        }
    )

    @rs.Asset(partitions_def=up_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=down_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi(
                    {
                        "region": rs.PartitionMapping.identity(),
                        "tier": (
                            "tier",
                            rs.PartitionMapping.static_(
                                {"basic": "free", "premium": "pro"}
                            ),
                        ),
                    }
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = make_repo([upstream, downstream])
    assert repo is not None


# ---------------------------------------------------------------------------
# Edge case: self-referencing mapping key
# ---------------------------------------------------------------------------


def test_mapping_key_is_own_name():
    """AssetDef.input() with the asset's own name (not a param) should fail at decoration time."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    with pytest.raises(ValueError, match="does not match any parameter"):

        @rs.Asset(
            partitions_def=parts,
            deps=[
                rs.AssetDef.input(
                    "downstream", partition_mapping=rs.PartitionMapping.identity()
                )
            ],
        )
        def downstream(upstream: Any) -> Any:
            return upstream + 1


# ---------------------------------------------------------------------------
# MultiToSingle mapping validation
# ---------------------------------------------------------------------------


def test_multi_to_single_upstream_multi_downstream_single():
    """MultiToSingle: upstream is Multi, downstream is single-dim (date dimension)."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
            "region": rs.PartitionsDefinition.static_(STATIC_KEYS),
        }
    )
    daily_parts = rs.PartitionsDefinition.daily(start=DAILY_START)

    @rs.Asset(partitions_def=multi_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=daily_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi_to_single(
                    dimension_name="date"
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    make_repo([upstream, downstream])


def test_multi_to_single_downstream_multi_upstream_single():
    """MultiToSingle: downstream is Multi, upstream is single-dim (region dimension)."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
            "region": rs.PartitionsDefinition.static_(STATIC_KEYS),
        }
    )
    static_parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset(partitions_def=static_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=multi_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi_to_single(
                    dimension_name="region"
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    make_repo([upstream, downstream])


def test_multi_to_single_downstream_multi_inner_orientation():
    """Downstream-Multi: the inner mapping's downstream is the named DIM and
    its upstream is the single def — a coarser dim over a finer upstream is
    a valid subgrid and must validate."""
    fmt = "%Y-%m-%dT%H:00"
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.time_window(
                start=DAILY_START, interval_seconds=21600, fmt=fmt
            ),
            "region": rs.PartitionsDefinition.static_(STATIC_KEYS),
        }
    )
    hourly = rs.PartitionsDefinition.time_window(
        start=DAILY_START, interval_seconds=3600, fmt=fmt
    )

    @rs.Asset(partitions_def=hourly)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=multi_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi_to_single(
                    dimension_name="date",
                    partition_mapping=rs.PartitionMapping.time_window(offset=-1),
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    assert make_repo([upstream, downstream]) is not None


def test_multi_to_single_downstream_multi_finer_dim_rejected():
    """Downstream-Multi with a FINER time dim than the single upstream: the
    inner subgrid check must reject it in the true orientation."""
    fmt = "%Y-%m-%dT%H:00"
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.time_window(
                start=DAILY_START, interval_seconds=3600, fmt=fmt
            ),
            "region": rs.PartitionsDefinition.static_(STATIC_KEYS),
        }
    )
    six_hourly = rs.PartitionsDefinition.time_window(
        start=DAILY_START, interval_seconds=21600, fmt=fmt
    )

    @rs.Asset(partitions_def=six_hourly)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=multi_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi_to_single(
                    dimension_name="date",
                    partition_mapping=rs.PartitionMapping.time_window(offset=-1),
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError,
        match=re.escape(
            "downstream interval 3600s is not a multiple of upstream interval 21600s"
        ),
    ):
        make_repo([upstream, downstream])


def test_multi_to_single_dimension_not_found():
    """MultiToSingle fails when the named dimension doesn't exist."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
            "region": rs.PartitionsDefinition.static_(STATIC_KEYS),
        }
    )
    daily_parts = rs.PartitionsDefinition.daily(start=DAILY_START)

    @rs.Asset(partitions_def=multi_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=daily_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi_to_single(
                    dimension_name="nonexistent"
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(PartitionValidationError, match="does not exist"):
        make_repo([upstream, downstream])


def test_multi_to_single_type_mismatch():
    """MultiToSingle fails when dimension type doesn't match the single side."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
            "region": rs.PartitionsDefinition.static_(STATIC_KEYS),
        }
    )
    static_parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset(partitions_def=multi_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=static_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi_to_single(
                    dimension_name="date"
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError, match="Static.*TimeWindow|TimeWindow.*Static"
    ):
        make_repo([upstream, downstream])


def test_multi_to_single_both_multi_rejected():
    """MultiToSingle fails when both sides are Multi."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
            "region": rs.PartitionsDefinition.static_(STATIC_KEYS),
        }
    )

    @rs.Asset(partitions_def=multi_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=multi_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi_to_single(
                    dimension_name="date"
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(PartitionValidationError, match="both are Multi"):
        make_repo([upstream, downstream])


def test_multi_to_single_neither_multi_rejected():
    """MultiToSingle fails when neither side is Multi."""
    daily_parts = rs.PartitionsDefinition.daily(start=DAILY_START)

    @rs.Asset(partitions_def=daily_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=daily_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi_to_single(
                    dimension_name="date"
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(PartitionValidationError, match="one side to be Multi"):
        make_repo([upstream, downstream])


def test_multi_to_single_static_dimension():
    """MultiToSingle works with a Static dimension extracted from Multi."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
            "region": rs.PartitionsDefinition.static_(STATIC_KEYS),
        }
    )
    static_parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset(partitions_def=multi_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=static_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi_to_single(
                    dimension_name="region"
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    make_repo([upstream, downstream])


# ---------------------------------------------------------------------------
# MultiToSingle with inner partition_mapping
# ---------------------------------------------------------------------------


def test_multi_to_single_with_time_window_inner_mapping():
    """MultiToSingle with a TimeWindow inner mapping (e.g. offset=-1)."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
            "region": rs.PartitionsDefinition.static_(STATIC_KEYS),
        }
    )
    daily_parts = rs.PartitionsDefinition.daily(start=DAILY_START)

    @rs.Asset(partitions_def=multi_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=daily_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi_to_single(
                    dimension_name="date",
                    partition_mapping=rs.PartitionMapping.time_window(offset=-1),
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    make_repo([upstream, downstream])


def test_multi_to_single_with_static_inner_mapping():
    """MultiToSingle with a Static inner mapping for the dimension."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
            "region": rs.PartitionsDefinition.static_(STATIC_KEYS),
        }
    )
    static_parts = rs.PartitionsDefinition.static_(["x", "y", "z"])

    @rs.Asset(partitions_def=multi_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=static_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi_to_single(
                    dimension_name="region",
                    partition_mapping=rs.PartitionMapping.static_(
                        {"x": "a", "y": "b", "z": "c"}
                    ),
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    make_repo([upstream, downstream])


def test_multi_to_single_inner_mapping_type_mismatch():
    """MultiToSingle with inner mapping that doesn't match the dimension type."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
            "region": rs.PartitionsDefinition.static_(STATIC_KEYS),
        }
    )
    static_parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset(partitions_def=multi_parts)
    def upstream() -> Any:
        return 1

    # region is Static but we use TimeWindow mapping
    @rs.Asset(
        partitions_def=static_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi_to_single(
                    dimension_name="region",
                    partition_mapping=rs.PartitionMapping.time_window(offset=-1),
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError, match="TimeWindow.*requires TimeWindow"
    ):
        make_repo([upstream, downstream])


def test_multi_to_single_rejects_multi_inner_mapping():
    """MultiToSingle rejects Multi as inner mapping."""
    with pytest.raises(PartitionValidationError, match="cannot be Multi"):
        rs.PartitionMapping.multi_to_single(
            dimension_name="date",
            partition_mapping=rs.PartitionMapping.multi(
                {"x": rs.PartitionMapping.identity()}
            ),
        )


def test_multi_to_single_rejects_nested_multi_to_single():
    """MultiToSingle rejects another MultiToSingle as inner mapping."""
    with pytest.raises(PartitionValidationError, match="cannot be MultiToSingle"):
        rs.PartitionMapping.multi_to_single(
            dimension_name="date",
            partition_mapping=rs.PartitionMapping.multi_to_single(dimension_name="x"),
        )


# ---------------------------------------------------------------------------
# ForKeys validation
# ---------------------------------------------------------------------------


def test_forkeys_accepted_partitioned_down_unpartitioned_up():
    """ForKeys is valid when downstream is partitioned and upstream is unpartitioned."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.for_keys(
                    [rs.PartitionKey.single("a")]
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    make_repo([upstream, downstream])


def test_forkeys_rejected_both_partitioned():
    """ForKeys is rejected when both sides are partitioned."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.for_keys(
                    [rs.PartitionKey.single("a")]
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    with pytest.raises(
        PartitionValidationError,
        match="ForKeys.*only valid when upstream is unpartitioned",
    ):
        make_repo([upstream, downstream])


def test_forkeys_rejected_both_unpartitioned():
    """ForKeys is rejected when neither side is partitioned."""

    @rs.Asset
    def upstream() -> Any:
        return 1

    @rs.Asset(
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.for_keys(
                    [rs.PartitionKey.single("a")]
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    with pytest.raises(
        PartitionValidationError, match="partition_mapping specified but neither"
    ):
        make_repo([upstream, downstream])


def test_forkeys_rejected_unpartitioned_down_partitioned_up():
    """ForKeys is rejected when downstream is unpartitioned and upstream is partitioned."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.for_keys(
                    [rs.PartitionKey.single("a")]
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    with pytest.raises(
        PartitionValidationError, match="only AllPartitions or SpecificPartitions"
    ):
        make_repo([upstream, downstream])


def test_forkeys_invalid_key_rejected():
    """ForKeys with a key not in downstream partition def is rejected."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.for_keys(
                    [rs.PartitionKey.single("nonexistent")]
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    with pytest.raises(
        PartitionValidationError,
        match="ForKeys key.*not a valid downstream partition key",
    ):
        make_repo([upstream, downstream])


def test_forkeys_range_unknown_endpoints_rejected():
    """Range selector endpoints must be partition keys of the downstream def
    — an unknown endpoint would otherwise silently match nothing."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.for_keys(
                    [rs.PartitionKeyRange.single(from_key="x", to_key="z")]
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    with pytest.raises(
        PartitionValidationError,
        match=re.escape(
            "Asset 'downstream' depends on 'upstream': "
            "Range endpoint 'x' is not a partition key"
        ),
    ):
        make_repo([upstream, downstream])


def test_forkeys_range_inverted_rejected():
    """An inverted range would silently Skip every downstream key — surface
    the swapped endpoints at resolve time instead."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.for_keys(
                    [rs.PartitionKeyRange.single(from_key="c", to_key="a")]
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    with pytest.raises(
        PartitionValidationError,
        match=re.escape(
            "Asset 'downstream' depends on 'upstream': from_key 'c' is after to_key 'a'"
        ),
    ):
        make_repo([upstream, downstream])


def _multi_parts():
    return rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.static_(["2024-01-01", "2024-01-02"]),
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
        }
    )


def test_forkeys_multi_unknown_dimension_rejected():
    """A multi-range selector naming a dimension the downstream doesn't have
    can never match a key — every downstream partition would silently Skip
    its dep. Surface the typo at resolve time."""
    parts = _multi_parts()

    @rs.Asset
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.for_keys(
                    [rs.PartitionKeyRange.multi({"regon": ["us"]})]
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    with pytest.raises(
        PartitionValidationError,
        match=re.escape(
            "Asset 'downstream' depends on 'upstream': "
            "Unknown dimension 'regon' in partition range; "
            "available dimensions: 'date', 'region'"
        ),
    ):
        make_repo([upstream, downstream])


def test_forkeys_multi_keys_selector_unknown_key_rejected():
    """Keys sub-selectors must be validated like Range endpoints — a bogus
    key silently matches nothing."""
    parts = _multi_parts()

    @rs.Asset
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.for_keys(
                    [rs.PartitionKeyRange.multi({"region": ["mars"]})]
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    with pytest.raises(
        PartitionValidationError,
        match=re.escape(
            "Asset 'downstream' depends on 'upstream': "
            "'mars' is not a partition key of dimension 'region'"
        ),
    ):
        make_repo([upstream, downstream])


def test_forkeys_single_range_on_multi_def_rejected():
    """A single-dim range can never match a Multi key (contains() returns
    False for the shape) — the edge must be rejected at resolve, not left to
    silently skip every dep load."""
    parts = _multi_parts()

    @rs.Asset
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.for_keys(
                    [
                        rs.PartitionKeyRange.single(
                            from_key="2024-01-01", to_key="2024-01-02"
                        )
                    ]
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    with pytest.raises(
        PartitionValidationError,
        match=re.escape(
            "Asset 'downstream' depends on 'upstream': a single-dimension "
            "range cannot select from Multi partitions; use "
            "PartitionKeyRange.multi() with dimensions: date, region"
        ),
    ):
        make_repo([upstream, downstream])


def test_forkeys_multi_valid_selectors_accepted():
    """Valid multi selectors (Range + Keys) pass the new validation."""
    parts = _multi_parts()

    @rs.Asset
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.for_keys(
                    [
                        rs.PartitionKeyRange.multi(
                            {
                                "date": ("2024-01-01", "2024-01-02"),
                                "region": ["us"],
                            }
                        )
                    ]
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    make_repo([upstream, downstream])


def test_forkeys_multiple_valid_keys():
    """ForKeys with multiple valid keys is accepted."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.for_keys(
                    [rs.PartitionKey.single("a"), rs.PartitionKey.single("b")]
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    make_repo([upstream, downstream])


def test_forkeys_one_invalid_in_multiple_keys():
    """ForKeys rejects if any key selector is invalid."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.for_keys(
                    [rs.PartitionKey.single("a"), rs.PartitionKey.single("bad")]
                ),
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    with pytest.raises(
        PartitionValidationError,
        match="ForKeys key.*not a valid downstream partition key",
    ):
        make_repo([upstream, downstream])


# ---------------------------------------------------------------------------
# Subset validation
# ---------------------------------------------------------------------------


def test_subset_accepted_same_type_subset_keys():
    """Subset is valid when both sides are partitioned and upstream keys ⊆ downstream keys."""
    down_parts = rs.PartitionsDefinition.static_(["a", "b", "c"])
    up_parts = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(partitions_def=up_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=down_parts,
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.subset()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    make_repo([upstream, downstream])


def test_subset_rejected_upstream_extra_keys():
    """Subset is rejected when upstream has keys not in downstream (Static)."""
    down_parts = rs.PartitionsDefinition.static_(["a", "b"])
    up_parts = rs.PartitionsDefinition.static_(["a", "b", "c"])

    @rs.Asset(partitions_def=up_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=down_parts,
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.subset()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    with pytest.raises(PartitionValidationError, match="upstream has extra keys"):
        make_repo([upstream, downstream])


def test_subset_rejected_different_partition_types():
    """Subset is rejected when partition types differ."""
    down_parts = rs.PartitionsDefinition.static_(STATIC_KEYS)
    up_parts = rs.PartitionsDefinition.daily(start=DAILY_START)

    @rs.Asset(partitions_def=up_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=down_parts,
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.subset()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    with pytest.raises(
        PartitionValidationError, match="Subset mapping requires same partition type"
    ):
        make_repo([upstream, downstream])


def test_subset_rejected_unpartitioned_upstream():
    """Subset is rejected when upstream is unpartitioned."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.subset()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    with pytest.raises(
        PartitionValidationError, match="only AllPartitions, ForKeys, or no mapping"
    ):
        make_repo([upstream, downstream])


def test_subset_accepted_same_keys():
    """Subset is valid when upstream and downstream have identical keys."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.subset()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    make_repo([upstream, downstream])


def test_subset_accepted_time_window():
    """Subset is valid for TimeWindow-to-TimeWindow (runtime validation)."""
    down_parts = rs.PartitionsDefinition.daily(start=DAILY_START)
    up_parts = rs.PartitionsDefinition.daily(start=DAILY_START)

    @rs.Asset(partitions_def=up_parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=down_parts,
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.subset()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream

    make_repo([upstream, downstream])


# ---------------------------------------------------------------------------
# ForKeys / Subset rejected inside Multi / MultiToSingle nesting
# ---------------------------------------------------------------------------


def test_multi_rejects_forkeys_inner_mapping():
    """Multi mapping rejects ForKeys as inner dimension mapping."""
    with pytest.raises(
        PartitionValidationError, match="Nested ForKeys.*not allowed inside Multi"
    ):
        rs.PartitionMapping.multi(
            {"dim": rs.PartitionMapping.for_keys([rs.PartitionKey.single("a")])}
        )


def test_multi_rejects_subset_inner_mapping():
    """Multi mapping rejects Subset as inner dimension mapping."""
    with pytest.raises(
        PartitionValidationError, match="Nested Subset.*not allowed inside Multi"
    ):
        rs.PartitionMapping.multi({"dim": rs.PartitionMapping.subset()})


def test_multi_to_single_rejects_forkeys_inner_mapping():
    """MultiToSingle rejects ForKeys as inner mapping."""
    with pytest.raises(PartitionValidationError, match="cannot be ForKeys"):
        rs.PartitionMapping.multi_to_single(
            dimension_name="date",
            partition_mapping=rs.PartitionMapping.for_keys(
                [rs.PartitionKey.single("a")]
            ),
        )


def test_multi_to_single_rejects_subset_inner_mapping():
    """MultiToSingle rejects Subset as inner mapping."""
    with pytest.raises(PartitionValidationError, match="cannot be Subset"):
        rs.PartitionMapping.multi_to_single(
            dimension_name="date",
            partition_mapping=rs.PartitionMapping.subset(),
        )
