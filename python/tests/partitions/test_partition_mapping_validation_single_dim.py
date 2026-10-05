"""Tests for partition mapping validation during graph resolution."""

import re
from datetime import datetime
from typing import Any

import pytest

import rivers as rs
from rivers.exceptions import PartitionValidationError

from .mapping_validation_helpers import (
    DAILY_START,
    STATIC_KEYS,
    _identity_edge,
    make_repo,
)


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
            "grid: downstream start 2024-01-01T00:30:00 is not aligned to "
            "the upstream grid (start 2024-01-01T00:00:00, interval 3600s)"
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


def test_subgrid_second_tick_past_probe_horizon_rejected():
    """A sparse downstream grid whose SECOND tick lies beyond the 1461-day
    probe horizon is otherwise validated on tick 0 alone; the probe must always
    check the first two ticks so a tick-1 divergence is still caught."""
    fmt = "%Y-%m-%d"
    # Upstream fires only on Jan 1 each year.
    up = rs.PartitionsDefinition.time_window(
        start=DAILY_START, cron_schedule="0 0 1 1 *", fmt=fmt
    )
    # Downstream steps every 1500 days (>1461): tick 0 = 2024-01-01 (on the
    # yearly grid), tick 1 = 2028-02-09 (off it) and past the probe horizon.
    down = rs.PartitionsDefinition.time_window(
        start=DAILY_START,
        end=datetime(2050, 1, 1),
        interval_seconds=1500 * 86400,
        fmt=fmt,
    )
    with pytest.raises(PartitionValidationError, match="is not on the upstream grid"):
        make_repo(_identity_edge(down, up))


def test_subgrid_probe_respects_downstream_end():
    """Range edges are a per-key concern: a downstream def whose explicit end
    precedes the first off-grid tick mints only on-grid keys, so the probe
    must not reject it for a window start it will never mint."""
    fmt = "%Y-%m-%d"
    # Mon 2024-01-01 .. Fri 2024-01-05 (end exclusive at Sat 2024-01-06).
    up = rs.PartitionsDefinition.time_window(
        start=DAILY_START, cron_schedule="0 0 * * 1-5", fmt=fmt
    )
    down = rs.PartitionsDefinition.time_window(
        start=DAILY_START,
        end=datetime(2024, 1, 6),
        interval_seconds=86400,
        fmt=fmt,
    )
    assert make_repo(_identity_edge(down, up)) is not None


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
