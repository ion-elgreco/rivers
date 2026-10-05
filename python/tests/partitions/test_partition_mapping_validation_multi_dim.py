import re
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
