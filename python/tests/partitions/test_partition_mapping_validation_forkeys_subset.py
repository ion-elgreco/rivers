import re
from typing import Any

import pytest

import rivers as rs
from rivers.exceptions import PartitionValidationError

from .mapping_validation_helpers import DAILY_START, STATIC_KEYS, make_repo


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
