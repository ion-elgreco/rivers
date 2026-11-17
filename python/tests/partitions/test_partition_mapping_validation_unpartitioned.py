import re
from typing import Any

import pytest

import rivers as rs
from rivers.exceptions import PartitionValidationError

from .mapping_validation_helpers import (
    KEY_LOOKUP_GRIDS,
    STATIC_KEYS,
    make_repo,
    validate_mapped_edge,
)


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


@pytest.mark.parametrize(("make_parts", "has", "lacks"), KEY_LOOKUP_GRIDS)
def test_specific_partitions_keys_must_be_upstream_keys(make_parts, has, lacks):
    """SpecificPartitions takes the upstream's keys in their exact text only."""
    parts = make_parts()
    validate_mapped_edge(None, parts, rs.PartitionMapping.specific_partitions(has))

    for key in lacks:
        mapping = rs.PartitionMapping.specific_partitions([*has, key])
        with pytest.raises(
            PartitionValidationError,
            match=f"SpecificPartitions key '{re.escape(key)}' is not a valid partition key for upstream",
        ):
            validate_mapped_edge(None, parts, mapping)


@pytest.mark.parametrize(
    ("make_parts", "match"),
    [
        pytest.param(
            lambda: rs.PartitionsDefinition.dynamic("colors"),
            "NotImplementedError: Cannot enumerate dynamic partition keys",
            id="dynamic",
        ),
        pytest.param(
            lambda: rs.PartitionsDefinition.multi(
                {
                    "letter": rs.PartitionsDefinition.static_(STATIC_KEYS),
                    "color": rs.PartitionsDefinition.dynamic("colors"),
                }
            ),
            "NotImplementedError: Cannot enumerate dynamic partition keys",
            id="multi-dynamic",
        ),
        pytest.param(
            lambda: rs.PartitionsDefinition.multi(
                {"letter": rs.PartitionsDefinition.static_(STATIC_KEYS)}
            ),
            "SpecificPartitions key 'a' is not a valid partition key for upstream",
            id="multi",
        ),
    ],
)
def test_specific_partitions_upstream_without_single_dim_keys_rejected(
    make_parts, match
):
    """SpecificPartitions needs an upstream whose single-dimension keys are known."""
    mapping = rs.PartitionMapping.specific_partitions(["a"])
    with pytest.raises(PartitionValidationError, match=re.escape(match)):
        validate_mapped_edge(None, make_parts(), mapping)


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
