from typing import Any

import pytest
import rivers as rs
from rivers.exceptions import PartitionValidationError


def test_variants():
    """PartitionMapping variant constructors and isinstance."""
    identity = rs.PartitionMapping.identity()
    assert isinstance(identity, rs.PartitionMapping.Identity)

    all_parts = rs.PartitionMapping.all_partitions()
    assert isinstance(all_parts, rs.PartitionMapping.AllPartitions)

    static = rs.PartitionMapping.static_({"a": "x", "b": "y"})
    assert isinstance(static, rs.PartitionMapping.Static)

    tw = rs.PartitionMapping.time_window(offset=-1)
    assert isinstance(tw, rs.PartitionMapping.TimeWindow)

    sp = rs.PartitionMapping.specific_partitions(["a", "b"])
    assert isinstance(sp, rs.PartitionMapping.SpecificPartitions)


def test_equality():
    """PartitionMapping equality."""
    assert rs.PartitionMapping.identity() == rs.PartitionMapping.identity()
    assert rs.PartitionMapping.identity() != rs.PartitionMapping.all_partitions()
    assert rs.PartitionMapping.time_window(
        offset=-1
    ) == rs.PartitionMapping.time_window(offset=-1)
    assert rs.PartitionMapping.time_window(
        offset=-1
    ) != rs.PartitionMapping.time_window(offset=0)


# ---------------------------------------------------------------------------
# PartitionMappingDict key types (str vs AssetDef)
# ---------------------------------------------------------------------------


def test_partition_mapping_dict_with_str_key():
    """partition_mapping with string keys works as before."""
    parts = rs.PartitionsDefinition.static_(["a", "b"])

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

    repo = rs.CodeRepository(assets=[upstream, downstream])
    repo.resolve()


def test_partition_mapping_dict_with_asset_def_key():
    """partition_mapping accepts AssetDef objects as keys, extracting .name."""
    parts = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    upstream_def = rs.AssetDef(name="upstream", partitions_def=parts)

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                upstream_def.name, partition_mapping=rs.PartitionMapping.identity()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = rs.CodeRepository(assets=[upstream, downstream])
    repo.resolve()


def test_partition_mapping_dict_with_asset_object_key():
    """partition_mapping accepts Asset (SingleAsset) objects as keys, extracting .name."""
    parts = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(partitions_def=parts)
    def upstream() -> Any:
        return 1

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                upstream.name, partition_mapping=rs.PartitionMapping.identity()
            )
        ],
    )
    def downstream(upstream: Any) -> Any:
        return upstream + 1

    repo = rs.CodeRepository(assets=[upstream, downstream])
    repo.resolve()


def test_partition_mapping_dict_mixed_keys():
    """partition_mapping accepts a mix of str and AssetDef keys."""
    parts = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(partitions_def=parts)
    def dep_one() -> Any:
        return 1

    @rs.Asset(partitions_def=parts)
    def dep_two() -> Any:
        return 2

    dep_two_def = rs.AssetDef(name="dep_two", partitions_def=parts)

    @rs.Asset(
        partitions_def=parts,
        deps=[
            rs.AssetDef.input(
                "dep_one", partition_mapping=rs.PartitionMapping.identity()
            ),
            rs.AssetDef.input(
                dep_two_def.name, partition_mapping=rs.PartitionMapping.identity()
            ),
        ],
    )
    def downstream(dep_one: Any, dep_two: Any) -> Any:
        return dep_one + dep_two

    repo = rs.CodeRepository(assets=[dep_one, dep_two, downstream])
    repo.resolve()


def test_partition_mapping_dict_invalid_key_type():
    """AssetDef.input() rejects non-string name."""
    with pytest.raises(TypeError):
        rs.AssetDef.input(42, partition_mapping=rs.PartitionMapping.identity())  # type: ignore[arg-type]


def test_partition_mapping_dict_on_asset_def():
    """AssetDef constructor accepts AssetDef keys in partition_mapping."""
    parts = rs.PartitionsDefinition.static_(["x", "y"])
    upstream_def = rs.AssetDef(name="upstream", partitions_def=parts)

    downstream_def = rs.AssetDef(
        name="downstream",
        partitions_def=parts,
        partition_mapping={upstream_def: rs.PartitionMapping.identity()},
    )
    assert downstream_def.partition_mapping == {
        "upstream": rs.PartitionMapping.identity()
    }


# ---------------------------------------------------------------------------
# PartitionMapping.multi() construction and validation
# ---------------------------------------------------------------------------


def test_multi_mapping_variant():
    """PartitionMapping.multi() creates a Multi variant."""
    m = rs.PartitionMapping.multi({"date": rs.PartitionMapping.identity()})
    assert isinstance(m, rs.PartitionMapping.Multi)


def test_multi_mapping_shorthand_same_dimension():
    """Bare PartitionMapping value maps to same-named dimension."""
    m = rs.PartitionMapping.multi(
        {
            "region": rs.PartitionMapping.identity(),
            "date": rs.PartitionMapping.time_window(offset=-1),
        }
    )
    assert isinstance(m, rs.PartitionMapping.Multi)


def test_multi_mapping_explicit_dimension_rename():
    """Tuple value maps upstream dim to differently-named downstream dim."""
    m = rs.PartitionMapping.multi(
        {
            "region": ("country", rs.PartitionMapping.identity()),
        }
    )
    assert isinstance(m, rs.PartitionMapping.Multi)


def test_multi_mapping_equality():
    """Multi mappings with same content are equal."""
    a = rs.PartitionMapping.multi({"x": rs.PartitionMapping.identity()})
    b = rs.PartitionMapping.multi({"x": rs.PartitionMapping.identity()})
    assert a == b


def test_multi_mapping_inequality_different_dims():
    """Multi mappings with different dimensions are not equal."""
    a = rs.PartitionMapping.multi({"x": rs.PartitionMapping.identity()})
    b = rs.PartitionMapping.multi({"y": rs.PartitionMapping.identity()})
    assert a != b


def test_multi_mapping_inequality_different_inner_mapping():
    """Multi mappings with different per-dim mappings are not equal."""
    a = rs.PartitionMapping.multi({"x": rs.PartitionMapping.identity()})
    b = rs.PartitionMapping.multi({"x": rs.PartitionMapping.all_partitions()})
    assert a != b


def test_multi_mapping_not_equal_to_identity():
    """Multi mapping is not equal to other mapping types."""
    m = rs.PartitionMapping.multi({"x": rs.PartitionMapping.identity()})
    assert m != rs.PartitionMapping.identity()


def test_multi_mapping_empty_rejected():
    """Empty dimension_mappings dict is rejected."""
    with pytest.raises(PartitionValidationError, match="at least one dimension"):
        rs.PartitionMapping.multi({})


def test_multi_mapping_nested_rejected():
    """Nested Multi mappings are rejected."""
    inner = rs.PartitionMapping.multi({"x": rs.PartitionMapping.identity()})
    with pytest.raises(PartitionValidationError, match="Nested Multi"):
        rs.PartitionMapping.multi({"outer": inner})


def test_multi_mapping_invalid_value_type():
    """Invalid value type in dimension_mappings is rejected."""
    with pytest.raises(TypeError, match="Multi mapping values"):
        rs.PartitionMapping.multi({"x": 42})  # type: ignore[dict-item]


# ---------------------------------------------------------------------------
# PartitionMapping.multi_to_single() construction and validation
# ---------------------------------------------------------------------------


def test_multi_to_single_variant():
    """multi_to_single() creates a MultiToSingle variant."""
    m = rs.PartitionMapping.multi_to_single(dimension_name="date")
    assert isinstance(m, rs.PartitionMapping.MultiToSingle)


def test_multi_to_single_equality():
    """MultiToSingle with same dimension_name are equal."""
    a = rs.PartitionMapping.multi_to_single(dimension_name="date")
    b = rs.PartitionMapping.multi_to_single(dimension_name="date")
    assert a == b


def test_multi_to_single_inequality():
    """MultiToSingle with different dimension_name are not equal."""
    a = rs.PartitionMapping.multi_to_single(dimension_name="date")
    b = rs.PartitionMapping.multi_to_single(dimension_name="region")
    assert a != b


def test_multi_to_single_not_equal_to_other_types():
    """MultiToSingle is not equal to other mapping types."""
    m = rs.PartitionMapping.multi_to_single(dimension_name="x")
    assert m != rs.PartitionMapping.identity()
    assert m != rs.PartitionMapping.all_partitions()


def test_multi_to_single_repr():
    """MultiToSingle has a useful repr."""
    m = rs.PartitionMapping.multi_to_single(dimension_name="date")
    assert "multi_to_single" in repr(m)
    assert "date" in repr(m)


def test_multi_to_single_with_inner_mapping():
    """multi_to_single() with an explicit partition_mapping."""
    m = rs.PartitionMapping.multi_to_single(
        dimension_name="date",
        partition_mapping=rs.PartitionMapping.time_window(offset=-1),
    )
    assert isinstance(m, rs.PartitionMapping.MultiToSingle)


def test_multi_to_single_equality_with_inner_mapping():
    """MultiToSingle with same dimension + inner mapping are equal."""
    a = rs.PartitionMapping.multi_to_single(
        dimension_name="date",
        partition_mapping=rs.PartitionMapping.time_window(offset=-1),
    )
    b = rs.PartitionMapping.multi_to_single(
        dimension_name="date",
        partition_mapping=rs.PartitionMapping.time_window(offset=-1),
    )
    assert a == b


def test_multi_to_single_inequality_different_inner_mapping():
    """MultiToSingle with different inner mappings are not equal."""
    a = rs.PartitionMapping.multi_to_single(
        dimension_name="date",
        partition_mapping=rs.PartitionMapping.time_window(offset=-1),
    )
    b = rs.PartitionMapping.multi_to_single(
        dimension_name="date",
        partition_mapping=rs.PartitionMapping.time_window(offset=0),
    )
    assert a != b


def test_multi_to_single_inequality_with_vs_without_inner():
    """MultiToSingle with inner mapping != without."""
    a = rs.PartitionMapping.multi_to_single(dimension_name="date")
    b = rs.PartitionMapping.multi_to_single(
        dimension_name="date",
        partition_mapping=rs.PartitionMapping.time_window(offset=-1),
    )
    assert a != b


def test_multi_to_single_repr_with_inner_mapping():
    """MultiToSingle repr includes the inner mapping."""
    m = rs.PartitionMapping.multi_to_single(
        dimension_name="date",
        partition_mapping=rs.PartitionMapping.time_window(offset=-1),
    )
    r = repr(m)
    assert "multi_to_single" in r
    assert "date" in r
    assert "time_window" in r


# ---------------------------------------------------------------------------
# PartitionMapping.specific_partitions() construction and validation
# ---------------------------------------------------------------------------


def test_specific_partitions_variant():
    """specific_partitions() creates a SpecificPartitions variant."""
    m = rs.PartitionMapping.specific_partitions(["a", "b"])
    assert isinstance(m, rs.PartitionMapping.SpecificPartitions)


def test_specific_partitions_equality():
    """SpecificPartitions with same keys are equal."""
    a = rs.PartitionMapping.specific_partitions(["a", "b"])
    b = rs.PartitionMapping.specific_partitions(["a", "b"])
    assert a == b


def test_specific_partitions_inequality_different_keys():
    """SpecificPartitions with different keys are not equal."""
    a = rs.PartitionMapping.specific_partitions(["a", "b"])
    b = rs.PartitionMapping.specific_partitions(["a", "c"])
    assert a != b


def test_specific_partitions_equality_different_order():
    """SpecificPartitions with different order are equal (keys are normalized)."""
    a = rs.PartitionMapping.specific_partitions(["a", "b"])
    b = rs.PartitionMapping.specific_partitions(["b", "a"])
    assert a == b


def test_specific_partitions_inequality_different_count():
    """SpecificPartitions with different number of keys are not equal."""
    a = rs.PartitionMapping.specific_partitions(["a", "b"])
    b = rs.PartitionMapping.specific_partitions(["a"])
    assert a != b


def test_specific_partitions_not_equal_to_other_types():
    """SpecificPartitions is not equal to other mapping types."""
    m = rs.PartitionMapping.specific_partitions(["a", "b"])
    assert m != rs.PartitionMapping.identity()
    assert m != rs.PartitionMapping.all_partitions()


def test_specific_partitions_repr():
    """SpecificPartitions has a useful repr."""
    m = rs.PartitionMapping.specific_partitions(["a", "b"])
    r = repr(m)
    assert "specific_partitions" in r
    assert "a" in r
    assert "b" in r


def test_specific_partitions_empty_rejected():
    """Empty partition_keys list is rejected."""
    with pytest.raises(PartitionValidationError, match="at least one partition key"):
        rs.PartitionMapping.specific_partitions([])


def test_specific_partitions_single_key():
    """SpecificPartitions with a single key works."""
    m = rs.PartitionMapping.specific_partitions(["a"])
    assert isinstance(m, rs.PartitionMapping.SpecificPartitions)


def test_specific_partitions_in_partition_mapping_dict():
    """SpecificPartitions can be used in a partition_mapping dict (unpartitioned downstream)."""
    parts_up = rs.PartitionsDefinition.static_(["a", "b", "c"])

    @rs.Asset(partitions_def=parts_up)
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

    repo = rs.CodeRepository(assets=[upstream, downstream])
    repo.resolve()
