"""Tests for partition mapping key transformation during materialization."""

from datetime import datetime

import pytest

import rivers as rs

from _helpers import TrackingHandler, make_repo

DAILY_START = datetime(2024, 1, 1)
STATIC_KEYS = ["a", "b", "c"]


# ---------------------------------------------------------------------------
# Multi partition context basics
# ---------------------------------------------------------------------------


def test_multi_partition_context_accessible():
    """Multi-partitioned asset gets correct has_partition_key and partition."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
            "region": rs.PartitionsDefinition.static_(STATIC_KEYS),
        }
    )
    captured = {}

    @rs.Asset(partitions_def=multi_parts)
    def my_asset(context: rs.AssetExecutionContext) -> int:
        captured["has_key"] = context.has_partition_key
        captured["partition"] = context.partition
        return 1

    repo = make_repo([my_asset])
    key = rs.PartitionKey.multi({"date": "2024-01-15", "region": "a"})
    result = repo.materialize(["my_asset"], partition_key=key)

    assert result.success
    assert captured["has_key"] is True
    assert captured["partition"].key == key
    assert isinstance(captured["partition"].definition, rs.PartitionsDefinition.Multi)


# ---------------------------------------------------------------------------
# Multi-to-Multi with Identity mapping
# ---------------------------------------------------------------------------


def test_multi_to_multi_identity_partition_context():
    """Multi-partitioned chain with identity mapping passes correct partition context."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
            "region": rs.PartitionsDefinition.static_(STATIC_KEYS),
        }
    )
    captured = {}

    @rs.Asset(partitions_def=multi_parts)
    def upstream(context: rs.AssetExecutionContext) -> dict:
        captured["upstream"] = context.partition
        return {"value": 1}

    @rs.Asset(
        partitions_def=multi_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi(
                    {
                        "date": rs.PartitionMapping.identity(),
                        "region": rs.PartitionMapping.identity(),
                    }
                ),
            ),
        ],
    )
    def downstream(context: rs.AssetExecutionContext, upstream: dict) -> dict:
        captured["downstream"] = context.partition
        return {"value": upstream["value"] + 1}

    repo = make_repo([upstream, downstream])
    key = rs.PartitionKey.multi({"date": "2024-01-15", "region": "a"})
    result = repo.materialize(["upstream", "downstream"], partition_key=key)

    assert result.success
    assert captured["upstream"].key == key
    assert captured["downstream"].key == key
    assert isinstance(captured["upstream"].definition, rs.PartitionsDefinition.Multi)
    assert isinstance(captured["downstream"].definition, rs.PartitionsDefinition.Multi)


# ---------------------------------------------------------------------------
# Multi-to-Multi dimension rename — key mapping through IO handler
# ---------------------------------------------------------------------------


def test_multi_to_multi_dimension_rename_key_mapping():
    """Multi mapping with dimension rename correctly maps partition key for upstream load."""
    up_parts = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
            "country": rs.PartitionsDefinition.static_(STATIC_KEYS),
        }
    )
    down_parts = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
            "region": rs.PartitionsDefinition.static_(STATIC_KEYS),
        }
    )
    handler = TrackingHandler()

    @rs.Asset(partitions_def=up_parts, io_handler=handler)
    def upstream() -> int:
        return 1

    @rs.Asset(
        partitions_def=down_parts,
        io_handler=handler,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi(
                    {
                        "country": ("region", rs.PartitionMapping.identity()),
                        "date": rs.PartitionMapping.identity(),
                    }
                ),
            ),
        ],
    )
    def downstream(upstream: int) -> int:
        return upstream + 1

    repo = make_repo([upstream, downstream])

    # Materialize only downstream — triggers load_input for upstream with mapped key
    down_key = rs.PartitionKey.multi({"date": "2024-01-15", "region": "a"})
    repo.materialize(["downstream"], partition_key=down_key)

    assert len(handler.load_input_partitions) == 1
    ctx = handler.load_input_partitions[0]
    assert ctx is not None
    # downstream "region" → upstream "country"
    expected_up_key = rs.PartitionKey.multi({"date": "2024-01-15", "country": "a"})
    assert ctx.key == expected_up_key


def test_multi_to_multi_static_submapping_key():
    """Multi mapping with Static per-dimension mapping transforms keys."""
    up_parts = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
        }
    )
    down_parts = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
            "region": rs.PartitionsDefinition.static_(["north_america", "europe"]),
        }
    )
    handler = TrackingHandler()

    @rs.Asset(partitions_def=up_parts, io_handler=handler)
    def upstream() -> int:
        return 1

    @rs.Asset(
        partitions_def=down_parts,
        io_handler=handler,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi(
                    {
                        "date": rs.PartitionMapping.identity(),
                        "region": rs.PartitionMapping.static_(
                            {"north_america": "us", "europe": "eu"}
                        ),
                    }
                ),
            ),
        ],
    )
    def downstream(upstream: int) -> int:
        return upstream + 1

    repo = make_repo([upstream, downstream])

    down_key = rs.PartitionKey.multi({"date": "2024-01-15", "region": "north_america"})
    repo.materialize(["downstream"], partition_key=down_key)

    assert len(handler.load_input_partitions) == 1
    ctx = handler.load_input_partitions[0]
    assert ctx is not None
    # "north_america" → "us" via Static mapping
    expected_up_key = rs.PartitionKey.multi({"date": "2024-01-15", "region": "us"})
    assert ctx.key == expected_up_key


# ---------------------------------------------------------------------------
# MultiToSingle — key mapping through IO handler
# ---------------------------------------------------------------------------


def test_multi_to_single_downstream_single_key():
    """MultiToSingle: downstream=Single, upstream=Multi — single key passed to upstream."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
            "region": rs.PartitionsDefinition.static_(STATIC_KEYS),
        }
    )
    daily_parts = rs.PartitionsDefinition.daily(start=DAILY_START)
    handler = TrackingHandler()

    @rs.Asset(partitions_def=multi_parts, io_handler=handler)
    def upstream() -> int:
        return 42

    @rs.Asset(
        partitions_def=daily_parts,
        io_handler=handler,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi_to_single(
                    dimension_name="date"
                ),
            ),
        ],
    )
    def downstream(upstream: int) -> int:
        return upstream + 1

    repo = make_repo([upstream, downstream])

    down_key = rs.PartitionKey.single("2024-01-15")
    result = repo.materialize(["downstream"], partition_key=down_key)

    assert result.success
    assert len(handler.load_input_partitions) == 1
    ctx = handler.load_input_partitions[0]
    assert ctx is not None
    # MultiToSingle fans in: mapped dimension gets the single key,
    # unmapped dimensions get ALL their partition keys
    expected_key = rs.PartitionKey.multi(
        {"date": "2024-01-15", "region": ["a", "b", "c"]}
    )
    assert ctx.key == expected_key
    # The definition is the upstream's Multi definition
    assert isinstance(ctx.definition, rs.PartitionsDefinition.Multi)


def test_multi_to_single_downstream_multi_extracts_dimension():
    """MultiToSingle: downstream=Multi, upstream=Single — extracts named dimension."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.daily(start=DAILY_START),
            "region": rs.PartitionsDefinition.static_(STATIC_KEYS),
        }
    )
    static_parts = rs.PartitionsDefinition.static_(STATIC_KEYS)
    handler = TrackingHandler(default_load_value="data")  # type: ignore[arg-type]

    @rs.Asset(partitions_def=static_parts, io_handler=handler)
    def upstream() -> str:
        return "data"

    @rs.Asset(
        partitions_def=multi_parts,
        io_handler=handler,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi_to_single(
                    dimension_name="region"
                ),
            ),
        ],
    )
    def downstream(upstream: str) -> str:
        return f"{upstream}-processed"

    repo = make_repo([upstream, downstream])

    down_key = rs.PartitionKey.multi({"date": "2024-01-15", "region": "a"})
    result = repo.materialize(["downstream"], partition_key=down_key)

    assert result.success
    assert len(handler.load_input_partitions) == 1
    ctx = handler.load_input_partitions[0]
    assert ctx is not None
    # Extracts "region" dimension → "a"
    assert ctx.key == rs.PartitionKey.single("a")
    assert isinstance(ctx.definition, rs.PartitionsDefinition.Static)


def test_multi_to_single_with_static_inner_mapping():
    """MultiToSingle with Static inner mapping transforms the dimension key."""
    multi_parts = rs.PartitionsDefinition.multi(
        {
            "category": rs.PartitionsDefinition.static_(["x", "y"]),
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
        }
    )
    static_parts = rs.PartitionsDefinition.static_(["north_america", "europe"])
    handler = TrackingHandler()

    @rs.Asset(partitions_def=multi_parts, io_handler=handler)
    def upstream() -> int:
        return 1

    @rs.Asset(
        partitions_def=static_parts,
        io_handler=handler,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.multi_to_single(
                    dimension_name="region",
                    partition_mapping=rs.PartitionMapping.static_(
                        {"north_america": "us", "europe": "eu"}
                    ),
                ),
            ),
        ],
    )
    def downstream(upstream: int) -> int:
        return upstream + 1

    repo = make_repo([upstream, downstream])

    down_key = rs.PartitionKey.single("north_america")
    repo.materialize(["downstream"], partition_key=down_key)

    assert len(handler.load_input_partitions) == 1
    ctx = handler.load_input_partitions[0]
    assert ctx is not None
    # "north_america" → Static inner mapping → "us" for region,
    # unmapped "category" dimension gets ALL keys
    expected_key = rs.PartitionKey.multi({"region": "us", "category": ["x", "y"]})
    assert ctx.key == expected_key


# ---------------------------------------------------------------------------
# Identity and Static mapping — key mapping through IO handler
# ---------------------------------------------------------------------------


def test_identity_mapping_key_passthrough():
    """Identity partition mapping passes the same key to upstream IO handler."""
    parts = rs.PartitionsDefinition.static_(STATIC_KEYS)
    handler = TrackingHandler()

    @rs.Asset(partitions_def=parts, io_handler=handler)
    def upstream() -> int:
        return 1

    @rs.Asset(
        partitions_def=parts,
        io_handler=handler,
        deps=[
            rs.AssetDef.input(
                "upstream", partition_mapping=rs.PartitionMapping.identity()
            ),
        ],
    )
    def downstream(upstream: int) -> int:
        return upstream + 1

    repo = make_repo([upstream, downstream])

    key = rs.PartitionKey.single("a")
    repo.materialize(["downstream"], partition_key=key)

    assert len(handler.load_input_partitions) == 1
    assert handler.load_input_partitions[0].key == key


def test_static_mapping_transforms_key():
    """Static partition mapping transforms the key for upstream IO handler."""
    up_parts = rs.PartitionsDefinition.static_(["x", "y"])
    down_parts = rs.PartitionsDefinition.static_(["a", "b"])
    handler = TrackingHandler()

    @rs.Asset(partitions_def=up_parts, io_handler=handler)
    def upstream() -> int:
        return 1

    @rs.Asset(
        partitions_def=down_parts,
        io_handler=handler,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.static_({"a": "x", "b": "y"}),
            ),
        ],
    )
    def downstream(upstream: int) -> int:
        return upstream + 1

    repo = make_repo([upstream, downstream])

    # downstream "a" → upstream "x"
    repo.materialize(["downstream"], partition_key=rs.PartitionKey.single("a"))

    assert len(handler.load_input_partitions) == 1
    assert handler.load_input_partitions[0].key == rs.PartitionKey.single("x")


# ---------------------------------------------------------------------------
# SpecificPartitions — key mapping through IO handler
# ---------------------------------------------------------------------------


def test_specific_partitions_both_partitioned_rejected():
    """SpecificPartitions is rejected when both sides are partitioned."""
    from rivers.exceptions import PartitionValidationError

    up_parts = rs.PartitionsDefinition.static_(["a", "b", "c", "d"])
    down_parts = rs.PartitionsDefinition.static_(["x", "y"])

    @rs.Asset(partitions_def=up_parts)
    def upstream() -> int:
        return 1

    @rs.Asset(
        partitions_def=down_parts,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.specific_partitions(["a", "b"]),
            ),
        ],
    )
    def downstream(upstream: int) -> int:
        return upstream + 1

    with pytest.raises(
        PartitionValidationError, match="SpecificPartitions.*only valid.*unpartitioned"
    ):
        make_repo([upstream, downstream])


def test_specific_partitions_unpartitioned_downstream_partitioned_upstream():
    """SpecificPartitions: unpartitioned downstream depends on partitioned upstream."""
    up_parts = rs.PartitionsDefinition.static_(["a", "b", "c"])
    handler = TrackingHandler()

    @rs.Asset(partitions_def=up_parts, io_handler=handler)
    def upstream() -> int:
        return 1

    @rs.Asset(
        io_handler=handler,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.specific_partitions(["a", "c"]),
            ),
        ],
    )
    def downstream(upstream: int) -> int:
        return upstream + 1

    repo = make_repo([upstream, downstream])

    result = repo.materialize(["downstream"])

    assert result.success
    assert len(handler.load_input_partitions) == 1
    ctx = handler.load_input_partitions[0]
    assert ctx is not None
    # Should load specific upstream keys ["a", "c"]
    assert ctx.key == rs.PartitionKey.single(["a", "c"])


def test_specific_partitions_multiple_upstreams():
    """SpecificPartitions can target different keys for different upstreams (unpartitioned downstream)."""
    parts = rs.PartitionsDefinition.static_(["a", "b", "c", "d"])
    handler = TrackingHandler()

    @rs.Asset(partitions_def=parts, io_handler=handler)
    def dep_one() -> int:
        return 1

    @rs.Asset(partitions_def=parts, io_handler=handler)
    def dep_two() -> int:
        return 2

    @rs.Asset(
        io_handler=handler,
        deps=[
            rs.AssetDef.input(
                "dep_one",
                partition_mapping=rs.PartitionMapping.specific_partitions(["a"]),
            ),
            rs.AssetDef.input(
                "dep_two",
                partition_mapping=rs.PartitionMapping.specific_partitions(["c", "d"]),
            ),
        ],
    )
    def downstream(dep_one: int, dep_two: int) -> int:
        return dep_one + dep_two

    repo = make_repo([dep_one, dep_two, downstream])

    result = repo.materialize(["downstream"])

    assert result.success
    # Both upstreams were loaded with their specific keys
    assert len(handler.load_input_partitions) == 2
    keys_loaded = {p.key for p in handler.load_input_partitions}
    assert rs.PartitionKey.single("a") in keys_loaded
    assert rs.PartitionKey.single(["c", "d"]) in keys_loaded


def test_specific_partitions_chain():
    """SpecificPartitions works in a chain: A (partitioned) → B (unpartitioned, specific)."""
    parts_a = rs.PartitionsDefinition.static_(["a", "b", "c"])
    handler = TrackingHandler()

    @rs.Asset(partitions_def=parts_a, io_handler=handler)
    def first() -> int:
        return 1

    @rs.Asset(
        io_handler=handler,
        deps=[
            rs.AssetDef.input(
                "first",
                partition_mapping=rs.PartitionMapping.specific_partitions(["a", "b"]),
            ),
        ],
    )
    def second(first: int) -> int:
        return first + 1

    repo = make_repo([first, second])

    result = repo.materialize(["second"])

    assert result.success
    assert len(handler.load_input_partitions) == 1
    assert handler.load_input_partitions[0].key == rs.PartitionKey.single(["a", "b"])


# ---------------------------------------------------------------------------
# Mixed mapping types in a single asset
# ---------------------------------------------------------------------------


def test_mixed_mapping_types_in_single_asset():
    """A single asset uses Static mapping for one dep and Identity for another."""
    shared_parts = rs.PartitionsDefinition.static_(STATIC_KEYS)
    other_parts = rs.PartitionsDefinition.static_(["x", "y", "z"])
    handler = TrackingHandler()

    @rs.Asset(partitions_def=shared_parts, io_handler=handler)
    def dep_identity() -> int:
        return 1

    @rs.Asset(partitions_def=other_parts, io_handler=handler)
    def dep_static() -> int:
        return 2

    @rs.Asset(
        partitions_def=shared_parts,
        io_handler=handler,
        deps=[
            rs.AssetDef.input(
                "dep_identity", partition_mapping=rs.PartitionMapping.identity()
            ),
            rs.AssetDef.input(
                "dep_static",
                partition_mapping=rs.PartitionMapping.static_(
                    {"a": "x", "b": "y", "c": "z"}
                ),
            ),
        ],
    )
    def downstream(dep_identity: int, dep_static: int) -> int:
        return dep_identity + dep_static

    repo = make_repo([dep_identity, dep_static, downstream])

    repo.materialize(["downstream"], partition_key=rs.PartitionKey.single("a"))

    assert len(handler.load_input_partitions) == 2
    keys_loaded = {p.key for p in handler.load_input_partitions}
    # Identity: "a" → "a"
    assert rs.PartitionKey.single("a") in keys_loaded
    # Static: "a" → "x"
    assert rs.PartitionKey.single("x") in keys_loaded
