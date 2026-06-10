"""Tests for dynamic partitions — storage-backed partition management."""

import re
from typing import Any

import pytest

import rivers as rs
from rivers.exceptions import (
    ExecutionError,
    PartitionDefinitionError,
    PartitionValidationError,
)

# ---------------------------------------------------------------------------
# PartitionsDefinition.dynamic() construction
# ---------------------------------------------------------------------------


def test_dynamic_construction():
    """Can create a Dynamic partition definition."""
    dyn = rs.PartitionsDefinition.dynamic("users")
    assert isinstance(dyn, rs.PartitionsDefinition.Dynamic)


def test_dynamic_repr():
    dyn = rs.PartitionsDefinition.dynamic("users")
    assert repr(dyn) == 'PartitionsDefinition.dynamic("users")'


def test_dynamic_empty_name():
    with pytest.raises(PartitionDefinitionError, match="cannot be empty"):
        rs.PartitionsDefinition.dynamic("")


def test_dynamic_get_partition_keys_raises():
    """Dynamic partitions can't enumerate keys without storage."""
    dyn = rs.PartitionsDefinition.dynamic("users")
    with pytest.raises(NotImplementedError):
        dyn.get_partition_keys()


def test_dynamic_equality():
    a = rs.PartitionsDefinition.dynamic("x")
    b = rs.PartitionsDefinition.dynamic("x")
    c = rs.PartitionsDefinition.dynamic("y")
    assert a == b
    assert a != c


# ---------------------------------------------------------------------------
# Storage API for dynamic partitions
# ---------------------------------------------------------------------------


def test_storage_add_and_get(storage):
    storage.add_dynamic_partitions("users", ["u1", "u2", "u3"])
    keys = storage.get_dynamic_partitions("users")
    assert keys == ["u1", "u2", "u3"]


def test_storage_idempotent_add(storage):
    storage.add_dynamic_partitions("users", ["u1", "u2"])
    storage.add_dynamic_partitions("users", ["u2", "u3"])
    keys = storage.get_dynamic_partitions("users")
    assert keys == ["u1", "u2", "u3"]


def test_storage_delete(storage):
    storage.add_dynamic_partitions("users", ["u1", "u2", "u3"])
    storage.delete_dynamic_partition("users", "u2")
    keys = storage.get_dynamic_partitions("users")
    assert keys == ["u1", "u3"]


def test_storage_delete_nonexistent(storage):
    # Should not raise
    storage.delete_dynamic_partition("users", "nonexistent")


def test_storage_has(storage):
    storage.add_dynamic_partitions("users", ["u1"])
    assert storage.has_dynamic_partition("users", "u1")
    assert not storage.has_dynamic_partition("users", "u2")


def test_storage_isolated_namespaces(storage):
    storage.add_dynamic_partitions("users", ["u1"])
    storage.add_dynamic_partitions("products", ["p1", "p2"])

    assert storage.get_dynamic_partitions("users") == ["u1"]
    assert storage.get_dynamic_partitions("products") == ["p1", "p2"]
    assert not storage.has_dynamic_partition("products", "u1")


def test_storage_empty(storage):
    with pytest.warns(
        UserWarning, match="No dynamic partitions found for 'nonexistent'"
    ):
        assert storage.get_dynamic_partitions("nonexistent") == []


def test_storage_add_empty_list(storage):
    storage.add_dynamic_partitions("users", [])
    with pytest.warns(UserWarning, match="No dynamic partitions found for 'users'"):
        assert storage.get_dynamic_partitions("users") == []


# ---------------------------------------------------------------------------
# Dynamic partitions with assets
# ---------------------------------------------------------------------------


def test_asset_with_dynamic_partitions_def():
    """Can define an asset with a dynamic partition definition."""
    dyn = rs.PartitionsDefinition.dynamic("customers")

    @rs.Asset(partitions_def=dyn)
    def customer_data() -> Any:
        return 1

    assert customer_data._name == "customer_data"


def test_two_dynamic_assets_same_def():
    """Two assets sharing the same dynamic partition def should resolve with Identity."""
    dyn = rs.PartitionsDefinition.dynamic("customers")

    @rs.Asset(partitions_def=dyn)
    def raw_customers() -> Any:
        return 1

    @rs.Asset(partitions_def=dyn)
    def processed_customers(raw_customers: Any) -> Any:
        return raw_customers

    repo = rs.CodeRepository(assets=[raw_customers, processed_customers])
    repo.resolve()


def test_dynamic_with_all_partitions_mapping():
    """Dynamic upstream with unpartitioned downstream using AllPartitions."""
    dyn = rs.PartitionsDefinition.dynamic("customers")

    @rs.Asset(partitions_def=dyn)
    def source() -> Any:
        return 1

    @rs.Asset(
        deps=[
            rs.AssetDef.input(
                "source", partition_mapping=rs.PartitionMapping.all_partitions()
            )
        ]
    )
    def sink(source: Any) -> Any:
        return source

    repo = rs.CodeRepository(assets=[source, sink])
    repo.resolve()


def test_unpartitioned_to_dynamic_requires_mapping():
    """Unpartitioned downstream depending on dynamic upstream needs explicit mapping."""
    dyn = rs.PartitionsDefinition.dynamic("customers")

    @rs.Asset(partitions_def=dyn)
    def source() -> Any:
        return 1

    @rs.Asset
    def sink(source: Any) -> Any:
        return source

    with pytest.raises(
        PartitionValidationError, match="partition_mapping.*is required"
    ):
        repo = rs.CodeRepository(assets=[source, sink])
        repo.resolve()


def test_dynamic_vs_static_identity_mismatch():
    """Dynamic and Static partition types should fail with Identity mapping."""
    dyn = rs.PartitionsDefinition.dynamic("customers")
    static = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(partitions_def=static)
    def source() -> Any:
        return 1

    @rs.Asset(partitions_def=dyn)
    def sink(source: Any) -> Any:
        return source

    with pytest.raises(
        PartitionValidationError, match="Identity mapping requires same partition type"
    ):
        repo = rs.CodeRepository(assets=[source, sink])
        repo.resolve()


# ---------------------------------------------------------------------------
# Dynamic partitions with storage integration (CodeRepository)
# ---------------------------------------------------------------------------


def test_dynamic_partitions_via_repo_storage():
    """Use repo storage to add dynamic partitions and verify."""
    dyn = rs.PartitionsDefinition.dynamic("items")

    @rs.Asset(partitions_def=dyn)
    def items() -> Any:
        return 1

    repo = rs.CodeRepository(assets=[items])
    repo.resolve()
    storage = repo.storage

    storage.add_dynamic_partitions("items", ["item_1", "item_2"])
    assert storage.get_dynamic_partitions("items") == ["item_1", "item_2"]
    assert storage.has_dynamic_partition("items", "item_1")

    storage.delete_dynamic_partition("items", "item_1")
    assert storage.get_dynamic_partitions("items") == ["item_2"]


# ---------------------------------------------------------------------------
# Dynamic partitions materialization
# ---------------------------------------------------------------------------


def test_dynamic_materialize_partition_context(storage):
    """Materializing a dynamic-partitioned asset provides correct PartitionContext."""
    dyn = rs.PartitionsDefinition.dynamic("tenants")
    captured = {}

    @rs.Asset(partitions_def=dyn)
    def tenant_data(context: rs.AssetExecutionContext) -> str:
        captured["asset_name"] = context.asset_name
        captured["has_partition_key"] = context.has_partition_key
        captured["partition_key"] = context.partition_key
        captured["partition"] = context.partition
        captured["partition_time_window"] = context.partition_time_window
        return f"data-for-{context.partition_key}"

    repo = rs.CodeRepository(assets=[tenant_data])
    repo.resolve(storage=storage)
    storage.add_dynamic_partitions("tenants", ["acme"])
    result = repo.materialize(
        ["tenant_data"], partition_key=rs.PartitionKey.single("acme")
    )

    assert result.success
    assert (
        repo.load_node("tenant_data", partition_key=rs.PartitionKey.single("acme"))
        == "data-for-acme"
    )
    assert captured["asset_name"] == "tenant_data"
    assert captured["has_partition_key"] is True
    assert captured["partition_key"] == "acme"
    assert isinstance(captured["partition"], rs.PartitionContext)
    assert captured["partition"].key == rs.PartitionKey.single("acme")
    assert isinstance(captured["partition"].definition, rs.PartitionsDefinition.Dynamic)
    assert captured["partition_time_window"] is None


def test_dynamic_materialize_chain(storage):
    """Two dynamic-partitioned assets in a chain receive the same partition key."""
    dyn = rs.PartitionsDefinition.dynamic("regions")
    seen_keys = {}

    @rs.Asset(partitions_def=dyn)
    def raw(context: rs.AssetExecutionContext) -> str:
        seen_keys["raw"] = context.partition_key
        return f"raw-{context.partition_key}"

    @rs.Asset(partitions_def=dyn)
    def processed(context: rs.AssetExecutionContext, raw: str) -> str:
        seen_keys["processed"] = context.partition_key
        return f"processed({raw})"

    repo = rs.CodeRepository(assets=[raw, processed])
    repo.resolve(storage=storage)
    storage.add_dynamic_partitions("regions", ["us-west"])
    result = repo.materialize(
        ["raw", "processed"], partition_key=rs.PartitionKey.single("us-west")
    )

    assert result.success
    assert seen_keys["raw"] == "us-west"
    assert seen_keys["processed"] == "us-west"
    pk = rs.PartitionKey.single("us-west")
    assert repo.load_node("raw", partition_key=pk) == "raw-us-west"
    assert repo.load_node("processed", partition_key=pk) == "processed(raw-us-west)"


def test_dynamic_materialize_io_handler_receives_partition(storage):
    """IO handler's OutputContext has the correct partition for dynamic assets."""
    dyn = rs.PartitionsDefinition.dynamic("datasets")
    captured_contexts = []

    class CapturingHandler(rs.BaseIOHandler):
        def handle_output(self, context, obj):
            captured_contexts.append(context)

        def load_input(self, context):
            return None

    @rs.Asset(io_handler=CapturingHandler(), partitions_def=dyn)
    def my_dataset() -> int:
        return 42

    repo = rs.CodeRepository(assets=[my_dataset])
    repo.resolve(storage=storage)
    storage.add_dynamic_partitions("datasets", ["2024-q1"])
    repo.materialize(["my_dataset"], partition_key=rs.PartitionKey.single("2024-q1"))

    assert len(captured_contexts) == 1
    ctx = captured_contexts[0]
    assert ctx.partition is not None
    assert ctx.partition.key == rs.PartitionKey.single("2024-q1")
    assert isinstance(ctx.partition.definition, rs.PartitionsDefinition.Dynamic)


def test_dynamic_materialize_without_partition_key_raises():
    """Materializing a dynamic-partitioned asset without partition_key raises."""
    dyn = rs.PartitionsDefinition.dynamic("tenants")

    @rs.Asset(partitions_def=dyn)
    def tenant_data() -> Any:
        return 1

    repo = rs.CodeRepository(assets=[tenant_data])
    repo.resolve()

    with pytest.raises(ExecutionError, match="partition_key"):
        repo.materialize(["tenant_data"])


def test_dynamic_materialize_unregistered_key_raises(storage):
    """A dynamic key that was never registered in storage is rejected at submit
    — the def can't know its keys, so membership is checked against storage."""
    dyn = rs.PartitionsDefinition.dynamic("tenants")

    @rs.Asset(partitions_def=dyn)
    def tenant_data() -> int:
        return 1

    repo = rs.CodeRepository(assets=[tenant_data])
    repo.resolve(storage=storage)
    storage.add_dynamic_partitions("tenants", ["acme"])

    with pytest.raises(
        ExecutionError,
        match=re.escape(
            "Invalid partition_key 'acmme' for asset 'tenant_data': not a "
            "registered dynamic partition key of namespace 'tenants'. Register "
            'it with add_dynamic_partitions("tenants", [...]) first.'
        ),
    ):
        repo.materialize(["tenant_data"], partition_key=rs.PartitionKey.single("acmme"))

    # No run/materialization recorded for the rejected key.
    assert storage.get_materialized_partitions("tenant_data") == []


def test_dynamic_materialize_deleted_key_raises(storage):
    """A dynamic key that was registered and later deleted is rejected."""
    dyn = rs.PartitionsDefinition.dynamic("tenants")

    @rs.Asset(partitions_def=dyn)
    def tenant_data() -> int:
        return 1

    repo = rs.CodeRepository(assets=[tenant_data])
    repo.resolve(storage=storage)
    storage.add_dynamic_partitions("tenants", ["acme", "globex"])
    storage.delete_dynamic_partition("tenants", "globex")

    result = repo.materialize(
        ["tenant_data"], partition_key=rs.PartitionKey.single("acme")
    )
    assert result.success

    with pytest.raises(
        ExecutionError,
        match=re.escape(
            "Invalid partition_key 'globex' for asset 'tenant_data': not a "
            "registered dynamic partition key of namespace 'tenants'. Register "
            'it with add_dynamic_partitions("tenants", [...]) first.'
        ),
    ):
        repo.materialize(
            ["tenant_data"], partition_key=rs.PartitionKey.single("globex")
        )
