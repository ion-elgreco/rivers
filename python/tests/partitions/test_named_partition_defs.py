"""Tests for the ``CodeRepository(partition_defs={...})`` registry.

Assets and tasks can reference a partition definition by name
(``partitions_def="name"``) instead of passing the object inline; the name
resolves against the repository registry at ``resolve()`` / ``validate()``.
"""

import asyncio

import pytest
import rivers as rs
from _helpers import CapturingHandler
from rivers.exceptions import ConfigurationError


def _regions() -> rs.PartitionsDefinition:
    return rs.PartitionsDefinition.static_(["us-east", "us-west", "eu"])


# ---------------------------------------------------------------------------
# Resolution + execution per asset shape
# ---------------------------------------------------------------------------


def test_named_def_on_single_asset(storage):
    """A single asset resolves partitions_def="name" and materializes a partition."""
    handler = CapturingHandler()

    @rs.Asset(partitions_def="regions", io_handler=handler)
    def sales(context: rs.AssetExecutionContext) -> str:
        return f"rows-{context.partition_key}"

    repo = rs.CodeRepository(
        assets=[sales],
        partition_defs={"regions": _regions()},
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    result = repo.materialize(partition_key=rs.PartitionKey.single("us-east"))

    assert result.success
    assert handler.store["sales"] == "rows-us-east"
    ctx = [c for c in handler.output_contexts if c.asset_name == "sales"][0]
    assert ctx.partition is not None
    assert ctx.partition.key == rs.PartitionKey.single("us-east")
    assert isinstance(ctx.partition.definition, rs.PartitionsDefinition.Static)
    assert storage.get_materialized_partitions("sales") == [
        rs.PartitionKey.single("us-east")
    ]


def test_named_def_on_async_single_asset(storage):
    """Async assets resolve named defs the same way."""

    @rs.Asset(partitions_def="regions")
    async def async_sales(context: rs.AssetExecutionContext) -> str:
        await asyncio.sleep(0)
        return context.partition_key

    repo = rs.CodeRepository(
        assets=[async_sales],
        partition_defs={"regions": _regions()},
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    result = repo.materialize(partition_key=rs.PartitionKey.single("eu"))

    assert result.success
    assert storage.get_materialized_partitions("async_sales") == [
        rs.PartitionKey.single("eu")
    ]


def test_named_def_on_multi_asset_outputs():
    """Per-output AssetDefs accept a registry name; partition context reaches IO."""
    handler = CapturingHandler()

    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("x", io_handler=handler, partitions_def="regions"),
            rs.AssetDef("y", io_handler=handler, partitions_def="regions"),
        ],
    )
    def multi():
        yield rs.Output(value=10, output_name="x")
        yield rs.Output(value=20, output_name="y")

    repo = rs.CodeRepository(
        assets=[multi],
        partition_defs={"regions": _regions()},
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize(partition_key=rs.PartitionKey.single("us-west"))

    assert result.success
    assert handler.store["x"] == 10
    assert handler.store["y"] == 20
    contexts = {c.asset_name: c for c in handler.output_contexts}
    assert contexts["x"].partition.key == rs.PartitionKey.single("us-west")
    assert contexts["y"].partition.key == rs.PartitionKey.single("us-west")


def test_named_def_on_multi_asset_top_level():
    """The multi-level partitions_def accepts a registry name."""
    handler = CapturingHandler()

    @rs.Asset.from_multi(
        partitions_def="regions",
        output_defs=[
            rs.AssetDef("a", io_handler=handler),
            rs.AssetDef("b", io_handler=handler),
        ],
    )
    def multi_top():
        yield rs.Output(value=1, output_name="a")
        yield rs.Output(value=2, output_name="b")

    repo = rs.CodeRepository(
        assets=[multi_top],
        partition_defs={"regions": _regions()},
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize(partition_key=rs.PartitionKey.single("eu"))

    assert result.success
    contexts = {c.asset_name: c for c in handler.output_contexts}
    assert contexts["a"].partition.key == rs.PartitionKey.single("eu")
    assert contexts["b"].partition.key == rs.PartitionKey.single("eu")


def test_named_def_on_graph_asset_propagates_to_tasks():
    """A graph asset's named def resolves and is inherited by internal tasks."""

    @rs.Task
    def step_one() -> int:
        return 10

    @rs.Task
    def step_two(step_one: int) -> int:
        return step_one * 2

    @rs.Asset.from_graph(name="pipeline", partitions_def="regions")
    def pipeline():
        a = step_one()
        return step_two(a)

    repo = rs.CodeRepository(
        assets=[pipeline],
        tasks=[step_one, step_two],
        partition_defs={"regions": _regions()},
        default_executor=rs.Executor.in_process(),
    )
    pk = rs.PartitionKey.single("us-east")
    result = repo.materialize(partition_key=pk)

    assert result.success
    assert repo.load_node("pipeline/step_one", partition_key=pk) == 10
    assert repo.load_node("pipeline/step_two", partition_key=pk) == 20
    assert repo.load_node("pipeline", partition_key=pk) == 20


def test_named_def_on_external_asset(storage):
    """External assets accept a registry name."""
    ext = rs.Asset.external(
        name="warehouse_table",
        io_handler=rs.InMemoryIOHandler(),
        partitions_def="regions",
    )

    repo = rs.CodeRepository(
        assets=[ext],
        partition_defs={"regions": _regions()},
    )
    repo.resolve(storage=storage)
    assert "warehouse_table" in repo.assets


def test_named_def_on_standalone_task(storage):
    """Standalone tasks accept a registry name."""

    @rs.Task(partitions_def="regions")
    def chore() -> int:
        return 1

    @rs.Asset
    def anchor() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[anchor],
        tasks=[chore],
        partition_defs={"regions": _regions()},
    )
    repo.resolve(storage=storage)


def test_named_def_shared_across_assets_in_job():
    """Two assets sharing one named def stay partition-compatible in a job."""
    handler = CapturingHandler()

    @rs.Asset(partitions_def="regions", io_handler=handler)
    def upstream(context: rs.AssetExecutionContext) -> str:
        return context.partition_key

    @rs.Asset(partitions_def="regions", io_handler=handler)
    def downstream(upstream: str) -> str:
        return f"{upstream}-derived"

    repo = rs.CodeRepository(
        assets=[upstream, downstream],
        partition_defs={"regions": _regions()},
        jobs=[
            rs.Job(
                name="both",
                assets=[upstream, downstream],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("both").execute(partition_key=rs.PartitionKey.single("eu"))

    assert handler.store["upstream"] == "eu"
    assert handler.store["downstream"] == "eu-derived"


def test_backfill_with_named_def(storage):
    """Backfill enumerates the resolved named def's keys."""
    materialized: list[str] = []

    @rs.Asset(partitions_def="regions")
    def per_region(context: rs.AssetExecutionContext) -> str:
        key = context.partition_key
        materialized.append(key)
        return key

    repo = rs.CodeRepository(
        assets=[per_region],
        partition_defs={"regions": _regions()},
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    result = repo.backfill(
        selection=["per_region"],
        partition_keys=[
            rs.PartitionKey.single("us-east"),
            rs.PartitionKey.single("eu"),
        ],
    )

    assert result.completed == 2
    assert result.failed == 0
    assert sorted(materialized) == ["eu", "us-east"]
    assert sorted(
        k.key[0] for k in storage.get_materialized_partitions("per_region")
    ) == ["eu", "us-east"]


def test_named_dynamic_def(storage):
    """A Dynamic definition registered by name works with storage-backed keys."""

    @rs.Asset(partitions_def="users")
    def per_user(context: rs.AssetExecutionContext) -> str:
        return context.partition_key

    repo = rs.CodeRepository(
        assets=[per_user],
        partition_defs={"users": rs.PartitionsDefinition.dynamic("users")},
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    storage.add_dynamic_partitions("users", ["u1", "u2"])
    result = repo.materialize(partition_key=rs.PartitionKey.single("u1"))

    assert result.success
    assert storage.get_materialized_partitions("per_user") == [
        rs.PartitionKey.single("u1")
    ]


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


def test_unknown_name_errors_at_resolve(storage):
    @rs.Asset(partitions_def="not_registered")
    def orphan() -> int:
        return 1

    repo = rs.CodeRepository(assets=[orphan], partition_defs={"regions": _regions()})
    with pytest.raises(
        ConfigurationError,
        match=r"unknown partitions definition 'not_registered'.*orphan.*regions",
    ):
        repo.resolve(storage=storage)


def test_unknown_name_errors_at_validate():
    """validate() catches unknown names without touching storage."""

    @rs.Asset(partitions_def="typo")
    def orphan() -> int:
        return 1

    repo = rs.CodeRepository(assets=[orphan], partition_defs={"real": _regions()})
    with pytest.raises(ConfigurationError, match="typo"):
        repo.validate()


def test_named_def_without_registry_errors():
    @rs.Asset(partitions_def="regions")
    def orphan() -> int:
        return 1

    repo = rs.CodeRepository(assets=[orphan])
    with pytest.raises(
        ConfigurationError, match=r"unknown partitions definition 'regions'"
    ):
        repo.validate()


def test_unknown_name_on_graph_asset_errors():
    @rs.Task
    def inner() -> int:
        return 1

    @rs.Asset.from_graph(name="graph", partitions_def="missing")
    def graph():
        return inner()

    repo = rs.CodeRepository(assets=[graph], tasks=[inner])
    with pytest.raises(ConfigurationError, match="missing.*graph"):
        repo.validate()


def test_unknown_name_on_task_errors():
    @rs.Task(partitions_def="missing")
    def chore() -> int:
        return 1

    @rs.Asset
    def anchor() -> int:
        return 1

    repo = rs.CodeRepository(assets=[anchor], tasks=[chore])
    with pytest.raises(ConfigurationError, match="missing.*chore"):
        repo.validate()


def test_mixed_named_variants_on_multi_outputs_error():
    """Named refs to different variants fail the multi-output check at validate()."""
    import datetime

    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("s", partitions_def="static"),
            rs.AssetDef("t", partitions_def="daily"),
        ],
    )
    def mixed():
        yield rs.Output(value=1, output_name="s")
        yield rs.Output(value=2, output_name="t")

    repo = rs.CodeRepository(
        assets=[mixed],
        partition_defs={
            "static": _regions(),
            "daily": rs.PartitionsDefinition.daily(datetime.datetime(2024, 1, 1)),
        },
    )
    with pytest.raises(ValueError, match="same partition type"):
        repo.validate()


def test_invalid_partitions_def_type_errors():
    with pytest.raises(ValueError, match="PartitionsDefinition or a str"):

        @rs.Asset(partitions_def=123)
        def bad() -> int:
            return 1


# ---------------------------------------------------------------------------
# Introspection
# ---------------------------------------------------------------------------


def test_partitions_def_property_returns_name_or_object():
    """Pre-resolution, the property surfaces exactly what was passed."""

    @rs.Asset(partitions_def="regions")
    def named() -> int:
        return 1

    inline_def = _regions()

    @rs.Asset(partitions_def=inline_def)
    def inline() -> int:
        return 1

    assert named.partitions_def == "regions"
    assert inline.partitions_def == inline_def


def test_asset_def_partitions_def_accepts_name():
    d = rs.AssetDef("out", partitions_def="regions")
    assert d.partitions_def == "regions"

    d.partitions_def = _regions()
    assert isinstance(d.partitions_def, rs.PartitionsDefinition.Static)


def test_asset_def_equality_with_names():
    a = rs.AssetDef("out", partitions_def="regions")
    b = rs.AssetDef("out", partitions_def="regions")
    c = rs.AssetDef("out", partitions_def="other")
    assert a == b
    assert a != c
