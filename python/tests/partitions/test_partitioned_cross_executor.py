"""Cross-executor + sync/async parametrized tests for partitioned multi-assets and graph assets.

Covers the key data-flow scenarios across:
- Executor: in_process, parallel
- Function type: sync, async
"""

import asyncio
from typing import Any

import rivers as rs


# ---------------------------------------------------------------------------
# Multi-asset + partitions (top-level partitions_def)
# ---------------------------------------------------------------------------


class TestMultiAssetPartitionedCrossExecutor:
    """Partitioned multi-assets across executors and sync/async."""

    def test_sync_multi_asset_partitioned(self, executor_env):
        """Sync multi-asset with top-level partitions_def."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset.from_multi(
            partitions_def=pd,
            output_defs=[
                rs.AssetDef("x", io_handler=handler),
                rs.AssetDef("y", io_handler=handler),
            ],
        )
        def multi(context: rs.AssetExecutionContext):
            val = {"a": 10, "b": 20}[context.partition_key]
            yield rs.Output(value=val, output_name="x")
            yield rs.Output(value=val * 2, output_name="y")

        repo = rs.CodeRepository(assets=[multi], default_executor=executor)
        result = repo.materialize(partition_key=rs.PartitionKey.single("a"))
        assert result.success
        assert repo.load_node("x", partition_key=rs.PartitionKey.single("a")) == 10
        assert repo.load_node("y", partition_key=rs.PartitionKey.single("a")) == 20

    def test_async_multi_asset_partitioned(self, executor_env):
        """Async multi-asset with top-level partitions_def."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset.from_multi(
            partitions_def=pd,
            output_defs=[
                rs.AssetDef("x", io_handler=handler),
                rs.AssetDef("y", io_handler=handler),
            ],
        )
        async def multi(context: rs.AssetExecutionContext):
            await asyncio.sleep(0)
            val = {"a": 10, "b": 20}[context.partition_key]
            yield rs.Output(value=val, output_name="x")
            yield rs.Output(value=val * 2, output_name="y")

        repo = rs.CodeRepository(assets=[multi], default_executor=executor)
        result = repo.materialize(partition_key=rs.PartitionKey.single("b"))
        assert result.success
        assert repo.load_node("x", partition_key=rs.PartitionKey.single("b")) == 20
        assert repo.load_node("y", partition_key=rs.PartitionKey.single("b")) == 40


# ---------------------------------------------------------------------------
# Multi-asset + downstream chain
# ---------------------------------------------------------------------------


class TestMultiAssetDownstreamCrossExecutor:
    """Partitioned multi-asset → downstream single asset across executors."""

    def test_sync_downstream(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = rs.PartitionsDefinition.static_(["p1", "p2"])

        @rs.Asset.from_multi(
            partitions_def=pd,
            output_defs=[
                rs.AssetDef("x", io_handler=handler),
                rs.AssetDef("y", io_handler=handler),
            ],
        )
        def producer(context: rs.AssetExecutionContext):
            val = {"p1": 10, "p2": 100}[context.partition_key]
            yield rs.Output(value=val, output_name="x")
            yield rs.Output(value=val * 2, output_name="y")

        @rs.Asset(io_handler=handler, partitions_def=pd)
        def consumer(x: int, y: int) -> int:
            return x + y

        repo = rs.CodeRepository(assets=[producer, consumer], default_executor=executor)
        result = repo.materialize(partition_key=rs.PartitionKey.single("p1"))
        assert result.success
        assert (
            repo.load_node("consumer", partition_key=rs.PartitionKey.single("p1")) == 30
        )

    def test_async_downstream(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = rs.PartitionsDefinition.static_(["p1", "p2"])

        @rs.Asset.from_multi(
            partitions_def=pd,
            output_defs=[
                rs.AssetDef("x", io_handler=handler),
                rs.AssetDef("y", io_handler=handler),
            ],
        )
        async def producer(context: rs.AssetExecutionContext):
            await asyncio.sleep(0)
            val = {"p1": 10, "p2": 100}[context.partition_key]
            yield rs.Output(value=val, output_name="x")
            yield rs.Output(value=val * 2, output_name="y")

        @rs.Asset(io_handler=handler, partitions_def=pd)
        async def consumer(x: int, y: int) -> int:
            return x + y

        repo = rs.CodeRepository(assets=[producer, consumer], default_executor=executor)
        result = repo.materialize(partition_key=rs.PartitionKey.single("p1"))
        assert result.success
        assert (
            repo.load_node("consumer", partition_key=rs.PartitionKey.single("p1")) == 30
        )


# ---------------------------------------------------------------------------
# Graph asset + partitions
# ---------------------------------------------------------------------------


class TestGraphAssetPartitionedCrossExecutor:
    """Partitioned graph assets across executors and sync/async."""

    def test_sync_graph_asset_partitioned(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset(io_handler=handler, partitions_def=pd)
        def source(context: rs.AssetExecutionContext) -> int:
            return {"a": 5, "b": 50}[context.partition_key]

        @rs.Task
        def double(source: int) -> int:
            return source * 2

        @rs.Asset.from_graph(name="pipeline", partitions_def=pd, io_handler=handler)
        def pipeline(source: int):
            return double(source)

        repo = rs.CodeRepository(
            assets=[source, pipeline], tasks=[double], default_executor=executor
        )
        result = repo.materialize(partition_key=rs.PartitionKey.single("a"))
        assert result.success
        assert (
            repo.load_node("pipeline", partition_key=rs.PartitionKey.single("a")) == 10
        )

    def test_async_graph_asset_upstream(self, executor_env):
        """Async upstream asset feeding into graph asset."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset(io_handler=handler, partitions_def=pd)
        async def source(context: rs.AssetExecutionContext) -> int:
            return {"a": 5, "b": 50}[context.partition_key]

        @rs.Task
        def double(source: int) -> int:
            return source * 2

        @rs.Asset.from_graph(name="pipeline", partitions_def=pd, io_handler=handler)
        def pipeline(source: int):
            return double(source)

        repo = rs.CodeRepository(
            assets=[source, pipeline], tasks=[double], default_executor=executor
        )
        result = repo.materialize(partition_key=rs.PartitionKey.single("a"))
        assert result.success
        assert (
            repo.load_node("pipeline", partition_key=rs.PartitionKey.single("a")) == 10
        )


# ---------------------------------------------------------------------------
# Graph asset + partition mappings
# ---------------------------------------------------------------------------


class TestGraphAssetMappingCrossExecutor:
    """Graph asset partition mappings across executors."""

    def test_sync_static_mapping(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()
        pd_graph = rs.PartitionsDefinition.static_(["x", "y"])
        pd_source = rs.PartitionsDefinition.static_(["1", "2"])

        @rs.Asset(io_handler=handler, partitions_def=pd_source)
        def source(context: rs.AssetExecutionContext) -> int:
            return {"1": 10, "2": 20}[context.partition_key]

        @rs.Task
        def transform(source: int) -> int:
            return source + 1

        @rs.Asset.from_graph(
            name="pipe",
            partitions_def=pd_graph,
            io_handler=handler,
            deps=[
                rs.AssetDef.input(
                    "source",
                    partition_mapping=rs.PartitionMapping.static_(
                        mapping={"x": "1", "y": "2"}
                    ),
                ),
            ],
        )
        def pipe(source: int):
            return transform(source)

        repo = rs.CodeRepository(
            assets=[source, pipe], tasks=[transform], default_executor=executor
        )

        # Pre-materialize source
        repo.materialize(["source"], partition_key=rs.PartitionKey.single("1"))
        repo.materialize(["source"], partition_key=rs.PartitionKey.single("2"))

        # x → source "1" (10) → transform → 11
        result = repo.materialize(["pipe"], partition_key=rs.PartitionKey.single("x"))
        assert result.success
        assert repo.load_node("pipe", partition_key=rs.PartitionKey.single("x")) == 11

        # y → source "2" (20) → transform → 21
        result = repo.materialize(["pipe"], partition_key=rs.PartitionKey.single("y"))
        assert result.success
        assert repo.load_node("pipe", partition_key=rs.PartitionKey.single("y")) == 21

    def test_async_static_mapping(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()
        pd_graph = rs.PartitionsDefinition.static_(["x", "y"])
        pd_source = rs.PartitionsDefinition.static_(["1", "2"])

        @rs.Asset(io_handler=handler, partitions_def=pd_source)
        async def source(context: rs.AssetExecutionContext) -> int:
            return {"1": 10, "2": 20}[context.partition_key]

        @rs.Task
        def transform(source: int) -> int:
            return source + 1

        @rs.Asset.from_graph(
            name="pipe",
            partitions_def=pd_graph,
            io_handler=handler,
            deps=[
                rs.AssetDef.input(
                    "source",
                    partition_mapping=rs.PartitionMapping.static_(
                        mapping={"x": "1", "y": "2"}
                    ),
                ),
            ],
        )
        def pipe(source: int):
            return transform(source)

        repo = rs.CodeRepository(
            assets=[source, pipe], tasks=[transform], default_executor=executor
        )

        repo.materialize(["source"], partition_key=rs.PartitionKey.single("1"))
        repo.materialize(["source"], partition_key=rs.PartitionKey.single("2"))

        result = repo.materialize(["pipe"], partition_key=rs.PartitionKey.single("x"))
        assert result.success
        assert repo.load_node("pipe", partition_key=rs.PartitionKey.single("x")) == 11


# ---------------------------------------------------------------------------
# Multi-asset deps (partition mapping, io_handler, metadata overrides)
# ---------------------------------------------------------------------------


class TestMultiAssetDepsCrossExecutor:
    """Multi-asset deps features across executors and sync/async."""

    def test_sync_input_dep_partition_mapping(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()
        pd_multi = rs.PartitionsDefinition.static_(["a", "b"])
        pd_source = rs.PartitionsDefinition.static_(["1", "2"])

        @rs.Asset(io_handler=handler, partitions_def=pd_source)
        def source(context: rs.AssetExecutionContext) -> int:
            return {"1": 10, "2": 20}[context.partition_key]

        @rs.Asset.from_multi(
            partitions_def=pd_multi,
            output_defs=[rs.AssetDef("out", io_handler=handler)],
            deps=[
                rs.AssetDef.input(
                    "source",
                    partition_mapping=rs.PartitionMapping.static_({"a": "1", "b": "2"}),
                ),
            ],
        )
        def multi(source: int):
            yield rs.Output(value=source + 1, output_name="out")

        repo = rs.CodeRepository(assets=[source, multi], default_executor=executor)

        repo.materialize(["source"], partition_key=rs.PartitionKey.single("1"))
        repo.materialize(["source"], partition_key=rs.PartitionKey.single("2"))

        result = repo.materialize(["out"], partition_key=rs.PartitionKey.single("a"))
        assert result.success
        assert repo.load_node("out", partition_key=rs.PartitionKey.single("a")) == 11

        result = repo.materialize(["out"], partition_key=rs.PartitionKey.single("b"))
        assert result.success
        assert repo.load_node("out", partition_key=rs.PartitionKey.single("b")) == 21

    def test_async_input_dep_partition_mapping(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()
        pd_multi = rs.PartitionsDefinition.static_(["a", "b"])
        pd_source = rs.PartitionsDefinition.static_(["1", "2"])

        @rs.Asset(io_handler=handler, partitions_def=pd_source)
        async def source(context: rs.AssetExecutionContext) -> int:
            return {"1": 10, "2": 20}[context.partition_key]

        @rs.Asset.from_multi(
            partitions_def=pd_multi,
            output_defs=[rs.AssetDef("out", io_handler=handler)],
            deps=[
                rs.AssetDef.input(
                    "source",
                    partition_mapping=rs.PartitionMapping.static_({"a": "1", "b": "2"}),
                ),
            ],
        )
        async def multi(source: int):
            yield rs.Output(value=source + 1, output_name="out")

        repo = rs.CodeRepository(assets=[source, multi], default_executor=executor)

        repo.materialize(["source"], partition_key=rs.PartitionKey.single("1"))

        result = repo.materialize(["out"], partition_key=rs.PartitionKey.single("a"))
        assert result.success
        assert repo.load_node("out", partition_key=rs.PartitionKey.single("a")) == 11

    def test_sync_dep_only_graph_edge(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()

        @rs.Asset(io_handler=handler)
        def trigger() -> int:
            return 1

        @rs.Asset.from_multi(
            output_defs=[rs.AssetDef("x", io_handler=handler)],
            deps=[rs.AssetDef.dep("trigger")],
        )
        def multi():
            yield rs.Output(value=99, output_name="x")

        repo = rs.CodeRepository(assets=[trigger, multi], default_executor=executor)
        result = repo.materialize()
        assert result.success
        assert repo.load_node("trigger") == 1
        assert repo.load_node("x") == 99

    def test_async_dep_only_graph_edge(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()

        @rs.Asset(io_handler=handler)
        async def trigger() -> int:
            return 1

        @rs.Asset.from_multi(
            output_defs=[rs.AssetDef("x", io_handler=handler)],
            deps=[rs.AssetDef.dep("trigger")],
        )
        async def multi():
            yield rs.Output(value=99, output_name="x")

        repo = rs.CodeRepository(assets=[trigger, multi], default_executor=executor)
        result = repo.materialize()
        assert result.success
        assert repo.load_node("trigger") == 1
        assert repo.load_node("x") == 99

    def test_sync_metadata_override(self, executor_env):
        """Metadata override flows correctly across executors."""
        executor, make_handler = executor_env
        handler = make_handler()
        captured_metadata: dict[str, Any] = {}

        class MetaHandler(rs.BaseIOHandler):
            def handle_output(self, context, obj):
                handler.handle_output(context, obj)

            def load_input(self, context):
                captured_metadata[context.asset_name] = context.asset_metadata
                return handler.load_input(context)

        meta_handler = MetaHandler()

        @rs.Asset(io_handler=meta_handler, metadata={"format": "parquet"})
        def source() -> int:
            return 42

        @rs.Asset.from_multi(
            output_defs=[rs.AssetDef("x", io_handler=handler)],
            deps=[
                rs.AssetDef.input("source", metadata={"columns": "a,b"}),
            ],
        )
        def multi(source: int):
            yield rs.Output(value=source, output_name="x")

        repo = rs.CodeRepository(assets=[source, multi], default_executor=executor)
        result = repo.materialize()
        assert result.success
        assert captured_metadata["source"] == {"columns": "a,b"}
