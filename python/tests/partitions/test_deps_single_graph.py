"""Tests for deps parameter on SingleAsset and GraphAsset.

These tests verify that AssetDef.input() and AssetDef.dep() work on
@Asset(...) and Asset.from_graph(...), including partition mappings,
IO handler overrides, metadata overrides, and lineage-only deps.
"""

from typing import Any

import pytest

import rivers as rs

from _helpers import CapturingHandler


class CustomLoader(rs.BaseIOHandler):
    """Always returns a fixed value, ignoring what was stored."""

    def handle_output(self, context, obj):
        pass

    def load_input(self, context):
        return 999


# ===========================================================================
# SingleAsset + deps
# ===========================================================================


class TestSingleAssetDeps:
    """Tests for @Asset(deps=[...])."""

    def test_input_dep_with_partition_mapping(self):
        """AssetDef.input() partition_mapping is applied when loading upstream."""
        handler = CapturingHandler()
        pd_down = rs.PartitionsDefinition.static_(["a", "b"])
        pd_source = rs.PartitionsDefinition.static_(["1", "2", "3"])

        @rs.Asset(io_handler=handler, partitions_def=pd_source)
        def source(context: rs.AssetExecutionContext) -> int:
            return {"1": 10, "2": 20, "3": 30}[context.partition_key]

        @rs.Asset(
            io_handler=handler,
            partitions_def=pd_down,
            deps=[
                rs.AssetDef.input(
                    "source",
                    partition_mapping=rs.PartitionMapping.static_({"a": "1", "b": "2"}),
                ),
            ],
        )
        def consumer(source: int) -> int:
            return source + 1

        repo = rs.CodeRepository(
            assets=[source, consumer],
            default_executor=rs.Executor.in_process(),
        )

        repo.materialize(["source"], partition_key=rs.PartitionKey.single("1"))
        repo.materialize(["source"], partition_key=rs.PartitionKey.single("2"))

        result = repo.materialize(
            ["consumer"], partition_key=rs.PartitionKey.single("a")
        )
        assert result.success
        assert handler.store["consumer"] == 11  # source "1" → 10, +1

        result = repo.materialize(
            ["consumer"], partition_key=rs.PartitionKey.single("b")
        )
        assert result.success
        assert handler.store["consumer"] == 21  # source "2" → 20, +1

    def test_dep_only_creates_graph_edge(self):
        """AssetDef.dep() adds a graph edge without loading data."""
        handler = CapturingHandler()

        @rs.Asset(io_handler=handler)
        def trigger() -> int:
            return 1

        @rs.Asset(
            io_handler=handler,
            deps=[rs.AssetDef.dep("trigger")],
        )
        def consumer() -> int:
            return 42

        repo = rs.CodeRepository(
            assets=[trigger, consumer],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize()
        assert result.success
        assert handler.store["consumer"] == 42
        assert handler.store["trigger"] == 1

        # trigger was materialized before consumer
        output_names = [ctx.asset_name for ctx in handler.output_contexts]
        assert output_names.index("trigger") < output_names.index("consumer")

    def test_input_dep_must_match_fn_param(self):
        """AssetDef.input() with a name not in fn params raises ValueError."""
        with pytest.raises(ValueError, match="does not match any parameter"):

            @rs.Asset(
                deps=[rs.AssetDef.input("nonexistent")],
            )
            def consumer() -> int:
                return 1

    def test_dep_only_does_not_need_fn_param(self):
        """AssetDef.dep() names don't need to match function parameters."""
        handler = CapturingHandler()

        @rs.Asset(io_handler=handler)
        def upstream() -> int:
            return 1

        @rs.Asset(
            io_handler=handler,
            deps=[rs.AssetDef.dep("upstream")],
        )
        def consumer() -> int:
            return 5

        repo = rs.CodeRepository(
            assets=[upstream, consumer],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize()
        assert result.success
        assert handler.store["consumer"] == 5

    def test_mixed_input_and_dep(self):
        """Both AssetDef.input() and AssetDef.dep() can be used together."""
        handler = CapturingHandler()

        @rs.Asset(io_handler=handler)
        def data_source() -> int:
            return 10

        @rs.Asset(io_handler=handler)
        def lineage_dep() -> int:
            return 0

        @rs.Asset(
            io_handler=handler,
            deps=[
                rs.AssetDef.input("data_source"),
                rs.AssetDef.dep("lineage_dep"),
            ],
        )
        def consumer(data_source: int) -> int:
            return data_source * 2

        repo = rs.CodeRepository(
            assets=[data_source, lineage_dep, consumer],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize()
        assert result.success
        assert handler.store["consumer"] == 20

    def test_input_dep_io_handler_override(self):
        """AssetDef.input() io_handler overrides the upstream's handler for loading."""
        output_handler = CapturingHandler()

        @rs.Asset(io_handler=output_handler)
        def source() -> int:
            return 1

        @rs.Asset(
            io_handler=output_handler,
            deps=[rs.AssetDef.input("source", io_handler=CustomLoader())],
        )
        def consumer(source: int) -> int:
            return source

        repo = rs.CodeRepository(
            assets=[source, consumer],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize()
        assert result.success
        # source produced 1, but CustomLoader returns 999
        assert output_handler.store["consumer"] == 999

    def test_input_dep_metadata_override(self):
        """AssetDef.input() metadata overrides upstream's metadata in InputContext."""
        captured_metadata = {}

        class MetadataCapturingHandler(rs.BaseIOHandler):
            store: dict[str, Any] = {}

            def handle_output(self, context, obj):
                self.store[context.asset_name] = obj

            def load_input(self, context):
                captured_metadata[context.asset_name] = context.asset_metadata
                return self.store.get(context.asset_name)

        handler = MetadataCapturingHandler()

        @rs.Asset(io_handler=handler, metadata={"format": "parquet", "version": "1"})
        def source() -> int:
            return 42

        @rs.Asset(
            io_handler=handler,
            deps=[rs.AssetDef.input("source", metadata={"columns": "a,b,c"})],
        )
        def consumer(source: int) -> int:
            return source

        repo = rs.CodeRepository(
            assets=[source, consumer],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize()
        assert result.success
        assert captured_metadata["source"] == {"columns": "a,b,c"}

    def test_input_dep_metadata_does_not_mutate_upstream(self):
        """Metadata override is scoped to the consumer's load only."""
        captured_metadata = {}

        class MetadataCapturingHandler(rs.BaseIOHandler):
            store: dict[str, Any] = {}

            def handle_output(self, context, obj):
                self.store[context.asset_name] = obj

            def load_input(self, context):
                captured_metadata.setdefault(context.downstream_asset, {})[
                    context.asset_name
                ] = context.asset_metadata
                return self.store.get(context.asset_name)

        handler = MetadataCapturingHandler()

        @rs.Asset(io_handler=handler, metadata={"format": "parquet"})
        def source() -> int:
            return 10

        @rs.Asset(
            io_handler=handler,
            deps=[rs.AssetDef.input("source", metadata={"columns": "a,b"})],
        )
        def consumer_a(source: int) -> int:
            return source + 1

        @rs.Asset(io_handler=handler)
        def consumer_b(source: int) -> int:
            return source + 100

        repo = rs.CodeRepository(
            assets=[source, consumer_a, consumer_b],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize()
        assert result.success

        # consumer_a's load of source sees the override
        assert captured_metadata["consumer_a"]["source"] == {"columns": "a,b"}
        # consumer_b's load of source sees the original metadata
        assert captured_metadata["consumer_b"]["source"] == {"format": "parquet"}


# ===========================================================================
# GraphAsset + deps
# ===========================================================================


class TestGraphAssetDeps:
    """Tests for Asset.from_graph(deps=[...])."""

    def test_deps_with_partition_mapping(self):
        """deps replaces partition_mappings for graph assets."""
        handler = CapturingHandler()
        pd_graph = rs.PartitionsDefinition.static_(["x", "y"])
        pd_source = rs.PartitionsDefinition.static_(["1", "2", "3"])

        @rs.Asset(io_handler=handler, partitions_def=pd_source)
        def source(context: rs.AssetExecutionContext) -> int:
            return {"1": 10, "2": 20, "3": 30}[context.partition_key]

        @rs.Task
        def transform(source: int) -> int:
            return source + 1

        @rs.Asset.from_graph(
            name="pipeline",
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
        def pipeline(source: int):
            return transform(source)

        repo = rs.CodeRepository(
            assets=[source, pipeline],
            tasks=[transform],
            default_executor=rs.Executor.in_process(),
        )

        repo.materialize(["source"], partition_key=rs.PartitionKey.single("1"))
        repo.materialize(["source"], partition_key=rs.PartitionKey.single("2"))

        result = repo.materialize(
            ["pipeline"], partition_key=rs.PartitionKey.single("x")
        )
        assert result.success
        assert handler.store["pipeline"] == 11

        result = repo.materialize(
            ["pipeline"], partition_key=rs.PartitionKey.single("y")
        )
        assert result.success
        assert handler.store["pipeline"] == 21

    def test_dep_only_creates_graph_edge(self):
        """AssetDef.dep() on a graph asset ensures ordering without data flow."""
        handler = CapturingHandler()

        @rs.Asset(io_handler=handler)
        def trigger() -> int:
            return 1

        @rs.Task
        def compute() -> int:
            return 42

        @rs.Asset.from_graph(
            name="pipeline",
            io_handler=handler,
            deps=[rs.AssetDef.dep("trigger")],
        )
        def pipeline():
            return compute()

        repo = rs.CodeRepository(
            assets=[trigger, pipeline],
            tasks=[compute],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize()
        assert result.success
        assert handler.store["pipeline"] == 42
        assert handler.store["trigger"] == 1

        output_names = [ctx.asset_name for ctx in handler.output_contexts]
        assert output_names.index("trigger") < output_names.index("pipeline")

    def test_input_dep_io_handler_override(self):
        """IO handler override on graph deps propagates to internal tasks."""
        handler = CapturingHandler()

        @rs.Asset(io_handler=handler)
        def source() -> int:
            return 1

        @rs.Task
        def process(source: int) -> int:
            return source

        @rs.Asset.from_graph(
            name="pipeline",
            io_handler=handler,
            deps=[rs.AssetDef.input("source", io_handler=CustomLoader())],
        )
        def pipeline(source: int):
            return process(source)

        repo = rs.CodeRepository(
            assets=[source, pipeline],
            tasks=[process],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize()
        assert result.success
        # source produced 1, but CustomLoader returns 999
        assert handler.store["pipeline"] == 999

    def test_input_dep_metadata_override(self):
        """Metadata override on graph deps propagates to internal tasks."""
        captured_metadata = {}

        class MetadataCapturingHandler(rs.BaseIOHandler):
            store: dict[str, Any] = {}

            def handle_output(self, context, obj):
                self.store[context.asset_name] = obj

            def load_input(self, context):
                captured_metadata[context.asset_name] = context.asset_metadata
                return self.store.get(context.asset_name)

        handler = MetadataCapturingHandler()

        @rs.Asset(io_handler=handler, metadata={"format": "parquet"})
        def source() -> int:
            return 42

        @rs.Task
        def process(source: int) -> int:
            return source

        @rs.Asset.from_graph(
            name="pipeline",
            io_handler=handler,
            deps=[rs.AssetDef.input("source", metadata={"columns": "x,y"})],
        )
        def pipeline(source: int):
            return process(source)

        repo = rs.CodeRepository(
            assets=[source, pipeline],
            tasks=[process],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize()
        assert result.success
        # The internal task's load of source should see the override metadata
        assert captured_metadata["source"] == {"columns": "x,y"}

    def test_mixed_deps(self):
        """Both input and dep-only deps on a graph asset."""
        handler = CapturingHandler()
        pd = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset(io_handler=handler, partitions_def=pd)
        def data_source(context: rs.AssetExecutionContext) -> int:
            return {"a": 10, "b": 100}[context.partition_key]

        @rs.Asset(io_handler=handler)
        def lineage_dep() -> int:
            return 0

        @rs.Task
        def process(data_source: int) -> int:
            return data_source * 2

        @rs.Asset.from_graph(
            name="pipeline",
            partitions_def=pd,
            io_handler=handler,
            deps=[
                rs.AssetDef.input("data_source"),
                rs.AssetDef.dep("lineage_dep"),
            ],
        )
        def pipeline(data_source: int):
            return process(data_source)

        repo = rs.CodeRepository(
            assets=[data_source, lineage_dep, pipeline],
            tasks=[process],
            default_executor=rs.Executor.in_process(),
        )

        repo.materialize(["data_source"], partition_key=rs.PartitionKey.single("a"))
        result = repo.materialize(
            ["pipeline"], partition_key=rs.PartitionKey.single("a")
        )
        assert result.success
        assert handler.store["pipeline"] == 20
