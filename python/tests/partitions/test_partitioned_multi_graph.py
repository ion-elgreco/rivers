"""Tests for partitioned execution of MultiAsset and GraphAsset.

These tests verify that partition context is correctly propagated when
executing multi-assets and graph-assets with partitions_def set.
"""

from typing import Any

import rivers as rs

from _helpers import CapturingHandler, DictIOHandler


# ===========================================================================
# MultiAsset + Partitions
# ===========================================================================


class TestMultiAssetPartitioned:
    """Tests for partitioned multi-asset execution."""

    def test_multi_asset_with_partitions_def_on_outputs(self):
        """Multi-asset output defs with partitions_def propagate partition context to IO."""
        handler = CapturingHandler()
        pd = rs.PartitionsDefinition.static_(["a", "b", "c"])

        @rs.Asset.from_multi(
            output_defs=[
                rs.AssetDef("x", io_handler=handler, partitions_def=pd),
                rs.AssetDef("y", io_handler=handler, partitions_def=pd),
            ],
        )
        def partitioned_multi():
            yield rs.Output(value=10, output_name="x")
            yield rs.Output(value=20, output_name="y")

        repo = rs.CodeRepository(
            assets=[partitioned_multi],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize(partition_key=rs.PartitionKey.single("a"))

        assert result.success
        assert handler.store["x"] == 10
        assert handler.store["y"] == 20

        # Verify partition context was propagated to output contexts
        out_names = {ctx.asset_name: ctx for ctx in handler.output_contexts}
        assert "x" in out_names
        assert "y" in out_names
        assert out_names["x"].partition is not None
        assert out_names["x"].partition.key == rs.PartitionKey.single("a")
        assert out_names["y"].partition is not None
        assert out_names["y"].partition.key == rs.PartitionKey.single("a")

    def test_multi_asset_partition_context_in_execution_context(self):
        """Multi-asset function receives AssetExecutionContext with partition info."""
        pd = rs.PartitionsDefinition.static_(["x", "y", "z"])
        captured = {}

        @rs.Asset.from_multi(
            output_defs=[
                rs.AssetDef("a", partitions_def=pd),
                rs.AssetDef("b", partitions_def=pd),
            ],
        )
        def ctx_multi(context: rs.AssetExecutionContext):
            captured["has_partition_key"] = context.has_partition_key
            captured["partition"] = context.partition
            yield rs.Output(value=1, output_name="a")
            yield rs.Output(value=2, output_name="b")

        repo = rs.CodeRepository(
            assets=[ctx_multi],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize(partition_key=rs.PartitionKey.single("x"))

        assert result.success
        assert captured["has_partition_key"] is True
        assert captured["partition"] is not None
        assert captured["partition"].key == rs.PartitionKey.single("x")

    def test_multi_asset_partitioned_downstream_receives_partition(self):
        """Downstream single asset receives partition context when depending on partitioned multi-asset."""
        handler = CapturingHandler()
        pd = rs.PartitionsDefinition.static_(["p1", "p2"])

        @rs.Asset.from_multi(
            output_defs=[
                rs.AssetDef("x", io_handler=handler, partitions_def=pd),
                rs.AssetDef("y", io_handler=handler, partitions_def=pd),
            ],
        )
        def producer(context: rs.AssetExecutionContext):
            val = {"p1": 10, "p2": 100}[context.partition_key]
            yield rs.Output(value=val, output_name="x")
            yield rs.Output(value=val * 2, output_name="y")

        @rs.Asset(io_handler=handler, partitions_def=pd)
        def consumer(x: int, y: int) -> int:
            return x + y

        repo = rs.CodeRepository(
            assets=[producer, consumer],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize(partition_key=rs.PartitionKey.single("p1"))

        assert result.success
        assert handler.store["x"] == 10
        assert handler.store["y"] == 20
        assert handler.store["consumer"] == 30

        # Consumer's output context should also have partition info
        consumer_ctx = [
            ctx for ctx in handler.output_contexts if ctx.asset_name == "consumer"
        ]
        assert consumer_ctx[0].partition.key == rs.PartitionKey.single("p1")

    def test_multi_asset_with_storage_events_partitioned(self, storage):
        """Partitioned multi-asset materialization records partition key in storage events."""
        handler = DictIOHandler()
        pd = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset.from_multi(
            output_defs=[
                rs.AssetDef("out_a", io_handler=handler, partitions_def=pd),
                rs.AssetDef("out_b", io_handler=handler, partitions_def=pd),
            ],
        )
        def stored_multi():
            yield rs.Output(value=100, output_name="out_a")
            yield rs.Output(value=200, output_name="out_b")

        repo = rs.CodeRepository(
            assets=[stored_multi],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)
        result = repo.materialize(partition_key=rs.PartitionKey.single("a"))

        assert result.success

        # Check events have partition key
        events_a = storage.get_events_for_asset("out_a")
        mat_events = [e for e in events_a if e.event_type == "Materialization"]
        assert len(mat_events) == 1
        assert mat_events[0].partition_key == rs.PartitionKey.single("a")

    def test_multi_asset_daily_partitions(self):
        """Multi-asset with daily time partitions executes correctly."""
        from datetime import datetime

        handler = CapturingHandler()
        pd = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))

        @rs.Asset.from_multi(
            output_defs=[
                rs.AssetDef("raw", io_handler=handler, partitions_def=pd),
                rs.AssetDef("clean", io_handler=handler, partitions_def=pd),
            ],
        )
        def etl(context: rs.AssetExecutionContext):
            yield rs.Output(value="raw_data", output_name="raw")
            yield rs.Output(value="clean_data", output_name="clean")

        repo = rs.CodeRepository(
            assets=[etl],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize(partition_key=rs.PartitionKey.single("2024-01-15"))

        assert result.success
        assert handler.store["raw"] == "raw_data"
        assert handler.store["clean"] == "clean_data"


class TestMultiAssetTopLevelPartitionsDef:
    """Tests for multi-asset with top-level partitions_def applied to all outputs."""

    def test_top_level_partitions_def_propagates_to_all_outputs(self):
        """partitions_def on from_multi applies to every output."""
        handler = CapturingHandler()
        pd = rs.PartitionsDefinition.static_(["a", "b", "c"])

        @rs.Asset.from_multi(
            partitions_def=pd,
            output_defs=[
                rs.AssetDef("x", io_handler=handler),
                rs.AssetDef("y", io_handler=handler),
            ],
        )
        def multi():
            yield rs.Output(value=10, output_name="x")
            yield rs.Output(value=20, output_name="y")

        repo = rs.CodeRepository(
            assets=[multi],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize(partition_key=rs.PartitionKey.single("b"))

        assert result.success
        assert handler.store["x"] == 10
        assert handler.store["y"] == 20

        out_names = {ctx.asset_name: ctx for ctx in handler.output_contexts}
        assert out_names["x"].partition.key == rs.PartitionKey.single("b")
        assert out_names["y"].partition.key == rs.PartitionKey.single("b")

    def test_top_level_partitions_def_with_downstream(self):
        """Downstream asset can depend on multi-asset with top-level partitions_def."""
        handler = CapturingHandler()
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

        repo = rs.CodeRepository(
            assets=[producer, consumer],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize(partition_key=rs.PartitionKey.single("p1"))

        assert result.success
        assert handler.store["x"] == 10
        assert handler.store["y"] == 20
        assert handler.store["consumer"] == 30

    def test_top_level_partitions_def_overrides_output_def(self):
        """Top-level partitions_def wins over AssetDef-level since all outputs share one function."""
        pd_top = rs.PartitionsDefinition.static_(["a", "b"])
        pd_output = rs.PartitionsDefinition.static_(["x", "y", "z"])

        @rs.Asset.from_multi(
            partitions_def=pd_top,
            output_defs=[
                rs.AssetDef("normal"),
                rs.AssetDef("custom", partitions_def=pd_output),
            ],
        )
        def multi():
            yield rs.Output(value=1, output_name="normal")
            yield rs.Output(value=2, output_name="custom")

        # Both outputs should use the top-level partitions_def
        defs = multi.output_defs
        normal_def = next(d for d in defs if d.name == "normal")
        custom_def = next(d for d in defs if d.name == "custom")
        assert normal_def.partitions_def is not None
        assert custom_def.partitions_def is not None
        # Both should be the same (top-level wins)
        assert normal_def.partitions_def is custom_def.partitions_def

    def test_partitions_def_getter_returns_top_level(self):
        """The partitions_def property on a multi-asset returns the top-level value."""
        pd = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset.from_multi(
            partitions_def=pd,
            output_defs=[rs.AssetDef("x"), rs.AssetDef("y")],
        )
        def multi():
            yield rs.Output(value=1, output_name="x")
            yield rs.Output(value=2, output_name="y")

        assert multi.partitions_def is not None

    def test_per_output_partitions_def_with_overlap(self):
        """Per-output partitions_defs are allowed if they share at least one common key."""
        handler = CapturingHandler()
        pd_a = rs.PartitionsDefinition.static_(["a", "b", "c"])
        pd_b = rs.PartitionsDefinition.static_(["b", "c", "d"])

        @rs.Asset.from_multi(
            output_defs=[
                rs.AssetDef("x", io_handler=handler, partitions_def=pd_a),
                rs.AssetDef("y", io_handler=handler, partitions_def=pd_b),
            ],
        )
        def multi():
            yield rs.Output(value=10, output_name="x")
            yield rs.Output(value=20, output_name="y")

        repo = rs.CodeRepository(
            assets=[multi],
            default_executor=rs.Executor.in_process(),
        )

        # "b" is in the intersection — both outputs can execute
        result = repo.materialize(partition_key=rs.PartitionKey.single("b"))
        assert result.success
        assert handler.store["x"] == 10
        assert handler.store["y"] == 20

    def test_per_output_partitions_def_disjoint_individual(self):
        """Outputs with overlapping per-output defs can be materialized individually on disjoint keys."""
        handler = CapturingHandler()
        pd_a = rs.PartitionsDefinition.static_(["a", "b", "c"])
        pd_b = rs.PartitionsDefinition.static_(["b", "c", "d"])

        @rs.Asset.from_multi(
            output_defs=[
                rs.AssetDef("x", io_handler=handler, partitions_def=pd_a),
                rs.AssetDef("y", io_handler=handler, partitions_def=pd_b),
            ],
        )
        def multi():
            yield rs.Output(value=1, output_name="x")
            yield rs.Output(value=2, output_name="y")

        repo = rs.CodeRepository(
            assets=[multi],
            default_executor=rs.Executor.in_process(),
        )

        # "a" is only in x's partition space — materialize x individually
        result = repo.materialize(["x"], partition_key=rs.PartitionKey.single("a"))
        assert result.success
        assert handler.store["x"] == 1
        assert "y" not in handler.store
        assert handler.output_contexts[-1].asset_name == "x"
        assert handler.output_contexts[-1].partition.key == rs.PartitionKey.single("a")

        # "d" is only in y's partition space — materialize y individually
        result = repo.materialize(["y"], partition_key=rs.PartitionKey.single("d"))
        assert result.success
        assert handler.store["y"] == 2
        assert handler.output_contexts[-1].asset_name == "y"
        assert handler.output_contexts[-1].partition.key == rs.PartitionKey.single("d")

    def test_per_output_no_overlap_rejected(self):
        """Per-output partitions_defs with zero overlap are rejected."""
        import pytest

        pd_a = rs.PartitionsDefinition.static_(["a", "b"])
        pd_b = rs.PartitionsDefinition.static_(["x", "y"])

        with pytest.raises(ValueError, match="no overlapping partition keys"):

            @rs.Asset.from_multi(
                output_defs=[
                    rs.AssetDef("x", partitions_def=pd_a),
                    rs.AssetDef("y", partitions_def=pd_b),
                ],
            )
            def multi():
                yield rs.Output(value=1, output_name="x")
                yield rs.Output(value=2, output_name="y")

    def test_per_output_mixed_variant_rejected(self):
        """Per-output partitions_defs with different variant types are rejected."""
        import pytest
        from datetime import datetime

        pd_static = rs.PartitionsDefinition.static_(["a", "b"])
        pd_time = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))

        with pytest.raises(ValueError, match="same partition type"):

            @rs.Asset.from_multi(
                output_defs=[
                    rs.AssetDef("x", partitions_def=pd_static),
                    rs.AssetDef("y", partitions_def=pd_time),
                ],
            )
            def multi():
                yield rs.Output(value=1, output_name="x")
                yield rs.Output(value=2, output_name="y")


# ===========================================================================
# MultiAsset + Deps (AssetDef.input / AssetDef.dep)
# ===========================================================================


class TestMultiAssetDeps:
    """Tests for multi-asset deps parameter with AssetDef.input() and AssetDef.dep()."""

    def test_input_dep_with_partition_mapping(self):
        """AssetDef.input() partition_mapping is applied when loading upstream."""
        handler = CapturingHandler()
        pd_multi = rs.PartitionsDefinition.static_(["a", "b"])
        pd_source = rs.PartitionsDefinition.static_(["1", "2", "3"])

        @rs.Asset(io_handler=handler, partitions_def=pd_source)
        def source(context: rs.AssetExecutionContext) -> int:
            return {"1": 10, "2": 20, "3": 30}[context.partition_key]

        @rs.Asset.from_multi(
            partitions_def=pd_multi,
            output_defs=[
                rs.AssetDef("x", io_handler=handler),
                rs.AssetDef("y", io_handler=handler),
            ],
            deps=[
                rs.AssetDef.input(
                    "source",
                    partition_mapping=rs.PartitionMapping.static_({"a": "1", "b": "2"}),
                ),
            ],
        )
        def multi(source: int):
            yield rs.Output(value=source + 1, output_name="x")
            yield rs.Output(value=source + 2, output_name="y")

        repo = rs.CodeRepository(
            assets=[source, multi],
            default_executor=rs.Executor.in_process(),
        )

        # Pre-materialize source partitions
        repo.materialize(["source"], partition_key=rs.PartitionKey.single("1"))
        repo.materialize(["source"], partition_key=rs.PartitionKey.single("2"))

        # "a" → source partition "1" (value 10), x=11, y=12
        result = repo.materialize(["x", "y"], partition_key=rs.PartitionKey.single("a"))
        assert result.success
        assert handler.store["x"] == 11
        assert handler.store["y"] == 12

        # "b" → source partition "2" (value 20), x=21, y=22
        result = repo.materialize(["x", "y"], partition_key=rs.PartitionKey.single("b"))
        assert result.success
        assert handler.store["x"] == 21
        assert handler.store["y"] == 22

    def test_dep_only_creates_graph_edge(self):
        """AssetDef.dep() adds a graph edge without loading data."""
        handler = CapturingHandler()

        @rs.Asset(io_handler=handler)
        def trigger() -> int:
            return 1

        @rs.Asset.from_multi(
            output_defs=[
                rs.AssetDef("x", io_handler=handler),
            ],
            deps=[
                rs.AssetDef.dep("trigger"),
            ],
        )
        def multi():
            yield rs.Output(value=99, output_name="x")

        repo = rs.CodeRepository(
            assets=[trigger, multi],
            default_executor=rs.Executor.in_process(),
        )

        # trigger must execute before multi (graph edge), but multi doesn't load it
        result = repo.materialize()
        assert result.success
        assert handler.store["x"] == 99
        assert handler.store["trigger"] == 1

        # Verify trigger was materialized before multi via output context order
        output_names = [ctx.asset_name for ctx in handler.output_contexts]
        assert output_names.index("trigger") < output_names.index("x")

    def test_input_dep_must_match_fn_param(self):
        """AssetDef.input() with a name not in fn params raises ValueError."""
        import pytest

        with pytest.raises(ValueError, match="does not match any parameter"):

            @rs.Asset.from_multi(
                output_defs=[rs.AssetDef("x")],
                deps=[rs.AssetDef.input("nonexistent")],
            )
            def multi():
                yield rs.Output(value=1, output_name="x")

    def test_dep_only_does_not_need_fn_param(self):
        """AssetDef.dep() names don't need to match function parameters."""
        handler = CapturingHandler()

        @rs.Asset(io_handler=handler)
        def upstream() -> int:
            return 1

        # "upstream" is not a parameter of multi — this should be fine
        @rs.Asset.from_multi(
            output_defs=[rs.AssetDef("x", io_handler=handler)],
            deps=[rs.AssetDef.dep("upstream")],
        )
        def multi():
            yield rs.Output(value=5, output_name="x")

        repo = rs.CodeRepository(
            assets=[upstream, multi],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize()
        assert result.success
        assert handler.store["x"] == 5

    def test_mixed_input_and_dep(self):
        """Both AssetDef.input() and AssetDef.dep() can be used together."""
        handler = CapturingHandler()
        pd = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset(io_handler=handler, partitions_def=pd)
        def data_source(context: rs.AssetExecutionContext) -> int:
            return {"a": 10, "b": 100}[context.partition_key]

        @rs.Asset(io_handler=handler)
        def lineage_dep() -> int:
            return 0

        @rs.Asset.from_multi(
            partitions_def=pd,
            output_defs=[rs.AssetDef("out", io_handler=handler)],
            deps=[
                rs.AssetDef.input("data_source"),
                rs.AssetDef.dep("lineage_dep"),
            ],
        )
        def multi(data_source: int):
            yield rs.Output(value=data_source * 2, output_name="out")

        repo = rs.CodeRepository(
            assets=[data_source, lineage_dep, multi],
            default_executor=rs.Executor.in_process(),
        )
        # Materialize data_source for both partitions
        repo.materialize(["data_source"], partition_key=rs.PartitionKey.single("a"))
        repo.materialize(["data_source"], partition_key=rs.PartitionKey.single("b"))

        result = repo.materialize(["out"], partition_key=rs.PartitionKey.single("a"))
        assert result.success
        assert handler.store["out"] == 20

        result = repo.materialize(["out"], partition_key=rs.PartitionKey.single("b"))
        assert result.success
        assert handler.store["out"] == 200

    def test_input_dep_io_handler_override(self):
        """AssetDef.input() io_handler overrides the upstream's handler for loading."""
        output_handler = CapturingHandler()

        class CustomLoader(rs.BaseIOHandler):
            """Always returns a fixed value, ignoring what was stored."""

            def handle_output(self, context, obj):
                pass

            def load_input(self, context):
                return 999

        @rs.Asset(io_handler=output_handler)
        def source() -> int:
            return 1

        @rs.Asset.from_multi(
            output_defs=[rs.AssetDef("x", io_handler=output_handler)],
            deps=[
                rs.AssetDef.input("source", io_handler=CustomLoader()),
            ],
        )
        def multi(source: int):
            yield rs.Output(value=source, output_name="x")

        repo = rs.CodeRepository(
            assets=[source, multi],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize()
        assert result.success
        # source produced 1, but CustomLoader returns 999 for load_input
        assert output_handler.store["x"] == 999

    def test_input_dep_io_handler_only_affects_specified_input(self):
        """io_handler override on one input does not affect other inputs."""
        handler = CapturingHandler()

        class OverrideLoader(rs.BaseIOHandler):
            def handle_output(self, context, obj):
                pass

            def load_input(self, context):
                return -1

        @rs.Asset(io_handler=handler)
        def dep_a() -> int:
            return 10

        @rs.Asset(io_handler=handler)
        def dep_b() -> int:
            return 20

        @rs.Asset.from_multi(
            output_defs=[rs.AssetDef("out", io_handler=handler)],
            deps=[
                rs.AssetDef.input("dep_a", io_handler=OverrideLoader()),
                rs.AssetDef.input("dep_b"),  # no override — uses dep_b's own handler
            ],
        )
        def multi(dep_a: int, dep_b: int):
            yield rs.Output(value=(dep_a, dep_b), output_name="out")

        repo = rs.CodeRepository(
            assets=[dep_a, dep_b, multi],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize()
        assert result.success
        # dep_a loaded via OverrideLoader → -1, dep_b loaded normally → 20
        assert handler.store["out"] == (-1, 20)

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

        @rs.Asset.from_multi(
            output_defs=[rs.AssetDef("x", io_handler=handler)],
            deps=[
                rs.AssetDef.input("source", metadata={"columns": "a,b,c"}),
            ],
        )
        def multi(source: int):
            yield rs.Output(value=source, output_name="x")

        repo = rs.CodeRepository(
            assets=[source, multi],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize()
        assert result.success

        # InputContext should have the override metadata, not the upstream's
        assert captured_metadata["source"] == {"columns": "a,b,c"}

    def test_input_dep_metadata_does_not_leak_to_other_inputs(self):
        """Metadata override on one input does not affect other inputs."""
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
        def dep_a() -> int:
            return 1

        @rs.Asset(io_handler=handler, metadata={"format": "csv"})
        def dep_b() -> int:
            return 2

        @rs.Asset.from_multi(
            output_defs=[rs.AssetDef("out", io_handler=handler)],
            deps=[
                rs.AssetDef.input("dep_a", metadata={"columns": "x,y"}),
                rs.AssetDef.input("dep_b"),  # no metadata override
            ],
        )
        def multi(dep_a: int, dep_b: int):
            yield rs.Output(value=dep_a + dep_b, output_name="out")

        repo = rs.CodeRepository(
            assets=[dep_a, dep_b, multi],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize()
        assert result.success

        # dep_a gets override metadata
        assert captured_metadata["dep_a"] == {"columns": "x,y"}
        # dep_b gets its own upstream metadata (no override)
        assert captured_metadata["dep_b"] == {"format": "csv"}

    def test_input_dep_metadata_does_not_mutate_upstream(self):
        """Metadata override is scoped to the multi-asset's load context only."""
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

        @rs.Asset.from_multi(
            output_defs=[rs.AssetDef("x", io_handler=handler)],
            deps=[
                rs.AssetDef.input("source", metadata={"columns": "a,b"}),
            ],
        )
        def multi(source: int):
            yield rs.Output(value=source + 1, output_name="x")

        # A separate downstream that also depends on source, no override
        @rs.Asset(io_handler=handler)
        def other_consumer(source: int) -> int:
            return source + 100

        repo = rs.CodeRepository(
            assets=[source, multi, other_consumer],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize()
        assert result.success

        # multi's load of source sees the override
        assert captured_metadata["x"]["source"] == {"columns": "a,b"}
        # other_consumer's load of source sees the original upstream metadata
        assert captured_metadata["other_consumer"]["source"] == {"format": "parquet"}


# ===========================================================================
# GraphAsset + Partitions
# ===========================================================================


class TestGraphAssetPartitioned:
    """Tests for partitioned graph-asset execution."""

    def test_graph_asset_with_partitions_def(self):
        """Graph asset with partitions_def propagates partition context through execution."""
        pd = rs.PartitionsDefinition.static_(["a", "b", "c"])
        pk = rs.PartitionKey.single("a")

        @rs.Task
        def step_one() -> int:
            return 10

        @rs.Task
        def step_two(step_one: int) -> int:
            return step_one * 2

        @rs.Asset.from_graph(name="pipeline", partitions_def=pd)
        def pipeline():
            a = step_one()
            return step_two(a)

        repo = rs.CodeRepository(
            assets=[pipeline],
            tasks=[step_one, step_two],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize(partition_key=pk)

        assert result.success
        assert repo.load_node("pipeline/step_one", partition_key=pk) == 10
        assert repo.load_node("pipeline/step_two", partition_key=pk) == 20
        assert repo.load_node("pipeline", partition_key=pk) == 20

    def test_graph_asset_partition_context_in_io(self):
        """Graph asset's IO handler receives partition context in output context."""
        handler = CapturingHandler()
        pd = rs.PartitionsDefinition.static_(["x", "y"])

        @rs.Task
        def compute() -> int:
            return 42

        @rs.Asset.from_graph(name="pipe", partitions_def=pd, io_handler=handler)
        def pipe():
            return compute()

        repo = rs.CodeRepository(
            assets=[pipe],
            tasks=[compute],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize(partition_key=rs.PartitionKey.single("x"))

        assert result.success

        # The graph asset output should have partition context
        pipe_ctx = [ctx for ctx in handler.output_contexts if ctx.asset_name == "pipe"]
        assert len(pipe_ctx) == 1
        assert pipe_ctx[0].partition is not None
        assert pipe_ctx[0].partition.key == rs.PartitionKey.single("x")

    def test_graph_asset_with_external_dep_partitioned(self):
        """Partitioned graph asset depending on partitioned upstream asset."""
        handler = CapturingHandler()
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
            assets=[source, pipeline],
            tasks=[double],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize(partition_key=rs.PartitionKey.single("a"))

        assert result.success
        assert handler.store["source"] == 5
        assert handler.store["pipeline"] == 10

    def test_graph_asset_downstream_partitioned(self):
        """Partitioned downstream asset depending on a partitioned graph asset."""
        handler = CapturingHandler()
        pd = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Task
        def step() -> int:
            return 42

        @rs.Asset.from_graph(name="graph_pipe", partitions_def=pd, io_handler=handler)
        def graph_pipe():
            return step()

        @rs.Asset(io_handler=handler, partitions_def=pd)
        def consumer(graph_pipe: int) -> str:
            return f"got {graph_pipe}"

        repo = rs.CodeRepository(
            assets=[graph_pipe, consumer],
            tasks=[step],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize(partition_key=rs.PartitionKey.single("a"))

        assert result.success
        assert handler.store["graph_pipe"] == 42
        assert handler.store["consumer"] == "got 42"

        consumer_ctx = [
            ctx for ctx in handler.output_contexts if ctx.asset_name == "consumer"
        ]
        assert len(consumer_ctx) == 1
        assert consumer_ctx[0].partition is not None
        assert consumer_ctx[0].partition.key == rs.PartitionKey.single("a")

    def test_graph_asset_partition_with_storage_events(self, storage):
        """Partitioned graph asset records partition key in storage events."""
        pd = rs.PartitionsDefinition.static_(["a", "b"])
        pk = rs.PartitionKey.single("a")

        @rs.Task
        def compute() -> int:
            return 99

        @rs.Asset.from_graph(name="pipe", partitions_def=pd)
        def pipe():
            return compute()

        repo = rs.CodeRepository(
            assets=[pipe],
            tasks=[compute],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)
        result = repo.materialize(partition_key=pk)

        assert result.success
        assert repo.load_node("pipe", partition_key=pk) == 99

        events = storage.get_events_for_asset("pipe")
        mat_events = [e for e in events if e.event_type == "Materialization"]
        assert len(mat_events) == 1
        assert mat_events[0].partition_key == pk

    def test_graph_asset_daily_partitions(self):
        """Graph asset with daily time partitions executes correctly."""
        from datetime import datetime

        pd = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))
        pk = rs.PartitionKey.single("2024-01-15")

        @rs.Task
        def transform() -> str:
            return "processed"

        @rs.Asset.from_graph(name="daily_pipe", partitions_def=pd)
        def daily_pipe():
            return transform()

        repo = rs.CodeRepository(
            assets=[daily_pipe],
            tasks=[transform],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize(partition_key=pk)

        assert result.success
        assert repo.load_node("daily_pipe/transform", partition_key=pk) == "processed"
        assert repo.load_node("daily_pipe", partition_key=pk) == "processed"

    def test_graph_asset_multi_partition(self):
        """Graph asset with multi-dimensional partitions."""
        from datetime import datetime

        pd = rs.PartitionsDefinition.multi(
            {
                "date": rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1)),
                "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            }
        )
        pk = rs.PartitionKey.multi({"date": "2024-01-15", "region": "us"})

        @rs.Task
        def compute() -> int:
            return 1

        @rs.Asset.from_graph(name="multi_part_pipe", partitions_def=pd)
        def multi_part_pipe():
            return compute()

        repo = rs.CodeRepository(
            assets=[multi_part_pipe],
            tasks=[compute],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize(partition_key=pk)

        assert result.success
        assert repo.load_node("multi_part_pipe/compute", partition_key=pk) == 1
        assert repo.load_node("multi_part_pipe", partition_key=pk) == 1


# ===========================================================================
# GraphAsset + Partition Mappings (partition_mappings)
# ===========================================================================


class TestGraphAssetPartitionMappings:
    """Tests for graph assets with different partition defs on external deps."""

    def test_graph_asset_same_partition_def_no_mapping_needed(self):
        """When graph and external asset share same partition def, identity mapping is inferred."""
        handler = CapturingHandler()
        pd = rs.PartitionsDefinition.static_(["a", "b", "c"])

        @rs.Asset(io_handler=handler, partitions_def=pd)
        def source(context: rs.AssetExecutionContext) -> int:
            return {"a": 7, "b": 8, "c": 9}[context.partition_key]

        @rs.Task
        def process(source: int) -> int:
            return source * 3

        @rs.Asset.from_graph(
            name="pipeline",
            partitions_def=pd,
            io_handler=handler,
        )
        def pipeline(source: int):
            return process(source)

        repo = rs.CodeRepository(
            assets=[source, pipeline],
            tasks=[process],
            default_executor=rs.Executor.in_process(),
        )

        pk = rs.PartitionKey.single("b")
        result = repo.materialize(partition_key=pk)

        assert result.success
        assert handler.store["source"] == 8
        assert handler.store["pipeline"] == 24

    def test_graph_asset_with_static_mapping(self):
        """Graph asset maps between different static partition defs using Static mapping.

        The upstream source must be pre-materialized since it has a different partition space.
        """
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

        # Pre-materialize source partitions "1" and "2"
        repo.materialize(["source"], partition_key=rs.PartitionKey.single("1"))
        repo.materialize(["source"], partition_key=rs.PartitionKey.single("2"))

        # "x" → source partition "1" (value 10), transform adds 1 → 11
        result = repo.materialize(
            ["pipeline"], partition_key=rs.PartitionKey.single("x")
        )
        assert result.success
        assert handler.store["pipeline"] == 11

        # "y" → source partition "2" (value 20), transform adds 1 → 21
        result = repo.materialize(
            ["pipeline"], partition_key=rs.PartitionKey.single("y")
        )
        assert result.success
        assert handler.store["pipeline"] == 21

    def test_graph_asset_mapping_propagates_to_multiple_tasks(self):
        """All internal tasks in a graph inherit the graph's partition_mappings."""
        handler = CapturingHandler()
        pd_graph = rs.PartitionsDefinition.static_(["a", "b"])
        pd_source = rs.PartitionsDefinition.static_(["1", "2", "3"])

        @rs.Asset(io_handler=handler, partitions_def=pd_source)
        def ext_data(context: rs.AssetExecutionContext) -> int:
            return {"1": 5, "2": 50, "3": 500}[context.partition_key]

        @rs.Task
        def step_one(ext_data: int) -> int:
            return ext_data * 10

        @rs.Task
        def step_two(step_one: int) -> int:
            return step_one + 1

        @rs.Asset.from_graph(
            name="pipeline",
            partitions_def=pd_graph,
            io_handler=handler,
            deps=[
                rs.AssetDef.input(
                    "ext_data",
                    partition_mapping=rs.PartitionMapping.static_(
                        mapping={"a": "1", "b": "2"}
                    ),
                ),
            ],
        )
        def pipeline(ext_data: int):
            a = step_one(ext_data)
            return step_two(a)

        repo = rs.CodeRepository(
            assets=[ext_data, pipeline],
            tasks=[step_one, step_two],
            default_executor=rs.Executor.in_process(),
        )

        # Pre-materialize ext_data partitions
        repo.materialize(["ext_data"], partition_key=rs.PartitionKey.single("1"))
        repo.materialize(["ext_data"], partition_key=rs.PartitionKey.single("2"))

        # "a" → ext_data partition "1" (value 5), step_one=50, step_two=51
        pk_a = rs.PartitionKey.single("a")
        result = repo.materialize(["pipeline"], partition_key=pk_a)
        assert result.success
        assert repo.load_node("pipeline/step_one", partition_key=pk_a) == 50
        assert repo.load_node("pipeline/step_two", partition_key=pk_a) == 51
        assert repo.load_node("pipeline", partition_key=pk_a) == 51

        # "b" → ext_data partition "2" (value 50), step_one=500, step_two=501
        pk_b = rs.PartitionKey.single("b")
        result = repo.materialize(["pipeline"], partition_key=pk_b)
        assert result.success
        assert repo.load_node("pipeline/step_one", partition_key=pk_b) == 500
        assert repo.load_node("pipeline/step_two", partition_key=pk_b) == 501
        assert repo.load_node("pipeline", partition_key=pk_b) == 501

    def test_graph_asset_mapping_validation_rejects_bad_mapping(self):
        """Invalid partition mapping on graph asset is rejected at resolve time."""
        import pytest

        pd_graph = rs.PartitionsDefinition.static_(["a", "b"])
        pd_source = rs.PartitionsDefinition.static_(["1", "2"])

        @rs.Asset(partitions_def=pd_source)
        def source() -> int:
            return 1

        @rs.Task
        def t(source: int) -> int:
            return source

        @rs.Asset.from_graph(
            name="pipe",
            partitions_def=pd_graph,
            deps=[
                # TimeWindow mapping on Static partitions → should fail
                rs.AssetDef.input(
                    "source",
                    partition_mapping=rs.PartitionMapping.time_window(offset=1),
                ),
            ],
        )
        def pipe(source: int):
            return t(source)

        repo = rs.CodeRepository(
            assets=[source, pipe],
            tasks=[t],
            default_executor=rs.Executor.in_process(),
        )
        with pytest.raises(Exception, match="TimeWindow"):
            repo.materialize(["pipe"], partition_key=rs.PartitionKey.single("a"))


# ===========================================================================
# GraphAsset + Partition context in tasks/assets
# ===========================================================================


class TestGraphAssetPartitionContext:
    """Tests that partition context is accessible inside tasks and assets within graph assets."""

    def test_task_receives_partition_context(self):
        """Internal task with TaskExecutionContext sees the graph's partition key."""
        pd = rs.PartitionsDefinition.static_(["a", "b"])
        pk = rs.PartitionKey.single("a")
        captured = {}

        @rs.Task
        def step(context: rs.TaskExecutionContext) -> int:
            captured["has_key"] = context.has_partition_key
            captured["partition"] = context.partition
            captured["partition_key"] = context.partition_key
            return 42

        @rs.Asset.from_graph(name="pipe", partitions_def=pd)
        def pipe():
            return step()

        repo = rs.CodeRepository(
            assets=[pipe],
            tasks=[step],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize(partition_key=pk)

        assert result.success
        assert captured["has_key"] is True
        assert captured["partition"].key == pk
        assert captured["partition_key"] == "a"
        assert isinstance(
            captured["partition"].definition, rs.PartitionsDefinition.Static
        )

    def test_chained_tasks_all_receive_partition_context(self):
        """Multiple chained tasks each see the partition key from the graph asset."""
        pd = rs.PartitionsDefinition.static_(["x", "y"])
        pk = rs.PartitionKey.single("y")
        captured_keys = []

        @rs.Task
        def first(context: rs.TaskExecutionContext) -> int:
            captured_keys.append(context.partition_key)
            return 1

        @rs.Task
        def second(context: rs.TaskExecutionContext, first: int) -> int:
            captured_keys.append(context.partition_key)
            return first + 1

        @rs.Asset.from_graph(name="chain", partitions_def=pd)
        def chain():
            a = first()
            return second(a)

        repo = rs.CodeRepository(
            assets=[chain],
            tasks=[first, second],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize(partition_key=pk)

        assert result.success
        assert captured_keys == ["y", "y"]

    def test_upstream_asset_receives_partition_via_asset_execution_context(self):
        """Upstream asset with AssetExecutionContext sees the partition key during execution."""
        pd = rs.PartitionsDefinition.static_(["a", "b"])
        pk = rs.PartitionKey.single("b")
        captured_asset = {}
        captured_task = {}

        @rs.Asset(partitions_def=pd)
        def source(context: rs.AssetExecutionContext) -> int:
            captured_asset["pk"] = context.partition_key
            captured_asset["has_key"] = context.has_partition_key
            captured_asset["partition"] = context.partition
            return 5

        @rs.Task
        def double(context: rs.TaskExecutionContext, source: int) -> int:
            captured_task["pk"] = context.partition_key
            return source * 2

        @rs.Asset.from_graph(name="pipe", partitions_def=pd)
        def pipe(source: int):
            return double(source)

        repo = rs.CodeRepository(
            assets=[source, pipe],
            tasks=[double],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize(partition_key=pk)

        assert result.success
        # Upstream asset sees partition key "b" via AssetExecutionContext
        assert captured_asset["has_key"] is True
        assert captured_asset["pk"] == "b"
        assert captured_asset["partition"].key == pk
        assert isinstance(
            captured_asset["partition"].definition, rs.PartitionsDefinition.Static
        )
        # Internal task also sees same partition key via TaskExecutionContext
        assert captured_task["pk"] == "b"

    def test_asset_execution_context_partition_fields(self):
        """AssetExecutionContext on upstream asset exposes all partition fields correctly."""
        from datetime import datetime

        pd = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))
        pk = rs.PartitionKey.single("2024-06-15")
        captured = {}

        @rs.Asset(partitions_def=pd)
        def daily_source(context: rs.AssetExecutionContext) -> str:
            captured["pk"] = context.partition_key
            captured["has_key"] = context.has_partition_key
            captured["tw"] = context.partition_time_window
            captured["asset_name"] = context.asset_name
            return "data"

        @rs.Task
        def transform(context: rs.TaskExecutionContext, daily_source: str) -> str:
            captured["task_pk"] = context.partition_key
            return daily_source + "_transformed"

        @rs.Asset.from_graph(name="pipe", partitions_def=pd)
        def pipe(daily_source: str):
            return transform(daily_source)

        repo = rs.CodeRepository(
            assets=[daily_source, pipe],
            tasks=[transform],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize(partition_key=pk)

        assert result.success
        # AssetExecutionContext on upstream asset
        assert captured["has_key"] is True
        assert captured["pk"] == "2024-06-15"
        assert captured["asset_name"] == "daily_source"
        assert captured["tw"] is not None
        assert captured["tw"][0] == datetime(2024, 6, 15)
        assert captured["tw"][1] == datetime(2024, 6, 16)
        # TaskExecutionContext on internal task
        assert captured["task_pk"] == "2024-06-15"

    def test_task_and_asset_context_with_static_mapping(self):
        """Both AssetExecutionContext and TaskExecutionContext see correct keys with mapping."""
        pd_graph = rs.PartitionsDefinition.static_(["x", "y"])
        pd_source = rs.PartitionsDefinition.static_(["1", "2"])
        pk = rs.PartitionKey.single("x")
        captured = {}

        handler = CapturingHandler()

        @rs.Asset(io_handler=handler, partitions_def=pd_source)
        def mapped_source(context: rs.AssetExecutionContext) -> int:
            captured["source_pk"] = context.partition_key
            return 50

        @rs.Task
        def process(context: rs.TaskExecutionContext, mapped_source: int) -> int:
            captured["task_pk"] = context.partition_key
            return mapped_source + 1

        @rs.Asset.from_graph(
            name="pipe",
            partitions_def=pd_graph,
            io_handler=handler,
            deps=[
                rs.AssetDef.input(
                    "mapped_source",
                    partition_mapping=rs.PartitionMapping.static_(
                        mapping={"x": "1", "y": "2"}
                    ),
                ),
            ],
        )
        def pipe(mapped_source: int):
            return process(mapped_source)

        repo = rs.CodeRepository(
            assets=[mapped_source, pipe],
            tasks=[process],
            default_executor=rs.Executor.in_process(),
        )

        # Pre-materialize source — it sees its own partition key "1"
        repo.materialize(["mapped_source"], partition_key=rs.PartitionKey.single("1"))
        assert captured["source_pk"] == "1"

        # Materialize pipeline — task sees graph's partition key "x"
        result = repo.materialize(["pipe"], partition_key=pk)
        assert result.success
        assert captured["task_pk"] == "x"
        assert handler.store["pipe"] == 51

    def test_multi_partition_context_in_asset_and_task(self):
        """Multi-partitioned graph: both asset and task contexts see multi partition key."""
        from datetime import datetime

        pd = rs.PartitionsDefinition.multi(
            {
                "date": rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1)),
                "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            }
        )
        pk = rs.PartitionKey.multi({"date": "2024-01-10", "region": "eu"})
        captured = {}

        @rs.Asset(partitions_def=pd)
        def source(context: rs.AssetExecutionContext) -> int:
            captured["asset_partition"] = context.partition
            return 1

        @rs.Task
        def compute(context: rs.TaskExecutionContext, source: int) -> int:
            captured["task_partition"] = context.partition
            return source + 1

        @rs.Asset.from_graph(name="multi_pipe", partitions_def=pd)
        def multi_pipe(source: int):
            return compute(source)

        repo = rs.CodeRepository(
            assets=[source, multi_pipe],
            tasks=[compute],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize(partition_key=pk)

        assert result.success
        # Asset sees multi partition key
        assert captured["asset_partition"].key == pk
        assert isinstance(
            captured["asset_partition"].definition, rs.PartitionsDefinition.Multi
        )
        # Task sees same multi partition key
        assert captured["task_partition"].key == pk
        assert isinstance(
            captured["task_partition"].definition, rs.PartitionsDefinition.Multi
        )

    def test_io_handler_output_context_has_partition(self):
        """IO handler on graph asset and its internal tasks receives partition in contexts."""
        pd = rs.PartitionsDefinition.static_(["a", "b"])
        pk = rs.PartitionKey.single("a")
        handler = CapturingHandler()

        @rs.Task
        def step() -> int:
            return 7

        @rs.Asset.from_graph(
            name="pipe",
            partitions_def=pd,
            io_handler=handler,
            node_io_handler=handler,
        )
        def pipe():
            return step()

        repo = rs.CodeRepository(
            assets=[pipe],
            tasks=[step],
            default_executor=rs.Executor.in_process(),
        )
        result = repo.materialize(partition_key=pk)

        assert result.success

        # Both internal task and graph asset output should have partition in context
        ctx_by_name = {ctx.asset_name: ctx for ctx in handler.output_contexts}
        assert "pipe/step" in ctx_by_name
        assert ctx_by_name["pipe/step"].partition is not None
        assert ctx_by_name["pipe/step"].partition.key == pk
        assert "pipe" in ctx_by_name
        assert ctx_by_name["pipe"].partition is not None
        assert ctx_by_name["pipe"].partition.key == pk


# ===========================================================================
# Job execution with partition key
# ===========================================================================


class TestJobPartitionedMultiGraph:
    """Tests for Job.execute with partitioned multi/graph assets."""

    def test_job_execute_partitioned_multi_asset(self):
        """Job.execute passes partition_key to partitioned multi-asset."""
        handler = CapturingHandler()
        pd = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset.from_multi(
            output_defs=[
                rs.AssetDef("x", io_handler=handler, partitions_def=pd),
                rs.AssetDef("y", io_handler=handler, partitions_def=pd),
            ],
        )
        def multi():
            yield rs.Output(value=1, output_name="x")
            yield rs.Output(value=2, output_name="y")

        repo = rs.CodeRepository(
            assets=[multi],
            jobs=[
                rs.Job(
                    name="test_job",
                    assets=[multi],
                    executor=rs.Executor.in_process(),
                )
            ],
        )
        repo.get_job("test_job").execute(partition_key=rs.PartitionKey.single("a"))

        assert handler.store["x"] == 1
        assert handler.store["y"] == 2

        out_names = {ctx.asset_name: ctx for ctx in handler.output_contexts}
        assert out_names["x"].partition is not None
        assert out_names["x"].partition.key == rs.PartitionKey.single("a")

    def test_job_execute_partitioned_graph_asset(self):
        """Job.execute passes partition_key to partitioned graph-asset."""
        pd = rs.PartitionsDefinition.static_(["a", "b"])
        pk = rs.PartitionKey.single("a")

        @rs.Task
        def compute() -> int:
            return 99

        @rs.Asset.from_graph(name="pipe", partitions_def=pd)
        def pipe():
            return compute()

        repo = rs.CodeRepository(
            assets=[pipe],
            tasks=[compute],
            jobs=[
                rs.Job(
                    name="test_job",
                    assets=[pipe],
                    executor=rs.Executor.in_process(),
                )
            ],
        )
        repo.get_job("test_job").execute(partition_key=pk)

        assert repo.load_node("pipe/compute", partition_key=pk) == 99
        assert repo.load_node("pipe", partition_key=pk) == 99
