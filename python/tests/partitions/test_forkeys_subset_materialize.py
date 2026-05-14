"""Tests for ForKeys and Subset partition mappings.

Covers:
- Materialization (ForKeys explicit keys, ForKeys range, Subset disjoint, mixed)
- Asset type coverage (regular, MultiAsset, graph asset, ExternalAsset)
- Cross-executor (in_process + parallel)
- Backfill tests
"""

import asyncio
from datetime import datetime
from typing import Any

import rivers as rs

STATIC_KEYS = ["a", "b", "c"]


# ---------------------------------------------------------------------------
# 6b: ForKeys — explicit keys
# ---------------------------------------------------------------------------


class TestForKeysExplicitKeys:
    """ForKeys with explicit PartitionKey selectors."""

    def test_matching_partition_loads_upstream(self, executor_env):
        """When downstream partition matches a ForKeys selector, upstream is loaded."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset(io_handler=handler)
        def source_a() -> dict:
            return {"origin": "a", "value": 10}

        @rs.Asset(
            io_handler=handler,
            partitions_def=pd,
            deps=[
                rs.AssetDef.input(
                    "source_a",
                    partition_mapping=rs.PartitionMapping.for_keys(
                        [rs.PartitionKey.single("a")]
                    ),
                ),
            ],
        )
        def downstream(context: rs.AssetExecutionContext, source_a: Any) -> dict:
            return {"key": context.partition_key, "got": source_a}

        repo = rs.CodeRepository(
            assets=[source_a, downstream], default_executor=executor
        )
        repo.materialize(["source_a"])

        result = repo.materialize(
            ["downstream"], partition_key=rs.PartitionKey.single("a")
        )
        assert result.success
        loaded = repo.load_node("downstream", partition_key=rs.PartitionKey.single("a"))
        assert loaded["got"] == {"origin": "a", "value": 10}

    def test_non_matching_partition_gets_none(self, executor_env):
        """When downstream partition doesn't match ForKeys selector, parameter is None."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset(io_handler=handler)
        def source_a() -> dict:
            return {"origin": "a"}

        @rs.Asset(
            io_handler=handler,
            partitions_def=pd,
            deps=[
                rs.AssetDef.input(
                    "source_a",
                    partition_mapping=rs.PartitionMapping.for_keys(
                        [rs.PartitionKey.single("a")]
                    ),
                ),
            ],
        )
        def downstream(source_a: Any) -> str:
            return "loaded" if source_a is not None else "skipped"

        repo = rs.CodeRepository(
            assets=[source_a, downstream], default_executor=executor
        )
        repo.materialize(["source_a"])

        result = repo.materialize(
            ["downstream"], partition_key=rs.PartitionKey.single("b")
        )
        assert result.success
        assert (
            repo.load_node("downstream", partition_key=rs.PartitionKey.single("b"))
            == "skipped"
        )

    def test_two_forkeys_upstreams(self, executor_env):
        """Two unpartitioned upstreams each map to different downstream partitions."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset(io_handler=handler)
        def src_a() -> int:
            return 10

        @rs.Asset(io_handler=handler)
        def src_b() -> int:
            return 20

        @rs.Asset(
            io_handler=handler,
            partitions_def=pd,
            deps=[
                rs.AssetDef.input(
                    "src_a",
                    partition_mapping=rs.PartitionMapping.for_keys(
                        [rs.PartitionKey.single("a")]
                    ),
                ),
                rs.AssetDef.input(
                    "src_b",
                    partition_mapping=rs.PartitionMapping.for_keys(
                        [rs.PartitionKey.single("b")]
                    ),
                ),
            ],
        )
        def merged(src_a: Any, src_b: Any) -> int:
            if src_a is not None:
                return src_a
            return src_b

        repo = rs.CodeRepository(
            assets=[src_a, src_b, merged], default_executor=executor
        )
        repo.materialize(["src_a"])
        repo.materialize(["src_b"])

        # partition "a" -> src_a=10, src_b=None -> 10
        repo.materialize(["merged"], partition_key=rs.PartitionKey.single("a"))
        assert repo.load_node("merged", partition_key=rs.PartitionKey.single("a")) == 10

        # partition "b" -> src_a=None, src_b=20 -> 20
        repo.materialize(["merged"], partition_key=rs.PartitionKey.single("b"))
        assert repo.load_node("merged", partition_key=rs.PartitionKey.single("b")) == 20

    def test_forkeys_multiple_selectors(self, executor_env):
        """ForKeys with multiple key selectors -- upstream loaded for any match."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd = rs.PartitionsDefinition.static_(STATIC_KEYS)

        @rs.Asset(io_handler=handler)
        def shared() -> int:
            return 42

        @rs.Asset(
            io_handler=handler,
            partitions_def=pd,
            deps=[
                rs.AssetDef.input(
                    "shared",
                    partition_mapping=rs.PartitionMapping.for_keys(
                        [
                            rs.PartitionKey.single("a"),
                            rs.PartitionKey.single("b"),
                        ]
                    ),
                ),
            ],
        )
        def downstream(shared: Any) -> str:
            return "loaded" if shared is not None else "skipped"

        repo = rs.CodeRepository(assets=[shared, downstream], default_executor=executor)
        repo.materialize(["shared"])

        for key, expected in [("a", "loaded"), ("b", "loaded"), ("c", "skipped")]:
            repo.materialize(["downstream"], partition_key=rs.PartitionKey.single(key))
            assert (
                repo.load_node("downstream", partition_key=rs.PartitionKey.single(key))
                == expected
            )


# ---------------------------------------------------------------------------
# 6b: ForKeys — range matching
# ---------------------------------------------------------------------------


class TestForKeysRange:
    """ForKeys with PartitionKeyRange selectors."""

    def test_range_matching_static(self, executor_env):
        """ForKeys with a range selector on static partitions uses positional ordering."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd = rs.PartitionsDefinition.static_(["p", "q", "r", "s", "t"])

        @rs.Asset(io_handler=handler)
        def source() -> int:
            return 99

        @rs.Asset(
            io_handler=handler,
            partitions_def=pd,
            deps=[
                rs.AssetDef.input(
                    "source",
                    partition_mapping=rs.PartitionMapping.for_keys(
                        [rs.PartitionKeyRange.single(from_key="q", to_key="s")]
                    ),
                ),
            ],
        )
        def downstream(source: Any) -> str:
            return "loaded" if source is not None else "skipped"

        repo = rs.CodeRepository(assets=[source, downstream], default_executor=executor)
        repo.materialize(["source"])

        # q, r, s are in range (positional indices 1-3); p and t are outside
        for key, expected in [
            ("p", "skipped"),
            ("q", "loaded"),
            ("r", "loaded"),
            ("s", "loaded"),
            ("t", "skipped"),
        ]:
            repo.materialize(["downstream"], partition_key=rs.PartitionKey.single(key))
            assert (
                repo.load_node("downstream", partition_key=rs.PartitionKey.single(key))
                == expected
            )

    def test_range_matching_time_window(self, executor_env):
        """ForKeys with a range on daily time-window partitions."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd = rs.PartitionsDefinition.daily(
            start=datetime(2024, 1, 1), end=datetime(2024, 1, 7)
        )

        @rs.Asset(io_handler=handler)
        def legacy() -> str:
            return "legacy_data"

        @rs.Asset(
            io_handler=handler,
            partitions_def=pd,
            deps=[
                rs.AssetDef.input(
                    "legacy",
                    partition_mapping=rs.PartitionMapping.for_keys(
                        [
                            rs.PartitionKeyRange.single(
                                from_key="2024-01-02", to_key="2024-01-04"
                            )
                        ]
                    ),
                ),
            ],
        )
        def downstream(legacy: Any) -> str:
            return "loaded" if legacy is not None else "skipped"

        repo = rs.CodeRepository(assets=[legacy, downstream], default_executor=executor)
        repo.materialize(["legacy"])

        # 01-02, 01-03, 01-04 are in range; 01-01 and 01-05 are outside
        for day, expected in [
            ("2024-01-01", "skipped"),
            ("2024-01-02", "loaded"),
            ("2024-01-03", "loaded"),
            ("2024-01-04", "loaded"),
            ("2024-01-05", "skipped"),
        ]:
            repo.materialize(["downstream"], partition_key=rs.PartitionKey.single(day))
            assert (
                repo.load_node("downstream", partition_key=rs.PartitionKey.single(day))
                == expected
            )


# ---------------------------------------------------------------------------
# 6b: Subset — disjoint upstreams (partition union)
# ---------------------------------------------------------------------------


class TestSubsetDisjoint:
    """Subset mapping with disjoint upstream key sets."""

    def test_subset_skip_and_load(self, executor_env):
        """Subset: upstream key exists -> load, missing key -> None."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd_up = rs.PartitionsDefinition.static_(["a", "b"])
        pd_down = rs.PartitionsDefinition.static_(STATIC_KEYS)

        @rs.Asset(io_handler=handler, partitions_def=pd_up)
        def region_ab(context: rs.AssetExecutionContext) -> int:
            return {"a": 1, "b": 2}[context.partition_key]

        @rs.Asset(
            io_handler=handler,
            partitions_def=pd_down,
            deps=[
                rs.AssetDef.input(
                    "region_ab",
                    partition_mapping=rs.PartitionMapping.subset(),
                ),
            ],
        )
        def downstream(region_ab: Any) -> str:
            if region_ab is not None:
                return f"got:{region_ab}"
            return "skipped"

        repo = rs.CodeRepository(
            assets=[region_ab, downstream], default_executor=executor
        )

        repo.materialize(["region_ab"], partition_key=rs.PartitionKey.single("a"))
        repo.materialize(["region_ab"], partition_key=rs.PartitionKey.single("b"))

        for key, expected in [("a", "got:1"), ("b", "got:2"), ("c", "skipped")]:
            repo.materialize(["downstream"], partition_key=rs.PartitionKey.single(key))
            assert (
                repo.load_node("downstream", partition_key=rs.PartitionKey.single(key))
                == expected
            )

    def test_subset_partition_union(self, executor_env):
        """Two disjoint Subset upstreams cover all downstream partitions."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd_ab = rs.PartitionsDefinition.static_(["a", "b"])
        pd_c = rs.PartitionsDefinition.static_(["c"])
        pd_down = rs.PartitionsDefinition.static_(STATIC_KEYS)

        @rs.Asset(io_handler=handler, partitions_def=pd_ab)
        def region_ab(context: rs.AssetExecutionContext) -> int:
            return {"a": 10, "b": 20}[context.partition_key]

        @rs.Asset(io_handler=handler, partitions_def=pd_c)
        def region_c(context: rs.AssetExecutionContext) -> int:
            return 30

        @rs.Asset(
            io_handler=handler,
            partitions_def=pd_down,
            deps=[
                rs.AssetDef.input(
                    "region_ab",
                    partition_mapping=rs.PartitionMapping.subset(),
                ),
                rs.AssetDef.input(
                    "region_c",
                    partition_mapping=rs.PartitionMapping.subset(),
                ),
            ],
        )
        def all_regions(region_ab: Any, region_c: Any) -> int:
            if region_ab is not None:
                return region_ab
            return region_c

        repo = rs.CodeRepository(
            assets=[region_ab, region_c, all_regions], default_executor=executor
        )

        repo.materialize(["region_ab"], partition_key=rs.PartitionKey.single("a"))
        repo.materialize(["region_ab"], partition_key=rs.PartitionKey.single("b"))
        repo.materialize(["region_c"], partition_key=rs.PartitionKey.single("c"))

        for key, expected in [("a", 10), ("b", 20), ("c", 30)]:
            repo.materialize(["all_regions"], partition_key=rs.PartitionKey.single(key))
            assert (
                repo.load_node("all_regions", partition_key=rs.PartitionKey.single(key))
                == expected
            )


# ---------------------------------------------------------------------------
# 6b: Mixed ForKeys + Subset
# ---------------------------------------------------------------------------


class TestMixedForKeysSubset:
    """Combined ForKeys and Subset mappings on the same downstream."""

    def test_mixed_unpartitioned_and_partitioned(self, executor_env):
        """Unpartitioned upstream (ForKeys) + partitioned upstream (Subset)."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd_new = rs.PartitionsDefinition.static_(["b", "c"])
        pd_down = rs.PartitionsDefinition.static_(STATIC_KEYS)

        @rs.Asset(io_handler=handler)
        def legacy() -> str:
            return "legacy_data"

        @rs.Asset(io_handler=handler, partitions_def=pd_new)
        def new_source(context: rs.AssetExecutionContext) -> str:
            return f"new_{context.partition_key}"

        @rs.Asset(
            io_handler=handler,
            partitions_def=pd_down,
            deps=[
                rs.AssetDef.input(
                    "legacy",
                    partition_mapping=rs.PartitionMapping.for_keys(
                        [rs.PartitionKey.single("a")]
                    ),
                ),
                rs.AssetDef.input(
                    "new_source",
                    partition_mapping=rs.PartitionMapping.subset(),
                ),
            ],
        )
        def unified(legacy: Any, new_source: Any) -> str:
            if legacy is not None:
                return legacy
            return new_source

        repo = rs.CodeRepository(
            assets=[legacy, new_source, unified], default_executor=executor
        )

        repo.materialize(["legacy"])
        repo.materialize(["new_source"], partition_key=rs.PartitionKey.single("b"))
        repo.materialize(["new_source"], partition_key=rs.PartitionKey.single("c"))

        for key, expected in [
            ("a", "legacy_data"),
            ("b", "new_b"),
            ("c", "new_c"),
        ]:
            repo.materialize(["unified"], partition_key=rs.PartitionKey.single(key))
            assert (
                repo.load_node("unified", partition_key=rs.PartitionKey.single(key))
                == expected
            )


# ---------------------------------------------------------------------------
# 6c: Asset type coverage — MultiAsset
# ---------------------------------------------------------------------------


class TestMultiAssetForKeysSubset:
    """ForKeys/Subset on MultiAsset downstream."""

    def test_multi_asset_with_forkeys(self, executor_env):
        """MultiAsset downstream with ForKeys dependency."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset(io_handler=handler)
        def source() -> int:
            return 42

        @rs.Asset.from_multi(
            partitions_def=pd,
            output_defs=[rs.AssetDef("out", io_handler=handler)],
            deps=[
                rs.AssetDef.input(
                    "source",
                    partition_mapping=rs.PartitionMapping.for_keys(
                        [rs.PartitionKey.single("a")]
                    ),
                ),
            ],
        )
        def multi(source: Any):
            val = source if source is not None else -1
            yield rs.Output(value=val, output_name="out")

        repo = rs.CodeRepository(assets=[source, multi], default_executor=executor)
        repo.materialize(["source"])

        repo.materialize(["out"], partition_key=rs.PartitionKey.single("a"))
        assert repo.load_node("out", partition_key=rs.PartitionKey.single("a")) == 42

        repo.materialize(["out"], partition_key=rs.PartitionKey.single("b"))
        assert repo.load_node("out", partition_key=rs.PartitionKey.single("b")) == -1

    def test_multi_asset_with_subset(self, executor_env):
        """MultiAsset downstream with Subset dependency."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd_up = rs.PartitionsDefinition.static_(["a"])
        pd_down = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset(io_handler=handler, partitions_def=pd_up)
        def source(context: rs.AssetExecutionContext) -> int:
            return 100

        @rs.Asset.from_multi(
            partitions_def=pd_down,
            output_defs=[rs.AssetDef("out", io_handler=handler)],
            deps=[
                rs.AssetDef.input(
                    "source",
                    partition_mapping=rs.PartitionMapping.subset(),
                ),
            ],
        )
        def multi(source: Any):
            val = source if source is not None else 0
            yield rs.Output(value=val, output_name="out")

        repo = rs.CodeRepository(assets=[source, multi], default_executor=executor)
        repo.materialize(["source"], partition_key=rs.PartitionKey.single("a"))

        repo.materialize(["out"], partition_key=rs.PartitionKey.single("a"))
        assert repo.load_node("out", partition_key=rs.PartitionKey.single("a")) == 100

        repo.materialize(["out"], partition_key=rs.PartitionKey.single("b"))
        assert repo.load_node("out", partition_key=rs.PartitionKey.single("b")) == 0


# ---------------------------------------------------------------------------
# 6c: Asset type coverage — Graph asset
# ---------------------------------------------------------------------------


class TestGraphAssetForKeysSubset:
    """ForKeys/Subset on graph asset downstream."""

    def test_graph_asset_with_forkeys(self, executor_env):
        """Graph asset downstream with ForKeys upstream."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset(io_handler=handler)
        def source() -> int:
            return 7

        @rs.Task
        def process(source: Any) -> int:
            if source is not None:
                return source * 3
            return -1

        @rs.Asset.from_graph(
            name="pipeline",
            partitions_def=pd,
            io_handler=handler,
            deps=[
                rs.AssetDef.input(
                    "source",
                    partition_mapping=rs.PartitionMapping.for_keys(
                        [rs.PartitionKey.single("a")]
                    ),
                ),
            ],
        )
        def pipeline(source: Any):
            return process(source)

        repo = rs.CodeRepository(
            assets=[source, pipeline],
            tasks=[process],
            default_executor=executor,
        )
        repo.materialize(["source"])

        repo.materialize(["pipeline"], partition_key=rs.PartitionKey.single("a"))
        assert (
            repo.load_node("pipeline", partition_key=rs.PartitionKey.single("a")) == 21
        )

        repo.materialize(["pipeline"], partition_key=rs.PartitionKey.single("b"))
        assert (
            repo.load_node("pipeline", partition_key=rs.PartitionKey.single("b")) == -1
        )

    def test_graph_asset_with_subset(self, executor_env):
        """Graph asset downstream with Subset upstream."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd_up = rs.PartitionsDefinition.static_(["a"])
        pd_down = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset(io_handler=handler, partitions_def=pd_up)
        def source(context: rs.AssetExecutionContext) -> int:
            return 5

        @rs.Task
        def double(source: Any) -> int:
            if source is not None:
                return source * 2
            return 0

        @rs.Asset.from_graph(
            name="pipe",
            partitions_def=pd_down,
            io_handler=handler,
            deps=[
                rs.AssetDef.input(
                    "source",
                    partition_mapping=rs.PartitionMapping.subset(),
                ),
            ],
        )
        def pipe(source: Any):
            return double(source)

        repo = rs.CodeRepository(
            assets=[source, pipe],
            tasks=[double],
            default_executor=executor,
        )
        repo.materialize(["source"], partition_key=rs.PartitionKey.single("a"))

        repo.materialize(["pipe"], partition_key=rs.PartitionKey.single("a"))
        assert repo.load_node("pipe", partition_key=rs.PartitionKey.single("a")) == 10

        repo.materialize(["pipe"], partition_key=rs.PartitionKey.single("b"))
        assert repo.load_node("pipe", partition_key=rs.PartitionKey.single("b")) == 0


# ---------------------------------------------------------------------------
# 6c: Asset type coverage — ExternalAsset with Subset
# ---------------------------------------------------------------------------


class TestExternalAssetSubset:
    """ExternalAsset as upstream with Subset mapping."""

    def test_external_asset_subset(self, executor_env):
        """ExternalAsset (partitioned) as upstream with Subset mapping."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd_ext = rs.PartitionsDefinition.static_(["a", "b"])
        pd_down = rs.PartitionsDefinition.static_(STATIC_KEYS)

        ext = rs.Asset.external(
            name="ext_source",
            io_handler=handler,
            partitions_def=pd_ext,
        )

        # Pre-populate via the handler (external assets have no compute fn)
        class FakeCtx:
            def __init__(self, name, pk):
                self.asset_name = name
                self.partition = type("P", (), {"key": pk})()

            def add_output_metadata(self, meta):
                pass

        for pk, val in [("a", 100), ("b", 200)]:
            handler.handle_output(
                FakeCtx("ext_source", rs.PartitionKey.single(pk)), val
            )

        @rs.Asset(
            io_handler=handler,
            partitions_def=pd_down,
            deps=[
                rs.AssetDef.input(
                    "ext_source",
                    partition_mapping=rs.PartitionMapping.subset(),
                ),
            ],
        )
        def consumer(ext_source: Any) -> str:
            if ext_source is not None:
                return f"got:{ext_source}"
            return "skipped"

        repo = rs.CodeRepository(assets=[ext, consumer], default_executor=executor)

        for key, expected in [("a", "got:100"), ("b", "got:200"), ("c", "skipped")]:
            repo.materialize(["consumer"], partition_key=rs.PartitionKey.single(key))
            assert (
                repo.load_node("consumer", partition_key=rs.PartitionKey.single(key))
                == expected
            )


# ---------------------------------------------------------------------------
# 6c: Asset type coverage — async variants
# ---------------------------------------------------------------------------


class TestAsyncForKeysSubset:
    """Async asset functions with ForKeys/Subset."""

    def test_async_forkeys(self, executor_env):
        """Async downstream with ForKeys dependency."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset(io_handler=handler)
        async def source() -> int:
            await asyncio.sleep(0)
            return 55

        @rs.Asset(
            io_handler=handler,
            partitions_def=pd,
            deps=[
                rs.AssetDef.input(
                    "source",
                    partition_mapping=rs.PartitionMapping.for_keys(
                        [rs.PartitionKey.single("a")]
                    ),
                ),
            ],
        )
        async def downstream(source: Any) -> int:
            await asyncio.sleep(0)
            return source if source is not None else -1

        repo = rs.CodeRepository(assets=[source, downstream], default_executor=executor)
        repo.materialize(["source"])

        repo.materialize(["downstream"], partition_key=rs.PartitionKey.single("a"))
        assert (
            repo.load_node("downstream", partition_key=rs.PartitionKey.single("a"))
            == 55
        )

        repo.materialize(["downstream"], partition_key=rs.PartitionKey.single("b"))
        assert (
            repo.load_node("downstream", partition_key=rs.PartitionKey.single("b"))
            == -1
        )

    def test_async_subset(self, executor_env):
        """Async downstream with Subset dependency."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd_up = rs.PartitionsDefinition.static_(["a"])
        pd_down = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset(io_handler=handler, partitions_def=pd_up)
        async def source(context: rs.AssetExecutionContext) -> int:
            await asyncio.sleep(0)
            return 77

        @rs.Asset(
            io_handler=handler,
            partitions_def=pd_down,
            deps=[
                rs.AssetDef.input(
                    "source",
                    partition_mapping=rs.PartitionMapping.subset(),
                ),
            ],
        )
        async def downstream(source: Any) -> int:
            await asyncio.sleep(0)
            return source if source is not None else 0

        repo = rs.CodeRepository(assets=[source, downstream], default_executor=executor)
        repo.materialize(["source"], partition_key=rs.PartitionKey.single("a"))

        repo.materialize(["downstream"], partition_key=rs.PartitionKey.single("a"))
        assert (
            repo.load_node("downstream", partition_key=rs.PartitionKey.single("a"))
            == 77
        )

        repo.materialize(["downstream"], partition_key=rs.PartitionKey.single("b"))
        assert (
            repo.load_node("downstream", partition_key=rs.PartitionKey.single("b")) == 0
        )


# ---------------------------------------------------------------------------
# 6e: Backfill tests
# ---------------------------------------------------------------------------


class TestBackfillForKeysSubset:
    """Backfill tests for ForKeys and Subset across multiple partition keys."""

    def test_backfill_forkeys(self, executor_env):
        """Backfill across all partitions -- ForKeys loads/skips correctly per key."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd = rs.PartitionsDefinition.static_(STATIC_KEYS)

        @rs.Asset(io_handler=handler)
        def source() -> int:
            return 999

        @rs.Asset(
            io_handler=handler,
            partitions_def=pd,
            deps=[
                rs.AssetDef.input(
                    "source",
                    partition_mapping=rs.PartitionMapping.for_keys(
                        [rs.PartitionKey.single("a"), rs.PartitionKey.single("b")]
                    ),
                ),
            ],
        )
        def downstream(source: Any) -> int:
            return source if source is not None else -1

        repo = rs.CodeRepository(assets=[source, downstream], default_executor=executor)
        repo.materialize(["source"])

        result = repo.backfill(
            selection=["downstream"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
                rs.PartitionKey.single("c"),
            ],
        )
        assert result.num_partitions == 3
        assert result.completed == 3
        assert result.failed == 0
        assert result.status == "CompletedSuccess"

        assert (
            repo.load_node("downstream", partition_key=rs.PartitionKey.single("a"))
            == 999
        )
        assert (
            repo.load_node("downstream", partition_key=rs.PartitionKey.single("b"))
            == 999
        )
        assert (
            repo.load_node("downstream", partition_key=rs.PartitionKey.single("c"))
            == -1
        )

    def test_backfill_subset(self, executor_env):
        """Backfill across all partitions -- Subset loads/skips correctly per key."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd_up = rs.PartitionsDefinition.static_(["a", "b"])
        pd_down = rs.PartitionsDefinition.static_(STATIC_KEYS)

        @rs.Asset(io_handler=handler, partitions_def=pd_up)
        def source(context: rs.AssetExecutionContext) -> int:
            return {"a": 10, "b": 20}[context.partition_key]

        @rs.Asset(
            io_handler=handler,
            partitions_def=pd_down,
            deps=[
                rs.AssetDef.input(
                    "source",
                    partition_mapping=rs.PartitionMapping.subset(),
                ),
            ],
        )
        def downstream(source: Any) -> int:
            return source if source is not None else 0

        repo = rs.CodeRepository(assets=[source, downstream], default_executor=executor)

        repo.backfill(
            selection=["source"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
            ],
        )

        result = repo.backfill(
            selection=["downstream"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
                rs.PartitionKey.single("c"),
            ],
        )
        assert result.num_partitions == 3
        assert result.completed == 3
        assert result.failed == 0
        assert result.status == "CompletedSuccess"

        assert (
            repo.load_node("downstream", partition_key=rs.PartitionKey.single("a"))
            == 10
        )
        assert (
            repo.load_node("downstream", partition_key=rs.PartitionKey.single("b"))
            == 20
        )
        assert (
            repo.load_node("downstream", partition_key=rs.PartitionKey.single("c")) == 0
        )

    def test_backfill_mixed_forkeys_subset(self, executor_env):
        """Backfill with mixed ForKeys + Subset on same downstream."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd_new = rs.PartitionsDefinition.static_(["b", "c"])
        pd_down = rs.PartitionsDefinition.static_(STATIC_KEYS)

        @rs.Asset(io_handler=handler)
        def legacy() -> str:
            return "old"

        @rs.Asset(io_handler=handler, partitions_def=pd_new)
        def new_src(context: rs.AssetExecutionContext) -> str:
            return f"new_{context.partition_key}"

        @rs.Asset(
            io_handler=handler,
            partitions_def=pd_down,
            deps=[
                rs.AssetDef.input(
                    "legacy",
                    partition_mapping=rs.PartitionMapping.for_keys(
                        [rs.PartitionKey.single("a")]
                    ),
                ),
                rs.AssetDef.input(
                    "new_src",
                    partition_mapping=rs.PartitionMapping.subset(),
                ),
            ],
        )
        def unified(legacy: Any, new_src: Any) -> str:
            if legacy is not None:
                return legacy
            if new_src is not None:
                return new_src
            return "nothing"

        repo = rs.CodeRepository(
            assets=[legacy, new_src, unified], default_executor=executor
        )

        repo.materialize(["legacy"])
        repo.backfill(
            selection=["new_src"],
            partition_keys=[
                rs.PartitionKey.single("b"),
                rs.PartitionKey.single("c"),
            ],
        )

        result = repo.backfill(
            selection=["unified"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
                rs.PartitionKey.single("c"),
            ],
        )
        assert result.num_partitions == 3
        assert result.completed == 3
        assert result.status == "CompletedSuccess"

        assert (
            repo.load_node("unified", partition_key=rs.PartitionKey.single("a"))
            == "old"
        )
        assert (
            repo.load_node("unified", partition_key=rs.PartitionKey.single("b"))
            == "new_b"
        )
        assert (
            repo.load_node("unified", partition_key=rs.PartitionKey.single("c"))
            == "new_c"
        )
