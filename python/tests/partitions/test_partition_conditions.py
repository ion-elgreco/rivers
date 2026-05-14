"""Tests for partition-aware condition evaluation.

Covers all combinations of PartitionsDefinition types × PartitionMapping types,
verifying that materialization correctly tracks per-partition state in storage
via get_materialized_partitions().

NOTE: An io_handler (e.g. InMemoryIOHandler) is required for Materialization
events to be emitted. Without one, only StepStart/StepSuccess events are stored
and get_materialized_partitions() returns nothing.
"""

from datetime import datetime

import rivers as rs

from _helpers import TrackingHandler, make_repo

DAILY_START = datetime(2024, 1, 1)
DAILY_END = datetime(2024, 1, 5)
STATIC_KEYS = ["a", "b", "c"]
IO = rs.InMemoryIOHandler


# ===========================================================================
# PartitionsDefinition × materialized partition tracking
# ===========================================================================


class TestStaticPartitionsMaterialization:
    """Static partitions: materialize individual keys, verify storage state."""

    def test_materialize_single_partition(self):
        parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def my_asset(context: rs.AssetExecutionContext) -> int:
            return 1

        repo = make_repo([my_asset])
        storage = repo.storage

        result = repo.materialize(
            ["my_asset"], partition_key=rs.PartitionKey.single("a")
        )
        assert result.success

        mat = storage.get_materialized_partitions("my_asset")
        assert rs.PartitionKey.single("a") in mat
        assert rs.PartitionKey.single("b") not in mat
        assert rs.PartitionKey.single("c") not in mat

    def test_materialize_all_partitions_sequentially(self):
        parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def my_asset() -> int:
            return 1

        repo = make_repo([my_asset])
        storage = repo.storage

        for key in STATIC_KEYS:
            result = repo.materialize(
                ["my_asset"], partition_key=rs.PartitionKey.single(key)
            )
            assert result.success

        mat = storage.get_materialized_partitions("my_asset")
        assert len(mat) == len(STATIC_KEYS)
        for key in STATIC_KEYS:
            assert rs.PartitionKey.single(key) in mat

    def test_rematerialize_same_partition(self):
        parts = rs.PartitionsDefinition.static_(STATIC_KEYS)
        call_count = 0

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def my_asset() -> int:
            nonlocal call_count
            call_count += 1
            return call_count

        repo = make_repo([my_asset])
        storage = repo.storage

        repo.materialize(["my_asset"], partition_key=rs.PartitionKey.single("a"))
        repo.materialize(["my_asset"], partition_key=rs.PartitionKey.single("a"))

        mat = storage.get_materialized_partitions("my_asset")
        assert len(mat) == 1  # deduplicated (GROUP BY)
        assert rs.PartitionKey.single("a") in mat
        assert rs.PartitionKey.single("b") not in mat
        assert call_count == 2  # but the function ran twice


class TestDailyPartitionsMaterialization:
    """TimeWindow (daily) partitions: materialize by date key."""

    def test_materialize_single_day(self):
        parts = rs.PartitionsDefinition.daily(start=DAILY_START, end=DAILY_END)

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def daily_asset(context: rs.AssetExecutionContext) -> str:
            return f"data-{context.partition_key}"

        repo = make_repo([daily_asset])
        storage = repo.storage

        result = repo.materialize(
            ["daily_asset"], partition_key=rs.PartitionKey.single("2024-01-02")
        )
        assert result.success

        mat = storage.get_materialized_partitions("daily_asset")
        assert rs.PartitionKey.single("2024-01-02") in mat
        assert rs.PartitionKey.single("2024-01-01") not in mat
        assert rs.PartitionKey.single("2024-01-03") not in mat

    def test_materialize_multiple_days(self):
        parts = rs.PartitionsDefinition.daily(start=DAILY_START, end=DAILY_END)

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def daily_asset() -> int:
            return 1

        repo = make_repo([daily_asset])
        storage = repo.storage

        days = ["2024-01-01", "2024-01-03"]
        for day in days:
            repo.materialize(["daily_asset"], partition_key=rs.PartitionKey.single(day))

        mat = storage.get_materialized_partitions("daily_asset")
        assert len(mat) == 2
        for day in days:
            assert rs.PartitionKey.single(day) in mat
        assert rs.PartitionKey.single("2024-01-02") not in mat


class TestHourlyPartitionsMaterialization:
    """TimeWindow (hourly) partitions."""

    def test_materialize_hourly(self):
        parts = rs.PartitionsDefinition.hourly(
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 1, 4),
        )

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def hourly_asset() -> int:
            return 1

        repo = make_repo([hourly_asset])
        storage = repo.storage

        result = repo.materialize(
            ["hourly_asset"], partition_key=rs.PartitionKey.single("2024-01-01T02:00")
        )
        assert result.success

        mat = storage.get_materialized_partitions("hourly_asset")
        assert rs.PartitionKey.single("2024-01-01T02:00") in mat
        assert rs.PartitionKey.single("2024-01-01T00:00") not in mat
        assert rs.PartitionKey.single("2024-01-01T01:00") not in mat


class TestMultiPartitionsMaterialization:
    """Multi-dimensional partitions (static × static, static × daily)."""

    def test_static_x_static(self):
        parts = rs.PartitionsDefinition.multi(
            {
                "region": rs.PartitionsDefinition.static_(["us", "eu"]),
                "env": rs.PartitionsDefinition.static_(["prod", "dev"]),
            }
        )

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def multi_asset(context: rs.AssetExecutionContext) -> int:
            return 1

        repo = make_repo([multi_asset])
        storage = repo.storage

        key = rs.PartitionKey.multi({"region": "us", "env": "prod"})
        result = repo.materialize(["multi_asset"], partition_key=key)
        assert result.success

        mat = storage.get_materialized_partitions("multi_asset")
        assert len(mat) == 1
        assert key in mat
        assert rs.PartitionKey.multi({"region": "eu", "env": "dev"}) not in mat

    def test_static_x_daily(self):
        parts = rs.PartitionsDefinition.multi(
            {
                "date": rs.PartitionsDefinition.daily(start=DAILY_START, end=DAILY_END),
                "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            }
        )

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def multi_asset() -> int:
            return 1

        repo = make_repo([multi_asset])
        storage = repo.storage

        key = rs.PartitionKey.multi({"date": "2024-01-01", "region": "eu"})
        result = repo.materialize(["multi_asset"], partition_key=key)
        assert result.success

        mat = storage.get_materialized_partitions("multi_asset")
        assert len(mat) == 1
        assert key in mat
        assert rs.PartitionKey.multi({"date": "2024-01-02", "region": "us"}) not in mat


class TestDynamicPartitionsMaterialization:
    """Dynamic partitions: storage-managed keys."""

    def test_dynamic_materialize_and_track(self, storage):
        dyn = rs.PartitionsDefinition.dynamic("items")

        @rs.Asset(partitions_def=dyn, io_handler=IO())
        def dynamic_asset() -> int:
            return 1

        repo = make_repo([dynamic_asset], storage=storage)
        storage.add_dynamic_partitions("items", ["item_a", "item_b", "item_c"])

        for k in ["item_a", "item_c"]:
            result = repo.materialize(
                ["dynamic_asset"], partition_key=rs.PartitionKey.single(k)
            )
            assert result.success

        mat = storage.get_materialized_partitions("dynamic_asset")
        assert rs.PartitionKey.single("item_a") in mat
        assert rs.PartitionKey.single("item_c") in mat
        assert rs.PartitionKey.single("item_b") not in mat


# ===========================================================================
# PartitionMapping × pipeline materialization
# ===========================================================================


class TestIdentityMappingPipeline:
    """Identity mapping: both assets have same partition definition."""

    def test_static_identity(self):
        parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def upstream() -> int:
            return 1

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def downstream(upstream: int) -> int:
            return upstream + 1

        repo = make_repo([upstream, downstream])
        storage = repo.storage

        result = repo.materialize(
            ["upstream", "downstream"], partition_key=rs.PartitionKey.single("b")
        )
        assert result.success
        assert rs.PartitionKey.single("b") in storage.get_materialized_partitions(
            "upstream"
        )
        assert rs.PartitionKey.single("b") in storage.get_materialized_partitions(
            "downstream"
        )
        assert rs.PartitionKey.single("a") not in storage.get_materialized_partitions(
            "upstream"
        )
        assert rs.PartitionKey.single("c") not in storage.get_materialized_partitions(
            "downstream"
        )

    def test_daily_identity(self):
        parts = rs.PartitionsDefinition.daily(start=DAILY_START, end=DAILY_END)

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def raw() -> int:
            return 1

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def processed(raw: int) -> int:
            return raw * 2

        repo = make_repo([raw, processed])
        storage = repo.storage

        repo.materialize(
            ["raw", "processed"], partition_key=rs.PartitionKey.single("2024-01-02")
        )

        assert rs.PartitionKey.single(
            "2024-01-02"
        ) in storage.get_materialized_partitions("raw")
        assert rs.PartitionKey.single(
            "2024-01-02"
        ) in storage.get_materialized_partitions("processed")
        assert rs.PartitionKey.single(
            "2024-01-01"
        ) not in storage.get_materialized_partitions("raw")
        assert rs.PartitionKey.single(
            "2024-01-03"
        ) not in storage.get_materialized_partitions("processed")

    def test_multi_identity(self):
        parts = rs.PartitionsDefinition.multi(
            {
                "date": rs.PartitionsDefinition.daily(start=DAILY_START, end=DAILY_END),
                "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            }
        )

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def source() -> int:
            return 1

        @rs.Asset(
            partitions_def=parts,
            io_handler=IO(),
            deps=[
                rs.AssetDef.input(
                    "source",
                    partition_mapping=rs.PartitionMapping.multi(
                        {
                            "date": rs.PartitionMapping.identity(),
                            "region": rs.PartitionMapping.identity(),
                        }
                    ),
                )
            ],
        )
        def sink(source: int) -> int:
            return source + 1

        repo = make_repo([source, sink])
        storage = repo.storage

        key = rs.PartitionKey.multi({"date": "2024-01-01", "region": "us"})
        result = repo.materialize(["source", "sink"], partition_key=key)
        assert result.success
        assert len(storage.get_materialized_partitions("source")) == 1
        assert len(storage.get_materialized_partitions("sink")) == 1
        assert key in storage.get_materialized_partitions("source")
        assert key in storage.get_materialized_partitions("sink")
        other = rs.PartitionKey.multi({"date": "2024-01-02", "region": "eu"})
        assert other not in storage.get_materialized_partitions("source")
        assert other not in storage.get_materialized_partitions("sink")


class TestStaticMappingPipeline:
    """Static key mapping between differently-keyed partitions."""

    def test_static_key_remap(self):
        up_parts = rs.PartitionsDefinition.static_(["x", "y", "z"])
        down_parts = rs.PartitionsDefinition.static_(["a", "b", "c"])
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
                    partition_mapping=rs.PartitionMapping.static_(
                        {"a": "x", "b": "y", "c": "z"}
                    ),
                )
            ],
        )
        def downstream(upstream: int) -> int:
            return upstream + 1

        repo = make_repo([upstream, downstream])
        storage = repo.storage

        repo.materialize(["upstream"], partition_key=rs.PartitionKey.single("x"))
        repo.materialize(["downstream"], partition_key=rs.PartitionKey.single("a"))

        assert rs.PartitionKey.single("x") in storage.get_materialized_partitions(
            "upstream"
        )
        assert rs.PartitionKey.single("y") not in storage.get_materialized_partitions(
            "upstream"
        )
        assert rs.PartitionKey.single("a") in storage.get_materialized_partitions(
            "downstream"
        )
        assert rs.PartitionKey.single("b") not in storage.get_materialized_partitions(
            "downstream"
        )

        # Verify IO handler saw the mapped key
        load_partitions = handler.load_input_partitions
        assert any(
            p is not None and p.key == rs.PartitionKey.single("x")
            for p in load_partitions
        )


class TestAllPartitionsMappingPipeline:
    """AllPartitions mapping: cross-type partition dependencies."""

    def test_partitioned_to_partitioned_all(self):
        """Use TrackingHandler to avoid InMemoryIOHandler cross-partition load issues."""
        static_parts = rs.PartitionsDefinition.static_(["a", "b"])
        daily_parts = rs.PartitionsDefinition.daily(start=DAILY_START, end=DAILY_END)
        handler = TrackingHandler(default_load_value=1)

        @rs.Asset(partitions_def=daily_parts, io_handler=handler)
        def daily_source() -> int:
            return 1

        @rs.Asset(
            partitions_def=static_parts,
            io_handler=handler,
            deps=[
                rs.AssetDef.input(
                    "daily_source",
                    partition_mapping=rs.PartitionMapping.all_partitions(),
                )
            ],
        )
        def static_sink(daily_source: int) -> int:
            return daily_source + 1

        repo = make_repo([daily_source, static_sink])
        storage = repo.storage

        repo.materialize(
            ["daily_source"], partition_key=rs.PartitionKey.single("2024-01-01")
        )
        repo.materialize(
            ["daily_source"], partition_key=rs.PartitionKey.single("2024-01-02")
        )

        assert len(storage.get_materialized_partitions("daily_source")) == 2

        repo.materialize(["static_sink"], partition_key=rs.PartitionKey.single("a"))
        assert rs.PartitionKey.single("a") in storage.get_materialized_partitions(
            "static_sink"
        )
        assert rs.PartitionKey.single("b") not in storage.get_materialized_partitions(
            "static_sink"
        )


class TestSpecificPartitionsMappingPipeline:
    """SpecificPartitions mapping: unpartitioned downstream loads specific upstream keys."""

    def test_unpartitioned_loads_specific(self):
        """Use TrackingHandler since InMemoryIOHandler can't resolve multi-key loads."""
        parts = rs.PartitionsDefinition.static_(["a", "b", "c", "d"])
        handler = TrackingHandler(default_load_value=1)

        @rs.Asset(partitions_def=parts, io_handler=handler)
        def partitioned_source() -> int:
            return 1

        @rs.Asset(
            io_handler=handler,
            deps=[
                rs.AssetDef.input(
                    "partitioned_source",
                    partition_mapping=rs.PartitionMapping.specific_partitions(
                        ["a", "c"]
                    ),
                )
            ],
        )
        def aggregated(partitioned_source: int) -> int:
            return partitioned_source + 1

        repo = make_repo([partitioned_source, aggregated])
        storage = repo.storage

        for k in ["a", "c"]:
            repo.materialize(
                ["partitioned_source"], partition_key=rs.PartitionKey.single(k)
            )

        result = repo.materialize(["aggregated"])
        assert result.success
        mat = storage.get_materialized_partitions("partitioned_source")
        assert len(mat) == 2
        assert rs.PartitionKey.single("a") in mat
        assert rs.PartitionKey.single("c") in mat
        assert rs.PartitionKey.single("b") not in mat
        assert rs.PartitionKey.single("d") not in mat


class TestMultiToSingleMappingPipeline:
    """MultiToSingle mapping: extract one dimension from multi-partitioned upstream."""

    def test_multi_to_single_extracts_dimension(self):
        """Use TrackingHandler since InMemoryIOHandler can't resolve multi→single loads."""
        multi_parts = rs.PartitionsDefinition.multi(
            {
                "date": rs.PartitionsDefinition.daily(start=DAILY_START, end=DAILY_END),
                "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            }
        )
        date_parts = rs.PartitionsDefinition.daily(start=DAILY_START, end=DAILY_END)
        handler = TrackingHandler(default_load_value=1)

        @rs.Asset(partitions_def=multi_parts, io_handler=handler)
        def multi_source() -> int:
            return 1

        @rs.Asset(
            partitions_def=date_parts,
            io_handler=handler,
            deps=[
                rs.AssetDef.input(
                    "multi_source",
                    partition_mapping=rs.PartitionMapping.multi_to_single("date"),
                )
            ],
        )
        def date_sink(multi_source: int) -> int:
            return multi_source + 1

        repo = make_repo([multi_source, date_sink])
        storage = repo.storage

        key = rs.PartitionKey.multi({"date": "2024-01-01", "region": "us"})
        repo.materialize(["multi_source"], partition_key=key)
        assert len(storage.get_materialized_partitions("multi_source")) == 1

        result = repo.materialize(
            ["date_sink"], partition_key=rs.PartitionKey.single("2024-01-01")
        )
        assert result.success
        assert rs.PartitionKey.single(
            "2024-01-01"
        ) in storage.get_materialized_partitions("date_sink")
        assert rs.PartitionKey.single(
            "2024-01-02"
        ) not in storage.get_materialized_partitions("date_sink")


class TestTimeWindowMappingPipeline:
    """TimeWindow offset mapping between daily-partitioned assets."""

    def test_time_window_offset(self):
        """Use TrackingHandler since InMemoryIOHandler can't resolve offset loads."""
        parts = rs.PartitionsDefinition.daily(start=DAILY_START, end=DAILY_END)
        handler = TrackingHandler(default_load_value=1)

        @rs.Asset(partitions_def=parts, io_handler=handler)
        def raw_daily() -> int:
            return 1

        @rs.Asset(
            partitions_def=parts,
            io_handler=handler,
            deps=[
                rs.AssetDef.input(
                    "raw_daily",
                    partition_mapping=rs.PartitionMapping.time_window(offset=-1),
                )
            ],
        )
        def lagged_daily(raw_daily: int) -> int:
            return raw_daily + 1

        repo = make_repo([raw_daily, lagged_daily])
        storage = repo.storage

        repo.materialize(
            ["raw_daily"], partition_key=rs.PartitionKey.single("2024-01-01")
        )
        assert rs.PartitionKey.single(
            "2024-01-01"
        ) in storage.get_materialized_partitions("raw_daily")

        result = repo.materialize(
            ["lagged_daily"], partition_key=rs.PartitionKey.single("2024-01-02")
        )
        assert result.success
        assert rs.PartitionKey.single(
            "2024-01-02"
        ) in storage.get_materialized_partitions("lagged_daily")
        assert rs.PartitionKey.single(
            "2024-01-01"
        ) not in storage.get_materialized_partitions("lagged_daily")


# ===========================================================================
# Multi-hop pipelines: partition tracking through chains
# ===========================================================================


class TestPartitionedChainPipeline:
    """Three-asset chain: verify partition state propagates through storage."""

    def test_static_chain_a_b_c(self):
        parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def step_a() -> int:
            return 1

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def step_b(step_a: int) -> int:
            return step_a + 1

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def step_c(step_b: int) -> int:
            return step_b + 1

        repo = make_repo([step_a, step_b, step_c])
        storage = repo.storage

        result = repo.materialize(
            ["step_a", "step_b", "step_c"], partition_key=rs.PartitionKey.single("a")
        )
        assert result.success

        for name in ["step_a", "step_b", "step_c"]:
            assert rs.PartitionKey.single("a") in storage.get_materialized_partitions(
                name
            )

        # Partition "b" only for step_a
        repo.materialize(["step_a"], partition_key=rs.PartitionKey.single("b"))
        assert rs.PartitionKey.single("b") in storage.get_materialized_partitions(
            "step_a"
        )
        assert rs.PartitionKey.single("b") not in storage.get_materialized_partitions(
            "step_b"
        )
        assert rs.PartitionKey.single("b") not in storage.get_materialized_partitions(
            "step_c"
        )

    def test_daily_chain_partial_materialization(self):
        parts = rs.PartitionsDefinition.daily(start=DAILY_START, end=DAILY_END)

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def raw() -> int:
            return 1

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def cleaned(raw: int) -> int:
            return raw * 2

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def aggregated(cleaned: int) -> int:
            return cleaned + 10

        repo = make_repo([raw, cleaned, aggregated])
        storage = repo.storage

        for day in ["2024-01-01", "2024-01-02"]:
            repo.materialize(
                ["raw", "cleaned", "aggregated"],
                partition_key=rs.PartitionKey.single(day),
            )

        repo.materialize(["raw"], partition_key=rs.PartitionKey.single("2024-01-03"))

        assert len(storage.get_materialized_partitions("raw")) == 3
        assert len(storage.get_materialized_partitions("cleaned")) == 2
        assert len(storage.get_materialized_partitions("aggregated")) == 2
        assert rs.PartitionKey.single(
            "2024-01-03"
        ) in storage.get_materialized_partitions("raw")
        assert rs.PartitionKey.single(
            "2024-01-03"
        ) not in storage.get_materialized_partitions("cleaned")


# ===========================================================================
# Mixed pipelines: partitioned → unpartitioned and vice versa
# ===========================================================================


class TestMixedPartitionPipelines:
    """Pipelines mixing partitioned and unpartitioned assets."""

    def test_partitioned_upstream_unpartitioned_downstream(self):
        """Use TrackingHandler since InMemoryIOHandler can't resolve multi-key loads."""
        parts = rs.PartitionsDefinition.static_(["x", "y", "z"])
        handler = TrackingHandler(default_load_value=1)

        @rs.Asset(partitions_def=parts, io_handler=handler)
        def partitioned() -> int:
            return 1

        @rs.Asset(
            io_handler=handler,
            deps=[
                rs.AssetDef.input(
                    "partitioned",
                    partition_mapping=rs.PartitionMapping.specific_partitions(
                        ["x", "y"]
                    ),
                )
            ],
        )
        def unpartitioned(partitioned: int) -> int:
            return partitioned + 1

        repo = make_repo([partitioned, unpartitioned])
        storage = repo.storage

        for k in ["x", "y"]:
            repo.materialize(["partitioned"], partition_key=rs.PartitionKey.single(k))

        result = repo.materialize(["unpartitioned"])
        assert result.success
        mat = storage.get_materialized_partitions("partitioned")
        assert len(mat) == 2
        assert rs.PartitionKey.single("x") in mat
        assert rs.PartitionKey.single("y") in mat
        assert rs.PartitionKey.single("z") not in mat

    def test_unpartitioned_upstream_partitioned_downstream(self):
        """Partitioned asset with no partitioned upstream deps."""
        parts = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def partitioned() -> int:
            return 1

        repo = make_repo([partitioned])
        storage = repo.storage

        result = repo.materialize(
            ["partitioned"], partition_key=rs.PartitionKey.single("a")
        )
        assert result.success
        assert rs.PartitionKey.single("a") in storage.get_materialized_partitions(
            "partitioned"
        )
        assert rs.PartitionKey.single("b") not in storage.get_materialized_partitions(
            "partitioned"
        )


# ===========================================================================
# Diamond pattern: partition state with shared dependencies
# ===========================================================================


class TestDiamondPartitionPattern:
    """Diamond: A → B, A → C, B → D, C → D. All partitioned."""

    def test_diamond_all_partitions_tracked(self):
        parts = rs.PartitionsDefinition.static_(["p1", "p2"])

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def a_root() -> int:
            return 1

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def b_left(a_root: int) -> int:
            return a_root + 1

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def c_right(a_root: int) -> int:
            return a_root + 2

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def d_merge(b_left: int, c_right: int) -> int:
            return b_left + c_right

        repo = make_repo([a_root, b_left, c_right, d_merge])
        storage = repo.storage

        result = repo.materialize(
            ["a_root", "b_left", "c_right", "d_merge"],
            partition_key=rs.PartitionKey.single("p1"),
        )
        assert result.success

        for name in ["a_root", "b_left", "c_right", "d_merge"]:
            assert rs.PartitionKey.single("p1") in storage.get_materialized_partitions(
                name
            )
            assert rs.PartitionKey.single(
                "p2"
            ) not in storage.get_materialized_partitions(name)


# ===========================================================================
# Edge cases
# ===========================================================================


class TestPartitionEdgeCases:
    """Edge cases: no materializations, single-key definitions, etc."""

    def test_no_materializations_returns_empty(self):
        parts = rs.PartitionsDefinition.static_(STATIC_KEYS)

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def untouched() -> int:
            return 1

        repo = make_repo([untouched])
        assert repo.storage.get_materialized_partitions("untouched") == []

    def test_single_key_partition_def(self):
        parts = rs.PartitionsDefinition.static_(["only_one"])

        @rs.Asset(partitions_def=parts, io_handler=IO())
        def single_part() -> int:
            return 42

        repo = make_repo([single_part])
        storage = repo.storage

        result = repo.materialize(
            ["single_part"], partition_key=rs.PartitionKey.single("only_one")
        )
        assert result.success

        mat = storage.get_materialized_partitions("single_part")
        assert len(mat) == 1
        assert mat[0] == rs.PartitionKey.single("only_one")

    def test_nonexistent_asset_returns_empty(self, storage):
        assert storage.get_materialized_partitions("does_not_exist") == []
