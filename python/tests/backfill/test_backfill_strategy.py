"""Integration tests for BackfillStrategy, SingleRun, PerDimension, mark_partition_failed.

Covers plain @rs.Asset across executor (in_process, parallel) and sync/async.
"""

import asyncio
from datetime import datetime

import pytest
import rivers as rs

from _helpers import multi_pd as _multi_pd
from _helpers import static_pd as _static_pd


# ---------------------------------------------------------------------------
# Strategy type construction (no executor needed)
# ---------------------------------------------------------------------------


class TestBackfillStrategyType:
    def test_multi_run(self):
        s = rs.BackfillStrategy.multi_run()
        assert repr(s) == "BackfillStrategy.multi_run()"

    def test_single_run(self):
        s = rs.BackfillStrategy.single_run()
        assert repr(s) == "BackfillStrategy.single_run()"

    def test_per_dimension(self):
        s = rs.BackfillStrategy.per_dimension(multi_run=["region"], single_run=["date"])
        assert "per_dimension" in repr(s)

    def test_per_dimension_overlap_error(self):
        with pytest.raises(Exception, match="cannot be in both"):
            rs.BackfillStrategy.per_dimension(multi_run=["date"], single_run=["date"])

    def test_per_dimension_empty_error(self):
        with pytest.raises(Exception):
            rs.BackfillStrategy.per_dimension(multi_run=[], single_run=["date"])


# ---------------------------------------------------------------------------
# SingleRun strategy
# ---------------------------------------------------------------------------


class TestSingleRunStrategy:
    def test_single_run_dry_run_shows_one_run(self, executor_env, is_async):
        executor, _ = executor_env

        if is_async:

            @rs.Asset(partitions_def=_static_pd(["a", "b", "c"]))
            async def asset(context: rs.AssetExecutionContext) -> int:
                await asyncio.sleep(0)
                return 1
        else:

            @rs.Asset(partitions_def=_static_pd(["a", "b", "c"]))
            def asset(context: rs.AssetExecutionContext) -> int:
                return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        result = repo.backfill(
            selection=["asset"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
                rs.PartitionKey.single("c"),
            ],
            strategy=rs.BackfillStrategy.single_run(),
            dry_run=True,
        )
        assert result.num_partitions == 3
        assert result.num_runs == 1

    def test_single_run_executes_all_partitions(self, executor_env, is_async):
        executor, _ = executor_env
        calls = []

        if is_async:

            @rs.Asset(partitions_def=_static_pd(["a", "b", "c"]))
            async def asset(context: rs.AssetExecutionContext) -> int:
                await asyncio.sleep(0)
                calls.append(context.partition.key)
                return 1
        else:

            @rs.Asset(partitions_def=_static_pd(["a", "b", "c"]))
            def asset(context: rs.AssetExecutionContext) -> int:
                calls.append(context.partition.key)
                return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        result = repo.backfill(
            selection=["asset"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
                rs.PartitionKey.single("c"),
            ],
            strategy=rs.BackfillStrategy.single_run(),
        )
        assert result.completed == 3
        assert len(calls) == 1


# ---------------------------------------------------------------------------
# PerDimension strategy
# ---------------------------------------------------------------------------


class TestPerDimensionStrategy:
    def test_per_dimension_dry_run_groups(self, executor_env, is_async):
        executor, _ = executor_env
        pd = rs.PartitionsDefinition.multi(
            {
                "region": rs.PartitionsDefinition.static_(["us", "eu"]),
                "date": rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1)),
            }
        )

        if is_async:

            @rs.Asset(partitions_def=pd)
            async def asset(context: rs.AssetExecutionContext) -> int:
                await asyncio.sleep(0)
                return 1
        else:

            @rs.Asset(partitions_def=pd)
            def asset(context: rs.AssetExecutionContext) -> int:
                return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        result = repo.backfill(
            selection=["asset"],
            partition_range=rs.PartitionKeyRange.multi(
                {
                    "region": ["us", "eu"],
                    "date": ("2024-01-01", "2024-01-03"),
                }
            ),
            strategy=rs.BackfillStrategy.per_dimension(
                multi_run=["region"], single_run=["date"]
            ),
            dry_run=True,
        )
        # 2 regions × 3 dates = 6 partitions, but 2 runs (one per region)
        assert result.num_partitions == 6
        assert result.num_runs == 2

    def test_per_dimension_executes(self, executor_env, is_async):
        executor, _ = executor_env
        calls = []

        if is_async:

            @rs.Asset(partitions_def=_multi_pd())
            async def asset(context: rs.AssetExecutionContext) -> int:
                await asyncio.sleep(0)
                calls.append(context.partition.key)
                return 1
        else:

            @rs.Asset(partitions_def=_multi_pd())
            def asset(context: rs.AssetExecutionContext) -> int:
                calls.append(context.partition.key)
                return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        result = repo.backfill(
            selection=["asset"],
            partition_keys=[
                rs.PartitionKey.multi({"region": "us", "date": "d1"}),
                rs.PartitionKey.multi({"region": "us", "date": "d2"}),
                rs.PartitionKey.multi({"region": "eu", "date": "d1"}),
                rs.PartitionKey.multi({"region": "eu", "date": "d2"}),
            ],
            strategy=rs.BackfillStrategy.per_dimension(
                multi_run=["region"], single_run=["date"]
            ),
        )
        assert result.completed == 4
        assert len(calls) == 2


# ---------------------------------------------------------------------------
# SingleRun / PerDimension run each group as ONE run that invokes the asset
# once over all the group's keys, emitting one materialization per key.
# ---------------------------------------------------------------------------


class TestSingleRunExecution:
    def test_single_run_creates_one_run_record(self, executor_env, is_async):
        executor, _ = executor_env

        if is_async:

            @rs.Asset(partitions_def=_static_pd(["a", "b", "c"]))
            async def asset(context: rs.AssetExecutionContext) -> int:
                await asyncio.sleep(0)
                return 1
        else:

            @rs.Asset(partitions_def=_static_pd(["a", "b", "c"]))
            def asset(context: rs.AssetExecutionContext) -> int:
                return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        result = repo.backfill(
            selection=["asset"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
                rs.PartitionKey.single("c"),
            ],
            strategy=rs.BackfillStrategy.single_run(),
        )
        assert result.completed == 3
        assert len(result.run_ids) == 1

    def test_single_run_invokes_asset_once_with_all_keys(self, executor_env, is_async):
        executor, _ = executor_env
        keys_per_call: list[int] = []

        if is_async:

            @rs.Asset(partitions_def=_static_pd(["a", "b", "c"]))
            async def asset(context: rs.AssetExecutionContext) -> int:
                await asyncio.sleep(0)
                keys_per_call.append(len(context.partition.keys))
                return 1
        else:

            @rs.Asset(partitions_def=_static_pd(["a", "b", "c"]))
            def asset(context: rs.AssetExecutionContext) -> int:
                keys_per_call.append(len(context.partition.keys))
                return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        result = repo.backfill(
            selection=["asset"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
                rs.PartitionKey.single("c"),
            ],
            strategy=rs.BackfillStrategy.single_run(),
        )
        assert result.completed == 3
        assert keys_per_call == [3]


class TestPerDimensionExecution:
    def test_per_dimension_creates_one_run_per_group(self, executor_env, is_async):
        executor, _ = executor_env

        if is_async:

            @rs.Asset(partitions_def=_multi_pd())
            async def asset(context: rs.AssetExecutionContext) -> int:
                await asyncio.sleep(0)
                return 1
        else:

            @rs.Asset(partitions_def=_multi_pd())
            def asset(context: rs.AssetExecutionContext) -> int:
                return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        result = repo.backfill(
            selection=["asset"],
            partition_keys=[
                rs.PartitionKey.multi({"region": "us", "date": "d1"}),
                rs.PartitionKey.multi({"region": "us", "date": "d2"}),
                rs.PartitionKey.multi({"region": "eu", "date": "d1"}),
                rs.PartitionKey.multi({"region": "eu", "date": "d2"}),
            ],
            strategy=rs.BackfillStrategy.per_dimension(
                multi_run=["region"], single_run=["date"]
            ),
        )
        assert result.completed == 4
        assert len(result.run_ids) == 2

    def test_per_dimension_invokes_asset_once_per_group(self, executor_env, is_async):
        executor, _ = executor_env
        keys_per_call: list[int] = []

        if is_async:

            @rs.Asset(partitions_def=_multi_pd())
            async def asset(context: rs.AssetExecutionContext) -> int:
                await asyncio.sleep(0)
                keys_per_call.append(len(context.partition.keys))
                return 1
        else:

            @rs.Asset(partitions_def=_multi_pd())
            def asset(context: rs.AssetExecutionContext) -> int:
                keys_per_call.append(len(context.partition.keys))
                return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        result = repo.backfill(
            selection=["asset"],
            partition_keys=[
                rs.PartitionKey.multi({"region": "us", "date": "d1"}),
                rs.PartitionKey.multi({"region": "us", "date": "d2"}),
                rs.PartitionKey.multi({"region": "eu", "date": "d1"}),
                rs.PartitionKey.multi({"region": "eu", "date": "d2"}),
            ],
            strategy=rs.BackfillStrategy.per_dimension(
                multi_run=["region"], single_run=["date"]
            ),
        )
        assert result.completed == 4
        assert sorted(keys_per_call) == [2, 2]


# ---------------------------------------------------------------------------
# Asset-level default strategy
# ---------------------------------------------------------------------------


class TestAssetBackfillStrategy:
    def test_asset_default_strategy_used(self, executor_env, is_async):
        executor, _ = executor_env

        if is_async:

            @rs.Asset(
                partitions_def=_static_pd(["a", "b", "c"]),
                backfill_strategy=rs.BackfillStrategy.single_run(),
            )
            async def asset(context: rs.AssetExecutionContext) -> int:
                return 1
        else:

            @rs.Asset(
                partitions_def=_static_pd(["a", "b", "c"]),
                backfill_strategy=rs.BackfillStrategy.single_run(),
            )
            def asset(context: rs.AssetExecutionContext) -> int:
                return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        result = repo.backfill(
            selection=["asset"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
            ],
            dry_run=True,
        )
        assert result.num_runs == 1

    def test_sync_explicit_strategy_overrides_asset(self, executor_env):
        executor, _ = executor_env

        @rs.Asset(
            partitions_def=_static_pd(["a", "b"]),
            backfill_strategy=rs.BackfillStrategy.single_run(),
        )
        def asset(context: rs.AssetExecutionContext) -> int:
            return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        result = repo.backfill(
            selection=["asset"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
            ],
            strategy=rs.BackfillStrategy.multi_run(),
            dry_run=True,
        )
        assert result.num_runs == 2

    def test_sync_mixed_strategies_defaults_to_multi_run(self, executor_env):
        executor, _ = executor_env

        @rs.Asset(
            partitions_def=_static_pd(["a", "b"]),
            backfill_strategy=rs.BackfillStrategy.single_run(),
        )
        def asset_a(context: rs.AssetExecutionContext) -> int:
            return 1

        @rs.Asset(
            partitions_def=_static_pd(["a", "b"]),
            backfill_strategy=rs.BackfillStrategy.multi_run(),
        )
        def asset_b(context: rs.AssetExecutionContext) -> int:
            return 1

        repo = rs.CodeRepository(assets=[asset_a, asset_b], default_executor=executor)
        result = repo.backfill(
            selection=["asset_a", "asset_b"],
            partition_keys=[rs.PartitionKey.single("a")],
            dry_run=True,
        )
        assert result.num_runs == 1


# ---------------------------------------------------------------------------
# mark_partition_failed (no executor needed)
# ---------------------------------------------------------------------------


class TestMarkPartitionFailed:
    def test_mark_partition_failed_not_in_keys_errors(self):
        ctx = rs.AssetExecutionContext(asset_name="test")
        with pytest.raises(Exception, match="not in this context's partition keys"):
            ctx.mark_partition_failed(rs.PartitionKey.single("a"), error="oops")

    def test_partition_context_keys_is_list(self):
        ctx = rs.PartitionContext(
            keys=[rs.PartitionKey.single("a")],
            definition=rs.PartitionsDefinition.static_(["a", "b"]),
        )
        assert len(ctx.keys) == 1
        assert ctx.key == rs.PartitionKey.single("a")
