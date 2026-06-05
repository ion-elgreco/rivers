"""Backfill tests for MultiAsset across executor, sync/async, partition types, and strategies."""

import asyncio

import rivers as rs

from _helpers import daily_pd as _daily_pd
from _helpers import multi_pd as _multi_pd
from _helpers import static_pd as _static_pd


# ---------------------------------------------------------------------------
# Explicit keys — static partitions
# ---------------------------------------------------------------------------


class TestMultiAssetBackfillStaticKeys:
    """MultiAsset backfill with static partitions and explicit keys."""

    def test_multi_asset_backfill(self, executor_env, is_async):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _static_pd(["a", "b", "c"])

        if is_async:

            @rs.Asset.from_multi(
                partitions_def=pd,
                output_defs=[
                    rs.AssetDef("x", io_handler=handler),
                    rs.AssetDef("y", io_handler=handler),
                ],
            )
            async def multi(context: rs.AssetExecutionContext):
                await asyncio.sleep(0)
                val = {"a": 1, "b": 2, "c": 3}[context.partition_key]
                yield rs.Output(value=val, output_name="x")
                yield rs.Output(value=val * 10, output_name="y")
        else:

            @rs.Asset.from_multi(
                partitions_def=pd,
                output_defs=[
                    rs.AssetDef("x", io_handler=handler),
                    rs.AssetDef("y", io_handler=handler),
                ],
            )
            def multi(context: rs.AssetExecutionContext):
                val = {"a": 1, "b": 2, "c": 3}[context.partition_key]
                yield rs.Output(value=val, output_name="x")
                yield rs.Output(value=val * 10, output_name="y")

        repo = rs.CodeRepository(assets=[multi], default_executor=executor)
        result = repo.backfill(
            selection=["x"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
            ],
        )
        assert result.num_partitions == 2
        assert result.completed == 2
        assert result.status == "CompletedSuccess"
        assert len(result.run_ids) == 2


# ---------------------------------------------------------------------------
# Explicit keys — daily (time-window) partitions
# ---------------------------------------------------------------------------


class TestMultiAssetBackfillDailyKeys:
    """MultiAsset backfill with daily partitions and explicit keys."""

    def test_daily_backfill(self, executor_env, is_async):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _daily_pd()

        if is_async:

            @rs.Asset.from_multi(
                partitions_def=pd,
                output_defs=[
                    rs.AssetDef("raw", io_handler=handler),
                    rs.AssetDef("clean", io_handler=handler),
                ],
            )
            async def etl(context: rs.AssetExecutionContext):
                await asyncio.sleep(0)
                yield rs.Output(value=1, output_name="raw")
                yield rs.Output(value=2, output_name="clean")
        else:

            @rs.Asset.from_multi(
                partitions_def=pd,
                output_defs=[
                    rs.AssetDef("raw", io_handler=handler),
                    rs.AssetDef("clean", io_handler=handler),
                ],
            )
            def etl(context: rs.AssetExecutionContext):
                yield rs.Output(value=1, output_name="raw")
                yield rs.Output(value=2, output_name="clean")

        repo = rs.CodeRepository(assets=[etl], default_executor=executor)
        result = repo.backfill(
            selection=["raw"],
            partition_keys=[
                rs.PartitionKey.single("2024-01-01"),
                rs.PartitionKey.single("2024-01-02"),
            ],
        )
        assert result.num_partitions == 2
        assert result.completed == 2
        assert result.status == "CompletedSuccess"


# ---------------------------------------------------------------------------
# Multi-dimensional partitions
# ---------------------------------------------------------------------------


class TestMultiAssetBackfillMultiDimensional:
    """MultiAsset backfill with multi-dimensional partitions."""

    def test_multi_dim_explicit_keys(self, executor_env, is_async):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _multi_pd()

        if is_async:

            @rs.Asset.from_multi(
                partitions_def=pd,
                output_defs=[
                    rs.AssetDef("out1", io_handler=handler),
                    rs.AssetDef("out2", io_handler=handler),
                ],
            )
            async def multi(context: rs.AssetExecutionContext):
                await asyncio.sleep(0)
                yield rs.Output(value=1, output_name="out1")
                yield rs.Output(value=2, output_name="out2")
        else:

            @rs.Asset.from_multi(
                partitions_def=pd,
                output_defs=[
                    rs.AssetDef("out1", io_handler=handler),
                    rs.AssetDef("out2", io_handler=handler),
                ],
            )
            def multi(context: rs.AssetExecutionContext):
                yield rs.Output(value=1, output_name="out1")
                yield rs.Output(value=2, output_name="out2")

        repo = rs.CodeRepository(assets=[multi], default_executor=executor)
        result = repo.backfill(
            selection=["out1"],
            partition_keys=[
                rs.PartitionKey.multi({"region": "us", "date": "d1"}),
                rs.PartitionKey.multi({"region": "eu", "date": "d2"}),
            ],
        )
        assert result.num_partitions == 2
        assert result.completed == 2
        assert result.status == "CompletedSuccess"

    def test_sync_multi_dim_range(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _multi_pd()

        @rs.Asset.from_multi(
            partitions_def=pd,
            output_defs=[
                rs.AssetDef("out1", io_handler=handler),
                rs.AssetDef("out2", io_handler=handler),
            ],
        )
        def multi(context: rs.AssetExecutionContext):
            yield rs.Output(value=1, output_name="out1")
            yield rs.Output(value=2, output_name="out2")

        repo = rs.CodeRepository(assets=[multi], default_executor=executor)
        result = repo.backfill(
            selection=["out1"],
            partition_range=rs.PartitionKeyRange.multi(
                {
                    "region": ["us"],
                    "date": ["d1", "d2"],
                }
            ),
            dry_run=True,
        )
        # 1 region × 2 dates = 2 keys
        assert result.num_partitions == 2
        assert result.is_dry_run


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------


class TestMultiAssetBackfillDryRun:
    """MultiAsset dry-run verification."""

    def test_dry_run_no_execution(self, executor_env, is_async):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _static_pd(["a", "b"])
        calls = []

        if is_async:

            @rs.Asset.from_multi(
                partitions_def=pd,
                output_defs=[
                    rs.AssetDef("x", io_handler=handler),
                ],
            )
            async def multi(context: rs.AssetExecutionContext):
                calls.append(1)
                yield rs.Output(value=1, output_name="x")
        else:

            @rs.Asset.from_multi(
                partitions_def=pd,
                output_defs=[
                    rs.AssetDef("x", io_handler=handler),
                ],
            )
            def multi(context: rs.AssetExecutionContext):
                calls.append(1)
                yield rs.Output(value=1, output_name="x")

        repo = rs.CodeRepository(assets=[multi], default_executor=executor)
        result = repo.backfill(
            selection=["x"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
            ],
            dry_run=True,
        )
        assert result.is_dry_run
        assert result.status == "dry_run"
        assert result.num_partitions == 2
        assert result.completed == 0
        assert len(result.run_ids) == 0
        assert len(calls) == 0


# ---------------------------------------------------------------------------
# Strategies — single_run, multi_run, per_dimension
# ---------------------------------------------------------------------------


class TestMultiAssetBackfillStrategySingleRun:
    """MultiAsset with single_run strategy."""

    def test_sync_single_run(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _static_pd(["a", "b", "c"])

        @rs.Asset.from_multi(
            partitions_def=pd,
            output_defs=[
                rs.AssetDef("x", io_handler=handler),
                rs.AssetDef("y", io_handler=handler),
            ],
        )
        def multi(context: rs.AssetExecutionContext):
            yield rs.Output(value=1, output_name="x")
            yield rs.Output(value=2, output_name="y")

        repo = rs.CodeRepository(assets=[multi], default_executor=executor)
        result = repo.backfill(
            selection=["x"],
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

    def test_async_single_run_executes_all(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _static_pd(["a", "b", "c"])
        calls = []

        @rs.Asset.from_multi(
            partitions_def=pd,
            output_defs=[
                rs.AssetDef("x", io_handler=handler),
            ],
        )
        async def multi(context: rs.AssetExecutionContext):
            await asyncio.sleep(0)
            calls.append(len(context.partition.keys))
            yield rs.Output(value=1, output_name="x")

        repo = rs.CodeRepository(assets=[multi], default_executor=executor)
        result = repo.backfill(
            selection=["x"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
                rs.PartitionKey.single("c"),
            ],
            strategy=rs.BackfillStrategy.single_run(),
        )
        assert result.completed == 3
        assert calls == [3]


class TestMultiAssetBackfillStrategyPerDimension:
    """MultiAsset with per_dimension strategy on multi-dimensional partitions."""

    def test_sync_per_dimension_dry_run(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _multi_pd()

        @rs.Asset.from_multi(
            partitions_def=pd,
            output_defs=[
                rs.AssetDef("out", io_handler=handler),
            ],
        )
        def multi(context: rs.AssetExecutionContext):
            yield rs.Output(value=1, output_name="out")

        repo = rs.CodeRepository(assets=[multi], default_executor=executor)
        result = repo.backfill(
            selection=["out"],
            partition_keys=[
                rs.PartitionKey.multi({"region": "us", "date": "d1"}),
                rs.PartitionKey.multi({"region": "us", "date": "d2"}),
                rs.PartitionKey.multi({"region": "eu", "date": "d1"}),
                rs.PartitionKey.multi({"region": "eu", "date": "d2"}),
            ],
            strategy=rs.BackfillStrategy.per_dimension(
                multi_run=["region"], single_run=["date"]
            ),
            dry_run=True,
        )
        # 4 partitions grouped into 2 runs (one per region)
        assert result.num_partitions == 4
        assert result.num_runs == 2

    def test_per_dimension_executes(self, executor_env, is_async):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _multi_pd()
        calls = []

        if is_async:

            @rs.Asset.from_multi(
                partitions_def=pd,
                output_defs=[
                    rs.AssetDef("out", io_handler=handler),
                ],
            )
            async def multi(context: rs.AssetExecutionContext):
                await asyncio.sleep(0)
                calls.append(context.partition.key)
                yield rs.Output(value=1, output_name="out")
        else:

            @rs.Asset.from_multi(
                partitions_def=pd,
                output_defs=[
                    rs.AssetDef("out", io_handler=handler),
                ],
            )
            def multi(context: rs.AssetExecutionContext):
                calls.append(context.partition.key)
                yield rs.Output(value=1, output_name="out")

        repo = rs.CodeRepository(assets=[multi], default_executor=executor)
        result = repo.backfill(
            selection=["out"],
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


class TestMultiAssetBackfillExplicitStrategy:
    """MultiAsset with explicit backfill strategy (from_multi does not accept backfill_strategy)."""

    def test_sync_explicit_single_run(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _static_pd(["a", "b"])

        @rs.Asset.from_multi(
            partitions_def=pd,
            output_defs=[
                rs.AssetDef("x", io_handler=handler),
            ],
        )
        def multi(context: rs.AssetExecutionContext):
            yield rs.Output(value=1, output_name="x")

        repo = rs.CodeRepository(assets=[multi], default_executor=executor)
        result = repo.backfill(
            selection=["x"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
            ],
            strategy=rs.BackfillStrategy.single_run(),
            dry_run=True,
        )
        assert result.num_runs == 1

    def test_sync_explicit_multi_run(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _static_pd(["a", "b"])

        @rs.Asset.from_multi(
            partitions_def=pd,
            output_defs=[
                rs.AssetDef("x", io_handler=handler),
            ],
        )
        def multi(context: rs.AssetExecutionContext):
            yield rs.Output(value=1, output_name="x")

        repo = rs.CodeRepository(assets=[multi], default_executor=executor)
        result = repo.backfill(
            selection=["x"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
            ],
            strategy=rs.BackfillStrategy.multi_run(),
            dry_run=True,
        )
        assert result.num_runs == 2


# ---------------------------------------------------------------------------
# Downstream deps
# ---------------------------------------------------------------------------


class TestMultiAssetBackfillWithDeps:
    """MultiAsset producing outputs consumed by a downstream asset."""

    def test_multi_asset_with_downstream(self, executor_env, is_async):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _static_pd(["a", "b"])

        if is_async:

            @rs.Asset.from_multi(
                partitions_def=pd,
                output_defs=[
                    rs.AssetDef("x", io_handler=handler),
                    rs.AssetDef("y", io_handler=handler),
                ],
            )
            async def producer(context: rs.AssetExecutionContext):
                await asyncio.sleep(0)
                val = {"a": 10, "b": 20}[context.partition_key]
                yield rs.Output(value=val, output_name="x")
                yield rs.Output(value=val * 2, output_name="y")

            @rs.Asset(partitions_def=pd, io_handler=handler)
            async def consumer(x: int, y: int) -> int:
                return x + y
        else:

            @rs.Asset.from_multi(
                partitions_def=pd,
                output_defs=[
                    rs.AssetDef("x", io_handler=handler),
                    rs.AssetDef("y", io_handler=handler),
                ],
            )
            def producer(context: rs.AssetExecutionContext):
                val = {"a": 10, "b": 20}[context.partition_key]
                yield rs.Output(value=val, output_name="x")
                yield rs.Output(value=val * 2, output_name="y")

            @rs.Asset(partitions_def=pd, io_handler=handler)
            def consumer(x: int, y: int) -> int:
                return x + y

        repo = rs.CodeRepository(assets=[producer, consumer], default_executor=executor)
        result = repo.backfill(
            selection=["x", "y", "consumer"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
            ],
        )
        assert result.completed == 2
        assert result.status == "CompletedSuccess"


# ---------------------------------------------------------------------------
# Failure policy
# ---------------------------------------------------------------------------


class TestMultiAssetBackfillFailurePolicy:
    """MultiAsset backfill failure handling."""

    def test_stop_on_failure(self, executor_env, is_async):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _static_pd(["a", "b", "c", "d"])

        if is_async:

            @rs.Asset.from_multi(
                partitions_def=pd,
                output_defs=[
                    rs.AssetDef("x", io_handler=handler),
                ],
            )
            async def multi(context: rs.AssetExecutionContext):
                await asyncio.sleep(0)
                if context.partition_key == "b":
                    raise ValueError("intentional failure")
                yield rs.Output(value=1, output_name="x")
        else:

            @rs.Asset.from_multi(
                partitions_def=pd,
                output_defs=[
                    rs.AssetDef("x", io_handler=handler),
                ],
            )
            def multi(context: rs.AssetExecutionContext):
                if context.partition_key == "b":
                    raise ValueError("intentional failure")
                yield rs.Output(value=1, output_name="x")

        repo = rs.CodeRepository(assets=[multi], default_executor=executor)
        result = repo.backfill(
            selection=["x"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
                rs.PartitionKey.single("c"),
                rs.PartitionKey.single("d"),
            ],
            failure_policy="stop_on_failure",
            max_concurrency=1,
        )
        assert result.failed >= 1
        assert result.canceled >= 1
        assert result.status in ("Canceled", "CompletedFailed")


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


class TestMultiAssetBackfillTags:
    """MultiAsset backfill tags propagate to runs."""

    def test_sync_tags(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _static_pd(["a"])

        @rs.Asset.from_multi(
            partitions_def=pd,
            output_defs=[
                rs.AssetDef("x", io_handler=handler),
            ],
        )
        def multi(context: rs.AssetExecutionContext):
            yield rs.Output(value=1, output_name="x")

        repo = rs.CodeRepository(assets=[multi], default_executor=executor)
        result = repo.backfill(
            selection=["x"],
            partition_keys=[rs.PartitionKey.single("a")],
            tags=[("team", "data")],
        )
        assert result.completed == 1
        status = repo.get_backfill(result.backfill_id)
        assert status is not None
        run = repo.storage.get_run(status.run_ids[0])
        assert run is not None
        tag_dict = dict(run.tags)
        assert tag_dict["team"] == "data"
        assert run.launched_by.kind == "backfill"
        assert run.launched_by.backfill_id == result.backfill_id


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class TestMultiAssetBackfillConfig:
    """MultiAsset backfill config forwarding."""

    def test_sync_config_passed(self, executor_env):
        from pydantic import BaseModel

        executor, make_handler = executor_env
        handler = make_handler()
        pd = _static_pd(["a"])
        captured = {}

        class Cfg(BaseModel):
            mode: str = "default"

        @rs.Asset.from_multi(
            partitions_def=pd,
            output_defs=[
                rs.AssetDef("x", io_handler=handler),
            ],
        )
        def multi(context: rs.AssetExecutionContext[Cfg]):
            captured["mode"] = context.config.mode
            yield rs.Output(value=1, output_name="x")

        repo = rs.CodeRepository(assets=[multi], default_executor=executor)
        result = repo.backfill(
            selection=["x"],
            partition_keys=[rs.PartitionKey.single("a")],
            config={"x": {"mode": "full_refresh"}},
        )
        assert result.completed == 1
        assert captured.get("mode") == "full_refresh"


# ---------------------------------------------------------------------------
# Status tracking
# ---------------------------------------------------------------------------


class TestMultiAssetBackfillStatus:
    """MultiAsset backfill status retrieval."""

    def test_sync_get_status(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _static_pd(["a", "b"])

        @rs.Asset.from_multi(
            partitions_def=pd,
            output_defs=[
                rs.AssetDef("x", io_handler=handler),
            ],
        )
        def multi(context: rs.AssetExecutionContext):
            yield rs.Output(value=1, output_name="x")

        repo = rs.CodeRepository(assets=[multi], default_executor=executor)
        result = repo.backfill(
            selection=["x"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
            ],
        )
        status = repo.get_backfill(result.backfill_id)
        assert status is not None
        assert status.backfill_id == result.backfill_id
        assert status.status == "CompletedSuccess"
        assert status.completed_partitions == 2
        assert status.total_partitions == 2
