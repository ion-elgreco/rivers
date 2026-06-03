"""Backfill tests for GraphAsset across executor, sync/async, partition types, and strategies."""

import asyncio

import rivers as rs

from _helpers import daily_pd as _daily_pd
from _helpers import multi_pd as _multi_pd
from _helpers import static_pd as _static_pd


# ---------------------------------------------------------------------------
# Explicit keys — static partitions
# ---------------------------------------------------------------------------


class TestGraphAssetBackfillStaticKeys:
    """GraphAsset backfill with static partitions and explicit keys."""

    def test_graph_asset_backfill(self, executor_env, is_async):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _static_pd(["a", "b", "c"])

        if is_async:

            @rs.Asset(partitions_def=pd, io_handler=handler)
            async def source(context: rs.AssetExecutionContext) -> int:
                await asyncio.sleep(0)
                return {"a": 1, "b": 2, "c": 3}[context.partition_key]
        else:

            @rs.Asset(partitions_def=pd, io_handler=handler)
            def source(context: rs.AssetExecutionContext) -> int:
                return {"a": 1, "b": 2, "c": 3}[context.partition_key]

        @rs.Task
        def double(source: int) -> int:
            return source * 2

        @rs.Asset.from_graph(name="pipeline", partitions_def=pd, io_handler=handler)
        def pipeline(source: int):
            return double(source)

        repo = rs.CodeRepository(
            assets=[source, pipeline], tasks=[double], default_executor=executor
        )
        result = repo.backfill(
            selection=["source", "pipeline"],
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


class TestGraphAssetBackfillDailyKeys:
    """GraphAsset backfill with daily partitions and explicit keys."""

    def test_daily_backfill(self, executor_env, is_async):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _daily_pd()

        if is_async:

            @rs.Asset(partitions_def=pd, io_handler=handler)
            async def raw(context: rs.AssetExecutionContext) -> int:
                return 1
        else:

            @rs.Asset(partitions_def=pd, io_handler=handler)
            def raw(context: rs.AssetExecutionContext) -> int:
                return 1

        @rs.Task
        def transform(raw: int) -> int:
            return raw + 100

        @rs.Asset.from_graph(name="processed", partitions_def=pd, io_handler=handler)
        def processed(raw: int):
            return transform(raw)

        repo = rs.CodeRepository(
            assets=[raw, processed], tasks=[transform], default_executor=executor
        )
        result = repo.backfill(
            selection=["raw", "processed"],
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


class TestGraphAssetBackfillMultiDimensional:
    """GraphAsset backfill with multi-dimensional partitions."""

    def test_multi_dim_explicit_keys(self, executor_env, is_async):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _multi_pd()

        if is_async:

            @rs.Asset(partitions_def=pd, io_handler=handler)
            async def source(context: rs.AssetExecutionContext) -> int:
                return 1
        else:

            @rs.Asset(partitions_def=pd, io_handler=handler)
            def source(context: rs.AssetExecutionContext) -> int:
                return 1

        @rs.Task
        def inc(source: int) -> int:
            return source + 1

        @rs.Asset.from_graph(name="pipe", partitions_def=pd, io_handler=handler)
        def pipe(source: int):
            return inc(source)

        repo = rs.CodeRepository(
            assets=[source, pipe], tasks=[inc], default_executor=executor
        )
        result = repo.backfill(
            selection=["source", "pipe"],
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

        @rs.Asset(partitions_def=pd, io_handler=handler)
        def source(context: rs.AssetExecutionContext) -> int:
            return 1

        @rs.Task
        def inc(source: int) -> int:
            return source + 1

        @rs.Asset.from_graph(name="pipe", partitions_def=pd, io_handler=handler)
        def pipe(source: int):
            return inc(source)

        repo = rs.CodeRepository(
            assets=[source, pipe], tasks=[inc], default_executor=executor
        )
        result = repo.backfill(
            selection=["source", "pipe"],
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


class TestGraphAssetBackfillDryRun:
    """GraphAsset dry-run verification."""

    def test_dry_run_no_execution(self, executor_env, is_async):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _static_pd(["a", "b"])
        calls = []

        if is_async:

            @rs.Asset(partitions_def=pd, io_handler=handler)
            async def source(context: rs.AssetExecutionContext) -> int:
                calls.append(1)
                return 1
        else:

            @rs.Asset(partitions_def=pd, io_handler=handler)
            def source(context: rs.AssetExecutionContext) -> int:
                calls.append(1)
                return 1

        @rs.Task
        def inc(source: int) -> int:
            return source + 1

        @rs.Asset.from_graph(name="pipe", partitions_def=pd, io_handler=handler)
        def pipe(source: int):
            return inc(source)

        repo = rs.CodeRepository(
            assets=[source, pipe], tasks=[inc], default_executor=executor
        )
        result = repo.backfill(
            selection=["source", "pipe"],
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


class TestGraphAssetBackfillStrategySingleRun:
    """GraphAsset with single_run strategy."""

    def test_sync_single_run(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _static_pd(["a", "b", "c"])

        @rs.Asset(partitions_def=pd, io_handler=handler)
        def source(context: rs.AssetExecutionContext) -> int:
            return 1

        @rs.Task
        def inc(source: int) -> int:
            return source + 1

        @rs.Asset.from_graph(name="pipe", partitions_def=pd, io_handler=handler)
        def pipe(source: int):
            return inc(source)

        repo = rs.CodeRepository(
            assets=[source, pipe], tasks=[inc], default_executor=executor
        )
        result = repo.backfill(
            selection=["source", "pipe"],
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

        @rs.Asset(partitions_def=pd, io_handler=handler)
        async def source(context: rs.AssetExecutionContext) -> int:
            calls.append(len(context.partition.keys))
            return 1

        @rs.Task
        def inc(source: int) -> int:
            return source + 1

        @rs.Asset.from_graph(name="pipe", partitions_def=pd, io_handler=handler)
        def pipe(source: int):
            return inc(source)

        repo = rs.CodeRepository(
            assets=[source, pipe], tasks=[inc], default_executor=executor
        )
        result = repo.backfill(
            selection=["source", "pipe"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
                rs.PartitionKey.single("c"),
            ],
            strategy=rs.BackfillStrategy.single_run(),
        )
        assert result.completed == 3
        assert calls == [3]


class TestGraphAssetBackfillStrategyPerDimension:
    """GraphAsset with per_dimension strategy on multi-dimensional partitions."""

    def test_sync_per_dimension_dry_run(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _multi_pd()

        @rs.Asset(partitions_def=pd, io_handler=handler)
        def source(context: rs.AssetExecutionContext) -> int:
            return 1

        @rs.Task
        def inc(source: int) -> int:
            return source + 1

        @rs.Asset.from_graph(name="pipe", partitions_def=pd, io_handler=handler)
        def pipe(source: int):
            return inc(source)

        repo = rs.CodeRepository(
            assets=[source, pipe], tasks=[inc], default_executor=executor
        )
        result = repo.backfill(
            selection=["source", "pipe"],
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

            @rs.Asset(partitions_def=pd, io_handler=handler)
            async def source(context: rs.AssetExecutionContext) -> int:
                calls.append(context.partition.key)
                return 1
        else:

            @rs.Asset(partitions_def=pd, io_handler=handler)
            def source(context: rs.AssetExecutionContext) -> int:
                calls.append(context.partition.key)
                return 1

        @rs.Task
        def inc(source: int) -> int:
            return source + 1

        @rs.Asset.from_graph(name="pipe", partitions_def=pd, io_handler=handler)
        def pipe(source: int):
            return inc(source)

        repo = rs.CodeRepository(
            assets=[source, pipe], tasks=[inc], default_executor=executor
        )
        result = repo.backfill(
            selection=["source", "pipe"],
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


class TestGraphAssetBackfillExplicitStrategy:
    """GraphAsset with explicit backfill strategy (from_graph does not accept backfill_strategy)."""

    def test_sync_explicit_single_run(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _static_pd(["a", "b"])

        @rs.Asset(partitions_def=pd, io_handler=handler)
        def source(context: rs.AssetExecutionContext) -> int:
            return 1

        @rs.Task
        def inc(source: int) -> int:
            return source + 1

        @rs.Asset.from_graph(name="pipe", partitions_def=pd, io_handler=handler)
        def pipe(source: int):
            return inc(source)

        repo = rs.CodeRepository(
            assets=[source, pipe], tasks=[inc], default_executor=executor
        )
        result = repo.backfill(
            selection=["source", "pipe"],
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

        @rs.Asset(partitions_def=pd, io_handler=handler)
        def source(context: rs.AssetExecutionContext) -> int:
            return 1

        @rs.Task
        def inc(source: int) -> int:
            return source + 1

        @rs.Asset.from_graph(name="pipe", partitions_def=pd, io_handler=handler)
        def pipe(source: int):
            return inc(source)

        repo = rs.CodeRepository(
            assets=[source, pipe], tasks=[inc], default_executor=executor
        )
        result = repo.backfill(
            selection=["source", "pipe"],
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


class TestGraphAssetBackfillWithDeps:
    """GraphAsset with a downstream single asset."""

    def test_graph_with_downstream(self, executor_env, is_async):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _static_pd(["a", "b"])

        if is_async:

            @rs.Asset(partitions_def=pd, io_handler=handler)
            async def source(context: rs.AssetExecutionContext) -> int:
                return {"a": 5, "b": 50}[context.partition_key]

            @rs.Asset(partitions_def=pd, io_handler=handler)
            async def final(pipe: int) -> int:
                return pipe + 1
        else:

            @rs.Asset(partitions_def=pd, io_handler=handler)
            def source(context: rs.AssetExecutionContext) -> int:
                return {"a": 5, "b": 50}[context.partition_key]

            @rs.Asset(partitions_def=pd, io_handler=handler)
            def final(pipe: int) -> int:
                return pipe + 1

        @rs.Task
        def double(source: int) -> int:
            return source * 2

        @rs.Asset.from_graph(name="pipe", partitions_def=pd, io_handler=handler)
        def pipe(source: int):
            return double(source)

        repo = rs.CodeRepository(
            assets=[source, pipe, final], tasks=[double], default_executor=executor
        )
        result = repo.backfill(
            selection=["source", "pipe", "final"],
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


class TestGraphAssetBackfillFailurePolicy:
    """GraphAsset backfill failure handling."""

    def test_stop_on_failure(self, executor_env, is_async):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _static_pd(["a", "b", "c", "d"])

        if is_async:

            @rs.Asset(partitions_def=pd, io_handler=handler)
            async def source(context: rs.AssetExecutionContext) -> int:
                if context.partition_key == "b":
                    raise ValueError("intentional failure")
                return 1
        else:

            @rs.Asset(partitions_def=pd, io_handler=handler)
            def source(context: rs.AssetExecutionContext) -> int:
                if context.partition_key == "b":
                    raise ValueError("intentional failure")
                return 1

        @rs.Task
        def inc(source: int) -> int:
            return source + 1

        @rs.Asset.from_graph(name="pipe", partitions_def=pd, io_handler=handler)
        def pipe(source: int):
            return inc(source)

        repo = rs.CodeRepository(
            assets=[source, pipe], tasks=[inc], default_executor=executor
        )
        result = repo.backfill(
            selection=["source", "pipe"],
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


class TestGraphAssetBackfillTags:
    """GraphAsset backfill tags propagate to runs."""

    def test_sync_tags(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _static_pd(["a"])

        @rs.Asset(partitions_def=pd, io_handler=handler)
        def source(context: rs.AssetExecutionContext) -> int:
            return 1

        @rs.Task
        def inc(source: int) -> int:
            return source + 1

        @rs.Asset.from_graph(name="pipe", partitions_def=pd, io_handler=handler)
        def pipe(source: int):
            return inc(source)

        repo = rs.CodeRepository(
            assets=[source, pipe], tasks=[inc], default_executor=executor
        )
        result = repo.backfill(
            selection=["source", "pipe"],
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
# Status tracking
# ---------------------------------------------------------------------------


class TestGraphAssetBackfillStatus:
    """GraphAsset backfill status retrieval."""

    def test_sync_get_status(self, executor_env):
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _static_pd(["a", "b"])

        @rs.Asset(partitions_def=pd, io_handler=handler)
        def source(context: rs.AssetExecutionContext) -> int:
            return 1

        @rs.Task
        def inc(source: int) -> int:
            return source + 1

        @rs.Asset.from_graph(name="pipe", partitions_def=pd, io_handler=handler)
        def pipe(source: int):
            return inc(source)

        repo = rs.CodeRepository(
            assets=[source, pipe], tasks=[inc], default_executor=executor
        )
        result = repo.backfill(
            selection=["source", "pipe"],
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


# ---------------------------------------------------------------------------
# node_io_handler
# ---------------------------------------------------------------------------


class TestGraphAssetBackfillNodeIOHandler:
    """GraphAsset backfill with separate node_io_handler for internal tasks."""

    def test_sync_node_io_handler(self, executor_env):
        executor, make_handler = executor_env
        graph_handler = make_handler()
        node_handler = make_handler()
        pd = _static_pd(["a", "b"])

        @rs.Asset(partitions_def=pd, io_handler=graph_handler)
        def source(context: rs.AssetExecutionContext) -> int:
            return {"a": 10, "b": 20}[context.partition_key]

        @rs.Task
        def double(source: int) -> int:
            return source * 2

        @rs.Asset.from_graph(
            name="pipe",
            partitions_def=pd,
            io_handler=graph_handler,
            node_io_handler=node_handler,
        )
        def pipe(source: int):
            return double(source)

        repo = rs.CodeRepository(
            assets=[source, pipe], tasks=[double], default_executor=executor
        )
        result = repo.backfill(
            selection=["source", "pipe"],
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
            ],
        )
        assert result.completed == 2
        assert result.status == "CompletedSuccess"
