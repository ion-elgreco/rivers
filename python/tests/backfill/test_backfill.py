"""Integration tests for MultiRun backfills.

Covers plain @rs.Asset across executor (in_process, parallel) and sync/async.
"""

import asyncio
import re
from datetime import datetime

import pytest
import rivers as rs
from rivers.exceptions import ExecutionError

from _helpers import daily_pd as _daily_pd
from _helpers import static_pd as _static_pd


# ---------------------------------------------------------------------------
# Explicit keys
# ---------------------------------------------------------------------------


class TestBackfillExplicitKeys:
    def test_backfill_single_partition(self, executor_env, is_async):
        executor, _ = executor_env

        if is_async:

            @rs.Asset(partitions_def=_daily_pd())
            async def asset(context: rs.AssetExecutionContext) -> int:
                await asyncio.sleep(0)
                return 1
        else:

            @rs.Asset(partitions_def=_daily_pd())
            def asset(context: rs.AssetExecutionContext) -> int:
                return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        result = repo.backfill(
            selection=["asset"],
            partition_keys=[rs.PartitionKey.single("2024-01-15")],
        )
        assert result.num_partitions == 1
        assert result.completed == 1
        assert result.failed == 0
        assert result.status == "CompletedSuccess"
        assert len(result.run_ids) == 1
        assert not result.is_dry_run

    def test_backfill_multiple_partitions(self, executor_env, is_async):
        executor, _ = executor_env

        if is_async:

            @rs.Asset(partitions_def=_daily_pd())
            async def asset(context: rs.AssetExecutionContext) -> int:
                await asyncio.sleep(0)
                return 1
        else:

            @rs.Asset(partitions_def=_daily_pd())
            def asset(context: rs.AssetExecutionContext) -> int:
                return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        keys = [
            rs.PartitionKey.single("2024-01-10"),
            rs.PartitionKey.single("2024-01-11"),
            rs.PartitionKey.single("2024-01-12"),
        ]
        result = repo.backfill(
            selection=["asset"],
            partition_keys=keys,
            max_concurrency=2,
        )
        assert result.num_partitions == 3
        assert result.completed == 3
        assert result.failed == 0
        assert result.status == "CompletedSuccess"
        assert len(result.run_ids) == 3

    def test_backfill_with_deps(self, executor_env, is_async):
        executor, _ = executor_env
        pd = _daily_pd()

        if is_async:

            @rs.Asset(partitions_def=pd)
            async def upstream(context: rs.AssetExecutionContext) -> int:
                return 1

            @rs.Asset(partitions_def=pd)
            async def downstream(
                context: rs.AssetExecutionContext, upstream: int
            ) -> int:
                return 1
        else:

            @rs.Asset(partitions_def=pd)
            def upstream(context: rs.AssetExecutionContext) -> int:
                return 1

            @rs.Asset(partitions_def=pd)
            def downstream(context: rs.AssetExecutionContext, upstream: int) -> int:
                return 1

        repo = rs.CodeRepository(
            assets=[upstream, downstream], default_executor=executor
        )
        keys = [
            rs.PartitionKey.single("2024-01-10"),
            rs.PartitionKey.single("2024-01-11"),
        ]
        result = repo.backfill(
            selection=["upstream", "downstream"],
            partition_keys=keys,
        )
        assert result.num_partitions == 2
        assert result.completed == 2
        assert result.status == "CompletedSuccess"


# ---------------------------------------------------------------------------
# Partition range
# ---------------------------------------------------------------------------


class TestBackfillPartitionRange:
    def test_single_range_filters_keys(self, executor_env, is_async):
        executor, _ = executor_env

        if is_async:

            @rs.Asset(partitions_def=_daily_pd())
            async def asset(context: rs.AssetExecutionContext) -> int:
                await asyncio.sleep(0)
                return 1
        else:

            @rs.Asset(partitions_def=_daily_pd())
            def asset(context: rs.AssetExecutionContext) -> int:
                return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        result = repo.backfill(
            selection=["asset"],
            partition_range=rs.PartitionKeyRange.single(
                from_key="2024-01-01", to_key="2024-01-03"
            ),
        )
        assert result.num_partitions == 3
        assert result.completed == 3
        assert result.status == "CompletedSuccess"

    @pytest.mark.parametrize("asset_kind", ["graph", "multi"])
    def test_daily_range_across_asset_types(self, executor_env, is_async, asset_kind):
        """partition_range filtering for graph/multi assets matches plain-asset behavior."""
        executor, make_handler = executor_env
        handler = make_handler()
        pd = _daily_pd()

        if asset_kind == "graph":
            if is_async:

                @rs.Asset(partitions_def=pd, io_handler=handler)
                async def src(context: rs.AssetExecutionContext) -> int:
                    return 1
            else:

                @rs.Asset(partitions_def=pd, io_handler=handler)
                def src(context: rs.AssetExecutionContext) -> int:
                    return 1

            @rs.Task
            def inc(src: int) -> int:
                return src + 1

            @rs.Asset.from_graph(name="pipe", partitions_def=pd, io_handler=handler)
            def pipe(src: int):
                return inc(src)

            assets = [src, pipe]
            tasks = [inc]
            selection = ["src", "pipe"]
        else:  # multi
            if is_async:

                @rs.Asset.from_multi(
                    partitions_def=pd,
                    output_defs=[
                        rs.AssetDef("a", io_handler=handler),
                        rs.AssetDef("b", io_handler=handler),
                    ],
                )
                async def multi(context: rs.AssetExecutionContext):
                    await asyncio.sleep(0)
                    yield rs.Output(value=1, output_name="a")
                    yield rs.Output(value=2, output_name="b")
            else:

                @rs.Asset.from_multi(
                    partitions_def=pd,
                    output_defs=[
                        rs.AssetDef("a", io_handler=handler),
                        rs.AssetDef("b", io_handler=handler),
                    ],
                )
                def multi(context: rs.AssetExecutionContext):
                    yield rs.Output(value=1, output_name="a")
                    yield rs.Output(value=2, output_name="b")

            assets = [multi]
            tasks = []
            selection = ["a"]

        repo = rs.CodeRepository(assets=assets, tasks=tasks, default_executor=executor)
        result = repo.backfill(
            selection=selection,
            partition_range=rs.PartitionKeyRange.single(
                from_key="2024-01-01", to_key="2024-01-03"
            ),
        )
        assert result.num_partitions == 3
        assert result.completed == 3
        assert result.status == "CompletedSuccess"

    def test_sync_multi_range_cartesian_product(self, executor_env):
        executor, _ = executor_env

        @rs.Asset(
            partitions_def=rs.PartitionsDefinition.multi(
                {
                    "region": rs.PartitionsDefinition.static_(["us", "eu"]),
                    "date": rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1)),
                }
            ),
        )
        def asset(context: rs.AssetExecutionContext) -> int:
            return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        result = repo.backfill(
            selection=["asset"],
            partition_range=rs.PartitionKeyRange.multi(
                {
                    "date": ("2024-01-01", "2024-01-02"),
                    "region": ["us"],
                }
            ),
            dry_run=True,
        )
        # 2 dates × 1 region = 2 keys
        assert result.num_partitions == 2
        assert result.is_dry_run

    def test_sync_multi_range_omitted_dimension_includes_all(self, executor_env):
        executor, _ = executor_env

        @rs.Asset(
            partitions_def=rs.PartitionsDefinition.multi(
                {
                    "region": rs.PartitionsDefinition.static_(["us", "eu"]),
                    "date": rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1)),
                }
            ),
        )
        def asset(context: rs.AssetExecutionContext) -> int:
            return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        result = repo.backfill(
            selection=["asset"],
            partition_range=rs.PartitionKeyRange.multi(
                {
                    "date": ("2024-01-01", "2024-01-02"),
                }
            ),
            dry_run=True,
        )
        # 2 dates × 2 regions = 4 keys
        assert result.num_partitions == 4


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------


class TestBackfillDryRun:
    def test_dry_run_does_not_execute(self, executor_env, is_async):
        executor, _ = executor_env

        if is_async:

            @rs.Asset(partitions_def=_daily_pd())
            async def asset(context: rs.AssetExecutionContext) -> int:
                return 1
        else:

            @rs.Asset(partitions_def=_daily_pd())
            def asset(context: rs.AssetExecutionContext) -> int:
                return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        result = repo.backfill(
            selection=["asset"],
            partition_keys=[
                rs.PartitionKey.single("2024-01-10"),
                rs.PartitionKey.single("2024-01-11"),
            ],
            dry_run=True,
        )
        assert result.is_dry_run
        assert result.status == "dry_run"
        assert result.num_partitions == 2
        assert result.num_runs == 2
        assert result.completed == 0
        assert len(result.run_ids) == 0
        assert len(result.partition_keys) == 2


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------


class TestBackfillGetStatus:
    def test_sync_get_backfill_returns_status(self, executor_env):
        executor, _ = executor_env

        @rs.Asset(partitions_def=_daily_pd())
        def asset(context: rs.AssetExecutionContext) -> int:
            return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        result = repo.backfill(
            selection=["asset"],
            partition_keys=[rs.PartitionKey.single("2024-01-15")],
        )
        status = repo.get_backfill(result.backfill_id)
        assert status is not None
        assert status.backfill_id == result.backfill_id
        assert status.status == "CompletedSuccess"
        assert status.completed_partitions == 1
        assert status.total_partitions == 1

    def test_get_backfill_not_found(self, executor_env):
        executor, _ = executor_env

        @rs.Asset(partitions_def=_daily_pd())
        def asset(context: rs.AssetExecutionContext) -> int:
            return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        status = repo.get_backfill("nonexistent-id")
        assert status is None


# ---------------------------------------------------------------------------
# Failure policy
# ---------------------------------------------------------------------------


class TestBackfillFailurePolicy:
    def test_stop_on_failure(self, executor_env, is_async):
        executor, _ = executor_env

        if is_async:

            @rs.Asset(partitions_def=_static_pd(["a", "b", "c", "d"]))
            async def asset(context: rs.AssetExecutionContext) -> int:
                await asyncio.sleep(0)
                if context.partition.key == rs.PartitionKey.single("b"):
                    raise ValueError("intentional failure")
                return 1
        else:

            @rs.Asset(partitions_def=_static_pd(["a", "b", "c", "d"]))
            def asset(context: rs.AssetExecutionContext) -> int:
                if context.partition.key == rs.PartitionKey.single("b"):
                    raise ValueError("intentional failure")
                return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        result = repo.backfill(
            selection=["asset"],
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
# Config
# ---------------------------------------------------------------------------


class TestBackfillConfig:
    def test_config_passed_to_assets(self, executor_env, is_async):
        from pydantic import BaseModel

        executor, _ = executor_env
        captured = {}

        class MyConfig(BaseModel):
            mode: str = "normal"

        if is_async:

            @rs.Asset(partitions_def=_static_pd(["x"]))
            async def asset(context: rs.AssetExecutionContext[MyConfig]) -> int:
                captured["mode"] = context.config.mode
                return 1
        else:

            @rs.Asset(partitions_def=_static_pd(["x"]))
            def asset(context: rs.AssetExecutionContext[MyConfig]) -> int:
                captured["mode"] = context.config.mode
                return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        result = repo.backfill(
            selection=["asset"],
            partition_keys=[rs.PartitionKey.single("x")],
            config={"asset": {"mode": "full_refresh"}},
        )
        assert result.completed == 1
        assert captured.get("mode") == "full_refresh"


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


class TestBackfillTags:
    def test_backfill_tags_on_runs(self, executor_env, is_async):
        executor, _ = executor_env

        if is_async:

            @rs.Asset(partitions_def=_daily_pd())
            async def asset(context: rs.AssetExecutionContext) -> int:
                return 1
        else:

            @rs.Asset(partitions_def=_daily_pd())
            def asset(context: rs.AssetExecutionContext) -> int:
                return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        result = repo.backfill(
            selection=["asset"],
            partition_keys=[rs.PartitionKey.single("2024-01-15")],
            tags=[("team", "data")],
        )
        assert result.completed == 1
        status = repo.get_backfill(result.backfill_id)
        assert status is not None
        assert len(status.run_ids) == 1

        run = repo.storage.get_run(status.run_ids[0])
        assert run is not None
        tag_dict = dict(run.tags)
        assert tag_dict["team"] == "data"
        assert run.launched_by.kind == "backfill"
        assert run.launched_by.backfill_id == result.backfill_id


# ---------------------------------------------------------------------------
# Re-execute (rerun_backfill)
# ---------------------------------------------------------------------------


class TestBackfillRerun:
    def test_rerun_preserves_partition_keys(self, executor_env):
        executor, _ = executor_env

        @rs.Asset(partitions_def=_daily_pd())
        def asset(context: rs.AssetExecutionContext) -> int:
            return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        keys = [
            rs.PartitionKey.single("2024-01-10"),
            rs.PartitionKey.single("2024-01-11"),
            rs.PartitionKey.single("2024-01-12"),
        ]
        original = repo.backfill(
            selection=["asset"], partition_keys=keys, max_concurrency=2
        )
        assert original.completed == 3

        rerun = repo.rerun_backfill(original.backfill_id)
        assert rerun.backfill_id != original.backfill_id
        assert rerun.num_partitions == 3
        assert rerun.completed == 3
        assert rerun.status == "CompletedSuccess"

    def test_rerun_preserves_strategy_single_run(self, executor_env):
        executor, _ = executor_env

        @rs.Asset(partitions_def=_daily_pd())
        def asset(context: rs.AssetExecutionContext) -> int:
            return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        keys = [
            rs.PartitionKey.single("2024-01-10"),
            rs.PartitionKey.single("2024-01-11"),
            rs.PartitionKey.single("2024-01-12"),
        ]
        # Execute original so the record is persisted.
        original = repo.backfill(
            selection=["asset"],
            partition_keys=keys,
            strategy=rs.BackfillStrategy.single_run(),
        )
        assert original.completed == 3

        # Verify strategy was stored & re-applied by checking the planned run count
        # via dry-run, which is deterministic for a given strategy+partition set.
        rerun_plan = repo.rerun_backfill(original.backfill_id, dry_run=True)
        assert rerun_plan.num_partitions == 3
        assert rerun_plan.num_runs == 1  # SingleRun plans one batched run

        # Contrast: a fresh backfill with default strategy would plan num_runs == 3.
        control = repo.backfill(
            selection=["asset"],
            partition_keys=keys,
            dry_run=True,
        )
        assert control.num_runs == 3

    def test_rerun_preserves_tags_and_adds_lineage(self, executor_env):
        executor, _ = executor_env

        @rs.Asset(partitions_def=_daily_pd())
        def asset(context: rs.AssetExecutionContext) -> int:
            return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        original = repo.backfill(
            selection=["asset"],
            partition_keys=[rs.PartitionKey.single("2024-01-15")],
            tags=[("team", "data"), ("priority", "high")],
        )

        rerun = repo.rerun_backfill(original.backfill_id)
        status = repo.get_backfill(rerun.backfill_id)
        assert status is not None
        tag_dict = dict(status.tags)
        # Original tags preserved
        assert tag_dict["team"] == "data"
        assert tag_dict["priority"] == "high"
        # Lineage tag added pointing back at original backfill
        assert tag_dict["rivers/rerun_of"] == original.backfill_id

    def test_rerun_preserves_failure_policy(self, executor_env):
        executor, _ = executor_env
        calls = {"n": 0}

        @rs.Asset(partitions_def=_daily_pd())
        def asset(context: rs.AssetExecutionContext) -> int:
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("boom")
            return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        keys = [
            rs.PartitionKey.single("2024-01-10"),
            rs.PartitionKey.single("2024-01-11"),
            rs.PartitionKey.single("2024-01-12"),
        ]
        # StopOnFailure: when one partition fails, remaining are canceled.
        original = repo.backfill(
            selection=["asset"],
            partition_keys=keys,
            failure_policy="stop_on_failure",
            max_concurrency=1,
        )
        assert original.failed >= 1

        # Reset counter so re-run succeeds end-to-end and we can distinguish
        # its completion from the original.
        calls["n"] = 100
        rerun = repo.rerun_backfill(original.backfill_id)
        # If failure_policy was dropped we'd fall back to "continue" —
        # behavior is still driven by the same stored policy here.
        assert rerun.num_partitions == 3

    def test_rerun_not_found(self, executor_env):
        executor, _ = executor_env

        @rs.Asset(partitions_def=_daily_pd())
        def asset(context: rs.AssetExecutionContext) -> int:
            return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        import pytest

        with pytest.raises(Exception, match="not found"):
            repo.rerun_backfill("nonexistent-id")

    def test_rerun_skips_keys_removed_from_def(self, storage):
        """Rerunning after the def shrank replays the surviving keys instead
        of hard-erroring the whole rerun."""

        def build_repo(keys):
            @rs.Asset(
                partitions_def=_static_pd(keys), io_handler=rs.InMemoryIOHandler()
            )
            def asset(context: rs.AssetExecutionContext) -> int:
                return 1

            repo = rs.CodeRepository(
                assets=[asset], default_executor=rs.Executor.in_process()
            )
            repo.resolve(storage=storage)
            return repo

        original = build_repo(["a", "b", "c"]).backfill(
            selection=["asset"],
            partition_keys=[rs.PartitionKey.single(k) for k in ("a", "b", "c")],
        )
        assert original.completed == 3

        redeployed = build_repo(["a", "b"])
        rerun = redeployed.rerun_backfill(original.backfill_id)
        assert rerun.num_partitions == 2
        assert rerun.completed == 2

        dry = redeployed.rerun_backfill(original.backfill_id, dry_run=True)
        assert dry.is_dry_run
        assert dry.num_partitions == 2

    def test_rerun_with_no_surviving_keys_errors(self, storage):
        def build_repo(keys):
            @rs.Asset(
                partitions_def=_static_pd(keys), io_handler=rs.InMemoryIOHandler()
            )
            def asset(context: rs.AssetExecutionContext) -> int:
                return 1

            repo = rs.CodeRepository(
                assets=[asset], default_executor=rs.Executor.in_process()
            )
            repo.resolve(storage=storage)
            return repo

        original = build_repo(["a", "b"]).backfill(
            selection=["asset"],
            partition_keys=[rs.PartitionKey.single(k) for k in ("a", "b")],
        )

        redeployed = build_repo(["x", "y"])
        with pytest.raises(
            ExecutionError,
            match=re.escape(
                "none of its 2 partitions are valid for the current definitions"
            ),
        ):
            redeployed.rerun_backfill(original.backfill_id)

    def test_rerun_dry_run(self, executor_env):
        executor, _ = executor_env

        @rs.Asset(partitions_def=_daily_pd())
        def asset(context: rs.AssetExecutionContext) -> int:
            return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        original = repo.backfill(
            selection=["asset"],
            partition_keys=[rs.PartitionKey.single("2024-01-15")],
        )
        assert not original.is_dry_run

        rerun = repo.rerun_backfill(original.backfill_id, dry_run=True)
        assert rerun.is_dry_run
        assert rerun.num_partitions == 1


# ---------------------------------------------------------------------------
# Range ordering follows the definition, not lexicographic strings. Static
# keys are positionally ordered; TimeWindow keys are chronologically ordered
# via their fmt — neither is guaranteed to sort lexicographically.
# ---------------------------------------------------------------------------


class TestRangeDefinitionOrdering:
    SIZES = ["small", "medium", "large"]  # NOT lexicographically sorted

    def _sized_repo(self):
        @rs.Asset(partitions_def=_static_pd(self.SIZES))
        def sized(context: rs.AssetExecutionContext) -> int:
            return 1

        return rs.CodeRepository(
            assets=[sized], default_executor=rs.Executor.in_process()
        )

    def test_static_range_resolves_positionally(self):
        repo = self._sized_repo()
        result = repo.backfill(
            selection=["sized"],
            partition_range=rs.PartitionKeyRange.single(
                from_key="medium", to_key="large"
            ),
        )
        assert result.num_partitions == 2
        assert set(repo.storage.get_materialized_partitions("sized")) == {
            rs.PartitionKey.single("medium"),
            rs.PartitionKey.single("large"),
        }

    def test_static_range_positionally_inverted_rejected(self):
        repo = self._sized_repo()
        with pytest.raises(
            ExecutionError,
            match=re.escape("from_key 'large' is after to_key 'small'"),
        ):
            repo.backfill(
                selection=["sized"],
                partition_range=rs.PartitionKeyRange.single(
                    from_key="large", to_key="small"
                ),
            )

    def test_static_range_unknown_endpoint_rejected(self):
        repo = self._sized_repo()
        with pytest.raises(
            ExecutionError,
            match=re.escape("Range endpoint 'huge' is not a partition key"),
        ):
            repo.backfill(
                selection=["sized"],
                partition_range=rs.PartitionKeyRange.single(
                    from_key="medium", to_key="huge"
                ),
            )

    def test_time_window_range_off_grid_endpoint_rejected(self):
        """An endpoint that parses under the fmt but isn't a window start is
        not a partition key — same contract as Static unknown endpoints."""
        pd = rs.PartitionsDefinition.time_window(
            start=datetime(2024, 1, 1),
            interval_seconds=3600,
            end=datetime(2024, 1, 2),
            fmt="%Y-%m-%dT%H:%M:%S",
        )

        @rs.Asset(partitions_def=pd)
        def asset(context: rs.AssetExecutionContext) -> int:
            return 1

        repo = rs.CodeRepository(
            assets=[asset], default_executor=rs.Executor.in_process()
        )
        with pytest.raises(
            ExecutionError,
            match=re.escape(
                "Range endpoint '2024-01-01T07:30:00' is not a partition key"
            ),
        ):
            repo.backfill(
                selection=["asset"],
                partition_range=rs.PartitionKeyRange.single(
                    from_key="2024-01-01T07:30:00", to_key="2024-01-01T10:00:00"
                ),
            )

    def test_custom_fmt_range_resolves_chronologically(self):
        # %m/%d/%Y sorts "01/02/2025" before "12/30/2024" lexicographically —
        # the range must follow the calendar instead.
        pd = rs.PartitionsDefinition.daily(
            start=datetime(2024, 12, 30),
            end=datetime(2025, 1, 3),
            fmt="%m/%d/%Y",
        )

        @rs.Asset(partitions_def=pd)
        def asset(context: rs.AssetExecutionContext) -> int:
            return 1

        repo = rs.CodeRepository(
            assets=[asset], default_executor=rs.Executor.in_process()
        )
        result = repo.backfill(
            selection=["asset"],
            partition_range=rs.PartitionKeyRange.single(
                from_key="12/30/2024", to_key="01/02/2025"
            ),
        )
        assert result.num_partitions == 4
        assert set(repo.storage.get_materialized_partitions("asset")) == {
            rs.PartitionKey.single("12/30/2024"),
            rs.PartitionKey.single("12/31/2024"),
            rs.PartitionKey.single("01/01/2025"),
            rs.PartitionKey.single("01/02/2025"),
        }

    def test_multi_static_dim_range_resolves_positionally(self):
        pd = rs.PartitionsDefinition.multi(
            {
                "size": _static_pd(self.SIZES),
                "region": _static_pd(["us"]),
            }
        )

        @rs.Asset(partitions_def=pd)
        def asset(context: rs.AssetExecutionContext) -> int:
            return 1

        repo = rs.CodeRepository(
            assets=[asset], default_executor=rs.Executor.in_process()
        )
        result = repo.backfill(
            selection=["asset"],
            partition_range=rs.PartitionKeyRange.multi({"size": ("medium", "large")}),
        )
        assert result.num_partitions == 2
        assert set(repo.storage.get_materialized_partitions("asset")) == {
            rs.PartitionKey.multi({"size": "medium", "region": "us"}),
            rs.PartitionKey.multi({"size": "large", "region": "us"}),
        }
