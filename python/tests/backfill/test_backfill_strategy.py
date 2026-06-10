"""Integration tests for BackfillStrategy, SingleRun, PerDimension, mark_partition_failed.

Covers plain @rs.Asset across executor (in_process, parallel) and sync/async.
"""

import asyncio
import re
from datetime import datetime

import pytest
import rivers as rs
from rivers.exceptions import ExecutionError

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

    def test_per_dimension_sparse_multi_dim_no_over_inclusion(self, executor_env):
        executor, _ = executor_env
        pd = rs.PartitionsDefinition.multi(
            {
                "region": rs.PartitionsDefinition.static_(["us", "eu"]),
                "date": rs.PartitionsDefinition.static_(["d1", "d2"]),
                "hour": rs.PartitionsDefinition.static_(["h1", "h2"]),
            }
        )
        keys_per_call: list[int] = []

        @rs.Asset(partitions_def=pd)
        def asset(context: rs.AssetExecutionContext) -> int:
            keys_per_call.append(len(context.partition.keys))
            return 1

        repo = rs.CodeRepository(assets=[asset], default_executor=executor)
        # Sparse within region=us: (d1,h1) and (d2,h2) only — not the 2x2 cartesian.
        result = repo.backfill(
            selection=["asset"],
            partition_keys=[
                rs.PartitionKey.multi({"region": "us", "date": "d1", "hour": "h1"}),
                rs.PartitionKey.multi({"region": "us", "date": "d2", "hour": "h2"}),
            ],
            strategy=rs.BackfillStrategy.per_dimension(
                multi_run=["region"], single_run=["date", "hour"]
            ),
        )
        # Exactly the 2 selected partitions run — the cartesian closure (4) does not.
        assert result.num_partitions == 2
        assert result.completed == 2
        # One batched run (the us group, bundled as an explicit Set), invoked once.
        assert len(result.run_ids) == 1
        assert keys_per_call == [2]


# ---------------------------------------------------------------------------
# Per-partition partial failure (mark_partition_failed) within a batched run
# ---------------------------------------------------------------------------


class TestBatchedPartialFailure:
    def _assert_partition_failure_event(self, repo, asset_key, key="b", error="boom"):
        # The marked partition must surface as a per-partition StepFailure event
        # (partition_key set + the user's error), not just an absent materialization.
        events = repo.storage.get_events_for_asset(asset_key)
        failures = [e for e in events if e.event_type == "StepFailure"]
        assert len(failures) == 1, [e.event_type for e in failures]
        assert failures[0].partition_key == rs.PartitionKey.single(key)
        assert dict(failures[0].metadata).get("error") == error

    def _assert_single_run_partial_failure(
        self, executor, io_handler=None, is_async=False
    ):
        kwargs = {"io_handler": io_handler} if io_handler is not None else {}

        if is_async:

            @rs.Asset(partitions_def=_static_pd(["a", "b", "c"]), **kwargs)
            async def asset(context: rs.AssetExecutionContext) -> int:
                await asyncio.sleep(0)
                context.mark_partition_failed(rs.PartitionKey.single("b"), error="boom")
                return 1
        else:

            @rs.Asset(partitions_def=_static_pd(["a", "b", "c"]), **kwargs)
            def asset(context: rs.AssetExecutionContext) -> int:
                context.mark_partition_failed(rs.PartitionKey.single("b"), error="boom")
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
        # One batched run, but a per-partition outcome: a, c done; b failed.
        assert len(result.run_ids) == 1
        assert result.completed == 2
        assert result.failed == 1
        # The persisted backfill record agrees on the counts.
        status = repo.get_backfill(result.backfill_id)
        assert (status.completed_partitions, status.failed_partitions) == (2, 1)
        # Only the succeeded partitions are materialized.
        assert set(repo.storage.get_materialized_partitions("asset")) == {
            rs.PartitionKey.single("a"),
            rs.PartitionKey.single("c"),
        }
        self._assert_partition_failure_event(repo, "asset")

    def test_single_run_partial_failure(self):
        self._assert_single_run_partial_failure(rs.Executor.in_process())

    def test_single_run_partial_failure_async(self):
        self._assert_single_run_partial_failure(rs.Executor.in_process(), is_async=True)

    def test_single_run_partial_failure_parallel(self, tmp_path):
        # The parallel backend short-circuits a *single* sync step to in-process
        # (execute.rs), so a lone asset never reaches a loky worker. Add an
        # independent sibling to force ≥2 sync instances onto the worker path;
        # the marking asset must then carry its marks back across the worker
        # boundary for the partition to be recorded failed.
        import obstore

        store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
        io = rs.PickleIOHandler(store=store)

        @rs.Asset(partitions_def=_static_pd(["a", "b", "c"]), io_handler=io)
        def marks(context: rs.AssetExecutionContext) -> int:
            context.mark_partition_failed(rs.PartitionKey.single("b"), error="boom")
            return 1

        @rs.Asset(partitions_def=_static_pd(["a", "b", "c"]), io_handler=io)
        def sibling(context: rs.AssetExecutionContext) -> int:
            return 1  # forces ≥2 sync instances → loky worker path

        repo = rs.CodeRepository(
            assets=[marks, sibling],
            default_executor=rs.Executor.parallel(max_workers=2),
        )
        repo.backfill(
            selection=["marks", "sibling"],
            partition_keys=[rs.PartitionKey.single(k) for k in ("a", "b", "c")],
            strategy=rs.BackfillStrategy.single_run(),
        )
        # The marking asset's failed partition must NOT be materialized.
        assert set(repo.storage.get_materialized_partitions("marks")) == {
            rs.PartitionKey.single("a"),
            rs.PartitionKey.single("c"),
        }
        self._assert_partition_failure_event(repo, "marks")

    def test_single_run_partial_failure_generator(self):
        # Single-output generator: marks are set only as the generator body runs
        # (lazily, during output iteration), so they must be drained from the
        # generator context before emission.
        handler = rs.InMemoryIOHandler()

        @rs.Asset.from_multi(
            partitions_def=_static_pd(["a", "b", "c"]),
            output_defs=[rs.AssetDef("gen", io_handler=handler)],
        )
        def gen_asset(context: rs.AssetExecutionContext):
            context.mark_partition_failed(rs.PartitionKey.single("b"), error="boom")
            yield rs.Output(value=1, output_name="gen")

        repo = rs.CodeRepository(
            assets=[gen_asset], default_executor=rs.Executor.in_process()
        )
        repo.backfill(
            selection=["gen"],
            partition_keys=[rs.PartitionKey.single(k) for k in ("a", "b", "c")],
            strategy=rs.BackfillStrategy.single_run(),
        )
        assert set(repo.storage.get_materialized_partitions("gen")) == {
            rs.PartitionKey.single("a"),
            rs.PartitionKey.single("c"),
        }
        self._assert_partition_failure_event(repo, "gen")

    def test_single_run_partial_failure_multi_asset(self):
        # The mark is step-level, so it must apply to every output of a
        # multi-asset (lookup keyed by output name, not the step name).
        handler = rs.InMemoryIOHandler()

        @rs.Asset.from_multi(
            partitions_def=_static_pd(["a", "b", "c"]),
            output_defs=[
                rs.AssetDef("x", io_handler=handler),
                rs.AssetDef("y", io_handler=handler),
            ],
        )
        def multi(context: rs.AssetExecutionContext):
            context.mark_partition_failed(rs.PartitionKey.single("b"), error="boom")
            yield rs.Output(value=1, output_name="x")
            yield rs.Output(value=2, output_name="y")

        repo = rs.CodeRepository(
            assets=[multi], default_executor=rs.Executor.in_process()
        )
        repo.backfill(
            selection=["x", "y"],
            partition_keys=[rs.PartitionKey.single(k) for k in ("a", "b", "c")],
            strategy=rs.BackfillStrategy.single_run(),
        )
        for out in ("x", "y"):
            assert set(repo.storage.get_materialized_partitions(out)) == {
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("c"),
            }, out
            self._assert_partition_failure_event(repo, out)

    def test_partial_failure_run_progress_not_inflated(self):
        # A batched partial-failure run is ONE step; its per-partition
        # StepFailure events must not inflate run progress past total_steps.
        @rs.Asset(partitions_def=_static_pd(["a", "b", "c"]))
        def asset(context: rs.AssetExecutionContext) -> int:
            context.mark_partition_failed(rs.PartitionKey.single("b"), error="boom")
            return 1

        repo = rs.CodeRepository(
            assets=[asset], default_executor=rs.Executor.in_process()
        )
        result = repo.backfill(
            selection=["asset"],
            partition_keys=[rs.PartitionKey.single(k) for k in ("a", "b", "c")],
            strategy=rs.BackfillStrategy.single_run(),
        )
        completed, total = repo.storage.get_run_progress(result.run_ids[0])
        assert (completed, total) == (1, 1), (completed, total)

    def test_stop_on_failure_reconciles_partial_failure_credit(self):
        # Group A partial-fails (marks A/d1, succeeds), group B hard-fails (→
        # stop), group C is canceled. The stop-on-failure terminal path must
        # still reconcile A/d1 as failed, not leave it credited completed.
        pd = rs.PartitionsDefinition.multi(
            {
                "region": rs.PartitionsDefinition.static_(["A", "B", "C"]),
                "date": rs.PartitionsDefinition.static_(["d1", "d2"]),
            }
        )
        a_d1 = rs.PartitionKey.multi({"region": "A", "date": "d1"})
        b_d1 = rs.PartitionKey.multi({"region": "B", "date": "d1"})

        @rs.Asset(partitions_def=pd)
        def asset(context: rs.AssetExecutionContext) -> int:
            keys = set(context.partition.keys)
            if b_d1 in keys:
                raise RuntimeError("boom")  # region B hard-fails → stop
            if a_d1 in keys:
                context.mark_partition_failed(a_d1, error="partial")
            return 1

        repo = rs.CodeRepository(
            assets=[asset], default_executor=rs.Executor.in_process()
        )
        result = repo.backfill(
            selection=["asset"],
            partition_keys=[
                rs.PartitionKey.multi({"region": r, "date": d})
                for r in ("A", "B", "C")
                for d in ("d1", "d2")
            ],
            strategy=rs.BackfillStrategy.per_dimension(
                multi_run=["region"], single_run=["date"]
            ),
            failure_policy="stop_on_failure",
        )
        assert result.status == "CompletedFailed", result.status
        assert result.completed == 1, result.completed  # A/d2
        assert result.failed == 3, result.failed  # A/d1, B/d1, B/d2
        assert result.canceled == 2, result.canceled  # C/d1, C/d2

    def test_per_dimension_partial_failure_keyed_by_run(self):
        # Two Success runs each mark a different partition; the batched crediting
        # query must attribute each failure to its own run.
        pd = rs.PartitionsDefinition.multi(
            {
                "region": rs.PartitionsDefinition.static_(["A", "B"]),
                "date": rs.PartitionsDefinition.static_(["d1", "d2"]),
            }
        )
        a_d1 = rs.PartitionKey.multi({"region": "A", "date": "d1"})
        b_d2 = rs.PartitionKey.multi({"region": "B", "date": "d2"})

        @rs.Asset(partitions_def=pd)
        def asset(context: rs.AssetExecutionContext) -> int:
            keys = set(context.partition.keys)
            if a_d1 in keys:
                context.mark_partition_failed(a_d1, error="a-fail")
            if b_d2 in keys:
                context.mark_partition_failed(b_d2, error="b-fail")
            return 1

        repo = rs.CodeRepository(
            assets=[asset], default_executor=rs.Executor.in_process()
        )
        result = repo.backfill(
            selection=["asset"],
            partition_keys=[
                rs.PartitionKey.multi({"region": r, "date": d})
                for r in ("A", "B")
                for d in ("d1", "d2")
            ],
            strategy=rs.BackfillStrategy.per_dimension(
                multi_run=["region"], single_run=["date"]
            ),
        )
        assert result.completed == 2, result.completed  # A/d2, B/d1
        assert result.failed == 2, result.failed  # A/d1, B/d2
        assert set(repo.storage.get_materialized_partitions("asset")) == {
            rs.PartitionKey.multi({"region": "A", "date": "d2"}),
            rs.PartitionKey.multi({"region": "B", "date": "d1"}),
        }


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


class TestPartitionedRaiseRecordsFailure:
    """A genuine ``raise`` records the failed partition(s) as a per-partition
    StepFailure (so automation sees them); a raised batch is one Set-keyed
    StepFailure, not one event per partition."""

    def test_multi_run_raise_records_partition_keyed_failure(self):
        handler = rs.InMemoryIOHandler()

        @rs.Asset(partitions_def=_static_pd(["a", "b", "c"]), io_handler=handler)
        def widget(context: rs.AssetExecutionContext) -> int:
            if rs.PartitionKey.single("b") in context.partition.keys:
                raise ValueError("boom")
            return 1

        repo = rs.CodeRepository(
            assets=[widget], default_executor=rs.Executor.in_process()
        )
        repo.backfill(
            selection=["widget"],
            partition_keys=[rs.PartitionKey.single(k) for k in ("a", "b", "c")],
            strategy=rs.BackfillStrategy.multi_run(),
        )
        # a, c materialized; b raised (its own per-partition run failed).
        assert set(repo.storage.get_materialized_partitions("widget")) == {
            rs.PartitionKey.single("a"),
            rs.PartitionKey.single("c"),
        }
        # b's run records a partition-keyed StepFailure so eager sees 'b' failed.
        events = repo.storage.get_events_for_asset("widget")
        pfails = [
            e
            for e in events
            if e.event_type == "StepFailure" and e.partition_key is not None
        ]
        assert [e.partition_key for e in pfails] == [rs.PartitionKey.single("b")]
        assert "boom" in dict(pfails[0].metadata).get("error", "")

    def test_single_run_raise_records_one_set_failure(self):
        handler = rs.InMemoryIOHandler()

        @rs.Asset(partitions_def=_static_pd(["a", "b", "c"]), io_handler=handler)
        def widget(context: rs.AssetExecutionContext) -> int:
            raise ValueError("boom")

        repo = rs.CodeRepository(
            assets=[widget], default_executor=rs.Executor.in_process()
        )
        repo.backfill(
            selection=["widget"],
            partition_keys=[rs.PartitionKey.single(k) for k in ("a", "b", "c")],
            strategy=rs.BackfillStrategy.single_run(),
        )
        # The whole batch raised → nothing materialized.
        assert set(repo.storage.get_materialized_partitions("widget")) == set()
        # Exactly ONE partition-keyed StepFailure (the whole Set), not one per partition.
        events = repo.storage.get_events_for_asset("widget")
        pfails = [
            e
            for e in events
            if e.event_type == "StepFailure" and e.partition_key is not None
        ]
        assert len(pfails) == 1, [e.partition_key for e in pfails]


# ---------------------------------------------------------------------------
# Strategy ↔ definition validation — a typo'd dimension name or a
# PerDimension strategy on a single-dim asset would otherwise silently
# collapse the whole backfill into ONE run.
# ---------------------------------------------------------------------------

UNKNOWN_DIM_MSG = (
    "BackfillStrategy.per_dimension references dimension '{dim}', "
    "which is not a dimension of asset 'asset'"
)
NOT_MULTI_MSG = (
    "BackfillStrategy.per_dimension requires Multi-partitioned assets; "
    "asset 'asset' is not Multi-partitioned"
)


class TestPerDimensionValidation:
    def _multi_repo(self):
        @rs.Asset(partitions_def=_multi_pd())
        def asset(context: rs.AssetExecutionContext) -> int:
            return 1

        return rs.CodeRepository(
            assets=[asset], default_executor=rs.Executor.in_process()
        )

    def test_unknown_multi_run_dim_rejected(self):
        repo = self._multi_repo()
        with pytest.raises(
            ExecutionError, match=re.escape(UNKNOWN_DIM_MSG.format(dim="reigon"))
        ):
            repo.backfill(
                selection=["asset"],
                partition_keys=[rs.PartitionKey.multi({"region": "us", "date": "d1"})],
                strategy=rs.BackfillStrategy.per_dimension(
                    multi_run=["reigon"], single_run=["date"]
                ),
            )
        assert repo.storage.get_runs(limit=10) == []

    def test_unknown_single_run_dim_rejected(self):
        repo = self._multi_repo()
        with pytest.raises(
            ExecutionError, match=re.escape(UNKNOWN_DIM_MSG.format(dim="daet"))
        ):
            repo.backfill(
                selection=["asset"],
                partition_keys=[rs.PartitionKey.multi({"region": "us", "date": "d1"})],
                strategy=rs.BackfillStrategy.per_dimension(
                    multi_run=["region"], single_run=["daet"]
                ),
            )

    def test_per_dimension_on_single_dim_asset_rejected(self):
        @rs.Asset(partitions_def=_static_pd(["a", "b"]))
        def asset(context: rs.AssetExecutionContext) -> int:
            return 1

        repo = rs.CodeRepository(
            assets=[asset], default_executor=rs.Executor.in_process()
        )
        with pytest.raises(ExecutionError, match=re.escape(NOT_MULTI_MSG)):
            repo.backfill(
                selection=["asset"],
                partition_keys=[rs.PartitionKey.single("a")],
                strategy=rs.BackfillStrategy.per_dimension(
                    multi_run=["region"], single_run=["date"]
                ),
            )

    def test_dry_run_also_validates(self):
        repo = self._multi_repo()
        with pytest.raises(
            ExecutionError, match=re.escape(UNKNOWN_DIM_MSG.format(dim="reigon"))
        ):
            repo.backfill(
                selection=["asset"],
                partition_keys=[rs.PartitionKey.multi({"region": "us", "date": "d1"})],
                strategy=rs.BackfillStrategy.per_dimension(
                    multi_run=["reigon"], single_run=["date"]
                ),
                dry_run=True,
            )
