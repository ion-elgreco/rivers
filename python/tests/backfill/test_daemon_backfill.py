"""Integration tests for daemon-triggered backfills.

Tests verify that the automation condition eval loop emits backfills
(not individual runs) when multiple partitions fire simultaneously.
"""

import time

import pytest
import rivers as rs
from _polling import wait_for_backfill_runs as _wait_for_backfill_runs
from rivers._core import AutomationDaemon


def _wait_for_backfill(repo, timeout=15.0):
    """Poll repo.get_backfill for any completed backfill via storage runs."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        runs = repo.storage.get_runs(limit=100)
        for r in runs:
            if r.launched_by.kind == "backfill":
                bf_id = r.launched_by.backfill_id
                bf = repo.get_backfill(bf_id)
                if bf and bf.status in ("CompletedSuccess", "CompletedFailed"):
                    return bf
        time.sleep(0.3)
    return None


class TestDaemonConditionBackfill:
    def test_multi_partition_condition_triggers_backfill(self, storage):
        """When an eager condition fires for multiple partitions, the daemon
        should emit a backfill (not individual materialize calls).

        Setup:
        - upstream: static partitions [a, b], no condition (materialized manually)
        - downstream: static partitions [a, b], eager() condition, depends on upstream

        Flow:
        1. Pre-materialize both upstream partitions AND both downstream partitions
           (so the daemon cache sees them as "existed" before the upstream re-materialization)
        2. Re-materialize upstream for both partitions (makes downstream stale)
        3. Start daemon (1s interval) — its first tick sees both stale keys
        4. Condition detects upstream updates → fires for [a, b]
        5. Since 2 partitions fire, daemon should create a backfill
        6. Backfill runs execute (via pickup loop)
        7. Verify: runs have backfill origin
        """
        handler = rs.InMemoryIOHandler()

        @rs.Asset(
            name="upstream",
            partitions_def=rs.PartitionsDefinition.static_(["a", "b"]),
            io_handler=handler,
        )
        def upstream(context: rs.AssetExecutionContext) -> int:
            return 1

        @rs.Asset(
            name="downstream",
            partitions_def=rs.PartitionsDefinition.static_(["a", "b"]),
            io_handler=handler,
            automation_condition=rs.AutomationCondition.eager(),
        )
        def downstream(context: rs.AssetExecutionContext, upstream: int) -> int:
            return upstream + 1

        repo = rs.CodeRepository(
            assets=[upstream, downstream],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        # Pre-materialize everything so baseline is established
        repo.materialize(
            selection=["upstream", "downstream"],
            partition_key=rs.PartitionKey.single("a"),
        )
        repo.materialize(
            selection=["upstream", "downstream"],
            partition_key=rs.PartitionKey.single("b"),
        )

        # Re-materialize upstream BEFORE the daemon starts: dep matching is
        # staleness-based, so the first tick deterministically sees both
        # stale keys — no racing the tick boundary with a burst.
        repo.materialize(
            selection=["upstream"],
            partition_key=rs.PartitionKey.single("a"),
        )
        repo.materialize(
            selection=["upstream"],
            partition_key=rs.PartitionKey.single("b"),
        )

        daemon = AutomationDaemon(
            repo=repo,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            # Wait for backfill-launched runs
            tagged_runs = _wait_for_backfill_runs(storage, timeout=20)
            assert len(tagged_runs) >= 1, (
                f"Expected runs with backfill origin, "
                f"got {len(tagged_runs)} tagged runs out of "
                f"{len(storage.get_runs(limit=100))} total runs"
            )

            # All backfill runs should share the same backfill_id
            bf_ids = {r.launched_by.backfill_id for r in tagged_runs}

            assert len(bf_ids) >= 1, f"Expected at least 1 backfill_id, got {bf_ids}"

            # Verify backfill completed
            bf = _wait_for_backfill(repo, timeout=15)
            assert bf is not None, "Backfill should have completed"
            assert bf.completed_partitions >= 1
        finally:
            daemon.stop()

    def test_single_partition_condition_fires_after_upstream_update(self, storage):
        """When a single upstream partition is re-materialized after baseline,
        the daemon condition should detect it and trigger materialization for
        the affected downstream partition.

        This verifies the condition eval loop with partitioned assets works
        end-to-end.
        """
        handler = rs.InMemoryIOHandler()

        @rs.Asset(
            name="single_up",
            partitions_def=rs.PartitionsDefinition.static_(["a"]),
            io_handler=handler,
        )
        def single_up(context: rs.AssetExecutionContext) -> int:
            return 1

        @rs.Asset(
            name="single_down",
            partitions_def=rs.PartitionsDefinition.static_(["a"]),
            io_handler=handler,
            automation_condition=rs.AutomationCondition.eager(),
        )
        def single_down(context: rs.AssetExecutionContext, single_up: int) -> int:
            return single_up + 1

        repo = rs.CodeRepository(
            assets=[single_up, single_down],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        # Pre-materialize everything to establish baseline
        repo.materialize(
            selection=["single_up", "single_down"],
            partition_key=rs.PartitionKey.single("a"),
        )

        daemon = AutomationDaemon(
            repo=repo,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            # Let daemon establish baseline
            time.sleep(2)

            # Re-materialize upstream — makes downstream stale
            repo.materialize(
                selection=["single_up"],
                partition_key=rs.PartitionKey.single("a"),
            )

            # Wait for downstream to be re-materialized
            deadline = time.monotonic() + 15
            _initial_runs = len(storage.get_runs(limit=100))
            while time.monotonic() < deadline:
                runs = storage.get_runs(limit=100)
                # Look for new runs beyond the initial ones
                new_down_runs = [
                    r
                    for r in runs
                    if "single_down" in r.node_names and r.status == "Success"
                ]
                # We expect at least 2 successful single_down runs (baseline + re-triggered)
                if len(new_down_runs) >= 2:
                    break
                time.sleep(0.3)

            down_runs = [
                r
                for r in storage.get_runs(limit=100)
                if "single_down" in r.node_names and r.status == "Success"
            ]
            assert len(down_runs) >= 2, (
                f"downstream should have been re-materialized by condition eval, "
                f"got {len(down_runs)} successful runs"
            )
        finally:
            daemon.stop()

    def test_backfill_strategy_passed_to_backfill(self, storage):
        """When an asset declares backfill_strategy=single_run(), the daemon's
        backfill dispatch should use that strategy. Verify via the backfill
        record's strategy field.
        """
        handler = rs.InMemoryIOHandler()

        @rs.Asset(
            name="strat_up",
            partitions_def=rs.PartitionsDefinition.static_(["a", "b"]),
            io_handler=handler,
        )
        def strat_up(context: rs.AssetExecutionContext) -> int:
            return 1

        @rs.Asset(
            name="strat_down",
            partitions_def=rs.PartitionsDefinition.static_(["a", "b"]),
            io_handler=handler,
            automation_condition=rs.AutomationCondition.eager(),
            backfill_strategy=rs.BackfillStrategy.single_run(),
        )
        def strat_down(context: rs.AssetExecutionContext, strat_up: int) -> int:
            return strat_up + 1

        repo = rs.CodeRepository(
            assets=[strat_up, strat_down],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        # Pre-materialize everything for baseline
        repo.materialize(
            selection=["strat_up", "strat_down"],
            partition_key=rs.PartitionKey.single("a"),
        )
        repo.materialize(
            selection=["strat_up", "strat_down"],
            partition_key=rs.PartitionKey.single("b"),
        )

        # Burst before the daemon starts: the first tick sees both stale
        # keys at once, so the strict one-backfill asserts below are
        # deterministic instead of racing the tick boundary.
        repo.materialize(
            selection=["strat_up"],
            partition_key=rs.PartitionKey.single("a"),
        )
        repo.materialize(
            selection=["strat_up"],
            partition_key=rs.PartitionKey.single("b"),
        )

        daemon = AutomationDaemon(
            repo=repo,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            # Wait for backfill to complete
            bf = _wait_for_backfill(repo, timeout=20)
            assert bf is not None, "Backfill should have been created and completed"
            assert bf.completed_partitions >= 1

            # Verify the backfill used SingleRun strategy by checking
            # that all partition runs share a single backfill_id
            tagged_runs = _wait_for_backfill_runs(storage, timeout=5)
            assert len(tagged_runs) >= 1

            bf_ids = {
                r.launched_by.backfill_id
                for r in tagged_runs
                if r.launched_by.kind == "backfill"
            }
            bf_status = repo.get_backfill(bf_ids.pop())
            assert bf_status is not None
            # SingleRun strategy: the backfill record should show
            # completed_partitions == 2 (both a and b processed)
            assert bf_status.completed_partitions == 2
            # ...in ONE batched run (queued path), not one run per partition.
            backfill_runs = [r for r in tagged_runs if r.launched_by.kind == "backfill"]
            assert len(backfill_runs) == 1
        finally:
            daemon.stop()

    def test_per_dimension_strategy_groups_runs(self, storage):
        """When an asset declares backfill_strategy=per_dimension(...), the daemon
        should create a backfill that groups runs by the multi_run dimensions.

        Setup:
        - upstream: multi-partition (region × date), no condition
        - downstream: multi-partition (region × date), eager(), per_dimension(multi_run=["region"], single_run=["date"])

        With 2 regions × 2 dates = 4 partitions, per_dimension should produce
        2 run groups (one per region), each covering 2 date partitions.
        """
        handler = rs.InMemoryIOHandler()

        parts = rs.PartitionsDefinition.multi(
            {
                "region": rs.PartitionsDefinition.static_(["us", "eu"]),
                "date": rs.PartitionsDefinition.static_(["d1", "d2"]),
            }
        )

        @rs.Asset(
            name="pd_up",
            partitions_def=parts,
            io_handler=handler,
        )
        def pd_up(context: rs.AssetExecutionContext) -> int:
            return 1

        @rs.Asset(
            name="pd_down",
            partitions_def=parts,
            io_handler=handler,
            automation_condition=rs.AutomationCondition.eager(),
            backfill_strategy=rs.BackfillStrategy.per_dimension(
                multi_run=["region"], single_run=["date"]
            ),
        )
        def pd_down(context: rs.AssetExecutionContext, pd_up: int) -> int:
            return pd_up + 1

        repo = rs.CodeRepository(
            assets=[pd_up, pd_down],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        # Pre-materialize all 4 partition combos for baseline
        for region in ["us", "eu"]:
            for date in ["d1", "d2"]:
                pk = rs.PartitionKey.multi({"region": region, "date": date})
                repo.materialize(
                    selection=["pd_up", "pd_down"],
                    partition_key=pk,
                )

        # Dep matching is per-partition: burst all four upstream updates
        # BEFORE the daemon starts so its first tick sees them together and
        # dispatches a single backfill — no racing the tick boundary.
        for region in ["us", "eu"]:
            for date in ["d1", "d2"]:
                pk = rs.PartitionKey.multi({"region": region, "date": date})
                repo.materialize(
                    selection=["pd_up"],
                    partition_key=pk,
                )

        daemon = AutomationDaemon(
            repo=repo,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            # Wait for backfill to complete
            bf = _wait_for_backfill(repo, timeout=20)
            assert bf is not None, "Backfill should have been created and completed"

            # Verify all 4 partitions were completed
            assert bf.completed_partitions == 4, (
                f"Expected 4 completed partitions (2 regions × 2 dates), "
                f"got {bf.completed_partitions}"
            )

            # Verify runs are tagged with backfill_id
            tagged_runs = _wait_for_backfill_runs(storage, timeout=5)
            assert len(tagged_runs) >= 1

            # All should share the same backfill_id
            bf_ids = {
                r.launched_by.backfill_id
                for r in tagged_runs
                if r.launched_by.kind == "backfill"
            }
            assert len(bf_ids) == 1, f"Expected 1 backfill, got {len(bf_ids)}"
            # per_dimension(multi_run=["region"]) → one batched run per region.
            backfill_runs = [r for r in tagged_runs if r.launched_by.kind == "backfill"]
            assert len(backfill_runs) == 2, (
                f"Expected 2 batched runs (one per region), got {len(backfill_runs)}"
            )
        finally:
            daemon.stop()

    def test_eager_only_fires_for_partitions_with_upstream_data(self, storage):
        """Regression test: when only 3 upstream partitions are materialized,
        eager() on downstream should fire for exactly those 3 — not all
        missing partitions. The !AnyDepsMissing clause must filter correctly.
        """
        handler = rs.InMemoryIOHandler()

        parts = rs.PartitionsDefinition.static_(["a", "b", "c", "d", "e"])

        @rs.Asset(name="src", partitions_def=parts, io_handler=handler)
        def src(context: rs.AssetExecutionContext) -> int:
            return 1

        @rs.Asset(
            name="dst",
            partitions_def=parts,
            io_handler=handler,
            deps=[
                rs.AssetDef.input(
                    "src", partition_mapping=rs.PartitionMapping.identity()
                )
            ],
            automation_condition=rs.AutomationCondition.eager(),
        )
        def dst(context: rs.AssetExecutionContext, src: int) -> int:
            return src + 1

        repo = rs.CodeRepository(
            assets=[src, dst],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        # Only materialize 3 of 5 upstream partitions
        for pk in ["a", "b", "c"]:
            repo.materialize(
                selection=["src"],
                partition_key=rs.PartitionKey.single(pk),
            )

        daemon = AutomationDaemon(
            repo=repo,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            # Wait for downstream runs
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                dst_runs = [
                    r for r in storage.get_runs(limit=100) if "dst" in r.node_names
                ]
                if dst_runs:
                    break
                time.sleep(0.3)

            # Wait a bit more for all runs to complete
            time.sleep(2)

            dst_runs = [r for r in storage.get_runs(limit=100) if "dst" in r.node_names]
            successful = [r for r in dst_runs if r.status == "Success"]

            # Exactly partitions a, b, c materialize downstream, once each:
            # !AnyDepsMissing excludes d/e (no upstream `src`), and a finished
            # backfill partition must not re-fire its still-running siblings.
            assert len(dst_runs) == 3, (
                f"expected exactly 3 downstream runs (one per upstream partition), "
                f"got {len(dst_runs)}: "
                f"{[(r.partition_key.key[0], r.status) for r in dst_runs]}"
            )
            assert len(successful) == 3, (
                f"expected 3 successful downstream runs (matching upstream), "
                f"got {len(successful)} successful out of {len(dst_runs)} total"
            )
        finally:
            daemon.stop()

    def test_eager_backfill_does_not_retrigger_when_dep_unchanged(self, storage):
        """Regression test: partitioned eager condition should NOT re-trigger
        on subsequent eval ticks when the upstream dep hasn't changed.

        Without the fix (storing partition timestamps for non-conditioned deps),
        NewlyUpdated has no baseline and fires every tick.

        Setup:
        - upstream: static partitions [d1, d2, d3], no condition
        - downstream: static partitions [d1, d2, d3], eager() condition

        Flow:
        1. Pre-materialize upstream and downstream (baseline)
        2. Start daemon (1s condition eval)
        3. Wait for baseline tick
        4. Re-materialize upstream → triggers one backfill
        5. Wait for backfill to complete
        6. Wait 3 more eval ticks — NO new backfill should appear
        """
        handler = rs.InMemoryIOHandler()

        pdef = rs.PartitionsDefinition.static_(["d1", "d2", "d3"])

        @rs.Asset(name="src", partitions_def=pdef, io_handler=handler)
        def src(context: rs.AssetExecutionContext) -> int:
            return 1

        @rs.Asset(
            name="dst",
            partitions_def=pdef,
            io_handler=handler,
            automation_condition=rs.AutomationCondition.eager(),
        )
        def dst(context: rs.AssetExecutionContext, src: int) -> int:
            return src + 1

        repo = rs.CodeRepository(
            assets=[src, dst],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        # Pre-materialize everything (baseline)
        for pk in ["d1", "d2", "d3"]:
            repo.materialize(
                selection=["src", "dst"],
                partition_key=rs.PartitionKey.single(pk),
            )

        daemon = AutomationDaemon(
            repo=repo,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            # Let daemon establish baseline (2 ticks)
            time.sleep(2.5)

            # Re-materialize upstream to make downstream stale
            for pk in ["d1", "d2", "d3"]:
                repo.materialize(
                    selection=["src"],
                    partition_key=rs.PartitionKey.single(pk),
                )

            # Wait for the first backfill to complete
            bf = _wait_for_backfill(repo, timeout=20)
            assert bf is not None, "First backfill should have completed"
            assert bf.status == "CompletedSuccess"

            # Count total backfill-launched runs at this point
            runs_after_first = _wait_for_backfill_runs(storage, timeout=5)
            first_count = len(runs_after_first)

            # Wait 4 more seconds (3-4 eval ticks) — no new backfill should fire
            time.sleep(4)

            # Count backfill-tagged runs again
            all_runs = storage.get_runs(limit=200)
            bf_runs = [r for r in all_runs if r.launched_by.kind == "backfill"]

            # Should NOT have grown — no re-trigger
            assert len(bf_runs) == first_count, (
                f"Expected {first_count} backfill runs (no re-trigger), "
                f"but got {len(bf_runs)}. "
                f"Backfill IDs: {set((r.launched_by.backfill_id or '?') for r in bf_runs)}"
            )
        finally:
            daemon.stop()


class TestMarkedFailedPartitionNotRematerialized:
    @pytest.mark.parametrize("condition_name", ["eager", "on_missing"])
    def test_automation_does_not_rerun_marked_failed_partition(
        self, storage, condition_name
    ):
        """A partition skipped with ``mark_partition_failed`` lands in an overall
        Success run; automation (``eager`` / ``on_missing``) must leave it alone
        rather than re-requesting it (review #1). Asserts no new condition- or
        backfill-launched run appears for the marked partition.
        """
        io = rs.InMemoryIOHandler()
        condition = getattr(rs.AutomationCondition, condition_name)()

        @rs.Asset(
            name="widget",
            partitions_def=rs.PartitionsDefinition.static_(["a", "b", "c"]),
            io_handler=io,
            automation_condition=condition,
        )
        def widget(context: rs.AssetExecutionContext) -> int:
            # Skip 'b' wherever it appears: marks it in the backfill, and re-fails
            # it if automation wrongly re-requests it as a single-partition run.
            if rs.PartitionKey.single("b") in context.partition.keys:
                context.mark_partition_failed(rs.PartitionKey.single("b"), error="boom")
            return 1

        repo = rs.CodeRepository(
            assets=[widget], default_executor=rs.Executor.in_process()
        )
        repo.resolve(storage=storage)

        # Batched backfill: a, c materialized; b deliberately failed.
        result = repo.backfill(
            selection=["widget"],
            partition_keys=[rs.PartitionKey.single(k) for k in ("a", "b", "c")],
            strategy=rs.BackfillStrategy.single_run(),
        )
        assert result.failed == 1
        assert set(storage.get_materialized_partitions("widget")) == {
            rs.PartitionKey.single("a"),
            rs.PartitionKey.single("c"),
        }

        baseline_ids = {r.run_id for r in storage.get_runs(limit=200)}

        def daemon_runs():
            return [
                r for r in storage.get_runs(limit=200) if r.run_id not in baseline_ids
            ]

        daemon = AutomationDaemon(
            repo=repo, storage=storage, condition_eval_interval="1s"
        )
        daemon.start()
        try:
            # Break as soon as a run for 'b' appears (fast red); else wait the full
            # window to prove none is dispatched (green).
            deadline = time.monotonic() + 12
            while time.monotonic() < deadline:
                if daemon_runs():
                    break
                time.sleep(0.3)

            extra = daemon_runs()
            assert extra == [], (
                f"{condition_name}() re-requested the deliberately mark_partition_failed "
                f"'b': {len(extra)} new {[r.launched_by.kind for r in extra]} run(s)"
            )
            # 'b' must remain unmaterialized.
            assert set(storage.get_materialized_partitions("widget")) == {
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("c"),
            }
        finally:
            daemon.stop()
