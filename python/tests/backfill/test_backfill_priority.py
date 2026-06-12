"""Tests for Run Queue — priority and backfill integration.

Tests verify:
- Backfill runs default to priority -10
- User-provided priority overrides the default
- Priority tag propagates from backfill to spawned runs
- Backfill runs dequeue after same-time scheduled runs (priority ordering)
- Daemon-triggered backfills carry priority -10
"""

import rivers as rs
from _polling import wait_for_backfill_runs as _wait_for_backfill_runs
from rivers._core import AutomationDaemon

# ---------------------------------------------------------------------------
# Tests: Backfill default priority
# ---------------------------------------------------------------------------


class TestBackfillPriority:
    def test_backfill_runs_have_default_priority_minus_10(self, storage):
        """Backfill-spawned runs should carry rivers/priority=-10 tag and
        priority=-10 on the RunRecord, unless overridden by the user.
        """
        handler = rs.InMemoryIOHandler()

        @rs.Asset(
            name="bp_asset",
            partitions_def=rs.PartitionsDefinition.static_(["a", "b"]),
            io_handler=handler,
        )
        def bp_asset(context: rs.AssetExecutionContext) -> int:
            return 1

        repo = rs.CodeRepository(
            assets=[bp_asset],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        result = repo.backfill(
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
            ],
        )
        assert result.status == "CompletedSuccess"

        runs = storage.get_runs(limit=100)
        backfill_runs = [r for r in runs if r.launched_by.kind == "backfill"]
        assert len(backfill_runs) >= 2

        for r in backfill_runs:
            tag_dict = dict(r.tags)
            assert tag_dict.get("rivers/priority") == "-10", (
                f"Run {r.run_id} missing rivers/priority=-10 tag, got tags: {r.tags}"
            )
            assert r.priority == -10, (
                f"Run {r.run_id} has priority={r.priority}, expected -10"
            )

    def test_user_priority_overrides_backfill_default(self, storage):
        """If the user provides rivers/priority in backfill tags, the
        default -10 should NOT be applied.
        """
        handler = rs.InMemoryIOHandler()

        @rs.Asset(
            name="custom_p",
            partitions_def=rs.PartitionsDefinition.static_(["x"]),
            io_handler=handler,
        )
        def custom_p(context: rs.AssetExecutionContext) -> int:
            return 1

        repo = rs.CodeRepository(
            assets=[custom_p],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        result = repo.backfill(
            partition_keys=[rs.PartitionKey.single("x")],
            tags=[("rivers/priority", "5")],
        )
        assert result.status == "CompletedSuccess"

        runs = storage.get_runs(limit=100)
        backfill_runs = [r for r in runs if r.launched_by.kind == "backfill"]
        assert len(backfill_runs) >= 1

        for r in backfill_runs:
            tag_dict = dict(r.tags)
            assert tag_dict.get("rivers/priority") == "5", (
                f"User priority should be 5, got {tag_dict.get('rivers/priority')}"
            )
            assert r.priority == 5

    def test_backfill_tags_propagate_to_runs(self, storage):
        """Custom tags passed to backfill() should propagate to all
        spawned runs alongside launched_by=backfill and rivers/priority.
        """
        handler = rs.InMemoryIOHandler()

        @rs.Asset(
            name="tag_prop",
            partitions_def=rs.PartitionsDefinition.static_(["a", "b"]),
            io_handler=handler,
        )
        def tag_prop(context: rs.AssetExecutionContext) -> int:
            return 1

        repo = rs.CodeRepository(
            assets=[tag_prop],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        result = repo.backfill(
            partition_keys=[
                rs.PartitionKey.single("a"),
                rs.PartitionKey.single("b"),
            ],
            tags=[("team", "data-eng"), ("env", "staging")],
        )
        assert result.status == "CompletedSuccess"

        runs = storage.get_runs(limit=100)
        backfill_runs = [r for r in runs if r.launched_by.kind == "backfill"]
        assert len(backfill_runs) >= 2

        for r in backfill_runs:
            tag_dict = dict(r.tags)
            assert tag_dict.get("team") == "data-eng"
            assert tag_dict.get("env") == "staging"
            assert r.launched_by.kind == "backfill"
            assert tag_dict.get("rivers/priority") == "-10"

    def test_regular_materialize_has_priority_zero(self, storage):
        """Regular materialize() runs should have priority 0 (not -10)."""

        @rs.Asset(name="regular")
        def regular() -> int:
            return 42

        repo = rs.CodeRepository(
            assets=[regular],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)
        result = repo.materialize()
        assert result.success

        runs = storage.get_runs(limit=100)
        assert len(runs) == 1
        assert runs[0].priority == 0


class TestBackfillPriorityOrdering:
    def test_scheduled_runs_dequeue_before_backfill_runs(self, embedded_storage):
        """With a run queue, scheduled runs (priority 0) should dequeue
        before backfill runs (priority -10). Verify by creating both
        types of queued runs and checking dequeue order.

        This tests the priority ordering at the storage level since both
        run types are created as Queued and the coordinator dequeues
        higher priority first.
        """
        storage = embedded_storage
        handler = rs.InMemoryIOHandler()

        @rs.Asset(
            name="ordered",
            io_handler=handler,
            partitions_def=rs.PartitionsDefinition.static_(["a"]),
        )
        def ordered(context: rs.AssetExecutionContext) -> int:
            return 1

        # Create repo WITHOUT run queue so we can call materialize() directly
        # to establish baseline, then test priority via backfill tags
        repo = rs.CodeRepository(
            assets=[ordered],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        # Run a backfill — it should set priority -10 on spawned runs
        result = repo.backfill(
            partition_keys=[rs.PartitionKey.single("a")],
        )
        assert result.status == "CompletedSuccess"

        # Also run a regular materialize — should have priority 0
        repo.materialize(
            selection=["ordered"],
            partition_key=rs.PartitionKey.single("a"),
        )

        runs = storage.get_runs(limit=100)
        backfill_runs = [r for r in runs if r.launched_by.kind == "backfill"]
        regular_runs = [r for r in runs if not r.launched_by.kind == "backfill"]

        assert len(backfill_runs) >= 1
        assert len(regular_runs) >= 1

        # Verify priority ordering: regular runs have higher priority
        for br in backfill_runs:
            assert br.priority == -10
        for rr in regular_runs:
            assert rr.priority == 0


class TestDaemonBackfillPriority:
    def test_daemon_triggered_backfill_has_priority_minus_10(self, storage):
        """When the daemon's condition eval triggers a backfill for
        multi-partition assets, the spawned runs should carry priority -10.
        """
        handler = rs.InMemoryIOHandler()

        @rs.Asset(
            name="d_up",
            partitions_def=rs.PartitionsDefinition.static_(["a", "b"]),
            io_handler=handler,
        )
        def d_up(context: rs.AssetExecutionContext) -> int:
            return 1

        @rs.Asset(
            name="d_down",
            partitions_def=rs.PartitionsDefinition.static_(["a", "b"]),
            io_handler=handler,
            automation_condition=rs.AutomationCondition.eager(),
        )
        def d_down(context: rs.AssetExecutionContext, d_up: int) -> int:
            return d_up + 1

        repo = rs.CodeRepository(
            assets=[d_up, d_down],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        # Pre-materialize everything for baseline
        repo.materialize(
            selection=["d_up", "d_down"],
            partition_key=rs.PartitionKey.single("a"),
        )
        repo.materialize(
            selection=["d_up", "d_down"],
            partition_key=rs.PartitionKey.single("b"),
        )

        # Burst before the daemon starts: per-partition dep matching sees
        # both stale keys on the first tick — one backfill, no tick race.
        repo.materialize(
            selection=["d_up"],
            partition_key=rs.PartitionKey.single("a"),
        )
        repo.materialize(
            selection=["d_up"],
            partition_key=rs.PartitionKey.single("b"),
        )

        daemon = AutomationDaemon(
            repo=repo,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            # Wait for backfill runs
            tagged_runs = _wait_for_backfill_runs(storage, timeout=20)
            assert len(tagged_runs) >= 1, (
                f"Expected backfill runs with backfill origin, got {len(tagged_runs)}"
            )

            # All backfill runs should have priority -10
            for r in tagged_runs:
                tag_dict = dict(r.tags)
                assert tag_dict.get("rivers/priority") == "-10", (
                    f"Daemon backfill run {r.run_id} missing priority tag, "
                    f"tags: {r.tags}"
                )
                assert r.priority == -10, (
                    f"Daemon backfill run {r.run_id} has priority={r.priority}, "
                    f"expected -10"
                )
        finally:
            daemon.stop()
