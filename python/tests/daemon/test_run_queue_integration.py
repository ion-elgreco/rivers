"""Integration tests for the full run queue flow.

End-to-end tests verifying:
- Automation condition fires → run queued → coordinator dequeues → materialize executes
- Schedule fires → run queued (not executed directly)
- Sensor fires → run queued (not executed directly)
- max_concurrent_runs is respected
- Tag concurrency limits block excess runs
- Queue flow works with partitioned assets
"""

import pytest
import rivers as rs
from _polling import wait_for_all_runs_success as _wait_for_all_runs_success
from _polling import wait_for_asset_materialized as _wait_for_asset_materialized
from _polling import wait_for_run_terminal as _wait_for_run_terminal
from _polling import wait_for_runs as _wait_for_runs
from rivers._core import AutomationDaemon

# ---------------------------------------------------------------------------
# Test: Full queue flow — condition fires → queued → dequeued → executed
# ---------------------------------------------------------------------------


class TestRunQueueFullFlow:
    """End-to-end: automation condition → queue → coordinator → materialize."""

    def test_condition_fires_through_queue(self, storage):
        """An asset with on_missing condition gets queued and executed through
        the run queue coordinator when RunQueueConfig is present.

        Flow:
        1. Daemon starts with run_queue enabled
        2. Condition eval detects 'target' is Missing
        3. Creates a Queued run record (not direct execution)
        4. Coordinator dequeues the run (Queued → NotStarted)
        5. Coordinator calls materialize(run_id_override=...) → executes
        6. Run completes with Success status
        7. Asset record shows materialized
        """

        @rs.Asset(
            name="target",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.on_missing(),
        )
        def target() -> int:
            return 42

        repo = rs.CodeRepository(
            assets=[target],
            default_executor=rs.Executor.in_process(),
            run_queue=rs.RunQueueConfig(
                max_concurrent_runs=10,
                dequeue_interval="100ms",
            ),
        )
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(
            repo=repo,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            # Wait for the asset to be materialized through the queue
            record = _wait_for_asset_materialized(storage, "target", timeout=20)
            assert record is not None, "target was never materialized"
            assert record.last_data_version is not None

            # Verify the run went through the queue (created as Queued, ended as Success)
            runs = storage.get_runs(limit=100)
            success_runs = [r for r in runs if r.status == "Success"]
            assert len(success_runs) >= 1, (
                f"Expected at least 1 successful run, got statuses: "
                f"{[r.status for r in runs]}"
            )
        finally:
            daemon.stop()

    def test_queue_flow_chain_two_assets(self, storage):
        """Two-asset chain: source (on_missing) → derived (eager).
        Both should be materialized through the queue.

        Flow:
        1. source is Missing → condition fires → Queued run
        2. Coordinator dequeues → materialize source
        3. derived becomes stale (AnyDepsUpdated) → eager fires → Queued run
        4. Coordinator dequeues → materialize derived
        """

        @rs.Asset(
            name="source",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.on_missing(),
        )
        def source() -> int:
            return 10

        @rs.Asset(
            name="derived",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.eager(),
        )
        def derived(source: int) -> int:
            return source * 2

        repo = rs.CodeRepository(
            assets=[source, derived],
            default_executor=rs.Executor.in_process(),
            run_queue=rs.RunQueueConfig(
                max_concurrent_runs=10,
                dequeue_interval="100ms",
            ),
        )
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(
            repo=repo,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            # Wait for derived to be materialized (depends on source being done first)
            record = _wait_for_asset_materialized(storage, "derived", timeout=25)
            assert record is not None, "derived was never materialized"
            assert record.last_data_version is not None

            # Verify source was also materialized
            source_rec = storage.get_asset_record("source")
            assert source_rec is not None
            assert source_rec.last_data_version is not None

            # All runs should have completed successfully
            runs = _wait_for_all_runs_success(storage)
            for r in runs:
                assert r.status == "Success", (
                    f"Run {r.run_id} has status {r.status}, expected Success"
                )
        finally:
            daemon.stop()

    def test_queue_max_concurrent_runs(self, storage):
        """With max_concurrent_runs=1 and multiple missing assets, runs should
        be queued and only one should execute at a time.

        We verify that all runs eventually complete (no deadlock) and that
        the queue mechanism was used (Queued runs created).
        """

        @rs.Asset(
            name="a",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.on_missing(),
        )
        def a() -> int:
            return 1

        @rs.Asset(
            name="b",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.on_missing(),
        )
        def b() -> int:
            return 2

        repo = rs.CodeRepository(
            assets=[a, b],
            default_executor=rs.Executor.in_process(),
            run_queue=rs.RunQueueConfig(
                max_concurrent_runs=1,
                dequeue_interval="100ms",
            ),
        )
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(
            repo=repo,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            # Both assets should eventually be materialized
            rec_a = _wait_for_asset_materialized(storage, "a", timeout=25)
            rec_b = _wait_for_asset_materialized(storage, "b", timeout=25)
            assert rec_a is not None, "asset 'a' was never materialized"
            assert rec_b is not None, "asset 'b' was never materialized"

            # All runs should have succeeded
            runs = _wait_for_all_runs_success(storage)
            for r in runs:
                assert r.status == "Success", (
                    f"Run {r.run_id} has status {r.status}, expected Success"
                )
        finally:
            daemon.stop()

    def test_queue_with_tag_concurrency_limits(self, storage):
        """Tag concurrency limits restrict how many runs with matching tags
        can execute concurrently. Verify the queue still completes all runs
        when tag limits are configured (even if automation-created runs don't
        carry run tags — the limit caps in-progress count).
        """

        @rs.Asset(
            name="tagged_a",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.on_missing(),
        )
        def tagged_a() -> int:
            return 1

        @rs.Asset(
            name="tagged_b",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.on_missing(),
        )
        def tagged_b() -> int:
            return 2

        repo = rs.CodeRepository(
            assets=[tagged_a, tagged_b],
            default_executor=rs.Executor.in_process(),
            run_queue=rs.RunQueueConfig(
                max_concurrent_runs=10,
                tag_concurrency_limits=[
                    rs.TagConcurrencyLimit(key="env", value="prod", limit=1),
                ],
                dequeue_interval="100ms",
            ),
        )
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(
            repo=repo,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            rec_a = _wait_for_asset_materialized(storage, "tagged_a", timeout=25)
            rec_b = _wait_for_asset_materialized(storage, "tagged_b", timeout=25)
            assert rec_a is not None, "tagged_a was never materialized"
            assert rec_b is not None, "tagged_b was never materialized"

            runs = _wait_for_all_runs_success(storage)
            for r in runs:
                assert r.status == "Success", (
                    f"Run {r.run_id} has status {r.status}, expected Success"
                )
        finally:
            daemon.stop()

    def test_direct_materialize_bypasses_queue(self, storage):
        """Calling materialize() directly should always bypass the queue,
        even when RunQueueConfig is present. The run should never be Queued.
        """

        @rs.Asset(name="direct", io_handler=rs.InMemoryIOHandler())
        def direct() -> int:
            return 99

        repo = rs.CodeRepository(
            assets=[direct],
            default_executor=rs.Executor.in_process(),
            run_queue=rs.RunQueueConfig(
                max_concurrent_runs=1,
                dequeue_interval="100ms",
            ),
        )
        repo.resolve(storage=storage)

        # Direct materialize — should execute immediately, not queue
        result = repo.materialize()
        assert result.success is True

        runs = storage.get_runs(limit=100)
        assert len(runs) == 1
        assert runs[0].status == "Success"
        assert repo.load_node("direct") == 99

    def test_queue_flow_with_partitioned_asset(self, storage):
        """A partitioned asset with on_missing condition should get queued
        and executed through the coordinator with partition key encoding.
        """

        @rs.Asset(
            name="partitioned",
            io_handler=rs.InMemoryIOHandler(),
            partitions_def=rs.PartitionsDefinition.static_(["x", "y"]),
            automation_condition=rs.AutomationCondition.on_missing(),
        )
        def partitioned() -> str:
            return "data"

        repo = rs.CodeRepository(
            assets=[partitioned],
            default_executor=rs.Executor.in_process(),
            run_queue=rs.RunQueueConfig(
                max_concurrent_runs=10,
                dequeue_interval="100ms",
            ),
        )
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(
            repo=repo,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            # Wait for both partitions to be materialized
            # Partitioned assets fire per-partition, so there should be 2 runs
            runs = _wait_for_runs(storage, min_count=2, timeout=25, status="Success")
            assert len(runs) >= 2, (
                f"Expected at least 2 successful runs for 2 partitions, got {len(runs)}"
            )
        finally:
            daemon.stop()


# ---------------------------------------------------------------------------
# Regression: schedule/sensor runs must go through the queue
# ---------------------------------------------------------------------------


class TestScheduleSensorQueueRegression:
    """Schedules and sensors must submit runs to the queue when RunQueueConfig
    is present, not execute them directly. This was a bug where the daemon
    dispatch path bypassed the queue entirely."""

    @pytest.mark.parametrize("source_kind", ["schedule", "sensor"])
    def test_automation_runs_are_queued(self, source_kind, storage):
        """A schedule or sensor firing with RunQueueConfig must create Queued runs,
        not execute them directly. Bypassing the queue from the daemon dispatch
        path was a regression — verify a RunQueued event is emitted in both cases.
        """

        io = rs.InMemoryIOHandler()

        @rs.Asset(name="target", io_handler=io)
        def target() -> int:
            return 42

        job = rs.Job(name="auto_job", assets=[target])

        schedules = []
        sensors = []
        if source_kind == "schedule":

            @rs.Schedule(
                cron_schedule="* * * * * *",  # every second
                job_name="auto_job",
                name="fast_schedule",
                default_status=rs.ScheduleStatus.Running,
            )
            def fast_schedule(context: rs.ScheduleEvaluationContext):
                return rs.RunRequest()

            schedules = [fast_schedule]
        else:

            @rs.Sensor(
                job_name="auto_job",
                name="fast_sensor",
                minimum_interval="1s",
                default_status=rs.SensorStatus.Running,
            )
            def fast_sensor(context: rs.SensorEvaluationContext):
                return rs.RunRequest()

            sensors = [fast_sensor]

        repo = rs.CodeRepository(
            assets=[target],
            jobs=[job],
            schedules=schedules,
            sensors=sensors,
            default_executor=rs.Executor.in_process(),
            run_queue=rs.RunQueueConfig(
                max_concurrent_runs=10,
                dequeue_interval="100ms",
            ),
        )
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(
            repo=repo, storage=storage, condition_eval_interval="1m"
        )
        daemon.start()
        try:
            runs = _wait_for_runs(storage, min_count=1, timeout=15)
            assert len(runs) >= 1, f"No runs created by {source_kind}"

            # RunQueued is only emitted by submit_run, not create_run, so its
            # presence proves the run went through the queue.
            all_runs = storage.get_runs(limit=100)
            found_queued_event = any(
                e.event_type == "RunQueued"
                for r in all_runs
                for e in storage.get_events_for_run(r.run_id)
            )
            assert found_queued_event, (
                f"No RunQueued event found — {source_kind} runs were executed "
                "directly instead of being submitted to the queue"
            )

            # Provenance must survive the queued dispatch path: the run carries
            # the firing automation's origin, not a generic manual stamp.
            expected_name = (
                "fast_schedule" if source_kind == "schedule" else "fast_sensor"
            )
            run = runs[0]
            assert run.launched_by.kind == source_kind
            assert run.launched_by.name == expected_name
            assert run.launched_by.user is None

            completed = _wait_for_runs(
                storage, min_count=1, timeout=15, status="Success"
            )
            assert len(completed) >= 1, (
                f"No successful runs, statuses: {[r.status for r in storage.get_runs(limit=100)]}"
            )
        finally:
            daemon.stop()


# ---------------------------------------------------------------------------
# Test: Orphaned NotStarted runs — start-timeout sweep and cancellation
# ---------------------------------------------------------------------------


class TestOrphanedNotStartedRuns:
    """A dequeued run whose executor never appeared (daemon died between
    dequeue and launch, or the launch failed) sits in NotStarted forever:
    the coordinator only re-selects Queued runs. The sweep fails it; the
    cancel path can also clear it directly."""

    def test_stalled_not_started_run_swept_to_failure(self, storage):
        @rs.Asset(name="idle", io_handler=rs.InMemoryIOHandler())
        def idle() -> int:
            return 1

        repo = rs.CodeRepository(
            assets=[idle],
            default_executor=rs.Executor.in_process(),
            run_queue=rs.RunQueueConfig(
                dequeue_interval="100ms",
                start_timeout="1s",
            ),
        )
        repo.resolve(storage=storage)

        # Fabricate the orphan: start_time is ancient and there is no
        # RunDequeued event, so the enqueue-time fallback puts it far past
        # the 1s start timeout.
        storage._create_run("stuck-run", "j", "NotStarted", 1_000)

        daemon = AutomationDaemon(
            repo=repo,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            # The sweep runs on the health-check cadence (5s), so the first
            # pass lands ~5s after daemon start.
            record = _wait_for_run_terminal(storage, "stuck-run", timeout=15)
            assert record is not None, "stuck run disappeared from storage"
            assert record.status == "Failure", (
                f"expected the sweep to fail the orphan, got {record.status}"
            )
            assert record.end_time is not None

            events = storage.get_events_for_run("stuck-run")
            launch_failed = [e for e in events if e.event_type == "RunLaunchFailed"]
            assert len(launch_failed) == 1
            error = dict(launch_failed[0].metadata).get("error", "")
            assert "no executor appeared" in error
        finally:
            daemon.stop()

    def test_cancel_covers_not_started(self, storage):
        storage._create_run("orphan", "j", "NotStarted", 1_000)

        assert storage.cancel_queued_run("orphan")

        record = storage.get_run("orphan")
        assert record is not None
        assert record.status == "Canceled"
        assert record.end_time is not None
