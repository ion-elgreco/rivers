"""Integration tests for the AutomationDaemon.

Tests cover:
- Daemon start/stop lifecycle
- Schedule tick evaluation and tick persistence
- Sensor tick evaluation with cursor tracking
- Skipped and failed tick storage
- End-to-end: sensor triggers materialization of assets
"""

import time

import rivers as rs
from rivers._core import AutomationDaemon

from _polling import wait_for_runs as _wait_for_runs
from _polling import wait_for_ticks as _wait_for_ticks


# ---------------------------------------------------------------------------
# Daemon lifecycle
# ---------------------------------------------------------------------------


class TestDaemonLifecycle:
    def test_start_stop_no_automations(self, storage):
        """Daemon starts and stops cleanly with no schedules or sensors."""

        @rs.Asset(name="a")
        def a() -> int:
            return 1

        repo = rs.CodeRepository(assets=[a])
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()
        daemon.stop()

    def test_start_stop_with_stopped_automations(self, storage):
        """Daemon ignores schedules/sensors with Stopped status."""

        @rs.Asset(name="a")
        def a() -> int:
            return 1

        job = rs.Job(name="my_job", assets=[a])
        sched = rs.Schedule(
            cron_schedule="0 0 * * *",
            job_name="my_job",
            default_status=rs.ScheduleStatus.Stopped,
        )
        sens = rs.Sensor(
            name="my_sensor",
            asset_selection=["a"],
            default_status=rs.SensorStatus.Stopped,
        )
        repo = rs.CodeRepository(
            assets=[a], jobs=[job], schedules=[sched], sensors=[sens]
        )
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()
        time.sleep(0.5)
        daemon.stop()

        # No ticks stored for stopped automations
        assert len(storage.get_ticks("my_job_schedule", limit=100)) == 0
        assert len(storage.get_ticks("my_sensor", limit=100)) == 0

    def test_stop_is_idempotent(self, storage):
        """Calling stop() multiple times does not raise."""

        @rs.Asset(name="a")
        def a() -> int:
            return 1

        repo = rs.CodeRepository(assets=[a])
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()
        daemon.stop()
        daemon.stop()  # Should not raise


# ---------------------------------------------------------------------------
# Sensor evaluation via daemon
# ---------------------------------------------------------------------------


class TestDaemonSensor:
    def test_sensor_tick_fires_and_stores(self, storage):
        """Running sensor evaluates and stores tick in storage."""

        @rs.Asset(name="a")
        def a() -> int:
            return 1

        @rs.Sensor(
            asset_selection=["a"],
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
        )
        def fast_sensor(context: rs.SensorEvaluationContext):
            return rs.RunRequest()

        repo = rs.CodeRepository(assets=[a], sensors=[fast_sensor])
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()
        try:
            ticks = _wait_for_ticks(storage, "fast_sensor", min_count=1)
            assert len(ticks) >= 1
            tick = ticks[0]
            assert tick.automation_name == "fast_sensor"
            assert tick.automation_type == "Sensor"
            assert tick.status == "Success"
            assert len(tick.run_ids) > 0
        finally:
            daemon.stop()

    def test_sensor_skip_stores_reason(self, storage):
        """Sensor returning SkipReason stores skip tick in storage."""

        @rs.Asset(name="a")
        def a() -> int:
            return 1

        @rs.Sensor(
            asset_selection=["a"],
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
        )
        def skip_sensor(context: rs.SensorEvaluationContext):
            return rs.SkipReason("nothing new")

        repo = rs.CodeRepository(assets=[a], sensors=[skip_sensor])
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()
        try:
            ticks = _wait_for_ticks(storage, "skip_sensor", min_count=1)
            assert len(ticks) >= 1
            tick = ticks[0]
            assert tick.status == "Skipped"
            assert tick.skip_reason == "nothing new"
            assert len(tick.run_ids) == 0
        finally:
            daemon.stop()

    def test_dispatch_failure_marks_tick_failed(self, storage):
        """A run request the dispatcher rejects (invalid partition key) must
        surface on the tick record instead of silently dropping the run."""
        pd = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset(name="part_a", partitions_def=pd)
        def part_a() -> int:
            return 1

        job = rs.Job(name="part_job", assets=[part_a])

        @rs.Sensor(
            asset_selection=["part_a"],
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
        )
        def bad_key_sensor(context: rs.SensorEvaluationContext):
            return rs.RunRequest(job_name="part_job", partition_key="nope")

        repo = rs.CodeRepository(assets=[part_a], jobs=[job], sensors=[bad_key_sensor])
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()
        try:
            ticks = _wait_for_ticks(storage, "bad_key_sensor", min_count=1)
            tick = ticks[0]
            assert tick.status == "Failed"
            assert "Invalid partition_key" in (tick.error or "")
            assert tick.run_ids == []
        finally:
            daemon.stop()

    def test_backfill_dispatch_failure_marks_tick_failed(self, storage):
        """A backfill request the dispatcher rejects flows through the same
        outcome path as run requests — Failed tick, error recorded, no ids."""
        pd = rs.PartitionsDefinition.static_(["a", "b"])

        @rs.Asset(name="part_b", partitions_def=pd)
        def part_b() -> int:
            return 1

        @rs.Sensor(
            asset_selection=["part_b"],
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
        )
        def bad_backfill_sensor(context: rs.SensorEvaluationContext):
            return rs.BackfillRequest(
                selection=["part_b"],
                partition_keys=[rs.PartitionKey.single("nope")],
            )

        repo = rs.CodeRepository(assets=[part_b], sensors=[bad_backfill_sensor])
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()
        try:
            ticks = _wait_for_ticks(storage, "bad_backfill_sensor", min_count=1)
            tick = ticks[0]
            assert tick.status == "Failed"
            assert "Invalid partition_key" in (tick.error or "")
            assert tick.backfill_ids == []
        finally:
            daemon.stop()

    def test_sensor_error_stores_failed_tick(self, storage):
        """Sensor that raises an exception stores a Failed tick."""

        @rs.Asset(name="a")
        def a() -> int:
            return 1

        @rs.Sensor(
            asset_selection=["a"],
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
        )
        def error_sensor(context: rs.SensorEvaluationContext):
            raise ValueError("sensor exploded")

        repo = rs.CodeRepository(assets=[a], sensors=[error_sensor])
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()
        try:
            ticks = _wait_for_ticks(storage, "error_sensor", min_count=1)
            assert len(ticks) >= 1
            tick = ticks[0]
            assert tick.status == "Failed"
            assert tick.error is not None
            assert "sensor exploded" in tick.error
        finally:
            daemon.stop()

    def test_sensor_cursor_persisted_across_ticks(self, storage):
        """Sensor cursor is loaded from storage and passed to next evaluation."""

        @rs.Asset(name="a")
        def a() -> int:
            return 1

        @rs.Sensor(
            asset_selection=["a"],
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
        )
        def cursor_sensor(context: rs.SensorEvaluationContext):
            offset = int(context.cursor) if context.cursor else 0
            return rs.SensorResult(
                run_requests=[rs.RunRequest()],
                cursor=str(offset + 1),
            )

        repo = rs.CodeRepository(assets=[a], sensors=[cursor_sensor])
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()
        try:
            ticks = _wait_for_ticks(storage, "cursor_sensor", min_count=2, timeout=15)
            assert len(ticks) >= 2
            # Ticks ordered newest first — latest should have cursor >= 2
            latest = ticks[0]
            assert latest.cursor is not None
            assert int(latest.cursor) >= 2
        finally:
            daemon.stop()


# ---------------------------------------------------------------------------
# End-to-end: sensor triggers asset materialization
# ---------------------------------------------------------------------------


class TestDaemonE2E:
    def test_sensor_triggers_materialization(self, storage):
        """Sensor returns RunRequest -> daemon materializes assets -> run in storage."""

        @rs.Asset(name="source", io_handler=rs.InMemoryIOHandler())
        def source() -> int:
            return 42

        @rs.Asset(name="downstream", io_handler=rs.InMemoryIOHandler())
        def downstream(source: int) -> int:
            return source * 2

        call_count = 0

        @rs.Sensor(
            asset_selection=["source", "downstream"],
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
        )
        def materialize_sensor(context: rs.SensorEvaluationContext):
            nonlocal call_count
            call_count += 1
            if call_count <= 1:
                return rs.RunRequest()
            return rs.SkipReason("already materialized")

        repo = rs.CodeRepository(
            assets=[source, downstream],
            sensors=[materialize_sensor],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()
        try:
            runs = _wait_for_runs(storage, min_count=1, timeout=15)
            assert len(runs) >= 1
            assert any(r.status == "Success" for r in runs)

            # Ticks are batch-written; wait for them to flush
            ticks = _wait_for_ticks(
                storage, "materialize_sensor", min_count=1, timeout=10
            )
            assert len(ticks) >= 1
            success_ticks = [t for t in ticks if t.status == "Success"]
            assert len(success_ticks) >= 1
            assert len(success_ticks[0].run_ids) > 0

            record = storage.get_asset_record("source")
            assert record is not None
            assert record.last_run_id is not None
        finally:
            daemon.stop()


# ---------------------------------------------------------------------------
# Schedule via daemon (uses very fast cron — every second)
# ---------------------------------------------------------------------------


class TestDaemonSchedule:
    def test_schedule_tick_fires_and_stores(self, storage):
        """Running schedule with per-second cron fires and stores tick."""

        @rs.Asset(name="a")
        def a() -> int:
            return 1

        @rs.Schedule(
            cron_schedule="* * * * * *",  # every second (6-field cron)
            job_name="all",
            default_status=rs.ScheduleStatus.Running,
        )
        def every_second(context: rs.ScheduleEvaluationContext):
            return rs.RunRequest()

        repo = rs.CodeRepository(
            assets=[a],
            jobs=[rs.Job(name="all", assets=[a])],
            schedules=[every_second],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()
        try:
            ticks = _wait_for_ticks(storage, "every_second", min_count=1, timeout=10)
            assert len(ticks) >= 1
            tick = ticks[0]
            assert tick.automation_name == "every_second"
            assert tick.automation_type == "Schedule"
            assert tick.status == "Success"
            assert len(tick.run_ids) > 0
        finally:
            daemon.stop()

    def test_schedule_skip_stores_reason(self, storage):
        """Schedule returning SkipReason stores skip tick."""

        @rs.Asset(name="a")
        def a() -> int:
            return 1

        @rs.Schedule(
            cron_schedule="* * * * * *",
            job_name="all",
            default_status=rs.ScheduleStatus.Running,
        )
        def skip_schedule(context: rs.ScheduleEvaluationContext):
            return rs.SkipReason("maintenance window")

        repo = rs.CodeRepository(
            assets=[a],
            jobs=[rs.Job(name="all", assets=[a])],
            schedules=[skip_schedule],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()
        try:
            ticks = _wait_for_ticks(storage, "skip_schedule", min_count=1, timeout=10)
            assert len(ticks) >= 1
            tick = ticks[0]
            assert tick.status == "Skipped"
            assert tick.skip_reason == "maintenance window"
        finally:
            daemon.stop()

    def test_schedule_error_stores_failed_tick(self, storage):
        """Schedule that raises stores a Failed tick."""

        @rs.Asset(name="a")
        def a() -> int:
            return 1

        @rs.Schedule(
            cron_schedule="* * * * * *",
            job_name="all",
            default_status=rs.ScheduleStatus.Running,
        )
        def failing_schedule(context: rs.ScheduleEvaluationContext):
            raise RuntimeError("schedule boom")

        repo = rs.CodeRepository(
            assets=[a],
            jobs=[rs.Job(name="all", assets=[a])],
            schedules=[failing_schedule],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()
        try:
            ticks = _wait_for_ticks(
                storage, "failing_schedule", min_count=1, timeout=10
            )
            assert len(ticks) >= 1
            tick = ticks[0]
            assert tick.status == "Failed"
            assert tick.error is not None
            assert "schedule boom" in tick.error
        finally:
            daemon.stop()


# ---------------------------------------------------------------------------
# eval_timeout property tests (fast, no daemon needed)
# ---------------------------------------------------------------------------


class TestEvalTimeoutProperty:
    def test_eval_timeout_on_sensor(self):
        """eval_timeout is accessible on sensor definitions."""

        @rs.Sensor(
            name="timeout_sensor",
            minimum_interval="30s",
            eval_timeout="60s",
        )
        def my_sensor(context: rs.SensorEvaluationContext):
            return rs.SkipReason("noop")

        assert my_sensor.eval_timeout == "60s"

    def test_eval_timeout_on_schedule(self):
        """eval_timeout is accessible on schedule definitions."""
        sched = rs.Schedule(
            cron_schedule="0 * * * *",
            job_name="job",
            eval_timeout="2m",
        )
        assert sched.eval_timeout == "2m"

    def test_eval_timeout_defaults_none_sensor(self):
        """eval_timeout defaults to None when not specified."""

        @rs.Sensor(
            name="default_timeout_sensor",
            minimum_interval="30s",
        )
        def my_sensor(context: rs.SensorEvaluationContext):
            return rs.SkipReason("noop")

        assert my_sensor.eval_timeout is None

    def test_eval_timeout_defaults_none_schedule(self):
        """eval_timeout defaults to None when not specified."""
        sched = rs.Schedule(cron_schedule="0 * * * *", job_name="job")
        assert sched.eval_timeout is None
