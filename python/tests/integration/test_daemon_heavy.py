"""Heavy integration tests for the AutomationDaemon.

These tests are slow (sleep-based timing, stress scenarios) and are intended
for CI pipelines, not the fast unit test suite. Run with:

    pytest python/tests/integration/ -v

Covers:
- GIL contention stress test (many sensors + schedules competing)
- Eval timeout (sensor and schedule)
- Sync concurrency via spawn_blocking
- Mixed eval modes (sensors + schedules together)
- Subprocess parallelism via loky
"""

import time

import pytest

import rivers as rs
from rivers._core import AutomationDaemon

from _polling import wait_for_ticks as _wait_for_ticks


# ---------------------------------------------------------------------------
# GIL contention stress test — many sensors + schedules competing
# ---------------------------------------------------------------------------


class TestDaemonGILContention:
    def test_many_sensors_and_schedules_contesting_gil(self, storage):
        """Stress test: 10 sensors + 5 schedules all competing for the GIL.

        Verifies no deadlocks, panics, or lost ticks when many automations
        simultaneously spawn OS threads that acquire the GIL.
        """

        @rs.Asset(name="stress_asset", io_handler=rs.InMemoryIOHandler())
        def stress_asset() -> int:
            return 1

        # Create 5 schedules (every-second cron)
        schedules = []
        for i in range(5):

            @rs.Schedule(
                cron_schedule="* * * * * *",
                job_name="stress_job",
                name=f"stress_sched_{i}",
                default_status=rs.ScheduleStatus.Running,
            )
            def make_sched(context: rs.ScheduleEvaluationContext):
                return rs.RunRequest()

            schedules.append(make_sched)

        # Create 10 sensors (10-second interval)
        sensors = []
        for i in range(10):

            @rs.Sensor(
                name=f"stress_sensor_{i}",
                asset_selection=["stress_asset"],
                minimum_interval="10s",
                default_status=rs.SensorStatus.Running,
            )
            def make_sensor(context: rs.SensorEvaluationContext):
                return rs.RunRequest()

            sensors.append(make_sensor)

        repo = rs.CodeRepository(
            assets=[stress_asset],
            jobs=[rs.Job(name="stress_job", assets=[stress_asset])],
            schedules=schedules,
            sensors=sensors,
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()
        try:
            # Wait for at least one tick from every automation
            all_names = [f"stress_sched_{i}" for i in range(5)] + [
                f"stress_sensor_{i}" for i in range(10)
            ]
            deadline = time.monotonic() + 100.0
            ticked = set()
            while time.monotonic() < deadline and len(ticked) < len(all_names):
                for name in all_names:
                    if name in ticked:
                        continue
                    ticks = storage.get_ticks(name, limit=100)
                    if len(ticks) >= 1:
                        ticked.add(name)
                time.sleep(0.3)

            # All 15 automations should have produced at least one tick
            missing = set(all_names) - ticked
            assert len(missing) == 0, (
                f"Automations that never ticked (possible deadlock): {missing}"
            )
        finally:
            daemon.stop()


# ---------------------------------------------------------------------------
# Daemon eval_timeout
# ---------------------------------------------------------------------------


class TestDaemonEvalTimeout:
    def test_sensor_eval_timeout_stores_failed_tick(self, storage):
        """Sensor that exceeds eval_timeout stores a Failed tick."""

        @rs.Asset(name="a")
        def a() -> int:
            return 1

        @rs.Sensor(
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
            eval_timeout="2s",
            asset_selection=["a"],
        )
        def slow_sensor(context: rs.SensorEvaluationContext):
            time.sleep(10)  # Will be killed by 2s timeout
            return rs.RunRequest()

        repo = rs.CodeRepository(assets=[a], sensors=[slow_sensor])
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()
        try:
            ticks = _wait_for_ticks(storage, "slow_sensor", min_count=1, timeout=15)
            assert len(ticks) >= 1
            tick = ticks[0]
            assert tick.status == "Failed"
            assert "timed out" in tick.error.lower() or "timeout" in tick.error.lower()
        finally:
            daemon.stop()

    def test_schedule_eval_timeout_stores_failed_tick(self, storage):
        """Schedule that exceeds eval_timeout stores a Failed tick."""

        @rs.Asset(name="a")
        def a() -> int:
            return 1

        @rs.Schedule(
            cron_schedule="* * * * * *",
            job_name="my_job",
            default_status=rs.ScheduleStatus.Running,
            eval_timeout="2s",
        )
        def slow_schedule(context: rs.ScheduleEvaluationContext):
            time.sleep(10)  # Will be killed by 2s timeout
            return rs.RunRequest()

        repo = rs.CodeRepository(
            assets=[a],
            jobs=[rs.Job(name="my_job", assets=[a])],
            schedules=[slow_schedule],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()
        try:
            ticks = _wait_for_ticks(storage, "slow_schedule", min_count=1, timeout=15)
            assert len(ticks) >= 1
            tick = ticks[0]
            assert tick.status == "Failed"
            assert "timed out" in tick.error.lower() or "timeout" in tick.error.lower()
        finally:
            daemon.stop()

    def test_sensor_eval_timeout_fires_within_expected_window(self, storage):
        """Sensor timeout fires close to the configured timeout, not much later."""

        @rs.Asset(name="a")
        def a() -> int:
            return 1

        @rs.Sensor(
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
            eval_timeout="2s",
            asset_selection=["a"],
        )
        def timed_sensor(context: rs.SensorEvaluationContext):
            time.sleep(30)
            return rs.RunRequest()

        repo = rs.CodeRepository(assets=[a], sensors=[timed_sensor])
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        start_time = time.monotonic()
        daemon.start()
        try:
            ticks = _wait_for_ticks(storage, "timed_sensor", min_count=1, timeout=15)
            elapsed = time.monotonic() - start_time
            assert len(ticks) >= 1
            assert ticks[0].status == "Failed"
            # Should timeout around 2s, give generous margin but not 30s
            assert elapsed < 10, f"Timeout took {elapsed:.1f}s, expected ~2s"
        finally:
            daemon.stop()


# ---------------------------------------------------------------------------
# Concurrent sync sensor I/O overlap via spawn_blocking
# ---------------------------------------------------------------------------


class TestDaemonSyncConcurrency:
    def test_sync_sensors_overlap_via_spawn_blocking(self, storage):
        """Multiple sync sensors with time.sleep overlap via spawn_blocking thread pool."""

        @rs.Asset(name="a")
        def a() -> int:
            return 1

        @rs.Sensor(
            name="slow_sensor_0",
            asset_selection=["a"],
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
            eval_mode=rs.EvalMode.InProcess,
        )
        def slow_sensor_0(context: rs.SensorEvaluationContext):
            time.sleep(2)
            return rs.SkipReason("done_0")

        @rs.Sensor(
            name="slow_sensor_1",
            asset_selection=["a"],
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
            eval_mode=rs.EvalMode.InProcess,
        )
        def slow_sensor_1(context: rs.SensorEvaluationContext):
            time.sleep(2)
            return rs.SkipReason("done_1")

        @rs.Sensor(
            name="slow_sensor_2",
            asset_selection=["a"],
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
            eval_mode=rs.EvalMode.InProcess,
        )
        def slow_sensor_2(context: rs.SensorEvaluationContext):
            time.sleep(2)
            return rs.SkipReason("done_2")

        repo = rs.CodeRepository(
            assets=[a],
            sensors=[slow_sensor_0, slow_sensor_1, slow_sensor_2],
        )
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        start = time.monotonic()
        daemon.start()
        try:
            ticks_0 = _wait_for_ticks(storage, "slow_sensor_0", min_count=1, timeout=15)
            ticks_1 = _wait_for_ticks(storage, "slow_sensor_1", min_count=1, timeout=15)
            ticks_2 = _wait_for_ticks(storage, "slow_sensor_2", min_count=1, timeout=15)
            elapsed = time.monotonic() - start

            assert ticks_0[0].status == "Skipped"
            assert ticks_1[0].status == "Skipped"
            assert ticks_2[0].status == "Skipped"
            # If sequential: 3 * 2s = 6s+. spawn_blocking overlap should be ~2s + overhead.
            assert elapsed < 8, f"Took {elapsed:.1f}s, expected < 8s (proves overlap)"
        finally:
            daemon.stop()


# ---------------------------------------------------------------------------
# Mixed eval modes (sensors + schedules together)
# ---------------------------------------------------------------------------


class TestDaemonMixedModes:
    def test_sensors_and_schedules_run_concurrently(self, storage):
        """Multiple sync sensors and a schedule all produce ticks together."""

        @rs.Asset(name="a")
        def a() -> int:
            return 1

        @rs.Sensor(
            name="mixed_sensor_a",
            asset_selection=["a"],
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
            eval_mode=rs.EvalMode.InProcess,
        )
        def sensor_a(context: rs.SensorEvaluationContext):
            return rs.SkipReason("sensor_a_done")

        @rs.Sensor(
            name="mixed_sensor_b",
            asset_selection=["a"],
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
            eval_mode=rs.EvalMode.InProcess,
        )
        def sensor_b(context: rs.SensorEvaluationContext):
            return rs.SkipReason("sensor_b_done")

        @rs.Schedule(
            cron_schedule="* * * * * *",
            job_name="my_job",
            default_status=rs.ScheduleStatus.Running,
        )
        def every_second(context: rs.ScheduleEvaluationContext):
            return rs.SkipReason("schedule_done")

        repo = rs.CodeRepository(
            assets=[a],
            jobs=[rs.Job(name="my_job", assets=[a])],
            sensors=[sensor_a, sensor_b],
            schedules=[every_second],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()
        try:
            ticks_a = _wait_for_ticks(
                storage, "mixed_sensor_a", min_count=1, timeout=10
            )
            ticks_b = _wait_for_ticks(
                storage, "mixed_sensor_b", min_count=1, timeout=10
            )
            ticks_sched = _wait_for_ticks(
                storage, "every_second", min_count=1, timeout=10
            )

            assert len(ticks_a) >= 1
            assert len(ticks_b) >= 1
            assert len(ticks_sched) >= 1
            assert ticks_a[0].status == "Skipped"
            assert ticks_b[0].status == "Skipped"
        finally:
            daemon.stop()


# ---------------------------------------------------------------------------
# Subprocess evaluation (requires loky)
# ---------------------------------------------------------------------------


def _has_loky():
    try:
        import loky  # noqa: F401

        return True
    except ImportError:
        return False


# Module-level function for subprocess pickling
def _subprocess_sleep_sensor(context):
    """Sensor eval function that sleeps 2s — must be module-level for loky pickling."""
    import time as _time

    import rivers as _rs

    _time.sleep(2)
    return _rs.SkipReason("done")


@pytest.mark.skipif(not _has_loky(), reason="loky not installed")
class TestDaemonSubprocessConcurrency:
    def test_subprocess_sensors_run_in_parallel(self, embedded_storage):
        """Multiple subprocess sensors complete faster than sequential would."""
        storage = embedded_storage

        @rs.Asset(name="a")
        def a() -> int:
            return 1

        sub_sensor_0 = rs.Sensor(
            name="sub_sensor_0",
            asset_selection=["a"],
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
            eval_mode=rs.EvalMode.Subprocess,
        )
        # Attach eval fn after construction since Sensor(func=...) is the first positional
        sub_sensor_0 = rs.Sensor(
            _subprocess_sleep_sensor,
            name="sub_sensor_0",
            asset_selection=["a"],
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
            eval_mode=rs.EvalMode.Subprocess,
        )
        sub_sensor_1 = rs.Sensor(
            _subprocess_sleep_sensor,
            name="sub_sensor_1",
            asset_selection=["a"],
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
            eval_mode=rs.EvalMode.Subprocess,
        )
        sub_sensor_2 = rs.Sensor(
            _subprocess_sleep_sensor,
            name="sub_sensor_2",
            asset_selection=["a"],
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
            eval_mode=rs.EvalMode.Subprocess,
        )

        repo = rs.CodeRepository(
            assets=[a],
            sensors=[sub_sensor_0, sub_sensor_1, sub_sensor_2],
        )
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        start = time.monotonic()
        daemon.start()
        try:
            ticks_0 = _wait_for_ticks(storage, "sub_sensor_0", min_count=1, timeout=15)
            ticks_1 = _wait_for_ticks(storage, "sub_sensor_1", min_count=1, timeout=15)
            ticks_2 = _wait_for_ticks(storage, "sub_sensor_2", min_count=1, timeout=15)
            elapsed = time.monotonic() - start

            assert ticks_0[0].status == "Skipped"
            assert ticks_1[0].status == "Skipped"
            assert ticks_2[0].status == "Skipped"
            # Sequential would be 3 * 2s = 6s+. Parallel should be ~2s + overhead.
            assert elapsed < 8, (
                f"Took {elapsed:.1f}s, expected < 8s (proves parallelism)"
            )
        finally:
            daemon.stop()
