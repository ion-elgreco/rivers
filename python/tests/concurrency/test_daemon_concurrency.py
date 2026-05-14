"""Tests for daemon non-blocking eval dispatch and per-sensor sequential eval.

Covers:
- Slow sensors do not block fast sensors from ticking
- A single sensor never has overlapping evals (sequential guarantee)
"""

import threading
import time

import rivers as rs
from rivers._core import AutomationDaemon

from _polling import wait_for_ticks as _wait_for_ticks


class TestDaemonNonBlocking:
    """A slow sensor must not prevent a fast sensor from ticking."""

    def test_slow_sensor_does_not_block_fast_sensor(self, embedded_storage):
        storage = embedded_storage

        @rs.Asset(name="a")
        def a() -> int:
            return 1

        @rs.Sensor(
            name="slow_sensor",
            asset_selection=["a"],
            minimum_interval="0s",
            default_status=rs.SensorStatus.Running,
        )
        def slow_sensor(context: rs.SensorEvaluationContext):
            time.sleep(3)
            return rs.SkipReason("slow done")

        @rs.Sensor(
            name="fast_sensor",
            asset_selection=["a"],
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
        )
        def fast_sensor(context: rs.SensorEvaluationContext):
            return rs.SkipReason("fast done")

        repo = rs.CodeRepository(assets=[a], sensors=[slow_sensor, fast_sensor])
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()
        try:
            # Wait long enough for the slow sensor to be mid-eval,
            # but the fast sensor should have ticked multiple times.
            fast_ticks = _wait_for_ticks(storage, "fast_sensor", min_count=3, timeout=8)
            assert len(fast_ticks) >= 3, (
                f"Fast sensor only ticked {len(fast_ticks)} times in 8s — "
                "slow sensor is blocking the daemon loop"
            )
        finally:
            daemon.stop()


class TestDaemonSequentialPerSensor:
    """A single sensor must never have overlapping (concurrent) evals."""

    def test_no_overlapping_evals_for_same_sensor(self, embedded_storage):
        storage = embedded_storage
        max_concurrent = 0
        current_concurrent = 0
        lock = threading.Lock()

        @rs.Asset(name="a")
        def a() -> int:
            return 1

        @rs.Sensor(
            name="overlap_sensor",
            asset_selection=["a"],
            minimum_interval="0s",
            default_status=rs.SensorStatus.Running,
        )
        def overlap_sensor(context: rs.SensorEvaluationContext):
            nonlocal max_concurrent, current_concurrent
            with lock:
                current_concurrent += 1
                if current_concurrent > max_concurrent:
                    max_concurrent = current_concurrent
            # Simulate work that takes some time
            time.sleep(0.5)
            with lock:
                current_concurrent -= 1
            return rs.SkipReason("done")

        repo = rs.CodeRepository(assets=[a], sensors=[overlap_sensor])
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage, max_ticks_retained=None)
        daemon.start()
        try:
            # Let it run long enough for several evals to complete
            ticks = _wait_for_ticks(storage, "overlap_sensor", min_count=4, timeout=10)
            assert len(ticks) >= 4, f"Sensor only completed {len(ticks)} ticks in 10s"
        finally:
            daemon.stop()

        assert max_concurrent == 1, (
            f"Sensor had {max_concurrent} concurrent evals — "
            "expected exactly 1 (sequential guarantee violated)"
        )

    def test_long_eval_does_not_double_fire(self, embedded_storage):
        """A sensor with interval=1s whose eval takes 3s should NOT start
        a second eval while the first is still running."""
        storage = embedded_storage
        eval_count = 0
        eval_count_lock = threading.Lock()

        @rs.Asset(name="a")
        def a() -> int:
            return 1

        @rs.Sensor(
            name="long_sensor",
            asset_selection=["a"],
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
        )
        def long_sensor(context: rs.SensorEvaluationContext):
            nonlocal eval_count
            with eval_count_lock:
                eval_count += 1
            time.sleep(3)
            return rs.SkipReason("long done")

        repo = rs.CodeRepository(assets=[a], sensors=[long_sensor])
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()
        try:
            # Run for 5 seconds. With 3s eval + 1s interval, at most
            # 2 evals should start (one at t=0, one at t=4).
            time.sleep(5)
        finally:
            daemon.stop()

        with eval_count_lock:
            count = eval_count

        assert count <= 2, (
            f"Long sensor had {count} evals in 5s — "
            "expected at most 2 (interval=1s, eval=3s). "
            "The daemon is double-firing while an eval is in-flight."
        )


class TestDaemonSleepTimerReset:
    """Regression: a far-off schedule must not starve fast sensors.

    When sensors complete quickly, the daemon's sleep timer must be
    reset to the sensor's next due time — not stuck on the schedule's
    next occurrence (which could be hours away).
    """

    def test_sensor_ticks_independently_of_distant_schedule(self, embedded_storage):
        storage = embedded_storage

        @rs.Asset(name="a")
        def a() -> int:
            return 1

        # Schedule that won't fire during the test (hourly cron)
        @rs.Schedule(
            cron_schedule="0 0 * * *",
            job_name="my_job",
            default_status=rs.ScheduleStatus.Running,
        )
        def distant_schedule(context):
            return rs.SkipReason("should not fire")

        # Sensor with 1-second interval — should tick multiple times in 5s
        @rs.Sensor(
            name="fast_sensor",
            asset_selection=["a"],
            minimum_interval="1s",
            default_status=rs.SensorStatus.Running,
        )
        def fast_sensor(context: rs.SensorEvaluationContext):
            return rs.SkipReason("tick")

        repo = rs.CodeRepository(
            assets=[a],
            jobs=[rs.Job(name="my_job", assets=[a])],
            sensors=[fast_sensor],
            schedules=[distant_schedule],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        daemon = AutomationDaemon(repo=repo, storage=storage)
        daemon.start()
        try:
            ticks = _wait_for_ticks(storage, "fast_sensor", min_count=3, timeout=8)
            assert len(ticks) >= 3, (
                f"Sensor only ticked {len(ticks)} times in 8s — "
                "the schedule's sleep deadline is starving the sensor"
            )
        finally:
            daemon.stop()
