"""Tests for sensor definitions and evaluation."""

import logging
import pickle
from typing import Any

import pytest

import rivers as rs
from rivers.exceptions import (
    ConfigurationError,
    ExecutionError,
    NodeNotFoundError,
    ResultDefinitionError,
    SensorDefinitionError,
)

# ---------------------------------------------------------------------------
# SensorDefinition
# ---------------------------------------------------------------------------


class TestSensorDefinition:
    def test_basic_construction(self):
        sensor = rs.Sensor(name="my_sensor")
        assert sensor.name == "my_sensor"
        assert sensor.job_name is None
        assert sensor.minimum_interval is None
        assert sensor.default_status == rs.SensorStatus.Stopped

    def test_with_all_options(self):
        sensor = rs.Sensor(
            name="my_sensor",
            job_name="my_job",
            minimum_interval="60s",
            default_status=rs.SensorStatus.Running,
            description="Watches for new files",
            tags={"env": "prod"},
            asset_selection=["asset_a", "asset_b"],
        )
        assert sensor.name == "my_sensor"
        assert sensor.job_name == "my_job"
        assert sensor.minimum_interval == "60s"
        assert sensor.default_status == rs.SensorStatus.Running
        assert sensor.description == "Watches for new files"
        assert sensor.tags == {"env": "prod"}
        assert sensor.asset_selection == ["asset_a", "asset_b"]

    def test_empty_name_raises(self):
        with pytest.raises(SensorDefinitionError, match="name"):
            rs.Sensor(name="")

    def test_repr(self):
        sensor = rs.Sensor(name="my_sensor", job_name="my_job")
        assert repr(sensor) == "Sensor(name='my_sensor', job_name=Some(\"my_job\"))"

    def test_default_status_is_stopped(self):
        sensor = rs.Sensor(name="my_sensor")
        assert sensor.default_status == rs.SensorStatus.Stopped


# ---------------------------------------------------------------------------
# SensorResult
# ---------------------------------------------------------------------------


class TestSensorResult:
    def test_empty_result(self):
        result = rs.SensorResult()
        assert result.run_requests is None
        assert result.skip_reason is None
        assert result.cursor is None

    def test_with_run_requests(self):
        req = rs.RunRequest(job_name="my_job")
        result = rs.SensorResult(run_requests=[req])
        assert result.run_requests is not None
        assert len(result.run_requests) == 1
        assert result.skip_reason is None

    def test_with_skip_reason_string(self):
        result = rs.SensorResult(skip_reason="not ready")
        assert result.skip_reason is not None
        assert result.skip_reason.message == "not ready"

    def test_with_skip_reason_object(self):
        skip = rs.SkipReason("not ready")
        result = rs.SensorResult(skip_reason=skip)
        assert result.skip_reason is not None
        assert result.skip_reason.message == "not ready"

    def test_with_cursor(self):
        result = rs.SensorResult(cursor="offset_42")
        assert result.cursor == "offset_42"

    def test_run_requests_and_skip_raises(self):
        req = rs.RunRequest()
        with pytest.raises(ResultDefinitionError, match="cannot have both"):
            rs.SensorResult(run_requests=[req], skip_reason="skip")

    def test_empty_run_requests_with_skip_ok(self):
        result = rs.SensorResult(run_requests=[], skip_reason="skip")
        assert result.skip_reason is not None

    def test_repr_with_skip(self):
        result = rs.SensorResult(skip_reason="skip")
        assert repr(result) == "SensorResult(skipped, cursor=None)"

    def test_repr_with_requests(self):
        result = rs.SensorResult(run_requests=[rs.RunRequest()])
        assert repr(result) == "SensorResult(run_requests=1, cursor=None)"


# ---------------------------------------------------------------------------
# @sensor decorator
# ---------------------------------------------------------------------------


class TestSensorDecorator:
    def test_decorator_with_args(self):
        @rs.Sensor(job_name="my_job")
        def my_sensor(context: rs.SensorEvaluationContext):
            return rs.RunRequest()

        assert isinstance(my_sensor, rs.Sensor)
        assert my_sensor.name == "my_sensor"
        assert my_sensor.job_name == "my_job"

    def test_decorator_bare(self):
        @rs.Sensor()
        def my_sensor(context: rs.SensorEvaluationContext):
            return rs.RunRequest()

        assert isinstance(my_sensor, rs.Sensor)
        assert my_sensor.name == "my_sensor"

    def test_decorator_with_custom_name(self):
        @rs.Sensor(job_name="my_job", name="custom_name")
        def my_sensor(context: rs.SensorEvaluationContext):
            return rs.RunRequest()

        assert my_sensor.name == "custom_name"

    def test_decorator_with_all_options(self):
        @rs.Sensor(
            job_name="my_job",
            minimum_interval="30s",
            default_status=rs.SensorStatus.Running,
            description="Watches things",
            tags={"team": "data"},
        )
        def watcher(context: rs.SensorEvaluationContext):
            return rs.RunRequest()

        assert watcher.minimum_interval == "30s"
        assert watcher.default_status == rs.SensorStatus.Running
        assert watcher.description == "Watches things"
        assert watcher.tags == {"team": "data"}

    def test_decorator_with_asset_selection(self):
        @rs.Sensor(asset_selection=["a", "b"])
        def asset_watcher(context: rs.SensorEvaluationContext):
            return rs.RunRequest()

        assert asset_watcher.asset_selection == ["a", "b"]


# ---------------------------------------------------------------------------
# Sensor registration with CodeRepository
# ---------------------------------------------------------------------------


class TestSensorRegistration:
    def test_repo_with_sensors(self):
        @rs.Asset
        def my_asset() -> Any:
            return 42

        sens = rs.Sensor(name="my_sensor", asset_selection=["my_asset"])
        repo = rs.CodeRepository(assets=[my_asset], sensors=[sens])
        assert len(repo.sensors) == 1
        assert repo.sensors[0].name == "my_sensor"

    def test_get_sensor_by_name(self):
        sens = rs.Sensor(name="watcher", job_name="job1")

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[sens])
        found = repo.get_sensor("watcher")
        assert found.name == "watcher"
        assert found.job_name == "job1"

    def test_get_sensor_not_found(self):
        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a])
        with pytest.raises(NodeNotFoundError, match="nonexistent"):
            repo.get_sensor("nonexistent")

    def test_repo_with_schedules_and_sensors(self):
        @rs.Asset
        def a() -> Any:
            return 1

        job = rs.Job(name="my_job", assets=[a])
        sched = rs.Schedule(cron_schedule="0 0 * * *", job_name="my_job")
        sens = rs.Sensor(name="my_sensor", asset_selection=["a"])
        repo = rs.CodeRepository(
            assets=[a], jobs=[job], schedules=[sched], sensors=[sens]
        )
        assert len(repo.schedules) == 1
        assert len(repo.sensors) == 1


# ---------------------------------------------------------------------------
# Resolve-time validation of sensor run targets
# ---------------------------------------------------------------------------


class TestSensorResolveValidation:
    """A sensor's `job_name` / `asset_selection` are checked at resolve time —
    typo'd targets would otherwise silently fail every dispatch (stuck in queue
    or logged-and-swallowed) instead of surfacing during repo construction.
    """

    def test_rejects_sensor_with_neither_job_nor_selection(self):
        sens = rs.Sensor(name="my_sensor")

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[sens])
        with pytest.raises(
            SensorDefinitionError,
            match="must declare either `job_name` or `asset_selection`",
        ):
            repo.resolve()

    def test_rejects_sensor_with_unknown_job_name(self):
        sens = rs.Sensor(name="my_sensor", job_name="missing_job")

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[sens])
        with pytest.raises(
            SensorDefinitionError,
            match=r"Sensor 'my_sensor' references unknown job 'missing_job'",
        ):
            repo.resolve()

    def test_rejects_sensor_with_unknown_asset_in_selection(self):
        sens = rs.Sensor(name="my_sensor", asset_selection=["a", "missing_asset"])

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[sens])
        with pytest.raises(
            SensorDefinitionError,
            match=(
                r"Sensor 'my_sensor' references unknown asset "
                r"'missing_asset' in `asset_selection`"
            ),
        ):
            repo.resolve()

    def test_accepts_sensor_targeting_known_job(self):
        @rs.Asset
        def a() -> Any:
            return 1

        job = rs.Job(name="my_job", assets=[a])
        sens = rs.Sensor(name="my_sensor", job_name="my_job")
        repo = rs.CodeRepository(assets=[a], jobs=[job], sensors=[sens])
        repo.resolve()  # should not raise

    def test_accepts_sensor_targeting_known_assets(self):
        @rs.Asset
        def a() -> Any:
            return 1

        @rs.Asset
        def b() -> Any:
            return 2

        sens = rs.Sensor(name="my_sensor", asset_selection=["a", "b"])
        repo = rs.CodeRepository(assets=[a, b], sensors=[sens])
        repo.resolve()  # should not raise


# ---------------------------------------------------------------------------
# Sensor evaluation
# ---------------------------------------------------------------------------


class TestSensorEvaluation:
    def test_evaluate_returns_run_request(self):
        @rs.Sensor(job_name="my_job")
        def my_sensor(context: rs.SensorEvaluationContext):
            return rs.RunRequest(tags={"cursor": context.cursor or "none"})

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[my_sensor])
        result = repo.evaluate_sensor("my_sensor", cursor="abc")

        assert result.sensor_name == "my_sensor"
        assert len(result.run_requests) == 1
        assert result.run_requests[0].tags is not None
        assert result.run_requests[0].tags["cursor"] == "abc"
        assert result.run_requests[0].job_name == "my_job"
        assert result.skip_reason is None

    def test_evaluate_returns_skip(self):
        @rs.Sensor(job_name="my_job")
        def my_sensor(context: rs.SensorEvaluationContext):
            return rs.SkipReason("nothing new")

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[my_sensor])
        result = repo.evaluate_sensor("my_sensor")

        assert len(result.run_requests) == 0
        assert result.skip_reason is not None
        assert result.skip_reason.message == "nothing new"

    def test_evaluate_returns_none_skips(self):
        @rs.Sensor(job_name="my_job")
        def my_sensor(context: rs.SensorEvaluationContext):
            return None

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[my_sensor])
        result = repo.evaluate_sensor("my_sensor")

        assert len(result.run_requests) == 0
        assert result.skip_reason is not None

    def test_evaluate_returns_multiple_requests(self):
        @rs.Sensor(job_name="my_job")
        def my_sensor(context: rs.SensorEvaluationContext):
            return [
                rs.RunRequest(partition_key="a"),
                rs.RunRequest(partition_key="b"),
            ]

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[my_sensor])
        result = repo.evaluate_sensor("my_sensor")

        assert len(result.run_requests) == 2
        assert result.run_requests[0].partition_key == "a"
        assert result.run_requests[1].partition_key == "b"

    def test_evaluate_returns_sensor_result(self):
        @rs.Sensor(job_name="my_job")
        def my_sensor(context: rs.SensorEvaluationContext):
            return rs.SensorResult(
                run_requests=[rs.RunRequest(run_key="k1")],
                cursor="new_cursor",
            )

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[my_sensor])
        result = repo.evaluate_sensor("my_sensor")

        assert len(result.run_requests) == 1
        assert result.run_requests[0].run_key == "k1"
        assert result.cursor == "new_cursor"
        assert result.skip_reason is None

    def test_evaluate_sensor_result_with_skip(self):
        @rs.Sensor()
        def my_sensor(context: rs.SensorEvaluationContext):
            return rs.SensorResult(
                skip_reason="not ready",
                cursor="still_42",
            )

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[my_sensor])
        result = repo.evaluate_sensor("my_sensor")

        assert len(result.run_requests) == 0
        assert result.skip_reason is not None
        assert result.skip_reason.message == "not ready"
        assert result.cursor == "still_42"

    def test_evaluate_context_has_sensor_name(self):
        seen_name = []

        @rs.Sensor(job_name="j")
        def my_sensor(context: rs.SensorEvaluationContext):
            seen_name.append(context.sensor_name)
            return rs.RunRequest()

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[my_sensor])
        repo.evaluate_sensor("my_sensor")

        assert seen_name == ["my_sensor"]

    def test_evaluate_context_has_cursor(self):
        seen_cursor = []

        @rs.Sensor(job_name="j")
        def my_sensor(context: rs.SensorEvaluationContext):
            seen_cursor.append(context.cursor)
            return rs.RunRequest()

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[my_sensor])
        repo.evaluate_sensor("my_sensor", cursor="offset_99")

        assert seen_cursor == ["offset_99"]

    def test_evaluate_context_has_last_tick_time(self):
        seen_time = []

        @rs.Sensor(job_name="j")
        def my_sensor(context: rs.SensorEvaluationContext):
            seen_time.append(context.last_tick_time)
            return rs.RunRequest()

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[my_sensor])
        repo.evaluate_sensor("my_sensor", last_tick_time=1234567890.5)

        assert seen_time == [1234567890.5]

    def test_evaluate_context_no_cursor(self):
        seen_cursor = []

        @rs.Sensor(job_name="j")
        def my_sensor(context: rs.SensorEvaluationContext):
            seen_cursor.append(context.cursor)
            return rs.RunRequest()

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[my_sensor])
        repo.evaluate_sensor("my_sensor")

        assert seen_cursor == [None]

    def test_evaluate_nonexistent_sensor_raises(self):
        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a])
        with pytest.raises(NodeNotFoundError, match="nope"):
            repo.evaluate_sensor("nope")

    def test_evaluate_invalid_return_type_raises(self):
        @rs.Sensor(job_name="j")
        def bad(context: rs.SensorEvaluationContext):
            return 42

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[bad])
        with pytest.raises(ExecutionError, match="must return"):
            repo.evaluate_sensor("bad")

    def test_default_job_name_propagated(self):
        """RunRequest without job_name gets the sensor's job_name."""

        @rs.Sensor(job_name="target_job")
        def my_sensor(context: rs.SensorEvaluationContext):
            return rs.RunRequest()

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[my_sensor])
        result = repo.evaluate_sensor("my_sensor")

        assert result.run_requests[0].job_name == "target_job"

    def test_sensor_no_eval_fn_raises(self):
        """SensorDefinition without evaluation_fn raises on evaluate."""
        sens = rs.Sensor(name="no_fn", job_name="j")

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[sens])
        with pytest.raises(ConfigurationError, match="no evaluation function"):
            repo.evaluate_sensor("no_fn")

    def test_sensor_tick_result_repr(self):
        @rs.Sensor(job_name="j")
        def my_sensor(context: rs.SensorEvaluationContext):
            return rs.RunRequest()

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[my_sensor])
        result = repo.evaluate_sensor("my_sensor")
        assert (
            repr(result)
            == "SensorTickResult(sensor='my_sensor', run_requests=1, cursor=None)"
        )

    def test_sensor_result_job_name_propagated(self):
        """RunRequests inside SensorResult also get default job_name."""

        @rs.Sensor(job_name="default_job")
        def my_sensor(context: rs.SensorEvaluationContext):
            return rs.SensorResult(
                run_requests=[rs.RunRequest(), rs.RunRequest(job_name="override")],
            )

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[my_sensor])
        result = repo.evaluate_sensor("my_sensor")

        assert result.run_requests[0].job_name == "default_job"
        assert result.run_requests[1].job_name == "override"

    def test_evaluate_no_context_param(self):
        """Eval function without a context parameter should still work."""

        @rs.Sensor(job_name="my_job")
        def my_sensor():
            return rs.RunRequest(tags={"source": "no_ctx"})

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[my_sensor])
        result = repo.evaluate_sensor("my_sensor")

        assert len(result.run_requests) == 1
        assert result.run_requests[0].tags["source"] == "no_ctx"
        assert result.skip_reason is None

    def test_evaluate_unknown_param_raises(self):
        """Unknown parameters (not context or resource) should raise."""

        @rs.Sensor(job_name="my_job")
        def my_sensor(some_arg: str, context: rs.SensorEvaluationContext):
            return rs.RunRequest()

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[my_sensor])
        with pytest.raises(SensorDefinitionError, match="Unknown parameter 'some_arg'"):
            repo.evaluate_sensor("my_sensor")

    def test_cursor_state_pattern(self):
        """Simulate a typical cursor-based sensor pattern."""
        events = ["event_1", "event_2", "event_3"]

        @rs.Sensor(job_name="process_events")
        def event_sensor(context: rs.SensorEvaluationContext):
            offset = int(context.cursor) if context.cursor else 0
            if offset >= len(events):
                return rs.SkipReason("no new events")
            return rs.SensorResult(
                run_requests=[rs.RunRequest(tags={"event": events[offset]})],
                cursor=str(offset + 1),
            )

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], sensors=[event_sensor])

        # First tick: no cursor
        r1 = repo.evaluate_sensor("event_sensor")
        assert len(r1.run_requests) == 1
        assert r1.run_requests[0].tags is not None
        assert r1.run_requests[0].tags["event"] == "event_1"
        assert r1.cursor == "1"

        # Second tick: cursor from first
        r2 = repo.evaluate_sensor("event_sensor", cursor=r1.cursor)
        assert r2.run_requests[0].tags is not None
        assert r2.run_requests[0].tags["event"] == "event_2"
        assert r2.cursor == "2"

        # Third tick
        r3 = repo.evaluate_sensor("event_sensor", cursor=r2.cursor)
        assert r3.run_requests[0].tags is not None
        assert r3.run_requests[0].tags["event"] == "event_3"
        assert r3.cursor == "3"

        # Fourth tick: no more events
        r4 = repo.evaluate_sensor("event_sensor", cursor=r3.cursor)
        assert len(r4.run_requests) == 0
        assert r4.skip_reason is not None
        assert r4.skip_reason.message == "no new events"


def test_sensor_context_log():
    """SensorEvaluationContext.log returns a named logger."""
    ctx = rs.SensorEvaluationContext("my_sensor")
    assert isinstance(ctx.log, logging.Logger)
    assert ctx.log.name == "code-repo.sensors.my_sensor"


def test_sensor_context_log_cached():
    """Repeated access returns the same logger instance."""
    ctx = rs.SensorEvaluationContext("my_sensor")
    assert ctx.log is ctx.log


def test_sensor_context_repr():
    """SensorEvaluationContext repr includes sensor_name and cursor."""
    ctx = rs.SensorEvaluationContext("my_sensor", cursor="offset_42")
    assert (
        repr(ctx)
        == "SensorEvaluationContext(sensor_name='my_sensor', cursor=Some(\"offset_42\"))"
    )


def test_sensor_context_repr_no_cursor():
    """SensorEvaluationContext repr with no cursor."""
    ctx = rs.SensorEvaluationContext("my_sensor")
    assert repr(ctx) == "SensorEvaluationContext(sensor_name='my_sensor', cursor=None)"


def test_sensor_context_pickle_roundtrip():
    """SensorEvaluationContext survives pickle/unpickle."""
    ctx = rs.SensorEvaluationContext(
        "my_sensor", cursor="c1", last_tick_time=1234567890.0
    )
    restored = pickle.loads(pickle.dumps(ctx))
    assert restored.sensor_name == "my_sensor"
    assert restored.cursor == "c1"
    assert restored.last_tick_time == 1234567890.0
