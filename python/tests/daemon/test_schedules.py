"""Tests for schedule definitions and evaluation."""

import logging
import pickle
from typing import Any

import pytest

import rivers as rs
from rivers.exceptions import ExecutionError, NodeNotFoundError, ScheduleDefinitionError

# ---------------------------------------------------------------------------
# RunRequest and SkipReason
# ---------------------------------------------------------------------------


class TestRunRequest:
    def test_default_construction(self):
        req = rs.RunRequest()
        assert req.run_key is None
        assert req.tags is None
        assert req.partition_key is None
        assert req.job_name is None

    def test_with_all_fields(self):
        req = rs.RunRequest(
            run_key="key1",
            tags={"env": "prod"},
            partition_key="2025-01-01",
            job_name="my_job",
        )
        assert req.run_key == "key1"
        assert req.tags == {"env": "prod"}
        assert req.partition_key == "2025-01-01"
        assert req.job_name == "my_job"

    def test_repr(self):
        req = rs.RunRequest(run_key="k")
        assert (
            repr(req)
            == 'RunRequest(run_key=Some("k"), tags=None, partition_key=None, job_name=None)'
        )


class TestSkipReason:
    def test_default_message(self):
        skip = rs.SkipReason()
        assert skip.message == ""

    def test_with_message(self):
        skip = rs.SkipReason("not ready")
        assert skip.message == "not ready"

    def test_repr(self):
        skip = rs.SkipReason("test")
        assert repr(skip) == "SkipReason('test')"


# ---------------------------------------------------------------------------
# Schedule
# ---------------------------------------------------------------------------


class TestSchedule:
    def test_basic_construction(self):
        sched = rs.Schedule(cron_schedule="0 0 * * *", job_name="my_job")
        assert sched.cron_schedule == "0 0 * * *"
        assert sched.job_name == "my_job"
        assert sched.name == "my_job_schedule"  # auto-generated

    def test_custom_name(self):
        sched = rs.Schedule(
            cron_schedule="0 0 * * *", job_name="my_job", name="nightly"
        )
        assert sched.name == "nightly"

    def test_with_all_options(self):
        sched = rs.Schedule(
            cron_schedule="0 12 * * *",
            job_name="daily_job",
            name="noon_schedule",
            default_status=rs.ScheduleStatus.Running,
            timezone="US/Eastern",
            tags={"env": "prod"},
            description="Runs at noon",
        )
        assert sched.cron_schedule == "0 12 * * *"
        assert sched.job_name == "daily_job"
        assert sched.name == "noon_schedule"
        assert sched.default_status == rs.ScheduleStatus.Running
        assert sched.timezone == "US/Eastern"
        assert sched.tags == {"env": "prod"}
        assert sched.description == "Runs at noon"

    def test_empty_cron_raises(self):
        with pytest.raises(ScheduleDefinitionError, match="cron_schedule"):
            rs.Schedule(cron_schedule="", job_name="my_job")

    def test_empty_job_name_raises(self):
        with pytest.raises(ScheduleDefinitionError, match="job_name"):
            rs.Schedule(cron_schedule="0 0 * * *", job_name="")

    def test_repr(self):
        sched = rs.Schedule(
            cron_schedule="0 0 * * *", job_name="my_job", name="nightly"
        )
        assert repr(sched) == "Schedule(name='nightly', cron='0 0 * * *', job='my_job')"

    def test_default_status_is_running(self):
        sched = rs.Schedule(cron_schedule="0 0 * * *", job_name="my_job")
        assert sched.default_status == rs.ScheduleStatus.Running


# ---------------------------------------------------------------------------
# @Schedule decorator
# ---------------------------------------------------------------------------


class TestScheduleDecorator:
    def test_decorator_with_args(self):
        @rs.Schedule(cron_schedule="0 0 * * *", job_name="my_job")
        def nightly(context: rs.ScheduleEvaluationContext):
            return rs.RunRequest()

        assert isinstance(nightly, rs.Schedule)
        assert nightly.name == "nightly"
        assert nightly.cron_schedule == "0 0 * * *"
        assert nightly.job_name == "my_job"

    def test_decorator_with_custom_name(self):
        @rs.Schedule(cron_schedule="0 0 * * *", job_name="my_job", name="custom")
        def my_sched(context: rs.ScheduleEvaluationContext):
            return rs.RunRequest()

        assert my_sched.name == "custom"

    def test_decorator_empty_cron_raises(self):
        with pytest.raises(ScheduleDefinitionError):

            @rs.Schedule(cron_schedule="", job_name="my_job")
            def bad(ctx: Any):
                pass


# ---------------------------------------------------------------------------
# Schedule registration with CodeRepository
# ---------------------------------------------------------------------------


class TestScheduleRegistration:
    def test_repo_with_schedules(self):
        @rs.Asset
        def my_asset() -> Any:
            return 42

        job = rs.Job(name="my_pipeline", assets=[my_asset])
        sched = rs.Schedule(cron_schedule="0 0 * * *", job_name="my_pipeline")

        repo = rs.CodeRepository(assets=[my_asset], jobs=[job], schedules=[sched])
        assert len(repo.schedules) == 1
        assert repo.schedules[0].name == "my_pipeline_schedule"

    def test_get_schedule_by_name(self):
        sched = rs.Schedule(cron_schedule="0 0 * * *", job_name="job1", name="nightly")

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], schedules=[sched])
        found = repo.get_schedule("nightly")
        assert found.cron_schedule == "0 0 * * *"

    def test_get_schedule_not_found(self):
        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a])
        with pytest.raises(NodeNotFoundError, match="nonexistent"):
            repo.get_schedule("nonexistent")


# ---------------------------------------------------------------------------
# Resolve-time validation of schedule run targets
# ---------------------------------------------------------------------------


class TestScheduleResolveValidation:
    """A schedule's `job_name` must reference a real job — checked at resolve
    time so a typo'd target fails during repo construction instead of producing
    failed runs at every cron tick.
    """

    def test_rejects_schedule_with_unknown_job_name(self):
        @rs.Asset
        def a() -> Any:
            return 1

        sched = rs.Schedule(
            cron_schedule="0 0 * * *", job_name="missing_job", name="nightly"
        )
        repo = rs.CodeRepository(assets=[a], schedules=[sched])
        with pytest.raises(
            ScheduleDefinitionError,
            match=r"Schedule 'nightly' references unknown job 'missing_job'",
        ):
            repo.resolve()

    def test_accepts_schedule_targeting_known_job(self):
        @rs.Asset
        def a() -> Any:
            return 1

        job = rs.Job(name="my_job", assets=[a])
        sched = rs.Schedule(cron_schedule="0 0 * * *", job_name="my_job")
        repo = rs.CodeRepository(assets=[a], jobs=[job], schedules=[sched])
        repo.resolve()  # should not raise


# ---------------------------------------------------------------------------
# Schedule evaluation
# ---------------------------------------------------------------------------


class TestScheduleEvaluation:
    def test_evaluate_returns_run_request(self):
        @rs.Schedule(cron_schedule="0 0 * * *", job_name="my_job")
        def my_sched(context: rs.ScheduleEvaluationContext):
            return rs.RunRequest(tags={"date": context.scheduled_execution_time})

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], schedules=[my_sched])
        result = repo.evaluate_schedule(
            "my_sched", execution_time="2025-03-10T00:00:00"
        )

        assert result.schedule_name == "my_sched"
        assert len(result.run_requests) == 1
        assert result.run_requests[0].tags is not None
        assert result.run_requests[0].tags["date"] == "2025-03-10T00:00:00"
        assert result.run_requests[0].job_name == "my_job"
        assert result.skip_reason is None

    def test_evaluate_returns_skip(self):
        @rs.Schedule(cron_schedule="0 0 * * *", job_name="my_job")
        def my_sched(context: rs.ScheduleEvaluationContext):
            return rs.SkipReason("not ready")

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], schedules=[my_sched])
        result = repo.evaluate_schedule("my_sched")

        assert len(result.run_requests) == 0
        assert result.skip_reason is not None
        assert result.skip_reason.message == "not ready"

    def test_evaluate_returns_none_skips(self):
        @rs.Schedule(cron_schedule="0 0 * * *", job_name="my_job")
        def my_sched(context: rs.ScheduleEvaluationContext):
            return None

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], schedules=[my_sched])
        result = repo.evaluate_schedule("my_sched")

        assert len(result.run_requests) == 0
        assert result.skip_reason is not None

    def test_evaluate_returns_multiple_requests(self):
        @rs.Schedule(cron_schedule="0 0 * * *", job_name="my_job")
        def my_sched(context: rs.ScheduleEvaluationContext):
            return [
                rs.RunRequest(partition_key="a"),
                rs.RunRequest(partition_key="b"),
            ]

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], schedules=[my_sched])
        result = repo.evaluate_schedule("my_sched")

        assert len(result.run_requests) == 2
        assert result.run_requests[0].partition_key == "a"
        assert result.run_requests[1].partition_key == "b"

    def test_evaluate_no_fn_creates_default_request(self):
        sched = rs.Schedule(
            cron_schedule="0 0 * * *",
            job_name="my_job",
            name="auto",
            tags={"source": "auto"},
        )

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], schedules=[sched])
        result = repo.evaluate_schedule("auto")

        assert len(result.run_requests) == 1
        assert result.run_requests[0].job_name == "my_job"
        assert result.run_requests[0].tags == {"source": "auto"}

    def test_evaluate_context_has_schedule_name(self):
        seen_name = []

        @rs.Schedule(cron_schedule="0 0 * * *", job_name="j")
        def my_sched(context: rs.ScheduleEvaluationContext):
            seen_name.append(context.schedule_name)
            return rs.RunRequest()

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], schedules=[my_sched])
        repo.evaluate_schedule("my_sched")

        assert seen_name == ["my_sched"]

    def test_evaluate_context_has_execution_time(self):
        seen_time = []

        @rs.Schedule(cron_schedule="0 0 * * *", job_name="j")
        def my_sched(context: rs.ScheduleEvaluationContext):
            seen_time.append(context.scheduled_execution_time)
            return rs.RunRequest()

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], schedules=[my_sched])
        repo.evaluate_schedule("my_sched", execution_time="2025-03-10")

        assert seen_time == ["2025-03-10"]

    def test_evaluate_nonexistent_schedule_raises(self):
        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a])
        with pytest.raises(NodeNotFoundError, match="nope"):
            repo.evaluate_schedule("nope")

    def test_evaluate_invalid_return_type_raises(self):
        @rs.Schedule(cron_schedule="0 0 * * *", job_name="j")
        def bad(context: rs.ScheduleEvaluationContext):
            return 42  # Invalid return type

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], schedules=[bad])
        with pytest.raises(ExecutionError, match="must return"):
            repo.evaluate_schedule("bad")

    def test_default_job_name_propagated_to_request(self):
        """RunRequest without job_name gets the schedule's job_name."""

        @rs.Schedule(cron_schedule="0 0 * * *", job_name="target_job")
        def my_sched(context: rs.ScheduleEvaluationContext):
            return rs.RunRequest()  # No job_name specified

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], schedules=[my_sched])
        result = repo.evaluate_schedule("my_sched")

        assert result.run_requests[0].job_name == "target_job"

    def test_evaluate_no_context_param(self):
        """Eval function without a context parameter should still work."""

        @rs.Schedule(cron_schedule="0 0 * * *", job_name="my_job")
        def my_sched():
            return rs.RunRequest(tags={"source": "no_ctx"})

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], schedules=[my_sched])
        result = repo.evaluate_schedule("my_sched")

        assert len(result.run_requests) == 1
        assert result.run_requests[0].tags["source"] == "no_ctx"
        assert result.skip_reason is None

    def test_evaluate_unknown_param_raises(self):
        """Unknown parameters (not context or resource) should raise."""

        @rs.Schedule(cron_schedule="0 0 * * *", job_name="my_job")
        def my_sched(some_arg: str, context: rs.ScheduleEvaluationContext):
            return rs.RunRequest()

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], schedules=[my_sched])
        with pytest.raises(
            ScheduleDefinitionError, match="Unknown parameter 'some_arg'"
        ):
            repo.evaluate_schedule("my_sched")

    def test_schedule_tick_result_repr(self):
        @rs.Schedule(cron_schedule="0 0 * * *", job_name="j")
        def my_sched(context: rs.ScheduleEvaluationContext):
            return rs.RunRequest()

        @rs.Asset
        def a() -> Any:
            return 1

        repo = rs.CodeRepository(assets=[a], schedules=[my_sched])
        result = repo.evaluate_schedule("my_sched")
        assert repr(result) == "ScheduleTickResult(schedule='my_sched', run_requests=1)"


def test_schedule_context_log():
    """ScheduleEvaluationContext.log returns a named logger."""
    ctx = rs.ScheduleEvaluationContext("2025-01-01T00:00:00", "my_sched")
    assert isinstance(ctx.log, logging.Logger)
    assert ctx.log.name == "code-repo.schedules.my_sched"


def test_schedule_context_log_cached():
    """Repeated access returns the same logger instance."""
    ctx = rs.ScheduleEvaluationContext("2025-01-01T00:00:00", "my_sched")
    assert ctx.log is ctx.log


def test_schedule_context_repr():
    """ScheduleEvaluationContext repr includes schedule_name and execution_time."""
    ctx = rs.ScheduleEvaluationContext("2025-03-10T00:00:00", "nightly")
    assert (
        repr(ctx)
        == "ScheduleEvaluationContext(schedule_name='nightly', scheduled_execution_time='2025-03-10T00:00:00')"
    )


def test_schedule_context_pickle_roundtrip():
    """ScheduleEvaluationContext survives pickle/unpickle."""
    ctx = rs.ScheduleEvaluationContext("2025-03-10T12:00:00", "daily_sched")
    restored = pickle.loads(pickle.dumps(ctx))
    assert restored.scheduled_execution_time == "2025-03-10T12:00:00"
    assert restored.schedule_name == "daily_sched"
