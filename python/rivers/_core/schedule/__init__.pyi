"""Schedules and the run/backfill request types they emit."""

import logging
from enum import IntEnum
from typing import Any, Callable, Generic, TypeVar, overload

ConfigT = TypeVar("ConfigT")

class EvalMode(IntEnum):
    """How a schedule / sensor evaluation function is run."""

    Auto = 0
    """Pick automatically based on whether the function is async-safe."""
    InProcess = 1
    """Run in the daemon process."""
    Subprocess = 2
    """Run in a loky worker subprocess (isolates user code, allows blocking I/O)."""

class RunRequest:
    """Request the daemon to launch a run.

    Yielded (or returned in a list) from a schedule / sensor evaluation function.
    """

    run_key: str | None
    """Idempotency key — duplicates with the same key are deduped."""
    tags: dict[str, str] | None
    """Run tags applied to the launched run."""
    partition_key: str | None
    """Partition to materialize (``None`` for non-partitioned)."""
    job_name: str | None
    """Job to launch (defaults to the schedule's bound job)."""

    def __init__(
        self,
        run_key: str | None = None,
        tags: dict[str, str] | None = None,
        partition_key: str | None = None,
        job_name: str | None = None,
    ) -> None:
        """Build a run request — see field docs for argument meanings."""
        ...

    def __repr__(self) -> str: ...

class BackfillRequest:
    """Request the daemon to launch a backfill across a set of partitions."""

    selection: list[str]
    partition_keys: list[Any] | None
    partition_range: Any | None
    strategy: Any | None
    failure_policy: str | None
    max_concurrency: int
    tags: dict[str, str] | None

    def __init__(
        self,
        selection: list[str],
        partition_keys: list[Any] | None = None,
        partition_range: Any | None = None,
        strategy: Any | None = None,
        failure_policy: str | None = None,
        max_concurrency: int = 4,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Mirror of :meth:`CodeRepository.backfill` arguments."""
        ...

    def __repr__(self) -> str: ...

class SkipReason:
    """Returned from a schedule/sensor to record why no run was requested."""

    message: str

    def __init__(self, message: str = "") -> None:
        """Construct a skip reason with the given human-readable message."""
        ...

    def __repr__(self) -> str: ...

class ScheduleEvaluationContext(Generic[ConfigT]):
    """Context object passed to a schedule's evaluation function."""

    scheduled_execution_time: str
    """RFC 3339 timestamp this tick is associated with."""
    schedule_name: str
    """Name of the schedule firing this tick."""
    @property
    def config(self) -> ConfigT:
        """Resolved schedule config (typed via the generic parameter)."""
        ...

    @property
    def log(self) -> logging.Logger:
        """Logger that ships records back through the run event log."""
        ...

    def __init__(
        self,
        scheduled_execution_time: str,
        schedule_name: str,
        config: Any | None = None,
    ) -> None:
        """Construct a context (typically only the daemon / tests)."""
        ...

class ScheduleStatus:
    """Whether a schedule is actively ticking."""

    Running: "ScheduleStatus"
    """Schedule is enabled and ticking."""
    Stopped: "ScheduleStatus"
    """Schedule is registered but paused."""

class Schedule:
    """A cron-driven trigger that emits :class:`RunRequest` / :class:`BackfillRequest`.

    Decorate a function with ``@Schedule(cron_schedule=..., job_name=...)``
    or construct directly. The function receives a
    :class:`ScheduleEvaluationContext` and may return / yield request objects
    or a :class:`SkipReason`.
    """

    name: str
    cron_schedule: str
    job_name: str
    evaluation_fn: Callable[..., Any] | None
    default_status: ScheduleStatus
    timezone: str | None
    tags: dict[str, str] | None
    description: str | None
    eval_mode: EvalMode
    eval_timeout: str | None

    @overload
    def __init__(
        self,
        func: Callable[..., Any],
        *,
        cron_schedule: str,
        job_name: str,
        name: str | None = None,
        default_status: ScheduleStatus = ...,
        timezone: str | None = None,
        tags: dict[str, str] | None = None,
        description: str | None = None,
        eval_mode: EvalMode = EvalMode.Auto,
        eval_timeout: str | None = None,
    ) -> None:
        """Construct a schedule wrapping ``func``.

        Args:
            func: Evaluation callable receiving a :class:`ScheduleEvaluationContext`.
            cron_schedule: Cron expression (e.g. ``"0 * * * *"``).
            job_name: Job that requested runs will execute.
            name: Override the schedule name (defaults to ``func.__name__``).
            default_status: Initial enabled / paused state.
            timezone: Optional timezone for the cron expression.
            tags: Tags applied to every requested run.
            description: Human-readable description.
            eval_mode: How the evaluation function is dispatched (in-process vs subprocess).
            eval_timeout: Per-tick timeout (humantime string).
        """
        ...

    @overload
    def __init__(
        self,
        *,
        cron_schedule: str,
        job_name: str,
        name: str | None = None,
        default_status: ScheduleStatus = ...,
        timezone: str | None = None,
        tags: dict[str, str] | None = None,
        description: str | None = None,
        eval_mode: EvalMode = EvalMode.Auto,
        eval_timeout: str | None = None,
    ) -> None:
        """Decorator-factory form — apply the resulting object to a function via ``@``."""
        ...
    def __call__(self, func: Callable[..., Any]) -> "Schedule":
        """Decorator form — ``@Schedule(cron_schedule=..., job_name=...)``."""
        ...

    def __repr__(self) -> str: ...

class ScheduleTickResult:
    """Outcome of one schedule evaluation — used by tests and the daemon."""

    schedule_name: str
    run_requests: list[RunRequest | BackfillRequest]
    skip_reason: SkipReason | None
    def __repr__(self) -> str: ...
