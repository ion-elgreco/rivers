"""Sensors — event-driven triggers (counterpart to time-driven Schedules)."""

import logging
from collections.abc import Sequence
from typing import Any, Callable, Generic, TypeVar, overload

from rivers._core.schedule import BackfillRequest, EvalMode, RunRequest, SkipReason

ConfigT = TypeVar("ConfigT")

class SensorEvaluationContext(Generic[ConfigT]):
    """Context object passed to a sensor's evaluation function."""

    sensor_name: str
    """Name of the sensor firing this tick."""
    cursor: str | None
    """The cursor returned by the previous tick (sensor-defined opaque string)."""
    last_tick_time: float | None
    """Seconds-since-epoch timestamp of the previous tick (``None`` on first run)."""
    @property
    def config(self) -> ConfigT:
        """Resolved sensor config (typed via the generic parameter)."""
        ...

    @property
    def log(self) -> logging.Logger:
        """Logger that ships records back through the run event log."""
        ...

    def __init__(
        self,
        sensor_name: str,
        cursor: str | None = None,
        last_tick_time: float | None = None,
        config: Any | None = None,
    ) -> None:
        """Construct a context (typically only the daemon / tests)."""
        ...

class SensorStatus:
    """Whether a sensor is actively ticking."""

    Running: SensorStatus
    """Sensor is enabled and ticking."""
    Stopped: SensorStatus
    """Sensor is registered but paused."""

class SensorResult:
    """Bundled return value: requests to launch, skip reason, and a new cursor."""

    run_requests: list[RunRequest | BackfillRequest] | None
    skip_reason: SkipReason | None
    cursor: str | None
    def __init__(
        self,
        run_requests: Sequence[RunRequest | BackfillRequest] | None = None,
        skip_reason: str | SkipReason | None = None,
        cursor: str | None = None,
    ) -> None:
        """Build a sensor result.

        Args:
            run_requests: New runs / backfills to launch.
            skip_reason: Optional explanation when no runs are requested.
            cursor: Cursor to surface to the next tick.
        """
        ...

class Sensor:
    """An event-driven trigger evaluated by the automation daemon.

    Decorate a function with ``@Sensor(job_name=..., minimum_interval=...)`` or
    construct directly. The function receives a :class:`SensorEvaluationContext`
    and returns a :class:`SensorResult`, an iterable of :class:`RunRequest`,
    or a :class:`SkipReason`.
    """

    name: str
    job_name: str | None
    evaluation_fn: Callable[..., Any] | None
    minimum_interval: str | None
    default_status: SensorStatus
    description: str | None
    tags: dict[str, str] | None
    asset_selection: list[str] | None
    eval_mode: EvalMode
    eval_timeout: str | None

    @overload
    def __init__(
        self,
        func: Callable[..., Any],
        *,
        name: str | None = None,
        job_name: str | None = None,
        minimum_interval: str | None = None,
        default_status: SensorStatus = ...,
        description: str | None = None,
        tags: dict[str, str] | None = None,
        asset_selection: list[str] | None = None,
        eval_mode: EvalMode = EvalMode.Auto,
        eval_timeout: str | None = None,
    ) -> None:
        """Construct a sensor wrapping ``func``.

        Args:
            func: Evaluation callable receiving a :class:`SensorEvaluationContext`.
            name: Override the sensor name (defaults to ``func.__name__``).
            job_name: Default job for emitted run requests (may be overridden per request).
            minimum_interval: Minimum gap between ticks (humantime, e.g. ``"30s"``).
            default_status: Initial enabled / paused state.
            description: Human-readable description.
            tags: Tags applied to every requested run.
            asset_selection: Restrict the sensor to listening for these assets only.
            eval_mode: How the evaluation function is dispatched (in-process vs subprocess).
            eval_timeout: Per-tick timeout (humantime string).
        """
        ...

    @overload
    def __init__(
        self,
        *,
        name: str | None = None,
        job_name: str | None = None,
        minimum_interval: str | None = None,
        default_status: SensorStatus = ...,
        description: str | None = None,
        tags: dict[str, str] | None = None,
        asset_selection: list[str] | None = None,
        eval_mode: EvalMode = EvalMode.Auto,
        eval_timeout: str | None = None,
    ) -> None:
        """Decorator-factory form — apply the resulting object to a function via ``@``."""
        ...
    def __call__(self, func: Callable[..., Any]) -> "Sensor":
        """Decorator form — ``@Sensor(job_name=...)``."""
        ...

    def __repr__(self) -> str: ...

class SensorTickResult:
    """Outcome of one sensor evaluation — used by tests and the daemon."""

    sensor_name: str
    run_requests: list[RunRequest | BackfillRequest]
    skip_reason: SkipReason | None
    cursor: str | None
