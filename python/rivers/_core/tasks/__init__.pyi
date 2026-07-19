"""Tasks — the unit of work composed inside ``@Asset.from_graph`` graphs."""

import datetime
import logging
from typing import Any, Generic, TypeVar

from rivers._core import IOHandler, RetryPolicy
from rivers._core.assets import AssetDef
from rivers._core.partitions import (
    PartitionContext,
    PartitionMapping,
    PartitionsDefinition,
)

ConfigT = TypeVar("ConfigT")

class BashTask:
    """A task that shells out to a command. Output is the command's stdout (str)."""

    @property
    def name(self) -> str:
        """Task name."""
        ...

    @property
    def tags(self) -> list[str] | None:
        """Tags propagated to runs that include this task."""
        ...

    @property
    def command(self) -> str | list[str]:
        """The shell command — string (interpreted by ``sh``) or argv list."""
        ...

    @property
    def env(self) -> dict[str, str] | None:
        """Extra environment variables merged on top of the inherited env."""
        ...

    @property
    def cwd(self) -> str | None:
        """Working directory for the subprocess, ``None`` to inherit."""
        ...

    def __init__(
        self,
        name: str,
        command: str | list[str],
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        tags: list[str] | None = None,
        partition_mapping: dict[str | AssetDef, PartitionMapping] | None = None,
        io_handler: IOHandler | str | None = None,
        retry: "RetryPolicy | str | None" = None,
    ) -> None:
        """Construct a bash task.

        Args:
            name: Task name (must be unique within the repository).
            command: Shell string or argv list.
            env: Extra environment variables.
            cwd: Working directory (default: caller cwd).
            tags: Run tags applied when this task executes.
            partition_mapping: Per-input partition mapping overrides.
            io_handler: Override the IO handler for this task's outputs.
            retry: Retry policy for this task's step, or the name of a policy
                registered in ``CodeRepository(retries={...})``.
        """
        ...

    def __call__(self, *args: Any, **kwargs: Any) -> str:
        """Direct invocation — runs the command and returns its stdout."""
        ...

class Task:
    """A Python-callable task wrapped for use in ``@Asset.from_graph``.

    Use as a decorator (``@Task`` / ``@Task(name=...)``) or a constructor
    around an existing function.
    """

    @property
    def is_async(self) -> bool:
        """``True`` when the wrapped function is a coroutine function."""
        ...

    @property
    def name(self) -> str | None:
        """Task name (defaults to the wrapped function name)."""
        ...

    @property
    def tags(self) -> list[str] | None:
        """Tags propagated to runs that include this task."""
        ...

    def __init__(
        self,
        wraps: Any | None = None,
        name: str | None = None,
        tags: list[str] | None = None,
        partitions_def: PartitionsDefinition | str | None = None,
        partition_mapping: dict[str | AssetDef, PartitionMapping] | None = None,
        io_handler: IOHandler | str | None = None,
        retry: "RetryPolicy | str | None" = None,
    ) -> None:
        """Construct a task or build a decorator factory.

        Args:
            wraps: Function to wrap; ``None`` for the ``@Task(...)`` form.
            name: Override the task name.
            tags: Run tags.
            partitions_def: Partitions definition for this task's outputs, or
                the name of a definition registered in
                ``CodeRepository(partition_defs={...})``.
            partition_mapping: Per-input partition mapping overrides.
            io_handler: Override the IO handler for this task's outputs.
            retry: Retry policy for this task's step, or the name of a policy
                registered in ``CodeRepository(retries={...})``.
        """
        ...

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Invoke the wrapped function (or apply this object as a decorator)."""
        ...

class TaskExecutionContext(Generic[ConfigT]):
    """Context object injected into task functions that declare a ``context`` parameter."""

    task_name: str
    """Name of the task currently executing."""
    tags: list[str] | None
    """Run tags."""
    partition: PartitionContext | None
    """Partition being materialized (``None`` for non-partitioned tasks)."""
    def __init__(
        self,
        task_name: str,
        tags: list[str] | None = None,
        partition: PartitionContext | None = None,
    ) -> None:
        """Construct a context (typically only the executor calls this)."""
        ...

    @property
    def has_partition_key(self) -> bool:
        """``True`` when ``partition`` is set."""
        ...

    @property
    def partition_key(self) -> str:
        """String form of the current partition key (raises when not partitioned)."""
        ...

    @property
    def partition_time_window(
        self,
    ) -> tuple[datetime.datetime, datetime.datetime] | None:
        """Time window for time-windowed partitions, else ``None``."""
        ...

    @property
    def log(self) -> logging.Logger:
        """Logger that ships records back through the run event log."""
        ...

    @property
    def config(self) -> ConfigT:
        """Resolved task config (typed via the generic parameter)."""
        ...

__all__ = [
    "BashTask",
    "Task",
    "TaskExecutionContext",
]
