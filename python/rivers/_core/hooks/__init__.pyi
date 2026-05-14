"""Lifecycle hooks fired around asset materializations."""

from typing import Any, Callable, Generic, TypeVar, overload

ConfigT = TypeVar("ConfigT")

class HookContext(Generic[ConfigT]):
    """Context passed to a hook callback when it fires."""

    asset_name: str
    """Asset whose materialization triggered the hook."""
    run_id: str
    """ID of the run this hook is firing inside."""
    hook_type: str
    """Either ``"success"`` or ``"failure"``."""
    output: Any | None
    """Asset output on success, ``None`` on failure."""
    error: str | None
    """Error message on failure, ``None`` on success."""
    metadata: dict[str, str] | None
    """Asset metadata recorded for the materialization."""
    @property
    def config(self) -> ConfigT:
        """Resolved hook config (typed via the generic parameter)."""
        ...

class Hook:
    """A callable hook attachable to assets via the ``hooks=[...]`` parameter."""

    @property
    def name(self) -> str:
        """Display name of the hook (defaults to the wrapped function name)."""
        ...

    def __repr__(self) -> str: ...
    def __call__(self, func: Callable[..., Any]) -> "Hook":
        """Allow ``@Hook(name=...)`` decoration syntax."""
        ...

    class Success(Hook):
        """Hook subtype that fires only on successful materializations."""

    class Failure(Hook):
        """Hook subtype that fires only on failed materializations."""

    @overload
    @staticmethod
    def success(func: Callable[..., Any]) -> Hook: ...
    @overload
    @staticmethod
    def success(*, name: str | None = None) -> Hook: ...
    @staticmethod
    def success(
        func: Callable[..., Any] | None = None,
        *,
        name: str | None = None,
    ) -> Hook:
        """Create a success hook from a callable.

        Use bare (``@Hook.success``) or with options (``@Hook.success(name="alert")``).
        """
        ...

    @overload
    @staticmethod
    def failure(func: Callable[..., Any]) -> Hook: ...
    @overload
    @staticmethod
    def failure(*, name: str | None = None) -> Hook: ...
    @staticmethod
    def failure(
        func: Callable[..., Any] | None = None,
        *,
        name: str | None = None,
    ) -> Hook:
        """Create a failure hook from a callable.

        Use bare (``@Hook.failure``) or with options (``@Hook.failure(name="page")``).
        """
        ...
