"""Base class for user-defined resources injected into asset/task functions."""

from pydantic_settings import BaseSettings


class Resource(BaseSettings):
    """Base class for rivers resources.

    Extends pydantic-settings BaseSettings with lifecycle hooks.
    Fields can be set explicitly or resolved from environment variables.
    Override setup() and teardown() only if your resource needs initialization/cleanup.
    """

    def setup(self) -> None:
        """Called once at resolve time. Override to initialize connections, pools, etc."""
        pass

    def teardown(self) -> None:
        """Called at repository shutdown. Override to close connections, flush buffers, etc."""
        pass
