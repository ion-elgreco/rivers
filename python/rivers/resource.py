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


def _worker_call_with_resources(func, resource_specs, resource_positions, *args):
    """Worker-side wrapper that deserializes resources, calls setup/teardown per worker.

    Called by loky in worker processes. Receives:
    - func: the actual asset function
    - resource_specs: list of (param_index, resource_class, json_data) tuples
    - resource_positions: list of param indices that are resource placeholders
    - *args: the pre-resolved non-resource arguments (resources replaced with None placeholders)

    For each resource: deserializes from JSON, calls setup(), injects into args,
    calls the function, then calls teardown() on all resources.
    """
    args = list(args)
    deserialized = []

    for param_idx, cls, json_data in resource_specs:
        resource = cls.model_validate_json(json_data)
        resource.setup()
        deserialized.append(resource)
        args[param_idx] = resource

    try:
        return func(*args)
    finally:
        for resource in deserialized:
            try:
                resource.teardown()
            except Exception:
                pass
