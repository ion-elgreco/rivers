"""The resource parameter of the asset functions that integrations make."""

import inspect
from collections.abc import Callable
from typing import Any, TypeVar

_F = TypeVar("_F", bound=Callable[..., Any])


def with_resource_key(fn: _F, resource_key: str) -> _F:
    """``fn``, with its resource parameter named ``resource_key``: rivers
    injects a resource into the parameter named after its key."""
    signature = inspect.signature(fn)
    context_param, resource_param = signature.parameters.values()
    fn.__signature__ = signature.replace(  # type: ignore[attr-defined]
        parameters=[context_param, resource_param.replace(name=resource_key)]
    )
    return fn
