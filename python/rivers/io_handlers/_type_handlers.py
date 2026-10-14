"""Discovery and lookup of the type handlers that a table-format IO handler delegates to."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

Candidate = tuple[str, str, Sequence[str]]
"""One type handler: ``(module, class name, constructor params)``."""


def discover(
    candidates: Sequence[Candidate], handler_config: dict[str, Any] | None
) -> dict[type, Any]:
    """Map each supported type to its handler, for the installed handlers only.

    A candidate whose library cannot be imported is skipped. The constructor
    params are read from ``handler_config``.
    """
    handlers: dict[type, Any] = {}
    for mod_path, cls_name, cls_params in candidates:
        try:
            mod = __import__(mod_path, fromlist=[cls_name])
            kwargs = {}
            if handler_config:
                kwargs = {param: handler_config.get(param) for param in cls_params}
            h = getattr(mod, cls_name)(**kwargs)
            for t in h.supported_types:
                handlers[t] = h
        except ImportError:
            pass
    return handlers


def resolve(
    handlers: dict[type, Any], target_type: type, extras: dict[str, str]
) -> Any:
    """The handler for ``target_type`` or for one of its base classes.

    Raises:
        TypeError: If no handler supports the type. The message names the
            install extra from ``extras`` (keyed by top-level module) when the
            type comes from a known library.
    """
    # Spark 4 DataFrames are pyspark.sql.classic subclasses of the registered class
    for supported, handler in handlers.items():
        if issubclass(target_type, supported):
            return handler

    extra = extras.get(target_type.__module__.split(".", 1)[0])
    if extra:
        raise TypeError(
            f"No handler for {target_type.__name__}. "
            f"Install the required extra: pip install rivers[{extra}]"
        )
    raise TypeError(
        f"No type handler found for {target_type!r}. Supported types: {list(handlers)}"
    )
