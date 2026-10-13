"""Integrations with external tools. Each module needs its extra, e.g. ``pip install rivers[slack]``.

An integration can check asset values typed with its library's types. rivers
calls its ``validate_output(value, hint)`` when a type hint, or an item of an
``Annotated`` hint, has a type from that library. The type hint is an asset's
return hint or a multi-asset output's ``AssetDef(type_hint=...)``. The
function returns the value to store, an ``Output`` that also carries
materialization metadata, or ``NotImplemented`` to leave the hint to the
``isinstance`` check. An exception fails the step with
``AssetOutputValidationError``.
"""

import functools
import importlib
import typing
from collections.abc import Callable
from typing import Any

# Integrations that define ``validate_output``, keyed by the top-level module
# of the library whose types they check.
_OUTPUT_VALIDATORS = {
    "dataframely": "rivers.integrations.dataframely",
    "pandera": "rivers.integrations.pandera",
}


def output_validator(hint: Any) -> Callable[[Any, Any], Any] | None:
    """Return the ``validate_output`` of the integration that checks ``hint``.

    Args:
        hint: A type hint, e.g. ``pandera.typing.DataFrame[Orders]``, or an
            item of an ``Annotated`` hint, e.g. a pandera ``DataFrameSchema``.

    Returns:
        The integration's ``validate_output``, or ``None`` when no
        integration checks the hint's type.
    """
    cls = typing.get_origin(hint) or hint
    return _validator_for(cls if isinstance(cls, type) else type(cls))


@functools.cache
def _validator_for(cls: type) -> Callable[[Any, Any], Any] | None:
    for base in cls.__mro__:
        module = _OUTPUT_VALIDATORS.get(base.__module__.partition(".")[0])
        if module is not None:
            return importlib.import_module(module).validate_output
    return None
