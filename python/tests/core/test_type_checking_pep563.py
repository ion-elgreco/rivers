"""Return type checks in a module with ``from __future__ import annotations``.

The return annotation is a string here, and rivers evaluates it in the
asset's module. For a parallel step the orchestrator evaluates it: a function
sent to a worker by value carries only the globals its code reads.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from _helpers import materialize_beside_sibling

import rivers as rs

if TYPE_CHECKING:
    from decimal import Decimal


def test_string_return_annotation_is_evaluated(storage, executor_env):
    executor, io_factory = executor_env

    @rs.Asset(io_handler=io_factory())
    def good() -> list[int]:
        return [1, 2]

    @rs.Asset(io_handler=io_factory())
    def bad() -> list[int]:
        return "oops"  # type: ignore

    repo, result = materialize_beside_sibling(
        storage, executor, io_factory(), good, bad
    )

    assert repo.load_node("good") == [1, 2]
    assert dict(result.failed_assets)["bad"] == (
        "AssetOutputValidationError: Asset 'bad' returned value of type 'str' "
        "but expected 'list[int]'"
    )


def test_return_annotation_that_does_not_evaluate_fails_the_step(storage, executor_env):
    executor, io_factory = executor_env

    @rs.Asset(io_handler=io_factory())
    def price() -> Decimal:
        return 1  # type: ignore

    _, result = materialize_beside_sibling(storage, executor, io_factory(), price)

    assert dict(result.failed_assets)["price"] == (
        "AssetOutputValidationError: Cannot evaluate the return annotation "
        "'Decimal': NameError: name 'Decimal' is not defined"
    )
