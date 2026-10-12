"""Return type checks in a module with ``from __future__ import annotations``.

The return annotation is a string here, and rivers evaluates it in the
asset's module. For a parallel step the orchestrator evaluates it: a function
sent to a worker by value carries only the globals its code reads.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import rivers as rs

if TYPE_CHECKING:
    from decimal import Decimal


def run(storage, executor_env, *assets) -> rs.CodeRepository:
    """Materialize ``assets`` next to a sibling step: the parallel executor
    runs a lone step in-process."""
    executor, io_factory = executor_env

    @rs.Asset(io_handler=io_factory())
    def sibling() -> int:
        return 1

    repo = rs.CodeRepository(assets=[*assets, sibling], default_executor=executor)
    repo.resolve(storage=storage)
    repo.materialize(raise_on_error=False)
    return repo


def step_error(storage, asset: str) -> str:
    [failure] = [
        e for e in storage.get_events_for_asset(asset) if e.event_type == "StepFailure"
    ]
    return dict(failure.metadata)["error"]


def test_string_return_annotation_is_evaluated(storage, executor_env):
    _, io_factory = executor_env

    @rs.Asset(io_handler=io_factory())
    def good() -> list[int]:
        return [1, 2]

    @rs.Asset(io_handler=io_factory())
    def bad() -> list[int]:
        return "oops"  # type: ignore

    repo = run(storage, executor_env, good, bad)

    assert repo.load_node("good") == [1, 2]
    assert step_error(storage, "bad") == (
        "AssetOutputValidationError: Asset 'bad' returned value of type 'str' "
        "but expected 'list[int]'"
    )


def test_return_annotation_that_does_not_evaluate_fails_the_step(storage, executor_env):
    _, io_factory = executor_env

    @rs.Asset(io_handler=io_factory())
    def price() -> Decimal:
        return 1  # type: ignore

    run(storage, executor_env, price)

    assert step_error(storage, "price") == (
        "AssetOutputValidationError: Cannot evaluate the return annotation "
        "'Decimal': NameError: name 'Decimal' is not defined"
    )
