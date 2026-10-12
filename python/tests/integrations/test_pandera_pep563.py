"""pandera hints in a module with ``from __future__ import annotations``.

The return annotation is a string here. rivers evaluates it, so the frame is
validated on both executors.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("pandera")
pd = pytest.importorskip("pandas")

import pandera.pandas as pa  # noqa: E402
from pandera.errors import SchemaErrors  # noqa: E402
from pandera.typing import DataFrame, Series  # noqa: E402

import rivers as rs  # noqa: E402


class Orders(pa.DataFrameModel):
    id: Series[int] = pa.Field(ge=0)


def test_string_hint_validates_the_frame(storage, executor_env):
    executor, io_factory = executor_env
    bad = pd.DataFrame({"id": [-1]})

    @rs.Asset(io_handler=io_factory())
    def orders() -> DataFrame[Orders]:
        return pd.DataFrame({"id": [1, 2]})

    @rs.Asset(io_handler=io_factory())
    def bad_orders() -> DataFrame[Orders]:
        return bad

    repo = rs.CodeRepository(assets=[orders, bad_orders], default_executor=executor)
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=False)

    assert not result.success
    [materialization] = [
        e
        for e in storage.get_events_for_asset("orders")
        if e.event_type == "Materialization"
    ]
    [(_, payload)] = json.loads(
        dict(materialization.metadata)["pandera/schema"]
    ).items()
    assert json.loads(payload["value"])["name"] == "Orders"
    [failure] = [
        e
        for e in storage.get_events_for_asset("bad_orders")
        if e.event_type == "StepFailure"
    ]
    with pytest.raises(SchemaErrors) as report:
        Orders.validate(bad, lazy=True)
    assert dict(failure.metadata)["error"] == (
        "AssetOutputValidationError: Asset 'bad_orders' returned a value that does "
        f"not match '{DataFrame[Orders]}': {report.value}"
    )
