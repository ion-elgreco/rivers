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
from _helpers import materialize_beside_sibling  # noqa: E402
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

    _, result = materialize_beside_sibling(
        storage, executor, io_factory(), orders, bad_orders
    )

    assert not result.success
    materialization = storage.get_latest_materialization("orders")
    [(_, payload)] = json.loads(
        dict(materialization.metadata)["pandera/schema"]
    ).items()
    assert json.loads(payload["value"])["name"] == "Orders"
    with pytest.raises(SchemaErrors) as report:
        Orders.validate(bad, lazy=True)
    assert dict(result.failed_assets)["bad_orders"] == (
        "AssetOutputValidationError: Asset 'bad_orders' returned a value that does "
        f"not match '{DataFrame[Orders]}': {report.value}"
    )
