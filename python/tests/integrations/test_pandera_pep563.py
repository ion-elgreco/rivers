"""pandera hints in a module with ``from __future__ import annotations``.

The return annotation is a string here. rivers evaluates it, so the frame is
validated on both executors.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pandera")
pd = pytest.importorskip("pandas")

import pandera.pandas as pa  # noqa: E402
from pandera.errors import SchemaErrors  # noqa: E402
from pandera.typing import DataFrame, Series  # noqa: E402

import rivers as rs  # noqa: E402


class Orders(pa.DataFrameModel):
    id: Series[int] = pa.Field(ge=0)


def test_string_hint_validates_the_frame(env):
    bad = pd.DataFrame({"id": [-1]})

    @rs.Asset(io_handler=env.handler)
    def orders() -> DataFrame[Orders]:
        return pd.DataFrame({"id": [1, 2]})

    @rs.Asset(io_handler=env.handler)
    def bad_orders() -> DataFrame[Orders]:
        return bad

    _, result = env.materialize(orders, bad_orders)

    assert not result.success
    assert env.stored_json("orders", "pandera/schema")["name"] == "Orders"
    with pytest.raises(SchemaErrors) as report:
        Orders.validate(bad, lazy=True)
    assert dict(result.failed_assets)["bad_orders"] == (
        "AssetOutputValidationError: Asset 'bad_orders' returned a value that does "
        f"not match '{DataFrame[Orders]}': {report.value}"
    )
