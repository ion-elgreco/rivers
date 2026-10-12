"""dataframely hints in a module with ``from __future__ import annotations``.

The return annotation is a string here. rivers evaluates it, so the frame is
validated on both executors.
"""

from __future__ import annotations

import pytest

dy = pytest.importorskip("dataframely")
pl = pytest.importorskip("polars")

import rivers as rs  # noqa: E402


class Orders(dy.Schema):
    id = dy.Int64(min=0)


def test_string_hint_validates_the_frame(env):
    bad = pl.DataFrame({"id": [-1]})

    @rs.Asset(io_handler=env.handler)
    def orders() -> dy.DataFrame[Orders]:
        return pl.DataFrame({"id": [1, 2]})

    @rs.Asset(io_handler=env.handler)
    def bad_orders() -> dy.DataFrame[Orders]:
        return bad

    _, result = env.materialize(orders, bad_orders)

    assert not result.success
    assert env.stored_json("orders", "dataframely/schema")["name"] == "Orders"
    with pytest.raises(dy.exc.ValidationError) as report:
        Orders.validate(bad)
    assert dict(result.failed_assets)["bad_orders"] == (
        "AssetOutputValidationError: Asset 'bad_orders' returned a value that does "
        f"not match '{dy.DataFrame[Orders]}': {report.value}"
    )
