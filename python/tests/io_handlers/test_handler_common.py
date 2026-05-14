"""Round-trip behaviors shared by every IOHandler implementation.

Handler-specific behaviors (paths, prefixes, metadata fields) live in the
per-handler test files; only semantics that should hold for *any* handler
belong here.
"""

import obstore.store
import pytest

import rivers as rs

from .helpers import make_multi_partition, make_partition


@pytest.fixture(params=["memory", "pickle"])
def handler(request):
    if request.param == "memory":
        return rs.InMemoryIOHandler()
    return rs.PickleIOHandler(store=obstore.store.MemoryStore())


def test_round_trip(handler):
    ctx_out = rs.OutputContext(asset_name="my_asset", asset_metadata=None)
    handler.handle_output(ctx_out, {"key": "value"})

    ctx_in = rs.InputContext(
        asset_name="my_asset", downstream_asset="downstream", asset_metadata=None
    )
    assert handler.load_input(ctx_in) == {"key": "value"}


def test_partition_isolation(handler):
    p_a = make_partition("a")
    p_b = make_partition("b")

    handler.handle_output(rs.OutputContext(asset_name="data", partition=p_a), "value_a")
    handler.handle_output(rs.OutputContext(asset_name="data", partition=p_b), "value_b")

    ctx_in_a = rs.InputContext(asset_name="data", downstream_asset="x", partition=p_a)
    ctx_in_b = rs.InputContext(asset_name="data", downstream_asset="x", partition=p_b)
    assert handler.load_input(ctx_in_a) == "value_a"
    assert handler.load_input(ctx_in_b) == "value_b"


def test_multi_partition(handler):
    p1 = make_multi_partition({"region": "us", "env": "prod"})
    p2 = make_multi_partition({"region": "eu", "env": "prod"})

    handler.handle_output(rs.OutputContext(asset_name="data", partition=p1), 100)
    handler.handle_output(rs.OutputContext(asset_name="data", partition=p2), 200)

    ctx_in_1 = rs.InputContext(asset_name="data", downstream_asset="x", partition=p1)
    ctx_in_2 = rs.InputContext(asset_name="data", downstream_asset="x", partition=p2)
    assert handler.load_input(ctx_in_1) == 100
    assert handler.load_input(ctx_in_2) == 200
