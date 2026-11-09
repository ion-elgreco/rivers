"""Asset.remote: a node another code location materializes."""

import json
from pathlib import Path

import obstore
import pytest

import rivers as rs
from rivers.exceptions import AssetDefinitionError

from _helpers import EXECUTORS, DictIOHandler, make_repo, materialize_beside_sibling

_ENV = (
    "RIVERS_CODE_LOCATION_ID",
    "RIVERS_CODE_LOCATION_NAME",
    "RIVERS_CODE_LOCATION_NAMESPACE",
    "RIVERS_NAMESPACE",
    "RIVERS_DEPLOYMENT",
    "RIVERS_RUN_ID",
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in _ENV:
        monkeypatch.delenv(name, raising=False)


def _pickle_handler(root: Path) -> rs.PickleIOHandler:
    return rs.PickleIOHandler(store=obstore.store.LocalStore(str(root), mkdir=True))


def _topology_nodes(storage: rs.Storage) -> dict[str, dict]:
    raw = storage.kv_get("graph_topology:default")
    assert raw is not None
    return {n["name"]: n for n in json.loads(raw)["nodes"]}


def test_remote_asset_properties():
    orders = rs.Asset.remote(
        "orders",
        code_location="sales",
        io_handler=DictIOHandler(),
        metadata={"owner": "sales"},
        group="upstream",
    )

    assert isinstance(orders, rs.RemoteAsset)
    assert orders.name == "orders"
    assert orders.code_location == "sales"
    assert orders.asset_key == "orders"
    assert orders.metadata == {"owner": "sales"}
    assert orders.group == "upstream"
    assert orders.is_remote is True
    assert orders.is_external is False
    assert orders.is_single is False
    assert orders.is_async is False
    assert orders.observe_fn is None


def test_remote_asset_key_alias():
    orders = rs.Asset.remote(
        "sales_orders", code_location="team-sales/warehouse", asset_key="orders"
    )

    assert orders.name == "sales_orders"
    assert orders.code_location == "team-sales/warehouse"
    assert orders.asset_key == "orders"


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"name": "", "code_location": "sales"}, "must be non-empty"),
        ({"name": "a/b", "code_location": "sales"}, "contain no '/'"),
        ({"name": "orders", "code_location": "sales", "asset_key": ""}, "asset_key"),
        ({"name": "orders", "code_location": ""}, "invalid code location"),
        ({"name": "orders", "code_location": "a/b/c"}, "invalid code location"),
        ({"name": "orders", "code_location": "my sales"}, "invalid code location"),
    ],
)
def test_remote_asset_rejects_bad_declarations(kwargs, message):
    name = kwargs.pop("name")
    with pytest.raises(AssetDefinitionError, match=message):
        rs.Asset.remote(name, **kwargs)


def test_remote_asset_takes_no_function():
    orders = rs.Asset.remote("orders", code_location="sales")

    with pytest.raises(AssetDefinitionError, match="takes no function"):
        orders(lambda: 1)


def test_topology_records_the_remote_ref(storage):
    orders = rs.Asset.remote(
        "orders", code_location="sales", io_handler=DictIOHandler()
    )
    refunds = rs.Asset.remote(
        "refunds", code_location="finance/ledger", asset_key="refund_events"
    )

    @rs.Asset(deps=[rs.AssetDef.dep("refunds")])
    def report(orders: list) -> int:
        return len(orders)

    make_repo([orders, refunds, report], storage)
    nodes = _topology_nodes(storage)

    assert nodes["orders"]["kind"] == "asset"
    assert nodes["orders"]["remote"] == {
        "code_location": "dev/sales",
        "asset_key": "orders",
    }
    assert nodes["refunds"]["remote"] == {
        "code_location": "finance/ledger",
        "asset_key": "refund_events",
    }
    assert "remote" not in nodes["report"]


def test_remote_bare_name_resolves_in_the_own_namespace(storage, monkeypatch):
    monkeypatch.setenv("RIVERS_CODE_LOCATION_NAMESPACE", "team-data")
    orders = rs.Asset.remote(
        "orders", code_location="sales", io_handler=DictIOHandler()
    )

    @rs.Asset
    def report(orders: list) -> int:
        return len(orders)

    make_repo([orders, report], storage)

    remote = _topology_nodes(storage)["orders"]["remote"]
    assert remote["code_location"] == "team-data/sales"


def test_remote_name_collides_with_local_asset():
    orders = rs.Asset.remote("orders", code_location="sales")

    @rs.Asset(name="orders")
    def local_orders() -> int:
        return 1

    with pytest.raises(AssetDefinitionError, match="duplicate asset name 'orders'"):
        rs.CodeRepository(assets=[orders, local_orders]).resolve()


def test_two_remotes_need_distinct_local_names():
    foo = rs.Asset.remote("raw_orders", code_location="foo")
    bar = rs.Asset.remote("raw_orders", code_location="bar")

    with pytest.raises(AssetDefinitionError, match="duplicate asset name 'raw_orders'"):
        rs.CodeRepository(assets=[foo, bar]).resolve()

    foo = rs.Asset.remote("foo_orders", code_location="foo", asset_key="raw_orders")
    bar = rs.Asset.remote("bar_orders", code_location="bar", asset_key="raw_orders")
    rs.CodeRepository(assets=[foo, bar]).resolve()


@pytest.mark.parametrize("executor", EXECUTORS)
def test_downstream_loads_the_owners_output(executor, tmp_path):
    handler = _pickle_handler(tmp_path)

    @rs.Asset(io_handler=handler)
    def orders() -> list[int]:
        return [1, 2, 3]

    make_repo([orders]).materialize()

    remote_orders = rs.Asset.remote("orders", code_location="sales", io_handler=handler)

    @rs.Asset(io_handler=handler)
    def order_count(orders: list[int]) -> int:
        return len(orders)

    repo, result = materialize_beside_sibling(
        None, executor, handler, remote_orders, order_count
    )

    assert result.success
    assert repo.load_node("order_count") == 3
    assert repo.storage.get_asset_record("orders") is None
    assert repo.storage.get_asset_record("order_count") is not None


def test_job_pulls_in_the_remote_for_loading():
    handler = DictIOHandler(store={"orders": [4, 5]})
    orders = rs.Asset.remote("orders", code_location="sales", io_handler=handler)

    @rs.Asset(io_handler=handler)
    def order_count(orders: list[int]) -> int:
        return len(orders)

    job = rs.Job(name="counts", assets=[order_count])
    repo = rs.CodeRepository(assets=[orders, order_count], jobs=[job])

    repo.get_job("counts").execute()
    assert repo.load_node("order_count") == 2


def test_lineage_only_remote_needs_no_io_handler(storage):
    orders = rs.Asset.remote("orders", code_location="sales")

    @rs.Asset(deps=[rs.AssetDef.dep("orders")])
    def report() -> int:
        return 1

    repo = make_repo([orders, report], storage)
    result = repo.materialize()

    assert result.success
    assert storage.get_asset_record("orders") is None
    assert [r.asset_key for r in storage.get_asset_records()] == ["report"]
