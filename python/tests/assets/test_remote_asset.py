"""Asset.remote: a node another code location materializes."""

import json
import re
from datetime import datetime
from pathlib import Path

import obstore
import pytest

import rivers as rs
from rivers.exceptions import (
    AssetDefinitionError,
    ExecutionError,
    GraphValidationError,
    SensorDefinitionError,
)

from _helpers import EXECUTORS, DictIOHandler, make_repo, materialize_beside_sibling

pytestmark = pytest.mark.usefixtures("clean_code_location_env")


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


def _key_row(storage: rs.Storage, digest: str) -> list[str] | None:
    """The static keys stored under ``digest``."""
    raw = storage.kv_get(f"partition_keys:default:{digest}")
    return None if raw is None else json.loads(raw)


def test_topology_stores_each_definition_and_each_key_list_once(storage):
    regions = rs.PartitionsDefinition.static_(["us", "eu"])
    days = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))
    orders = rs.Asset.remote(
        "orders", code_location="sales", io_handler=DictIOHandler(), partitions_def=days
    )

    @rs.Asset(partitions_def=regions)
    def stores() -> int:
        return 1

    @rs.Asset(partitions_def=rs.PartitionsDefinition.static_(["eu", "us"]))
    def staff() -> int:
        return 1

    @rs.Asset(partitions_def=days)
    def daily_orders(orders: list) -> int:
        return len(orders)

    @rs.Asset(
        partitions_def=rs.PartitionsDefinition.multi({"region": regions, "date": days})
    )
    def store_days() -> int:
        return 1

    @rs.Asset
    def summary() -> int:
        return 1

    make_repo([orders, stores, staff, daily_orders, store_days, summary], storage)
    nodes = {
        name: node.get("partitions") for name, node in _topology_nodes(storage).items()
    }

    static = nodes["stores"]
    daily = nodes["orders"]
    assert static["kind"] == "static"
    assert static["count"] == 2
    assert re.fullmatch(r"[0-9a-f]{32}", static["digest"])
    assert "keys" not in static
    assert nodes["staff"] == static
    assert nodes["daily_orders"] == daily
    assert daily["kind"] == "time_window"
    assert daily["cron_schedule"] == "0 0 * * *"
    assert daily["fmt"] == "%Y-%m-%d"
    assert daily["start"].startswith("2024-01-01T00:00:00")
    assert nodes["store_days"] == {
        "kind": "multi",
        "dimensions": [["date", daily], ["region", static]],
    }
    assert nodes["summary"] is None
    assert _key_row(storage, static["digest"]) == ["eu", "us"]


def test_static_keys_are_stored_once_under_their_digest(embedded_storage):
    keys = [f"customer-{i:06}" for i in range(100_000)]

    def resolve(keys: list[str]) -> dict:
        @rs.Asset(partitions_def=rs.PartitionsDefinition.static_(keys))
        def customers() -> int:
            return 1

        @rs.Asset(
            partitions_def=rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))
        )
        def daily_orders() -> int:
            return 1

        @rs.Asset
        def summary() -> int:
            return 1

        make_repo([customers, daily_orders, summary], embedded_storage)
        return _topology_nodes(embedded_storage)["customers"]["partitions"]

    first = resolve(list(reversed(keys)))
    topology = embedded_storage.kv_get("graph_topology:default")
    assert topology is not None
    assert len(topology) < 1_000, len(topology)
    assert b"customer-" not in topology
    assert first["kind"] == "static"
    assert first["count"] == 100_000
    assert _key_row(embedded_storage, first["digest"]) == keys

    marker = json.dumps(["not written again"]).encode()
    embedded_storage.kv_set(f"partition_keys:default:{first['digest']}", marker)
    assert resolve(keys) == first
    assert (
        embedded_storage.kv_get(f"partition_keys:default:{first['digest']}") == marker
    )

    changed = resolve([*keys, "customer-new"])
    assert changed["count"] == 100_001
    assert changed["digest"] != first["digest"]
    assert _key_row(embedded_storage, first["digest"]) is None
    assert _key_row(embedded_storage, changed["digest"]) == [*keys, "customer-new"]


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


_NOT_MATERIALIZABLE = (
    "Cannot materialize remote asset 'orders': code location 'dev/sales' owns it"
)


def _remote_and_consumer():
    handler = DictIOHandler(store={"orders": [1, 2]})
    orders = rs.Asset.remote("orders", code_location="sales", io_handler=handler)

    @rs.Asset(io_handler=handler)
    def order_count(orders: list[int]) -> int:
        return len(orders)

    return orders, order_count


@pytest.mark.parametrize("check", ["validate", "resolve"])
def test_loading_a_lineage_only_remote_is_rejected(check):
    orders = rs.Asset.remote("orders", code_location="sales")

    @rs.Asset
    def order_count(orders: list[int]) -> int:
        return len(orders)

    repo = rs.CodeRepository(assets=[orders, order_count])
    with pytest.raises(
        GraphValidationError, match="'order_count' loads remote asset 'orders'"
    ):
        getattr(repo, check)()


def test_input_handler_loads_a_lineage_only_remote():
    handler = DictIOHandler(store={"orders": [1, 2, 3]})
    orders = rs.Asset.remote("orders", code_location="sales")

    @rs.Asset(
        io_handler=handler, deps=[rs.AssetDef.input("orders", io_handler=handler)]
    )
    def order_count(orders: list[int]) -> int:
        return len(orders)

    repo = rs.CodeRepository(assets=[orders, order_count])
    assert repo.materialize().success
    assert repo.load_node("order_count") == 3


def test_materializing_a_remote_is_rejected():
    repo = rs.CodeRepository(assets=list(_remote_and_consumer()))

    with pytest.raises(ExecutionError, match=_NOT_MATERIALIZABLE):
        repo.materialize(selection=["orders"])
    with pytest.raises(ExecutionError, match=_NOT_MATERIALIZABLE):
        repo.materialize(selection=["order_count", "orders"])


def test_backfilling_a_remote_is_rejected():
    pd = rs.PartitionsDefinition.static_(["a", "b"])
    orders = rs.Asset.remote("orders", code_location="sales", partitions_def=pd)

    @rs.Asset(partitions_def=pd, deps=[rs.AssetDef.dep("orders")])
    def report() -> int:
        return 1

    repo = rs.CodeRepository(assets=[orders, report])
    with pytest.raises(ExecutionError, match=_NOT_MATERIALIZABLE):
        repo.backfill(
            selection=["orders"], partition_keys=[rs.PartitionKey.single("a")]
        )


def test_job_with_a_remote_is_rejected():
    orders, order_count = _remote_and_consumer()
    job = rs.Job(name="counts", assets=[orders, order_count])

    repo = rs.CodeRepository(assets=[orders, order_count], jobs=[job])
    with pytest.raises(
        GraphValidationError, match=f"Job 'counts': {_NOT_MATERIALIZABLE}"
    ):
        repo.resolve()


def test_sensor_targeting_a_remote_is_rejected():
    orders, order_count = _remote_and_consumer()

    @rs.Sensor(asset_selection=["orders"], name="watch")
    def watch(context: rs.SensorEvaluationContext):
        return rs.RunRequest()

    repo = rs.CodeRepository(assets=[orders, order_count], sensors=[watch])
    with pytest.raises(
        SensorDefinitionError, match=f"Sensor 'watch': {_NOT_MATERIALIZABLE}"
    ):
        repo.resolve()


@pytest.mark.parametrize(
    "pd",
    [
        pytest.param(rs.PartitionsDefinition.dynamic("regions"), id="dynamic"),
        pytest.param(
            rs.PartitionsDefinition.multi(
                {
                    "date": rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1)),
                    "region": rs.PartitionsDefinition.dynamic("regions"),
                }
            ),
            id="multi-with-dynamic",
        ),
    ],
)
def test_dynamic_mirror_is_rejected(pd):
    with pytest.raises(
        AssetDefinitionError, match="dynamic partitions definition cannot be mirrored"
    ):
        rs.Asset.remote("orders", code_location="sales", partitions_def=pd)

    orders = rs.Asset.remote("orders", code_location="sales", partitions_def="mirror")
    repo = rs.CodeRepository(assets=[orders], partition_defs={"mirror": pd})
    with pytest.raises(
        AssetDefinitionError, match="dynamic partitions definition cannot be mirrored"
    ):
        repo.resolve()
