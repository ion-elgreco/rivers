"""Remote inputs load the owner's output through the consumer's handler.

Two repositories share one store under the identities ``warehouse`` (the
owner) and ``analytics`` (the consumer).
"""

from datetime import datetime
from pathlib import Path

import obstore
import pytest

import rivers as rs

from _helpers import EXECUTORS, CapturingHandler

_ENV = (
    "RIVERS_CODE_LOCATION_ID",
    "RIVERS_CODE_LOCATION_NAME",
    "RIVERS_CODE_LOCATION_NAMESPACE",
    "RIVERS_NAMESPACE",
    "RIVERS_DEPLOYMENT",
    "RIVERS_RUN_ID",
)

DAILY = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in _ENV:
        monkeypatch.delenv(name, raising=False)


def _pickle_handler(root: Path) -> rs.PickleIOHandler:
    return rs.PickleIOHandler(store=obstore.store.LocalStore(str(root), mkdir=True))


def _resolve_as(repo, storage, monkeypatch, identity: str) -> rs.CodeRepository:
    monkeypatch.setenv("RIVERS_CODE_LOCATION_ID", identity)
    repo.resolve(storage=storage)
    return repo


def _sibling(handler) -> rs.SingleAsset:
    """A second step: the parallel executor runs a lone step in-process."""

    @rs.Asset(io_handler=handler)
    def sibling() -> int:
        return 1

    return sibling


@pytest.mark.parametrize("executor", EXECUTORS)
def test_aliased_remote_loads_the_owners_output(
    executor, embedded_storage, tmp_path, monkeypatch
):
    handler = _pickle_handler(tmp_path / "io")

    @rs.Asset(io_handler=handler)
    def raw_orders() -> list[int]:
        return [1, 2, 3]

    owner = rs.CodeRepository(assets=[raw_orders])
    assert (
        _resolve_as(owner, embedded_storage, monkeypatch, "warehouse")
        .materialize()
        .success
    )

    orders = rs.Asset.remote(
        "orders", code_location="warehouse", asset_key="raw_orders", io_handler=handler
    )

    @rs.Asset(io_handler=handler)
    def order_count(orders: list[int]) -> int:
        return len(orders)

    consumer = rs.CodeRepository(
        assets=[orders, order_count, _sibling(handler)], default_executor=executor
    )
    result = _resolve_as(
        consumer, embedded_storage, monkeypatch, "analytics"
    ).materialize()

    assert result.success
    assert consumer.load_node("order_count") == 3
    assert consumer.load_node("orders") == [1, 2, 3]


@pytest.mark.parametrize("executor", EXECUTORS)
def test_partitioned_remote_loads_through_a_time_window_mapping(
    executor, embedded_storage, tmp_path, monkeypatch
):
    handler = _pickle_handler(tmp_path / "io")

    @rs.Asset(io_handler=handler, partitions_def=DAILY)
    def events(context: rs.AssetExecutionContext) -> str:
        return f"events of {context.partition_key}"

    owner = _resolve_as(
        rs.CodeRepository(assets=[events]), embedded_storage, monkeypatch, "warehouse"
    )
    for day in ("2024-01-01", "2024-01-02"):
        assert owner.materialize(partition_key=rs.PartitionKey.single(day)).success

    remote_events = rs.Asset.remote(
        "events", code_location="warehouse", io_handler=handler, partitions_def=DAILY
    )

    @rs.Asset(
        io_handler=handler,
        partitions_def=DAILY,
        deps=[
            rs.AssetDef.input(
                "events", partition_mapping=rs.PartitionMapping.time_window(offset=-1)
            )
        ],
    )
    def previous_day(events: str) -> str:
        return events

    @rs.Asset(io_handler=handler, partitions_def=DAILY)
    def sibling() -> int:
        return 1

    consumer = rs.CodeRepository(
        assets=[remote_events, previous_day, sibling], default_executor=executor
    )
    key = rs.PartitionKey.single("2024-01-02")
    result = _resolve_as(
        consumer, embedded_storage, monkeypatch, "analytics"
    ).materialize(partition_key=key)

    assert result.success
    assert (
        consumer.load_node("previous_day", partition_key=key) == "events of 2024-01-01"
    )


def test_handler_sees_the_owners_key_and_location(monkeypatch):
    monkeypatch.setenv("RIVERS_CODE_LOCATION_NAMESPACE", "team-data")
    handler = CapturingHandler(store={"raw_orders": [1, 2]}, input_contexts=[])
    orders = rs.Asset.remote(
        "orders", code_location="warehouse", asset_key="raw_orders", io_handler=handler
    )

    @rs.Asset(io_handler=handler)
    def order_count(orders: list[int]) -> int:
        return len(orders)

    @rs.Asset(io_handler=handler)
    def doubled(order_count: int) -> int:
        return order_count * 2

    repo = rs.CodeRepository(assets=[orders, order_count, doubled])
    assert repo.materialize().success

    seen = {
        c.downstream_asset: (c.asset_name, c.code_location)
        for c in handler.input_contexts
    }
    assert seen["order_count"] == ("raw_orders", "team-data/warehouse")
    assert seen["doubled"] == ("order_count", None)
    assert repo.load_node("doubled") == 4


def test_input_context_takes_a_code_location():
    ctx = rs.InputContext("raw_orders", "order_count", code_location="dev/warehouse")

    assert ctx.code_location == "dev/warehouse"
    assert rs.InputContext("a", "b").code_location is None
