"""The consumer's condition daemon follows a remote asset's owner.

Two repositories share one store under the identities ``warehouse`` (the
owner of ``raw_orders``) and ``analytics`` (the consumer, whose daemon runs).
"""

import threading
import time
from datetime import datetime
from pathlib import Path

import obstore
import pytest

import rivers as rs
from rivers._core import AutomationDaemon

from _polling import wait_for

_ENV = (
    "RIVERS_CODE_LOCATION_ID",
    "RIVERS_CODE_LOCATION_NAME",
    "RIVERS_CODE_LOCATION_NAMESPACE",
    "RIVERS_NAMESPACE",
    "RIVERS_DEPLOYMENT",
    "RIVERS_RUN_ID",
)

DAILY = rs.PartitionsDefinition.daily(
    start=datetime(2024, 1, 1), end=datetime(2024, 1, 5)
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in _ENV:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def handler(tmp_path: Path) -> rs.PickleIOHandler:
    return rs.PickleIOHandler(
        store=obstore.store.LocalStore(str(tmp_path / "io"), mkdir=True)
    )


def _resolve_as(assets, storage, monkeypatch, identity: str) -> rs.CodeRepository:
    repo = rs.CodeRepository(assets=assets, default_executor=rs.Executor.in_process())
    monkeypatch.setenv("RIVERS_CODE_LOCATION_ID", identity)
    repo.resolve(storage=storage)
    return repo


def _consumer_runs(storage) -> list:
    return storage._scoped("analytics").get_runs()


def _start(consumer, storage) -> AutomationDaemon:
    daemon = AutomationDaemon(
        repo=consumer, storage=storage, condition_eval_interval="1s"
    )
    daemon.start()
    return daemon


def _summary_assets(handler, code_location="warehouse"):
    orders = rs.Asset.remote(
        "orders",
        code_location=code_location,
        asset_key="raw_orders",
        io_handler=handler,
    )

    @rs.Asset(io_handler=handler, automation_condition=rs.AutomationCondition.eager())
    def order_summary(orders: list[int]) -> int:
        return sum(orders)

    return [orders, order_summary]


def test_owner_materializing_launches_the_consumer_once(storage, handler, monkeypatch):
    @rs.Asset(io_handler=handler)
    def raw_orders() -> list[int]:
        return [1, 2, 3]

    owner = _resolve_as([raw_orders], storage, monkeypatch, "warehouse")
    consumer = _resolve_as(_summary_assets(handler), storage, monkeypatch, "analytics")

    daemon = _start(consumer, storage)
    try:
        time.sleep(2.5)
        assert _consumer_runs(storage) == [], "raw_orders is missing in warehouse"

        assert owner.materialize().success
        wait_for(lambda: len(_consumer_runs(storage)) == 1, 15, "the consumer run")
        time.sleep(3.5)
    finally:
        daemon.stop()

    runs = _consumer_runs(storage)
    assert [r.node_names for r in runs] == [["order_summary"]]
    wait_for(lambda: consumer.load_node("order_summary") == 6, 10, "order_summary")


def test_consumer_waits_for_the_owners_run_to_end(storage, handler, monkeypatch):
    release = threading.Event()
    release.set()

    @rs.Asset(io_handler=handler)
    def raw_orders() -> list[int]:
        return [1, 2, 3]

    @rs.Asset(io_handler=handler)
    def slow(raw_orders: list[int]) -> int:
        release.wait(timeout=30)
        return len(raw_orders)

    owner = _resolve_as([raw_orders, slow], storage, monkeypatch, "warehouse")
    assert owner.materialize().success
    consumer = _resolve_as(_summary_assets(handler), storage, monkeypatch, "analytics")
    assert consumer.materialize().success

    daemon = _start(consumer, storage)
    owner_run = threading.Thread(target=owner.materialize)
    release.clear()
    try:
        time.sleep(1.5)
        owner_run.start()
        time.sleep(3.5)
        assert len(_consumer_runs(storage)) == 1, "quiet while warehouse's run is live"

        release.set()
        owner_run.join(timeout=30)
        wait_for(lambda: len(_consumer_runs(storage)) == 2, 15, "the consumer run")
        time.sleep(3.5)
        assert len(_consumer_runs(storage)) == 2
    finally:
        release.set()
        daemon.stop()


def test_previous_day_mapping_waits_for_the_owners_partition(
    storage, handler, monkeypatch
):
    @rs.Asset(io_handler=handler, partitions_def=DAILY)
    def events(context: rs.AssetExecutionContext) -> str:
        return f"events of {context.partition_key}"

    owner = _resolve_as([events], storage, monkeypatch, "warehouse")
    assert owner.materialize(partition_key=rs.PartitionKey.single("2024-01-02")).success

    remote_events = rs.Asset.remote(
        "events", code_location="warehouse", io_handler=handler, partitions_def=DAILY
    )

    @rs.Asset(
        io_handler=handler,
        partitions_def=DAILY,
        automation_condition=rs.AutomationCondition.eager(),
        deps=[
            rs.AssetDef.input(
                "events", partition_mapping=rs.PartitionMapping.time_window(offset=-1)
            )
        ],
    )
    def summary(events: str) -> str:
        return events

    consumer = _resolve_as([remote_events, summary], storage, monkeypatch, "analytics")
    scoped = storage._scoped("analytics")
    built = lambda: set(scoped.get_materialized_partitions("summary"))  # noqa: E731

    daemon = _start(consumer, storage)
    try:
        wait_for(
            lambda: rs.PartitionKey.single("2024-01-03") in built(),
            20,
            "summary for the day after warehouse's partition",
        )
        time.sleep(3.5)
    finally:
        daemon.stop()

    assert rs.PartitionKey.single("2024-01-02") not in built()
    assert rs.PartitionKey.single("2024-01-04") not in built()
    key = rs.PartitionKey.single("2024-01-03")
    assert consumer.load_node("summary", partition_key=key) == "events of 2024-01-02"


def test_unregistered_owner_launches_nothing(storage, handler, monkeypatch):
    consumer = _resolve_as(
        _summary_assets(handler, code_location="nowhere"),
        storage,
        monkeypatch,
        "analytics",
    )

    daemon = _start(consumer, storage)
    try:
        time.sleep(3.5)
    finally:
        daemon.stop()

    assert _consumer_runs(storage) == []


@pytest.mark.parametrize(
    ("mirror", "launches"),
    [
        pytest.param(["a", "b"], True, id="same"),
        pytest.param(["a"], True, id="owner-added-keys"),
        pytest.param(["a", "b", "c"], False, id="owner-removed-a-key"),
    ],
)
def test_incompatible_mirror_pauses_the_consumer(
    storage, handler, monkeypatch, mirror, launches
):
    @rs.Asset(
        io_handler=handler, partitions_def=rs.PartitionsDefinition.static_(["a", "b"])
    )
    def raw_orders(context: rs.AssetExecutionContext) -> str:
        return f"orders of {context.partition_key}"

    owner = _resolve_as([raw_orders], storage, monkeypatch, "warehouse")
    assert owner.materialize(partition_key=rs.PartitionKey.single("a")).success

    keys = rs.PartitionsDefinition.static_(mirror)
    orders = rs.Asset.remote(
        "orders",
        code_location="warehouse",
        asset_key="raw_orders",
        io_handler=handler,
        partitions_def=keys,
    )

    @rs.Asset(
        io_handler=handler,
        partitions_def=keys,
        automation_condition=rs.AutomationCondition.eager(),
    )
    def summary(orders: str) -> str:
        return orders

    consumer = _resolve_as([orders, summary], storage, monkeypatch, "analytics")
    scoped = storage._scoped("analytics")
    built = lambda: set(scoped.get_materialized_partitions("summary"))  # noqa: E731

    daemon = _start(consumer, storage)
    try:
        if launches:
            wait_for(
                lambda: rs.PartitionKey.single("a") in built(), 20, "summary for 'a'"
            )
        else:
            time.sleep(4)
    finally:
        daemon.stop()

    assert (rs.PartitionKey.single("a") in built()) == launches
