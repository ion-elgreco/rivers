"""Registration records the version that the code declares for each asset.
``resolve()`` writes the version records before it replaces the stored
topology."""

import json
from datetime import datetime

import pytest

import rivers as rs

FORMS = pytest.mark.parametrize("form", ["decorator", "class", "multi", "graph"])


def _orders(form, **declared) -> dict:
    """The ``CodeRepository`` arguments for an asset named ``orders`` (for
    ``multi``, the one output of ``ingest``) that declares ``declared``."""
    if form == "decorator":

        @rs.Asset(**declared)
        def orders() -> int:
            return 1

        return {"assets": [orders]}

    if form == "class":
        body = {"materialize": classmethod(lambda cls: 1), **declared}
        return {"assets": [type("Orders", (rs.Asset,), body)]}

    if form == "multi":

        @rs.Asset.from_multi(output_defs=[rs.AssetDef("orders")], **declared)
        def ingest():
            return {"orders": 1}

        return {"assets": [ingest]}

    @rs.Task
    def load() -> int:
        return 1

    @rs.Asset.from_graph(**declared)
    def orders():
        return load()

    return {"assets": [orders], "tasks": [load]}


def _register(storage, assets, tasks=()) -> rs.CodeRepository:
    repo = rs.CodeRepository(
        assets=list(assets),
        tasks=list(tasks),
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    return repo


def _version(storage, asset_key="orders"):
    """The applied version, the state and the target version of the asset's
    record."""
    record = storage.get_asset_version(asset_key)
    if record is None:
        return None
    return record.applied_version, record.state, record.target_version


@FORMS
def test_an_asset_without_stored_data_starts_at_its_declared_version(form):
    storage = rs.Storage.memory()

    _register(storage, **_orders(form, schema_version=3))

    record = storage.get_asset_version("orders")
    assert record.asset_key == "orders"
    assert _version(storage) == (3, "ready", None)
    assert record.partitions is None
    assert record.revision == 1


@FORMS
def test_version_1_writes_no_record(form):
    storage = rs.Storage.memory()

    _register(storage, **_orders(form))

    assert storage.get_asset_version("orders") is None


@FORMS
def test_stored_data_waits_for_a_migration_from_version_1(form):
    """Data written before the asset declared a version is at version 1."""
    storage = rs.Storage.memory()
    assert _register(storage, **_orders(form)).materialize().success

    _register(storage, **_orders(form, schema_version=2))

    assert _version(storage) == (1, "pending", 2)


def test_the_record_keeps_the_partitions_definition_of_the_stored_data():
    """The stored topology still describes the stored data when registration
    runs. An asset without stored data takes the code's definition."""
    storage = rs.Storage.memory()

    def daily_from(year, **declared):
        days = rs.PartitionsDefinition.daily(start=datetime(year, 1, 1))

        @rs.Asset(partitions_def=days, **declared)
        def orders() -> int:
            return 1

        @rs.Asset(partitions_def=days, **declared)
        def customers() -> int:
            return 1

        return [orders, customers]

    repo = _register(storage, daily_from(2024))
    day = rs.PartitionKey.single("2024-01-02")
    assert repo.materialize(["orders"], partition_key=day).success

    _register(storage, daily_from(2023, schema_version=2))

    def start(asset_key):
        partitions = storage.get_asset_version(asset_key).partitions
        return json.loads(partitions)["start"][:10]

    assert _version(storage, "orders") == (1, "pending", 2)
    assert start("orders") == "2024-01-01"
    assert _version(storage, "customers") == (2, "ready", None)
    assert start("customers") == "2023-01-01"


def test_a_higher_version_makes_the_record_pending_and_old_code_changes_nothing():
    storage = rs.Storage.memory()
    _register(storage, **_orders("decorator", schema_version=2))

    _register(storage, **_orders("decorator", schema_version=4))
    pending = storage.get_asset_version("orders")
    assert _version(storage) == (2, "pending", 4)

    # Old code during a rollout.
    _register(storage, **_orders("decorator", schema_version=3))
    assert storage.get_asset_version("orders").revision == pending.revision


def test_a_reload_of_the_same_code_writes_nothing():
    """``rivers dev`` resolves the code again in a new process at each
    reload."""
    storage = rs.Storage.memory()

    for _ in range(2):
        _register(storage, **_orders("decorator", schema_version=2))

    assert storage.get_asset_version("orders").revision == 1


def test_each_multi_asset_output_has_its_own_record():
    @rs.Asset.from_multi(
        schema_version=2,
        output_defs=[
            rs.AssetDef("customers"),
            rs.AssetDef("orders", schema_version=3),
            rs.AssetDef("payments", schema_version=1),
        ],
    )
    def ingest():
        return {"customers": 1, "orders": 2, "payments": 3}

    storage = rs.Storage.memory()

    _register(storage, [ingest])

    assert {
        key: _version(storage, key) for key in ["customers", "orders", "payments"]
    } == {
        "customers": (2, "ready", None),
        "orders": (3, "ready", None),
        "payments": None,
    }


def test_a_run_process_does_not_register(monkeypatch):
    """The process of a run resolves the code too. Only the code location
    registers."""
    monkeypatch.setenv("RIVERS_RUN_ID", "run-1")
    storage = rs.Storage.memory()

    _register(storage, **_orders("decorator", schema_version=2))

    assert storage.get_asset_version("orders") is None
