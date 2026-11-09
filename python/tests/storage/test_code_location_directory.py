"""resolve() lists the code location in the code location directory."""

import json

import pytest

import rivers as rs

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


def _repo() -> rs.CodeRepository:
    @rs.Asset
    def a() -> int:
        return 1

    return rs.CodeRepository(assets=[a])


def _entry(storage: rs.Storage, key: str) -> dict | None:
    raw = storage.kv_get(key)
    return None if raw is None else json.loads(raw)


def test_resolve_lists_dev_code_location(storage):
    _repo().resolve(storage=storage)

    assert _entry(storage, "code_location:dev/default") == {"identity": "default"}


def test_resolve_lists_namespace_and_name(storage, monkeypatch):
    monkeypatch.setenv("RIVERS_CODE_LOCATION_NAMESPACE", "team")
    monkeypatch.setenv("RIVERS_CODE_LOCATION_NAME", "wh")
    monkeypatch.setenv("RIVERS_CODE_LOCATION_ID", "uuid-1")

    _repo().resolve(storage=storage)

    assert _entry(storage, "code_location:team/wh") == {"identity": "uuid-1"}
    assert _entry(storage, "code_location:dev/default") is None


def test_run_pods_do_not_list_the_code_location(storage, monkeypatch):
    monkeypatch.setenv("RIVERS_RUN_ID", "run-1")

    _repo().resolve(storage=storage)

    assert _entry(storage, "code_location:dev/default") is None


def _consumer(*, with_remote: bool) -> rs.CodeRepository:
    @rs.Asset
    def local() -> int:
        return 1

    if not with_remote:
        return rs.CodeRepository(assets=[local])

    orders = rs.Asset.remote(
        "orders",
        code_location="warehouse",
        asset_key="raw_orders",
        io_handler=rs.InMemoryIOHandler(),
    )

    @rs.Asset
    def order_count(orders: list[int]) -> int:
        return len(orders)

    @rs.Asset(deps=[rs.AssetDef.dep("orders")])
    def report() -> int:
        return 1

    return rs.CodeRepository(assets=[local, orders, order_count, report])


def test_entry_lists_the_remote_assets_the_location_consumes(storage):
    _consumer(with_remote=True).resolve(storage=storage)

    assert _entry(storage, "code_location:dev/default") == {
        "identity": "default",
        "remotes": [
            {
                "local": "orders",
                "code_location": "dev/warehouse",
                "asset_key": "raw_orders",
                "downstream": ["order_count", "report"],
            }
        ],
    }


def test_resolving_without_the_remote_drops_it(storage):
    _consumer(with_remote=True).resolve(storage=storage)
    _consumer(with_remote=False).resolve(storage=storage)

    assert _entry(storage, "code_location:dev/default") == {"identity": "default"}
