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
