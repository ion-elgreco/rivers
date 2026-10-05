"""The host side of ``rivers dev``: one embedded store served to every
process over loopback."""

import pytest
import rivers as rs
from rivers._core import DevHost
from rivers._core.storage import Storage


@rs.Asset(name="served")
def served() -> int:
    return 1


def test_two_clients_share_the_served_store(tmp_path):
    host = DevHost(str(tmp_path / "storage"))
    try:
        assert host.endpoint.startswith("ws://127.0.0.1:")

        writer = Storage.connect(host.endpoint)
        repo = rs.CodeRepository(assets=[served])
        repo.resolve(storage=writer)

        reader = Storage.connect(host.endpoint)
        assert [r.asset_key for r in reader.get_asset_records()] == ["served"]

        repo._release_storage()
        del writer, reader
    finally:
        host.stop()


def test_stop_releases_the_store_for_the_next_opener(tmp_path):
    path = str(tmp_path / "storage")
    host = DevHost(path)
    host.stop()
    host.stop()

    # RocksDB takes one lock per process; a stopped host has let go of it.
    again = DevHost(path)
    try:
        reader = Storage.connect(again.endpoint)
        assert reader.get_asset_records() == []
        del reader
    finally:
        again.stop()


def test_an_endpoint_wins_over_a_storage_path(tmp_path):
    host = DevHost(str(tmp_path / "storage"), "ws://127.0.0.1:1")
    assert host.endpoint == "ws://127.0.0.1:1"
    assert not (tmp_path / "storage").exists(), "nothing is served"
    host.stop()


def test_a_host_needs_a_store_or_an_endpoint():
    with pytest.raises(ValueError, match="storage path"):
        DevHost()
