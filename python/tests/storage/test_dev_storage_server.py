"""The host side of ``rivers dev``: one embedded store served to every
process over loopback."""

import pytest
import rivers as rs
from rivers._core import DevHost
from rivers._core.storage import Storage

# A new store on disk syncs each commit of its migrations: close to a minute
# on a Windows runner.
pytestmark = pytest.mark.timeout(180)


@rs.Asset(name="served")
def served() -> int:
    return 1


def test_two_clients_share_the_served_store(tmp_path):
    host = DevHost(str(tmp_path / "storage"))
    try:
        assert host.url.startswith("ws://127.0.0.1:")

        writer = Storage.open(host.url)
        repo = rs.CodeRepository(assets=[served])
        repo.resolve(storage=writer)

        reader = Storage.open(host.url)
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

    # SurrealKV takes one lock per process; a stopped host has let go of it.
    again = DevHost(path)
    try:
        reader = Storage.open(again.url)
        assert reader.get_asset_records() == []
        del reader
    finally:
        again.stop()


@pytest.mark.parametrize(
    "url", ["ws://127.0.0.1:1", "postgres://rivers:secret@127.0.0.1:1/rivers"]
)
def test_a_server_url_wins_over_a_storage_path(tmp_path, url):
    host = DevHost(str(tmp_path / "storage"), url)
    assert host.url == url
    assert not (tmp_path / "storage").exists(), "nothing is served"
    host.stop()


def test_a_surrealkv_url_is_served_like_a_path(tmp_path):
    host = DevHost(None, f"surrealkv://{tmp_path / 'storage'}")
    try:
        assert host.url.startswith("ws://127.0.0.1:")
        assert (tmp_path / "storage").is_dir()
    finally:
        host.stop()


def test_a_host_needs_a_store_or_a_url():
    with pytest.raises(ValueError, match="storage path"):
        DevHost()


def test_a_host_refuses_an_in_memory_store():
    """The code location runs in another process, which cannot reach it."""
    with pytest.raises(ValueError, match="in-memory"):
        DevHost(None, "mem://")
