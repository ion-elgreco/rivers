"""The gRPC API shows the version of each asset's stored data, and retries,
cancels or starts its migration."""

import contextlib
import datetime

import pytest

pytest.importorskip("grpc")

import grpc

import rivers as rs
from _polling import wait_for
from rivers._core import AutomationDaemon

LATER = datetime.datetime(2100, 1, 1, tzinfo=datetime.timezone.utc)


def _repo(storage, io, **declared):
    @rs.Asset(io_handler=io, **declared)
    def orders() -> int:
        return 1

    repo = rs.CodeRepository(assets=[orders], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    return repo


@contextlib.contextmanager
def _serve(repo, grpc_stubs):
    port = repo._start_grpc_server("127.0.0.1", 0)
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    try:
        grpc.channel_ready_future(channel).result(timeout=5)
        pb2, pb2_grpc = grpc_stubs
        yield pb2_grpc.CodeLocationServiceStub(channel), pb2
    finally:
        channel.close()
        repo._stop_grpc_server()


def _convert(fail=False, start_at=None):
    def add_currency(ctx: rs.MigrationContext) -> None:
        if fail:
            raise RuntimeError("the warehouse is down")

    return [rs.AssetMigration(to=2, start_at=start_at)(add_currency)]


def test_assets_info_shows_the_declared_and_the_stored_version(storage, grpc_stubs):
    io = rs.InMemoryIOHandler()
    assert _repo(storage, io).materialize().success
    repo = _repo(storage, io, schema_version=2, migrations=_convert())

    with _serve(repo, grpc_stubs) as (stub, pb2):
        (orders,) = stub.GetAssetsInfo(pb2.GetAssetsInfoRequest()).assets

    stored = orders.stored_version
    assert orders.schema_version == 2
    assert (stored.applied_version, stored.state, stored.target_version) == (
        1,
        "pending",
        2,
    )
    assert not stored.HasField("reason")


def test_a_failed_migration_runs_again_after_a_retry(storage, grpc_stubs):
    io = rs.InMemoryIOHandler()
    assert _repo(storage, io).materialize().success
    repo = _repo(storage, io, schema_version=2, migrations=_convert(fail=True))
    (record,) = repo.migrate(raise_on_error=False)
    assert record.state == "failed"

    with _serve(repo, grpc_stubs) as (stub, pb2):
        (orders,) = stub.GetAssetsInfo(pb2.GetAssetsInfoRequest()).assets
        assert "the warehouse is down" in orders.stored_version.reason

        retry = pb2.RetryMigrationRequest(asset_key="orders")
        assert stub.RetryMigration(retry).success
        assert not stub.RetryMigration(retry).success
        with pytest.raises(grpc.RpcError) as missing:
            stub.RetryMigration(pb2.RetryMigrationRequest(asset_key="nope"))

    assert missing.value.code() == grpc.StatusCode.NOT_FOUND
    assert missing.value.details() == "Asset 'nope' not found"
    row = storage.get_asset_version("orders")
    assert (row.state, row.reason, row.owner) == ("draining", None, None)


def test_a_pending_migration_can_be_canceled_after_a_rollback(storage, grpc_stubs):
    io = rs.InMemoryIOHandler()
    assert _repo(storage, io).materialize().success
    new = _repo(storage, io, schema_version=2, migrations=_convert())
    cancel = None

    with _serve(new, grpc_stubs) as (stub, pb2):
        cancel = pb2.CancelMigrationRequest(asset_key="orders")
        with pytest.raises(grpc.RpcError) as refused:
            stub.CancelMigration(cancel)

    assert refused.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert refused.value.details() == (
        "this code declares version 2 of orders; its migration would start again"
    )
    assert storage.get_asset_version("orders").state == "pending"

    rolled_back = _repo(storage, io)
    with _serve(rolled_back, grpc_stubs) as (stub, pb2):
        assert stub.CancelMigration(cancel).success
        assert not stub.CancelMigration(cancel).success

    row = storage.get_asset_version("orders")
    assert (row.state, row.applied_version, row.target_version) == ("ready", 1, None)
    assert rolled_back.materialize().success


def test_a_migration_that_starts_later_starts_now_on_request(storage, grpc_stubs):
    io = rs.InMemoryIOHandler()
    assert _repo(storage, io).materialize().success
    repo = _repo(storage, io, schema_version=2, migrations=_convert(start_at=LATER))

    with _serve(repo, grpc_stubs) as (stub, pb2):
        (orders,) = stub.GetAssetsInfo(pb2.GetAssetsInfoRequest()).assets
        assert orders.stored_version.start_at == int(LATER.timestamp()) * 10**9

        start = pb2.StartMigrationRequest(asset_key="orders")
        assert stub.StartMigration(start).success
        assert not stub.StartMigration(start).success
        (orders,) = stub.GetAssetsInfo(pb2.GetAssetsInfoRequest()).assets
        assert not orders.stored_version.HasField("start_at")
        with pytest.raises(grpc.RpcError) as missing:
            stub.StartMigration(pb2.StartMigrationRequest(asset_key="nope"))

    assert missing.value.code() == grpc.StatusCode.NOT_FOUND
    row = storage.get_asset_version("orders")
    assert (row.state, row.owner) == ("draining", None)
    daemon = AutomationDaemon(repo=repo, storage=storage, condition_eval_interval="1s")
    daemon.start()
    try:
        wait_for(
            lambda: storage.get_asset_version("orders").state == "ready",
            30.0,
            "the migration of orders to end",
        )
    finally:
        daemon.stop()
    assert storage.get_asset_version("orders").applied_version == 2


def test_code_that_does_not_run_the_migration_cannot_start_it(storage, grpc_stubs):
    io = rs.InMemoryIOHandler()
    assert _repo(storage, io).materialize().success
    _repo(storage, io, schema_version=2, migrations=_convert(start_at=LATER))
    rolled_back = _repo(storage, io)

    with _serve(rolled_back, grpc_stubs) as (stub, pb2):
        with pytest.raises(grpc.RpcError) as refused:
            stub.StartMigration(pb2.StartMigrationRequest(asset_key="orders"))

    assert refused.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert refused.value.details() == (
        "this code declares version 1 of orders; it does not run the migration to version 2"
    )
    assert storage.get_asset_version("orders").state == "pending"
