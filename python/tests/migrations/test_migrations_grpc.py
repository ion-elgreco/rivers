"""The gRPC API shows the version of each asset's stored data, and retries or
cancels its migration."""

import contextlib

import pytest

pytest.importorskip("grpc")

import grpc

import rivers as rs


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


def _convert(fail=False):
    def add_currency(ctx: rs.MigrationContext) -> None:
        if fail:
            raise RuntimeError("the warehouse is down")

    return [rs.AssetMigration(to=2)(add_currency)]


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
