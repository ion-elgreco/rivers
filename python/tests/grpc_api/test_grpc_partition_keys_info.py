"""gRPC `GetAssetsInfo` exposes enumerable partition keys.

The UI's "Execute job" partition picker reads this — the previous behavior
returned empty `keys` for every kind except `Static`, so TimeWindow and
Multi assets had no picker. These tests pin that all enumerable kinds
(Static / TimeWindow / Multi) round-trip their keys, and that Dynamic
still returns empty (it can't be enumerated).
"""

from datetime import datetime

import grpc
import pytest

import rivers as rs


@pytest.fixture
def partition_kinds_grpc_channel(grpc_stubs):
    pd_static = rs.PartitionsDefinition.static_(["red", "green", "blue"])
    pd_daily = rs.PartitionsDefinition.daily(
        start=datetime(2024, 1, 1), end=datetime(2024, 1, 4)
    )
    pd_dyn = rs.PartitionsDefinition.dynamic("customers")
    pd_multi = rs.PartitionsDefinition.multi(
        {
            "color": rs.PartitionsDefinition.static_(["r", "g"]),
            "size": rs.PartitionsDefinition.static_(["s", "m"]),
        }
    )

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), partitions_def=pd_static)
    def static_asset():
        return 1

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), partitions_def=pd_daily)
    def daily_asset():
        return 1

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), partitions_def=pd_dyn)
    def dynamic_asset():
        return 1

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), partitions_def=pd_multi)
    def multi_asset():
        return 1

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def plain_asset():
        return 1

    repo = rs.CodeRepository(
        assets=[static_asset, daily_asset, dynamic_asset, multi_asset, plain_asset],
        default_executor=rs.Executor.in_process(),
    )
    port = repo._start_grpc_server("127.0.0.1", 0)

    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    grpc.channel_ready_future(channel).result(timeout=5)

    pb2, pb2_grpc = grpc_stubs
    yield channel, pb2, pb2_grpc
    channel.close()


def _asset(response, name):
    return next(a for a in response.assets if a.asset_key == name)


def test_static_partition_keys_enumerated(partition_kinds_grpc_channel):
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    resp = stub.GetAssetsInfo(pb2.GetAssetsInfoRequest())
    a = _asset(resp, "static_asset")
    assert a.partition_def.kind == "Static"
    assert list(a.partition_def.keys) == ["red", "green", "blue"]


def test_timewindow_partition_keys_enumerated(partition_kinds_grpc_channel):
    """Regression: previously TimeWindow returned empty `keys` so the UI
    couldn't render a partition picker for daily-partitioned assets."""
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    resp = stub.GetAssetsInfo(pb2.GetAssetsInfoRequest())
    a = _asset(resp, "daily_asset")
    assert a.partition_def.kind == "TimeWindow"
    # 3 days from 2024-01-01 (inclusive) up to 2024-01-04 (exclusive).
    assert list(a.partition_def.keys) == [
        "2024-01-01",
        "2024-01-02",
        "2024-01-03",
    ]


def test_multi_partition_per_dimension_keys(partition_kinds_grpc_channel):
    """Multi partitions surface per-dimension keys (not the cartesian
    product) so the UI can render one selector per dimension."""
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    resp = stub.GetAssetsInfo(pb2.GetAssetsInfoRequest())
    a = _asset(resp, "multi_asset")
    assert a.partition_def.kind == "Multi"
    # `keys` is empty for Multi — the per-dim keys live in `dimensions`
    # so the UI doesn't have to parse "dim=val|dim=val" strings back out.
    assert list(a.partition_def.keys) == []
    by_name = {d.name: list(d.keys) for d in a.partition_def.dimensions}
    assert by_name == {"color": ["r", "g"], "size": ["s", "m"]}


def test_dynamic_partition_keys_empty(partition_kinds_grpc_channel):
    """Dynamic keys can't be enumerated server-side — they're storage-managed.
    The UI's partition picker treats empty `keys` as 'no enumerable keys'."""
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    resp = stub.GetAssetsInfo(pb2.GetAssetsInfoRequest())
    a = _asset(resp, "dynamic_asset")
    assert a.partition_def.kind == "Dynamic"
    assert list(a.partition_def.keys) == []


def test_unpartitioned_asset_has_no_partition_def(partition_kinds_grpc_channel):
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    resp = stub.GetAssetsInfo(pb2.GetAssetsInfoRequest())
    a = _asset(resp, "plain_asset")
    # proto3: optional fields default to a default-constructed message;
    # assert by checking presence via the well-known `kind` field.
    assert a.partition_def.kind == ""
