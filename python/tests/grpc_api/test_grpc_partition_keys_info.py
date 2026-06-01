"""gRPC `GetAssetsInfo` exposes enumerable partition keys.

The UI's "Execute job" partition picker reads this — the previous behavior
returned empty `keys` for every kind except `Static`, so TimeWindow and
Multi assets had no picker. These tests pin that all enumerable kinds
(Static / TimeWindow / Multi) round-trip their keys, and that Dynamic
still returns empty (it can't be enumerated).
"""

from datetime import datetime, timedelta

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
    # A Multi whose "day" dimension overflows the 1000-key inline window, so the
    # UI must page that dimension (product 2 × 1500 = 3000, under the cap).
    pd_multi_big = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "day": rs.PartitionsDefinition.static_([f"d{i:04d}" for i in range(1500)]),
        }
    )
    # 100 days of one-second windows = 8,640,000 partitions — far past the 1M
    # cap. Closed-form count + seek must serve it without enumerating.
    pd_interval_big = rs.PartitionsDefinition.time_window(
        start=datetime(2024, 1, 1), interval_seconds=1, end=datetime(2024, 4, 10)
    )
    # 1 hour of one-second windows = 3600 partitions — small enough to filter.
    pd_interval_small = rs.PartitionsDefinition.time_window(
        start=datetime(2024, 1, 1),
        interval_seconds=1,
        end=datetime(2024, 1, 1, 1, 0, 0),
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

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), partitions_def=pd_multi_big)
    def multi_big_asset():
        return 1

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), partitions_def=pd_interval_big)
    def interval_big_asset():
        return 1

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), partitions_def=pd_interval_small)
    def interval_small_asset():
        return 1

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def plain_asset():
        return 1

    repo = rs.CodeRepository(
        assets=[
            static_asset,
            daily_asset,
            dynamic_asset,
            multi_asset,
            multi_big_asset,
            interval_big_asset,
            interval_small_asset,
            plain_asset,
        ],
        default_executor=rs.Executor.in_process(),
    )
    port = repo._start_grpc_server("127.0.0.1", 0)

    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    grpc.channel_ready_future(channel).result(timeout=5)

    pb2, pb2_grpc = grpc_stubs
    yield channel, pb2, pb2_grpc
    channel.close()
    repo._stop_grpc_server()


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
    # The namespace is shipped so the UI can source count + keys from storage
    # (the def-level total_count stays 0).
    assert a.partition_def.dynamic_name == "customers"
    assert a.partition_def.total_count == 0


def test_unpartitioned_asset_has_no_partition_def(partition_kinds_grpc_channel):
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    resp = stub.GetAssetsInfo(pb2.GetAssetsInfoRequest())
    a = _asset(resp, "plain_asset")
    # proto3: optional fields default to a default-constructed message;
    # assert by checking presence via the well-known `kind` field.
    assert a.partition_def.kind == ""


# --- GetPartitionKeys: paged browse + search filter ---
#
# Empty `query` browses (window + full count); non-empty filters (window + match
# count). `GetPartitionKeyIndex` resolves a key to its index for the jump.


def test_get_partition_keys_browse_window(partition_kinds_grpc_channel):
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    # Window [0, 2) of the 3 static keys, in definition order.
    resp = stub.GetPartitionKeys(
        pb2.GetPartitionKeysRequest(
            asset_key="static_asset", offset=0, limit=2, query=""
        )
    )
    assert list(resp.keys) == ["red", "green"]
    assert resp.total == 3
    # Window [2, 4) returns the tail; `total` is still the full count.
    resp = stub.GetPartitionKeys(
        pb2.GetPartitionKeysRequest(
            asset_key="static_asset", offset=2, limit=2, query=""
        )
    )
    assert list(resp.keys) == ["blue"]
    assert resp.total == 3


def test_get_partition_keys_search_filters_static(partition_kinds_grpc_channel):
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    # Substring "re": "red" and "green" match (definition order), "blue" doesn't.
    resp = stub.GetPartitionKeys(
        pb2.GetPartitionKeysRequest(
            asset_key="static_asset", offset=0, limit=50, query="re"
        )
    )
    assert list(resp.keys) == ["red", "green"]
    assert resp.total == 2
    # `total` is the match count and the window pages within the matches.
    resp = stub.GetPartitionKeys(
        pb2.GetPartitionKeysRequest(
            asset_key="static_asset", offset=1, limit=50, query="re"
        )
    )
    assert list(resp.keys) == ["green"]
    assert resp.total == 2


def test_get_partition_keys_search_filters_timewindow(partition_kinds_grpc_channel):
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    resp = stub.GetPartitionKeys(
        pb2.GetPartitionKeysRequest(
            asset_key="daily_asset", offset=0, limit=50, query="01-02"
        )
    )
    assert list(resp.keys) == ["2024-01-02"]
    assert resp.total == 1


def test_get_partition_keys_search_on_multi_is_empty(partition_kinds_grpc_channel):
    """Search/jump are single-dim only — Multi uses per-dimension selectors,
    so a filtered query against a Multi asset yields no keys."""
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    resp = stub.GetPartitionKeys(
        pb2.GetPartitionKeysRequest(
            asset_key="multi_asset", offset=0, limit=50, query="r"
        )
    )
    assert list(resp.keys) == []
    assert resp.total == 0


def test_get_partition_key_index_static(partition_kinds_grpc_channel):
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    for key, expected in [("red", 0), ("green", 1), ("blue", 2)]:
        resp = stub.GetPartitionKeyIndex(
            pb2.GetPartitionKeyIndexRequest(asset_key="static_asset", key=key)
        )
        assert resp.index == expected
    # Absent key → -1 (picker shows "not found", scrolls nowhere).
    resp = stub.GetPartitionKeyIndex(
        pb2.GetPartitionKeyIndexRequest(asset_key="static_asset", key="purple")
    )
    assert resp.index == -1


def test_get_partition_key_index_timewindow(partition_kinds_grpc_channel):
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    resp = stub.GetPartitionKeyIndex(
        pb2.GetPartitionKeyIndexRequest(asset_key="daily_asset", key="2024-01-03")
    )
    assert resp.index == 2
    resp = stub.GetPartitionKeyIndex(
        pb2.GetPartitionKeyIndexRequest(asset_key="daily_asset", key="2025-12-31")
    )
    assert resp.index == -1


# --- Interval TimeWindow past the 1M cap: closed-form count + seek ---
#
# An 8.64M-partition asset must page instantly; before lazy windowing it errored
# past the 1M cap (dead picker).

FMT = "%Y-%m-%dT%H:%M:%S"


def test_interval_timewindow_count_is_closed_form(partition_kinds_grpc_channel):
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    # 100 days × 86,400 s = 8,640,000 — reported exactly (well past the 1M cap),
    # and the first window comes back without enumerating the whole series.
    resp = stub.GetPartitionKeys(
        pb2.GetPartitionKeysRequest(
            asset_key="interval_big_asset", offset=0, limit=2, query=""
        )
    )
    assert resp.total == 8_640_000
    assert list(resp.keys) == [
        datetime(2024, 1, 1).strftime(FMT),
        (datetime(2024, 1, 1) + timedelta(seconds=1)).strftime(FMT),
    ]


def test_interval_timewindow_far_window_seeks(partition_kinds_grpc_channel):
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    # Offset 8,000,000 — a closed-form seek, not an 8M-step walk.
    resp = stub.GetPartitionKeys(
        pb2.GetPartitionKeysRequest(
            asset_key="interval_big_asset", offset=8_000_000, limit=2, query=""
        )
    )
    assert resp.total == 8_640_000
    assert list(resp.keys) == [
        (datetime(2024, 1, 1) + timedelta(seconds=8_000_000)).strftime(FMT),
        (datetime(2024, 1, 1) + timedelta(seconds=8_000_001)).strftime(FMT),
    ]


def test_interval_timewindow_index_is_closed_form(partition_kinds_grpc_channel):
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    far_key = (datetime(2024, 1, 1) + timedelta(seconds=5_000_000)).strftime(FMT)
    resp = stub.GetPartitionKeyIndex(
        pb2.GetPartitionKeyIndexRequest(asset_key="interval_big_asset", key=far_key)
    )
    assert resp.index == 5_000_000
    # A timestamp off the one-second grid (sub-second) doesn't exist.
    resp = stub.GetPartitionKeyIndex(
        pb2.GetPartitionKeyIndexRequest(
            asset_key="interval_big_asset", key="2024-01-01T00:00:00.5"
        )
    )
    assert resp.index == -1


def test_interval_timewindow_filter_and_window(partition_kinds_grpc_channel):
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    # Small 3600-key asset: a minute-prefixed query matches exactly its 60 seconds.
    resp = stub.GetPartitionKeys(
        pb2.GetPartitionKeysRequest(
            asset_key="interval_small_asset", offset=0, limit=5, query="T00:05:"
        )
    )
    assert resp.total == 60
    assert list(resp.keys) == [
        datetime(2024, 1, 1, 0, 5, s).strftime(FMT) for s in range(5)
    ]
    # Browse window + exact count for the small asset.
    resp = stub.GetPartitionKeys(
        pb2.GetPartitionKeysRequest(
            asset_key="interval_small_asset", offset=0, limit=3, query=""
        )
    )
    assert resp.total == 3600
    assert list(resp.keys) == [
        datetime(2024, 1, 1, 0, 0, s).strftime(FMT) for s in range(3)
    ]


# --- Multi per-dimension paging ---
#
# A Multi dimension can overflow the 1000-key window; the picker pages it via the
# `dimension` field on the same RPC.


def test_multi_dimension_truncation_flags(partition_kinds_grpc_channel):
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    resp = stub.GetAssetsInfo(pb2.GetAssetsInfoRequest())
    a = _asset(resp, "multi_big_asset")
    by_name = {d.name: d for d in a.partition_def.dimensions}
    # Small dimension: full, not truncated.
    assert by_name["region"].total_count == 2
    assert by_name["region"].keys_truncated is False
    assert list(by_name["region"].keys) == ["us", "eu"]
    # Large dimension: windowed to 1000, flagged truncated, true size reported.
    assert by_name["day"].total_count == 1500
    assert by_name["day"].keys_truncated is True
    assert len(by_name["day"].keys) == 1000
    assert by_name["day"].keys[0] == "d0000"


def test_multi_dimension_paging(partition_kinds_grpc_channel):
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    # Page past the inline window into the large "day" dimension.
    resp = stub.GetPartitionKeys(
        pb2.GetPartitionKeysRequest(
            asset_key="multi_big_asset", dimension="day", offset=1200, limit=3, query=""
        )
    )
    assert resp.total == 1500
    assert list(resp.keys) == ["d1200", "d1201", "d1202"]
    # The small dimension via the same path.
    resp = stub.GetPartitionKeys(
        pb2.GetPartitionKeysRequest(
            asset_key="multi_big_asset",
            dimension="region",
            offset=0,
            limit=10,
            query="",
        )
    )
    assert resp.total == 2
    assert list(resp.keys) == ["us", "eu"]


def test_multi_dimension_search_and_jump(partition_kinds_grpc_channel):
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    # Substring filter within the dimension: "d149" → d1490..d1499.
    resp = stub.GetPartitionKeys(
        pb2.GetPartitionKeysRequest(
            asset_key="multi_big_asset",
            dimension="day",
            offset=0,
            limit=100,
            query="d149",
        )
    )
    assert resp.total == 10
    assert list(resp.keys) == [f"d149{i}" for i in range(10)]
    # Jump within the dimension resolves the key's index.
    resp = stub.GetPartitionKeyIndex(
        pb2.GetPartitionKeyIndexRequest(
            asset_key="multi_big_asset", dimension="day", key="d1300"
        )
    )
    assert resp.index == 1300


def test_multi_unknown_dimension_errors(partition_kinds_grpc_channel):
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    with pytest.raises(grpc.RpcError):
        stub.GetPartitionKeys(
            pb2.GetPartitionKeysRequest(
                asset_key="multi_big_asset",
                dimension="nonexistent",
                offset=0,
                limit=10,
                query="",
            )
        )


def test_multi_full_product_window_is_lazy(partition_kinds_grpc_channel):
    channel, pb2, pb2_grpc = partition_kinds_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    # Empty `dimension` windows the cartesian product (region × day) — built by
    # mixed-radix seek, not by enumerating all 3000 combos. (This is the path the
    # asset-detail heatmap uses for a Multi asset.)
    resp = stub.GetPartitionKeys(
        pb2.GetPartitionKeysRequest(
            asset_key="multi_big_asset", offset=2000, limit=2, query=""
        )
    )
    assert resp.total == 3000  # 2 regions × 1500 days
    # Combo 2000: region idx 2000//1500 = 1 (eu), day idx 2000%1500 = 500.
    # The display sorts dimensions by name.
    assert list(resp.keys) == [
        "day=d0500|region=eu",
        "day=d0501|region=eu",
    ]


def test_multi_window_past_one_million(grpc_stubs):
    """A Multi whose cartesian product exceeds 1M used to error (the old cap);
    now it reports the true count and windows the tail via lazy seek."""
    pd = rs.PartitionsDefinition.multi(
        {
            "region": rs.PartitionsDefinition.static_(["us", "eu"]),
            "item": rs.PartitionsDefinition.static_(
                [f"i{n:06d}" for n in range(600_000)]
            ),
        }
    )

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), partitions_def=pd)
    def huge_multi():
        return 1

    repo = rs.CodeRepository(
        assets=[huge_multi], default_executor=rs.Executor.in_process()
    )
    port = repo._start_grpc_server("127.0.0.1", 0)
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    grpc.channel_ready_future(channel).result(timeout=5)
    pb2, pb2_grpc = grpc_stubs
    try:
        stub = pb2_grpc.CodeLocationServiceStub(channel)
        resp = stub.GetPartitionKeys(
            pb2.GetPartitionKeysRequest(
                asset_key="huge_multi", offset=1_100_000, limit=2, query=""
            )
        )
        assert resp.total == 1_200_000  # 2 × 600_000 — no cap
        # k=1_100_000: region idx 1_100_000//600_000 = 1 (eu), item idx 500_000.
        assert list(resp.keys) == [
            "item=i500000|region=eu",
            "item=i500001|region=eu",
        ]
    finally:
        channel.close()
        repo._stop_grpc_server()
