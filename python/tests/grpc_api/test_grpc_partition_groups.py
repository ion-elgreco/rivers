"""gRPC `GetPartitionGroups`: the keys of one dimension as rows of an outline.

The Partitions tab of an asset asks the code location how the keys of a
dimension split into groups (months, days), and counts the partitions of
each group in storage. A group must be a run of keys that follow each other
in sorted order, and no reply may grow with the number of keys.
"""

import pytest

pytest.importorskip("grpc")

from datetime import datetime

import grpc

import rivers as rs

# The ticks of a grid: its cron schedule, start and end. The ticks of the
# first grids are a fixed time apart.
_CRON_GRIDS = {
    "every_hour": ("0 * * * *", datetime(2024, 1, 1), datetime(2024, 3, 1)),
    "every_day": ("0 0 * * *", datetime(2024, 1, 1), datetime(2024, 6, 1)),
    "every_quarter_hour": ("*/15 * * * *", datetime(2024, 1, 1), datetime(2024, 1, 20)),
    "every_six_hours": ("0 */6 * * *", datetime(2024, 1, 1), datetime(2024, 3, 1)),
    "every_night": ("30 2 * * *", datetime(2024, 1, 1), datetime(2024, 6, 1)),
    "every_hour_with_seconds": (
        "0 0 * * * *",
        datetime(2024, 1, 1),
        datetime(2024, 3, 1),
    ),
    "start_off_the_grid": (
        "0 * * * *",
        datetime(2024, 1, 1, 0, 30),
        datetime(2024, 1, 4),
    ),
    "end_off_the_grid": (
        "0 * * * *",
        datetime(2024, 1, 1),
        datetime(2024, 1, 3, 12, 30),
    ),
    "every_five_hours": ("0 */5 * * *", datetime(2024, 1, 1), datetime(2024, 3, 1)),
    "office_hours": ("0 9-17 * * 1-5", datetime(2024, 1, 1), datetime(2024, 3, 1)),
    "every_month": ("0 0 1 * *", datetime(2024, 1, 1), datetime(2026, 1, 1)),
    "every_ten_seconds": (
        "*/10 * * * * *",
        datetime(2024, 1, 1),
        datetime(2024, 1, 1, 3),
    ),
}


@pytest.fixture(scope="module")
def code_location(grpc_stubs):
    hours = rs.PartitionsDefinition.hourly(
        start=datetime(2024, 1, 1), end=datetime(2024, 7, 1)
    )
    zones = rs.PartitionsDefinition.static_([f"{zone:03d}" for zone in range(1, 266)])
    definitions = {
        "trips": rs.PartitionsDefinition.multi({"zone": zones, "hour": hours}),
        "daily": rs.PartitionsDefinition.daily(
            start=datetime(2024, 8, 1), end=datetime(2024, 10, 30)
        ),
        "day_first": rs.PartitionsDefinition.daily(
            start=datetime(2024, 1, 1), end=datetime(2024, 3, 1), fmt="%d/%m/%Y"
        ),
        "sizes": rs.PartitionsDefinition.static_(["small", "medium", "large"]),
        # 100 days of one-second windows: 8,640,000 keys.
        "seconds": rs.PartitionsDefinition.time_window(
            start=datetime(2024, 1, 1), interval_seconds=1, end=datetime(2024, 4, 10)
        ),
        "customers": rs.PartitionsDefinition.dynamic("customers"),
    }
    for name, (cron, start, end) in _CRON_GRIDS.items():
        definitions[f"cron_{name}"] = rs.PartitionsDefinition.time_window(
            start=start, cron_schedule=cron, end=end
        )

    assets = []
    for name, definition in definitions.items():

        @rs.Asset(
            name=name, io_handler=rs.InMemoryIOHandler(), partitions_def=definition
        )
        def asset():
            return 1

        assets.append(asset)

    repo = rs.CodeRepository(assets=assets, default_executor=rs.Executor.in_process())
    port = repo._start_grpc_server("127.0.0.1", 0)
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    grpc.channel_ready_future(channel).result(timeout=5)

    pb2, pb2_grpc = grpc_stubs
    yield pb2_grpc.CodeLocationServiceStub(channel), pb2
    channel.close()
    repo._stop_grpc_server()


@pytest.fixture(scope="module")
def groups_stub(code_location):
    stub, pb2 = code_location

    def groups(asset_key, **fields):
        return stub.GetPartitionGroups(
            pb2.GetPartitionGroupsRequest(asset_key=asset_key, **fields)
        )

    return groups


@pytest.fixture(scope="module")
def listed_keys(code_location):
    """Every key of an asset, as `GetPartitionKeys` lists them."""
    stub, pb2 = code_location

    def keys(asset_key):
        reply = stub.GetPartitionKeys(
            pb2.GetPartitionKeysRequest(asset_key=asset_key, limit=1_000_000)
        )
        assert reply.total == len(reply.keys)
        return list(reply.keys)

    return keys


def _labels(response):
    return [group.label for group in response.groups]


def test_finest_split_with_at_most_the_asked_groups(groups_stub):
    months = groups_stub("trips", dimension="hour", max_groups=62)
    days = groups_stub("trips", dimension="hour", max_groups=400)

    assert _labels(months) == [f"2024-0{month}" for month in range(1, 7)]
    assert months.unit == "month"
    assert months.key_unit == "hour"
    assert months.total_keys == 4368
    assert months.ascending and not months.of_keys
    assert months.time
    february = months.groups[1]
    assert (february.first_key, february.last_key, february.keys) == (
        "2024-02-01T00:00",
        "2024-02-29T23:00",
        696,
    )

    assert len(days.groups) == 182
    assert days.unit == "day"
    assert days.groups[0].label == "2024-01-01"
    assert days.groups[0].keys == 24


def test_next_split_of_a_group(groups_stub):
    of_june = groups_stub(
        "trips",
        dimension="hour",
        first_key="2024-06-01T00:00",
        last_key="2024-06-30T23:00",
    )
    of_a_day = groups_stub(
        "trips",
        dimension="hour",
        first_key="2024-06-30T00:00",
        last_key="2024-06-30T23:00",
    )

    assert len(of_june.groups) == 30
    assert of_june.unit == "day"
    assert of_june.groups[-1].label == "2024-06-30"

    assert of_a_day.of_keys
    assert of_a_day.unit == ""
    assert _labels(of_a_day)[-2:] == ["2024-06-30T22:00", "2024-06-30T23:00"]
    assert all(group.keys == 1 for group in of_a_day.groups)


def test_keys_with_no_shared_start_are_listed(groups_stub):
    zones = groups_stub("trips", dimension="zone", max_groups=400)
    parts = groups_stub("trips", dimension="zone", max_groups=62)

    assert zones.of_keys
    assert _labels(zones)[:2] == ["001", "002"]
    assert len(zones.groups) == 265
    assert zones.key_unit == ""
    assert not zones.time

    assert not parts.of_keys
    assert parts.groups[0].label == "001 … 005"
    assert sum(group.keys for group in parts.groups) == 265


def test_few_keys_of_one_dimension_are_listed(groups_stub):
    days = groups_stub("daily", max_groups=120)

    assert days.of_keys
    assert len(days.groups) == 90
    assert days.key_unit == "day"
    assert _labels(days)[0] == "2024-08-01"


@pytest.mark.parametrize(("asset_key", "total_keys"), [("day_first", 60), ("sizes", 3)])
def test_keys_that_do_not_ascend_have_no_groups(groups_stub, asset_key, total_keys):
    response = groups_stub(asset_key, max_groups=400)

    assert not response.ascending
    assert len(response.groups) == 0
    assert response.total_keys == total_keys


def test_a_large_dimension_is_split_without_a_read_of_every_key(groups_stub):
    days = groups_stub("seconds", max_groups=400)
    of_a_minute = groups_stub(
        "seconds",
        first_key=days.groups[41].first_key[:10] + "T05:07:00",
        last_key=days.groups[41].first_key[:10] + "T05:07:59",
    )

    assert days.total_keys == 8_640_000
    assert len(days.groups) == 100
    assert days.unit == "day"
    assert days.key_unit == "second"
    assert days.groups[41].keys == 86_400
    assert len(of_a_minute.groups) == 60
    assert of_a_minute.of_keys


def _dimensions(response):
    return [
        (dimension.name, dimension.keys, dimension.dynamic_name)
        for dimension in response.dimensions
    ]


@pytest.mark.parametrize("grid", _CRON_GRIDS)
@pytest.mark.parametrize("max_groups", [0, 31, 400])
def test_the_groups_of_a_cron_grid_hold_the_keys_of_the_grid(
    groups_stub, listed_keys, grid, max_groups
):
    keys = listed_keys(f"cron_{grid}")

    response = groups_stub(f"cron_{grid}", max_groups=max_groups)

    assert response.ascending and response.time
    assert response.total_keys == len(keys)
    assert _dimensions(response) == [("", len(keys), "")]
    at = 0
    for group in response.groups:
        last = at + group.keys - 1
        assert (group.first_key, group.last_key) == (keys[at], keys[last])
        at += group.keys
    assert at == len(keys)


def test_a_cron_grid_starts_at_its_first_tick_and_ends_before_its_end(groups_stub):
    late_start = groups_stub("cron_start_off_the_grid", max_groups=400)
    early_end = groups_stub("cron_end_off_the_grid", max_groups=400)
    nights = groups_stub("cron_every_night", max_groups=400)

    assert late_start.total_keys == 71
    assert late_start.groups[0].first_key == "2024-01-01T01:00:00"
    assert late_start.groups[-1].last_key == "2024-01-03T23:00:00"

    assert early_end.total_keys == 61
    assert early_end.groups[-1].last_key == "2024-01-03T12:00:00"

    assert nights.total_keys == 152
    assert nights.groups[0].first_key == "2024-01-01T02:30:00"
    assert nights.groups[-1].last_key == "2024-05-31T02:30:00"


def test_the_next_split_of_a_cron_grid_is_a_run_of_its_keys(groups_stub, listed_keys):
    keys = listed_keys("cron_every_six_hours")

    of_february = groups_stub(
        "cron_every_six_hours",
        first_key="2024-02-01T00:00:00",
        last_key="2024-02-29T18:00:00",
    )
    of_a_day = groups_stub(
        "cron_every_six_hours",
        first_key="2024-02-29T00:00:00",
        last_key="2024-02-29T18:00:00",
    )

    assert _labels(of_february) == [f"2024-02-{day:02d}" for day in range(1, 30)]
    assert all(group.keys == 4 for group in of_february.groups)
    assert of_a_day.of_keys
    assert _labels(of_a_day) == [key for key in keys if key.startswith("2024-02-29")]
    assert len(of_a_day.groups) == 4


def test_the_reply_names_the_dimensions_in_the_order_of_their_names(groups_stub):
    trips = groups_stub("trips", dimension="zone", max_groups=10)
    daily = groups_stub("daily", max_groups=10)

    assert _dimensions(trips) == [("hour", 4368, ""), ("zone", 265, "")]
    assert _dimensions(daily) == [("", 90, "")]


def test_the_first_dimension_by_name_is_split_when_none_is_named(groups_stub):
    first = groups_stub("trips", max_groups=62)

    assert first == groups_stub("trips", dimension="hour", max_groups=62)
    assert first.total_keys == 4368


def test_a_dynamic_set_has_no_groups(groups_stub):
    customers = groups_stub("customers", max_groups=10)

    assert _dimensions(customers) == [("", 0, "customers")]
    assert len(customers.groups) == 0
    assert not customers.ascending


@pytest.mark.parametrize(
    ("asset_key", "dimension", "message"),
    [
        ("trips", "nope", "no partition dimension"),
        ("daily", "hour", "no partition dimension"),
        ("missing", "", "not found"),
    ],
)
def test_requests_with_no_keys_to_split_fail(
    groups_stub, asset_key, dimension, message
):
    with pytest.raises(grpc.RpcError) as error:
        groups_stub(asset_key, dimension=dimension, max_groups=10)

    assert message in error.value.details()
