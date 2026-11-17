"""gRPC reads of the keys of one dimension: a page by its place, the place of
a key, and the keys that hold a text.

The launch dialog and the Partitions tab read pages of keys, jump to a key
and search the keys. Each reply must hold the keys that the definition
lists, and no read may walk a grid of millions of keys.
"""

import pytest

pytest.importorskip("grpc")

from datetime import datetime, timedelta

import grpc

import rivers as rs

_DAY = 86_400

# The time grids: the arguments of `PartitionsDefinition.time_window`.
_GRIDS = {
    "cron_every_hour": dict(
        cron_schedule="0 * * * *", start=datetime(2024, 1, 1), end=datetime(2024, 3, 1)
    ),
    "cron_every_five_hours": dict(
        cron_schedule="0 */5 * * *",
        start=datetime(2024, 1, 1),
        end=datetime(2024, 3, 1),
    ),
    "cron_office_hours": dict(
        cron_schedule="0 9-17 * * 1-5",
        start=datetime(2024, 1, 1),
        end=datetime(2024, 6, 1),
    ),
    "cron_office_minutes": dict(
        cron_schedule="*/20 8-18 * * 1-5",
        start=datetime(2024, 1, 5, 17, 50),
        end=datetime(2024, 2, 5, 8, 20),
    ),
    "cron_last_day_of_the_month": dict(
        cron_schedule="0 0 L * *", start=datetime(2023, 1, 1), end=datetime(2025, 1, 1)
    ),
    "cron_every_ten_seconds": dict(
        cron_schedule="*/10 * * * * *",
        start=datetime(2024, 1, 1, 0, 0, 5),
        end=datetime(2024, 1, 1, 3),
    ),
    "cron_start_off_the_grid": dict(
        cron_schedule="0 * * * *",
        start=datetime(2024, 1, 1, 0, 30),
        end=datetime(2024, 1, 4),
    ),
    "cron_end_off_the_grid": dict(
        cron_schedule="30 2 * * *",
        start=datetime(2024, 1, 1, 3),
        end=datetime(2024, 6, 1, 2, 30),
    ),
    "cron_daily_by_name": dict(
        cron_schedule="@daily", start=datetime(2024, 1, 1), end=datetime(2024, 6, 1)
    ),
    "cron_weekly_by_name": dict(
        cron_schedule="@weekly", start=datetime(2024, 1, 1), end=datetime(2025, 1, 1)
    ),
    "cron_hourly_by_name": dict(
        cron_schedule="@hourly",
        start=datetime(2024, 1, 1, 0, 30),
        end=datetime(2024, 1, 20),
    ),
    # A field for the year: the keys of this grid are listed by a walk.
    "cron_with_a_year": dict(
        cron_schedule="0 0 */6 * * * 2024",
        start=datetime(2023, 6, 1),
        end=datetime(2026, 1, 1),
    ),
    "cron_keys_with_no_separator": dict(
        cron_schedule="0 * * * *",
        start=datetime(2024, 1, 1),
        end=datetime(2024, 3, 1),
        fmt="%Y%m%d%H",
    ),
    "cron_keys_with_month_names": dict(
        cron_schedule="0 0 * * *",
        start=datetime(2024, 1, 1),
        end=datetime(2024, 6, 1),
        fmt="%Y-%b-%d",
    ),
    "interval_hour": dict(
        interval_seconds=3600, start=datetime(2024, 1, 1), end=datetime(2024, 3, 1)
    ),
    "interval_quarter_hour_off_midnight": dict(
        interval_seconds=900,
        start=datetime(2024, 1, 1, 5, 7),
        end=datetime(2024, 1, 9, 13, 1),
    ),
    "interval_seven_hours": dict(
        interval_seconds=7 * 3600,
        start=datetime(2024, 1, 1),
        end=datetime(2024, 3, 1),
    ),
    "interval_week": dict(
        interval_seconds=7 * _DAY, start=datetime(2024, 1, 1), end=datetime(2025, 1, 1)
    ),
    "interval_keys_with_day_of_year": dict(
        interval_seconds=6 * 3600,
        start=datetime(2023, 12, 30),
        end=datetime(2024, 1, 3, 6),
        fmt="%Y-%jT%H",
    ),
}

# Grids with more keys than a test can walk in the time that a call gets.
_LARGE_GRIDS = {
    # 315,619,200 keys.
    "large_every_second": dict(
        cron_schedule="* * * * * *",
        start=datetime(2015, 1, 1),
        end=datetime(2025, 1, 1),
    ),
    # 84,531,600 keys.
    "large_office_seconds": dict(
        cron_schedule="* * 9-17 * * 1-5",
        start=datetime(2015, 1, 1),
        end=datetime(2025, 1, 1),
    ),
}

_DEFINITIONS = {
    name: rs.PartitionsDefinition.time_window(**grid)
    for name, grid in (_GRIDS | _LARGE_GRIDS).items()
}
_DEFINITIONS["static"] = rs.PartitionsDefinition.static_(
    [f"customer-{number:04d}" for number in range(1_500)]
)
_DEFINITIONS["hours_of_zones"] = rs.PartitionsDefinition.multi(
    {
        "zone": rs.PartitionsDefinition.static_(["a", "b", "c"]),
        "hour": _DEFINITIONS["cron_office_hours"],
    }
)

_LISTED = [name for name in _DEFINITIONS if name not in _LARGE_GRIDS]
_ONE_DIMENSION = [name for name in _LISTED if name != "hours_of_zones"]

# The seconds that a call on a large grid gets. A walk of such a grid takes
# more than a minute.
_SECONDS_MOST = 10


@pytest.fixture(scope="module")
def stub(grpc_stubs):
    assets = []
    for name, definition in _DEFINITIONS.items():

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
def keys_of():
    """The keys of a definition as the definition lists them."""
    listed = {}

    def keys(name):
        if name not in listed:
            listed[name] = [str(key) for key in _DEFINITIONS[name].get_partition_keys()]
        return listed[name]

    return keys


def _page(stub, asset_key, **fields):
    service, pb2 = stub
    return service.GetPartitionKeys(
        pb2.GetPartitionKeysRequest(asset_key=asset_key, **fields),
        timeout=_SECONDS_MOST,
    )


def _place(stub, asset_key, key):
    service, pb2 = stub
    reply = service.GetPartitionKeyIndex(
        pb2.GetPartitionKeyIndexRequest(asset_key=asset_key, key=key),
        timeout=_SECONDS_MOST,
    )
    return reply.index


def _samples(keys):
    """Keys at the start, in the middle and at the end, with their places."""
    places = sorted(
        {0, 1, len(keys) // 3, len(keys) // 2, len(keys) - 2, len(keys) - 1}
    )
    return [(place, keys[place]) for place in places]


@pytest.mark.parametrize("name", _LISTED)
def test_a_page_holds_the_keys_at_its_places(stub, keys_of, name):
    keys = keys_of(name)
    assert len(keys) > 10

    for offset in [0, 1, len(keys) // 2, len(keys) - 3, len(keys), len(keys) + 5]:
        for limit in [1, 7, 100]:
            page = _page(stub, name, offset=offset, limit=limit)

            assert list(page.keys) == keys[offset : offset + limit], (offset, limit)
            assert list(page.indices) == list(range(offset, offset + len(page.keys)))
            assert page.total == len(keys)


@pytest.mark.parametrize("name", _ONE_DIMENSION)
def test_a_key_has_the_place_that_the_definition_gives_it(stub, keys_of, name):
    keys = keys_of(name)

    for place, key in _samples(keys):
        assert _place(stub, name, key) == place, key


@pytest.mark.parametrize("name", [name for name in _ONE_DIMENSION if name in _GRIDS])
def test_a_time_that_is_not_a_tick_of_the_grid_has_no_place(stub, keys_of, name):
    keys = set(keys_of(name))
    definition = _DEFINITIONS[name]
    fmt = _GRIDS[name].get("fmt", "%Y-%m-%dT%H:%M:%S")
    times = [
        datetime(2023, 12, 25, 3, 0, 1) + step * timedelta(hours=11, seconds=1)
        for step in range(900)
    ]
    off_the_grid = [
        key for key in (time.strftime(fmt) for time in times) if key not in keys
    ]
    assert len(off_the_grid) > 100

    for key in off_the_grid:
        assert _place(stub, name, key) == -1, key
        assert not definition.validate_partition_key(rs.PartitionKey.single(key))


@pytest.mark.parametrize(
    "name", [name for name in _ONE_DIMENSION if not name.startswith("interval")]
)
def test_a_text_that_is_not_written_as_a_key_has_no_place(stub, keys_of, name):
    key = keys_of(name)[0]

    for text in ["", "nope", key + "0", key[:-1], " " + key, key + " "]:
        assert _place(stub, name, text) == -1, text


@pytest.mark.parametrize("name", _ONE_DIMENSION)
def test_a_search_gives_the_keys_that_hold_the_text(stub, keys_of, name):
    keys = keys_of(name)
    texts = {"0", "1", "zz", "-13-", "T25"}
    for _, key in _samples(keys):
        texts |= {key, key[:4], key[-5:], key[3:9], key[-1], key + "0"}

    for text in sorted(texts):
        matches = [(place, key) for place, key in enumerate(keys) if text in key]
        for offset, limit in [(0, 100), (3, 2), (len(matches), 5)]:
            page = _page(stub, name, query=text, offset=offset, limit=limit)

            expected = matches[offset : offset + limit]
            assert list(zip(page.indices, page.keys)) == expected, (text, offset)
            assert page.total == len(matches), text


def test_the_keys_of_a_large_grid_are_read_with_no_walk_of_the_grid(stub):
    seconds = 3_653 * _DAY

    first = _page(stub, "large_every_second", limit=3)
    far = _page(stub, "large_every_second", offset=seconds - 2, limit=5)
    office = _page(stub, "large_office_seconds", offset=84_531_599, limit=5)

    assert first.total == seconds
    assert list(first.keys) == [
        "2015-01-01T00:00:00",
        "2015-01-01T00:00:01",
        "2015-01-01T00:00:02",
    ]
    assert list(far.keys) == ["2024-12-31T23:59:58", "2024-12-31T23:59:59"]
    assert office.total == 84_531_600
    assert list(office.keys) == ["2024-12-31T17:59:59"]


def test_a_key_of_a_large_grid_is_placed_with_no_walk_of_the_grid(stub):
    assert _place(stub, "large_every_second", "2024-12-31T23:59:59") == 315_619_199
    assert _place(stub, "large_every_second", "2016-02-29T23:59:50") == 36_719_990
    assert _place(stub, "large_every_second", "2025-01-01T00:00:00") == -1
    assert _place(stub, "large_office_seconds", "2015-01-01T09:00:00") == 0
    assert _place(stub, "large_office_seconds", "2015-01-03T09:00:00") == -1
    assert _place(stub, "large_office_seconds", "2015-01-05T08:59:59") == -1
    assert _place(stub, "large_office_seconds", "2015-01-05T09:00:01") == 2 * 32_400 + 1


def test_a_search_of_a_large_grid_reads_no_key(stub):
    leap_days = _page(stub, "large_every_second", query="02-29T23:59:5", limit=3)
    every_key = _page(
        stub, "large_every_second", query="0", offset=315_619_190, limit=2
    )
    no_key = _page(stub, "large_every_second", query="zz", limit=10)
    office = _page(stub, "large_office_seconds", query="-06T12:00:0", limit=100)

    assert leap_days.total == 30
    assert list(zip(leap_days.indices, leap_days.keys)) == [
        (36_719_990, "2016-02-29T23:59:50"),
        (36_719_991, "2016-02-29T23:59:51"),
        (36_719_992, "2016-02-29T23:59:52"),
    ]
    assert every_key.total == 315_619_200
    assert list(every_key.keys) == ["2024-12-31T23:59:50", "2024-12-31T23:59:51"]
    assert (no_key.total, list(no_key.keys)) == (0, [])
    # The sixth day of a month is a weekday in 87 of the 120 months.
    assert office.total == 87 * 10
    assert office.keys[0] == "2015-01-06T12:00:00"


def test_the_groups_of_a_large_grid_are_split_with_no_walk_of_the_grid(stub):
    service, pb2 = stub

    def groups(asset_key, **fields):
        return service.GetPartitionGroups(
            pb2.GetPartitionGroupsRequest(asset_key=asset_key, **fields),
            timeout=_SECONDS_MOST,
        )

    years = groups("large_office_seconds", max_groups=31)
    of_a_day = groups(
        "large_office_seconds",
        first_key="2024-12-31T09:00:00",
        last_key="2024-12-31T17:59:59",
    )

    assert years.total_keys == 84_531_600
    assert [group.label for group in years.groups] == [
        str(year) for year in range(2015, 2025)
    ]
    assert years.unit == "year"
    assert sum(group.keys for group in years.groups) == years.total_keys
    assert [group.label for group in of_a_day.groups] == [
        f"2024-12-31T{hour:02d}" for hour in range(9, 18)
    ]
    assert all(group.keys == 3_600 for group in of_a_day.groups)


def test_keys_with_no_separator_split_at_the_fields_of_their_format(stub):
    service, pb2 = stub

    def groups(**fields):
        return service.GetPartitionGroups(
            pb2.GetPartitionGroupsRequest(
                asset_key="cron_keys_with_no_separator", **fields
            )
        )

    months = groups(max_groups=31)
    days = groups(first_key="2024020100", last_key="2024022923")
    hours = groups(first_key="2024022900", last_key="2024022923")

    assert [group.label for group in months.groups] == ["202401", "202402"]
    assert (months.unit, months.key_unit) == ("month", "hour")
    assert [group.keys for group in months.groups] == [31 * 24, 29 * 24]
    assert list(months.cuts) == [4, 6, 8, 10]

    assert [group.label for group in days.groups] == [
        f"202402{day:02d}" for day in range(1, 30)
    ]
    assert days.unit == "day"
    assert hours.of_keys
    assert [group.label for group in hours.groups][:2] == ["2024022900", "2024022901"]


def test_keys_with_a_separator_have_the_cuts_of_their_format(stub):
    service, pb2 = stub

    hours = service.GetPartitionGroups(
        pb2.GetPartitionGroupsRequest(asset_key="cron_every_hour", max_groups=31)
    )
    names = service.GetPartitionGroups(
        pb2.GetPartitionGroupsRequest(asset_key="static", max_groups=31)
    )

    assert list(hours.cuts) == [4, 7, 10, 13, 16, 19]
    assert [group.label for group in hours.groups] == ["2024-01", "2024-02"]
    assert list(names.cuts) == []
