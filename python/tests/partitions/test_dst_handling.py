"""Time-window keys are a pure wall-clock grid — DST plays no part.

Definitions take tz-naive datetimes and keys are wall-clock labels: the cron
grid is enumerated on the naive timeline, so DST transitions in the host
timezone neither drop keys (spring-forward gap) nor duplicate them
(fall-back hour), and enumeration round-trips through validation on any
host. (Resolving keys against a real timezone — gaps, ambiguous hours —
would require an explicit `timezone` parameter on the definition; today the
grid is timezone-independent by construction.)
"""

import datetime

import rivers as rs


def test_hourly_grid_on_fall_back_day_round_trips():
    """2024-11-03 (US fall-back): one key per wall-clock hour, no duplicate
    01:00, and every enumerated key validates."""
    pd = rs.PartitionsDefinition.hourly(
        start=datetime.datetime(2024, 11, 3, 0, 0),
        end=datetime.datetime(2024, 11, 3, 6, 0),
    )
    keys = [k.key[0] for k in pd.get_partition_keys()]
    assert keys == [
        "2024-11-03T00:00",
        "2024-11-03T01:00",
        "2024-11-03T02:00",
        "2024-11-03T03:00",
        "2024-11-03T04:00",
        "2024-11-03T05:00",
    ]
    for k in pd.get_partition_keys():
        assert pd.validate_partition_key(k) is True, k


def test_hourly_grid_on_spring_forward_day_round_trips():
    """2024-03-10 (US spring-forward): the wall-clock grid still carries
    every hour — 02:00 is a label, not an instant — and round-trips."""
    pd = rs.PartitionsDefinition.hourly(
        start=datetime.datetime(2024, 3, 10, 0, 0),
        end=datetime.datetime(2024, 3, 10, 6, 0),
    )
    keys = [k.key[0] for k in pd.get_partition_keys()]
    assert keys == [
        "2024-03-10T00:00",
        "2024-03-10T01:00",
        "2024-03-10T02:00",
        "2024-03-10T03:00",
        "2024-03-10T04:00",
        "2024-03-10T05:00",
    ]
    for k in pd.get_partition_keys():
        assert pd.validate_partition_key(k) is True, k
    # Off-grid wall times stay invalid.
    assert (
        pd.validate_partition_key(rs.PartitionKey.single("2024-03-10T02:30")) is False
    )


def test_hourly_key_time_window_preserves_hour():
    """The hourly fmt carries no minutes — parsing must not collapse the key
    to midnight."""
    pd = rs.PartitionsDefinition.hourly(
        start=datetime.datetime(2024, 11, 3, 0, 0),
        end=datetime.datetime(2024, 11, 3, 6, 0),
    )
    ctx = rs.PartitionContext(
        keys=[rs.PartitionKey.single("2024-11-03T01:00")], definition=pd
    )
    window = ctx.time_window()
    assert window is not None
    start, end = window
    assert start == datetime.datetime(2024, 11, 3, 1, 0)
    assert end == datetime.datetime(2024, 11, 3, 2, 0)


def test_daily_grid_across_midnight_transition_zone_dates():
    """Daily keys around 2024-09-08 (midnight DST transition in
    America/Santiago) enumerate the full date grid and validate — wall-clock
    labels are host-timezone-independent."""
    pd = rs.PartitionsDefinition.daily(
        start=datetime.datetime(2024, 9, 7),
        end=datetime.datetime(2024, 9, 11),
    )
    keys = [k.key[0] for k in pd.get_partition_keys()]
    assert keys == ["2024-09-07", "2024-09-08", "2024-09-09", "2024-09-10"]
    for k in pd.get_partition_keys():
        assert pd.validate_partition_key(k) is True, k
