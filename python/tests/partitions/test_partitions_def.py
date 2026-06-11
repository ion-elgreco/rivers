import datetime

import pytest

import rivers as rs
from rivers.exceptions import PartitionDefinitionError


# ---------------------------------------------------------------------------
# Static
# ---------------------------------------------------------------------------


def test_static():
    """StaticPartitionsDefinition creates and enumerates keys."""
    pd = rs.PartitionsDefinition.static_(["us-east", "us-west", "eu"])
    assert isinstance(pd, rs.PartitionsDefinition.Static)
    keys = pd.get_partition_keys()
    assert len(keys) == 3
    assert all(isinstance(k, rs.PartitionKey.Single) for k in keys)
    key_values = [k.key for k in keys if isinstance(k, rs.PartitionKey.Single)]
    assert key_values == [["us-east"], ["us-west"], ["eu"]]


def test_static_validate():
    """StaticPartitionsDefinition validates keys."""
    pd = rs.PartitionsDefinition.static_(["a", "b", "c"])
    assert pd.validate_partition_key(rs.PartitionKey.single("a")) is True
    assert pd.validate_partition_key(rs.PartitionKey.single("d")) is False
    # Multi key is not valid for Static
    assert pd.validate_partition_key(rs.PartitionKey.multi({"x": "a"})) is False


def test_static_empty_rejected():
    """Static partitions must have at least one key."""
    with pytest.raises(PartitionDefinitionError):
        rs.PartitionsDefinition.static_([])


def test_static_repr_eq():
    """Static partitions repr and equality."""
    p1 = rs.PartitionsDefinition.static_(["a", "b"])
    p2 = rs.PartitionsDefinition.static_(["a", "b"])
    p3 = rs.PartitionsDefinition.static_(["a", "c"])
    assert p1 == p2
    assert p1 != p3
    assert repr(p1) == 'PartitionsDefinition.static_(["a", "b"])'


# ---------------------------------------------------------------------------
# TimeWindow
# ---------------------------------------------------------------------------


def test_daily():
    """Daily partitions enumerate date keys."""
    start = datetime.datetime(2024, 1, 1)
    end = datetime.datetime(2024, 1, 5)
    pd = rs.PartitionsDefinition.daily(start, end=end)
    assert isinstance(pd, rs.PartitionsDefinition.TimeWindow)
    keys = pd.get_partition_keys()
    assert len(keys) == 4  # Jan 1, 2, 3, 4 (end is exclusive)
    assert isinstance(keys[0], rs.PartitionKey.Single)
    assert keys[0].key == ["2024-01-01"]
    assert isinstance(keys[-1], rs.PartitionKey.Single)
    assert keys[-1].key == ["2024-01-04"]


def test_hourly():
    """Hourly partitions enumerate hourly keys."""
    start = datetime.datetime(2024, 1, 1, 0, 0)
    end = datetime.datetime(2024, 1, 1, 4, 0)
    pd = rs.PartitionsDefinition.hourly(start, end=end)
    keys = pd.get_partition_keys()
    assert len(keys) == 4
    assert isinstance(keys[0], rs.PartitionKey.Single)
    assert keys[0].key == ["2024-01-01T00:00"]
    assert isinstance(keys[-1], rs.PartitionKey.Single)
    assert keys[-1].key == ["2024-01-01T03:00"]


def test_time_window_interval():
    """TimeWindow with second-grained interval enumerates per-second keys."""
    start = datetime.datetime(2024, 1, 1, 0, 0, 0)
    end = datetime.datetime(2024, 1, 1, 0, 0, 3)
    pd = rs.PartitionsDefinition.time_window(start, interval_seconds=1.0, end=end)
    keys = pd.get_partition_keys()
    assert len(keys) == 3


def test_time_window_subsecond_interval():
    """`interval_seconds` is f64 (ns-precision); a 500ms interval should
    enumerate `[t, t+500ms, t+1s, t+1.5s]` with millisecond-precision keys.
    Default fmt truncates fractional seconds, so the test passes a custom
    `%.3f`-suffixed fmt to expose the full key string."""
    start = datetime.datetime(2024, 1, 1, 0, 0, 0)
    end = datetime.datetime(2024, 1, 1, 0, 0, 2)
    pd = rs.PartitionsDefinition.time_window(
        start,
        interval_seconds=0.5,
        end=end,
        fmt="%Y-%m-%dT%H:%M:%S%.3f",
    )
    keys = pd.get_partition_keys()
    assert [k.key for k in keys if isinstance(k, rs.PartitionKey.Single)] == [
        ["2024-01-01T00:00:00.000"],
        ["2024-01-01T00:00:00.500"],
        ["2024-01-01T00:00:01.000"],
        ["2024-01-01T00:00:01.500"],
    ]


def test_time_window_six_field_cron():
    """The Rust parser uses `with_seconds_optional()`, so a 6-field cron like
    `*/5 * * * * *` should enumerate keys spaced 5 seconds apart. Regression
    coverage: prior to wiring the seconds-optional parser through the UI
    server fns, only the daemon's parsing path was exercised end-to-end."""
    start = datetime.datetime(2024, 1, 1, 0, 0, 0)
    end = datetime.datetime(2024, 1, 1, 0, 0, 25)
    pd = rs.PartitionsDefinition.time_window(
        start,
        cron_schedule="*/5 * * * * *",
        end=end,
    )
    keys = pd.get_partition_keys()
    assert [k.key for k in keys if isinstance(k, rs.PartitionKey.Single)] == [
        ["2024-01-01T00:00:00"],
        ["2024-01-01T00:00:05"],
        ["2024-01-01T00:00:10"],
        ["2024-01-01T00:00:15"],
        ["2024-01-01T00:00:20"],
    ]


def test_time_window_fmt_must_roundtrip_grid():
    """A fmt coarser than the grid collapses distinct windows into one key
    string — enumerate yields duplicates and backfills double-dispatch."""
    # Hourly cron with a date-only fmt: 24 windows share each key.
    with pytest.raises(
        PartitionDefinitionError, match="cannot represent the partition grid"
    ):
        rs.PartitionsDefinition.time_window(
            datetime.datetime(2024, 1, 1),
            cron_schedule="0 * * * *",
            fmt="%Y-%m-%d",
        )
    # Sub-second interval with the second-grained default fmt.
    with pytest.raises(
        PartitionDefinitionError, match="cannot represent the partition grid"
    ):
        rs.PartitionsDefinition.time_window(
            datetime.datetime(2024, 1, 1), interval_seconds=0.5
        )


def test_daily_fmt_override_must_roundtrip():
    """daily(fmt='%Y-%m') collapses ~30 windows per key."""
    with pytest.raises(
        PartitionDefinitionError, match="cannot represent the partition grid"
    ):
        rs.PartitionsDefinition.daily(datetime.datetime(2024, 1, 1), fmt="%Y-%m")


def test_coarse_fmt_on_equally_coarse_grid_allowed():
    """A coarse fmt is fine when the grid is equally coarse."""
    pd = rs.PartitionsDefinition.time_window(
        datetime.datetime(2024, 1, 1),
        cron_schedule="0 0 1 * *",
        fmt="%Y-%m",
        end=datetime.datetime(2024, 4, 1),
    )
    keys = [
        k.key for k in pd.get_partition_keys() if isinstance(k, rs.PartitionKey.Single)
    ]
    assert keys == [["2024-01"], ["2024-02"], ["2024-03"]]


def test_time_window_requires_schedule_or_interval():
    """TimeWindow requires either cron_schedule or interval_seconds."""
    with pytest.raises(PartitionDefinitionError):
        rs.PartitionsDefinition.time_window(datetime.datetime(2024, 1, 1))
    with pytest.raises(PartitionDefinitionError):
        rs.PartitionsDefinition.time_window(
            datetime.datetime(2024, 1, 1),
            cron_schedule="0 0 * * *",
            interval_seconds=60.0,
        )


def test_daily_validate():
    """Daily partitions validate keys."""
    start = datetime.datetime(2024, 1, 1)
    end = datetime.datetime(2024, 1, 5)
    pd = rs.PartitionsDefinition.daily(start, end=end)
    assert pd.validate_partition_key(rs.PartitionKey.single("2024-01-01")) is True
    assert pd.validate_partition_key(rs.PartitionKey.single("2024-01-10")) is False


# ---------------------------------------------------------------------------
# Multi
# ---------------------------------------------------------------------------


def test_multi():
    """Multi-dimensional partitions with cartesian product."""
    region = rs.PartitionsDefinition.static_(["us", "eu"])
    env = rs.PartitionsDefinition.static_(["prod", "staging"])
    pd = rs.PartitionsDefinition.multi({"region": region, "env": env})
    assert isinstance(pd, rs.PartitionsDefinition.Multi)
    keys = pd.get_partition_keys()
    assert len(keys) == 4  # 2 * 2
    assert all(isinstance(k, rs.PartitionKey.Multi) for k in keys)
    # Check that all combinations exist
    dim_combos = [k.keys for k in keys if isinstance(k, rs.PartitionKey.Multi)]
    assert {"region": ["us"], "env": ["prod"]} in dim_combos
    assert {"region": ["eu"], "env": ["staging"]} in dim_combos


def test_multi_validate():
    """Multi partitions validate multi-dimensional keys."""
    region = rs.PartitionsDefinition.static_(["us", "eu"])
    env = rs.PartitionsDefinition.static_(["prod"])
    pd = rs.PartitionsDefinition.multi({"region": region, "env": env})
    assert (
        pd.validate_partition_key(
            rs.PartitionKey.multi({"region": "us", "env": "prod"})
        )
        is True
    )
    assert (
        pd.validate_partition_key(
            rs.PartitionKey.multi({"region": "us", "env": "staging"})
        )
        is False
    )
    # Single key not valid for Multi
    assert pd.validate_partition_key(rs.PartitionKey.single("us")) is False


def test_multi_nested_rejected():
    """Multi cannot contain nested Multi dimensions."""
    inner = rs.PartitionsDefinition.multi({"a": rs.PartitionsDefinition.static_(["x"])})
    with pytest.raises(PartitionDefinitionError):
        rs.PartitionsDefinition.multi({"outer": inner})


# ---------------------------------------------------------------------------
# Dynamic
# ---------------------------------------------------------------------------


def test_dynamic_construction():
    """Dynamic partitions can be created with a name."""
    dyn = rs.PartitionsDefinition.dynamic("my_set")
    assert isinstance(dyn, rs.PartitionsDefinition.Dynamic)


# ---------------------------------------------------------------------------
# Empty keys never validate — empty vectors otherwise pass via vacuous
# iteration (`all()` / a never-entered loop) on every kind.
# ---------------------------------------------------------------------------


def test_empty_single_key_invalid_for_static():
    pd = rs.PartitionsDefinition.static_(["a", "b"])
    pk = rs.PartitionKey.from_json('{"single":[]}')
    assert pd.validate_partition_key(pk) is False


def test_empty_single_key_invalid_for_timewindow():
    pd = rs.PartitionsDefinition.daily(
        start=datetime.datetime(2024, 1, 1), end=datetime.datetime(2024, 1, 5)
    )
    pk = rs.PartitionKey.from_json('{"single":[]}')
    assert pd.validate_partition_key(pk) is False


def test_empty_single_key_invalid_for_dynamic():
    pd = rs.PartitionsDefinition.dynamic("users")
    pk = rs.PartitionKey.from_json('{"single":[]}')
    assert pd.validate_partition_key(pk) is False


def test_empty_dim_values_invalid_for_multi():
    pd = rs.PartitionsDefinition.multi(
        {"region": rs.PartitionsDefinition.static_(["us"])}
    )
    pk = rs.PartitionKey.from_json('{"multi":{"region":[]}}')
    assert pd.validate_partition_key(pk) is False


def test_multi_key_with_extra_dimension_invalid():
    """A key carrying a dimension the def doesn't declare must not validate —
    the def-dims loop can't see extra dims."""
    pd = rs.PartitionsDefinition.multi(
        {"region": rs.PartitionsDefinition.static_(["us"])}
    )
    key = rs.PartitionKey.multi({"region": "us", "bogus": "x"})
    assert pd.validate_partition_key(key) is False


def test_empty_set_key_invalid():
    pd = rs.PartitionsDefinition.static_(["a"])
    pk = rs.PartitionKey.from_json('{"set":[]}')
    assert pd.validate_partition_key(pk) is False


# ---------------------------------------------------------------------------
# '|' separates dimensions and ',' separates values in the canonical display
# form (`dim=v|dim=v`); keys containing them cannot round-trip through
# partition-string lookups, so they are rejected at construction.
# ---------------------------------------------------------------------------


def test_static_key_with_reserved_char_rejected():
    for key in ("us|eu", "a,b"):
        with pytest.raises(PartitionDefinitionError, match="reserved character"):
            rs.PartitionsDefinition.static_([key])


def test_static_rejects_empty_string_key():
    """Empty key strings render as nothing in the display form and are
    unreachable via string lookups — same guard as the dynamic write path."""
    for keys in ([""], ["a", ""]):
        with pytest.raises(PartitionDefinitionError, match="must not be empty"):
            rs.PartitionsDefinition.static_(keys)


def test_multi_dim_name_with_reserved_char_rejected():
    """Dim names also sit left of '=' in `dim=value` segments, so '=' is
    reserved for them as well."""
    inner = rs.PartitionsDefinition.static_(["x"])
    for name in ("a|b", "a,b", "a=b"):
        with pytest.raises(PartitionDefinitionError, match="reserved character"):
            rs.PartitionsDefinition.multi({name: inner})


def test_multi_empty_dim_name_rejected():
    inner = rs.PartitionsDefinition.static_(["x"])
    with pytest.raises(PartitionDefinitionError, match="cannot be empty"):
        rs.PartitionsDefinition.multi({"": inner})


def test_time_window_fmt_with_reserved_char_rejected():
    """A fmt whose rendered keys contain '|' breaks display parsing."""
    with pytest.raises(PartitionDefinitionError, match="reserved character"):
        rs.PartitionsDefinition.time_window(
            start=datetime.datetime(2024, 1, 1),
            interval_seconds=3600.0,
            fmt="%Y|%m-%dT%H:%M:%S",
        )


# ---------------------------------------------------------------------------
# Non-positive / sub-nanosecond intervals are rejected at construction —
# interval_ns == 0 would otherwise panic on `n % 0` during key validation.
# ---------------------------------------------------------------------------


def test_time_window_zero_interval_rejected():
    with pytest.raises(PartitionDefinitionError, match="interval_seconds"):
        rs.PartitionsDefinition.time_window(
            start=datetime.datetime(2024, 1, 1), interval_seconds=0.0
        )


def test_time_window_negative_interval_rejected():
    with pytest.raises(PartitionDefinitionError, match="interval_seconds"):
        rs.PartitionsDefinition.time_window(
            start=datetime.datetime(2024, 1, 1), interval_seconds=-3600.0
        )


def test_time_window_sub_nanosecond_interval_rejected():
    """1e-12 seconds is positive but truncates to 0 nanoseconds internally."""
    with pytest.raises(PartitionDefinitionError, match="interval_seconds"):
        rs.PartitionsDefinition.time_window(
            start=datetime.datetime(2024, 1, 1), interval_seconds=1e-12
        )


def test_time_window_subsecond_cron_start_rejected():
    """A start with sub-second precision puts every cron tick off the whole-
    second grid; a second-grained fmt cannot round-trip those keys. The fmt
    check walks the same grid enumerate uses, so this fails at construction
    instead of minting keys that fail their own validation."""
    with pytest.raises(
        PartitionDefinitionError, match="cannot represent the partition grid"
    ):
        rs.PartitionsDefinition.time_window(
            start=datetime.datetime(2024, 1, 1, microsecond=500),
            cron_schedule="0 * * * *",
        )
