"""Resolve-time partition compatibility check on user-defined jobs.

A run executes a single partition_key, so a job whose partitioned assets
have disjoint partition definitions is unrunnable end-to-end. The
resolve-time check folds `PartitionsDefinition::intersect` across every
partitioned asset and rejects the job up front instead of letting the
failure surface at every execute click.

The check applies only to user-defined `Job`s. `repo.materialize(selection=...)`
builds an ephemeral job per call and is exempt — callers can pick any
compatible subset.
"""

from datetime import datetime

import pytest

import rivers as rs
from rivers.exceptions import PartitionValidationError


def _resolve(storage, *assets, jobs=()):
    repo = rs.CodeRepository(assets=list(assets), jobs=list(jobs))
    repo.resolve(storage=storage)
    return repo


def _make_partitioned_asset(name, partitions_def):
    """Build a uniquely-named partitioned asset. Closure-shadowing avoids
    rebuilding the decorator inside each test case."""
    handler = rs.InMemoryIOHandler()

    @rs.Asset(io_handler=handler, partitions_def=partitions_def, name=name)
    def _asset():
        return 1

    return _asset


# ── Static ──


def test_static_overlapping_keys_resolves(storage):
    a = _make_partitioned_asset("a", rs.PartitionsDefinition.static_(["x", "y", "z"]))
    b = _make_partitioned_asset("b", rs.PartitionsDefinition.static_(["y", "z", "w"]))
    _resolve(storage, a, b, jobs=[rs.Job(name="j", assets=[a, b])])


def test_static_disjoint_keys_rejected(storage):
    a = _make_partitioned_asset("a", rs.PartitionsDefinition.static_(["x", "y"]))
    b = _make_partitioned_asset("b", rs.PartitionsDefinition.static_(["w", "z"]))
    with pytest.raises(PartitionValidationError) as exc:
        _resolve(storage, a, b, jobs=[rs.Job(name="j", assets=[a, b])])
    msg = str(exc.value)
    assert "Job 'j'" in msg
    assert "disjoint Static keys" in msg
    # Both asset names appear so the user knows which to fix.
    assert "'a'" in msg or '"a"' in msg
    assert "'b'" in msg or '"b"' in msg


# ── Cross-kind ──


def test_static_plus_timewindow_rejected(storage):
    a = _make_partitioned_asset("a", rs.PartitionsDefinition.static_(["x"]))
    b = _make_partitioned_asset(
        "b",
        rs.PartitionsDefinition.daily(
            start=datetime(2024, 1, 1), end=datetime(2024, 1, 5)
        ),
    )
    with pytest.raises(PartitionValidationError) as exc:
        _resolve(storage, a, b, jobs=[rs.Job(name="j", assets=[a, b])])
    assert "different partition kinds (Static vs TimeWindow)" in str(exc.value)


def test_static_plus_dynamic_rejected(storage):
    a = _make_partitioned_asset("a", rs.PartitionsDefinition.static_(["x"]))
    b = _make_partitioned_asset("b", rs.PartitionsDefinition.dynamic("users"))
    with pytest.raises(PartitionValidationError) as exc:
        _resolve(storage, a, b, jobs=[rs.Job(name="j", assets=[a, b])])
    assert "different partition kinds (Static vs Dynamic)" in str(exc.value)


# ── TimeWindow ──


def test_timewindow_overlapping_ranges_resolves(storage):
    a = _make_partitioned_asset(
        "a",
        rs.PartitionsDefinition.daily(
            start=datetime(2024, 1, 1), end=datetime(2024, 1, 10)
        ),
    )
    b = _make_partitioned_asset(
        "b",
        rs.PartitionsDefinition.daily(
            start=datetime(2024, 1, 5), end=datetime(2024, 1, 15)
        ),
    )
    _resolve(storage, a, b, jobs=[rs.Job(name="j", assets=[a, b])])


def test_timewindow_disjoint_ranges_rejected(storage):
    a = _make_partitioned_asset(
        "a",
        rs.PartitionsDefinition.daily(
            start=datetime(2024, 1, 1), end=datetime(2024, 1, 5)
        ),
    )
    b = _make_partitioned_asset(
        "b",
        rs.PartitionsDefinition.daily(
            start=datetime(2024, 2, 1), end=datetime(2024, 2, 5)
        ),
    )
    with pytest.raises(PartitionValidationError) as exc:
        _resolve(storage, a, b, jobs=[rs.Job(name="j", assets=[a, b])])
    assert "TimeWindow date ranges don't overlap" in str(exc.value)


def test_timewindow_cadence_mismatch_rejected(storage):
    """Daily and hourly TimeWindows have the same date range but different
    cadences — every execute would fail because their tick streams don't
    align."""
    a = _make_partitioned_asset(
        "a",
        rs.PartitionsDefinition.daily(
            start=datetime(2024, 1, 1), end=datetime(2024, 1, 2)
        ),
    )
    b = _make_partitioned_asset(
        "b",
        rs.PartitionsDefinition.hourly(
            start=datetime(2024, 1, 1), end=datetime(2024, 1, 2)
        ),
    )
    with pytest.raises(PartitionValidationError) as exc:
        _resolve(storage, a, b, jobs=[rs.Job(name="j", assets=[a, b])])
    msg = str(exc.value)
    assert "TimeWindow cadence mismatch" in msg
    assert "cron='0 0 * * *'" in msg
    assert "cron='0 * * * *'" in msg


# ── Multi ──


def test_multi_compatible_dimensions_resolves(storage):
    pd_a = rs.PartitionsDefinition.multi(
        {
            "color": rs.PartitionsDefinition.static_(["r", "g", "b"]),
            "size": rs.PartitionsDefinition.static_(["s", "m", "l"]),
        }
    )
    pd_b = rs.PartitionsDefinition.multi(
        {
            "color": rs.PartitionsDefinition.static_(["g", "b"]),
            "size": rs.PartitionsDefinition.static_(["m", "l"]),
        }
    )
    a = _make_partitioned_asset("a", pd_a)
    b = _make_partitioned_asset("b", pd_b)
    _resolve(storage, a, b, jobs=[rs.Job(name="j", assets=[a, b])])


def test_multi_different_dimension_names_rejected(storage):
    pd_a = rs.PartitionsDefinition.multi(
        {"color": rs.PartitionsDefinition.static_(["r"])}
    )
    pd_b = rs.PartitionsDefinition.multi(
        {"size": rs.PartitionsDefinition.static_(["s"])}
    )
    a = _make_partitioned_asset("a", pd_a)
    b = _make_partitioned_asset("b", pd_b)
    with pytest.raises(PartitionValidationError) as exc:
        _resolve(storage, a, b, jobs=[rs.Job(name="j", assets=[a, b])])
    assert "Multi dimension name mismatch" in str(exc.value)


def test_multi_per_dimension_disjoint_rejected(storage):
    """Same dimension names but one dimension has no overlap. The error
    surfaces the offending dimension by name plus the recursive cause."""
    pd_a = rs.PartitionsDefinition.multi(
        {
            "color": rs.PartitionsDefinition.static_(["r"]),
            "size": rs.PartitionsDefinition.static_(["s"]),
        }
    )
    pd_b = rs.PartitionsDefinition.multi(
        {
            "color": rs.PartitionsDefinition.static_(["r"]),
            "size": rs.PartitionsDefinition.static_(["m"]),
        }
    )
    a = _make_partitioned_asset("a", pd_a)
    b = _make_partitioned_asset("b", pd_b)
    with pytest.raises(PartitionValidationError) as exc:
        _resolve(storage, a, b, jobs=[rs.Job(name="j", assets=[a, b])])
    msg = str(exc.value)
    assert "Multi dimension 'size'" in msg
    assert "disjoint Static keys" in msg


# ── Dynamic ──


def test_dynamic_same_name_resolves(storage):
    a = _make_partitioned_asset("a", rs.PartitionsDefinition.dynamic("users"))
    b = _make_partitioned_asset("b", rs.PartitionsDefinition.dynamic("users"))
    _resolve(storage, a, b, jobs=[rs.Job(name="j", assets=[a, b])])


def test_dynamic_different_names_rejected(storage):
    a = _make_partitioned_asset("a", rs.PartitionsDefinition.dynamic("users"))
    b = _make_partitioned_asset("b", rs.PartitionsDefinition.dynamic("orders"))
    with pytest.raises(PartitionValidationError) as exc:
        _resolve(storage, a, b, jobs=[rs.Job(name="j", assets=[a, b])])
    msg = str(exc.value)
    assert "Dynamic namespace mismatch" in msg
    assert "'users'" in msg
    assert "'orders'" in msg


# ── Edge cases ──


def test_unpartitioned_assets_dont_constrain(storage):
    """A job with one partitioned asset and any number of unpartitioned
    assets resolves: the unpartitioned ones don't participate in the
    intersection. (Whether they accept a key at materialize time is a
    separate concern — see test_partition_submit_validation.)"""
    a = _make_partitioned_asset("a", rs.PartitionsDefinition.static_(["x", "y"]))

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def plain():
        return 1

    _resolve(storage, a, plain, jobs=[rs.Job(name="j", assets=[a, plain])])


def test_single_partitioned_asset_resolves(storage):
    """One partitioned asset has no intersection partner — nothing to
    check."""
    a = _make_partitioned_asset("a", rs.PartitionsDefinition.static_(["x"]))
    _resolve(storage, a, jobs=[rs.Job(name="j", assets=[a])])


def test_three_assets_fold_intersect(storage):
    """Three assets whose pairwise intersections are non-empty but whose
    common intersection is empty — fold-style failure surfaces on the
    third asset."""
    a = _make_partitioned_asset("a", rs.PartitionsDefinition.static_(["x", "y"]))
    b = _make_partitioned_asset("b", rs.PartitionsDefinition.static_(["y", "z"]))
    c = _make_partitioned_asset("c", rs.PartitionsDefinition.static_(["x", "z"]))
    with pytest.raises(PartitionValidationError) as exc:
        _resolve(storage, a, b, c, jobs=[rs.Job(name="j", assets=[a, b, c])])
    msg = str(exc.value)
    # `a` and `b` intersect on {y}; adding `c` (which has no `y`) fails.
    assert "disjoint Static keys" in msg
    assert '"a"' in msg
    assert '"b"' in msg
    assert "'c'" in msg


def test_no_job_with_mixed_partitions_resolves(storage):
    """Without a user-defined `Job`, partition compatibility isn't enforced —
    a code location with two incompatible partition kinds still resolves so
    that `materialize(selection=...)` for compatible subsets keeps working."""
    a = _make_partitioned_asset("a", rs.PartitionsDefinition.static_(["x"]))
    b = _make_partitioned_asset(
        "b",
        rs.PartitionsDefinition.daily(
            start=datetime(2024, 1, 1), end=datetime(2024, 1, 5)
        ),
    )
    _resolve(storage, a, b)
