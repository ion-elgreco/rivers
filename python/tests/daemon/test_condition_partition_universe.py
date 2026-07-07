"""Per-tick partition universe refresh for automation conditions.

The condition engine's view of an asset's partitions must track reality:
dynamic keys registered while the daemon runs enter the universe on the
next tick, and retired keys leave it. A universe frozen at daemon start
silently kills automation for dynamic-partitioned assets.
"""

import datetime as _dt

import rivers as rs
from _polling import wait_for_runs as _wait_for_runs
from _polling import wait_until as _wait_until
from rivers._core import AutomationDaemon


def test_condition_fires_for_dynamic_partition_added_after_start(storage):
    """``on_missing`` must fire for a dynamic key registered after daemon start."""

    @rs.Asset(
        name="colored",
        io_handler=rs.InMemoryIOHandler(),
        partitions_def=rs.PartitionsDefinition.dynamic("colors"),
        automation_condition=rs.AutomationCondition.on_missing(),
    )
    def colored() -> str:
        return "data"

    repo = rs.CodeRepository(
        assets=[colored],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)

    # 'blue' exists before the daemon starts; its materialization proves the
    # eval loop is ticking before 'red' is registered.
    storage.add_dynamic_partitions("colors", ["blue"])

    daemon = AutomationDaemon(
        repo=repo,
        storage=storage,
        condition_eval_interval="500ms",
    )
    daemon.start()
    try:
        _wait_for_runs(storage, min_count=1, timeout=25, status="Success")
        assert storage.get_latest_materialization("colored", "blue") is not None

        # The running daemon has never seen this key — only the per-tick
        # universe refresh can pick it up.
        storage.add_dynamic_partitions("colors", ["red"])
        runs = _wait_for_runs(storage, min_count=2, timeout=25, status="Success")
        assert len(runs) >= 2, "condition never fired for the registered dynamic key"
        ev = storage.get_latest_materialization("colored", "red")
        assert ev is not None, "the dynamic key 'red' was never materialized"
    finally:
        daemon.stop()


def test_condition_does_not_materialize_future_time_windows(storage):
    """A time-window partition with an explicit FUTURE end must not enumerate
    not-yet-arrived windows as materializable.

    ``on_missing`` fires only for windows up to now; future windows enter via the
    per-tick universe refresh as wall-clock advances, not as an initial backfill
    of the whole future range (Dagster bounds the active set at current_time).
    """
    today = _dt.date.today()
    start = _dt.datetime.combine(today - _dt.timedelta(days=2), _dt.time.min)
    end = _dt.datetime.combine(today + _dt.timedelta(days=10), _dt.time.min)
    past_key = (today - _dt.timedelta(days=1)).strftime("%Y-%m-%d")
    future_key = (today + _dt.timedelta(days=5)).strftime("%Y-%m-%d")

    @rs.Asset(
        name="daily_fut",
        io_handler=rs.InMemoryIOHandler(),
        partitions_def=rs.PartitionsDefinition.daily(start=start, end=end),
        automation_condition=rs.AutomationCondition.on_missing(),
    )
    def daily_fut() -> str:
        return "data"

    repo = rs.CodeRepository(
        assets=[daily_fut],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)

    daemon = AutomationDaemon(
        repo=repo,
        storage=storage,
        condition_eval_interval="500ms",
    )
    daemon.start()
    try:
        # Wait for the past windows to materialize, then let several more ticks
        # pass so any (buggy) future materializations would have landed.
        _wait_for_runs(storage, min_count=1, timeout=25, status="Success")
        _wait_for_runs(storage, min_count=999, timeout=8, status="Success")

        assert storage.get_latest_materialization("daily_fut", past_key) is not None, (
            "a past window must materialize — the daemon must be ticking"
        )
        assert storage.get_latest_materialization("daily_fut", future_key) is None, (
            f"future window {future_key} must not be materialized; the automation "
            "universe enumerated windows past now"
        )
    finally:
        daemon.stop()


def test_multi_dimension_change_does_not_flood_future_time_windows(storage):
    """Multi [dynamic color x daily(end in the future)] with ``on_missing``:
    registering a new color rebuilds the cartesian universe from the dimension
    key sets. Those sets must stay capped at now — seeded uncapped, the rebuild
    floods every future window back in and the daemon backfills the whole
    future range (the single-dim bug above resurfacing through Multi seeding).
    """
    today = _dt.date.today()
    start = _dt.datetime.combine(today - _dt.timedelta(days=2), _dt.time.min)
    end = _dt.datetime.combine(today + _dt.timedelta(days=10), _dt.time.min)
    past_key = (today - _dt.timedelta(days=1)).strftime("%Y-%m-%d")
    future_key = (today + _dt.timedelta(days=5)).strftime("%Y-%m-%d")

    @rs.Asset(
        name="colored_daily",
        io_handler=rs.InMemoryIOHandler(),
        partitions_def=rs.PartitionsDefinition.multi(
            {
                "color": rs.PartitionsDefinition.dynamic("mcolors"),
                "date": rs.PartitionsDefinition.daily(start=start, end=end),
            }
        ),
        automation_condition=rs.AutomationCondition.on_missing(),
    )
    def colored_daily() -> str:
        return "data"

    repo = rs.CodeRepository(
        assets=[colored_daily],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    storage.add_dynamic_partitions("mcolors", ["blue"])

    daemon = AutomationDaemon(
        repo=repo,
        storage=storage,
        condition_eval_interval="500ms",
    )
    daemon.start()
    try:
        _wait_for_runs(storage, min_count=1, timeout=25, status="Success")

        # The running daemon picks 'red' up via the per-tick dimension refresh,
        # which rebuilds the cartesian universe.
        storage.add_dynamic_partitions("mcolors", ["red"])
        assert _wait_until(
            lambda: storage.get_latest_materialization(
                "colored_daily", f"color=red|date={past_key}"
            )
            is not None,
            timeout=25,
        ), "red x past window never materialized — the cartesian rebuild never ran"

        # Let several more ticks pass so any (buggy) future materializations land.
        _wait_for_runs(storage, min_count=999, timeout=6, status="Success")
        for color in ("blue", "red"):
            assert (
                storage.get_latest_materialization(
                    "colored_daily", f"color={color}|date={future_key}"
                )
                is None
            ), (
                f"future window color={color}|date={future_key} must not be "
                "materialized; the dimension rebuild flooded windows past now in"
            )
    finally:
        daemon.stop()


def test_future_end_upstream_does_not_keep_any_deps_missing_true(storage):
    """An unconditioned upstream declared with an explicit FUTURE end feeds a
    partitioned downstream via an all_partitions mapping. The upstream pivot
    universe must be capped at now: seeded uncapped, its future windows can
    never materialize, ``any_deps_missing()`` stays true forever, and the
    daemon re-dispatches the downstream on every tick.
    """
    today = _dt.date.today()
    start = _dt.datetime.combine(today - _dt.timedelta(days=2), _dt.time.min)
    end = _dt.datetime.combine(today + _dt.timedelta(days=10), _dt.time.min)

    @rs.Asset(
        name="up_fut",
        io_handler=rs.InMemoryIOHandler(),
        partitions_def=rs.PartitionsDefinition.daily(start=start, end=end),
    )
    def up_fut() -> str:
        return "data"

    @rs.Asset(
        name="down_watcher",
        io_handler=rs.InMemoryIOHandler(),
        partitions_def=rs.PartitionsDefinition.static_(["x"]),
        deps=[
            rs.AssetDef.input(
                "up_fut",
                partition_mapping=rs.PartitionMapping.all_partitions(),
            ),
        ],
        automation_condition=rs.AutomationCondition.any_deps_missing(),
    )
    def down_watcher(up_fut: str) -> str:
        return "d"

    # Unpartitioned canary: proves the eval loop is ticking while we assert
    # the watcher stays quiet.
    @rs.Asset(
        name="canary_missing",
        io_handler=rs.InMemoryIOHandler(),
        automation_condition=rs.AutomationCondition.on_missing(),
    )
    def canary_missing() -> str:
        return "c"

    repo = rs.CodeRepository(
        assets=[up_fut, down_watcher, canary_missing],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)

    # Materialize every window up to now, plus tomorrow, so a capped pivot
    # universe has nothing missing and the watcher stays quiet. Tomorrow is
    # included so that if wall-clock crosses midnight during the run, the new
    # current-day window the universe refresh adds is already materialized —
    # otherwise the watcher would legitimately fire and flake the test.
    for day in (2, 1, 0, -1):
        key = (today - _dt.timedelta(days=day)).strftime("%Y-%m-%d")
        result = repo.materialize(
            ["up_fut"], partition_key=rs.PartitionKey.single(key)
        )
        assert result.success

    daemon = AutomationDaemon(
        repo=repo,
        storage=storage,
        condition_eval_interval="500ms",
    )
    daemon.start()
    try:
        assert _wait_until(
            lambda: storage.get_latest_materialization("canary_missing") is not None,
            timeout=25,
        ), "the canary never materialized — the eval loop is not ticking"

        # Let several more ticks pass; a buggy uncapped pivot universe fires
        # the watcher on every one of them.
        _wait_for_runs(storage, min_count=999, timeout=5)
        watcher_runs = [
            r for r in storage.get_runs(limit=100) if "down_watcher" in r.node_names
        ]
        assert not watcher_runs, (
            "any_deps_missing() fired although every existing upstream window "
            "is materialized — the pivot universe enumerated windows past now"
        )
    finally:
        daemon.stop()
