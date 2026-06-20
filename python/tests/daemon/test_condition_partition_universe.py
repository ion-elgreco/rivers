"""Per-tick partition universe refresh for automation conditions.

The condition engine's view of an asset's partitions must track reality:
dynamic keys registered while the daemon runs enter the universe on the
next tick, and retired keys leave it. A universe frozen at daemon start
silently kills automation for dynamic-partitioned assets.
"""

import datetime as _dt

import rivers as rs
from _polling import wait_for_runs as _wait_for_runs
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
