"""Per-tick partition universe refresh for automation conditions.

The condition engine's view of an asset's partitions must track reality:
dynamic keys registered while the daemon runs enter the universe on the
next tick, and retired keys leave it. A universe frozen at daemon start
silently kills automation for dynamic-partitioned assets.
"""

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
