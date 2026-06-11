"""Dep-based conditions on partitioned assets with the default Identity mapping.

An unmapped partitioned->partitioned edge defaults to Identity at resolve time.
The condition engine must honor that per-partition: an upstream materialization
of one key may only make the matching downstream key eligible — never the whole
universe.
"""

import time

import rivers as rs
from rivers._core import AutomationDaemon


def test_implicit_identity_dep_fires_only_matching_partition(storage):
    """``any_deps_updated`` with no explicit mapping targets the updated key only."""
    pd = rs.PartitionsDefinition.static_(["a", "b", "c"])

    @rs.Asset(name="up", io_handler=rs.InMemoryIOHandler(), partitions_def=pd)
    def up() -> str:
        return "u"

    @rs.Asset(
        name="down",
        io_handler=rs.InMemoryIOHandler(),
        partitions_def=pd,
        automation_condition=rs.AutomationCondition.any_deps_updated(),
    )
    def down(up: str) -> str:
        return up + "!"

    repo = rs.CodeRepository(
        assets=[up, down],
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
        repo.materialize(selection=["up"], partition_key=rs.PartitionKey.single("a"))

        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            if storage.get_latest_materialization("down", "a") is not None:
                break
            time.sleep(0.2)
        assert storage.get_latest_materialization("down", "a") is not None, (
            "the condition never fired for the updated upstream key"
        )

        # The broadcast bug dispatches 'b'/'c' in the same classify pass that
        # dispatched 'a' (their runs then fail on the missing upstream dep),
        # so a short settle window over run records is enough to catch it.
        forbidden = {rs.PartitionKey.single("b"), rs.PartitionKey.single("c")}
        settle = time.monotonic() + 3
        while time.monotonic() < settle:
            stray = [
                r.partition_key
                for r in storage.get_runs(limit=100)
                if r.partition_key in forbidden
            ]
            assert not stray, (
                f"condition dispatched runs for {stray} whose upstream was never materialized"
            )
            time.sleep(0.25)
    finally:
        daemon.stop()
