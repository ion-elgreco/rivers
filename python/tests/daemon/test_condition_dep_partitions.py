"""Dep-based conditions on partitioned assets.

An unmapped partitioned->partitioned edge defaults to Identity at resolve time.
The condition engine must honor that per-partition: an upstream materialization
of one key may only make the matching downstream key eligible — never the whole
universe. The same containment must hold per-dimension under an explicit
multi-dimensional mapping, where a single fanned-out dimension must not
escalate the whole selection.
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


def test_multi_mapping_all_partitions_dim_stays_within_pinned_dim(storage):
    """A ``Multi`` mapping with one pinned and one fanned-out dimension must
    expand *within* the pinned dimension, never escalate the whole selection.

    ``down`` maps ``up`` with ``{date: identity, region: all_partitions}``: a
    downstream cell depends on its own date across *every* region. An upstream
    update at ``date=D`` may therefore make only the ``date=D`` downstream cells
    (any region) eligible — never a different date.

    Before the fan-out expanded against the downstream universe, a single
    ``all_partitions`` sub-mapping collapsed the whole ``Multi`` mapping to
    ``All``, so an update at ``2024-01-01`` also dispatched every ``2024-01-02``
    cell (their runs then fail on the missing upstream).
    """
    dims = {
        "date": rs.PartitionsDefinition.static_(["2024-01-01", "2024-01-02"]),
        "region": rs.PartitionsDefinition.static_(["us", "eu"]),
    }

    @rs.Asset(
        name="up",
        io_handler=rs.InMemoryIOHandler(),
        partitions_def=rs.PartitionsDefinition.multi(dims),
    )
    def up() -> str:
        return "u"

    @rs.Asset(
        name="down",
        io_handler=rs.InMemoryIOHandler(),
        partitions_def=rs.PartitionsDefinition.multi(dims),
        deps=[
            rs.AssetDef.input(
                "up",
                partition_mapping=rs.PartitionMapping.multi(
                    {
                        "date": rs.PartitionMapping.identity(),
                        "region": rs.PartitionMapping.all_partitions(),
                    }
                ),
            ),
        ],
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
        # Materialize both regions of the pinned date so every downstream cell
        # for that date can load its all_partitions upstream inputs and reach
        # Success — the fan-out crosses regions within a date, never dates.
        for region in ("us", "eu"):
            result = repo.materialize(
                selection=["up"],
                partition_key=rs.PartitionKey.multi(
                    {"date": "2024-01-01", "region": region}
                ),
            )
            assert result.success

        want = {
            rs.PartitionKey.multi({"date": "2024-01-01", "region": "us"}),
            rs.PartitionKey.multi({"date": "2024-01-01", "region": "eu"}),
        }
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            if want <= set(storage.get_materialized_partitions("down")):
                break
            time.sleep(0.2)
        mat = set(storage.get_materialized_partitions("down"))
        assert want <= mat, (
            f"the pinned date's regions never fully fanned out; materialized {mat}"
        )

        # The escalation bug dispatches the other date in the same classify pass
        # that dispatched 2024-01-01 (its runs then fail on the missing
        # upstream), so a short settle window over run records catches it.
        forbidden = {
            rs.PartitionKey.multi({"date": "2024-01-02", "region": "us"}),
            rs.PartitionKey.multi({"date": "2024-01-02", "region": "eu"}),
        }
        settle = time.monotonic() + 3
        while time.monotonic() < settle:
            stray = [
                r.partition_key
                for r in storage.get_runs(limit=200)
                if "down" in r.node_names and r.partition_key in forbidden
            ]
            assert not stray, (
                f"the all_partitions dimension escalated the mapping to All and "
                f"dispatched unrelated dates {stray}"
            )
            time.sleep(0.25)
    finally:
        daemon.stop()
