"""Demonstrates LastRunIncludesTarget in eager().

eager() now uses `any_deps_match(newly_updated & ~last_run_includes_target)`,
which suppresses redundant fires after joint runs while correctly firing
for solo dep updates.

Three tests:
1. eager() + joint run → B does NOT fire redundantly
2. eager() + solo dep update → B fires correctly
3. Naive condition (without filter) + joint run → fires redundantly (contrast)
"""

import time

import rivers as rs
from rivers._core import AutomationDaemon


def _count_successful_runs_for(storage, asset_name):
    """Count completed (Success) runs that include the given asset."""
    runs = storage.get_runs(limit=100, status="Success")
    return sum(1 for r in runs if asset_name in r.node_names)


def _wait_for_n_runs(storage, asset_name, n, timeout=15.0):
    """Poll until there are at least n successful runs for the asset."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _count_successful_runs_for(storage, asset_name) >= n:
            return True
        time.sleep(0.3)
    return _count_successful_runs_for(storage, asset_name) >= n


class TestLastRunIncludesTarget:
    def test_eager_no_redundant_fire_after_joint_run(self, storage):
        """eager() does NOT fire redundantly after a joint run.

        eager() includes ~last_run_includes_target, so when A and B are
        materialized together, A's run includes B → filtered → no extra fire.
        """

        @rs.Asset(name="a", io_handler=rs.InMemoryIOHandler())
        def a() -> int:
            return 1

        @rs.Asset(
            name="b",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.eager(),
        )
        def b(a: int) -> int:
            return a + 10

        repo = rs.CodeRepository(
            assets=[a, b],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        repo.materialize()
        assert _count_successful_runs_for(storage, "b") == 1

        daemon = AutomationDaemon(
            repo=repo,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            time.sleep(2)

            repo.materialize(selection=["a", "b"])
            assert _count_successful_runs_for(storage, "b") == 2

            time.sleep(5)
            count = _count_successful_runs_for(storage, "b")
            assert count == 2, (
                f"eager() should NOT fire a redundant run after joint "
                f"[a,b] run, but B has {count} runs (expected 2)"
            )
        finally:
            daemon.stop()

    def test_eager_fires_after_solo_dep_update(self, storage):
        """eager() fires correctly when only the dep is updated."""

        @rs.Asset(name="a", io_handler=rs.InMemoryIOHandler())
        def a() -> int:
            return 1

        @rs.Asset(
            name="b",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=rs.AutomationCondition.eager(),
        )
        def b(a: int) -> int:
            return a + 10

        repo = rs.CodeRepository(
            assets=[a, b],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        repo.materialize()
        assert _count_successful_runs_for(storage, "b") == 1

        daemon = AutomationDaemon(
            repo=repo,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            time.sleep(3)

            repo.materialize(selection=["a"])
            assert _count_successful_runs_for(storage, "b") == 1

            got_fire = _wait_for_n_runs(storage, "b", 2, timeout=15)
            assert got_fire, (
                f"eager() should fire B when dep A is updated by solo run, "
                f"but B only has {_count_successful_runs_for(storage, 'b')} runs"
            )
        finally:
            daemon.stop()

    def test_naive_condition_self_suppresses_after_joint_run(self, storage):
        """Even without ~last_run_includes_target, a joint run must not re-fire.

        Dep-updated compares staleness against the root's own record: after a
        joint run the dep is no newer than the root, so the naive
        any_deps_match(newly_updated) form is self-suppressing too (the
        filter remains for explicitness). This used to fire a redundant
        third run under fire-time baselines.
        """

        @rs.Asset(name="a", io_handler=rs.InMemoryIOHandler())
        def a() -> int:
            return 1

        # Naive: any_deps_match(newly_updated) WITHOUT ~last_run_includes_target
        naive_eager = (
            (
                rs.AutomationCondition.missing().newly_true()
                | rs.AutomationCondition.any_deps_match(
                    rs.AutomationCondition.newly_updated()
                )
            ).since_last_handled()
            & ~rs.AutomationCondition.any_deps_missing()
            & ~rs.AutomationCondition.any_deps_in_progress()
            & ~rs.AutomationCondition.in_progress()
        )

        @rs.Asset(
            name="b",
            io_handler=rs.InMemoryIOHandler(),
            automation_condition=naive_eager,
        )
        def b(a: int) -> int:
            return a + 10

        repo = rs.CodeRepository(
            assets=[a, b],
            default_executor=rs.Executor.in_process(),
        )
        repo.resolve(storage=storage)

        repo.materialize()
        assert _count_successful_runs_for(storage, "b") == 1

        daemon = AutomationDaemon(
            repo=repo,
            storage=storage,
            condition_eval_interval="1s",
        )
        daemon.start()
        try:
            time.sleep(2)

            repo.materialize(selection=["a", "b"])
            assert _count_successful_runs_for(storage, "b") == 2

            got_extra = _wait_for_n_runs(storage, "b", 3, timeout=5)
            assert not got_extra, (
                f"a joint run must not re-trigger B (dep is not newer than "
                f"the root), but B has "
                f"{_count_successful_runs_for(storage, 'b')} runs"
            )
        finally:
            daemon.stop()
