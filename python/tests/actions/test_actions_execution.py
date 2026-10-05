"""Asset actions: registration and the run spine.

Actions execute through the existing run machinery — run records carry the
verb, plans never pull in upstream, ActionCompleted events land on the
timeline, and retry policies never leak over from materialize.
"""

import asyncio

import pytest

import rivers as rs
from _helpers import EXECUTORS, IP, event_types
from rivers.exceptions import GraphValidationError


# ---------------------------------------------------------------------------
# Execution across executors and definition forms
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("executor", EXECUTORS)
@pytest.mark.parametrize("form", ["decorator", "class"])
def test_run_action_executes_and_records_verb(executor, form, tmp_path):
    import obstore.store

    handler = rs.PickleIOHandler(
        store=obstore.store.LocalStore(str(tmp_path), mkdir=True)
    )
    calls = []

    if form == "decorator":

        def _opt(ctx):
            calls.append(ctx.asset_name)

        opt = rs.AssetAction(name="optimize", outcome=rs.Outcome.Unchanged)(_opt)

        @rs.Asset(actions=[opt], io_handler=handler)
        def orders() -> int:
            return 1

        @rs.Asset(actions=[opt], io_handler=handler)
        def customers() -> int:
            return 2

        assets = [orders, customers]
    else:

        class OptBase(rs.Asset):
            io_handler = handler

            @rs.action(outcome=rs.Outcome.Unchanged)
            @classmethod
            def optimize(cls, ctx):
                calls.append(ctx.asset_name)

        class Orders(OptBase):
            @classmethod
            def materialize(cls):
                return 1

        class Customers(OptBase):
            @classmethod
            def materialize(cls):
                return 2

        assets = [Orders, Customers]

    repo = rs.CodeRepository(assets=assets, default_executor=executor)
    repo.materialize()
    result = repo.run_action("optimize")

    assert result.success
    assert sorted(calls) == ["customers", "orders"]
    run = repo.storage.get_run(result.run_id)
    assert run.action == "optimize"
    types = event_types(repo, result.run_id)
    assert types.count("ActionCompleted") == 2
    assert types.count("StepSuccess") == 2
    assert "Materialization" not in types


@pytest.mark.parametrize("executor", EXECUTORS)
def test_async_action(executor):
    calls = []

    class Nightly(rs.Asset):
        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        async def refresh(cls, ctx):
            await asyncio.sleep(0)
            calls.append(ctx.asset_name)

    repo = rs.CodeRepository(assets=[Nightly], default_executor=executor)
    repo.materialize()
    assert repo.run_action("refresh").success
    assert calls == ["nightly"]


def test_action_context_surface(tmp_path):
    import obstore.store

    handler = rs.PickleIOHandler(
        store=obstore.store.LocalStore(str(tmp_path), mkdir=True)
    )
    seen = {}

    class Events(rs.Asset):
        io_handler = handler
        metadata = {"delta/root_name": "events_v2"}
        partitions_def = rs.PartitionsDefinition.static_(["a", "b"])

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext):
            return f"row-{context.partition_key}"

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def compact(cls, ctx):
            seen.update(
                asset_name=ctx.asset_name,
                action=ctx.action,
                partition_key=ctx.partition_key,
                handler_is_ours=ctx.io_handler is handler,
                metadata=dict(ctx.asset_metadata),
                has_run_id=bool(ctx.run_id),
            )

    repo = rs.CodeRepository(assets=[Events], default_executor=IP)
    pk = rs.PartitionKey.single("a")
    repo.materialize(partition_key=pk)
    assert repo.run_action("compact", partition_key=pk).success

    assert seen["asset_name"] == "events"
    assert seen["action"] == "compact"
    assert seen["partition_key"] == "a"
    assert seen["handler_is_ours"] is True
    assert seen["metadata"]["delta/root_name"] == "events_v2"
    assert seen["has_run_id"] is True


def test_action_never_runs_materialize_or_upstream():
    ran = []

    @rs.Asset
    def upstream() -> int:
        ran.append("upstream")
        return 1

    class Downstream(rs.Asset):
        @classmethod
        def materialize(cls, upstream: int) -> int:
            ran.append("materialize")
            return upstream + 1

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def touch(cls, ctx):
            ran.append("touch")

    repo = rs.CodeRepository(assets=[upstream, Downstream], default_executor=IP)
    repo.materialize()
    ran.clear()
    repo.run_action("touch", selection=["downstream"])
    assert ran == ["touch"]


def test_action_failure_does_not_inherit_asset_retry():
    attempts = []

    class Flaky(rs.Asset):
        retry = rs.RetryPolicy(max_retries=3)

        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def boom(cls, ctx):
            attempts.append(1)
            raise RuntimeError("kaboom")

    repo = rs.CodeRepository(assets=[Flaky], default_executor=IP)
    repo.materialize()
    result = repo.run_action("boom", raise_on_error=False)

    assert not result.success
    assert len(attempts) == 1
    types = event_types(repo, result.run_id)
    assert "StepRetry" not in types
    assert "StepFailure" in types


def test_action_partitioning_declaration():
    """partitioning= rides AssetAction and @rs.action; Required is the default."""
    act = rs.AssetAction(
        name="compact_all",
        outcome=rs.Outcome.Unchanged,
        partitioning=rs.ActionPartitioning.Keyless,
    )
    assert act.partitioning == rs.ActionPartitioning.Keyless
    default = rs.AssetAction(name="touch", outcome=rs.Outcome.Unchanged)
    assert default.partitioning == rs.ActionPartitioning.Required

    class Purgeable(rs.Asset):
        @rs.action(
            outcome=rs.Outcome.Unmaterialize,
            partitioning=rs.ActionPartitioning.Optional,
        )
        @classmethod
        def purge(cls, ctx) -> None: ...

        @classmethod
        def materialize(cls) -> int:
            return 1

    desugared = rs._core.assets.desugar(Purgeable)
    (purge,) = [a for a in desugared.actions if a.name == "purge"]
    assert purge.partitioning == rs.ActionPartitioning.Optional


def test_downstream_first_ordering():
    order = []

    class Purgeable(rs.Asset):
        @rs.action(
            outcome=rs.Outcome.Unchanged,
            ordering=rs.ActionOrdering.DownstreamFirst,
        )
        @classmethod
        def purge(cls, ctx):
            order.append(ctx.asset_name)

    class Events(Purgeable):
        @classmethod
        def materialize(cls):
            return 1

    class Rollups(Purgeable):
        @classmethod
        def materialize(cls, events: int) -> int:
            return events + 1

    repo = rs.CodeRepository(assets=[Events, Rollups], default_executor=IP)
    repo.materialize()
    assert repo.run_action("purge").success
    assert order == ["rollups", "events"]


def test_multi_asset_per_output_actions():
    calls = []

    def _zorder(ctx):
        calls.append(("zorder", ctx.asset_name))

    zorder = rs.AssetAction(name="zorder", outcome=rs.Outcome.Unchanged)(_zorder)

    class Ingest(rs.MultiAsset):
        m_left = rs.AssetDef(actions=[zorder])
        m_right = rs.AssetDef()

        @classmethod
        def materialize(cls):
            return {"m_left": 1, "m_right": 2}

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def compact(cls, ctx):
            calls.append(("compact", ctx.asset_name))

    repo = rs.CodeRepository(assets=[Ingest], default_executor=IP)
    repo.materialize()

    assert repo.run_action("compact").success
    assert sorted(c for c in calls if c[0] == "compact") == [
        ("compact", "m_left"),
        ("compact", "m_right"),
    ]

    calls.clear()
    assert repo.run_action("zorder").success
    assert calls == [("zorder", "m_left")]

    with pytest.raises(GraphValidationError, match="does not define action 'zorder'"):
        repo.run_action("zorder", selection=["m_right"])


def test_graph_asset_action_targets_output_only():
    ran = []

    @rs.Asset
    def g_src() -> int:
        ran.append("src")
        return 5

    @rs.Task
    def g_double(g_src: int) -> int:
        ran.append("task")
        return g_src * 2

    class Pipe(rs.GraphAsset):
        @classmethod
        def compose(cls, g_src: int):
            return g_double(g_src)

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def refresh_views(cls, ctx):
            ran.append(("action", ctx.asset_name))

    repo = rs.CodeRepository(
        assets=[g_src, Pipe], tasks=[g_double], default_executor=IP
    )
    repo.materialize()
    ran.clear()
    assert repo.run_action("refresh_views", selection=["pipe"]).success
    assert ran == [("action", "pipe")]
