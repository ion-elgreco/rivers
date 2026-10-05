import asyncio

import pytest
from pydantic import BaseModel

import rivers as rs
from _helpers import EXECUTORS, IP


# ---------------------------------------------------------------------------
# Action config: ActionContext[Config]
# ---------------------------------------------------------------------------


class TuneConfig(BaseModel):
    target_size_mb: int = 128
    force: bool = False


def test_action_context_can_be_built_for_unit_tests():
    """An action body is a plain function of its context: tests build one by
    hand, like `AssetExecutionContext(asset_name=...)`, without running the
    whole run spine."""

    def purge(ctx: rs.ActionContext) -> None:
        for key in ctx.partition.keys:
            if "p2" in str(key):
                ctx.mark_partition_failed(key, "locked")

    pd = rs.PartitionsDefinition.static_(["p1", "p2"])
    keys = [rs.PartitionKey.single("p1"), rs.PartitionKey.single("p2")]
    ctx = rs.ActionContext(
        asset_name="events",
        action="purge",
        partition=rs.PartitionContext(keys, pd),
        asset_metadata={"delta/root_name": "evt"},
    )
    assert (ctx.asset_name, ctx.action, ctx.run_id) == ("events", "purge", "")
    assert ctx.asset_metadata == {"delta/root_name": "evt"}
    purge(ctx)
    bare = rs.ActionContext(asset_name="events", action="vacuum", run_id="r1")
    assert (bare.partition, bare.io_handler, bare.config, bare.run_id) == (
        None,
        None,
        None,
        "r1",
    )
    assert not bare.has_partition_key


def test_action_context_subscriptable():
    alias = rs.ActionContext[TuneConfig]
    assert alias.__origin__ is rs.ActionContext
    assert alias.__args__ == (TuneConfig,)


@pytest.mark.parametrize("executor", EXECUTORS)
@pytest.mark.parametrize("style", ["sync", "async"])
def test_action_config_defaults_and_overrides(executor, style):
    seen = {}

    if style == "sync":

        def _tune(ctx: rs.ActionContext[TuneConfig]):
            seen[ctx.asset_name] = (ctx.config.target_size_mb, ctx.config.force)

    else:

        async def _tune(ctx: rs.ActionContext[TuneConfig]):
            await asyncio.sleep(0)
            seen[ctx.asset_name] = (ctx.config.target_size_mb, ctx.config.force)

    tune = rs.AssetAction(name="tune", outcome=rs.Outcome.Unchanged)(_tune)

    @rs.Asset(actions=[tune])
    def orders() -> int:
        return 1

    repo = rs.CodeRepository(assets=[orders], default_executor=executor)
    repo.materialize()

    assert repo.run_action("tune").success
    assert seen == {"orders": (128, False)}

    assert repo.run_action(
        "tune",
        config={
            "assets": {"orders": {"config": {"target_size_mb": 512, "force": True}}}
        },
    ).success
    assert seen == {"orders": (512, True)}


def test_action_config_class_form():
    seen = {}

    class EventLog(rs.Asset):
        @classmethod
        def materialize(cls) -> int:
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def tune(cls, ctx: rs.ActionContext[TuneConfig]) -> None:
            seen["cfg"] = (ctx.config.target_size_mb, ctx.config.force)

    repo = rs.CodeRepository(assets=[EventLog], default_executor=IP)
    assert repo.run_action(
        "tune", config={"assets": {"event_log": {"config": {"target_size_mb": 64}}}}
    ).success
    assert seen["cfg"] == (64, False)


def test_action_config_absent_without_annotation():
    seen = {}

    def _plain(ctx):
        seen["plain"] = ctx.config

    def _bare(ctx: rs.ActionContext):
        seen["bare"] = ctx.config

    plain = rs.AssetAction(name="plain", outcome=rs.Outcome.Unchanged)(_plain)
    bare = rs.AssetAction(name="bare", outcome=rs.Outcome.Unchanged)(_bare)

    @rs.Asset(actions=[plain, bare])
    def orders() -> int:
        return 1

    repo = rs.CodeRepository(assets=[orders], default_executor=IP)
    assert repo.run_action(
        "plain", config={"assets": {"orders": {"config": {"target_size_mb": 1}}}}
    ).success
    assert repo.run_action(
        "bare", config={"assets": {"orders": {"config": {"target_size_mb": 1}}}}
    ).success
    assert seen == {"plain": None, "bare": None}


def test_action_config_validation_error_fails_run():
    def _tune(ctx: rs.ActionContext[TuneConfig]):
        raise AssertionError("action body must not run on invalid config")

    tune = rs.AssetAction(name="tune", outcome=rs.Outcome.Unchanged)(_tune)

    @rs.Asset(actions=[tune])
    def orders() -> int:
        return 1

    repo = rs.CodeRepository(assets=[orders], default_executor=IP)
    result = repo.run_action(
        "tune",
        config={"assets": {"orders": {"config": {"target_size_mb": "not-an-int"}}}},
        raise_on_error=False,
    )
    assert not result.success
    # The tripwire also yields success=False — pin that validation rejected
    # the config before the body ran, not that the body tripwired.
    assert result.failed_assets, "expected the validation error on failed_assets"
    assert "action body must not run" not in result.failed_assets[0][1]
    assert "target_size_mb" in result.failed_assets[0][1]


def test_observe_config_override():
    seen = {}

    class Feed(rs.ExternalAsset):
        io_handler = rs.InMemoryIOHandler()

        @classmethod
        def observe(
            cls, context: rs.AssetExecutionContext[TuneConfig]
        ) -> rs.Observation:
            seen["cfg"] = context.config.target_size_mb
            return rs.Observation(
                metadata={"rows": rs.MetadataValue.int(1)}, data_version="dv-cfg"
            )

    repo = rs.CodeRepository(assets=[Feed], default_executor=IP)
    assert repo.run_action(
        "observe", config={"assets": {"feed": {"config": {"target_size_mb": 42}}}}
    ).success
    assert seen["cfg"] == 42


# ---------------------------------------------------------------------------
# Resource parameters: injected by name, like materialize functions
# ---------------------------------------------------------------------------


class ProbeResource(rs.Resource):
    prefix: str = "probe"


@pytest.mark.parametrize("executor", EXECUTORS)
@pytest.mark.parametrize("style", ["sync", "async"])
def test_action_resource_param_injection(executor, style):
    seen = {}

    if style == "sync":

        def _tag(ctx, probe: ProbeResource):
            seen[ctx.asset_name] = probe.prefix

    else:

        async def _tag(ctx, probe: ProbeResource):
            await asyncio.sleep(0)
            seen[ctx.asset_name] = probe.prefix

    tag = rs.AssetAction(name="tag", outcome=rs.Outcome.Unchanged)(_tag)

    @rs.Asset(actions=[tag])
    def orders() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[orders],
        resources={"probe": ProbeResource(prefix="from-repo")},
        default_executor=executor,
    )
    repo.materialize()
    assert repo.run_action("tag").success
    assert seen == {"orders": "from-repo"}


def test_action_resource_param_class_form():
    seen = {}

    class EventLog(rs.Asset):
        @classmethod
        def materialize(cls) -> int:
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def compact(cls, ctx, probe: ProbeResource) -> None:
            seen["prefix"] = probe.prefix

    repo = rs.CodeRepository(
        assets=[EventLog],
        resources={"probe": ProbeResource(prefix="cf")},
        default_executor=IP,
    )
    assert repo.run_action("compact").success
    assert seen == {"prefix": "cf"}


def test_action_unknown_param_rejected():
    def _bad(ctx, warehouse):
        del warehouse

    bad = rs.AssetAction(name="bad", outcome=rs.Outcome.Unchanged)(_bad)

    @rs.Asset(actions=[bad])
    def orders() -> int:
        return 1

    repo = rs.CodeRepository(assets=[orders], default_executor=IP)
    result = repo.run_action("bad", raise_on_error=False)
    assert not result.success
    assert "does not match any resource" in str(result.failed_assets[0][1])


def test_action_context_must_be_first_param():
    def _bad(ctx, extra: rs.ActionContext):
        del extra

    bad = rs.AssetAction(name="bad2", outcome=rs.Outcome.Unchanged)(_bad)

    @rs.Asset(actions=[bad])
    def orders() -> int:
        return 1

    repo = rs.CodeRepository(assets=[orders], default_executor=IP)
    result = repo.run_action("bad2", raise_on_error=False)
    assert not result.success
    assert "Context must be the first parameter" in str(result.failed_assets[0][1])


def test_action_resource_param_with_config_overrides():
    seen = {}

    def _tag(ctx: rs.ActionContext[TuneConfig], probe: ProbeResource):
        seen["vals"] = (ctx.config.target_size_mb, probe.prefix)

    tag = rs.AssetAction(name="tag", outcome=rs.Outcome.Unchanged)(_tag)

    @rs.Asset(actions=[tag])
    def orders() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[orders],
        resources={"probe": ProbeResource(prefix="base")},
        default_executor=IP,
    )
    assert repo.run_action(
        "tag", config={"assets": {"orders": {"config": {"target_size_mb": 9}}}}
    ).success
    assert seen["vals"] == (9, "base")


def test_action_without_parameters():
    ran = []

    def _touch():
        ran.append(True)

    touch = rs.AssetAction(name="touch", outcome=rs.Outcome.Unchanged)(_touch)

    @rs.Asset(actions=[touch])
    def orders() -> int:
        return 1

    repo = rs.CodeRepository(assets=[orders], default_executor=IP)
    assert repo.run_action("touch").success
    assert ran == [True]


def test_action_context_has_no_resources_attr():
    seen = {}

    def _check(ctx):
        seen["has"] = hasattr(ctx, "resources")

    check = rs.AssetAction(name="check", outcome=rs.Outcome.Unchanged)(_check)

    @rs.Asset(actions=[check])
    def orders() -> int:
        return 1

    repo = rs.CodeRepository(assets=[orders], default_executor=IP)
    assert repo.run_action("check").success
    assert seen == {"has": False}


def test_job_level_retry_on_an_action_job_is_rejected():
    """A job-level retry never applied to action runs — it was silently
    dropped, so the job read as retrying when it wasn't."""

    class Table(rs.Asset):
        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def compact(cls, ctx):
            return None

    with pytest.raises(Exception, match="job-level retry does not apply"):
        rs.Job(
            name="j",
            assets=[Table],
            action="compact",
            retry=rs.RetryPolicy(max_retries=2),
        )


def test_action_first_param_named_like_a_resource_is_rejected():
    """The first parameter is always the context; naming it after a resource
    used to bind the context there anyway and fail deep inside the body."""

    class Store(rs.Resource):
        value: int = 1

    class Table(rs.Asset):
        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def compact(cls, store):
            return store.value

    repo = rs.CodeRepository(
        assets=[Table], resources={"store": Store()}, default_executor=IP
    )
    repo.materialize()
    result = repo.run_action("compact", raise_on_error=False)
    assert not result.success
    assert any(
        "first parameter is always the ActionContext" in str(err)
        for _, err in result.failed_assets
    )


def test_backfill_request_carries_the_action(storage):
    """A schedule/sensor must be able to request an action backfill — the verb
    was dropped between `rs.BackfillRequest` and the backfill record."""
    calls = []

    class Events(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = rs.PartitionsDefinition.static_(["p1", "p2"])

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext):
            return context.partition_key

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def compact(cls, ctx):
            calls.append(ctx.partition_key)

    job = rs.Job(name="events_job", assets=[Events])

    @rs.Schedule(cron_schedule="* * * * *", name="compact_sched", job_name="events_job")
    def compact_sched(context: rs.ScheduleEvaluationContext):
        return rs.BackfillRequest(
            selection=["events"],
            partition_keys=[rs.PartitionKey.single("p1")],
            action="compact",
        )

    repo = rs.CodeRepository(
        assets=[Events], jobs=[job], schedules=[compact_sched], default_executor=IP
    )
    repo.resolve(storage=storage)
    repo.materialize(partition_key=rs.PartitionKey.single("p1"))

    request = repo.evaluate_schedule("compact_sched").run_requests[0]
    assert request.action == "compact"

    # The request the daemon dispatches carries the verb end-to-end: launching
    # it produces an action backfill whose children run the action.
    result = repo.backfill(
        selection=request.selection,
        partition_keys=request.partition_keys,
        action=request.action,
    )
    assert repo.get_backfill(result.backfill_id).action == "compact"
    assert calls == ["p1"]


def test_backfill_of_a_whole_asset_verb_is_rejected_up_front():
    """A backfill always runs keyed, and a whole-asset verb refuses keys — so
    such a backfill can never succeed. It was accepted (a dry run even
    reported the runs) and every child failed at launch."""
    calls = []

    class Events(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = rs.PartitionsDefinition.static_(["p1", "p2"])

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext):
            return context.partition_key

        @rs.action(
            outcome=rs.Outcome.Unchanged, partitioning=rs.ActionPartitioning.Keyless
        )
        @classmethod
        def optimize(cls, ctx):
            calls.append(ctx.asset_name)

    repo = rs.CodeRepository(
        assets=[Events],
        jobs=[rs.Job(name="nightly_optimize", assets=[Events], action="optimize")],
        default_executor=IP,
    )
    keys = [rs.PartitionKey.single("p1"), rs.PartitionKey.single("p2")]
    for k in keys:
        repo.materialize(partition_key=k)
    runs_before = len(repo.storage.get_runs(limit=50))

    for dry_run in (True, False):
        with pytest.raises(Exception, match="Action 'optimize' is whole-asset"):
            repo.backfill(
                selection=["events"],
                partition_keys=keys,
                action="optimize",
                dry_run=dry_run,
            )
    with pytest.raises(Exception, match="Action 'optimize' is whole-asset"):
        repo.backfill(
            selection=["events"],
            partition_range=rs.PartitionKeyRange.single("p1", "p2"),
            action="optimize",
        )
    assert calls == []
    assert len(repo.storage.get_runs(limit=50)) == runs_before


def test_empty_action_backfill_selection_is_rejected(storage):
    """An empty selection with a verb ran the verb on every asset declaring
    it — a sensor that filtered its candidates down to [] fanned a delete
    across the code location. gRPC and the UI refuse this; so must a
    BackfillRequest and repo.backfill. `selection=None` stays the explicit
    "every asset that defines the verb", as for `run_action`."""
    deleted = []

    class Events(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = rs.PartitionsDefinition.static_(["p1"])

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext):
            return context.partition_key

        @rs.action(outcome=rs.Outcome.Unmaterialize)
        @classmethod
        def delete(cls, ctx):
            deleted.append(ctx.partition_key)

    p1 = rs.PartitionKey.single("p1")
    with pytest.raises(ValueError, match="empty selection"):
        rs.BackfillRequest(selection=[], partition_keys=[p1], action="delete")
    # Without a verb an empty selection keeps its meaning (every asset).
    assert rs.BackfillRequest(selection=[], partition_keys=[p1]).selection == []

    repo = rs.CodeRepository(assets=[Events], default_executor=IP)
    repo.resolve(storage=storage)
    repo.materialize(partition_key=p1)
    with pytest.raises(Exception, match="empty selection"):
        repo.backfill(selection=[], partition_keys=[p1], action="delete")
    assert deleted == []

    repo.backfill(selection=None, partition_keys=[p1], action="delete")
    assert deleted == ["p1"]


def test_unchanged_reports_metadata_on_the_event():
    """An action that changes nothing still has something to report (rows
    scanned, bytes reclaimed) — the outcome carries metadata now."""

    class Table(rs.Asset):
        io_handler = rs.InMemoryIOHandler()

        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def vacuum(cls, ctx):
            return rs.ActionResult.unchanged(metadata={"files_removed": 3})

    repo = rs.CodeRepository(assets=[Table], default_executor=IP)
    repo.materialize()
    result = repo.run_action("vacuum")
    assert result.success

    event = next(
        e
        for e in repo.storage.get_events_for_run(result.run_id)
        if str(e.event_type) == "ActionCompleted"
    )
    md = dict(event.metadata)
    assert "vacuum" in md["action"]
    assert "3" in md["files_removed"]


def test_hooks_never_fire_for_action_runs():
    """Hooks belong to the materialize path — an action completing is not a
    materialization, and firing success hooks would misreport freshness."""
    fired = []

    @rs.Hook.success
    def track(context):
        fired.append(context.asset_name)

    class Table(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        hooks = [track]

        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def vacuum(cls, ctx):
            return None

        @rs.action(outcome=rs.Outcome.Unmaterialize)
        @classmethod
        def delete(cls, ctx):
            return None

    repo = rs.CodeRepository(assets=[Table], default_executor=IP)
    repo.materialize()
    assert fired == ["table"], "materialize still fires hooks"

    fired.clear()
    assert repo.run_action("vacuum").success
    assert repo.run_action("delete").success
    assert fired == []


def test_asset_actions_attribute_matches_stub():
    """The stub types ``Asset.actions`` as ``list[AssetAction] | None``; the
    runtime getter must exist — it was the only declarable of the class-body
    sweep without one, so ``orders.actions`` raised AttributeError."""

    def _compact(ctx):
        return None

    compact = rs.AssetAction(name="compact", outcome=rs.Outcome.Unchanged)(_compact)

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), actions=[compact])
    def orders():
        return 1

    assert [a.name for a in orders.actions] == ["compact"]
    assert orders.actions[0].outcome == rs.Outcome.Unchanged

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def plain():
        return 1

    assert plain.actions is None
