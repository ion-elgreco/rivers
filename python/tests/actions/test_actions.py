"""Asset actions: registration and the run spine.

Actions execute through the existing run machinery — run records carry the
verb, plans never pull in upstream, ActionCompleted events land on the
timeline, and retry policies never leak over from materialize.
"""

import asyncio

import pytest
from pydantic import BaseModel

import rivers as rs
from _polling import wait_for_run_terminal, wait_until
from rivers._core import AutomationDaemon
from rivers.exceptions import AssetDefinitionError, GraphValidationError

IP = rs.Executor.in_process()
MP = rs.Executor.parallel(max_workers=2)

EXECUTORS = [pytest.param(IP, id="in_process"), pytest.param(MP, id="parallel")]


def event_types(repo, run_id):
    return sorted(str(e.event_type) for e in repo.storage.get_events_for_run(run_id))


# Module-level so loky workers can import them by reference.
@rs.Task
def _fan_echo(x: int) -> int:
    return x


@rs.Task
def _fan_total(values: list) -> int:
    return sum(values)


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def test_reserved_names_rejected():
    for verb in ("materialize", "observe", "compose"):
        with pytest.raises(AssetDefinitionError, match="reserved verb"):
            rs.AssetAction(name=verb, outcome=rs.Outcome.Unchanged)


def test_observe_outcome_reserved():
    with pytest.raises(AssetDefinitionError, match="reserved for the built-in observe"):
        rs.AssetAction(name="probe", outcome=rs.Outcome.Observe)


def test_unbound_action_rejected_at_decorator():
    with pytest.raises(AssetDefinitionError, match="has no function"):

        @rs.Asset(actions=[rs.AssetAction(name="opt", outcome=rs.Outcome.Unchanged)])
        def orders() -> int:
            return 1


def test_duplicate_action_names_rejected():
    def _body(ctx):
        return None

    a = rs.AssetAction(name="opt", outcome=rs.Outcome.Unchanged)(_body)
    b = rs.AssetAction(name="opt", outcome=rs.Outcome.Unchanged)(_body)
    with pytest.raises(AssetDefinitionError, match="duplicate action"):

        @rs.Asset(actions=[a, b])
        def orders() -> int:
            return 1


def test_rebinding_a_bound_action_rejected():
    def _body(ctx):
        return None

    bound = rs.AssetAction(name="opt", outcome=rs.Outcome.Unchanged)(_body)
    with pytest.raises(AssetDefinitionError, match="already bound"):
        bound(_body)


def test_shadowing_inherited_action_rejected():
    class Base(rs.Asset):
        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def optimize(cls, ctx):
            return None

    class Child(Base):
        optimize = "not an action"

        @classmethod
        def materialize(cls):
            return 1

    from rivers._core.assets import desugar

    with pytest.raises(AssetDefinitionError, match="shadows the inherited action"):
        desugar(Child)


def test_action_metadata_exposed():
    def _body(ctx):
        return None

    act = rs.AssetAction(
        name="vacuum",
        outcome=rs.Outcome.Unchanged,
        concurrency=rs.ActionConcurrency.Exclusive,
        description="clean up",
    )(_body)
    assert act.name == "vacuum"
    assert act.exclusive is True
    assert act.outcome == rs.Outcome.Unchanged
    assert act.description == "clean up"


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
            calls.append(ctx.asset_key)

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
                calls.append(ctx.asset_key)

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
            calls.append(ctx.asset_key)

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
                asset_key=ctx.asset_key,
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

    assert seen["asset_key"] == "events"
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


def test_reverse_topological_ordering_downstream_first():
    order = []

    class Purgeable(rs.Asset):
        @rs.action(
            outcome=rs.Outcome.Unchanged,
            ordering=rs.ActionOrdering.ReverseTopological,
        )
        @classmethod
        def purge(cls, ctx):
            order.append(ctx.asset_key)

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
        calls.append(("zorder", ctx.asset_key))

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
            calls.append(("compact", ctx.asset_key))

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
            ran.append(("action", ctx.asset_key))

    repo = rs.CodeRepository(
        assets=[g_src, Pipe], tasks=[g_double], default_executor=IP
    )
    repo.materialize()
    ran.clear()
    assert repo.run_action("refresh_views", selection=["pipe"]).success
    assert ran == [("action", "pipe")]


# ---------------------------------------------------------------------------
# Jobs, schedules, backfills
# ---------------------------------------------------------------------------


def test_job_action_validates_targets():
    class Plain(rs.Asset):
        @classmethod
        def materialize(cls):
            return 1

    with pytest.raises(GraphValidationError, match="does not define action"):
        rs.CodeRepository(
            assets=[Plain],
            jobs=[rs.Job(name="j", assets=[Plain], action="optimize", executor=IP)],
        ).resolve()


def test_backfill_action_children_inherit_verb():
    calls = []

    class Events(rs.Asset):
        partitions_def = rs.PartitionsDefinition.static_(["p1", "p2", "p3"])

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext):
            return context.partition_key

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def compact(cls, ctx):
            calls.append(ctx.partition_key)

    repo = rs.CodeRepository(assets=[Events], default_executor=IP)
    for p in ("p1", "p2", "p3"):
        repo.materialize(partition_key=rs.PartitionKey.single(p))

    res = repo.backfill(
        selection=["events"],
        partition_keys=[rs.PartitionKey.single("p1"), rs.PartitionKey.single("p2")],
        action="compact",
    )
    assert res.status == "CompletedSuccess"
    assert sorted(calls) == ["p1", "p2"]

    record = repo.get_backfill(res.backfill_id)
    assert record.action == "compact"
    child_verbs = {repo.storage.get_run(rid).action for rid in record.run_ids}
    assert child_verbs == {"compact"}

    calls.clear()
    rerun = repo.rerun_backfill(res.backfill_id)
    assert repo.get_backfill(rerun.backfill_id).action == "compact"
    assert sorted(calls) == ["p1", "p2"]


def test_backfill_action_rejects_asset_without_verb():
    class Plain(rs.Asset):
        partitions_def = rs.PartitionsDefinition.static_(["p1"])

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext):
            return 1

    repo = rs.CodeRepository(assets=[Plain], default_executor=IP)
    with pytest.raises(GraphValidationError, match="does not define action"):
        repo.backfill(
            selection=["plain"],
            partition_keys=[rs.PartitionKey.single("p1")],
            action="compact",
        )


def test_action_with_kubernetes_executor_runs_in_orchestrator(storage):
    """Action steps never ship to K8s step pods — like the parallel executor,
    a K8s-executor repo runs them in the orchestrator process. Regression:
    the K8s backend was verb-blind and its step pods silently materialized."""
    calls = []

    class Events(rs.Asset):
        io_handler = rs.InMemoryIOHandler()

        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def compact(cls, ctx):
            calls.append(ctx.asset_key)

    repo = rs.CodeRepository(
        assets=[Events],
        default_executor=rs.Executor.kubernetes("img:latest", namespace="ns"),
    )
    repo.resolve(storage=storage)

    result = repo.run_action("compact")
    assert result.success
    assert calls == ["events"]
    run = repo.storage.get_run(result.run_id)
    assert run.action == "compact"


def test_run_action_run_id_override_reuses_record():
    """`run_id_override` mirrors materialize's seam contract — the K8s run pod
    re-executes an existing action run record under its original id."""

    class Events(rs.Asset):
        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def compact(cls, ctx):
            pass

    repo = rs.CodeRepository(assets=[Events], default_executor=IP)
    result = repo.run_action("compact", run_id_override="fixed-rid")
    assert result.success
    assert result.run_id == "fixed-rid"
    assert repo.storage.get_run("fixed-rid").action == "compact"

    # Run again under the same id: the record must be reused (the pod
    # re-executes an existing run), not minted a second time.
    again = repo.run_action("compact", run_id_override="fixed-rid")
    assert again.success and again.run_id == "fixed-rid"
    assert [r.run_id for r in repo.storage.get_runs(limit=10)] == ["fixed-rid"]


def test_queued_backfill_action_children_run_the_action(storage):
    """Queued-mode backfill children must carry and execute the backfill's
    verb — regression: the verb was dropped at submission and every child
    silently materialized instead."""
    calls = []
    materialized = []

    class Events(rs.Asset):
        partitions_def = rs.PartitionsDefinition.static_(["p1", "p2"])
        io_handler = rs.InMemoryIOHandler()

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext):
            materialized.append(context.partition_key)
            return context.partition_key

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def compact(cls, ctx):
            calls.append(ctx.partition_key)

    repo = rs.CodeRepository(
        assets=[Events],
        default_executor=IP,
        run_queue=rs.RunQueueConfig(max_concurrent_runs=2, dequeue_interval="50ms"),
    )
    repo.resolve(storage=storage)

    res = repo.backfill(
        selection=["events"],
        partition_keys=[rs.PartitionKey.single("p1"), rs.PartitionKey.single("p2")],
        action="compact",
        block=False,
    )
    repo.execute_backfill_queued(res.backfill_id)

    record = repo.get_backfill(res.backfill_id)
    assert len(record.run_ids) == 2
    for rid in record.run_ids:
        assert repo.storage.get_run(rid).action == "compact"

    daemon = AutomationDaemon(repo=repo, storage=storage, condition_eval_interval="10s")
    daemon.start()
    try:
        for rid in record.run_ids:
            run = wait_for_run_terminal(storage, rid, timeout=20)
            assert run is not None and run.status == "Success"
    finally:
        daemon.stop()

    assert sorted(calls) == ["p1", "p2"]
    assert materialized == []


def test_observe_runs_through_spine():
    class Feed(rs.ExternalAsset):
        io_handler = rs.InMemoryIOHandler()

        @classmethod
        def observe(cls) -> rs.Observation:
            return rs.Observation(
                metadata={"rows": rs.MetadataValue.int(3)}, data_version="dv-1"
            )

    repo = rs.CodeRepository(assets=[Feed], default_executor=IP)
    result = repo.observe()
    assert result.success
    run = repo.storage.get_run(result.run_id)
    assert run.action == "observe"
    types = event_types(repo, result.run_id)
    assert "Observation" in str(types)
    assert "ActionCompleted" not in types


def test_keyless_action_on_partitioned_asset_names_the_verb():
    """The missing-key error must name the verb the user asked for — it said
    "materialize" for every action, which reads as a rivers bug."""

    class Events(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = rs.PartitionsDefinition.static_(["p1"])

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext):
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def compact(cls, ctx):
            return None

    repo = rs.CodeRepository(assets=[Events], default_executor=IP)
    with pytest.raises(Exception, match="Cannot run 'compact' without partition_key"):
        repo.run_action("compact")


def test_observe_with_partitioned_observable_external():
    """`observe()` is whole-asset — a partitioned observable external must not
    make it demand a partition key. Regression: routing observe through the
    action spine applied materialize's partition gate, so one partitioned
    observable bricked `repo.observe()` for the whole repo."""

    class Feed(rs.ExternalAsset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = rs.PartitionsDefinition.static_(["p1", "p2"])

        @classmethod
        def observe(cls) -> rs.Observation:
            return rs.Observation(data_version="dv-1")

    repo = rs.CodeRepository(assets=[Feed], default_executor=IP)
    result = repo.observe()
    assert result.success
    assert repo.storage.get_run(result.run_id).action == "observe"


def test_keyless_action_job_on_partitioned_asset_is_rejected():
    """A job carrying a verb must get the same partition gate as `run_action`.

    `PyJob::run_inner` validated nothing, so `Job(action="purge")` with no key
    on a partitioned asset emitted a whole-asset Deletion and cleared *every*
    partition's materialization state — a silent, unrecoverable wipe.
    """

    class Orders(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = rs.PartitionsDefinition.static_(["p1", "p2", "p3"])

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext):
            return context.partition_key

        @rs.action(outcome=rs.Outcome.Unmaterialize)
        @classmethod
        def purge(cls, ctx):
            return None

    repo = rs.CodeRepository(
        assets=[Orders],
        jobs=[rs.Job(name="purge_job", assets=[Orders], action="purge", executor=IP)],
        default_executor=IP,
    )
    for p in ["p1", "p2", "p3"]:
        repo.materialize(partition_key=rs.PartitionKey.single(p))
    before = {str(k) for k in repo.storage.get_materialized_partitions("orders")}
    assert len(before) == 3

    with pytest.raises(Exception, match="Cannot run 'purge' without partition_key"):
        repo.get_job("purge_job").execute()

    after = {str(k) for k in repo.storage.get_materialized_partitions("orders")}
    assert after == before, "a rejected action job must not clear any state"


def test_keyed_action_job_on_partitioned_asset_runs():
    """Falsifier for the guard above: with a key the same job must still work."""
    purged = []

    class Orders(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = rs.PartitionsDefinition.static_(["p1", "p2"])

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext):
            return context.partition_key

        @rs.action(outcome=rs.Outcome.Unmaterialize)
        @classmethod
        def purge(cls, ctx):
            purged.append(ctx.partition_key)
            return None

    repo = rs.CodeRepository(
        assets=[Orders],
        jobs=[rs.Job(name="purge_job", assets=[Orders], action="purge", executor=IP)],
        default_executor=IP,
    )
    for p in ["p1", "p2"]:
        repo.materialize(partition_key=rs.PartitionKey.single(p))

    result = repo.get_job("purge_job").execute(
        partition_key=rs.PartitionKey.single("p1")
    )
    assert result.success
    assert purged == ["p1"]
    remaining = {str(k) for k in repo.storage.get_materialized_partitions("orders")}
    assert remaining == {'PartitionKey("p2")'}


def test_keyless_observe_job_on_partitioned_observable_is_allowed():
    """The keyless-observe carve-out applies to the job path too: `observe()` is
    whole-asset, so a partitioned observable must not be forced to supply a key.
    """

    class Feed(rs.ExternalAsset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = rs.PartitionsDefinition.static_(["p1", "p2"])

        @classmethod
        def observe(cls) -> rs.Observation:
            return rs.Observation(data_version="dv-1")

    repo = rs.CodeRepository(
        assets=[Feed],
        jobs=[rs.Job(name="refresh", assets=[Feed], action="observe", executor=IP)],
        default_executor=IP,
    )
    result = repo.get_job("refresh").execute()
    assert result.success
    assert repo.storage.get_run(result.run_id).action == "observe"


def test_keyed_observe_sees_its_partition_key():
    """A keyed observe records its result against that partition, so the body
    has to be able to see which one it is."""
    seen = {}

    class Feed(rs.ExternalAsset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = rs.PartitionsDefinition.static_(["us", "eu"])

        @classmethod
        def observe(cls, ctx: rs.AssetExecutionContext) -> rs.Observation:
            seen["has_key"] = ctx.has_partition_key
            seen["key"] = ctx.partition_key
            return rs.Observation(data_version=f"dv-{ctx.partition_key}")

    repo = rs.CodeRepository(assets=[Feed], default_executor=IP)
    pk = rs.PartitionKey.single("us")
    assert repo.run_action("observe", ["feed"], partition_key=pk).success

    assert seen["has_key"] is True
    assert seen["key"] == "us"


def test_batched_observe_can_fail_one_partition():
    """`mark_partition_failed` works on any batched run, observe included.

    Giving observe a real partition context made the call succeed where it used
    to raise, but nothing drained the marks — so a key the body declared broken
    still got an Observation recording a data version for it.
    """

    class Feed(rs.ExternalAsset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = rs.PartitionsDefinition.static_(["us", "eu"])

        @classmethod
        def observe(cls, ctx: rs.AssetExecutionContext) -> rs.Observation:
            for key in ctx.partition.keys:
                if "eu" in str(key):
                    ctx.mark_partition_failed(key, "feed unreachable")
            return rs.Observation(data_version="dv-1")

    repo = rs.CodeRepository(assets=[Feed], default_executor=IP)
    repo.backfill(
        selection=["feed"],
        partition_keys=[rs.PartitionKey.single(p) for p in ("us", "eu")],
        action="observe",
        strategy=rs.BackfillStrategy.single_run(),
    )

    events = repo.storage.get_events_for_asset("feed")
    observed = sorted(
        str(e.partition_key) for e in events if e.event_type == "Observation"
    )
    assert len(observed) == 1, (
        f"eu was marked failed, so only us is observed: {observed}"
    )
    assert "us" in observed[0]

    failures = [
        e
        for e in events
        if e.event_type == "StepFailure" and e.partition_key is not None
    ]
    assert len(failures) == 1 and "eu" in str(failures[0].partition_key)


def test_keyed_observe_rejects_invalid_key():
    """The keyless-observe carve-out must not skip validation once a key IS
    given — a bad key on observe is still a caller error."""

    class Feed(rs.ExternalAsset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = rs.PartitionsDefinition.static_(["us", "eu"])

        @classmethod
        def observe(cls, ctx: rs.AssetExecutionContext) -> rs.Observation:
            return rs.Observation(data_version="dv")

    repo = rs.CodeRepository(assets=[Feed], default_executor=IP)
    with pytest.raises(rs.exceptions.ExecutionError, match="Invalid partition_key"):
        repo.run_action(
            "observe", ["feed"], partition_key=rs.PartitionKey.single("nope")
        )


def test_observe_skips_non_observable_names():
    """`observe(asset_names=...)` filters to the observable externals it can
    serve; unknown or non-observable names are skipped, not fatal. Regression:
    the action spine hard-errored on them (and on an empty list)."""

    class Feed(rs.ExternalAsset):
        io_handler = rs.InMemoryIOHandler()

        @classmethod
        def observe(cls) -> rs.Observation:
            return rs.Observation(data_version="dv-1")

    class Table(rs.Asset):
        io_handler = rs.InMemoryIOHandler()

        @classmethod
        def materialize(cls):
            return 1

    repo = rs.CodeRepository(assets=[Feed, Table], default_executor=IP)

    mixed = repo.observe(asset_names=["feed", "table"])
    assert mixed.success
    assert repo.storage.get_run(mixed.run_id).node_names == ["feed"]

    # Nothing observable in the selection — a successful no-op, like an
    # empty list and like a repo with no observables at all.
    assert repo.observe(asset_names=["table"]).success
    assert repo.observe(asset_names=[]).success
    assert repo.observe(asset_names=["nope"]).success


# ---------------------------------------------------------------------------
# Exclusive concurrency: the implicit per-asset one-slot pool
# ---------------------------------------------------------------------------


class _ExclusiveTable(rs.Asset):
    io_handler = rs.InMemoryIOHandler()

    @classmethod
    def materialize(cls):
        return 1

    @rs.action(outcome=rs.Outcome.Unchanged, concurrency=rs.ActionConcurrency.Exclusive)
    @classmethod
    def optimize(cls, ctx):
        return None

    @rs.action(outcome=rs.Outcome.Unchanged)
    @classmethod
    def vacuum(cls, ctx):
        return None


def test_exclusive_action_registers_unlimited_pool():
    """Exclusion is decided by partition overlap, not slot count, so the pool
    registers as unlimited — a finite sentinel only fed the UI a fake
    capacity."""
    from rivers.testing import memory_storage

    storage = memory_storage()
    repo = rs.CodeRepository(assets=[_ExclusiveTable], default_executor=IP)
    repo.resolve(storage=storage)
    info = storage.get_pool_info("__asset__:_exclusive_table")
    # EXCLUSIVE_POOL_CAPACITY in python/src/executor/dispatch/context.rs.
    assert info.slot_limit == -1


def test_exclusive_action_and_materialize_claim_the_pool():
    repo = rs.CodeRepository(assets=[_ExclusiveTable], default_executor=IP)
    mat = repo.materialize()
    act = repo.run_action("optimize")
    shared = repo.run_action("vacuum")

    def pools_claimed(run_id):
        return [
            dict(e.metadata).get("pools")
            for e in repo.storage.get_events_for_run(run_id)
            if e.event_type == "StepSlotClaimed"
        ]

    assert pools_claimed(mat.run_id) == ["__asset__:_exclusive_table"]
    assert pools_claimed(act.run_id) == ["__asset__:_exclusive_table"]
    # A Shared action never touches the pool.
    assert pools_claimed(shared.run_id) == []


def test_graph_asset_materialize_claims_the_exclusive_pool():
    """A graph asset's data is written by its inner tasks — those steps must
    claim the asset's implicit pool, or an exclusive action can overlap the
    composition. Regression: the graph asset's own step is composition-only
    (never executed), so materialize claimed nothing at all."""

    @rs.Task
    def gx_load() -> int:
        return 1

    class GxPipe(rs.GraphAsset):
        io_handler = rs.InMemoryIOHandler()

        @classmethod
        def compose(cls):
            return gx_load()

        @rs.action(
            outcome=rs.Outcome.Unchanged, concurrency=rs.ActionConcurrency.Exclusive
        )
        @classmethod
        def optimize(cls, ctx):
            return None

    repo = rs.CodeRepository(assets=[GxPipe], tasks=[gx_load], default_executor=IP)
    mat = repo.materialize()
    assert mat.success

    claimed = [
        dict(e.metadata).get("pools")
        for e in repo.storage.get_events_for_run(mat.run_id)
        if e.event_type == "StepSlotClaimed"
    ]
    assert claimed, "the graph's inner steps must claim the asset's pool"
    assert all("__asset__:gx_pipe" in c for c in claimed)

    act = repo.run_action("optimize")
    assert act.success
    act_claimed = [
        dict(e.metadata).get("pools")
        for e in repo.storage.get_events_for_run(act.run_id)
        if e.event_type == "StepSlotClaimed"
    ]
    assert act_claimed == ["__asset__:gx_pipe"]


def test_asset_without_exclusive_action_is_unaffected():
    class Plain(rs.Asset):
        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def touch(cls, ctx):
            return None

    repo = rs.CodeRepository(assets=[Plain], default_executor=IP)
    mat = repo.materialize()
    types = event_types(repo, mat.run_id)
    assert "StepSlotClaimed" not in types


def test_multi_materialize_claims_every_output_pool():
    class Ingest(rs.MultiAsset):
        m_left = rs.AssetDef()
        m_right = rs.AssetDef()

        @classmethod
        def materialize(cls):
            return {"m_left": 1, "m_right": 2}

        @rs.action(
            outcome=rs.Outcome.Unchanged, concurrency=rs.ActionConcurrency.Exclusive
        )
        @classmethod
        def compact(cls, ctx):
            return None

    repo = rs.CodeRepository(assets=[Ingest], default_executor=IP)
    mat = repo.materialize()
    claimed = [
        dict(e.metadata).get("pools")
        for e in repo.storage.get_events_for_run(mat.run_id)
        if e.event_type == "StepSlotClaimed"
    ]
    assert len(claimed) == 1
    assert "__asset__:m_left" in claimed[0]
    assert "__asset__:m_right" in claimed[0]


def test_resume_skips_completed_action_steps():
    """A crashed action run resumes instead of re-running its side effects.

    The K8s operator restarts a crashed executor pod with ``--resume``
    unconditionally, so without this every already-completed delete/compact
    step runs its side effect a second time.
    """
    calls = []

    class Base(rs.Asset):
        io_handler = rs.InMemoryIOHandler()

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def purge(cls, ctx):
            calls.append(ctx.asset_key)
            if ctx.asset_key == "second" and len(calls) < 3:
                raise RuntimeError("crash after first")

    class First(Base):
        @classmethod
        def materialize(cls):
            return 1

    class Second(Base):
        @classmethod
        def materialize(cls):
            return 2

    repo = rs.CodeRepository(assets=[First, Second], default_executor=IP)
    repo.materialize()

    run_id = "action-resume-test"
    result = repo.run_action(
        "purge", run_id_override=run_id, raise_on_error=False, resume=False
    )
    assert not result.success
    assert sorted(calls) == ["first", "second"]

    result = repo.run_action(
        "purge", run_id_override=run_id, raise_on_error=False, resume=True
    )
    assert result.success
    assert calls.count("first") == 1, "a completed action step re-ran its side effect"
    assert calls.count("second") == 2


def test_declaring_exclusive_action_does_not_serialize_materialize(tmp_path):
    """The implicit pool is a reader/writer lock, not a global mutex.

    Merely *declaring* an exclusive action must not make ordinary materialize
    steps contend for a single slot — that collapses graph-asset fan-out for an
    action nobody invoked, and serializes every run of the asset.
    """
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def items() -> list:
        return [1, 2, 3, 4, 5, 6]

    class Fanned(rs.GraphAsset):
        io_handler = handler
        node_io_handler = handler

        @classmethod
        def compose(cls):
            return _fan_total(items().map(_fan_echo).collect())

        @rs.action(
            outcome=rs.Outcome.Unchanged, concurrency=rs.ActionConcurrency.Exclusive
        )
        @classmethod
        def optimize(cls, ctx):
            return None

    repo = rs.CodeRepository(
        assets=[items, Fanned],
        tasks=[_fan_echo, _fan_total],
        default_executor=MP,
    )
    result = repo.materialize()
    assert result.success
    assert repo.load_node("fanned") == 21

    waiting = [
        e
        for e in repo.storage.get_events_for_run(result.run_id)
        if str(e.event_type) == "StepSlotWaiting"
    ]
    assert not waiting, (
        f"{len(waiting)} fan-out instances queued behind each other on the "
        "exclusive action's pool"
    )


# Internal budget (Event waits + thread joins) exceeds the 60s project-wide
# `timeout`, and `timeout_method = "thread"` calls os._exit(1) — which kills the
# whole pytest run with no report. Raise the ceiling so this test's own
# assertions fire first and name what actually stalled.
@pytest.mark.timeout(180)
def test_exclusive_action_serializes_with_materialize():
    import threading

    started = threading.Event()
    release = threading.Event()
    windows = {}

    class SlowTable(rs.Asset):
        io_handler = rs.InMemoryIOHandler()

        @classmethod
        def materialize(cls):
            import time

            windows["mat"] = (time.monotonic(), None)
            v = 1
            windows["mat"] = (windows["mat"][0], time.monotonic())
            return v

        @rs.action(
            outcome=rs.Outcome.Unchanged, concurrency=rs.ActionConcurrency.Exclusive
        )
        @classmethod
        def optimize(cls, ctx):
            import time

            start = time.monotonic()
            started.set()
            assert release.wait(timeout=30), "test driver never released the action"
            windows["opt"] = (start, time.monotonic())

    repo = rs.CodeRepository(assets=[SlowTable], default_executor=IP)
    repo.materialize()

    action_result = {}
    t_action = threading.Thread(
        target=lambda: action_result.update(
            r=repo.run_action("optimize", raise_on_error=False)
        )
    )
    t_action.start()
    assert started.wait(timeout=30)

    result = {}
    t_mat = threading.Thread(target=lambda: result.update(r=repo.materialize()))
    t_mat.start()
    # Wait for the materialize step to actually reach the claim loop and emit
    # StepSlotWaiting, rather than sleeping a guessed interval.
    wait_until(
        lambda: any(
            e.event_type == "StepSlotWaiting"
            for r in repo.storage.get_runs(limit=10)
            for e in repo.storage.get_events_for_run(r.run_id)
        ),
        timeout=30,
    )
    release.set()
    t_action.join(timeout=60)
    t_mat.join(timeout=60)

    # A bare thread swallows the action's outcome — `raise_on_error=True`
    # turns a failure into an exception threading only prints. Assert it, or
    # the timing assertions above prove nothing about a verb that never ran.
    assert action_result["r"].success
    assert result["r"].success
    # The materialize body may only run after the action released the slot.
    assert windows["mat"][0] >= windows["opt"][1]
    waiting = [
        e.event_type for e in repo.storage.get_events_for_run(result["r"].run_id)
    ]
    assert "StepSlotWaiting" in waiting


def _slow_partitioned_exclusive(started, release, windows):
    """Asset whose exclusive `purge` blocks until the driver releases it."""

    class Events(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = rs.PartitionsDefinition.static_(["p1", "p2"])

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext):
            import time

            key = context.partition_key
            windows[f"mat_{key}"] = (time.monotonic(), None)
            windows[f"mat_{key}"] = (windows[f"mat_{key}"][0], time.monotonic())
            return key

        @rs.action(
            outcome=rs.Outcome.Unchanged, concurrency=rs.ActionConcurrency.Exclusive
        )
        @classmethod
        def purge(cls, ctx):
            import time

            start = time.monotonic()
            started.set()
            assert release.wait(timeout=30), "test driver never released the action"
            windows["purge"] = (start, time.monotonic())

    return Events


# Internal budget (Event waits + thread joins) exceeds the 60s project-wide
# `timeout`, and `timeout_method = "thread"` calls os._exit(1) — which kills the
# whole pytest run with no report. Raise the ceiling so this test's own
# assertions fire first and name what actually stalled.
@pytest.mark.timeout(180)
def test_exclusive_action_does_not_block_a_different_partition():
    """The point of scoping the pool by partition: `purge(p1)` holds the asset's
    implicit pool, but a materialize of p2 touches different data and must not
    queue behind it."""
    import threading

    started, release, windows = threading.Event(), threading.Event(), {}
    repo = rs.CodeRepository(
        assets=[_slow_partitioned_exclusive(started, release, windows)],
        default_executor=IP,
    )
    for p in ("p1", "p2"):
        repo.materialize(partition_key=rs.PartitionKey.single(p))
    windows.clear()

    action_result = {}
    t_action = threading.Thread(
        target=lambda: action_result.update(
            r=repo.run_action(
                "purge",
                partition_key=rs.PartitionKey.single("p1"),
                raise_on_error=False,
            )
        )
    )
    t_action.start()
    assert started.wait(timeout=30)

    result = {}
    t_mat = threading.Thread(
        target=lambda: result.update(
            r=repo.materialize(partition_key=rs.PartitionKey.single("p2"))
        )
    )
    t_mat.start()
    try:
        t_mat.join(timeout=60)
        assert not t_mat.is_alive(), "materialize of p2 never finished"
        # The assertions below only mean anything while purge(p1) still holds
        # the pool — otherwise they pass vacuously on a slow machine.
        assert t_action.is_alive(), (
            "purge(p1) already exited, so this proves nothing about scoping"
        )
        assert result["r"].success
        assert windows["mat_p2"][1] is not None
        waiting = [
            e.event_type for e in repo.storage.get_events_for_run(result["r"].run_id)
        ]
        assert "StepSlotWaiting" not in waiting, (
            "a disjoint partition must not wait on the pool"
        )
    finally:
        release.set()
        t_action.join(timeout=60)

    # A bare thread swallows the action's outcome, so assert it: otherwise a
    # failed purge leaves the timing assertions above proving nothing.
    assert action_result["r"].success
    # The action really did outlive the disjoint materialize.
    assert windows["purge"][1] >= windows["mat_p2"][1]


# Internal budget (Event waits + thread joins) exceeds the 60s project-wide
# `timeout`, and `timeout_method = "thread"` calls os._exit(1) — which kills the
# whole pytest run with no report. Raise the ceiling so this test's own
# assertions fire first and name what actually stalled.
@pytest.mark.timeout(180)
def test_exclusive_action_still_blocks_its_own_partition():
    """The other half: the same partition must still serialize."""
    import threading

    started, release, windows = threading.Event(), threading.Event(), {}
    repo = rs.CodeRepository(
        assets=[_slow_partitioned_exclusive(started, release, windows)],
        default_executor=IP,
    )
    repo.materialize(partition_key=rs.PartitionKey.single("p1"))
    windows.clear()

    action_result = {}
    t_action = threading.Thread(
        target=lambda: action_result.update(
            r=repo.run_action(
                "purge",
                partition_key=rs.PartitionKey.single("p1"),
                raise_on_error=False,
            )
        )
    )
    t_action.start()
    assert started.wait(timeout=30)

    result = {}
    t_mat = threading.Thread(
        target=lambda: result.update(
            r=repo.materialize(partition_key=rs.PartitionKey.single("p1"))
        )
    )
    t_mat.start()
    try:
        # Wait for the materialize to actually reach the claim loop and report
        # itself blocked, rather than sleeping and hoping.
        def _is_waiting():
            return any(
                e.event_type == "StepSlotWaiting"
                for r in repo.storage.get_runs(limit=10)
                for e in repo.storage.get_events_for_run(r.run_id)
            )

        wait_until(_is_waiting, timeout=30)
    finally:
        release.set()
        t_action.join(timeout=60)
        t_mat.join(timeout=60)

    # A bare thread swallows the action's outcome, so assert it: otherwise a
    # failed purge leaves the timing assertions above proving nothing.
    assert action_result["r"].success
    assert result["r"].success
    # Same partition — the materialize body may only run after purge released.
    assert windows["mat_p1"][0] >= windows["purge"][1]
    waiting = [
        e.event_type for e in repo.storage.get_events_for_run(result["r"].run_id)
    ]
    assert "StepSlotWaiting" in waiting


# Internal budget (Event waits + thread joins) exceeds the 60s project-wide
# `timeout`, and `timeout_method = "thread"` calls os._exit(1) — which kills the
# whole pytest run with no report. Raise the ceiling so this test's own
# assertions fire first and name what actually stalled.
@pytest.mark.timeout(180)
def test_two_exclusive_actions_on_different_partitions_overlap():
    """The reported bug, directly: a partition-by-partition `delete` backfill
    serialized every run on one asset-wide lock and the tail timed out at
    CLAIM_TIMEOUT. Two actions on disjoint partitions must now overlap."""
    import threading

    started, release, windows = threading.Event(), threading.Event(), {}
    repo = rs.CodeRepository(
        assets=[_slow_partitioned_exclusive(started, release, windows)],
        default_executor=IP,
    )
    for p in ("p1", "p2"):
        repo.materialize(partition_key=rs.PartitionKey.single(p))

    # purge(p1) blocks until released, holding the asset's implicit pool.
    first_result = {}
    t1 = threading.Thread(
        target=lambda: first_result.update(
            r=repo.run_action(
                "purge",
                partition_key=rs.PartitionKey.single("p1"),
                raise_on_error=False,
            )
        )
    )
    t1.start()
    assert started.wait(timeout=30)

    # purge(p2) is a second *exclusive* action. It must not queue behind p1.
    started.clear()
    result = {}
    t2 = threading.Thread(
        target=lambda: result.update(
            r=repo.run_action("purge", partition_key=rs.PartitionKey.single("p2"))
        )
    )
    t2.start()
    try:
        assert started.wait(timeout=30), (
            "purge(p2) never entered its body — it is queued behind purge(p1)"
        )
        assert t1.is_alive(), "purge(p1) exited early, so the overlap proves nothing"
    finally:
        release.set()
        t1.join(timeout=60)
        t2.join(timeout=60)
    # A bare thread swallows the first action's outcome, so assert it too.
    assert first_result["r"].success
    assert result["r"].success
    waiting = [
        e.event_type for e in repo.storage.get_events_for_run(result["r"].run_id)
    ]
    assert "StepSlotWaiting" not in waiting


def test_action_attribute_survives_cloudpickle(tmp_path):
    """The ``optimize = some_action`` spelling ships to a loky worker.

    A locally-defined class asset can't be resolved by import path, so the
    whole class travels by value. An ``AssetAction`` attribute that can't
    pickle takes the task down with it — and the docs present that spelling and
    ``@rs.action`` as equivalent.
    """
    import cloudpickle
    import obstore.store

    def _opt(ctx):
        return None

    shared = rs.AssetAction(
        name="optimize",
        outcome=rs.Outcome.Unchanged,
        concurrency=rs.ActionConcurrency.Exclusive,
        description="clean up",
    )(_opt)

    # cloudpickle is what loky ships tasks with; the bound body is a local fn.
    revived = cloudpickle.loads(cloudpickle.dumps(shared))
    assert revived.name == "optimize"
    assert revived.outcome == rs.Outcome.Unchanged
    assert revived.exclusive is True
    assert revived.description == "clean up"

    handler = rs.PickleIOHandler(
        store=obstore.store.LocalStore(str(tmp_path), mkdir=True)
    )

    # Two assets so the level is 2-wide and really crosses the loky transport.
    class LocalA(rs.Asset):
        io_handler = handler
        optimize = shared

        @classmethod
        def materialize(cls) -> int:
            return 1

    class LocalB(rs.Asset):
        io_handler = handler
        optimize = shared

        @classmethod
        def materialize(cls) -> int:
            return 2

    repo = rs.CodeRepository(assets=[LocalA, LocalB], default_executor=MP)
    assert repo.materialize().success
    assert repo.load_node("local_a") == 1
    assert repo.load_node("local_b") == 2


# ---------------------------------------------------------------------------
# Dynamic outcome reporting (ActionResult / MayMaterialize)
# ---------------------------------------------------------------------------


class TestActionResultAccessors:
    """An ``ActionResult`` must be readable from Python.

    The ``materialized`` field shares its name with the ``materialized()``
    constructor, so a plain ``#[pyo3(get)]`` would be shadowed by the
    staticmethod and read as a truthy bound function for every result,
    including ``unchanged()``. The outcome is exposed under its own name.
    """

    def test_unchanged_reports_no_materialization(self):
        r = rs.ActionResult.unchanged(metadata={"files_scanned": 12})
        assert r.is_materialized is False
        assert r.data_version is None
        assert r.metadata == {"files_scanned": rs.MetadataValue.int(12)}

    def test_materialized_reports_materialization(self):
        r = rs.ActionResult.materialized(metadata={"rows_merged": 3}, data_version="v9")
        assert r.is_materialized is True
        assert r.data_version == "v9"
        assert r.metadata == {"rows_merged": rs.MetadataValue.int(3)}

    def test_metadata_defaults_to_empty(self):
        assert rs.ActionResult.unchanged().metadata == {}


class _MergeTable(rs.Asset):
    io_handler = rs.InMemoryIOHandler()
    late_rows = 0

    @classmethod
    def materialize(cls):
        return 1

    @rs.action(outcome=rs.Outcome.MayMaterialize)
    @classmethod
    def merge_late(cls, ctx):
        if cls.late_rows == 0:
            return rs.ActionResult.unchanged()
        return rs.ActionResult.materialized(
            metadata={"rows_merged": cls.late_rows}, data_version="merged-v2"
        )


def test_no_op_merge_does_not_cascade():
    class Table(_MergeTable):
        late_rows = 0

    repo = rs.CodeRepository(assets=[Table], default_executor=IP)
    repo.materialize()
    result = repo.run_action("merge_late")

    assert result.success
    types = event_types(repo, result.run_id)
    assert "ActionCompleted" in types
    assert "Materialization" not in types


def test_merge_with_data_emits_materialization():
    class Table(_MergeTable):
        late_rows = 7

    repo = rs.CodeRepository(assets=[Table], default_executor=IP)
    repo.materialize()
    result = repo.run_action("merge_late")

    assert result.success
    events = repo.storage.get_events_for_run(result.run_id)
    mats = [e for e in events if e.event_type == "Materialization"]
    assert len(mats) == 1
    assert mats[0].data_version == "merged-v2"
    meta = dict(mats[0].metadata)
    assert "rows_merged" in meta
    assert "ActionCompleted" not in [e.event_type for e in events]


def test_merge_preserves_upstream_provenance():
    """A merge consumes no upstream, so it must not erase what was consumed.

    Reporting `materialized()` writes an empty input-data-version list; if that
    reaches the asset row, the asset loses its provenance and reads Stale
    against a dependency nothing has touched.
    """

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def source() -> int:
        return 1

    class Derived(rs.Asset):
        io_handler = rs.InMemoryIOHandler()

        @classmethod
        def materialize(cls, source: int) -> int:
            return source + 1

        @rs.action(outcome=rs.Outcome.MayMaterialize)
        @classmethod
        def merge_late(cls, ctx):
            return rs.ActionResult.materialized(data_version="merged-v2")

    repo = rs.CodeRepository(assets=[source, Derived], default_executor=IP)
    repo.materialize()
    before = repo.storage.get_asset_record("derived").last_input_data_versions
    assert before, "precondition: derived consumed source"

    assert repo.run_action("merge_late", selection=["derived"]).success

    record = repo.storage.get_asset_record("derived")
    assert record.last_input_data_versions == before
    assert record.last_data_version == "merged-v2"
    assert repo.storage.compute_staleness()["derived"][0] == "UpToDate"


def test_unchanged_action_reporting_materialized_fails():
    class Table(rs.Asset):
        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def sneaky(cls, ctx):
            return rs.ActionResult.materialized()

    repo = rs.CodeRepository(assets=[Table], default_executor=IP)
    repo.materialize()
    result = repo.run_action("sneaky", raise_on_error=False)

    assert not result.success
    assert "declared Outcome.Unchanged" in result.failed_assets[0][1]


def test_garbage_action_return_fails():
    class Table(rs.Asset):
        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def oops(cls, ctx):
            return {"not": "an ActionResult"}

    repo = rs.CodeRepository(assets=[Table], default_executor=IP)
    repo.materialize()
    result = repo.run_action("oops", raise_on_error=False)

    assert not result.success
    assert "actions return rs.ActionResult or None" in result.failed_assets[0][1]


def test_action_inline_retry_policy_runs_the_ladder():
    attempts = []

    class Flaky(rs.Asset):
        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged, retry=rs.RetryPolicy(max_retries=2))
        @classmethod
        def wobbly(cls, ctx):
            attempts.append(1)
            if len(attempts) < 3:
                raise RuntimeError("transient")
            return None

    repo = rs.CodeRepository(assets=[Flaky], default_executor=IP)
    repo.materialize()
    result = repo.run_action("wobbly")

    assert result.success
    assert len(attempts) == 3
    types = event_types(repo, result.run_id)
    assert types.count("StepRetry") == 2


def test_action_named_retry_policy_runs_the_ladder():
    attempts = []

    class Table(rs.Asset):
        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged, retry="my_policy")
        @classmethod
        def verbed(cls, ctx):
            attempts.append(1)
            if len(attempts) < 3:
                raise RuntimeError("transient")
            return None

    repo = rs.CodeRepository(
        assets=[Table],
        default_executor=IP,
        retries={"my_policy": rs.RetryPolicy(max_retries=2)},
    )
    repo.materialize()
    result = repo.run_action("verbed")

    assert result.success
    assert len(attempts) == 3
    assert event_types(repo, result.run_id).count("StepRetry") == 2


def test_action_unknown_named_retry_rejected():
    class Table(rs.Asset):
        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unchanged, retry="missing")
        @classmethod
        def verbed(cls, ctx):
            return None

    repo = rs.CodeRepository(assets=[Table], default_executor=IP)
    repo.materialize()
    with pytest.raises(Exception, match="unknown retry policy 'missing'"):
        repo.run_action("verbed")


# ---------------------------------------------------------------------------
# Unmaterialize (delete)
# ---------------------------------------------------------------------------


class _Deletable(rs.Asset):
    io_handler = rs.InMemoryIOHandler()

    @classmethod
    def materialize(cls):
        return 1

    @rs.action(outcome=rs.Outcome.Unmaterialize)
    @classmethod
    def delete(cls, ctx):
        return None


def test_delete_clears_asset_record():
    class Table(_Deletable):
        pass

    repo = rs.CodeRepository(assets=[Table], default_executor=IP)
    repo.materialize()
    record = repo.storage.get_asset_record("table")
    assert record.last_data_version is not None

    result = repo.run_action("delete")
    assert result.success
    types = event_types(repo, result.run_id)
    assert "Deletion" in types
    assert "ActionCompleted" not in types

    record = repo.storage.get_asset_record("table")
    assert record.last_data_version is None
    # The deletion is the asset's last event, so timelines point at it — but
    # the asset holds no run's data anymore, so conditions read it as missing.
    assert record.last_event_id is not None
    assert record.last_run_id is None


def test_partitioned_delete_clears_only_that_partition():
    class Events(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = rs.PartitionsDefinition.static_(["p1", "p2"])

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext):
            return context.partition_key

        @rs.action(outcome=rs.Outcome.Unmaterialize)
        @classmethod
        def delete(cls, ctx):
            return None

    repo = rs.CodeRepository(assets=[Events], default_executor=IP)
    for p in ("p1", "p2"):
        repo.materialize(partition_key=rs.PartitionKey.single(p))

    def materialized_keys():
        return sorted(
            str(k) for k in repo.storage.get_materialized_partitions("events")
        )

    assert len(materialized_keys()) == 2

    result = repo.run_action("delete", partition_key=rs.PartitionKey.single("p1"))
    assert result.success

    remaining = materialized_keys()
    assert len(remaining) == 1
    assert "p2" in remaining[0]
    # Whole-asset state is untouched by a partition-scoped delete.
    assert repo.storage.get_asset_record("events").last_data_version is not None


def test_failed_partitioned_action_does_not_floor_the_partition():
    """A failed action is not a failed materialization attempt.

    A partition-scoped StepFailure feeds ``get_failed_partitions``, which the
    condition cache reads as "this partition failed to materialize" and uses to
    suppress the partition from ``eager()`` — wedging exactly the automation
    that would have to run to clear the floor.
    """

    class Events(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = rs.PartitionsDefinition.static_(["p1", "p2"])

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext):
            return context.partition_key

        @rs.action(outcome=rs.Outcome.Unmaterialize)
        @classmethod
        def delete(cls, ctx):
            raise RuntimeError("boom")

    repo = rs.CodeRepository(assets=[Events], default_executor=IP)
    pk = rs.PartitionKey.single("p1")
    repo.materialize(partition_key=pk)

    result = repo.run_action("delete", partition_key=pk, raise_on_error=False)
    assert not result.success

    events = repo.storage.get_events_for_run(result.run_id)
    failures = [e for e in events if e.event_type == "StepFailure"]
    assert failures, "the run itself must still report the failure"
    assert all(e.partition_key is None for e in failures), (
        "an action failure must not land as a per-partition materialization failure"
    )


def test_delete_reporting_unchanged_preserves_state():
    class Careful(rs.Asset):
        io_handler = rs.InMemoryIOHandler()

        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unmaterialize)
        @classmethod
        def delete(cls, ctx):
            return rs.ActionResult.unchanged()  # nothing to delete

    repo = rs.CodeRepository(assets=[Careful], default_executor=IP)
    repo.materialize()
    result = repo.run_action("delete")

    assert result.success
    types = event_types(repo, result.run_id)
    assert "Deletion" not in types
    assert "ActionCompleted" in types
    assert repo.storage.get_asset_record("careful").last_data_version is not None


def test_delete_reporting_materialized_fails():
    class Wrong(rs.Asset):
        @classmethod
        def materialize(cls):
            return 1

        @rs.action(outcome=rs.Outcome.Unmaterialize)
        @classmethod
        def delete(cls, ctx):
            return rs.ActionResult.materialized()

    repo = rs.CodeRepository(assets=[Wrong], default_executor=IP)
    repo.materialize()
    result = repo.run_action("delete", raise_on_error=False)

    assert not result.success
    assert "declared Outcome.Unmaterialize" in result.failed_assets[0][1]


def test_delete_backfill_over_partition_range():
    calls = []

    class Events(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = rs.PartitionsDefinition.static_(["p1", "p2", "p3"])

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext):
            return context.partition_key

        @rs.action(outcome=rs.Outcome.Unmaterialize)
        @classmethod
        def delete(cls, ctx):
            calls.append(ctx.partition_key)

    repo = rs.CodeRepository(assets=[Events], default_executor=IP)
    for p in ("p1", "p2", "p3"):
        repo.materialize(partition_key=rs.PartitionKey.single(p))

    res = repo.backfill(
        selection=["events"],
        partition_keys=[rs.PartitionKey.single("p1"), rs.PartitionKey.single("p2")],
        action="delete",
    )
    assert res.status == "CompletedSuccess"
    assert sorted(calls) == ["p1", "p2"]
    remaining = [str(k) for k in repo.storage.get_materialized_partitions("events")]
    assert len(remaining) == 1
    assert "p3" in remaining[0]


@pytest.mark.parametrize("executor", EXECUTORS)
@pytest.mark.parametrize("style", ["sync", "async"])
def test_batched_action_can_fail_one_partition(executor, style):
    """A batched action is not all-or-nothing.

    Over a key range, one corrupt partition must be reportable without either
    claiming it succeeded or throwing away the keys that did. The async backend
    takes its own branch through the invoke path, and the marks are read back
    off the context *after* the call returns — so both styles must be covered.
    """
    if style == "sync":

        def _delete(ctx):
            for key in ctx.partition.keys:
                if "p2" in str(key):
                    ctx.mark_partition_failed(key, "corrupt segment")
    else:

        async def _delete(ctx):
            await asyncio.sleep(0)
            for key in ctx.partition.keys:
                if "p2" in str(key):
                    ctx.mark_partition_failed(key, "corrupt segment")

    delete = rs.AssetAction(name="delete", outcome=rs.Outcome.Unmaterialize)(_delete)

    class Events(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = rs.PartitionsDefinition.static_(["p1", "p2", "p3"])
        actions = [delete]

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext):
            return context.partition_key

    repo = rs.CodeRepository(assets=[Events], default_executor=executor)
    for p in ("p1", "p2", "p3"):
        repo.materialize(partition_key=rs.PartitionKey.single(p))

    res = repo.backfill(
        selection=["events"],
        partition_keys=[rs.PartitionKey.single(p) for p in ("p1", "p2", "p3")],
        action="delete",
        strategy=rs.BackfillStrategy.single_run(),
    )
    # The point of the fix: 2 keys done, 1 reported failed — not all-or-nothing.
    assert (res.completed, res.failed) == (2, 1)

    # p2 was marked failed, so its state survives; p1/p3 are gone.
    remaining = sorted(
        str(k) for k in repo.storage.get_materialized_partitions("events")
    )
    assert len(remaining) == 1, remaining
    assert "p2" in remaining[0]


def _batched_action_repo(body):
    delete = rs.AssetAction(name="delete", outcome=rs.Outcome.Unmaterialize)(body)

    class Events(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = rs.PartitionsDefinition.static_(["p1", "p2"])
        actions = [delete]

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext):
            return context.partition_key

    repo = rs.CodeRepository(assets=[Events], default_executor=IP)
    for p in ("p1", "p2"):
        repo.materialize(partition_key=rs.PartitionKey.single(p))
    return repo


def _run_batched(repo):
    return repo.backfill(
        selection=["events"],
        partition_keys=[rs.PartitionKey.single(p) for p in ("p1", "p2")],
        action="delete",
        strategy=rs.BackfillStrategy.single_run(),
    )


def test_batched_action_partition_key_is_ambiguous():
    """A batched run has no single key — `ctx.partition_key` must refuse and
    point at `ctx.partition.keys`, not hand back an arbitrary member."""
    seen = {}

    def _delete(ctx):
        try:
            _ = ctx.partition_key
        except rs.exceptions.PartitionValidationError as e:
            seen["err"] = str(e)

    repo = _batched_action_repo(_delete)
    assert _run_batched(repo).completed == 2
    assert "ambiguous" in seen["err"] and "partition.keys" in seen["err"]


def test_action_mark_partition_failed_rejects_foreign_key():
    """Marking a key outside the batch is a body bug — rejected, so a typo
    can't silently exempt a real key from the outcome."""
    seen = {}

    def _delete(ctx):
        try:
            ctx.mark_partition_failed(rs.PartitionKey.single("zz"), "typo")
        except Exception as e:
            seen["err"] = f"{type(e).__name__}: {e}"

    repo = _batched_action_repo(_delete)
    assert _run_batched(repo).completed == 2
    assert "not in this context's partition keys" in seen["err"]


# ---------------------------------------------------------------------------
# Action config: ActionContext[Config]
# ---------------------------------------------------------------------------


class TuneConfig(BaseModel):
    target_size_mb: int = 128
    force: bool = False


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
            seen[ctx.asset_key] = (ctx.config.target_size_mb, ctx.config.force)

    else:

        async def _tune(ctx: rs.ActionContext[TuneConfig]):
            await asyncio.sleep(0)
            seen[ctx.asset_key] = (ctx.config.target_size_mb, ctx.config.force)

    tune = rs.AssetAction(name="tune", outcome=rs.Outcome.Unchanged)(_tune)

    @rs.Asset(actions=[tune])
    def orders() -> int:
        return 1

    repo = rs.CodeRepository(assets=[orders], default_executor=executor)
    repo.materialize()

    assert repo.run_action("tune").success
    assert seen == {"orders": (128, False)}

    assert repo.run_action(
        "tune", config={"orders": {"target_size_mb": 512, "force": True}}
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
    assert repo.run_action("tune", config={"event_log": {"target_size_mb": 64}}).success
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
    assert repo.run_action("plain", config={"orders": {"target_size_mb": 1}}).success
    assert repo.run_action("bare", config={"orders": {"target_size_mb": 1}}).success
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
        config={"orders": {"target_size_mb": "not-an-int"}},
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
    assert repo.run_action("observe", config={"feed": {"target_size_mb": 42}}).success
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
            seen[ctx.asset_key] = probe.prefix

    else:

        async def _tag(ctx, probe: ProbeResource):
            await asyncio.sleep(0)
            seen[ctx.asset_key] = probe.prefix

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
    assert repo.run_action("tag", config={"orders": {"target_size_mb": 9}}).success
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
