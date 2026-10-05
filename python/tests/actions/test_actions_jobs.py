import pytest

import rivers as rs
from _helpers import IP, event_types
from _polling import wait_for_run_terminal, wait_until
from rivers._core import AutomationDaemon
from rivers.exceptions import GraphValidationError


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
            calls.append(ctx.asset_name)

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


def _vacuumable(name, keys, partitioning):
    vacuum = rs.AssetAction(
        name="vacuum", outcome=rs.Outcome.Unchanged, partitioning=partitioning
    )(lambda ctx: None)
    return rs.Asset(
        name=name,
        io_handler=rs.InMemoryIOHandler(),
        partitions_def=rs.PartitionsDefinition.static_(keys),
        actions=[vacuum],
    )(lambda context: context.partition_key)


def test_whole_asset_action_job_skips_partition_compatibility():
    """A job over assets with disjoint partition definitions can never share a
    key — which only matters when its verb takes one. A whole-asset vacuum job
    across a daily and a regional table failed resolve() for the whole code
    location, although `run_action("vacuum")` over the same assets works."""
    daily = _vacuumable("daily", ["2024-01-01"], rs.ActionPartitioning.Keyless)
    region = _vacuumable("region", ["eu", "us"], rs.ActionPartitioning.Keyless)
    repo = rs.CodeRepository(
        assets=[daily, region],
        jobs=[rs.Job(name="fleet_vacuum", assets=[daily, region], action="vacuum")],
        default_executor=IP,
    )
    assert repo.get_job("fleet_vacuum").execute().success

    # A materialize job over the same assets is still unrunnable and rejected.
    with pytest.raises(Exception, match="incompatible partition definitions"):
        rs.CodeRepository(
            assets=[daily, region],
            jobs=[rs.Job(name="fleet", assets=[daily, region])],
            default_executor=IP,
        ).resolve()


def test_action_job_mixing_keyed_and_whole_asset_targets_is_rejected():
    """One run has one partition key: a verb that is whole-asset on one target
    and needs a key on another can never run as one job — say so at resolve."""
    daily = _vacuumable("daily", ["2024-01-01"], rs.ActionPartitioning.Keyless)
    region = _vacuumable("region", ["2024-01-01"], rs.ActionPartitioning.Required)
    with pytest.raises(
        Exception, match="whole-asset on .*daily.* but needs a key on .*region"
    ):
        rs.CodeRepository(
            assets=[daily, region],
            jobs=[rs.Job(name="mixed", assets=[daily, region], action="vacuum")],
            default_executor=IP,
        ).resolve()


def _queued_vacuum_repo(storage, calls, **repo_kwargs):
    """Partitioned table with a whole-asset `vacuum` and an Optional `purge`,
    each behind a Job, in run-queue mode."""

    class Events(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = rs.PartitionsDefinition.static_(["p1", "p2"])

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext):
            return context.partition_key

        @rs.action(
            outcome=rs.Outcome.Unchanged,
            partitioning=rs.ActionPartitioning.Keyless,
        )
        @classmethod
        def vacuum(cls, ctx):
            calls.append(("vacuum", ctx.has_partition_key))

        @rs.action(
            outcome=rs.Outcome.Unmaterialize,
            partitioning=rs.ActionPartitioning.Optional,
        )
        @classmethod
        def purge(cls, ctx):
            calls.append(("purge", ctx.has_partition_key))

    repo = rs.CodeRepository(
        assets=[Events],
        jobs=[
            rs.Job(name="nightly_vacuum", assets=[Events], action="vacuum"),
            rs.Job(name="purge_job", assets=[Events], action="purge"),
        ],
        default_executor=IP,
        run_queue=rs.RunQueueConfig(max_concurrent_runs=2, dequeue_interval="50ms"),
        **repo_kwargs,
    )
    repo.resolve(storage=storage)
    return repo


def test_queued_action_job_validates_partitions_for_its_verb(storage):
    """The run queue must apply the job's verb to the partition check — it
    checked materialize rules, so a whole-asset verb could never be queued
    without a key, and a key it must refuse was accepted and failed at launch."""
    repo = _queued_vacuum_repo(storage, [])

    handle = repo._submit_run(job_name="nightly_vacuum")
    run = repo.storage.get_run(handle.run_id)
    assert (run.status, run.action, run.partition_key) == ("Queued", "vacuum", None)

    with pytest.raises(Exception, match="Action 'vacuum' is whole-asset"):
        repo._submit_run(
            job_name="nightly_vacuum", partition_key=rs.PartitionKey.single("p1")
        )

    # An Optional verb takes either form.
    keyless = repo.storage.get_run(repo._submit_run(job_name="purge_job").run_id)
    assert (keyless.action, keyless.partition_key) == ("purge", None)
    keyed = repo._submit_run(
        job_name="purge_job", partition_key=rs.PartitionKey.single("p2")
    )
    assert repo.storage.get_run(keyed.run_id).action == "purge"


def test_scheduled_whole_asset_action_job_runs_through_the_queue(storage):
    """The documented nightly-maintenance pattern — a Schedule over an action
    Job — on a partitioned asset in run-queue mode. Every tick failed with
    "Cannot run 'materialize' without partition_key" before."""
    calls = []

    @rs.Schedule(
        cron_schedule="* * * * * *",
        job_name="nightly_vacuum",
        name="vacuum_schedule",
        default_status=rs.ScheduleStatus.Running,
    )
    def vacuum_schedule(context: rs.ScheduleEvaluationContext):
        return rs.RunRequest()

    repo = _queued_vacuum_repo(storage, calls, schedules=[vacuum_schedule])
    daemon = AutomationDaemon(repo=repo, storage=storage, condition_eval_interval="1m")
    daemon.start()
    try:
        assert wait_until(lambda: ("vacuum", False) in calls, timeout=20), (
            f"no vacuum ran; runs: {[(r.status, r.action) for r in storage.get_runs(limit=20)]}"
        )
    finally:
        daemon.stop()

    ran = [r for r in storage.get_runs(limit=20) if r.action == "vacuum"]
    assert ran and all(r.partition_key is None for r in ran)
    assert any(r.status == "Success" for r in ran)


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
