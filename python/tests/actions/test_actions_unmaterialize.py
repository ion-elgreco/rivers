import asyncio

import pytest

import rivers as rs
from _helpers import EXECUTORS, IP, event_types


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


@pytest.mark.parametrize("executor", EXECUTORS)
@pytest.mark.parametrize("style", ["sync", "async"])
def test_downstream_first_keeps_the_source_of_a_failed_key(executor, style):
    """DownstreamFirst deletes a rollup before its source, so a failure leaves a
    missing rollup, never a rollup built from deleted data. In a batched run
    the rollup step can fail one key; the source step must then leave that key
    alone. The ordering read only step-level failures, so the source step
    deleted every key of the batch."""
    seen = {}

    def record(ctx):
        seen[ctx.asset_name] = sorted(str(k) for k in ctx.partition.keys)
        if ctx.asset_name == "rollups":
            for key in ctx.partition.keys:
                if "p2" in str(key):
                    ctx.mark_partition_failed(key, "rollup locked")

    if style == "sync":

        def _purge(ctx):
            record(ctx)
    else:

        async def _purge(ctx):
            await asyncio.sleep(0)
            record(ctx)

    purge = rs.AssetAction(
        name="purge",
        outcome=rs.Outcome.Unmaterialize,
        ordering=rs.ActionOrdering.DownstreamFirst,
    )(_purge)

    class Events(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = rs.PartitionsDefinition.static_(["p1", "p2", "p3"])
        actions = [purge]

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext):
            return context.partition_key

    class Rollups(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = rs.PartitionsDefinition.static_(["p1", "p2", "p3"])
        actions = [purge]

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext, events):
            return events

    repo = rs.CodeRepository(assets=[Events, Rollups], default_executor=executor)
    for p in ("p1", "p2", "p3"):
        repo.materialize(partition_key=rs.PartitionKey.single(p))

    repo.backfill(
        selection=["events", "rollups"],
        partition_keys=[rs.PartitionKey.single(p) for p in ("p1", "p2", "p3")],
        action="purge",
        strategy=rs.BackfillStrategy.single_run(),
    )

    assert len(seen["rollups"]) == 3
    assert len(seen["events"]) == 2 and not any("p2" in k for k in seen["events"]), (
        f"the source step acted on the key its rollup failed: {seen['events']}"
    )
    for asset in ("events", "rollups"):
        remaining = [str(k) for k in repo.storage.get_materialized_partitions(asset)]
        assert len(remaining) == 1 and "p2" in remaining[0], (asset, remaining)


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
