"""A step's claim on its asset's pool covers the step's write and event time.

An Exclusive verb and a materialize of the same partitions never overlap. So
a step keeps its claim until its data is written and its event is stamped.
Otherwise a waiting verb runs during the write, or an event stamped late
overrides an event that really happened after it.
"""

import asyncio
import os
import threading
import time

import obstore.store
import pytest

import rivers as rs
from _polling import wait_until

IP = rs.Executor.in_process()
MP = rs.Executor.parallel(max_workers=2)

P1 = rs.PartitionKey.single("p1")


def _events(repo, run_id, kind, asset_key):
    return [
        e
        for e in repo.storage.get_events_for_run(run_id)
        if e.event_type == kind and e.asset_key == asset_key
    ]


def _materialized_keys(repo, asset_key):
    return sorted(str(k) for k in repo.storage.get_materialized_partitions(asset_key))


def _delete_action(body):
    return rs.AssetAction(
        name="delete",
        outcome=rs.Outcome.Unmaterialize,
        concurrency=rs.ActionConcurrency.Exclusive,
        partitioning=rs.ActionPartitioning.Optional,
    )(body)


# Internal budget (Event waits + thread joins) exceeds the 60s project-wide
# `timeout`, and `timeout_method = "thread"` calls os._exit(1). Raise the
# ceiling so the test's own assertions fire first.
@pytest.mark.timeout(180)
@pytest.mark.parametrize(
    ("executor", "style"),
    [
        pytest.param(IP, "sync", id="in_process-sync"),
        pytest.param(IP, "async", id="in_process-async"),
        pytest.param(MP, "sync", id="parallel-sync"),
        pytest.param(MP, "async", id="parallel-async"),
    ],
)
def test_exclusive_verb_waits_for_the_materialize_write(executor, style):
    """The step freed the asset's pool before its IO handler wrote the data,
    so a waiting Exclusive verb ran during the write: on a Delta table, two
    transactions at once. A one-step level on the parallel executor runs in
    this process, like the in-process executor."""
    writing = threading.Event()
    release = threading.Event()
    order = []

    class SlowWriter(rs.BaseIOHandler):
        def handle_output(self, context, obj):
            writing.set()
            assert release.wait(timeout=60), "the driver never released the write"
            order.append("write")

        def load_input(self, context):
            return 1

    optimize = rs.AssetAction(
        name="optimize",
        outcome=rs.Outcome.Unchanged,
        concurrency=rs.ActionConcurrency.Exclusive,
    )(lambda ctx: order.append("optimize"))

    if style == "sync":

        @rs.Asset(io_handler=SlowWriter(), actions=[optimize])
        def table() -> int:
            return 1

    else:

        @rs.Asset(io_handler=SlowWriter(), actions=[optimize])
        async def table() -> int:
            return 1

    repo = rs.CodeRepository(assets=[table], default_executor=executor)
    mat, act = {}, {}
    t_mat = threading.Thread(
        target=lambda: mat.update(r=repo.materialize(raise_on_error=False))
    )
    t_act = threading.Thread(
        target=lambda: act.update(r=repo.run_action("optimize", raise_on_error=False))
    )
    t_mat.start()
    try:
        assert writing.wait(timeout=30), "the write never started"
        t_act.start()

        def verb_waits_or_ran():
            if order:
                return True
            return any(
                e.event_type == "StepSlotWaiting"
                for r in repo.storage.get_runs(limit=10)
                if r.action == "optimize"
                for e in repo.storage.get_events_for_run(r.run_id)
            )

        assert wait_until(verb_waits_or_ran, timeout=30), "the verb never started"
        assert order == [], "the verb ran while the materialize was still writing"
    finally:
        release.set()
        t_mat.join(timeout=60)
        if t_act.ident is not None:
            t_act.join(timeout=60)

    assert mat["r"].success
    assert act["r"].success
    assert order == ["write", "optimize"]


@pytest.mark.timeout(180)
def test_delete_of_a_key_a_parallel_step_wrote_is_not_undone(tmp_path):
    """A worker wrote its key and its step freed the asset's pool, but the
    step's Materialization was stamped only when the whole level finished. A
    delete of the key in between was undone by that later stamp: the key read
    materialized while its data was gone."""
    handler = rs.PickleIOHandler(
        store=obstore.store.LocalStore(str(tmp_path / "data"), mkdir=True)
    )
    fast_ran = tmp_path / "fast_ran"
    release = tmp_path / "release_slow"
    delete = _delete_action(lambda ctx: None)
    pd = rs.PartitionsDefinition.static_(["p1", "p2"])

    @rs.Asset(io_handler=handler, partitions_def=pd, actions=[delete])
    def fast() -> int:
        fast_ran.write_text(str(os.getpid()))
        return 1

    @rs.Asset(io_handler=handler, partitions_def=pd, actions=[delete])
    def slow() -> int:
        deadline = time.monotonic() + 90
        while not release.exists():
            assert time.monotonic() < deadline, "the driver never released slow"
            time.sleep(0.05)
        return 2

    repo = rs.CodeRepository(assets=[fast, slow], default_executor=MP)
    mat = {}
    t_mat = threading.Thread(
        target=lambda: mat.update(
            r=repo.materialize(partition_key=P1, raise_on_error=False)
        )
    )
    t_mat.start()
    try:
        assert wait_until(fast_ran.exists, timeout=90), "fast never ran"
        deleted = repo.run_action("delete", selection=["fast"], partition_key=P1)
        assert t_mat.is_alive(), "the level finished before the delete"
    finally:
        release.touch()
        t_mat.join(timeout=90)

    assert mat["r"].success, mat["r"].failed_assets
    assert deleted.success
    assert int(fast_ran.read_text()) != os.getpid(), "fast did not run in a worker"
    assert _materialized_keys(repo, "fast") == []
    assert _materialized_keys(repo, "slow") == ['PartitionKey("p1")']
    [materialized] = _events(repo, mat["r"].run_id, "Materialization", "fast")
    [deletion] = _events(repo, deleted.run_id, "Deletion", "fast")
    assert materialized.timestamp < deletion.timestamp


@pytest.mark.timeout(180)
def test_materialize_of_a_key_an_async_delete_cleared_is_not_undone():
    """The same on the verb side: an async delete freed the asset's pool when
    its body returned, but its Deletion was stamped only when the whole level
    finished. A materialize of the key in between was undone by that later
    stamp: the key read missing while its data existed."""
    fast_deleted = threading.Event()
    release = threading.Event()

    async def _delete(ctx):
        if ctx.asset_name == "slow_table":
            while not release.is_set():
                await asyncio.sleep(0.05)
        else:
            fast_deleted.set()

    delete = _delete_action(_delete)
    pd = rs.PartitionsDefinition.static_(["p1", "p2"])

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), partitions_def=pd, actions=[delete])
    def fast_table() -> int:
        return 1

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), partitions_def=pd, actions=[delete])
    def slow_table() -> int:
        return 2

    repo = rs.CodeRepository(assets=[fast_table, slow_table], default_executor=IP)
    repo.materialize(partition_key=P1)

    act = {}
    t_act = threading.Thread(
        target=lambda: act.update(
            r=repo.run_action("delete", partition_key=P1, raise_on_error=False)
        )
    )
    t_act.start()
    try:
        assert fast_deleted.wait(timeout=30), "the fast delete never ran"
        rematerialized = repo.materialize(selection=["fast_table"], partition_key=P1)
        assert t_act.is_alive(), "the delete run finished before the materialize"
    finally:
        release.set()
        t_act.join(timeout=60)

    assert act["r"].success
    assert rematerialized.success
    assert _materialized_keys(repo, "fast_table") == ['PartitionKey("p1")']
    assert _materialized_keys(repo, "slow_table") == []
    [deletion] = _events(repo, act["r"].run_id, "Deletion", "fast_table")
    [materialized] = _events(
        repo, rematerialized.run_id, "Materialization", "fast_table"
    )
    assert deletion.timestamp < materialized.timestamp


@pytest.mark.timeout(180)
@pytest.mark.parametrize("style", ["sync", "async"])
def test_delete_of_a_graph_asset_its_final_task_wrote_is_not_undone(style):
    """A graph asset's final task writes the graph's data, but the graph's
    Materialization was stamped one level later. A delete admitted in between
    (here, while a slow sibling kept the level open) was undone by that later
    stamp: the asset read materialized while its data was gone."""
    final_ran = threading.Event()
    sibling_started = threading.Event()
    release = threading.Event()

    if style == "sync":

        @rs.Task
        def build_report() -> int:
            final_ran.set()
            return 1

    else:

        @rs.Task
        async def build_report() -> int:
            final_ran.set()
            return 1

    class Report(rs.GraphAsset):
        io_handler = rs.InMemoryIOHandler()

        @classmethod
        def compose(cls):
            return build_report()

        @rs.action(
            outcome=rs.Outcome.Unmaterialize,
            concurrency=rs.ActionConcurrency.Exclusive,
        )
        @classmethod
        def delete(cls, ctx):
            return None

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    async def sibling() -> int:
        sibling_started.set()
        while not release.is_set():
            await asyncio.sleep(0.05)
        return 2

    repo = rs.CodeRepository(
        assets=[Report, sibling], tasks=[build_report], default_executor=IP
    )
    mat = {}
    t_mat = threading.Thread(
        target=lambda: mat.update(r=repo.materialize(raise_on_error=False))
    )
    t_mat.start()
    try:
        assert final_ran.wait(timeout=30), "the final task never ran"
        assert sibling_started.wait(timeout=30), "the sibling never started"
        deleted = repo.run_action("delete", selection=["report"])
        assert t_mat.is_alive(), "the level finished before the delete"
    finally:
        release.set()
        t_mat.join(timeout=60)

    assert mat["r"].success, mat["r"].failed_assets
    assert deleted.success
    record = repo.storage.get_asset_record("report")
    assert record.last_run_id is None
    assert record.last_timestamp is None
    [materialized] = _events(repo, mat["r"].run_id, "Materialization", "report")
    [deletion] = _events(repo, deleted.run_id, "Deletion", "report")
    assert materialized.timestamp < deletion.timestamp
