import os
import subprocess
import sys
import textwrap

import pytest

import rivers as rs
from _helpers import EXECUTORS, IP, MP, event_types
from _polling import wait_until


# Module-level so loky workers can import them by reference.
@rs.Task
def _fan_echo(x: int) -> int:
    return x


@rs.Task
def _fan_total(values: list) -> int:
    return sum(values)


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


@pytest.mark.parametrize("executor", EXECUTORS)
@pytest.mark.parametrize(
    "resume", [pytest.param(False, id="retry-pod"), pytest.param(True, id="resume")]
)
def test_rerun_step_takes_over_its_killed_attempts_pool_slot(
    storage, tmp_path, executor, resume
):
    """A materialize step killed mid-body (a pod OOM kill) never releases its
    asset-pool slot. The Kubernetes retry pod, or a ``--resume``, runs the step
    again under the same run and step key. Its claim collided with the leftover
    row, so the step failed with "Failed to claim pool slots" and never ran."""
    import obstore.store

    handler = rs.PickleIOHandler(
        store=obstore.store.LocalStore(str(tmp_path), mkdir=True)
    )

    class Ledger(rs.Asset):
        io_handler = handler

        @classmethod
        def materialize(cls):
            return 1

        @rs.action(
            outcome=rs.Outcome.Unchanged, concurrency=rs.ActionConcurrency.Exclusive
        )
        @classmethod
        def optimize(cls, ctx):
            return None

    repo = rs.CodeRepository(assets=[Ledger], default_executor=executor)
    repo.resolve(storage=storage)
    run_id = "killed-run"
    pool = "__asset__:ledger"
    # The killed attempt's claim, never released.
    storage._claim_concurrency_slots([(pool, 1)], run_id, "ledger")

    result = repo.materialize(
        selection=["ledger"],
        run_id_override=run_id,
        resume=resume,
        raise_on_error=False,
    )

    assert result.success, result.failed_assets
    events = sorted(
        (e.event_type, e.asset_key)
        for e in storage.get_events_for_run(run_id)
        if e.event_type in ("StepSlotClaimed", "Materialization")
    )
    assert events == [("Materialization", "ledger"), ("StepSlotClaimed", "ledger")]
    assert storage.get_pool_slot_holders(pool) == []


def test_resume_skips_completed_action_steps():
    """A crashed action run resumes instead of re-running its side effects.

    The K8s operator restarts a crashed executor pod with ``--resume``
    unconditionally, so without this every already-completed delete/compact
    step runs its side effect a second time. A step that already failed stays
    failed: an action without a retry policy runs at most once.
    """
    calls = []

    class Base(rs.Asset):
        io_handler = rs.InMemoryIOHandler()

        @rs.action(outcome=rs.Outcome.Unchanged)
        @classmethod
        def purge(cls, ctx):
            calls.append(ctx.asset_name)
            if ctx.asset_name == "second" and len(calls) < 3:
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
    assert not result.success
    assert calls.count("first") == 1, "a completed action step re-ran its side effect"
    assert calls.count("second") == 1, (
        "a failed action step without a retry policy re-ran"
    )


_INTERRUPTED_ACTION_SCRIPT = textwrap.dedent(
    """
    import os, sys, threading, time
    import rivers as rs
    from rivers.testing import embedded_storage

    path, phase, verb, calls_file, style = sys.argv[1:6]

    def body(ctx):
        with open(calls_file, "a") as f:
            f.write(ctx.asset_name + "\\n")
        if phase == "crash-at-once":
            # Die as the body begins, with no wait for any event to land.
            os._exit(3)
        attempts = len(open(calls_file).read().splitlines())
        if phase in ("crash", "resume-crash"):
            # Die mid-body once this attempt's StepStart is durable — a pod
            # OOM kill.
            deadline = time.time() + 20
            while time.time() < deadline:
                events = storage.get_events_for_run(ctx.run_id)
                if sum(e.event_type == "StepStart" for e in events) >= attempts:
                    os._exit(3)
                time.sleep(0.05)
            raise SystemExit("StepStart never became visible")
        if phase == "crash-in-backoff":
            # Fail, then die while the retry backoff sleeps.
            def die_in_backoff():
                deadline = time.time() + 20
                while time.time() < deadline:
                    events = storage.get_events_for_run(ctx.run_id)
                    if any(e.event_type == "StepRetry" for e in events):
                        os._exit(3)
                    time.sleep(0.05)
                os._exit(4)

            threading.Thread(target=die_in_backoff, daemon=True).start()
            raise RuntimeError("first attempt fails")

    async def async_body(ctx):
        body(ctx)

    policies = {
        "merge": None,
        "merge_retry": rs.RetryPolicy(max_retries=1),
        "merge_spent": rs.RetryPolicy(max_retries=0),
        "merge_conn": rs.RetryPolicy(max_retries=3, retry_on=[ConnectionError]),
        "merge_backoff": rs.RetryPolicy(max_retries=1, backoff=rs.Backoff.constant(60)),
    }
    actions = [
        rs.AssetAction(name=name, outcome=rs.Outcome.MayMaterialize, retry=retry)(
            async_body if style == "async" else body
        )
        for name, retry in policies.items()
    ]

    class Table(rs.Asset):
        io_handler = rs.InMemoryIOHandler()

        @classmethod
        def materialize(cls):
            return 1

    Table.actions = actions

    storage = embedded_storage(path)
    repo = rs.CodeRepository(assets=[Table], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    result = repo.run_action(
        verb,
        run_id_override="crashed-run",
        resume=phase.startswith("resume"),
        raise_on_error=False,
    )
    print("RESULT", result.success)
    for asset, error in result.failed_assets:
        print("FAILED", asset, error)
    """
)


def _run_interrupted_action(db, phase, verb, calls, style="sync"):
    return subprocess.run(
        [
            sys.executable,
            "-c",
            _INTERRUPTED_ACTION_SCRIPT,
            db,
            phase,
            verb,
            str(calls),
            style,
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )


@pytest.mark.parametrize(
    ("verb", "reruns"),
    [
        pytest.param("merge", False, id="no-retry-policy"),
        pytest.param("merge_spent", False, id="budget-used-up"),
        pytest.param("merge_retry", True, id="budget-left"),
        # A crash is an infrastructure failure; this policy never retries one.
        pytest.param("merge_conn", False, id="retry-on-excludes-crashes"),
    ],
)
def test_resume_runs_an_interrupted_action_at_most_once(tmp_path, verb, reruns):
    """A pod killed mid-action restarts with --resume. The cut-off step had no
    terminal event, so resume ran it again — an automatic retry of a possibly
    half-applied merge that the action never asked for. Without budget left it
    now fails as interrupted; with budget it runs again."""
    db = str(tmp_path / "db")
    calls = tmp_path / "calls.txt"

    crashed = _run_interrupted_action(db, "crash", verb, calls)
    assert crashed.returncode == 3, crashed.stderr[-2000:]
    assert calls.read_text().splitlines() == ["table"]

    resumed = _run_interrupted_action(db, "resume", verb, calls)
    assert resumed.returncode == 0, resumed.stderr[-2000:]
    ran_again = calls.read_text().splitlines() == ["table", "table"]
    assert ran_again is reruns, calls.read_text()
    assert f"RESULT {reruns}" in resumed.stdout, resumed.stdout[-2000:]


def test_repeated_crashes_use_up_the_retry_budget(tmp_path):
    """Every crashed attempt used a try. The resume check read one bool
    "started", so a crash-looping merge ran again on every restart."""
    db = str(tmp_path / "db")
    calls = tmp_path / "calls.txt"

    for phase in ("crash", "resume-crash"):
        crashed = _run_interrupted_action(db, phase, "merge_retry", calls)
        assert crashed.returncode == 3, crashed.stderr[-2000:]
    resumed = _run_interrupted_action(db, "resume", "merge_retry", calls)
    assert resumed.returncode == 0, resumed.stderr[-2000:]
    assert calls.read_text().splitlines() == ["table", "table"], (
        "a third attempt ran past a budget of two"
    )
    assert "RESULT False" in resumed.stdout, resumed.stdout[-2000:]


def test_a_crash_in_the_retry_backoff_keeps_the_granted_retry(tmp_path):
    """The retry was granted before the crash, so resume runs it — the step
    that slept in its backoff was not cut off mid-attempt."""
    db = str(tmp_path / "db")
    calls = tmp_path / "calls.txt"

    crashed = _run_interrupted_action(db, "crash-in-backoff", "merge_backoff", calls)
    assert crashed.returncode == 3, crashed.stderr[-2000:]
    resumed = _run_interrupted_action(db, "resume", "merge_backoff", calls)
    assert resumed.returncode == 0, resumed.stderr[-2000:]
    assert calls.read_text().splitlines() == ["table", "table"]
    assert "RESULT True" in resumed.stdout, resumed.stdout[-2000:]


@pytest.mark.parametrize("style", ["sync", "async"])
def test_an_action_killed_as_its_body_starts_is_not_run_again(tmp_path, style):
    """The StepStart went to the batched event writer and the body began at
    once, so a kill inside the flush window left no StepStart row. Resume then
    took the merge for never begun and ran it a second time."""
    db = str(tmp_path / "db")
    calls = tmp_path / "calls.txt"

    crashed = _run_interrupted_action(db, "crash-at-once", "merge", calls, style)
    assert crashed.returncode == 3, crashed.stderr[-2000:]
    assert calls.read_text().splitlines() == ["table"]

    resumed = _run_interrupted_action(db, "resume", "merge", calls, style)
    assert resumed.returncode == 0, resumed.stderr[-2000:]
    assert calls.read_text().splitlines() == ["table"], "the merge ran a second time"
    assert "RESULT False" in resumed.stdout, resumed.stdout[-2000:]
    failed = [line for line in resumed.stdout.splitlines() if line.startswith("FAILED")]
    assert failed == [
        "FAILED table ExecutionError: Interrupted by a restart; an action with no "
        "retry budget left is not run again"
    ], resumed.stdout[-2000:]


_DOWNSTREAM_FIRST_RESUME_SCRIPT = textwrap.dedent(
    """
    import os, sys, time
    import rivers as rs
    from rivers.testing import embedded_storage

    path, phase, calls_file = sys.argv[1:4]

    def body(ctx):
        keys = ";".join(sorted(str(k) for k in ctx.partition.keys))
        with open(calls_file, "a") as f:
            f.write(f"{ctx.asset_name} {keys}\\n")
        if ctx.asset_name == "rollups":
            for key in ctx.partition.keys:
                if "p2" in str(key):
                    ctx.mark_partition_failed(key, "rollup locked")
        elif phase == "crash":
            # Die mid-body once this step's StepStart is durable — a pod
            # evicted after the rollup step finished.
            deadline = time.time() + 20
            while time.time() < deadline:
                if any(
                    e.event_type == "StepStart" and e.asset_key == "events"
                    for e in storage.get_events_for_run(ctx.run_id)
                ):
                    os._exit(3)
                time.sleep(0.05)
            raise SystemExit("StepStart never became visible")

    purge = rs.AssetAction(
        name="purge",
        outcome=rs.Outcome.Unmaterialize,
        ordering=rs.ActionOrdering.DownstreamFirst,
        retry=rs.RetryPolicy(max_retries=1),
    )(body)
    parts = rs.PartitionsDefinition.static_(["p1", "p2"])

    class Events(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = parts
        actions = [purge]

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext):
            return context.partition_key

    class Rollups(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        partitions_def = parts
        actions = [purge]

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext, events):
            return events

    storage = embedded_storage(path)
    repo = rs.CodeRepository(
        assets=[Events, Rollups], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    if phase == "crash":
        for p in ("p1", "p2"):
            repo.materialize(partition_key=rs.PartitionKey.single(p))
        repo.backfill(
            selection=["events", "rollups"],
            partition_keys=[rs.PartitionKey.single(p) for p in ("p1", "p2")],
            action="purge",
            strategy=rs.BackfillStrategy.single_run(),
        )
    else:
        run = next(r for r in storage.get_runs(limit=20) if r.action == "purge")
        result = repo.run_action(
            "purge",
            selection=run.node_names,
            partition_key=run.partition_key,
            run_id_override=run.run_id,
            resume=True,
            raise_on_error=False,
        )
        print("RESULT", result.success)
    """
)


def test_resume_keeps_the_keys_a_finished_downstream_step_failed(tmp_path):
    """DownstreamFirst leaves the source of a key the rollup step failed. The
    per-key failures lived only in memory, so a resumed run handed the source
    step every key of the batch and deleted the failed key's source."""
    db = str(tmp_path / "db")
    calls = tmp_path / "calls.txt"

    def run(phase):
        return subprocess.run(
            [
                sys.executable,
                "-c",
                _DOWNSTREAM_FIRST_RESUME_SCRIPT,
                db,
                phase,
                str(calls),
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )

    crashed = run("crash")
    assert crashed.returncode == 3, crashed.stderr[-2000:]
    resumed = run("resume")
    assert resumed.returncode == 0, resumed.stderr[-2000:]

    lines = calls.read_text().splitlines()
    source_calls = [line for line in lines if line.startswith("events ")]
    assert len(source_calls) == 2, lines  # the cut-off attempt, then the resumed one
    assert all("p2" not in line for line in source_calls), lines


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


@pytest.mark.timeout(180)
@pytest.mark.parametrize("style", ["sync", "async"])
def test_cancelled_action_waiting_on_its_asset_pool_never_runs(style):
    """Asset-pool waits never time out, so a cancel is the only way out of one:
    the delete used to keep waiting and run once the holder finished."""
    import threading

    holding = threading.Event()
    release = threading.Event()
    purged = []

    def _purge(ctx):
        purged.append(ctx.asset_name)

    async def _apurge(ctx):
        purged.append(ctx.asset_name)

    class SlowTable(rs.Asset):
        io_handler = rs.InMemoryIOHandler()
        purge = rs.AssetAction(
            name="purge",
            outcome=rs.Outcome.Unmaterialize,
            concurrency=rs.ActionConcurrency.Exclusive,
        )(_purge if style == "sync" else _apurge)

        @classmethod
        def materialize(cls):
            holding.set()
            assert release.wait(timeout=60), "test driver never released the holder"
            return 1

    repo = rs.CodeRepository(assets=[SlowTable], default_executor=IP)
    act = {}
    t_mat = threading.Thread(target=repo.materialize)
    t_act = threading.Thread(
        target=lambda: act.update(r=repo.run_action("purge", raise_on_error=False))
    )
    t_mat.start()
    try:
        assert holding.wait(timeout=30)
        t_act.start()

        def waiting_run():
            for r in repo.storage.get_runs(limit=10):
                events = repo.storage.get_events_for_run(r.run_id)
                if r.action == "purge" and any(
                    e.event_type == "StepSlotWaiting" for e in events
                ):
                    return r.run_id
            return None

        assert wait_until(lambda: waiting_run() is not None, timeout=30)
        run_id = waiting_run()
        repo.storage.request_cancellation(run_id)
        t_act.join(timeout=30)
        assert not t_act.is_alive(), "the cancelled step kept waiting on the pool"
    finally:
        release.set()
        t_mat.join(timeout=60)
        if t_act.ident is not None:
            t_act.join(timeout=60)

    assert purged == []
    assert repo.storage.get_run(run_id).status == "Canceled"
    kinds = [str(e.event_type) for e in repo.storage.get_events_for_run(run_id)]
    assert "StepStart" not in kinds and "StepFailure" not in kinds, kinds


_CANCEL_TABLES = [f"table_{i}" for i in range(6)]


def _asset_keys_by_event(repo, run_id):
    by_kind = {}
    for e in repo.storage.get_events_for_run(run_id):
        by_kind.setdefault(str(e.event_type), []).append(e.asset_key)
    return by_kind


@pytest.mark.parametrize("style", ["sync", "async"])
def test_cancelled_action_starts_no_more_targets(style):
    """A cancel stops every target that has not started, not only those that
    wait on a pool: a Shared verb claims none, so its other targets used to
    run after the cancel. In-process runs a level's sync bodies before its
    async ones, so in the async case one sync body cancels before the async
    bodies can start."""
    handler = rs.InMemoryIOHandler()
    purged = []

    def _purge(ctx):
        purged.append(ctx.asset_name)
        if len(purged) == 1:
            repo.storage.request_cancellation(ctx.run_id)

    async def _apurge(ctx):
        _purge(ctx)

    sync_purge = rs.AssetAction(name="purge", outcome=rs.Outcome.Unmaterialize)(_purge)
    async_purge = rs.AssetAction(name="purge", outcome=rs.Outcome.Unmaterialize)(
        _apurge
    )

    def table(name, action):
        @rs.Asset(name=name, io_handler=handler, actions=[action])
        def _table() -> int:
            return 1

        return _table

    sync_count = len(_CANCEL_TABLES) if style == "sync" else 1
    repo = rs.CodeRepository(
        assets=[
            table(name, sync_purge if i < sync_count else async_purge)
            for i, name in enumerate(_CANCEL_TABLES)
        ],
        default_executor=IP,
    )
    repo.materialize()
    result = repo.run_action("purge", raise_on_error=False)

    assert len(purged) == 1, f"targets ran after the cancel: {purged[1:]}"
    first = purged[0]
    assert repo.storage.get_run(result.run_id).status == "Canceled"
    by_kind = _asset_keys_by_event(repo, result.run_id)
    assert by_kind.get("Deletion") == [first]
    assert by_kind.get("StepStart") == [first]
    assert "ActionCompleted" not in by_kind and "StepFailure" not in by_kind
    assert repo.storage.get_asset_record(first).last_data_version is None
    for name in _CANCEL_TABLES:
        if name != first:
            record = repo.storage.get_asset_record(name)
            assert record.last_data_version is not None, name


@pytest.mark.parametrize("style", ["sync", "async"])
def test_cancelled_materialize_starts_no_more_steps(style):
    """The same rule for a materialize run, set up like the action test above:
    the level's other steps claim no pool, and they used to start after the
    cancel."""
    handler = rs.InMemoryIOHandler()
    ran = []

    def _body(context):
        ran.append(context.asset_name)
        if len(ran) == 1:
            repo.storage.request_cancellation(context.run_id)
        return 1

    def table(name, is_async):
        if is_async:

            @rs.Asset(name=name, io_handler=handler)
            async def _table(context: rs.AssetExecutionContext) -> int:
                return _body(context)

        else:

            @rs.Asset(name=name, io_handler=handler)
            def _table(context: rs.AssetExecutionContext) -> int:
                return _body(context)

        return _table

    sync_count = len(_CANCEL_TABLES) if style == "sync" else 1
    repo = rs.CodeRepository(
        assets=[table(name, i >= sync_count) for i, name in enumerate(_CANCEL_TABLES)],
        default_executor=IP,
    )
    result = repo.materialize(raise_on_error=False)

    assert len(ran) == 1, f"steps ran after the cancel: {ran[1:]}"
    first = ran[0]
    assert repo.storage.get_run(result.run_id).status == "Canceled"
    by_kind = _asset_keys_by_event(repo, result.run_id)
    assert by_kind.get("Materialization") == [first]
    assert by_kind.get("StepStart") == [first]
    assert "StepFailure" not in by_kind


_CUT_SHORT = "Cancelled before every map instance ran"


def _fanned(handler, items, echo, pooled, pdef=None):
    """A graph asset that maps ``echo`` over ``items``. With ``pooled``, it
    declares an Exclusive action, so its inner tasks claim the asset's pool."""

    class Fanned(rs.GraphAsset):
        io_handler = handler
        node_io_handler = handler
        if pdef is not None:
            partitions_def = pdef

        @classmethod
        def compose(cls):
            return _fan_total(items().map(echo).collect())

        if pooled:

            @rs.action(
                outcome=rs.Outcome.Unchanged,
                concurrency=rs.ActionConcurrency.Exclusive,
            )
            @classmethod
            def optimize(cls, ctx):
                return None

    return Fanned


def _assert_fan_out_cut_short(repo, result, mapped, ran):
    """Only the instances in ``ran`` have step events. The mapped step started,
    so a StepFailure that names the cancel closes it. The run stays Canceled
    and fails nothing: no failed asset, and no keyed failure to floor a
    partition."""
    assert repo.storage.get_run(result.run_id).status == "Canceled"
    assert result.failed_assets == []
    by_kind = _asset_keys_by_event(repo, result.run_id)
    assert mapped in by_kind.get("StepStart", []), by_kind
    assert mapped not in by_kind.get("StepSuccess", []), by_kind
    failures = [
        (e.asset_key, e.metadata, e.partition_key)
        for e in repo.storage.get_events_for_run(result.run_id)
        if str(e.event_type) == "StepFailure"
    ]
    assert failures == [(mapped, [("error", _CUT_SHORT)], None)], by_kind
    completed, started = repo.storage.get_run_progress(result.run_id)
    assert completed == started, by_kind
    for kind in ("StepStart", "Materialization", "StepSuccess"):
        instances = [k for k in by_kind.get(kind, []) if k.startswith(f"{mapped}__")]
        assert instances == ran, (kind, by_kind)


@pytest.mark.parametrize(
    "partitioned", [False, True], ids=["unpartitioned", "partitioned"]
)
@pytest.mark.parametrize("pooled", [False, True], ids=["no_pool", "asset_pool"])
@pytest.mark.parametrize("style", ["sync", "async"])
def test_cancelled_fan_out_does_not_succeed(style, pooled, partitioned):
    """A cancel skips the fan-out instances that have not started, so the
    mapped step did not finish. It used to get a StepSuccess all the same,
    and then no end event at all, so it stayed Running in the Canceled run.
    Now a StepFailure that names the cancel closes it. Sync instances run one
    at a time, so the first one cancels the run. Async ones start up to four
    at a time, so there a step of the same level cancels first: a level runs
    its single steps before its mapped ones."""
    handler = rs.InMemoryIOHandler()
    pdef = rs.PartitionsDefinition.static_(["p1", "p2"]) if partitioned else None
    part = {"partitions_def": pdef} if partitioned else {}
    run_ids = []
    ran = []

    def cancel():
        repo.storage.request_cancellation(run_ids[0])

    @rs.Asset(io_handler=handler, **part)
    def items(context: rs.AssetExecutionContext) -> list:
        run_ids.append(context.run_id)
        return [1, 2, 3, 4, 5, 6]

    if style == "sync":

        @rs.Task
        def echo(x: int) -> int:
            ran.append(x)
            if len(ran) == 1:
                cancel()
            return x

        siblings = []
    else:

        @rs.Task
        async def echo(x: int) -> int:
            ran.append(x)
            return x

        @rs.Asset(io_handler=handler, **part)
        def stopper(items: list) -> int:
            cancel()
            return 0

        siblings = [stopper]

    repo = rs.CodeRepository(
        assets=[items, _fanned(handler, items, echo, pooled, pdef), *siblings],
        tasks=[echo, _fan_total],
        default_executor=IP,
    )
    pk = rs.PartitionKey.single("p1") if partitioned else None
    result = repo.materialize(partition_key=pk, raise_on_error=False)

    assert ran == ([1] if style == "sync" else [])
    first = ["fanned/echo__0"] if style == "sync" else []
    _assert_fan_out_cut_short(repo, result, "fanned/echo", first)


@pytest.mark.parametrize(
    ("style", "pooled"),
    [("sync", True), ("async", False), ("async", True)],
    ids=["sync-asset_pool", "async-no_pool", "async-asset_pool"],
)
def test_cancelled_fan_out_does_not_succeed_on_parallel(tmp_path, style, pooled):
    """The same rule on the parallel executor. Sync instance bodies run in
    worker processes, so a step of the same level cancels first. A sync
    instance with no pool goes to a worker with no cancel check, so that case
    is left out."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def items() -> list:
        return [1, 2, 3, 4, 5, 6]

    @rs.Asset(io_handler=handler)
    def stopper(context: rs.AssetExecutionContext, items: list) -> int:
        repo.storage.request_cancellation(context.run_id)
        return 0

    if style == "sync":
        echo = _fan_echo
    else:

        @rs.Task
        async def echo(x: int) -> int:
            return x

    repo = rs.CodeRepository(
        assets=[items, _fanned(handler, items, echo, pooled), stopper],
        tasks=[echo, _fan_total],
        default_executor=MP,
    )
    result = repo.materialize(raise_on_error=False)

    _assert_fan_out_cut_short(repo, result, f"fanned/{echo.name}", [])


def test_asset_pool_wait_outlives_the_claim_timeout():
    """A materialize waiting on its asset's exclusive-action pool must wait for
    the action, not fail at the claim timeout: the failure set a floor that
    stopped eager for good. A user pool still times out. Subprocess because
    the timeout is read once per process."""
    script = textwrap.dedent(
        """
        import threading, time
        import rivers as rs
        from rivers.testing import memory_storage

        started = threading.Event()

        class Table(rs.Asset):
            io_handler = rs.InMemoryIOHandler()

            @classmethod
            def materialize(cls):
                return 1

            @rs.action(
                outcome=rs.Outcome.Unchanged,
                concurrency=rs.ActionConcurrency.Exclusive,
            )
            @classmethod
            def optimize(cls, ctx):
                started.set()
                time.sleep(3)

        @rs.Asset(pool="db", io_handler=rs.InMemoryIOHandler())
        def pooled():
            return 1

        storage = memory_storage()
        repo = rs.CodeRepository(
            assets=[Table, pooled], default_executor=rs.Executor.in_process()
        )
        repo.resolve(storage=storage)
        repo.materialize(selection=["table"])

        out = {}
        t = threading.Thread(
            target=lambda: out.update(r=repo.run_action("optimize", raise_on_error=False))
        )
        t.start()
        assert started.wait(30), "optimize never started"
        repo.materialize(selection=["table"])
        t.join(60)
        assert out["r"].success, "optimize failed"

        storage.set_pool_limit("db", 1)
        storage._claim_concurrency_slots([("db", 1)], "other-run", "other-step")
        try:
            repo.materialize(selection=["pooled"])
        except Exception as e:
            assert "timed out waiting for pool slots" in str(e), e
        else:
            raise SystemExit("a user-pool wait must still time out")
        """
    )
    env = dict(
        os.environ,
        RIVERS_CLAIM_TIMEOUT="1s",
        RIVERS_CLAIM_POLL_INTERVAL="100ms",
        RIVERS_CLAIM_POLL_JITTER="0s",
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, (
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr[-3000:]}"
    )


@pytest.mark.timeout(120)
def test_busy_target_does_not_hold_up_the_others():
    """A fleet-wide exclusive action runs its targets one at a time. A target
    whose asset is being materialized waited in place, and every target after
    it waited behind it. It now steps aside: free targets run first."""
    import threading

    order = []
    writing = threading.Event()
    release = threading.Event()

    class Table(rs.Asset):
        io_handler = rs.InMemoryIOHandler()

        @rs.action(
            outcome=rs.Outcome.Unchanged, concurrency=rs.ActionConcurrency.Exclusive
        )
        @classmethod
        def optimize(cls, ctx):
            order.append(ctx.asset_name)

    class ATable(Table):
        @classmethod
        def materialize(cls):
            return 1

    class BOrders(Table):
        @classmethod
        def materialize(cls):
            writing.set()
            assert release.wait(timeout=60), "driver never released the write"
            return 2

    class CTable(Table):
        @classmethod
        def materialize(cls):
            return 3

    repo = rs.CodeRepository(assets=[ATable, BOrders, CTable], default_executor=IP)
    repo.materialize(selection=["a_table", "c_table"])

    mat = threading.Thread(target=lambda: repo.materialize(selection=["b_orders"]))
    mat.start()
    assert writing.wait(timeout=30), "the b_orders write never started"

    out = {}
    act = threading.Thread(target=lambda: out.update(r=repo.run_action("optimize")))
    act.start()
    try:
        # b_orders sorts between the other two; c_table must not wait for it.
        assert wait_until(lambda: "c_table" in order, timeout=20), (
            f"c_table waited behind the busy b_orders: ran {order}"
        )
        assert "b_orders" not in order
    finally:
        release.set()
        mat.join(timeout=60)
        act.join(timeout=60)
    assert out["r"].success
    assert order == ["a_table", "c_table", "b_orders"]


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
