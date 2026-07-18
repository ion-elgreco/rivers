"""Declarative step retries (``@Asset(retry=...)``, repo ``retries`` registry)."""

import json

import pytest

import rivers as rs


def test_retry_succeeds_after_transient_failures(storage):
    calls = {"n": 0}

    @rs.Asset(retry=rs.RetryPolicy(max_retries=3, retry_on=[ValueError]))
    def flaky() -> int:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("transient")
        return 42

    repo = rs.CodeRepository(assets=[flaky], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    result = repo.materialize()

    assert result.success
    assert calls["n"] == 3
    types = [e.event_type for e in storage.get_events_for_asset("flaky")]
    assert types.count("StepRetry") == 2
    assert "StepSuccess" in types
    assert "StepFailure" not in types


def test_retry_budget_exhausted_fails(storage):
    calls = {"n": 0}

    @rs.Asset(retry=rs.RetryPolicy(max_retries=2))
    def always_fails() -> int:
        calls["n"] += 1
        raise ValueError("permanent")

    repo = rs.CodeRepository(
        assets=[always_fails], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=False)

    assert not result.success
    assert calls["n"] == 3  # initial attempt + 2 retries
    types = [e.event_type for e in storage.get_events_for_asset("always_fails")]
    assert types.count("StepRetry") == 2
    assert "StepFailure" in types


def test_transient_preset_does_not_retry_deterministic_error(storage):
    calls = {"n": 0}

    @rs.Asset(retry=rs.RetryPolicy(max_retries=3, retry_on=rs.RetryOn.TRANSIENT))
    def deterministic() -> int:
        calls["n"] += 1
        raise ValueError("bug, not transient")

    repo = rs.CodeRepository(
        assets=[deterministic], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=False)

    assert not result.success
    assert calls["n"] == 1
    types = [e.event_type for e in storage.get_events_for_asset("deterministic")]
    assert "StepRetry" not in types


def test_exception_allowlist_matches_subclass(storage):
    calls = {"n": 0}

    @rs.Asset(retry=rs.RetryPolicy(max_retries=2, retry_on=[ConnectionError]))
    def reconnects() -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ConnectionResetError("dropped")  # subclass of ConnectionError
        return 1

    repo = rs.CodeRepository(
        assets=[reconnects], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    result = repo.materialize()

    assert result.success
    assert calls["n"] == 2


def test_exception_allowlist_rejects_unlisted(storage):
    calls = {"n": 0}

    @rs.Asset(retry=rs.RetryPolicy(max_retries=2, retry_on=[ConnectionError]))
    def wrong_kind() -> int:
        calls["n"] += 1
        raise ValueError("not connection-related")

    repo = rs.CodeRepository(
        assets=[wrong_kind], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=False)

    assert not result.success
    assert calls["n"] == 1


def test_named_policy_from_registry(storage):
    calls = {"n": 0}

    @rs.Asset(retry="flaky_io")
    def named() -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("once")
        return 7

    repo = rs.CodeRepository(
        assets=[named],
        retries={"flaky_io": rs.RetryPolicy(max_retries=2, retry_on=[ValueError])},
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    result = repo.materialize()

    assert result.success
    assert calls["n"] == 2
    types = [e.event_type for e in storage.get_events_for_asset("named")]
    assert types.count("StepRetry") == 1


def test_unknown_retry_name_errors_at_resolve(storage):
    @rs.Asset(retry="not_registered")
    def orphan() -> int:
        return 1

    repo = rs.CodeRepository(assets=[orphan])
    with pytest.raises(rs.exceptions.ConfigurationError, match="not_registered"):
        repo.resolve(storage=storage)


def test_step_retry_event_metadata(storage):
    calls = {"n": 0}

    @rs.Asset(retry=rs.RetryPolicy(max_retries=1))
    def once_flaky() -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("boom")
        return 1

    repo = rs.CodeRepository(
        assets=[once_flaky], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    assert repo.materialize().success

    retries = [
        e
        for e in storage.get_events_for_asset("once_flaky")
        if e.event_type == "StepRetry"
    ]
    assert len(retries) == 1
    meta = dict(retries[0].metadata)
    assert meta["rivers/attempt"] == "1"
    assert meta["rivers/failure_reason"] == "error"
    assert meta["rivers/next_delay_ms"] == "0"  # no backoff configured


@pytest.mark.parametrize(
    ("exc", "expected_reason"),
    [
        (TimeoutError, "timeout"),
        (MemoryError, "out_of_memory"),
        (ValueError, "error"),
    ],
)
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_step_failure_event_carries_failure_reason(
    storage, exc, expected_reason, is_async
):
    """StepFailure metadata classifies the failure — the K8s orchestrator
    reads it back to drive retry_on across the pod boundary."""
    if is_async:

        @rs.Asset
        async def doomed() -> int:
            raise exc("boom")

    else:

        @rs.Asset
        def doomed() -> int:
            raise exc("boom")

    repo = rs.CodeRepository(assets=[doomed], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=False)

    assert not result.success
    failures = [
        e
        for e in storage.get_events_for_asset("doomed")
        if e.event_type == "StepFailure"
    ]
    assert len(failures) == 1
    meta = dict(failures[0].metadata)
    assert meta["rivers/failure_reason"] == expected_reason
    exc_types = json.loads(meta["rivers/exc_type"])
    assert f"builtins.{exc.__name__}" in exc_types
    assert "builtins.BaseException" in exc_types  # full MRO, derived-first


def test_step_failure_reason_from_loky_subprocess(tmp_path, storage):
    """Classification survives the loky IPC hop (exception is re-raised
    from a pickled subprocess error)."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def times_out() -> int:
        raise TimeoutError("too slow")

    # Sibling keeps the loky path engaged (a lone sync step runs in-process).
    @rs.Asset(io_handler=handler)
    def steady() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[times_out, steady],
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=False)

    assert not result.success
    failures = [
        e
        for e in storage.get_events_for_asset("times_out")
        if e.event_type == "StepFailure"
    ]
    assert len(failures) == 1
    assert dict(failures[0].metadata)["rivers/failure_reason"] == "timeout"


def test_cancellation_interrupts_backoff(storage):
    """A cancelled run stops waiting out the backoff and does not retry."""
    import threading
    import time

    calls = {"n": 0}

    @rs.Asset(retry=rs.RetryPolicy(max_retries=1, backoff=rs.Backoff.constant(20.0)))
    def stuck() -> int:
        calls["n"] += 1
        raise ValueError("always")

    repo = rs.CodeRepository(assets=[stuck], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)

    def cancel_once_retry_scheduled():
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if any(
                e.event_type == "StepRetry"
                for e in storage.get_events_for_asset("stuck")
            ):
                for run in storage.get_runs(10):
                    storage.request_cancellation(run.run_id)
                return
            time.sleep(0.1)

    canceller = threading.Thread(target=cancel_once_retry_scheduled)
    canceller.start()
    start = time.monotonic()
    result = repo.materialize(raise_on_error=False)
    elapsed = time.monotonic() - start
    canceller.join()

    assert not result.success
    assert calls["n"] == 1  # backoff interrupted — no second attempt
    assert elapsed < 10  # nowhere near the 20s backoff


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_cancellation_stops_zero_backoff_retries(storage, is_async):
    """With no backoff there is no sleep to interrupt — the ladder must still
    observe cancellation between attempts instead of burning the budget."""
    import threading
    import time

    calls = {"n": 0}

    def body() -> int:
        calls["n"] += 1
        time.sleep(0.1)
        raise ValueError("always")

    if is_async:

        @rs.Asset(retry=rs.RetryPolicy(max_retries=50))
        async def stubborn() -> int:
            return body()

    else:

        @rs.Asset(retry=rs.RetryPolicy(max_retries=50))
        def stubborn() -> int:
            return body()

    repo = rs.CodeRepository(
        assets=[stubborn], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)

    def cancel_once_retrying():
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if any(
                e.event_type == "StepRetry"
                for e in storage.get_events_for_asset("stubborn")
            ):
                for run in storage.get_runs(10):
                    storage.request_cancellation(run.run_id)
                return
            time.sleep(0.05)

    canceller = threading.Thread(target=cancel_once_retrying)
    canceller.start()
    result = repo.materialize(raise_on_error=False)
    canceller.join()

    assert not result.success
    assert calls["n"] < 25  # cancellation cut the 50-attempt budget short


@pytest.mark.parametrize(
    "executor_factory,is_async",
    [
        pytest.param(rs.Executor.in_process, False, id="in_process_sync"),
        pytest.param(rs.Executor.in_process, True, id="in_process_async"),
        pytest.param(rs.Executor.parallel, False, id="parallel_sync"),
    ],
)
def test_pool_slot_released_during_backoff(
    tmp_path, storage, executor_factory, is_async
):
    """A backoff sleep releases the step's pool slots and re-claims them for
    the next attempt — one StepSlotClaimed per attempt."""
    marker = str(tmp_path / "attempted")

    def flaky_body() -> int:
        import os

        if not os.path.exists(marker):
            with open(marker, "w") as f:
                f.write("1")
            raise ValueError("first attempt fails")
        return 1

    policy = rs.RetryPolicy(
        max_retries=2, retry_on=[ValueError], backoff=rs.Backoff.constant(0.2)
    )
    if is_async:

        @rs.Asset(
            pool="db", pool_slots=1, io_handler=rs.InMemoryIOHandler(), retry=policy
        )
        async def pooled_flaky() -> int:
            return flaky_body()

    else:

        @rs.Asset(
            pool="db", pool_slots=1, io_handler=rs.InMemoryIOHandler(), retry=policy
        )
        def pooled_flaky() -> int:
            return flaky_body()

    storage.set_pool_limit("db", 1)
    repo = rs.CodeRepository(assets=[pooled_flaky], default_executor=executor_factory())
    repo.resolve(storage=storage)
    result = repo.materialize()
    assert result.success

    events = storage.get_events_for_run(result.run_id)
    claimed = [e for e in events if e.event_type == "StepSlotClaimed"]
    released = [e for e in events if e.event_type == "StepSlotReleased"]
    assert len(claimed) == 2  # initial attempt + re-claim after the backoff
    assert len(released) == 2


def test_backoff_delay_is_applied(storage):
    import time

    calls = {"n": 0}

    @rs.Asset(
        retry=rs.RetryPolicy(max_retries=1, backoff=rs.Backoff.constant(0.3)),
    )
    def slow_retry() -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("wait for it")
        return 1

    repo = rs.CodeRepository(
        assets=[slow_retry], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    start = time.monotonic()
    assert repo.materialize().success
    assert time.monotonic() - start >= 0.3
    assert calls["n"] == 2


def test_async_asset_retry(storage):
    calls = {"n": 0}

    @rs.Asset(retry=rs.RetryPolicy(max_retries=2, retry_on=[ValueError]))
    async def flaky_async() -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("transient")
        return 1

    repo = rs.CodeRepository(
        assets=[flaky_async], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    assert repo.materialize().success
    assert calls["n"] == 2
    types = [e.event_type for e in storage.get_events_for_asset("flaky_async")]
    assert types.count("StepRetry") == 1
    assert "StepFailure" not in types


def test_parallel_executor_retry(tmp_path, storage):
    """Retry a step whose exception was raised in a loky subprocess."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)
    marker = str(tmp_path / "attempted")

    @rs.Asset(
        io_handler=handler,
        retry=rs.RetryPolicy(max_retries=2, retry_on=[ValueError]),
    )
    def flaky_subprocess() -> int:
        import os

        if not os.path.exists(marker):
            with open(marker, "w") as f:
                f.write("1")
            raise ValueError("first attempt fails")
        return 5

    # A sibling step at the same level keeps the loky pool path engaged
    # (a lone sync step would be run in-process by the parallel backend).
    @rs.Asset(io_handler=handler)
    def steady() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[flaky_subprocess, steady],
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    repo.resolve(storage=storage)
    assert repo.materialize().success
    types = [e.event_type for e in storage.get_events_for_asset("flaky_subprocess")]
    assert types.count("StepRetry") == 1
    assert "StepSuccess" in types
    assert "StepFailure" not in types


def test_mapped_max_concurrency_respected_with_retry(tmp_path, storage):
    """Map instances honor ``max_concurrency`` windowing when the job carries
    a retry policy. Pins the window across both parallel paths (task nodes
    don't take asset/job retry policies today, so instances stay on the
    windowed fast path; the pool/lifecycle path is semaphore-gated too)."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path / "data"), mkdir=True)
    io = rs.PickleIOHandler(store=store)
    active_dir = str(tmp_path / "active")
    peak_file = str(tmp_path / "peak")
    import os

    os.mkdir(active_dir)

    @rs.Task
    def tracked(x: int) -> int:
        import os
        import time
        import uuid

        token = os.path.join(active_dir, uuid.uuid4().hex)
        with open(token, "w") as f:
            f.write("1")
        try:
            for _ in range(2):
                with open(peak_file, "a") as f:
                    f.write(f"{len(os.listdir(active_dir))}\n")
                time.sleep(0.2)
        finally:
            os.remove(token)
        return x * 2

    @rs.Task
    def sum_all(values: list) -> int:
        return sum(values)

    @rs.Asset(io_handler=io)
    def numbers() -> list:
        return [1, 2, 3]

    @rs.Asset.from_graph(io_handler=io, node_io_handler=io)
    def result():
        nums = numbers()
        mapped = nums.map(tracked, max_concurrency=1)
        return sum_all(mapped.collect())

    repo = rs.CodeRepository(
        assets=[numbers, result],
        tasks=[tracked, sum_all],
        jobs=[
            rs.Job(
                name="pipeline",
                assets=[numbers, result],
                executor=rs.Executor.parallel(max_workers=4),
                retry=rs.RetryPolicy(max_retries=1),
            )
        ],
    )
    repo.resolve(storage=storage)
    repo.get_job("pipeline").execute()

    assert repo.load_node("result") == 12
    with open(peak_file) as f:
        peak = max(int(line) for line in f.read().splitlines())
    assert peak == 1  # never more than one tracked instance in flight


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_resume_continues_retry_budget(storage, is_async):
    """materialize(resume=True) seeds the ladder from recorded StepRetry
    events instead of granting a fresh budget (parity with the K8s executor)."""
    calls = {"n": 0}

    def body() -> int:
        calls["n"] += 1
        raise ValueError("always")

    if is_async:

        @rs.Asset(retry=rs.RetryPolicy(max_retries=2, retry_on=[ValueError]))
        async def doomed_resume() -> int:
            return body()

    else:

        @rs.Asset(retry=rs.RetryPolicy(max_retries=2, retry_on=[ValueError]))
        def doomed_resume() -> int:
            return body()

    repo = rs.CodeRepository(
        assets=[doomed_resume], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)

    run_id = "resume-budget-test"
    result = repo.materialize(raise_on_error=False, run_id_override=run_id)
    assert not result.success
    assert calls["n"] == 3  # initial attempt + 2 retries

    result = repo.materialize(raise_on_error=False, run_id_override=run_id, resume=True)
    assert not result.success
    assert calls["n"] == 4  # budget already spent: exactly one more attempt
    retries = [
        e
        for e in storage.get_events_for_asset("doomed_resume")
        if e.event_type == "StepRetry"
    ]
    assert len(retries) == 2  # no duplicate ladder on resume


def test_worker_death_classified_infrastructure_and_retried(tmp_path, storage):
    """A loky worker killed mid-step surfaces as TerminatedWorkerError — an
    environmental failure, so retry_on=TRANSIENT retries it."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)
    marker = str(tmp_path / "attempted")
    transient = rs.RetryPolicy(max_retries=2, retry_on=rs.RetryOn.TRANSIENT)

    @rs.Asset(io_handler=handler, retry=transient)
    def dies_once() -> int:
        import os

        if not os.path.exists(marker):
            with open(marker, "w") as f:
                f.write("1")
            os._exit(42)  # hard-kill the worker process mid-step
        return 5

    # Sibling keeps the loky path engaged; carries the same policy in case
    # the pool break collaterally kills its future.
    @rs.Asset(io_handler=handler, retry=transient)
    def steady() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[dies_once, steady],
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    repo.resolve(storage=storage)
    assert repo.materialize().success

    retries = [
        e
        for e in storage.get_events_for_asset("dies_once")
        if e.event_type == "StepRetry"
    ]
    assert len(retries) == 1
    assert dict(retries[0].metadata)["rivers/failure_reason"] == "infrastructure"


def test_job_level_retry_default(storage):
    calls = {"n": 0}

    @rs.Asset
    def job_flaky() -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("once")
        return 1

    job = rs.Job(
        name="retry_job",
        assets=[job_flaky],
        executor=rs.Executor.in_process(),
        retry=rs.RetryPolicy(max_retries=2, retry_on=[ValueError]),
    )
    repo = rs.CodeRepository(assets=[job_flaky], jobs=[job])
    repo.resolve(storage=storage)
    result = repo.get_job("retry_job").execute()
    assert result.success
    assert calls["n"] == 2


def test_asset_policy_overrides_job_policy(storage):
    calls = {"n": 0}

    @rs.Asset(retry=rs.RetryPolicy(max_retries=0))
    def stubborn() -> int:
        calls["n"] += 1
        raise ValueError("always")

    job = rs.Job(
        name="override_job",
        assets=[stubborn],
        executor=rs.Executor.in_process(),
        retry=rs.RetryPolicy(max_retries=5, retry_on=[ValueError]),
    )
    repo = rs.CodeRepository(assets=[stubborn], jobs=[job])
    repo.resolve(storage=storage)
    result = repo.get_job("override_job").execute(raise_on_error=False)
    assert not result.success
    assert calls["n"] == 1  # asset's max_retries=0 wins over the job's 5


def test_repo_default_retry_policy(storage):
    calls = {"n": 0}

    @rs.Asset
    def repo_flaky() -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("once")
        return 1

    repo = rs.CodeRepository(
        assets=[repo_flaky],
        default_retry_policy=rs.RetryPolicy(max_retries=2, retry_on=[ValueError]),
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    assert repo.materialize().success
    assert calls["n"] == 2


def test_materialize_retry_kwarg(storage):
    calls = {"n": 0}

    @rs.Asset
    def kw_flaky() -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("once")
        return 1

    repo = rs.CodeRepository(
        assets=[kw_flaky], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    result = repo.materialize(
        retry=rs.RetryPolicy(max_retries=2, retry_on=[ValueError])
    )
    assert result.success
    assert calls["n"] == 2


def test_multi_asset_output_retry(storage):
    """A dict-returning multi-asset retries as one unit via from_multi(retry=)."""
    calls = {"n": 0}

    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("mr_a"),
            rs.AssetDef("mr_b"),
        ],
        retry=rs.RetryPolicy(max_retries=2, retry_on=[ValueError]),
    )
    def multi_flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("once")
        return {"mr_a": 1, "mr_b": 2}

    repo = rs.CodeRepository(
        assets=[multi_flaky], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    assert repo.materialize().success
    assert calls["n"] == 2
    types_a = [e.event_type for e in storage.get_events_for_asset("mr_a")]
    types_b = [e.event_type for e in storage.get_events_for_asset("mr_b")]
    assert types_a.count("StepRetry") == 1
    assert types_b.count("StepRetry") == 1
    assert "StepSuccess" in types_a and "StepSuccess" in types_b


@pytest.mark.parametrize("executor_kind", ["in_process", "parallel"])
def test_task_retry_succeeds(storage, tmp_path, executor_kind):
    """A task with its own retry= policy retries like an asset step. The
    marker file carries flakiness across loky subprocess attempts."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)
    marker = tmp_path / "task_attempted"

    @rs.Task(
        retry=rs.RetryPolicy(max_retries=2, retry_on=[ValueError]),
        io_handler=handler,
    )
    def flaky_task() -> int:
        if not marker.exists():
            marker.write_text("x")
            raise ValueError("transient")
        return 7

    # Sibling keeps the loky path engaged (a lone sync step runs in-process).
    @rs.Asset(io_handler=handler)
    def steady() -> int:
        return 1

    executor = (
        rs.Executor.in_process()
        if executor_kind == "in_process"
        else rs.Executor.parallel(max_workers=2)
    )
    repo = rs.CodeRepository(
        assets=[steady],
        tasks=[flaky_task],
        jobs=[rs.Job(name="tjob", assets=[steady, flaky_task], executor=executor)],
    )
    repo.resolve(storage=storage)
    repo.get_job("tjob").execute()

    types = [e.event_type for e in storage.get_events_for_asset("flaky_task")]
    assert types.count("StepRetry") == 1
    assert "StepSuccess" in types


def test_async_task_retry(storage):
    calls = {"n": 0}

    @rs.Task(retry=rs.RetryPolicy(max_retries=2, retry_on=[ValueError]))
    async def async_flaky() -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("transient")
        return 3

    repo = rs.CodeRepository(
        assets=[],
        tasks=[async_flaky],
        jobs=[rs.Job(name="ajob", assets=[async_flaky])],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    repo.get_job("ajob").execute()

    assert calls["n"] == 2
    types = [e.event_type for e in storage.get_events_for_asset("async_flaky")]
    assert types.count("StepRetry") == 1
    assert "StepSuccess" in types


def test_bash_task_retry(storage, tmp_path):
    # Quoted POSIX form — sh (Git Bash on Windows runners) eats the
    # backslashes of a raw Windows path, mangling where the marker lands.
    marker = tmp_path / "bash_attempted"
    marker_sh = marker.as_posix()
    flaky_bash = rs.BashTask(
        name="flaky_bash",
        command=f"test -f '{marker_sh}' || {{ touch '{marker_sh}'; exit 1; }}",
        retry=rs.RetryPolicy(max_retries=2),
    )

    repo = rs.CodeRepository(
        assets=[],
        tasks=[flaky_bash],
        jobs=[rs.Job(name="bjob", assets=[flaky_bash])],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    repo.get_job("bjob").execute()

    assert marker.exists()
    types = [e.event_type for e in storage.get_events_for_asset("flaky_bash")]
    assert types.count("StepRetry") == 1
    assert "StepSuccess" in types


def test_job_retry_fills_task_steps(storage):
    """A job-level policy covers task steps that don't set their own."""
    calls = {"n": 0}

    @rs.Task
    def plain_task() -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("transient")
        return 1

    repo = rs.CodeRepository(
        assets=[],
        tasks=[plain_task],
        jobs=[
            rs.Job(
                name="mixed",
                assets=[plain_task],
                retry=rs.RetryPolicy(max_retries=2),
            )
        ],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    repo.get_job("mixed").execute()

    assert calls["n"] == 2
    types = [e.event_type for e in storage.get_events_for_asset("plain_task")]
    assert types.count("StepRetry") == 1
    assert "StepSuccess" in types


def test_graph_asset_internal_task_retry(storage):
    """A graph asset's internal task steps retry via the task's own policy."""
    calls = {"n": 0}
    io = rs.InMemoryIOHandler()

    @rs.Task(retry=rs.RetryPolicy(max_retries=2, retry_on=[ValueError]))
    def flaky_inner() -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("transient")
        return 5

    @rs.Asset.from_graph(io_handler=io, node_io_handler=io)
    def wrapper():
        return flaky_inner()

    repo = rs.CodeRepository(
        assets=[wrapper],
        tasks=[flaky_inner],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    assert repo.materialize().success

    assert calls["n"] == 2
    types = [e.event_type for e in storage.get_events_for_asset("wrapper/flaky_inner")]
    assert types.count("StepRetry") == 1


def test_task_named_retry_policy(storage):
    calls = {"n": 0}

    @rs.Task(retry="net")
    def named_flaky() -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("transient")
        return 3

    repo = rs.CodeRepository(
        assets=[],
        tasks=[named_flaky],
        jobs=[rs.Job(name="njob", assets=[named_flaky])],
        retries={"net": rs.RetryPolicy(max_retries=2)},
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    repo.get_job("njob").execute()

    assert calls["n"] == 2


def test_graph_asset_retry_named_ref_resolves(storage):
    """from_graph(retry='name') resolves against the registry at resolve();
    an unknown name errors. (The policy drives the outer-pod ladder on the
    K8s executor — covered by the K8s integration suite.)"""
    io = rs.InMemoryIOHandler()

    @rs.Task
    def inner() -> int:
        return 1

    @rs.Asset.from_graph(io_handler=io, node_io_handler=io, retry="net")
    def wrapped():
        return inner()

    repo = rs.CodeRepository(
        assets=[wrapped],
        tasks=[inner],
        retries={"net": rs.RetryPolicy(max_retries=2)},
    )
    repo.resolve(storage=storage)

    @rs.Asset.from_graph(io_handler=io, node_io_handler=io, retry="nope")
    def broken():
        return inner()

    repo2 = rs.CodeRepository(assets=[broken], tasks=[inner])
    with pytest.raises(
        rs.exceptions.ConfigurationError, match="unknown retry policy 'nope'"
    ):
        repo2.resolve(storage=storage)


def test_task_unknown_named_retry_errors(storage):
    @rs.Task(retry="nope")
    def bad_task() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[],
        tasks=[bad_task],
        jobs=[rs.Job(name="bad_job", assets=[bad_task])],
    )
    with pytest.raises(
        rs.exceptions.ConfigurationError, match="unknown retry policy 'nope'"
    ):
        repo.resolve(storage=storage)


def test_downstream_runs_after_upstream_retry_succeeds(storage):
    calls = {"n": 0}

    @rs.Asset(retry=rs.RetryPolicy(max_retries=2, retry_on=[ValueError]))
    def upstream() -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("transient")
        return 10

    @rs.Asset
    def downstream(upstream: int) -> int:
        return upstream + 1

    repo = rs.CodeRepository(
        assets=[upstream, downstream], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    result = repo.materialize()

    assert result.success
    types = [e.event_type for e in storage.get_events_for_asset("downstream")]
    assert "StepSuccess" in types
    assert "StepFailure" not in types
