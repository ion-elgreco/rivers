"""Declarative step retries (``@Asset(retry=...)``, repo ``retries`` registry)."""

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
    types = [
        e.event_type for e in storage.get_events_for_asset("flaky_subprocess")
    ]
    assert types.count("StepRetry") == 1
    assert "StepSuccess" in types
    assert "StepFailure" not in types


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
    """A dict-returning multi-asset retries as one unit via AssetDef(retry=)."""
    calls = {"n": 0}

    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("mr_a", retry=rs.RetryPolicy(max_retries=2, retry_on=[ValueError])),
            rs.AssetDef("mr_b"),
        ],
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


def test_multi_asset_conflicting_policies_error_at_resolve(storage):
    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("cx_a", retry=rs.RetryPolicy(max_retries=1)),
            rs.AssetDef("cx_b", retry=rs.RetryPolicy(max_retries=5)),
        ],
    )
    def conflicted():
        return {"cx_a": 1, "cx_b": 2}

    repo = rs.CodeRepository(assets=[conflicted])
    with pytest.raises(
        rs.exceptions.ConfigurationError, match="different retry policies"
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
