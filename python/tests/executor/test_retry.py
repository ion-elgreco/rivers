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
