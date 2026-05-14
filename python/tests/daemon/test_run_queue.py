"""Tests for Run Queue — materialize() stays direct, queue for daemon/API."""

import rivers as rs


# ── Helpers ──


def make_simple_repo(run_queue=None):
    """Create a minimal repo with one asset."""

    @rs.Asset
    def alpha():
        return 42

    return rs.CodeRepository(
        assets=[alpha],
        default_executor=rs.Executor.in_process(),
        run_queue=run_queue,
    )


# ── Tests: materialize() always executes directly ──


def test_materialize_no_queue():
    """materialize() without queue config executes directly."""
    repo = make_simple_repo()
    result = repo.materialize()
    assert isinstance(result, rs.RunResult)
    assert result.success is True
    assert repo.load_node("alpha") == 42


def test_materialize_with_queue_still_direct():
    """materialize() with queue config still executes directly (not queued)."""
    repo = make_simple_repo(run_queue=rs.RunQueueConfig(max_concurrent_runs=10))
    result = repo.materialize()
    assert isinstance(result, rs.RunResult)
    assert result.success is True
    assert repo.load_node("alpha") == 42


def test_materialize_run_record_stored(storage):
    """After materialize(), the run record has Success status in storage."""

    @rs.Asset
    def gamma():
        return 99

    repo = rs.CodeRepository(
        assets=[gamma],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    result = repo.materialize()

    runs = storage.get_runs(limit=10)
    matching = [r for r in runs if r.run_id == result.run_id]
    assert len(matching) == 1
    assert matching[0].status == "Success"


def test_materialize_with_queue_config_run_record(storage):
    """materialize() with queue config creates a Started (not Queued) run record."""

    @rs.Asset
    def beta():
        return 7

    repo = rs.CodeRepository(
        assets=[beta],
        default_executor=rs.Executor.in_process(),
        run_queue=rs.RunQueueConfig(),
    )
    repo.resolve(storage=storage)
    result = repo.materialize()

    runs = storage.get_runs(limit=10)
    matching = [r for r in runs if r.run_id == result.run_id]
    assert len(matching) == 1
    # materialize() executes directly — run goes Started → Success, not Queued
    assert matching[0].status == "Success"


def test_materialize_returns_run_result_type():
    """materialize() always returns RunResult, never RunHandle."""
    repo = make_simple_repo(run_queue=rs.RunQueueConfig())
    result = repo.materialize()
    assert isinstance(result, rs.RunResult)
    assert result.success is True


def test_materialize_selection_with_queue():
    """materialize() with selection works the same with or without queue config."""

    @rs.Asset
    def a():
        return 1

    @rs.Asset
    def b(a: int):
        return a + 1

    repo = rs.CodeRepository(
        assets=[a, b],
        default_executor=rs.Executor.in_process(),
        run_queue=rs.RunQueueConfig(),
    )
    result = repo.materialize(selection=["a"])
    assert result.success is True
    assert "a" in result.materialized_assets


# ── Tests: RunHandle is importable and registered ──


def test_run_handle_importable():
    """RunHandle class is importable from rivers."""
    assert hasattr(rs, "RunHandle")


# ── Tests: RunQueueConfig ──


def test_run_queue_config_defaults():
    """RunQueueConfig has sensible defaults."""
    cfg = rs.RunQueueConfig()
    assert cfg.max_concurrent_runs == 10
    assert cfg.dequeue_interval == "250ms"
    assert cfg.tag_concurrency_limits == []


def test_run_queue_config_custom():
    """RunQueueConfig accepts custom values."""
    limit = rs.TagConcurrencyLimit(key="env", limit=2)
    cfg = rs.RunQueueConfig(
        max_concurrent_runs=5,
        tag_concurrency_limits=[limit],
        dequeue_interval="500ms",
    )
    assert cfg.max_concurrent_runs == 5
    assert cfg.dequeue_interval == "500ms"
    assert len(cfg.tag_concurrency_limits) == 1


# ── Tests: _submit_run creates Queued runs ──


def test_submit_run_creates_queued_record(storage):
    """_submit_run() creates a run with Queued status in storage."""

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def alpha():
        return 1

    repo = rs.CodeRepository(
        assets=[alpha],
        default_executor=rs.Executor.in_process(),
        run_queue=rs.RunQueueConfig(),
    )
    repo.resolve(storage=storage)

    handle = repo._submit_run()
    assert handle.run_id

    runs = storage.get_runs(limit=10)
    matching = [r for r in runs if r.run_id == handle.run_id]
    assert len(matching) == 1
    assert matching[0].status == "Queued"


def test_submit_run_with_selection(storage):
    """_submit_run(selection=[...]) creates a Queued run for specific assets."""

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def a():
        return 1

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def b(a: int):
        return a + 1

    repo = rs.CodeRepository(
        assets=[a, b],
        default_executor=rs.Executor.in_process(),
        run_queue=rs.RunQueueConfig(),
    )
    repo.resolve(storage=storage)

    handle = repo._submit_run(selection=["a"])
    runs = storage.get_runs(limit=10)
    matching = [r for r in runs if r.run_id == handle.run_id]
    assert len(matching) == 1
    assert matching[0].status == "Queued"
    assert "a" in matching[0].node_names


def test_submit_run_without_queue_config_errors(storage):
    """_submit_run() raises when no RunQueueConfig is set."""

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def alpha():
        return 1

    repo = rs.CodeRepository(
        assets=[alpha],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)

    import pytest

    with pytest.raises(Exception, match="no RunQueueConfig"):
        repo._submit_run()


def test_submit_run_multiple_creates_separate_queued_runs(storage):
    """Multiple _submit_run() calls create separate Queued run records."""

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def alpha():
        return 1

    repo = rs.CodeRepository(
        assets=[alpha],
        default_executor=rs.Executor.in_process(),
        run_queue=rs.RunQueueConfig(),
    )
    repo.resolve(storage=storage)

    h1 = repo._submit_run()
    h2 = repo._submit_run()
    h3 = repo._submit_run()

    assert h1.run_id != h2.run_id != h3.run_id

    runs = storage.get_runs(limit=10)
    queued = [r for r in runs if r.status == "Queued"]
    assert len(queued) == 3


def test_submit_run_emits_queued_event(storage):
    """_submit_run() emits a RunQueued event."""

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def alpha():
        return 1

    repo = rs.CodeRepository(
        assets=[alpha],
        default_executor=rs.Executor.in_process(),
        run_queue=rs.RunQueueConfig(),
    )
    repo.resolve(storage=storage)

    handle = repo._submit_run()
    events = storage.get_events_for_run(handle.run_id)
    queued_events = [e for e in events if e.event_type == "RunQueued"]
    assert len(queued_events) == 1
