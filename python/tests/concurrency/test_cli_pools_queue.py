"""Tests for pool/queue storage API and CLI commands.

Tests the storage API methods (get_queued_runs, get_pool_slot_holders,
cancel_queued_run, get_all_pool_infos) and the CLI commands (pools
list/info/set, queue list/cancel/why).

Some tests exercise the storage API directly (cheaper, no RocksDB lock
issues). CLI-via-CliRunner tests use a fresh storage_path per invocation.
"""

from typer.testing import CliRunner

from rivers.cli import app

runner = CliRunner()


# ═══════════════════════════════════════════════════════════════════════════
# Storage API: get_pool_slot_holders
# ═══════════════════════════════════════════════════════════════════════════


def test_get_pool_slot_holders_returns_claimed_slots(storage):
    """get_pool_slot_holders returns active slot holders for a pool."""
    storage.set_pool_limit("db", 10)
    storage._claim_concurrency_slots([("db", 1)], "r1", "step_a", 0, "5m")
    storage._claim_concurrency_slots([("db", 2)], "r2", "step_b", 0, "5m")

    holders = storage.get_pool_slot_holders("db")
    assert len(holders) == 2
    run_ids = {h.run_id for h in holders}
    assert run_ids == {"r1", "r2"}
    h1 = next(h for h in holders if h.run_id == "r1")
    assert h1.step_key == "step_a"
    assert h1.slots_consumed == 1
    assert h1.lease_expires_at > h1.claimed_at


def test_get_pool_slot_holders_empty(storage):
    """get_pool_slot_holders returns empty list for pool with no slots."""
    storage.set_pool_limit("db", 5)
    holders = storage.get_pool_slot_holders("db")
    assert holders == []


# ═══════════════════════════════════════════════════════════════════════════
# Storage API: get_queued_runs
# ═══════════════════════════════════════════════════════════════════════════


def test_get_queued_runs_returns_queued(storage):
    """get_queued_runs returns runs in Queued status."""
    storage._create_run("q1", "job1", "Queued", 1000, priority=5)
    storage._create_run("q2", "job2", "Queued", 2000, priority=0)
    storage._create_run("r1", "job1", "Started", 3000)  # not queued

    queued = storage.get_queued_runs()
    assert len(queued) == 2
    ids = {r.run_id for r in queued}
    assert ids == {"q1", "q2"}


def test_get_queued_runs_empty(storage):
    """get_queued_runs returns empty list when no runs are queued."""
    queued = storage.get_queued_runs()
    assert queued == []


# ═══════════════════════════════════════════════════════════════════════════
# Storage API: cancel_queued_run
# ═══════════════════════════════════════════════════════════════════════════


def test_cancel_queued_run_success(storage):
    """cancel_queued_run transitions Queued → Canceled."""
    storage._create_run("q1", "job1", "Queued", 1000, block_reason="global limit")

    canceled = storage.cancel_queued_run("q1")
    assert canceled is True

    run = storage.get_run("q1")
    assert run.status == "Canceled"
    assert run.end_time is not None


def test_cancel_queued_run_cleans_pending_steps(storage):
    """cancel_queued_run removes orphaned pending_steps rows."""
    storage.set_pool_limit("db", 1)
    # Fill the pool so next claim goes to pending
    storage._claim_concurrency_slots([("db", 1)], "blocker", "blocker_step", 0, "5m")
    # This claim will go to pending_steps
    storage._create_run("q1", "job1", "Queued", 1000)
    status = storage._claim_concurrency_slots([("db", 1)], "q1", "step_a", 0, "5m")
    assert status.status == "pending"

    info = storage.get_pool_info("db")
    assert info.pending_count == 1

    # Cancel the queued run — should clean up pending_steps
    canceled = storage.cancel_queued_run("q1")
    assert canceled is True

    info = storage.get_pool_info("db")
    assert info.pending_count == 0


def test_cancel_queued_run_noop_for_started(storage):
    """cancel_queued_run returns False for a non-Queued run."""
    storage._create_run("r1", "job1", "Started", 1000)
    canceled = storage.cancel_queued_run("r1")
    assert canceled is False

    run = storage.get_run("r1")
    assert run.status == "Started"


def test_cancel_queued_run_not_found(storage):
    """cancel_queued_run returns False for non-existent run."""
    canceled = storage.cancel_queued_run("nonexistent")
    assert canceled is False


# ═══════════════════════════════════════════════════════════════════════════
# Storage API: get_all_pool_infos
# ═══════════════════════════════════════════════════════════════════════════


def test_get_all_pool_infos_batched(storage):
    """get_all_pool_infos returns all pools with claimed/pending counts."""
    storage.set_pool_limit("database", 5)
    storage.set_pool_limit("api", 10)
    storage._claim_concurrency_slots([("database", 1)], "r1", "step_a", 0, "5m")

    infos = storage.get_all_pool_infos()
    assert len(infos) == 2
    by_key = {i.pool_key: i for i in infos}
    assert by_key["database"].slot_limit == 5
    assert by_key["database"].claimed_count == 1
    assert by_key["api"].slot_limit == 10
    assert by_key["api"].claimed_count == 0


def test_get_all_pool_infos_empty(storage):
    """get_all_pool_infos returns empty list when no pools configured."""
    infos = storage.get_all_pool_infos()
    assert infos == []


# ═══════════════════════════════════════════════════════════════════════════
# Storage API: queue data
# ═══════════════════════════════════════════════════════════════════════════


def test_queued_runs_data(storage):
    """Queued runs have correct priority, status, and block_reason."""
    storage._create_run(
        "q1",
        "etl_daily",
        "Queued",
        1000,
        priority=5,
        block_reason="global limit: 2/2 in progress",
    )
    storage._create_run("q2", "ml_train", "Queued", 2000, priority=0)

    queued = storage.get_queued_runs()
    assert len(queued) == 2

    q1 = storage.get_run("q1")
    assert q1.status == "Queued"
    assert q1.priority == 5
    assert q1.block_reason == "global limit: 2/2 in progress"


def test_queued_runs_sort_order(storage):
    """Queued runs sort by priority DESC, start_time ASC."""
    storage._create_run(
        "q1",
        "etl",
        "Queued",
        1000,
        priority=10,
        block_reason="global limit: 2/2 in progress",
        tags=[("team", "data")],
    )
    storage._create_run("q2", "ml", "Queued", 2000, priority=0)

    queued = storage.get_queued_runs()
    queued.sort(key=lambda r: (-r.priority, r.start_time))
    assert queued[0].run_id == "q1"  # highest priority first


def test_non_queued_run_status(storage):
    """Non-queued run has correct status."""
    storage._create_run("r1", "job1", "Success", 1000)
    run = storage.get_run("r1")
    assert run.status == "Success"


# ═══════════════════════════════════════════════════════════════════════════
# CLI: rivers pools list
# ═══════════════════════════════════════════════════════════════════════════


def test_cli_pools_list_empty(tmp_path):
    """pools list with no pools shows 'No pools configured.'"""
    path = str(tmp_path / "cli_db")
    result = runner.invoke(app, ["pools", "list", "--storage-path", path])
    assert result.exit_code == 0
    assert "No pools configured" in result.output


# ═══════════════════════════════════════════════════════════════════════════
# CLI: rivers pools set
# ═══════════════════════════════════════════════════════════════════════════


def test_cli_pools_set_creates_pool(tmp_path):
    """pools set creates a new pool."""
    path = str(tmp_path / "cli_db")
    result = runner.invoke(app, ["pools", "set", "gpu", "3", "--storage-path", path])
    assert result.exit_code == 0
    assert "gpu" in result.output
    assert "limit=3" in result.output


def test_cli_pools_set_with_lease(tmp_path):
    """pools set with --lease-duration sets the lease."""
    path = str(tmp_path / "cli_db")
    result = runner.invoke(
        app,
        ["pools", "set", "gpu", "3", "--lease-duration", "10m", "--storage-path", path],
    )
    assert result.exit_code == 0
    assert "lease_duration=10m" in result.output


# ═══════════════════════════════════════════════════════════════════════════
# CLI: rivers queue list
# ═══════════════════════════════════════════════════════════════════════════


def test_cli_queue_list_empty(tmp_path):
    """queue list with no queued runs shows 'No queued runs.'"""
    path = str(tmp_path / "cli_db")
    result = runner.invoke(app, ["queue", "list", "--storage-path", path])
    assert result.exit_code == 0
    assert "No queued runs" in result.output


# ═══════════════════════════════════════════════════════════════════════════
# CLI: rivers queue cancel
# ═══════════════════════════════════════════════════════════════════════════


def test_cli_queue_cancel_not_found(tmp_path):
    """queue cancel for non-existent run shows error."""
    path = str(tmp_path / "cli_db")
    result = runner.invoke(app, ["queue", "cancel", "fake-id", "--storage-path", path])
    assert result.exit_code == 1


# ═══════════════════════════════════════════════════════════════════════════
# CLI: rivers queue why
# ═══════════════════════════════════════════════════════════════════════════


def test_cli_queue_why_not_found(tmp_path):
    """queue why for non-existent run shows error."""
    path = str(tmp_path / "cli_db")
    result = runner.invoke(app, ["queue", "why", "fake-id", "--storage-path", path])
    assert result.exit_code == 1
