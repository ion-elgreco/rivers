"""Tests for error propagation through executors and IO handlers."""

import pytest

import rivers as rs


class FailingOutputHandler(rs.BaseIOHandler):
    def handle_output(self, context, obj):
        raise RuntimeError("handle_output exploded")

    def load_input(self, context):
        return None


class FailingInputHandler(rs.BaseIOHandler):
    store: dict = {}

    def handle_output(self, context, obj):
        self.store[context.asset_name] = obj

    def load_input(self, context):
        raise RuntimeError("load_input exploded")


# ---------------------------------------------------------------------------
# Asset function errors
# ---------------------------------------------------------------------------


def test_asset_function_exception_propagates():
    """Asset function that raises propagates through in-process executor."""

    @rs.Asset
    def broken():
        raise ValueError("computation failed")

    repo = rs.CodeRepository(
        assets=[broken],
        default_executor=rs.Executor.in_process(),
    )
    with pytest.raises(ValueError, match="computation failed"):
        repo.materialize()


def test_asset_function_exception_propagates_parallel():
    """Asset function exception propagates through parallel executor."""

    @rs.Asset
    def broken():
        raise ValueError("mp computation failed")

    repo = rs.CodeRepository(
        assets=[broken],
        default_executor=rs.Executor.parallel(max_workers=1),
    )
    with pytest.raises(Exception, match="mp computation failed"):
        repo.materialize()


# ---------------------------------------------------------------------------
# IO handler errors
# ---------------------------------------------------------------------------


def test_io_handler_handle_output_exception():
    """Exception in handle_output propagates to caller."""

    @rs.Asset(io_handler=FailingOutputHandler())
    def asset_with_bad_output() -> int:
        return 42

    repo = rs.CodeRepository(
        assets=[asset_with_bad_output],
        default_executor=rs.Executor.in_process(),
    )
    with pytest.raises(RuntimeError, match="handle_output exploded"):
        repo.materialize()


def test_io_handler_load_input_exception():
    """Exception in load_input propagates when loading incomplete dep."""

    @rs.Asset(io_handler=FailingInputHandler())
    def upstream() -> int:
        return 1

    @rs.Asset
    def downstream(upstream: int) -> int:
        return upstream + 1

    job = rs.Job(
        name="test",
        assets=[downstream],
        executor=rs.Executor.in_process(),
        allow_incomplete_deps=True,
    )
    repo = rs.CodeRepository(assets=[upstream, downstream], jobs=[job])
    with pytest.raises(RuntimeError, match="load_input exploded"):
        repo.get_job("test").execute()


# ---------------------------------------------------------------------------
# BashTask failure
# ---------------------------------------------------------------------------


def test_bash_task_failure_propagates():
    """BashTask that exits non-zero raises OSError."""
    task = rs.BashTask(name="fail_task", command="exit 1")

    repo = rs.CodeRepository(
        assets=[],
        tasks=[task],
        default_executor=rs.Executor.in_process(),
    )
    with pytest.raises(OSError, match="Command failed"):
        repo.materialize()


def test_bash_task_nonexistent_command():
    """BashTask with a non-existent command raises OSError."""
    task = rs.BashTask(name="bad_cmd", command=["nonexistent_binary_xyz_123"])
    with pytest.raises(OSError):
        task()


def test_dep_fail_chain_parallel(tmp_path):
    """Three-level chain where root fails under Parallel — mid and leaf skipped."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset(io_handler=handler)
    def chain_root() -> int:
        raise ValueError("root fails")

    @rs.Asset(io_handler=handler)
    def chain_mid(chain_root: int) -> int:
        return chain_root + 1

    @rs.Asset(io_handler=handler)
    def chain_leaf(chain_mid: int) -> int:
        return chain_mid + 1

    assets = [chain_root, chain_mid, chain_leaf]
    repo = rs.CodeRepository(
        assets=assets,
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    result = repo.materialize(raise_on_error=False)
    assert not result.success
    failure_names = [name for name, _ in result.failed_assets]
    assert "chain_root" in failure_names
    assert "chain_mid" in failure_names
    assert "chain_leaf" in failure_names
