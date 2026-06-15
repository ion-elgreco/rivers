"""Shared fixtures for the rivers test suite.

Fixtures defined here cascade to every test in `python/tests/` and its
subdirectories. Helper modules (`_helpers.py`, `_polling.py`) live alongside
this file and can be imported directly because pytest adds this directory to
`sys.path` automatically (no `__init__.py`).
"""

from __future__ import annotations

import importlib
import os
import sys
import types
from pathlib import Path

# Configure Rust tracing before `import rivers` triggers `_core`'s pymodule
# init — the `EnvFilter` is built once at that point from `RUST_LOG`. Pytest
# captures stderr per test, so on a green run this is invisible; on a
# failure/timeout the captured logs surface alongside the traceback, which
# is exactly when we want to see the storage open / retry trail.
os.environ.setdefault("RUST_LOG", "rivers=debug,rivers_core=debug,rivers_ui=info,warn")

import obstore.store
import pytest

import rivers as rs
from rivers.testing import embedded_storage as _embedded_storage_factory
from rivers.testing import memory_storage as _memory_storage_factory

PROTO_PATH = Path(__file__).resolve().parents[2] / "proto"


@pytest.fixture
def storage():
    """In-memory Storage. Default for tests that don't need transaction-conflict
    semantics — avoids the per-test RocksDB resource accumulation that hangs CI
    after ~18 tests (router-task shutdown is async; instances pile up faster
    than they drain). Tests that genuinely exercise write-write conflicts
    should request :func:`embedded_storage` instead.

    Uses :func:`rivers.testing.memory_storage` so the storage owns its own
    tokio runtime; drop releases all router-task state synchronously and
    prevents the shared ``io_rt`` from saturating across the test session.
    """
    return _memory_storage_factory()


@pytest.fixture
def embedded_storage(tmp_path):
    """Embedded RocksDB-backed Storage with a per-test directory.

    Use only when transaction-conflict semantics matter — kv-mem misses some
    write-write conflicts (race in commit-queue check causes lost updates).

    Uses :func:`rivers.testing.embedded_storage` so drop releases the RocksDB
    file lock synchronously, letting the next test open a fresh path without
    contention.
    """
    return _embedded_storage_factory(str(tmp_path / "test_db"))


@pytest.fixture(scope="session", autouse=True)
def _shutdown_loky_workers():
    """Tear down loky's reusable process pool at session end.

    `loky.get_reusable_executor()` returns a process-wide singleton that is
    never shut down; without this, its background threads (QueueFeederThread,
    ExecutorManagerThread) and worker subprocesses survive until the pytest
    process exits.
    """
    yield
    try:
        import loky

        loky.get_reusable_executor().shutdown(wait=True, kill_workers=True)
    except ImportError:
        pass


@pytest.fixture(params=["in_process", "parallel"])
def executor_env(request, tmp_path):
    """Parametrized (executor, io_handler_factory) for both executor types."""
    if request.param == "in_process":
        store = obstore.store.MemoryStore()
        return rs.Executor.in_process(), lambda: rs.PickleIOHandler(store=store)
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    return rs.Executor.parallel(max_workers=2), lambda: rs.PickleIOHandler(store=store)


def _load_module(name: str, path: Path) -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="session")
def grpc_stubs(tmp_path_factory):
    """Compile rivers.proto into Python gRPC stubs once per test session."""
    from grpc_tools import protoc

    out_dir = tmp_path_factory.mktemp("grpc_stubs")
    result = protoc.main(
        [
            "grpc_tools.protoc",
            f"-I{PROTO_PATH}",
            f"--python_out={out_dir}",
            f"--grpc_python_out={out_dir}",
            str(PROTO_PATH / "rivers.proto"),
        ]
    )
    assert result == 0, f"protoc failed with exit code {result}"

    pb2 = _load_module("rivers_pb2", out_dir / "rivers_pb2.py")
    # grpc stub imports rivers_pb2 by name — temporarily inject so the loader finds it
    sys.modules["rivers_pb2"] = pb2
    pb2_grpc = _load_module("rivers_pb2_grpc", out_dir / "rivers_pb2_grpc.py")
    del sys.modules["rivers_pb2"]

    return pb2, pb2_grpc


def pytest_runtest_setup(item):
    if item.get_closest_marker("spark_test"):
        if sys.platform == "win32":
            pytest.skip(
                "Skipping PySpark test on Windows since it requires winutils for Hadoop."
            )
