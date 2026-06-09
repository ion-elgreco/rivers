"""Integration test for graceful shutdown.

Verifies that in-flight materializations complete before the process exits
when a terminate signal is received during execution.

Run with:
    pytest python/tests/integration/test_graceful_shutdown.py -v
"""

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

FIXTURES = Path(__file__).parent / "shutdown_fixtures"
REPO_ROOT = Path(__file__).resolve().parents[3]
PROTO_PATH = str(REPO_ROOT / "proto")


_NEW_PROCESS_GROUP = (
    subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
)
_TERMINATE_SIGNAL = (
    signal.CTRL_BREAK_EVENT if sys.platform == "win32" else signal.SIGTERM
)


def _wait_for_port(port: int, timeout: float = 15.0) -> bool:
    """Poll until a TCP port is accepting connections."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def _find_free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _start_server(env, err_path: Path) -> subprocess.Popen:
    """Start the server subprocess, sending its stderr to a file.

    stderr must be a file, not a `subprocess.PIPE`. The server logs
    continuously, and a pipe the parent doesn't drain fills its fixed OS buffer
    (only a few KB on Windows) and then *blocks* the server's own writes — including
    the signal handler's log line, which runs right before it cancels the drain
    token. That wedged graceful shutdown until the parent happened to read the
    pipe. A file has no buffer limit, so the server never blocks on logging.
    """
    with open(err_path, "wb") as err_f:
        return subprocess.Popen(
            [sys.executable, str(FIXTURES / "server_runner.py")],
            stdout=subprocess.PIPE,
            stderr=err_f,
            env=env,
            creationflags=_NEW_PROCESS_GROUP,
        )


def _read_ready_line(
    proc: subprocess.Popen, err_path: Path, timeout: float = 15.0
) -> str:
    """Read the first stdout line from a server subprocess, with diagnostics.

    `proc.stdout.readline()` blocks until a newline arrives or EOF (when the
    subprocess exits and closes stdout). On empty result, the subprocess died
    before printing — surface its exit code + stderr so the failure is
    actually debuggable instead of "assert ''.startswith('READY:')".
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        line = proc.stdout.readline().decode().strip()
        if line:
            return line
        if proc.poll() is not None:
            stderr = err_path.read_text(errors="replace") if err_path.exists() else ""
            raise AssertionError(
                f"Server subprocess exited before printing READY "
                f"(exit code {proc.returncode}).\nstderr:\n{stderr}"
            )
        time.sleep(0.05)
    raise AssertionError(
        f"Server subprocess did not print READY within {timeout}s "
        f"and is still running (pid={proc.pid})."
    )


def _server_env(tmp_path, pipeline_module, grpc_port, **extra):
    env = os.environ.copy()
    env.update(
        {
            "PYTHONUNBUFFERED": "1",
            "PYTHONPATH": str(FIXTURES),
            "PIPELINE_MODULE": pipeline_module,
            "STORAGE_PATH": str(tmp_path / "storage"),
            "GRPC_PORT": str(grpc_port),
        }
    )
    env.update(extra)
    return env


class TestGracefulShutdown:
    def test_inflight_materialization_completes_on_sigterm(self, tmp_path):
        """Start a gRPC server, trigger a slow materialization, send a terminate
        signal mid-flight, and verify the work completed before the process
        exited."""
        marker = tmp_path / "completed.marker"
        err_path = tmp_path / "server.err"
        grpc_port = _find_free_port()

        env = _server_env(tmp_path, "pipeline_slow", grpc_port, MARKER_PATH=str(marker))
        server = _start_server(env, err_path)

        trigger_proc = None
        try:
            ready_line = _read_ready_line(server, err_path)
            assert ready_line.startswith("READY:"), f"Unexpected output: {ready_line}"
            actual_port = int(ready_line.split(":")[1])
            assert _wait_for_port(actual_port, timeout=10), "gRPC server did not start"

            trigger_env = os.environ.copy()
            trigger_env.update(
                {
                    "PYTHONUNBUFFERED": "1",
                    "GRPC_PORT": str(actual_port),
                    "PROTO_PATH": PROTO_PATH,
                }
            )
            trigger_proc = subprocess.Popen(
                [sys.executable, str(FIXTURES / "grpc_trigger.py")],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=trigger_env,
            )

            calling_line = trigger_proc.stdout.readline().decode().strip()
            assert calling_line == "CALLING", (
                f"Unexpected trigger output: {calling_line}"
            )

            time.sleep(1.5)
            server.send_signal(_TERMINATE_SIGNAL)
            try:
                server.wait(timeout=20)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)
            try:
                trigger_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                trigger_proc.kill()
        finally:
            for p in [server, trigger_proc]:
                if p is not None and p.poll() is None:
                    p.kill()
                    p.wait(timeout=5)

        stderr = err_path.read_text(errors="replace")
        assert server.returncode == 0, (
            f"Server exited with {server.returncode}; marker_written={marker.exists()}\n"
            f"stderr:\n{stderr}"
        )
        assert marker.exists(), (
            f"Marker file not created — materialization was killed before completion.\n"
            f"stderr:\n{stderr}"
        )
        assert "drain signal received" in stderr or "SIGTERM" in stderr, (
            f"No drain signal in logs.\nstderr:\n{stderr}"
        )
        assert "shutdown complete" in stderr, (
            f"Shutdown did not complete.\nstderr:\n{stderr}"
        )

    def test_idle_server_shuts_down_cleanly(self, tmp_path):
        """An idle server should shut down quickly on a terminate signal."""
        err_path = tmp_path / "server.err"
        grpc_port = _find_free_port()
        env = _server_env(tmp_path, "pipeline_noop", grpc_port)

        proc = _start_server(env, err_path)
        try:
            ready_line = _read_ready_line(proc, err_path)
            assert ready_line.startswith("READY:"), f"Unexpected output: {ready_line}"
            actual_port = int(ready_line.split(":")[1])
            assert _wait_for_port(actual_port, timeout=10)

            proc.send_signal(_TERMINATE_SIGNAL)
            proc.wait(timeout=10)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)

        stderr = err_path.read_text(errors="replace")
        assert proc.returncode == 0, f"Exit code {proc.returncode}\nstderr:\n{stderr}"
        assert "shutdown complete" in stderr, (
            f"No shutdown complete in logs.\nstderr:\n{stderr}"
        )
