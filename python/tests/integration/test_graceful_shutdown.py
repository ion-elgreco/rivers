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
from contextlib import contextmanager
from pathlib import Path

import pytest
from _polling import free_port, wait_for

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


def _ready_port(server: subprocess.Popen, err_path: Path) -> int:
    """The port a server subprocess reports in its READY line, once it accepts."""
    ready_line = _read_ready_line(server, err_path)
    assert ready_line.startswith("READY:"), f"Unexpected output: {ready_line}"
    port = int(ready_line.split(":")[1])
    assert _wait_for_port(port, timeout=10), "gRPC server did not start"
    return port


def _start_trigger(port: int) -> subprocess.Popen:
    """Start a slow materialization over gRPC; returns once the call is in flight."""
    env = os.environ.copy()
    env.update(
        {"PYTHONUNBUFFERED": "1", "GRPC_PORT": str(port), "PROTO_PATH": PROTO_PATH}
    )
    proc = subprocess.Popen(
        [sys.executable, str(FIXTURES / "grpc_trigger.py")],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    calling_line = proc.stdout.readline().decode().strip()
    assert calling_line == "CALLING", f"Unexpected trigger output: {calling_line}"
    return proc


def _port_closed(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            return False
    except OSError:
        return True


def _kill_leftovers(*procs: subprocess.Popen | None) -> None:
    for p in procs:
        if p is not None and p.poll() is None:
            p.kill()
            p.wait(timeout=5)


@contextmanager
def _retiring_slow_run(tmp_path):
    """A retiring server with a slow run in flight: the first terminate signal
    has released its gRPC port while the run goes on. Yields the server, the
    trigger client, the run's completion marker and the server's stderr path."""
    pytest.importorskip("grpc")
    pytest.importorskip("grpc_tools")
    marker = tmp_path / "completed.marker"
    err_path = tmp_path / "server.err"
    env = _server_env(
        tmp_path, "pipeline_slow", free_port(), MARKER_PATH=str(marker), RETIRE="1"
    )
    server = _start_server(env, err_path)
    trigger_proc = None
    try:
        port = _ready_port(server, err_path)
        trigger_proc = _start_trigger(port)
        time.sleep(1.0)
        server.send_signal(_TERMINATE_SIGNAL)
        wait_for(lambda: _port_closed(port), 10, "the gRPC port to be released")
        yield server, trigger_proc, marker, err_path
    finally:
        _kill_leftovers(server, trigger_proc)


class TestGracefulShutdown:
    def test_inflight_materialization_completes_on_sigterm(self, tmp_path):
        """Start a gRPC server, trigger a slow materialization, send a terminate
        signal mid-flight, and verify the work completed before the process
        exited."""
        # The trigger client is a grpcio script.
        pytest.importorskip("grpc")
        pytest.importorskip("grpc_tools")
        marker = tmp_path / "completed.marker"
        err_path = tmp_path / "server.err"
        grpc_port = free_port()

        env = _server_env(tmp_path, "pipeline_slow", grpc_port, MARKER_PATH=str(marker))
        server = _start_server(env, err_path)

        trigger_proc = None
        try:
            trigger_proc = _start_trigger(_ready_port(server, err_path))

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
            _kill_leftovers(server, trigger_proc)

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
        grpc_port = free_port()
        env = _server_env(tmp_path, "pipeline_noop", grpc_port)

        proc = _start_server(env, err_path)
        try:
            _ready_port(proc, err_path)
            proc.send_signal(_TERMINATE_SIGNAL)
            proc.wait(timeout=10)
        finally:
            _kill_leftovers(proc)

        stderr = err_path.read_text(errors="replace")
        assert proc.returncode == 0, f"Exit code {proc.returncode}\nstderr:\n{stderr}"
        assert "shutdown complete" in stderr, (
            f"No shutdown complete in logs.\nstderr:\n{stderr}"
        )


class TestRetire:
    """``wait_for_exit(retire=True)``: the first terminate signal releases the
    gRPC port as soon as the daemon stopped scheduling and drains in-flight
    runs without a cap; the next signals bring the cap back, then force."""

    def test_port_is_released_while_the_run_finishes(self, tmp_path):
        with _retiring_slow_run(tmp_path) as (server, trigger_proc, marker, err_path):
            assert not marker.exists(), "the port was released only after the run"
            server.wait(timeout=20)
            trigger_proc.wait(timeout=10)

        stderr = err_path.read_text(errors="replace")
        assert server.returncode == 0, f"exit {server.returncode}\nstderr:\n{stderr}"
        assert marker.exists(), f"the run did not finish\nstderr:\n{stderr}"
        assert "shutdown complete" in stderr, stderr
        assert "timed out" not in stderr, stderr

    def test_port_is_released_once_the_daemon_stopped_scheduling(self, tmp_path):
        eval_marker = tmp_path / "eval.marker"
        err_path = tmp_path / "server.err"
        env = _server_env(
            tmp_path,
            "pipeline_slow_sensor",
            free_port(),
            EVAL_MARKER=str(eval_marker),
            EVAL_SLEEP="4",
            DAEMON="1",
            RETIRE="1",
        )
        server = _start_server(env, err_path)
        try:
            port = _ready_port(server, err_path)
            wait_for(eval_marker.exists, 15, "the first sensor evaluation")
            signalled = time.monotonic()
            server.send_signal(_TERMINATE_SIGNAL)
            wait_for(lambda: _port_closed(port), 20, "the gRPC port to be released")
            released_after = time.monotonic() - signalled
            server.wait(timeout=20)
        finally:
            _kill_leftovers(server)

        stderr = err_path.read_text(errors="replace")
        assert server.returncode == 0, f"exit {server.returncode}\nstderr:\n{stderr}"
        assert released_after >= 2.0, (
            f"port released {released_after:.1f}s after the signal, "
            f"while a sensor evaluation was still running\nstderr:\n{stderr}"
        )
        assert "daemon stopped scheduling" in stderr, stderr
        assert "shutdown complete" in stderr, stderr

    def test_second_signal_caps_the_drain_and_third_forces_exit(self, tmp_path):
        with _retiring_slow_run(tmp_path) as (server, _trigger_proc, marker, err_path):
            server.send_signal(_TERMINATE_SIGNAL)
            wait_for(
                lambda: (
                    "capping the drain at 30s" in err_path.read_text(errors="replace")
                ),
                10,
                "the second signal to arm the cap",
            )
            server.send_signal(_TERMINATE_SIGNAL)
            server.wait(timeout=10)

        stderr = err_path.read_text(errors="replace")
        assert server.returncode == 1, f"exit {server.returncode}\nstderr:\n{stderr}"
        assert "force exiting" in stderr, stderr
        assert not marker.exists(), "the third signal did not cut the run short"
