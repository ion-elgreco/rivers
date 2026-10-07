"""``rivers dev`` reloads its code location in a fresh interpreter while the
storage server, the web UI and this host process stay up."""

import json
import os
import signal
import subprocess
import sys
import urllib.error
import urllib.request
from contextlib import contextmanager
from pathlib import Path

import pytest
from _polling import free_port, wait_for

pytest.importorskip("grpc")
pytest.importorskip("grpc_tools")
import grpc  # noqa: E402

pytestmark = [
    pytest.mark.skipif(sys.platform == "win32", reason="SIGTERM is POSIX"),
    pytest.mark.timeout(300),
]

MODULE_V1 = """
import rivers as rs


@rs.Asset(name="first")
def first() -> int:
    return 1


repo = rs.CodeRepository(assets=[first])
"""

MODULE_V2 = """
import rivers as rs


@rs.Asset(name="first")
def first() -> int:
    return 1


@rs.Asset(name="second")
def second(first: int) -> int:
    return first + 1


repo = rs.CodeRepository(assets=[first, second])
"""

MODULE_BROKEN = "import rivers as rs\nrepo = rs.CodeRepository(assets=[not_defined])\n"

# An asset that blocks until RELEASE_PATH exists, then writes MARKER_PATH.
BLOCKING_ASSET = """
import os
import time

import rivers as rs


def _wait_for_release() -> None:
    while not os.path.exists(os.environ["RELEASE_PATH"]):
        time.sleep(0.1)


@rs.Asset(name="first")
def first() -> int:
    return 1


@rs.Asset(name="blocking")
def blocking() -> int:
    _wait_for_release()
    with open(os.environ["MARKER_PATH"], "w") as f:
        f.write("done")
    return 1
"""

MODULE_BLOCKING = (
    BLOCKING_ASSET
    + """

repo = rs.CodeRepository(assets=[first, blocking])
"""
)

MODULE_BLOCKING_V2 = (
    BLOCKING_ASSET
    + """

@rs.Asset(name="second")
def second(first: int) -> int:
    return first + 1


repo = rs.CodeRepository(assets=[first, blocking, second])
"""
)

# A partitioned asset whose runs block until RELEASE_PATH exists; one run at a time.
MODULE_BACKFILL = """
import os
import time

import rivers as rs


@rs.Asset(name="part", partitions_def=rs.PartitionsDefinition.static_(["p1", "p2", "p3", "p4"]))
def part() -> int:
    while not os.path.exists(os.environ["RELEASE_PATH"]):
        time.sleep(0.1)
    return 1


repo = rs.CodeRepository(
    assets=[part],
    run_queue=rs.RunQueueConfig(max_concurrent_runs=1, dequeue_interval="100ms"),
)
"""


class DevHostProcess:
    """A ``rivers dev`` process on free ports, with its logs in files."""

    def __init__(
        self, tmp_path: Path, stubs, env: dict[str, str] | None = None, daemon=False
    ) -> None:
        self.tmp_path = tmp_path
        self.pb2, self.pb2_grpc = stubs
        self.ui_port = free_port()
        self.grpc_port = free_port()
        self.storage_dir = tmp_path / "storage"
        self.out_path = tmp_path / "dev.out"
        self.err_path = tmp_path / "dev.err"
        full_env = {k: v for k, v in os.environ.items() if not k.startswith("RIVERS_")}
        full_env["PYTHONUNBUFFERED"] = "1"
        full_env.update(env or {})
        self.out_file = open(self.out_path, "wb")
        self.err_file = open(self.err_path, "wb")
        self.proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "rivers",
                "dev",
                "pipe_mod",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.ui_port),
                "--grpc-port",
                str(self.grpc_port),
                "--storage-path",
                str(self.storage_dir),
            ]
            + ([] if daemon else ["--no-daemon"]),
            cwd=tmp_path,
            env=full_env,
            stdout=self.out_file,
            stderr=self.err_file,
        )

    @contextmanager
    def _stub(self):
        """A gRPC stub on the code location, closed with the block."""
        with grpc.insecure_channel(f"127.0.0.1:{self.grpc_port}") as channel:
            yield self.pb2_grpc.CodeLocationServiceStub(channel)

    def materialize(self, asset: str) -> str:
        """Start a run of ``asset`` over gRPC; returns its run id."""
        with self._stub() as stub:
            resp = stub.Materialize(
                self.pb2.MaterializeRequest(selection=[asset]), timeout=10
            )
        assert resp.run_id, resp
        return resp.run_id

    def launch_backfill(self, asset: str, keys: list[str]) -> str:
        with self._stub() as stub:
            resp = stub.LaunchBackfill(
                self.pb2.LaunchBackfillRequest(
                    selection=[asset],
                    partition_keys=[
                        self.pb2.ProtoPartitionKey(
                            single=self.pb2.SinglePartitionKey(keys=[k])
                        )
                        for k in keys
                    ],
                    failure_policy="continue",
                    max_concurrency=1,
                ),
                timeout=10,
            )
        return resp.backfill_id

    def backfill_status(self, backfill_id: str):
        with self._stub() as stub:
            return stub.GetBackfillStatus(
                self.pb2.GetBackfillStatusRequest(backfill_id=backfill_id), timeout=10
            )

    def ui_up(self) -> bool:
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{self.ui_port}/healthz", timeout=2
            ) as resp:
                return resp.status == 200
        except (urllib.error.URLError, OSError):
            return False

    def asset_names(self) -> set[str] | None:
        """Names the code location serves over gRPC; ``None`` while it is down."""
        try:
            with self._stub() as stub:
                resp = stub.GetAssetsInfo(self.pb2.GetAssetsInfoRequest(), timeout=5)
        except grpc.RpcError:
            return None
        return {a.asset_key for a in resp.assets}

    def wait_ready(self) -> None:
        def up():
            assert self.proc.poll() is None, f"rivers dev exited early:\n{self.logs()}"
            return self.ui_up() and self.asset_names() is not None

        wait_for(up, 120, "the dev host and its code location")

    def _post(self, path: str) -> object:
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.ui_port}{path}",
            data=b"",
            method="POST",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        with urllib.request.urlopen(request, timeout=5) as resp:
            body = resp.read()
        return json.loads(body) if body else None

    def reload_state(self) -> dict:
        return self._post("/api/dev/reload-state")

    def reload_via_http(self) -> None:
        self._post("/api/dev/reload")

    def logs(self) -> str:
        for f in (self.out_file, self.err_file):
            if not f.closed:
                f.flush()
        return (
            f"--- stdout ---\n{self.out_path.read_text(errors='replace')}\n"
            f"--- stderr ---\n{self.err_path.read_text(errors='replace')}"
        )

    def stop(self) -> int:
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=90)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        return self.proc.returncode

    def close(self) -> None:
        self.out_file.close()
        self.err_file.close()


@pytest.fixture
def start_dev_host(tmp_path, grpc_stubs):
    """Write ``pipe_mod.py`` from a source string and start ``rivers dev`` on it."""
    hosts = []

    def start(module_source: str, **kwargs) -> DevHostProcess:
        (tmp_path / "pipe_mod.py").write_text(module_source)
        host = DevHostProcess(tmp_path, grpc_stubs, **kwargs)
        hosts.append(host)
        return host

    try:
        yield start
    finally:
        for host in hosts:
            host.stop()
            host.close()


@pytest.fixture
def dev_host(start_dev_host):
    return start_dev_host(MODULE_V1)


def test_reload_serves_the_edited_module(dev_host, tmp_path):
    dev_host.wait_ready()
    assert dev_host.asset_names() == {"first"}
    state = dev_host.reload_state()
    assert state == {"enabled": True, "generation": 0, "error": None}

    (tmp_path / "pipe_mod.py").write_text(MODULE_V2)
    dev_host.reload_via_http()

    wait_for(
        lambda: dev_host.asset_names() == {"first", "second"},
        120,
        "the reloaded code location to serve both assets",
    )
    assert dev_host.ui_up(), "the UI never went down"
    wait_for(
        lambda: dev_host.reload_state()["generation"] == 1,
        30,
        "the host to report the reload",
    )
    assert dev_host.reload_state()["error"] is None

    code = dev_host.stop()
    logs = dev_host.logs()
    assert code == 0, logs
    assert "Code location reloaded." in logs
    assert not dev_host.storage_dir.exists(), "embedded storage is removed at exit"


def test_a_broken_edit_keeps_the_session_alive(dev_host, tmp_path):
    dev_host.wait_ready()
    host_pid = dev_host.proc.pid

    (tmp_path / "pipe_mod.py").write_text(MODULE_BROKEN)
    dev_host.reload_via_http()
    wait_for(
        lambda: dev_host.reload_state()["error"] is not None,
        120,
        "the host to report the failed reload",
    )
    assert dev_host.proc.poll() is None, "the host survives a broken edit"
    assert dev_host.ui_up()
    assert dev_host.reload_state()["generation"] == 0

    (tmp_path / "pipe_mod.py").write_text(MODULE_V2)
    dev_host.reload_via_http()
    wait_for(
        lambda: dev_host.asset_names() == {"first", "second"},
        120,
        "the fixed module to be served",
    )
    wait_for(
        lambda: (
            dev_host.reload_state() == {"enabled": True, "generation": 1, "error": None}
        ),
        30,
        "the host to clear the error",
    )
    assert dev_host.proc.pid == host_pid

    code = dev_host.stop()
    assert code == 0, dev_host.logs()


def test_the_child_entry_reports_a_missing_module(tmp_path):
    """The code-location entry prints the error ``rivers serve`` would and exits 1."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("RIVERS_")}
    env.update(
        {
            "RIVERS_MODULE": "nonexistent_module_xyz",
            "RIVERS_DEV_REPO_VAR": "repo",
            "RIVERS_DEV_HOST": "127.0.0.1",
            "RIVERS_DEV_GRPC_PORT": str(free_port()),
            "RIVERS_SURREAL_ENDPOINT": "ws://127.0.0.1:1",
            "RIVERS_DEV_NO_DAEMON": "1",
        }
    )
    result = subprocess.run(
        [sys.executable, "-c", "import rivers._core as c; c.serve_dev_code_location()"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 1, result.stderr
    assert "module 'nonexistent_module_xyz' not found" in result.stderr


def test_a_run_in_flight_finishes_in_the_retired_generation(start_dev_host, tmp_path):
    """A reload does not wait for runs: the old generation keeps running them
    in the background while the new one already serves the edited module."""
    release, marker = tmp_path / "release", tmp_path / "completed.marker"
    dev_host = start_dev_host(
        MODULE_BLOCKING,
        env={"RELEASE_PATH": str(release), "MARKER_PATH": str(marker)},
    )
    dev_host.wait_ready()
    dev_host.materialize("blocking")

    (tmp_path / "pipe_mod.py").write_text(MODULE_BLOCKING_V2)
    dev_host.reload_via_http()
    wait_for(
        lambda: dev_host.asset_names() == {"first", "blocking", "second"},
        120,
        "the reloaded code location to serve the edited module",
    )
    assert not marker.exists(), "the reload waited for the run"
    assert "Generation 0 finished." not in dev_host.logs()

    release.write_text("go")
    wait_for(marker.exists, 30, "the run to finish in the retired generation")
    wait_for(
        lambda: "Generation 0 finished." in dev_host.logs(),
        30,
        "the retired generation to exit on its own",
    )
    assert dev_host.reload_state() == {"enabled": True, "generation": 1, "error": None}

    code = dev_host.stop()
    logs = dev_host.logs()
    assert code == 0, logs
    assert "exited with code" not in logs


def test_a_backfill_completes_across_a_reload(start_dev_host, tmp_path):
    """Queued runs and backfills live in storage: the retired generation's
    daemon stops picking work up, the new one carries the backfill to the end."""
    release = tmp_path / "release"
    dev_host = start_dev_host(
        MODULE_BACKFILL, env={"RELEASE_PATH": str(release)}, daemon=True
    )
    dev_host.wait_ready()
    backfill_id = dev_host.launch_backfill("part", ["p1", "p2", "p3", "p4"])
    wait_for(
        lambda: len(dev_host.backfill_status(backfill_id).run_ids) == 4,
        60,
        "the daemon to submit the backfill's runs",
    )

    dev_host.reload_via_http()
    wait_for(
        lambda: dev_host.reload_state()["generation"] == 1,
        120,
        "the host to report the reload",
    )
    assert dev_host.backfill_status(backfill_id).completed_partitions == 0

    release.write_text("go")
    wait_for(
        lambda: dev_host.backfill_status(backfill_id).status.startswith("Completed"),
        120,
        "the backfill to complete in the new generation",
    )
    status = dev_host.backfill_status(backfill_id)
    assert status.status == "CompletedSuccess", status
    assert status.completed_partitions == 4, status
    assert status.failed_partitions == 0, status
    wait_for(
        lambda: "Generation 0 finished." in dev_host.logs(),
        30,
        "the retired generation to exit on its own",
    )

    code = dev_host.stop()
    assert code == 0, dev_host.logs()
