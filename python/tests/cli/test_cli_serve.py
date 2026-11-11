"""``rivers serve`` writes its gRPC endpoint to the code location directory."""

import json
import os
import subprocess
import sys
import time

import pytest
from _polling import free_port, wait_for

from rivers._core import DevHost
from rivers._core.storage import Storage

pytestmark = pytest.mark.timeout(180)

MODULE = """
import rivers as rs


@rs.Asset
def first() -> int:
    return 1


repo = rs.CodeRepository(assets=[first])
"""

SERVICE_ENDPOINT = "warehouse-grpc.team-data.svc.cluster.local:3001"


@pytest.mark.parametrize("from_operator", [True, False], ids=["operator", "bind"])
def test_serve_writes_its_endpoint_at_start_and_every_10_s(
    resolved_tmp_path, from_operator
):
    (resolved_tmp_path / "serve_mod.py").write_text(MODULE)
    port = free_port()
    env = {
        **os.environ,
        "RIVERS_CODE_LOCATION_ID": "uuid-1",
        "RIVERS_CODE_LOCATION_NAME": "warehouse",
        "RIVERS_CODE_LOCATION_NAMESPACE": "team-data",
    }
    if from_operator:
        env["RIVERS_CODE_LOCATION_GRPC_ENDPOINT"] = SERVICE_ENDPOINT
    expected = {
        "identity": "uuid-1",
        "module": "serve_mod",
        "grpc_endpoint": SERVICE_ENDPOINT
        if from_operator
        else f"http://127.0.0.1:{port}",
    }

    host = DevHost(str(resolved_tmp_path / "storage"))
    # Connected first, so the server finds the store migrated.
    storage = Storage.connect(host.endpoint)
    err_path = resolved_tmp_path / "serve.err"
    with open(err_path, "wb") as err:
        server = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "from rivers.cli import main; main()",
                "serve",
                "serve_mod",
                "--host",
                "127.0.0.1",
                "--grpc-port",
                str(port),
                "--surreal-endpoint",
                host.endpoint,
                "--no-daemon",
            ],
            cwd=resolved_tmp_path,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=err,
        )

    def entry() -> dict:
        raw = storage.kv_get("code_location:team-data/warehouse")
        return {} if raw is None else json.loads(raw)

    def heartbeat() -> int | None:
        assert server.poll() is None, f"rivers serve exited with {server.returncode}"
        return entry().get("last_seen")

    try:
        wait_for(heartbeat, 60, "the first heartbeat")
        first = entry()
        first_seen = first.pop("last_seen")
        first_schema = first.pop("schema_version")
        assert first_schema > 0
        assert first.pop("rivers_version")
        assert first == expected
        assert abs(time.time_ns() - first_seen) < 30 * 10**9

        wait_for(lambda: (heartbeat() or 0) > first_seen, 30, "the next heartbeat")
        later = entry()
        assert later.pop("last_seen") > first_seen
        assert later.pop("schema_version") == first_schema
        assert later.pop("rivers_version")
        assert later == expected
    except AssertionError as e:
        log = err_path.read_text(errors="replace")[-4000:]
        raise AssertionError(f"{e}\nrivers serve stderr (tail):\n{log}") from None
    finally:
        server.terminate()
        try:
            server.wait(timeout=30)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
        # The client closes before the server it talks to.
        storage = None
        host.stop()
