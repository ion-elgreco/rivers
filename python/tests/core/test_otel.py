from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import threading
from pathlib import Path

import pytest

# One sensor evaluation is the only code path that opens a span, so the
# exporter has something to send.
EMIT_SPAN_SCRIPT = """
import time

import rivers as rs
from rivers._core import AutomationDaemon
from rivers.testing import memory_storage


@rs.Asset(name="a")
def a() -> int:
    return 1


@rs.Sensor(
    name="s",
    asset_selection=["a"],
    minimum_interval="0s",
    default_status=rs.SensorStatus.Running,
)
def s(context: rs.SensorEvaluationContext):
    return rs.SkipReason("noop")


storage = memory_storage()
repo = rs.CodeRepository(assets=[a], sensors=[s])
repo.resolve(storage=storage)
daemon = AutomationDaemon(repo=repo, storage=storage)
daemon.start()
deadline = time.monotonic() + 15
while time.monotonic() < deadline and not storage.get_ticks("s", limit=1):
    time.sleep(0.1)
daemon.stop()
time.sleep(2)
"""


def _otel_env(**overrides: str) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "OTEL_BSP_SCHEDULE_DELAY": "100",
            "OTEL_EXPORTER_OTLP_TIMEOUT": "1000",
        }
    )
    env.update(overrides)
    return env


def _run_and_capture_first_bytes(
    env: dict[str, str],
) -> tuple[subprocess.CompletedProcess[str], bytes]:
    """Return the first bytes the exporter sends to a local listener."""
    received: dict[str, bytes] = {}
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        server.settimeout(40)
        port = server.getsockname()[1]

        def accept() -> None:
            conn, _ = server.accept()
            with conn:
                conn.settimeout(10)
                received["head"] = conn.recv(16)

        thread = threading.Thread(target=accept, daemon=True)
        thread.start()
        env["OTEL_EXPORTER_OTLP_ENDPOINT"] = f"https://127.0.0.1:{port}"
        result = subprocess.run(
            [sys.executable, "-c", EMIT_SPAN_SCRIPT],
            env=env,
            capture_output=True,
            text=True,
            timeout=90,
        )
        thread.join(timeout=40)
    return result, received.get("head", b"")


def test_import_with_otlp_endpoint() -> None:
    env = os.environ.copy()
    env["OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://127.0.0.1:4317"

    subprocess.run(
        [sys.executable, "-c", "import rivers"],
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )


def test_https_endpoint_negotiates_tls() -> None:
    result, head = _run_and_capture_first_bytes(
        _otel_env(OTEL_EXPORTER_OTLP_HEADERS="authorization=Bearer test-token")
    )

    assert result.returncode == 0, result.stderr
    assert "OpenTelemetry export disabled" not in result.stderr
    assert head, "the exporter never connected"
    # TLS record header; plaintext HTTP/2 would start with "PRI * HTTP/2.0".
    assert head[:2] == b"\x16\x03", head


def test_mutual_tls_material_is_accepted(tmp_path: Path) -> None:
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("openssl CLI not available")
    cert, key = tmp_path / "client.pem", tmp_path / "client.key"
    subprocess.run(
        [
            openssl,
            "req",
            "-x509",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-subj",
            "/CN=rivers-test",
            "-days",
            "1",
        ],
        check=True,
        capture_output=True,
    )

    result, head = _run_and_capture_first_bytes(
        _otel_env(
            OTEL_EXPORTER_OTLP_CERTIFICATE=str(cert),
            OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE=str(cert),
            OTEL_EXPORTER_OTLP_CLIENT_KEY=str(key),
        )
    )

    assert result.returncode == 0, result.stderr
    assert "OpenTelemetry export disabled" not in result.stderr
    assert head[:2] == b"\x16\x03", head


def test_unreadable_certificate_disables_export_and_names_the_variable(
    tmp_path: Path,
) -> None:
    env = os.environ.copy()
    env["OTEL_EXPORTER_OTLP_ENDPOINT"] = "https://127.0.0.1:4317"
    env["OTEL_EXPORTER_OTLP_CERTIFICATE"] = str(tmp_path / "missing.pem")

    result = subprocess.run(
        [sys.executable, "-c", "import rivers"],
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert "OpenTelemetry export disabled" in result.stderr
    assert (
        f"OTEL_EXPORTER_OTLP_CERTIFICATE ({tmp_path / 'missing.pem'})" in result.stderr
    )
