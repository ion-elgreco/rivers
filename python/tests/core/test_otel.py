from __future__ import annotations

import datetime as dt
import ipaddress
import os
import subprocess
import sys
from concurrent import futures
from dataclasses import dataclass, field
from pathlib import Path

import grpc
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

TRACE_SERVICE = "opentelemetry.proto.collector.trace.v1.TraceService"
TOKEN = "Bearer test-token"

# One sensor evaluation is the only code path that opens a span, so the
# exporter has something to send. The script exits right after, with every
# span still queued: `_emit_span` sets batch limits the run never reaches.
EMIT_SPAN_SCRIPT = """
import time

import rivers as rs
from rivers._core import AutomationDaemon
from rivers.testing import memory_storage


@rs.Asset(name="a")
def a() -> int:
    return 1


@rs.Sensor(
    name="otel_probe",
    asset_selection=["a"],
    minimum_interval="0s",
    default_status=rs.SensorStatus.Running,
)
def otel_probe(context: rs.SensorEvaluationContext):
    return rs.SkipReason("noop")


storage = memory_storage()
repo = rs.CodeRepository(assets=[a], sensors=[otel_probe])
repo.resolve(storage=storage)
daemon = AutomationDaemon(repo=repo, storage=storage)
daemon.start()
deadline = time.monotonic() + 15
while time.monotonic() < deadline and not storage.get_ticks("otel_probe", limit=1):
    time.sleep(0.1)
daemon.stop()
"""


@dataclass
class Pem:
    cert: bytes
    key: bytes


def _pem(
    common_name: str,
    *,
    issuer: Pem | None = None,
    ca: bool = False,
    usage: x509.ObjectIdentifier | None = None,
) -> Pem:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    issuer_cert = x509.load_pem_x509_certificate(issuer.cert) if issuer else None
    issuer_key = serialization.load_pem_private_key(issuer.key, None) if issuer else key
    now = dt.datetime.now(dt.timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(issuer_cert.subject if issuer_cert else name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=1))
        .not_valid_after(now + dt.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
    )
    if usage is not None:
        builder = builder.add_extension(
            x509.ExtendedKeyUsage([usage]), critical=False
        ).add_extension(
            x509.SubjectAlternativeName(
                [x509.IPAddress(ipaddress.IPv4Address("127.0.0.1"))]
            ),
            critical=False,
        )
    cert = builder.sign(issuer_key, hashes.SHA256())
    return Pem(
        cert=cert.public_bytes(serialization.Encoding.PEM),
        key=key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ),
    )


@dataclass
class Export:
    metadata: dict[str, str]
    body: bytes
    peer_common_name: str | None


@dataclass
class Receiver:
    """A TLS gRPC server that accepts OTLP trace exports and records them."""

    server_pem: Pem
    ca_pem: Pem
    require_client_auth: bool
    exports: list[Export] = field(default_factory=list)
    port: int = 0
    _server: grpc.Server = field(init=False)

    def __enter__(self) -> Receiver:
        def export(request: bytes, context: grpc.ServicerContext) -> bytes:
            auth = context.auth_context()
            common_name = auth.get("x509_common_name", [b""])[0].decode() or None
            self.exports.append(
                Export(dict(context.invocation_metadata()), request, common_name)
            )
            return b""  # an empty ExportTraceServiceResponse means success

        handler = grpc.method_handlers_generic_handler(
            TRACE_SERVICE,
            {"Export": grpc.unary_unary_rpc_method_handler(export)},
        )
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        self._server.add_generic_rpc_handlers((handler,))
        credentials = grpc.ssl_server_credentials(
            [(self.server_pem.key, self.server_pem.cert)],
            root_certificates=self.ca_pem.cert,
            require_client_auth=self.require_client_auth,
        )
        self.port = self._server.add_secure_port("127.0.0.1:0", credentials)
        self._server.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._server.stop(grace=None)


def _emit_span(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", EMIT_SPAN_SCRIPT],
        env={
            **os.environ,
            "OTEL_BSP_SCHEDULE_DELAY": "60000",
            "OTEL_BSP_MAX_QUEUE_SIZE": "1000000",
            "OTEL_BSP_MAX_EXPORT_BATCH_SIZE": "1000000",
            "OTEL_EXPORTER_OTLP_TIMEOUT": "2000",
            **env,
        },
        capture_output=True,
        text=True,
        timeout=90,
    )


def _write(tmp_path: Path, name: str, data: bytes) -> str:
    path = tmp_path / name
    path.write_bytes(data)
    return str(path)


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


def test_export_over_mutual_tls_with_bearer_header(tmp_path: Path) -> None:
    ca = _pem("rivers-test-ca", ca=True)
    server = _pem("otlp-receiver", issuer=ca, usage=ExtendedKeyUsageOID.SERVER_AUTH)
    client = _pem("rivers-client", issuer=ca, usage=ExtendedKeyUsageOID.CLIENT_AUTH)

    with Receiver(server, ca, require_client_auth=True) as receiver:
        result = _emit_span(
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": f"https://127.0.0.1:{receiver.port}",
                "OTEL_EXPORTER_OTLP_HEADERS": f"authorization={TOKEN}",
                "OTEL_EXPORTER_OTLP_CERTIFICATE": _write(tmp_path, "ca.pem", ca.cert),
                "OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE": _write(
                    tmp_path, "client.pem", client.cert
                ),
                "OTEL_EXPORTER_OTLP_CLIENT_KEY": _write(
                    tmp_path, "client.key", client.key
                ),
            }
        )

    assert result.returncode == 0, result.stderr
    assert "OpenTelemetry export disabled" not in result.stderr
    assert receiver.exports, f"no export received; stderr:\n{result.stderr}"
    export = receiver.exports[0]
    assert export.metadata["authorization"] == TOKEN
    assert export.peer_common_name == "rivers-client"
    # Protobuf strings are raw UTF-8: the service name, span name and sensor name.
    assert b"rivers" in export.body
    assert b"eval" in export.body
    assert b"otel_probe" in export.body


def test_spans_still_queued_at_exit_are_exported(tmp_path: Path) -> None:
    ca = _pem("rivers-test-ca", ca=True)
    server = _pem("otlp-receiver", issuer=ca, usage=ExtendedKeyUsageOID.SERVER_AUTH)

    with Receiver(server, ca, require_client_auth=False) as receiver:
        result = _emit_span(
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": f"https://127.0.0.1:{receiver.port}",
                "OTEL_EXPORTER_OTLP_CERTIFICATE": _write(tmp_path, "ca.pem", ca.cert),
            }
        )

    assert result.returncode == 0, result.stderr
    assert receiver.exports, f"no export received; stderr:\n{result.stderr}"
    assert b"otel_probe" in receiver.exports[0].body


def test_empty_traces_endpoint_falls_back_to_generic_endpoint(tmp_path: Path) -> None:
    ca = _pem("rivers-test-ca", ca=True)
    server = _pem("otlp-receiver", issuer=ca, usage=ExtendedKeyUsageOID.SERVER_AUTH)

    with Receiver(server, ca, require_client_auth=False) as receiver:
        result = _emit_span(
            {
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "",
                "OTEL_EXPORTER_OTLP_ENDPOINT": f"https://127.0.0.1:{receiver.port}",
                "OTEL_EXPORTER_OTLP_CERTIFICATE": _write(tmp_path, "ca.pem", ca.cert),
            }
        )

    assert result.returncode == 0, result.stderr
    assert "OpenTelemetry export disabled" not in result.stderr
    assert receiver.exports, f"no export received; stderr:\n{result.stderr}"
    assert b"otel_probe" in receiver.exports[0].body


def test_receiver_requiring_a_client_certificate_gets_nothing_without_one(
    tmp_path: Path,
) -> None:
    ca = _pem("rivers-test-ca", ca=True)
    server = _pem("otlp-receiver", issuer=ca, usage=ExtendedKeyUsageOID.SERVER_AUTH)

    with Receiver(server, ca, require_client_auth=True) as receiver:
        result = _emit_span(
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": f"https://127.0.0.1:{receiver.port}",
                "OTEL_EXPORTER_OTLP_HEADERS": f"authorization={TOKEN}",
                "OTEL_EXPORTER_OTLP_CERTIFICATE": _write(tmp_path, "ca.pem", ca.cert),
            }
        )

    assert result.returncode == 0, result.stderr
    assert receiver.exports == []


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


def test_certificate_with_plain_http_endpoint_disables_export(tmp_path: Path) -> None:
    ca = _pem("rivers-test-ca", ca=True)
    env = os.environ.copy()
    env["OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://127.0.0.1:4317"
    env["OTEL_EXPORTER_OTLP_CERTIFICATE"] = _write(tmp_path, "ca.pem", ca.cert)

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
        "OTEL_EXPORTER_OTLP_CERTIFICATE is set but OTEL_EXPORTER_OTLP_ENDPOINT "
        "(http://127.0.0.1:4317) is not https://"
    ) in result.stderr
