"""The gRPC health service the Kubernetes readiness probe of a code-location pod asks."""

import pytest

pytest.importorskip("grpc")

import grpc

import rivers as rs

# The operator's readiness probe asks for this service.
CODE_LOCATION_SERVICE = "rivers.v1.CodeLocationService"
SERVING = 1


def _health_check(channel: grpc.Channel, service: str) -> int:
    """``grpc.health.v1.Health/Check`` for ``service``, encoded by hand.

    The request is field 1 (string); the reply is field 1 (enum).
    """
    name = service.encode()
    check = channel.unary_unary("/grpc.health.v1.Health/Check")
    reply = check(b"\x0a" + bytes([len(name)]) + name, timeout=5)
    if not reply:
        return 0
    assert reply[0] == 0x08, reply
    return reply[1]


def test_code_location_service_is_serving_once_its_server_starts():
    @rs.Asset()
    def one():
        return 1

    repo = rs.CodeRepository(assets=[one])
    port = repo._start_grpc_server("127.0.0.1", 0)
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    try:
        assert _health_check(channel, CODE_LOCATION_SERVICE) == SERVING
        assert _health_check(channel, "") == SERVING
    finally:
        channel.close()
        repo._stop_grpc_server()
