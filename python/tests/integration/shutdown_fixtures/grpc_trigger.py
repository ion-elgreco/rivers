"""Call Materialize on a gRPC server with a long timeout.

Keeps the gRPC stream alive so tonic's graceful drain waits for the
in-flight RPC to complete.

Env vars:
    GRPC_PORT  — port of the gRPC server
    PROTO_PATH — path to the directory containing rivers.proto
"""

import importlib
import os
import sys
import tempfile

import grpc
from grpc_tools import protoc

out = tempfile.mkdtemp()
proto_path = os.environ["PROTO_PATH"]
protoc.main(
    [
        "grpc_tools.protoc",
        f"-I{proto_path}",
        f"--python_out={out}",
        f"--grpc_python_out={out}",
        os.path.join(proto_path, "rivers.proto"),
    ]
)


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


pb2 = _load("rivers_pb2", os.path.join(out, "rivers_pb2.py"))
sys.modules["rivers_pb2"] = pb2
pb2_grpc = _load("rivers_pb2_grpc", os.path.join(out, "rivers_pb2_grpc.py"))

port = os.environ["GRPC_PORT"]
channel = grpc.insecure_channel(f"127.0.0.1:{port}")
grpc.channel_ready_future(channel).result(timeout=5)
stub = pb2_grpc.CodeLocationServiceStub(channel)

print("CALLING", flush=True)
resp = stub.Materialize(
    pb2.MaterializeRequest(selection=["slow_asset"]),
    timeout=30,
)
print(f"DONE:{resp.success}", flush=True)
channel.close()
