"""IO handlers and run settings that both demo code locations use.

`pipeline` writes through these handlers and `downstream` reads the same
objects as remote assets, so both must point at one store.
"""

import hashlib
import os
import pickle

import obstore.store
from rivers import DeltaIOHandler, Executor, OutputContext, PickleIOHandler


class VersionedPickleIOHandler(PickleIOHandler):
    """PickleIOHandler that registers a data version (content hash) on each write."""

    def handle_output(self, context: OutputContext, obj: object) -> None:
        data_version = hashlib.sha256(pickle.dumps(obj)).hexdigest()
        context.register_data_version(data_version)
        super().handle_output(context, obj)


DEPLOYMENT = os.environ.get("RIVERS_DEPLOYMENT", "dev")

if DEPLOYMENT == "cloud":
    _bucket = os.environ.get("RIVERS_S3_BUCKET", "rivers-io")
    _endpoint = os.environ.get("RIVERS_S3_ENDPOINT", "http://rustfs.rivers.svc:9000")
    _store = obstore.store.S3Store(
        bucket=_bucket,
        config={
            "endpoint": _endpoint,
            "access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", ""),
            "secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
            "region": os.environ.get("AWS_REGION", "us-east-1"),
        },
        client_options={"allow_http": "true"},
    )
    delta_io = DeltaIOHandler(
        table_uri=f"s3://{_bucket}/delta",
        mode="append",
        storage_options={
            "AWS_ENDPOINT_URL": _endpoint,
            "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
            "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
            "AWS_ALLOW_HTTP": "true",
        },
    )
else:
    _io_root = os.path.join(os.getcwd(), ".rivers", "demo-io")
    os.makedirs(_io_root, exist_ok=True)
    _store = obstore.store.LocalStore(_io_root)
    delta_io = DeltaIOHandler(table_uri=os.path.join(_io_root, "delta"), mode="append")


def pickle_io(prefix: str) -> VersionedPickleIOHandler:
    """A pickle handler that stores its objects under `prefix` in the demo store."""
    return VersionedPickleIOHandler(store=_store, prefix=prefix)


raw_io = pickle_io("raw")
processed_io = pickle_io("processed")
output_io = pickle_io("output")


def repo_kwargs() -> dict:
    """Executor and run backend: Kubernetes pods in the cluster, in-process locally."""
    if DEPLOYMENT == "cloud":
        from rivers import RunBackendConfig

        return {
            "default_executor": Executor.kubernetes(),
            "run_backend": RunBackendConfig.kubernetes(),
        }
    return {"default_executor": Executor.in_process()}
