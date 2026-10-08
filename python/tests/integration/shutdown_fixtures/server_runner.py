"""Start a gRPC server and block on the shutdown coordinator.

Env vars:
    PIPELINE_MODULE — dotted module path to import (must export `repo`)
    STORAGE_PATH   — path for embedded SurrealDB storage
    GRPC_PORT      — port to bind the gRPC server on
    DAEMON         — "1" to also run the automation daemon
    RETIRE         — "1" to retire on the first terminate signal
"""

import importlib
import os

from rivers._core import install_signal_handler, wait_for_exit
from rivers._core.storage import Storage

install_signal_handler()

module = os.environ["PIPELINE_MODULE"]
storage_path = os.environ["STORAGE_PATH"]
grpc_port = int(os.environ["GRPC_PORT"])

repo = importlib.import_module(module).repo

storage = Storage.embedded(storage_path)
repo.resolve(storage=storage)
port = repo._start_grpc_server("127.0.0.1", grpc_port)
daemon = None
if os.environ.get("DAEMON") == "1":
    from rivers._core import AutomationDaemon

    # Kept alive until exit: dropping the daemon object stops its loops.
    daemon = AutomationDaemon(repo=repo, storage=storage)
    daemon.start()
print(f"READY:{port}", flush=True)

wait_for_exit(retire=os.environ.get("RETIRE") == "1")
