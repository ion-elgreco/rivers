from __future__ import annotations

import atexit
import importlib
import importlib.util
import os
import sys

import typer

from rivers._core.storage import Storage
from rivers.cli._app import app
from rivers.cli._common import _cleanup_storage, _open_or_prompt_migrate
from rivers.cli.config import RiversConfig


@app.command()
def dev(
    module: str | None = typer.Argument(
        None,
        help="Python module path containing CodeRepository",
    ),
    repo_var: str | None = typer.Option(
        None, help="Variable name of CodeRepository in module"
    ),
    host: str | None = typer.Option(None, help="Host to bind to"),
    port: int | None = typer.Option(None, help="Port to bind to"),
    grpc_port: int | None = typer.Option(None, help="Port for gRPC backend server"),
    storage_path: str | None = typer.Option(None, help="Path for embedded storage"),
    surreal_endpoint: str | None = typer.Option(
        None, help="Remote SurrealDB endpoint (overrides --storage-path)"
    ),
    no_daemon: bool | None = typer.Option(
        None, help="Disable schedule/sensor automation daemon"
    ),
    synthetic: str | None = typer.Option(
        None, help="Override graph with synthetic DAG (e.g. 100, 1k, 10k, 50k)"
    ),
) -> None:
    """Start the rivers development server.

    Serves the embedded storage and the web UI from this process, and runs the
    code location (gRPC backend and automation daemon) in a child process. A
    reload restarts that child in a fresh interpreter, so every edit is picked
    up: press the reload button in the UI.
    """
    cfg = RiversConfig.from_cli(
        module=module,
        repo_var=repo_var,
        host=host,
        port=port,
        grpc_port=grpc_port,
        storage_path=storage_path,
        surreal_endpoint=surreal_endpoint,
        no_daemon=no_daemon,
        synthetic=synthetic,
    )

    os.environ["RIVERS_DEPLOYMENT"] = "dev"
    if cfg.module.path:
        os.environ["RIVERS_MODULE"] = cfg.module.path

    if cfg.module.path is None:
        typer.echo(
            "Error: no module configured. Set 'module' in [rivers] config "
            "or pass a module argument",
            err=True,
        )
        raise typer.Exit(1)

    # The child imports the module; this only fails fast on a bad name.
    # Absolute cwd, not "." — the "." finder caches listings across chdirs.
    sys.path.insert(0, os.getcwd())
    if not _module_exists(cfg.module.path):
        typer.echo(f"Error: module '{cfg.module.path}' not found", err=True)
        raise typer.Exit(1)

    _serve_dev(cfg, cfg.module.path)


def _module_exists(name: str) -> bool:
    try:
        return importlib.util.find_spec(name) is not None
    except (ImportError, ValueError):
        return False


def _serve_dev(cfg: RiversConfig, module: str) -> None:
    """Run the dev host: storage server, web UI, and the supervised code location."""
    from rivers._core import DevHost

    host = DevHost(cfg.storage.path, cfg.storage.endpoint)
    os.environ["RIVERS_SURREAL_ENDPOINT"] = host.endpoint
    if cfg.storage.endpoint is None:
        atexit.register(_cleanup_storage, cfg.storage.path)

    try:
        storage = _open_or_prompt_migrate(
            lambda: Storage.connect(host.endpoint),
            lambda: Storage.migrate_remote(host.endpoint),
        )
        # The UI comes up while the code location imports and resolves.
        host.start_ui(
            storage,
            cfg.server.host,
            cfg.server.port,
            f"http://{cfg.server.host}:{cfg.server.grpc_port}",
            cfg.synthetic.size,
        )
        if not host.start_code_location(
            sys.executable,
            module,
            cfg.module.repo_var,
            cfg.server.host,
            cfg.server.grpc_port,
            cfg.daemon.no_daemon,
        ):
            raise typer.Exit(1)
        typer.echo(f"Code location {module} is up; reload it from the UI.")
        host.run()
    finally:
        host.stop_code_location()
        host.stop_ui()
        # This process's client closes before the server it talks to.
        storage = None
        host.stop()


@app.command()
def serve(
    module: str = typer.Argument(help="Python module path containing CodeRepository"),
    repo_var: str = typer.Option(
        "repo", help="Variable name of CodeRepository in module"
    ),
    host: str = typer.Option("0.0.0.0", help="Host to bind to"),
    grpc_port: int = typer.Option(3001, help="Port for gRPC backend server"),
    surreal_endpoint: str = typer.Option(
        ..., envvar="RIVERS_SURREAL_ENDPOINT", help="Remote SurrealDB endpoint"
    ),
    no_daemon: bool = typer.Option(
        False, help="Disable schedule/sensor automation daemon"
    ),
) -> None:
    """Start rivers code location server for Kubernetes deployment.

    Connects to a remote SurrealDB instance, starts the gRPC backend, and runs
    the automation daemon. Designed to run inside a K8s code-location pod.
    """
    os.environ["RIVERS_MODULE"] = module
    os.environ["RIVERS_DEPLOYMENT"] = "cloud"
    os.environ["RIVERS_SURREAL_ENDPOINT"] = surreal_endpoint

    storage = Storage.connect(surreal_endpoint)

    sys.path.insert(0, ".")
    repo_obj = _import_repo(module, repo_var)
    repo_obj.resolve(storage=storage)

    _serve_code_location(repo_obj, storage, host, grpc_port, no_daemon)


def _import_repo(module: str, repo_var: str):
    """Import ``module`` and return its ``CodeRepository``, or exit with the reason."""
    try:
        mod = importlib.import_module(module)
    except ModuleNotFoundError:
        typer.echo(f"Error: module '{module}' not found", err=True)
        raise typer.Exit(1)

    repo_obj = getattr(mod, repo_var, None)
    if repo_obj is None:
        typer.echo(f"Error: '{repo_var}' not found in module '{module}'", err=True)
        raise typer.Exit(1)

    from rivers import CodeRepository

    if not isinstance(repo_obj, CodeRepository):
        typer.echo(f"Error: '{repo_var}' is not a CodeRepository", err=True)
        raise typer.Exit(1)
    return repo_obj


def _serve_code_location(
    repo_obj, storage, host: str, grpc_port: int, no_daemon: bool
) -> None:
    """Start the gRPC backend and the automation daemon, then block."""
    actual_port = repo_obj._start_grpc_server(host, grpc_port)
    if actual_port != grpc_port:
        typer.echo(
            f"Warning: gRPC port {grpc_port} is in use; serving on {actual_port}",
            err=True,
        )

    if not no_daemon:
        from rivers._core import AutomationDaemon

        daemon = AutomationDaemon(repo=repo_obj, storage=storage)
        daemon.start()

    from rivers._core import wait_for_exit

    wait_for_exit()
