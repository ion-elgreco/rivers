from __future__ import annotations

import atexit
import importlib
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
    """Start rivers development UI.

    Resolves the repository (registering assets and graph topology in storage),
    then starts the gRPC backend and web UI servers in-process.
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
    if cfg.storage.endpoint:
        os.environ["RIVERS_SURREAL_ENDPOINT"] = cfg.storage.endpoint

    if cfg.module.path is None:
        typer.echo(
            "Error: no module configured. Set 'module' in [rivers] config "
            "or pass a module argument",
            err=True,
        )
        raise typer.Exit(1)

    # Import user module and resolve repository before opening storage —
    # otherwise a bad module name strands a RocksDB-locked dir on disk.
    # Absolute cwd, not "." — the "." finder caches listings across chdirs.
    sys.path.insert(0, os.getcwd())
    try:
        mod = importlib.import_module(cfg.module.path)
    except ModuleNotFoundError:
        typer.echo(f"Error: module '{cfg.module.path}' not found", err=True)
        raise typer.Exit(1)

    repo_obj = getattr(mod, cfg.module.repo_var, None)
    if repo_obj is None:
        typer.echo(
            f"Error: '{cfg.module.repo_var}' not found in module '{cfg.module.path}'",
            err=True,
        )
        raise typer.Exit(1)

    from rivers import CodeRepository

    if not isinstance(repo_obj, CodeRepository):
        typer.echo(f"Error: '{cfg.module.repo_var}' is not a CodeRepository", err=True)
        raise typer.Exit(1)

    storage_endpoint = cfg.storage.endpoint

    if storage_endpoint is not None:
        storage = _open_or_prompt_migrate(
            lambda: Storage.connect(storage_endpoint),
            lambda: Storage.migrate_remote(storage_endpoint),
        )
    else:
        storage = _open_or_prompt_migrate(
            lambda: Storage.embedded(cfg.storage.path),
            lambda: Storage.migrate_embedded(cfg.storage.path),
        )
        atexit.register(_cleanup_storage, cfg.storage.path)

    repo_obj.resolve(storage=storage)

    _serve_dev(cfg, repo_obj, storage)


def _serve_dev(cfg: RiversConfig, repo_obj, storage) -> None:
    """Start the gRPC backend, web UI, and automation daemon, then block."""
    # Start gRPC backend server (returns actual port, may differ if requested was in use)
    actual_grpc_port = repo_obj._start_grpc_server(
        cfg.server.host, cfg.server.grpc_port
    )

    # Start UI server in-process (shares same storage, no lock conflict)
    grpc_url = f"http://{cfg.server.host}:{actual_grpc_port}"
    repo_obj._start_ui_server(
        cfg.server.host,
        cfg.server.port,
        grpc_url,
        synthetic=cfg.synthetic.size,
    )

    # Start automation daemon (schedules + sensors)
    if not cfg.daemon.no_daemon:
        from rivers._core import AutomationDaemon

        daemon = AutomationDaemon(repo=repo_obj, storage=storage)
        daemon.start()

    from rivers._core import wait_for_exit

    wait_for_exit()


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

    Connects to a remote SurrealDB instance, starts the gRPC backend and web UI,
    and runs the automation daemon. Designed to run inside a K8s code-location pod.
    """
    os.environ["RIVERS_MODULE"] = module
    os.environ["RIVERS_DEPLOYMENT"] = "cloud"
    os.environ["RIVERS_SURREAL_ENDPOINT"] = surreal_endpoint

    storage = Storage.connect(surreal_endpoint)

    sys.path.insert(0, ".")
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

    repo_obj.resolve(storage=storage)

    repo_obj._start_grpc_server(host, grpc_port)

    if not no_daemon:
        from rivers._core import AutomationDaemon

        daemon = AutomationDaemon(repo=repo_obj, storage=storage)
        daemon.start()

    from rivers._core import wait_for_exit

    wait_for_exit()
