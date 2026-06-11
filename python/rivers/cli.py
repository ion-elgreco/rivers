"""rivers CLI — development server and materialization commands."""

from __future__ import annotations

import atexit
import importlib
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import typer
from pydantic import BaseModel, Field
from pydantic_settings import (
    BaseSettings,
    EnvSettingsSource,
    PydanticBaseSettingsSource,
    PyprojectTomlConfigSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

from rivers._core.storage import Storage

app = typer.Typer(name="rivers", help="rivers orchestration CLI")
pools_app = typer.Typer(name="pools", help="Inspect and manage concurrency pools")
queue_app = typer.Typer(name="queue", help="Inspect and manage the run queue")
app.add_typer(pools_app)
app.add_typer(queue_app)


class ModuleConfig(BaseModel):
    path: str | None = None
    repo_var: str = "repo"


class StorageConfig(BaseModel):
    path: str = ".rivers/storage/"
    endpoint: str | None = None


class ServerConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 3000
    grpc_port: int = 3001


class DaemonConfig(BaseModel):
    no_daemon: bool = False


class SyntheticConfig(BaseModel):
    size: str | None = None


class RiversEnvSource(EnvSettingsSource):
    def _load_env_vars(self):
        env = super()._load_env_vars()
        collisions = {
            f"{self.env_prefix}{name}".lower()
            for name in self.settings_cls.model_fields
        }
        return {k: v for k, v in env.items() if k.lower() not in collisions}


class RiversConfig(BaseSettings):
    module: ModuleConfig = Field(default_factory=ModuleConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    server: ServerConfig = Field(default_factory=ServerConfig)
    daemon: DaemonConfig = Field(default_factory=DaemonConfig)
    synthetic: SyntheticConfig = Field(default_factory=SyntheticConfig)

    model_config = SettingsConfigDict(
        env_prefix="RIVERS_",
        env_nested_delimiter="_",
        env_nested_max_split=1,
        pyproject_toml_table_header=("tool", "rivers"),
        extra="ignore",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        sources: list[PydanticBaseSettingsSource] = [
            init_settings,
            RiversEnvSource(settings_cls),
        ]

        # Using _find_toml() to recursively lookup the TOML configuration files
        # within the current code location. This makes it possible to run operations
        # such as `rivers dev` from any project directory.
        rivers_toml = _find_toml("rivers.toml")
        pyproject_toml = _find_toml("pyproject.toml")

        print(f"rivers_toml={rivers_toml}")
        print(f"pyproject_toml={pyproject_toml}")

        if rivers_toml:
            sources.append(
                TomlConfigSettingsSource(
                    settings_cls,
                    toml_file=rivers_toml,
                )
            )
        if pyproject_toml:
            sources.append(
                PyprojectTomlConfigSettingsSource(
                    settings_cls,
                    toml_file=pyproject_toml,
                )
            )

        return tuple(sources)


def _find_toml(filename: str, start_path: Path = Path.cwd()) -> Path | None:
    """Recursively lookup TOML-file from current terminal path."""
    for directory in [start_path, *start_path.parents]:
        path = directory / filename
        if path.exists():
            return path
    return None


def _parse_partition_key(raw: str | None):
    """Parse a partition key from CLI arg. Accepts JSON (from step pods) or plain string."""
    if raw is None:
        return None
    from rivers import PartitionKey as PK

    if raw.startswith("{"):
        return PK.from_json(raw)
    return PK.single(raw)


def _cleanup_storage(path: str) -> None:
    """Remove embedded storage directory on exit."""
    p = Path(path)
    if p.exists():
        shutil.rmtree(p, ignore_errors=True)
        # Also remove the parent .rivers dir if it's now empty
        parent = p.parent
        if parent.name == ".rivers" and parent.exists() and not any(parent.iterdir()):
            parent.rmdir()


def _create_storage(memory: bool, storage_path: str) -> Storage:
    """Create storage backend based on CLI flags."""
    if memory:
        return Storage.memory()
    storage = Storage.embedded(storage_path)
    atexit.register(_cleanup_storage, storage_path)
    return storage


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
    no_daemon: bool = typer.Option(
        False, help="Disable schedule/sensor automation daemon"
    ),
    synthetic: str | None = typer.Option(
        None, help="Override graph with synthetic DAG (e.g. 100, 1k, 10k, 50k)"
    ),
) -> None:
    """Start rivers development UI.

    Resolves the repository (registering assets and graph topology in storage),
    then starts the gRPC backend and web UI servers in-process.
    """
    cfg = RiversConfig(
        **{
            k: v
            for k, v in {
                "module": module,
                "repo_var": repo_var,
                "host": host,
                "port": port,
                "grpc_port": grpc_port,
                "storage_path": storage_path,
                "surreal_endpoint": surreal_endpoint,
                "no_daemon": no_daemon,
                "synthetic": synthetic,
            }.items()
            if v is not None
        }
    )

    os.environ["RIVERS_DEPLOYMENT"] = "dev"
    if cfg.module.path:
        os.environ["RIVERS_MODULE"] = cfg.module.path
    if cfg.storage.endpoint:
        os.environ["RIVERS_SURREAL_ENDPOINT"] = cfg.storage.endpoint

    if cfg.module.path is None:
        typer.echo(
            "Error: no module configured. Set 'module' in [rivers] config or pass --module",
            err=True,
        )
        raise typer.Exit(1)

    # Import user module and resolve repository before opening storage —
    # otherwise a bad module name strands a RocksDB-locked dir on disk.
    try:
        mod = importlib.import_module(cfg.module.path)
    except ModuleNotFoundError:
        typer.echo(f"Error: module '{module}' not found", err=True)
        raise typer.Exit(1)

    repo_obj = getattr(mod, cfg.module.repo_var, None)
    if repo_obj is None:
        typer.echo(f"Error: '{repo_var}' not found in module '{module}'", err=True)
        raise typer.Exit(1)

    from rivers import CodeRepository

    if not isinstance(repo_obj, CodeRepository):
        typer.echo(f"Error: '{repo_var}' is not a CodeRepository", err=True)
        raise typer.Exit(1)

    if surreal_endpoint:
        storage = Storage.connect(surreal_endpoint)
    else:
        storage = Storage.embedded(cfg.storage.path)
        atexit.register(_cleanup_storage, cfg.storage.path)

    repo_obj.resolve(storage=storage)

    # Start gRPC backend server (returns actual port, may differ if requested was in use)
    actual_grpc_port = repo_obj._start_grpc_server(
        cfg.server.host, cfg.server.grpc_port
    )

    # Start UI server in-process (shares same storage, no lock conflict)
    grpc_url = f"http://{host}:{actual_grpc_port}"
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


@app.command()
def execute(
    module: str = typer.Argument(help="Python module path containing CodeRepository"),
    run_id: str = typer.Option(..., help="Pre-assigned run ID"),
    surreal_endpoint: str = typer.Option(
        ..., help="Remote SurrealDB endpoint (e.g. ws://host:8000)"
    ),
    repo_var: str = typer.Option(
        "repo", help="Variable name of CodeRepository in module"
    ),
    target: str | None = typer.Option(
        None, help="Comma-separated asset names to execute (default: all)"
    ),
    partition_key: str | None = typer.Option(None, help="Partition key to materialize"),
    resume: bool = typer.Option(
        False, help="Resume a crashed run, skipping completed steps"
    ),
) -> None:
    """Execute a run against a remote SurrealDB. Designed for K8s executor pods."""
    os.environ["RIVERS_DEPLOYMENT"] = "cloud"
    os.environ["RIVERS_RUN_ID"] = run_id
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

    pk = _parse_partition_key(partition_key)
    selection = [a.strip() for a in target.split(",")] if target else None

    try:
        result = repo_obj.materialize(
            selection=selection,
            partition_key=pk,
            run_id_override=run_id,
            raise_on_error=False,
            resume=resume,
        )
        completed = len(result.materialized_assets) - len(result.failed_assets)
        total = len(result.materialized_assets)

        if storage.is_cancelled(run_id):
            storage.set_run_outcome(run_id, "Cancelled", completed, total)
            typer.echo(f"Run {run_id} cancelled: {completed}/{total} steps completed")
            raise typer.Exit(2)
        elif result.success:
            storage.set_run_outcome(run_id, "Success", completed, total)
            typer.echo(
                f"Run {run_id} succeeded: {completed}/{total} assets materialized"
            )
        else:
            failed_names = [name for name, _ in result.failed_assets]
            msg = f"Failed assets: {', '.join(failed_names)}"
            storage.set_run_outcome(run_id, "Failure", completed, total, message=msg)
            typer.echo(f"Run {run_id} failed: {msg}", err=True)
            raise typer.Exit(1)
    except SystemExit:
        raise
    except BaseException as exc:
        storage.set_run_outcome(run_id, "Failure", 0, 0, message=str(exc))
        typer.echo(f"Run {run_id} failed: {exc}", err=True)
        raise typer.Exit(1)


@app.command(name="execute-step")
def execute_step(
    module: str = typer.Argument(help="Python module path containing CodeRepository"),
    step_key: str = typer.Option(..., help="Asset key of the step to execute"),
    run_id: str = typer.Option(..., help="Run ID this step belongs to"),
    repo_var: str = typer.Option(
        "repo", help="Variable name of CodeRepository in module"
    ),
    partition_key: str | None = typer.Option(None, help="Partition key"),
    mapping_key: str | None = typer.Option(
        None, help="Mapping key for mapped step instances"
    ),
) -> None:
    """Execute a single step within a run. Designed for K8s step pods."""
    os.environ["RIVERS_DEPLOYMENT"] = "cloud"
    os.environ["RIVERS_RUN_ID"] = run_id
    surreal_endpoint = os.environ.get("RIVERS_SURREAL_ENDPOINT")
    if not surreal_endpoint:
        typer.echo("Error: RIVERS_SURREAL_ENDPOINT env var is required", err=True)
        raise typer.Exit(1)
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

    pk = _parse_partition_key(partition_key)

    if mapping_key:
        os.environ["RIVERS_MAPPING_KEY"] = mapping_key

    try:
        _ = repo_obj.materialize(
            selection=[step_key],
            partition_key=pk,
            run_id_override=run_id,
            raise_on_error=True,
        )
        label = f"{step_key}[{mapping_key}]" if mapping_key else step_key
        typer.echo(f"Step {label} completed in run {run_id}")
    except SystemExit:
        raise
    except BaseException as exc:
        label = f"{step_key}[{mapping_key}]" if mapping_key else step_key
        typer.echo(f"Step {label} failed: {exc}", err=True)
        raise typer.Exit(1)


@app.command()
def materialize(
    module: str = typer.Argument(help="Python module path containing CodeRepository"),
    repo_var: str = typer.Option(
        "repo", help="Variable name of CodeRepository in module"
    ),
    partition_key: str | None = typer.Option(None, help="Partition key to materialize"),
    memory: bool = typer.Option(
        False, help="Use in-memory storage instead of embedded"
    ),
    storage_path: str = typer.Option(
        ".rivers/storage/", help="Path for embedded storage"
    ),
) -> None:
    """Materialize all assets in a repository."""
    from rivers import PartitionKey

    repo_obj = _load_repo(module, repo_var, memory, storage_path)
    pk = PartitionKey.single(partition_key) if partition_key else None
    result = repo_obj.materialize(partition_key=pk)
    typer.echo(f"Materialization complete. Assets: {result.materialized_assets}")


def _load_repo(module: str, repo_var: str, memory: bool, storage_path: str):
    """Load and resolve a CodeRepository from a module."""
    sys.path.insert(0, ".")
    mod = importlib.import_module(module)
    repo_obj = getattr(mod, repo_var, None)
    if repo_obj is None:
        typer.echo(f"Error: '{repo_var}' not found in module '{module}'", err=True)
        raise typer.Exit(1)

    from rivers import CodeRepository

    if not isinstance(repo_obj, CodeRepository):
        typer.echo(f"Error: '{repo_var}' is not a CodeRepository", err=True)
        raise typer.Exit(1)

    storage = _create_storage(memory, storage_path)
    repo_obj.resolve(storage=storage)
    return repo_obj


def _parse_strategy(strategy_str: str | None):
    """Parse --strategy flag into BackfillStrategy."""
    if strategy_str is None:
        return None
    from rivers import BackfillStrategy

    if strategy_str == "multi_run":
        return BackfillStrategy.multi_run()
    if strategy_str == "single_run":
        return BackfillStrategy.single_run()
    # Per-dimension: "foo=multi_run,bar=single_run"
    if "=" in strategy_str:
        multi_run = []
        single_run = []
        for part in strategy_str.split(","):
            dim, mode = part.strip().split("=", 1)
            if mode.strip() == "multi_run":
                multi_run.append(dim.strip())
            elif mode.strip() == "single_run":
                single_run.append(dim.strip())
        return BackfillStrategy.per_dimension(
            multi_run=multi_run, single_run=single_run
        )
    return None


@app.command()
def backfill(
    module: str = typer.Argument(help="Python module path containing CodeRepository"),
    repo_var: str = typer.Option("repo", help="Variable name of CodeRepository"),
    assets: str | None = typer.Option(
        None, "--assets", "-a", help="Comma-separated asset names"
    ),
    partitions: str | None = typer.Option(
        None, "--partitions", "-p", help="Comma-separated partition keys"
    ),
    from_key: str | None = typer.Option(None, "--from", help="Range start (inclusive)"),
    to_key: str | None = typer.Option(None, "--to", help="Range end (inclusive)"),
    range_flag: list[str] | None = typer.Option(
        None, "--range", help="Per-dimension range (dim=from..to or dim=k1,k2)"
    ),
    strategy: str | None = typer.Option(
        None, "--strategy", help="multi_run, single_run, or dim=mode,..."
    ),
    concurrency: int = typer.Option(
        4, "--concurrency", "-c", help="Max concurrent partition runs"
    ),
    on_failure: str = typer.Option(
        "continue", "--on-failure", help="continue or stop_on_failure"
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview without executing"),
    memory: bool = typer.Option(False, help="Use in-memory storage"),
    storage_path: str = typer.Option(
        ".rivers/storage/", help="Path for embedded storage"
    ),
) -> None:
    """Backfill partitions for selected assets."""
    from rivers import PartitionKey, PartitionKeyRange

    repo_obj = _load_repo(module, repo_var, memory, storage_path)

    selection = [a.strip() for a in assets.split(",")] if assets else None

    # Resolve partition keys or range
    pk_list: list[PartitionKey] | None = None
    pk_range = None
    if partitions:
        pk_list = [PartitionKey.single(k.strip()) for k in partitions.split(",")]  # type: ignore[list-item]
    elif from_key and to_key:
        pk_range = PartitionKeyRange.single(from_key=from_key, to_key=to_key)
    elif range_flag:
        dims = {}
        for flag in range_flag:
            dim, spec = flag.split("=", 1)
            dim = dim.strip()
            if ".." in spec:
                f, t = spec.split("..", 1)
                dims[dim] = (f.strip(), t.strip())
            else:
                dims[dim] = [k.strip() for k in spec.split(",")]
        pk_range = PartitionKeyRange.multi(dims)
    else:
        typer.echo("Error: provide --partitions, --from/--to, or --range", err=True)
        raise typer.Exit(1)

    resolved_strategy = _parse_strategy(strategy)

    result = repo_obj.backfill(
        selection=selection,
        partition_keys=pk_list,
        partition_range=pk_range,
        strategy=resolved_strategy,
        failure_policy=on_failure,
        max_concurrency=concurrency,
        block=True,
        dry_run=dry_run,
    )

    if dry_run:
        typer.echo(
            f"Dry run: {result.num_partitions} partitions, {result.num_runs} runs"
        )
    else:
        typer.echo(
            f"Backfill {result.backfill_id}: {result.status} — "
            f"{result.completed} completed, {result.failed} failed, {result.canceled} canceled"
        )


@app.command(name="backfill-status")
def backfill_status(
    backfill_id: str = typer.Argument(help="Backfill ID to check"),
    module: str = typer.Argument(help="Python module path"),
    repo_var: str = typer.Option("repo", help="Variable name of CodeRepository"),
    memory: bool = typer.Option(False, help="Use in-memory storage"),
    storage_path: str = typer.Option(
        ".rivers/storage/", help="Path for embedded storage"
    ),
) -> None:
    """Check status of a backfill."""
    repo_obj = _load_repo(module, repo_var, memory, storage_path)
    status = repo_obj.get_backfill(backfill_id)
    if status is None:
        typer.echo(f"Backfill '{backfill_id}' not found", err=True)
        raise typer.Exit(1)
    typer.echo(
        f"Backfill {status.backfill_id}: {status.status}\n"
        f"  Partitions: {status.completed_partitions}/{status.total_partitions} completed, "
        f"{status.failed_partitions} failed, {status.canceled_partitions} canceled\n"
        f"  Runs: {len(status.run_ids)}"
    )
    if status.error:
        typer.echo(f"  Error: {status.error}")


@app.command(name="backfill-cancel")
def backfill_cancel(
    backfill_id: str = typer.Argument(help="Backfill ID to cancel"),
    module: str = typer.Argument(help="Python module path"),
    repo_var: str = typer.Option("repo", help="Variable name of CodeRepository"),
    memory: bool = typer.Option(False, help="Use in-memory storage"),
    storage_path: str = typer.Option(
        ".rivers/storage/", help="Path for embedded storage"
    ),
) -> None:
    """Cancel a running backfill."""
    repo_obj = _load_repo(module, repo_var, memory, storage_path)
    success = repo_obj.cancel_backfill(backfill_id)
    if success:
        typer.echo(
            f"Backfill '{backfill_id}' canceled (in-process coordinator signaled)"
        )
    else:
        typer.echo(
            f"Backfill '{backfill_id}' cancel requested (not running in this process)"
        )


def _ns_to_iso(ns: int) -> str:
    """Convert nanosecond timestamp to human-readable ISO 8601 string."""
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


_STORAGE_PATH_OPT = typer.Option(".rivers/storage/", help="Path for embedded storage")
_QUEUE_SORT_KEY = lambda r: (-r.priority, r.start_time)  # noqa: E731

# ── Pool commands ──


@pools_app.command("list")
def pools_list(storage_path: str = _STORAGE_PATH_OPT) -> None:
    """List all configured concurrency pools."""
    storage = Storage.embedded(storage_path)
    infos = storage.get_all_pool_infos()
    if not infos:
        typer.echo("No pools configured.")
        return
    typer.echo(f"{'POOL':<24} {'LIMIT':>6} {'CLAIMED':>8} {'PENDING':>8} {'LEASE':>10}")
    typer.echo("-" * 60)
    for info in infos:
        typer.echo(
            f"{info.pool_key:<24} {info.slot_limit:>6} {info.claimed_count:>8} "
            f"{info.pending_count:>8} {info.lease_duration_secs:>10}"
        )


@pools_app.command("info")
def pools_info(
    pool: str = typer.Argument(help="Pool key to inspect"),
    storage_path: str = _STORAGE_PATH_OPT,
) -> None:
    """Show detailed info for a concurrency pool, including active slot holders."""
    storage = Storage.embedded(storage_path)
    try:
        info = storage.get_pool_info(pool)
    except Exception as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)

    typer.echo(f"Pool:           {info.pool_key}")
    typer.echo(f"Slot limit:     {info.slot_limit}")
    typer.echo(f"Lease duration: {info.lease_duration_secs}s")
    typer.echo(f"Claimed:        {info.claimed_count}/{info.slot_limit}")
    typer.echo(f"Pending:        {info.pending_count}")

    holders = storage.get_pool_slot_holders(pool)
    if holders:
        typer.echo(f"\nActive slot holders ({len(holders)}):")
        typer.echo(
            f"  {'RUN ID':<38} {'STEP KEY':<30} {'SLOTS':>5} {'LEASE EXPIRES':>24}"
        )
        typer.echo("  " + "-" * 99)
        for h in holders:
            typer.echo(
                f"  {h.run_id:<38} {h.step_key:<30} {h.slots_consumed:>5} "
                f"{_ns_to_iso(h.lease_expires_at):>24}"
            )
    else:
        typer.echo("\nNo active slot holders.")


@pools_app.command("set")
def pools_set(
    pool: str = typer.Argument(help="Pool key"),
    limit: int = typer.Argument(help="New slot limit"),
    lease_duration: str = typer.Option(
        "5m", help="Lease duration (e.g. '5m', '1h', '30s')"
    ),
    storage_path: str = _STORAGE_PATH_OPT,
) -> None:
    """Set (upsert) the slot limit for a concurrency pool."""
    storage = Storage.embedded(storage_path)
    storage.set_pool_limit(pool, limit, lease_duration)
    typer.echo(f"Pool '{pool}' set to limit={limit}, lease_duration={lease_duration}")


# ── Queue commands ──


@queue_app.command("list")
def queue_list(storage_path: str = _STORAGE_PATH_OPT) -> None:
    """List all queued runs with priority and block reason."""
    storage = Storage.embedded(storage_path)
    runs = storage.get_queued_runs()
    if not runs:
        typer.echo("No queued runs.")
        return
    runs.sort(key=_QUEUE_SORT_KEY)
    typer.echo(
        f"{'POS':>4} {'RUN ID':<38} {'JOB':<20} {'PRI':>4} {'QUEUED AT':>24} {'BLOCK REASON'}"
    )
    typer.echo("-" * 120)
    for i, r in enumerate(runs, 1):
        reason = r.block_reason or "-"
        typer.echo(
            f"{i:>4} {r.run_id:<38} {r.job_name:<20} {r.priority:>4} "
            f"{_ns_to_iso(r.start_time):>24} {reason}"
        )


@queue_app.command("cancel")
def queue_cancel(
    run_id: str = typer.Argument(help="Run ID to cancel"),
    storage_path: str = _STORAGE_PATH_OPT,
) -> None:
    """Cancel a queued run."""
    storage = Storage.embedded(storage_path)
    canceled = storage.cancel_queued_run(run_id)
    if canceled:
        typer.echo(f"Run '{run_id}' canceled.")
    else:
        typer.echo(f"Run '{run_id}' not found or not in Queued status.", err=True)
        raise typer.Exit(1)


@queue_app.command("why")
def queue_why(
    run_id: str = typer.Argument(help="Run ID to inspect"),
    storage_path: str = _STORAGE_PATH_OPT,
) -> None:
    """Explain why a run is queued (show block reason and queue position)."""
    storage = Storage.embedded(storage_path)
    run = storage.get_run(run_id)
    if run is None:
        typer.echo(f"Run '{run_id}' not found.", err=True)
        raise typer.Exit(1)
    if run.status != "Queued":
        typer.echo(f"Run '{run_id}' is not queued (status: {run.status}).")
        return

    all_queued = storage.get_queued_runs()
    all_queued.sort(key=_QUEUE_SORT_KEY)
    position = next(
        (i for i, r in enumerate(all_queued, 1) if r.run_id == run_id), None
    )

    typer.echo(f"Run:          {run.run_id}")
    typer.echo(f"Job:          {run.job_name}")
    typer.echo(f"Priority:     {run.priority}")
    typer.echo(f"Position:     {position}/{len(all_queued)}")
    typer.echo(f"Queued since: {_ns_to_iso(run.start_time)}")
    if run.block_reason:
        typer.echo(f"Block reason: {run.block_reason}")
    else:
        typer.echo("Block reason: waiting for capacity (no specific block recorded)")
    if run.tags:
        typer.echo(f"Tags:         {', '.join(f'{k}={v}' for k, v in run.tags)}")


def main() -> None:
    """Entry point for the ``rivers`` console script."""
    app()


if __name__ == "__main__":
    main()
