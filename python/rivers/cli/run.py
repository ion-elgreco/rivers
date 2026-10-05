from __future__ import annotations

import importlib
import os
import sys

import typer

from rivers._core.storage import Storage
from rivers.cli._app import app
from rivers.cli._common import (
    _CODE_LOCATION_ID_OPT,
    _load_repo,
    _parse_partition_key,
    _split_names,
    _warn_if_destructive_on_scratch,
)


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
    job: str | None = typer.Option(
        None,
        help="Job name to execute — runs the actual job so job-level config "
        "(retry, executor) applies; takes precedence over --target",
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

    # The run record is the only place the verb lives; without the record
    # nothing can run. Exit 1 lets the operator retry — the record may be
    # missing transiently, or the run was deleted.
    record = storage.get_run(run_id)
    if record is None:
        typer.echo(f"Error: no run record for '{run_id}' — cannot execute", err=True)
        raise typer.Exit(1)
    action = record.action
    config = record.config

    try:
        if job:
            job_obj = repo_obj.get_job(job)
            # A job runs its own verb, which in this image may not be the one
            # the run was launched with. The run never started: exit 1 with a
            # stored outcome, which the operator honors, writing the run's
            # terminal status.
            if job_obj.action != action:
                msg = (
                    f"job '{job}' now runs '{job_obj.action or 'materialize'}', "
                    f"not '{action or 'materialize'}'"
                )
                storage.set_run_outcome(run_id, "Failure", 0, 0, message=msg)
                typer.echo(f"Run {run_id} failed: {msg}", err=True)
                raise typer.Exit(1)
            result = job_obj._execute_run(
                run_id,
                partition_key=pk,
                config=config,
                resume=resume,
                raise_on_error=False,
            )
        elif action is not None:
            result = repo_obj.run_action(
                action,
                selection=selection,
                partition_key=pk,
                config=config,
                run_id_override=run_id,
                raise_on_error=False,
                resume=resume,
            )
        else:
            result = repo_obj.materialize(
                selection=selection,
                partition_key=pk,
                config=config,
                run_id_override=run_id,
                raise_on_error=False,
                resume=resume,
            )
        completed = len(result.materialized_assets) - len(result.failed_assets)
        total = len(result.materialized_assets)

        # A run that COMPLETED — even as a failure or cancellation — exits 0:
        # the outcome travels via storage and the operator honors it. A
        # non-zero exit means "crashed" and triggers the operator's
        # restart-with-resume, which must not fire for a deliberate failure.
        if storage.is_cancelled(run_id):
            storage.set_run_outcome(run_id, "Cancelled", completed, total)
            typer.echo(f"Run {run_id} cancelled: {completed}/{total} steps completed")
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
    except (SystemExit, typer.Exit):
        raise
    except BaseException as exc:
        # Crash path: no outcome is written — the operator restarts the
        # executor with resume. (Writing one here would make the operator
        # honor it and skip the restart.)
        typer.echo(f"Run {run_id} failed: {exc}", err=True)
        raise typer.Exit(1)


@app.command(name="execute-step")
def execute_step(
    module: str = typer.Argument(help="Python module path containing CodeRepository"),
    step_key: list[str] = typer.Option(
        ...,
        help="Asset key(s) of the step to execute — repeated for a "
        "multi-asset step so every output materializes",
    ),
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

    # The run's config overrides live only on its record.
    record = storage.get_run(run_id)
    if record is None:
        typer.echo(f"Error: no run record for '{run_id}' — cannot execute", err=True)
        raise typer.Exit(1)

    name = ", ".join(step_key)
    label = f"{name}[{mapping_key}]" if mapping_key else name
    try:
        _ = repo_obj.materialize(
            selection=list(step_key),
            partition_key=pk,
            config=record.config,
            run_id_override=run_id,
            raise_on_error=True,
        )
        typer.echo(f"Step {label} completed in run {run_id}")
    except SystemExit:
        raise
    except BaseException as exc:
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
    storage_path: str | None = typer.Option(
        None,
        help="Path for embedded storage (default: a scratch store, removed at exit)",
    ),
    surreal_endpoint: str | None = typer.Option(
        None,
        envvar="RIVERS_SURREAL_ENDPOINT",
        help="Remote SurrealDB endpoint (overrides --storage-path)",
    ),
    code_location_id: str | None = _CODE_LOCATION_ID_OPT,
) -> None:
    """Materialize all assets in a repository."""
    from rivers import PartitionKey

    repo_obj = _load_repo(
        module, repo_var, memory, storage_path, surreal_endpoint, code_location_id
    )
    pk = PartitionKey.single(partition_key) if partition_key else None
    result = repo_obj.materialize(partition_key=pk)
    typer.echo(f"Materialization complete. Assets: {result.materialized_assets}")


@app.command(name="run-action")
def run_action(
    module: str = typer.Argument(help="Python module path containing CodeRepository"),
    action: str = typer.Argument(help="The verb to run, e.g. optimize or delete"),
    repo_var: str = typer.Option(
        "repo", help="Variable name of CodeRepository in module"
    ),
    select: str | None = typer.Option(
        None,
        "--select",
        "-s",
        help="Comma-separated asset names (default: every asset that defines the verb)",
    ),
    partition_key: str | None = typer.Option(None, help="Partition key to act on"),
    memory: bool = typer.Option(
        False, help="Use in-memory storage instead of embedded"
    ),
    storage_path: str | None = typer.Option(
        None,
        help="Path for embedded storage (default: a scratch store, removed at exit)",
    ),
    surreal_endpoint: str | None = typer.Option(
        None,
        envvar="RIVERS_SURREAL_ENDPOINT",
        help="Remote SurrealDB endpoint (overrides --storage-path)",
    ),
    code_location_id: str | None = _CODE_LOCATION_ID_OPT,
) -> None:
    """Run an asset action (a verb besides materialize) over a selection."""
    from rivers import PartitionKey

    selection = _split_names(select)
    if selection == []:
        typer.echo(
            "Error: --select is empty — omit it to run the verb on every asset "
            "that defines it",
            err=True,
        )
        raise typer.Exit(1)
    repo_obj = _load_repo(
        module, repo_var, memory, storage_path, surreal_endpoint, code_location_id
    )
    _warn_if_destructive_on_scratch(
        repo_obj, action, selection, memory, storage_path, surreal_endpoint
    )
    pk = PartitionKey.single(partition_key) if partition_key is not None else None
    result = repo_obj.run_action(action, selection=selection, partition_key=pk)
    typer.echo(f"Action '{action}' complete. Run: {result.run_id}")
