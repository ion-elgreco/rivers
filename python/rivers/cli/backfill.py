from __future__ import annotations

import typer

from rivers.cli._app import app
from rivers.cli._common import (
    _CODE_LOCATION_ID_OPT,
    _load_repo,
    _parse_strategy,
    _split_names,
    _warn_if_destructive_on_scratch,
)


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
    action: str | None = typer.Option(
        None, "--action", help="Run this verb instead of materializing"
    ),
    memory: bool = typer.Option(False, help="Use in-memory storage"),
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
    """Backfill partitions for selected assets."""
    from rivers import PartitionKey, PartitionKeyRange

    repo_obj = _load_repo(
        module, repo_var, memory, storage_path, surreal_endpoint, code_location_id
    )

    selection = _split_names(assets)

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

    if action is not None:
        _warn_if_destructive_on_scratch(
            repo_obj, action, selection, memory, storage_path, surreal_endpoint
        )

    result = repo_obj.backfill(
        selection=selection,
        partition_keys=pk_list,
        partition_range=pk_range,
        strategy=resolved_strategy,
        failure_policy=on_failure,
        max_concurrency=concurrency,
        block=True,
        dry_run=dry_run,
        action=action,
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
    """Check status of a backfill."""
    repo_obj = _load_repo(
        module, repo_var, memory, storage_path, surreal_endpoint, code_location_id
    )
    status = repo_obj.get_backfill(backfill_id)
    if status is None:
        typer.echo(f"Backfill '{backfill_id}' not found", err=True)
        raise typer.Exit(1)
    typer.echo(_format_backfill_status(status))


def _format_backfill_status(status) -> str:
    lines = [
        f"Backfill {status.backfill_id}: {status.status}",
        f"  Partitions: {status.completed_partitions}/{status.total_partitions} completed, "
        f"{status.failed_partitions} failed, {status.canceled_partitions} canceled",
        f"  Runs: {len(status.run_ids)}",
    ]
    if status.action:
        lines.append(f"  Action: {status.action}")
    if status.error:
        lines.append(f"  Error: {status.error}")
    return "\n".join(lines)


@app.command(name="backfill-cancel")
def backfill_cancel(
    backfill_id: str = typer.Argument(help="Backfill ID to cancel"),
    module: str = typer.Argument(help="Python module path"),
    repo_var: str = typer.Option("repo", help="Variable name of CodeRepository"),
    memory: bool = typer.Option(False, help="Use in-memory storage"),
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
    """Cancel a running backfill."""
    repo_obj = _load_repo(
        module, repo_var, memory, storage_path, surreal_endpoint, code_location_id
    )
    success = repo_obj.cancel_backfill(backfill_id)
    if success:
        typer.echo(
            f"Backfill '{backfill_id}' canceled (in-process coordinator signaled)"
        )
    else:
        typer.echo(
            f"Backfill '{backfill_id}' cancel requested (not running in this process)"
        )
