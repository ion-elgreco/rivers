from __future__ import annotations

import typer

from rivers._core.storage import Storage
from rivers.cli._app import queue_app
from rivers.cli._common import _STORAGE_PATH_OPT, _ns_to_iso

_QUEUE_SORT_KEY = lambda r: (-r.priority, r.start_time)  # noqa: E731


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
        f"{'POS':>4} {'RUN ID':<38} {'JOB':<20} {'VERB':<12} {'PRI':>4} "
        f"{'QUEUED AT':>24} {'BLOCK REASON'}"
    )
    typer.echo("-" * 133)
    for i, r in enumerate(runs, 1):
        reason = r.block_reason or "-"
        # An ad-hoc run has no job; a materialize run has no verb.
        typer.echo(
            f"{i:>4} {r.run_id:<38} {r.job_name or '-':<20} {r.action or 'materialize':<12} "
            f"{r.priority:>4} {_ns_to_iso(r.start_time):>24} {reason}"
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
    typer.echo(f"Job:          {run.job_name or '-'}")
    typer.echo(f"Action:       {run.action or 'materialize'}")
    typer.echo(f"Priority:     {run.priority}")
    typer.echo(f"Position:     {position}/{len(all_queued)}")
    typer.echo(f"Queued since: {_ns_to_iso(run.start_time)}")
    if run.block_reason:
        typer.echo(f"Block reason: {run.block_reason}")
    else:
        typer.echo("Block reason: waiting for capacity (no specific block recorded)")
    if run.tags:
        typer.echo(f"Tags:         {', '.join(f'{k}={v}' for k, v in run.tags)}")
