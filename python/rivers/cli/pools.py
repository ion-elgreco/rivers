from __future__ import annotations

import typer

from rivers._core.storage import Storage
from rivers.cli._app import pools_app
from rivers.cli._common import _STORAGE_PATH_OPT, _ns_to_iso


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
