from __future__ import annotations

import typer

from rivers._core.storage import Storage
from rivers.cli._app import db_app


# ── Database commands ──


@db_app.command("migrate")
def db_migrate(
    storage_path: str = typer.Option(
        ".rivers/storage/", help="Path for embedded storage"
    ),
    surreal_endpoint: str | None = typer.Option(
        None,
        envvar="RIVERS_SURREAL_ENDPOINT",
        help="Remote SurrealDB endpoint (overrides --storage-path)",
    ),
) -> None:
    """Apply pending storage schema migrations.

    Brings the database up to this rivers build's schema version, running any
    data-heal steps under a cross-process lease. Idempotent — a no-op when the
    database is already current. Run this after upgrading rivers when a code
    location or the UI reports that the database needs migration.
    """
    target = surreal_endpoint or storage_path
    if surreal_endpoint:
        Storage.migrate_remote(surreal_endpoint)
    else:
        Storage.migrate_embedded(storage_path)
    typer.echo(f"Storage schema is up to date ({target}).")
