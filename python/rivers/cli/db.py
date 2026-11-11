from __future__ import annotations

import time

import typer

from rivers._core.storage import ProcessStatus, SchemaStatus, Storage
from rivers.cli._app import db_app
from rivers.exceptions import MigrationBlockedError


# ── Database commands ──


def _ago(since_ns: int, now_ns: int) -> str:
    seconds = max(0, (now_ns - since_ns) // 10**9)
    for unit, size in (("d", 86400), ("h", 3600), ("min", 60)):
        if seconds >= size:
            return f"{seconds // size} {unit} ago"
    return f"{seconds} s ago"


def _process_row(process: ProcessStatus, now_ns: int) -> tuple[str, ...]:
    if process.seen is None:
        seen = "never"
    elif process.code_location:
        seen = _ago(process.seen, now_ns)
    else:
        seen = f"started {_ago(process.seen, now_ns)}"

    note = ""
    if process.stops:
        note = "stops after migration"
        if process.schema_version is None:
            note += " (version unknown)"

    return (
        process.name,
        "code location" if process.code_location else "component",
        "?" if process.schema_version is None else str(process.schema_version),
        process.rivers_version or "?",
        seen,
        note,
    )


def _process_table(processes: list[ProcessStatus], now_ns: int) -> list[str]:
    rows = [("NAME", "KIND", "SCHEMA", "RIVERS", "SEEN", "")]
    rows += [_process_row(process, now_ns) for process in processes]
    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]
    return [
        "  ".join(cell.ljust(width) for cell, width in zip(row, widths)).rstrip()
        for row in rows
    ]


def _status_lines(status: SchemaStatus, now_ns: int) -> list[str]:
    build = (
        f"schema {status.build_version} (read floor {status.build_min_reader}, "
        f"write floor {status.build_min_writer})"
    )
    if status.version is None:
        return ["Database:   not initialized", f"This build: {build}"]

    if status.version < status.build_version:
        state = "migration needed: run `rivers db migrate`"
    elif status.version > status.build_version:
        state = "the database is ahead of this build"
    else:
        state = "up to date"

    lines = [
        f"Database:   schema {status.version} (read floor {status.min_reader}, "
        f"write floor {status.min_writer})",
        f"This build: {build}",
        f"State:      {state}",
    ]
    if status.processes:
        lines += ["", *_process_table(status.processes, now_ns)]
    return lines


@db_app.command("status")
def db_status(
    storage_path: str = typer.Option(
        ".rivers/storage/", help="Path for embedded storage"
    ),
    surreal_endpoint: str | None = typer.Option(
        None,
        envvar="RIVERS_SURREAL_ENDPOINT",
        help="Remote SurrealDB endpoint (overrides --storage-path)",
    ),
) -> None:
    """Show the storage schema version and what a migration stops.

    Lists each code location, and the operator and the UI, with the rivers
    build it runs. A migration to this build stops the ones it marks. The
    database is not changed.
    """
    if surreal_endpoint:
        status = Storage.schema_status_remote(surreal_endpoint)
    else:
        status = Storage.schema_status_embedded(storage_path)

    for line in _status_lines(status, time.time_ns()):
        typer.echo(line)


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
    force: bool = typer.Option(
        False,
        "--force",
        help="Migrate even when it stops a code location, the operator or the UI",
    ),
) -> None:
    """Apply pending storage schema migrations.

    Brings the database up to this rivers build's schema version, running any
    data-heal steps under a cross-process lease. Idempotent — a no-op when the
    database is already current. Run this after upgrading rivers when a code
    location or the UI reports that the database needs migration.

    A migration of a remote database that stops a code location, the operator
    or the UI is refused. `rivers db status` lists them; `--force` migrates.
    """
    target = surreal_endpoint or storage_path
    if surreal_endpoint:
        try:
            Storage.migrate_remote(surreal_endpoint, force=force)
        except MigrationBlockedError:
            status = Storage.schema_status_remote(surreal_endpoint)
            stopped = [process for process in status.processes if process.stops]
            for line in _process_table(stopped, time.time_ns()):
                typer.echo(line, err=True)
            typer.echo(
                "\nNot migrated: the migration stops the processes above. "
                "Update them to a newer rivers, or migrate with --force.",
                err=True,
            )
            raise typer.Exit(1) from None
    else:
        Storage.migrate_embedded(storage_path)
    typer.echo(f"Storage schema is up to date ({target}).")
