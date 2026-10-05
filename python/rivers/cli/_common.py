from __future__ import annotations

import atexit
import importlib
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import typer

from rivers._core.storage import Storage
from rivers.exceptions import SchemaMigrationNeededError


def _parse_partition_key(raw: str | None):
    """Parse a partition key from CLI arg. Accepts JSON (from step pods) or plain string."""
    if raw is None:
        return None
    from rivers import PartitionKey as PK

    if raw.startswith("{"):
        return PK.from_json(raw)
    return PK.single(raw)


def _split_names(raw: str | None) -> list[str] | None:
    """``None`` when the flag is omitted. An explicit empty value stays ``[]``:
    a caller's list that came out empty, never "every asset"."""
    if raw is None:
        return None
    return [name.strip() for name in raw.split(",") if name.strip()]


def _cleanup_storage(path: str) -> None:
    """Remove embedded storage directory on exit."""
    p = Path(path)
    if p.exists():
        shutil.rmtree(p, ignore_errors=True)
        # Also remove the parent .rivers dir if it's now empty
        parent = p.parent
        if parent.name == ".rivers" and parent.exists() and not any(parent.iterdir()):
            parent.rmdir()


_CODE_LOCATION_ID_OPT = typer.Option(
    None,
    envvar="RIVERS_CODE_LOCATION_ID",
    help="Code location to record runs, asset state and pool claims under "
    "(default: 'default')",
)


def _create_storage(
    memory: bool,
    storage_path: str | None,
    surreal_endpoint: str | None,
    code_location_id: str | None = None,
) -> tuple[Storage, str | None]:
    """Create storage backend based on CLI flags.

    The storage and ``resolve()`` read the code location from the
    environment. Also returns the scratch store's directory, which the caller
    removes at exit; a path the user passed is never removed.
    """
    if code_location_id is not None:
        os.environ["RIVERS_CODE_LOCATION_ID"] = code_location_id
    if surreal_endpoint is not None:
        if not code_location_id:
            typer.echo(
                "Warning: without --code-location-id, runs, asset state and pool "
                "claims go to code location 'default', which a deployed code "
                "location does not read.",
                err=True,
            )
        return Storage.connect(surreal_endpoint), None
    if memory:
        return Storage.memory(), None
    if storage_path is not None:
        return Storage.embedded(storage_path), None
    scratch = tempfile.mkdtemp(prefix="rivers-scratch-")
    return Storage._scratch(scratch), scratch  # type: ignore[attr-defined]


def _remove_scratch(repo_obj, scratch: str) -> None:
    """Windows cannot remove open files: the repository lets go of the store
    first, which closes it."""
    repo_obj._release_storage()
    shutil.rmtree(scratch, ignore_errors=True)


def _open_or_prompt_migrate(open_fn, migrate_fn) -> Storage:
    """Open storage; if the database is behind this build's schema, offer to
    migrate it (``rivers dev`` is interactive). Any other error propagates, as
    does a declined prompt. ``serve`` deliberately does not prompt — a cloud
    deployment migrates via an explicit ``rivers db migrate`` init step.
    """
    try:
        return open_fn()
    except SchemaMigrationNeededError as exc:
        typer.echo(f"Storage schema is behind this rivers build:\n  {exc}", err=True)
        if not typer.confirm("Run the migration now?", default=False):
            raise
        migrate_fn()
        return open_fn()


def _warn_if_destructive_on_scratch(
    repo_obj,
    verb: str,
    selection: list[str] | None,
    memory: bool,
    storage_path: str | None,
    surreal_endpoint: str | None,
) -> None:
    """Warn when a verb that clears materialization state keeps its state and
    pool claims in a store that is gone at exit (the scratch store or memory)."""
    if surreal_endpoint is not None or (storage_path is not None and not memory):
        return
    from rivers import MultiAsset, Outcome

    for name, asset in repo_obj.assets.items():
        if selection is not None and name not in selection:
            continue
        actions = asset.actions
        if actions is None and isinstance(asset, MultiAsset):
            # A multi-asset's verbs live on each output.
            actions = next((d.actions for d in asset.output_defs if d.name == name), [])
        if any(
            a.name == verb and a.outcome == Outcome.Unmaterialize for a in actions or []
        ):
            typer.echo(
                f"Warning: '{verb}' is destructive, but its state and pool claims go "
                "to a scratch store that is removed at exit; pass --surreal-endpoint "
                "to record them in shared storage.",
                err=True,
            )
            return


def _load_repo(
    module: str,
    repo_var: str,
    memory: bool,
    storage_path: str | None,
    surreal_endpoint: str | None,
    code_location_id: str | None,
):
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

    storage, scratch = _create_storage(
        memory, storage_path, surreal_endpoint, code_location_id
    )
    repo_obj.resolve(storage=storage)
    if scratch is not None:
        atexit.register(_remove_scratch, repo_obj, scratch)
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


def _ns_to_iso(ns: int) -> str:
    """Convert nanosecond timestamp to human-readable ISO 8601 string."""
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


_STORAGE_PATH_OPT = typer.Option(".rivers/storage/", help="Path for embedded storage")
