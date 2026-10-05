"""rivers CLI — development server and materialization commands."""

from __future__ import annotations

# Import order sets the command order in --help.
from rivers.cli import server, run, backfill, pools, run_queue, db  # noqa: F401
from rivers.cli._app import app, db_app, pools_app, queue_app  # noqa: F401


def main() -> None:
    """Entry point for the ``rivers`` console script."""
    app()


if __name__ == "__main__":
    main()
