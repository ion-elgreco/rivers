from pathlib import Path

import typer

from rivers.cli.config import SETTING_NAMES, RiversConfig

app = typer.Typer(name="rivers", help="rivers orchestration CLI")
pools_app = typer.Typer(name="pools", help="Inspect and manage concurrency pools")
queue_app = typer.Typer(name="queue", help="Inspect and manage the run queue")
db_app = typer.Typer(name="db", help="Storage schema management")
app.add_typer(pools_app)
app.add_typer(queue_app)
app.add_typer(db_app)


@app.callback()
def _load_config(
    ctx: typer.Context,
    config: Path | None = typer.Option(
        None,
        envvar="RIVERS_CONFIG",
        help="Path to a rivers.toml (replaces the discovered one)",
    ),
) -> None:
    if ctx.resilient_parsing:
        return

    ctx.obj = config
    cfg = RiversConfig.from_cli(config=config)
    ctx.default_map = cfg.default_map(ctx.command)
    ctx.meta[SETTING_NAMES] = cfg.setting_names()
