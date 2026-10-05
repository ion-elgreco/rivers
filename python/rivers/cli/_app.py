import typer

app = typer.Typer(name="rivers", help="rivers orchestration CLI")
pools_app = typer.Typer(name="pools", help="Inspect and manage concurrency pools")
queue_app = typer.Typer(name="queue", help="Inspect and manage the run queue")
db_app = typer.Typer(name="db", help="Storage schema management")
app.add_typer(pools_app)
app.add_typer(queue_app)
app.add_typer(db_app)
