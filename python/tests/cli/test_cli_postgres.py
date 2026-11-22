"""``rivers db`` and ``rivers materialize`` on a PostgreSQL store, each in a
schema of its own. Skipped without ``RIVERS_TEST_POSTGRES_URL``."""

import importlib

import rivers as rs
from typer.testing import CliRunner

from rivers.cli import app

runner = CliRunner()


def test_db_migrate_and_status(postgres_url):
    status = runner.invoke(app, ["db", "status", "--storage-url", postgres_url])
    assert status.exit_code == 0, status.output
    assert "Database:   not initialized" in status.output

    migrated = runner.invoke(app, ["db", "migrate", "--storage-url", postgres_url])
    assert migrated.exit_code == 0, migrated.output
    redacted = rs.Storage.redact_url(postgres_url)
    assert f"Storage schema is up to date ({redacted})." in migrated.output

    status = runner.invoke(
        app, ["db", "status"], env={"RIVERS_STORAGE_URL": postgres_url}
    )
    assert status.exit_code == 0, status.output
    assert "State:      up to date" in status.output


MODULE = """
import rivers as rs


@rs.Asset(io_handler=rs.InMemoryIOHandler())
def tiny() -> int:
    return 1


repo = rs.CodeRepository(assets=[tiny], default_executor=rs.Executor.in_process())
"""


def test_materialize_records_in_the_store_of_its_url(postgres_url, resolved_tmp_path):
    (resolved_tmp_path / "defs_postgres.py").write_text(MODULE)
    # The command imports the module off `sys.path.insert(0, ".")`.
    importlib.invalidate_caches()

    result = runner.invoke(
        app,
        ["materialize", "defs_postgres", "--code-location-id", "default"],
        env={"RIVERS_STORAGE_URL": postgres_url},
    )

    assert result.exit_code == 0, result.output
    assert not (resolved_tmp_path / ".rivers").exists()
    (run,) = rs.Storage.open(postgres_url).get_runs()
    assert run.status == "Success"
