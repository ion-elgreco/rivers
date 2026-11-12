"""`rivers backfill --detach` records the backfill and returns.

The command ran every backfill in its own process, so a large backfill needed
a terminal that stayed open. With `--detach` the daemon of the code location
runs it.
"""

import importlib
import re
import subprocess
import sys
import time

import pytest
from rivers._core import AutomationDaemon
from rivers.testing import embedded_storage
from typer.testing import CliRunner

from rivers.cli import app

_CLI_TIMEOUT_S = 120
pytestmark = pytest.mark.timeout(2 * _CLI_TIMEOUT_S)

runner = CliRunner()

MODULE = """
import rivers as rs


@rs.Asset(
    name="events",
    io_handler=rs.InMemoryIOHandler(),
    partitions_def=rs.PartitionsDefinition.static_(["p1", "p2", "p3"]),
)
def events(context: rs.AssetExecutionContext):
    return context.partition_key


repo = rs.CodeRepository(assets=[events], default_executor=rs.Executor.in_process())
"""


def _write_module(tmp, name):
    importlib.invalidate_caches()
    (tmp / f"{name}.py").write_text(MODULE)


def _backfill_events(module, *args):
    return ["backfill", module, "--assets", "events", "-p", "p1,p2,p3", *args]


def test_detach_records_a_backfill_that_the_daemon_runs(resolved_tmp_path, monkeypatch):
    _write_module(resolved_tmp_path, "defs_detach")
    # A process of its own: the backfill must still be there when it is gone.
    launched = subprocess.run(
        [
            sys.executable,
            "-c",
            "from rivers.cli import main; main()",
            *_backfill_events("defs_detach", "--detach", "--storage-path", "store"),
        ],
        cwd=resolved_tmp_path,
        capture_output=True,
        text=True,
        timeout=_CLI_TIMEOUT_S,
    )
    assert launched.returncode == 0, launched.stderr
    printed = re.search(
        r"^Backfill (\S+): Requested — 3 partitions, 3 runs$",
        launched.stdout,
        re.MULTILINE,
    )
    assert printed, launched.stdout
    backfill_id = printed.group(1)

    storage = embedded_storage(str(resolved_tmp_path / "store"))
    assert storage.get_runs() == []
    monkeypatch.syspath_prepend(str(resolved_tmp_path))
    repo = importlib.import_module("defs_detach").repo
    repo.resolve(storage=storage)
    requested = repo.get_backfill(backfill_id)
    assert (requested.status, requested.total_partitions) == ("Requested", 3)

    daemon = AutomationDaemon(repo=repo, storage=storage, condition_eval_interval="1s")
    daemon.start()
    try:
        deadline = time.monotonic() + 60
        status = repo.get_backfill(backfill_id)
        while (
            status.status in ("Requested", "InProgress") and time.monotonic() < deadline
        ):
            time.sleep(0.2)
            status = repo.get_backfill(backfill_id)
    finally:
        daemon.stop()

    assert status.status == "CompletedSuccess", status
    assert (status.completed_partitions, len(status.run_ids)) == (3, 3)
    assert sorted(str(k) for k in storage.get_materialized_partitions("events")) == [
        "p1",
        "p2",
        "p3",
    ]


@pytest.mark.parametrize(
    "storage_args",
    [[], ["--memory"], ["--memory", "--storage-path", "store"]],
    ids=["scratch", "memory", "memory-over-path"],
)
def test_detach_needs_a_store_that_a_daemon_reads(resolved_tmp_path, storage_args):
    module = f"defs_detach_refused_{len(storage_args)}"
    _write_module(resolved_tmp_path, module)

    refused = runner.invoke(app, _backfill_events(module, "--detach", *storage_args))

    assert refused.exit_code == 1, refused.output
    assert "--detach" in refused.stderr
    assert "--surreal-endpoint or --storage-path" in refused.stderr
    assert not (resolved_tmp_path / "store").exists()


def test_detach_dry_run_only_previews(resolved_tmp_path):
    _write_module(resolved_tmp_path, "defs_detach_dry_run")

    previewed = runner.invoke(
        app, _backfill_events("defs_detach_dry_run", "--detach", "--dry-run")
    )

    assert previewed.exit_code == 0, previewed.output
    assert "Dry run: 3 partitions, 3 runs" in previewed.stdout
