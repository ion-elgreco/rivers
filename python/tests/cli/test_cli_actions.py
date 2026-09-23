"""The CLI launches and shows asset actions like the other surfaces.

`rivers backfill` had no `--action`, no command ran a verb, and the status and
queue commands never showed one — a queued purge read as an ordinary run.
"""

import importlib
from types import SimpleNamespace

import pytest
from rivers.testing import embedded_storage
from typer.testing import CliRunner

from rivers.cli import _format_backfill_status, app

runner = CliRunner()

ACTION_MODULE = """
import rivers as rs


def _compact(ctx):
    with open("calls.txt", "a") as f:
        f.write(ctx.partition_key + "\\n")


compact = rs.AssetAction(name="compact", outcome=rs.Outcome.Unchanged)(_compact)


@rs.Asset(
    name="events",
    io_handler=rs.InMemoryIOHandler(),
    partitions_def=rs.PartitionsDefinition.static_(["p1", "p2"]),
    actions=[compact],
)
def events(context: rs.AssetExecutionContext):
    return context.partition_key


repo = rs.CodeRepository(assets=[events])
"""


OPTIONAL_VERB_MODULE = """
import rivers as rs


def _purge(ctx):
    key = ctx.partition_key if ctx.has_partition_key else "*"
    with open("calls.txt", "a") as f:
        f.write(f"{ctx.asset_name}:{key}\\n")


purge = rs.AssetAction(
    name="purge",
    outcome=rs.Outcome.Unmaterialize,
    partitioning=rs.ActionPartitioning.Optional,
)(_purge)

PARTS = rs.PartitionsDefinition.static_(["p1", "p2"])


@rs.Asset(
    name="events", io_handler=rs.InMemoryIOHandler(), partitions_def=PARTS, actions=[purge]
)
def events(context: rs.AssetExecutionContext):
    return context.partition_key


@rs.Asset(
    name="rollup", io_handler=rs.InMemoryIOHandler(), partitions_def=PARTS, actions=[purge]
)
def rollup(context: rs.AssetExecutionContext):
    return context.partition_key


repo = rs.CodeRepository(assets=[events, rollup])
"""


def _write_module(tmp, name, source=ACTION_MODULE):
    # The CLI imports the module off `sys.path.insert(0, ".")`; each test runs
    # in its own tmp cwd, so the finder's cache for "." is stale.
    importlib.invalidate_caches()
    (tmp / f"{name}.py").write_text(source)


@pytest.mark.parametrize(
    ("module", "args"),
    [
        (
            "defs_empty_key",
            [
                "run-action",
                "defs_empty_key",
                "purge",
                "-s",
                "events",
                "--partition-key",
                "",
            ],
        ),
        (
            "defs_empty_select",
            [
                "run-action",
                "defs_empty_select",
                "purge",
                "-s",
                "",
                "--partition-key",
                "p1",
            ],
        ),
        (
            "defs_empty_assets",
            [
                "backfill",
                "defs_empty_assets",
                "--assets",
                "",
                "-p",
                "p1",
                "--action",
                "purge",
            ],
        ),
    ],
    ids=["empty-key", "empty-select", "backfill-empty-assets"],
)
def test_an_empty_value_never_widens_a_destructive_verb(
    resolved_tmp_path, module, args
):
    """An empty `$DAY` or `$ASSETS` in a script must fail, not purge the whole
    table or every asset that declares the verb."""
    _write_module(resolved_tmp_path, module, OPTIONAL_VERB_MODULE)
    result = runner.invoke(app, [*args, "--memory"])
    assert result.exit_code != 0, result.output
    assert not (resolved_tmp_path / "calls.txt").exists()


def test_backfill_takes_an_action(resolved_tmp_path):
    _write_module(resolved_tmp_path, "defs_bf_action")
    result = runner.invoke(
        app,
        [
            "backfill",
            "defs_bf_action",
            "--partitions",
            "p1,p2",
            "--action",
            "compact",
            "--memory",
        ],
    )
    assert result.exit_code == 0, result.output
    calls = (resolved_tmp_path / "calls.txt").read_text().split()
    assert sorted(calls) == ["p1", "p2"]


def test_run_action_command_runs_the_verb(resolved_tmp_path):
    _write_module(resolved_tmp_path, "defs_run_action")
    result = runner.invoke(
        app,
        [
            "run-action",
            "defs_run_action",
            "compact",
            "--partition-key",
            "p1",
            "--memory",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "compact" in result.output
    assert (resolved_tmp_path / "calls.txt").read_text().split() == ["p1"]


def test_backfill_status_names_the_verb():
    status = SimpleNamespace(
        backfill_id="bf1",
        status="CompletedSuccess",
        completed_partitions=2,
        total_partitions=2,
        failed_partitions=0,
        canceled_partitions=0,
        run_ids=["r1", "r2"],
        error=None,
        action="delete",
    )
    assert "Action: delete" in _format_backfill_status(status)
    status.action = None
    assert "Action" not in _format_backfill_status(status)


def test_queue_commands_name_the_verb(resolved_tmp_path, monkeypatch):
    path = str(resolved_tmp_path / "queue_db")
    storage = embedded_storage(path)
    # An ad-hoc run (no job) crashed `queue list` on its None job name.
    storage._create_run(
        "q-purge", "", "Queued", 1000, node_names=["events"], action="delete"
    )
    storage._create_run("q-etl", "etl", "Queued", 2000)
    # RocksDB allows one opener per process: hand the CLI the open store.
    monkeypatch.setattr(
        "rivers.cli.Storage",
        type("_S", (), {"embedded": staticmethod(lambda *a, **k: storage)}),
    )

    listed = runner.invoke(app, ["queue", "list", "--storage-path", path])
    assert listed.exit_code == 0, listed.output
    purge_line = next(line for line in listed.output.splitlines() if "q-purge" in line)
    assert "delete" in purge_line
    assert "etl" in listed.output

    why = runner.invoke(app, ["queue", "why", "q-purge", "--storage-path", path])
    assert why.exit_code == 0, why.output
    assert "Action:       delete" in why.output
