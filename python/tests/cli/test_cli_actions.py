"""The CLI launches and shows asset actions like the other surfaces.

`rivers backfill` had no `--action`, no command ran a verb, and the status and
queue commands never showed one — a queued purge read as an ordinary run.
"""

import importlib
import os
import re
import subprocess
import sys
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


PURGE_MODULE = """
import rivers as rs


def _record(ctx):
    with open("calls.txt", "a") as f:
        f.write(f"{ctx.asset_name}:{ctx.partition_key}\\n")


purge = rs.AssetAction(name="purge", outcome=rs.Outcome.Unmaterialize)(_record)
touch = rs.AssetAction(name="touch", outcome=rs.Outcome.Unchanged)(_record)

PARTS = rs.PartitionsDefinition.static_(["p1", "p2"])


@rs.Asset(
    name="events",
    io_handler=rs.InMemoryIOHandler(),
    partitions_def=PARTS,
    actions=[purge, touch],
)
def events(context: rs.AssetExecutionContext):
    return context.partition_key


@rs.Asset(
    name="rollup", io_handler=rs.InMemoryIOHandler(), partitions_def=PARTS, actions=[purge]
)
def rollup(context: rs.AssetExecutionContext):
    return context.partition_key


# Materialize runs here: InMemoryIOHandler cannot cross a worker process.
repo = rs.CodeRepository(
    assets=[events, rollup], default_executor=rs.Executor.in_process()
)
"""

ENDPOINT = "ws://surrealdb:8000"
# A deployed code location's scope: its CodeLocation's spec.identity.
CODE_LOCATION_ID = "550e8400-e29b-41d4-a716-446655440000"

PURGE_WARNING = (
    "Warning: 'purge' is destructive, but its state and pool claims go to a "
    "scratch store that is removed at exit; pass --surreal-endpoint to record "
    "them in shared storage."
)
DEFAULT_SCOPE_WARNING = (
    "Warning: without --code-location-id, runs, asset state and pool claims go "
    "to code location 'default', which a deployed code location does not read."
)


def _warnings(stderr):
    return [line for line in stderr.splitlines() if line.startswith("Warning:")]


def _verb_on_events_p1(command, module, verb="purge"):
    """The same verb on `events` p1, as `run-action` or as `backfill --action`."""
    if command == "run-action":
        return ["run-action", module, verb, "-s", "events", "--partition-key", "p1"]
    return ["backfill", module, "--assets", "events", "-p", "p1", "--action", verb]


def _backfill_id(stdout):
    return re.search(r"^Backfill (\S+):", stdout, re.MULTILINE).group(1)


def _rivers(cwd, *args, env=None):
    # A process of its own: the CLI decides the store's fate at exit.
    return subprocess.run(
        [sys.executable, "-c", "from rivers.cli import main; main()", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=120,
        env=None if env is None else {**os.environ, **env},
    )


def _assert_events_p1_purged(storage):
    assert storage.get_materialized_partitions("events") == []
    assert [str(k) for k in storage.get_materialized_partitions("rollup")] == [
        'PartitionKey("p1")'
    ]
    runs = [r for r in storage.get_runs() if r.action == "purge"]
    assert [(r.status, r.node_names) for r in runs] == [("Success", ["events"])]
    events = storage.get_events_for_run(runs[0].run_id)
    assert "Deletion" in [e.event_type for e in events]


def _connect_to(monkeypatch, path):
    """Stand in for a shared SurrealDB with an embedded store.

    The stub has `connect` only: a command that opened a scratch or embedded
    store instead fails.
    """
    store = embedded_storage(str(path))
    endpoints = []

    def connect(endpoint):
        endpoints.append(endpoint)
        return store

    monkeypatch.setattr(
        "rivers.cli.Storage", type("_S", (), {"connect": staticmethod(connect)})
    )
    return store, endpoints


def _use_endpoint(monkeypatch, via):
    """Name ENDPOINT by flag, or by env var as the operator sets it on a pod."""
    if via == "env":
        monkeypatch.setenv("RIVERS_SURREAL_ENDPOINT", ENDPOINT)
        return []
    return ["--surreal-endpoint", ENDPOINT]


def _in_code_location(via, args):
    """One command that names CODE_LOCATION_ID by flag, or by env var as the
    operator sets it on a pod. Like a process of its own, the command starts
    and ends without the variable otherwise."""
    if via == "env":
        return runner.invoke(
            app, args, env={"RIVERS_CODE_LOCATION_ID": CODE_LOCATION_ID}
        )
    return runner.invoke(
        app,
        [*args, "--code-location-id", CODE_LOCATION_ID],
        env={"RIVERS_CODE_LOCATION_ID": None},
    )


def _as_code_location(monkeypatch, module, store):
    """`store` as the deployed code location reads it."""
    monkeypatch.setenv("RIVERS_CODE_LOCATION_ID", CODE_LOCATION_ID)
    repo = sys.modules[module].repo
    repo.resolve(storage=store)
    return repo.storage


@pytest.mark.parametrize("command", ["run-action", "backfill"])
def test_a_storage_path_the_user_passes_keeps_the_verbs_state(
    resolved_tmp_path, command
):
    """The CLI removed a `--storage-path` the user passed at exit, with the
    delete's state in it — while the verb had already changed the data."""
    module = f"defs_kept_{command.replace('-', '_')}"
    _write_module(resolved_tmp_path, module, PURGE_MODULE)
    path = resolved_tmp_path / "shared_db"

    for args in (
        ["materialize", module, "--partition-key", "p1"],
        _verb_on_events_p1(command, module),
    ):
        done = _rivers(resolved_tmp_path, *args, "--storage-path", str(path))
        assert done.returncode == 0, done.stderr

    assert path.exists()
    _assert_events_p1_purged(embedded_storage(str(path)))


@pytest.mark.parametrize("via", ["flag", "env"])
@pytest.mark.parametrize("command", ["run-action", "backfill"])
def test_surreal_endpoint_records_the_verb_in_the_code_location(
    resolved_tmp_path, monkeypatch, command, via
):
    """A delete from the CLI must land where the code location reads: its
    store, under its id. Under code location "default", the code location
    still read the partition as materialized, and its runs did not wait for
    the delete's claim. A cron pod names both only by env var."""
    module = f"defs_remote_{command.replace('-', '_')}_{via}"
    _write_module(resolved_tmp_path, module, PURGE_MODULE)
    store, endpoints = _connect_to(monkeypatch, resolved_tmp_path / "shared_db")
    endpoint_args = _use_endpoint(monkeypatch, via)

    for args in (
        ["materialize", module, "--partition-key", "p1"],
        _verb_on_events_p1(command, module),
    ):
        done = _in_code_location(via, [*args, *endpoint_args])
        assert done.exit_code == 0, done.output or done.exception
        assert _warnings(done.stderr) == []

    assert endpoints == [ENDPOINT, ENDPOINT]
    assert not (resolved_tmp_path / ".rivers").exists()
    # `store` reads code location "default". It has no asset pool either, so
    # the verb's claim on `events` was not taken there.
    assert store.get_runs() == []
    assert store.get_materialized_partitions("rollup") == []
    assert store.get_pool_limits() == []
    _assert_events_p1_purged(_as_code_location(monkeypatch, module, store))


@pytest.mark.parametrize("via", ["flag", "env"])
def test_backfill_status_and_cancel_read_the_shared_store(
    resolved_tmp_path, monkeypatch, via
):
    module = f"defs_remote_status_{via}"
    _write_module(resolved_tmp_path, module, PURGE_MODULE)
    store, endpoints = _connect_to(monkeypatch, resolved_tmp_path / "shared_db")
    endpoint_args = _use_endpoint(monkeypatch, via)
    ran = _in_code_location(
        via, [*_verb_on_events_p1("backfill", module), *endpoint_args]
    )
    assert ran.exit_code == 0, ran.output or ran.exception
    backfill_id = _backfill_id(ran.stdout)

    status = _in_code_location(
        via, ["backfill-status", backfill_id, module, *endpoint_args]
    )
    assert status.exit_code == 0, status.output or status.exception
    assert f"Backfill {backfill_id}: CompletedSuccess" in status.output
    assert "Action: purge" in status.output

    canceled = _in_code_location(
        via, ["backfill-cancel", backfill_id, module, *endpoint_args]
    )
    assert canceled.exit_code == 0, canceled.output or canceled.exception
    assert endpoints == [ENDPOINT] * 3
    assert [_warnings(done.stderr) for done in (ran, status, canceled)] == [[]] * 3
    # Each command registers the assets under the code location, not "default".
    assert store.get_asset_records() == []


@pytest.mark.parametrize("via", ["flag", "env"])
def test_the_endpoint_without_a_code_location_id_warns(
    resolved_tmp_path, monkeypatch, via
):
    """Outside a rivers pod, nothing names the code location: the command
    records under code location "default", which a deployed code location
    does not read. It says so, and still runs."""
    module = f"defs_default_scope_{via}"
    _write_module(resolved_tmp_path, module, PURGE_MODULE)
    store, _ = _connect_to(monkeypatch, resolved_tmp_path / "shared_db")
    endpoint_args = _use_endpoint(monkeypatch, via)

    results = [
        runner.invoke(app, [*args, *endpoint_args])
        for args in (
            ["materialize", module, "--partition-key", "p1"],
            _verb_on_events_p1("run-action", module),
            _verb_on_events_p1("backfill", module),
        )
    ]
    backfill_id = _backfill_id(results[-1].stdout)
    results += [
        runner.invoke(app, [command, backfill_id, module, *endpoint_args])
        for command in ("backfill-status", "backfill-cancel")
    ]

    for result in results:
        assert result.exit_code == 0, result.output or result.exception
        assert _warnings(result.stderr) == [DEFAULT_SCOPE_WARNING]
    assert sorted(r.action or "materialize" for r in store.get_runs()) == [
        "materialize",
        "purge",
        "purge",
    ]


@pytest.mark.parametrize("command", ["run-action", "backfill"])
def test_without_a_storage_flag_the_scratch_store_is_removed_at_exit(
    resolved_tmp_path, command
):
    module = f"defs_scratch_{command.replace('-', '_')}"
    _write_module(resolved_tmp_path, module, PURGE_MODULE)
    scratch_root = resolved_tmp_path / "tmp"
    scratch_root.mkdir()

    done = _rivers(
        resolved_tmp_path,
        *_verb_on_events_p1(command, module),
        env={"TMPDIR": str(scratch_root)},
    )

    assert done.returncode == 0, done.stderr
    assert (resolved_tmp_path / "calls.txt").read_text().split() == ["events:p1"]
    assert list(scratch_root.iterdir()) == []
    assert not (resolved_tmp_path / ".rivers").exists()


def test_a_command_without_a_storage_flag_leaves_a_kept_store_alone(
    resolved_tmp_path,
):
    """The scratch store was `.rivers/storage/`, a path `--storage-path` keeps:
    a command without a storage flag printed the backfill kept there, then
    removed every run, backfill, asset state and pool limit in it at exit."""
    _write_module(resolved_tmp_path, "defs_kept_store", PURGE_MODULE)
    kept = ["--storage-path", ".rivers/storage/"]
    scratch_root = resolved_tmp_path / "tmp"
    scratch_root.mkdir()
    ran = _rivers(
        resolved_tmp_path,
        "backfill",
        "defs_kept_store",
        "-a",
        "events",
        "-p",
        "p1",
        *kept,
    )
    assert ran.returncode == 0, ran.stderr
    backfill_id = _backfill_id(ran.stdout)

    flagless = _rivers(
        resolved_tmp_path,
        "backfill-status",
        backfill_id,
        "defs_kept_store",
        env={"TMPDIR": str(scratch_root)},
    )
    assert flagless.returncode == 1, flagless.stdout
    assert f"Backfill '{backfill_id}' not found" in flagless.stderr
    assert list(scratch_root.iterdir()) == []

    status = _rivers(
        resolved_tmp_path, "backfill-status", backfill_id, "defs_kept_store", *kept
    )
    assert status.returncode == 0, status.stderr
    assert f"Backfill {backfill_id}: CompletedSuccess" in status.stdout


@pytest.mark.parametrize("command", ["run-action", "backfill"])
@pytest.mark.parametrize(
    ("verb", "storage_args", "warnings"),
    [
        ("purge", [], [PURGE_WARNING]),
        ("purge", ["--memory"], [PURGE_WARNING]),
        ("purge", ["--storage-path", "kept_db"], []),
        ("touch", [], []),
    ],
    ids=["scratch", "memory", "storage-path", "not-destructive"],
)
def test_a_destructive_verb_on_a_scratch_store_warns(
    resolved_tmp_path, command, verb, storage_args, warnings
):
    """A delete on a scratch store changes real data, but its state and pool
    claims live where no other process reads them: the CLI says so and still
    runs the verb."""
    _write_module(resolved_tmp_path, "defs_warn", PURGE_MODULE)

    done = _rivers(
        resolved_tmp_path,
        *_verb_on_events_p1(command, "defs_warn", verb),
        *storage_args,
    )

    assert done.returncode == 0, done.stderr
    assert _warnings(done.stderr) == warnings
    assert (resolved_tmp_path / "calls.txt").read_text().split() == ["events:p1"]


MULTI_PURGE_MODULE = """
import rivers as rs


def _record(ctx):
    with open("calls.txt", "a") as f:
        f.write(ctx.asset_name + "\\n")


purge = rs.AssetAction(name="purge", outcome=rs.Outcome.Unmaterialize)(_record)


class Ingest(rs.MultiAsset):
    m_left = rs.AssetDef(actions=[purge])
    m_right = rs.AssetDef()

    @classmethod
    def materialize(cls):
        return {"m_left": 1, "m_right": 2}


repo = rs.CodeRepository(assets=[Ingest])
"""


def test_a_multi_asset_output_verb_warns_too(resolved_tmp_path):
    """A multi-asset keeps its verbs on each output, not on the asset."""
    _write_module(resolved_tmp_path, "defs_warn_multi", MULTI_PURGE_MODULE)

    done = runner.invoke(app, ["run-action", "defs_warn_multi", "purge", "--memory"])

    assert done.exit_code == 0, done.output or done.exception
    assert _warnings(done.stderr) == [PURGE_WARNING]
    assert (resolved_tmp_path / "calls.txt").read_text().split() == ["m_left"]
