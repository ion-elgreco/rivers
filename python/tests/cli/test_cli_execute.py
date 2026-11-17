"""`rivers execute` — the K8s run-pod entry point.

The verb never rides the pod args: the command recovers it from the run
record, which makes the record read load-bearing. The default suite otherwise
never exercises this command (`norecursedirs` excludes the only integration
test that does).
"""

import time
from datetime import datetime

import pytest
import rivers as rs
from rivers.testing import embedded_storage
from typer.testing import CliRunner

from rivers.cli import app

runner = CliRunner()

REPO_MODULE = """
import rivers as rs


@rs.Asset(name="tiny")
def tiny() -> int:
    return 1


repo = rs.CodeRepository(assets=[tiny])
"""


def _cloud_env(monkeypatch, path):
    """Point `Storage.connect` at an embedded store and satisfy the cloud-mode
    env contract, so the command runs without a remote SurrealDB."""
    import importlib

    # The command imports the definitions module off `sys.path.insert(0, ".")`.
    # Each test runs in its own tmp cwd, so the finder's cache for "." is stale
    # from any earlier test in this file.
    importlib.invalidate_caches()
    monkeypatch.setenv("RIVERS_CODE_LOCATION_ID", "default")
    store = embedded_storage(str(path))
    monkeypatch.setattr(
        "rivers.cli.run.Storage",
        type("_S", (), {"connect": staticmethod(lambda *a, **k: store)}),
    )
    return store


def test_execute_without_a_run_record_fails_loudly(resolved_tmp_path, monkeypatch):
    """A missing record must not default to materialize.

    The verb lives only on the record here, so falling back to `None` ran a
    materialize for a run that may have been queued as a destructive action —
    silently, and reported as success.
    """
    (resolved_tmp_path / "defs_exec.py").write_text(REPO_MODULE)
    _cloud_env(monkeypatch, resolved_tmp_path / "storage")

    result = runner.invoke(
        app,
        [
            "execute",
            "defs_exec",
            "--run-id",
            "no-such-run",
            "--surreal-endpoint",
            "ws://unused",
        ],
    )
    assert result.exit_code == 1, result.output
    assert "no run record for 'no-such-run'" in result.output


ACTION_REPO_MODULE = """
import rivers as rs


def _touch(ctx):
    return None


touch = rs.AssetAction(name="touch", outcome=rs.Outcome.Unchanged)(_touch)


@rs.Asset(name="tiny", actions=[touch])
def tiny() -> int:
    return 1


repo = rs.CodeRepository(assets=[tiny])
"""


def test_execute_runs_the_verb_from_the_record(resolved_tmp_path, monkeypatch):
    """A record with a verb routes to the action, with no flag on the pod.

    Falsifier: routing to materialize instead would emit a Materialization
    event for `tiny`."""
    (resolved_tmp_path / "defs_exec4.py").write_text(ACTION_REPO_MODULE)
    store = _cloud_env(monkeypatch, resolved_tmp_path / "storage4")

    def _touch(ctx):
        return None

    touch = rs.AssetAction(name="touch", outcome=rs.Outcome.Unchanged)(_touch)
    seed = rs.CodeRepository(assets=[rs.Asset(name="tiny", actions=[touch])(lambda: 1)])
    seed.resolve(storage=store)
    seed.run_action("touch", run_id_override="pod-run-3")

    result = runner.invoke(
        app,
        [
            "execute",
            "defs_exec4",
            "--run-id",
            "pod-run-3",
            "--surreal-endpoint",
            "ws://unused",
        ],
    )
    assert result.exit_code == 0, result.output
    events = store.get_events_for_asset("tiny")
    assert not [e for e in events if e.event_type == "Materialization"]
    outcome = store.kv_get("run_outcome:pod-run-3")
    assert outcome is not None and b"Success" in outcome


def test_execute_routes_a_materialize_run(resolved_tmp_path, monkeypatch):
    """Falsifier: a record with no verb still routes to materialize."""
    (resolved_tmp_path / "defs_exec2.py").write_text(REPO_MODULE)
    store = _cloud_env(monkeypatch, resolved_tmp_path / "storage2")

    # Create the record the pod would find, by running the same asset once under
    # a known id; `--resume` then re-enters it with every step complete.
    seed = rs.CodeRepository(assets=[rs.Asset(name="tiny")(lambda: 1)])
    seed.resolve(storage=store)
    seed.materialize(run_id_override="pod-run-1")
    assert store.get_run("pod-run-1").action is None

    result = runner.invoke(
        app,
        [
            "execute",
            "defs_exec2",
            "--run-id",
            "pod-run-1",
            "--surreal-endpoint",
            "ws://unused",
            "--resume",
        ],
    )
    assert result.exit_code == 0, result.output


JOB_REPO_MODULE = """
import rivers as rs


def _touch(ctx):
    return None


touch = rs.AssetAction(name="touch", outcome=rs.Outcome.Unchanged)(_touch)


@rs.Asset(name="tiny", actions=[touch])
def tiny() -> int:
    return 1


repo = rs.CodeRepository(
    assets=[tiny], jobs=[rs.Job(name="nightly", assets=[tiny], action={action!r})]
)
"""


def _execute_nightly(resolved_tmp_path, monkeypatch, recorded, current):
    """Run the pod command on a `nightly` run recorded with verb `recorded`,
    from definitions whose `nightly` runs `current`."""
    # One module name per pair: the command imports it, and an imported module
    # stays bound to the store of the test that first imported it.
    module = f"defs_nightly_{recorded or 'materialize'}_{current or 'materialize'}"
    (resolved_tmp_path / f"{module}.py").write_text(
        JOB_REPO_MODULE.format(action=current)
    )
    store = _cloud_env(monkeypatch, resolved_tmp_path / "storage")
    store._create_run(
        "pod-run-5",
        "nightly",
        "NotStarted",
        time.time_ns(),
        node_names=["tiny"],
        action=recorded,
    )
    result = runner.invoke(
        app,
        [
            "execute",
            module,
            "--run-id",
            "pod-run-5",
            "--job",
            "nightly",
            "--surreal-endpoint",
            "ws://unused",
        ],
    )
    return store, result


@pytest.mark.parametrize(
    "recorded, current", [(None, "touch"), ("touch", None)], ids=["gained", "lost"]
)
def test_execute_job_refuses_a_verb_the_run_was_not_launched_with(
    resolved_tmp_path, monkeypatch, recorded, current
):
    """The record keeps the verb the run was launched with, but the pod ran
    the job as the image it starts defines it, which a redeploy may have
    changed: a queued materialize ran the job's new delete. The pod fails
    the run instead. It stores the outcome and exits non-zero, so the
    operator writes the terminal status of a run that never started."""
    store, result = _execute_nightly(resolved_tmp_path, monkeypatch, recorded, current)

    message = (
        f"job 'nightly' now runs '{current or 'materialize'}', "
        f"not '{recorded or 'materialize'}'"
    )
    assert result.exit_code == 1, result.output
    assert message in result.output
    outcome = store.kv_get("run_outcome:pod-run-5")
    assert outcome is not None and b"Failure" in outcome
    assert message.encode() in outcome
    assert store.get_events_for_run("pod-run-5") == []
    assert store.get_run("pod-run-5").status == "NotStarted"


def test_execute_job_runs_the_verb_the_run_was_launched_with(
    resolved_tmp_path, monkeypatch
):
    """Falsifier: the job's verb and the record's agree, so the run runs."""
    store, result = _execute_nightly(resolved_tmp_path, monkeypatch, "touch", "touch")

    assert result.exit_code == 0, result.output
    types = {e.event_type for e in store.get_events_for_run("pod-run-5")}
    assert "ActionCompleted" in types and "Materialization" not in types
    outcome = store.kv_get("run_outcome:pod-run-5")
    assert outcome is not None and b"Success" in outcome


CONFIG_REPO_MODULE = """
from pathlib import Path

import rivers as rs
from pydantic import BaseModel


class Cfg(BaseModel):
    threshold: float = 0.5


@rs.Asset(name="cfg", metadata={"tier": "bronze"})
def cfg(context: rs.AssetExecutionContext[Cfg]) -> float:
    seen = f"{context.config.threshold} {context.asset_metadata['tier']}"
    Path("cfg_seen.txt").write_text(seen)
    return context.config.threshold


repo = rs.CodeRepository(assets=[cfg])
"""


def test_execute_applies_the_records_config(resolved_tmp_path, monkeypatch):
    """The launch document rides the run record, not the pod args: a run
    launched from the UI or the queue applies it in its pod."""
    (resolved_tmp_path / "defs_cfg.py").write_text(CONFIG_REPO_MODULE)
    store = _cloud_env(monkeypatch, resolved_tmp_path / "storage_cfg")
    store._create_run(
        "pod-cfg",
        "",
        "Queued",
        1,
        node_names=["cfg"],
        config={
            "assets": {
                "cfg": {"config": {"threshold": 0.9}, "metadata": {"tier": "gold"}}
            }
        },
    )

    result = runner.invoke(
        app,
        [
            "execute",
            "defs_cfg",
            "--run-id",
            "pod-cfg",
            "--surreal-endpoint",
            "ws://unused",
        ],
    )
    assert result.exit_code == 0, result.output
    assert (resolved_tmp_path / "cfg_seen.txt").read_text() == "0.9 gold"
    outcome = store.kv_get("run_outcome:pod-cfg")
    assert outcome is not None and b"Success" in outcome


def test_execute_step_applies_the_records_config(resolved_tmp_path, monkeypatch):
    """A step pod runs one step of a run the run pod started: it reads the
    same record for the overrides."""
    (resolved_tmp_path / "defs_cfg_step.py").write_text(CONFIG_REPO_MODULE)
    store = _cloud_env(monkeypatch, resolved_tmp_path / "storage_cfg_step")
    monkeypatch.setenv("RIVERS_SURREAL_ENDPOINT", "ws://unused")
    store._create_run(
        "pod-cfg-step",
        "",
        "Started",
        1,
        node_names=["cfg"],
        config={"assets": {"cfg": {"config": {"threshold": 0.75}}}},
    )

    result = runner.invoke(
        app,
        [
            "execute-step",
            "defs_cfg_step",
            "--step-key",
            "cfg",
            "--run-id",
            "pod-cfg-step",
        ],
    )
    assert result.exit_code == 0, result.output
    assert (resolved_tmp_path / "cfg_seen.txt").read_text() == "0.75 bronze"


def test_execute_step_without_a_run_record_fails_loudly(resolved_tmp_path, monkeypatch):
    (resolved_tmp_path / "defs_cfg_missing.py").write_text(CONFIG_REPO_MODULE)
    _cloud_env(monkeypatch, resolved_tmp_path / "storage_cfg_missing")
    monkeypatch.setenv("RIVERS_SURREAL_ENDPOINT", "ws://unused")

    result = runner.invoke(
        app,
        ["execute-step", "defs_cfg_missing", "--step-key", "cfg", "--run-id", "absent"],
    )
    assert result.exit_code == 1
    assert "no run record for 'absent'" in result.output
    assert not (resolved_tmp_path / "cfg_seen.txt").exists()


TRIPS_REPO_MODULE = """
from datetime import datetime
from pathlib import Path

import rivers as rs


@rs.Asset(
    name="trips",
    partitions_def=rs.PartitionsDefinition.multi(
        {
            "hour": rs.PartitionsDefinition.hourly(
                start=datetime(2024, 1, 1), end=datetime(2024, 1, 3)
            ),
            "zone": rs.PartitionsDefinition.static_(["132", "7"]),
        }
    ),
)
def trips(context: rs.AssetExecutionContext) -> None:
    if Path("trips_fail").exists():
        raise ValueError("no trips")
    keys = sorted(str(key) for key in context.partition.keys)
    Path("trips_seen.txt").write_text("\\n".join(keys))


repo = rs.CodeRepository(
    assets=[trips],
    jobs=[rs.Job(name="trips_job", assets=[trips])],
    default_executor=rs.Executor.in_process(),
)
"""

TRIPS_HOURS = [
    f"2024-01-{day:02d}T{hour:02d}:00" for day in (1, 2) for hour in range(24)
]


def _queue_trips_backfill(store):
    """Queue the two runs of a backfill of `trips`, one for each zone, as a
    daemon does. Returns the id of the run of zone 132."""
    seed = rs.CodeRepository(
        assets=[
            rs.Asset(
                name="trips",
                partitions_def=rs.PartitionsDefinition.multi(
                    {
                        "hour": rs.PartitionsDefinition.hourly(
                            start=datetime(2024, 1, 1), end=datetime(2024, 1, 3)
                        ),
                        "zone": rs.PartitionsDefinition.static_(["132", "7"]),
                    }
                ),
            )(lambda context: None)
        ],
        run_queue=rs.RunQueueConfig(max_concurrent_runs=2, dequeue_interval="50ms"),
    )
    seed.resolve(storage=store)
    launched = seed.backfill(
        selection=["trips"],
        partition_range=rs.PartitionKeyRange.multi(
            {"hour": (TRIPS_HOURS[0], TRIPS_HOURS[-1])}
        ),
        strategy=rs.BackfillStrategy.per_dimension(
            multi_run=["zone"], single_run=["hour"]
        ),
        block=False,
    )
    seed.execute_backfill_queued(launched.backfill_id)

    span = f"hour={TRIPS_HOURS[0]}..{TRIPS_HOURS[-1]}|zone=132"
    (run_id,) = [
        run.run_id
        for run in store.get_queued_runs()
        if store._stored_keys(run.run_id)[0] == span
    ]
    return run_id


def _assert_zone_132_ran(resolved_tmp_path, store, hours=TRIPS_HOURS):
    members = [f"hour={hour}|zone=132" for hour in hours]
    assert (resolved_tmp_path / "trips_seen.txt").read_text().split("\n") == members
    assert sorted(str(key) for key in store.get_materialized_partitions("trips")) == (
        members
    )


def test_execute_takes_the_partition_key_from_the_run_record(
    resolved_tmp_path, monkeypatch
):
    """The key of a run of half a year of hours does not fit in the arguments
    of a pod, so the pod gets none: the run record names the partitions."""
    (resolved_tmp_path / "defs_trips.py").write_text(TRIPS_REPO_MODULE)
    store = _cloud_env(monkeypatch, resolved_tmp_path / "storage_trips")
    run_id = _queue_trips_backfill(store)

    result = runner.invoke(
        app,
        [
            "execute",
            "defs_trips",
            "--run-id",
            run_id,
            "--target",
            "trips",
            "--surreal-endpoint",
            "ws://unused",
        ],
    )

    assert result.exit_code == 0, result.output
    _assert_zone_132_ran(resolved_tmp_path, store)
    outcome = store.kv_get(f"run_outcome:{run_id}")
    assert outcome is not None and b"Success" in outcome


def test_execute_job_takes_the_partition_key_from_the_run_record(
    resolved_tmp_path, monkeypatch
):
    (resolved_tmp_path / "defs_trips_job.py").write_text(TRIPS_REPO_MODULE)
    store = _cloud_env(monkeypatch, resolved_tmp_path / "storage_trips_job")
    hours = TRIPS_HOURS[:6]
    store._create_run(
        "pod-trips-job",
        "trips_job",
        "NotStarted",
        time.time_ns(),
        node_names=["trips"],
        partition_key=rs.PartitionKey.multi({"hour": hours, "zone": "132"}),
    )

    result = runner.invoke(
        app,
        [
            "execute",
            "defs_trips_job",
            "--run-id",
            "pod-trips-job",
            "--job",
            "trips_job",
            "--surreal-endpoint",
            "ws://unused",
        ],
    )

    assert result.exit_code == 0, result.output
    _assert_zone_132_ran(resolved_tmp_path, store, hours)


def test_execute_step_takes_the_partition_key_from_the_run_record(
    resolved_tmp_path, monkeypatch
):
    """A step pod runs one step of the run: the same record names its
    partitions."""
    (resolved_tmp_path / "defs_trips_step.py").write_text(TRIPS_REPO_MODULE)
    store = _cloud_env(monkeypatch, resolved_tmp_path / "storage_trips_step")
    monkeypatch.setenv("RIVERS_SURREAL_ENDPOINT", "ws://unused")
    run_id = _queue_trips_backfill(store)

    result = runner.invoke(
        app,
        ["execute-step", "defs_trips_step", "--step-key", "trips", "--run-id", run_id],
    )

    assert result.exit_code == 0, result.output
    _assert_zone_132_ran(resolved_tmp_path, store)


@pytest.mark.parametrize("command", ["execute", "execute-step"])
def test_a_partition_key_argument_still_names_the_partitions(
    resolved_tmp_path, monkeypatch, command
):
    """A Run object that an older code location wrote holds the key, and the
    operator passes it on."""
    module = f"defs_trips_arg_{command.replace('-', '_')}"
    (resolved_tmp_path / f"{module}.py").write_text(TRIPS_REPO_MODULE)
    store = _cloud_env(monkeypatch, resolved_tmp_path / "storage_trips_arg")
    monkeypatch.setenv("RIVERS_SURREAL_ENDPOINT", "ws://unused")
    hours = TRIPS_HOURS[:6]
    key = rs.PartitionKey.multi({"hour": hours, "zone": "132"})
    store._create_run(
        "pod-trips-arg", "", "Queued", 1, node_names=["trips"], partition_key=key
    )
    args = {
        "execute": ["--target", "trips", "--surreal-endpoint", "ws://unused"],
        "execute-step": ["--step-key", "trips"],
    }[command]

    result = runner.invoke(
        app,
        [command, module, "--run-id", "pod-trips-arg", *args]
        + ["--partition-key", key.to_json()],
    )

    assert result.exit_code == 0, result.output
    _assert_zone_132_ran(resolved_tmp_path, store, hours)


@pytest.mark.parametrize("with_argument", [False, True])
def test_the_failure_of_a_run_names_the_partitions_that_ran(
    resolved_tmp_path, monkeypatch, with_argument
):
    """The failure holds the key of the run record, as the record holds it.
    With a key in the arguments, that key ran."""
    module = f"defs_trips_fail_{int(with_argument)}"
    (resolved_tmp_path / f"{module}.py").write_text(TRIPS_REPO_MODULE)
    (resolved_tmp_path / "trips_fail").touch()
    store = _cloud_env(monkeypatch, resolved_tmp_path / "storage_trips_fail")
    run_id = _queue_trips_backfill(store)
    args = []
    failed = f"hour={TRIPS_HOURS[0]}..{TRIPS_HOURS[-1]}|zone=132"
    if with_argument:
        hours = TRIPS_HOURS[:6]
        key = rs.PartitionKey.multi({"hour": hours, "zone": "132"})
        args = ["--partition-key", key.to_json()]
        failed = f"hour={','.join(hours)}|zone=132"

    result = runner.invoke(
        app,
        ["execute", module, "--run-id", run_id, "--target", "trips"]
        + ["--surreal-endpoint", "ws://unused", *args],
    )

    assert result.exit_code == 0, result.output
    outcome = store.kv_get(f"run_outcome:{run_id}")
    assert outcome is not None and b"Failure" in outcome
    failures = [
        key for kind, key in store._stored_keys(run_id)[1] if kind == "StepFailure"
    ]
    assert failures == [failed]
