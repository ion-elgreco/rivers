"""`rivers execute` — the K8s run-pod entry point.

The verb never rides the pod args: the command recovers it from the run
record, which makes the record read load-bearing. The default suite otherwise
never exercises this command (`norecursedirs` excludes the only integration
test that does).
"""

import rivers as rs
from rivers._core.storage import Storage
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
    store = Storage.embedded(str(path))
    monkeypatch.setattr(
        "rivers.cli.Storage",
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
