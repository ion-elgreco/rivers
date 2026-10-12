"""A resource's secrets reach code in another process with their values.

A loky worker (parallel executor) and a subprocess-evaluated sensor or
schedule rebuild the resource from its fields. Its secrets arrive as the
real values, still ``SecretStr``/``SecretBytes``, so their reprs stay masked
and the values never reach stored events or logs.
"""

import os
import pickle

import obstore.store
import pytest

import rivers as rs
from rivers._core import AutomationDaemon

from _helpers import VAULT_SECRETS, VAULT_VALUES, Vault, make_vault, reveal
from _polling import wait_until


class Sink(rs.Resource):
    path: str


def _reveal(vault: Vault) -> dict:
    """What `vault` holds, secrets revealed, and the process that saw it."""
    return {"pid": os.getpid(), **reveal(vault)}


def _asset(name: str, handler: rs.BaseIOHandler, is_async: bool):
    """An asset that prints `vault` (masked) and returns what it holds."""
    if is_async:

        async def body(vault: Vault) -> dict:
            print(vault)
            return _reveal(vault)

    else:

        def body(vault: Vault) -> dict:
            print(vault)
            return _reveal(vault)

    return rs.Asset(name=name, io_handler=handler)(body)


def _executor(kind: str):
    if kind == "parallel":
        return rs.Executor.parallel(max_workers=2)
    return rs.Executor.in_process()


def _stored_text(storage, run_id: str) -> str:
    """Everything the run's events and captured logs hold."""
    events = [
        f"{e.event_type} {e.asset_key} {e.metadata} {e.data_version}"
        for e in storage.get_events_for_run(run_id)
    ]
    logs = [
        f"{log.stdout} {log.stderr} {log.logs} {log.traceback}"
        for log in storage.get_run_logs(run_id)
    ]
    return "\n".join(events + logs)


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("executor", ["in_process", "parallel"])
def test_secrets_reach_the_asset_body(storage, tmp_path, executor, is_async):
    """Two steps, so the parallel executor sends sync ones to loky workers;
    it runs async steps in this process."""
    import rivers._capture as cap

    cap._installed = False  # capture this test's stdout, which pytest swapped

    handler = rs.PickleIOHandler(
        store=obstore.store.LocalStore(str(tmp_path), mkdir=True)
    )
    repo = rs.CodeRepository(
        assets=[_asset(n, handler, is_async) for n in ("left", "right")],
        resources={"vault": make_vault()},
        default_executor=_executor(executor),
    )
    repo.resolve(storage=storage)
    result = repo.materialize()
    assert result.success

    in_worker = executor == "parallel" and not is_async
    for name in ("left", "right"):
        seen = repo.load_node(name)
        assert (seen.pop("pid") != os.getpid()) == in_worker
        assert seen == VAULT_VALUES

    stored = _stored_text(storage, result.run_id)
    assert stored.count("token=SecretStr('**********')") == 2, stored
    assert [s for s in VAULT_SECRETS if s in stored] == []


def test_a_runs_rebuilt_resource_keeps_its_secrets_in_workers(storage, tmp_path):
    """A launch document that sets one field rebuilds the resource with every
    other value, secrets included, and the workers get that copy."""
    handler = rs.PickleIOHandler(
        store=obstore.store.LocalStore(str(tmp_path), mkdir=True)
    )
    repo = rs.CodeRepository(
        assets=[_asset(n, handler, False) for n in ("left", "right")],
        resources={"vault": make_vault()},
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    repo.resolve(storage=storage)
    document = {"resources": {"vault": {"region": "us"}}}
    result = repo.materialize(config=document)
    assert result.success

    for name in ("left", "right"):
        seen = repo.load_node(name)
        assert seen.pop("pid") != os.getpid()
        assert seen == {**VAULT_VALUES, "region": "us"}
    assert storage.get_run(result.run_id).config == document


def _record(sink: Sink, vault: Vault) -> None:
    """Write what `vault` holds for the test to read; the rename keeps a
    reader from seeing half a file."""
    partial = f"{sink.path}.partial"
    with open(partial, "wb") as fh:
        pickle.dump(_reveal(vault), fh)
    os.replace(partial, sink.path)


def _sensor_fn(context: rs.SensorEvaluationContext, vault: Vault, sink: Sink):
    _record(sink, vault)
    return rs.SkipReason("recorded")


def _schedule_fn(context: rs.ScheduleEvaluationContext, vault: Vault, sink: Sink):
    _record(sink, vault)
    return rs.SkipReason("recorded")


@pytest.mark.parametrize("kind", ["sensor", "schedule"])
def test_secrets_reach_a_subprocess_evaluation(storage, tmp_path, kind):
    seen_file = tmp_path / "seen.pkl"

    @rs.Asset
    def a() -> int:
        return 1

    if kind == "sensor":
        sensors = [
            rs.Sensor(
                _sensor_fn,
                name="watch",
                asset_selection=["a"],
                minimum_interval="1s",
                default_status=rs.SensorStatus.Running,
                eval_mode=rs.EvalMode.Subprocess,
            )
        ]
        schedules = []
    else:
        sensors = []
        schedules = [
            rs.Schedule(
                _schedule_fn,
                name="watch",
                cron_schedule="* * * * * *",
                job_name="j",
                default_status=rs.ScheduleStatus.Running,
                eval_mode=rs.EvalMode.Subprocess,
            )
        ]
    repo = rs.CodeRepository(
        assets=[a],
        jobs=[rs.Job(name="j", assets=[a])],
        sensors=sensors,
        schedules=schedules,
        resources={"vault": make_vault(), "sink": Sink(path=str(seen_file))},
    )
    repo.resolve(storage=storage)

    daemon = AutomationDaemon(repo=repo, storage=storage)
    daemon.start()
    try:
        assert wait_until(seen_file.exists, timeout=20), storage.get_ticks(
            "watch", limit=5
        )
    finally:
        daemon.stop()

    with open(seen_file, "rb") as fh:
        seen = pickle.load(fh)
    assert seen.pop("pid") != os.getpid()
    assert seen == VAULT_VALUES
