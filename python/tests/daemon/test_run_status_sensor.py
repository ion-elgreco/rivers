"""Run-status sensors evaluated by the AutomationDaemon.

Each sensor function writes one JSON line per call through the ``Recorder``
resource, so the same check works in-process and in a loky subprocess.
"""

import asyncio
import json
import os
import time
from contextlib import contextmanager

import pytest
from pydantic import BaseModel

import rivers as rs
from rivers._core import AutomationDaemon

from _polling import wait_for_ticks, wait_until

EVAL_MODES = ["sync", "async", "subprocess"]


class Recorder(rs.Resource):
    out_file: str

    def write(self, context: rs.RunStatusSensorContext) -> None:
        line = {
            "sensor": context.sensor_name,
            "run_id": context.run.run_id,
            "status": context.run.status,
            "job": context.run.job_name,
            "failures": [[f.asset_name, f.error] for f in context.step_failures],
            "launch_error": context.launch_error,
        }
        with open(self.out_file, "a") as fh:
            fh.write(json.dumps(line) + "\n")

    def lines(self) -> list[dict]:
        if not os.path.exists(self.out_file):
            return []
        with open(self.out_file) as fh:
            return [json.loads(line) for line in fh if line.strip()]


def _record(context: rs.RunStatusSensorContext, recorder: Recorder) -> None:
    recorder.write(context)


async def _record_async(context: rs.RunStatusSensorContext, recorder: Recorder) -> None:
    await asyncio.sleep(0)
    recorder.write(context)


def _record_unless_tagged(
    context: rs.RunStatusSensorContext, recorder: Recorder
) -> None:
    if ("explode", "1") in context.run.tags:
        raise RuntimeError("sensor function exploded")
    recorder.write(context)


def _return_text(context: rs.RunStatusSensorContext) -> str:
    return "not None"


class Threshold(BaseModel):
    limit: int = 3


def _record_with_config(
    context: rs.RunStatusSensorContext[Threshold], recorder: Recorder
) -> None:
    assert context.config.limit == 3
    recorder.write(context)


def _sensor(
    mode: str = "sync",
    fn=None,
    *,
    status=rs.RunStatus.Failure,
    name: str = "watch",
    minimum_interval: str = "1s",
    **kwargs,
) -> rs.Sensor:
    fn = fn or (_record_async if mode == "async" else _record)
    eval_mode = rs.EvalMode.Subprocess if mode == "subprocess" else rs.EvalMode.Auto
    return rs.Sensor.run_status(
        status,
        name=name,
        minimum_interval=minimum_interval,
        default_status=rs.SensorStatus.Running,
        eval_mode=eval_mode,
        **kwargs,
    )(fn)


def _ticks(storage, status: str | None = None):
    ticks = storage.get_ticks("watch", limit=100)
    return [t for t in ticks if status is None or t.status == status]


@contextmanager
def _daemon(repo, storage, *, fresh: bool = True):
    """Run a daemon. With ``fresh``, wait for the tick that starts watching,
    so runs made inside the block end after the sensor's start time."""
    daemon = AutomationDaemon(repo=repo, storage=storage)
    daemon.start()
    try:
        if fresh:
            assert wait_until(
                lambda: any(
                    (t.skip_reason or "").startswith("Watching for")
                    for t in _ticks(storage)
                )
            ), f"no start tick: {_ticks(storage)}"
        yield daemon
    finally:
        daemon.stop()


def _wait_more_ticks(storage, count: int = 2) -> None:
    seen = len(_ticks(storage))
    wait_for_ticks(storage, "watch", min_count=seen + count, timeout=15)


def _repo(storage, sensor, recorder=None, io_factory=None, **kwargs):
    """A resolved repo with a failing ``boom`` and a succeeding ``fine`` asset."""
    handler = {"io_handler": io_factory()} if io_factory else {}

    @rs.Asset(name="boom", **handler)
    def boom() -> int:
        raise ValueError("boom went the asset")

    @rs.Asset(name="fine", **handler)
    def fine() -> int:
        return 1

    jobs = [rs.Job(name=name, assets=[boom]) for name in kwargs.pop("job_names", [])]
    repo = rs.CodeRepository(
        assets=[boom, fine],
        jobs=jobs or None,
        sensors=[sensor],
        resources={"recorder": recorder} if recorder else None,
        **kwargs,
    )
    repo.resolve(storage=storage)
    return repo


@pytest.fixture
def recorder(tmp_path):
    return Recorder(out_file=str(tmp_path / "calls.jsonl"))


@pytest.mark.parametrize("mode", EVAL_MODES)
def test_failed_run_calls_function_once(storage, executor_env, recorder, mode):
    executor, io_factory = executor_env
    repo = _repo(
        storage, _sensor(mode), recorder, io_factory, default_executor=executor
    )

    with _daemon(repo, storage):
        assert repo.materialize(["fine"]).success
        failed = repo.materialize(["boom"], raise_on_error=False)
        assert not failed.success
        assert wait_until(lambda: recorder.lines(), timeout=15)
        _wait_more_ticks(storage)

    [line] = recorder.lines()
    [[asset, error]] = line.pop("failures")
    assert (asset, "boom went the asset" in error) == ("boom", True), error
    assert line == {
        "sensor": "watch",
        "run_id": failed.run_id,
        "status": "Failure",
        "job": None,
        "launch_error": None,
    }
    [handled] = _ticks(storage, "Success")
    assert handled.run_ids == []
    assert not _ticks(storage, "Failed")


def test_success_sensor_ignores_failures(storage, recorder):
    repo = _repo(storage, _sensor(status=rs.RunStatus.Success), recorder)

    with _daemon(repo, storage):
        assert not repo.materialize(["boom"], raise_on_error=False).success
        ok = repo.materialize(["fine"])
        assert wait_until(lambda: recorder.lines(), timeout=15)
        _wait_more_ticks(storage)

    assert [
        (line["run_id"], line["status"], line["failures"]) for line in recorder.lines()
    ] == [(ok.run_id, "Success", [])]


def test_canceled_sensor_sees_canceled_run(storage, recorder):
    repo = _repo(storage, _sensor(status=rs.RunStatus.Canceled), recorder)

    with _daemon(repo, storage):
        storage._create_run("queued-run", "j", "Queued", time.time_ns())
        assert storage.cancel_queued_run("queued-run")
        assert not repo.materialize(["boom"], raise_on_error=False).success
        assert wait_until(lambda: recorder.lines(), timeout=15)
        _wait_more_ticks(storage)

    assert [
        (line["run_id"], line["status"], line["job"]) for line in recorder.lines()
    ] == [("queued-run", "Canceled", "j")]


def test_runs_before_start_are_not_reported(storage, recorder):
    repo = _repo(storage, _sensor(), recorder)
    early = repo.materialize(["boom"], raise_on_error=False)

    with _daemon(repo, storage):
        _wait_more_ticks(storage)
        assert recorder.lines() == []
        late = repo.materialize(["boom"], raise_on_error=False)
        assert wait_until(lambda: recorder.lines(), timeout=15)

    assert early.run_id != late.run_id
    assert [line["run_id"] for line in recorder.lines()] == [late.run_id]


def test_monitored_jobs_filters_runs(storage, recorder):
    repo = _repo(
        storage,
        _sensor(monitored_jobs=["watched"]),
        recorder,
        job_names=["watched", "other"],
    )

    with _daemon(repo, storage):
        assert not repo.get_job("other").execute(raise_on_error=False).success
        assert not repo.materialize(["boom"], raise_on_error=False).success
        watched = repo.get_job("watched").execute(raise_on_error=False)
        assert wait_until(lambda: recorder.lines(), timeout=15)
        _wait_more_ticks(storage)

    assert [(line["run_id"], line["job"]) for line in recorder.lines()] == [
        (watched.run_id, "watched")
    ]


def test_launch_failure_reports_launch_error(storage, recorder):
    repo = _repo(
        storage,
        _sensor(),
        recorder,
        default_executor=rs.Executor.in_process(),
        run_queue=rs.RunQueueConfig(dequeue_interval="100ms", start_timeout="1s"),
    )

    with _daemon(repo, storage):
        # No RunDequeued event and an ancient start time: the start-timeout
        # sweep (5s cadence) fails it with a RunLaunchFailed event.
        storage._create_run("stuck-run", "j", "NotStarted", 1_000)
        assert wait_until(lambda: recorder.lines(), timeout=20)

    [line] = recorder.lines()
    assert (line["run_id"], line["status"], line["failures"]) == (
        "stuck-run",
        "Failure",
        [],
    )
    assert "no executor appeared" in line["launch_error"]


def test_more_than_five_runs_drain_over_ticks(storage, recorder):
    repo = _repo(storage, _sensor(), recorder)
    with _daemon(repo, storage):
        pass
    failed = [repo.materialize(["boom"], raise_on_error=False).run_id for _ in range(7)]

    # A slow interval leaves time to read the first handling tick alone.
    repo = _repo(storage, _sensor(minimum_interval="3s"), recorder)
    with _daemon(repo, storage, fresh=False):
        assert wait_until(lambda: _ticks(storage, "Success"))
        first_tick = [line["run_id"] for line in recorder.lines()]
        assert wait_until(lambda: len(recorder.lines()) >= 7, timeout=15)
        _wait_more_ticks(storage, 1)

    assert first_tick == failed[:5]
    assert [line["run_id"] for line in recorder.lines()] == failed


def test_function_error_fails_tick_and_moves_on(storage, recorder):
    repo = _repo(storage, _sensor(fn=_record_unless_tagged), recorder)

    with _daemon(repo, storage):
        exploding = repo.materialize(
            ["boom"], tags=[("explode", "1")], raise_on_error=False
        )
        assert wait_until(lambda: _ticks(storage, "Failed"), timeout=15)
        later = repo.materialize(["boom"], raise_on_error=False)
        assert wait_until(lambda: recorder.lines(), timeout=15)
        _wait_more_ticks(storage)

    [failed_tick] = _ticks(storage, "Failed")
    assert (
        failed_tick.error
        == f"run {exploding.run_id}: RuntimeError: sensor function exploded"
    )
    assert [line["run_id"] for line in recorder.lines()] == [later.run_id]


def test_non_none_return_is_an_error(storage):
    repo = _repo(storage, _sensor(fn=_return_text))

    with _daemon(repo, storage):
        failed = repo.materialize(["boom"], raise_on_error=False)
        assert wait_until(lambda: _ticks(storage, "Failed"), timeout=15)

    [tick] = _ticks(storage, "Failed")
    assert tick.error == (
        f"run {failed.run_id}: ExecutionError: a run-status sensor function must return None; got str"
    )


def test_cursor_survives_restart(storage, recorder):
    repo = _repo(storage, _sensor(), recorder)

    with _daemon(repo, storage):
        first = repo.materialize(["boom"], raise_on_error=False)
        assert wait_until(lambda: recorder.lines(), timeout=15)
        _wait_more_ticks(storage, 1)
    second = repo.materialize(["boom"], raise_on_error=False)

    with _daemon(repo, storage, fresh=False):
        assert wait_until(lambda: len(recorder.lines()) >= 2, timeout=15)
        _wait_more_ticks(storage)

    assert [line["run_id"] for line in recorder.lines()] == [
        first.run_id,
        second.run_id,
    ]


@pytest.mark.parametrize("mode", ["sync", "subprocess"])
def test_config_and_resources_reach_the_function(storage, recorder, mode):
    repo = _repo(storage, _sensor(mode, fn=_record_with_config), recorder)

    with _daemon(repo, storage):
        failed = repo.materialize(["boom"], raise_on_error=False)
        assert wait_until(lambda: recorder.lines(), timeout=15)

    assert [line["run_id"] for line in recorder.lines()] == [failed.run_id]
    assert not _ticks(storage, "Failed")
