"""The run keys of a backfill hold a row of time windows as a span.

A span is a form of storage: the run record and the events of the run hold
``first..last`` in place of each window. User code and the Python API get
every window, as before.
"""

import asyncio
import contextlib
import time
from datetime import datetime, timedelta

import grpc
import pytest
import rivers as rs
from rivers._core import AutomationDaemon

START = datetime(2024, 1, 1)
ZONES = ["132", "7"]
GRIDS = ["cron", "interval"]


def _hour_keys(grid, count=48):
    fmt = "%Y-%m-%dT%H:00" if grid == "cron" else "%Y-%m-%dT%H:%M:%S"
    return [(START + timedelta(hours=hour)).strftime(fmt) for hour in range(count)]


def _hours_def(grid):
    end = START + timedelta(hours=48)
    if grid == "cron":
        return rs.PartitionsDefinition.hourly(start=START, end=end)
    return rs.PartitionsDefinition.time_window(
        start=START, interval_seconds=3600, end=end
    )


def _span(hours):
    return f"{hours[0]}..{hours[-1]}"


def _trips_def(grid):
    return rs.PartitionsDefinition.multi(
        {"hour": _hours_def(grid), "zone": rs.PartitionsDefinition.static_(ZONES)}
    )


def _note_batch(context, seen):
    keys = context.partition.keys
    zones = sorted({key.keys["zone"][0] for key in keys})
    seen.append((zones, sorted(key.keys["hour"][0] for key in keys)))


def _trips(seen, grid="cron", is_async=False, error=None):
    """One asset over hour x zone. It notes the keys of each call in `seen`."""
    per_zone = rs.BackfillStrategy.per_dimension(
        multi_run=["zone"], single_run=["hour"]
    )

    if is_async:

        @rs.Asset(
            name="trips", partitions_def=_trips_def(grid), backfill_strategy=per_zone
        )
        async def trips(context: rs.AssetExecutionContext) -> None:
            await asyncio.sleep(0)
            _note_batch(context, seen)
            if error:
                raise ValueError(error)
    else:

        @rs.Asset(
            name="trips", partitions_def=_trips_def(grid), backfill_strategy=per_zone
        )
        def trips(context: rs.AssetExecutionContext) -> None:
            _note_batch(context, seen)
            if error:
                raise ValueError(error)

    return trips


def _all_hours(grid):
    hours = _hour_keys(grid)
    return rs.PartitionKeyRange.multi({"hour": (hours[0], hours[-1])})


def _zone_key(hours, zone):
    return rs.PartitionKey.multi({"hour": hours, "zone": zone})


@pytest.mark.parametrize("grid", GRIDS)
def test_blocking_backfill_stores_each_run_key_as_a_span(executor_env, is_async, grid):
    executor, _ = executor_env
    hours = _hour_keys(grid)
    seen = []
    repo = rs.CodeRepository(
        assets=[_trips(seen, grid, is_async)], default_executor=executor
    )

    result = repo.backfill(selection=["trips"], partition_range=_all_hours(grid))

    assert (result.status, result.completed, result.failed) == (
        "CompletedSuccess",
        96,
        0,
    )
    assert len(result.run_ids) == 2
    storage = repo.storage
    assert sorted(storage._stored_keys(run_id)[0] for run_id in result.run_ids) == [
        f"hour={_span(hours)}|zone=132",
        f"hour={_span(hours)}|zone=7",
    ]

    # User code and the Python API get every hour.
    assert sorted(seen) == [(["132"], hours), (["7"], hours)]
    assert {storage.get_run(run_id).partition_key for run_id in result.run_ids} == {
        _zone_key(hours, zone) for zone in ZONES
    }
    assert set(storage.get_materialized_partitions("trips")) == {
        _zone_key(hour, zone) for hour in hours for zone in ZONES
    }


@pytest.mark.parametrize("grid", GRIDS)
def test_single_run_backfill_of_one_time_dimension_stores_one_span(grid):
    hours = _hour_keys(grid)
    seen = []

    @rs.Asset(
        name="hourly",
        partitions_def=_hours_def(grid),
        backfill_strategy=rs.BackfillStrategy.single_run(),
    )
    def hourly(context: rs.AssetExecutionContext) -> None:
        seen.append(sorted(key.key[0] for key in context.partition.keys))

    repo = rs.CodeRepository(assets=[hourly], default_executor=rs.Executor.in_process())

    result = repo.backfill(
        selection=["hourly"],
        partition_range=rs.PartitionKeyRange.single(hours[0], hours[-1]),
    )

    assert (result.completed, len(result.run_ids)) == (48, 1)
    (run_id,) = result.run_ids
    assert repo.storage._stored_keys(run_id)[0] == _span(hours)
    assert repo.storage.get_run(run_id).partition_key == rs.PartitionKey.single(hours)
    assert seen == [hours]


def test_hours_apart_stay_in_one_run_with_a_span_for_each_row():
    hours = _hour_keys("cron")
    wanted = hours[:12] + hours[13:21] + [hours[30]]
    seen = []
    repo = rs.CodeRepository(
        assets=[_trips(seen)], default_executor=rs.Executor.in_process()
    )

    result = repo.backfill(
        selection=["trips"], partition_keys=[_zone_key(wanted, "132")]
    )

    assert (result.completed, len(result.run_ids)) == (21, 1)
    (run_id,) = result.run_ids
    assert repo.storage._stored_keys(run_id)[0] == (
        f"hour={_span(hours[:12])},{_span(hours[13:21])},{hours[30]}|zone=132"
    )
    assert seen == [(["132"], wanted)]


def test_a_run_of_one_partition_keeps_its_key():
    hours = _hour_keys("cron")
    seen = []
    repo = rs.CodeRepository(
        assets=[_trips(seen)], default_executor=rs.Executor.in_process()
    )

    result = repo.backfill(
        selection=["trips"],
        partition_keys=[_zone_key(hours[:4], "132")],
        strategy=rs.BackfillStrategy.multi_run(),
    )

    assert (result.completed, len(result.run_ids)) == (4, 4)
    assert sorted(
        repo.storage._stored_keys(run_id)[0] for run_id in result.run_ids
    ) == [f"hour={hour}|zone=132" for hour in hours[:4]]


def test_a_batched_run_that_a_user_starts_keeps_its_key_as_given():
    hours = _hour_keys("cron")[:6]
    seen = []
    repo = rs.CodeRepository(
        assets=[_trips(seen)], default_executor=rs.Executor.in_process()
    )

    result = repo.materialize(
        selection=["trips"], partition_key=_zone_key(hours, "132")
    )

    assert result.success
    assert repo.storage._stored_keys(result.run_id)[0] == (
        f"hour={','.join(hours)}|zone=132"
    )
    assert seen == [(["132"], hours)]


@pytest.mark.parametrize("grid", GRIDS)
def test_failed_run_of_a_backfill_names_its_partitions_in_one_span(
    executor_env, is_async, grid
):
    executor, _ = executor_env
    hours = _hour_keys(grid)
    seen = []
    repo = rs.CodeRepository(
        assets=[_trips(seen, grid, is_async, error="boom")], default_executor=executor
    )

    result = repo.backfill(selection=["trips"], partition_range=_all_hours(grid))

    assert (result.status, result.completed, result.failed) == (
        "CompletedFailed",
        0,
        96,
    )
    storage = repo.storage
    stored = sorted(storage._stored_keys(run_id) for run_id in result.run_ids)
    assert stored == [
        (
            f"hour={_span(hours)}|zone={zone}",
            [("StepFailure", f"hour={_span(hours)}|zone={zone}")],
        )
        for zone in ZONES
    ]

    # The Python API gives every partition of the failure.
    failures = [
        event
        for event in storage.get_events_for_asset("trips")
        if event.event_type == "StepFailure" and event.partition_key is not None
    ]
    assert {event.partition_key for event in failures} == {
        _zone_key(hours, zone) for zone in ZONES
    }
    assert all("boom" in dict(event.metadata)["error"] for event in failures)
    assert storage.get_materialized_partitions("trips") == []


def test_failed_batched_run_that_a_user_starts_keeps_its_key_in_the_failure():
    hours = _hour_keys("cron")[:6]
    repo = rs.CodeRepository(
        assets=[_trips([], error="boom")], default_executor=rs.Executor.in_process()
    )

    result = repo.materialize(
        selection=["trips"],
        partition_key=_zone_key(hours, "132"),
        raise_on_error=False,
    )

    assert not result.success
    key = f"hour={','.join(hours)}|zone=132"
    assert repo.storage._stored_keys(result.run_id) == (key, [("StepFailure", key)])


def _run_queue_until_done(repo, storage, backfill_id):
    """Run the queued runs of a backfill with a daemon. Returns the final status."""
    daemon = AutomationDaemon(repo=repo, storage=storage, condition_eval_interval="10s")
    daemon.start()
    try:
        deadline = time.monotonic() + 60
        status = repo.get_backfill(backfill_id)
        while status.status == "InProgress" and time.monotonic() < deadline:
            time.sleep(0.1)
            status = repo.get_backfill(backfill_id)
    finally:
        daemon.stop()
    return status


def _queue_repo(storage, assets, **kwargs):
    repo = rs.CodeRepository(
        assets=assets,
        default_executor=rs.Executor.in_process(),
        run_queue=rs.RunQueueConfig(max_concurrent_runs=4, dequeue_interval="50ms"),
        **kwargs,
    )
    repo.resolve(storage=storage)
    return repo


@pytest.mark.parametrize("grid", GRIDS)
def test_queued_backfill_stores_each_run_key_as_a_span(storage, grid):
    hours = _hour_keys(grid)
    seen = []
    repo = _queue_repo(storage, [_trips(seen, grid)])

    launched = repo.backfill(
        selection=["trips"], partition_range=_all_hours(grid), block=False
    )
    repo.execute_backfill_queued(launched.backfill_id)

    run_ids = repo.get_backfill(launched.backfill_id).run_ids
    assert len(run_ids) == 2
    spans = [f"hour={_span(hours)}|zone={zone}" for zone in ZONES]
    assert sorted(storage._stored_keys(run_id) for run_id in run_ids) == [
        (span, [("RunQueued", span)]) for span in spans
    ]
    assert {run.partition_key for run in storage.get_queued_runs()} == {
        _zone_key(hours, zone) for zone in ZONES
    }

    status = _run_queue_until_done(repo, storage, launched.backfill_id)

    assert (status.status, status.completed_partitions) == ("CompletedSuccess", 96)
    assert sorted(seen) == [(["132"], hours), (["7"], hours)]
    assert sorted(storage._stored_keys(run_id)[0] for run_id in run_ids) == spans


@contextlib.contextmanager
def _serving(repo, grpc_stubs):
    port = repo._start_grpc_server("127.0.0.1", 0)
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    grpc.channel_ready_future(channel).result(timeout=5)
    pb2, pb2_grpc = grpc_stubs
    try:
        yield pb2, pb2_grpc.CodeLocationServiceStub(channel)
    finally:
        channel.close()
        repo._stop_grpc_server()


def _launch_job_backfill(repo, grpc_stubs, hours):
    with _serving(repo, grpc_stubs) as (pb2, stub):
        dimensions = [
            pb2.MultiPartitionDimension(name="hour", keys=hours),
            pb2.MultiPartitionDimension(name="zone", keys=ZONES),
        ]
        key = pb2.ProtoPartitionKey(multi=pb2.MultiPartitionKey(dimensions=dimensions))
        return stub.LaunchBackfill(
            pb2.LaunchBackfillRequest(
                job_name="trips_job",
                partition_keys=[key],
                failure_policy="continue",
                max_concurrency=2,
            )
        ).backfill_id


@pytest.mark.parametrize("pickup", ["execute_backfill", "execute_backfill_queued"])
def test_job_backfill_stores_each_run_key_as_a_span(grpc_stubs, storage, pickup):
    hours = _hour_keys("cron")
    seen = []
    trips = _trips(seen)
    jobs = [rs.Job(name="trips_job", assets=[trips])]
    if pickup == "execute_backfill_queued":
        repo = _queue_repo(storage, [trips], jobs=jobs)
    else:
        repo = rs.CodeRepository(
            assets=[trips], jobs=jobs, default_executor=rs.Executor.in_process()
        )
        repo.resolve(storage=storage)
    backfill_id = _launch_job_backfill(repo, grpc_stubs, hours)

    getattr(repo, pickup)(backfill_id)

    run_ids = repo.get_backfill(backfill_id).run_ids
    assert len(run_ids) == 2
    assert sorted(storage._stored_keys(run_id)[0] for run_id in run_ids) == [
        f"hour={_span(hours)}|zone=132",
        f"hour={_span(hours)}|zone=7",
    ]
    assert {storage.get_run(run_id).job_name for run_id in run_ids} == {"trips_job"}

    status = repo.get_backfill(backfill_id)
    if pickup == "execute_backfill_queued":
        status = _run_queue_until_done(repo, storage, backfill_id)

    assert (status.status, status.completed_partitions) == ("CompletedSuccess", 96)
    assert sorted(seen) == [(["132"], hours), (["7"], hours)]


@pytest.mark.parametrize("pickup", ["execute_backfill", "execute_backfill_queued"])
def test_failed_job_run_of_a_backfill_names_its_partitions_in_one_span(
    grpc_stubs, storage, pickup
):
    hours = _hour_keys("cron")
    trips = _trips([], error="boom")
    jobs = [rs.Job(name="trips_job", assets=[trips])]
    if pickup == "execute_backfill_queued":
        repo = _queue_repo(storage, [trips], jobs=jobs)
    else:
        repo = rs.CodeRepository(
            assets=[trips], jobs=jobs, default_executor=rs.Executor.in_process()
        )
        repo.resolve(storage=storage)
    backfill_id = _launch_job_backfill(repo, grpc_stubs, hours)

    getattr(repo, pickup)(backfill_id)
    run_ids = repo.get_backfill(backfill_id).run_ids

    status = repo.get_backfill(backfill_id)
    if pickup == "execute_backfill_queued":
        status = _run_queue_until_done(repo, storage, backfill_id)

    assert (status.status, status.failed_partitions) == ("CompletedFailed", 96)
    for run_id in run_ids:
        run_key, event_keys = storage._stored_keys(run_id)
        assert run_key in {f"hour={_span(hours)}|zone={zone}" for zone in ZONES}
        assert [key for kind, key in event_keys if kind == "StepFailure"] == [run_key]
