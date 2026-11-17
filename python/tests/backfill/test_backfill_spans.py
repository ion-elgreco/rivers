"""The run keys of a backfill hold a row of time windows as a span.

A span is a form of storage: the run record and the events of the run hold
``first..last`` in place of each window. User code and the Python API get
every window, as before.
"""

import asyncio
import contextlib
import importlib.util
import time
from datetime import datetime, timedelta
from typing import NamedTuple

import pytest
import rivers as rs
from rivers._core import AutomationDaemon

START = datetime(2024, 1, 1)
ZONES = ["132", "7"]

# The free-threaded venv has no grpcio.
needs_grpc = pytest.mark.skipif(
    importlib.util.find_spec("grpc") is None, reason="needs grpcio"
)

# The first and the last of the 48 hours of each grid, in the text of its keys.
GRIDS = pytest.mark.parametrize(
    ("grid", "first", "last"),
    [
        ("cron", "2024-01-01T00:00", "2024-01-02T23:00"),
        ("interval", "2024-01-01T00:00:00", "2024-01-02T23:00:00"),
    ],
)


class Call(NamedTuple):
    """The partitions that one call of the asset got."""

    zones: list[str]
    hours: list[str]


class Stored(NamedTuple):
    """The keys of a run as storage holds them: a span shows as `first..last`."""

    run_key: str
    # (type, key) of each event of the run that has a key.
    keyed_events: list[tuple[str, str]]


def _stored(storage, run_id):
    return Stored(*storage._stored_keys(run_id))


def _hours(first, last):
    """Every hour from `first` to `last`, in the text of `first`."""
    fmt = "%Y-%m-%dT%H:%M:%S" if first.count(":") == 2 else "%Y-%m-%dT%H:%M"
    start, end = datetime.fromisoformat(first), datetime.fromisoformat(last)
    count = (end - start) // timedelta(hours=1) + 1
    return [(start + timedelta(hours=hour)).strftime(fmt) for hour in range(count)]


def _hours_def(grid):
    end = START + timedelta(hours=48)
    if grid == "cron":
        return rs.PartitionsDefinition.hourly(start=START, end=end)
    return rs.PartitionsDefinition.time_window(
        start=START, interval_seconds=3600, end=end
    )


def _trips_def(grid):
    return rs.PartitionsDefinition.multi(
        {"hour": _hours_def(grid), "zone": rs.PartitionsDefinition.static_(ZONES)}
    )


def _note_call(context, calls):
    keys = context.partition.keys
    calls.append(
        Call(
            zones=sorted({key.keys["zone"][0] for key in keys}),
            hours=sorted(key.keys["hour"][0] for key in keys),
        )
    )


def _trips(calls, grid="cron", is_async=False, error=None):
    """One asset over hour x zone, one run per zone. Each call goes in `calls`."""
    per_zone = rs.BackfillStrategy.per_dimension(
        multi_run=["zone"], single_run=["hour"]
    )

    if is_async:

        @rs.Asset(
            name="trips", partitions_def=_trips_def(grid), backfill_strategy=per_zone
        )
        async def trips(context: rs.AssetExecutionContext) -> None:
            await asyncio.sleep(0)
            _note_call(context, calls)
            if error:
                raise ValueError(error)
    else:

        @rs.Asset(
            name="trips", partitions_def=_trips_def(grid), backfill_strategy=per_zone
        )
        def trips(context: rs.AssetExecutionContext) -> None:
            _note_call(context, calls)
            if error:
                raise ValueError(error)

    return trips


def _trips_key(hours, zone):
    """A key of the trips asset, as the Python API gives it."""
    return rs.PartitionKey.multi({"hour": hours, "zone": zone})


@GRIDS
def test_blocking_backfill_stores_each_run_key_as_a_span(
    executor_env, is_async, grid, first, last
):
    executor, _ = executor_env
    calls = []
    repo = rs.CodeRepository(
        assets=[_trips(calls, grid, is_async)], default_executor=executor
    )

    result = repo.backfill(
        selection=["trips"],
        partition_range=rs.PartitionKeyRange.multi({"hour": (first, last)}),
    )

    assert result.status == "CompletedSuccess"
    assert result.completed == 96  # 48 hours x 2 zones
    assert result.failed == 0

    # One run per zone. Its record holds the 48 hours as one span.
    storage = repo.storage
    assert sorted(_stored(storage, run_id).run_key for run_id in result.run_ids) == [
        f"hour={first}..{last}|zone=132",
        f"hour={first}..{last}|zone=7",
    ]

    # User code and the Python API get every hour.
    hours = _hours(first, last)
    assert sorted(calls) == [
        Call(zones=["132"], hours=hours),
        Call(zones=["7"], hours=hours),
    ]
    assert {storage.get_run(run_id).partition_key for run_id in result.run_ids} == {
        _trips_key(hours, "132"),
        _trips_key(hours, "7"),
    }
    assert set(storage.get_materialized_partitions("trips")) == {
        _trips_key(hour, zone) for hour in hours for zone in ["132", "7"]
    }


@GRIDS
def test_single_run_backfill_of_one_time_dimension_stores_one_span(grid, first, last):
    calls = []

    @rs.Asset(
        name="hourly",
        partitions_def=_hours_def(grid),
        backfill_strategy=rs.BackfillStrategy.single_run(),
    )
    def hourly(context: rs.AssetExecutionContext) -> None:
        calls.append(sorted(key.key[0] for key in context.partition.keys))

    repo = rs.CodeRepository(assets=[hourly], default_executor=rs.Executor.in_process())

    result = repo.backfill(
        selection=["hourly"], partition_range=rs.PartitionKeyRange.single(first, last)
    )

    assert result.completed == 48
    assert len(result.run_ids) == 1
    run_id = result.run_ids[0]

    # The record holds one span; the Python API and user code get every hour.
    assert _stored(repo.storage, run_id).run_key == f"{first}..{last}"
    hours = _hours(first, last)
    assert repo.storage.get_run(run_id).partition_key == rs.PartitionKey.single(hours)
    assert calls == [hours]


def test_hours_apart_stay_in_one_run_with_a_span_for_each_row():
    wanted = (
        _hours("2024-01-01T00:00", "2024-01-01T11:00")
        + _hours("2024-01-01T13:00", "2024-01-01T20:00")
        + ["2024-01-02T06:00"]
    )
    calls = []
    repo = rs.CodeRepository(
        assets=[_trips(calls)], default_executor=rs.Executor.in_process()
    )

    result = repo.backfill(
        selection=["trips"], partition_keys=[_trips_key(wanted, "132")]
    )

    assert result.completed == 21
    assert len(result.run_ids) == 1

    # One span for each row; the lone hour stays as it is.
    assert _stored(repo.storage, result.run_ids[0]).run_key == (
        "hour=2024-01-01T00:00..2024-01-01T11:00,"
        "2024-01-01T13:00..2024-01-01T20:00,"
        "2024-01-02T06:00|zone=132"
    )
    assert calls == [Call(zones=["132"], hours=wanted)]


def test_a_run_of_one_partition_keeps_its_key():
    repo = rs.CodeRepository(
        assets=[_trips([])], default_executor=rs.Executor.in_process()
    )

    result = repo.backfill(
        selection=["trips"],
        partition_keys=[
            _trips_key(_hours("2024-01-01T00:00", "2024-01-01T03:00"), "132")
        ],
        strategy=rs.BackfillStrategy.multi_run(),
    )

    assert result.completed == 4

    # One run per hour, each with its own hour and no span.
    run_keys = sorted(
        _stored(repo.storage, run_id).run_key for run_id in result.run_ids
    )
    assert run_keys == [
        "hour=2024-01-01T00:00|zone=132",
        "hour=2024-01-01T01:00|zone=132",
        "hour=2024-01-01T02:00|zone=132",
        "hour=2024-01-01T03:00|zone=132",
    ]


def test_a_batched_run_that_a_user_starts_keeps_its_key_as_given():
    hours = _hours("2024-01-01T00:00", "2024-01-01T05:00")
    calls = []
    repo = rs.CodeRepository(
        assets=[_trips(calls)], default_executor=rs.Executor.in_process()
    )

    result = repo.materialize(
        selection=["trips"], partition_key=_trips_key(hours, "132")
    )

    assert result.success

    # Only a backfill writes spans: the record holds each of the 6 hours.
    assert _stored(repo.storage, result.run_id).run_key == (
        "hour=2024-01-01T00:00,2024-01-01T01:00,2024-01-01T02:00,"
        "2024-01-01T03:00,2024-01-01T04:00,2024-01-01T05:00|zone=132"
    )
    assert calls == [Call(zones=["132"], hours=hours)]


@GRIDS
def test_failed_run_of_a_backfill_names_its_partitions_in_one_span(
    executor_env, is_async, grid, first, last
):
    executor, _ = executor_env
    repo = rs.CodeRepository(
        assets=[_trips([], grid, is_async, error="boom")], default_executor=executor
    )

    result = repo.backfill(
        selection=["trips"],
        partition_range=rs.PartitionKeyRange.multi({"hour": (first, last)}),
    )

    assert result.status == "CompletedFailed"
    assert result.completed == 0
    assert result.failed == 96  # 48 hours x 2 zones

    # The record and its one keyed event, the failure, hold the same span.
    storage = repo.storage
    span_132 = f"hour={first}..{last}|zone=132"
    span_7 = f"hour={first}..{last}|zone=7"
    assert sorted(_stored(storage, run_id) for run_id in result.run_ids) == [
        Stored(run_key=span_132, keyed_events=[("StepFailure", span_132)]),
        Stored(run_key=span_7, keyed_events=[("StepFailure", span_7)]),
    ]

    # The Python API gives every partition of each failure.
    hours = _hours(first, last)
    failures = [
        event
        for event in storage.get_events_for_asset("trips")
        if event.event_type == "StepFailure" and event.partition_key is not None
    ]
    assert {event.partition_key for event in failures} == {
        _trips_key(hours, "132"),
        _trips_key(hours, "7"),
    }
    assert all("boom" in dict(event.metadata)["error"] for event in failures)
    assert storage.get_materialized_partitions("trips") == []


def test_failed_batched_run_that_a_user_starts_keeps_its_key_in_the_failure():
    hours = _hours("2024-01-01T00:00", "2024-01-01T05:00")
    repo = rs.CodeRepository(
        assets=[_trips([], error="boom")], default_executor=rs.Executor.in_process()
    )

    result = repo.materialize(
        selection=["trips"],
        partition_key=_trips_key(hours, "132"),
        raise_on_error=False,
    )

    assert not result.success

    # The record and the failure hold each of the 6 hours, no span.
    key = (
        "hour=2024-01-01T00:00,2024-01-01T01:00,2024-01-01T02:00,"
        "2024-01-01T03:00,2024-01-01T04:00,2024-01-01T05:00|zone=132"
    )
    assert _stored(repo.storage, result.run_id) == Stored(
        run_key=key, keyed_events=[("StepFailure", key)]
    )


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


@GRIDS
def test_queued_backfill_stores_each_run_key_as_a_span(storage, grid, first, last):
    calls = []
    repo = _queue_repo(storage, [_trips(calls, grid)])

    launched = repo.backfill(
        selection=["trips"],
        partition_range=rs.PartitionKeyRange.multi({"hour": (first, last)}),
        block=False,
    )
    repo.execute_backfill_queued(launched.backfill_id)

    # Queued: one run per zone. The record and its RunQueued event hold the span.
    run_ids = repo.get_backfill(launched.backfill_id).run_ids
    span_132 = f"hour={first}..{last}|zone=132"
    span_7 = f"hour={first}..{last}|zone=7"
    assert sorted(_stored(storage, run_id) for run_id in run_ids) == [
        Stored(run_key=span_132, keyed_events=[("RunQueued", span_132)]),
        Stored(run_key=span_7, keyed_events=[("RunQueued", span_7)]),
    ]
    hours = _hours(first, last)
    assert {run.partition_key for run in storage.get_queued_runs()} == {
        _trips_key(hours, "132"),
        _trips_key(hours, "7"),
    }

    status = _run_queue_until_done(repo, storage, launched.backfill_id)

    # Run: user code gets every hour, and the records still hold the span.
    assert status.status == "CompletedSuccess"
    assert status.completed_partitions == 96  # 48 hours x 2 zones
    assert sorted(calls) == [
        Call(zones=["132"], hours=hours),
        Call(zones=["7"], hours=hours),
    ]
    assert sorted(_stored(storage, run_id).run_key for run_id in run_ids) == [
        span_132,
        span_7,
    ]


@contextlib.contextmanager
def _serving(repo, grpc_stubs):
    import grpc

    port = repo._start_grpc_server("127.0.0.1", 0)
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    grpc.channel_ready_future(channel).result(timeout=5)
    pb2, pb2_grpc = grpc_stubs
    try:
        yield pb2, pb2_grpc.CodeLocationServiceStub(channel)
    finally:
        channel.close()
        repo._stop_grpc_server()


def _job_repo(storage, trips, pickup):
    """A repo with `trips_job`, and a run queue when `pickup` queues the runs."""
    jobs = [rs.Job(name="trips_job", assets=[trips])]
    if pickup == "execute_backfill_queued":
        return _queue_repo(storage, [trips], jobs=jobs)
    repo = rs.CodeRepository(
        assets=[trips], jobs=jobs, default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    return repo


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


def _pick_up(repo, storage, backfill_id, pickup):
    """Run the backfill to its end with `pickup`. Returns the final status.

    `execute_backfill` runs the runs itself; `execute_backfill_queued` only
    queues them, so a daemon runs them.
    """
    getattr(repo, pickup)(backfill_id)
    if pickup == "execute_backfill_queued":
        return _run_queue_until_done(repo, storage, backfill_id)
    return repo.get_backfill(backfill_id)


@needs_grpc
@pytest.mark.parametrize("pickup", ["execute_backfill", "execute_backfill_queued"])
def test_job_backfill_stores_each_run_key_as_a_span(grpc_stubs, storage, pickup):
    hours = _hours("2024-01-01T00:00", "2024-01-02T23:00")
    calls = []
    repo = _job_repo(storage, _trips(calls), pickup)
    backfill_id = _launch_job_backfill(repo, grpc_stubs, hours)

    status = _pick_up(repo, storage, backfill_id, pickup)

    assert status.status == "CompletedSuccess"
    assert status.completed_partitions == 96  # 48 hours x 2 zones
    assert sorted(calls) == [
        Call(zones=["132"], hours=hours),
        Call(zones=["7"], hours=hours),
    ]

    # One run of the job per zone. Its record holds the 48 hours as one span.
    assert sorted(_stored(storage, run_id).run_key for run_id in status.run_ids) == [
        "hour=2024-01-01T00:00..2024-01-02T23:00|zone=132",
        "hour=2024-01-01T00:00..2024-01-02T23:00|zone=7",
    ]
    assert {storage.get_run(run_id).job_name for run_id in status.run_ids} == {
        "trips_job"
    }


@needs_grpc
@pytest.mark.parametrize("pickup", ["execute_backfill", "execute_backfill_queued"])
def test_failed_job_run_of_a_backfill_names_its_partitions_in_one_span(
    grpc_stubs, storage, pickup
):
    hours = _hours("2024-01-01T00:00", "2024-01-02T23:00")
    repo = _job_repo(storage, _trips([], error="boom"), pickup)
    backfill_id = _launch_job_backfill(repo, grpc_stubs, hours)

    status = _pick_up(repo, storage, backfill_id, pickup)

    assert status.status == "CompletedFailed"
    assert status.failed_partitions == 96  # 48 hours x 2 zones

    # The record and its failure hold the same span.
    span_132 = "hour=2024-01-01T00:00..2024-01-02T23:00|zone=132"
    span_7 = "hour=2024-01-01T00:00..2024-01-02T23:00|zone=7"
    stored = sorted(_stored(storage, run_id) for run_id in status.run_ids)
    assert [run.run_key for run in stored] == [span_132, span_7]
    assert [
        [key for kind, key in run.keyed_events if kind == "StepFailure"]
        for run in stored
    ] == [[span_132], [span_7]]
