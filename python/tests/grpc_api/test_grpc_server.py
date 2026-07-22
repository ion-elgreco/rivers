"""Integration tests for the gRPC CodeLocation server."""

import grpc
import pytest

import rivers as rs
from _polling import wait_for_asset_materialized as _wait_for_asset_materialized


# ── Test helpers ──

_STORE: dict = {}


class DictIOHandler(rs.BaseIOHandler):
    def handle_output(self, context: rs.OutputContext, obj):
        _STORE[context.asset_name] = obj

    def load_input(self, context: rs.InputContext):
        return _STORE.get(context.asset_name, 0)


@pytest.fixture
def grpc_channel(grpc_stubs):
    """Start gRPC server with a simple repo and return a connected channel + stubs."""
    handler = DictIOHandler()

    @rs.Asset(io_handler=handler)
    def upstream():
        return 1

    @rs.Asset(io_handler=handler)
    def downstream(upstream):
        return upstream + 1

    ext = rs.Asset.external(name="ext_feed", io_handler=handler)

    repo = rs.CodeRepository(assets=[upstream, downstream, ext])
    port = repo._start_grpc_server("127.0.0.1", 0)

    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    grpc.channel_ready_future(channel).result(timeout=5)

    pb2, pb2_grpc = grpc_stubs
    yield channel, pb2, pb2_grpc, repo
    channel.close()
    repo._stop_grpc_server()


@pytest.fixture
def full_grpc_channel(grpc_stubs):
    """gRPC server with jobs, schedules, sensors, and observable assets."""
    handler = DictIOHandler()

    @rs.Asset(io_handler=handler)
    def source():
        return 42

    @rs.Asset(io_handler=handler)
    def sink(source):
        return source * 2

    @rs.Asset.external(io_handler=handler)
    def observed(context: rs.AssetExecutionContext):
        context.add_output_metadata({"row_count": rs.MetadataValue.int(100)})

    job = rs.Job(name="my_job", assets=[source, sink])

    @rs.Schedule(
        cron_schedule="0 0 * * *",
        job_name="my_job",
        name="daily_sched",
        default_status=rs.ScheduleStatus.Running,
    )
    def daily_sched(context: rs.ScheduleEvaluationContext):
        return rs.RunRequest()

    @rs.Sensor(
        job_name="my_job",
        name="my_sensor",
        default_status=rs.SensorStatus.Running,
    )
    def my_sensor(context: rs.SensorEvaluationContext):
        return rs.RunRequest(tags={"tick": "1"})

    repo = rs.CodeRepository(
        assets=[source, sink, observed],
        jobs=[job],
        schedules=[daily_sched],
        sensors=[my_sensor],
    )
    port = repo._start_grpc_server("127.0.0.1", 0)

    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    grpc.channel_ready_future(channel).result(timeout=5)

    pb2, pb2_grpc = grpc_stubs
    yield channel, pb2, pb2_grpc, repo
    channel.close()
    repo._stop_grpc_server()


# ── Tests ──


def test_ping(grpc_channel):
    channel, pb2, pb2_grpc, _ = grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    response = stub.Ping(pb2.PingRequest())
    assert response.status == "ok"
    assert response.location == ""


def test_get_info(grpc_channel):
    channel, pb2, pb2_grpc, _ = grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    response = stub.GetInfo(pb2.GetInfoRequest())
    assert set(response.asset_names) == {"upstream", "downstream", "ext_feed"}
    # Repository has no user-defined jobs in this fixture.
    assert list(response.job_names) == []


def test_get_assets_info(grpc_channel):
    channel, pb2, pb2_grpc, _ = grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    response = stub.GetAssetsInfo(pb2.GetAssetsInfoRequest())
    asset_keys = {a.asset_key for a in response.assets}
    assert "upstream" in asset_keys
    assert "downstream" in asset_keys
    assert "ext_feed" in asset_keys

    # Verify ext_feed is external
    ext = next(a for a in response.assets if a.asset_key == "ext_feed")
    assert ext.is_external is True

    # Verify upstream is not external
    up = next(a for a in response.assets if a.asset_key == "upstream")
    assert up.is_external is False


def test_get_jobs(grpc_channel):
    channel, pb2, pb2_grpc, _ = grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    response = stub.GetJobs(pb2.GetJobsRequest())
    # Repository has no user-defined jobs in this fixture; ad-hoc materialize
    # runs no longer surface a synthetic job.
    assert list(response.jobs) == []


def test_get_schedules(grpc_channel):
    channel, pb2, pb2_grpc, _ = grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    response = stub.GetSchedules(pb2.GetSchedulesRequest())
    assert len(response.schedules) == 0


def test_get_sensors(grpc_channel):
    channel, pb2, pb2_grpc, _ = grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    response = stub.GetSensors(pb2.GetSensorsRequest())
    assert len(response.sensors) == 0


def test_materialize(grpc_channel):
    channel, pb2, pb2_grpc, _ = grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    response = stub.Materialize(pb2.MaterializeRequest(selection=["upstream"]))
    # gRPC `materialize` is fire-and-forget: returns the freshly minted
    # run_id immediately. The dispatcher mode comes back in `status`
    # ("direct" here, since the test fixture has no run-queue config).
    assert response.run_id != ""
    assert response.status == "direct"


def test_materialize_nonexistent_returns_error(grpc_channel):
    channel, pb2, pb2_grpc, _ = grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.Materialize(pb2.MaterializeRequest(selection=["nonexistent"]))
    assert exc_info.value.code() == grpc.StatusCode.INTERNAL


# ── Tests using full_grpc_channel (jobs, schedules, sensors, observe) ──


def test_execute_job(full_grpc_channel):
    channel, pb2, pb2_grpc, _ = full_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    response = stub.ExecuteJob(pb2.ExecuteJobRequest(job_name="my_job"))
    assert response.success is True
    assert response.run_id != ""


def test_execute_job_nonexistent(full_grpc_channel):
    """An unknown job is rejected at the boundary — nothing is dispatched."""
    channel, pb2, pb2_grpc, _ = full_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.ExecuteJob(pb2.ExecuteJobRequest(job_name="no_such_job"))
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND
    assert "no_such_job" in exc_info.value.details()


def test_get_schedules_with_schedule(full_grpc_channel):
    channel, pb2, pb2_grpc, _ = full_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    response = stub.GetSchedules(pb2.GetSchedulesRequest())
    assert len(response.schedules) == 1
    sched = response.schedules[0]
    assert sched.name == "daily_sched"
    assert sched.cron_schedule == "0 0 * * *"
    assert sched.job_name == "my_job"
    assert sched.status == "RUNNING"


def test_get_sensors_with_sensor(full_grpc_channel):
    channel, pb2, pb2_grpc, _ = full_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    response = stub.GetSensors(pb2.GetSensorsRequest())
    assert len(response.sensors) == 1
    sensor = response.sensors[0]
    assert sensor.name == "my_sensor"
    assert sensor.job_name == "my_job"
    assert sensor.status == "RUNNING"


def test_get_jobs_with_job(full_grpc_channel):
    channel, pb2, pb2_grpc, _ = full_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    response = stub.GetJobs(pb2.GetJobsRequest())
    job_names = [j.name for j in response.jobs]
    assert "my_job" in job_names
    my_job = next(j for j in response.jobs if j.name == "my_job")
    assert set(my_job.asset_selection) == {"source", "sink"}


def test_evaluate_schedule(full_grpc_channel):
    channel, pb2, pb2_grpc, _ = full_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    response = stub.EvaluateSchedule(
        pb2.EvaluateScheduleRequest(schedule_name="daily_sched")
    )
    assert len(response.run_ids) == 1
    assert response.skip_reason == ""


def test_evaluate_schedule_nonexistent(full_grpc_channel):
    channel, pb2, pb2_grpc, _ = full_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.EvaluateSchedule(
            pb2.EvaluateScheduleRequest(schedule_name="no_such_schedule")
        )
    assert exc_info.value.code() == grpc.StatusCode.INTERNAL


def test_evaluate_sensor(full_grpc_channel):
    channel, pb2, pb2_grpc, _ = full_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    response = stub.EvaluateSensor(pb2.EvaluateSensorRequest(sensor_name="my_sensor"))
    assert len(response.run_ids) == 1
    assert response.skip_reason == ""


def test_evaluate_sensor_nonexistent(full_grpc_channel):
    channel, pb2, pb2_grpc, _ = full_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.EvaluateSensor(pb2.EvaluateSensorRequest(sensor_name="no_such_sensor"))
    assert exc_info.value.code() == grpc.StatusCode.INTERNAL


def test_observe_asset(full_grpc_channel):
    channel, pb2, pb2_grpc, _ = full_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    response = stub.ObserveAsset(pb2.ObserveAssetRequest(asset_key="observed"))
    assert response.success is True
    assert response.error == ""


# ── Tests: Run queue via gRPC ──


@pytest.fixture
def queued_grpc_channel(grpc_stubs, storage):
    """gRPC server with run_queue configured — runs should be queued, not executed."""
    handler = DictIOHandler()

    @rs.Asset(io_handler=handler)
    def alpha():
        return 1

    @rs.Asset(io_handler=handler)
    def beta(alpha):
        return alpha + 1

    job = rs.Job(name="test_job", assets=[alpha, beta])

    repo = rs.CodeRepository(
        assets=[alpha, beta],
        jobs=[job],
        default_executor=rs.Executor.in_process(),
        run_queue=rs.RunQueueConfig(max_concurrent_runs=2, dequeue_interval="50ms"),
    )
    repo.resolve(storage=storage)
    port = repo._start_grpc_server("127.0.0.1", 0)

    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    grpc.channel_ready_future(channel).result(timeout=5)

    pb2, pb2_grpc = grpc_stubs
    yield channel, pb2, pb2_grpc, repo, storage
    channel.close()
    repo._stop_grpc_server()


def test_materialize_with_run_queue_creates_queued_run(queued_grpc_channel):
    """When run_queue is configured, Materialize gRPC should queue the run, not execute it."""
    channel, pb2, pb2_grpc, _, storage = queued_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    response = stub.Materialize(pb2.MaterializeRequest(selection=["alpha"]))
    assert response.status == "queued"
    assert response.run_id != ""

    # Verify the run record is Queued in storage
    runs = storage.get_runs(limit=10)
    matching = [r for r in runs if r.run_id == response.run_id]
    assert len(matching) == 1
    assert matching[0].status == "Queued"


def test_execute_job_with_run_queue_creates_queued_run(queued_grpc_channel):
    """When run_queue is configured, ExecuteJob gRPC should queue the run, not execute it."""
    channel, pb2, pb2_grpc, _, storage = queued_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    response = stub.ExecuteJob(pb2.ExecuteJobRequest(job_name="test_job"))
    assert response.success is True
    assert response.run_id != ""

    import time

    time.sleep(0.5)  # brief wait for async thread to create the record

    runs = storage.get_runs(limit=10)
    matching = [r for r in runs if r.run_id == response.run_id]
    assert len(matching) == 1
    assert matching[0].status == "Queued"
    # Regression: queued runs from ExecuteJob must carry the requested job_name,
    # not the default. The jobs-list page's "last run" column joins on it.
    assert matching[0].job_name == "test_job"


def test_execute_job_nonexistent_queued_creates_no_run(queued_grpc_channel):
    """Regression: the queued path used to dispatch an unknown job as a
    materialize-everything run; it must be rejected with no record written."""
    channel, pb2, pb2_grpc, _, storage = queued_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    before = len(storage.get_runs(limit=100))
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.ExecuteJob(pb2.ExecuteJobRequest(job_name="no_such_job"))
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND

    import time

    time.sleep(0.5)  # would-be async record creation window
    assert len(storage.get_runs(limit=100)) == before


def test_multiple_submits_all_queued(queued_grpc_channel):
    """Multiple gRPC calls should all create separate Queued runs."""
    channel, pb2, pb2_grpc, _, storage = queued_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    run_ids = []
    for _ in range(3):
        resp = stub.Materialize(pb2.MaterializeRequest(selection=["alpha"]))
        assert resp.run_id
        run_ids.append(resp.run_id)

    assert len(set(run_ids)) == 3  # all unique

    runs = storage.get_runs(limit=20)
    queued = [r for r in runs if r.run_id in run_ids and r.status == "Queued"]
    assert len(queued) == 3


def test_cancel_run_cancels_queued_run(queued_grpc_channel):
    """CancelRun on a still-queued run flips it to Canceled and removes it from
    the queue — regression: only the cancellation flag was set (consumed solely
    by the executor), so a queued run stayed Queued forever."""
    channel, pb2, pb2_grpc, _, storage = queued_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    resp = stub.ExecuteJob(pb2.ExecuteJobRequest(job_name="test_job"))
    assert resp.run_id

    cancel_resp = stub.CancelRun(pb2.CancelRunRequest(run_id=resp.run_id))
    assert cancel_resp.success

    assert storage.is_cancelled(resp.run_id)
    run = storage.get_run(resp.run_id)
    assert run is not None
    assert run.status == "Canceled"
    assert resp.run_id not in [r.run_id for r in storage.get_queued_runs()]


def test_rerun_rejects_run_from_another_code_location(grpc_stubs, storage, monkeypatch):
    """RerunRun must refuse a run owned by a different code location —
    regression: location bar rebuilt foo's run against its own definitions and
    dispatched a mangled copy stamped with bar's identity."""
    handler = DictIOHandler()

    def make_repo(cl_id):
        @rs.Asset(io_handler=handler)
        def alpha():
            return 1

        job = rs.Job(name="test_job", assets=[alpha])
        monkeypatch.setenv("RIVERS_CODE_LOCATION_ID", cl_id)
        repo = rs.CodeRepository(
            assets=[alpha],
            jobs=[job],
            default_executor=rs.Executor.in_process(),
            run_queue=rs.RunQueueConfig(max_concurrent_runs=2, dequeue_interval="50ms"),
        )
        repo.resolve(storage=storage)
        return repo

    pb2, pb2_grpc = grpc_stubs

    # One gRPC server at a time: create a queued run under cl-foo, stop.
    foo = make_repo("cl-foo")
    port = foo._start_grpc_server("127.0.0.1", 0)
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    grpc.channel_ready_future(channel).result(timeout=5)
    run_id = pb2_grpc.CodeLocationServiceStub(channel).ExecuteJob(
        pb2.ExecuteJobRequest(job_name="test_job")
    ).run_id
    channel.close()
    foo._stop_grpc_server()

    bar = make_repo("cl-bar")
    port = bar._start_grpc_server("127.0.0.1", 0)
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    grpc.channel_ready_future(channel).result(timeout=5)
    try:
        with pytest.raises(grpc.RpcError) as exc_info:
            pb2_grpc.CodeLocationServiceStub(channel).RerunRun(
                pb2.RerunRunRequest(run_id=run_id)
            )
        assert "code location" in exc_info.value.details()
        # The original run is untouched.
        assert storage.get_run(run_id).status == "Queued"
    finally:
        channel.close()
        bar._stop_grpc_server()


@pytest.fixture
def slow_grpc_channel(grpc_stubs, storage):
    """gRPC server with a slow job (no run queue) — runs execute on background thread."""
    import time as _time

    handler = DictIOHandler()

    @rs.Asset(io_handler=handler)
    def slow_step():
        _time.sleep(5)
        return 1

    @rs.Asset(io_handler=handler)
    def after_slow(slow_step: int):
        return slow_step + 1

    job = rs.Job(name="slow_job", assets=[slow_step, after_slow])

    repo = rs.CodeRepository(
        assets=[slow_step, after_slow],
        jobs=[job],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    port = repo._start_grpc_server("127.0.0.1", 0)

    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    grpc.channel_ready_future(channel).result(timeout=5)

    pb2, pb2_grpc = grpc_stubs
    yield channel, pb2, pb2_grpc, repo, storage
    channel.close()
    repo._stop_grpc_server()


def test_cancel_run_during_execution(slow_grpc_channel):
    """CancelRun gRPC cancels an actively executing run with local backend."""
    import time

    channel, pb2, pb2_grpc, _, storage = slow_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    resp = stub.ExecuteJob(pb2.ExecuteJobRequest(job_name="slow_job"))
    assert resp.run_id
    run_id = resp.run_id

    # Wait for run to start
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        runs = storage.get_runs(limit=10)
        run = next((r for r in runs if r.run_id == run_id), None)
        if run and run.status == "Started":
            break
        time.sleep(0.1)

    cancel_resp = stub.CancelRun(pb2.CancelRunRequest(run_id=run_id))
    assert cancel_resp.success
    assert storage.is_cancelled(run_id)

    # Wait for run to finish (slow_step completes, after_slow skipped due to cancel)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        runs = storage.get_runs(limit=10)
        run = next((r for r in runs if r.run_id == run_id), None)
        if run and run.status in ("Success", "Failure"):
            break
        time.sleep(0.2)

    assert run is not None
    assert run.status == "Canceled"


# ── Tests: Backfill via gRPC ──


@pytest.fixture
def backfill_grpc_channel(grpc_stubs, storage):
    """gRPC server with a partitioned asset for backfill tests."""
    handler = DictIOHandler()
    pd = rs.PartitionsDefinition.static_(["p1", "p2", "p3"])

    @rs.Asset(io_handler=handler, partitions_def=pd)
    def partitioned_asset(context: rs.AssetExecutionContext):
        return context.partition_key

    repo = rs.CodeRepository(
        assets=[partitioned_asset],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    port = repo._start_grpc_server("127.0.0.1", 0)

    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    grpc.channel_ready_future(channel).result(timeout=5)

    pb2, pb2_grpc = grpc_stubs
    yield channel, pb2, pb2_grpc, repo, storage
    channel.close()
    repo._stop_grpc_server()


def _single_partition_key(pb2, key: str):
    return pb2.ProtoPartitionKey(single=pb2.SinglePartitionKey(keys=[key]))


def test_launch_backfill(backfill_grpc_channel):
    channel, pb2, pb2_grpc, _, _ = backfill_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    response = stub.LaunchBackfill(
        pb2.LaunchBackfillRequest(
            selection=["partitioned_asset"],
            partition_keys=[
                _single_partition_key(pb2, "p1"),
                _single_partition_key(pb2, "p2"),
            ],
            failure_policy="continue",
            max_concurrency=1,
            dry_run=False,
        )
    )
    assert response.backfill_id != ""
    assert response.num_partitions == 2
    assert response.is_dry_run is False


def test_launch_backfill_dry_run(backfill_grpc_channel):
    channel, pb2, pb2_grpc, _, _ = backfill_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    response = stub.LaunchBackfill(
        pb2.LaunchBackfillRequest(
            selection=["partitioned_asset"],
            partition_keys=[_single_partition_key(pb2, "p1")],
            failure_policy="continue",
            max_concurrency=1,
            dry_run=True,
        )
    )
    assert response.is_dry_run is True
    assert response.num_partitions == 1


@pytest.fixture
def multi_backfill_grpc_channel(grpc_stubs, storage):
    """gRPC server with a Multi-partitioned asset for strategy validation."""
    handler = DictIOHandler()
    pd = rs.PartitionsDefinition.multi(
        {
            "date": rs.PartitionsDefinition.static_(["d1", "d2"]),
            "region": rs.PartitionsDefinition.static_(["eu", "us"]),
        }
    )

    @rs.Asset(io_handler=handler, partitions_def=pd)
    def multi_asset(context: rs.AssetExecutionContext):
        return context.partition_key

    repo = rs.CodeRepository(
        assets=[multi_asset],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    port = repo._start_grpc_server("127.0.0.1", 0)

    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    grpc.channel_ready_future(channel).result(timeout=5)

    pb2, pb2_grpc = grpc_stubs
    yield channel, pb2, pb2_grpc
    channel.close()
    repo._stop_grpc_server()


def _multi_partition_key(pb2, date: str, region: str):
    return pb2.ProtoPartitionKey(
        multi=pb2.MultiPartitionKey(
            dimensions=[
                pb2.MultiPartitionDimension(name="date", keys=[date]),
                pb2.MultiPartitionDimension(name="region", keys=[region]),
            ]
        )
    )


@pytest.mark.parametrize(
    "multi_run, single_run, message",
    [
        ([], ["region"], "multi_run must contain at least one dimension"),
        (["date"], [], "single_run must contain at least one dimension"),
        (
            ["date"],
            ["date"],
            "dimension 'date' cannot be in both multi_run and single_run",
        ),
    ],
)
def test_launch_backfill_per_dimension_invariants_enforced(
    multi_backfill_grpc_channel, multi_run, single_run, message
):
    """The proto path must enforce the same PerDimension invariants as the
    Python constructor — an empty multi_run list otherwise collapses the
    whole backfill into a single run."""
    channel, pb2, pb2_grpc = multi_backfill_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    with pytest.raises(grpc.RpcError) as exc_info:
        stub.LaunchBackfill(
            pb2.LaunchBackfillRequest(
                selection=["multi_asset"],
                partition_keys=[
                    _multi_partition_key(pb2, "d1", "eu"),
                    _multi_partition_key(pb2, "d2", "us"),
                ],
                strategy=pb2.BackfillStrategyProto(
                    per_dimension=pb2.PerDimensionStrategy(
                        multi_run_dimensions=multi_run,
                        single_run_dimensions=single_run,
                    )
                ),
                failure_policy="continue",
                max_concurrency=1,
                dry_run=True,
            )
        )
    assert message in exc_info.value.details()


def test_get_backfill_status(backfill_grpc_channel):
    channel, pb2, pb2_grpc, _, _ = backfill_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    launch = stub.LaunchBackfill(
        pb2.LaunchBackfillRequest(
            selection=["partitioned_asset"],
            partition_keys=[
                _single_partition_key(pb2, "p1"),
                _single_partition_key(pb2, "p2"),
            ],
            failure_policy="continue",
            max_concurrency=1,
        )
    )
    assert launch.backfill_id

    status = stub.GetBackfillStatus(
        pb2.GetBackfillStatusRequest(backfill_id=launch.backfill_id)
    )
    assert status.backfill_id == launch.backfill_id
    assert status.total_partitions == 2


def test_get_backfill_status_not_found(backfill_grpc_channel):
    channel, pb2, pb2_grpc, _, _ = backfill_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    with pytest.raises(grpc.RpcError) as exc_info:
        stub.GetBackfillStatus(pb2.GetBackfillStatusRequest(backfill_id="missing-id"))
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND


def test_cancel_backfill(backfill_grpc_channel):
    channel, pb2, pb2_grpc, _, _ = backfill_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    launch = stub.LaunchBackfill(
        pb2.LaunchBackfillRequest(
            selection=["partitioned_asset"],
            partition_keys=[_single_partition_key(pb2, "p1")],
            failure_policy="continue",
            max_concurrency=1,
        )
    )
    assert launch.backfill_id

    cancel = stub.CancelBackfill(
        pb2.CancelBackfillRequest(backfill_id=launch.backfill_id)
    )
    # success=true means a live coordinator was signalled; the no-coordinator
    # branch (Requested/already-finished record) returns false but still flips
    # the storage record to Canceled. Both are valid here.
    assert isinstance(cancel.success, bool)

    # Status should reflect cancellation regardless of which branch fired.
    status = stub.GetBackfillStatus(
        pb2.GetBackfillStatusRequest(backfill_id=launch.backfill_id)
    )
    assert status.status == "Canceled"


def test_rerun_backfill(backfill_grpc_channel):
    channel, pb2, pb2_grpc, _, _ = backfill_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    original = stub.LaunchBackfill(
        pb2.LaunchBackfillRequest(
            selection=["partitioned_asset"],
            partition_keys=[
                _single_partition_key(pb2, "p1"),
                _single_partition_key(pb2, "p2"),
            ],
            failure_policy="continue",
            max_concurrency=1,
        )
    )
    assert original.backfill_id

    rerun = stub.RerunBackfill(
        pb2.RerunBackfillRequest(backfill_id=original.backfill_id, dry_run=False)
    )
    assert rerun.backfill_id != ""
    assert rerun.backfill_id != original.backfill_id
    assert rerun.num_partitions == 2


def test_rerun_backfill_dry_run(backfill_grpc_channel):
    channel, pb2, pb2_grpc, _, _ = backfill_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    original = stub.LaunchBackfill(
        pb2.LaunchBackfillRequest(
            selection=["partitioned_asset"],
            partition_keys=[_single_partition_key(pb2, "p1")],
            failure_policy="continue",
            max_concurrency=1,
        )
    )
    assert original.backfill_id

    rerun = stub.RerunBackfill(
        pb2.RerunBackfillRequest(backfill_id=original.backfill_id, dry_run=True)
    )
    assert rerun.is_dry_run is True


def test_rerun_backfill_not_found(backfill_grpc_channel):
    channel, pb2, pb2_grpc, _, _ = backfill_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    with pytest.raises(grpc.RpcError) as exc_info:
        stub.RerunBackfill(
            pb2.RerunBackfillRequest(backfill_id="missing-id", dry_run=False)
        )
    assert exc_info.value.code() == grpc.StatusCode.INTERNAL


# ── rerun_run ──


@pytest.fixture
def rerun_grpc_channel(grpc_stubs, storage):
    """gRPC server with a partitioned asset and a job over it, for rerun_run tests."""
    handler = DictIOHandler()
    pd = rs.PartitionsDefinition.static_(["p1", "p2", "p3"])

    @rs.Asset(io_handler=handler, partitions_def=pd)
    def part_asset(context: rs.AssetExecutionContext):
        return context.partition_key

    job = rs.Job(name="part_job", assets=[part_asset])
    repo = rs.CodeRepository(
        assets=[part_asset],
        jobs=[job],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    port = repo._start_grpc_server("127.0.0.1", 0)

    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    grpc.channel_ready_future(channel).result(timeout=5)

    pb2, pb2_grpc = grpc_stubs
    yield channel, pb2, pb2_grpc, repo, storage
    channel.close()
    repo._stop_grpc_server()


def test_rerun_run_reuses_partition(rerun_grpc_channel):
    """Re-executing a materialization run replays it on the SAME partition and
    links back via the rerun_of tag — the run-detail "Retry from" path (#49)."""
    channel, pb2, pb2_grpc, _, storage = rerun_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    original = stub.Materialize(
        pb2.MaterializeRequest(
            selection=["part_asset"],
            partition_key=_single_partition_key(pb2, "p2"),
        )
    )
    assert original.run_id != ""
    orig_record = storage.get_run(original.run_id)
    assert orig_record is not None
    assert orig_record.partition_key == rs.PartitionKey.single("p2")

    rerun = stub.RerunRun(pb2.RerunRunRequest(run_id=original.run_id))
    assert rerun.run_id != ""
    assert rerun.run_id != original.run_id

    rerun_record = storage.get_run(rerun.run_id)
    assert rerun_record is not None
    # No job; partition reused, origin tagged.
    assert rerun_record.job_name is None
    assert rerun_record.partition_key == rs.PartitionKey.single("p2")
    assert ("rivers/rerun_of", original.run_id) in rerun_record.tags


def test_rerun_run_job_reuses_partition(rerun_grpc_channel):
    """The job branch of rerun_run replays the same job on the same partition."""
    channel, pb2, pb2_grpc, _, storage = rerun_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    original = stub.ExecuteJob(
        pb2.ExecuteJobRequest(
            job_name="part_job",
            partition_key=_single_partition_key(pb2, "p3"),
        )
    )
    assert original.run_id != ""

    rerun = stub.RerunRun(pb2.RerunRunRequest(run_id=original.run_id))
    assert rerun.run_id != ""
    assert rerun.run_id != original.run_id

    rerun_record = storage.get_run(rerun.run_id)
    assert rerun_record is not None
    assert rerun_record.job_name == "part_job"
    assert rerun_record.partition_key == rs.PartitionKey.single("p3")
    # No rerun_of-tag assertion here: job reruns don't persist per-run tags yet
    # (`RunRequestData.tags` is unused), so the marker only lands on materialization reruns.


def test_rerun_run_not_found(rerun_grpc_channel):
    channel, pb2, pb2_grpc, _, _ = rerun_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    with pytest.raises(grpc.RpcError) as exc_info:
        stub.RerunRun(pb2.RerunRunRequest(run_id="missing-id"))
    assert exc_info.value.code() == grpc.StatusCode.INTERNAL


# ── materialize_missing ──


def test_materialize_missing_backfills_all_when_none_materialized(
    backfill_grpc_channel,
):
    """Nothing materialized yet → the backfill covers the full partition set."""
    channel, pb2, pb2_grpc, _, _ = backfill_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    resp = stub.MaterializeMissing(
        pb2.MaterializeMissingRequest(asset_key="partitioned_asset", max_concurrency=4)
    )
    assert resp.backfill_id != ""
    assert resp.num_partitions == 3  # p1, p2, p3 — all missing


def test_materialize_missing_excludes_already_materialized(backfill_grpc_channel):
    """The backfill skips partitions already materialized in storage."""
    channel, pb2, pb2_grpc, _, storage = backfill_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    # Materialize p1, then wait until storage records it.
    stub.Materialize(
        pb2.MaterializeRequest(
            selection=["partitioned_asset"],
            partition_key=_single_partition_key(pb2, "p1"),
        )
    )
    _wait_for_asset_materialized(storage, "partitioned_asset")
    assert rs.PartitionKey.single("p1") in storage.get_materialized_partitions(
        "partitioned_asset"
    )

    resp = stub.MaterializeMissing(
        pb2.MaterializeMissingRequest(asset_key="partitioned_asset", max_concurrency=4)
    )
    assert resp.backfill_id != ""
    assert resp.num_partitions == 2  # only p2, p3 remain


def test_materialize_missing_unpartitioned_errors(backfill_grpc_channel):
    channel, pb2, pb2_grpc, _, _ = backfill_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    with pytest.raises(grpc.RpcError) as exc_info:
        stub.MaterializeMissing(
            pb2.MaterializeMissingRequest(asset_key="does_not_exist", max_concurrency=4)
        )
    assert exc_info.value.code() == grpc.StatusCode.INTERNAL


# ── launch_backfill: job-aware vs ad-hoc target ──


def test_launch_backfill_for_job_runs_as_job(rerun_grpc_channel):
    """A job-targeted backfill runs each partition with the job's own spec — its
    runs are attributed to the job (job_name set), not ad-hoc materializations."""
    channel, pb2, pb2_grpc, repo, storage = rerun_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    resp = stub.LaunchBackfill(
        pb2.LaunchBackfillRequest(
            job_name="part_job",
            partition_keys=[_single_partition_key(pb2, k) for k in ("p1", "p2", "p3")],
            failure_policy="continue",
            max_concurrency=1,
        )
    )
    assert resp.backfill_id != ""
    assert resp.num_partitions == 3

    # No daemon in this fixture, so execute inline, then check attribution.
    repo.execute_backfill(resp.backfill_id)
    job_runs = [r for r in storage.get_runs(100) if r.job_name == "part_job"]
    assert len(job_runs) == 3
    assert {repr(r.partition_key) for r in job_runs} == {
        repr(rs.PartitionKey.single(k)) for k in ("p1", "p2", "p3")
    }


def test_launch_backfill_without_job_is_ad_hoc(rerun_grpc_channel):
    """A selection-targeted backfill (no job_name) runs ad-hoc materializations —
    its runs carry no job_name."""
    channel, pb2, pb2_grpc, repo, storage = rerun_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    resp = stub.LaunchBackfill(
        pb2.LaunchBackfillRequest(
            selection=["part_asset"],
            partition_keys=[_single_partition_key(pb2, k) for k in ("p1", "p2")],
            failure_policy="continue",
            max_concurrency=1,
        )
    )
    assert resp.num_partitions == 2

    repo.execute_backfill(resp.backfill_id)
    runs = [r for r in storage.get_runs(100) if r.partition_key is not None]
    assert runs, "expected partitioned runs"
    assert all(r.job_name is None for r in runs)
