"""Integration tests for the gRPC CodeLocation server."""

import grpc
import pytest

import rivers as rs


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
    channel, pb2, pb2_grpc, _ = full_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.ExecuteJob(pb2.ExecuteJobRequest(job_name="no_such_job"))
    assert exc_info.value.code() == grpc.StatusCode.INTERNAL


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


def test_cancel_run_sets_cancellation_flag(queued_grpc_channel):
    """CancelRun gRPC sets cancellation flag in storage for local backend."""
    channel, pb2, pb2_grpc, _, storage = queued_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    resp = stub.ExecuteJob(pb2.ExecuteJobRequest(job_name="test_job"))
    assert resp.run_id

    cancel_resp = stub.CancelRun(pb2.CancelRunRequest(run_id=resp.run_id))
    assert cancel_resp.success

    assert storage.is_cancelled(resp.run_id)


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
