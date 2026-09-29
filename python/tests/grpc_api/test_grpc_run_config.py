"""gRPC launches carry per-asset config overrides (issue #203).

The overrides ride the run record, so every launcher — direct dispatch, the
run queue, backfill children, reruns — applies the values an in-process
``materialize(config=...)`` does. ``GetAssetsInfo`` exposes each config
class's JSON schema so the UI can pre-fill its editor.
"""

import json

import grpc
import pytest
import rivers as rs
from _polling import wait_for_run_terminal
from pydantic import BaseModel
from rivers._core import AutomationDaemon

_STORE: dict = {}
_SEEN: dict = {}


class DictIOHandler(rs.BaseIOHandler):
    def handle_output(self, context: rs.OutputContext, obj):
        _STORE[context.asset_name] = obj

    def load_input(self, context: rs.InputContext):
        return _STORE.get(context.asset_name, 0)


class ThresholdConfig(BaseModel):
    threshold: float = 0.5
    max_retries: int = 3


class CompactConfig(BaseModel):
    target_size_mb: int = 128


def _build_repo(run_queue=None):
    handler = DictIOHandler()

    @rs.Asset(io_handler=handler)
    def plain():
        return 1

    def _compact(ctx: rs.ActionContext[CompactConfig]):
        _SEEN["compact"] = ctx.config.target_size_mb

    compact = rs.AssetAction(name="compact", outcome=rs.Outcome.Unchanged)(_compact)

    @rs.Asset(io_handler=handler, actions=[compact])
    def configured(context: rs.AssetExecutionContext[ThresholdConfig]):
        return {
            "threshold": context.config.threshold,
            "max_retries": context.config.max_retries,
        }

    @rs.Asset(
        io_handler=handler,
        partitions_def=rs.PartitionsDefinition.static_(["a", "b", "c"]),
    )
    def part_cfg(context: rs.AssetExecutionContext[ThresholdConfig]):
        _SEEN.setdefault("part_cfg", []).append(
            (context.partition_key, context.config.threshold)
        )
        return context.config.threshold

    job = rs.Job(name="cfg_job", assets=[configured])
    return rs.CodeRepository(
        assets=[plain, configured, part_cfg],
        jobs=[job],
        default_executor=rs.Executor.in_process(),
        run_queue=run_queue,
    )


def _connect(repo, grpc_stubs):
    port = repo._start_grpc_server("127.0.0.1", 0)
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    grpc.channel_ready_future(channel).result(timeout=5)
    pb2, pb2_grpc = grpc_stubs
    return channel, pb2_grpc.CodeLocationServiceStub(channel), pb2


@pytest.fixture(autouse=True)
def _clear_state():
    _STORE.clear()
    _SEEN.clear()


@pytest.fixture
def direct(grpc_stubs):
    """Direct dispatch: a launch runs on its own thread right away."""
    repo = _build_repo()
    channel, stub, pb2 = _connect(repo, grpc_stubs)
    yield stub, pb2, repo
    channel.close()
    repo._stop_grpc_server()


@pytest.fixture
def queued(grpc_stubs, storage):
    """Run queue + daemon: a launch is a Queued record the coordinator
    hands to the local run backend."""
    repo = _build_repo(
        run_queue=rs.RunQueueConfig(max_concurrent_runs=2, dequeue_interval="50ms")
    )
    repo.resolve(storage=storage)
    channel, stub, pb2 = _connect(repo, grpc_stubs)
    daemon = AutomationDaemon(repo=repo, storage=storage)
    daemon.start()
    yield stub, pb2, repo
    daemon.stop()
    channel.close()
    repo._stop_grpc_server()


def _cfg(**per_asset):
    return json.dumps(per_asset)


def _single(pb2, key):
    return pb2.ProtoPartitionKey(single=pb2.SinglePartitionKey(keys=[key]))


# ── Schemas ──


def test_get_assets_info_exposes_config_schemas(direct):
    stub, pb2, _ = direct
    by_key = {
        a.asset_key: a for a in stub.GetAssetsInfo(pb2.GetAssetsInfoRequest()).assets
    }

    schema = json.loads(by_key["configured"].config_schema)
    assert schema["title"] == "ThresholdConfig"
    assert schema["properties"]["threshold"]["default"] == 0.5
    assert schema["properties"]["max_retries"]["default"] == 3
    assert not by_key["plain"].HasField("config_schema")

    (compact,) = [a for a in by_key["configured"].actions if a.name == "compact"]
    assert json.loads(compact.config_schema)["properties"]["target_size_mb"] == {
        "default": 128,
        "title": "Target Size Mb",
        "type": "integer",
    }


# ── Materialize ──


def test_materialize_applies_config_and_records_it(direct):
    stub, pb2, repo = direct
    resp = stub.Materialize(
        pb2.MaterializeRequest(
            selection=["configured"], config=_cfg(configured={"threshold": 0.9})
        )
    )
    run = wait_for_run_terminal(repo.storage, resp.run_id)
    assert run.status == "Success"
    # The override merged with the defaults, as in-process materialize does.
    assert _STORE["configured"] == {"threshold": 0.9, "max_retries": 3}
    assert run.config == {"configured": {"threshold": 0.9}}


def test_materialize_without_config_records_none(direct):
    stub, pb2, repo = direct
    resp = stub.Materialize(pb2.MaterializeRequest(selection=["configured"]))
    run = wait_for_run_terminal(repo.storage, resp.run_id)
    assert run.status == "Success"
    assert _STORE["configured"] == {"threshold": 0.5, "max_retries": 3}
    assert run.config is None


@pytest.mark.parametrize(
    ("config", "detail"),
    [
        ("nope", "config is not valid JSON"),
        ("[1]", "config must be a JSON object keyed by asset name"),
        ('{"other": {"x": 1}}', "config names 'other', which is not in the selection"),
        ('{"configured": 1}', "config for 'configured' must be a JSON object"),
    ],
)
def test_materialize_rejects_malformed_config(direct, config, detail):
    """Rejected at the boundary: a bad config must never become a run_id that
    points at a run failing later, or one silently using the defaults."""
    stub, pb2, repo = direct
    with pytest.raises(grpc.RpcError) as exc:
        stub.Materialize(
            pb2.MaterializeRequest(selection=["configured"], config=config)
        )
    assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert detail in exc.value.details()
    assert repo.storage.get_runs(limit=10) == []


def test_empty_config_means_defaults(direct):
    stub, pb2, repo = direct
    resp = stub.Materialize(
        pb2.MaterializeRequest(selection=["configured"], config="{}")
    )
    run = wait_for_run_terminal(repo.storage, resp.run_id)
    assert run.status == "Success"
    assert run.config is None


# ── Actions, jobs, reruns ──


def test_run_action_applies_its_own_config(direct):
    stub, pb2, repo = direct
    resp = stub.RunAction(
        pb2.RunActionRequest(
            action="compact",
            selection=["configured"],
            config=_cfg(configured={"target_size_mb": 512}),
        )
    )
    assert resp.success, resp.error
    run = wait_for_run_terminal(repo.storage, resp.run_id)
    assert run.status == "Success"
    assert _SEEN["compact"] == 512
    assert run.config == {"configured": {"target_size_mb": 512}}


def test_execute_job_applies_config(direct):
    stub, pb2, repo = direct
    resp = stub.ExecuteJob(
        pb2.ExecuteJobRequest(
            job_name="cfg_job", config=_cfg(configured={"max_retries": 7})
        )
    )
    run = wait_for_run_terminal(repo.storage, resp.run_id)
    assert run.status == "Success"
    assert _STORE["configured"] == {"threshold": 0.5, "max_retries": 7}
    assert run.config == {"configured": {"max_retries": 7}}


def test_execute_job_rejects_config_for_an_asset_outside_the_job(direct):
    stub, pb2, _ = direct
    with pytest.raises(grpc.RpcError) as exc:
        stub.ExecuteJob(
            pb2.ExecuteJobRequest(job_name="cfg_job", config=_cfg(plain={"x": 1}))
        )
    assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_rerun_replays_the_original_config(direct):
    stub, pb2, repo = direct
    first = stub.Materialize(
        pb2.MaterializeRequest(
            selection=["configured"], config=_cfg(configured={"threshold": 0.9})
        )
    )
    assert wait_for_run_terminal(repo.storage, first.run_id).status == "Success"
    _STORE.clear()

    rerun = stub.RerunRun(pb2.RerunRunRequest(run_id=first.run_id))
    run = wait_for_run_terminal(repo.storage, rerun.run_id)
    assert run.status == "Success"
    assert _STORE["configured"] == {"threshold": 0.9, "max_retries": 3}
    assert run.config == {"configured": {"threshold": 0.9}}


# ── Backfills ──


def test_launch_backfill_keeps_config_for_every_child_run(direct):
    stub, pb2, repo = direct
    launch = stub.LaunchBackfill(
        pb2.LaunchBackfillRequest(
            selection=["part_cfg"],
            partition_keys=[_single(pb2, "a"), _single(pb2, "b")],
            max_concurrency=1,
            config=_cfg(part_cfg={"threshold": 0.25}),
        )
    )
    # A daemon picks the Requested backfill up later, from the record alone.
    repo.execute_backfill(launch.backfill_id)

    assert sorted(_SEEN["part_cfg"]) == [("a", 0.25), ("b", 0.25)]
    status = repo.get_backfill(launch.backfill_id)
    assert status is not None and status.status == "CompletedSuccess"
    for run_id in status.run_ids:
        run = repo.storage.get_run(run_id)
        assert run is not None
        assert run.config == {"part_cfg": {"threshold": 0.25}}


# ── Run queue ──


def test_queued_materialize_applies_config_when_dequeued(queued):
    """The queued record carries the config; the local run backend reads it
    from the record when the coordinator launches the run."""
    stub, pb2, repo = queued
    resp = stub.Materialize(
        pb2.MaterializeRequest(
            selection=["configured"], config=_cfg(configured={"threshold": 0.7})
        )
    )
    assert resp.status == "queued"
    queued_record = repo.storage.get_run(resp.run_id)
    assert queued_record is not None
    assert queued_record.config == {"configured": {"threshold": 0.7}}

    run = wait_for_run_terminal(repo.storage, resp.run_id)
    assert run.status == "Success"
    assert _STORE["configured"] == {"threshold": 0.7, "max_retries": 3}


def test_queued_job_applies_config_when_dequeued(queued):
    stub, pb2, repo = queued
    resp = stub.ExecuteJob(
        pb2.ExecuteJobRequest(
            job_name="cfg_job", config=_cfg(configured={"max_retries": 9})
        )
    )
    run = wait_for_run_terminal(repo.storage, resp.run_id)
    assert run.status == "Success"
    assert _STORE["configured"] == {"threshold": 0.5, "max_retries": 9}
    assert run.config == {"configured": {"max_retries": 9}}
