"""A manual launch over gRPC that the asset version gate holds or refuses
fails with FAILED_PRECONDITION and the reason, before any record."""

import contextlib
from typing import NamedTuple

import pytest

pytest.importorskip("grpc")

import grpc

import rivers as rs

EU = rs.PartitionKey.single("eu")


def _repo(storage, run_queue=None, **declared):
    """A resolved repository of a partitioned ``orders`` at ``declared``,
    with the action ``optimize`` and the job ``orders_job``."""

    def optimize(ctx) -> None:
        return None

    @rs.Asset(
        partitions_def=rs.PartitionsDefinition.static_(["eu", "us"]),
        actions=[
            rs.AssetAction(name="optimize", outcome=rs.Outcome.Unchanged)(optimize)
        ],
        io_handler=rs.InMemoryIOHandler(),
        **declared,
    )
    def orders() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[orders],
        jobs=[rs.Job(name="orders_job", assets=[orders])],
        default_executor=rs.Executor.in_process(),
        run_queue=run_queue,
    )
    repo.resolve(storage=storage)
    return repo


@contextlib.contextmanager
def _serve(repo, grpc_stubs):
    """The stub and the messages of a gRPC server of ``repo``. In run-queue
    mode, a launch writes a queued record and runs nothing."""
    port = repo._start_grpc_server("127.0.0.1", 0)
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    try:
        grpc.channel_ready_future(channel).result(timeout=5)
        pb2, pb2_grpc = grpc_stubs
        yield pb2_grpc.CodeLocationServiceStub(channel), pb2
    finally:
        channel.close()
        repo._stop_grpc_server()


def _eu(pb2):
    return pb2.ProtoPartitionKey(single=pb2.SinglePartitionKey(keys=["eu"]))


REQUESTS = {
    "materialize": lambda stub, pb2, old: stub.Materialize(
        pb2.MaterializeRequest(selection=["orders"], partition_key=_eu(pb2))
    ),
    "execute_job": lambda stub, pb2, old: stub.ExecuteJob(
        pb2.ExecuteJobRequest(job_name="orders_job", partition_key=_eu(pb2))
    ),
    "run_action": lambda stub, pb2, old: stub.RunAction(
        pb2.RunActionRequest(
            action="optimize", selection=["orders"], partition_key=_eu(pb2)
        )
    ),
    "launch_backfill": lambda stub, pb2, old: stub.LaunchBackfill(
        pb2.LaunchBackfillRequest(selection=["orders"], partition_keys=[_eu(pb2)])
    ),
    "materialize_missing": lambda stub, pb2, old: stub.MaterializeMissing(
        pb2.MaterializeMissingRequest(asset_key="orders")
    ),
    "rerun_run": lambda stub, pb2, old: stub.RerunRun(
        pb2.RerunRunRequest(run_id=old.run_id)
    ),
    "rerun_backfill": lambda stub, pb2, old: stub.RerunBackfill(
        pb2.RerunBackfillRequest(backfill_id=old.backfill_id)
    ),
}


class Launches(NamedTuple):
    """A run and a backfill of the code before."""

    run_id: str
    backfill_id: str


@pytest.mark.parametrize("launch", REQUESTS.values(), ids=REQUESTS.keys())
def test_a_held_launch_fails_its_precondition(grpc_stubs, storage, launch):
    old = _repo(storage)
    old_launches = Launches(
        run_id=old.materialize(["orders"], partition_key=EU).run_id,
        backfill_id=old.backfill(["orders"], partition_keys=[EU]).backfill_id,
    )
    repo = _repo(storage, run_queue=rs.RunQueueConfig(), schema_version=2)
    runs = len(storage.get_runs())

    with _serve(repo, grpc_stubs) as (stub, pb2):
        with pytest.raises(grpc.RpcError) as error:
            launch(stub, pb2, old_launches)

    assert error.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert error.value.details() == "orders is migrating to version 2"
    assert len(storage.get_runs()) == runs


def test_a_launch_of_old_code_fails_its_precondition(grpc_stubs, storage):
    _repo(storage, schema_version=3)
    old = _repo(storage, run_queue=rs.RunQueueConfig(), schema_version=2)

    with _serve(old, grpc_stubs) as (stub, pb2):
        with pytest.raises(grpc.RpcError) as error:
            REQUESTS["materialize"](stub, pb2, None)

    assert error.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert error.value.details() == (
        "this code declares orders version 2; the stored data is at version 3"
    )
    assert storage.get_runs() == []


def test_a_dry_run_backfill_is_not_held(grpc_stubs, storage):
    """A dry run starts no run."""
    assert _repo(storage).materialize(partition_key=EU).success
    repo = _repo(storage, run_queue=rs.RunQueueConfig(), schema_version=2)

    with _serve(repo, grpc_stubs) as (stub, pb2):
        response = stub.LaunchBackfill(
            pb2.LaunchBackfillRequest(
                selection=["orders"], partition_keys=[_eu(pb2)], dry_run=True
            )
        )

    assert (response.is_dry_run, response.num_runs) == (True, 1)
