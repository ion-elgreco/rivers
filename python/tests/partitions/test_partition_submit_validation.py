"""Synchronous partition-key validation at submit time.

Covers `validate_partition_for_selection` reachable through every
execution-launching path (`materialize`, `_submit_run`, gRPC `ExecuteJob`,
gRPC `Materialize`). The previous behavior was that a queued run with a
missing or invalid partition key would only fail once the coordinator
dequeued it — these tests pin the new fail-fast behavior.
"""

from datetime import datetime

import grpc
import pytest

import rivers as rs
from rivers.exceptions import ExecutionError
from rivers.testing import embedded_storage as _embedded_storage_factory


# ── Expected error messages — exact match. The Python API surfaces just
# the validator message; the gRPC layer maps Python exceptions through
# `to_string()` which prefixes with the exception class name. ──

MISSING_KEY_MSG = (
    'Cannot materialize without partition_key: assets ["part"] have '
    "partition definitions. Provide a partition_key or exclude them from "
    "selection."
)

INVALID_STATIC_GARBAGE_MSG = (
    "Invalid partition_key Single { key: [\"garbage\"] } for asset 'part': "
    "not a member of its partition definition."
)

INVALID_TIMEWINDOW_MSG = (
    "Invalid partition_key Single { key: [\"2023-12-25\"] } for asset 'part': "
    "not a member of its partition definition."
)


def _grpc_msg(direct_msg: str) -> str:
    """gRPC `Status::internal` wraps the Python exception via `to_string()`,
    which prefixes with the exception class name."""
    return f"ExecutionError: {direct_msg}"


# ── Proto partition-key constructors — small helpers so the gRPC call
# sites stay readable. The proto's `partition_key` is now an
# `optional ProtoPartitionKey` (was: `string`); these wrap the Single /
# Multi shapes so we don't repeat the four-deep nested `pb2.X(Y(Z(...)))`
# construction at every call site. ──


def _single_pk(pb2, key: str):
    return pb2.ProtoPartitionKey(single=pb2.SinglePartitionKey(keys=[key]))


def _multi_pk(pb2, **dims: str):
    return pb2.ProtoPartitionKey(
        multi=pb2.MultiPartitionKey(
            dimensions=[
                pb2.MultiPartitionDimension(name=name, keys=[value])
                for name, value in dims.items()
            ]
        )
    )


# ── Direct Python API ──


def test_materialize_partitioned_without_key_raises():
    pd = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), partitions_def=pd)
    def part():
        return 1

    repo = rs.CodeRepository(assets=[part], default_executor=rs.Executor.in_process())

    with pytest.raises(ExecutionError) as exc:
        repo.materialize()
    assert str(exc.value) == MISSING_KEY_MSG


def test_materialize_invalid_static_key_raises():
    pd = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), partitions_def=pd)
    def part():
        return 1

    repo = rs.CodeRepository(assets=[part], default_executor=rs.Executor.in_process())

    with pytest.raises(ExecutionError) as exc:
        repo.materialize(partition_key=rs.PartitionKey.single("garbage"))
    assert str(exc.value) == INVALID_STATIC_GARBAGE_MSG


def test_materialize_valid_static_key_succeeds():
    pd = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), partitions_def=pd)
    def part():
        return 1

    repo = rs.CodeRepository(assets=[part], default_executor=rs.Executor.in_process())
    result = repo.materialize(partition_key=rs.PartitionKey.single("a"))
    assert result.success is True


def test_materialize_invalid_timewindow_key_raises():
    pd = rs.PartitionsDefinition.daily(
        start=datetime(2024, 1, 1), end=datetime(2024, 1, 5)
    )

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), partitions_def=pd)
    def part():
        return 1

    repo = rs.CodeRepository(assets=[part], default_executor=rs.Executor.in_process())

    # Out-of-range key (before `start`) must be rejected.
    with pytest.raises(ExecutionError) as exc:
        repo.materialize(partition_key=rs.PartitionKey.single("2023-12-25"))
    assert str(exc.value) == INVALID_TIMEWINDOW_MSG


def test_materialize_unpartitioned_no_key_succeeds():
    """Plain unpartitioned assets need no partition_key."""

    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def plain():
        return 1

    repo = rs.CodeRepository(assets=[plain], default_executor=rs.Executor.in_process())
    result = repo.materialize()
    assert result.success is True


# ── _submit_run path (run-queue) ──


def _make_queued_repo(asset, storage):
    repo = rs.CodeRepository(
        assets=[asset],
        default_executor=rs.Executor.in_process(),
        run_queue=rs.RunQueueConfig(),
    )
    repo.resolve(storage=storage)
    return repo


def test_submit_run_partitioned_without_key_raises(storage):
    """The run-queue path must reject the run synchronously — otherwise the
    coordinator dequeues a doomed run that fails much later."""
    pd = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), partitions_def=pd)
    def part():
        return 1

    repo = _make_queued_repo(part, storage)

    with pytest.raises(ExecutionError) as exc:
        repo._submit_run()
    assert str(exc.value) == MISSING_KEY_MSG

    # No run record should have been created.
    assert storage.get_runs(limit=10) == []


def test_submit_run_invalid_partition_key_raises(storage):
    pd = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), partitions_def=pd)
    def part():
        return 1

    repo = _make_queued_repo(part, storage)

    with pytest.raises(ExecutionError) as exc:
        repo._submit_run(partition_key=rs.PartitionKey.single("garbage"))
    assert str(exc.value) == INVALID_STATIC_GARBAGE_MSG

    assert storage.get_runs(limit=10) == []


def test_submit_run_valid_partition_key_creates_queued_run(storage):
    pd = rs.PartitionsDefinition.static_(["a", "b"])

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), partitions_def=pd)
    def part():
        return 1

    repo = _make_queued_repo(part, storage)

    handle = repo._submit_run(partition_key=rs.PartitionKey.single("a"))
    runs = storage.get_runs(limit=10)
    assert len(runs) == 1
    assert runs[0].run_id == handle.run_id
    assert runs[0].status == "Queued"
    assert runs[0].node_names == ["part"]
    assert runs[0].partition_key == rs.PartitionKey.single("a")


# ── gRPC error propagation ──

# Asset name in the gRPC fixture is `part_alpha`; rebuild expected strings
# with that asset name.
GRPC_MISSING_KEY_MSG = _grpc_msg(
    'Cannot materialize without partition_key: assets ["part_alpha", '
    '"part_beta"] have partition definitions. Provide a partition_key or '
    "exclude them from selection."
)
GRPC_BAD_KEY_PART_ALPHA = _grpc_msg(
    'Invalid partition_key Single { key: ["garbage"] } for asset '
    "'part_alpha': not a member of its partition definition."
)


@pytest.fixture(scope="module")
def partitioned_grpc_channel(grpc_stubs, tmp_path_factory):
    """gRPC server with run_queue + a partitioned job, used to verify that
    submit-time validation errors are surfaced to the gRPC client (not
    swallowed as 'thread panicked').

    Module-scoped: spinning up a gRPC server + storage is expensive and
    every test in this file just inspects validation behavior, never
    mutating shared state in a way that bleeds across tests.
    """

    pd = rs.PartitionsDefinition.static_(["a", "b", "c"])

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), partitions_def=pd)
    def part_alpha():
        return 1

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), partitions_def=pd)
    def part_beta(part_alpha):
        return part_alpha + 1

    job = rs.Job(name="part_job", assets=[part_alpha, part_beta])
    tmp_path = tmp_path_factory.mktemp("partition_grpc")
    storage = _embedded_storage_factory(str(tmp_path / "part_db"))

    repo = rs.CodeRepository(
        assets=[part_alpha, part_beta],
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


def test_grpc_execute_job_partitioned_no_key_propagates_error(
    partitioned_grpc_channel,
):
    channel, pb2, pb2_grpc, _, storage = partitioned_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    with pytest.raises(grpc.RpcError) as exc:
        stub.ExecuteJob(pb2.ExecuteJobRequest(job_name="part_job"))
    assert exc.value.code() == grpc.StatusCode.INTERNAL
    # Regression: previously this error was swallowed and the gRPC client
    # saw the unhelpful "thread panicked" instead of the real message.
    # ExecuteJob routes through the daemon's RunDispatcherKind, which wraps
    # per-request errors with "(job 'X', partition Y): " context — assert
    # the underlying message is present rather than exact-matching the wrap.
    assert GRPC_MISSING_KEY_MSG in exc.value.details()

    # And no run was queued.
    assert storage.get_runs(limit=10) == []


def test_grpc_execute_job_invalid_partition_key_propagates_error(
    partitioned_grpc_channel,
):
    channel, pb2, pb2_grpc, _, storage = partitioned_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    with pytest.raises(grpc.RpcError) as exc:
        stub.ExecuteJob(
            pb2.ExecuteJobRequest(
                job_name="part_job", partition_key=_single_pk(pb2, "garbage")
            )
        )
    assert exc.value.code() == grpc.StatusCode.INTERNAL
    assert GRPC_BAD_KEY_PART_ALPHA in exc.value.details()

    assert storage.get_runs(limit=10) == []


def test_grpc_execute_job_valid_partition_key_queues_run(
    partitioned_grpc_channel,
):
    channel, pb2, pb2_grpc, _, storage = partitioned_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    resp = stub.ExecuteJob(
        pb2.ExecuteJobRequest(job_name="part_job", partition_key=_single_pk(pb2, "a"))
    )
    assert resp.success is True
    assert resp.run_id != ""

    import time

    time.sleep(0.5)  # let the spawned thread persist the record

    runs = storage.get_runs(limit=10)
    assert len(runs) == 1
    assert runs[0].run_id == resp.run_id
    assert runs[0].status == "Queued"
    assert runs[0].job_name == "part_job"
    assert set(runs[0].node_names) == {"part_alpha", "part_beta"}
    assert runs[0].partition_key == rs.PartitionKey.single("a")


def test_grpc_materialize_invalid_partition_key_propagates_error(
    partitioned_grpc_channel,
):
    channel, pb2, pb2_grpc, _, _ = partitioned_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    with pytest.raises(grpc.RpcError) as exc:
        stub.Materialize(
            pb2.MaterializeRequest(
                selection=["part_alpha"],
                partition_key=_single_pk(pb2, "garbage"),
            )
        )
    assert exc.value.code() == grpc.StatusCode.INTERNAL
    assert exc.value.details() == GRPC_BAD_KEY_PART_ALPHA


# ── Multi partition gRPC execute path ──


@pytest.fixture(scope="module")
def multi_partitioned_grpc_channel(grpc_stubs, tmp_path_factory):
    """gRPC server with a Multi-partitioned job. The UI submits keys
    encoded as `"dim=val|dim=val"`; the server's parse_partition_key_string
    rebuilds them into PyPartitionKey::Multi before validation runs."""
    pd = rs.PartitionsDefinition.multi(
        {
            "color": rs.PartitionsDefinition.static_(["r", "g"]),
            "size": rs.PartitionsDefinition.static_(["s", "m"]),
        }
    )

    @rs.Asset(io_handler=rs.InMemoryIOHandler(), partitions_def=pd)
    def cell():
        return 1

    job = rs.Job(name="multi_job", assets=[cell])
    tmp_path = tmp_path_factory.mktemp("multi_partition_grpc")
    storage = _embedded_storage_factory(str(tmp_path / "multi_db"))

    repo = rs.CodeRepository(
        assets=[cell],
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


def test_grpc_execute_job_multi_partition_key_queues_run(
    multi_partitioned_grpc_channel,
):
    """Regression for the silent-failure bug: a Multi-partitioned job
    with a `dim=val|dim=val` key must round-trip into PyPartitionKey::Multi
    and pass validate_partition_key, not be rejected as a Single key."""
    channel, pb2, pb2_grpc, _, storage = multi_partitioned_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    resp = stub.ExecuteJob(
        pb2.ExecuteJobRequest(
            job_name="multi_job",
            partition_key=_multi_pk(pb2, color="r", size="s"),
        )
    )
    assert resp.success is True
    assert resp.run_id != ""

    import time

    time.sleep(0.5)

    runs = storage.get_runs(limit=10)
    assert len(runs) == 1
    assert runs[0].run_id == resp.run_id
    assert runs[0].status == "Queued"
    assert runs[0].job_name == "multi_job"


def test_grpc_execute_job_multi_partition_invalid_dimension_value_rejected(
    multi_partitioned_grpc_channel,
):
    """A Multi key whose value is outside the dimension's static keys
    must be rejected at submit time, not silently queued."""
    channel, pb2, pb2_grpc, _, storage = multi_partitioned_grpc_channel
    stub = pb2_grpc.CodeLocationServiceStub(channel)

    runs_before = len(storage.get_runs(limit=10))

    with pytest.raises(grpc.RpcError) as exc:
        stub.ExecuteJob(
            pb2.ExecuteJobRequest(
                job_name="multi_job",
                partition_key=_multi_pk(pb2, color="purple", size="s"),
            )
        )
    assert exc.value.code() == grpc.StatusCode.INTERNAL
    assert "Invalid partition_key" in exc.value.details()

    # No new run got queued.
    assert len(storage.get_runs(limit=10)) == runs_before
