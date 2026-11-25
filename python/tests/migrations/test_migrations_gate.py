"""The asset version gate. A run that would write or read an asset whose
stored data is not at the version that its code declares runs no step:
in-process calls raise, and a new run gets no record."""

import os
import subprocess
import sys
import textwrap

import pytest

import rivers as rs
from _helpers import EXECUTORS, IP, event_types
from rivers.exceptions import AssetMigrationPendingError, AssetVersionMismatchError

FORMS = pytest.mark.parametrize("form", ["decorator", "class", "multi", "graph"])
PENDING = "^orders is migrating to version 2$"
REFUSED = "^this code declares orders version 2; the stored data is at version 3$"
EU = rs.PartitionKey.single("eu")


def _orders(form, **declared) -> tuple[list, list]:
    """The assets and tasks of an asset named ``orders`` (for ``multi``, the
    one output of ``ingest``) that declares ``declared``. Its own IO handler
    lets a run read it without materializing it."""
    declared = {"io_handler": rs.InMemoryIOHandler(), **declared}
    if form == "decorator":

        @rs.Asset(**declared)
        def orders() -> int:
            return 1

        return [orders], []

    if form == "class":
        body = {"materialize": classmethod(lambda cls: 1), **declared}
        return [type("Orders", (rs.Asset,), body)], []

    if form == "multi":

        @rs.Asset.from_multi(output_defs=[rs.AssetDef("orders")], **declared)
        def ingest():
            return {"orders": 1}

        return [ingest], []

    @rs.Task
    def load() -> int:
        return 1

    @rs.Asset.from_graph(**declared)
    def orders():
        return load()

    return [orders], [load]


def _no_others() -> tuple[list, list]:
    return [], []


def _customers() -> tuple[list, list]:
    @rs.Asset
    def customers() -> int:
        return 1

    return [customers], []


def _summary(reads):
    """A builder of ``summary``, which reads ``orders`` as an input, as a dep,
    or in a task of a graph asset."""

    def build() -> tuple[list, list]:
        if reads == "input":

            @rs.Asset
            def summary(orders: int) -> int:
                return orders

            return [summary], []

        if reads == "dep":

            @rs.Asset(deps=[rs.AssetDef.dep("orders")])
            def summary() -> int:
                return 1

            return [summary], []

        @rs.Task
        def total(value: int) -> int:
            return value

        @rs.Asset.from_graph()
        def summary(orders: int):
            return total(value=orders)

        return [summary], [total]

    return build


def _repo(storage, form, others=_no_others, executor=IP, **declared):
    """A resolved repository of ``orders`` at ``declared`` and the assets
    and tasks that ``others`` builds."""
    assets, tasks = _orders(form, **declared)
    other_assets, other_tasks = others()
    repo = rs.CodeRepository(
        assets=assets + other_assets,
        tasks=tasks + other_tasks,
        default_executor=executor,
    )
    repo.resolve(storage=storage)
    return repo


def _pending(storage, form, others=_no_others, executor=IP):
    """A repository whose ``orders`` waits for its migration to version 2:
    data stored at version 1, then code that declares version 2."""
    assert _repo(storage, form, others).materialize().success
    repo = _repo(storage, form, others, executor, schema_version=2)
    assert storage.get_asset_version("orders").state == "pending"
    return repo


@pytest.mark.parametrize("executor", EXECUTORS)
@FORMS
def test_a_migration_holds_the_runs_that_write_its_asset(storage, form, executor):
    repo = _pending(storage, form, executor=executor)
    runs = len(storage.get_runs())

    with pytest.raises(AssetMigrationPendingError, match=PENDING):
        repo.materialize()

    assert len(storage.get_runs()) == runs


@FORMS
@pytest.mark.parametrize("reads", ["input", "dep", "graph"])
def test_a_migration_holds_the_runs_that_read_its_asset(storage, form, reads):
    repo = _pending(storage, form, _summary(reads))

    with pytest.raises(AssetMigrationPendingError, match=PENDING):
        repo.materialize(["summary"])


@FORMS
def test_runs_of_other_assets_go(storage, form):
    repo = _pending(storage, form, _customers)

    assert repo.materialize(["customers"]).success


@FORMS
def test_old_code_never_writes_newer_data(storage, form):
    """Code that declares a lower version than the stored data, as during a
    rollout. Readers in the same code location deploy with the asset."""
    _repo(storage, form, _summary("dep"), schema_version=3)
    old = _repo(storage, form, _summary("dep"), schema_version=2)

    with pytest.raises(AssetVersionMismatchError, match=REFUSED):
        old.materialize()

    assert storage.get_runs() == []
    assert old.materialize(["summary"]).success


def _partitioned(storage, run_queue=None, **declared):
    """A resolved repository of a partitioned ``orders`` at ``declared``,
    with the action ``optimize`` and the job ``orders_job``."""

    def optimize(ctx) -> None:
        return None

    @rs.Asset(
        partitions_def=rs.PartitionsDefinition.static_(["eu", "us"]),
        actions=[
            rs.AssetAction(name="optimize", outcome=rs.Outcome.Unchanged)(optimize)
        ],
        **declared,
    )
    def orders() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[orders],
        jobs=[rs.Job(name="orders_job", assets=[orders])],
        default_executor=IP,
        run_queue=run_queue,
    )
    repo.resolve(storage=storage)
    return repo


LAUNCHES = {
    "materialize": lambda repo: repo.materialize(["orders"], partition_key=EU),
    "job": lambda repo: repo.get_job("orders_job").execute(partition_key=EU),
    "action": lambda repo: repo.run_action("optimize", ["orders"], partition_key=EU),
    "backfill": lambda repo: repo.backfill(["orders"], partition_keys=[EU]),
}
EACH_LAUNCH = pytest.mark.parametrize("launch", LAUNCHES.values(), ids=LAUNCHES.keys())


@EACH_LAUNCH
def test_each_launch_of_a_held_run_raises(storage, launch):
    assert _partitioned(storage).materialize(partition_key=EU).success
    repo = _partitioned(storage, schema_version=2)
    runs = len(storage.get_runs())

    with pytest.raises(AssetMigrationPendingError, match=PENDING):
        launch(repo)

    assert len(storage.get_runs()) == runs


@EACH_LAUNCH
def test_each_launch_of_old_code_raises(storage, launch):
    _partitioned(storage, schema_version=3)
    old = _partitioned(storage, schema_version=2)

    with pytest.raises(AssetVersionMismatchError, match=REFUSED):
        launch(old)

    assert storage.get_runs() == []


def test_a_dry_run_backfill_is_not_held(storage):
    """A dry run starts no run."""
    assert _partitioned(storage).materialize(partition_key=EU).success
    repo = _partitioned(storage, schema_version=2)

    result = repo.backfill(["orders"], partition_keys=[EU], dry_run=True)

    assert (result.is_dry_run, result.num_runs) == (True, 1)


STORED_RUNS = {
    "materialize": (
        lambda repo: repo._submit_run(["orders"], partition_key=EU),
        lambda repo, run_id: repo.materialize(
            ["orders"], partition_key=EU, run_id_override=run_id
        ),
    ),
    "job": (
        lambda repo: repo._submit_run(job_name="orders_job", partition_key=EU),
        lambda repo, run_id: repo.get_job("orders_job")._execute_run(
            run_id, partition_key=EU
        ),
    ),
}


@pytest.mark.parametrize(
    ("submit", "execute"), STORED_RUNS.values(), ids=STORED_RUNS.keys()
)
def test_a_held_run_that_has_a_record_fails_with_the_reason(storage, submit, execute):
    """A run that a queue or a dispatcher recorded before the migration."""
    old = _partitioned(storage, run_queue=rs.RunQueueConfig())
    assert old.materialize(partition_key=EU).success
    run_id = submit(old).run_id
    repo = _partitioned(storage, schema_version=2)

    with pytest.raises(AssetMigrationPendingError, match=PENDING):
        execute(repo, run_id)

    assert storage.get_run(run_id).status == "Failure"
    assert "RunLaunchFailed" in event_types(repo, run_id)


def test_a_step_pod_runs_the_steps_of_a_started_run():
    """A K8s step pod runs a step of a run that its run pod started before the
    migration began, and the migration's drain waits for that run. A
    subprocess, because a process reads the step-pod flag once."""
    script = textwrap.dedent(
        """
        import rivers as rs
        from rivers.testing import memory_storage

        storage = memory_storage()

        def repo(**declared):
            @rs.Asset(**declared)
            def orders() -> rs.Materialization:
                return rs.Materialization()

            repo = rs.CodeRepository(
                assets=[orders],
                default_executor=rs.Executor.in_process(),
                run_queue=rs.RunQueueConfig(),
            )
            repo.resolve(storage=storage)
            return repo

        old = repo()
        assert old.materialize().success
        run_id = old._submit_run(["orders"]).run_id
        new = repo(schema_version=2)
        assert storage.get_asset_version("orders").state == "pending"

        assert new.materialize(["orders"], run_id_override=run_id).success
        """
    )
    env = dict(os.environ, RIVERS_STEP_POD="1")
    proc = subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
