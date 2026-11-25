"""The convert of an asset migration runs as the built-in ``migrate`` action
of the asset, once, in the run that its migration started."""

import contextlib
import inspect
import time

import pytest
from pydantic import BaseModel

import rivers as rs
from _helpers import EXECUTORS, IP, event_types
from _polling import wait_for_run_terminal
from rivers._core import AutomationDaemon
from rivers.exceptions import (
    AssetDefinitionError,
    ConfigurationError,
    ExecutionError,
)

RUN = "convert-1"
NOT_STARTED = (
    "no migration of orders started this run; "
    "the migrate action runs only for a migration"
)
FORMS = pytest.mark.parametrize("form", ["decorator", "class", "multi", "graph"])
PD = rs.PartitionsDefinition.static_(["eu", "us", "ap"])


def _as_method(convert):
    """``convert`` as a classmethod of a class-form asset, sync or async as
    ``convert`` is."""
    if inspect.iscoroutinefunction(convert):

        async def method(cls, ctx):
            return await convert(ctx)

    else:

        def method(cls, ctx):
            return convert(ctx)

    return classmethod(method)


def _orders(form, convert=None, **declared) -> tuple[list, list]:
    """The assets and tasks of an asset named ``orders`` (for ``multi``, the
    one output of ``ingest``) that declares ``declared`` and the convert to
    version 2 ``convert``, a function of the context."""
    declared = {"io_handler": rs.InMemoryIOHandler(), **declared}
    if form == "class":
        body = {"materialize": classmethod(lambda cls: 1), **declared}
        if convert is not None:
            body["add_currency"] = rs.migration(to=2)(_as_method(convert))
        return [type("Orders", (rs.Asset,), body)], []

    if convert is not None:
        declared["migrations"] = [rs.AssetMigration(to=2)(convert)]
    if form == "decorator":

        @rs.Asset(**declared)
        def orders() -> int:
            return 1

        return [orders], []

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


def _repo(storage, form="decorator", readers=(), executor=IP, repo=None, **declared):
    """A resolved repository of ``orders`` and ``readers``; ``repo`` holds
    more arguments of the repository."""
    assets, tasks = _orders(form, **declared)
    code = rs.CodeRepository(
        assets=assets + list(readers),
        tasks=tasks,
        default_executor=executor,
        **(repo or {}),
    )
    code.resolve(storage=storage)
    return code


def _converting(storage, form="decorator", readers=(), keys=(None,), **code):
    """A repository of ``orders`` at version 2 with ``code``. Its data was
    stored at version 1, for each partition of ``keys``, and its migration to
    version 2 has started the run ``RUN``."""
    declared = {k: v for k, v in code.items() if k == "partitions_def"}
    old = _repo(storage, form, readers, **declared)
    for key in keys:
        assert old.materialize(partition_key=key).success
    repo = _repo(storage, form, readers, schema_version=2, **code)
    storage._put_asset_version("orders", "running", 1, target_version=2, run_id=RUN)
    return repo


@FORMS
@pytest.mark.parametrize("body", ["sync", "async"])
@pytest.mark.parametrize("executor", EXECUTORS)
def test_a_convert_runs_in_the_run_of_its_migration(storage, form, body, executor):
    seen = []

    if body == "sync":

        def add_currency(ctx):
            seen.append(ctx)

    else:

        async def add_currency(ctx):
            seen.append(ctx)

    repo = _converting(storage, form, convert=add_currency, executor=executor)

    assert repo.run_action("migrate", ["orders"], run_id_override=RUN).success

    (ctx,) = seen
    assert isinstance(ctx, rs.MigrationContext)
    assert (ctx.asset_name, ctx.from_version, ctx.to_version, ctx.run_id) == (
        "orders",
        1,
        2,
        RUN,
    )
    assert ctx.io_handler is not None
    assert event_types(repo, RUN) == ["ActionCompleted", "StepStart", "StepSuccess"]


@pytest.mark.parametrize(
    ("result", "reader"),
    [
        (None, "UpToDate"),
        (rs.ActionResult.unchanged(), "UpToDate"),
        (rs.ActionResult.materialized(), "Stale"),
    ],
    ids=["none", "unchanged", "materialized"],
)
def test_readers_go_stale_only_when_the_convert_changed_the_data(
    storage, result, reader
):
    @rs.Asset(io_handler=rs.InMemoryIOHandler())
    def summary(orders: int) -> int:
        return orders

    repo = _converting(storage, readers=[summary], convert=lambda ctx: result)

    assert repo.run_action("migrate", ["orders"], run_id_override=RUN).success

    assert storage.compute_staleness()["summary"][0] == reader


@pytest.mark.parametrize("changed", [True, False])
def test_a_convert_that_changed_the_data_materializes_each_stored_partition(
    storage, changed
):
    result = (
        rs.ActionResult.materialized(data_version="v2")
        if changed
        else rs.ActionResult.unchanged()
    )
    keys = [rs.PartitionKey.single("eu"), rs.PartitionKey.single("us")]
    repo = _converting(
        storage, keys=keys, partitions_def=PD, convert=lambda ctx: result
    )

    assert repo.run_action("migrate", ["orders"], run_id_override=RUN).success

    _, keyed = storage._stored_keys(RUN)
    materialized = sorted(key for event, key in keyed if event == "Materialization")
    versions = {
        e.data_version
        for e in storage.get_events_for_run(RUN)
        if e.event_type == "Materialization"
    }
    if changed:
        assert (materialized, versions) == (["eu", "us"], {"v2"})
    else:
        assert (materialized, versions) == ([], set())


def test_only_its_migration_runs_a_convert(storage):
    """Data stored at version 2 from the start: no migration runs."""
    calls = []
    repo = _repo(storage, schema_version=2, convert=calls.append)

    with pytest.raises(ExecutionError, match=f"^{NOT_STARTED}$"):
        repo.run_action("migrate", ["orders"])

    assert (calls, storage.get_runs()) == ([], [])


def test_a_queued_convert_run_that_no_migration_started_fails(storage):
    calls = []
    repo = _repo(
        storage,
        schema_version=2,
        convert=calls.append,
        repo={"run_queue": rs.RunQueueConfig(dequeue_interval="50ms")},
    )
    storage._create_run(
        "queued-1",
        "",
        "Queued",
        time.time_ns(),
        node_names=["orders"],
        action="migrate",
    )

    daemon = AutomationDaemon(repo=repo, storage=storage, condition_eval_interval="1s")
    daemon.start()
    try:
        run = wait_for_run_terminal(storage, "queued-1")
    finally:
        daemon.stop()

    assert run.status == "Failure"
    errors = [
        dict(event.metadata)["error"]
        for event in storage.get_events_for_run("queued-1")
        if event.event_type == "RunLaunchFailed"
    ]
    assert (errors[0], calls) == (NOT_STARTED, [])


def test_migrate_is_a_reserved_verb():
    with pytest.raises(AssetDefinitionError, match="'migrate' is a reserved verb"):
        rs.AssetAction(name="migrate", outcome=rs.Outcome.Unchanged)


class Batch(BaseModel):
    size: int = 500


class Warehouse(rs.Resource):
    schema_name: str = "public"


def test_a_convert_gets_its_config_and_resources(storage):
    seen = {}

    def add_currency(ctx: rs.MigrationContext[Batch], warehouse: Warehouse) -> None:
        seen.update(size=ctx.config.size, schema_name=warehouse.schema_name)

    repo = _converting(
        storage,
        convert=add_currency,
        repo={"resources": {"warehouse": Warehouse(schema_name="sales")}},
    )

    assert repo.run_action("migrate", ["orders"], run_id_override=RUN).success

    assert seen == {"size": 500, "schema_name": "sales"}


@pytest.mark.parametrize(
    "retry", [rs.RetryPolicy(max_retries=2), "twice"], ids=["inline", "named"]
)
def test_a_convert_runs_again_per_its_retry_policy(storage, retry):
    attempts = []

    def add_currency(ctx) -> None:
        attempts.append(ctx.to_version)
        if len(attempts) < 3:
            raise RuntimeError("the warehouse is busy")

    migration = rs.AssetMigration(to=2, retry=retry)(add_currency)
    old = _repo(storage)
    assert old.materialize().success
    repo = _repo(
        storage,
        schema_version=2,
        migrations=[migration],
        repo={"retries": {"twice": rs.RetryPolicy(max_retries=2)}},
    )
    storage._put_asset_version("orders", "running", 1, target_version=2, run_id=RUN)

    assert repo.run_action("migrate", ["orders"], run_id_override=RUN).success

    assert attempts == [2, 2, 2]
    assert event_types(repo, RUN).count("StepRetry") == 2


def test_an_unknown_retry_policy_of_a_convert_fails_resolve(storage):
    migration = rs.AssetMigration(to=2, retry="missing")(lambda ctx: None)

    with pytest.raises(
        ConfigurationError,
        match="unknown retry policy 'missing' referenced by migration to version 2 "
        "of asset 'orders'",
    ):
        _repo(storage, schema_version=2, migrations=[migration])


@contextlib.contextmanager
def _serve(repo, grpc_stubs):
    import grpc

    port = repo._start_grpc_server("127.0.0.1", 0)
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    try:
        grpc.channel_ready_future(channel).result(timeout=5)
        pb2, pb2_grpc = grpc_stubs
        yield pb2_grpc.CodeLocationServiceStub(channel), pb2
    finally:
        channel.close()
        repo._stop_grpc_server()


def test_the_ui_offers_no_migrate_action(storage, grpc_stubs):
    """The convert runs only for its migration."""
    optimize = rs.AssetAction(name="optimize", outcome=rs.Outcome.Unchanged)(
        lambda ctx: None
    )
    repo = _repo(
        storage, schema_version=2, convert=lambda ctx: None, actions=[optimize]
    )

    with _serve(repo, grpc_stubs) as (stub, pb2):
        assets = stub.GetAssetsInfo(pb2.GetAssetsInfoRequest()).assets

    (orders,) = [a for a in assets if a.asset_key == "orders"]
    assert [a.name for a in orders.actions] == ["optimize"]


def test_a_migration_context_can_be_built_for_unit_tests():
    ctx = rs.MigrationContext("orders", to_version=3)

    assert (ctx.asset_name, ctx.from_version, ctx.to_version, ctx.run_id) == (
        "orders",
        2,
        3,
        "",
    )
    assert (ctx.io_handler, ctx.config, ctx.asset_metadata) == (None, None, None)
    assert ctx.log.name == "code-repo.migrations.orders"
    assert repr(ctx) == (
        "MigrationContext(asset_name='orders', from_version=2, to_version=3, run_id='')"
    )
    assert rs.MigrationContext[Batch].__origin__ is rs.MigrationContext
    with pytest.raises(ValueError, match="^to_version must be 2 or more, got 1$"):
        rs.MigrationContext("orders", to_version=1)
