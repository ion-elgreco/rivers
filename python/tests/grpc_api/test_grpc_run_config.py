"""gRPC launches carry the launch document (issue #203).

The document rides the run record, so every launcher — direct dispatch, the
run queue, backfill children, reruns — applies the config values and
metadata overrides an in-process ``materialize(config=...)`` does.
``GetAssetsInfo`` exposes each config class's JSON schema and each asset's
metadata so the UI can pre-fill its editor.
"""

import pytest

pytest.importorskip("grpc")

import json
import os

import grpc
import rivers as rs
from _polling import wait_for_run_terminal
from pydantic import BaseModel, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings
from rivers._core import AutomationDaemon

_STORE: dict = {}
_SEEN: dict = {}


class DictIOHandler(rs.BaseIOHandler):
    def handle_output(self, context: rs.OutputContext, obj):
        _STORE[context.asset_name] = obj
        _SEEN.setdefault("output_metadata", {})[context.asset_name] = dict(
            context.asset_metadata or {}
        )

    def load_input(self, context: rs.InputContext):
        return _STORE.get(context.asset_name, 0)


class ThresholdConfig(BaseModel):
    threshold: float = 0.5
    max_retries: int = 3


class CompactConfig(BaseModel):
    target_size_mb: int = 128


class StrictConfig(BaseModel):
    """What only the class can check: a pattern, a bound, a list, a required
    field and a validator of its own."""

    code: str = Field("ab", pattern=r"^[a-z]+$")
    limit: int = Field(1, ge=1)
    tags: list[int] = []
    token: str

    @field_validator("code")
    @classmethod
    def _not_reserved(cls, value: str) -> str:
        if value == "nope":
            raise ValueError("'nope' is reserved")
        return value


class EnvConfig(BaseSettings):
    """A field the run's environment may set: left unset, it is reported as
    `missing` and does not stop a launch."""

    token_from_env: str
    batch: int = 10


class DbResource(rs.Resource):
    dsn: str = "memory://"
    pool_size: int = Field(2, ge=1)
    token: SecretStr = SecretStr("s3cret")


def _build_repo(tmp_path, run_queue=None):
    import obstore.store

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
            (
                context.partition_key,
                context.config.threshold,
                (context.asset_metadata or {}).get("tier"),
            )
        )
        return context.config.threshold

    @rs.Asset(io_handler=handler)
    def strict(context: rs.AssetExecutionContext[StrictConfig]):
        return context.config.token

    @rs.Asset(io_handler=handler)
    def from_env(context: rs.AssetExecutionContext[EnvConfig]):
        return context.config.token_from_env

    @rs.Asset(io_handler=handler, metadata={"tier": "bronze"})
    def tagged(context: rs.AssetExecutionContext):
        return dict(context.asset_metadata or {})

    # A picklable handler: a `rivers/executor` override may move these
    # steps to a worker process (two of them: a lone step stays in-process).
    pickled = rs.PickleIOHandler(
        store=obstore.store.LocalStore(str(tmp_path / "pid"), mkdir=True)
    )

    @rs.Asset(io_handler=pickled)
    def pid():
        return os.getpid()

    @rs.Asset(io_handler=pickled)
    def pid2():
        return os.getpid()

    @rs.Task
    def tidy(context: rs.TaskExecutionContext) -> int:
        return 1

    @rs.Asset(io_handler=handler)
    def uses_db(db: DbResource):
        return f"{db.dsn}/{db.pool_size}/{db.token.get_secret_value()}"

    # A bash task has no function, so no config class.
    shell = rs.BashTask(name="shell", command="true")

    job = rs.Job(name="cfg_job", assets=[configured])
    tidy_job = rs.Job(name="tidy_job", assets=[tidy])
    shell_job = rs.Job(name="shell_job", assets=[shell, configured])
    return rs.CodeRepository(
        assets=[
            plain,
            configured,
            part_cfg,
            strict,
            from_env,
            tagged,
            pid,
            pid2,
            uses_db,
        ],
        tasks=[tidy, shell],
        jobs=[job, tidy_job, shell_job],
        resources={"db": DbResource(), "store": handler},
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
def direct(grpc_stubs, tmp_path):
    """Direct dispatch: a launch runs on its own thread right away."""
    repo = _build_repo(tmp_path)
    channel, stub, pb2 = _connect(repo, grpc_stubs)
    yield stub, pb2, repo
    channel.close()
    repo._stop_grpc_server()


@pytest.fixture
def queued(grpc_stubs, storage, tmp_path):
    """Run queue + daemon: a launch is a Queued record the coordinator
    hands to the local run backend."""
    repo = _build_repo(
        tmp_path,
        run_queue=rs.RunQueueConfig(max_concurrent_runs=2, dequeue_interval="50ms"),
    )
    repo.resolve(storage=storage)
    channel, stub, pb2 = _connect(repo, grpc_stubs)
    daemon = AutomationDaemon(repo=repo, storage=storage)
    daemon.start()
    yield stub, pb2, repo
    daemon.stop()
    channel.close()
    repo._stop_grpc_server()


def _stored(**per_asset):
    """The launch document with `config` overrides, one asset per keyword."""
    return {"assets": {name: {"config": fields} for name, fields in per_asset.items()}}


def _cfg(**per_asset):
    return json.dumps(_stored(**per_asset))


def _at(asset):
    """Where an asset's config sits in the document, as `ValidateConfig` reports it."""
    return ("assets", asset, "config")


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
    assert dict(by_key["tagged"].metadata) == {"tier": "bronze"}
    assert dict(by_key["plain"].metadata) == {}

    (compact,) = [a for a in by_key["configured"].actions if a.name == "compact"]
    assert json.loads(compact.config_schema)["properties"]["target_size_mb"] == {
        "default": 128,
        "title": "Target Size Mb",
        "type": "integer",
    }


def test_get_assets_info_exposes_resource_schemas(direct):
    """Each overridable resource with its class schema: an instance's current
    values as the defaults, no field required, a secret's value left out.
    An IO handler is not listed."""
    stub, pb2, _ = direct
    resources = {
        r.key: json.loads(r.config_schema)
        for r in stub.GetAssetsInfo(pb2.GetAssetsInfoRequest()).resources
    }
    assert list(resources) == ["db"]
    schema = resources["db"]
    assert schema["title"] == "DbResource"
    assert "required" not in schema
    assert schema["properties"]["dsn"]["default"] == "memory://"
    assert schema["properties"]["pool_size"]["default"] == 2
    assert schema["properties"]["token"]["writeOnly"] is True
    assert "default" not in schema["properties"]["token"]


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
    assert run.config == _stored(configured={"threshold": 0.9})


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
        ("[1]", "config must be a JSON object"),
        ('{"configured": {"x": 1}}', "unknown section 'configured'; expected assets"),
        (
            '{"assets": {"other": {"config": {}}}}',
            "config names 'other', which this launch does not run",
        ),
        (
            '{"assets": {"configured": 1}}',
            "config.assets.configured must be a JSON object",
        ),
        (
            '{"assets": {"configured": {"metadata": {"k": 1}}}}',
            "config.assets.configured.metadata.k must be a string",
        ),
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
    assert run.config == _stored(configured={"target_size_mb": 512})


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
    assert run.config == _stored(configured={"max_retries": 7})


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
    assert run.config == _stored(configured={"threshold": 0.9})


def _launch_configured(stub, pb2, kind, fields):
    """Launch `configured` as `kind` with `fields` for the class that run uses."""
    config = _cfg(configured=fields)
    if kind == "materialize":
        return stub.Materialize(
            pb2.MaterializeRequest(selection=["configured"], config=config)
        ).run_id
    if kind == "job":
        return stub.ExecuteJob(
            pb2.ExecuteJobRequest(job_name="cfg_job", config=config)
        ).run_id
    return stub.RunAction(
        pb2.RunActionRequest(action="compact", selection=["configured"], config=config)
    ).run_id


@pytest.mark.parametrize(
    ("kind", "first", "updated", "seen"),
    [
        (
            "materialize",
            {"threshold": 0.9},
            {"threshold": 0.1},
            lambda: _STORE["configured"]["threshold"],
        ),
        (
            "job",
            {"max_retries": 7},
            {"max_retries": 2},
            lambda: _STORE["configured"]["max_retries"],
        ),
        (
            "action",
            {"target_size_mb": 512},
            {"target_size_mb": 64},
            lambda: _SEEN["compact"],
        ),
    ],
)
def test_rerun_with_config_replaces_the_stored_document(
    direct, kind, first, updated, seen
):
    stub, pb2, repo = direct
    first_id = _launch_configured(stub, pb2, kind, first)
    assert wait_for_run_terminal(repo.storage, first_id).status == "Success"

    rerun = stub.RerunRun(
        pb2.RerunRunRequest(run_id=first_id, config=_cfg(configured=updated))
    )
    run = wait_for_run_terminal(repo.storage, rerun.run_id)
    assert run.status == "Success"
    assert seen() == next(iter(updated.values()))
    assert run.config == _stored(configured=updated)
    assert repo.storage.get_run(first_id).config == _stored(configured=first)


def test_rerun_with_empty_config_runs_the_definitions(direct):
    stub, pb2, repo = direct
    first_id = _launch_configured(stub, pb2, "materialize", {"threshold": 0.9})
    assert wait_for_run_terminal(repo.storage, first_id).status == "Success"

    rerun = stub.RerunRun(pb2.RerunRunRequest(run_id=first_id, config=""))
    run = wait_for_run_terminal(repo.storage, rerun.run_id)
    assert run.status == "Success"
    assert _STORE["configured"] == {"threshold": 0.5, "max_retries": 3}
    assert run.config is None


@pytest.mark.parametrize(
    ("config", "detail"),
    [
        (_cfg(configured={"threshold": "hot"}), "assets.configured.config.threshold"),
        (_cfg(plain={"x": 1}), "plain"),
    ],
    ids=["value-the-class-refuses", "asset-outside-the-run"],
)
def test_rerun_rejects_a_bad_config(direct, config, detail):
    stub, pb2, repo = direct
    first_id = _launch_configured(stub, pb2, "job", {"max_retries": 7})
    assert wait_for_run_terminal(repo.storage, first_id).status == "Success"
    runs_before = len(repo.storage.get_runs(limit=100))

    with pytest.raises(grpc.RpcError) as exc:
        stub.RerunRun(pb2.RerunRunRequest(run_id=first_id, config=config))
    assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert detail in exc.value.details()
    assert len(repo.storage.get_runs(limit=100)) == runs_before


@pytest.mark.parametrize("launch", ["materialize", "execute_job"])
def test_a_launch_with_a_bash_task_checks_the_rest(direct, launch):
    stub, pb2, repo = direct
    config = _cfg(configured={"threshold": 0.2})
    if launch == "materialize":
        run_id = stub.Materialize(
            pb2.MaterializeRequest(selection=["shell", "configured"], config=config)
        ).run_id
    else:
        run_id = stub.ExecuteJob(
            pb2.ExecuteJobRequest(job_name="shell_job", config=config)
        ).run_id
    run = wait_for_run_terminal(repo.storage, run_id)
    assert run.status == "Success"
    assert _STORE["configured"]["threshold"] == 0.2

    with pytest.raises(grpc.RpcError) as exc:
        stub.ExecuteJob(
            pb2.ExecuteJobRequest(
                job_name="shell_job", config=_cfg(configured={"threshold": "hot"})
            )
        )
    assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert "assets.configured.config.threshold" in exc.value.details()


# ── Backfills ──


def test_launch_backfill_keeps_the_document_for_every_child_run(direct):
    stub, pb2, repo = direct
    document = {
        "assets": {
            "part_cfg": {"config": {"threshold": 0.25}, "metadata": {"tier": "gold"}}
        }
    }
    launch = stub.LaunchBackfill(
        pb2.LaunchBackfillRequest(
            selection=["part_cfg"],
            partition_keys=[_single(pb2, "a"), _single(pb2, "b")],
            max_concurrency=1,
            config=json.dumps(document),
        )
    )
    # A daemon picks the Requested backfill up later, from the record alone.
    repo.execute_backfill(launch.backfill_id)

    assert sorted(_SEEN["part_cfg"]) == [("a", 0.25, "gold"), ("b", 0.25, "gold")]
    status = repo.get_backfill(launch.backfill_id)
    assert status is not None and status.status == "CompletedSuccess"
    for run_id in status.run_ids:
        run = repo.storage.get_run(run_id)
        assert run is not None
        assert run.config == document


# ── Metadata ──


def test_materialize_applies_metadata_overrides_for_the_run_only(direct):
    stub, pb2, repo = direct
    document = {"assets": {"tagged": {"metadata": {"tier": "gold", "owner": "me"}}}}
    resp = stub.Materialize(
        pb2.MaterializeRequest(selection=["tagged"], config=json.dumps(document))
    )
    run = wait_for_run_terminal(repo.storage, resp.run_id)
    assert run.status == "Success"
    # The step and the IO handler see the merged metadata; the record keeps it.
    assert _STORE["tagged"] == {"tier": "gold", "owner": "me"}
    assert _SEEN["output_metadata"]["tagged"] == {"tier": "gold", "owner": "me"}
    assert run.config == document

    # The definition is unchanged for the next run.
    resp = stub.Materialize(pb2.MaterializeRequest(selection=["tagged"]))
    assert wait_for_run_terminal(repo.storage, resp.run_id).status == "Success"
    assert _STORE["tagged"] == {"tier": "bronze"}


def test_metadata_override_picks_the_executor(direct):
    """`rivers/executor` in the document moves steps to another executor."""
    stub, pb2, repo = direct
    resp = stub.Materialize(pb2.MaterializeRequest(selection=["pid", "pid2"]))
    assert wait_for_run_terminal(repo.storage, resp.run_id).status == "Success"
    assert repo.load_node("pid") == os.getpid()
    assert repo.load_node("pid2") == os.getpid()

    moved = {"metadata": {"rivers/executor": "parallel"}}
    document = {"assets": {"pid": moved, "pid2": moved}}
    resp = stub.Materialize(
        pb2.MaterializeRequest(selection=["pid", "pid2"], config=json.dumps(document))
    )
    assert wait_for_run_terminal(repo.storage, resp.run_id).status == "Success"
    assert repo.load_node("pid") != os.getpid()
    assert repo.load_node("pid2") != os.getpid()


def test_execution_picks_the_runs_executor(direct):
    stub, pb2, repo = direct
    document = {"execution": {"executor": "parallel", "max_workers": 2}}
    resp = stub.Materialize(
        pb2.MaterializeRequest(selection=["pid", "pid2"], config=json.dumps(document))
    )
    run = wait_for_run_terminal(repo.storage, resp.run_id)
    assert run.status == "Success"
    assert repo.load_node("pid") != os.getpid()
    assert repo.load_node("pid2") != os.getpid()
    assert run.config == document


@pytest.mark.parametrize(
    ("execution", "detail"),
    [
        ({"executor": "kubernetes"}, "executor must be one of in_process, parallel"),
        ({"max_workers": 2}, "max_workers applies to the parallel executor"),
        ({"executor": "parallel", "max_async_concurrent": 0}, "positive integer"),
    ],
)
def test_execution_shape_is_rejected_at_the_boundary(direct, execution, detail):
    stub, pb2, repo = direct
    document = json.dumps({"execution": execution})
    for call in (
        lambda: stub.ValidateConfig(
            pb2.ValidateConfigRequest(selection=["plain"], config=document)
        ),
        lambda: stub.Materialize(
            pb2.MaterializeRequest(selection=["plain"], config=document)
        ),
    ):
        with pytest.raises(grpc.RpcError) as exc:
            call()
        assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert detail in exc.value.details()
    assert repo.storage.get_runs(limit=10) == []


def test_metadata_on_a_task_is_refused(direct):
    stub, pb2, repo = direct
    document = {"assets": {"tidy": {"metadata": {"k": "v"}}}}
    assert _validate(stub, pb2, document, ["tidy"]) == [
        (
            ("assets", "tidy", "metadata"),
            (),
            "invalid",
            "metadata overrides apply to assets; 'tidy' is a task",
        )
    ]
    with pytest.raises(grpc.RpcError) as exc:
        stub.ExecuteJob(
            pb2.ExecuteJobRequest(job_name="tidy_job", config=json.dumps(document))
        )
    assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert (
        "assets.tidy.metadata: metadata overrides apply to assets"
        in exc.value.details()
    )
    assert repo.storage.get_runs(limit=10) == []


# ── Resources ──


def test_materialize_rebuilds_a_resource_for_the_run(direct):
    stub, pb2, repo = direct
    document = {"resources": {"db": {"dsn": "postgres://replica", "pool_size": 8}}}
    resp = stub.Materialize(
        pb2.MaterializeRequest(selection=["uses_db"], config=json.dumps(document))
    )
    run = wait_for_run_terminal(repo.storage, resp.run_id)
    assert run.status == "Success"
    # The secret kept its value through the rebuild.
    assert _STORE["uses_db"] == "postgres://replica/8/s3cret"
    assert run.config == document

    resp = stub.Materialize(pb2.MaterializeRequest(selection=["uses_db"]))
    assert wait_for_run_terminal(repo.storage, resp.run_id).status == "Success"
    assert _STORE["uses_db"] == "memory:///2/s3cret"


def test_validate_config_checks_resources(direct):
    stub, pb2, repo = direct
    assert _validate(stub, pb2, {"resources": {"db": {"pool_size": 3}}}, []) == []
    errors = _validate(
        stub,
        pb2,
        {"resources": {"db": {"pool_size": 0}, "nope": {"x": 1}, "store": {"x": 1}}},
        [],
    )
    assert errors == [
        (
            ("resources", "db"),
            ("pool_size",),
            "greater_than_equal",
            "Input should be greater than or equal to 1",
        ),
        (("resources", "nope"), (), "unknown", "no resource 'nope'"),
        (
            ("resources", "store"),
            (),
            "invalid",
            "an IO handler cannot be overridden per run",
        ),
    ]

    with pytest.raises(grpc.RpcError) as exc:
        stub.Materialize(
            pb2.MaterializeRequest(
                selection=["uses_db"],
                config=json.dumps({"resources": {"db": {"pool_size": 0}}}),
            )
        )
    assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert "config: resources.db.pool_size" in exc.value.details()
    assert repo.storage.get_runs(limit=10) == []


# ── Run queue ──


def test_queued_materialize_applies_the_document_when_dequeued(queued):
    """The queued record carries the document; the local run backend reads
    it from the record when the coordinator launches the run."""
    stub, pb2, repo = queued
    document = {
        "assets": {
            "configured": {"config": {"threshold": 0.7}},
            "tagged": {"metadata": {"tier": "gold"}},
        }
    }
    resp = stub.Materialize(
        pb2.MaterializeRequest(
            selection=["configured", "tagged"], config=json.dumps(document)
        )
    )
    assert resp.status == "queued"
    queued_record = repo.storage.get_run(resp.run_id)
    assert queued_record is not None
    assert queued_record.config == document

    run = wait_for_run_terminal(repo.storage, resp.run_id)
    assert run.status == "Success"
    assert _STORE["configured"] == {"threshold": 0.7, "max_retries": 3}
    assert _STORE["tagged"] == {"tier": "gold"}


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
    assert run.config == _stored(configured={"max_retries": 9})


# ── ValidateConfig ──


def _validate(stub, pb2, document, selection, action=None):
    """`(path, loc, kind, message)` per error reported for `document`."""
    request = pb2.ValidateConfigRequest(
        selection=list(selection), config=json.dumps(document)
    )
    if action is not None:
        request.action = action
    return [
        (
            tuple(e.path),
            tuple(p.key if p.HasField("key") else p.index for p in e.loc),
            e.kind,
            e.message,
        )
        for e in stub.ValidateConfig(request).errors
    ]


def test_validate_config_skips_a_bash_task(direct):
    stub, pb2, _ = direct
    assert _validate(stub, pb2, {}, ["shell"]) == []
    errors = _validate(
        stub,
        pb2,
        {"assets": {"configured": {"config": {"threshold": "hot"}}}},
        ["shell", "configured"],
    )
    assert [e[:3] for e in errors] == [
        (_at("configured"), ("threshold",), "float_parsing")
    ]


def test_validate_config_reports_what_the_class_rejects(direct):
    stub, pb2, _ = direct
    valid = _stored(strict={"token": "t", "code": "ab", "limit": 2})
    assert _validate(stub, pb2, valid, ["strict"]) == []

    errors = _validate(
        stub,
        pb2,
        _stored(strict={"token": 1, "code": "AB", "limit": 0, "tags": [1, "x"]}),
        ["strict"],
    )
    assert {(loc, kind) for _, loc, kind, _ in errors} == {
        (("token",), "string_type"),
        (("code",), "string_pattern_mismatch"),
        (("limit",), "greater_than_equal"),
        (("tags", 1), "int_parsing"),
    }
    assert {path for path, *_ in errors} == {_at("strict")}

    # A validator's own message; a field a plain model needs is `required`.
    errors = _validate(stub, pb2, _stored(strict={"code": "nope"}), ["strict"])
    assert (
        _at("strict"),
        ("code",),
        "value_error",
        "Value error, 'nope' is reserved",
    ) in errors
    assert (_at("strict"), ("token",), "required", "Field required") in errors

    # A BaseSettings field the environment may set is `missing`, reported
    # even when the document says nothing about the asset.
    assert _validate(stub, pb2, {}, ["from_env"]) == [
        (_at("from_env"), ("token_from_env",), "missing", "Field required")
    ]


def test_validate_config_uses_the_verbs_class_and_skips_assets_without_one(direct):
    stub, pb2, _ = direct
    errors = _validate(
        stub,
        pb2,
        _stored(configured={"target_size_mb": "big"}),
        ["configured"],
        action="compact",
    )
    assert [(loc, kind) for _, loc, kind, _ in errors] == [
        (("target_size_mb",), "int_parsing")
    ]
    # The asset's own class ignores a field it does not know, as pydantic does.
    assert (
        _validate(
            stub, pb2, _stored(configured={"target_size_mb": "big"}), ["configured"]
        )
        == []
    )
    # No config class: nothing to check. An empty document is still checked
    # against the selection's classes.
    assert _validate(stub, pb2, _stored(plain={"x": 1}), ["plain"]) == []
    assert _validate(stub, pb2, {}, ["strict"]) == [
        (_at("strict"), ("token",), "required", "Field required")
    ]


@pytest.mark.parametrize(
    "config",
    ["[1]", '{"assets": {"other": {"config": {}}}}', '{"assets": {"strict": 1}}'],
)
def test_validate_config_rejects_a_malformed_document(direct, config):
    stub, pb2, _ = direct
    with pytest.raises(grpc.RpcError) as exc:
        stub.ValidateConfig(
            pb2.ValidateConfigRequest(selection=["strict"], config=config)
        )
    assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_launch_rejects_values_the_class_rejects(direct):
    """The same check guards a launch, so no run exists for a config its
    class rejects at start, nor for a plain model's field left unset. A
    BaseSettings field left unset passes: the run's environment may fill
    it, which this process cannot see."""
    stub, pb2, repo = direct
    with pytest.raises(grpc.RpcError) as exc:
        stub.Materialize(
            pb2.MaterializeRequest(
                selection=["configured"], config=_cfg(configured={"threshold": "hot"})
            )
        )
    assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert (
        "config: assets.configured.config.threshold: Input should be a valid number"
        in exc.value.details()
    )

    with pytest.raises(grpc.RpcError) as exc:
        stub.RunAction(
            pb2.RunActionRequest(
                action="compact",
                selection=["configured"],
                config=_cfg(configured={"target_size_mb": "big"}),
            )
        )
    assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert "assets.configured.config.target_size_mb" in exc.value.details()

    with pytest.raises(grpc.RpcError) as exc:
        stub.ExecuteJob(
            pb2.ExecuteJobRequest(
                job_name="cfg_job", config=_cfg(configured={"max_retries": 1.5})
            )
        )
    assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT

    with pytest.raises(grpc.RpcError) as exc:
        stub.LaunchBackfill(
            pb2.LaunchBackfillRequest(
                selection=["part_cfg"],
                partition_keys=[_single(pb2, "a")],
                max_concurrency=1,
                config=_cfg(part_cfg={"threshold": "hot"}),
            )
        )
    assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT

    for request in (
        pb2.MaterializeRequest(
            selection=["strict"], config=_cfg(strict={"code": "ab"})
        ),
        pb2.MaterializeRequest(selection=["strict"]),
    ):
        with pytest.raises(grpc.RpcError) as exc:
            stub.Materialize(request)
        assert exc.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert (
            "config: assets.strict.config.token: Field required" in exc.value.details()
        )
    assert repo.storage.get_runs(limit=10) == []

    resp = stub.Materialize(pb2.MaterializeRequest(selection=["from_env"]))
    assert wait_for_run_terminal(repo.storage, resp.run_id).status == "Failure"
