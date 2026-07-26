"""Guards the API claims made by ``skills/migrate-to-rivers``.

The skill's reference files document a signature-exact rivers surface for AI agents
porting Dagster/Prefect projects. If an API is renamed or its semantics change, these
tests fail and the skill must be updated with it — otherwise agents emit code that
looks right and does not run.
"""

from datetime import datetime

import pytest
from obstore.store import LocalStore
from pydantic import BaseModel

import rivers as rs

# ---------------------------------------------------------------------------
# Names the reference files spell out. A rename here rots the skill silently.
# ---------------------------------------------------------------------------

PARTITION_DEF_FACTORIES = [
    "daily",
    "hourly",
    "time_window",
    "static_",
    "multi",
    "dynamic",
]

PARTITION_MAPPINGS = [
    "identity",
    "all_partitions",
    "static_",
    "time_window",
    "multi",
    "multi_to_single",
    "specific_partitions",
    "for_keys",
    "subset",
]

CONDITION_LEAVES = [
    "eager",
    "on_cron",
    "on_missing",
    "missing",
    "in_progress",
    "execution_failed",
    "newly_updated",
    "newly_requested",
    "code_version_changed",
    "cron_tick_passed",
    "in_latest_time_window",
    "initial_evaluation",
    "data_version_changed",
    "backfill_in_progress",
    "in_flight",
    "will_be_requested",
    "last_run_includes_target",
    "last_executed_with_tags",
    "has_run_with_tags",
    "all_runs_have_tags",
    "any_deps_missing",
    "any_deps_in_progress",
    "any_deps_updated",
    "any_deps_match",
    "all_deps_match",
    "all_deps_updated_since_cron",
]

CONDITION_METHODS = [
    "newly_true",
    "since",
    "since_last_handled",
    "replace",
    "without",
    "with_label",
    "on_selected",
]

METADATA_FACTORIES = [
    "text",
    "int",
    "float_",
    "bool_",
    "url",
    "path",
    "json",
    "md",
    "timestamp",
    "null",
    "bytes",
    "duration",
    "sql",
    "code_block",
    "image",
    "percentage",
    "list_",
    "date_range",
    "schema",
    "data_version",
]

BACKOFF_FACTORIES = ["constant", "linear", "exponential", "fixed"]


@pytest.mark.parametrize("name", PARTITION_DEF_FACTORIES)
def test_partitions_definition_factories_exist(name):
    assert hasattr(rs.PartitionsDefinition, name)


@pytest.mark.parametrize("name", PARTITION_MAPPINGS)
def test_partition_mapping_factories_exist(name):
    assert hasattr(rs.PartitionMapping, name)


@pytest.mark.parametrize("name", CONDITION_LEAVES + CONDITION_METHODS)
def test_automation_condition_surface_exists(name):
    assert hasattr(rs.AutomationCondition, name)


@pytest.mark.parametrize("name", METADATA_FACTORIES)
def test_metadata_value_factories_exist(name):
    assert hasattr(rs.MetadataValue, name)


@pytest.mark.parametrize("name", BACKOFF_FACTORIES)
def test_backoff_factories_exist(name):
    assert hasattr(rs.Backoff, name)


# ---------------------------------------------------------------------------
# Gotchas the skill warns about. Each asserts the trap is still a trap — if one
# starts passing the other way, the corresponding warning must be deleted.
# ---------------------------------------------------------------------------


def test_asset_has_no_description_kwarg():
    """Dagster's `description=` does not exist; the skill says use a docstring."""
    with pytest.raises(TypeError):
        rs.Asset(description="nope")


def test_partition_start_rejects_a_string():
    """Dagster takes `start_date="2024-01-01"`; rivers requires a datetime."""
    with pytest.raises(TypeError):
        rs.PartitionsDefinition.daily(start="2024-01-01")


def test_no_weekly_or_monthly_partition_helpers():
    """The skill routes these through `time_window(cron_schedule=...)`."""
    assert not hasattr(rs.PartitionsDefinition, "weekly")
    assert not hasattr(rs.PartitionsDefinition, "monthly")


def test_schedule_requires_a_job_name():
    """No asset-targeting form like Dagster's `target=[...]`."""
    with pytest.raises(TypeError):
        rs.Schedule(cron_schedule="0 2 * * *")


def test_sensor_context_has_no_update_cursor():
    """Cursors are returned on SensorResult, not set imperatively."""
    assert not hasattr(rs.SensorEvaluationContext, "update_cursor")


def test_automation_condition_has_no_allow_or_ignore():
    """Dagster's `.allow()`/`.ignore()`; rivers uses `.without()`/`.on_selected()`."""
    condition = rs.AutomationCondition.missing()
    assert not hasattr(condition, "allow")
    assert not hasattr(condition, "ignore")


def test_external_asset_requires_an_io_handler():
    with pytest.raises(rs.exceptions.AssetDefinitionError):
        rs.Asset.external(name="feed")


def test_materialize_excludes_upstream_by_default():
    import inspect

    param = inspect.signature(rs.CodeRepository.materialize).parameters[
        "include_upstream"
    ]
    assert param.default is False


def test_graph_asset_tasks_must_be_registered():
    """Registering only the graph asset is not enough — its tasks need `tasks=[...]`."""

    @rs.Task
    def download() -> bytes:
        return b"raw-bytes"

    @rs.Task
    def parse(raw: bytes) -> list[dict]:
        return [{"n": len(raw)}]

    @rs.Asset.from_graph()
    def documents():
        return parse(download())

    with pytest.raises(rs.exceptions.GraphValidationError):
        rs.CodeRepository(assets=[documents]).validate()

    rs.CodeRepository(assets=[documents], tasks=[download, parse]).validate()


# ---------------------------------------------------------------------------
# The reference files' worked examples, executed end to end.
# ---------------------------------------------------------------------------


def test_asset_graph_example_materializes():
    @rs.Asset
    def users() -> list[dict]:
        return [{"id": 1, "active": True}]

    @rs.Asset(group="analytics", code_version="v2")
    def active_users(users: list[dict]) -> list[dict]:
        return [u for u in users if u["active"]]

    with rs.CodeRepository(
        assets=[users, active_users], default_executor=rs.Executor.in_process()
    ) as repo:
        repo.resolve()
        result = repo.materialize(selection=["users", "active_users"])

        assert result.success, result.failed_assets
        assert repo.load_node("active_users") == [{"id": 1, "active": True}]


def test_graph_asset_example_materializes():
    @rs.Task
    def download() -> bytes:
        return b"raw-bytes"

    @rs.Task
    def parse(raw: bytes) -> list[dict]:
        return [{"n": len(raw)}]

    @rs.Asset.from_graph()
    def documents():
        raw = download()
        return parse(raw)

    with rs.CodeRepository(
        assets=[documents],
        tasks=[download, parse],
        default_executor=rs.Executor.in_process(),
    ) as repo:
        repo.resolve()
        result = repo.materialize(selection=["documents"])

        assert result.success, result.failed_assets
        assert repo.load_node("documents") == [{"n": 9}]


def test_map_collect_example_materializes():
    @rs.Task
    def produce_items() -> list[int]:
        return [1, 2, 3]

    @rs.Task
    def process(produce_items: int) -> int:
        return produce_items * 10

    @rs.Task
    def summarize(process: list) -> int:
        return sum(process)

    @rs.Asset.from_graph()
    def processed():
        mapped = produce_items().map(process)
        return summarize(mapped.collect())

    with rs.CodeRepository(
        assets=[processed],
        tasks=[produce_items, process, summarize],
        default_executor=rs.Executor.in_process(),
    ) as repo:
        repo.resolve()
        result = repo.materialize(selection=["processed"])

        assert result.success, result.failed_assets
        assert repo.load_node("processed") == 60


def test_config_reaches_the_asset_through_the_context_generic():
    """The skill's #1 Dagster gotcha: config is the context generic, not a parameter."""

    class Thresholds(BaseModel):
        minimum: float = 0.0

    class Warehouse(rs.Resource):
        dsn: str = "postgres://localhost"

        def query(self, minimum: float) -> list[dict]:
            return [{"v": minimum}]

    @rs.Asset
    def filtered(
        context: rs.AssetExecutionContext[Thresholds], warehouse: Warehouse
    ) -> list[dict]:
        return warehouse.query(context.config.minimum)

    with rs.CodeRepository(
        assets=[filtered],
        resources={"warehouse": Warehouse()},
        default_executor=rs.Executor.in_process(),
    ) as repo:
        repo.resolve()
        result = repo.materialize(
            selection=["filtered"], config={"filtered": {"minimum": 7.0}}
        )

        assert result.success, result.failed_assets
        assert repo.load_node("filtered") == [{"v": 7.0}]


def test_static_partition_mapping_is_downstream_to_upstream(tmp_path):
    """Dagster keys this dict by upstream; rivers by downstream. The skill says invert."""
    io = rs.PickleIOHandler(store=LocalStore(prefix=str(tmp_path)))

    @rs.Asset(partitions_def=rs.PartitionsDefinition.static_(["a", "b"]), io_handler=io)
    def upstream(context: rs.AssetExecutionContext) -> str:
        return f"data-for-{context.partition_key}"

    @rs.Asset(
        partitions_def=rs.PartitionsDefinition.static_(["x", "y"]),
        io_handler=io,
        deps=[
            rs.AssetDef.input(
                "upstream",
                partition_mapping=rs.PartitionMapping.static_({"x": "a", "y": "b"}),
            )
        ],
    )
    def downstream(context: rs.AssetExecutionContext, upstream: str) -> str:
        return f"{context.partition_key} <- {upstream}"

    with rs.CodeRepository(
        assets=[upstream, downstream], default_executor=rs.Executor.in_process()
    ) as repo:
        repo.resolve()
        for key in ("a", "b"):
            repo.materialize(
                selection=["upstream"], partition_key=rs.PartitionKey.single(key)
            )
        result = repo.materialize(
            selection=["downstream"], partition_key=rs.PartitionKey.single("x")
        )

        assert result.success, result.failed_assets
        loaded = repo.load_node("downstream", partition_key=rs.PartitionKey.single("x"))
        assert loaded == "x <- data-for-a"


def test_repository_kitchen_sink_validates():
    """Every construct the reference files show, assembled into one repository."""
    daily = rs.PartitionsDefinition.daily(start=datetime(2024, 1, 1))
    handler = rs.InMemoryIOHandler()

    @rs.Asset(
        partitions_def=daily,
        pool="warehouse",
        pool_slots=2,
        retry=rs.RetryPolicy(
            max_retries=3,
            backoff=rs.Backoff.exponential(1.0, factor=2.0, jitter=0.1, max_delay=60.0),
            retry_on=rs.RetryOn.TRANSIENT,
        ),
        compute=rs.Compute(cpu="500m", memory="1Gi"),
        automation_condition=rs.AutomationCondition.eager().without(
            ~rs.AutomationCondition.any_deps_in_progress()
        ),
        backfill_strategy=rs.BackfillStrategy.single_run(),
    )
    def events(context: rs.AssetExecutionContext) -> list[dict]:
        context.add_output_metadata({"rows": rs.MetadataValue.int(1)})
        return [{"day": context.partition_key}]

    @rs.Asset.from_multi(output_defs=[rs.AssetDef("customers"), rs.AssetDef("orders")])
    def ingest():
        yield rs.Output([{"id": 1}], output_name="customers")
        yield rs.Output([{"id": 9}], output_name="orders")

    @rs.Asset.external(io_handler=handler)
    def vendor_feed() -> rs.Observation:
        return rs.Observation(metadata={"rows": 1}, data_version="abc")

    @rs.Hook.failure
    def alert(context: rs.HookContext) -> None:
        pass

    @rs.Asset(hooks=[alert], deps=[rs.AssetDef.dep("vendor_feed")])
    def report() -> int:
        return 1

    job = rs.Job(
        name="nightly", assets=[ingest, report], executor=rs.Executor.in_process()
    )

    @rs.Schedule(
        cron_schedule="0 2 * * *", job_name="nightly", timezone="Europe/Amsterdam"
    )
    def nightly(context: rs.ScheduleEvaluationContext):
        return rs.RunRequest(tags={"team": "data"})

    @rs.Sensor(job_name="nightly", minimum_interval="30s")
    def inbox(context: rs.SensorEvaluationContext):
        if context.cursor:
            return rs.SkipReason("nothing new")
        return rs.SensorResult(run_requests=[rs.RunRequest(run_key="f1")], cursor="f1")

    repo = rs.CodeRepository(
        assets=[events, ingest, vendor_feed, report],
        jobs=[job],
        schedules=[nightly],
        sensors=[inbox],
        default_executor=rs.Executor.parallel(max_workers=2),
        partition_defs={"daily": daily},
        retries={"flaky": rs.RetryPolicy(max_retries=2)},
        run_queue=rs.RunQueueConfig(
            max_concurrent_runs=4,
            tag_concurrency_limits=[
                rs.TagConcurrencyLimit("team", 2, per_unique_value=True)
            ],
        ),
        run_backend=rs.RunBackendConfig.local(),
        pool_limits={"warehouse": 4},
    )

    repo.validate()
