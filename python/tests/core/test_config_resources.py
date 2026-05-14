"""Tests for config management and resources."""

from typing import get_args, get_origin

import pytest
from pydantic import BaseModel
from pydantic_settings import BaseSettings

import rivers as rs
from rivers.exceptions import ConfigurationError

# ---------------------------------------------------------------------------
# Config fixtures
# ---------------------------------------------------------------------------


class ThresholdConfig(BaseModel):
    threshold: float = 0.5
    max_retries: int = 3


class EnvConfig(BaseSettings):
    api_url: str = "http://default"
    timeout: int = 30

    model_config = {"env_prefix": "TEST_CFG_"}


# ---------------------------------------------------------------------------
# Resource fixtures
# ---------------------------------------------------------------------------


class DummyResource(rs.Resource):
    """A simple resource that tracks setup/teardown calls."""

    connection_string: str = "memory://"


class CounterResource(rs.Resource):
    """Resource that counts how many times it's used."""

    prefix: str = "counter"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__dict__["_count"] = 0  # type: ignore[index]

    def get_next(self) -> str:
        val = self.__dict__["_count"]  # type: ignore[index]
        self.__dict__["_count"] = val + 1  # type: ignore[index]
        return f"{self.prefix}_{val}"


# ---------------------------------------------------------------------------
# Tests: Config (BaseModel — static)
# ---------------------------------------------------------------------------


def test_asset_with_basemodel_config():
    """Asset with a plain BaseModel config gets defaults via context.config."""

    @rs.Asset
    def configured_asset(context: rs.AssetExecutionContext[ThresholdConfig]):
        return {
            "threshold": context.config.threshold,
            "max_retries": context.config.max_retries,
        }

    repo = rs.CodeRepository(
        assets=[configured_asset],
        jobs=[rs.Job(name="j", assets=[configured_asset])],
    )
    repo.get_job("j").execute()
    assert repo.load_node("configured_asset")["threshold"] == 0.5
    assert repo.load_node("configured_asset")["max_retries"] == 3


def test_asset_with_basesettings_config(monkeypatch):
    """Asset with BaseSettings config resolves env vars."""

    @rs.Asset
    def env_asset(context: rs.AssetExecutionContext[EnvConfig]):
        return {"api_url": context.config.api_url, "timeout": context.config.timeout}

    repo = rs.CodeRepository(
        assets=[env_asset],
        jobs=[rs.Job(name="j", assets=[env_asset])],
    )

    monkeypatch.setenv("TEST_CFG_API_URL", "http://test-server")
    repo.get_job("j").execute()
    assert repo.load_node("env_asset")["api_url"] == "http://test-server"
    assert repo.load_node("env_asset")["timeout"] == 30  # default


def test_asset_without_config_has_none():
    """Asset with no config: context.config is None."""

    @rs.Asset
    def plain_asset(context: rs.AssetExecutionContext):
        return context.config

    repo = rs.CodeRepository(
        assets=[plain_asset],
        jobs=[rs.Job(name="j", assets=[plain_asset])],
    )
    repo.get_job("j").execute()
    assert repo.load_node("plain_asset") is None


# ---------------------------------------------------------------------------
# Tests: Resources
# ---------------------------------------------------------------------------


def test_resource_injection_basic():
    """Resources are injected into asset functions by parameter name."""

    dummy = DummyResource(connection_string="test://db")

    @rs.Asset
    def my_asset(context: rs.AssetExecutionContext, db: DummyResource):
        return db.connection_string

    repo = rs.CodeRepository(
        assets=[my_asset],
        resources={"db": dummy},
        jobs=[rs.Job(name="j", assets=[my_asset])],
    )
    repo.get_job("j").execute()
    assert repo.load_node("my_asset") == "test://db"


def test_resource_setup_called():
    """Resource.setup() is called during CodeRepository resolve."""

    setup_log = []

    class TrackingResource(rs.Resource):
        prefix: str = "track"

        def setup(self) -> None:
            setup_log.append("setup")

    @rs.Asset
    def my_asset(tracker: TrackingResource):
        return "ok"

    repo = rs.CodeRepository(
        assets=[my_asset],
        resources={"tracker": TrackingResource()},
        jobs=[rs.Job(name="j", assets=[my_asset])],
    )
    # setup() is called during lazy resolve (triggered by execute)
    repo.get_job("j").execute()
    assert "setup" in setup_log
    assert repo.load_node("my_asset") == "ok"


def test_multiple_resources():
    """Multiple resources injected into a single asset."""

    db = DummyResource(connection_string="db://prod")
    counter = CounterResource(prefix="ticket")

    @rs.Asset
    def multi_resource_asset(
        context: rs.AssetExecutionContext, db: DummyResource, counter: CounterResource
    ):
        return {
            "conn": db.connection_string,
            "id": counter.get_next(),
        }

    repo = rs.CodeRepository(
        assets=[multi_resource_asset],
        resources={"db": db, "counter": counter},
        jobs=[rs.Job(name="j", assets=[multi_resource_asset])],
    )
    repo.get_job("j").execute()
    assert repo.load_node("multi_resource_asset")["conn"] == "db://prod"
    assert repo.load_node("multi_resource_asset")["id"] == "ticket_0"


def test_resource_shared_across_assets():
    """Same resource instance is shared across multiple assets in a run."""

    counter = CounterResource(prefix="shared")

    @rs.Asset
    def first(counter: CounterResource) -> str:
        return counter.get_next()

    @rs.Asset
    def second(first: str, counter: CounterResource) -> str:
        return counter.get_next()

    repo = rs.CodeRepository(
        assets=[first, second],
        resources={"counter": counter},
        jobs=[rs.Job(name="j", assets=[first, second])],
    )
    repo.get_job("j").execute()
    assert repo.load_node("first") == "shared_0"
    assert repo.load_node("second") == "shared_1"


def test_resource_with_config_together():
    """An asset can have both a config and receive resources."""

    counter = CounterResource(prefix="combined")

    @rs.Asset
    def combined_asset(
        context: rs.AssetExecutionContext[ThresholdConfig], counter: CounterResource
    ):
        return {
            "threshold": context.config.threshold,
            "id": counter.get_next(),
        }

    repo = rs.CodeRepository(
        assets=[combined_asset],
        resources={"counter": counter},
        jobs=[rs.Job(name="j", assets=[combined_asset])],
    )
    repo.get_job("j").execute()
    assert repo.load_node("combined_asset")["threshold"] == 0.5
    assert repo.load_node("combined_asset")["id"] == "combined_0"


def test_resource_not_found_raises_error():
    """Referencing a resource key that doesn't exist raises an error at resolve time."""

    @rs.Asset
    def needs_db(db: DummyResource):
        return "ok"

    # Without providing the "db" resource, the graph builder treats "db"
    # as an unresolved upstream asset and raises an error during lazy resolve.
    repo = rs.CodeRepository(
        assets=[needs_db],
        jobs=[rs.Job(name="j", assets=[needs_db])],
    )
    with pytest.raises(BaseException):
        repo.get_job("j").execute()


def test_resource_invalid_type_rejected():
    """Passing a non-Resource instance as a resource raises ConfigurationError."""

    @rs.Asset
    def my_asset(db: str):
        return "ok"

    with pytest.raises(ConfigurationError):
        rs.CodeRepository(
            assets=[my_asset],
            resources={"db": "not-a-resource"},
        )


def test_resource_with_upstream_and_resource():
    """Resource injection works alongside upstream dependency injection."""

    counter = CounterResource(prefix="up")

    @rs.Asset
    def source() -> int:
        return 42

    @rs.Asset
    def consumer(source: int, counter: CounterResource) -> str:
        return f"{source}_{counter.get_next()}"

    repo = rs.CodeRepository(
        assets=[source, consumer],
        resources={"counter": counter},
        jobs=[rs.Job(name="j", assets=[source, consumer])],
    )
    repo.get_job("j").execute()
    assert repo.load_node("source") == 42
    assert repo.load_node("consumer") == "42_up_0"


def test_materialize_with_resources():
    """Resources work with repo.materialize() as well."""

    counter = CounterResource(prefix="mat")

    @rs.Asset
    def mat_asset(counter: CounterResource) -> str:
        return counter.get_next()

    repo = rs.CodeRepository(
        assets=[mat_asset],
        resources={"counter": counter},
    )
    repo.materialize()
    assert repo.load_node("mat_asset") == "mat_0"


def test_resource_base_class():
    """Resource base class extends pydantic-settings BaseSettings."""

    assert issubclass(rs.Resource, BaseSettings)

    r = rs.Resource()
    r.setup()
    r.teardown()


def test_resource_injection_into_task():
    """Resources are injected into task functions by parameter name."""

    counter = CounterResource(prefix="task")

    @rs.Task
    def my_task(context: rs.TaskExecutionContext, counter: CounterResource) -> str:
        return counter.get_next()

    repo = rs.CodeRepository(
        assets=[],
        tasks=[my_task],
        resources={"counter": counter},
        jobs=[rs.Job(name="j", assets=[my_task])],
    )
    repo.get_job("j").execute()
    assert repo.load_node("my_task") == "task_0"


def test_multi_asset_with_config():
    """Multi-asset with config gets config via context annotation."""

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef(name="out_a"), rs.AssetDef(name="out_b")],
    )
    def my_multi(context: rs.AssetExecutionContext[ThresholdConfig]):
        return {"out_a": context.config.threshold, "out_b": context.config.max_retries}

    repo = rs.CodeRepository(
        assets=[my_multi],
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize()
    # Multi-asset outputs are sliced per output name
    assert repo.load_node("out_a") == 0.5
    assert repo.load_node("out_b") == 3


def test_multi_asset_with_basesettings_config(monkeypatch):
    """Multi-asset with BaseSettings config resolves env vars."""

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef(name="m_a"), rs.AssetDef(name="m_b")],
    )
    def env_multi(context: rs.AssetExecutionContext[EnvConfig]):
        return {"m_a": context.config.api_url, "m_b": context.config.timeout}

    repo = rs.CodeRepository(
        assets=[env_multi],
        default_executor=rs.Executor.in_process(),
    )

    monkeypatch.setenv("TEST_CFG_API_URL", "http://multi-server")
    repo.materialize()
    assert repo.load_node("m_a") == "http://multi-server"
    assert repo.load_node("m_b") == 30


def test_multi_asset_without_config_has_none():
    """Multi-asset without config: context.config is None."""

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef(name="nc_a"), rs.AssetDef(name="nc_b")],
    )
    def no_cfg_multi(context: rs.AssetExecutionContext):
        return {"nc_a": context.config, "nc_b": "ok"}

    repo = rs.CodeRepository(
        assets=[no_cfg_multi],
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize()
    assert repo.load_node("nc_a") is None


def test_external_asset_with_config():
    """External asset with config: observe_fn receives config via context annotation."""

    class DictIOHandler(rs.BaseIOHandler):
        store: dict = {}

        def handle_output(self, context, obj):
            self.store[context.asset_name] = obj

        def load_input(self, context):
            return self.store[context.asset_name]

    observed = {}

    @rs.Asset.external(
        name="ext_cfg",
        io_handler=DictIOHandler(),
    )
    def ext_cfg(context: rs.AssetExecutionContext[ThresholdConfig]):
        observed["config"] = context.config

    repo = rs.CodeRepository(assets=[ext_cfg])
    repo.observe()
    assert isinstance(observed["config"], ThresholdConfig)
    assert observed["config"].threshold == 0.5


def test_external_asset_with_basesettings_config(monkeypatch):
    """External asset with BaseSettings config resolves env vars in observe."""

    class DictIOHandler(rs.BaseIOHandler):
        store: dict = {}

        def handle_output(self, context, obj):
            self.store[context.asset_name] = obj

        def load_input(self, context):
            return self.store[context.asset_name]

    observed = {}

    @rs.Asset.external(
        name="ext_env",
        io_handler=DictIOHandler(),
    )
    def ext_env(context: rs.AssetExecutionContext[EnvConfig]):
        observed["url"] = context.config.api_url

    monkeypatch.setenv("TEST_CFG_API_URL", "http://ext-server")
    repo = rs.CodeRepository(assets=[ext_env])
    repo.observe()
    assert observed["url"] == "http://ext-server"


# ---------------------------------------------------------------------------
# Generic[ConfigT] typing tests
# ---------------------------------------------------------------------------


def test_asset_context_class_getitem_creates_generic_alias():
    """AssetExecutionContext[T] creates a valid generic alias at runtime."""
    alias = rs.AssetExecutionContext[ThresholdConfig]
    # Should be a typing._GenericAlias
    assert get_origin(alias) is not None
    assert get_origin(alias) is rs.AssetExecutionContext
    assert get_args(alias) == (ThresholdConfig,)


def test_task_context_class_getitem_creates_generic_alias():
    """TaskExecutionContext[T] creates a valid generic alias at runtime."""
    alias = rs.TaskExecutionContext[ThresholdConfig]
    assert get_origin(alias) is not None
    assert get_origin(alias) is rs.TaskExecutionContext
    assert get_args(alias) == (ThresholdConfig,)


def test_context_class_getitem_with_builtin_types():
    """Context[T] works with arbitrary types, not just pydantic models."""
    alias_int = rs.AssetExecutionContext[int]
    assert get_args(alias_int) == (int,)

    alias_str = rs.TaskExecutionContext[str]
    assert get_args(alias_str) == (str,)


def test_asset_with_typed_context_annotation():
    """An asset using AssetExecutionContext[Config] as annotation works at runtime."""

    @rs.Asset
    def typed_asset(context: rs.AssetExecutionContext[ThresholdConfig]):
        assert isinstance(context.config, ThresholdConfig)
        return context.config.threshold

    repo = rs.CodeRepository(assets=[typed_asset])
    repo.materialize()
    assert repo.load_node("typed_asset") == 0.5


def test_task_with_typed_context_annotation():
    """A task using TaskExecutionContext[Config] as annotation runs without error."""

    @rs.Task
    def typed_task(context: rs.TaskExecutionContext[ThresholdConfig]):
        assert isinstance(context.config, ThresholdConfig)
        return "ok"

    repo = rs.CodeRepository(
        assets=[],
        tasks=[typed_task],
        jobs=[rs.Job(name="j", assets=[typed_task])],
    )
    repo.get_job("j").execute()
    assert repo.load_node("typed_task") == "ok"


def test_context_generic_alias_is_isinstance_compatible():
    """Bare AssetExecutionContext still works for isinstance checks."""
    ctx = rs.AssetExecutionContext(asset_name="test")
    assert isinstance(ctx, rs.AssetExecutionContext)


def test_context_subscript_multiple_times():
    """Subscripting the same class with different types produces distinct aliases."""
    a = rs.AssetExecutionContext[ThresholdConfig]
    b = rs.AssetExecutionContext[EnvConfig]
    assert get_args(a) != get_args(b)
    # Both share the same origin
    assert get_origin(a) is get_origin(b)


# ---------------------------------------------------------------------------
# Tests: IOHandler as resource reference (string key)
# ---------------------------------------------------------------------------


class ResourceIOHandler(rs.BaseIOHandler):
    """A BaseIOHandler that also carries config fields."""

    prefix: str = "res"
    store: dict = {}

    def handle_output(self, context, obj):
        self.store[context.asset_name] = obj

    def load_input(self, context):
        return self.store[context.asset_name]


def test_io_handler_as_resource_ref():
    """io_handler='my_io' resolves to resources['my_io'] at resolve time."""

    handler = ResourceIOHandler(prefix="test")

    @rs.Asset(io_handler="my_io")
    def source() -> int:
        return 42

    @rs.Asset
    def consumer(source: int) -> int:
        return source + 1

    repo = rs.CodeRepository(
        assets=[source, consumer],
        resources={"my_io": handler},
    )
    repo.materialize()
    assert repo.load_node("source") == 42
    assert repo.load_node("consumer") == 43
    # Verify the handler actually stored the value
    assert handler.store["source"] == 42


def test_io_handler_resource_ref_load_input():
    """Downstream asset loads upstream via io_handler resource reference."""

    handler = ResourceIOHandler(prefix="load")

    @rs.Asset(io_handler="store")
    def upstream() -> int:
        return 100

    @rs.Asset
    def downstream(upstream: int) -> int:
        return upstream * 2

    repo = rs.CodeRepository(
        assets=[upstream, downstream],
        resources={"store": handler},
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize(selection=["upstream"])
    assert repo.load_node("upstream") == 100

    # Now downstream should load via io_handler
    repo.materialize(selection=["downstream"])
    assert repo.load_node("downstream") == 200


def test_io_handler_resource_ref_missing_resource():
    """io_handler='nonexistent' raises error at resolve time."""

    @rs.Asset(io_handler="nonexistent")
    def bad_asset() -> int:
        return 1

    repo = rs.CodeRepository(assets=[bad_asset])
    with pytest.raises(BaseException, match="nonexistent"):
        repo.materialize()


def test_io_handler_resource_ref_invalid_protocol():
    """io_handler referencing a resource without handle_output/load_input fails."""

    class NotAnIOHandler(rs.Resource):
        value: str = "oops"

    @rs.Asset(io_handler="bad")
    def bad_asset() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[bad_asset],
        resources={"bad": NotAnIOHandler()},
    )
    with pytest.raises(BaseException, match="does not implement"):
        repo.materialize()


def test_io_handler_resource_ref_on_external_asset():
    """External asset can use io_handler='key' to reference a resource."""

    handler = ResourceIOHandler(prefix="ext")
    handler.store["ext_res"] = "pre-loaded"

    ext = rs.Asset.external(
        name="ext_res",
        io_handler="ext_handler",
    )

    @rs.Asset
    def consumer(ext_res: int) -> str:
        return f"got: {ext_res}"

    repo = rs.CodeRepository(
        assets=[ext, consumer],
        resources={"ext_handler": handler},
    )
    repo.materialize()
    assert repo.load_node("consumer") == "got: pre-loaded"


# ---------------------------------------------------------------------------
# Tests: Resolve-time validation of resource references
# ---------------------------------------------------------------------------


def test_resolve_time_validation_unknown_param():
    """Unknown parameter that's not a resource or upstream raises at resolve time."""

    @rs.Asset
    def bad_asset(unknown_param: str) -> str:
        return "ok"

    repo = rs.CodeRepository(assets=[bad_asset])
    with pytest.raises(BaseException, match="unknown_param"):
        repo.materialize()


def test_resolve_time_validation_passes_with_resource():
    """No error when parameter matches a resource key."""

    counter = CounterResource(prefix="val")

    @rs.Asset
    def ok_asset(counter: CounterResource) -> str:
        return counter.get_next()

    repo = rs.CodeRepository(
        assets=[ok_asset],
        resources={"counter": counter},
    )
    repo.materialize()
    assert repo.load_node("ok_asset") == "val_0"


# ---------------------------------------------------------------------------
# Tests: Resource teardown lifecycle
# ---------------------------------------------------------------------------


def test_teardown_called_on_context_manager_exit():
    """Resource teardown() is called when CodeRepository is used as context manager."""

    teardown_log = []

    class TeardownResource(rs.Resource):
        label: str = "test"

        def teardown(self):
            teardown_log.append(f"teardown:{self.label}")

    @rs.Asset
    def my_asset(res: TeardownResource) -> str:
        return "ok"

    with rs.CodeRepository(
        assets=[my_asset],
        resources={"res": TeardownResource(label="ctx")},
    ) as repo:
        repo.materialize()
        assert repo.load_node("my_asset") == "ok"

    assert "teardown:ctx" in teardown_log


def test_shutdown_calls_teardown():
    """Explicit shutdown() calls teardown on resources."""

    teardown_log = []

    class TeardownResource(rs.Resource):
        label: str = "test"

        def teardown(self):
            teardown_log.append(f"teardown:{self.label}")

    @rs.Asset
    def my_asset(res: TeardownResource) -> str:
        return "ok"

    repo = rs.CodeRepository(
        assets=[my_asset],
        resources={"res": TeardownResource(label="shut")},
    )
    repo.materialize()
    repo.shutdown()
    assert "teardown:shut" in teardown_log


# ---------------------------------------------------------------------------
# Tests: Materialize-time config overrides
# ---------------------------------------------------------------------------


def test_materialize_config_override():
    """Config overrides at materialize time replace default config values."""

    @rs.Asset
    def cfg_asset(context: rs.AssetExecutionContext[ThresholdConfig]):
        return {
            "threshold": context.config.threshold,
            "max_retries": context.config.max_retries,
        }

    repo = rs.CodeRepository(assets=[cfg_asset])
    repo.materialize(
        config={"cfg_asset": {"threshold": 0.9, "max_retries": 10}},
    )
    output = repo.load_node("cfg_asset")
    assert output["threshold"] == 0.9
    assert output["max_retries"] == 10


def test_materialize_config_override_partial():
    """Partial config overrides merge with defaults."""

    @rs.Asset
    def partial_cfg(context: rs.AssetExecutionContext[ThresholdConfig]):
        return {
            "threshold": context.config.threshold,
            "max_retries": context.config.max_retries,
        }

    repo = rs.CodeRepository(assets=[partial_cfg])
    repo.materialize(
        config={"partial_cfg": {"threshold": 0.8}},
    )
    output = repo.load_node("partial_cfg")
    assert output["threshold"] == 0.8
    assert output["max_retries"] == 3  # default


# ---------------------------------------------------------------------------
# Tests: parallel parallel resource injection
# ---------------------------------------------------------------------------


def test_parallel_parallel_resource_injection(tmp_path):
    """Resources are injected into assets running in parallel via parallel executor."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)
    counter = CounterResource(prefix="mp")

    @rs.Asset(io_handler=handler)
    def mp_a(counter: CounterResource) -> str:
        return counter.get_next()

    @rs.Asset(io_handler=handler)
    def mp_b(counter: CounterResource) -> str:
        return counter.get_next()

    repo = rs.CodeRepository(
        assets=[mp_a, mp_b],
        resources={"counter": counter},
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    _result = repo.materialize()
    # Both assets should have received the counter resource
    a_ctx = rs.InputContext(asset_name="mp_a", downstream_asset="test")
    b_ctx = rs.InputContext(asset_name="mp_b", downstream_asset="test")
    assert handler.load_input(a_ctx).startswith("mp_")
    assert handler.load_input(b_ctx).startswith("mp_")


def test_parallel_resource_setup_teardown_per_worker(tmp_path):
    """Resources get setup()/teardown() called per worker process."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    class LifecycleResource(rs.Resource):
        label: str = "worker"

        def setup(self):
            # Non-serializable state created in setup — would fail with naive pickling
            self.__dict__["_initialized"] = True  # type: ignore[index]

        def teardown(self):
            self.__dict__["_initialized"] = False  # type: ignore[index]

    @rs.Asset(io_handler=handler)
    def w_a(res: LifecycleResource) -> bool:
        # Worker should have called setup(), creating _initialized
        return res.__dict__.get("_initialized", False)

    @rs.Asset(io_handler=handler)
    def w_b(res: LifecycleResource) -> bool:
        return res.__dict__.get("_initialized", False)

    repo = rs.CodeRepository(
        assets=[w_a, w_b],
        resources={"res": LifecycleResource()},
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    repo.materialize()
    # Both workers should have gotten setup() called
    a_ctx = rs.InputContext(asset_name="w_a", downstream_asset="test")
    b_ctx = rs.InputContext(asset_name="w_b", downstream_asset="test")
    assert handler.load_input(a_ctx) is True
    assert handler.load_input(b_ctx) is True


# ---------------------------------------------------------------------------
# Tests: Schedule/sensor resolve-time resource validation
# ---------------------------------------------------------------------------


def test_schedule_invalid_resource_ref_fails_at_resolve_time():
    """Schedule referencing a non-existent resource raises at resolve time."""

    @rs.Asset
    def dummy_asset() -> int:
        return 1

    @rs.Schedule(cron_schedule="0 * * * *", job_name="j")
    def bad_schedule(context: rs.ScheduleEvaluationContext, nonexistent: DummyResource):
        return rs.RunRequest()

    repo = rs.CodeRepository(
        assets=[dummy_asset],
        schedules=[bad_schedule],
        jobs=[rs.Job(name="j", assets=[dummy_asset])],
    )
    with pytest.raises(BaseException, match="nonexistent"):
        repo.materialize()


def test_sensor_invalid_resource_ref_fails_at_resolve_time():
    """Sensor referencing a non-existent resource raises at resolve time."""

    @rs.Asset
    def dummy_asset2() -> int:
        return 1

    @rs.Sensor(name="bad_sensor", job_name="j")
    def bad_sensor(context: rs.SensorEvaluationContext, missing_db: DummyResource):
        return rs.SkipReason("nope")

    repo = rs.CodeRepository(
        assets=[dummy_asset2],
        sensors=[bad_sensor],
        jobs=[rs.Job(name="j", assets=[dummy_asset2])],
    )
    with pytest.raises(BaseException, match="missing_db"):
        repo.materialize()


def test_schedule_with_valid_resource_passes_validation():
    """Schedule with a valid resource reference resolves successfully."""

    counter = CounterResource(prefix="sched")

    @rs.Asset
    def sched_asset() -> int:
        return 1

    @rs.Schedule(cron_schedule="0 * * * *", job_name="j")
    def valid_schedule(context: rs.ScheduleEvaluationContext, counter: CounterResource):
        return rs.RunRequest()

    repo = rs.CodeRepository(
        assets=[sched_asset],
        schedules=[valid_schedule],
        resources={"counter": counter},
        jobs=[rs.Job(name="j", assets=[sched_asset])],
    )
    # Should not raise — just triggers resolve
    result = repo.materialize()
    assert result.success


# ---------------------------------------------------------------------------
# Tests: Auto-derive config from Context[ConfigT]
# ---------------------------------------------------------------------------


def test_asset_auto_derive_config_from_annotation():
    """Config is auto-derived from AssetExecutionContext[ConfigT]."""

    @rs.Asset
    def auto_cfg(context: rs.AssetExecutionContext[ThresholdConfig]):
        assert isinstance(context.config, ThresholdConfig)
        return context.config.threshold

    repo = rs.CodeRepository(assets=[auto_cfg])
    repo.materialize()
    assert repo.load_node("auto_cfg") == 0.5


def test_task_auto_derive_config_from_annotation():
    """Config is auto-derived from TaskExecutionContext[ConfigT]."""

    @rs.Task
    def auto_task(context: rs.TaskExecutionContext[ThresholdConfig]):
        assert isinstance(context.config, ThresholdConfig)
        return context.config.max_retries

    repo = rs.CodeRepository(
        assets=[],
        tasks=[auto_task],
        jobs=[rs.Job(name="j", assets=[auto_task])],
    )
    repo.get_job("j").execute()
    assert repo.load_node("auto_task") == 3


def test_asset_auto_derive_config_with_overrides():
    """Auto-derived config still supports materialize-time overrides."""

    @rs.Asset
    def override_cfg(context: rs.AssetExecutionContext[ThresholdConfig]):
        return context.config.threshold

    repo = rs.CodeRepository(assets=[override_cfg])
    repo.materialize(
        config={"override_cfg": {"threshold": 0.99}},
    )
    assert repo.load_node("override_cfg") == 0.99


def test_asset_auto_derive_basesettings_config(monkeypatch):
    """Auto-derived config from BaseSettings resolves env vars."""

    @rs.Asset
    def env_auto(context: rs.AssetExecutionContext[EnvConfig]):
        return context.config.api_url

    monkeypatch.setenv("TEST_CFG_API_URL", "http://auto-env")
    repo = rs.CodeRepository(assets=[env_auto])
    repo.materialize()
    assert repo.load_node("env_auto") == "http://auto-env"


# ---------------------------------------------------------------------------
# Tests: ConfigT on ScheduleEvaluationContext and SensorEvaluationContext
# ---------------------------------------------------------------------------


def test_schedule_context_class_getitem():
    """ScheduleEvaluationContext[T] creates a valid generic alias."""
    alias = rs.ScheduleEvaluationContext[ThresholdConfig]
    assert get_origin(alias) is not None
    assert get_origin(alias) is rs.ScheduleEvaluationContext
    assert get_args(alias) == (ThresholdConfig,)


def test_sensor_context_class_getitem():
    """SensorEvaluationContext[T] creates a valid generic alias."""
    alias = rs.SensorEvaluationContext[ThresholdConfig]
    assert get_origin(alias) is not None
    assert get_origin(alias) is rs.SensorEvaluationContext
    assert get_args(alias) == (ThresholdConfig,)


def test_hook_context_class_getitem():
    """HookContext[T] creates a valid generic alias."""
    alias = rs.HookContext[ThresholdConfig]
    assert get_origin(alias) is not None
    assert get_origin(alias) is rs.HookContext
    assert get_args(alias) == (ThresholdConfig,)


def test_schedule_auto_derive_config():
    """Schedule eval_fn gets config via ScheduleEvaluationContext[ConfigT]."""

    received = {}

    @rs.Asset
    def sched_dummy() -> int:
        return 1

    @rs.Schedule(cron_schedule="0 * * * *", job_name="j")
    def typed_schedule(context: rs.ScheduleEvaluationContext[ThresholdConfig]):
        received["config"] = context.config
        return rs.RunRequest()

    repo = rs.CodeRepository(
        assets=[sched_dummy],
        schedules=[typed_schedule],
        jobs=[rs.Job(name="j", assets=[sched_dummy])],
    )
    repo.resolve()
    repo.evaluate_schedule("typed_schedule")
    assert isinstance(received["config"], ThresholdConfig)
    assert received["config"].threshold == 0.5


def test_sensor_auto_derive_config():
    """Sensor eval_fn gets config via SensorEvaluationContext[ConfigT]."""

    received = {}

    @rs.Asset
    def sensor_dummy() -> int:
        return 1

    @rs.Sensor(name="typed_sensor", job_name="j")
    def typed_sensor(context: rs.SensorEvaluationContext[ThresholdConfig]):
        received["config"] = context.config
        return rs.SkipReason("test")

    repo = rs.CodeRepository(
        assets=[sensor_dummy],
        sensors=[typed_sensor],
        jobs=[rs.Job(name="j", assets=[sensor_dummy])],
    )
    repo.resolve()
    repo.evaluate_sensor("typed_sensor")
    assert isinstance(received["config"], ThresholdConfig)
    assert received["config"].max_retries == 3


def test_hook_receives_config_from_asset():
    """Hook context receives the asset's config instance."""

    received = {}

    def on_success(context: rs.HookContext[ThresholdConfig]):
        received["config"] = context.config

    @rs.Asset(hooks=[rs.Hook.success(on_success)])
    def hooked_asset(context: rs.AssetExecutionContext[ThresholdConfig]):
        return context.config.threshold

    repo = rs.CodeRepository(assets=[hooked_asset])
    repo.materialize()
    assert repo.load_node("hooked_asset") == 0.5
    assert isinstance(received["config"], ThresholdConfig)
    assert received["config"].threshold == 0.5


# ---------------------------------------------------------------------------
# Tests: Non-Resource IOHandler (plain object, not inheriting rs.Resource)
# ---------------------------------------------------------------------------


class SimpleIOHandler(rs.BaseIOHandler):
    """BaseIOHandler without extra fields — just the protocol methods."""

    store: dict = {}

    def handle_output(self, context, obj):
        self.store[context.asset_name] = obj

    def load_input(self, context):
        return self.store[context.asset_name]


def test_simple_io_handler_as_resource():
    """A BaseIOHandler subclass can be used as a resource io_handler."""

    handler = SimpleIOHandler()

    @rs.Asset(io_handler="plain")
    def source() -> int:
        return 99

    @rs.Asset
    def consumer(source: int) -> int:
        return source + 1

    repo = rs.CodeRepository(
        assets=[source, consumer],
        resources={"plain": handler},
    )
    repo.materialize()
    assert repo.load_node("source") == 99
    assert repo.load_node("consumer") == 100
    assert handler.store["source"] == 99


def test_simple_io_handler_load_input():
    """Downstream loads upstream value via BaseIOHandler resource reference."""

    handler = SimpleIOHandler()

    @rs.Asset(io_handler="store")
    def upstream() -> int:
        return 50

    @rs.Asset
    def downstream(upstream: int) -> int:
        return upstream * 3

    repo = rs.CodeRepository(
        assets=[upstream, downstream],
        resources={"store": handler},
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize(selection=["upstream"])
    assert repo.load_node("upstream") == 50

    repo.materialize(selection=["downstream"])
    assert repo.load_node("downstream") == 150


def test_simple_io_handler_on_external_asset():
    """External asset can use a BaseIOHandler as resource reference."""

    handler = SimpleIOHandler(store={"ext": "preloaded"})

    ext = rs.Asset.external(name="ext", io_handler="my_handler")

    @rs.Asset
    def consumer(ext: str) -> str:
        return f"got:{ext}"

    repo = rs.CodeRepository(
        assets=[ext, consumer],
        resources={"my_handler": handler},
    )
    repo.materialize()
    assert repo.load_node("consumer") == "got:preloaded"


# ---------------------------------------------------------------------------
# Tests: Pydantic BaseModel / BaseSettings as io_handler resource
# ---------------------------------------------------------------------------


class BaseIOHandlerWithPrefix(rs.BaseIOHandler):
    """A BaseIOHandler subclass with extra config fields."""

    prefix: str = "bm"
    store: dict = {}

    def handle_output(self, context, obj):
        self.store[context.asset_name] = obj

    def load_input(self, context):
        return self.store[context.asset_name]


def test_base_io_handler_as_resource():
    """A BaseIOHandler subclass works as a resource reference for io_handler."""

    handler = BaseIOHandlerWithPrefix(prefix="test")

    @rs.Asset(io_handler="bm_handler")
    def source() -> int:
        return 10

    @rs.Asset
    def consumer(source: int) -> int:
        return source + 5

    repo = rs.CodeRepository(
        assets=[source, consumer],
        resources={"bm_handler": handler},
    )
    repo.materialize()
    assert repo.load_node("source") == 10
    assert repo.load_node("consumer") == 15
    assert handler.store["source"] == 10


def test_duck_typed_io_handler_rejected():
    """A BaseModel/BaseSettings with handle_output+load_input but not extending
    BaseIOHandler is rejected as an io_handler resource reference."""

    class DuckTypedHandler(BaseModel):
        store: dict = {}

        def handle_output(self, context, obj):
            self.store[context.asset_name] = obj

        def load_input(self, context):
            return self.store[context.asset_name]

    @rs.Asset(io_handler="duck")
    def source() -> int:
        return 10

    repo = rs.CodeRepository(
        assets=[source],
        resources={"duck": DuckTypedHandler()},
    )
    with pytest.raises(BaseException, match="does not implement"):
        repo.materialize()


def test_basemodel_without_protocol_rejected_as_io_handler_ref():
    """A BaseModel instance is accepted as a resource, but rejected as io_handler ref."""

    class PlainConfig(BaseModel):
        value: int = 0

    @rs.Asset(io_handler="cfg")
    def bad_asset() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[bad_asset],
        resources={"cfg": PlainConfig()},
    )
    with pytest.raises(BaseException, match="does not implement"):
        repo.materialize()


def test_basemodel_instance_accepted_as_resource():
    """A plain BaseModel instance can be used as an injectable resource."""

    class MyConfig(BaseModel):
        multiplier: int = 10

    config = MyConfig(multiplier=5)

    @rs.Asset
    def compute(cfg: MyConfig) -> int:
        return cfg.multiplier * 2

    repo = rs.CodeRepository(
        assets=[compute],
        resources={"cfg": config},
    )
    repo.materialize()
    assert repo.load_node("compute") == 10


def test_basesettings_instance_accepted_as_resource():
    """A plain BaseSettings instance can be used as an injectable resource."""

    class MySettings(BaseSettings):
        factor: int = 3
        model_config = {"env_prefix": "TEST_PLAIN_BS_"}

    settings = MySettings(factor=7)

    @rs.Asset
    def compute(cfg: MySettings) -> int:
        return cfg.factor + 1

    repo = rs.CodeRepository(
        assets=[compute],
        resources={"cfg": settings},
    )
    repo.materialize()
    assert repo.load_node("compute") == 8


def test_basemodel_io_handler_load_input_across_runs():
    """BaseIOHandler persists data across materialize calls."""

    handler = BaseIOHandlerWithPrefix()

    @rs.Asset(io_handler="store")
    def upstream() -> int:
        return 77

    @rs.Asset
    def downstream(upstream: int) -> int:
        return upstream + 3

    repo = rs.CodeRepository(
        assets=[upstream, downstream],
        resources={"store": handler},
        default_executor=rs.Executor.in_process(),
    )
    repo.materialize(selection=["upstream"])
    assert repo.load_node("upstream") == 77

    repo.materialize(selection=["downstream"])
    assert repo.load_node("downstream") == 80


def test_basesettings_instance_as_resource_resolved_and_injected():
    """A BaseSettings instance in resources is resolved and injected into assets."""

    class DbSettings(BaseSettings):
        host: str = "localhost"
        port: int = 5432
        model_config = {"env_prefix": "TEST_DB_RES_"}

    settings = DbSettings(host="myhost", port=9999)

    @rs.Asset
    def connect_info(db: DbSettings) -> str:
        return f"{db.host}:{db.port}"

    @rs.Asset
    def consumer(connect_info: str) -> str:
        return f"connected to {connect_info}"

    repo = rs.CodeRepository(
        assets=[connect_info, consumer],
        resources={"db": settings},
    )
    repo.materialize()
    assert repo.load_node("connect_info") == "myhost:9999"
    assert repo.load_node("consumer") == "connected to myhost:9999"


def test_basesettings_without_protocol_rejected_as_io_handler_ref():
    """A BaseSettings instance without IOHandler protocol is rejected as io_handler ref."""

    class PlainSettings(BaseSettings):
        value: str = "nope"
        model_config = {"env_prefix": "TEST_PLAIN_SET_"}

    @rs.Asset(io_handler="cfg")
    def bad_asset() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[bad_asset],
        resources={"cfg": PlainSettings()},
    )
    with pytest.raises(BaseException, match="IOHandler protocol"):
        repo.materialize()


# ---------------------------------------------------------------------------
# Tests: PydanticModelInstance / Resource injection into sensors and schedules
# ---------------------------------------------------------------------------


def test_basemodel_instance_injected_into_sensor():
    """BaseModel instance resource is injected into sensor by parameter name."""

    class SensorConfig(BaseModel):
        threshold: float = 0.75

    received = {}

    @rs.Asset
    def dummy() -> int:
        return 1

    @rs.Sensor(name="cfg_sensor", job_name="j")
    def cfg_sensor(context: rs.SensorEvaluationContext, cfg: SensorConfig):
        received["threshold"] = cfg.threshold
        return rs.SkipReason("test")

    repo = rs.CodeRepository(
        assets=[dummy],
        sensors=[cfg_sensor],
        resources={"cfg": SensorConfig(threshold=0.42)},
        jobs=[rs.Job(name="j", assets=[dummy])],
    )
    repo.resolve()
    repo.evaluate_sensor("cfg_sensor")
    assert received["threshold"] == 0.42


def test_basemodel_instance_injected_into_schedule():
    """BaseModel instance resource is injected into schedule by parameter name."""

    class SchedConfig(BaseModel):
        batch_size: int = 100

    received = {}

    @rs.Asset
    def dummy() -> int:
        return 1

    @rs.Schedule(cron_schedule="0 * * * *", job_name="j")
    def cfg_schedule(context: rs.ScheduleEvaluationContext, cfg: SchedConfig):
        received["batch_size"] = cfg.batch_size
        return rs.RunRequest()

    repo = rs.CodeRepository(
        assets=[dummy],
        schedules=[cfg_schedule],
        resources={"cfg": SchedConfig(batch_size=50)},
        jobs=[rs.Job(name="j", assets=[dummy])],
    )
    repo.resolve()
    repo.evaluate_schedule("cfg_schedule")
    assert received["batch_size"] == 50


def test_basesettings_instance_injected_into_sensor():
    """BaseSettings instance resource is injected into sensor by parameter name."""

    class SensorSettings(BaseSettings):
        interval: int = 30
        model_config = {"env_prefix": "TEST_SENSOR_SET_"}

    received = {}

    @rs.Asset
    def dummy() -> int:
        return 1

    @rs.Sensor(name="set_sensor", job_name="j")
    def set_sensor(context: rs.SensorEvaluationContext, cfg: SensorSettings):
        received["interval"] = cfg.interval
        return rs.SkipReason("test")

    repo = rs.CodeRepository(
        assets=[dummy],
        sensors=[set_sensor],
        resources={"cfg": SensorSettings(interval=60)},
        jobs=[rs.Job(name="j", assets=[dummy])],
    )
    repo.resolve()
    repo.evaluate_sensor("set_sensor")
    assert received["interval"] == 60


def test_basesettings_instance_injected_into_schedule():
    """BaseSettings instance resource is injected into schedule by parameter name."""

    class SchedSettings(BaseSettings):
        max_workers: int = 4
        model_config = {"env_prefix": "TEST_SCHED_SET_"}

    received = {}

    @rs.Asset
    def dummy() -> int:
        return 1

    @rs.Schedule(cron_schedule="0 * * * *", job_name="j")
    def set_schedule(context: rs.ScheduleEvaluationContext, cfg: SchedSettings):
        received["max_workers"] = cfg.max_workers
        return rs.RunRequest()

    repo = rs.CodeRepository(
        assets=[dummy],
        schedules=[set_schedule],
        resources={"cfg": SchedSettings(max_workers=8)},
        jobs=[rs.Job(name="j", assets=[dummy])],
    )
    repo.resolve()
    repo.evaluate_schedule("set_schedule")
    assert received["max_workers"] == 8


def test_asset_auto_derive_config_from_instance_annotation():
    """AssetExecutionContext[ModelType] auto-derives config from a BaseModel instance."""

    class InstanceConfig(BaseModel):
        rate: float = 1.0

    @rs.Asset
    def inst_cfg(context: rs.AssetExecutionContext[InstanceConfig]):
        return context.config.rate

    repo = rs.CodeRepository(assets=[inst_cfg])
    repo.materialize()
    assert repo.load_node("inst_cfg") == 1.0


def test_schedule_auto_derive_config_from_instance_annotation():
    """ScheduleEvaluationContext[ModelType] with BaseModel instance in resources."""

    class SchedCfg(BaseModel):
        retries: int = 5

    received = {}

    @rs.Asset
    def dummy() -> int:
        return 1

    @rs.Schedule(cron_schedule="0 * * * *", job_name="j")
    def inst_schedule(context: rs.ScheduleEvaluationContext[SchedCfg]):
        received["retries"] = context.config.retries
        return rs.RunRequest()

    repo = rs.CodeRepository(
        assets=[dummy],
        schedules=[inst_schedule],
        jobs=[rs.Job(name="j", assets=[dummy])],
    )
    repo.resolve()
    repo.evaluate_schedule("inst_schedule")
    assert received["retries"] == 5


def test_sensor_auto_derive_config_from_instance_annotation():
    """SensorEvaluationContext[ModelType] with BaseModel instance in resources."""

    class SensorCfg(BaseModel):
        window: int = 10

    received = {}

    @rs.Asset
    def dummy() -> int:
        return 1

    @rs.Sensor(name="inst_sensor", job_name="j")
    def inst_sensor(context: rs.SensorEvaluationContext[SensorCfg]):
        received["window"] = context.config.window
        return rs.SkipReason("test")

    repo = rs.CodeRepository(
        assets=[dummy],
        sensors=[inst_sensor],
        jobs=[rs.Job(name="j", assets=[dummy])],
    )
    repo.resolve()
    repo.evaluate_sensor("inst_sensor")
    assert received["window"] == 10


def test_basemodel_instance_config_with_materialize_overrides():
    """BaseModel instance config is overridden via model_copy at materialize time."""

    class OverridableConfig(BaseModel):
        rate: float = 1.0
        label: str = "default"

    @rs.Asset
    def overridden(context: rs.AssetExecutionContext[OverridableConfig]):
        return {"rate": context.config.rate, "label": context.config.label}

    repo = rs.CodeRepository(assets=[overridden])

    # Without overrides — defaults used
    repo.materialize()
    assert repo.load_node("overridden") == {"rate": 1.0, "label": "default"}

    # With overrides — only specified fields change
    repo.materialize(
        config={"overridden": {"rate": 9.9}},
    )
    assert repo.load_node("overridden") == {"rate": 9.9, "label": "default"}


def test_basemodel_instance_in_asset_fn_config_with_materialize_overrides():
    """BaseModel instance config is overridden via model_copy at materialize time."""

    class OverridableConfig(BaseModel):
        rate: float = 1.0
        label: str = "default"

    @rs.Asset
    def overridden(resource: OverridableConfig):
        return {"rate": resource.rate, "label": resource.label}

    repo = rs.CodeRepository(
        assets=[overridden], resources={"resource": OverridableConfig()}
    )

    # Without overrides — defaults used
    repo.materialize()
    assert repo.load_node("overridden") == {"rate": 1.0, "label": "default"}

    # With overrides — only specified fields change
    repo.materialize(
        config={"overridden": {"rate": 9.9}},
    )
    assert repo.load_node("overridden") == {"rate": 9.9, "label": "default"}


def test_basemodel_uninitialized_instance_in_asset_fn_config_with_materialize_overrides():
    """BaseModel instance config is overridden via model_copy at materialize time."""

    class OverridableConfig(BaseModel):
        rate: float = 1.0
        label: str = "default"

    @rs.Asset
    def overridden(resource: OverridableConfig):
        return {"rate": resource.rate, "label": resource.label}

    repo = rs.CodeRepository(
        assets=[overridden], resources={"resource": OverridableConfig()}
    )

    # Without overrides — defaults used
    repo.materialize()
    assert repo.load_node("overridden") == {"rate": 1.0, "label": "default"}

    # With overrides — only specified fields change
    repo.materialize(
        config={"overridden": {"rate": 9.9}},
    )
    assert repo.load_node("overridden") == {"rate": 9.9, "label": "default"}
