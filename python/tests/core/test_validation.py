"""Tests for input validation and error paths."""

import pytest

import rivers as rs
from rivers.exceptions import (
    AssetDefinitionError,
    ExecutionError,
    GraphValidationError,
    InvalidMetadataError,
    PartitionDefinitionError,
    TaskDefinitionError,
)

# ---------------------------------------------------------------------------
# BashTask validation
# ---------------------------------------------------------------------------


def test_bash_task_empty_command_list_raises():
    """BashTask with empty list raises ValueError."""
    with pytest.raises(TaskDefinitionError, match="command list must not be empty"):
        rs.BashTask(name="x", command=[])


def test_bash_task_invalid_command_type_raises():
    """BashTask with non-str/list command raises TypeError."""
    with pytest.raises(TypeError, match="command must be a str or list"):
        rs.BashTask(name="x", command=123)  # type: ignore


# ---------------------------------------------------------------------------
# PartitionKey validation
# ---------------------------------------------------------------------------


def test_partition_key_single_empty_list_raises():
    """PartitionKey.single([]) raises ValueError."""
    with pytest.raises(PartitionDefinitionError, match="must not be empty"):
        rs.PartitionKey.single([])


def test_partition_key_single_multi_value():
    """PartitionKey.single(["a", "b"]) stores a multi-value single key."""
    key = rs.PartitionKey.single(["a", "b"])
    assert key.key == ["a", "b"]


# ---------------------------------------------------------------------------
# External asset validation
# ---------------------------------------------------------------------------


class DummyHandler(rs.BaseIOHandler):
    def handle_output(self, context, obj):
        pass

    def load_input(self, context):
        return None


def test_external_asset_no_io_handler_raises():
    """Asset.external() without io_handler raises ValueError."""
    with pytest.raises(AssetDefinitionError, match="require an io_handler"):
        rs.Asset.external(name="x")  # type: ignore


def test_io_handler_non_base_io_handler_raises():
    """io_handler that is not a BaseIOHandler subclass raises ValueError."""

    class BadHandler:
        def handle_output(self, context, obj):
            pass

        def load_input(self, context):
            return None

    with pytest.raises(AssetDefinitionError, match="BaseIOHandler"):
        rs.Asset.external(name="x", io_handler=BadHandler())  # type: ignore


def test_io_handler_duck_typing_rejected():
    """io_handler that duck-types the protocol (no inheritance) is rejected."""

    class DuckHandler:
        def handle_output(self, context, obj):
            pass

        def load_input(self, context):
            return None

    with pytest.raises(AssetDefinitionError, match="BaseIOHandler"):
        rs.Asset.external(name="x", io_handler=DuckHandler())  # type: ignore


def test_asset_invalid_io_handler_raises():
    """Asset with invalid io_handler (non-string, non-IOHandler) raises at definition time."""
    with pytest.raises(AssetDefinitionError, match="BaseIOHandler"):
        # not a string ref, not a BaseIOHandler
        @rs.Asset(io_handler=42)  # type: ignore
        def my_asset():
            return 1


def test_external_asset_observe_no_fn_skipped():
    """External asset without observe_fn is skipped by repo.observe()."""
    ext = rs.Asset.external(name="x", io_handler=DummyHandler())
    repo = rs.CodeRepository(assets=[ext])
    result = repo.observe()
    assert "x" not in result


def test_in_latest_time_window_requires_time_partitioning():
    """Root-scope in_latest_time_window() on a non-time-partitioned asset fails resolve."""

    @rs.Asset(
        partitions_def=rs.PartitionsDefinition.Static(["us", "eu"]),
        automation_condition=rs.AutomationCondition.missing()
        & rs.AutomationCondition.in_latest_time_window(),
    )
    def static_asset():
        return 1

    repo = rs.CodeRepository(assets=[static_asset])
    with pytest.raises(AssetDefinitionError, match="in_latest_time_window"):
        repo.resolve()


def test_in_latest_time_window_allowed_inside_dep_aggregate():
    """Dep-scoped in_latest_time_window() filters the dep's partitions and resolves fine."""

    @rs.Asset(
        automation_condition=rs.AutomationCondition.any_deps_match(
            rs.AutomationCondition.newly_updated()
            & rs.AutomationCondition.in_latest_time_window()
        ),
    )
    def watcher():
        return 1

    repo = rs.CodeRepository(assets=[watcher])
    repo.resolve()


# ---------------------------------------------------------------------------
# Job validation
# ---------------------------------------------------------------------------


def test_job_non_asset_object_raises():
    """Passing a plain Python object to Job raises ValueError."""
    with pytest.raises(GraphValidationError, match="must be Asset, Task, or BashTask"):
        rs.Job(name="bad", assets=["not_an_asset"])  # type: ignore


def test_job_execute_before_validation_raises():
    """Executing a standalone job (not added to repo) raises ValueError."""

    @rs.Asset
    def a():
        return 1

    job = rs.Job(name="orphan", assets=[a], executor=rs.Executor.in_process())
    with pytest.raises(ExecutionError, match="not been validated"):
        job.execute()


# ---------------------------------------------------------------------------
# CodeRepository validation
# ---------------------------------------------------------------------------


def test_repo_empty_assets():
    """CodeRepository with empty assets is valid."""
    repo = rs.CodeRepository(assets=[])
    result = repo.materialize()
    assert result.success is True


def test_repo_observe_no_externals():
    """repo.observe() returns empty dict when no external assets exist."""

    @rs.Asset
    def a():
        return 1

    repo = rs.CodeRepository(assets=[a])
    assert repo.observe() == {}


# ---------------------------------------------------------------------------
# MetadataValue validation
# ---------------------------------------------------------------------------


def test_metadata_url_invalid_raises():
    """MetadataValue.url() with an invalid URL raises ValueError."""
    with pytest.raises(InvalidMetadataError, match="Invalid URL"):
        rs.MetadataValue.url("not a url")


def test_metadata_coerce_non_primitive_raises():
    """Passing a non-coercible value to add_output_metadata raises TypeError."""
    ctx = rs.OutputContext(asset_name="test", asset_metadata=None)
    with pytest.raises(TypeError, match="Cannot coerce"):
        ctx.add_output_metadata({"bad": {"nested": "dict"}})  # type: ignore


# ---------------------------------------------------------------------------
# Dep detection from parameter signatures
# ---------------------------------------------------------------------------


def test_unannotated_param_matches_asset_name_treated_as_dep(storage):
    """Bare param names matching an asset are treated as upstream deps — the
    type annotation is informational only. Backed by `inspect.signature`
    rather than `__annotations__` (see `enumerate_params`)."""

    @rs.Asset
    def upstream() -> int:
        return 42

    @rs.Asset
    def downstream(upstream) -> int:  # noqa: ANN001 -- intentionally unannotated
        return upstream + 1

    repo = rs.CodeRepository(
        assets=[upstream, downstream],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=False)
    assert result.success, f"materialize failed: {result.failed_assets}"
    assert result.materialized_assets == ["upstream", "downstream"]


# ---------------------------------------------------------------------------
# Cycle detection
# ---------------------------------------------------------------------------


def test_simple_cycle_raises_graph_validation_error():
    """Two assets depending on each other raises GraphValidationError."""

    @rs.Asset
    def a(b: int) -> int:
        return b + 1

    @rs.Asset
    def b(a: int) -> int:
        return a + 1

    repo = rs.CodeRepository(assets=[a, b])
    with pytest.raises(
        GraphValidationError, match="Cycle detected in asset graph: b -> a -> b"
    ):
        repo.resolve()


def test_three_node_cycle_names_all_assets():
    """Cycle of three assets names all three in the error message."""

    @rs.Asset
    def random(x: int) -> int:
        return x + 1

    @rs.Asset
    def x(z: int) -> int:
        return z

    @rs.Asset
    def y(x: int) -> int:
        return x

    @rs.Asset
    def z(y: int) -> int:
        return y

    repo = rs.CodeRepository(assets=[x, y, z, random])
    with pytest.raises(
        GraphValidationError,
        match="Cycle detected in asset graph: y -> z -> x -> y",
    ):
        repo.resolve()


def test_cycle_does_not_name_non_cyclic_nodes():
    """Non-cyclic nodes are not mentioned in the cycle error."""

    @rs.Asset
    def root() -> int:
        return 1

    @rs.Asset
    def a(b: int, root: int) -> int:
        return b

    @rs.Asset
    def b(a: int) -> int:
        return a

    repo = rs.CodeRepository(assets=[root, a, b])
    with pytest.raises(
        GraphValidationError, match="Cycle detected in asset graph: b -> a -> b"
    ):
        repo.resolve()


def test_self_dependency_raises_with_hint():
    """An asset listing itself as a parameter raises a clear error pointing to SelfDependency."""

    @rs.Asset
    def foo(foo: int) -> int:
        return foo

    repo = rs.CodeRepository(assets=[foo])
    with pytest.raises(
        GraphValidationError,
        match=r"Asset 'foo': depends on itself.*SelfDependency",
    ):
        repo.resolve()


def test_acyclic_graph_resolves_without_error():
    """A valid DAG resolves without raising."""

    @rs.Asset
    def a() -> int:
        return 1

    @rs.Asset
    def b(a: int) -> int:
        return a + 1

    @rs.Asset
    def c(a: int, b: int) -> int:
        return a + b

    repo = rs.CodeRepository(assets=[a, b, c])
    # Should not raise
    assert "a" in repo.assets
    assert "b" in repo.assets
    assert "c" in repo.assets


# ---------------------------------------------------------------------------
# CodeRepository.validate (storage-independent dry-run)
# ---------------------------------------------------------------------------


def test_validate_passes_on_valid_repo():
    """validate() returns Ok on a clean repo without resolving storage."""

    @rs.Asset
    def a() -> int:
        return 1

    @rs.Asset
    def b(a: int) -> int:
        return a + 1

    repo = rs.CodeRepository(assets=[a, b])
    repo.validate()  # should not raise


def test_validate_re_runs_unlike_resolve():
    """Unlike resolve(), validate() has no idempotency guard — it can be called
    repeatedly. This is the property that makes it useful for IDE/UI live checks."""

    @rs.Asset
    def a() -> int:
        return 1

    repo = rs.CodeRepository(assets=[a])
    for _ in range(3):
        repo.validate()  # should not raise on repeat calls


def test_validate_does_not_call_resource_setup():
    """validate() must not invoke resource.setup() — that's a side effect
    reserved for full resolve()."""

    setup_calls: list[str] = []

    class TrackingResource(rs.Resource):
        def setup(self) -> None:
            setup_calls.append("called")

    @rs.Asset
    def a(my_res: TrackingResource) -> int:
        return 1

    repo = rs.CodeRepository(assets=[a], resources={"my_res": TrackingResource()})
    repo.validate()
    assert setup_calls == [], "validate() must not invoke resource.setup()"

    # By contrast, resolve() does invoke it.
    repo.resolve()
    assert setup_calls == ["called"]


def test_validate_surfaces_missing_resource_reference():
    """validate() catches the same graph errors that resolve() would,
    without needing storage."""

    @rs.Asset
    def needs_db(db: object) -> int:
        return 1

    repo = rs.CodeRepository(assets=[needs_db])
    with pytest.raises(GraphValidationError, match="db"):
        repo.validate()


def test_validate_surfaces_external_observe_fn_required():
    """validate() catches the external+automation_condition without observe_fn rule."""
    bad = rs.Asset.external(
        name="bad",
        io_handler=rs.InMemoryIOHandler(),
        automation_condition=rs.AutomationCondition.missing(),
    )
    repo = rs.CodeRepository(assets=[bad])
    with pytest.raises(AssetDefinitionError, match="observe function"):
        repo.validate()
