import rivers as rs


def test_graph_asset_creation():
    @rs.Task
    def task_1():
        return 10

    @rs.Asset
    def asset_2():
        return 20

    @rs.Asset.from_graph(name="my_graph", kinds="polars", group="Sales")
    def my_graph():
        asset_2(task_1())

    assert isinstance(my_graph, rs.GraphAsset)
    assert my_graph.name == "my_graph"
    assert my_graph.is_graph


def test_graph_asset_name_derived():
    @rs.Task
    def t():
        return 1

    @rs.Asset.from_graph()
    def derived_name():
        t()

    assert derived_name.name == "derived_name"


def test_graph_asset_isinstance_checks():
    @rs.Asset.from_graph()
    def g():
        pass

    assert isinstance(g, rs.GraphAsset)
    assert not isinstance(g, rs.SingleAsset)
    assert not isinstance(g, rs.MultiAsset)


def test_graph_asset_in_repository():
    @rs.Asset
    def source():
        return 1

    @rs.Task
    def double():
        return 2

    @rs.Asset.from_graph(name="result")
    def result(source: int):
        double()

    repo = rs.CodeRepository(assets=[source, result], tasks=[double])
    assert "result" in repo.assets
    assert "source" in repo.assets


def test_graph_asset_composition_captures_nodes():
    """Verify that tasks and assets called inside from_graph are captured."""

    @rs.Task
    def step_1():
        return 1

    @rs.Task
    def step_2():
        return 2

    @rs.Asset.from_graph(name="pipeline")
    def pipeline():
        a = step_1()
        step_2(a)

    assert pipeline.is_graph
    assert pipeline.name == "pipeline"


# ---------------------------------------------------------------------------
# Execution tests
# ---------------------------------------------------------------------------


def test_graph_asset_executes_internal_tasks():
    """Graph asset executes its internal tasks and produces their outputs."""

    @rs.Task
    def step_one() -> int:
        return 10

    @rs.Task
    def step_two(step_one: int) -> int:
        return step_one * 2

    @rs.Asset.from_graph(name="pipeline")
    def pipeline():
        a = step_one()
        step_two(a)

    repo = rs.CodeRepository(
        assets=[pipeline],
        tasks=[step_one, step_two],
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize()

    assert result.success
    assert repo.load_node("pipeline/step_one") == 10
    assert repo.load_node("pipeline/step_two") == 20


def test_graph_asset_with_external_dependency():
    """Graph asset receives upstream asset output and passes it to internal tasks."""

    @rs.Asset
    def source() -> int:
        return 5

    @rs.Task()
    def double(source: int) -> int:
        return source * 2

    @rs.Asset.from_graph(name="pipeline")
    def pipeline(source: int):
        return double(source)

    repo = rs.CodeRepository(
        assets=[source, pipeline],
        tasks=[double],
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize()

    assert result.success
    assert repo.load_node("source") == 5
    assert repo.load_node("pipeline/double") == 10
    assert repo.load_node("pipeline") == 10


def test_downstream_asset_depends_on_graph_output():
    """A regular asset depends on the graph asset's output, not internal tasks."""

    @rs.Task
    def step() -> int:
        return 42

    @rs.Asset.from_graph(name="graph_pipe")
    def graph_pipe():
        return step()

    @rs.Asset
    def consumer(graph_pipe: int) -> str:
        return f"got {graph_pipe}"

    repo = rs.CodeRepository(
        assets=[graph_pipe, consumer],
        tasks=[step],
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize()

    assert result.success
    assert repo.load_node("graph_pipe/step") == 42
    assert repo.load_node("graph_pipe") == 42
    assert repo.load_node("consumer") == "got 42"


def test_graph_asset_error_propagation():
    """Error in a graph asset's internal task propagates correctly."""
    import pytest

    @rs.Task
    def bad_task() -> int:
        raise ValueError("boom")

    @rs.Asset.from_graph(name="pipe")
    def pipe():
        bad_task()

    repo = rs.CodeRepository(
        assets=[pipe],
        tasks=[bad_task],
        default_executor=rs.Executor.in_process(),
    )

    with pytest.raises(ValueError, match="boom"):
        repo.materialize()


def test_graph_asset_error_collecting_skips_downstream():
    """In collecting mode, a failed graph task skips the graph asset and its downstream."""

    @rs.Task
    def failing_task() -> int:
        raise ValueError("fail")

    @rs.Asset.from_graph(name="pipe")
    def pipe():
        return failing_task()

    @rs.Asset
    def downstream(pipe: int) -> int:
        return pipe + 1

    repo = rs.CodeRepository(
        assets=[pipe, downstream],
        tasks=[failing_task],
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize(raise_on_error=False)

    assert not result.success
    failure_names = [name for name, _ in result.failed_assets]
    assert "pipe/failing_task" in failure_names
    assert "downstream" in failure_names


def test_graph_asset_with_bash_task():
    """Graph asset can compose BashTasks."""

    echo = rs.BashTask(name="echo_step", command="echo hello")

    @rs.Asset.from_graph(name="bash_pipe")
    def bash_pipe():
        return echo()

    repo = rs.CodeRepository(
        assets=[bash_pipe],
        tasks=[echo],
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize()

    assert result.success
    assert repo.load_node("bash_pipe/echo_step") == "hello"
    assert repo.load_node("bash_pipe") == "hello"


def test_graph_asset_in_job():
    """Graph assets work through Job.execute(). Internal tasks are auto-included."""

    @rs.Task
    def compute() -> int:
        return 99

    @rs.Asset.from_graph(name="pipe")
    def pipe():
        return compute()

    repo = rs.CodeRepository(
        assets=[pipe],
        tasks=[compute],
        jobs=[
            rs.Job(
                name="test",
                assets=[pipe],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("test").execute()

    assert repo.load_node("pipe/compute") == 99
    assert repo.load_node("pipe") == 99


def test_graph_asset_multi_step_chain():
    """Graph asset with a chain of three tasks executes in correct order."""

    @rs.Task
    def a() -> int:
        return 1

    @rs.Task
    def b(value: int) -> int:
        return value + 10

    @rs.Task
    def c(value: int) -> int:
        return value + 100

    @rs.Asset.from_graph(name="chain")
    def chain():
        x = a()
        y = b(x)
        c(y)

    repo = rs.CodeRepository(
        assets=[chain],
        tasks=[a, b, c],
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize()

    assert result.success
    assert repo.load_node("chain/a") == 1
    assert repo.load_node("chain/b") == 11
    assert repo.load_node("chain/c") == 111


def test_graph_asset_multi_step_chain_with_task_input_override_and_asset_upstream():
    """Graph asset with mixed wiring: explicit kwarg + implicit asset resolution."""

    @rs.Asset
    def int_asset() -> int:
        return 1

    @rs.Task
    def a() -> int:
        return 1

    @rs.Task
    def b(value: int) -> int:
        return value + 10

    @rs.Task
    def c(value: int, int_asset: int) -> int:
        return value + 100 + int_asset

    @rs.Asset.from_graph(name="chain")
    def chain():
        x = a()
        y = b(x)
        return c(value=y)

    repo = rs.CodeRepository(
        assets=[int_asset, chain],
        tasks=[a, b, c],
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize()

    assert result.success
    assert repo.load_node("chain/a") == 1
    assert repo.load_node("chain/b") == 11
    assert repo.load_node("chain/c") == 112
    assert repo.load_node("chain") == 112


def test_two_graph_assets_share_same_task():
    """Two graph assets using the same task get independent namespaced copies."""

    @rs.Task
    def transform(value: int) -> int:
        return value * 2

    @rs.Asset
    def source_a() -> int:
        return 5

    @rs.Asset
    def source_b() -> int:
        return 10

    @rs.Asset.from_graph(name="pipeline_a")
    def pipeline_a(source_a: int):
        return transform(source_a)

    @rs.Asset.from_graph(name="pipeline_b")
    def pipeline_b(source_b: int):
        return transform(source_b)

    repo = rs.CodeRepository(
        assets=[source_a, source_b, pipeline_a, pipeline_b],
        tasks=[transform],
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize()

    assert result.success
    assert repo.load_node("pipeline_a/transform") == 10  # 5 * 2
    assert repo.load_node("pipeline_b/transform") == 20  # 10 * 2
    assert repo.load_node("pipeline_a") == 10
    assert repo.load_node("pipeline_b") == 20


# ---------------------------------------------------------------------------
# node_io_handler and rivers/node/executor
# ---------------------------------------------------------------------------


def test_node_io_handler_used_for_internal_tasks(tmp_path):
    """Internal tasks use node_io_handler while the graph asset uses io_handler."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    pickle_handler = rs.PickleIOHandler(store=store)

    @rs.Task
    def step_a() -> int:
        return 10

    @rs.Task
    def step_b(value: int) -> int:
        return value * 3

    @rs.Asset.from_graph(
        name="pipeline",
        node_io_handler=rs.InMemoryIOHandler(),
        io_handler=pickle_handler,
    )
    def pipeline():
        x = step_a()
        return step_b(x)

    repo = rs.CodeRepository(
        assets=[pipeline],
        tasks=[step_a, step_b],
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize()

    assert result.success
    # The graph asset output is persisted via pickle (io_handler)
    loaded = pickle_handler.load_input(
        rs.InputContext(asset_name="pipeline", downstream_asset="t")
    )
    assert loaded == 30
    # Internal tasks use InMemoryIOHandler — verify via load_node
    assert repo.load_node("pipeline/step_a") == 10
    assert repo.load_node("pipeline/step_b") == 30
    assert repo.load_node("pipeline") == 30


def test_graph_io_handler_does_not_propagate_to_tasks():
    """Graph asset's io_handler is for its own output, not for internal tasks.

    Internal tasks without their own io_handler use the default handler.
    Only node_io_handler propagates to internal tasks."""
    stored = {}

    class TrackingHandler(rs.BaseIOHandler):
        def handle_output(self, context: rs.OutputContext, obj):
            stored[context.asset_name] = obj

        def load_input(self, context: rs.InputContext):
            return stored[context.asset_name]

    handler = TrackingHandler()

    @rs.Task
    def compute() -> int:
        return 42

    @rs.Asset.from_graph(name="pipe", io_handler=handler)
    def pipe():
        return compute()

    repo = rs.CodeRepository(
        assets=[pipe],
        tasks=[compute],
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize()

    assert result.success
    # Graph output uses the handler, internal task uses default (in-memory)
    assert "pipe/compute" not in stored
    assert stored["pipe"] == 42


def test_node_io_handler_default_falls_back_to_in_memory():
    """When neither node_io_handler nor io_handler is set, internal tasks use InMemoryIOHandler."""

    @rs.Task
    def step() -> int:
        return 99

    @rs.Asset.from_graph(name="pipe")
    def pipe():
        return step()

    repo = rs.CodeRepository(
        assets=[pipe],
        tasks=[step],
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize()

    assert result.success
    assert repo.load_node("pipe/step") == 99
    assert repo.load_node("pipe") == 99


def test_node_executor_override():
    """rivers/node/executor metadata controls internal task executor."""

    @rs.Task
    def step() -> int:
        return 42

    # node_io_handler=InMemory means internal tasks don't need PickleIOHandler.
    # Without rivers/node/executor=in_process, parallel would reject InMemory.
    @rs.Asset.from_graph(
        name="pipe",
        metadata={"rivers/node/executor": "in_process"},
    )
    def pipe():
        return step()

    repo = rs.CodeRepository(
        assets=[pipe],
        tasks=[step],
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    result = repo.materialize()

    assert result.success
    assert repo.load_node("pipe/step") == 42


def test_node_executor_falls_back_to_graph_executor():
    """When rivers/node/executor is not set, internal tasks use rivers/executor."""

    @rs.Task
    def step() -> int:
        return 7

    # rivers/executor=in_process applies to both the graph asset AND its internal tasks
    @rs.Asset.from_graph(
        name="pipe",
        metadata={"rivers/executor": "in_process"},
    )
    def pipe():
        return step()

    repo = rs.CodeRepository(
        assets=[pipe],
        tasks=[step],
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    result = repo.materialize()

    assert result.success
    assert repo.load_node("pipe/step") == 7


def test_node_io_handler_resource_ref(tmp_path):
    """node_io_handler can be a string resource reference, resolved at resolve time."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    pickle_handler = rs.PickleIOHandler(store=store)
    mem_handler = rs.InMemoryIOHandler()

    @rs.Task
    def step() -> int:
        return 55

    @rs.Asset.from_graph(
        name="pipe",
        node_io_handler="my_handler",
        io_handler=pickle_handler,
    )
    def pipe():
        return step()

    repo = rs.CodeRepository(
        assets=[pipe],
        tasks=[step],
        resources={"my_handler": mem_handler},
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize()

    assert result.success
    # Internal task wrote to the InMemoryIOHandler's inner storage
    assert "pipe/step" in mem_handler._storage
    assert mem_handler._storage["pipe/step"] == 55
    # Graph asset output uses pickle (io_handler)
    loaded = pickle_handler.load_input(
        rs.InputContext(asset_name="pipe", downstream_asset="t")
    )
    assert loaded == 55


def test_io_handler_resource_ref_on_graph_asset(tmp_path):
    """io_handler on graph asset can be a string resource reference.

    The graph's io_handler applies to the graph output only, not internal tasks.
    Internal tasks use the default handler (in-memory)."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    pickle_handler = rs.PickleIOHandler(store=store)

    @rs.Task
    def step() -> int:
        return 77

    @rs.Asset.from_graph(
        name="pipe",
        io_handler="shared_handler",
    )
    def pipe():
        return step()

    repo = rs.CodeRepository(
        assets=[pipe],
        tasks=[step],
        resources={"shared_handler": pickle_handler},
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize()

    assert result.success
    # Only the graph output is written to pickle, internal task uses default
    import obstore

    files = sorted(obj["path"] for obj in obstore.list(store).collect())
    assert files == ["pipe.pkl"]
    loaded_pipe = pickle_handler.load_input(
        rs.InputContext(asset_name="pipe", downstream_asset="t")
    )
    assert loaded_pipe == 77


def test_input_dep_io_handler_resource_ref_on_graph_asset():
    """Graph asset's input-dep io_handler given as a resource-ref string is
    resolved to its Instance for the namespaced composition tasks.

    Regression: ``input_io_handler_override`` is propagated from the parent
    graph asset to its namespaced ``ResolvedTask`` nodes during
    ``build_unresolved_graph``, *before* ``resolve_io_handler_refs`` runs.
    Without a re-resolution pass, the override stays as
    ``IOHandler::ResourceRef("override_handler")`` and tripping
    ``expect_resolved_handler`` panics with ``unreachable!``.
    """

    class RecordingHandler(rs.BaseIOHandler):
        store: dict[str, object] = {}
        loads: list[str] = []

        def handle_output(self, context, obj):
            self.store[context.asset_name] = obj

        def load_input(self, context):
            self.loads.append(context.asset_name)
            return 999  # distinguishable from upstream's real value

    upstream_handler = RecordingHandler()
    upstream_handler.store = {}
    upstream_handler.loads = []
    override_handler = RecordingHandler()
    override_handler.store = {}
    override_handler.loads = []

    @rs.Asset(io_handler=upstream_handler)
    def upstream() -> int:
        return 42

    @rs.Task
    def step(value: int) -> int:
        return value + 1

    @rs.Asset.from_graph(
        name="pipe",
        deps=[rs.AssetDef.input("upstream", io_handler="override_handler")],
    )
    def pipe(upstream: int):
        return step(value=upstream)

    repo = rs.CodeRepository(
        assets=[upstream, pipe],
        tasks=[step],
        resources={"override_handler": override_handler},
        default_executor=rs.Executor.in_process(),
    )
    result = repo.materialize()

    assert result.success
    # The override handler (not upstream's own) was used to load `upstream`
    # into `pipe/step`; its load_input returns 999, so pipe/step computes 1000.
    assert "upstream" in override_handler.loads
    assert repo.load_node("pipe/step") == 1000


# ---------------------------------------------------------------------------
# Event ordering tests
# ---------------------------------------------------------------------------


def test_graph_asset_event_ordering(storage):
    """Graph asset step_start fires before first internal task, materialization + success after last."""

    @rs.Task
    def step_a() -> int:
        return 1

    @rs.Task
    def step_b(value: int) -> int:
        return value + 10

    @rs.Asset.from_graph(name="pipe")
    def pipe():
        x = step_a()
        return step_b(x)

    repo = rs.CodeRepository(
        assets=[pipe],
        tasks=[step_a, step_b],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    result = repo.materialize()
    assert result.success

    # Collect all events for all assets in order
    events = []
    for name in ["pipe", "pipe/step_a", "pipe/step_b"]:
        for ev in storage.get_events_for_asset(name, limit=100):
            events.append((ev.event_type, ev.asset_key))

    # Sort by timestamp to get chronological order
    all_events = []
    for name in ["pipe", "pipe/step_a", "pipe/step_b"]:
        for ev in storage.get_events_for_asset(name, limit=100):
            all_events.append((ev.timestamp, ev.event_type, ev.asset_key))
    all_events.sort(key=lambda x: x[0])
    _ordered = [(et, ak) for (_, et, ak) in all_events]

    # pipe StepStart must come before pipe/step_a StepStart
    pipe_start_ts = next(
        t for t, et, ak in all_events if et == "StepStart" and ak == "pipe"
    )
    step_a_start_ts = next(
        t for t, et, ak in all_events if et == "StepStart" and ak == "pipe/step_a"
    )
    assert pipe_start_ts <= step_a_start_ts, (
        f"pipe StepStart ({pipe_start_ts}) should be <= pipe/step_a StepStart ({step_a_start_ts})"
    )

    # pipe/step_b success must come before or at pipe Materialization
    step_b_success_ts = next(
        t for t, et, ak in all_events if et == "StepSuccess" and ak == "pipe/step_b"
    )
    pipe_mat_ts = next(
        t for t, et, ak in all_events if et == "Materialization" and ak == "pipe"
    )
    assert step_b_success_ts <= pipe_mat_ts, (
        f"pipe/step_b StepSuccess ({step_b_success_ts}) should be <= pipe Materialization ({pipe_mat_ts})"
    )

    # All expected event types are present for the graph asset
    pipe_types = {et for _, et, ak in all_events if ak == "pipe"}
    assert pipe_types == {"StepStart", "StepSuccess", "Materialization"}


def test_graph_asset_materialization_event_recorded(storage):
    """Graph asset has a Materialization event in storage after execution."""

    @rs.Task
    def compute() -> int:
        return 42

    @rs.Asset.from_graph(name="pipe")
    def pipe():
        return compute()

    repo = rs.CodeRepository(
        assets=[pipe],
        tasks=[compute],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    repo.materialize()

    mat = storage.get_latest_materialization("pipe")
    assert mat is not None
    assert mat.asset_key == "pipe"

    # Internal task also has its own materialization
    task_mat = storage.get_latest_materialization("pipe/compute")
    assert task_mat is not None
    assert task_mat.asset_key == "pipe/compute"


def test_graph_asset_failure_emits_no_success(storage):
    """When a graph internal task fails, the graph asset does not emit success."""

    @rs.Task
    def failing() -> int:
        raise ValueError("boom")

    @rs.Asset.from_graph(name="pipe")
    def pipe():
        return failing()

    repo = rs.CodeRepository(
        assets=[pipe],
        tasks=[failing],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=False)

    assert not result.success

    # pipe should have StepStart but no StepSuccess or Materialization
    pipe_events = storage.get_events_for_asset("pipe", limit=100)
    pipe_types = [ev.event_type for ev in pipe_events]
    assert "StepStart" in pipe_types
    assert "StepSuccess" not in pipe_types
    assert "Materialization" not in pipe_types


def _collect_events(storage, asset_names):
    """Collect all events sorted by timestamp, return list of (timestamp, event_type, asset_key)."""
    all_events = []
    for name in asset_names:
        for ev in storage.get_events_for_asset(name, limit=100):
            all_events.append((ev.timestamp, ev.event_type, ev.asset_key))
    all_events.sort(key=lambda x: x[0])
    return all_events


def _assert_no_duplicate_events(events, asset_name):
    """Assert no duplicate event types for an asset (each type fires exactly once)."""
    types = [et for (_, et, ak) in events if ak == asset_name]
    for et in ["StepStart", "StepSuccess", "Materialization"]:
        count = types.count(et)
        assert count <= 1, f"{asset_name} has {count} {et} events (expected at most 1)"


def _assert_event_present(events, asset_name, event_type):
    """Assert an event type exists for an asset."""
    assert any(et == event_type and ak == asset_name for (_, et, ak) in events), (
        f"Missing {event_type} for {asset_name}"
    )


def _assert_order(events, before_name, before_type, after_name, after_type):
    """Assert event A's timestamp <= event B's timestamp."""
    ts_a = next(t for t, et, ak in events if et == before_type and ak == before_name)
    ts_b = next(t for t, et, ak in events if et == after_type and ak == after_name)
    assert ts_a <= ts_b, (
        f"{before_name} {before_type} ({ts_a}) should be <= {after_name} {after_type} ({ts_b})"
    )


# ---------------------------------------------------------------------------
# Scenario tests: asset invocations inside graph assets + event validation
# ---------------------------------------------------------------------------


def test_graph_asset_with_asset_params(storage):
    """Assets passed as graph function parameters."""

    @rs.Asset
    def source() -> int:
        return 5

    @rs.Asset
    def config() -> dict:
        return {"multiplier": 3}

    @rs.Task
    def transform(source: int) -> int:
        return source * 2

    @rs.Task
    def apply_config(value: int, config: dict) -> int:
        return value * config["multiplier"]

    @rs.Asset.from_graph(name="pipeline")
    def pipeline(source: int, config: dict):
        x = transform(source)
        return apply_config(x, config)

    repo = rs.CodeRepository(
        assets=[source, config, pipeline],
        tasks=[transform, apply_config],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    result = repo.materialize()

    assert result.success
    assert repo.load_node("source") == 5
    assert repo.load_node("config") == {"multiplier": 3}
    assert repo.load_node("pipeline/transform") == 10
    assert repo.load_node("pipeline/apply_config") == 30
    assert repo.load_node("pipeline") == 30

    names = [
        "source",
        "config",
        "pipeline",
        "pipeline/transform",
        "pipeline/apply_config",
    ]
    events = _collect_events(storage, names)
    for name in names:
        _assert_no_duplicate_events(events, name)
        _assert_event_present(events, name, "Materialization")
    _assert_order(events, "pipeline", "StepStart", "pipeline/transform", "StepStart")


def test_graph_asset_with_asset_called_in_body(storage):
    """Asset called inside the graph body (not as a parameter)."""

    @rs.Asset
    def shared_data() -> int:
        return 100

    @rs.Task
    def step_a() -> int:
        return 5

    @rs.Task
    def step_b(value: int, shared_data: int) -> int:
        return value + shared_data

    @rs.Asset.from_graph(name="pipeline")
    def pipeline():
        data = shared_data()
        x = step_a()
        return step_b(x, data)

    repo = rs.CodeRepository(
        assets=[shared_data, pipeline],
        tasks=[step_a, step_b],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    result = repo.materialize()

    assert result.success
    assert repo.load_node("shared_data") == 100
    assert repo.load_node("pipeline/step_a") == 5
    assert repo.load_node("pipeline/step_b") == 105
    assert repo.load_node("pipeline") == 105

    names = ["shared_data", "pipeline", "pipeline/step_a", "pipeline/step_b"]
    events = _collect_events(storage, names)
    for name in names:
        _assert_no_duplicate_events(events, name)
        _assert_event_present(events, name, "Materialization")
    _assert_order(events, "pipeline", "StepStart", "pipeline/step_a", "StepStart")
    _assert_order(
        events, "pipeline/step_b", "StepSuccess", "pipeline", "Materialization"
    )


def test_graph_asset_shared_with_external_downstream(storage):
    """Asset used both inside a graph and by an external downstream asset."""

    @rs.Asset
    def external_source() -> int:
        return 42

    @rs.Task
    def process(external_source: int) -> int:
        return external_source * 2

    @rs.Asset.from_graph(name="graph_a")
    def graph_a():
        return process(external_source())

    @rs.Asset
    def downstream(graph_a: int, external_source: int) -> str:
        return f"graph={graph_a}, source={external_source}"

    repo = rs.CodeRepository(
        assets=[external_source, graph_a, downstream],
        tasks=[process],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    result = repo.materialize()

    assert result.success
    assert repo.load_node("external_source") == 42
    assert repo.load_node("graph_a/process") == 84
    assert repo.load_node("graph_a") == 84
    assert repo.load_node("downstream") == "graph=84, source=42"

    names = ["external_source", "graph_a", "graph_a/process", "downstream"]
    events = _collect_events(storage, names)
    for name in names:
        _assert_no_duplicate_events(events, name)
        _assert_event_present(events, name, "Materialization")


def test_two_graphs_sharing_external_asset(storage):
    """Two graph assets both invoking the same external asset."""

    @rs.Asset
    def shared() -> int:
        return 10

    @rs.Task
    def double(value: int) -> int:
        return value * 2

    @rs.Task
    def triple(value: int) -> int:
        return value * 3

    @rs.Asset.from_graph(name="graph_a")
    def graph_a():
        return double(shared())

    @rs.Asset.from_graph(name="graph_b")
    def graph_b():
        return triple(shared())

    repo = rs.CodeRepository(
        assets=[shared, graph_a, graph_b],
        tasks=[double, triple],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    result = repo.materialize()

    assert result.success
    assert repo.load_node("shared") == 10
    assert repo.load_node("graph_a/double") == 20
    assert repo.load_node("graph_a") == 20
    assert repo.load_node("graph_b/triple") == 30
    assert repo.load_node("graph_b") == 30

    names = ["shared", "graph_a", "graph_a/double", "graph_b", "graph_b/triple"]
    events = _collect_events(storage, names)
    for name in names:
        _assert_no_duplicate_events(events, name)
        _assert_event_present(events, name, "Materialization")

    # shared should only have one materialization (not duplicated per graph)
    shared_mats = [
        1 for (_, et, ak) in events if et == "Materialization" and ak == "shared"
    ]
    assert len(shared_mats) == 1


def test_graph_asset_kwarg_wiring_with_asset(storage):
    """Asset passed via kwarg inside graph body."""

    @rs.Asset
    def lookup() -> dict:
        return {"key": "value"}

    @rs.Task
    def generate() -> int:
        return 7

    @rs.Task
    def use_lookup(data: int, lookup: dict) -> str:
        return f"{data}-{lookup['key']}"

    @rs.Asset.from_graph(name="pipe")
    def pipe():
        x = generate()
        lk = lookup()
        return use_lookup(data=x, lookup=lk)

    repo = rs.CodeRepository(
        assets=[lookup, pipe],
        tasks=[generate, use_lookup],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    result = repo.materialize()

    assert result.success
    assert repo.load_node("pipe/generate") == 7
    assert repo.load_node("pipe/use_lookup") == "7-value"
    assert repo.load_node("pipe") == "7-value"

    names = ["lookup", "pipe", "pipe/generate", "pipe/use_lookup"]
    events = _collect_events(storage, names)
    for name in names:
        _assert_no_duplicate_events(events, name)
        _assert_event_present(events, name, "Materialization")
    _assert_order(events, "pipe", "StepStart", "pipe/generate", "StepStart")


def test_graph_asset_composition_order_preserved(storage):
    """Tasks at the same level execute in composition order, not arbitrary topo order."""

    @rs.Asset
    def data() -> int:
        return 1

    @rs.Task
    def first_task(data: int) -> int:
        return data + 10

    @rs.Task
    def second_task(data: int) -> int:
        return data + 20

    @rs.Asset.from_graph(name="pipe")
    def pipe(data: int):
        # first_task should execute before second_task (composition order)
        first_task(data)
        second_task(data)

    repo = rs.CodeRepository(
        assets=[data, pipe],
        tasks=[first_task, second_task],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    result = repo.materialize()

    assert result.success
    assert repo.load_node("pipe/first_task") == 11
    assert repo.load_node("pipe/second_task") == 21

    events = _collect_events(storage, ["pipe", "pipe/first_task", "pipe/second_task"])
    # first_task should start before or at the same time as second_task
    _assert_order(
        events, "pipe/first_task", "StepStart", "pipe/second_task", "StepStart"
    )
