"""Tests for multi-asset generator yield protocol and subsetting."""

from typing import Any

import obstore.store
import pytest

import rivers as rs

from _helpers import DictIOHandler


@pytest.fixture(params=["in_process", "parallel"])
def executor_env(request, tmp_path):
    """Provide (executor, io_handler_factory) appropriate for the executor type."""
    if request.param == "in_process":
        return rs.Executor.in_process(), lambda: DictIOHandler()
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    return rs.Executor.parallel(max_workers=2), lambda: rs.PickleIOHandler(store=store)


# ── Output/Observation output_name field ──────────────────────────────


def test_output_output_name_field():
    """Output accepts output_name keyword and exposes it as a property."""
    o = rs.Output(value=42, output_name="my_out")
    assert o.output_name == "my_out"
    assert o.value == 42


def test_output_output_name_defaults_to_none():
    """Output.output_name defaults to None when not provided."""
    o = rs.Output(value=42)
    assert o.output_name is None


def test_output_output_name_in_repr():
    """Output repr includes output_name when set."""
    o = rs.Output(value=1, output_name="x")
    assert "output_name='x'" in repr(o)


def test_output_repr_without_output_name():
    """Output repr omits output_name when not set."""
    o = rs.Output(value=1)
    assert "output_name" not in repr(o)


def test_observation_output_name_field():
    """Observation accepts output_name keyword and exposes it."""
    o = rs.Observation(output_name="obs_out", data_version="v1")
    assert o.output_name == "obs_out"
    assert o.data_version == "v1"


def test_observation_output_name_defaults_to_none():
    """Observation.output_name defaults to None."""
    o = rs.Observation(data_version="v1")
    assert o.output_name is None


def test_observation_output_name_in_repr():
    """Observation repr includes output_name when set."""
    o = rs.Observation(output_name="x")
    assert "output_name='x'" in repr(o)


def test_observation_repr_without_output_name():
    """Observation repr omits output_name when not set."""
    o = rs.Observation(data_version="v1")
    assert "output_name" not in repr(o)


def test_output_backwards_compat_positional_value():
    """Existing Output(value) positional call still works."""
    o = rs.Output(42)
    assert o.value == 42
    assert o.output_name is None


def test_output_backwards_compat_keyword_metadata():
    """Existing Output(value, metadata={...}) call still works."""
    o = rs.Output(42, metadata={"k": "v"})
    assert o.value == 42
    assert o.output_name is None
    assert o.metadata is not None


# ── Generator / dict-return multi-asset across executors ──────────────


def test_generator_multi_asset(executor_env, storage):
    """Generator multi-asset yields Output per output and emits one Materialization each."""
    executor, make_handler = executor_env
    io = make_handler()

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("a", io_handler=io), rs.AssetDef("b", io_handler=io)],
    )
    def gen_multi():
        yield rs.Output(value=10, output_name="a")
        yield rs.Output(value=20, output_name="b")

    repo = rs.CodeRepository(assets=[gen_multi], default_executor=executor)
    repo.resolve(storage=storage)
    result = repo.materialize()

    assert result.success is True
    assert repo.load_node("a") == 10
    assert repo.load_node("b") == 20

    for name in ("a", "b"):
        events = storage.get_events_for_asset(name)
        mats = [e for e in events if e.event_type == "Materialization"]
        assert len(mats) == 1


def test_generator_multi_asset_with_metadata_and_data_version(storage):
    """Generator yield carries per-output metadata and data_version to storage events."""
    handler = DictIOHandler()

    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("a", io_handler=handler),
            rs.AssetDef("b", io_handler=handler),
        ],
    )
    def gen_meta():
        yield rs.Output(
            value=1,
            output_name="a",
            metadata={"rows": rs.MetadataValue.int(100)},
            data_version="va",
        )
        yield rs.Output(
            value=2,
            output_name="b",
            metadata={"rows": rs.MetadataValue.int(200)},
            data_version="vb",
        )

    repo = rs.CodeRepository(
        assets=[gen_meta], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    repo.materialize()

    # Check events for output "a"
    events_a = storage.get_events_for_asset("a")
    mat_a = [e for e in events_a if e.event_type == "Materialization"]
    assert len(mat_a) == 1
    assert mat_a[0].data_version == "va"
    meta_a = dict(mat_a[0].metadata)
    assert "rows" in meta_a

    # Check events for output "b"
    events_b = storage.get_events_for_asset("b")
    mat_b = [e for e in events_b if e.event_type == "Materialization"]
    assert len(mat_b) == 1
    assert mat_b[0].data_version == "vb"
    meta_b = dict(mat_b[0].metadata)
    assert "rows" in meta_b


def test_generator_multi_asset_context_available():
    """Generator multi-asset receives AssetExecutionContext with correct fields."""
    captured = {}

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("p"), rs.AssetDef("q")],
    )
    def gen_ctx(context: rs.AssetExecutionContext):
        captured["asset_name"] = context.asset_name
        captured["is_multi_asset"] = context.is_multi_asset
        captured["output_selection"] = list(context.output_selection)
        yield rs.Output(value=1, output_name="p")
        yield rs.Output(value=2, output_name="q")

    repo = rs.CodeRepository(
        assets=[gen_ctx], default_executor=rs.Executor.in_process()
    )
    repo.materialize()

    assert captured["asset_name"] == "gen_ctx"
    assert captured["is_multi_asset"] is True
    assert set(captured["output_selection"]) == {"p", "q"}


def test_generator_multi_asset_missing_output_name_fails():
    """Yielding Output without output_name in multi-asset raises ExecutionError."""

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("a"), rs.AssetDef("b")],
    )
    def bad_gen():
        yield rs.Output(value=1)  # missing output_name

    repo = rs.CodeRepository(
        assets=[bad_gen], default_executor=rs.Executor.in_process()
    )
    result = repo.materialize(raise_on_error=False)
    assert result.success is False


def test_generator_multi_asset_raw_yield_fails():
    """Yielding a raw value (not Output/Observation) in multi-asset raises error."""

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("a")],
    )
    def raw_gen():
        yield 42  # not Output or Observation

    repo = rs.CodeRepository(
        assets=[raw_gen], default_executor=rs.Executor.in_process()
    )
    result = repo.materialize(raise_on_error=False)
    assert result.success is False


def test_generator_multi_asset_missing_selected_output_fails(storage):
    """Generator that doesn't yield all selected outputs fails for missing ones."""
    handler = DictIOHandler()

    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("a", io_handler=handler),
            rs.AssetDef("b", io_handler=handler),
        ],
    )
    def partial_gen():
        yield rs.Output(value=1, output_name="a")
        # "b" is never yielded

    repo = rs.CodeRepository(
        assets=[partial_gen], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    result = repo.materialize(raise_on_error=False)

    assert result.success is False
    # "a" was materialized
    assert handler.store["a"] == 1
    # "b" should have a failure event
    events_b = storage.get_events_for_asset("b")
    failure_events = [e for e in events_b if e.event_type == "StepFailure"]
    assert len(failure_events) == 1


def test_generator_multi_asset_observation_yield(storage):
    """Observation yield emits observation event, no IO."""
    handler = DictIOHandler()

    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("a", io_handler=handler),
            rs.AssetDef("b", io_handler=handler),
        ],
    )
    def obs_gen():
        yield rs.Output(value=10, output_name="a")
        yield rs.Observation(
            output_name="b", data_version="obs-v1", metadata={"status": "skipped"}
        )

    repo = rs.CodeRepository(
        assets=[obs_gen], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    repo.materialize()

    # "a" should have a materialization event and IO
    assert handler.store["a"] == 10
    events_a = storage.get_events_for_asset("a")
    mat_events_a = [e for e in events_a if e.event_type == "Materialization"]
    assert len(mat_events_a) == 1

    # "b" should have an observation event, NOT materialization
    events_b = storage.get_events_for_asset("b")
    obs_events_b = [e for e in events_b if e.event_type == "Observation"]
    assert len(obs_events_b) == 1
    assert obs_events_b[0].data_version == "obs-v1"
    # "b" should NOT be in the IO handler store (no IO for observations)
    assert "b" not in handler.store


def test_generator_multi_asset_downstream_receives_values():
    """Downstream assets receive values from generator multi-asset outputs."""
    handler = DictIOHandler()

    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("x", io_handler=handler),
            rs.AssetDef("y", io_handler=handler),
        ],
    )
    def producer():
        yield rs.Output(value=10, output_name="x")
        yield rs.Output(value=20, output_name="y")

    @rs.Asset
    def consumer(x: int, y: int) -> int:
        return x + y

    repo = rs.CodeRepository(
        assets=[producer, consumer], default_executor=rs.Executor.in_process()
    )
    repo.materialize()

    assert repo.load_node("consumer") == 30


def test_generator_multi_asset_io_failure_aborts():
    """IO failure on one yield aborts generator, remaining outputs fail."""
    call_order = []

    class FailOnSecondHandler(rs.BaseIOHandler):
        store: dict[str, Any] = {}

        def handle_output(self, context: rs.OutputContext, obj):
            call_order.append(context.asset_name)
            if context.asset_name == "b":
                raise RuntimeError("IO failed for b")
            self.store[context.asset_name] = obj

        def load_input(self, context: rs.InputContext):
            return self.store.get(context.asset_name)

    handler = FailOnSecondHandler()

    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("a", io_handler=handler),
            rs.AssetDef("b", io_handler=handler),
            rs.AssetDef("c", io_handler=handler),
        ],
    )
    def fail_gen():
        yield rs.Output(value=1, output_name="a")
        yield rs.Output(value=2, output_name="b")  # IO fails here
        yield rs.Output(value=3, output_name="c")  # should not be reached

    repo = rs.CodeRepository(
        assets=[fail_gen], default_executor=rs.Executor.in_process()
    )
    result = repo.materialize(raise_on_error=False)

    assert result.success is False
    assert "a" in call_order
    assert "b" in call_order
    assert "c" not in call_order  # aborted before reaching c


# ── Subsetting (output_selection) ─────────────────────────────────────


def test_output_selection_reflects_actual_selection():
    """context.output_selection shows only the requested outputs when subsetting."""
    captured = {}

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("a"), rs.AssetDef("b"), rs.AssetDef("c")],
    )
    def multi(context: rs.AssetExecutionContext):
        captured["output_selection"] = list(context.output_selection)
        return {"a": 1, "b": 2, "c": 3}

    repo = rs.CodeRepository(assets=[multi], default_executor=rs.Executor.in_process())
    # Select only output "a" from the multi-asset
    repo.materialize(selection=["a"])

    assert captured["output_selection"] == ["a"]


def test_output_selection_contains_all_when_no_subsetting():
    """context.output_selection contains all outputs when no subsetting is requested."""
    captured = {}

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("a"), rs.AssetDef("b")],
    )
    def multi(context: rs.AssetExecutionContext):
        captured["output_selection"] = sorted(context.output_selection)
        return {"a": 1, "b": 2}

    repo = rs.CodeRepository(assets=[multi], default_executor=rs.Executor.in_process())
    repo.materialize()

    assert captured["output_selection"] == ["a", "b"]


def test_dict_return_subsetting_discards_non_selected(storage):
    """Dict-return multi-asset: non-selected outputs are not persisted."""
    handler_a = DictIOHandler()
    handler_b = DictIOHandler()

    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("a", io_handler=handler_a),
            rs.AssetDef("b", io_handler=handler_b),
        ],
    )
    def multi():
        return {"a": 10, "b": 20}

    repo = rs.CodeRepository(assets=[multi], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    # Select only "a"
    repo.materialize(selection=["a"])

    # "a" should be materialized
    assert handler_a.store["a"] == 10
    events_a = storage.get_events_for_asset("a")
    mat_a = [e for e in events_a if e.event_type == "Materialization"]
    assert len(mat_a) == 1

    # "b" should NOT be materialized (not selected)
    assert "b" not in handler_b.store
    events_b = storage.get_events_for_asset("b")
    mat_b = [e for e in events_b if e.event_type == "Materialization"]
    assert len(mat_b) == 0


def test_generator_subsetting_skips_non_selected_yields(storage):
    """Generator yield for non-selected output is silently skipped."""
    handler_a = DictIOHandler()
    handler_b = DictIOHandler()
    yield_order = []

    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("a", io_handler=handler_a),
            rs.AssetDef("b", io_handler=handler_b),
        ],
    )
    def gen_multi():
        yield_order.append("a")
        yield rs.Output(value=10, output_name="a")
        yield_order.append("b")
        yield rs.Output(value=20, output_name="b")

    repo = rs.CodeRepository(
        assets=[gen_multi], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    # Select only "a"
    repo.materialize(selection=["a"])

    # Both yields happened (generator ran fully)
    assert yield_order == ["a", "b"]
    # "a" should be in IO store (selected)
    assert handler_a.store["a"] == 10
    # "b" should NOT be in IO store (not selected, silently skipped)
    assert "b" not in handler_b.store
    # No materialization event for "b"
    events_b = storage.get_events_for_asset("b")
    mat_b = [e for e in events_b if e.event_type == "Materialization"]
    assert len(mat_b) == 0


def test_generator_subsetting_optimization_via_output_selection():
    """Function can check output_selection to skip computation entirely."""
    computed = []

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("a"), rs.AssetDef("b")],
    )
    def gen_opt(context: rs.AssetExecutionContext):
        if "a" in context.output_selection:
            computed.append("a")
            yield rs.Output(value=1, output_name="a")
        if "b" in context.output_selection:
            computed.append("b")
            yield rs.Output(value=2, output_name="b")

    repo = rs.CodeRepository(
        assets=[gen_opt], default_executor=rs.Executor.in_process()
    )
    # Select only "a"
    repo.materialize(selection=["a"])

    # Only "a" was computed
    assert computed == ["a"]


# ── Dict-return still works (backwards compat) ───────────────────────


def test_dict_return_multi_asset(executor_env, storage):
    """Dict-return multi-asset (legacy form) emits one Materialization per output."""
    executor, make_handler = executor_env
    io = make_handler()

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("a", io_handler=io), rs.AssetDef("b", io_handler=io)],
    )
    def dict_multi():
        return {"a": 10, "b": 20}

    repo = rs.CodeRepository(assets=[dict_multi], default_executor=executor)
    repo.resolve(storage=storage)
    result = repo.materialize()

    assert result.success is True
    assert repo.load_node("a") == 10
    assert repo.load_node("b") == 20

    for name in ("a", "b"):
        events = storage.get_events_for_asset(name)
        mats = [e for e in events if e.event_type == "Materialization"]
        assert len(mats) == 1


# ── Success hooks fire per-yield ──────────────────────────────────────


def test_generator_multi_asset_success_hooks_fire_per_output():
    """Success hooks fire per yielded output."""
    hook_calls = []

    @rs.Hook.success
    def on_success(context: rs.HookContext):
        hook_calls.append(context.asset_name)

    handler = DictIOHandler()

    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("a", io_handler=handler),
            rs.AssetDef("b", io_handler=handler),
        ],
        hooks=[on_success],
    )
    def gen_hooks():
        yield rs.Output(value=1, output_name="a")
        yield rs.Output(value=2, output_name="b")

    repo = rs.CodeRepository(
        assets=[gen_hooks], default_executor=rs.Executor.in_process()
    )
    repo.materialize()

    assert "a" in hook_calls
    assert "b" in hook_calls
    assert len(hook_calls) == 2


# ── parallel observation yields ──────────────────────────────────


def test_generator_observation_yield_parallel(tmp_path, storage):
    """parallel: Observation yield emits Observation event, not Materialization."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    io = rs.PickleIOHandler(store=store)

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("a", io_handler=io), rs.AssetDef("b", io_handler=io)],
    )
    def gen_obs_mp():
        yield rs.Output(value=42, output_name="a")
        yield rs.Observation(output_name="b", data_version="obs-v1")

    repo = rs.CodeRepository(
        assets=[gen_obs_mp],
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    repo.resolve(storage=storage)
    result = repo.materialize()

    assert result.success is True

    # "a" should have a Materialization event
    events_a = storage.get_events_for_asset("a")
    mat_a = [e for e in events_a if e.event_type == "Materialization"]
    obs_a = [e for e in events_a if e.event_type == "Observation"]
    assert len(mat_a) == 1
    assert len(obs_a) == 0

    # "b" should have an Observation event, NOT Materialization
    events_b = storage.get_events_for_asset("b")
    mat_b = [e for e in events_b if e.event_type == "Materialization"]
    obs_b = [e for e in events_b if e.event_type == "Observation"]
    assert len(mat_b) == 0
    assert len(obs_b) == 1


def test_generator_observation_metadata_preserved_parallel(tmp_path, storage):
    """parallel: Observation's per-yield metadata and data_version reach the stored event,
    and materialization metadata from Output does NOT leak into the Observation event."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    io = rs.PickleIOHandler(store=store)

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("a", io_handler=io), rs.AssetDef("b", io_handler=io)],
    )
    def gen_obs_meta_mp():
        yield rs.Output(
            value=1,
            output_name="a",
            data_version="mat-v1",
            metadata={"mat_only_key": rs.MetadataValue.text("should_not_leak")},
        )
        yield rs.Observation(
            output_name="b",
            data_version="obs-v2",
            metadata={"obs_key": rs.MetadataValue.text("obs_value")},
        )

    repo = rs.CodeRepository(
        assets=[gen_obs_meta_mp],
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    repo.resolve(storage=storage)
    result = repo.materialize()

    assert result.success is True

    # "b" observation event must carry the per-yield data_version
    events_b = storage.get_events_for_asset("b")
    obs_b = [e for e in events_b if e.event_type == "Observation"]
    assert len(obs_b) == 1
    assert obs_b[0].data_version == "obs-v2"

    # observation metadata must contain obs_key
    meta_b = dict(obs_b[0].metadata)
    assert "obs_key" in meta_b

    # materialization-only metadata must NOT leak into the observation event
    assert "mat_only_key" not in meta_b


def test_generator_missing_output_parallel_fails(tmp_path, storage):
    """parallel: generator that skips a selected output should not emit
    a false StepSuccess/Materialization for the missing output."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    io = rs.PickleIOHandler(store=store)

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("a", io_handler=io), rs.AssetDef("b", io_handler=io)],
    )
    def gen_partial_mp():
        yield rs.Output(value=1, output_name="a")
        # "b" is never yielded

    repo = rs.CodeRepository(
        assets=[gen_partial_mp],
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    repo.resolve(storage=storage)
    _result = repo.materialize(raise_on_error=False)

    # "a" was yielded — should have a Materialization event
    events_a = storage.get_events_for_asset("a")
    mat_a = [e for e in events_a if e.event_type == "Materialization"]
    assert len(mat_a) == 1

    # "b" was never yielded — must NOT have a Materialization event
    events_b = storage.get_events_for_asset("b")
    mat_b = [e for e in events_b if e.event_type == "Materialization"]
    assert len(mat_b) == 0

    # "b" must have a StepFailure event
    fail_b = [e for e in events_b if e.event_type == "StepFailure"]
    assert len(fail_b) == 1


def test_generator_per_output_metadata_not_merged_parallel(tmp_path, storage):
    """parallel: per-output metadata from different yields must NOT bleed
    across outputs. If 'a' sets key 'rows'=100 and 'b' sets 'rows'=200,
    each materialization event must carry its own value."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    io = rs.PickleIOHandler(store=store)

    @rs.Asset.from_multi(
        output_defs=[rs.AssetDef("a", io_handler=io), rs.AssetDef("b", io_handler=io)],
    )
    def gen_conflicting_meta():
        yield rs.Output(
            value=1,
            output_name="a",
            metadata={
                "rows": rs.MetadataValue.int(100),
                "only_a": rs.MetadataValue.text("a"),
            },
        )
        yield rs.Output(
            value=2,
            output_name="b",
            metadata={
                "rows": rs.MetadataValue.int(200),
                "only_b": rs.MetadataValue.text("b"),
            },
        )

    repo = rs.CodeRepository(
        assets=[gen_conflicting_meta],
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    repo.resolve(storage=storage)
    result = repo.materialize()
    assert result.success is True

    events_a = storage.get_events_for_asset("a")
    mat_a = [e for e in events_a if e.event_type == "Materialization"]
    assert len(mat_a) == 1
    assert mat_a[0].metadata == [
        ("rows", '{"Int":{"value":100}}'),
        ("only_a", '{"Text":{"value":"a"}}'),
    ]

    events_b = storage.get_events_for_asset("b")
    mat_b = [e for e in events_b if e.event_type == "Materialization"]
    assert len(mat_b) == 1
    assert mat_b[0].metadata == [
        ("rows", '{"Int":{"value":200}}'),
        ("only_b", '{"Text":{"value":"b"}}'),
    ]


def test_generator_context_metadata_applies_to_all_yields(storage):
    """context.add_output_metadata() in a generator applies as shared metadata to all yields."""

    @rs.Asset.from_multi(output_defs=[rs.AssetDef("a"), rs.AssetDef("b")])
    def gen(context: rs.AssetExecutionContext):
        context.add_output_metadata({"shared": rs.MetadataValue.text("from_ctx")})
        yield rs.Output(
            value=1,
            output_name="a",
            metadata={"only_a": rs.MetadataValue.text("a_val")},
        )
        yield rs.Output(value=2, output_name="b")

    repo = rs.CodeRepository(assets=[gen], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    repo.materialize()

    # "a" gets shared + per-yield metadata
    events_a = storage.get_events_for_asset("a")
    mat_a = [e for e in events_a if e.event_type == "Materialization"]
    assert len(mat_a) == 1
    meta_a = dict(mat_a[0].metadata)
    assert "shared" in meta_a
    assert "only_a" in meta_a

    # "b" gets shared metadata only
    events_b = storage.get_events_for_asset("b")
    mat_b = [e for e in events_b if e.event_type == "Materialization"]
    assert len(mat_b) == 1
    meta_b = dict(mat_b[0].metadata)
    assert "shared" in meta_b
    assert "only_a" not in meta_b


# ── Parallel executor: async multi-assets ──


def test_async_generator_multi_asset_parallel(tmp_path):
    """Async generator multi-asset under Parallel executor with PickleIOHandler."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("ag_x", io_handler=handler),
            rs.AssetDef("ag_y", io_handler=handler),
        ],
    )
    async def par_async_gen():
        import asyncio

        await asyncio.sleep(0.01)
        yield rs.Output(value=10, output_name="ag_x")
        await asyncio.sleep(0.01)
        yield rs.Output(value=20, output_name="ag_y")

    repo = rs.CodeRepository(
        assets=[par_async_gen],
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    result = repo.materialize(raise_on_error=False)
    assert result.success

    def load(name):
        return handler.load_input(
            rs.InputContext(asset_name=name, downstream_asset="t")
        )

    assert load("ag_x") == 10
    assert load("ag_y") == 20


# ── Regression: per-output attribution under loky (forces sync_instances > 1) ──
#
# The existing parallel multi-asset tests above have only the multi-asset at
# the dependency level, so parallel's `sync_instances.len() == 1` shortcut
# falls back to the InProcess backend — which preserves per-output metadata
# correctly via dispatch/results.rs:handle_dict_multi / handle_generator_multi.
# To exercise the actual loky path (worker_execute_step in parallel/worker.rs),
# the level must contain >1 sync instance. These tests pin a sibling root asset
# to the same level so the multi-asset is dispatched via loky.


def test_parallel_loky_multi_asset_per_output_metadata_and_dv_dict(tmp_path, storage):
    """parallel/loky: dict-return multi-asset must preserve per-output metadata
    and data_version. Forces loky by adding a sibling sync asset at the same
    level so parallel does not fall back to the size-1 InProcess shortcut."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    io = rs.PickleIOHandler(store=store)

    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("a", io_handler=io),
            rs.AssetDef("b", io_handler=io),
        ],
    )
    def dict_meta_loky():
        return {
            "a": rs.Output(
                value=1,
                metadata={
                    "rows": rs.MetadataValue.int(100),
                    "only_a": rs.MetadataValue.text("a"),
                },
                data_version="dv_a",
            ),
            "b": rs.Output(
                value=2,
                metadata={
                    "rows": rs.MetadataValue.int(200),
                    "only_b": rs.MetadataValue.text("b"),
                },
                data_version="dv_b",
            ),
        }

    @rs.Asset(io_handler=io)
    def sibling_dict():
        return 0

    repo = rs.CodeRepository(
        assets=[dict_meta_loky, sibling_dict],
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    repo.resolve(storage=storage)
    result = repo.materialize()
    assert result.success is True

    events_a = storage.get_events_for_asset("a")
    mat_a = [e for e in events_a if e.event_type == "Materialization"]
    assert len(mat_a) == 1, mat_a
    meta_a = dict(mat_a[0].metadata)
    assert meta_a.get("rows") == '{"Int":{"value":100}}', meta_a
    assert "only_a" in meta_a, meta_a
    assert "only_b" not in meta_a, f"only_b leaked into a: {meta_a}"
    assert mat_a[0].data_version == "dv_a", mat_a[0].data_version

    events_b = storage.get_events_for_asset("b")
    mat_b = [e for e in events_b if e.event_type == "Materialization"]
    assert len(mat_b) == 1, mat_b
    meta_b = dict(mat_b[0].metadata)
    assert meta_b.get("rows") == '{"Int":{"value":200}}', meta_b
    assert "only_b" in meta_b, meta_b
    assert "only_a" not in meta_b, f"only_a leaked into b: {meta_b}"
    assert mat_b[0].data_version == "dv_b", mat_b[0].data_version


def test_parallel_loky_multi_asset_per_output_metadata_and_dv_generator(
    tmp_path, storage
):
    """parallel/loky: generator multi-asset must preserve per-output metadata
    and data_version. Same forcing trick as the dict variant above."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    io = rs.PickleIOHandler(store=store)

    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("a", io_handler=io),
            rs.AssetDef("b", io_handler=io),
        ],
    )
    def gen_meta_loky():
        yield rs.Output(
            value=1,
            output_name="a",
            metadata={
                "rows": rs.MetadataValue.int(100),
                "only_a": rs.MetadataValue.text("a"),
            },
            data_version="dv_a",
        )
        yield rs.Output(
            value=2,
            output_name="b",
            metadata={
                "rows": rs.MetadataValue.int(200),
                "only_b": rs.MetadataValue.text("b"),
            },
            data_version="dv_b",
        )

    @rs.Asset(io_handler=io)
    def sibling_gen():
        return 0

    repo = rs.CodeRepository(
        assets=[gen_meta_loky, sibling_gen],
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    repo.resolve(storage=storage)
    result = repo.materialize()
    assert result.success is True

    events_a = storage.get_events_for_asset("a")
    mat_a = [e for e in events_a if e.event_type == "Materialization"]
    assert len(mat_a) == 1, mat_a
    meta_a = dict(mat_a[0].metadata)
    assert meta_a.get("rows") == '{"Int":{"value":100}}', meta_a
    assert "only_a" in meta_a, meta_a
    assert "only_b" not in meta_a, f"only_b leaked into a: {meta_a}"
    assert mat_a[0].data_version == "dv_a", mat_a[0].data_version

    events_b = storage.get_events_for_asset("b")
    mat_b = [e for e in events_b if e.event_type == "Materialization"]
    assert len(mat_b) == 1, mat_b
    meta_b = dict(mat_b[0].metadata)
    assert meta_b.get("rows") == '{"Int":{"value":200}}', meta_b
    assert "only_b" in meta_b, meta_b
    assert "only_a" not in meta_b, f"only_a leaked into b: {meta_b}"
    assert mat_b[0].data_version == "dv_b", mat_b[0].data_version


def test_async_dict_multi_asset_parallel(tmp_path):
    """Async dict-return multi-asset under Parallel executor with PickleIOHandler."""
    import obstore.store

    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    @rs.Asset.from_multi(
        output_defs=[
            rs.AssetDef("ad_p", io_handler=handler),
            rs.AssetDef("ad_q", io_handler=handler),
        ],
    )
    async def par_async_dict():
        import asyncio

        await asyncio.sleep(0.01)
        return {"ad_p": 100, "ad_q": 200}

    repo = rs.CodeRepository(
        assets=[par_async_dict],
        default_executor=rs.Executor.parallel(max_workers=2),
    )
    result = repo.materialize(raise_on_error=False)
    assert result.success

    def load(name):
        return handler.load_input(
            rs.InputContext(asset_name=name, downstream_asset="t")
        )

    assert load("ad_p") == 100
    assert load("ad_q") == 200
