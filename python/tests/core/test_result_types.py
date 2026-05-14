"""Tests for Output and Observation result types."""

import rivers as rs

from _helpers import DictIOHandler


# ── Output basics ──────────────────────────────────────────────────────


def test_output_value_propagates():
    """Asset returning Output(value=42) — value propagates downstream."""

    @rs.Asset
    def a() -> rs.Output:
        return rs.Output(value=42)

    @rs.Asset
    def b(a: int) -> int:
        return a + 1

    repo = rs.CodeRepository(assets=[a, b])
    repo.materialize()

    assert repo.load_node("a") == 42
    assert repo.load_node("b") == 43


def test_output_io_handler_receives_unwrapped_value():
    """IO handler receives the unwrapped value, not the Output wrapper."""
    handler = DictIOHandler()

    @rs.Asset(io_handler=handler)
    def a() -> rs.Output:
        return rs.Output(value={"key": "data"})

    repo = rs.CodeRepository(assets=[a])
    repo.materialize()

    assert handler.store["a"] == {"key": "data"}
    assert not isinstance(handler.store["a"], rs.Output)


def test_output_with_metadata(storage):
    """Output metadata appears in materialization event."""
    handler = DictIOHandler()

    @rs.Asset(io_handler=handler)
    def a() -> rs.Output:
        return rs.Output(
            value=42,
            metadata={"row_count": rs.MetadataValue.int(100)},
        )

    repo = rs.CodeRepository(assets=[a])
    repo.resolve(storage=storage)
    repo.materialize()

    events = storage.get_events_for_asset("a")
    mat_events = [e for e in events if e.event_type == "Materialization"]
    assert len(mat_events) == 1
    meta_keys = [k for k, _ in mat_events[0].metadata]
    assert "row_count" in meta_keys


def test_output_with_data_version(storage):
    """Output data_version overrides auto UUID."""
    handler = DictIOHandler()

    @rs.Asset(io_handler=handler)
    def a() -> rs.Output:
        return rs.Output(value=42, data_version="v2.0")

    repo = rs.CodeRepository(assets=[a])
    repo.resolve(storage=storage)
    repo.materialize()

    events = storage.get_events_for_asset("a")
    mat_events = [e for e in events if e.event_type == "Materialization"]
    assert len(mat_events) == 1
    assert mat_events[0].data_version == "v2.0"


def test_output_none_value():
    """Output with no value still works — value defaults to None."""

    @rs.Asset
    def a() -> rs.Output:
        return rs.Output(metadata={"status": "done"})

    repo = rs.CodeRepository(assets=[a])
    repo.materialize()

    assert repo.load_node("a") is None


# ── Backward compatibility ─────────────────────────────────────────────


def test_raw_value_still_works():
    """Returning a raw value (not Output) still works identically."""

    @rs.Asset
    def a() -> int:
        return 42

    repo = rs.CodeRepository(assets=[a])
    repo.materialize()
    assert repo.load_node("a") == 42


# ── Metadata merging ──────────────────────────────────────────────────


def test_context_and_output_metadata_merged(storage):
    """Both context.add_output_metadata and Output metadata are merged.
    Output metadata takes precedence on conflict."""
    handler = DictIOHandler()

    @rs.Asset(io_handler=handler)
    def a(context: rs.AssetExecutionContext) -> rs.Output:
        context.add_output_metadata(
            {
                "from_ctx": rs.MetadataValue.text("ctx"),
                "shared": rs.MetadataValue.text("old"),
            }
        )
        return rs.Output(
            value=1,
            metadata={
                "from_output": rs.MetadataValue.text("out"),
                "shared": rs.MetadataValue.text("new"),
            },
        )

    repo = rs.CodeRepository(assets=[a])
    repo.resolve(storage=storage)
    repo.materialize()

    events = storage.get_events_for_asset("a")
    mat_events = [e for e in events if e.event_type == "Materialization"]
    assert len(mat_events) == 1
    meta = dict(mat_events[0].metadata)
    assert "from_ctx" in meta
    assert "from_output" in meta
    # "shared" should have the Output value (precedence)
    assert "shared" in meta
    assert "new" in meta["shared"]  # Output wins


def test_context_and_output_data_version(storage):
    """Output data_version overrides context.register_data_version."""
    handler = DictIOHandler()

    @rs.Asset(io_handler=handler)
    def a(context: rs.AssetExecutionContext) -> rs.Output:
        context.register_data_version("from_ctx")
        return rs.Output(value=1, data_version="from_output")

    repo = rs.CodeRepository(assets=[a])
    repo.resolve(storage=storage)
    repo.materialize()

    events = storage.get_events_for_asset("a")
    mat_events = [e for e in events if e.event_type == "Materialization"]
    assert mat_events[0].data_version == "from_output"


# ── Type validation ────────────────────────────────────────────────────


def test_type_hint_output_skips_validation():
    """Return type -> Output skips inner value type validation."""

    @rs.Asset
    def a() -> rs.Output:
        return rs.Output(value="a string, not an int")

    repo = rs.CodeRepository(assets=[a])
    repo.materialize()
    assert repo.load_node("a") == "a string, not an int"


def test_type_hint_int_with_output_validates_unwrapped():
    """Return type -> int with Output(value=42) validates the unwrapped int."""

    @rs.Asset
    def a() -> int:
        return rs.Output(value=42)

    repo = rs.CodeRepository(assets=[a])
    repo.materialize()
    assert repo.load_node("a") == 42


# ── Observation ────────────────────────────────────────────────────────


def test_observation_from_observe_fn():
    """External asset observe_fn returning Observation carries metadata."""
    handler = DictIOHandler()

    @rs.Asset.external(io_handler=handler)
    def source(context: rs.AssetExecutionContext):
        return rs.Observation(
            metadata={"row_count": rs.MetadataValue.int(42)},
            data_version="v1",
        )

    repo = rs.CodeRepository(assets=[source])
    result = repo.observe()

    assert "source" in result
    assert result["source"]["row_count"].raw_value() == 42


def test_observation_metadata_merges_with_context():
    """Observation metadata merges with context metadata (Observation wins)."""
    handler = DictIOHandler()

    @rs.Asset.external(io_handler=handler)
    def source(context: rs.AssetExecutionContext):
        context.add_output_metadata({"from_ctx": rs.MetadataValue.text("ctx")})
        return rs.Observation(
            metadata={"from_obs": rs.MetadataValue.text("obs")},
        )

    repo = rs.CodeRepository(assets=[source])
    result = repo.observe()

    assert "source" in result
    meta = result["source"]
    assert meta["from_ctx"].raw_value() == "ctx"
    assert meta["from_obs"].raw_value() == "obs"


def test_observation_emits_storage_event(storage):
    """Observation emits EventType::Observation to storage."""
    handler = DictIOHandler()

    @rs.Asset.external(io_handler=handler)
    def source(context: rs.AssetExecutionContext):
        return rs.Observation(data_version="obs-v1")

    repo = rs.CodeRepository(assets=[source])
    repo.resolve(storage=storage)
    repo.observe()

    events = storage.get_events_for_asset("source")
    obs_events = [e for e in events if e.event_type == "Observation"]
    assert len(obs_events) == 1
    assert obs_events[0].data_version == "obs-v1"


# ── RunResult rename ───────────────────────────────────────────────────


def test_run_result_type():
    """materialize() returns a RunResult."""

    @rs.Asset
    def a() -> int:
        return 1

    repo = rs.CodeRepository(assets=[a])
    result = repo.materialize()
    assert isinstance(result, rs.RunResult)


# ── Output repr ────────────────────────────────────────────────────────


def test_output_repr():
    o = rs.Output(value=42, metadata={"k": "v"})
    assert "Output(" in repr(o)


def test_observation_repr():
    o = rs.Observation(data_version="v1")
    assert "Observation(" in repr(o)


# ── Direct construction and getter tests ──────────────────────────────


def test_output_tags_getter():
    o = rs.Output(value=1, tags=["raw", "daily"])
    assert o.tags == ["raw", "daily"]


def test_output_tags_none_by_default():
    o = rs.Output(value=1)
    assert o.tags is None


def test_output_output_name_getter():
    o = rs.Output(value=1, output_name="my_out")
    assert o.output_name == "my_out"


def test_output_output_name_none_by_default():
    o = rs.Output()
    assert o.output_name is None


def test_output_value_defaults_to_none():
    o = rs.Output()
    assert o.value is None


def test_output_data_version_getter():
    o = rs.Output(value=1, data_version="v42")
    assert o.data_version == "v42"


def test_output_repr_all_fields():
    o = rs.Output(value=42, output_name="out", data_version="v1", metadata={"k": "v"})
    r = repr(o)
    assert "output_name='out'" in r
    assert "data_version='v1'" in r
    assert "has_value=true" in r.lower() or "has_value=True" in r


def test_output_repr_no_fields():
    o = rs.Output()
    r = repr(o)
    assert "has_value=false" in r.lower() or "has_value=False" in r


def test_observation_output_name_getter():
    o = rs.Observation(output_name="obs_out", data_version="v1")
    assert o.output_name == "obs_out"
    assert o.data_version == "v1"


def test_observation_repr_with_output_name():
    o = rs.Observation(output_name="obs_out")
    assert "output_name='obs_out'" in repr(o)


def test_observation_repr_empty():
    o = rs.Observation()
    assert repr(o) == "Observation()"


def test_observation_metadata_none_by_default():
    o = rs.Observation()
    assert o.metadata is None


def test_dynamic_output_repr():
    d = rs.DynamicOutput(key="batch_1", value=42)
    assert repr(d) == "DynamicOutput(key='batch_1')"


def test_dynamic_output_getters():
    d = rs.DynamicOutput(key="k1", value=[1, 2, 3])
    assert d.key == "k1"
    assert d.value == [1, 2, 3]


# ── Materialization (user-managed persistence) ────────────────────────


def test_materialization_skips_io_handler():
    """Returning Materialization(...) must not invoke handle_output."""
    handler = DictIOHandler()
    handler.store.clear()

    @rs.Asset(io_handler=handler)
    def push_to_api() -> rs.Materialization:
        return rs.Materialization(
            metadata={"status_code": rs.MetadataValue.int(200)},
            data_version="etag-abc",
        )

    repo = rs.CodeRepository(assets=[push_to_api])
    repo.materialize()

    assert "push_to_api" not in handler.store


def test_materialization_emits_event_with_metadata_and_dv(storage):
    """Materialization records a Materialization event with metadata + dv."""
    handler = DictIOHandler()
    handler.store.clear()

    @rs.Asset(io_handler=handler)
    def push_to_api() -> rs.Materialization:
        return rs.Materialization(
            metadata={"row_count": rs.MetadataValue.int(123)},
            data_version="etag-xyz",
        )

    repo = rs.CodeRepository(assets=[push_to_api])
    repo.resolve(storage=storage)
    repo.materialize()

    events = storage.get_events_for_asset("push_to_api")
    mat_events = [e for e in events if e.event_type == "Materialization"]
    assert len(mat_events) == 1
    assert mat_events[0].data_version == "etag-xyz"
    meta_keys = [k for k, _ in mat_events[0].metadata]
    assert "row_count" in meta_keys


def test_materialization_default_uuid_dv_when_unspecified(storage):
    """No explicit data_version: materialization gets an auto-UUID dv."""

    @rs.Asset
    def push_to_api() -> rs.Materialization:
        return rs.Materialization(metadata={"k": rs.MetadataValue.int(1)})

    repo = rs.CodeRepository(assets=[push_to_api])
    repo.resolve(storage=storage)
    repo.materialize()

    events = storage.get_events_for_asset("push_to_api")
    mat_events = [e for e in events if e.event_type == "Materialization"]
    assert len(mat_events) == 1
    assert mat_events[0].data_version  # non-empty


def test_materialization_repr():
    m = rs.Materialization(data_version="v1", metadata={"k": "v"})
    r = repr(m)
    assert "Materialization(" in r
    assert "data_version='v1'" in r


def test_materialization_repr_empty():
    m = rs.Materialization()
    assert "Materialization(" in repr(m)


def test_materialization_getters():
    m = rs.Materialization(
        output_name="out",
        data_version="v1",
        tags=["t"],
        metadata={"k": rs.MetadataValue.int(1)},
    )
    assert m.output_name == "out"
    assert m.data_version == "v1"
    assert m.tags == ["t"]
    assert m.metadata is not None


def test_materialization_defaults_to_none():
    m = rs.Materialization()
    assert m.output_name is None
    assert m.data_version is None
    assert m.tags is None
    assert m.metadata is None


def test_materialization_under_parallel_executor(storage):
    """Materialization must skip IO under Executor.parallel() too — same uniform path."""

    @rs.Asset
    def push_to_api() -> rs.Materialization:
        return rs.Materialization(
            metadata={"k": rs.MetadataValue.int(1)},
            data_version="parallel-v1",
        )

    repo = rs.CodeRepository(
        assets=[push_to_api], default_executor=rs.Executor.parallel(max_workers=1)
    )
    repo.resolve(storage=storage)
    repo.materialize()

    events = storage.get_events_for_asset("push_to_api")
    mat_events = [e for e in events if e.event_type == "Materialization"]
    assert len(mat_events) == 1
    assert mat_events[0].data_version == "parallel-v1"
