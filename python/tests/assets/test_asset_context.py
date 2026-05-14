import logging
from datetime import datetime

import pytest
import rivers as rs
from rivers.exceptions import ExecutionError, PartitionValidationError


def test_context_injection_basic():
    """Asset with context: AssetExecutionContext gets context injected."""

    @rs.Asset
    def my_asset(context: rs.AssetExecutionContext):
        return context.asset_name

    repo = rs.CodeRepository(
        assets=[my_asset],
        jobs=[
            rs.Job(
                name="context_injection_basic",
                assets=[my_asset],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("context_injection_basic").execute()
    assert repo.load_node("my_asset") == "my_asset"


def test_context_asset_properties():
    """Verify tags, kinds, group, code_version from asset definition."""

    @rs.Asset(
        tags=["tag1", "tag2"], kinds="table", group="analytics", code_version="v1"
    )
    def props_asset(context: rs.AssetExecutionContext):
        return {
            "tags": context.tags,
            "kinds": context.kinds,
            "group": context.group,
            "code_version": context.code_version,
        }

    repo = rs.CodeRepository(
        assets=[props_asset],
        jobs=[
            rs.Job(
                name="context_asset_properties",
                assets=[props_asset],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("context_asset_properties").execute()
    output = repo.load_node("props_asset")
    assert output["tags"] == ["tag1", "tag2"]
    assert output["kinds"] == ["table"]
    assert output["group"] == "analytics"
    assert output["code_version"] == "v1"


def test_context_metadata():
    """Verify asset_metadata from @Asset(metadata={...})."""

    @rs.Asset(metadata={"env": "prod", "team": "data"})
    def meta_asset(context: rs.AssetExecutionContext):
        return context.asset_metadata

    repo = rs.CodeRepository(
        assets=[meta_asset],
        jobs=[
            rs.Job(
                name="context_metadata",
                assets=[meta_asset],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("context_metadata").execute()
    assert repo.load_node("meta_asset") == {"env": "prod", "team": "data"}


def test_context_add_output_metadata_flows_to_io_handler():
    """Asset calls context.add_output_metadata(...), verify IO handler receives it."""
    captured = {}

    class CapturingHandler(rs.BaseIOHandler):
        def handle_output(self, context: rs.OutputContext, obj):
            captured["metadata"] = context.output_metadata

        def load_input(self, context: rs.InputContext):
            pass

    @rs.Asset(io_handler=CapturingHandler())
    def meta_output(context: rs.AssetExecutionContext):
        context.add_output_metadata({"rows": 42, "status": "ok"})
        return "data"

    repo = rs.CodeRepository(
        assets=[meta_output],
        jobs=[
            rs.Job(
                name="context_output_metadata",
                assets=[meta_output],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("context_output_metadata").execute()
    assert captured["metadata"] is not None
    assert captured["metadata"]["rows"].raw_value() == 42
    assert captured["metadata"]["status"].raw_value() == "ok"


def test_context_no_partition():
    """Verify has_partition_key is False and partition_key raises for non-partitioned asset."""

    @rs.Asset
    def no_part(context: rs.AssetExecutionContext):
        return {
            "has_key": context.has_partition_key,
            "partition": context.partition,
        }

    repo = rs.CodeRepository(
        assets=[no_part],
        jobs=[
            rs.Job(
                name="context_no_partition",
                assets=[no_part],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("context_no_partition").execute()
    output = repo.load_node("no_part")
    assert output["has_key"] is False
    assert output["partition"] is None


def test_context_partition_key_raises_when_no_partition():
    """Accessing partition_key on non-partitioned asset raises ValueError."""

    ctx = rs.AssetExecutionContext("test")
    with pytest.raises(PartitionValidationError, match="No partition key"):
        _ = ctx.partition_key


def test_context_log():
    """Verify context.log returns a logger with name code-repo.assets.<name>."""

    @rs.Asset
    def log_asset(context: rs.AssetExecutionContext):
        return context.log.name

    repo = rs.CodeRepository(
        assets=[log_asset],
        jobs=[
            rs.Job(
                name="context_log",
                assets=[log_asset],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("context_log").execute()
    assert repo.load_node("log_asset") == "code-repo.assets.log_asset"


def test_context_log_is_logger_instance():
    """Verify context.log is a standard Python logger."""
    ctx = rs.AssetExecutionContext("test_asset")
    assert isinstance(ctx.log, logging.Logger)


def test_context_not_injected_when_not_requested():
    """Asset without context param still works normally."""

    @rs.Asset
    def plain():
        return 42

    repo = rs.CodeRepository(
        assets=[plain],
        jobs=[
            rs.Job(
                name="context_not_injected",
                assets=[plain],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("context_not_injected").execute()
    assert repo.load_node("plain") == 42


def test_context_with_upstream_inputs():
    """Asset with both context and upstream data params."""

    @rs.Asset
    def source():
        return 10

    @rs.Asset
    def consumer(context: rs.AssetExecutionContext, source: int):
        return {"name": context.asset_name, "value": source * 2}

    repo = rs.CodeRepository(
        assets=[source, consumer],
        jobs=[
            rs.Job(
                name="context_upstream_inputs",
                assets=[source, consumer],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("context_upstream_inputs").execute()
    output = repo.load_node("consumer")
    assert output["name"] == "consumer"
    assert output["value"] == 20


def test_context_not_first_param_raises():
    """Asset with AssetExecutionContext as second param raises ValueError."""

    @rs.Asset
    def source():
        return 1

    @rs.Asset
    def bad_asset(source: int, context: rs.AssetExecutionContext):
        return source

    repo = rs.CodeRepository(
        assets=[source, bad_asset],
        jobs=[
            rs.Job(
                name="context_not_first_param",
                assets=[source, bad_asset],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    with pytest.raises(ExecutionError, match="Context must be the first parameter"):
        repo.get_job("context_not_first_param").execute()


def test_context_partition_with_static_partitions():
    """Verify partition context with a statically partitioned asset."""
    parts_def = rs.PartitionsDefinition.static_(["a", "b", "c"])

    @rs.Asset(partitions_def=parts_def)
    def partitioned(context: rs.AssetExecutionContext):
        return {
            "has_key": context.has_partition_key,
            "key": context.partition_key,
        }

    repo = rs.CodeRepository(
        assets=[partitioned],
        jobs=[
            rs.Job(
                name="context_static_partitions",
                assets=[partitioned],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("context_static_partitions").execute(
        partition_key=rs.PartitionKey.single(["a"])
    )
    output = repo.load_node("partitioned", partition_key=rs.PartitionKey.single(["a"]))
    assert output["has_key"] is True
    assert output["key"] == "a"


def test_partition_key_on_unpartitioned_asset_raises():
    """Passing a partition_key to a job with unpartitioned assets raises ValueError."""

    @rs.Asset
    def no_partitions():
        return 1

    repo = rs.CodeRepository(
        assets=[no_partitions],
        jobs=[
            rs.Job(
                name="partition_key_unpartitioned",
                assets=[no_partitions],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    with pytest.raises(PartitionValidationError, match="not partitioned"):
        repo.get_job("partition_key_unpartitioned").execute(
            partition_key=rs.PartitionKey.single(["x"])
        )


def test_partition_key_not_in_definition_raises():
    """Passing a partition key that's not in the static definition raises ValueError."""
    pd = rs.PartitionsDefinition.static_(["a", "b", "c"])

    @rs.Asset(partitions_def=pd)
    def partitioned():
        return 1

    repo = rs.CodeRepository(
        assets=[partitioned],
        jobs=[
            rs.Job(
                name="partition_key_not_in_def",
                assets=[partitioned],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    with pytest.raises(PartitionValidationError, match="not valid"):
        repo.get_job("partition_key_not_in_def").execute(
            partition_key=rs.PartitionKey.single(["z"])
        )


def test_context_repr():
    """Verify __repr__ output."""
    ctx = rs.AssetExecutionContext("my_asset")
    assert repr(ctx) == "AssetExecutionContext(asset_name='my_asset')"


def test_context_output_metadata_none_when_empty():
    """output_metadata returns None when nothing has been added."""
    ctx = rs.AssetExecutionContext("test")
    assert ctx.output_metadata is None


def test_context_add_output_metadata_duplicate_keys_last_wins():
    """Calling add_output_metadata multiple times with the same key keeps the last value."""
    ctx = rs.AssetExecutionContext("test")
    ctx.add_output_metadata({"key": "first", "other": 1})
    ctx.add_output_metadata({"key": "second"})

    meta = ctx.output_metadata
    assert meta["key"].raw_value() == "second"
    assert meta["other"].raw_value() == 1
    assert len(meta) == 2


def test_context_add_output_metadata_override_flows_to_io_handler():
    """Duplicate key override via add_output_metadata is visible to IO handler."""
    captured = {}

    class CapturingHandler(rs.BaseIOHandler):
        def handle_output(self, context: rs.OutputContext, obj):
            captured["metadata"] = context.output_metadata

        def load_input(self, context: rs.InputContext):
            pass

    @rs.Asset(io_handler=CapturingHandler())
    def override_meta(context: rs.AssetExecutionContext):
        context.add_output_metadata({"version": "v1", "rows": 10})
        context.add_output_metadata({"version": "v2"})
        return "data"

    repo = rs.CodeRepository(
        assets=[override_meta],
        jobs=[
            rs.Job(
                name="override_meta_job",
                assets=[override_meta],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("override_meta_job").execute()
    meta = captured["metadata"]
    assert meta["version"].raw_value() == "v2"
    assert meta["rows"].raw_value() == 10


def test_context_register_data_version_stored(storage):
    """register_data_version propagates to storage as the materialization's data_version."""

    class StubHandler(rs.BaseIOHandler):
        def handle_output(self, context: rs.OutputContext, obj):
            pass

        def load_input(self, context: rs.InputContext):
            pass

    @rs.Asset(io_handler=StubHandler())
    def versioned(context: rs.AssetExecutionContext) -> int:
        context.register_data_version("hash-abc")
        return 1

    repo = rs.CodeRepository(assets=[versioned])
    repo.resolve(storage=storage)
    repo.materialize()

    events = storage.get_events_for_asset("versioned")
    mat_events = [e for e in events if e.event_type == "Materialization"]
    assert len(mat_events) == 1
    assert mat_events[0].data_version == "hash-abc"


def test_context_partition_time_window_returns_tuple():
    """partition_time_window returns (start, end) datetimes for a time-windowed partition."""
    pd = rs.PartitionsDefinition.daily(start=datetime(2025, 1, 1))

    @rs.Asset(partitions_def=pd)
    def daily_asset(context: rs.AssetExecutionContext):
        return {
            "window": context.partition_time_window,
        }

    repo = rs.CodeRepository(
        assets=[daily_asset],
        jobs=[
            rs.Job(
                name="tw_job",
                assets=[daily_asset],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("tw_job").execute(partition_key=rs.PartitionKey.single(["2025-01-01"]))
    output = repo.load_node(
        "daily_asset", partition_key=rs.PartitionKey.single(["2025-01-01"])
    )
    window = output["window"]
    assert window is not None
    start, end = window
    assert start == datetime(2025, 1, 1)
    assert end == datetime(2025, 1, 2)


def test_output_context_add_output_metadata_duplicate_keys_last_wins():
    """OutputContext.add_output_metadata with duplicate keys keeps the last value."""
    ctx = rs.OutputContext("test_asset")
    ctx.add_output_metadata({"key": "first", "other": 1})
    ctx.add_output_metadata({"key": "second"})

    meta = ctx.output_metadata
    assert meta["key"].raw_value() == "second"
    assert meta["other"].raw_value() == 1
    assert len(meta) == 2


def test_output_context_metadata_override_in_io_handler():
    """IO handler calling add_output_metadata on OutputContext deduplicates keys."""
    captured = {}

    class OverridingHandler(rs.BaseIOHandler):
        def handle_output(self, context: rs.OutputContext, obj):
            context.add_output_metadata({"source": "first", "rows": 10})
            context.add_output_metadata({"source": "second"})
            captured["metadata"] = context.output_metadata

        def load_input(self, context: rs.InputContext):
            pass

    @rs.Asset(io_handler=OverridingHandler())
    def io_meta() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[io_meta],
        jobs=[
            rs.Job(
                name="io_meta_job",
                assets=[io_meta],
                executor=rs.Executor.in_process(),
            )
        ],
    )
    repo.get_job("io_meta_job").execute()
    meta = captured["metadata"]
    assert meta["source"].raw_value() == "second"
    assert meta["rows"].raw_value() == 10
    assert len(meta) == 2
