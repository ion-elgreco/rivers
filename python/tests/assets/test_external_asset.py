import rivers as rs

from _helpers import DictIOHandler


def test_external_asset_properties():
    """External asset exposes name, tags, kinds, group, metadata, is_external."""
    handler = DictIOHandler()

    ext = rs.Asset.external(
        name="source",
        io_handler=handler,
        tags=["raw"],
        kinds="table",
        group="external",
        metadata={"path": "/data/source"},
    )

    assert ext.name == "source"
    assert ext.tags == ["raw"]
    assert ext.kinds == ["table"]
    assert ext.group == "external"
    assert ext.metadata == {"path": "/data/source"}
    assert ext.is_external is True
    assert ext.is_single is False
    assert ext.is_multi is False
    assert ext.is_graph is False


def test_external_asset_in_graph():
    """External asset as dependency, downstream loads its data via io_handler."""
    handler = DictIOHandler(store={"source": [1, 2, 3]})

    source = rs.Asset.external(name="source", io_handler=handler)

    @rs.Asset
    def processed(source: list) -> list:
        return [x * 2 for x in source]

    repo = rs.CodeRepository(assets=[source, processed])
    repo.materialize()

    assert repo.load_node("processed") == [2, 4, 6]


def test_external_asset_not_materialized():
    """External asset is excluded from execution plan — materialize only runs downstream."""
    handler = DictIOHandler(store={"ext": 42})

    ext = rs.Asset.external(name="ext", io_handler=handler)

    @rs.Asset
    def downstream(ext: int) -> int:
        return ext + 1

    repo = rs.CodeRepository(assets=[ext, downstream])
    repo.materialize()

    assert repo.load_node("downstream") == 43


def test_external_asset_in_job():
    """Job with downstream depending on external — external auto-included for loading."""
    handler = DictIOHandler(store={"ext_source": "hello"})

    ext_source = rs.Asset.external(name="ext_source", io_handler=handler)

    @rs.Asset
    def consumer(ext_source: str) -> str:
        return ext_source.upper()

    job = rs.Job(name="my_job", assets=[consumer])
    repo = rs.CodeRepository(assets=[ext_source, consumer], jobs=[job])

    repo.get_job("my_job").execute()
    assert repo.load_node("consumer") == "HELLO"


def test_external_asset_observe():
    """Observation function called, metadata collected via repo.observe()."""
    handler = DictIOHandler()

    @rs.Asset.external(io_handler=handler)
    def observed(context: rs.AssetExecutionContext):
        context.add_output_metadata({"row_count": rs.MetadataValue.int(42)})

    repo = rs.CodeRepository(assets=[observed])
    result = repo.observe()

    assert "observed" in result
    assert result["observed"]["row_count"].raw_value() == 42


def test_external_asset_observe_decorator():
    """@Asset.external(io_handler=h) decorator sets observe_fn from decorated function."""
    handler = DictIOHandler()

    @rs.Asset.external(io_handler=handler, name="my_source")
    def my_source(context: rs.AssetExecutionContext):
        context.add_output_metadata({"status": rs.MetadataValue.text("fresh")})

    assert my_source.name == "my_source"
    assert my_source.is_external is True

    repo = rs.CodeRepository(assets=[my_source])
    result = repo.observe()
    assert result["my_source"]["status"].raw_value() == "fresh"


def test_external_asset_partitioned():
    """Partitioned external asset works as dependency with partition key."""
    partitions = rs.PartitionsDefinition.static_(["us", "eu"])
    captured = []

    class PartitionedHandler(rs.BaseIOHandler):
        def handle_output(self, context: rs.OutputContext, obj):
            captured.append(("output", context.asset_name, context.partition))

        def load_input(self, context: rs.InputContext):
            captured.append(("input", context.asset_name, context.partition))
            return {"us": 100, "eu": 200}

    handler = PartitionedHandler()

    ext = rs.Asset.external(
        name="ext_source",
        io_handler=handler,
        partitions_def=partitions,
    )

    @rs.Asset(io_handler=handler, partitions_def=partitions)
    def downstream(ext_source: dict) -> int:
        return ext_source["us"] + ext_source["eu"]

    job = rs.Job(
        name="partitioned_test",
        assets=[downstream],
        executor=rs.Executor.in_process(),
    )
    repo = rs.CodeRepository(assets=[ext, downstream], jobs=[job])
    repo.get_job("partitioned_test").execute(partition_key=rs.PartitionKey.single("us"))

    # downstream's handle_output should have been called with its computed result
    output_calls = [c for c in captured if c[0] == "output"]
    assert any(c[1] == "downstream" for c in output_calls)
    # load_input should have been called for ext_source
    input_calls = [c for c in captured if c[0] == "input"]
    assert len(input_calls) == 1
    assert input_calls[0][1] == "ext_source"
    assert input_calls[0][2] is not None
    assert input_calls[0][2].key == rs.PartitionKey.single("us")


def test_external_asset_observe_filtered():
    """repo.observe(asset_names=...) only observes specified assets."""
    handler = DictIOHandler()

    @rs.Asset.external(io_handler=handler)
    def ext_a(context: rs.AssetExecutionContext):
        context.add_output_metadata({"val": rs.MetadataValue.int(1)})

    @rs.Asset.external(io_handler=handler)
    def ext_b(context: rs.AssetExecutionContext):
        context.add_output_metadata({"val": rs.MetadataValue.int(2)})

    repo = rs.CodeRepository(assets=[ext_a, ext_b])
    result = repo.observe(asset_names=["ext_a"])

    assert "ext_a" in result
    assert "ext_b" not in result


def test_external_asset_requires_observe_fn_with_condition():
    """External asset with automation_condition but no observe_fn should fail at resolve."""
    import pytest

    handler = DictIOHandler()
    cond = rs.AutomationCondition.any_deps_updated()

    # Construction succeeds (wraps may be set later via decorator)
    ext = rs.Asset.external(
        name="source",
        io_handler=handler,
        automation_condition=cond,
    )

    # But resolve() fails because no observe function was provided
    repo = rs.CodeRepository(assets=[ext])
    with pytest.raises(rs.exceptions.AssetDefinitionError, match="no observe function"):
        repo.resolve()


def test_external_asset_observe_with_automation_condition():
    """External asset with observe_fn and automation condition works end-to-end."""
    handler = DictIOHandler()
    cond = rs.AutomationCondition.on_missing()

    @rs.Asset.external(io_handler=handler, automation_condition=cond)
    def monitored(context: rs.AssetExecutionContext):
        context.add_output_metadata({"status": rs.MetadataValue.text("ok")})

    repo = rs.CodeRepository(assets=[monitored])
    result = repo.observe()

    assert "monitored" in result
    assert result["monitored"]["status"].raw_value() == "ok"


def test_external_asset_with_condition_and_observe_fn_resolves():
    """External asset with both automation_condition and observe_fn resolves."""
    handler = DictIOHandler()
    cond = rs.AutomationCondition.on_cron("0 * * * *")

    @rs.Asset.external(io_handler=handler, automation_condition=cond)
    def my_feed(context: rs.AssetExecutionContext):
        return rs.Observation(data_version="v1")

    repo = rs.CodeRepository(assets=[my_feed])
    repo.resolve()
    assert my_feed.is_external
    assert my_feed.automation_condition is not None


def test_external_observation_updates_asset_record(storage):
    """Observing an external asset updates last_timestamp on the asset record."""
    handler = DictIOHandler()

    @rs.Asset.external(io_handler=handler)
    def tracked_feed(context: rs.AssetExecutionContext):
        return rs.Observation(data_version="obs_v1")

    repo = rs.CodeRepository(assets=[tracked_feed])
    repo.resolve(storage=storage)

    # Before observation
    record = storage.get_asset_record("tracked_feed")
    assert record is not None
    assert record.last_timestamp is None
    assert record.last_data_version is None

    # Observe
    repo.observe(asset_names=["tracked_feed"])

    # After observation
    record = storage.get_asset_record("tracked_feed")
    assert record.last_timestamp is not None
    assert record.last_data_version == "obs_v1"


def test_external_observation_visible_to_downstream(storage):
    """After observing an external asset, downstream assets can read its data."""
    handler = DictIOHandler()

    @rs.Asset.external(io_handler=handler)
    def upstream_feed(context: rs.AssetExecutionContext):
        handler.store["upstream_feed"] = {"value": 42}
        return rs.Observation(data_version="v1")

    @rs.Asset(io_handler=handler)
    def downstream(upstream_feed: dict) -> dict:
        return {"received": upstream_feed["value"]}

    repo = rs.CodeRepository(assets=[upstream_feed, downstream])
    repo.resolve(storage=storage)

    # Observe external asset first
    repo.observe(asset_names=["upstream_feed"])

    # Now materialize downstream — it should read from the io_handler
    result = repo.materialize(selection=["downstream"])
    assert result.success
    assert repo.load_node("downstream") == {"received": 42}
