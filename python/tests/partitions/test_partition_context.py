import datetime
import pickle

import pytest

import rivers as rs


def test_on_output_context():
    """PartitionContext accessible on OutputContext."""
    pd = rs.PartitionsDefinition.static_(["a", "b"])
    ctx = rs.PartitionContext(
        keys=[rs.PartitionKey.single("a")],
        definition=pd,
    )
    out = rs.OutputContext(asset_name="test", partition=ctx)
    assert out.partition is not None
    assert out.partition.key == rs.PartitionKey.single("a")
    assert isinstance(out.partition.definition, rs.PartitionsDefinition.Static)


def test_on_input_context():
    """PartitionContext accessible on InputContext."""
    pd = rs.PartitionsDefinition.static_(["x", "y"])
    ctx = rs.PartitionContext(
        keys=[rs.PartitionKey.single("x")],
        definition=pd,
    )
    inp = rs.InputContext(
        asset_name="upstream", downstream_asset="downstream", partition=ctx
    )
    assert inp.partition is not None
    assert isinstance(inp.partition.key, rs.PartitionKey.Single)
    assert inp.partition.key.key == ["x"]


def test_none_without_partitions():
    """OutputContext/InputContext have None partition without partitions."""
    out = rs.OutputContext(asset_name="test")
    assert out.partition is None
    inp = rs.InputContext(asset_name="up", downstream_asset="down")
    assert inp.partition is None


def test_time_window():
    """PartitionContext.time_window() returns window bounds for time partitions."""
    start = datetime.datetime(2024, 1, 1)
    end = datetime.datetime(2024, 1, 5)
    pd = rs.PartitionsDefinition.daily(start, end=end)
    keys = pd.get_partition_keys()
    ctx = rs.PartitionContext(keys=[keys[0]], definition=pd)
    window = ctx.time_window()
    assert window is not None
    assert window[0] == datetime.datetime(2024, 1, 1)
    assert window[1] == datetime.datetime(2024, 1, 2)


def test_time_window_none_for_static():
    """PartitionContext.time_window() returns None for static partitions."""
    pd = rs.PartitionsDefinition.static_(["a"])
    ctx = rs.PartitionContext(keys=[rs.PartitionKey.single("a")], definition=pd)
    assert ctx.time_window() is None


def test_keys_and_key():
    """keys is the full list, key is a convenience getter for keys[0]."""
    pd = rs.PartitionsDefinition.static_(["a", "b"])
    ctx = rs.PartitionContext(
        keys=[rs.PartitionKey.single("a"), rs.PartitionKey.single("b")],
        definition=pd,
    )
    assert len(ctx.keys) == 2
    assert ctx.key == rs.PartitionKey.single("a")


def test_empty_keys_rejected():
    """PartitionContext(keys=[]) raises ValueError instead of arming an
    index-out-of-bounds panic on `.key`."""
    pd = rs.PartitionsDefinition.static_(["a"])
    with pytest.raises(ValueError, match="must not be empty"):
        rs.PartitionContext(keys=[], definition=pd)


def test_reads_share_one_partition():
    """Every read returns the same frozen objects; `keys` is a new list each read."""
    pd = rs.PartitionsDefinition.static_(["a", "b"])
    keys = [rs.PartitionKey.single("a"), rs.PartitionKey.single("b")]
    ctx = rs.AssetExecutionContext(
        "asset", partition=rs.PartitionContext(keys=keys, definition=pd)
    )

    assert ctx.partition is ctx.partition
    assert ctx.partition.keys[0] is ctx.partition.keys[0]
    assert ctx.partition.key is ctx.partition.keys[0]
    assert ctx.partition.definition is ctx.partition.definition

    ctx.partition.keys.clear()
    assert ctx.partition.keys == keys
    with pytest.raises(AttributeError):
        ctx.partition.keys = []


def test_pickle_round_trip():
    """A pickled PartitionContext comes back with equal keys and definition."""
    pd = rs.PartitionsDefinition.static_(["a", "b"])
    keys = [rs.PartitionKey.single("a"), rs.PartitionKey.single("b")]
    restored = pickle.loads(pickle.dumps(rs.PartitionContext(keys=keys, definition=pd)))

    assert restored.keys == keys
    assert all(isinstance(k, rs.PartitionKey.Single) for k in restored.keys)
    assert isinstance(restored.definition, rs.PartitionsDefinition.Static)
    assert restored.definition == pd


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_executor_built_partition(executor_env, is_async):
    """A step reads one shared PartitionContext whose keys and definition are
    the variant classes. Two steps in one level, so the parallel executor
    pickles the partition to its workers."""
    executor, make_handler = executor_env
    handler = make_handler()
    pd = rs.PartitionsDefinition.static_(["a", "b", "c"])

    def members(context: rs.AssetExecutionContext) -> list[str]:
        partition = context.partition
        assert partition is context.partition
        assert partition.keys[0] is partition.keys[0]
        assert all(isinstance(k, rs.PartitionKey.Single) for k in partition.keys)
        assert isinstance(partition.definition, rs.PartitionsDefinition.Static)
        return [k.key[0] for k in partition.keys]

    if is_async:

        @rs.Asset(io_handler=handler, partitions_def=pd)
        async def left(context: rs.AssetExecutionContext) -> list[str]:
            return members(context)

        @rs.Asset(io_handler=handler, partitions_def=pd)
        async def right(context: rs.AssetExecutionContext) -> list[str]:
            return members(context)

    else:

        @rs.Asset(io_handler=handler, partitions_def=pd)
        def left(context: rs.AssetExecutionContext) -> list[str]:
            return members(context)

        @rs.Asset(io_handler=handler, partitions_def=pd)
        def right(context: rs.AssetExecutionContext) -> list[str]:
            return members(context)

    repo = rs.CodeRepository(assets=[left, right], default_executor=executor)
    pk = rs.PartitionKey.single(["a", "b"])
    assert repo.materialize(partition_key=pk).success
    assert repo.load_node("left", partition_key=pk) == ["a", "b"]
    assert repo.load_node("right", partition_key=pk) == ["a", "b"]
