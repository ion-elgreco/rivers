import datetime

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
