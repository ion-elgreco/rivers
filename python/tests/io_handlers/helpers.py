from datetime import datetime

import rivers as rs


def make_partition(key_val: str | list[str]) -> rs.PartitionContext:
    """Create a PartitionContext with a static definition."""
    keys = [key_val] if isinstance(key_val, str) else key_val
    pd = rs.PartitionsDefinition.static_(keys)
    return rs.PartitionContext(keys=[rs.PartitionKey.single(key_val)], definition=pd)


def make_multi_partition(dims: dict[str, str | list[str]]) -> rs.PartitionContext:
    """Create a multi-dimension PartitionContext."""
    defs: dict[str, rs.PartitionsDefinition] = {}
    for k, v in dims.items():
        vals = [v] if isinstance(v, str) else v
        defs[k] = rs.PartitionsDefinition.static_(vals)
    pd = rs.PartitionsDefinition.multi(defs)
    return rs.PartitionContext(keys=[rs.PartitionKey.multi(dims)], definition=pd)


def make_daily_partition(key_val: str) -> rs.PartitionContext:
    """Create a PartitionContext with a daily time window definition."""
    start = datetime.fromisoformat(key_val)
    pd = rs.PartitionsDefinition.daily(start=start)
    return rs.PartitionContext(keys=[rs.PartitionKey.single(key_val)], definition=pd)


def make_multi_partition_with_daily(
    dims: dict[str, str], daily_dims: set[str]
) -> rs.PartitionContext:
    """Create a multi-dimension PartitionContext where some dims are daily time windows."""
    defs: dict[str, rs.PartitionsDefinition] = {}
    for k, v in dims.items():
        if k in daily_dims:
            defs[k] = rs.PartitionsDefinition.daily(start=datetime.fromisoformat(v))
        else:
            defs[k] = rs.PartitionsDefinition.static_([v])
    pd = rs.PartitionsDefinition.multi(defs)
    return rs.PartitionContext(keys=[rs.PartitionKey.multi(dims)], definition=pd)
