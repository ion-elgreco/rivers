from datetime import datetime
from typing import Any

import pyarrow as pa

import rivers as rs


def iceberg_handler(tmp_path, **kwargs: Any):
    """An ``IcebergIOHandler`` on a SQLite catalog with its warehouse in ``tmp_path``.

    The catalog tables are created here, before steps in other processes
    connect to the database.
    """
    from rivers.io_handlers.iceberg import IcebergIOHandler

    handler = IcebergIOHandler(
        **{
            "namespace": "ns",
            "catalog_name": "rivers_test",
            "catalog_properties": {
                "type": "sql",
                "uri": f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}",
                "warehouse": str(tmp_path / "warehouse"),
            },
            **kwargs,
        }
    )
    handler.pyiceberg_catalog().close()
    return handler


def iceberg_table(handler, name: str):
    """The whole Iceberg table of asset ``name``, as a ``pa.Table``."""
    return handler.load_table(name).scan().to_arrow()


def write_output(handler, obj, name: str = "t", **context: Any) -> rs.OutputContext:
    """Write ``obj`` as asset ``name`` with ``handler``, and return the context."""
    ctx = rs.OutputContext(asset_name=name, **context)
    handler.handle_output(ctx, obj)
    return ctx


def read_input(
    handler, type_hint: Any = pa.Table, name: str = "t", **context: Any
) -> Any:
    """Read asset ``name`` with ``handler`` as ``type_hint``."""
    return handler.load_input(
        rs.InputContext(
            asset_name=name, downstream_asset="d", type_hint=type_hint, **context
        )
    )


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
