import obstore
import obstore.store

import rivers as rs

from .helpers import make_multi_partition, make_partition


def test_writes_file_to_local_store(tmp_path):
    """PickleIOHandler writes a .pkl file at the asset's path on disk."""
    store = obstore.store.LocalStore(str(tmp_path), mkdir=True)
    handler = rs.PickleIOHandler(store=store)

    ctx_out = rs.OutputContext(asset_name="test_asset", asset_metadata=None)
    handler.handle_output(ctx_out, [1, 2, 3])

    assert (tmp_path / "test_asset.pkl").exists()


def test_with_memory_store():
    """PickleIOHandler works with MemoryStore."""
    store = obstore.store.MemoryStore()
    handler = rs.PickleIOHandler(store=store)

    ctx_out = rs.OutputContext(asset_name="mem_asset", asset_metadata=None)
    handler.handle_output(ctx_out, {"key": "value"})

    ctx_in = rs.InputContext(
        asset_name="mem_asset", downstream_asset="consumer", asset_metadata=None
    )
    loaded = handler.load_input(ctx_in)
    assert loaded == {"key": "value"}


def test_with_prefix():
    """PickleIOHandler uses prefix for object keys."""
    store = obstore.store.MemoryStore()
    handler = rs.PickleIOHandler(store=store, prefix="my/prefix")

    ctx_out = rs.OutputContext(asset_name="prefixed", asset_metadata=None)
    handler.handle_output(ctx_out, 42)

    # Verify stored at prefixed path
    result = obstore.get(store, "my/prefix/prefixed.pkl")
    assert result is not None

    ctx_in = rs.InputContext(
        asset_name="prefixed", downstream_asset="consumer", asset_metadata=None
    )
    assert handler.load_input(ctx_in) == 42


def test_populates_output_metadata():
    """PickleIOHandler populates output metadata on the context."""
    store = obstore.store.MemoryStore()
    handler = rs.PickleIOHandler(store=store)

    ctx = rs.OutputContext(asset_name="asset_a", asset_metadata=None)
    handler.handle_output(ctx, [1, 2, 3])

    meta = ctx.output_metadata
    assert meta is not None
    assert meta["path"].raw_value() == "asset_a.pkl"
    assert meta["serializer"].raw_value() == "pickle"
    assert isinstance(meta["size_bytes"], rs.MetadataValue.Int)
    assert isinstance(meta["write_duration_s"], rs.MetadataValue.Float)


def test_partition_path_format():
    """Single-dimension partitions write to ``{asset}/{key}/data.pkl``."""
    store = obstore.store.MemoryStore()
    handler = rs.PickleIOHandler(store=store)

    ctx = rs.OutputContext(asset_name="daily", partition=make_partition("2024-01-01"))
    handler.handle_output(ctx, "day1")

    assert ctx.output_metadata is not None
    assert ctx.output_metadata["path"].raw_value() == "daily/2024-01-01/data.pkl"


def test_multi_partition_path_format():
    """Multi-dimension partitions encode dims as sorted ``key=value`` segments."""
    store = obstore.store.MemoryStore()
    handler = rs.PickleIOHandler(store=store, prefix="data")

    p = make_multi_partition({"region": "us", "env": "prod"})
    ctx = rs.OutputContext(asset_name="sales", partition=p)
    handler.handle_output(ctx, [1, 2, 3])

    assert ctx.output_metadata is not None
    assert (
        ctx.output_metadata["path"].raw_value()
        == "data/sales/env=prod/region=us/data.pkl"
    )


def test_no_partition_unchanged():
    """PickleIOHandler path unchanged without partition."""
    store = obstore.store.MemoryStore()
    handler = rs.PickleIOHandler(store=store)

    ctx_out = rs.OutputContext(asset_name="plain")
    handler.handle_output(ctx_out, 99)

    assert ctx_out.output_metadata is not None
    assert ctx_out.output_metadata["path"].raw_value() == "plain.pkl"
