import rivers as rs


def test_populates_output_metadata():
    """InMemoryIOHandler populates output metadata on the context."""
    handler = rs.InMemoryIOHandler()

    ctx = rs.OutputContext(asset_name="asset_b", asset_metadata=None)
    handler.handle_output(ctx, {"key": "value"})

    meta = ctx.output_metadata
    assert meta is not None
    assert meta["storage"].raw_value() == "memory"
    assert isinstance(meta["size_bytes"], rs.MetadataValue.Int)


def test_no_partition_unchanged():
    """InMemoryIOHandler still works without partition."""
    handler = rs.InMemoryIOHandler()

    ctx_out = rs.OutputContext(asset_name="plain")
    handler.handle_output(ctx_out, 42)

    ctx_in = rs.InputContext(asset_name="plain", downstream_asset="x")
    assert handler.load_input(ctx_in) == 42
    assert "plain" in handler._storage
