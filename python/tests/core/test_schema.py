import pyarrow as pa
import pytest

import rivers as rs
from rivers._core import Schema


def _sample_schema():
    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.utf8()),
            pa.field("price", pa.float64()),
        ]
    )


def test_schema_from_pyarrow():
    pa_schema = _sample_schema()
    schema = rs.Schema(pa_schema)
    assert schema.names == ["id", "name", "price"]
    assert len(schema) == 3


def test_schema_ipc_roundtrip():
    pa_schema = _sample_schema()
    schema = rs.Schema(pa_schema)
    ipc_bytes = schema.to_ipc()
    assert isinstance(ipc_bytes, bytes)
    assert len(ipc_bytes) > 0

    restored = rs.Schema.from_ipc(ipc_bytes)
    assert restored == schema
    assert restored.names == schema.names


def test_schema_repr():
    schema = rs.Schema(pa.schema([pa.field("x", pa.int32())]))
    assert repr(schema) == "Schema({x: Int32})"


def test_schema_equality():
    s1 = rs.Schema(pa.schema([pa.field("a", pa.utf8())]))
    s2 = rs.Schema(pa.schema([pa.field("a", pa.utf8())]))
    s3 = rs.Schema(pa.schema([pa.field("b", pa.int64())]))
    assert s1 == s2
    assert s1 != s3


def test_schema_hashable():
    s1 = rs.Schema(pa.schema([pa.field("a", pa.utf8())]))
    s2 = rs.Schema(pa.schema([pa.field("a", pa.utf8())]))
    s3 = rs.Schema(pa.schema([pa.field("b", pa.int64())]))
    assert hash(s1) == hash(s2)
    assert {s1, s2, s3} == {s1, s3}


def test_schema_from_ipc_invalid():
    with pytest.raises(ValueError):
        rs.Schema.from_ipc(b"not arrow ipc bytes")


def test_schema_arrow_c_schema_export():
    """Schema can be exported back via __arrow_c_schema__ and consumed by PyArrow."""
    pa_schema = _sample_schema()
    schema = rs.Schema(pa_schema)

    # Roundtrip: rs.Schema -> rs.Schema via PyCapsule
    roundtripped = rs.Schema(schema)
    assert roundtripped.names == pa_schema.names
    assert roundtripped == schema


def test_metadata_value_schema():
    pa_schema = _sample_schema()
    mv = rs.MetadataValue.schema(pa_schema)
    assert isinstance(mv, rs.MetadataValue.Schema)
    assert repr(mv) == "MetadataValue.schema({id: Int64, name: Utf8, price: Float64})"


def test_metadata_value_schema_raw_value():
    pa_schema = _sample_schema()
    mv = rs.MetadataValue.schema(pa_schema)
    raw = mv.raw_value()
    assert isinstance(raw, rs.Schema)
    assert raw.names == ["id", "name", "price"]


def test_metadata_value_schema_equality():
    pa_schema = _sample_schema()
    mv1 = rs.MetadataValue.schema(pa_schema)
    mv2 = rs.MetadataValue.schema(pa_schema)
    assert mv1 == mv2


def test_schema_auto_coerce():
    """Arrow schema objects are auto-coerced to MetadataValue.Schema in add_output_metadata."""
    pa_schema = _sample_schema()
    ctx = rs.OutputContext(asset_name="test")
    ctx.add_output_metadata({"rivers/schema": pa_schema})
    meta = ctx.output_metadata
    assert meta is not None
    assert isinstance(meta["rivers/schema"], rs.MetadataValue.Schema)


def test_schema_invalid_input():
    with pytest.raises((ValueError, TypeError, AttributeError)):
        rs.Schema("not a schema")  # type: ignore


def test_schema_nested_types():
    """Schema with nested types (list, struct) roundtrips correctly."""
    pa_schema = pa.schema(
        [
            pa.field("tags", pa.list_(pa.utf8())),
            pa.field(
                "address",
                pa.struct([pa.field("city", pa.utf8()), pa.field("zip", pa.int32())]),
            ),
        ]
    )
    schema = rs.Schema(pa_schema)
    assert schema.names == ["tags", "address"]
    assert len(schema) == 2

    # IPC roundtrip
    restored = rs.Schema.from_ipc(schema.to_ipc())
    assert restored == schema


# --- Schema in context tests ---


def test_output_context_schema_via_metadata_value():
    """Schema attached as MetadataValue.schema() is retrievable from output context."""
    pa_schema = _sample_schema()
    ctx = rs.OutputContext(asset_name="my_asset")
    ctx.add_output_metadata({"rivers/schema": rs.MetadataValue.schema(pa_schema)})

    meta = ctx.output_metadata
    assert meta is not None
    assert "rivers/schema" in meta
    schema_mv = meta["rivers/schema"]
    assert isinstance(schema_mv, rs.MetadataValue.Schema)

    # raw_value() returns an rs.Schema with correct fields
    raw = schema_mv.raw_value()
    assert isinstance(raw, rs.Schema)
    assert raw.names == ["id", "name", "price"]
    assert len(raw) == 3


def test_output_context_schema_auto_coerce_roundtrip():
    """PyArrow schema auto-coerced in context preserves field names and types through raw_value."""
    pa_schema = pa.schema(
        [
            pa.field("ts", pa.timestamp("us")),
            pa.field("value", pa.float32()),
        ]
    )
    ctx = rs.OutputContext(asset_name="sensor_data")
    ctx.add_output_metadata({"rivers/schema": pa_schema})
    assert ctx.output_metadata is not None

    raw = ctx.output_metadata["rivers/schema"].raw_value()
    assert isinstance(raw, Schema)
    assert raw.names == ["ts", "value"]
    assert len(raw) == 2


def test_output_context_schema_with_other_metadata():
    """Schema coexists with other metadata entries on the same context."""
    pa_schema = _sample_schema()
    ctx = rs.OutputContext(asset_name="products")
    ctx.add_output_metadata(
        {
            "rivers/schema": rs.MetadataValue.schema(pa_schema),
            "num_rows": 42,
            "table_uri": "s3://bucket/products",
        }
    )

    meta = ctx.output_metadata
    assert meta is not None
    assert isinstance(meta["rivers/schema"], rs.MetadataValue.Schema)
    assert meta["num_rows"] == rs.MetadataValue.int(42)
    assert meta["table_uri"] == rs.MetadataValue.text("s3://bucket/products")


def test_output_context_schema_export_back_to_pyarrow():
    """Schema stored in context can be exported back to PyArrow via PyCapsule."""
    pa_schema = _sample_schema()
    ctx = rs.OutputContext(asset_name="test")
    ctx.add_output_metadata({"rivers/schema": pa_schema})
    assert ctx.output_metadata is not None

    raw = ctx.output_metadata["rivers/schema"].raw_value()
    # rs.Schema implements __arrow_c_schema__, so PyArrow can consume it
    restored_pa = pa.schema(raw)
    assert restored_pa.names == pa_schema.names
    for orig, restored in zip(pa_schema, restored_pa):
        assert orig.type == restored.type


def test_output_context_multiple_add_metadata_calls():
    """Schema can be added in a separate add_output_metadata call from other metadata."""
    pa_schema = _sample_schema()
    ctx = rs.OutputContext(asset_name="test")
    ctx.add_output_metadata({"num_rows": 100})
    ctx.add_output_metadata({"rivers/schema": rs.MetadataValue.schema(pa_schema)})

    meta = ctx.output_metadata
    assert meta is not None
    assert meta["num_rows"] == rs.MetadataValue.int(100)
    assert isinstance(meta["rivers/schema"], rs.MetadataValue.Schema)
    raw_value = meta["rivers/schema"].raw_value()
    assert isinstance(raw_value, Schema)
    assert raw_value.names == ["id", "name", "price"]
