import datetime

import pytest

import rivers as rs

# ---------------------------------------------------------------------------
# Primitive variants
# ---------------------------------------------------------------------------


def test_output_context_add_output_metadata():
    """add_output_metadata collects key-value pairs from a dict."""
    ctx = rs.OutputContext(asset_name="test", asset_metadata=None)
    assert ctx.output_metadata is None

    ctx.add_output_metadata({"path": "test.pkl", "size_bytes": 42})

    meta = ctx.output_metadata
    assert meta is not None
    assert meta["path"] == rs.MetadataValue.text("test.pkl")
    assert meta["size_bytes"] == rs.MetadataValue.int(42)
    assert meta["path"].raw_value() == "test.pkl"
    assert meta["size_bytes"].raw_value() == 42


def test_output_context_add_typed_metadata():
    """add_output_metadata accepts MetadataValue directly in a dict."""
    ctx = rs.OutputContext(asset_name="test", asset_metadata=None)

    ctx.add_output_metadata(
        {
            "path": rs.MetadataValue.path("/tmp/test.pkl"),
            "size": rs.MetadataValue.bytes(1024),
            "dur": rs.MetadataValue.duration(0.5),
            "flag": True,
        }
    )

    meta = ctx.output_metadata
    assert meta is not None
    assert isinstance(meta["path"], rs.MetadataValue.Path)
    assert isinstance(meta["size"], rs.MetadataValue.Bytes)
    assert isinstance(meta["dur"], rs.MetadataValue.Duration)
    assert meta["flag"] == rs.MetadataValue.bool_(True)


# ---------------------------------------------------------------------------
# New variant types
# ---------------------------------------------------------------------------


def test_metadata_value_sql():
    """SQL variant with and without dialect."""
    v = rs.MetadataValue.sql("SELECT * FROM users")
    assert isinstance(v, rs.MetadataValue.Sql)
    assert v.raw_value() == "SELECT * FROM users"
    assert repr(v) == 'MetadataValue.sql("SELECT * FROM users")'

    v2 = rs.MetadataValue.sql("SELECT 1", dialect="postgres")
    assert isinstance(v2, rs.MetadataValue.Sql)
    assert v2.raw_value() == "SELECT 1"
    assert repr(v2) == 'MetadataValue.sql("SELECT 1", dialect="postgres")'

    assert v != v2
    assert v == rs.MetadataValue.sql("SELECT * FROM users")


def test_metadata_value_code_block():
    """CodeBlock variant with and without language."""
    v = rs.MetadataValue.code_block("print('hi')")
    assert isinstance(v, rs.MetadataValue.CodeBlock)
    assert v.raw_value() == "print('hi')"

    v2 = rs.MetadataValue.code_block("print('hi')", language="python")
    assert isinstance(v2, rs.MetadataValue.CodeBlock)
    assert repr(v2) == """MetadataValue.code_block("print('hi')", language="python")"""

    assert v != v2
    assert v2 == rs.MetadataValue.code_block("print('hi')", language="python")


def test_metadata_value_image():
    """Image variant."""
    v = rs.MetadataValue.image("data:image/png;base64,abc123")
    assert isinstance(v, rs.MetadataValue.Image)
    assert v.raw_value() == "data:image/png;base64,abc123"
    assert v == rs.MetadataValue.image("data:image/png;base64,abc123")


def test_metadata_value_percentage():
    """Percentage variant."""
    v = rs.MetadataValue.percentage(0.85)
    assert isinstance(v, rs.MetadataValue.Percentage)
    assert v.raw_value() == 0.85
    assert v == rs.MetadataValue.percentage(0.85)


def test_metadata_value_list():
    """List variant with mixed types."""
    items = [
        rs.MetadataValue.text("hello"),
        rs.MetadataValue.int(42),
        rs.MetadataValue.bool_(True),
    ]
    v = rs.MetadataValue.list_(items)
    assert isinstance(v, rs.MetadataValue.List)

    raw = v.raw_value()
    assert isinstance(raw, list)
    assert len(raw) == 3

    assert v == rs.MetadataValue.list_(
        [
            rs.MetadataValue.text("hello"),
            rs.MetadataValue.int(42),
            rs.MetadataValue.bool_(True),
        ]
    )


def test_metadata_value_date_range():
    """DateRange variant with Python datetime objects."""
    start = datetime.datetime(2024, 1, 1, 0, 0, 0)
    end = datetime.datetime(2024, 1, 31, 23, 59, 59)

    v = rs.MetadataValue.date_range(start, end)
    assert isinstance(v, rs.MetadataValue.DateRange)

    raw = v.raw_value()
    assert isinstance(raw, tuple)
    assert len(raw) == 2
    assert raw[0] == start
    assert raw[1] == end

    assert v == rs.MetadataValue.date_range(start, end)
    assert v != rs.MetadataValue.date_range(start, start)


# ---------------------------------------------------------------------------
# isinstance checks and variant attributes
# ---------------------------------------------------------------------------


def test_metadata_value_isinstance_checks():
    """Each variant is a distinct subtype of MetadataValue, usable with isinstance."""
    variants = {
        rs.MetadataValue.Text: rs.MetadataValue.text("hello"),
        rs.MetadataValue.Int: rs.MetadataValue.int(42),
        rs.MetadataValue.Float: rs.MetadataValue.float_(3.14),
        rs.MetadataValue.Bool: rs.MetadataValue.bool_(True),
        rs.MetadataValue.Url: rs.MetadataValue.url("https://example.com"),
        rs.MetadataValue.Path: rs.MetadataValue.path("/tmp/test"),
        rs.MetadataValue.Json: rs.MetadataValue.json('{"a": 1}'),
        rs.MetadataValue.Markdown: rs.MetadataValue.md("# Title"),
        rs.MetadataValue.Timestamp: rs.MetadataValue.timestamp(1700000000.0),
        rs.MetadataValue.Null: rs.MetadataValue.null(),
        rs.MetadataValue.Bytes: rs.MetadataValue.bytes(1024),
        rs.MetadataValue.Duration: rs.MetadataValue.duration(1.5),
        rs.MetadataValue.Sql: rs.MetadataValue.sql("SELECT 1"),
        rs.MetadataValue.CodeBlock: rs.MetadataValue.code_block("x = 1"),
        rs.MetadataValue.Image: rs.MetadataValue.image("base64data"),
        rs.MetadataValue.Percentage: rs.MetadataValue.percentage(0.95),
        rs.MetadataValue.List: rs.MetadataValue.list_([rs.MetadataValue.int(1)]),
        rs.MetadataValue.DateRange: rs.MetadataValue.date_range(
            datetime.datetime(2024, 1, 1), datetime.datetime(2024, 12, 31)
        ),
    }

    for variant_type, value in variants.items():
        # Every value is a MetadataValue
        assert isinstance(value, rs.MetadataValue), f"{value!r} should be MetadataValue"
        # Every value matches its specific variant type
        assert isinstance(value, variant_type), f"{value!r} should be {variant_type}"

    # Cross-check: a Text is NOT an Int, etc.
    text_val = rs.MetadataValue.text("hello")
    assert not isinstance(text_val, rs.MetadataValue.Int)
    assert not isinstance(text_val, rs.MetadataValue.Sql)

    sql_val = rs.MetadataValue.sql("SELECT 1", dialect="postgres")
    assert isinstance(sql_val, rs.MetadataValue.Sql)
    assert not isinstance(sql_val, rs.MetadataValue.Text)

    # Multi-field variants expose attributes
    assert sql_val.query == "SELECT 1"
    assert sql_val.dialect == "postgres"

    cb = rs.MetadataValue.code_block("x = 1", language="python")
    assert cb.code == "x = 1"
    assert cb.language == "python"

    dr = variants[rs.MetadataValue.DateRange]
    assert dr.start == datetime.datetime(2024, 1, 1)
    assert dr.end == datetime.datetime(2024, 12, 31)


# ---------------------------------------------------------------------------
# raw_value for every primitive variant
# ---------------------------------------------------------------------------


def test_metadata_value_text_raw_value():
    assert rs.MetadataValue.text("hello").raw_value() == "hello"


def test_metadata_value_int_raw_value():
    assert rs.MetadataValue.int(42).raw_value() == 42


def test_metadata_value_float_raw_value():
    assert rs.MetadataValue.float_(3.14).raw_value() == 3.14


def test_metadata_value_bool_raw_value():
    assert rs.MetadataValue.bool_(True).raw_value() is True
    assert rs.MetadataValue.bool_(False).raw_value() is False


def test_metadata_value_null_raw_value():
    assert rs.MetadataValue.null().raw_value() is None


def test_metadata_value_url_raw_value():
    """raw_value returns the canonicalized URL form produced by parsing."""
    v = rs.MetadataValue.url("https://example.com")
    assert v.raw_value() == "https://example.com/"


def test_metadata_value_path_raw_value():
    assert rs.MetadataValue.path("/tmp/foo").raw_value() == "/tmp/foo"


def test_metadata_value_json_raw_value():
    assert rs.MetadataValue.json('{"a":1}').raw_value() == '{"a":1}'


def test_metadata_value_md_raw_value():
    assert rs.MetadataValue.md("# Hello").raw_value() == "# Hello"


def test_metadata_value_timestamp_raw_value():
    assert rs.MetadataValue.timestamp(1700000000.0).raw_value() == 1700000000.0


def test_metadata_value_bytes_raw_value():
    assert rs.MetadataValue.bytes(1024).raw_value() == 1024


def test_metadata_value_duration_raw_value():
    assert rs.MetadataValue.duration(1.5).raw_value() == 1.5


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_metadata_value_empty_list():
    """Empty list is valid."""
    v = rs.MetadataValue.list_([])
    assert isinstance(v, rs.MetadataValue.List)
    assert v.raw_value() == []


def test_metadata_value_cross_type_inequality():
    """Different variant types are never equal even with same underlying value."""
    assert rs.MetadataValue.int(1) != rs.MetadataValue.float_(1.0)
    assert rs.MetadataValue.text("1") != rs.MetadataValue.int(1)
    assert rs.MetadataValue.null() != rs.MetadataValue.int(0)


def test_metadata_coerce_bool_before_int():
    """Bool coercion must produce MetadataValue.Bool, not Int (Python bool subclasses int)."""
    ctx = rs.OutputContext(asset_name="test", asset_metadata=None)
    ctx.add_output_metadata({"flag": True, "off": False})
    meta = ctx.output_metadata
    assert meta is not None
    assert isinstance(meta["flag"], rs.MetadataValue.Bool)
    assert isinstance(meta["off"], rs.MetadataValue.Bool)
    assert meta["flag"].raw_value() is True
    assert meta["off"].raw_value() is False


def test_metadata_coerce_none_to_null():
    """None coerces to MetadataValue.Null."""
    ctx = rs.OutputContext(asset_name="test", asset_metadata=None)
    ctx.add_output_metadata({"empty": None})
    meta = ctx.output_metadata
    assert meta is not None
    assert isinstance(meta["empty"], rs.MetadataValue.Null)
    assert meta["empty"].raw_value() is None


def test_add_output_metadata_accumulates():
    """Multiple add_output_metadata calls accumulate entries."""
    ctx = rs.OutputContext(asset_name="test", asset_metadata=None)
    ctx.add_output_metadata({"a": 1})
    ctx.add_output_metadata({"b": 2})
    meta = ctx.output_metadata
    assert meta is not None
    assert meta["a"] == rs.MetadataValue.int(1)
    assert meta["b"] == rs.MetadataValue.int(2)


# ---------------------------------------------------------------------------
# DataVersion variant
# ---------------------------------------------------------------------------


def test_metadata_value_data_version():
    """DataVersion variant constructor, raw_value, equality, repr."""
    v = rs.MetadataValue.data_version("v1.2.3")
    assert isinstance(v, rs.MetadataValue.DataVersion)
    assert v.raw_value() == "v1.2.3"
    assert v == rs.MetadataValue.data_version("v1.2.3")
    assert v != rs.MetadataValue.data_version("v2.0.0")
    assert repr(v) == 'MetadataValue.data_version("v1.2.3")'


def test_metadata_value_data_version_not_equal_to_text():
    """DataVersion is distinct from Text even with same string."""
    assert rs.MetadataValue.data_version("abc") != rs.MetadataValue.text("abc")


# ---------------------------------------------------------------------------
# URL validation
# ---------------------------------------------------------------------------


def test_metadata_value_url_invalid_raises():
    """Invalid URL raises an error."""
    with pytest.raises(Exception, match="Invalid URL"):
        rs.MetadataValue.url("not a url")


# ---------------------------------------------------------------------------
# repr round-tripping (correctness regressions)
# ---------------------------------------------------------------------------


def test_metadata_value_float_repr_whole_number():
    """Float repr of a whole number must be Python-valid (1.0, not 1).

    Otherwise eval(repr(...)) round-trips to MetadataValue.Int instead of Float.
    """
    v = rs.MetadataValue.float_(1.0)
    assert repr(v) == "MetadataValue.float_(1.0)"


def test_metadata_value_timestamp_repr_whole_number():
    """Timestamp repr of an integer-valued timestamp must remain a float literal."""
    v = rs.MetadataValue.timestamp(1700000000.0)
    # Currently emits MetadataValue.timestamp(1700000000) — not a float literal.
    assert repr(v) == "MetadataValue.timestamp(1700000000.0)"


def test_metadata_value_duration_repr_whole_number():
    v = rs.MetadataValue.duration(2.0)
    assert repr(v) == "MetadataValue.duration(2.0)"


def test_metadata_value_percentage_repr_whole_number():
    v = rs.MetadataValue.percentage(50.0)
    assert repr(v) == "MetadataValue.percentage(50.0)"


def test_metadata_value_date_range_repr_python_valid():
    """DateRange repr should be a valid Python expression that reconstructs the value."""
    start = datetime.datetime(2024, 1, 1, 0, 0, 0)
    end = datetime.datetime(2024, 1, 31, 23, 59, 59)
    v = rs.MetadataValue.date_range(start, end)
    rebuilt = eval(
        repr(v),
        {"MetadataValue": rs.MetadataValue, "datetime": datetime},
    )
    assert rebuilt == v


def test_metadata_value_text_repr_non_ascii():
    """Text repr with non-ASCII characters must be valid Python (no \\u{e9} escapes)."""
    v = rs.MetadataValue.text("héllo")
    rebuilt = eval(repr(v), {"MetadataValue": rs.MetadataValue})
    assert rebuilt == v


# ---------------------------------------------------------------------------
# Url canonicalization
# ---------------------------------------------------------------------------


def test_metadata_value_url_canonicalized():
    """Two URLs that parse to the same canonical form should compare equal."""
    v1 = rs.MetadataValue.url("https://example.com")
    v2 = rs.MetadataValue.url("https://example.com/")
    assert v1 == v2
