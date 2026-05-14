import pytest

import rivers as rs
from rivers.exceptions import PartitionDefinitionError


def test_single():
    """Single-dimension partition key."""
    key = rs.PartitionKey.single("2024-01-15")
    assert key.key == ["2024-01-15"]
    assert repr(key) == 'PartitionKey("2024-01-15")'


def test_multi():
    """Multi-dimension partition key."""
    key = rs.PartitionKey.multi({"region": "us-east", "date": "2024-01-15"})
    assert key.keys == {"region": ["us-east"], "date": ["2024-01-15"]}
    assert repr(key) == 'PartitionKey({"date": "2024-01-15", "region": "us-east"})'


def test_equality():
    """PartitionKey equality and hashing."""
    k1 = rs.PartitionKey.single("a")
    k2 = rs.PartitionKey.single("a")
    k3 = rs.PartitionKey.single("b")
    assert k1 == k2
    assert k1 != k3
    assert hash(k1) == hash(k2)

    m1 = rs.PartitionKey.multi({"x": "1", "y": "2"})
    m2 = rs.PartitionKey.multi({"x": "1", "y": "2"})
    m3 = rs.PartitionKey.multi({"x": "1", "y": "3"})
    assert m1 == m2
    assert m1 != m3
    assert hash(m1) == hash(m2)

    # Single != Multi
    assert k1 != m1


def test_isinstance():
    """PartitionKey variant isinstance checks."""
    single = rs.PartitionKey.single("abc")
    multi = rs.PartitionKey.multi({"x": "1"})
    assert isinstance(single, rs.PartitionKey)
    assert isinstance(multi, rs.PartitionKey)
    assert isinstance(single, rs.PartitionKey.Single)
    assert isinstance(multi, rs.PartitionKey.Multi)
    assert not isinstance(single, rs.PartitionKey.Multi)
    assert not isinstance(multi, rs.PartitionKey.Single)


def test_validation():
    """PartitionKey rejects invalid inputs."""
    with pytest.raises(PartitionDefinitionError):
        rs.PartitionKey.multi({})


# ── JSON round-trip tests ──


def test_json_round_trip_single_date():
    pk = rs.PartitionKey.single("2025-01-16")
    assert rs.PartitionKey.from_json(pk.to_json()) == pk


def test_json_round_trip_single_region():
    pk = rs.PartitionKey.single("us-east-1")
    assert rs.PartitionKey.from_json(pk.to_json()) == pk


def test_json_round_trip_single_composite():
    pk = rs.PartitionKey.single(["2025-01-16", "us-east"])
    assert rs.PartitionKey.from_json(pk.to_json()) == pk


def test_json_round_trip_multi_two_dims():
    pk = rs.PartitionKey.multi({"region": "us-east", "date": "2025-01-16"})
    assert rs.PartitionKey.from_json(pk.to_json()) == pk


def test_json_round_trip_multi_three_dims():
    pk = rs.PartitionKey.multi(
        {"region": "eu-west", "date": "2025-03-01", "tier": "premium"}
    )
    assert rs.PartitionKey.from_json(pk.to_json()) == pk


def test_json_round_trip_multi_composite_values():
    pk = rs.PartitionKey.multi({"region": ["us-east", "us-west"], "date": "2025-01-16"})
    assert rs.PartitionKey.from_json(pk.to_json()) == pk


def test_json_round_trip_special_characters():
    pk = rs.PartitionKey.single("path/to/data.csv")
    assert rs.PartitionKey.from_json(pk.to_json()) == pk


def test_json_round_trip_unicode():
    pk = rs.PartitionKey.single("日本語")
    assert rs.PartitionKey.from_json(pk.to_json()) == pk


def test_json_round_trip_empty_looking_string():
    pk = rs.PartitionKey.single(" ")
    assert rs.PartitionKey.from_json(pk.to_json()) == pk


def test_json_format_single():
    import json

    pk = rs.PartitionKey.single("2025-01-16")
    data = json.loads(pk.to_json())
    assert data == {"single": ["2025-01-16"]}


def test_json_format_multi():
    import json

    pk = rs.PartitionKey.multi({"region": "us-east"})
    data = json.loads(pk.to_json())
    assert data == {"multi": {"region": ["us-east"]}}


def test_from_json_invalid():
    with pytest.raises(PartitionDefinitionError):
        rs.PartitionKey.from_json("not json")


def test_from_json_missing_key():
    with pytest.raises(PartitionDefinitionError):
        rs.PartitionKey.from_json('{"unknown": [1]}')
