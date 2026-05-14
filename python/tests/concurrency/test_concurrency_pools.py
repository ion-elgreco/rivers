"""Concurrency Pools — Storage Layer tests.

Tests pool limit CRUD, pool info queries, and asset decorator pool/pool_slots parameters.
"""

import pytest
from rivers import Asset, AssetDef
from rivers._core.storage import PoolInfo


# ── Pool limit CRUD ──


def test_set_and_get_pool_limits(storage):
    """Pool limits can be created and retrieved."""
    storage.set_pool_limit("database", 10)
    storage.set_pool_limit("api_quota", 5, lease_duration="10m")

    pools = storage.get_pool_limits()
    assert len(pools) == 2
    # Ordered alphabetically
    assert pools[0].pool_key == "api_quota"
    assert pools[0].slot_limit == 5
    assert pools[0].lease_duration_secs == 600
    assert pools[1].pool_key == "database"
    assert pools[1].slot_limit == 10
    assert pools[1].lease_duration_secs == 300  # default


def test_set_pool_limit_upsert(storage):
    """Setting a pool limit twice overwrites the previous value."""
    storage.set_pool_limit("database", 10)
    storage.set_pool_limit("database", 20, lease_duration="15m")

    pools = storage.get_pool_limits()
    assert len(pools) == 1
    assert pools[0].slot_limit == 20
    assert pools[0].lease_duration_secs == 900


def test_get_pool_info(storage):
    """Pool info returns config and zero counts when no slots are claimed."""
    storage.set_pool_limit("database", 10)

    info = storage.get_pool_info("database")
    assert isinstance(info, PoolInfo)
    assert info.pool_key == "database"
    assert info.slot_limit == 10
    assert info.claimed_count == 0
    assert info.pending_count == 0


def test_get_pool_info_not_found(storage):
    """Querying info for a nonexistent pool raises an error."""
    with pytest.raises(Exception, match="not configured"):
        storage.get_pool_info("nonexistent")


def test_pool_limits_initially_empty(storage):
    """No pools configured by default."""
    assert storage.get_pool_limits() == []


# ── Asset decorator pool/pool_slots ──


def test_asset_decorator_single_pool():
    """@Asset(pool='database') normalizes to [(database, 1)]."""

    @Asset(pool="database")
    def my_asset():
        pass

    assert my_asset.pool == [("database", 1)]


def test_asset_decorator_pool_with_slots():
    """@Asset(pool='ml_gpu', pool_slots=2) sets custom slot count."""

    @Asset(pool="ml_gpu", pool_slots=2)
    def train_model():
        pass

    assert train_model.pool == [("ml_gpu", 2)]


def test_asset_decorator_multi_pool():
    """@Asset(pool=['database', 'api']) normalizes to [(database, 1), (api, 1)]."""

    @Asset(pool=["database", "api"])
    def sync_orders():
        pass

    assert sync_orders.pool == [("database", 1), ("api", 1)]


def test_asset_decorator_multi_pool_with_dict_slots():
    """Per-pool slot weights via dict."""

    @Asset(pool=["database", "api"], pool_slots={"database": 3, "api": 1})
    def heavy_sync():
        pass

    assert heavy_sync.pool == [("database", 3), ("api", 1)]


def test_asset_decorator_multi_pool_uniform_slots():
    """Uniform pool_slots applies to all pools."""

    @Asset(pool=["a", "b"], pool_slots=5)
    def multi():
        pass

    assert multi.pool == [("a", 5), ("b", 5)]


def test_asset_decorator_no_pool():
    """No pool = empty list."""

    @Asset
    def plain():
        pass

    assert plain.pool == []


def test_asset_def_with_pool():
    """AssetDef accepts pool and pool_slots."""
    defn = AssetDef("my_output", pool="database", pool_slots=2)
    assert defn.pool == [("database", 2)]


def test_asset_def_no_pool():
    """AssetDef without pool has empty list."""
    defn = AssetDef("my_output")
    assert defn.pool == []


def test_asset_decorator_pool_invalid_type():
    """pool must be str or list[str]."""
    with pytest.raises(Exception, match="pool must be"):

        @Asset(pool=123)
        def bad():
            pass


def test_asset_decorator_pool_slots_invalid_type():
    """pool_slots must be int or dict."""
    with pytest.raises(Exception, match="pool_slots must be"):

        @Asset(pool="db", pool_slots="invalid")
        def bad():
            pass


def test_asset_decorator_pool_empty_key():
    """Empty pool key is rejected."""
    with pytest.raises(Exception, match="non-empty"):

        @Asset(pool="")
        def bad():
            pass


def test_asset_decorator_pool_slots_unknown_key():
    """pool_slots dict keys must match pool list."""
    with pytest.raises(Exception, match="not in pool list"):

        @Asset(pool="database", pool_slots={"typo_db": 3})
        def bad():
            pass
