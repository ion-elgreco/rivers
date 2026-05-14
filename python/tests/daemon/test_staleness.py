"""Integration tests for staleness detection."""

import rivers as rs


def _stale(storage, key):
    """Live staleness for one asset. ``stale_status`` is no longer persisted —
    callers go through ``Storage.compute_staleness()``."""
    return storage.compute_staleness().get(key, ("Missing", []))


def test_never_materialized_is_missing(storage):
    """Assets that have never been materialized have stale_status 'Missing'."""

    @rs.Asset(name="a")
    def a() -> int:
        return 1

    repo = rs.CodeRepository(assets=[a])
    repo.resolve(storage=storage)

    record = storage.get_asset_record("a")
    assert record is not None
    status, causes = _stale(storage, "a")
    assert status == "Missing"
    assert causes == []


def test_materialized_becomes_up_to_date(storage):
    """After materializing, assets become 'UpToDate'."""

    @rs.Asset(name="a", io_handler=rs.InMemoryIOHandler())
    def a() -> int:
        return 42

    repo = rs.CodeRepository(assets=[a])
    repo.resolve(storage=storage)
    repo.materialize()

    record = storage.get_asset_record("a")
    assert record is not None
    status, causes = _stale(storage, "a")
    assert status == "UpToDate"
    assert causes == []
    assert record.last_data_version is not None


def test_code_version_change_makes_stale(storage):
    """Changing code_version after materialization makes asset stale."""

    @rs.Asset(name="a", code_version="v1", io_handler=rs.InMemoryIOHandler())
    def a_v1() -> int:
        return 1

    repo = rs.CodeRepository(assets=[a_v1])
    repo.resolve(storage=storage)
    repo.materialize()

    record = storage.get_asset_record("a")
    assert record is not None
    assert _stale(storage, "a")[0] == "UpToDate"
    assert record.last_materialization_code_version == "v1"

    # Re-resolve with new code version
    @rs.Asset(name="a", code_version="v2", io_handler=rs.InMemoryIOHandler())
    def a_v2() -> int:
        return 2

    repo2 = rs.CodeRepository(assets=[a_v2])
    repo2.resolve(storage=storage)

    status, causes = _stale(storage, "a")
    assert status == "Stale"
    assert len(causes) == 1
    assert causes[0].category == "Code"
    assert causes[0].dependency is None


def test_upstream_rematerialization_makes_downstream_stale(storage):
    """When upstream gets new data, downstream becomes stale."""

    @rs.Asset(name="raw", io_handler=rs.InMemoryIOHandler())
    def raw() -> int:
        return 1

    @rs.Asset(name="processed", io_handler=rs.InMemoryIOHandler())
    def processed(raw: int) -> int:
        return raw * 2

    repo = rs.CodeRepository(assets=[raw, processed])
    repo.resolve(storage=storage)
    repo.materialize()

    # Both should be up-to-date
    snapshot = storage.compute_staleness()
    assert snapshot["raw"][0] == "UpToDate"
    assert snapshot["processed"][0] == "UpToDate"

    # Re-materialize only upstream
    repo.materialize(selection=["raw"])

    # Downstream should be stale (upstream has new data_version)
    snapshot = storage.compute_staleness()
    assert snapshot["raw"][0] == "UpToDate"
    proc_status, proc_causes = snapshot["processed"]
    assert proc_status == "Stale"
    assert len(proc_causes) >= 1
    assert proc_causes[0].category == "Data"
    assert proc_causes[0].dependency == "raw"


def test_full_rematerialization_makes_all_up_to_date(storage):
    """Full rematerialization after staleness makes everything up-to-date again."""

    @rs.Asset(name="a", code_version="v1", io_handler=rs.InMemoryIOHandler())
    def a_v1() -> int:
        return 1

    @rs.Asset(name="b", io_handler=rs.InMemoryIOHandler())
    def b(a: int) -> int:
        return a + 1

    repo = rs.CodeRepository(assets=[a_v1, b])
    repo.resolve(storage=storage)
    repo.materialize()

    # Make a stale by changing code version
    @rs.Asset(name="a", code_version="v2", io_handler=rs.InMemoryIOHandler())
    def a_v2() -> int:
        return 10

    repo2 = rs.CodeRepository(assets=[a_v2, b])
    repo2.resolve(storage=storage)

    assert _stale(storage, "a")[0] == "Stale"

    # Re-materialize everything
    repo2.materialize()

    snapshot = storage.compute_staleness()
    assert snapshot["a"][0] == "UpToDate"
    assert snapshot["b"][0] == "UpToDate"


def test_provenance_recorded_on_asset_record(storage):
    """Materialization records provenance on the asset record."""

    @rs.Asset(name="src", code_version="v1", io_handler=rs.InMemoryIOHandler())
    def src() -> int:
        return 42

    @rs.Asset(name="dst", code_version="v1", io_handler=rs.InMemoryIOHandler())
    def dst(src: int) -> int:
        return src * 2

    repo = rs.CodeRepository(assets=[src, dst])
    repo.resolve(storage=storage)
    repo.materialize()

    # Check src asset record provenance
    src_record = storage.get_asset_record("src")
    assert src_record is not None
    assert src_record.last_materialization_code_version == "v1"
    assert src_record.last_input_data_versions == []

    # Check dst asset record provenance — should have consumed src's data version
    dst_record = storage.get_asset_record("dst")
    assert dst_record is not None
    assert dst_record.last_materialization_code_version == "v1"
    assert len(dst_record.last_input_data_versions) == 1
    assert dst_record.last_input_data_versions[0][0] == "src"
    # The consumed version should match src's current data version
    assert dst_record.last_input_data_versions[0][1] == src_record.last_data_version


def test_transitive_staleness(storage):
    """If A is stale, B (depends on A) and C (depends on B) are also stale."""

    @rs.Asset(name="a", code_version="v1", io_handler=rs.InMemoryIOHandler())
    def a() -> int:
        return 1

    @rs.Asset(name="b", io_handler=rs.InMemoryIOHandler())
    def b(a: int) -> int:
        return a + 1

    @rs.Asset(name="c", io_handler=rs.InMemoryIOHandler())
    def c(b: int) -> int:
        return b + 1

    repo = rs.CodeRepository(assets=[a, b, c])
    repo.resolve(storage=storage)
    repo.materialize()

    # Change a's code version
    @rs.Asset(name="a", code_version="v2", io_handler=rs.InMemoryIOHandler())
    def a_v2() -> int:
        return 10

    repo2 = rs.CodeRepository(assets=[a_v2, b, c])
    repo2.resolve(storage=storage)

    snapshot = storage.compute_staleness()
    assert snapshot["a"][0] == "Stale"
    assert snapshot["b"][0] == "Stale"
    assert snapshot["c"][0] == "Stale"


def test_concurrent_upstream_rematerialization_detected(storage):
    """Race condition regression: if upstream is re-materialized between the time
    downstream reads it and the time the materialization event is stored, the
    provenance should reflect what downstream actually consumed, not the latest
    storage value.

    This test simulates the race by materializing the full pipeline, then
    re-materializing only upstream (giving it a new data_version), and verifying
    that downstream is correctly marked stale because it consumed the old version.
    """

    @rs.Asset(name="source", io_handler=rs.InMemoryIOHandler())
    def source() -> int:
        return 1

    @rs.Asset(name="sink", io_handler=rs.InMemoryIOHandler())
    def sink(source: int) -> int:
        return source + 1

    repo = rs.CodeRepository(assets=[source, sink])
    repo.resolve(storage=storage)

    # Materialize everything — both become UpToDate
    repo.materialize()
    src_rec = storage.get_asset_record("source")
    sink_rec = storage.get_asset_record("sink")
    assert src_rec is not None
    assert sink_rec is not None
    snapshot = storage.compute_staleness()
    assert snapshot["source"][0] == "UpToDate"
    assert snapshot["sink"][0] == "UpToDate"

    # Record what version sink consumed
    consumed_version = sink_rec.last_input_data_versions
    assert len(consumed_version) == 1
    assert consumed_version[0][0] == "source"
    original_source_version = consumed_version[0][1]

    # Re-materialize only source — it gets a NEW data_version
    repo.materialize(selection=["source"])

    src_rec = storage.get_asset_record("source")
    assert src_rec is not None
    assert _stale(storage, "source")[0] == "UpToDate"
    # Source should have a different version now
    assert src_rec.last_data_version != original_source_version

    # Sink should be stale: it consumed the old version, source now has a new one
    sink_rec = storage.get_asset_record("sink")
    assert sink_rec is not None
    sink_status, sink_causes = _stale(storage, "sink")
    assert sink_status == "Stale"
    assert len(sink_causes) >= 1
    assert sink_causes[0].category == "Data"
    assert sink_causes[0].dependency == "source"

    # Verify sink's provenance still shows the old consumed version
    assert sink_rec.last_input_data_versions[0][1] == original_source_version
