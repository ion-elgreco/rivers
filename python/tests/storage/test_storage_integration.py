"""Integration tests for storage wiring across the API surface."""

from pathlib import Path

import pytest
from typer.testing import CliRunner

import rivers as rs
from rivers.cli import _cleanup_storage, _create_storage, app

runner = CliRunner()


def test_repository_has_storage():
    """CodeRepository auto-creates in-memory storage."""

    @rs.Asset(name="a")
    def a() -> int:
        return 1

    repo = rs.CodeRepository(assets=[a])
    storage = repo.storage
    assert storage is not None
    assert storage.type == rs.StorageType.Memory


def test_repository_accepts_explicit_storage():
    """CodeRepository accepts an explicit Storage instance via resolve()."""
    storage = rs.Storage.memory()
    assert storage.type == rs.StorageType.Memory

    @rs.Asset(name="a")
    def a() -> int:
        return 1

    repo = rs.CodeRepository(assets=[a])
    repo.resolve(storage=storage)
    assert repo.storage is not None
    assert repo.storage.type == rs.StorageType.Memory


def test_execute_creates_run():
    """Executing a job creates a run record with Success status."""
    storage = rs.Storage.memory()

    @rs.Asset(name="a")
    def a() -> int:
        return 42

    repo = rs.CodeRepository(assets=[a])
    repo.resolve(storage=storage)
    repo.materialize()

    runs = storage.get_runs()
    assert len(runs) == 1
    assert runs[0].status == "Success"
    assert runs[0].job_name is None  # ad-hoc materialize() run
    assert runs[0].start_time is not None
    assert runs[0].end_time is not None


def test_materialization_events():
    """Materialization emits events for assets with io_handlers."""
    storage = rs.Storage.memory()

    @rs.Asset(name="a", io_handler=rs.InMemoryIOHandler())
    def a() -> int:
        return 1

    repo = rs.CodeRepository(assets=[a], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    repo.materialize()

    events = storage.get_events_for_asset("a")
    event_types = [e.event_type for e in events]
    assert "Materialization" in event_types


def test_step_events():
    """Each step emits StepStart and StepSuccess events."""
    storage = rs.Storage.memory()

    @rs.Asset(name="a")
    def a() -> int:
        return 1

    @rs.Asset(name="b")
    def b(a: int) -> int:
        return a + 1

    repo = rs.CodeRepository(assets=[a, b], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    repo.materialize()

    events_a = storage.get_events_for_asset("a")
    types_a = {e.event_type for e in events_a}
    assert "StepStart" in types_a
    assert "StepSuccess" in types_a

    events_b = storage.get_events_for_asset("b")
    types_b = {e.event_type for e in events_b}
    assert "StepStart" in types_b
    assert "StepSuccess" in types_b


def test_failed_run_tracked():
    """A failing asset produces a Failure run record."""
    storage = rs.Storage.memory()

    @rs.Asset(name="bad")
    def bad() -> int:
        raise ValueError("boom")

    repo = rs.CodeRepository(assets=[bad], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)

    try:
        repo.materialize()
    except Exception:
        pass

    runs = storage.get_runs()
    assert len(runs) == 1
    assert runs[0].status == "Failure"

    # Should also have a StepFailure event
    events = storage.get_events_for_asset("bad")
    types = {e.event_type for e in events}
    assert "StepFailure" in types


def test_kv_roundtrip():
    """Basic kv_set/kv_get round-trip."""
    storage = rs.Storage.memory()
    storage.kv_set("test_key", b"hello")
    result = storage.kv_get("test_key")
    assert result == b"hello"


def test_kv_get_missing_key():
    """kv_get returns None for missing keys."""
    storage = rs.Storage.memory()
    assert storage.kv_get("nonexistent") is None


def test_asset_records_after_materialization():
    """Asset records are updated after materialization with io_handler."""
    storage = rs.Storage.memory()

    @rs.Asset(name="x", io_handler=rs.InMemoryIOHandler())
    def x() -> int:
        return 99

    repo = rs.CodeRepository(assets=[x], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    repo.materialize()

    record = storage.get_asset_record("x")
    assert record is not None
    assert record.asset_key == "x"
    assert record.last_run_id is not None
    assert record.last_timestamp is not None


def test_multiple_runs_tracked():
    """Multiple materializations create multiple run records."""
    storage = rs.Storage.memory()

    @rs.Asset(name="a")
    def a() -> int:
        return 1

    repo = rs.CodeRepository(assets=[a], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    repo.materialize()
    repo.materialize()

    runs = storage.get_runs()
    assert len(runs) == 2
    assert all(r.status == "Success" for r in runs)


def test_run_events_linked_by_run_id():
    """All events from a run share the same run_id."""
    storage = rs.Storage.memory()

    @rs.Asset(name="a")
    def a() -> int:
        return 1

    @rs.Asset(name="b")
    def b(a: int) -> int:
        return a + 1

    repo = rs.CodeRepository(assets=[a, b], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    repo.materialize()

    runs = storage.get_runs()
    assert len(runs) == 1
    run_id = runs[0].run_id

    events = storage.get_events_for_run(run_id)
    assert len(events) > 0
    assert all(e.run_id == run_id for e in events)


def test_embedded_storage_type(tmp_path: Path):
    """Embedded storage reports StorageType.Embedded."""
    path = str(tmp_path / "db")
    storage = rs.Storage.embedded(path)
    assert storage.type == rs.StorageType.Embedded


def test_embedded_storage_persists_data(tmp_path: Path):
    """Embedded storage writes to disk and can be read back."""
    path = str(tmp_path / "db")
    storage = rs.Storage.embedded(path)
    storage.kv_set("persist_key", b"persist_value")
    assert storage.kv_get("persist_key") == b"persist_value"
    assert Path(path).exists()


def test_cleanup_storage_removes_directory(tmp_path: Path):
    """_cleanup_storage removes the storage directory."""
    storage_path = tmp_path / ".rivers" / "storage"
    storage_path.mkdir(parents=True)
    (storage_path / "dummy.db").write_bytes(b"data")

    _cleanup_storage(str(storage_path))

    assert not storage_path.exists()


def test_cleanup_storage_removes_empty_parent(tmp_path: Path):
    """_cleanup_storage removes .rivers parent if it becomes empty."""
    rivers_dir = tmp_path / ".rivers"
    storage_path = rivers_dir / "storage"
    storage_path.mkdir(parents=True)

    _cleanup_storage(str(storage_path))

    assert not rivers_dir.exists()


def test_cleanup_storage_keeps_nonempty_parent(tmp_path: Path):
    """_cleanup_storage keeps .rivers parent if it still has other contents."""
    rivers_dir = tmp_path / ".rivers"
    storage_path = rivers_dir / "storage"
    storage_path.mkdir(parents=True)
    (rivers_dir / "config.toml").write_text("keep me")

    _cleanup_storage(str(storage_path))

    assert not storage_path.exists()
    assert rivers_dir.exists()
    assert (rivers_dir / "config.toml").exists()


def test_cleanup_storage_noop_if_missing(tmp_path: Path):
    """_cleanup_storage is a no-op if the path doesn't exist."""
    _cleanup_storage(str(tmp_path / "nonexistent"))  # Should not raise


def test_create_storage_memory():
    """_create_storage with memory=True returns in-memory storage."""
    storage = _create_storage(memory=True, storage_path="unused")
    assert storage.type == rs.StorageType.Memory


def test_create_storage_embedded(tmp_path: Path):
    """_create_storage with memory=False returns embedded storage."""
    path = str(tmp_path / "db")
    storage = _create_storage(memory=False, storage_path=path)
    assert storage.type == rs.StorageType.Embedded
    assert Path(path).exists()


# --- CLI integration tests ---


def test_cli_dev_missing_module(tmp_path: Path):
    """rivers dev with nonexistent module fails gracefully."""
    result = runner.invoke(
        app,
        ["dev", "nonexistent_module_xyz", "--storage-path", str(tmp_path / "storage")],
    )
    assert result.exit_code != 0
    assert "not found" in result.output.lower() or result.exit_code == 1


def test_cli_dev_requires_module_arg():
    """rivers dev requires a module argument."""
    result = runner.invoke(app, ["dev"])
    assert result.exit_code != 0


def test_cli_materialize_missing_module(tmp_path: Path):
    """rivers materialize with nonexistent module fails gracefully."""
    result = runner.invoke(
        app,
        [
            "materialize",
            "nonexistent_module_xyz",
            "--storage-path",
            str(tmp_path / "storage"),
        ],
    )
    assert result.exit_code != 0


def test_cli_materialize_memory_missing_module():
    """rivers materialize --memory with nonexistent module fails gracefully."""
    result = runner.invoke(app, ["materialize", "--memory", "nonexistent_module_xyz"])
    assert result.exit_code != 0


def test_migrate_embedded_initializes_store(tmp_path: Path):
    """``Storage.migrate_embedded`` brings up a fresh embedded store.

    Each test opens a unique path exactly once: reopening the same RocksDB path
    in-process is flaky (the router task releases the file lock asynchronously),
    so migration correctness — idempotence, downgrade refusal, the lease — is
    covered by the rivers-core tests; this confirms the Python binding is wired.
    """
    path = str(tmp_path / "db")
    rs.Storage.migrate_embedded(path)
    assert Path(path).exists()


def test_cli_db_migrate_embedded(tmp_path: Path):
    """``rivers db migrate`` initializes an embedded store and reports success."""
    path = str(tmp_path / "db")
    result = runner.invoke(app, ["db", "migrate", "--storage-path", path])
    assert result.exit_code == 0, result.output
    assert "up to date" in result.output
    assert Path(path).exists()


def test_schema_migration_needed_error_is_a_storage_error():
    """The behind-build open raises a typed `SchemaMigrationNeededError`
    (caught by `rivers dev`); it subclasses `StorageError` so existing
    `except StorageError` handlers still catch it. The actual behind-build
    trigger is covered by the rivers-core open tests.
    """
    from rivers.exceptions import SchemaMigrationNeededError, StorageError

    assert issubclass(SchemaMigrationNeededError, StorageError)


# --- Asset catalog tests ---


def test_asset_catalog_has_tags_kind_group():
    """Asset catalog stores tags, kinds, group, and code_version after resolve."""
    storage = rs.Storage.memory()

    @rs.Asset(
        name="x",
        tags=["etl", "daily"],
        kinds="table",
        group="analytics",
        code_version="v1",
    )
    def x() -> int:
        return 1

    repo = rs.CodeRepository(assets=[x])
    repo.resolve(storage=storage)

    record = storage.get_asset_record("x")
    assert record is not None
    assert record.asset_key == "x"
    assert record.tags == ["etl", "daily"]
    assert record.kinds == ["table"]
    assert record.group == "analytics"
    assert record.code_version == "v1"
    assert record.last_event_id is None
    assert record.last_run_id is None
    assert record.last_timestamp is None


def test_get_assets_by_tag():
    """get_assets_by_tag filters assets by tag."""
    storage = rs.Storage.memory()

    @rs.Asset(name="a", tags=["etl", "daily"])
    def a() -> int:
        return 1

    @rs.Asset(name="b", tags=["ml"])
    def b() -> int:
        return 2

    @rs.Asset(name="c", tags=["etl"])
    def c() -> int:
        return 3

    repo = rs.CodeRepository(assets=[a, b, c])
    repo.resolve(storage=storage)

    etl_assets = storage.get_assets_by_tag("etl")
    assert len(etl_assets) == 2
    etl_by_key = {r.asset_key: r for r in etl_assets}
    assert set(etl_by_key.keys()) == {"a", "c"}
    assert etl_by_key["a"].tags == ["etl", "daily"]
    assert etl_by_key["a"].kinds == []
    assert etl_by_key["a"].group is None
    assert etl_by_key["a"].code_version is None
    assert etl_by_key["a"].last_event_id is None
    assert etl_by_key["a"].last_run_id is None
    assert etl_by_key["a"].last_timestamp is None
    assert etl_by_key["c"].tags == ["etl"]
    assert etl_by_key["c"].kinds == []
    assert etl_by_key["c"].group is None

    ml_assets = storage.get_assets_by_tag("ml")
    assert len(ml_assets) == 1
    assert ml_assets[0].asset_key == "b"
    assert ml_assets[0].tags == ["ml"]
    assert ml_assets[0].kinds == []
    assert ml_assets[0].group is None

    empty = storage.get_assets_by_tag("nonexistent")
    assert len(empty) == 0


def test_get_assets_by_kind():
    """get_assets_by_kind filters assets by kind."""
    storage = rs.Storage.memory()

    @rs.Asset(name="a", kinds="table")
    def a() -> int:
        return 1

    @rs.Asset(name="b", kinds="model")
    def b() -> int:
        return 2

    repo = rs.CodeRepository(assets=[a, b])
    repo.resolve(storage=storage)

    tables = storage.get_assets_by_kind("table")
    assert len(tables) == 1
    assert tables[0].asset_key == "a"
    assert tables[0].tags == []
    assert tables[0].kinds == ["table"]
    assert tables[0].group is None
    assert tables[0].code_version is None
    assert tables[0].last_event_id is None
    assert tables[0].last_run_id is None
    assert tables[0].last_timestamp is None

    models = storage.get_assets_by_kind("model")
    assert len(models) == 1
    assert models[0].asset_key == "b"
    assert models[0].kinds == ["model"]

    empty = storage.get_assets_by_kind("nonexistent")
    assert len(empty) == 0


def test_get_assets_by_group():
    """get_assets_by_group filters assets by group."""
    storage = rs.Storage.memory()

    @rs.Asset(name="a", group="analytics")
    def a() -> int:
        return 1

    @rs.Asset(name="b", group="analytics")
    def b() -> int:
        return 2

    @rs.Asset(name="c", group="ml")
    def c() -> int:
        return 3

    repo = rs.CodeRepository(assets=[a, b, c])
    repo.resolve(storage=storage)

    analytics = storage.get_assets_by_group("analytics")
    assert len(analytics) == 2
    analytics_by_key = {r.asset_key: r for r in analytics}
    assert set(analytics_by_key.keys()) == {"a", "b"}
    assert analytics_by_key["a"].tags == []
    assert analytics_by_key["a"].kinds == []
    assert analytics_by_key["a"].group == "analytics"
    assert analytics_by_key["a"].code_version is None
    assert analytics_by_key["a"].last_event_id is None
    assert analytics_by_key["a"].last_run_id is None
    assert analytics_by_key["a"].last_timestamp is None
    assert analytics_by_key["b"].group == "analytics"

    ml = storage.get_assets_by_group("ml")
    assert len(ml) == 1
    assert ml[0].asset_key == "c"
    assert ml[0].group == "ml"
    assert ml[0].tags == []
    assert ml[0].kinds == []

    empty = storage.get_assets_by_group("nonexistent")
    assert len(empty) == 0


def test_catalog_preserved_across_materializations():
    """Re-materializing preserves catalog fields while updating run fields."""
    storage = rs.Storage.memory()

    @rs.Asset(
        name="x",
        tags=["etl"],
        kinds="table",
        group="warehouse",
        io_handler=rs.InMemoryIOHandler(),
    )
    def x() -> int:
        return 1

    repo = rs.CodeRepository(assets=[x], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    repo.materialize()

    record1 = storage.get_asset_record("x")
    assert record1 is not None
    run_id_1 = record1.last_run_id

    repo.materialize()

    record2 = storage.get_asset_record("x")
    assert record2 is not None
    # Catalog fields preserved
    assert record2.tags == ["etl"]
    assert record2.kinds == ["table"]
    assert record2.group == "warehouse"
    # Run fields updated
    assert record2.last_run_id != run_id_1


# --- Async storage tests ---


@pytest.mark.asyncio
async def test_async_kv_roundtrip():
    """Async kv_set/kv_get round-trip."""
    storage = rs.Storage.memory()
    await storage.async_kv_set("test_key", b"hello")
    result = await storage.async_kv_get("test_key")
    assert result == b"hello"


@pytest.mark.asyncio
async def test_async_kv_get_missing_key():
    """Async kv_get returns None for missing keys."""
    storage = rs.Storage.memory()
    assert await storage.async_kv_get("nonexistent") is None


@pytest.mark.asyncio
async def test_async_get_asset_records():
    """Async get_asset_records returns records after resolve."""
    storage = rs.Storage.memory()

    @rs.Asset(name="x", tags=["etl"], kinds="table", group="analytics")
    def x() -> int:
        return 1

    repo = rs.CodeRepository(assets=[x])
    repo.resolve(storage=storage)

    records = await storage.async_get_asset_records()
    assert len(records) == 1
    assert records[0].asset_key == "x"
    assert records[0].tags == ["etl"]
    assert records[0].kinds == ["table"]
    assert records[0].group == "analytics"


@pytest.mark.asyncio
async def test_async_get_asset_record():
    """Async get_asset_record returns a single record."""
    storage = rs.Storage.memory()

    @rs.Asset(name="a")
    def a() -> int:
        return 1

    repo = rs.CodeRepository(assets=[a])
    repo.resolve(storage=storage)

    record = await storage.async_get_asset_record("a")
    assert record is not None
    assert record.asset_key == "a"

    missing = await storage.async_get_asset_record("nonexistent")
    assert missing is None


@pytest.mark.asyncio
async def test_async_get_runs_after_materialize():
    """Async get_runs returns run records after materialization."""
    storage = rs.Storage.memory()

    @rs.Asset(name="a")
    def a() -> int:
        return 42

    repo = rs.CodeRepository(assets=[a])
    repo.resolve(storage=storage)
    repo.materialize()

    runs = await storage.async_get_runs()
    assert len(runs) == 1
    assert runs[0].status == "Success"

    run = await storage.async_get_run(runs[0].run_id)
    assert run is not None
    assert run.run_id == runs[0].run_id


@pytest.mark.asyncio
async def test_async_get_events_for_asset():
    """Async get_events_for_asset returns events."""
    storage = rs.Storage.memory()

    @rs.Asset(name="a", io_handler=rs.InMemoryIOHandler())
    def a() -> int:
        return 1

    repo = rs.CodeRepository(assets=[a], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    repo.materialize()

    events = await storage.async_get_events_for_asset("a")
    event_types = [e.event_type for e in events]
    assert "Materialization" in event_types

    runs = await storage.async_get_runs()
    run_events = await storage.async_get_events_for_run(runs[0].run_id)
    assert len(run_events) > 0


@pytest.mark.asyncio
async def test_async_get_latest_materialization():
    """Async get_latest_materialization returns the latest event."""
    storage = rs.Storage.memory()

    @rs.Asset(name="a", io_handler=rs.InMemoryIOHandler())
    def a() -> int:
        return 1

    repo = rs.CodeRepository(assets=[a], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    repo.materialize()

    latest = await storage.async_get_latest_materialization("a")
    assert latest is not None
    assert latest.event_type == "Materialization"

    missing = await storage.async_get_latest_materialization("nonexistent")
    assert missing is None


@pytest.mark.asyncio
async def test_async_dynamic_partitions():
    """Async dynamic partition operations."""
    storage = rs.Storage.memory()

    await storage.async_add_dynamic_partitions("colors", ["red", "blue"])
    keys = await storage.async_get_dynamic_partitions("colors")
    assert set(keys) == {"red", "blue"}

    assert await storage.async_has_dynamic_partition("colors", "red") is True
    assert await storage.async_has_dynamic_partition("colors", "green") is False

    await storage.async_delete_dynamic_partition("colors", "red")
    keys = await storage.async_get_dynamic_partitions("colors")
    assert keys == ["blue"]


@pytest.mark.asyncio
async def test_async_get_assets_by_filters():
    """Async asset filter methods work correctly."""
    storage = rs.Storage.memory()

    @rs.Asset(name="a", tags=["etl"], kinds="table", group="analytics")
    def a() -> int:
        return 1

    @rs.Asset(name="b", tags=["ml"], kinds="model", group="ml")
    def b() -> int:
        return 2

    repo = rs.CodeRepository(assets=[a, b])
    repo.resolve(storage=storage)

    by_tag = await storage.async_get_assets_by_tag("etl")
    assert len(by_tag) == 1
    assert by_tag[0].asset_key == "a"

    by_kind = await storage.async_get_assets_by_kind("model")
    assert len(by_kind) == 1
    assert by_kind[0].asset_key == "b"

    by_group = await storage.async_get_assets_by_group("analytics")
    assert len(by_group) == 1
    assert by_group[0].asset_key == "a"


@pytest.mark.parametrize(
    "status",
    ["Queued", "NotStarted", "Started", "Success", "Failure", "Canceled"],
)
def test_run_status_round_trip(status: str):
    """Run status spelling is symmetric: a value the API accepts on write
    is read back unchanged."""
    storage = rs.Storage.memory()
    run_id = f"run-{status.lower()}"
    storage._create_run(run_id, "job", status, 0)

    record = storage.get_run(run_id)
    assert record is not None
    assert record.status == status
