"""DuckDBAsset and DuckLakeAsset actions: delete, optimize, vacuum."""

import json

import pytest

duckdb = pytest.importorskip("duckdb")

import pyarrow as pa  # noqa: E402

import rivers as rs  # noqa: E402
from rivers.integrations.duckdb import (  # noqa: E402
    DuckDBAsset,
    DuckDBIOHandler,
    DuckDBResource,
    DuckLake,
    DuckLakeAsset,
)

_DAYS = rs.PartitionsDefinition.static_(["a", "b"])


def _file_handler(tmp_path) -> DuckDBIOHandler:
    return DuckDBIOHandler(resource=DuckDBResource(database=str(tmp_path / "w.duckdb")))


@pytest.fixture
def lake_handler(tmp_path):
    handler = DuckDBIOHandler(
        resource=DuckDBResource(
            ducklake=DuckLake(
                metadata=str(tmp_path / "meta.ducklake"),
                data_path=str(tmp_path / "data") + "/",
                attach_options={"DATA_INLINING_ROW_LIMIT": 0},
            )
        ),
        mode="append",
    )
    yield handler
    handler.resource.teardown()


def _rows(handler, sql: str) -> list[tuple]:
    with handler.resource.get_connection() as con:
        return con.sql(sql).fetchall()


def _repo(storage, *assets) -> rs.CodeRepository:
    repo = rs.CodeRepository(
        assets=list(assets), default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    return repo


def _deletions(storage, name: str) -> int:
    return sum(e.event_type == "Deletion" for e in storage.get_events_for_asset(name))


def _partitioned_events(handler, base=DuckDBAsset, meta=None):
    class Events(base):
        name = "events"
        io_handler = handler
        partitions_def = _DAYS
        metadata = {"duckdb/partition_expr": "day"} if meta is None else meta

        @classmethod
        def materialize(cls, context: rs.AssetExecutionContext) -> pa.Table:
            return pa.table({"day": [context.partition_key] * 2, "v": [1, 2]})

    return Events


@pytest.mark.parametrize("base", [DuckDBAsset, DuckLakeAsset])
def test_keyed_delete_removes_one_partition(tmp_path, storage, lake_handler, base):
    handler = _file_handler(tmp_path) if base is DuckDBAsset else lake_handler
    repo = _repo(storage, _partitioned_events(handler, base))
    for day in ("a", "b"):
        repo.materialize(partition_key=rs.PartitionKey.single(day))

    assert repo.run_action("delete", partition_key=rs.PartitionKey.single("a")).success

    assert _rows(handler, "SELECT day, v FROM events ORDER BY v") == [
        ("b", 1),
        ("b", 2),
    ]
    assert _deletions(storage, "events") == 1


def test_keyless_delete_removes_every_row(tmp_path, storage):
    handler = _file_handler(tmp_path)

    class Users(DuckDBAsset):
        name = "users"
        io_handler = handler

        @classmethod
        def materialize(cls) -> pa.Table:
            return pa.table({"id": [1, 2]})

    repo = _repo(storage, Users)
    repo.materialize()
    assert repo.run_action("delete").success

    assert _rows(handler, "SELECT count(*) FROM users") == [(0,)]
    assert _deletions(storage, "users") == 1
    assert storage.get_asset_record("users").last_run_id is None


def test_delete_on_a_missing_table_clears_state(tmp_path, storage):
    handler = _file_handler(tmp_path)

    class Users(DuckDBAsset):
        name = "users"
        io_handler = handler

        @classmethod
        def materialize(cls) -> pa.Table:
            return pa.table({"id": [1]})

    repo = _repo(storage, Users)
    repo.materialize()
    with handler.resource.get_connection() as con:
        con.execute("DROP TABLE users")

    assert repo.run_action("delete").success
    assert _deletions(storage, "users") == 1


def test_keyed_delete_without_partition_expr_names_the_key(tmp_path, storage):
    handler = _file_handler(tmp_path)
    events = _partitioned_events(handler, meta={})

    ctx = rs.ActionContext(
        asset_name="events",
        action="delete",
        io_handler=handler,
        asset_metadata={},
        partition=rs.PartitionContext(
            keys=[rs.PartitionKey.single("a")], definition=_DAYS
        ),
    )
    with pytest.raises(ValueError, match="'duckdb/partition_expr'"):
        events.delete(ctx)


def _deleted_fraction_table(handler, storage, options: dict | None):
    """A 100-row table in one file, then 20 rows deleted outside rivers."""
    table_meta = {"ducklake/options": json.dumps(options)} if options else {}

    class Scores(DuckLakeAsset):
        name = "scores"
        io_handler = handler
        metadata = table_meta

        @classmethod
        def materialize(cls) -> pa.Table:
            return pa.table({"v": list(range(100))})

    repo = _repo(storage, Scores)
    repo.materialize()
    with handler.resource.get_connection() as con:
        con.execute("DELETE FROM scores WHERE v < 20")
    return repo


def _file_counts(handler, table: str) -> tuple[int, int]:
    [(files, delete_files)] = _rows(
        handler,
        "SELECT file_count, delete_file_count FROM lake.table_info() "
        f"WHERE table_name = '{table}'",
    )
    return files, delete_files


def test_optimize_rewrites_files_past_the_table_threshold(storage, lake_handler):
    repo = _deleted_fraction_table(
        lake_handler, storage, {"rewrite_delete_threshold": 0.1}
    )
    assert _file_counts(lake_handler, "scores") == (1, 1)

    assert repo.run_action("optimize").success

    assert _file_counts(lake_handler, "scores") == (1, 0)
    assert _rows(lake_handler, "SELECT count(*), min(v) FROM scores") == [(80, 20)]


def test_optimize_keeps_files_under_the_default_threshold(storage, lake_handler):
    repo = _deleted_fraction_table(lake_handler, storage, None)

    assert repo.run_action("optimize").success

    assert _file_counts(lake_handler, "scores") == (1, 1)
    assert _rows(lake_handler, "SELECT count(*) FROM scores") == [(80,)]


def test_optimize_merges_small_files(storage, lake_handler):
    class Logs(DuckLakeAsset):
        name = "logs"
        io_handler = lake_handler

        @classmethod
        def materialize(cls) -> pa.Table:
            return pa.table({"v": [1, 2, 3]})

    repo = _repo(storage, Logs)
    for _ in range(5):
        repo.materialize()
    assert _file_counts(lake_handler, "logs") == (5, 0)

    assert repo.run_action("optimize").success

    assert _file_counts(lake_handler, "logs") == (1, 0)
    assert _rows(lake_handler, "SELECT count(*), sum(v) FROM logs") == [(15, 30)]
    mats = [
        e
        for e in storage.get_events_for_asset("logs")
        if e.event_type == "Materialization"
    ]
    assert len(mats) == 5


def _overwritten_three_times(tmp_path, storage, options: dict):
    handler = DuckDBIOHandler(
        resource=DuckDBResource(
            ducklake=DuckLake(
                metadata=str(tmp_path / "meta.ducklake"),
                data_path=str(tmp_path / "data") + "/",
                attach_options={"DATA_INLINING_ROW_LIMIT": 0},
                options=options,
            )
        )
    )

    class Users(DuckLakeAsset):
        name = "users"
        io_handler = handler

        @classmethod
        def materialize(cls) -> pa.Table:
            return pa.table({"id": [1, 2, 3]})

    repo = _repo(storage, Users)
    for _ in range(3):
        repo.materialize()
    return handler, repo


def _parquet_files(tmp_path) -> int:
    return len(list((tmp_path / "data").rglob("*.parquet")))


def _snapshots(handler) -> int:
    return _rows(handler, "SELECT count(*) FROM lake.snapshots()")[0][0]


def test_vacuum_without_options_changes_nothing(tmp_path, storage):
    handler, repo = _overwritten_three_times(tmp_path, storage, {})
    snapshots, files = _snapshots(handler), _parquet_files(tmp_path)

    assert repo.run_action("vacuum").success

    assert (_snapshots(handler), _parquet_files(tmp_path)) == (snapshots, files)
    handler.resource.teardown()


def test_vacuum_expires_snapshots_and_deletes_files(tmp_path, storage):
    handler, repo = _overwritten_three_times(
        tmp_path,
        storage,
        {"expire_older_than": "0 seconds", "delete_older_than": "0 seconds"},
    )
    assert _parquet_files(tmp_path) == 3

    assert repo.run_action("vacuum").success

    assert _snapshots(handler) == 1
    assert _parquet_files(tmp_path) == 1
    assert _rows(handler, "FROM users ORDER BY id") == [(1,), (2,), (3,)]
    handler.resource.teardown()


@pytest.mark.parametrize("verb", ["optimize", "vacuum"])
def test_ducklake_verbs_need_a_ducklake_resource(tmp_path, verb):
    ctx = rs.ActionContext(
        asset_name="users", action=verb, io_handler=_file_handler(tmp_path)
    )
    with pytest.raises(TypeError, match="need a DuckDBResource with ducklake"):
        getattr(DuckLakeAsset, verb)(ctx)


def test_verbs_need_a_duckdb_handler(tmp_path):
    ctx = rs.ActionContext(
        asset_name="users", action="delete", io_handler=rs.InMemoryIOHandler()
    )
    with pytest.raises(TypeError, match="DuckDBAsset actions need a DuckDBIOHandler"):
        DuckDBAsset.delete(ctx)
