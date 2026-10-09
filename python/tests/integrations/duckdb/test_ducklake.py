"""DuckDBIOHandler on a DuckLake catalog in a DuckDB file."""

import json
import subprocess
import sys
import threading
from datetime import datetime

import pytest

duckdb = pytest.importorskip("duckdb")

import polars as pl  # noqa: E402
import pyarrow as pa  # noqa: E402

import rivers as rs  # noqa: E402
from rivers.integrations.duckdb import (  # noqa: E402
    DuckDBIOHandler,
    DuckDBResource,
    DuckLake,
)
from rivers.integrations.duckdb import io_handler as io_module  # noqa: E402
from rivers.integrations.duckdb import resource as resource_module  # noqa: E402

_TABLE = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
_REGIONS = rs.PartitionsDefinition.static_([f"r{i}" for i in range(6)])
_BY_REGION = {"duckdb/partition_expr": "region"}


def _resource(tmp_path, **kwargs) -> DuckDBResource:
    return DuckDBResource(
        ducklake=DuckLake(
            metadata=str(tmp_path / "meta.ducklake"),
            data_path=str(tmp_path / "data") + "/",
            attach_options={"DATA_INLINING_ROW_LIMIT": 0},
        ),
        **kwargs,
    )


@pytest.fixture
def lake(tmp_path):
    handler = DuckDBIOHandler(resource=_resource(tmp_path))
    yield handler
    handler.resource.teardown()


def _rows(handler, sql: str, params=None) -> list[tuple]:
    with handler.resource.get_connection() as con:
        return con.execute(sql, params).fetchall()


def _snapshots(handler) -> int:
    return _rows(handler, "SELECT count(*) FROM lake.snapshots()")[0][0]


def _region(handler, region: str, values: list[int], **meta) -> rs.OutputContext:
    ctx = rs.OutputContext(
        asset_name="t",
        asset_metadata={**_BY_REGION, **meta},
        partition=rs.PartitionContext(
            keys=[rs.PartitionKey.single(region)], definition=_REGIONS
        ),
    )
    handler.handle_output(
        ctx, pa.table({"region": [region] * len(values), "v": values})
    )
    return ctx


def _load(handler, name, type_hint, **kwargs):
    return handler.load_input(
        rs.InputContext(
            asset_name=name, downstream_asset="x", type_hint=type_hint, **kwargs
        )
    )


@pytest.mark.parametrize(
    "make",
    [
        pytest.param(lambda: _TABLE, id="pyarrow"),
        pytest.param(lambda: pl.DataFrame(_TABLE), id="polars"),
        pytest.param(
            lambda: duckdb.sql(
                "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) v(id, name)"
            ),
            id="relation",
        ),
    ],
)
def test_round_trip(lake, make):
    lake.handle_output(rs.OutputContext(asset_name="t"), make())

    assert _load(lake, "t", pa.Table).sort_by("id").to_pylist() == _TABLE.to_pylist()
    assert _load(lake, "t", pl.DataFrame).sort("id").rows() == [
        (1, "a"),
        (2, "b"),
        (3, "c"),
    ]
    rel = _load(lake, "t", duckdb.DuckDBPyRelation)
    assert rel.order("id").fetchall() == [(1, "a"), (2, "b"), (3, "c")]
    assert rel.query("r", "SELECT current_database() FROM r LIMIT 1").fetchall() == [
        ("lake",)
    ]


def test_relation_in_relation_out(lake):
    lake.handle_output(rs.OutputContext(asset_name="users"), _TABLE)
    users = _load(lake, "users", duckdb.DuckDBPyRelation)
    lake.handle_output(
        rs.OutputContext(asset_name="initials"), users.select("upper(name) AS initial")
    )
    assert _rows(lake, "FROM initials ORDER BY initial") == [("A",), ("B",), ("C",)]


def test_partitioned_table_and_one_snapshot_per_overwrite(lake):
    _region(lake, "r0", [1, 2])
    _region(lake, "r1", [3])
    before = _snapshots(lake)
    _region(lake, "r0", [4])

    assert _snapshots(lake) == before + 1
    assert _rows(lake, "FROM t ORDER BY v") == [("r1", 3), ("r0", 4)]
    assert _rows(
        lake,
        "SELECT col.column_name FROM __ducklake_metadata_lake.ducklake_partition_column pc "
        "JOIN __ducklake_metadata_lake.ducklake_column col "
        "ON pc.column_id = col.column_id AND col.end_snapshot IS NULL",
    ) == [("region",)]
    files = _rows(lake, "SELECT data_file FROM ducklake_list_files('lake', 't')")
    assert sorted(f.split("/")[-2] for (f,) in files) == ["region=r0", "region=r1"]


def test_existing_table_is_not_repartitioned(lake):
    lake.handle_output(
        rs.OutputContext(asset_name="t"), pa.table({"region": ["r0"], "v": [0]})
    )
    _region(lake, "r1", [1])
    assert _rows(
        lake, "SELECT count(*) FROM __ducklake_metadata_lake.ducklake_partition_column"
    ) == [(0,)]
    assert _rows(lake, "FROM t ORDER BY v") == [("r0", 0), ("r1", 1)]


def test_data_version_is_the_snapshot_id(lake):
    ctx = rs.OutputContext(asset_name="t")
    lake.handle_output(ctx, _TABLE)

    [(snapshot,)] = _rows(lake, "SELECT max(snapshot_id) FROM lake.snapshots()")
    assert ctx.output_metadata["ducklake/snapshot_id"].raw_value() == snapshot
    assert ctx.drain_data_version() == str(snapshot)


def test_downstream_goes_stale_after_an_upstream_write(storage, lake):
    @rs.Asset(io_handler=lake)
    def events() -> pa.Table:
        return _TABLE

    @rs.Asset(io_handler=lake)
    def counts(events: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
        return events.aggregate("count(*) AS n")

    repo = rs.CodeRepository(
        assets=[events, counts], default_executor=rs.Executor.in_process()
    )
    repo.resolve(storage=storage)
    repo.materialize()

    [(snapshot,)] = _rows(
        lake,
        "SELECT max(snapshot_id) FROM lake.snapshots() "
        "WHERE list_contains(changes['tables_created'], 'main.events')",
    )
    assert storage.get_asset_record("events").last_data_version == str(snapshot)
    assert storage.compute_staleness()["counts"][0] == "UpToDate"
    assert _rows(lake, "FROM counts") == [(3,)]

    repo.materialize(selection=["events"])
    assert storage.compute_staleness()["counts"][0] == "Stale"


def test_version_reads_old_data(lake):
    first = rs.OutputContext(asset_name="t")
    lake.handle_output(first, _TABLE)
    lake.handle_output(rs.OutputContext(asset_name="t"), pa.table({"other": [True]}))
    version = first.output_metadata["ducklake/snapshot_id"].raw_value()

    old = _load(lake, "t", pa.Table, asset_metadata={"duckdb/version": str(version)})
    assert old.sort_by("id").equals(_TABLE)
    assert _load(lake, "t", pa.Table).to_pylist() == [{"other": True}]


def test_table_options(lake):
    options = {"rewrite_delete_threshold": 0.5, "parquet_row_group_size": 2048}
    _region(lake, "r0", [1], **{"ducklake/options": json.dumps(options)})

    assert _rows(
        lake,
        "SELECT option_name, value FROM lake.options() "
        "WHERE scope = 'TABLE' AND scope_entry = 'main.t' ORDER BY option_name",
    ) == [
        ("parquet_row_group_size", "2048"),
        ("rewrite_delete_threshold", "0.500000"),
    ]


def _conflicting_writes(handler, n: int, monkeypatch) -> list[BaseException]:
    """``n`` threads overwrite one partition each and commit at the same moment."""
    barrier = threading.Barrier(n, timeout=30)
    first_attempt = threading.local()
    real_write = io_module._write

    def write_then_wait(*args):
        real_write(*args)
        if not getattr(first_attempt, "done", False):
            first_attempt.done = True
            barrier.wait()

    monkeypatch.setattr(io_module, "_write", write_then_wait)
    errors: list[BaseException] = []

    def write(i: int) -> None:
        try:
            _region(handler, f"r{i}", [i] * 20)
        except BaseException as e:
            errors.append(e)

    threads = [threading.Thread(target=write, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return errors


def test_conflicting_partition_writes_are_retried(lake, monkeypatch):
    _region(lake, "r0", [0])
    delays: list[float] = []
    monkeypatch.setattr(io_module, "sleep", delays.append)

    assert _conflicting_writes(lake, 4, monkeypatch) == []
    assert delays and delays[0] == 0.1
    assert _rows(
        lake, "SELECT region, count(*), min(v) FROM t GROUP BY ALL ORDER BY 1"
    ) == [(f"r{i}", 20, i) for i in range(4)]


def test_no_retries_surface_the_conflict(tmp_path, monkeypatch):
    handler = DuckDBIOHandler(
        resource=_resource(tmp_path, connection_config={"ducklake_max_retry_count": 0})
    )
    _region(handler, "r0", [0])

    errors = _conflicting_writes(handler, 2, monkeypatch)
    handler.resource.teardown()
    assert len(errors) == 1
    assert isinstance(errors[0], duckdb.TransactionException)
    assert "conflict" in str(errors[0]).lower()


def test_async_backfill_runs_write_partitions_of_one_table(storage, lake):
    @rs.Asset(io_handler=lake, partitions_def=_REGIONS, metadata=_BY_REGION)
    async def t(context: rs.AssetExecutionContext) -> pa.Table:
        region = context.partition_key
        return pa.table({"region": [region] * 20, "v": list(range(20))})

    repo = rs.CodeRepository(assets=[t], default_executor=rs.Executor.in_process())
    repo.resolve(storage=storage)
    result = repo.backfill(
        selection=["t"],
        partition_keys=[rs.PartitionKey.single(f"r{i}") for i in range(6)],
        max_concurrency=6,
    )

    assert result.completed == 6
    assert _rows(lake, "SELECT region, count(*) FROM t GROUP BY ALL ORDER BY 1") == [
        (f"r{i}", 20) for i in range(6)
    ]


def test_second_process_gets_the_one_process_error(tmp_path, monkeypatch):
    resource = _resource(tmp_path)
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import duckdb, sys, time\n"
            "con = duckdb.connect()\n"
            "con.execute(sys.argv[1])\n"
            "print('held', flush=True)\n"
            "time.sleep(30)\n",
            f"ATTACH 'ducklake:{tmp_path / 'meta.ducklake'}' AS lake "
            f"(DATA_PATH '{tmp_path / 'data'}/')",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert holder.stdout is not None
    assert holder.stdout.readline().strip() == "held"
    monkeypatch.setattr(resource_module, "sleep", lambda s: None)
    try:
        with pytest.raises(duckdb.IOException, match="allows one process") as err:
            resource.connect()
    finally:
        holder.kill()
        holder.wait()
    assert "PostgreSQL" in str(err.value)
    assert resource.model_dump_json() not in resource_module._roots


def test_hourly_partitions_on_a_timestamp_column(lake):
    hourly = rs.PartitionsDefinition.hourly(start=datetime(2024, 1, 1))
    meta = {"duckdb/partition_expr": "ts"}

    def write(key, minute):
        lake.handle_output(
            rs.OutputContext(
                asset_name="t",
                asset_metadata=meta,
                partition=rs.PartitionContext(
                    keys=[rs.PartitionKey.single(key)], definition=hourly
                ),
            ),
            duckdb.sql(
                f"SELECT TIMESTAMP '{key.replace('T', ' ')}' + INTERVAL {minute} MINUTE AS ts"
            ),
        )

    write("2024-01-01T05:00", 10)
    write("2024-01-01T06:00", 20)
    write("2024-01-01T05:00", 30)
    assert _rows(lake, "SELECT strftime(ts, '%H:%M') FROM t ORDER BY ts") == [
        ("05:30",),
        ("06:20",),
    ]
