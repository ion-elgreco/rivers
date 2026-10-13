"""DuckDBResource: connections, extensions, secrets, lock retry, and DuckLake."""

import importlib
import json
import subprocess
import sys
import threading

import pytest

duckdb = pytest.importorskip("duckdb")

import cloudpickle  # noqa: E402

import rivers as rs  # noqa: E402
from rivers.integrations.duckdb import DuckDBResource, DuckLake  # noqa: E402
from rivers.integrations.duckdb import resource as resource_module  # noqa: E402
from rivers.integrations.duckdb._sql import (  # noqa: E402
    quote_ident,
    render_attach,
    render_secret,
)
from rivers.integrations.duckdb.resource import set_lake_options  # noqa: E402

from _polling import wait_for  # noqa: E402


def _lake(tmp_path, **kwargs) -> DuckDBResource:
    return DuckDBResource(
        database=DuckLake(
            metadata=str(tmp_path / "meta.ducklake"),
            data_path=str(tmp_path / "data") + "/",
            **kwargs,
        )
    )


def _hold_lock(path: str, seconds: float) -> subprocess.Popen:
    """A subprocess that opens ``path`` read-write for ``seconds``."""
    proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import duckdb, sys, time\n"
            "con = duckdb.connect(sys.argv[1])\n"
            "print('held', flush=True)\n"
            "time.sleep(float(sys.argv[2]))\n",
            path,
            str(seconds),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    assert proc.stdout.readline().strip() == "held"
    return proc


def test_get_connection_round_trip(tmp_path):
    db = DuckDBResource(database=str(tmp_path / "w.duckdb"))
    with db.get_connection() as con:
        con.execute(
            "CREATE TABLE t AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) v(id, name)"
        )
    with db.get_connection() as con:
        assert con.sql("FROM t ORDER BY id").fetchall() == [(1, "a"), (2, "b")]


def test_extensions_and_secrets(tmp_path):
    db = DuckDBResource(
        extensions=["delta"],
        secrets={
            "minio": {
                "type": "s3",
                "key_id": "key",
                "secret": "it's secret",
                "endpoint": "localhost:9000",
                "use_ssl": False,
                "scope": "s3://bucket",
            }
        },
    )
    with db.get_connection() as con:
        [(loaded,)] = con.sql(
            "SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'delta'"
        ).fetchall()
        secrets = con.sql(
            "SELECT name, type, provider, scope FROM duckdb_secrets()"
        ).fetchall()
    assert loaded is True
    assert secrets == [("minio", "s3", "config", ["s3://bucket"])]


def test_fields_from_env(monkeypatch, tmp_path):
    path = str(tmp_path / "env.duckdb")
    monkeypatch.setenv("DUCKDB_DATABASE", path)
    monkeypatch.setenv("DUCKDB_CONNECTION_CONFIG", '{"threads": 2}')
    db = DuckDBResource()
    assert db.database == path
    with db.get_connection() as con:
        assert con.sql("SELECT current_setting('threads')").fetchone() == (2,)
        assert con.sql("SELECT current_database()").fetchone() == ("env",)


def test_database_from_env_can_be_a_ducklake(monkeypatch, tmp_path):
    metadata = str(tmp_path / "meta.ducklake")
    monkeypatch.setenv("DUCKDB_DATABASE", json.dumps({"metadata": metadata}))
    db = DuckDBResource()
    assert db.database == DuckLake(metadata=metadata)
    assert db.ducklake == DuckLake(metadata=metadata)
    assert DuckDBResource(database="w.duckdb").ducklake is None


def test_connect_waits_for_another_process(tmp_path):
    path = str(tmp_path / "w.duckdb")
    holder = _hold_lock(path, 0.5)
    try:
        with DuckDBResource(database=path).get_connection() as con:
            assert con.sql("SELECT 42").fetchone() == (42,)
    finally:
        holder.wait()


def test_connect_gives_up_after_the_retries(monkeypatch, tmp_path):
    path = str(tmp_path / "w.duckdb")
    delays: list[float] = []
    monkeypatch.setattr(resource_module, "sleep", delays.append)
    holder = _hold_lock(path, 30)
    try:
        with pytest.raises(
            duckdb.IOException, match="Could not set lock|being used by another process"
        ):
            DuckDBResource(database=path).connect()
    finally:
        holder.kill()
        holder.wait()
    assert delays == [0.1 * 2**i for i in range(10)]


@pytest.mark.parametrize(
    ("message", "locked"),
    [
        (
            'IO Error: Could not set lock on file "w.duckdb": Conflicting lock is '
            "held in /usr/bin/python3.11 (PID 3533)",
            True,
        ),
        (
            'IO Error: Cannot open file "C:\\w.duckdb": The process cannot access the '
            "file because it is being used by another process.\r\n\nFile is already "
            "open in \nC:\\python.exe (PID 4160)",
            True,
        ),
        (
            'IO Error: Cannot open file "C:\\w.duckdb": Der Prozess kann nicht auf '
            "die Datei zugreifen.\r\n\nFile is already open in \nC:\\python.exe (PID 1)",
            True,
        ),
        ('IO Error: Cannot open file "w.duckdb": Permission denied', False),
    ],
)
def test_lock_errors_on_each_platform(message, locked):
    assert resource_module._is_lock_error(duckdb.IOException(message)) is locked


def test_shared_connection_lives_while_its_relations_do(tmp_path):
    path = str(tmp_path / "w.duckdb")
    db = DuckDBResource(database=path)
    with db.get_connection() as con:
        con.execute("CREATE TABLE t AS SELECT 1 AS i")

    a = db.shared_connection().sql("FROM t").set_alias("a")
    b = (
        DuckDBResource(database=path)
        .shared_connection()
        .sql("SELECT i + 1 AS j FROM t")
    )
    assert a.join(b.set_alias("b"), "a.i + 1 = b.j").fetchall() == [(1, 2)]
    other: list = []
    thread = threading.Thread(target=lambda: other.append(db.shared_connection()))
    thread.start()
    thread.join()
    with pytest.raises(duckdb.InvalidInputException, match="different connections"):
        a.join(other.pop().sql("FROM t"), "true")

    del a, b
    _hold_lock(path, 0).wait()


def test_ducklake_threads_share_one_catalog(tmp_path):
    lake = _lake(tmp_path)
    with lake.get_connection() as con:
        con.execute("CREATE TABLE t (i INTEGER)")
    # The first insert creates the table's data directory. DuckDB 1.5 on Windows
    # fails when two threads create it at once (it accepts EEXIST only on POSIX).
    (tmp_path / "data" / "main" / "t").mkdir(parents=True, exist_ok=True)
    errors: list[BaseException] = []

    def insert(i: int) -> None:
        try:
            with lake.get_connection() as con:
                con.execute("INSERT INTO t VALUES (?)", [i])
        except BaseException as e:
            errors.append(e)

    threads = [threading.Thread(target=insert, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    with lake.get_connection() as con:
        assert con.sql("SELECT i FROM t ORDER BY i").fetchall() == [
            (i,) for i in range(8)
        ]
        assert con.sql("SELECT current_database()").fetchone() == ("lake",)
    lake.teardown()


def test_ducklake_options_set_at_attach(tmp_path):
    options = {"expire_older_than": "7 days", "rewrite_delete_threshold": 0.5}
    lake = _lake(tmp_path, options=options)
    with lake.get_connection() as con:
        stored = con.sql(
            "SELECT option_name, value FROM lake.options() "
            "WHERE option_name IN ('expire_older_than', 'rewrite_delete_threshold')"
            "ORDER BY option_name"
        ).fetchall()
    lake.teardown()
    assert stored == [
        ("expire_older_than", "7 days"),
        ("rewrite_delete_threshold", "0.500000"),
    ]


class _Recorder:
    """Wraps a connection and records each SQL statement."""

    def __init__(self, con):
        self.con = con
        self.sql: list[str] = []

    def execute(self, sql, params=None):
        self.sql.append(sql)
        return self.con.execute(sql, params)


def test_options_set_only_when_they_differ(tmp_path):
    lake = _lake(tmp_path)
    con = _Recorder(lake.connect())
    con.con.execute("CREATE TABLE t (i INTEGER)")
    options = {"expire_older_than": "7 days", "per_thread_output": True}
    table_options = {"rewrite_delete_threshold": 0.25, "parquet_row_group_size": 1000}

    set_lake_options(con, "lake", options)
    set_lake_options(con, "lake", table_options, ("main", "t"))
    first = [s for s in con.sql if "set_option" in s]
    con.sql.clear()
    set_lake_options(con, "lake", options)
    set_lake_options(con, "lake", table_options, ("main", "t"))
    second = [s for s in con.sql if "set_option" in s]
    stored = con.con.sql(
        "SELECT option_name, value, scope, scope_entry FROM lake.options() "
        "WHERE option_name IN ('expire_older_than', 'per_thread_output', "
        "'rewrite_delete_threshold', 'parquet_row_group_size') ORDER BY option_name"
    ).fetchall()
    con.con.close()
    lake.teardown()

    assert len(first) == 4
    assert second == []
    assert stored == [
        ("expire_older_than", "7 days", "GLOBAL", None),
        ("parquet_row_group_size", "1000", "TABLE", "main.t"),
        ("per_thread_output", "true", "GLOBAL", None),
        ("rewrite_delete_threshold", "0.250000", "TABLE", "main.t"),
    ]


def test_equal_resources_share_the_root(tmp_path):
    lake = _lake(tmp_path, options={"expire_older_than": "1 day"})
    with lake.get_connection() as con:
        con.execute("CREATE TABLE t AS SELECT 1 AS i")

    copy = cloudpickle.loads(cloudpickle.dumps(lake))
    from_json = DuckDBResource.model_validate_json(lake.model_dump_json())
    assert copy == lake
    assert from_json == lake
    with copy.get_connection() as a, from_json.get_connection() as b:
        a.execute("INSERT INTO t VALUES (2)")
        assert b.sql("FROM t ORDER BY i").fetchall() == [(1,), (2,)]
    key = lake.model_dump_json()
    assert key in resource_module._roots

    copy.teardown()
    assert key not in resource_module._roots
    with lake.get_connection() as con:
        assert con.sql("FROM t ORDER BY i").fetchall() == [(1,), (2,)]
    lake.teardown()


def _read_after_teardown(lake: DuckDBResource) -> list[tuple]:
    """Read a relation made before teardown; it is gone when this returns.

    On Python 3.12 and older, DuckDB's replacement scan keeps the calling
    frame's locals until that frame returns, so the relation must not be a
    local of the test.
    """
    rel = lake.connect().sql("FROM t")
    lake.teardown()
    return rel.fetchall()


def test_relations_outlive_teardown(tmp_path):
    lake = _lake(tmp_path)
    with lake.get_connection() as con:
        con.execute("CREATE TABLE t AS SELECT 1 AS i")
    assert _read_after_teardown(lake) == [(1,)]

    attach = [
        sys.executable,
        "-c",
        "import duckdb, sys\n"
        "duckdb.connect().execute(f\"ATTACH 'ducklake:{sys.argv[1]}' AS lake\")",
        str(tmp_path / "meta.ducklake"),
    ]

    def closed() -> bool:
        proc = subprocess.run(attach, capture_output=True, text=True)
        locked = any(m in proc.stderr for m in resource_module._LOCK_MESSAGES)
        assert proc.returncode == 0 or locked, proc.stderr
        return proc.returncode == 0

    wait_for(closed, timeout=10, what="the catalog to close")


def test_render_secret_quotes_names_and_values():
    assert render_secret(
        "s3 main", {"TYPE": "s3", "SECRET": "it's", "USE_SSL": False, "PORT": 9000}
    ) == (
        'CREATE OR REPLACE SECRET "s3 main" '
        """("TYPE" 's3', "SECRET" 'it''s', "USE_SSL" false, "PORT" 9000)"""
    )


def test_secret_names_cannot_break_out_of_the_statement():
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT 1 AS i")

    with pytest.raises(duckdb.Error):
        con.execute(render_secret("s", {'TYPE" s3); DROP TABLE t; --': "s3"}))
    con.execute(render_secret('x" (TYPE s3); DROP TABLE t; --', {"TYPE": "s3"}))

    assert con.sql("FROM t").fetchall() == [(1,)]
    # DuckDB stores secret names in lower case
    assert con.sql("SELECT name FROM duckdb_secrets()").fetchall() == [
        ('x" (type s3); drop table t; --',)
    ]


def test_render_attach():
    lake = DuckLake(
        metadata="postgres:dbname=lake host=pg",
        data_path="s3://b/lake/",
        name='my "lake"',
        attach_options={"METADATA_SCHEMA": "meta", "READ_ONLY": True},
    )
    assert render_attach(lake) == (
        'ATTACH \'ducklake:postgres:dbname=lake host=pg\' AS "my ""lake""" '
        """("DATA_PATH" 's3://b/lake/', "METADATA_SCHEMA" 'meta', "READ_ONLY" true)"""
    )
    assert render_attach(DuckLake(metadata="m.ducklake")) == (
        "ATTACH 'ducklake:m.ducklake' AS \"lake\""
    )
    assert quote_ident('a"b') == '"a""b"'


def test_missing_duckdb_names_the_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "duckdb", None)
    for name in list(sys.modules):
        if name.startswith("rivers.integrations.duckdb"):
            monkeypatch.delitem(sys.modules, name)
    with pytest.raises(ImportError, match=r"pip install rivers\[duckdb\]"):
        importlib.import_module("rivers.integrations.duckdb")


def test_assets_use_the_resource(storage, executor_env, tmp_path):
    executor, io_factory = executor_env
    path = str(tmp_path / "w.duckdb")
    with duckdb.connect(path) as con:
        con.execute(
            "CREATE TABLE users AS SELECT * FROM (VALUES (1, 'ada'), (2, 'bo')) v(id, name)"
        )

    @rs.Asset(io_handler=io_factory())
    def names(warehouse: DuckDBResource) -> list[str]:
        with warehouse.get_connection() as con:
            return [
                r[0] for r in con.sql("SELECT name FROM users ORDER BY id").fetchall()
            ]

    @rs.Asset(io_handler=io_factory())
    def user_count(warehouse: DuckDBResource) -> int:
        with warehouse.get_connection() as con:
            return con.sql("SELECT count(*) FROM users").fetchone()[0]

    repo = rs.CodeRepository(
        assets=[names, user_count],
        resources={"warehouse": DuckDBResource(database=path)},
        default_executor=executor,
    )
    repo.resolve(storage=storage)
    repo.materialize()

    assert repo.load_node("names") == ["ada", "bo"]
    assert repo.load_node("user_count") == 2
