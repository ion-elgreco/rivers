"""Sling integration: replications from CSV files into SQLite, run by the sling binary.

SQLite with ``use_bulk: false`` and CSV files need nothing beyond the sling
binary, so no other tool is downloaded.
"""

import importlib
import json
import os
import sqlite3
import sys
from contextlib import closing
from datetime import date
from pathlib import Path

import pytest

pytest.importorskip("rivers.integrations.sling", exc_type=ImportError)
if not os.environ.get("SLING_BINARY"):
    pytest.skip("no sling binary (see conftest.py)", allow_module_level=True)

from pydantic import SecretStr, ValidationError  # noqa: E402

import rivers as rs  # noqa: E402
from rivers.integrations.sling import (  # noqa: E402
    SlingConnection,
    SlingReplication,
    SlingResource,
    SlingStream,
)

from _helpers import make_repo  # noqa: E402

ORDERS = [(1, "apple", 10.5), (2, "pear", 20.0), (3, "plum", 30.25)]
USERS = [(1, "a@x.io"), (2, "b@x.io")]


@pytest.fixture
def src(tmp_path) -> Path:
    src = tmp_path / "src"
    src.mkdir()
    (src / "orders.csv").write_text(
        "id,item,amount\n1,apple,10.5\n2,pear,20\n3,plum,30.25\n"
    )
    (src / "users.csv").write_text("id,email\n1,a@x.io\n2,b@x.io\n")
    return src


def _uri(path: Path) -> str:
    """The ``file://`` URI sling reads on every OS (``file://C:/...`` on Windows)."""
    return f"file://{path.as_posix()}"


def _replication(src: Path, *names: str, schema: str = "main") -> dict:
    return {
        "source": "FILES",
        "target": "WAREHOUSE",
        "defaults": {"mode": "full-refresh", "target_options": {"use_bulk": False}},
        "streams": {
            _uri(src / f"{name}.csv"): {"object": f"{schema}.{name}"} for name in names
        },
    }


def _resource(tmp_path: Path) -> SlingResource:
    return SlingResource(
        connections=[
            SlingConnection(name="FILES", type="file"),
            SlingConnection(
                name="WAREHOUSE",
                type="sqlite",
                instance=(tmp_path / "warehouse.db").as_posix(),
            ),
        ]
    )


def _rows(tmp_path: Path, sql: str) -> list[tuple]:
    with closing(sqlite3.connect(tmp_path / "warehouse.db")) as con:
        return con.execute(sql).fetchall()


def _tables(tmp_path: Path) -> set[str]:
    return {
        name
        for (name,) in _rows(
            tmp_path, "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }


def _events(storage, asset: str, event_type: str) -> list:
    return [
        e for e in storage.get_events_for_asset(asset) if e.event_type == event_type
    ]


def _metadata(event) -> dict[str, tuple[str, object]]:
    """``{key: (kind, value)}`` from a stored event's ``(key, '{"Kind": {"value": v}}')`` pairs."""
    return {
        key: (kind, data["value"])
        for key, value in event.metadata
        for kind, data in json.loads(value).items()
    }


def _values(event) -> dict[str, object]:
    return {key: value for key, (_, value) in _metadata(event).items()}


def _repo(
    tmp_path,
    storage,
    replication,
    *assets,
    executor=None,
    resource=None,
    io_handler=None,
) -> rs.CodeRepository:
    """The replication as the multi-asset ``raw``, next to an independent asset.

    Two steps in one level send both to the parallel executor's workers.
    """

    @replication.assets(name="raw", io_handler=io_handler)
    def raw(context: rs.AssetExecutionContext, sling: SlingResource):
        (tmp_path / "raw.pid").write_text(str(os.getpid()))
        yield from sling.replicate(context, replication)

    @rs.Asset
    def bystander() -> rs.Materialization:
        return rs.Materialization()

    return make_repo(
        [raw, bystander, *assets],
        storage,
        executor=executor,
        resources={"sling": resource or _resource(tmp_path)},
    )


def test_replicates_each_stream_into_its_output(tmp_path, src, storage, executor_env):
    replication = SlingReplication(_replication(src, "orders", "users"))

    @rs.Asset(deps=[rs.AssetDef.dep("main.orders")], io_handler=executor_env[1]())
    def order_total() -> float:
        [(total,)] = _rows(tmp_path, "SELECT sum(amount) FROM orders")
        return total

    repo = _repo(tmp_path, storage, replication, order_total, executor=executor_env[0])

    result = repo.materialize()

    assert result.success
    assert _rows(tmp_path, "SELECT id, item, amount FROM orders ORDER BY id") == ORDERS
    assert _rows(tmp_path, "SELECT id, email FROM users ORDER BY id") == USERS
    assert repo.load_node("order_total") == 60.75

    [orders] = _events(storage, "main.orders", "Materialization")
    meta = _metadata(orders)
    kind, duration = meta.pop("sling/duration")
    assert kind == "Duration" and duration >= 0
    assert meta == {
        "sling/stream": ("Text", _uri(src / "orders.csv")),
        "sling/object": ("Text", "main.orders"),
        "sling/rows_written": ("Int", 3),
    }
    [users] = _events(storage, "main.users", "Materialization")
    assert _values(users)["sling/rows_written"] == 2

    in_worker = int((tmp_path / "raw.pid").read_text()) != os.getpid()
    assert in_worker == isinstance(executor_env[0], rs.Executor.Parallel)


def test_selection_runs_only_the_selected_streams(tmp_path, src, storage, executor_env):
    replication = SlingReplication(_replication(src, "orders", "users"))
    repo = _repo(tmp_path, storage, replication, executor=executor_env[0])

    # two steps, so the parallel executor runs them in its workers
    result = repo.materialize(selection=["main.users", "bystander"])

    assert result.success
    assert _tables(tmp_path) == {"users"}
    assert _rows(tmp_path, "SELECT id, email FROM users ORDER BY id") == USERS
    assert not _events(storage, "main.orders", "Materialization")
    [users] = _events(storage, "main.users", "Materialization")
    assert _values(users)["sling/rows_written"] == 2


def test_failed_stream_fails_only_its_output(tmp_path, src, storage, executor_env):
    replication = SlingReplication(_replication(src, "orders", "missing", "users"))
    repo = _repo(tmp_path, storage, replication, executor=executor_env[0])

    result = repo.materialize(raise_on_error=False)

    assert not result.success
    # sling goes on with the streams after the failed one
    assert _rows(tmp_path, "SELECT id, item, amount FROM orders ORDER BY id") == ORDERS
    assert _rows(tmp_path, "SELECT id, email FROM users ORDER BY id") == USERS
    [failure] = _events(storage, "main.missing", "StepFailure")
    error = dict(failure.metadata)["error"]
    assert error.startswith("SlingError: sling exited with code 1\n")
    assert f"--- {_uri(src / 'missing.csv')} ---" in error

    for asset, rows in (("main.orders", 3), ("main.users", 2)):
        [materialization] = _events(storage, asset, "Materialization")
        assert _values(materialization)["sling/rows_written"] == rows
        assert _events(storage, asset, "StepSuccess")
        assert not _events(storage, asset, "StepFailure")


def test_asset_def_fn_names_outputs_and_links_deps(tmp_path, src, storage):
    names = {
        _uri(src / "orders.csv"): "shop_orders",
        _uri(src / "users.csv"): "shop_users",
    }
    replication = SlingReplication(
        _replication(src, "orders", "users"),
        asset_def_fn=lambda stream: stream.output_def(
            name=names[stream.name], group="shop", deps=[rs.AssetDef.dep("exports")]
        ),
    )

    @rs.Asset
    def exports() -> None:
        return None

    defs = {d.name: d for d in replication.output_defs}
    assert list(defs) == ["shop_orders", "shop_users"]
    assert defs["shop_orders"].group == "shop"
    assert defs["shop_orders"].kinds == ["sling"]
    assert [dep.name for dep in defs["shop_orders"].deps] == ["exports"]
    assert defs["shop_orders"].metadata == {"sling/stream": _uri(src / "orders.csv")}

    repo = _repo(tmp_path, storage, replication, exports)
    result = repo.materialize(selection=["exports", "shop_orders"])

    assert result.success
    assert _tables(tmp_path) == {"orders"}
    [orders] = _events(storage, "shop_orders", "Materialization")
    assert _values(orders)["sling/object"] == "main.orders"


def _duckdb_table_def(stream: SlingStream) -> rs.AssetDef:
    """The Sling guide's ``asset_def`` for ``DuckDBIOHandler``."""
    metadata = stream.output_def().metadata
    schema, table = stream.config["object"].split(".")
    return stream.output_def(
        name=f"{schema}_{table}",
        metadata=metadata | {"duckdb/schema": schema, "duckdb/table": table},
    )


def test_downstream_reads_the_table_with_an_io_handler(
    tmp_path, src, storage, executor_env
):
    duckdb = pytest.importorskip("duckdb")
    pa = pytest.importorskip("pyarrow")
    from rivers.integrations.duckdb import DuckDBIOHandler, DuckDBResource

    replication = SlingReplication(
        _replication(src, "orders", "users", schema="raw"),
        asset_def_fn=_duckdb_table_def,
    )

    @rs.Asset(io_handler=executor_env[1]())
    def loaded(raw_orders: pa.Table, raw_users: pa.Table) -> dict:
        return {
            name: [tuple(row.values()) for row in table.sort_by("id").to_pylist()]
            for name, table in (("orders", raw_orders), ("users", raw_users))
        }

    assert {d.name: d.metadata for d in replication.output_defs} == {
        "raw_orders": {
            "sling/stream": _uri(src / "orders.csv"),
            "duckdb/schema": "raw",
            "duckdb/table": "orders",
        },
        "raw_users": {
            "sling/stream": _uri(src / "users.csv"),
            "duckdb/schema": "raw",
            "duckdb/table": "users",
        },
    }

    # the tables that sling writes for the replication
    warehouse = str(tmp_path / "warehouse.duckdb")
    with duckdb.connect(warehouse) as con:
        con.execute("CREATE SCHEMA raw")
        con.execute("CREATE TABLE raw.orders (id INT, item TEXT, amount DOUBLE)")
        con.execute("CREATE TABLE raw.users (id INT, email TEXT)")
        con.executemany("INSERT INTO raw.orders VALUES (?, ?, ?)", ORDERS)
        con.executemany("INSERT INTO raw.users VALUES (?, ?)", USERS)

    repo = _repo(
        tmp_path,
        storage,
        replication,
        loaded,
        executor=executor_env[0],
        io_handler=DuckDBIOHandler(resource=DuckDBResource(database=warehouse)),
    )

    # sling does not run; two steps, so the parallel executor runs them in its workers
    assert repo.materialize(selection=["loaded", "bystander"]).success
    assert repo.load_node("loaded") == {"orders": ORDERS, "users": USERS}


def test_wildcard_stream_is_one_output(tmp_path, src, storage):
    streams = {_uri(src / "*.csv"): {"object": "main.{stream_file_name}"}}
    replication = SlingReplication(
        _replication(src) | {"streams": streams},
        asset_def_fn=lambda stream: stream.output_def(name="csv_files"),
    )
    repo = _repo(tmp_path, storage, replication)

    assert repo.materialize().success
    # sling 1.4 keeps the extension in {stream_file_name}: orders_csv
    orders, users = sorted(_tables(tmp_path))
    assert orders.startswith("orders") and users.startswith("users")
    assert (
        _rows(tmp_path, f"SELECT id, item, amount FROM {orders} ORDER BY id") == ORDERS
    )
    assert _rows(tmp_path, f"SELECT id, email FROM {users} ORDER BY id") == USERS
    [files] = _events(storage, "csv_files", "Materialization")
    assert _metadata(files) == {"sling/stream": ("Text", _uri(src / "*.csv"))}


def test_replication_from_yaml_file(tmp_path, src, storage):
    path = tmp_path / "replication.yaml"
    path.write_text(
        f"""
source: FILES
target: WAREHOUSE
env:
  RUN_DATE: 2026-10-11
defaults:
  mode: full-refresh
  target_options:
    use_bulk: false
streams:
  {_uri(src / "orders.csv")}:
    object: main.orders
  {_uri(src / "users.csv")}:
    object: main.users
    disabled: true
"""
    )
    replication = SlingReplication(path)

    assert [d.name for d in replication.output_defs] == ["main.orders"]
    assert replication.config["env"] == {"RUN_DATE": date(2026, 10, 11)}

    repo = _repo(tmp_path, storage, replication)
    assert repo.materialize().success
    assert _tables(tmp_path) == {"orders"}


def test_code_version_follows_the_stream_config(src):
    def versions(config: dict) -> dict:
        return {d.name: d.code_version for d in SlingReplication(config).output_defs}

    config = _replication(src, "orders", "users")
    before = versions(config)
    config["streams"][_uri(src / "users.csv")]["mode"] = "truncate"
    after = versions(config)

    assert before["main.orders"] == after["main.orders"]
    assert before["main.users"] != after["main.users"]
    assert (
        SlingReplication(config, asset_def_fn=lambda s: s.output_def(code_version="v1"))
        .output_defs[0]
        .code_version
        == "v1"
    )


@pytest.mark.parametrize("form", ["env", "secret"])
def test_secrets_reach_sling_and_are_masked(
    tmp_path, src, storage, monkeypatch, caplog, form
):
    db = (tmp_path / "warehouse.db").as_posix()
    monkeypatch.setenv("RIVERS_TEST_SLING_DB", db)
    monkeypatch.setenv("RIVERS_TEST_SLING_SECRET", src.as_posix())
    if form == "env":
        secret, instance = "${RIVERS_TEST_SLING_SECRET}", "${RIVERS_TEST_SLING_DB}"
    else:
        secret, instance = SecretStr(src.as_posix()), SecretStr(db)
    # `note` is a property sling ignores, so the secret shows in sling's log lines
    resource = SlingResource(
        connections=[
            SlingConnection(name="FILES", type="file", note=secret),
            SlingConnection(name="WAREHOUSE", type="sqlite", instance=instance),
        ]
    )
    replication = SlingReplication(_replication(src, "orders", "missing"))
    repo = _repo(tmp_path, storage, replication, resource=resource)

    with caplog.at_level("INFO", logger="code-repo.assets.raw"):
        repo.materialize(raise_on_error=False)

    assert _rows(tmp_path, "SELECT id, item, amount FROM orders ORDER BY id") == ORDERS
    assert src.as_posix() not in repr(resource)

    lines = [r.getMessage() for r in caplog.records if r.name == "code-repo.assets.raw"]
    assert "[1 / 2] running stream file://***/orders.csv" in lines
    assert not [line for line in lines if src.as_posix() in line]
    [failure] = _events(storage, "main.missing", "StepFailure")
    error = dict(failure.metadata)["error"]
    assert "--- file://***/missing.csv ---" in error
    assert src.as_posix() not in error


def test_url_and_password_are_secrets():
    connection = SlingConnection(
        name="PG", url="postgresql://etl:pw@db/app", password="pw"
    )

    assert connection.url == SecretStr("postgresql://etl:pw@db/app")
    assert connection.password == SecretStr("pw")
    assert "pw" not in repr(connection)


def test_unset_env_reference_fails_before_sling_runs(tmp_path, src, monkeypatch):
    monkeypatch.delenv("RIVERS_TEST_SLING_UNSET", raising=False)
    resource = SlingResource(
        connections=[
            SlingConnection(
                name="DB", type="sqlite", instance="${RIVERS_TEST_SLING_UNSET}"
            )
        ]
    )
    replication = SlingReplication(_replication(src, "orders"))
    context = rs.AssetExecutionContext(
        "raw", is_multi_asset=True, output_selection=["main.orders"]
    )

    with pytest.raises(
        ValueError,
        match="'DB': environment variable RIVERS_TEST_SLING_UNSET is not set",
    ):
        next(resource.replicate(context, replication))


def test_secret_property_reaches_sling(tmp_path, src, storage, executor_env):
    db = SecretStr((tmp_path / "warehouse.db").as_posix())
    resource = SlingResource(
        connections=[
            SlingConnection(name="FILES", type="file"),
            SlingConnection(name="WAREHOUSE", type="sqlite", instance=db),
        ]
    )
    replication = SlingReplication(_replication(src, "orders"))
    repo = _repo(
        tmp_path, storage, replication, executor=executor_env[0], resource=resource
    )

    assert repo.materialize().success
    assert _rows(tmp_path, "SELECT id, item, amount FROM orders ORDER BY id") == ORDERS


def test_sling_package_finds_the_binary(tmp_path, src, storage, monkeypatch):
    monkeypatch.delenv("SLING_BINARY")
    replication = SlingReplication(_replication(src, "orders"))
    repo = _repo(tmp_path, storage, replication)

    assert repo.materialize().success
    assert _rows(tmp_path, "SELECT id, item, amount FROM orders ORDER BY id") == ORDERS


@pytest.mark.parametrize(
    ("name", "config", "asset_name"),
    [
        ("public.users", {}, "public.users"),
        ('"Public"."Users"', {}, "public.users"),
        ("public.users", {"object": "raw.app_users"}, "raw.app_users"),
        ("public.users", {"object": "raw.{stream_table}"}, "public.users"),
        ("file://data/orders.csv", {}, "file_data_orders.csv"),
        ("public.*", {}, "public._"),
    ],
)
def test_default_asset_name(name, config, asset_name):
    stream = SlingStream(name=name, config=config)
    output = stream.output_def()

    assert stream.asset_name == asset_name
    assert output.name == asset_name
    assert output.kinds == ["sling"]
    assert output.metadata == {"sling/stream": name}


def test_replication_errors(src):
    with pytest.raises(ValueError, match="no enabled streams"):
        SlingReplication({"source": "A", "target": "B", "streams": {}})
    with pytest.raises(ValueError, match="no enabled streams"):
        SlingReplication(
            {"source": "A", "target": "B", "streams": {"a.b": {"disabled": True}}}
        )
    with pytest.raises(
        ValueError, match="'a.users' and 'A.USERS' both map to the output 'a.users'"
    ):
        SlingReplication(
            {
                "source": "A",
                "target": "B",
                "streams": {"a.users": None, "A.USERS": None},
            }
        )
    with pytest.raises(ValidationError, match="'DB' needs a type or a url"):
        SlingConnection(name="DB", host="localhost")


@pytest.mark.parametrize("module", ["sling", "yaml"])
def test_missing_dependency_names_the_extra(monkeypatch, module):
    monkeypatch.setitem(sys.modules, module, None)
    monkeypatch.delitem(sys.modules, "rivers.integrations.sling")
    with pytest.raises(ImportError, match=r"pip install rivers\[sling\]"):
        importlib.import_module("rivers.integrations.sling")
