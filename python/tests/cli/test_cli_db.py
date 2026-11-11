"""``rivers db`` commands. The storage is a stub: the rivers-core tests cover
what the store reports."""

from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

import rivers as rs
from rivers.cli import app
from rivers.exceptions import StorageError

runner = CliRunner()

NOW_NS = 1_000 * 10**9


def _process(name, **fields):
    defaults = dict(
        code_location=True,
        writes=True,
        schema_version=13,
        rivers_version="0.6.0",
        seen=NOW_NS - 4 * 10**9,
        stops=False,
    )
    return SimpleNamespace(name=name, **{**defaults, **fields})


def _status(version=12, floors=(12, 12), processes=()):
    return SimpleNamespace(
        version=version,
        min_reader=None if version is None else floors[0],
        min_writer=None if version is None else floors[1],
        build_version=13,
        build_min_reader=13,
        build_min_writer=13,
        processes=list(processes),
    )


@pytest.fixture
def storage(monkeypatch):
    """Stub ``Storage`` of the db commands; records each call."""
    calls = []
    stub = SimpleNamespace(calls=calls, status=_status())

    def record(kind):
        def call(target, **kwargs):
            calls.append((kind, target, kwargs))
            return stub.status

        return staticmethod(call)

    kinds = ("schema_status_embedded", "schema_status_remote")
    monkeypatch.setattr(
        "rivers.cli.db.Storage", type("_Storage", (), {k: record(k) for k in kinds})
    )
    monkeypatch.setattr("rivers.cli.db.time.time_ns", lambda: NOW_NS)
    return stub


def test_status_shows_the_stamps_and_each_process(storage):
    storage.status = _status(
        processes=[
            _process("rivers/analytics"),
            _process("rivers/billing", schema_version=12, stops=True),
            _process(
                "rivers/legacy", schema_version=None, rivers_version=None, stops=True
            ),
            _process("rivers/idle", seen=None),
            _process("operator", code_location=False, seen=NOW_NS - 3 * 3600 * 10**9),
        ]
    )

    result = runner.invoke(app, ["db", "status", "--storage-path", "store"])

    assert result.exit_code == 0, result.output
    assert storage.calls == [("schema_status_embedded", "store", {})]
    assert result.output.splitlines() == [
        "Database:   schema 12 (read floor 12, write floor 12)",
        "This build: schema 13 (read floor 13, write floor 13)",
        "State:      migration needed: run `rivers db migrate`",
        "",
        "NAME              KIND           SCHEMA  RIVERS  SEEN",
        "rivers/analytics  code location  13      0.6.0   4 s ago",
        "rivers/billing    code location  12      0.6.0   4 s ago"
        "          stops after migration",
        "rivers/legacy     code location  ?       ?       4 s ago"
        "          stops after migration (version unknown)",
        "rivers/idle       code location  13      0.6.0   never",
        "operator          component      13      0.6.0   started 3 h ago",
    ]


@pytest.mark.parametrize(
    ("version", "state"),
    [
        (13, "up to date"),
        (14, "the database is ahead of this build"),
    ],
)
def test_status_names_the_state(storage, version, state):
    storage.status = _status(version=version)

    result = runner.invoke(app, ["db", "status"])

    assert result.exit_code == 0, result.output
    assert f"State:      {state}" in result.output


def test_status_of_a_store_that_is_not_initialized(storage):
    storage.status = _status(version=None)

    result = runner.invoke(app, ["db", "status"])

    assert result.exit_code == 0, result.output
    assert result.output.splitlines() == [
        "Database:   not initialized",
        "This build: schema 13 (read floor 13, write floor 13)",
    ]


def test_status_reads_a_remote_database(storage):
    result = runner.invoke(app, ["db", "status", "--surreal-endpoint", "ws://db:8000"])

    assert result.exit_code == 0, result.output
    assert storage.calls == [("schema_status_remote", "ws://db:8000", {})]


def test_schema_status_embedded_leaves_an_empty_store_alone(resolved_tmp_path):
    path = resolved_tmp_path / "db"
    path.mkdir()

    status = rs.Storage.schema_status_embedded(str(path))

    assert status.version is None
    assert status.processes == []
    assert status.build_version >= 13
    assert status.build_min_writer >= status.build_min_reader


def test_schema_status_embedded_needs_a_store(resolved_tmp_path):
    with pytest.raises(StorageError, match="no storage at"):
        rs.Storage.schema_status_embedded(str(resolved_tmp_path / "missing"))
    assert not (resolved_tmp_path / "missing").exists()
