"""A migration with a start time waits until then. The asset waits for its
migration from the deploy on; when the time has passed, the migration starts
at once."""

import contextlib
import datetime

import pytest

import rivers as rs
from _polling import wait_for
from rivers._core import AutomationDaemon
from rivers.exceptions import AssetMigrationPendingError

LATER = datetime.datetime(2100, 1, 1, tzinfo=datetime.timezone.utc)


def _repo(storage, io, **migrations):
    """A resolved repository of ``orders`` and ``customers``. An asset named
    in ``migrations`` declares version 2 with those migrations."""

    def asset(name):
        declared = (
            {"schema_version": 2, "migrations": migrations[name]}
            if name in migrations
            else {}
        )

        @rs.Asset(name=name, io_handler=io, **declared)
        def body() -> int:
            return 1

        return body

    repo = rs.CodeRepository(
        assets=[asset("orders"), asset("customers")],
        default_executor=rs.Executor.in_process(),
    )
    repo.resolve(storage=storage)
    return repo


def _starting_at(start_at):
    """A convert to version 2 that starts at ``start_at``, and the versions
    that it converted to."""
    calls = []

    def convert(ctx: rs.MigrationContext) -> None:
        calls.append(ctx.to_version)

    return [rs.AssetMigration(to=2, start_at=start_at)(convert)], calls


def _ready(storage, key):
    wait_for(
        lambda: storage.get_asset_version(key).state == "ready",
        30.0,
        f"the migration of {key} to end",
    )


@contextlib.contextmanager
def _daemon(repo, storage):
    daemon = AutomationDaemon(repo=repo, storage=storage, condition_eval_interval="1s")
    daemon.start()
    try:
        yield
    finally:
        daemon.stop()


def test_runs_wait_for_a_migration_that_starts_later(storage):
    io = rs.InMemoryIOHandler()
    assert _repo(storage, io).materialize().success
    migrations, calls = _starting_at(LATER)
    now, _ = _starting_at(None)
    repo = _repo(storage, io, orders=migrations, customers=now)

    with _daemon(repo, storage):
        # The daemon passed: it migrated customers.
        _ready(storage, "customers")
        assert storage.get_asset_version("orders").state == "pending"
        with pytest.raises(
            AssetMigrationPendingError,
            match=r"^orders waits for its migration to version 2, which starts at "
            r"2100-01-01T00:00:00Z; to migrate in this process, call repo\.migrate\(\)$",
        ):
            repo.materialize(["orders"])

    assert calls == []


def test_a_start_time_that_passed_starts_the_migration_at_once(storage):
    io = rs.InMemoryIOHandler()
    assert _repo(storage, io).materialize().success
    passed = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
    migrations, calls = _starting_at(passed)
    repo = _repo(storage, io, orders=migrations)

    with _daemon(repo, storage):
        _ready(storage, "orders")

    assert storage.get_asset_version("orders").applied_version == 2
    assert calls == [2]


def test_repo_migrate_starts_a_migration_before_its_start_time(storage):
    io = rs.InMemoryIOHandler()
    assert _repo(storage, io).materialize().success
    migrations, calls = _starting_at(LATER)
    repo = _repo(storage, io, orders=migrations)

    (record,) = repo.migrate(["orders"])

    assert (record.state, record.applied_version) == ("ready", 2)
    assert calls == [2]
