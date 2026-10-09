"""DuckDB connections to a database file, MotherDuck, or a DuckLake catalog."""

from __future__ import annotations

import threading
import weakref
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from time import sleep
from typing import TypeVar

import duckdb
from pydantic import BaseModel, ConfigDict, model_validator
from pydantic_settings import SettingsConfigDict

from rivers.integrations.duckdb._sql import quote_ident, render_attach, render_secret
from rivers.resource import Resource

_T = TypeVar("_T")

# One DuckLake root per resource config and process: in-memory databases that
# attach the same DuckDB-file catalog at the same time conflict.
_roots: dict[str, duckdb.DuckDBPyConnection] = {}
_roots_lock = threading.Lock()


class _Shared(threading.local):
    def __init__(self) -> None:
        self.connections: weakref.WeakValueDictionary[
            str, duckdb.DuckDBPyConnection
        ] = weakref.WeakValueDictionary()


_shared = _Shared()

_LOCK_RETRIES = 10
_ONE_PROCESS_HINT = (
    "A DuckLake catalog in a DuckDB file allows one process. Use a PostgreSQL "
    "catalog with the parallel executor or Kubernetes."
)


class DuckLake(BaseModel):
    """A DuckLake catalog attached to every connection of a :class:`DuckDBResource`.

    Args:
        metadata: The catalog database: a DuckDB file (``"meta.ducklake"``, one
            process only), ``"postgres:dbname=lake host=pg"``, or
            ``"sqlite:meta.sqlite"``.
        data_path: Where DuckLake writes Parquet files, e.g. ``"s3://bucket/lake/"``.
        name: The catalog name in SQL.
        attach_options: Other ``ATTACH`` options, e.g. ``{"METADATA_SCHEMA": "lake"}``.
        options: Lake-wide DuckLake options, e.g. ``{"expire_older_than": "7 days"}``.
            Each one that differs from the value in the catalog is set with
            ``set_option`` when the catalog is attached.
    """

    model_config = ConfigDict(extra="forbid")

    metadata: str
    data_path: str | None = None
    name: str = "lake"
    attach_options: dict[str, str | int | bool] = {}
    options: dict[str, str | int | float | bool] = {}


class DuckDBResource(Resource):
    """Opens DuckDB connections, with extensions, secrets, and an optional DuckLake catalog.

    A database file or ``md:`` database gets a new connection per call; DuckDB
    shares one instance per file in a process and releases the file when the
    last connection closes. With ``ducklake``, the catalog is attached once per
    process to an in-memory database, and each call gets a cursor on it. That
    root connection stays open until :meth:`teardown` and until the last
    cursor made from it is gone. Resources with equal fields share it.

    Opening a file that another process holds is retried 10 times, with a
    delay of 0.1 s that doubles each time.

    Fields are read from ``DUCKDB_*`` environment variables when not given.

    Args:
        database: A file path, ``":memory:"``, or ``"md:<database>"`` for MotherDuck.
        connection_config: DuckDB settings passed to ``duckdb.connect(config=...)``,
            e.g. ``{"threads": 4, "ducklake_max_retry_count": 20}``.
        extensions: Extensions to install and load on each connection.
        secrets: Secrets to create on each connection: name to
            ``CREATE SECRET`` parameters, e.g.
            ``{"s3": {"type": "s3", "provider": "credential_chain"}}``.
        ducklake: A DuckLake catalog to attach. Needs ``database=":memory:"``.

    Example::

        warehouse = DuckDBResource(database="warehouse.duckdb")

        @rs.Asset
        def top_users(warehouse: DuckDBResource) -> pa.Table:
            with warehouse.get_connection() as con:
                return con.sql("FROM users ORDER BY score DESC LIMIT 10").to_arrow_table()
    """

    model_config = SettingsConfigDict(env_prefix="DUCKDB_")

    database: str = ":memory:"
    connection_config: dict[str, str | bool | int | float | list[str]] = {}
    extensions: list[str] = []
    secrets: dict[str, dict[str, str | int | bool]] = {}
    ducklake: DuckLake | None = None

    @model_validator(mode="after")
    def _ducklake_in_memory(self) -> DuckDBResource:
        if self.ducklake is not None and self.database != ":memory:":
            raise ValueError(
                "ducklake needs database=':memory:': the catalog is attached "
                "to an in-memory database"
            )
        return self

    @contextmanager
    def get_connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """A connection that is closed when the block exits.

        Relations made from it stop working after the block. Use
        :meth:`connect` to return a relation.
        """
        with self.connect() as con:
            yield con

    def connect(self) -> duckdb.DuckDBPyConnection:
        """A new connection. The caller closes it, or lets a relation keep it.

        With ``ducklake``, the connection is a cursor with the catalog as the
        default database.
        """
        if self.ducklake is None:
            con = _retry_on_lock(
                lambda: duckdb.connect(self.database, config=self.connection_config)
            )
            self._prepare(con)
            return con

        con = self._ducklake_root(self.ducklake).cursor()
        con.execute(f"USE {quote_ident(self.ducklake.name)}")
        return con

    def shared_connection(self) -> duckdb.DuckDBPyConnection:
        """The calling thread's connection for relations that must be combined.

        DuckDB joins relations only when they come from one connection. Each
        thread gets one connection per resource config; it closes when the
        last relation made from it is gone, and the next call opens a new one.
        """
        key = self.model_dump_json()
        con = _shared.connections.get(key)
        if con is None:
            con = _shared.connections[key] = self.connect()
        return con

    def teardown(self) -> None:
        """Release the DuckLake root connection.

        The catalog closes when the last cursor or relation made from it is
        gone, so relations returned by an asset still work.
        """
        with _roots_lock:
            _roots.pop(self.model_dump_json(), None)

    def _prepare(self, con: duckdb.DuckDBPyConnection) -> None:
        for extension in self.extensions:
            con.install_extension(extension)
            con.load_extension(extension)

        for name, params in self.secrets.items():
            con.execute(render_secret(name, params))

    def _ducklake_root(self, ducklake: DuckLake) -> duckdb.DuckDBPyConnection:
        key = self.model_dump_json()
        with _roots_lock:
            if key not in _roots:
                _roots[key] = self._attach_ducklake(ducklake)
            return _roots[key]

    def _attach_ducklake(self, ducklake: DuckLake) -> duckdb.DuckDBPyConnection:
        root = duckdb.connect(":memory:", config=self.connection_config)
        try:
            self._prepare(root)
            _retry_on_lock(
                lambda: root.execute(render_attach(ducklake)), hint=_ONE_PROCESS_HINT
            )
            set_lake_options(root, ducklake.name, ducklake.options)
        except BaseException:
            root.close()
            raise

        return root


def lake_options(
    con: duckdb.DuckDBPyConnection, catalog: str, table: tuple[str, str] | None = None
) -> dict[str, str]:
    """The options stored in DuckLake ``catalog``, lake-wide or for one ``(schema, table)``."""
    if table is None:
        where, args = "scope = 'GLOBAL'", []
    else:
        where, args = "scope = 'TABLE' AND scope_entry = ?", [".".join(table)]

    return dict(
        con.execute(
            f"SELECT option_name, value FROM {quote_ident(catalog)}.options() WHERE {where}",
            args,
        ).fetchall()
    )


def set_lake_options(
    con: duckdb.DuckDBPyConnection,
    catalog: str,
    options: dict[str, str | int | float | bool],
    table: tuple[str, str] | None = None,
) -> None:
    """Set each option that differs from DuckLake ``catalog``, lake-wide or for one ``(schema, table)``."""
    if not options:
        return

    stored = lake_options(con, catalog, table)
    named = ", schema => ?, table_name => ?" if table else ""
    for key, value in options.items():
        if not _same_option(stored.get(key), value):
            con.execute(
                f"CALL {quote_ident(catalog)}.set_option(?, ?{named})",
                [key, value, *(table or ())],
            )


def _same_option(stored: str | None, value: str | int | float | bool) -> bool:
    """DuckLake stores option values as text, numbers normalized (``0.5`` as ``0.500000``)."""
    if stored is None:
        return False

    if isinstance(value, bool):
        return stored == str(value).lower()
    if isinstance(value, (int, float)):
        try:
            return float(stored) == value
        except ValueError:
            return False

    return stored == value


def _retry_on_lock(fn: Callable[[], _T], hint: str | None = None) -> _T:
    """Call ``fn``, retrying while another process holds the database file."""
    delay = 0.1
    for _ in range(_LOCK_RETRIES):
        try:
            return fn()
        except duckdb.IOException as e:
            if not _is_lock_error(e):
                raise
        sleep(delay)
        delay *= 2

    try:
        return fn()
    except duckdb.IOException as e:
        if hint is None or not _is_lock_error(e):
            raise
        raise duckdb.IOException(f"{e}\n{hint}") from e


def _is_lock_error(e: duckdb.IOException) -> bool:
    return "Could not set lock" in str(e)
