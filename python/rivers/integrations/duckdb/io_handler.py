"""IO handler that stores each asset as a table in DuckDB or DuckLake."""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable, Iterator
from functools import cache
from time import monotonic, sleep
from enum import StrEnum
from typing import Any

import duckdb

from rivers._core import InputContext, MetadataValue, OutputContext
from rivers._core.partitions import PartitionContext
from rivers.integrations.duckdb._sql import quote_ident, quote_table, select_sql
from rivers.integrations.duckdb.resource import DuckDBResource, set_lake_options
from rivers.io_handlers._partition_sql import _build_predicate, _resolve_partition_expr
from rivers.io_handlers.base import BaseIOHandler

_SOURCE = "_rivers_src"
_STAGE = "_rivers_stage"
# DuckLake's defaults for ducklake_max_retry_count / _retry_wait_ms / _retry_backoff
_DEFAULT_RETRY = (10, 100.0, 1.5)


class DuckDBWriteMode(StrEnum):
    """How :class:`DuckDBIOHandler` writes a table."""

    OVERWRITE = "overwrite"
    APPEND = "append"


class DuckDBIOHandler(BaseIOHandler):
    """Stores each asset as a table: ``<schema>.<asset name>``.

    With a ``DuckDBResource(ducklake=...)``, tables are DuckLake tables:
    partitioned assets get partitioned tables, each write registers the
    snapshot id as the data version, and ``duckdb/version`` reads an older
    snapshot.

    A write runs in one transaction. A partitioned overwrite deletes the
    partition's rows and inserts the new ones. Conflicting writes are retried
    with DuckLake's ``ducklake_max_retry_count``, ``ducklake_retry_wait_ms``,
    and ``ducklake_retry_backoff`` settings (10, 100 ms, 1.5 by default).

    Per-asset metadata keys: ``duckdb/schema``, ``duckdb/table`` (replaces the
    asset name), ``duckdb/mode``, ``duckdb/partition_expr``, ``duckdb/columns``
    (JSON list, read), ``duckdb/version`` (DuckLake, read), and
    ``ducklake/options`` (JSON object of table options).

    Args:
        resource: Opens the connections.
        schema_name: The schema for tables (``"main"`` by default).
        mode: ``OVERWRITE`` replaces the table, or the partition's rows;
            ``APPEND`` inserts rows. The strings ``"overwrite"`` and
            ``"append"`` work too.
    """

    resource: DuckDBResource
    schema_name: str = "main"
    mode: DuckDBWriteMode = DuckDBWriteMode.OVERWRITE

    def table_name(
        self, asset_name: str, asset_metadata: dict[str, str] | None
    ) -> tuple[str, str]:
        """The ``(schema, table)`` for an asset, honoring ``duckdb/schema`` and ``duckdb/table``."""
        meta = asset_metadata or {}
        return (
            meta.get("duckdb/schema", self.schema_name),
            meta.get("duckdb/table", asset_name),
        )

    def partition_predicate(
        self,
        asset_metadata: dict[str, str] | None,
        partition: PartitionContext,
    ) -> str:
        """SQL predicate for the partition's rows, from ``duckdb/partition_expr``.

        Raises:
            ValueError: If the metadata has no ``duckdb/partition_expr``.
        """
        expr = _resolve_partition_expr(asset_metadata or {}, "duckdb/partition_expr")
        if expr is None:
            raise ValueError(
                "a partitioned DuckDB asset needs the 'duckdb/partition_expr' "
                "metadata key to map the partition to the table's columns"
            )

        return _build_predicate(partition, expr)

    def handle_output(self, context: OutputContext, obj: object) -> None:
        """Write ``obj`` to the asset's table and record table metadata.

        Accepts a DuckDB relation, a pyarrow ``Table`` or ``RecordBatchReader``,
        an arro3 ``RecordBatchReader``, a polars ``DataFrame`` or ``LazyFrame``,
        or a pandas ``DataFrame``.
        """
        meta = context.asset_metadata or {}
        schema, table = self.table_name(context.asset_name, meta)
        ducklake = self.resource.ducklake

        raw_mode = meta.get("duckdb/mode", self.mode)
        try:
            mode = DuckDBWriteMode(raw_mode)
        except ValueError:
            raise ValueError(
                f"duckdb/mode must be one of {[m.value for m in DuckDBWriteMode]}, "
                f"got {raw_mode!r}"
            ) from None

        table_options = json.loads(meta.get("ducklake/options", "{}"))
        if table_options and ducklake is None:
            raise ValueError("ducklake/options needs a DuckDBResource with ducklake")

        predicate = None
        partition_by: list[str] = []
        if context.partition is not None:
            if mode is DuckDBWriteMode.OVERWRITE:
                predicate = self.partition_predicate(meta, context.partition)
            expr = _resolve_partition_expr(meta, "duckdb/partition_expr")
            if expr is not None and ducklake is not None:
                partition_by = expr.partition_columns

        start = monotonic()
        with self.resource.get_connection() as con:
            scan = _register_source(con, obj)
            _commit_with_retry(
                con,
                lambda: _write(con, schema, table, mode, predicate, partition_by, scan),
                ducklake=ducklake is not None,
            )
            duration = monotonic() - start

            # one transaction: on DuckLake each autocommit read is a catalog transaction
            con.begin()
            target = quote_table(schema, table)
            [(num_rows,)] = con.execute(f"SELECT count(*) FROM {target}").fetchall()
            output_meta: dict[str, Any] = {
                "duckdb/table": f"{schema}.{table}",
                "duckdb/num_rows": num_rows,
                "duckdb/write_duration_s": round(duration, 6),
            }
            if (arrow_schema := _arrow_schema(con, target)) is not None:
                output_meta["rivers/schema"] = MetadataValue.schema(arrow_schema)

            if ducklake is not None:
                [(snapshot,)] = con.execute(
                    f"SELECT id FROM {quote_ident(ducklake.name)}.last_committed_snapshot()"
                ).fetchall()
                output_meta["ducklake/snapshot_id"] = snapshot
                context.register_data_version(str(snapshot))
                set_lake_options(con, ducklake.name, table_options, (schema, table))
            con.commit()

        context.add_output_metadata(output_meta)

    def load_input(self, context: InputContext) -> Any:
        """Read the asset's table as ``context.type_hint``.

        A relation, ``RecordBatchReader``, or ``LazyFrame`` is lazy and keeps
        its connection open. Lazy inputs read in one thread share
        :meth:`DuckDBResource.shared_connection`, so relations can be joined.
        The other types are read eagerly.
        """
        lazy, read = _reader(context.type_hint)

        meta = context.asset_metadata or {}
        schema, table = self.table_name(context.asset_name, meta)
        source = quote_table(schema, table)
        if (version := meta.get("duckdb/version")) is not None:
            if self.resource.ducklake is None:
                raise ValueError(
                    "duckdb/version needs a DuckDBResource with ducklake; "
                    "a DuckDB database keeps no old versions"
                )
            source += f" AT (VERSION => {int(version)})"

        columns = json.loads(meta.get("duckdb/columns", "null"))
        predicate = None
        if context.partition is not None:
            predicate = self.partition_predicate(meta, context.partition)
        sql = select_sql(source, columns, predicate)

        if lazy:
            return read(_query(self.resource.shared_connection(), sql, schema, table))
        with self.resource.get_connection() as con:
            con.begin()  # binding and reading share one catalog transaction
            return read(_query(con, sql, schema, table))


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    """Whether ``schema.table`` exists in the connection's default database."""
    [(found,)] = con.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_catalog = current_database() AND table_schema = ? AND table_name = ?",
        [schema, table],
    ).fetchall()
    return found > 0


def _query(
    con: duckdb.DuckDBPyConnection, sql: str, schema: str, table: str
) -> duckdb.DuckDBPyRelation:
    try:
        return con.sql(sql)
    except duckdb.CatalogException as e:
        if table_exists(con, schema, table):
            raise
        raise duckdb.CatalogException(
            f"no DuckDB table {schema}.{table}: materialize the asset first"
        ) from e


def _register_source(con: duckdb.DuckDBPyConnection, obj: object) -> Callable[[], str]:
    """Register ``obj`` on ``con``. The callable prepares one scan and returns its name."""
    if isinstance(obj, duckdb.DuckDBPyRelation):

        def rescan() -> str:
            con.register(_SOURCE, obj.to_arrow_reader())
            return _SOURCE

        return rescan

    try:
        con.register(_SOURCE, obj)
    except duckdb.InvalidInputException as e:
        raise TypeError(
            f"DuckDBIOHandler cannot write {type(obj).__name__}. Supported types: "
            "duckdb.DuckDBPyRelation, pyarrow.Table, pyarrow.RecordBatchReader, "
            "arro3 RecordBatchReader, polars.DataFrame, polars.LazyFrame, pandas.DataFrame"
        ) from e

    if not isinstance(obj, _rescannable_types()):
        # a stream is consumed by one scan; stage it so a retry can scan again
        con.execute(f"CREATE TEMP TABLE {_STAGE} AS SELECT * FROM {_SOURCE}")
        con.unregister(_SOURCE)
        return lambda: _STAGE
    return lambda: _SOURCE


@cache
def _rescannable_types() -> tuple[type, ...]:
    """The installed output types that DuckDB can scan more than once."""
    types: list[type] = []
    for module, names in (
        ("pyarrow", ("Table",)),
        ("polars", ("DataFrame", "LazyFrame")),
        ("pandas", ("DataFrame",)),
    ):
        try:
            imported = importlib.import_module(module)
        except ImportError:
            continue

        types += [getattr(imported, name) for name in names]

    return tuple(types)


def _write(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    mode: DuckDBWriteMode,
    predicate: str | None,
    partition_by: list[str],
    scan: Callable[[], str],
) -> None:
    target = quote_table(schema, table)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema)}")

    if mode is DuckDBWriteMode.OVERWRITE and predicate is None:
        con.execute(f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM {scan()}")
        return

    if not table_exists(con, schema, table):
        if not partition_by:
            con.execute(f"CREATE TABLE {target} AS SELECT * FROM {scan()}")
            return
        con.execute(f"CREATE TABLE {target} AS SELECT * FROM {scan()} LIMIT 0")
        columns = ", ".join(map(quote_ident, partition_by))
        con.execute(f"ALTER TABLE {target} SET PARTITIONED BY ({columns})")
    elif predicate is not None:
        con.execute(f"DELETE FROM {target} WHERE {predicate}")

    con.execute(f"INSERT INTO {target} BY NAME SELECT * FROM {scan()}")


def _commit_with_retry(
    con: duckdb.DuckDBPyConnection, write: Callable[[], None], ducklake: bool
) -> None:
    """Run ``write`` in a transaction; retry it when the commit conflicts.

    The retry budget is read on a conflict: DuckLake's ``ducklake_max_retry_count``,
    ``ducklake_retry_wait_ms``, and ``ducklake_retry_backoff``, or their defaults.
    """
    attempt = 0
    while True:
        con.begin()
        try:
            write()
            con.commit()
            return
        except duckdb.TransactionException as e:
            _rollback(con)
            if "conflict" not in str(e).lower():
                raise
            retries, wait_ms, backoff = (
                _retry_settings(con) if ducklake else _DEFAULT_RETRY
            )
            if attempt >= retries:
                raise
        except BaseException:
            _rollback(con)
            raise

        sleep(wait_ms / 1000 * backoff**attempt)
        attempt += 1


def _rollback(con: duckdb.DuckDBPyConnection) -> None:
    try:
        con.rollback()
    except duckdb.TransactionException:
        pass  # a failed COMMIT already ended the transaction


def _retry_settings(con: duckdb.DuckDBPyConnection) -> tuple[int, float, float]:
    [settings] = con.execute(
        "SELECT current_setting('ducklake_max_retry_count'), "
        "current_setting('ducklake_retry_wait_ms'), "
        "current_setting('ducklake_retry_backoff')"
    ).fetchall()
    return settings


def _arrow_schema(con: duckdb.DuckDBPyConnection, target: str) -> Any:
    """The table's Arrow schema, read with pyarrow or arro3; ``None`` without both."""
    for module in ("pyarrow", "arro3.core"):
        try:
            reader = importlib.import_module(module).RecordBatchReader
        except ImportError:
            continue

        return reader.from_stream(con.sql(f"FROM {target} LIMIT 0")).schema

    return None


def _record_batch_reader(rel: duckdb.DuckDBPyRelation) -> Any:
    """A stream that keeps ``rel``, and so its connection, until the stream ends.

    A ``to_arrow_reader()`` stream that outlives its connection object makes
    the next ``duckdb.connect`` to the same file hang.
    """
    import pyarrow as pa

    reader = rel.to_arrow_reader()

    def batches() -> Iterator[Any]:
        try:
            yield from reader
        finally:
            reader.close()
            rel.close()

    return pa.RecordBatchReader.from_batches(reader.schema, batches())


_Reader = tuple[type, bool, Callable[[duckdb.DuckDBPyRelation], Any]]


@cache
def _readers() -> list[_Reader]:
    """``(type, lazy, read)`` for each installed input type."""
    readers: list[_Reader] = [(duckdb.DuckDBPyRelation, True, lambda rel: rel)]

    try:
        import pyarrow as pa

        readers += [
            (pa.RecordBatchReader, True, _record_batch_reader),
            (pa.Table, False, lambda rel: rel.to_arrow_table()),
        ]
    except ImportError:
        pass

    try:
        import polars as pl

        readers += [
            (pl.LazyFrame, True, lambda rel: rel.pl(lazy=True)),
            (pl.DataFrame, False, lambda rel: rel.pl()),
        ]
    except ImportError:
        pass

    try:
        import pandas as pd

        readers.append((pd.DataFrame, False, lambda rel: rel.df()))
    except ImportError:
        pass

    return readers


def _reader(
    target_type: type | None,
) -> tuple[bool, Callable[[duckdb.DuckDBPyRelation], Any]]:
    if isinstance(target_type, type):
        for supported, lazy, read in _readers():
            if issubclass(target_type, supported):
                return lazy, read

    names = [t.__name__ for t, _, _ in _readers()]
    if target_type is None:
        raise TypeError(
            f"No type_hint provided on InputContext. Supported types: {names}"
        )
    raise TypeError(
        f"DuckDBIOHandler cannot read {target_type!r}. Supported types: {names}"
    )
