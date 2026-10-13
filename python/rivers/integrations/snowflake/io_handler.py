"""IO handler that stores each asset as a Snowflake table."""

import io
import json
import sys
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager
from datetime import datetime
from enum import StrEnum
from functools import cache
from time import monotonic
from typing import Any, NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor

from rivers._core import InputContext, MetadataValue, OutputContext
from rivers._core.partitions import PartitionContext
from rivers.integrations.snowflake.resource import SnowflakeResource
from rivers.io_handlers._partition_sql import (
    PartitionExpr,
    _build_predicate,
    _resolve_partition_expr,
)
from rivers.io_handlers.base import BaseIOHandler

_FILE_FORMAT = "TYPE = PARQUET USE_LOGICAL_TYPE = TRUE BINARY_AS_TEXT = FALSE"
_PRECISION = {"s": 0, "ms": 3, "us": 6, "ns": 9}
# UTC: partition bounds are UTC times without a zone. Without the decimal
# setting, a NUMBER column with a scale is read as float64. pyformat: the
# handler binds %s, whatever paramstyle the resource or the process sets.
_SESSION = {
    "timezone": "UTC",
    "arrow_number_to_decimal": True,
    "paramstyle": "pyformat",
}
# Arrow bytes per staged file. Snowflake loads the files of a stage in
# parallel and recommends 100 to 250 MB per file.
_FILE_SIZE = 100 * 2**20


class SnowflakeWriteMode(StrEnum):
    """How :class:`SnowflakeIOHandler` writes a table."""

    OVERWRITE = "overwrite"
    APPEND = "append"


class SnowflakeIOHandler(BaseIOHandler):
    """Stores each asset as a table ``<schema>.<asset name>`` in the resource's database.

    Schema, table, and column names are uppercased, the form Snowflake gives
    unquoted names. A read lowercases the column names.

    A write streams the rows to a temporary stage as Parquet files, each from
    about 100 MB of Arrow data, one file in memory at a time. The first write
    creates the table from the output's Arrow types. Then ``COPY INTO`` loads
    the files; an overwrite deletes the old rows first, in the same
    transaction. The table keeps its columns, grants, and policies: an output
    fails if it has a column that the table does not have, or a column whose
    values the table's column type would change.

    The handler's sessions use the UTC time zone: daily and hourly partition
    bounds and the timestamps that a read returns are in UTC.

    Per-asset metadata keys: ``snowflake/schema``, ``snowflake/table``
    (replaces the asset name), ``snowflake/mode``,
    ``snowflake/partition_expr``, and ``snowflake/columns`` (JSON list, read).

    Args:
        resource: Opens the connections.
        schema_name: The schema for tables (``"PUBLIC"`` by default).
        mode: ``OVERWRITE`` replaces the table's rows, or the partition's
            rows; ``APPEND`` adds rows. The strings ``"overwrite"`` and
            ``"append"`` work too.
    """

    resource: SnowflakeResource
    schema_name: str = "PUBLIC"
    mode: SnowflakeWriteMode = SnowflakeWriteMode.OVERWRITE

    def table_name(
        self, asset_name: str, asset_metadata: dict[str, str] | None
    ) -> tuple[str, str]:
        """The uppercased ``(schema, table)`` for an asset, honoring ``snowflake/schema`` and ``snowflake/table``."""
        meta = asset_metadata or {}
        return (
            meta.get("snowflake/schema", self.schema_name).upper(),
            meta.get("snowflake/table", asset_name).upper(),
        )

    def partition_predicate(
        self,
        asset_metadata: dict[str, str] | None,
        partition: PartitionContext,
        columns: Mapping[str, "Column"] | None = None,
    ) -> str:
        """SQL predicate for the partition's rows, from ``snowflake/partition_expr``.

        A daily or hourly key selects a range. On a ``DATE`` or timestamp
        column in ``columns``, the range bounds are timestamps. On other
        columns and on expressions, they are text in the partition's format.

        Args:
            asset_metadata: The asset's metadata.
            partition: The partition keys.
            columns: The table's columns, from :func:`table_columns`.

        Raises:
            ValueError: If the metadata has no ``snowflake/partition_expr``.
        """
        expr = require_partition_expr(asset_metadata)
        times = {
            name
            for name, column in (columns or {}).items()
            if column.type in _TIMESTAMP_BOUNDS
        }

        def bound(col: str, at: datetime) -> str | None:
            return _timestamp(at) if col.upper() in times else None

        return _build_predicate(partition, expr, quote=_literal, bound=bound)

    def get_connection(self) -> AbstractContextManager[SnowflakeConnection]:
        """A connection in the handler's session, closed when the block exits.

        The session uses UTC, the time zone of partition bounds, fetches
        ``NUMBER`` columns with a scale as decimals, and binds parameters as
        ``%s`` (pyformat).
        """
        return self.resource.get_connection(**_SESSION)

    def handle_output(self, context: OutputContext, obj: object) -> None:
        """Write ``obj`` to the asset's table and record table metadata.

        Accepts a pyarrow ``Table`` or ``RecordBatchReader``, a polars
        ``DataFrame`` or ``LazyFrame``, a pandas ``DataFrame``, or another
        object that exports an Arrow stream (``__arrow_c_stream__``). A
        ``LazyFrame`` runs in batches (``collect_batches``), so its whole
        result is never in memory.

        Raises:
            TypeError: If ``obj`` is of another type, or has a column type
                that Snowflake cannot store.
            ValueError: If ``obj`` has a column that the table does not have,
                or a column whose values the table's column type would
                change, such as floats for a ``NUMBER(38,0)`` column.
        """
        meta = context.asset_metadata or {}
        schema, table = self.table_name(context.asset_name, meta)

        raw_mode = meta.get("snowflake/mode", self.mode)
        try:
            mode = SnowflakeWriteMode(raw_mode)
        except ValueError:
            raise ValueError(
                f"snowflake/mode must be one of {[m.value for m in SnowflakeWriteMode]}, "
                f"got {raw_mode!r}"
            ) from None

        overwrite = mode is SnowflakeWriteMode.OVERWRITE
        if overwrite and context.partition is not None:
            require_partition_expr(meta)  # fails before connecting

        reader = _to_reader(obj)
        definition = ", ".join(
            f"{_quote(field.name)} {_snowflake_type(field.type)}"
            for field in reader.schema
        )
        target = quote_table(schema, table)
        stage = quote_table(schema, f"RIVERS_STAGE_{uuid.uuid4().hex}")

        start = monotonic()
        with self.get_connection() as con:
            cur = con.cursor()
            columns = _prepare_table(cur, schema, table, reader.schema)
            cur.execute(f"CREATE TEMPORARY STAGE {stage}")
            for number, parquet in enumerate(_parquet_files(reader)):
                with parquet:
                    cur.execute(
                        f"PUT 'file:///rivers/{number}.parquet' @{stage}",
                        file_stream=parquet,
                    )

            # after the upload, so that an output that fails to read makes no table
            if not columns:
                cur.execute(f"CREATE TABLE IF NOT EXISTS {target} ({definition})")
                columns = table_columns(cur, schema, table)
                # another write can create the table during the upload
                _check_columns(schema, table, columns, reader.schema)

            delete = None
            if overwrite:
                delete = f"DELETE FROM {target}"
                if context.partition is not None:
                    where = self.partition_predicate(meta, context.partition, columns)
                    delete += f" WHERE {where}"
            _write(cur, target, stage, delete)
            duration = monotonic() - start

            cur.execute(f"SELECT COUNT(*), CURRENT_DATABASE() FROM {target}")
            [(num_rows, database)] = cur.fetchall()

        context.add_output_metadata(
            {
                "snowflake/table": f"{database}.{schema}.{table}",
                "snowflake/num_rows": num_rows,
                "snowflake/write_duration_s": round(duration, 6),
                "rivers/schema": MetadataValue.schema(reader.schema),
            }
        )

    def load_input(self, context: InputContext) -> Any:
        """Read the asset's table as ``context.type_hint``, with lowercased column names.

        The rows are fetched as Arrow. A ``RecordBatchReader`` or ``LazyFrame``
        wraps the fetched rows.

        Raises:
            TypeError: If ``context.type_hint`` is missing or not supported.
        """
        read = _reader(context.type_hint)

        meta = context.asset_metadata or {}
        schema, table = self.table_name(context.asset_name, meta)
        selected = json.loads(meta.get("snowflake/columns", "null"))
        select = ", ".join(map(_quote, selected)) if selected else "*"
        sql = f"SELECT {select} FROM {quote_table(schema, table)}"
        if context.partition is not None:
            require_partition_expr(meta)  # fails before connecting

        with self.get_connection() as con:
            cur = con.cursor()
            if context.partition is not None:
                columns = table_columns(cur, schema, table)
                where = self.partition_predicate(meta, context.partition, columns)
                sql += f" WHERE {where}"
            cur.execute(sql)
            result = cur.fetch_arrow_all(force_return_table=True)

        return read(_from_snowflake(result))


def _quote(name: str) -> str:
    """Quote ``name``, uppercased, as a Snowflake identifier."""
    return '"' + name.upper().replace('"', '""') + '"'


def _literal(value: str) -> str:
    """``value`` as a Snowflake string literal, in which a backslash starts an escape."""
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


_TIMESTAMP_BOUNDS = {"DATE", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"}


def _timestamp(at: datetime) -> str:
    """``at`` as a ``TIMESTAMP_NTZ``, parsed with a fixed format, not the session's.

    A ``DATE`` compares as its midnight, and a ``TIMESTAMP_LTZ`` or
    ``TIMESTAMP_TZ`` column in the session's time zone.
    """
    return (
        f"TO_TIMESTAMP_NTZ('{at.isoformat(' ', 'microseconds')}', "
        "'YYYY-MM-DD HH24:MI:SS.FF6')"
    )


def require_partition_expr(asset_metadata: dict[str, str] | None) -> PartitionExpr:
    """The asset's ``snowflake/partition_expr``.

    Raises:
        ValueError: If the metadata has none.
    """
    expr = _resolve_partition_expr(asset_metadata or {}, "snowflake/partition_expr")
    if expr is None:
        raise ValueError(
            "a partitioned Snowflake asset needs the 'snowflake/partition_expr' "
            "metadata key to map the partition to the table's columns"
        )
    return expr


def quote_table(schema: str, table: str) -> str:
    return f"{_quote(schema)}.{_quote(table)}"


class Column(NamedTuple):
    """A column's type, as ``information_schema.columns`` reports it.

    ``type`` is Snowflake's name for it, such as ``NUMBER``, ``TEXT``, or
    ``TIMESTAMP_LTZ``.
    """

    type: str
    precision: int | None
    scale: int | None

    def __str__(self) -> str:
        if self.type == "NUMBER":
            return f"NUMBER({self.precision},{self.scale})"
        return self.type


def table_columns(cur: SnowflakeCursor, schema: str, table: str) -> dict[str, Column]:
    """The columns of ``schema.table`` by uppercased name, empty if the table does not exist.

    Reads ``information_schema.columns`` of the cursor's database. ``cur``
    must bind ``%s`` parameters, as the cursors of
    :meth:`SnowflakeIOHandler.get_connection` do.
    """
    cur.execute(
        "SELECT column_name, data_type, numeric_precision, numeric_scale "
        "FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    return {name.upper(): Column(*column) for name, *column in cur.fetchall()}


def _prepare_table(
    cur: SnowflakeCursor, schema: str, table: str, output: pa.Schema
) -> dict[str, Column]:
    """The columns of ``schema.table``, empty if it does not exist.

    Checks that an existing table holds ``output``, or creates a missing
    schema. The schema is looked up first: a ``CREATE`` statement can need
    the create privilege even when the object exists.

    Raises:
        ValueError: If the table exists without a column of ``output``, or
            with a column type that would change the column's values.
    """
    if columns := table_columns(cur, schema, table):
        _check_columns(schema, table, columns, output)
        return columns

    cur.execute(
        "SELECT COUNT(*) FROM information_schema.schemata "
        "WHERE catalog_name = CURRENT_DATABASE() AND schema_name = %s",
        (schema,),
    )
    [(schemas,)] = cur.fetchall()
    if not schemas:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote(schema)}")
    return {}


def _check_columns(
    schema: str, table: str, columns: dict[str, Column], output: pa.Schema
) -> None:
    """Check that the table's ``columns`` take each column of ``output`` without changing its values.

    A ``null`` output column fits any column: it has no values to change.

    Raises:
        ValueError: If a column is missing, or its type would change the
            output's values.
    """
    if missing := [name for name in output.names if name.upper() not in columns]:
        raise ValueError(
            f"table {schema}.{table} has no column {', '.join(missing)}: add it "
            "with ALTER TABLE, or drop the table to create it on the next write"
        )

    changed = []
    for field in output:
        column = columns[field.name.upper()]
        snowflake_type = _snowflake_type(field.type)
        if not pa.types.is_null(field.type) and not _holds(column, snowflake_type):
            changed.append(f"{field.name} {column} (output {snowflake_type})")

    if changed:
        raise ValueError(
            f"table {schema}.{table} has column {', '.join(changed)}: the load would "
            "change the values. Alter the column with ALTER TABLE, or drop the table "
            "to create it on the next write"
        )


# The column types that hold an output type, where that is not only the type
# itself: Snowflake reports VARCHAR columns as TEXT, and a TIMESTAMP_TZ column
# keeps the instants of TIMESTAMP_LTZ values.
_HOLDERS = {
    "VARCHAR": ("TEXT",),
    "TIMESTAMP_LTZ": ("TIMESTAMP_LTZ", "TIMESTAMP_TZ"),
}


def _holds(column: Column, snowflake_type: str) -> bool:
    """Whether loading values of ``snowflake_type`` into ``column`` keeps them.

    The column must have the output's type; its precision can differ. A
    ``NUMBER`` column also needs at least the output's scale: a load rounds
    values to the column's scale. A ``VARIANT`` column holds any type.
    """
    if column.type == "VARIANT":
        return True

    kind, _, size = snowflake_type.partition("(")
    if kind == "NUMBER":
        _, scale = size.removesuffix(")").split(",")
        return column.type == "NUMBER" and int(scale) <= (column.scale or 0)
    return column.type in _HOLDERS.get(kind, (kind,))


def _write(cur: SnowflakeCursor, target: str, stage: str, delete: str | None) -> None:
    """Load the staged files. With ``delete``, run it first, in one transaction."""
    # FORCE: Snowflake skips a file that it loaded into the table before
    copy = (
        f"COPY INTO {target} FROM @{stage} FILE_FORMAT = ({_FILE_FORMAT}) "
        "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE"
    )
    if delete is None:
        cur.execute(copy)
        return

    cur.execute("BEGIN")
    try:
        cur.execute(delete)
        cur.execute(copy)
        cur.execute("COMMIT")
    except BaseException:
        cur.execute("ROLLBACK")
        raise


def _from_snowflake(table: pa.Table) -> pa.Table:
    """``table`` with lowercased column names and integer columns as int64.

    Snowflake sends a NUMBER column in the smallest integer type that holds
    its values.
    """
    schema = pa.schema(
        pa.field(
            field.name.lower(),
            pa.int64() if pa.types.is_integer(field.type) else field.type,
        )
        for field in table.schema
    )
    return table.rename_columns(schema.names).cast(schema)


def _to_reader(obj: Any) -> pa.RecordBatchReader:
    """``obj`` as an Arrow stream, without importing polars or pandas."""
    if isinstance(obj, pa.Table):
        return obj.to_reader()
    if isinstance(obj, pa.RecordBatchReader):
        return obj

    pl = sys.modules.get("polars")
    if pl is not None and isinstance(obj, pl.DataFrame):
        return obj.to_arrow().to_reader()
    if pl is not None and isinstance(obj, pl.LazyFrame):
        # to_arrow, not the frames' Arrow streams: the Parquet writer cannot
        # write the dictionary<string_view> columns that a stream exports
        schema = pl.DataFrame(schema=obj.collect_schema()).to_arrow().schema
        batches = (
            batch
            for frame in obj.collect_batches()
            for batch in frame.to_arrow().to_batches()
        )
        return pa.RecordBatchReader.from_batches(schema, batches)

    pd = sys.modules.get("pandas")
    if pd is not None and isinstance(obj, pd.DataFrame):
        return pa.Table.from_pandas(obj, preserve_index=False).to_reader()

    if hasattr(obj, "__arrow_c_stream__"):
        return pa.RecordBatchReader.from_stream(obj)

    raise TypeError(
        f"SnowflakeIOHandler cannot write {type(obj).__name__}. Supported types: "
        "pyarrow.Table, pyarrow.RecordBatchReader, polars.DataFrame, "
        "polars.LazyFrame, pandas.DataFrame, or an object with __arrow_c_stream__"
    )


def _parquet_files(reader: pa.RecordBatchReader) -> Iterator[io.BytesIO]:
    """The rows as Parquet files, each from about ``_FILE_SIZE`` bytes of Arrow data.

    A file is made when the previous one is consumed. Large batches are
    sliced, so one batch does not make one large file.
    """
    batches: list[pa.RecordBatch] = []
    size = 0
    for batch in reader:
        rows = max(1, batch.num_rows * _FILE_SIZE // max(batch.nbytes, 1))
        for offset in range(0, batch.num_rows, rows):
            batches.append(batch.slice(offset, rows))
            size += batches[-1].nbytes
            if size >= _FILE_SIZE:
                file = _parquet(reader.schema, batches)
                batches, size = [], 0
                yield file

    if batches:
        yield _parquet(reader.schema, batches)


def _parquet(schema: pa.Schema, batches: list[pa.RecordBatch]) -> io.BytesIO:
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_batches(batches, schema), buffer)
    buffer.seek(0)
    return buffer


def _snowflake_type(arrow_type: pa.DataType) -> str:
    """The Snowflake column type for an Arrow type, as Snowflake's ADBC driver maps it.

    Raises:
        TypeError: If Snowflake has no column type for ``arrow_type``.
    """
    t = arrow_type
    if pa.types.is_dictionary(t):
        return _snowflake_type(t.value_type)
    if isinstance(t, pa.BaseExtensionType):
        return _snowflake_type(t.storage_type)

    if pa.types.is_integer(t):
        return "NUMBER(38,0)"
    if pa.types.is_floating(t):
        return "FLOAT"
    if pa.types.is_decimal(t):
        return f"NUMBER({t.precision},{t.scale})"
    if pa.types.is_boolean(t):
        return "BOOLEAN"
    if (
        pa.types.is_string(t)
        or pa.types.is_large_string(t)
        or pa.types.is_string_view(t)
        or pa.types.is_null(t)
    ):
        return "VARCHAR"
    if (
        pa.types.is_binary(t)
        or pa.types.is_large_binary(t)
        or pa.types.is_binary_view(t)
        or pa.types.is_fixed_size_binary(t)
    ):
        return "BINARY"
    if pa.types.is_date(t):
        return "DATE"
    if pa.types.is_time(t):
        return f"TIME({_PRECISION[t.unit]})"
    if pa.types.is_timestamp(t):
        return f"TIMESTAMP_{'LTZ' if t.tz else 'NTZ'}({_PRECISION[t.unit]})"
    if (
        pa.types.is_list(t)
        or pa.types.is_large_list(t)
        or pa.types.is_fixed_size_list(t)
    ):
        return "ARRAY"
    if pa.types.is_struct(t) or pa.types.is_map(t):
        return "OBJECT"

    raise TypeError(f"Snowflake has no column type for Arrow type {t}")


_Reader = tuple[type, Callable[[pa.Table], Any]]


@cache
def _readers() -> list[_Reader]:
    """``(type, read)`` for each installed input type."""
    readers: list[_Reader] = [
        (pa.Table, lambda table: table),
        (pa.RecordBatchReader, lambda table: table.to_reader()),
    ]

    try:
        import polars as pl

        readers += [(pl.DataFrame, pl.DataFrame), (pl.LazyFrame, pl.LazyFrame)]
    except ImportError:
        pass

    try:
        import pandas as pd

        readers.append((pd.DataFrame, lambda table: table.to_pandas()))
    except ImportError:
        pass

    return readers


def _reader(target_type: type | None) -> Callable[[pa.Table], Any]:
    if isinstance(target_type, type):
        for supported, read in _readers():
            if issubclass(target_type, supported):
                return read

    names = [t.__name__ for t, _ in _readers()]
    if target_type is None:
        raise TypeError(
            f"No type_hint provided on InputContext. Supported types: {names}"
        )
    raise TypeError(
        f"SnowflakeIOHandler cannot read {target_type!r}. Supported types: {names}"
    )
