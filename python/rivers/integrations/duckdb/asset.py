"""Asset bases for DuckDB and DuckLake tables, with maintenance actions built in.

Subclass :class:`DuckDBAsset` or :class:`DuckLakeAsset`, define
``materialize``, and the asset carries the actions below, run against its
:class:`~rivers.integrations.duckdb.DuckDBIOHandler`. Override a verb in the
subclass to replace it.
"""

from __future__ import annotations

import duckdb

from rivers._core.assets import (
    ActionConcurrency,
    ActionContext,
    ActionOrdering,
    ActionPartitioning,
    ActionResult,
    Asset,
    Outcome,
    action,
)
from rivers.integrations.duckdb._sql import quote_table
from rivers.integrations.duckdb.io_handler import DuckDBIOHandler, table_exists
from rivers.integrations.duckdb.resource import lake_options

__all__ = ["DuckDBAsset", "DuckLakeAsset"]

_RETENTION_OPTIONS = ("expire_older_than", "delete_older_than")


class DuckDBAsset(Asset):
    """Asset base for DuckDB tables, with a ``delete`` action.

    The asset must resolve a ``DuckDBIOHandler`` (its own ``io_handler`` or
    the repository default).
    """

    kinds = "duckdb"

    @classmethod
    def _handler(cls, ctx: ActionContext) -> DuckDBIOHandler:
        handler = ctx.io_handler
        if not isinstance(handler, DuckDBIOHandler):
            raise TypeError(
                f"'{ctx.asset_name}': {cls.__name__} actions need a DuckDBIOHandler, "
                f"got {type(handler).__name__}"
            )
        return handler

    @action(
        outcome=Outcome.Unmaterialize,
        concurrency=ActionConcurrency.Exclusive,
        ordering=ActionOrdering.DownstreamFirst,
        partitioning=ActionPartitioning.Optional,
        description="Delete rows (partition-scoped with a key) and clear state",
    )
    @classmethod
    def delete(cls, ctx: ActionContext) -> None:
        """Delete rows and clear materialization state.

        With a partition key, deletes that partition's rows (needs
        ``duckdb/partition_expr``); without one, deletes every row. A missing
        table only clears the state.

        Raises:
            ValueError: For a keyed delete on an asset without
                ``duckdb/partition_expr``.
        """
        handler = cls._handler(ctx)
        schema, table = handler.table_name(ctx.asset_name, ctx.asset_metadata)
        target = quote_table(schema, table)

        predicate = None
        if ctx.partition is not None:
            predicate = handler.partition_predicate(ctx.asset_metadata, ctx.partition)

        with handler.resource.get_connection() as con:
            if not table_exists(con, schema, table):
                ctx.log.info(
                    "[delete] no table %s.%s — clearing state only", schema, table
                )
                return None

            if predicate is None:
                con.execute(f"DELETE FROM {target}")
                ctx.log.info("[delete] %s.%s: deleted all rows", schema, table)
            else:
                con.execute(f"DELETE FROM {target} WHERE {predicate}")
                ctx.log.info(
                    "[delete] %s.%s: deleted rows where %s", schema, table, predicate
                )

        return None


class DuckLakeAsset(DuckDBAsset):
    """Asset base for DuckLake tables: ``delete``, ``optimize``, and ``vacuum``.

    The actions take no config. They read DuckLake's options: set table
    options with the ``ducklake/options`` metadata key, and lake-wide options
    with ``DuckLake(options=...)``.
    """

    kinds = "ducklake"

    @classmethod
    def _ducklake(cls, ctx: ActionContext) -> tuple[DuckDBIOHandler, str]:
        handler = cls._handler(ctx)
        if handler.resource.ducklake is None:
            raise TypeError(
                f"'{ctx.asset_name}': DuckLakeAsset actions need a DuckDBResource "
                "with ducklake"
            )
        return handler, handler.resource.ducklake.name

    @action(
        outcome=Outcome.Unchanged,
        concurrency=ActionConcurrency.Exclusive,
        partitioning=ActionPartitioning.Keyless,
        description="Flush inlined rows, rewrite files with deletes, merge small files",
    )
    @classmethod
    def optimize(cls, ctx: ActionContext) -> ActionResult:
        """Flush inlined rows to Parquet, rewrite files with deletes, and merge small files.

        Uses the table's ``rewrite_delete_threshold`` and ``target_file_size``
        options.

        Returns:
            ``ActionResult.unchanged()`` with the counts of rows flushed, files
            rewritten, and files merged.
        """
        handler, catalog = cls._ducklake(ctx)
        schema, table = handler.table_name(ctx.asset_name, ctx.asset_metadata)
        with handler.resource.get_connection() as con:
            if not table_exists(con, schema, table):
                ctx.log.info("[optimize] no table %s.%s yet", schema, table)
                return ActionResult.unchanged()

            flushed = _fetch_int(
                con,
                "SELECT coalesce(sum(rows_flushed), 0) FROM "
                "ducklake_flush_inlined_data(?, schema_name => ?, table_name => ?)",
                [catalog, schema, table],
            )
            rewritten = _fetch_int(
                con,
                "SELECT coalesce(sum(files_processed), 0) FROM "
                "ducklake_rewrite_data_files(?, ?, schema => ?)",
                [catalog, table, schema],
            )
            merged = _fetch_int(
                con,
                "SELECT coalesce(sum(files_processed), 0) FROM "
                "ducklake_merge_adjacent_files(?, ?, schema => ?)",
                [catalog, table, schema],
            )

        ctx.log.info(
            "[optimize] %s.%s: %d rows flushed, %d files rewritten, %d files merged",
            schema,
            table,
            flushed,
            rewritten,
            merged,
        )

        return ActionResult.unchanged(
            metadata={
                "rows_flushed": flushed,
                "files_rewritten": rewritten,
                "files_merged": merged,
            }
        )

    @action(
        outcome=Outcome.Unchanged,
        concurrency=ActionConcurrency.Exclusive,
        partitioning=ActionPartitioning.Keyless,
        description="Expire old snapshots and delete their files (whole lake)",
    )
    @classmethod
    def vacuum(cls, ctx: ActionContext) -> ActionResult:
        """Expire old snapshots and delete files no snapshot uses, for the whole lake.

        DuckLake expires snapshots per catalog, so this acts on every table.
        Uses the lake's ``expire_older_than`` and ``delete_older_than``
        options; without them it does nothing.

        Returns:
            ``ActionResult.unchanged()`` with the counts of snapshots expired
            and files deleted.
        """
        handler, catalog = cls._ducklake(ctx)
        with handler.resource.get_connection() as con:
            if not lake_options(con, catalog).keys() & set(_RETENTION_OPTIONS):
                ctx.log.info(
                    "[vacuum] catalog %s sets neither %s nor %s: nothing to do",
                    catalog,
                    *_RETENTION_OPTIONS,
                )
                return ActionResult.unchanged(
                    metadata={"snapshots_expired": 0, "files_deleted": 0}
                )

            expired = _fetch_int(
                con, "SELECT count(*) FROM ducklake_expire_snapshots(?)", [catalog]
            )
            deleted = _fetch_int(
                con, "SELECT count(*) FROM ducklake_cleanup_old_files(?)", [catalog]
            )

        ctx.log.info(
            "[vacuum] catalog %s: %d snapshots expired, %d files deleted",
            catalog,
            expired,
            deleted,
        )

        return ActionResult.unchanged(
            metadata={"snapshots_expired": expired, "files_deleted": deleted}
        )


def _fetch_int(con: duckdb.DuckDBPyConnection, sql: str, params: list[str]) -> int:
    [(value,)] = con.execute(sql, params).fetchall()
    return int(value)
