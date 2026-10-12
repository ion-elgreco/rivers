"""Asset base for Snowflake tables, with a ``delete`` action built in.

Subclass :class:`SnowflakeAsset`, define ``materialize``, and the asset carries
a ``delete`` action, run against its
:class:`~rivers.integrations.snowflake.SnowflakeIOHandler`. Override it in the
subclass to replace it.
"""

from rivers._core.assets import (
    ActionConcurrency,
    ActionContext,
    ActionOrdering,
    ActionPartitioning,
    Asset,
    Outcome,
    action,
)
from rivers.integrations.snowflake.io_handler import (
    SnowflakeIOHandler,
    _quote_table,
    _table_columns,
)

__all__ = ["SnowflakeAsset"]


class SnowflakeAsset(Asset):
    """Asset base for Snowflake tables, with a ``delete`` action.

    The asset must resolve a ``SnowflakeIOHandler`` (its own ``io_handler`` or
    the repository default). There is no ``optimize`` or ``vacuum``: Snowflake
    merges small micro-partitions itself, and frees old storage when Time
    Travel and Fail-safe end.
    """

    kinds = "snowflake"

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
        ``snowflake/partition_expr``); without one, deletes every row. The
        table keeps its columns, grants, and policies. A missing table only
        clears the state.

        Raises:
            TypeError: If the asset's IO handler is not a ``SnowflakeIOHandler``.
            ValueError: For a keyed delete on an asset without
                ``snowflake/partition_expr``.
        """
        handler = ctx.io_handler
        if not isinstance(handler, SnowflakeIOHandler):
            raise TypeError(
                f"'{ctx.asset_name}': {cls.__name__} actions need a SnowflakeIOHandler, "
                f"got {type(handler).__name__}"
            )
        schema, table = handler.table_name(ctx.asset_name, ctx.asset_metadata)

        where = ""
        if ctx.partition is not None:
            predicate = handler.partition_predicate(ctx.asset_metadata, ctx.partition)
            where = f" WHERE {predicate}"

        with handler.get_connection() as con:
            cur = con.cursor()
            if not _table_columns(cur, schema, table):
                ctx.log.info(
                    "[delete] no table %s.%s — clearing state only", schema, table
                )
                return None

            cur.execute(f"DELETE FROM {_quote_table(schema, table)}{where}")
            deleted = cur.rowcount

        ctx.log.info("[delete] %s.%s: deleted %d rows%s", schema, table, deleted, where)
        return None
