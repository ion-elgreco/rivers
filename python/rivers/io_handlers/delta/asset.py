"""First-class Delta asset base: standard maintenance verbs built in.

Subclass :class:`DeltaAsset`, define ``materialize``, and the asset carries
``optimize``, ``vacuum``, and ``delete`` actions resolved against its
:class:`~rivers.io_handlers.delta.DeltaIOHandler` — same URI and storage
options the write path uses. Override any verb in the subclass to replace it.
"""

# No `from __future__ import annotations`: the executor reads raw
# `__annotations__` to detect the context parameter's config generic, so
# `ActionContext[...]` must stay a real object, not a string.
from typing import TYPE_CHECKING

from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError
from pydantic import BaseModel

from rivers._core.assets import (
    ActionConcurrency,
    ActionContext,
    ActionOrdering,
    ActionResult,
    Asset,
    Outcome,
    action,
)

if TYPE_CHECKING:
    from rivers.io_handlers.delta import DeltaIOHandler

__all__ = ["DeltaAsset", "OptimizeConfig", "VacuumConfig"]


class OptimizeConfig(BaseModel):
    """Config for :meth:`DeltaAsset.optimize`.

    Override per asset via ``run_action("optimize", config={"<asset>": {...}})``.

    Args:
        target_size: Desired file size in bytes (``None`` uses the table/
            deltalake default).
        z_order_by: Columns to z-order by; when set, optimize z-orders
            instead of plain compaction.
    """

    target_size: int | None = None
    z_order_by: list[str] | None = None


class VacuumConfig(BaseModel):
    """Config for :meth:`DeltaAsset.vacuum`.

    Args:
        retention_hours: Files older than this are removed (``None`` uses the
            table's configured retention, 168h by default).
        enforce_retention_duration: Refuse retention windows shorter than the
            table's configured minimum. Disable only when you know the table
            has no readers pinned to old versions.
    """

    retention_hours: int | None = None
    enforce_retention_duration: bool = True


class DeltaAsset(Asset):
    """Asset base for Delta tables with the standard maintenance verbs.

    Subclasses define ``materialize`` (and any custom verbs); ``optimize``,
    ``vacuum``, and ``delete`` come built in and appear as actions in the UI
    and ``run_action``. The verbs require the asset to resolve a
    ``DeltaIOHandler`` (own ``io_handler`` or the repository default).
    """

    kinds = "delta"

    @classmethod
    def _delta(cls, ctx: ActionContext) -> "tuple[DeltaIOHandler, DeltaTable]":
        """Resolve the handler and open the asset's Delta table."""
        from rivers.io_handlers.delta import DeltaIOHandler

        handler = ctx.io_handler
        if not isinstance(handler, DeltaIOHandler):
            raise TypeError(
                f"'{ctx.asset_name}': DeltaAsset verbs need a DeltaIOHandler, "
                f"got {type(handler).__name__}"
            )
        table = DeltaTable(
            handler.asset_table_uri(ctx.asset_name, ctx.asset_metadata),
            storage_options=handler.storage_options,
        )
        return handler, table

    @action(
        outcome=Outcome.Unchanged,
        concurrency=ActionConcurrency.Exclusive,
        description="Compact small files (z-order when configured)",
    )
    @classmethod
    def optimize(cls, ctx: ActionContext[OptimizeConfig]) -> ActionResult:
        if ctx.partition is not None:
            raise ValueError("optimize is table-wide; run it without a partition key")
        cfg = ctx.config or OptimizeConfig()
        try:
            _, table = cls._delta(ctx)
        except TableNotFoundError:
            ctx.log.info("[optimize] no table for %s yet", ctx.asset_name)
            return ActionResult.unchanged()
        if cfg.z_order_by:
            metrics = table.optimize.z_order(
                cfg.z_order_by, target_size=cfg.target_size
            )
        else:
            metrics = table.optimize.compact(target_size=cfg.target_size)
        ctx.log.info("[optimize] %s: %s", ctx.asset_name, metrics)
        return ActionResult.unchanged(
            metadata={
                "files_added": int(metrics.get("numFilesAdded", 0)),
                "files_removed": int(metrics.get("numFilesRemoved", 0)),
            }
        )

    @action(
        outcome=Outcome.Unchanged,
        concurrency=ActionConcurrency.Exclusive,
        description="Remove files no longer referenced by the table",
    )
    @classmethod
    def vacuum(cls, ctx: ActionContext[VacuumConfig]) -> ActionResult:
        if ctx.partition is not None:
            raise ValueError("vacuum is table-wide; run it without a partition key")
        cfg = ctx.config or VacuumConfig()
        try:
            _, table = cls._delta(ctx)
        except TableNotFoundError:
            ctx.log.info("[vacuum] no table for %s yet", ctx.asset_name)
            return ActionResult.unchanged()
        removed = table.vacuum(
            retention_hours=cfg.retention_hours,
            enforce_retention_duration=cfg.enforce_retention_duration,
            dry_run=False,
        )
        ctx.log.info("[vacuum] %s: removed %d files", ctx.asset_name, len(removed))
        return ActionResult.unchanged(metadata={"files_deleted": len(removed)})

    @action(
        outcome=Outcome.Unmaterialize,
        concurrency=ActionConcurrency.Exclusive,
        ordering=ActionOrdering.DownstreamFirst,
        description="Delete rows (partition-scoped with a key) and clear state",
    )
    @classmethod
    def delete(cls, ctx: ActionContext) -> ActionResult | None:
        try:
            handler, table = cls._delta(ctx)
        except TableNotFoundError:
            ctx.log.info("[delete] no table for %s — nothing to delete", ctx.asset_name)
            return ActionResult.unchanged()
        if ctx.partition is not None:
            predicate = handler.partition_predicate(ctx.asset_metadata, ctx.partition)
            table.delete(predicate)
            ctx.log.info(
                "[delete] %s: deleted rows where %s", ctx.asset_name, predicate
            )
        else:
            table.delete()
            ctx.log.info("[delete] %s: deleted all rows", ctx.asset_name)
        return None
