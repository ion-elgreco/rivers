"""First-class Delta asset base: standard maintenance verbs built in.

Subclass :class:`DeltaAsset`, define ``materialize``, and the asset carries
``optimize``, ``vacuum``, and ``delete`` actions resolved against its
:class:`~rivers.io_handlers.delta.DeltaIOHandler` — same URI and storage
options the write path uses. Override any verb in the subclass to replace it.
"""

from collections.abc import Callable
from typing import TYPE_CHECKING, TypeVar

from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError
from pydantic import BaseModel, ConfigDict

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

if TYPE_CHECKING:
    from rivers.io_handlers.delta import DeltaIOHandler

__all__ = ["DeltaAsset", "OptimizeConfig", "VacuumConfig"]

_T = TypeVar("_T")


class OptimizeConfig(BaseModel):
    """Config for :meth:`DeltaAsset.optimize`.

    Override per asset via ``run_action("optimize", config={"<asset>": {...}})``.

    Args:
        target_size: Desired file size in bytes (``None`` uses the table/
            deltalake default).
        z_order_by: Columns to z-order by; when set, optimize z-orders
            instead of plain compaction.
    """

    model_config = ConfigDict(extra="forbid")

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

    model_config = ConfigDict(extra="forbid")

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

    @classmethod
    def _keyed(cls, ctx: ActionContext, verb: str, build: Callable[..., _T]) -> _T:
        """``build(asset_metadata, partition)``, naming the missing metadata key."""
        try:
            return build(ctx.asset_metadata, ctx.partition)
        except ValueError as e:
            raise ValueError(
                f"keyed {verb} on '{ctx.asset_name}' needs the "
                f"'delta/partition_expr' metadata key to map the partition "
                f"to the table's partition columns"
            ) from e

    @action(
        outcome=Outcome.Unchanged,
        concurrency=ActionConcurrency.Exclusive,
        partitioning=ActionPartitioning.Optional,
        description="Compact small files (z-order when configured)",
    )
    @classmethod
    def optimize(cls, ctx: ActionContext[OptimizeConfig]) -> ActionResult:
        """Compact small files, or z-order when ``z_order_by`` is configured.

        The partition key is optional (``ActionPartitioning.Optional``): keyed
        runs rewrite only that partition's files (via
        :meth:`~rivers.io_handlers.delta.DeltaIOHandler.partition_filters`),
        keyless runs the whole table. Uses the writer/commit properties the
        write path resolves for this asset.

        Args:
            ctx: Action context; config via :class:`OptimizeConfig`.

        Returns:
            ``ActionResult.unchanged()`` with file metrics — also when the
            table does not exist yet, so fleet-wide runs skip
            never-materialized assets.

        Raises:
            ValueError: For a keyed optimize on an asset without the
                ``delta/partition_expr`` metadata key.
        """
        cfg = ctx.config or OptimizeConfig()
        try:
            handler, table = cls._delta(ctx)
        except TableNotFoundError:
            ctx.log.info("[optimize] no table for %s yet", ctx.asset_name)
            return ActionResult.unchanged()
        writer_properties, commit_properties = handler.resolved_properties(
            ctx.asset_metadata
        )
        filter_sets = (
            [None]
            if ctx.partition is None
            else cls._keyed(ctx, "optimize", handler.partition_filters)
        )
        files_added = files_removed = 0
        for filters in filter_sets:
            if cfg.z_order_by:
                metrics = table.optimize.z_order(
                    cfg.z_order_by,
                    partition_filters=filters,
                    target_size=cfg.target_size,
                    writer_properties=writer_properties,
                    commit_properties=commit_properties,
                )
            else:
                metrics = table.optimize.compact(
                    partition_filters=filters,
                    target_size=cfg.target_size,
                    writer_properties=writer_properties,
                    commit_properties=commit_properties,
                )
            ctx.log.info("[optimize] %s %s: %s", ctx.asset_name, filters or "", metrics)
            files_added += int(metrics.get("numFilesAdded", 0))
            files_removed += int(metrics.get("numFilesRemoved", 0))
        return ActionResult.unchanged(
            metadata={"files_added": files_added, "files_removed": files_removed}
        )

    @action(
        outcome=Outcome.Unchanged,
        concurrency=ActionConcurrency.Exclusive,
        partitioning=ActionPartitioning.Keyless,
        description="Remove files no longer referenced by the table",
    )
    @classmethod
    def vacuum(cls, ctx: ActionContext[VacuumConfig]) -> ActionResult:
        """Remove files no longer referenced by the table.

        Table-wide (``ActionPartitioning.Keyless``): runs without a partition
        key, on partitioned assets too. Vacuum commits carry the asset's
        resolved commit properties.

        Args:
            ctx: Action context; config via :class:`VacuumConfig`.

        Returns:
            ``ActionResult.unchanged()`` with the deleted-file count — also
            when the table does not exist yet.
        """
        if ctx.partition is not None:
            raise ValueError("vacuum is table-wide; run it without a partition key")
        cfg = ctx.config or VacuumConfig()
        try:
            handler, table = cls._delta(ctx)
        except TableNotFoundError:
            ctx.log.info("[vacuum] no table for %s yet", ctx.asset_name)
            return ActionResult.unchanged()
        _, commit_properties = handler.resolved_properties(ctx.asset_metadata)
        removed = table.vacuum(
            retention_hours=cfg.retention_hours,
            enforce_retention_duration=cfg.enforce_retention_duration,
            dry_run=False,
            commit_properties=commit_properties,
        )
        ctx.log.info("[vacuum] %s: removed %d files", ctx.asset_name, len(removed))
        return ActionResult.unchanged(metadata={"files_deleted": len(removed)})

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

        The partition key is optional (``ActionPartitioning.Optional``): keyed
        runs delete one partition's rows via ``partition_predicate``, keyless
        runs delete every row. Returning ``None`` applies the declared
        ``Unmaterialize`` outcome — including when the physical table is
        already gone, so dangling state stays clearable.

        Args:
            ctx: Action context.

        Raises:
            ValueError: For a keyed delete on an asset without the
                ``delta/partition_expr`` metadata key (nothing maps the
                partition to rows).
        """
        try:
            handler, table = cls._delta(ctx)
        except TableNotFoundError:
            ctx.log.info(
                "[delete] no table for %s — clearing state only", ctx.asset_name
            )
            return None
        writer_properties, commit_properties = handler.resolved_properties(
            ctx.asset_metadata
        )
        if ctx.partition is not None:
            predicate = cls._keyed(ctx, "delete", handler.partition_predicate)
            table.delete(
                predicate,
                writer_properties=writer_properties,
                commit_properties=commit_properties,
            )
            ctx.log.info(
                "[delete] %s: deleted rows where %s", ctx.asset_name, predicate
            )
        else:
            table.delete(
                writer_properties=writer_properties,
                commit_properties=commit_properties,
            )
            ctx.log.info("[delete] %s: deleted all rows", ctx.asset_name)
        return None
