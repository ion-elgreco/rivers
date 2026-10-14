"""First-class Iceberg asset base: maintenance actions built in.

Subclass :class:`IcebergAsset`, define ``materialize``, and the asset carries
``delete`` and ``expire_snapshots`` actions, run against its
:class:`~rivers.io_handlers.iceberg.IcebergIOHandler`: the same catalog,
table, and snapshot properties the write path uses. Override a verb in the
subclass to replace it.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from itertools import islice
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table
from pyiceberg.table.snapshots import ancestors_of

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
    from rivers.io_handlers.iceberg import IcebergIOHandler

__all__ = ["ExpireSnapshotsConfig", "IcebergAsset"]

# Iceberg's table properties for snapshot expiry, with their defaults
_MAX_SNAPSHOT_AGE_MS = ("history.expire.max-snapshot-age-ms", 432_000_000)
_MIN_SNAPSHOTS_TO_KEEP = ("history.expire.min-snapshots-to-keep", 1)


class ExpireSnapshotsConfig(BaseModel):
    """Config for :meth:`IcebergAsset.expire_snapshots`.

    Override per asset via ``run_action("expire_snapshots", config=...)``.

    Args:
        retention_hours: Expire snapshots older than this. ``None`` uses the
            table's ``history.expire.max-snapshot-age-ms`` property (5 days
            by default).
        retain_last: Keep this many of the newest snapshots in the table's
            history, whatever their age. ``None`` uses the
            ``history.expire.min-snapshots-to-keep`` property (1 by default).
    """

    model_config = ConfigDict(extra="forbid")

    retention_hours: float | None = None
    retain_last: int | None = None


class IcebergAsset(Asset):
    """Asset base for Iceberg tables with ``delete`` and ``expire_snapshots``.

    Subclasses define ``materialize`` (and any custom verbs). The verbs need
    the asset to resolve an ``IcebergIOHandler`` (its own ``io_handler`` or
    the repository default).
    """

    kinds = "iceberg"

    @classmethod
    def _iceberg(cls, ctx: ActionContext) -> tuple[IcebergIOHandler, Table]:
        """Resolve the handler and load the asset's table."""
        from rivers.io_handlers.iceberg import IcebergIOHandler

        handler = ctx.io_handler
        if not isinstance(handler, IcebergIOHandler):
            raise TypeError(
                f"'{ctx.asset_name}': IcebergAsset verbs need an IcebergIOHandler, "
                f"got {type(handler).__name__}"
            )
        return handler, handler.load_table(ctx.asset_name, ctx.asset_metadata)

    @action(
        outcome=Outcome.Unchanged,
        concurrency=ActionConcurrency.Exclusive,
        partitioning=ActionPartitioning.Keyless,
        description="Expire old snapshots (their data files stay)",
    )
    @classmethod
    def expire_snapshots(
        cls, ctx: ActionContext[ExpireSnapshotsConfig]
    ) -> ActionResult:
        """Remove old snapshots from the table's metadata.

        Table-wide (``ActionPartitioning.Keyless``). Branch and tag heads stay,
        and so do the newest ``retain_last`` snapshots of the table's history.
        pyiceberg removes the snapshots only: their data files stay in storage.

        Args:
            ctx: Action context; config via :class:`ExpireSnapshotsConfig`.

        Returns:
            ``ActionResult.unchanged()`` with the count of expired snapshots,
            also when the table does not exist yet.
        """
        if ctx.partition is not None:
            raise ValueError(
                "expire_snapshots is table-wide; run it without a partition key"
            )
        cfg = ctx.config or ExpireSnapshotsConfig()
        try:
            _, table = cls._iceberg(ctx)
        except NoSuchTableError:
            ctx.log.info("[expire_snapshots] no table for %s yet", ctx.asset_name)
            return ActionResult.unchanged(metadata={"snapshots_expired": 0})

        if cfg.retention_hours is None:
            max_age_ms = int(table.properties.get(*_MAX_SNAPSHOT_AGE_MS))
        else:
            max_age_ms = int(cfg.retention_hours * 3_600_000)
        retain_last = cfg.retain_last
        if retain_last is None:
            retain_last = int(table.properties.get(*_MIN_SNAPSHOTS_TO_KEEP))

        cutoff = datetime.now(timezone.utc) - timedelta(milliseconds=max_age_ms)
        keep = {ref.snapshot_id for ref in table.metadata.refs.values()}
        keep.update(
            snapshot.snapshot_id
            for snapshot in islice(
                ancestors_of(table.current_snapshot(), table.metadata), retain_last
            )
        )
        expired = [
            snapshot.snapshot_id
            for snapshot in table.snapshots()
            if snapshot.timestamp_ms < cutoff.timestamp() * 1000
            and snapshot.snapshot_id not in keep
        ]
        if expired:
            table.maintenance.expire_snapshots().by_ids(expired).commit()

        ctx.log.info(
            "[expire_snapshots] %s: expired %d snapshots", ctx.asset_name, len(expired)
        )
        return ActionResult.unchanged(metadata={"snapshots_expired": len(expired)})

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
        runs delete that partition's rows, keyless runs delete every row.
        Returning ``None`` applies the declared ``Unmaterialize`` outcome,
        also when the table is already gone, so dangling state stays
        clearable. The delete commit carries the asset's snapshot properties.

        Args:
            ctx: Action context.

        Raises:
            ValueError: For a keyed delete on an asset without the
                ``iceberg/partition_expr`` metadata key.
        """
        try:
            handler, table = cls._iceberg(ctx)
        except NoSuchTableError:
            ctx.log.info(
                "[delete] no table for %s — clearing state only", ctx.asset_name
            )
            return None

        snapshot_properties = handler.resolved_snapshot_properties(ctx.asset_metadata)
        if ctx.partition is None:
            table.delete(snapshot_properties=snapshot_properties)
            ctx.log.info("[delete] %s: deleted all rows", ctx.asset_name)
            return None

        row_filter = handler.partition_filter(
            ctx.asset_metadata, ctx.partition, table.schema()
        )
        table.delete(row_filter, snapshot_properties=snapshot_properties)
        ctx.log.info("[delete] %s: deleted rows where %s", ctx.asset_name, row_filter)
        return None
