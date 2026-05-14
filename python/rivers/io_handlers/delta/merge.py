"""Helpers that translate :class:`MergeConfig` into a Delta-Lake MERGE call."""

from __future__ import annotations

from typing import Any

from deltalake import CommitProperties, DeltaTable, WriterProperties

from rivers.io_handlers.delta.config import MergeConfig


def _merge_execute(
    uri: str,
    data: object,
    merge_config: MergeConfig | None,
    partition_predicate: str | None,
    storage_options: dict[str, str] | None,
    writer_properties: WriterProperties | None,
    commit_properties: CommitProperties | None,
    merge_predicate_override: str | None = None,
) -> dict[str, Any]:
    """Execute a MERGE on the table at ``uri`` and return the deltalake stats dict.

    AND-combines the per-partition predicate (when present) with the configured
    merge predicate, then dispatches to the right ``TableMerger`` clause sequence
    based on ``merge_config.merge_type``. ``"custom"`` walks
    :class:`MergeOperationsConfig` and applies each declared clause in order.

    Raises:
        ValueError: When ``merge_config`` is ``None``, the merge type is
            ``"custom"`` without ``operations``, or the type is unknown.
    """
    if merge_config is None:
        raise ValueError("merge_config is required when mode='merge'")

    dt = DeltaTable(uri, storage_options=storage_options)
    predicate = merge_predicate_override or merge_config.predicate
    if partition_predicate is not None:
        predicate = f"{predicate} AND {partition_predicate}"

    merger = dt.merge(
        source=data,  # type: ignore[arg-type]
        predicate=predicate,
        source_alias=merge_config.source_alias,
        target_alias=merge_config.target_alias,
        error_on_type_mismatch=merge_config.error_on_type_mismatch,
        writer_properties=writer_properties,
        commit_properties=commit_properties,
    )

    mt = merge_config.merge_type
    if mt == "update_only":
        return merger.when_matched_update_all().execute()
    elif mt == "deduplicate_insert":
        return merger.when_not_matched_insert_all().execute()
    elif mt == "upsert":
        return merger.when_matched_update_all().when_not_matched_insert_all().execute()
    elif mt == "replace_delete_unmatched":
        return (
            merger.when_matched_update_all()
            .when_not_matched_by_source_delete()
            .execute()
        )
    elif mt == "custom":
        ops = merge_config.operations
        if ops is None:
            raise ValueError("operations is required when merge_type='custom'")
        if ops.when_matched_update is not None:
            for op in ops.when_matched_update:
                merger = merger.when_matched_update(op.updates, predicate=op.predicate)
        if ops.when_matched_update_all is not None:
            for op in ops.when_matched_update_all:
                merger = merger.when_matched_update_all(
                    predicate=op.predicate, except_cols=op.except_cols
                )
        if ops.when_matched_delete is not None:
            for op in ops.when_matched_delete:
                merger = merger.when_matched_delete(predicate=op.predicate)
        if ops.when_not_matched_insert is not None:
            for op in ops.when_not_matched_insert:
                merger = merger.when_not_matched_insert(
                    op.updates, predicate=op.predicate
                )
        if ops.when_not_matched_insert_all is not None:
            for op in ops.when_not_matched_insert_all:
                merger = merger.when_not_matched_insert_all(
                    predicate=op.predicate, except_cols=op.except_cols
                )
        if ops.when_not_matched_by_source_delete is not None:
            for op in ops.when_not_matched_by_source_delete:
                merger = merger.when_not_matched_by_source_delete(
                    predicate=op.predicate
                )
        if ops.when_not_matched_by_source_update is not None:
            for op in ops.when_not_matched_by_source_update:
                merger = merger.when_not_matched_by_source_update(
                    op.updates, predicate=op.predicate
                )
        return merger.execute()
    else:
        raise ValueError(f"Unknown merge_type: {mt!r}")
