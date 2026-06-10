"""Helpers that translate :class:`MergeConfig` into a Delta-Lake MERGE call."""

from __future__ import annotations

from typing import Any

from delta._typing import ColumnMapping
from delta.tables import DeltaTable as SparkDeltaTable
from deltalake import CommitProperties, DeltaTable, WriterProperties
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

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


def _to_spark_column_mapping(mapping: dict[str, str]) -> ColumnMapping:
    """Returns a ColumnMapping representation of specified dict."""

    return {col: F.expr(expr) for col, expr in mapping.items()}


def _build_updates_mapping(
    *,
    sdf: SparkDataFrame,
    source_alias: str,
    except_cols: list[str],
) -> ColumnMapping:
    """Builds updates mapping to exclude columns specified by exclude_cols."""
    return _to_spark_column_mapping(
        {
            col.name: f"{source_alias}.`{col.name}`"
            for col in sdf.schema
            if col.name not in except_cols
        }
    )


def _merge_execute_spark(
    *,
    uri: str,
    sdf: SparkDataFrame,
    merge_config: MergeConfig | None,
    partition_predicate: str | None,
    merge_predicate_override: str | None = None,
) -> dict[str, Any]:
    """Execute a Delta Lake merge using delta-spark, returns a metrics dict."""

    if merge_config is None:
        raise ValueError("merge_config is required when mode='merge'")

    predicate = merge_predicate_override or merge_config.predicate

    if partition_predicate is not None:
        predicate = f"({predicate}) AND ({partition_predicate})"

    if not predicate.strip():
        raise ValueError("Merge predicate cannot be empty.")

    spark_session = sdf.sparkSession

    if not SparkDeltaTable.isDeltaTable(spark_session, uri):
        sdf_count = sdf.count()
        sdf.write.format("delta").mode("overwrite").save(uri)
        return {
            "numOutputRows": sdf_count,
            "numTargetRowsInserted": sdf_count,
            "numTargetRowsUpdated": 0,
            "numTargetRowsDeleted": 0,
            "version": 0,
        }

    delta_table = SparkDeltaTable.forPath(spark_session, uri)

    merge_builder = delta_table.alias(merge_config.target_alias).merge(
        sdf.alias(merge_config.source_alias),
        predicate,
    )

    mt = merge_config.merge_type

    if mt == "update_only":
        merge_builder = merge_builder.whenMatchedUpdateAll()
    elif mt == "deduplicate_insert":
        merge_builder = merge_builder.whenNotMatchedInsertAll()
    elif mt == "upsert":
        merge_builder = merge_builder.whenMatchedUpdateAll().whenNotMatchedInsertAll()
    elif mt == "replace_delete_unmatched":
        merge_builder = (
            merge_builder.whenMatchedUpdateAll().whenNotMatchedBySourceDelete()
        )
    elif mt == "custom":
        ops = merge_config.operations

        if ops is None:
            raise ValueError("operations is required when merge_type='custom'")

        # when matched update
        if ops.when_matched_update is not None:
            for op in ops.when_matched_update:
                merge_builder = merge_builder.whenMatchedUpdate(
                    condition=op.predicate,
                    set=_to_spark_column_mapping(op.updates),
                )

        # when matched update all
        if ops.when_matched_update_all is not None:
            for op in ops.when_matched_update_all:
                kwargs = {}
                if op.predicate:
                    kwargs["condition"] = op.predicate
                if op.except_cols:
                    kwargs["set"] = _build_updates_mapping(
                        sdf=sdf,
                        source_alias=merge_config.source_alias,
                        except_cols=op.except_cols,
                    )
                    merge_builder = merge_builder.whenMatchedUpdate(**kwargs)
                else:
                    merge_builder = merge_builder.whenMatchedUpdateAll(**kwargs)

        # when matched delete
        if ops.when_matched_delete is not None:
            for op in ops.when_matched_delete:
                kwargs = {}
                if op.predicate:
                    kwargs["condition"] = op.predicate
                merge_builder = merge_builder.whenMatchedDelete(**kwargs)

        # when not matched insert
        if ops.when_not_matched_insert is not None:
            for op in ops.when_not_matched_insert:
                kwargs = {}
                values = _to_spark_column_mapping(op.updates)
                if op.predicate:
                    kwargs["condition"] = op.predicate
                if values:
                    kwargs["values"] = values
                merge_builder = merge_builder.whenNotMatchedInsert(**kwargs)

        # when not matched insert all
        if ops.when_not_matched_insert_all is not None:
            for op in ops.when_not_matched_insert_all:
                kwargs = {}
                if op.predicate:
                    kwargs["condition"] = op.predicate
                if op.except_cols:
                    kwargs["set"] = _build_updates_mapping(
                        sdf=sdf,
                        source_alias=merge_config.source_alias,
                        except_cols=op.except_cols,
                    )
                    merge_builder = merge_builder.whenNotMatchedInsert(**kwargs)
                else:
                    merge_builder = merge_builder.whenNotMatchedInsertAll(**kwargs)

        # when not matched by source delete
        if ops.when_not_matched_by_source_delete is not None:
            for op in ops.when_not_matched_by_source_delete:
                kwargs = {}
                if op.predicate:
                    kwargs["condition"] = op.predicate
                merge_builder = merge_builder.whenNotMatchedBySourceDelete(**kwargs)

        # when not matched by source update
        if ops.when_not_matched_by_source_update is not None:
            for op in ops.when_not_matched_by_source_update:
                kwargs = {}
                set = _to_spark_column_mapping(op.updates)
                if op.predicate:
                    kwargs["condition"] = op.predicate
                if set:
                    kwargs["set"] = set
                merge_builder = merge_builder.whenNotMatchedBySourceUpdate(**kwargs)

    else:
        raise ValueError(f"Unknown merge_type: {mt!r}")

    merge_builder.execute()

    hist = delta_table.history(1).collect()[0]
    metrics = dict(hist.operationMetrics or {})
    metrics["version"] = hist.version
    return metrics
