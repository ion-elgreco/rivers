"""Pydantic models describing Delta IO partitioning and MERGE configuration."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class PartitionExpr(BaseModel):
    """Maps partition dimensions to Delta table column names.

    For single-partition assets, ``expr`` is a column name string.
    For multi-partition assets, ``expr`` is a dict mapping dimension names to column names.
    """

    expr: str | dict[str, str]

    @property
    def partition_columns(self) -> list[str]:
        """Flatten ``expr`` into the list of physical column names used to partition the table."""
        if isinstance(self.expr, str):
            return [self.expr]
        return list(self.expr.values())


_MergeType = Literal[
    "deduplicate_insert",
    "update_only",
    "upsert",
    "replace_delete_unmatched",
    "custom",
]


class WhenNotMatchedInsert(BaseModel):
    """Insert clause for source rows with no target match — explicit column updates."""

    predicate: str | None = None
    updates: dict[str, str]


class WhenNotMatchedInsertAll(BaseModel):
    """Insert clause for source rows with no target match — copy all columns (minus ``except_cols``)."""

    predicate: str | None = None
    except_cols: list[str] | None = None


class WhenMatchedUpdate(BaseModel):
    """Update clause applied when the merge predicate matches — explicit column updates."""

    predicate: str | None = None
    updates: dict[str, str]


class WhenMatchedUpdateAll(BaseModel):
    """Update clause applied when the merge predicate matches — copy all columns (minus ``except_cols``)."""

    predicate: str | None = None
    except_cols: list[str] | None = None


class WhenMatchedDelete(BaseModel):
    """Delete clause applied when the merge predicate matches."""

    predicate: str | None = None


class WhenNotMatchedBySourceDelete(BaseModel):
    """Delete clause applied to target rows that have no matching source row."""

    predicate: str | None = None


class WhenNotMatchedBySourceUpdate(BaseModel):
    """Update clause applied to target rows that have no matching source row."""

    predicate: str | None = None
    updates: dict[str, str]


class MergeOperationsConfig(BaseModel):
    """Fine-grained configuration for custom MERGE operations.

    Each field is a list of operations applied in sequence to the TableMerger.
    """

    when_not_matched_insert: list[WhenNotMatchedInsert] | None = None
    when_not_matched_insert_all: list[WhenNotMatchedInsertAll] | None = None
    when_matched_update: list[WhenMatchedUpdate] | None = None
    when_matched_update_all: list[WhenMatchedUpdateAll] | None = None
    when_matched_delete: list[WhenMatchedDelete] | None = None
    when_not_matched_by_source_delete: list[WhenNotMatchedBySourceDelete] | None = None
    when_not_matched_by_source_update: list[WhenNotMatchedBySourceUpdate] | None = None


class MergeConfig(BaseModel):
    """Configuration for MERGE INTO operations.

    Args:
        merge_type: The type of merge to perform.
        predicate: SQL merge condition (e.g. ``"s.id = t.id"``).
        source_alias: Alias for the source table (default ``"s"``).
        target_alias: Alias for the target table (default ``"t"``).
        error_on_type_mismatch: Fail if source/target types differ (default True).
        operations: Fine-grained merge operations (required when merge_type='custom').
    """

    merge_type: _MergeType
    predicate: str
    source_alias: str = "s"
    target_alias: str = "t"
    error_on_type_mismatch: bool = True
    operations: MergeOperationsConfig | None = None
