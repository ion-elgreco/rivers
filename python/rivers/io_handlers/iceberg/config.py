"""Pydantic model for Iceberg upserts."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class UpsertConfig(BaseModel):
    """How ``mode="upsert"`` matches rows, with pyiceberg's ``Table.upsert``.

    Args:
        join_cols: Columns that identify a row. ``None`` uses the table's
            identifier fields. The ``iceberg/join_cols`` metadata key
            overrides it per asset.
        when_matched_update_all: Update matched rows whose values changed.
        when_not_matched_insert_all: Insert rows without a match.
    """

    model_config = ConfigDict(extra="forbid")

    join_cols: list[str] | None = None
    when_matched_update_all: bool = True
    when_not_matched_insert_all: bool = True
