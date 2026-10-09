"""SQL text built from resource config: identifiers, secrets, and the DuckLake ATTACH."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import duckdb

if TYPE_CHECKING:
    from rivers.integrations.duckdb.resource import DuckLake


def quote_ident(name: str) -> str:
    """Quote ``name`` as a DuckDB identifier."""
    return '"' + name.replace('"', '""') + '"'


def quote_table(schema: str, table: str) -> str:
    """Quote ``schema.table`` as a DuckDB table name."""
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def select_sql(source: str, columns: list[str] | None, predicate: str | None) -> str:
    """``SELECT <quoted columns | *> FROM <source> [WHERE <predicate>]``."""
    select = ", ".join(map(quote_ident, columns)) if columns else "*"
    sql = f"SELECT {select} FROM {source}"
    return f"{sql} WHERE {predicate}" if predicate is not None else sql


def _options(params: Mapping[str, str | int | float | bool]) -> str:
    return ", ".join(
        f"{quote_ident(key)} {duckdb.ConstantExpression(value)}"
        for key, value in params.items()
    )


def render_secret(name: str, params: Mapping[str, str | int | bool]) -> str:
    """``CREATE OR REPLACE SECRET "name" ("KEY" value, ...)``."""
    return f"CREATE OR REPLACE SECRET {quote_ident(name)} ({_options(params)})"


def render_attach(ducklake: DuckLake) -> str:
    """``ATTACH 'ducklake:<metadata>' AS "<name>" ("DATA_PATH" ..., <attach_options>)``."""
    options: dict[str, str | int | float | bool] = (
        {"DATA_PATH": ducklake.data_path} if ducklake.data_path else {}
    )
    options.update(ducklake.attach_options)

    metadata = duckdb.ConstantExpression("ducklake:" + ducklake.metadata)
    sql = f"ATTACH {metadata} AS {quote_ident(ducklake.name)}"
    return f"{sql} ({_options(options)})" if options else sql
