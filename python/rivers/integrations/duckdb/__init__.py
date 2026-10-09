"""DuckDB integration: a connection resource, an IO handler for DuckDB and DuckLake tables, and asset classes with maintenance actions.

Install with ``pip install rivers[duckdb]``.

Example::

    import rivers as rs
    from rivers.integrations.duckdb import DuckDBIOHandler, DuckDBResource

    warehouse = DuckDBResource(database="warehouse.duckdb")

    @rs.Asset(io_handler=DuckDBIOHandler(resource=warehouse))
    def orders() -> pa.Table: ...
"""

try:
    import duckdb  # noqa: F401
except ImportError as e:
    raise ImportError(
        "rivers.integrations.duckdb needs duckdb: pip install rivers[duckdb]"
    ) from e

from rivers.integrations.duckdb.asset import DuckDBAsset, DuckLakeAsset
from rivers.integrations.duckdb.io_handler import DuckDBIOHandler, DuckDBWriteMode
from rivers.integrations.duckdb.resource import DuckDBResource, DuckLake

__all__ = [
    "DuckDBAsset",
    "DuckDBIOHandler",
    "DuckDBResource",
    "DuckDBWriteMode",
    "DuckLake",
    "DuckLakeAsset",
]
