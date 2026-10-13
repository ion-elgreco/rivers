"""Snowflake integration: a connection resource, an IO handler for Snowflake tables, and an asset class with a delete action.

Install with ``pip install rivers[snowflake]``.

Example::

    import polars as pl
    import rivers as rs
    from rivers.integrations.snowflake import SnowflakeIOHandler, SnowflakeResource

    # account, user, and private key from SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY
    warehouse = SnowflakeResource(database="ANALYTICS", warehouse="TRANSFORM_WH")

    @rs.Asset(io_handler=SnowflakeIOHandler(resource=warehouse))
    def orders() -> pl.DataFrame: ...
"""

try:
    import snowflake.connector  # noqa: F401
except ImportError as e:
    raise ImportError(
        "rivers.integrations.snowflake needs snowflake-connector-python: "
        "pip install rivers[snowflake]"
    ) from e

from rivers.integrations.snowflake.asset import SnowflakeAsset
from rivers.integrations.snowflake.io_handler import (
    SnowflakeIOHandler,
    SnowflakeWriteMode,
    table_columns,
)
from rivers.integrations.snowflake.resource import SnowflakeResource

__all__ = [
    "SnowflakeAsset",
    "SnowflakeIOHandler",
    "SnowflakeResource",
    "SnowflakeWriteMode",
    "table_columns",
]
