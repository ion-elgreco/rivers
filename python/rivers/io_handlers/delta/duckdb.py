"""DuckDB ↔ Delta Lake bridge — handles ``duckdb.DuckDBPyRelation``."""

from __future__ import annotations

from collections.abc import Sequence
from urllib.parse import urlparse

from arro3.core import RecordBatchReader
from duckdb import ConstantExpression, DuckDBPyRelation

from rivers.integrations.duckdb import DuckDBResource
from rivers.integrations.duckdb._secrets import (
    azure_params,
    normalize,
    s3_params,
    scoped_secret,
)
from rivers.integrations.duckdb._sql import select_sql
from rivers.io_handlers.delta.base import ArrowDeltaTypeHandler


class DuckDBTypeHandler(ArrowDeltaTypeHandler[DuckDBPyRelation]):
    """Reads Delta tables with DuckDB's ``delta_scan``; writes stream through delta-rs.

    Args:
        duckdb: The resource that opens the read connection. The default is an
            in-memory database. Pass it as ``handler_config={"duckdb": ...}``.
    """

    def __init__(self, duckdb: DuckDBResource | None = None) -> None:
        self._resource = duckdb or DuckDBResource()

    @property
    def supported_types(self) -> Sequence[type[DuckDBPyRelation]]:
        """DuckDB types this handler accepts as asset outputs / inputs."""
        return [DuckDBPyRelation]

    def to_arrow(self, obj: DuckDBPyRelation) -> RecordBatchReader:
        """Stream the relation's result into ``write_deltalake``."""
        return RecordBatchReader.from_stream(obj)

    def load_input(
        self,
        table_uri: str,
        table_name: str,
        storage_options: dict[str, str] | None,
        predicate: str | None,
        target_type: type[DuckDBPyRelation],
        columns: list[str] | None = None,
        version: int | None = None,
    ) -> DuckDBPyRelation:
        """A lazy ``delta_scan`` relation; the projection and predicate prune files.

        Credentials in ``storage_options`` become a DuckDB secret scoped to the
        table's bucket or container. Relations read in one thread share
        :meth:`DuckDBResource.shared_connection`, so they can be joined.
        """
        con = self._resource.shared_connection()
        if secret := delta_secret(table_uri, storage_options):
            con.execute(secret)

        args = str(ConstantExpression(table_uri))
        if version is not None:
            args += f", version => {int(version)}"

        return con.sql(select_sql(f"delta_scan({args})", columns, predicate))


def delta_secret(table_uri: str, storage_options: dict[str, str] | None) -> str | None:
    """A ``CREATE SECRET`` for the table's store, from delta-rs ``storage_options``.

    Returns ``None`` for local tables. Keys are matched without case and with
    or without the ``aws_`` / ``azure_storage_`` prefix. Keys that only
    configure the writer are ignored.

    Raises:
        ValueError: For ``gs://`` tables; the DuckDB reader needs GCS HMAC keys,
            set them with ``DuckDBResource(secrets=...)``.
    """
    url = urlparse(table_uri)
    scheme = url.scheme.lower()
    if scheme in ("s3", "s3a"):
        params = s3_params(normalize(storage_options, "aws_"))
    elif scheme in ("az", "azure", "abfs", "abfss"):
        params = azure_params(
            normalize(storage_options, "azure_", "storage_"),
            url,
            "azure_storage_account_name",
        )
    elif scheme in ("gs", "gcs"):
        raise ValueError(
            f"the DuckDB Delta reader cannot use storage_options for {table_uri}: "
            "DuckDB reads GCS with HMAC keys only. Set them with "
            "DuckDBResource(secrets={...}) and pass the resource as "
            "handler_config={'duckdb': ...}"
        )
    else:
        return None

    return scoped_secret("delta", url, params)
