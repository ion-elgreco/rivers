"""DuckDB ↔ Iceberg bridge — handles ``duckdb.DuckDBPyRelation``."""

from __future__ import annotations

from collections.abc import Sequence
from urllib.parse import urlparse

import pyarrow as pa
from duckdb import ConstantExpression, DuckDBPyRelation

from rivers.integrations.duckdb import DuckDBResource
from rivers.integrations.duckdb._secrets import azure_params, s3_params, scoped_secret
from rivers.integrations.duckdb._sql import select_sql
from rivers.io_handlers.iceberg.base import ArrowIcebergTypeHandler, IcebergReadRequest


class DuckDBTypeHandler(ArrowIcebergTypeHandler[DuckDBPyRelation]):
    """Reads Iceberg tables with DuckDB's ``iceberg_scan``; writes stream through pyiceberg.

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

    def to_arrow(self, obj: DuckDBPyRelation) -> pa.RecordBatchReader:
        """Stream the relation's result into pyiceberg."""
        return obj.to_arrow_reader()

    def load_input(
        self, request: IcebergReadRequest, target_type: type[DuckDBPyRelation]
    ) -> DuckDBPyRelation:
        """A lazy ``iceberg_scan`` relation over the table's current metadata file.

        The projection and the partition predicate prune the files DuckDB
        reads. The table's storage properties become a DuckDB secret scoped to
        its bucket or container. Relations read in one thread share
        :meth:`DuckDBResource.shared_connection`, so they can be joined.
        """
        table = request.load_table()
        con = self._resource.shared_connection()
        if secret := iceberg_secret(table.metadata_location, table.io.properties):
            con.execute(secret)

        args = str(ConstantExpression(table.metadata_location))
        if request.snapshot_id is not None:
            args += f", snapshot_from_id => {int(request.snapshot_id)}"

        return con.sql(
            select_sql(f"iceberg_scan({args})", request.columns, request.predicate)
        )


def iceberg_secret(location: str, properties: dict[str, str]) -> str | None:
    """A ``CREATE SECRET`` for the table's store, from pyiceberg FileIO properties.

    Returns ``None`` for local tables. Reads the ``s3.*`` keys (``client.*``
    as fallback) and the ``adls.*`` keys; the others are ignored.

    Raises:
        ValueError: For ``gs://`` tables; the DuckDB reader needs GCS HMAC
            keys, set them with ``DuckDBResource(secrets=...)``.
    """
    url = urlparse(location)
    scheme = url.scheme.lower()
    if scheme in ("s3", "s3a", "s3n"):
        params = s3_params(_file_io_options(properties, "client.", "s3."))
    elif scheme in ("abfs", "abfss"):
        params = azure_params(
            _file_io_options(properties, "adls."), url, "adls.account-name"
        )
    elif scheme in ("gs", "gcs"):
        raise ValueError(
            f"the DuckDB Iceberg reader cannot use the FileIO properties for "
            f"{location}: DuckDB reads GCS with HMAC keys only. Set them with "
            "DuckDBResource(secrets={...}) and pass the resource as "
            "handler_config={'duckdb': ...}"
        )
    else:
        return None

    return scoped_secret("iceberg", url, params)


def _file_io_options(properties: dict[str, str], *prefixes: str) -> dict[str, str]:
    """The properties under ``prefixes``, keyed like ``access_key_id``; later prefixes win."""
    options = {
        key.removeprefix(prefix).replace("-", "_"): value
        for prefix in prefixes
        for key, value in properties.items()
        if key.startswith(prefix)
    }
    if "force_virtual_addressing" in options:
        options["virtual_hosted_style_request"] = options["force_virtual_addressing"]

    return options
