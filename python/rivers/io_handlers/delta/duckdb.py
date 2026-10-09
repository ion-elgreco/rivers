"""DuckDB ↔ Delta Lake bridge — handles ``duckdb.DuckDBPyRelation``."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from urllib.parse import ParseResult, urlparse

from arro3.core import RecordBatchReader
from duckdb import ConstantExpression, DuckDBPyRelation

from rivers.integrations.duckdb import DuckDBResource
from rivers.integrations.duckdb._sql import render_secret, select_sql
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
        params = _s3_params(_normalize(storage_options, "aws_"))
    elif scheme in ("az", "azure", "abfs", "abfss"):
        params = _azure_params(_normalize(storage_options, "azure_", "storage_"), url)
    elif scheme in ("gs", "gcs"):
        raise ValueError(
            f"the DuckDB Delta reader cannot use storage_options for {table_uri}: "
            "DuckDB reads GCS with HMAC keys only. Set them with "
            "DuckDBResource(secrets={...}) and pass the resource as "
            "handler_config={'duckdb': ...}"
        )
    else:
        return None

    params["SCOPE"] = f"{scheme}://{url.netloc}"
    digest = hashlib.sha256(repr(sorted(params.items())).encode()).hexdigest()[:16]
    return render_secret(f"rivers_delta_{scheme}_{digest}", params)


def _normalize(options: dict[str, str] | None, *prefixes: str) -> dict[str, str]:
    normalized = {}
    for key, value in (options or {}).items():
        key = key.lower()
        for prefix in prefixes:
            key = key.removeprefix(prefix)
        normalized[key] = value

    return normalized


def _truthy(value: str | None) -> bool:
    return str(value).lower() in ("true", "1")


def _s3_params(opts: dict[str, str]) -> dict[str, str | int | bool]:
    params: dict[str, str | int | bool] = {"TYPE": "s3"}

    key, secret = opts.get("access_key_id"), opts.get("secret_access_key")
    if key and secret:
        params.update(PROVIDER="config", KEY_ID=key, SECRET=secret)
        if token := opts.get("session_token"):
            params["SESSION_TOKEN"] = token
    else:
        params["PROVIDER"] = "credential_chain"

    if region := opts.get("region"):
        params["REGION"] = region

    if endpoint := opts.get("endpoint_url") or opts.get("endpoint"):
        scheme, _, host = endpoint.rpartition("://")
        params["ENDPOINT"] = host.rstrip("/")
        params["USE_SSL"] = scheme == "https" or (
            not scheme and not _truthy(opts.get("allow_http"))
        )

    params["URL_STYLE"] = (
        "vhost" if _truthy(opts.get("virtual_hosted_style_request")) else "path"
    )

    return params


def _azure_params(
    opts: dict[str, str], url: ParseResult
) -> dict[str, str | int | bool]:
    # abfss://<container>@<account>.dfs.core.windows.net/... names the account
    host_account = url.hostname.split(".")[0] if url.username and url.hostname else None
    account = opts.get("account_name") or host_account
    params: dict[str, str | int | bool] = {"TYPE": "azure"}

    if key := opts.get("account_key"):
        credential = f"AccountKey={key}"
    elif sas := opts.get("sas_token"):
        credential = f"SharedAccessSignature={sas.lstrip('?')}"
    else:
        credential = None

    if credential is not None:
        if account is None:
            raise ValueError(
                "an Azure account key or SAS token needs azure_storage_account_name"
            )
        params["CONNECTION_STRING"] = f"AccountName={account};{credential}"
        return params

    client_id, client_secret = opts.get("client_id"), opts.get("client_secret")
    if client_id and client_secret and (tenant := opts.get("tenant_id")):
        params.update(
            PROVIDER="service_principal",
            TENANT_ID=tenant,
            CLIENT_ID=client_id,
            CLIENT_SECRET=client_secret,
        )
    else:
        params["PROVIDER"] = "credential_chain"

    if account:
        params["ACCOUNT_NAME"] = account

    return params
