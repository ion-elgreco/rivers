"""DuckDB secrets for the object store under a table, from a table format's storage options."""

from __future__ import annotations

import hashlib
from urllib.parse import ParseResult

from rivers.integrations.duckdb._sql import render_secret


def _truthy(value: str | None) -> bool:
    return str(value).lower() in ("true", "1")


def s3_params(opts: dict[str, str]) -> dict[str, str | int | bool]:
    """S3 secret parameters from normalized options (``access_key_id``, ``region``, ...)."""
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


def azure_params(
    opts: dict[str, str], url: ParseResult, account_option: str
) -> dict[str, str | int | bool]:
    """Azure secret parameters from normalized options (``account_name``, ``sas_token``, ...).

    Raises:
        ValueError: For an account key or SAS token without an account name;
            the message names ``account_option``.
    """
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
                f"an Azure account key or SAS token needs {account_option}"
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


def scoped_secret(
    table_format: str, url: ParseResult, params: dict[str, str | int | bool]
) -> str:
    """``CREATE SECRET`` with ``params``, scoped to the bucket or container of ``url``.

    The name is a hash of the parameters, so equal stores share one secret.
    """
    scheme = url.scheme.lower()
    params["SCOPE"] = f"{scheme}://{url.netloc}"
    digest = hashlib.sha256(repr(sorted(params.items())).encode()).hexdigest()[:16]
    return render_secret(f"rivers_{table_format}_{scheme}_{digest}", params)
