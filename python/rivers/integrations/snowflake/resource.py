"""Snowflake connections with password, key-pair, or other authentication."""

import base64
import copy
from collections.abc import Iterator
from contextlib import contextmanager
from functools import cache
from pathlib import Path
from typing import Any, Self

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.types import PrivateKeyTypes
from pydantic import SecretStr, model_validator
from pydantic_settings import SettingsConfigDict

from rivers.resource import Resource


class SnowflakeResource(Resource):
    """Opens Snowflake connections with ``snowflake.connector``.

    Fields are read from ``SNOWFLAKE_*`` environment variables when not given,
    for example ``SNOWFLAKE_ACCOUNT`` and ``SNOWFLAKE_PRIVATE_KEY``.

    Args:
        account: Account identifier, e.g. ``"myorg-myaccount"``.
        user: Login name.
        password: Password, or a programmatic access token.
        private_key: PEM private key for key-pair authentication. Base64 of the
            PEM text works too, for environment variables without newlines.
        private_key_path: Path to a PEM private key file.
        private_key_password: Passphrase of an encrypted private key.
        authenticator: Authentication method, e.g. ``"externalbrowser"`` or
            ``"oauth"``. Key-pair authentication needs none.
        database: Default database.
        schema_name: Default schema.
        warehouse: Warehouse that runs the queries.
        role: Role of the session.
        connection_args: Other keyword arguments for
            ``snowflake.connector.connect``, e.g. ``{"login_timeout": 30}``.

    Raises:
        ValueError: If more than one of ``password``, ``private_key``, and
            ``private_key_path`` is given.

    Example::

        warehouse = SnowflakeResource(database="ANALYTICS", warehouse="TRANSFORM_WH")

        @rs.Asset
        def top_users(warehouse: SnowflakeResource) -> pa.Table:
            with warehouse.get_connection() as con:
                cur = con.cursor()
                cur.execute("SELECT * FROM users ORDER BY score DESC LIMIT 10")
                return cur.fetch_arrow_all(force_return_table=True)
    """

    model_config = SettingsConfigDict(env_prefix="SNOWFLAKE_")

    account: str | None = None
    user: str | None = None
    password: SecretStr | None = None
    private_key: SecretStr | None = None
    private_key_path: str | None = None
    private_key_password: SecretStr | None = None
    authenticator: str | None = None
    database: str | None = None
    schema_name: str | None = None
    warehouse: str | None = None
    role: str | None = None
    connection_args: dict[str, Any] = {}

    @model_validator(mode="after")
    def _one_credential(self) -> Self:
        given = [
            name
            for name in ("password", "private_key", "private_key_path")
            if getattr(self, name) is not None
        ]
        if len(given) > 1:
            raise ValueError(
                f"give one of password, private_key, and private_key_path, got {' and '.join(given)}"
            )
        return self

    @contextmanager
    def get_connection(
        self, **overrides: Any
    ) -> Iterator[snowflake.connector.SnowflakeConnection]:
        """A connection that is closed when the block exits.

        With autocommit off, the connector commits when the block exits
        normally and rolls back when it raises.

        Args:
            **overrides: Arguments for ``snowflake.connector.connect`` that
                replace the resource's, e.g. ``warehouse="LARGE_WH"``.
        """
        args = {**self._connect_args(), **overrides}
        with snowflake.connector.connect(**args) as con:
            yield con

    def _connect_args(self) -> dict[str, Any]:
        args = {
            "account": self.account,
            "user": self.user,
            "password": _reveal(self.password),
            "private_key": self._private_key(),
            "authenticator": self.authenticator,
            "database": self.database,
            "schema": self.schema_name,
            "warehouse": self.warehouse,
            "role": self.role,
        }
        # a copy: the connector writes session parameters into the dicts it gets
        return {
            **{key: value for key, value in args.items() if value is not None},
            **copy.deepcopy(self.connection_args),
        }

    def _private_key(self) -> PrivateKeyTypes | None:
        if self.private_key is not None:
            pem = self.private_key.get_secret_value().encode()
            if not pem.lstrip().startswith(b"-----BEGIN"):
                pem = base64.b64decode(pem)
        elif self.private_key_path is not None:
            pem = Path(self.private_key_path).expanduser().read_bytes()
        else:
            return None

        return _load_key(pem, _reveal(self.private_key_password))


@cache
def _load_key(pem: bytes, password: str | None) -> PrivateKeyTypes:
    """``pem`` as a key object, which the connector takes as is.

    Cached: loading checks the key, which takes over 100 ms for an RSA key.
    """
    return serialization.load_pem_private_key(
        pem, password=password.encode() if password is not None else None
    )


def _reveal(secret: SecretStr | None) -> str | None:
    return secret.get_secret_value() if secret is not None else None
