"""SnowflakeResource: the arguments that reach ``snowflake.connector.connect``.

A fake ``connect`` records its keyword arguments, so nothing reaches Snowflake.
"""

import base64
import importlib
import sys

import pytest

snowflake_connector = pytest.importorskip("snowflake.connector")

from cryptography.hazmat.primitives import serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: E402
from pydantic import SecretStr  # noqa: E402

from rivers.integrations.snowflake import (  # noqa: E402
    SnowflakeIOHandler,
    SnowflakeResource,
)


class FakeConnection:
    def __init__(self, kwargs: dict):
        self.kwargs = kwargs
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.closed = True


@pytest.fixture
def connect(monkeypatch) -> list[FakeConnection]:
    """The connections opened through ``snowflake.connector.connect``."""
    opened: list[FakeConnection] = []

    def fake_connect(**kwargs):
        opened.append(FakeConnection(kwargs))
        return opened[-1]

    monkeypatch.setattr(snowflake_connector, "connect", fake_connect)
    return opened


@pytest.fixture(scope="module")
def rsa_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _pem(key: rsa.RSAPrivateKey, password: bytes | None) -> bytes:
    encryption = (
        serialization.BestAvailableEncryption(password)
        if password
        else serialization.NoEncryption()
    )
    return key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, encryption
    )


def _der(key: rsa.RSAPrivateKey) -> bytes:
    return key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def test_password_auth(connect):
    resource = SnowflakeResource(
        account="myorg-myaccount",
        user="ETL",
        password="hunter2",
        database="ANALYTICS",
        schema_name="RAW",
        warehouse="TRANSFORM_WH",
        role="LOADER",
        connection_args={
            "login_timeout": 5,
            "session_parameters": {"QUERY_TAG": "rivers"},
        },
    )
    with resource.get_connection() as con:
        assert not con.closed

    assert con.closed
    assert con.kwargs == {
        "account": "myorg-myaccount",
        "user": "ETL",
        "password": "hunter2",
        "database": "ANALYTICS",
        "schema": "RAW",
        "warehouse": "TRANSFORM_WH",
        "role": "LOADER",
        "login_timeout": 5,
        "session_parameters": {"QUERY_TAG": "rivers"},
    }


def test_connector_cannot_change_connection_args(connect):
    """The connector writes session parameters into the dict that it gets."""
    resource = SnowflakeResource(
        connection_args={"session_parameters": {"QUERY_TAG": "etl"}}
    )
    with resource.get_connection(timezone="UTC") as con:
        con.kwargs["session_parameters"]["TIMEZONE"] = "UTC"

    assert resource.connection_args == {"session_parameters": {"QUERY_TAG": "etl"}}


def test_overrides_replace_the_resource_args(connect):
    resource = SnowflakeResource(
        account="acct", user="ETL", password="pw", warehouse="SMALL_WH"
    )
    with resource.get_connection(warehouse="LARGE_WH", timezone="UTC"):
        pass

    assert [c.kwargs for c in connect] == [
        {
            "account": "acct",
            "user": "ETL",
            "password": "pw",
            "warehouse": "LARGE_WH",
            "timezone": "UTC",
        }
    ]


@pytest.mark.parametrize("password", [b"key-pass", None], ids=["encrypted", "plain"])
@pytest.mark.parametrize("source", ["text", "base64", "path"])
def test_key_pair_auth(connect, rsa_key, tmp_path, source, password):
    pem = _pem(rsa_key, password)
    if source == "path":
        path = tmp_path / "rsa_key.p8"
        path.write_bytes(pem)
        key_fields = {"private_key_path": str(path)}
    elif source == "base64":
        key_fields = {"private_key": base64.b64encode(pem).decode()}
    else:
        key_fields = {"private_key": pem.decode()}
    if password:
        key_fields["private_key_password"] = password.decode()

    resource = SnowflakeResource(account="acct", user="ETL", **key_fields)
    for _ in range(2):
        with resource.get_connection():
            pass

    first, second = (c.kwargs for c in connect)
    assert first["private_key"] is second["private_key"]
    assert _der(first.pop("private_key")) == _der(rsa_key)
    assert first == {"account": "acct", "user": "ETL"}


def test_authenticator_without_secrets(connect):
    resource = SnowflakeResource(
        account="acct", user="me@example.com", authenticator="externalbrowser"
    )
    with resource.get_connection():
        pass

    assert [c.kwargs for c in connect] == [
        {
            "account": "acct",
            "user": "me@example.com",
            "authenticator": "externalbrowser",
        }
    ]


def test_one_credential_at_a_time():
    with pytest.raises(ValueError, match="got password and private_key"):
        SnowflakeResource(password="pw", private_key="pem")
    with pytest.raises(ValueError, match="got private_key and private_key_path"):
        SnowflakeResource(private_key="pem", private_key_path="rsa_key.p8")


@pytest.mark.parametrize("in_handler", [False, True], ids=["resource", "handler"])
@pytest.mark.parametrize("source", ["code", "env"])
def test_two_credentials_fail_without_their_values(monkeypatch, source, in_handler):
    """A handler validates a resource given as a dict, and its error shows
    the handler's input."""
    fields = {"account": "acct", "password": "s3cr3t-pw", "private_key": "s3cr3t-pem"}
    if source == "env":
        for name in ("password", "private_key"):
            monkeypatch.setenv(f"SNOWFLAKE_{name.upper()}", fields.pop(name))

    with pytest.raises(ValueError, match="got password and private_key") as error:
        if in_handler:
            SnowflakeIOHandler(resource=fields)
        else:
            SnowflakeResource(**fields)

    assert "s3cr3t" not in str(error.value)


def test_fields_from_env(monkeypatch, connect):
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "myorg-env")
    monkeypatch.setenv("SNOWFLAKE_USER", "ENV_USER")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "from-env")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA_NAME", "STAGING")
    monkeypatch.setenv("SNOWFLAKE_CONNECTION_ARGS", '{"login_timeout": 7}')
    with SnowflakeResource(database="DB").get_connection():
        pass

    assert [c.kwargs for c in connect] == [
        {
            "account": "myorg-env",
            "user": "ENV_USER",
            "password": "from-env",
            "database": "DB",
            "schema": "STAGING",
            "login_timeout": 7,
        }
    ]


@pytest.mark.parametrize("given", ["password", "private_key", "private_key_path"])
def test_a_given_credential_hides_the_env_credentials(
    monkeypatch, connect, rsa_key, tmp_path, given
):
    """The environment holds the two other kinds. A worker rebuilds the
    resource from its fields with ``model_validate``, which reads the
    environment again."""
    pem = _pem(rsa_key, None)
    path = tmp_path / "rsa_key.p8"
    path.write_bytes(pem)
    credentials = {
        "password": "s3cr3t-pw",
        "private_key": pem.decode(),
        "private_key_path": str(path),
    }
    for name, value in credentials.items():
        if name != given:
            monkeypatch.setenv(f"SNOWFLAKE_{name.upper()}", value)

    resource = SnowflakeResource(
        account="acct", user="ETL", **{given: credentials[given]}
    )
    with resource.get_connection() as con:
        pass

    assert [n for n in credentials if getattr(resource, n) is not None] == [given]
    expected = {"account": "acct", "user": "ETL"}
    if given == "password":
        expected["password"] = "s3cr3t-pw"
    else:
        assert _der(con.kwargs.pop("private_key")) == _der(rsa_key)
    assert con.kwargs == expected

    rebuilt = SnowflakeResource.model_validate(resource.model_dump(round_trip=True))
    assert rebuilt == resource


def test_none_is_not_a_credential(monkeypatch, connect, rsa_key):
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY", _pem(rsa_key, None).decode())

    resource = SnowflakeResource(account="acct", user="ETL", password=None)
    with resource.get_connection() as con:
        pass

    assert _der(con.kwargs.pop("private_key")) == _der(rsa_key)
    assert con.kwargs == {"account": "acct", "user": "ETL"}


@pytest.mark.parametrize("key_source", ["code", "env"])
def test_empty_env_vars_are_unset(monkeypatch, connect, rsa_key, key_source):
    """A CI secret that is not defined becomes an empty variable."""
    pem = _pem(rsa_key, None).decode()
    for name in ("PASSWORD", "PRIVATE_KEY_PATH", "PRIVATE_KEY_PASSWORD", "ROLE"):
        monkeypatch.setenv(f"SNOWFLAKE_{name}", "")
    key = {"private_key": pem}
    if key_source == "env":
        monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY", pem)
        key = {}

    resource = SnowflakeResource(account="acct", user="ETL", **key)
    with resource.get_connection() as con:
        pass

    assert _der(con.kwargs.pop("private_key")) == _der(rsa_key)
    assert con.kwargs == {"account": "acct", "user": "ETL"}


def test_secrets_stay_out_of_repr():
    resource = SnowflakeResource(
        user="ETL", private_key="pem-text", private_key_password="key-pass"
    )
    assert isinstance(resource.private_key, SecretStr)
    assert resource.private_key.get_secret_value() == "pem-text"
    assert "pem-text" not in repr(resource)
    assert "key-pass" not in repr(resource)
    assert "hunter2" not in repr(SnowflakeResource(password="hunter2"))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("private_key_file_pwd", "s3cr3t-pass"),
        ("connection_args", '{"token": "s3cr3t-token"}'),
    ],
    ids=["connector-passphrase-name", "connection-args-as-text"],
)
def test_validation_errors_hide_the_values(field, value):
    with pytest.raises(ValueError, match=field) as error:
        SnowflakeResource(account="acct", user="ETL", **{field: value})

    assert "s3cr3t" not in str(error.value)


def test_missing_connector_names_the_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "snowflake.connector", None)
    for name in list(sys.modules):
        if name.startswith("rivers.integrations.snowflake"):
            monkeypatch.delitem(sys.modules, name)
    with pytest.raises(ImportError, match=r"pip install rivers\[snowflake\]"):
        importlib.import_module("rivers.integrations.snowflake")
