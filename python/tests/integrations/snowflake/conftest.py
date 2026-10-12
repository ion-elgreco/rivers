import os
import uuid

import pytest


@pytest.fixture(autouse=True)
def _no_snowflake_env(monkeypatch):
    """SnowflakeResource reads ``SNOWFLAKE_*`` variables: keep the shell's out of the tests."""
    for name in list(os.environ):
        if name.startswith("SNOWFLAKE_"):
            monkeypatch.delenv(name)


@pytest.fixture(scope="session")
def server():
    """One server: ``fakesnow.server()`` replaces itself with its submodule on the first call."""
    fakesnow = pytest.importorskip("fakesnow")
    with fakesnow.server() as kwargs:
        yield kwargs


@pytest.fixture
def warehouse(server):
    """A resource on a new database of the fakesnow server."""
    from rivers.integrations.snowflake import SnowflakeResource

    return SnowflakeResource(
        account=server["account"],
        user=server["user"],
        password=server["password"],
        database=f"TEST_{uuid.uuid4().hex[:12].upper()}",
        connection_args={
            key: server[key]
            for key in (
                "host",
                "port",
                "protocol",
                "session_parameters",
                "network_timeout",
            )
        },
    )
