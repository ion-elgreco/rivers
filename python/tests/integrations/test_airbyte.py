"""Airbyte integration: the resource, sync jobs, and the connection asset.

A fake Airbyte API runs on a local port with the endpoints the resource calls,
so nothing reaches Airbyte.
"""

import asyncio
import base64
import json
import logging
import os
import socket
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, NamedTuple

import pytest
from obstore.store import LocalStore
from pydantic import SecretStr, ValidationError

import rivers as rs
from rivers.exceptions import GraphValidationError
from rivers.integrations.airbyte import (
    AirbyteConnection,
    AirbyteError,
    AirbyteJob,
    AirbyteResource,
    AirbyteStream,
)
from rivers.io_handlers import PickleIOHandler

API = "/api/public/v1"
CONFIG_API = "/api/v1"
CONNECTION_ID = "9924bcd0-99be-453d-ba47-c2c9766f7da5"
JOB_ID = 12345
CONNECTION_PATH = f"{API}/connections/{CONNECTION_ID}"
JOB_PATH = f"{API}/jobs/{JOB_ID}"
STATS_PATH = f"{CONFIG_API}/jobs/get_without_logs"
TOKEN_PATH = f"{API}/applications/token"
TOKEN_BODY = {
    "client_id": "client",
    "client_secret": "secret",
    "grant-type": "client_credentials",
}
OUTPUTS = ["pg_users", "pg_orders", "pg_events"]

STREAMS = [
    {
        "name": "users",
        "namespace": "public",
        "syncMode": "incremental_append",
        "cursorField": ["updated_at"],
        "primaryKey": [["id"]],
    },
    {"name": "orders", "namespace": "public", "syncMode": "full_refresh_overwrite"},
    {"name": "events", "syncMode": "full_refresh_append"},
]

# `events` committed nothing, so Airbyte reports no counts for it.
STREAM_STATS = [
    {
        "streamName": "users",
        "streamNamespace": "public",
        "recordsEmitted": 7,
        "bytesEmitted": 2048,
        "recordsCommitted": 7,
        "bytesCommitted": 2048,
        "recordsRejected": 0,
        "wasBackfilled": False,
        "wasResumed": False,
    },
    {
        "streamName": "orders",
        "streamNamespace": "public",
        "recordsEmitted": 3,
        "bytesEmitted": 1024,
        "recordsCommitted": 3,
        "bytesCommitted": 1024,
        "recordsRejected": 0,
    },
]


def connection_json(streams: list[dict] | None = None) -> dict:
    """A ``GET /connections/{id}`` response of the Airbyte API."""
    return {
        "connectionId": CONNECTION_ID,
        "name": "Postgres To Snowflake",
        "sourceId": "0c31738c-0b2d-4887-b506-e2cd1c39cc35",
        "destinationId": "18dccc91-0ab1-4f72-9ed7-0b8fc27c5826",
        "workspaceId": "744cc0ed-7f05-4949-9e60-2a814f90c035",
        "status": "active",
        "schedule": {"scheduleType": "manual"},
        "namespaceDefinition": "destination",
        "prefix": "pg_",
        "configurations": {"streams": STREAMS if streams is None else streams},
        "createdAt": 1760000000,
        "tags": [],
    }


def job_json(status: str) -> dict:
    """A job response of the Airbyte API."""
    return {
        "jobId": JOB_ID,
        "status": status,
        "jobType": "sync",
        "startTime": "2026-10-11T10:00:00Z",
        "connectionId": CONNECTION_ID,
        "rowsSynced": 10,
        "bytesSynced": 3072,
    }


class Call(NamedTuple):
    method: str
    path: str
    auth: str | None
    body: Any


class FakeAirbyte:
    """The Airbyte endpoints that ``AirbyteResource`` calls, on a local port.

    ``statuses`` are the job statuses that successive ``GET /jobs/{id}`` calls
    return; the last one repeats. ``overrides`` queue responses for one
    endpoint ahead of the normal one.
    """

    def __init__(self) -> None:
        self.calls: list[Call] = []
        self.headers: list[dict[str, str]] = []
        self.connection = connection_json()
        self.statuses = ["running", "succeeded"]
        self.expires_in = 900
        self.overrides: dict[tuple[str, str], list[tuple[int, Any]]] = {}
        self._tokens = 0
        self._lock = threading.Lock()
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _handler(self))
        self._thread = threading.Thread(
            target=self._server.serve_forever, kwargs={"poll_interval": 0.01}
        )
        self._thread.start()
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()

    def respond(
        self, method: str, path: str, headers: dict[str, str], body: Any
    ) -> tuple[int, Any]:
        with self._lock:
            self.calls.append(Call(method, path, headers.get("Authorization"), body))
            self.headers.append(headers)
            queued = self.overrides.get((method, path))
            if queued:
                return queued.pop(0)
            return self._route(method, path)

    def _route(self, method: str, path: str) -> tuple[int, Any]:
        if (method, path) == ("POST", TOKEN_PATH):
            self._tokens += 1
            return 200, {
                "access_token": f"token-{self._tokens}",
                "token_type": "Bearer",
                "expires_in": self.expires_in,
            }
        if (method, path) == ("GET", CONNECTION_PATH):
            return 200, self.connection
        if (method, path) == ("POST", f"{API}/jobs"):
            return 200, job_json("pending")
        if (method, path) == ("GET", JOB_PATH):
            status = (
                self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
            )
            return 200, job_json(status)
        if (method, path) == ("DELETE", JOB_PATH):
            return 200, job_json("cancelled")
        if (method, path) == ("POST", STATS_PATH):
            return 200, {
                "job": {
                    "id": JOB_ID,
                    "configType": "sync",
                    "configId": CONNECTION_ID,
                    "status": "succeeded",
                    "streamAggregatedStats": STREAM_STATS,
                },
                "attempts": [],
            }
        return 404, {"message": f"no route for {method} {path}"}


def _handler(fake: FakeAirbyte) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def _serve(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length)) if length else None
            headers = {
                name: value
                for name in ("Accept", "Authorization", "Content-Type", "User-Agent")
                if (value := self.headers.get(name)) is not None
            }
            status, payload = fake.respond(self.command, self.path, headers, body)
            data = payload if isinstance(payload, str) else json.dumps(payload)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data.encode())))
            self.end_headers()
            self.wfile.write(data.encode())

        do_GET = do_POST = do_DELETE = _serve

        def log_message(self, format: str, *args: Any) -> None:
            pass

    return Handler


@pytest.fixture(autouse=True)
def _no_airbyte_env(monkeypatch):
    for name in [name for name in os.environ if name.startswith("AIRBYTE_")]:
        monkeypatch.delenv(name)


@pytest.fixture
def fake():
    server = FakeAirbyte()
    yield server
    server.close()


@pytest.fixture
def other_fake():
    """A second Airbyte API."""
    server = FakeAirbyte()
    yield server
    server.close()


@pytest.fixture(params=["in_process", "parallel"])
def executor(request) -> str:
    return request.param


@pytest.fixture(params=["sync", "async"])
def sync(request) -> Callable[..., AirbyteJob]:
    """Calls ``sync``, or ``async_sync`` in an event loop."""

    def sync(airbyte: AirbyteResource, connection_id: str, **kwargs: Any) -> AirbyteJob:
        if request.param == "sync":
            return airbyte.sync(connection_id, **kwargs)
        return asyncio.run(airbyte.async_sync(connection_id, **kwargs))

    return sync


def make_resource(fake: FakeAirbyte, **fields: Any) -> AirbyteResource:
    defaults = {
        "api_url": fake.url + API,
        "configuration_api_url": fake.url + CONFIG_API,
        "client_id": "client",
        "client_secret": "secret",
        "poll_interval": 0,
    }
    return AirbyteResource(**{**defaults, **fields})


def pickle_handler(tmp_path) -> PickleIOHandler:
    """An IO handler that the parallel executor can send to its workers."""
    return PickleIOHandler(store=LocalStore(str(tmp_path), mkdir=True))


def make_repo(
    storage,
    executor: str,
    tmp_path,
    *assets: rs.Asset,
    resources: dict[str, AirbyteResource] | None = None,
) -> rs.CodeRepository:
    """A repository with a second step, so the parallel executor uses its workers."""

    @rs.Asset(io_handler=pickle_handler(tmp_path))
    def pid() -> int:
        return os.getpid()

    repo = rs.CodeRepository(
        assets=[*assets, pid],
        resources=resources,
        default_executor=rs.Executor.in_process()
        if executor == "in_process"
        else rs.Executor.parallel(max_workers=2),
    )
    repo.resolve(storage=storage)
    return repo


def materializations(storage, name: str) -> list[dict]:
    return [
        {key: json.loads(value) for key, value in event.metadata}
        for event in storage.get_events_for_asset(name)
        if event.event_type == "Materialization"
    ]


def step_errors(storage, name: str) -> list[str]:
    return [
        dict(event.metadata)["error"]
        for event in storage.get_events_for_asset(name)
        if event.event_type == "StepFailure"
    ]


def without_auth(calls: list) -> list[tuple]:
    """The requests without their credentials and without token requests."""
    return [
        (method, path, body) for method, path, _, body in calls if path != TOKEN_PATH
    ]


def stream_metadata(stream: str, records: int | None = None, size: int = 0) -> dict:
    """The stored materialization metadata of one stream."""
    metadata = {
        "airbyte/connection_id": {"Text": {"value": CONNECTION_ID}},
        "airbyte/job_id": {"Int": {"value": JOB_ID}},
        "airbyte/stream": {"Text": {"value": stream}},
    }
    if records is not None:
        metadata["airbyte/records_committed"] = {"Int": {"value": records}}
        metadata["airbyte/bytes_committed"] = {"Bytes": {"value": size}}
    return metadata


def run_log(storage, run_id: str) -> str:
    return "".join(row.logs or "" for row in storage.get_run_logs(run_id))


def connection_asset(airbyte: AirbyteResource, **options: Any) -> rs.MultiAsset:
    """The asset of the test connection, read with ``airbyte``."""
    return airbyte.connection_asset(airbyte.get_connection(CONNECTION_ID), **options)


# ── Resource ─────────────────────────────────────────────────────────


def test_resource_reads_env(monkeypatch):
    monkeypatch.setenv("AIRBYTE_CLIENT_ID", "client")
    monkeypatch.setenv("AIRBYTE_CLIENT_SECRET", "s3cr3t")
    monkeypatch.setenv("AIRBYTE_POLL_TIMEOUT", "3600")

    airbyte = AirbyteResource()

    assert airbyte.model_dump() == {
        "api_url": "https://api.airbyte.com/v1",
        "configuration_api_url": "https://cloud.airbyte.com/api/v1",
        "client_id": "client",
        "client_secret": SecretStr("s3cr3t"),
        "username": None,
        "password": None,
        "request_timeout": 15.0,
        "request_max_retries": 3,
        "request_retry_delay": 0.25,
        "poll_interval": 10.0,
        "poll_timeout": 3600.0,
        "cancel_on_interrupt": True,
    }
    assert "s3cr3t" not in repr(airbyte)


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"client_id": "c"}, "set both client_id and client_secret, or neither"),
        ({"password": "p"}, "set both username and password, or neither"),
        (
            {"client_id": "c", "client_secret": "s", "username": "u", "password": "p"},
            "not both",
        ),
    ],
)
def test_credentials_come_in_pairs(fields, message):
    with pytest.raises(ValidationError, match=message):
        AirbyteResource(**fields)


def test_get_connection_reads_the_enabled_streams(fake):
    connection = make_resource(fake).get_connection(CONNECTION_ID)

    assert connection == AirbyteConnection(
        connection_id=CONNECTION_ID,
        name="Postgres To Snowflake",
        prefix="pg_",
        streams=[
            AirbyteStream(
                name="users", namespace="public", sync_mode="incremental_append"
            ),
            AirbyteStream(
                name="orders", namespace="public", sync_mode="full_refresh_overwrite"
            ),
            AirbyteStream(name="events", sync_mode="full_refresh_append"),
        ],
    )
    assert fake.calls == [
        ("POST", TOKEN_PATH, None, TOKEN_BODY),
        ("GET", CONNECTION_PATH, "Bearer token-1", None),
    ]
    assert fake.headers == [
        {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "rivers",
        },
        {
            "Accept": "application/json",
            "Authorization": "Bearer token-1",
            "User-Agent": "rivers",
        },
    ]


def test_connection_round_trips_through_json():
    """A connection saved as JSON builds the same asset without the API."""
    connection = AirbyteConnection(
        connection_id=CONNECTION_ID,
        name="Postgres To Snowflake",
        streams=[AirbyteStream(name="users", namespace="public")],
    )
    assert AirbyteConnection.model_validate_json(connection.model_dump_json()) == (
        connection
    )


@pytest.mark.parametrize(
    ("expires_in", "auth"),
    [
        (900, ["Bearer token-1", "Bearer token-1"]),
        # The resource fetches a token again 30 s before it expires.
        (30, ["Bearer token-1", "Bearer token-2"]),
    ],
)
def test_token_is_fetched_again_before_it_expires(fake, expires_in, auth):
    fake.expires_in = expires_in
    airbyte = make_resource(fake)

    airbyte.get_connection(CONNECTION_ID)
    airbyte.get_connection(CONNECTION_ID)

    assert [call.auth for call in fake.calls if call.path == CONNECTION_PATH] == auth
    assert [call.body for call in fake.calls if call.method == "POST"] == [
        TOKEN_BODY
    ] * len(set(auth))


def test_basic_auth(fake):
    make_resource(
        fake, client_id=None, client_secret=None, username="airbyte", password="pw"
    ).get_connection(CONNECTION_ID)

    basic = base64.b64encode(b"airbyte:pw").decode()
    assert fake.calls == [("GET", CONNECTION_PATH, f"Basic {basic}", None)]


def test_no_auth(fake):
    make_resource(fake, client_id=None, client_secret=None).get_connection(
        CONNECTION_ID
    )

    assert fake.calls == [("GET", CONNECTION_PATH, None, None)]


def test_server_errors_are_retried(fake):
    fake.overrides[("GET", CONNECTION_PATH)] = [
        (503, {"message": "unavailable"}),
        (429, {"message": "slow down"}),
    ]

    connection = make_resource(fake, request_retry_delay=0).get_connection(
        CONNECTION_ID
    )

    assert connection.name == "Postgres To Snowflake"
    assert [call.path for call in fake.calls] == [
        TOKEN_PATH,
        CONNECTION_PATH,
        CONNECTION_PATH,
        CONNECTION_PATH,
    ]


def test_retries_stop_at_request_max_retries(fake):
    fake.overrides[("GET", CONNECTION_PATH)] = [(502, "bad gateway")] * 3
    airbyte = make_resource(fake, request_retry_delay=0, request_max_retries=2)

    with pytest.raises(
        AirbyteError, match=f"GET {fake.url}{CONNECTION_PATH}: HTTP 502 bad gateway"
    ):
        airbyte.get_connection(CONNECTION_ID)

    assert [call.path for call in fake.calls].count(CONNECTION_PATH) == 3


def test_client_errors_are_not_retried(fake):
    fake.overrides[("GET", CONNECTION_PATH)] = [
        (404, {"message": "connection not found"})
    ]

    with pytest.raises(
        AirbyteError, match='HTTP 404 {"message": "connection not found"}'
    ):
        make_resource(fake).get_connection(CONNECTION_ID)

    assert [call.path for call in fake.calls].count(CONNECTION_PATH) == 1


def test_unreachable_api_raises_after_retries():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    airbyte = AirbyteResource(
        api_url=f"http://127.0.0.1:{port}/v1",
        request_max_retries=1,
        request_retry_delay=0,
    )

    with pytest.raises(
        AirbyteError, match=rf"GET http://127\.0\.0\.1:{port}/v1/connections/c: "
    ):
        airbyte.get_connection("c")


def test_a_response_that_is_not_json_names_the_url(fake):
    fake.overrides[("GET", CONNECTION_PATH)] = [(200, "<!doctype html>")]

    with pytest.raises(AirbyteError, match="the response is not JSON; check the URL"):
        make_resource(fake).get_connection(CONNECTION_ID)


def test_a_response_with_the_wrong_shape_names_the_url(fake):
    fake.overrides[("GET", CONNECTION_PATH)] = [(200, {"name": "Postgres"})]

    with pytest.raises(AirbyteError) as e:
        make_resource(fake).get_connection(CONNECTION_ID)

    assert str(e.value).startswith(
        f"GET {fake.url}{CONNECTION_PATH}: unexpected response: "
        "1 validation error for AirbyteConnection\nconnectionId\n  Field required"
    )


# ── sync() and async_sync() ──────────────────────────────────────────


def test_sync_polls_until_the_job_succeeds(fake, caplog, sync):
    fake.statuses = ["pending", "running", "succeeded"]

    with caplog.at_level(logging.INFO, logger="rivers.integrations.airbyte"):
        job = sync(make_resource(fake), CONNECTION_ID)

    assert job == AirbyteJob(
        job_id=JOB_ID, status="succeeded", rows_synced=10, bytes_synced=3072
    )
    assert fake.calls == [
        ("POST", TOKEN_PATH, None, TOKEN_BODY),
        (
            "POST",
            f"{API}/jobs",
            "Bearer token-1",
            {"connectionId": CONNECTION_ID, "jobType": "sync"},
        ),
        ("GET", JOB_PATH, "Bearer token-1", None),
        ("GET", JOB_PATH, "Bearer token-1", None),
        ("GET", JOB_PATH, "Bearer token-1", None),
    ]
    assert caplog.messages == [
        f"Airbyte job {JOB_ID} started for connection {CONNECTION_ID}",
        f"Airbyte job {JOB_ID}: running",
        f"Airbyte job {JOB_ID}: succeeded",
        f"Airbyte job {JOB_ID} committed 10 records, 3072 bytes",
    ]


@pytest.mark.parametrize("status", ["failed", "cancelled"])
def test_sync_raises_when_the_job_does_not_succeed(fake, status, sync):
    fake.statuses = ["running", status]

    with pytest.raises(
        AirbyteError,
        match=f"Airbyte job {JOB_ID} of connection {CONNECTION_ID} ended with status '{status}'",
    ):
        sync(make_resource(fake), CONNECTION_ID)

    assert [call.method for call in fake.calls].count("DELETE") == 0


@pytest.mark.parametrize("cancel", [True, False])
def test_poll_timeout_raises(fake, cancel, sync):
    fake.statuses = ["running"]
    airbyte = make_resource(
        fake, poll_interval=0.01, poll_timeout=0.05, cancel_on_interrupt=cancel
    )

    with pytest.raises(
        AirbyteError, match=rf"^Airbyte job {JOB_ID} did not finish within 0\.05 s$"
    ):
        sync(airbyte, CONNECTION_ID)

    cancels = [call for call in fake.calls if call.method == "DELETE"]
    assert cancels == ([("DELETE", JOB_PATH, "Bearer token-1", None)] if cancel else [])


def test_running_sync_conflict_is_not_retried(fake, sync):
    fake.overrides[("POST", f"{API}/jobs")] = [
        (
            409,
            {
                "status": 409,
                "title": "try-again-later",
                "detail": f"A sync is already running for: {CONNECTION_ID}",
            },
        )
    ]

    with pytest.raises(AirbyteError, match="HTTP 409 .*A sync is already running"):
        sync(make_resource(fake), CONNECTION_ID)

    assert [call.path for call in fake.calls] == [
        TOKEN_PATH,
        f"{API}/jobs",
    ]


class _InterruptWhenRunning(logging.Logger):
    def info(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        if msg % args == f"Airbyte job {JOB_ID}: running":
            raise KeyboardInterrupt


@pytest.mark.parametrize("cancel", [True, False])
def test_interrupt_cancels_the_job(fake, cancel, sync):
    fake.statuses = ["running"]
    airbyte = make_resource(fake, cancel_on_interrupt=cancel)

    with pytest.raises(KeyboardInterrupt):
        sync(airbyte, CONNECTION_ID, log=_InterruptWhenRunning("airbyte"))

    cancels = [call for call in fake.calls if call.method == "DELETE"]
    assert cancels == ([("DELETE", JOB_PATH, "Bearer token-1", None)] if cancel else [])


@pytest.mark.parametrize("cancel", [True, False])
def test_cancelling_async_sync_cancels_the_job(fake, cancel):
    fake.statuses = ["running"]
    airbyte = make_resource(fake, cancel_on_interrupt=cancel)

    async def cancel_while_running() -> None:
        task = asyncio.create_task(airbyte.async_sync(CONNECTION_ID))
        while ("GET", JOB_PATH) not in [(c.method, c.path) for c in fake.calls]:
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(cancel_while_running())

    cancels = [call for call in fake.calls if call.method == "DELETE"]
    assert cancels == ([("DELETE", JOB_PATH, "Bearer token-1", None)] if cancel else [])


def test_failed_cancel_keeps_the_original_error(fake, sync):
    fake.statuses = ["running"]
    fake.overrides[("DELETE", JOB_PATH)] = [(409, {"detail": "Job is not running"})]

    with pytest.raises(AirbyteError, match="did not finish within 0.01 s"):
        sync(make_resource(fake, poll_timeout=0.01), CONNECTION_ID)

    assert fake.calls[-1] == ("DELETE", JOB_PATH, "Bearer token-1", None)


def test_sync_in_your_own_asset(fake, storage, executor, tmp_path):
    """A sync asset that calls ``sync`` on the registered resource. The parallel
    executor runs it in a worker."""

    @rs.Asset(io_handler=pickle_handler(tmp_path))
    def synced(context: rs.AssetExecutionContext, airbyte: AirbyteResource) -> int:
        return airbyte.sync(CONNECTION_ID, log=context.log).rows_synced

    repo = make_repo(
        storage, executor, tmp_path, synced, resources={"airbyte": make_resource(fake)}
    )

    assert repo.materialize().success
    assert (repo.load_node("pid") != os.getpid()) == (executor == "parallel")
    assert repo.load_node("synced") == 10


# ── connection_asset() ───────────────────────────────────────────────


def test_connection_asset_names_outputs_like_destination_tables():
    connection = AirbyteConnection(
        connection_id=CONNECTION_ID,
        name="Postgres To Snowflake",
        prefix="pg_",
        streams=[
            AirbyteStream(name="users", namespace="public"),
            AirbyteStream(name="events"),
        ],
    )

    asset = AirbyteResource().connection_asset(connection)

    assert asset.name == "airbyte_postgres_to_snowflake"
    assert [(d.name, d.kinds) for d in asset.output_defs] == [
        ("pg_users", ["airbyte"]),
        ("pg_events", ["airbyte"]),
    ]


@pytest.mark.parametrize(("prefix", "name"), [("pg_", "pg_users"), (None, "users")])
def test_output_def_is_named_like_the_destination_table(prefix, name):
    stream = AirbyteStream(name="users", namespace="public")
    connection = AirbyteConnection(
        connection_id=CONNECTION_ID, name="pg", prefix=prefix, streams=[stream]
    )

    assert connection.output_def(stream) == rs.AssetDef(name)


def test_output_def_sets_or_replaces_fields():
    stream = AirbyteStream(name="users", namespace="public")
    connection = AirbyteConnection(
        connection_id=CONNECTION_ID, name="pg", prefix="pg_", streams=[stream]
    )

    output = connection.output_def(stream, group="raw", name="x")

    assert output == rs.AssetDef("x", group="raw")


def test_asset_def_fn_and_asset_options():
    connection = AirbyteConnection(
        connection_id=CONNECTION_ID,
        name="pg",
        streams=[
            AirbyteStream(name="users", namespace="public"),
            AirbyteStream(name="events"),
        ],
    )

    asset = AirbyteResource().connection_asset(
        connection,
        name="ingest",
        asset_def_fn=lambda connection, stream: rs.AssetDef(
            f"raw/{stream.namespace or connection.name}/{stream.name}",
            kinds=["airbyte", "snowflake"],
        ),
        group="raw",
    )

    assert asset.name == "ingest"
    assert [(d.name, d.group, d.kinds) for d in asset.output_defs] == [
        ("raw/public/users", "raw", ["airbyte", "snowflake"]),
        ("raw/pg/events", "raw", ["airbyte", "snowflake"]),
    ]


def test_asset_def_fn_changes_fields_of_the_default_output():
    connection = AirbyteConnection(
        connection_id=CONNECTION_ID,
        name="pg",
        prefix="pg_",
        streams=[
            AirbyteStream(name="users", namespace="public"),
            AirbyteStream(name="events"),
        ],
    )

    asset = AirbyteResource().connection_asset(
        connection,
        asset_def_fn=lambda connection, stream: connection.output_def(
            stream, group="raw"
        ),
    )

    assert [(d.name, d.group, d.kinds) for d in asset.output_defs] == [
        ("pg_users", "raw", ["airbyte"]),
        ("pg_events", "raw", ["airbyte"]),
    ]


def test_streams_with_one_output_name_raise():
    connection = AirbyteConnection(
        connection_id=CONNECTION_ID,
        name="pg",
        streams=[
            AirbyteStream(name="users", namespace="a"),
            AirbyteStream(name="users", namespace="b"),
        ],
    )

    with pytest.raises(
        ValueError,
        match="Airbyte streams 'a.users' and 'b.users' both have output name 'users'",
    ):
        AirbyteResource().connection_asset(connection)


def test_connection_without_streams_raises():
    connection = AirbyteConnection(connection_id=CONNECTION_ID, name="pg")

    with pytest.raises(
        ValueError, match="Airbyte connection 'pg' has no enabled streams"
    ):
        AirbyteResource().connection_asset(connection)


def test_run_materializes_every_stream(fake, storage, executor, tmp_path):
    """The asset's function is async, so both executors run it in this process."""
    airbyte = make_resource(fake)
    asset = connection_asset(airbyte, io_handler=pickle_handler(tmp_path))
    repo = make_repo(storage, executor, tmp_path, asset, resources={"airbyte": airbyte})

    result = repo.materialize()

    assert result.success
    assert asset.is_async
    assert materializations(storage, "pg_users") == [
        stream_metadata("public.users", 7, 2048)
    ]
    assert materializations(storage, "pg_orders") == [
        stream_metadata("public.orders", 3, 1024)
    ]
    assert materializations(storage, "pg_events") == [stream_metadata("events")]
    assert fake.calls == [
        ("POST", TOKEN_PATH, None, TOKEN_BODY),
        ("GET", CONNECTION_PATH, "Bearer token-1", None),
        ("GET", CONNECTION_PATH, "Bearer token-1", None),
        (
            "POST",
            f"{API}/jobs",
            "Bearer token-1",
            {"connectionId": CONNECTION_ID, "jobType": "sync"},
        ),
        ("GET", JOB_PATH, "Bearer token-1", None),
        ("GET", JOB_PATH, "Bearer token-1", None),
        ("POST", STATS_PATH, "Bearer token-1", {"id": JOB_ID}),
    ]
    log = run_log(storage, result.run_id)
    assert f"Airbyte job {JOB_ID} started for connection {CONNECTION_ID}" in log
    assert f"Airbyte job {JOB_ID}: succeeded" in log
    assert f"Airbyte job {JOB_ID} committed 10 records, 3072 bytes" in log


def test_run_uses_the_registered_resource(
    fake, other_fake, storage, executor, tmp_path
):
    """A run calls the API with the resource registered under ``airbyte``. The
    instance that made the asset only reads the connection at load."""
    loader = make_resource(other_fake)
    asset = connection_asset(loader, io_handler=pickle_handler(tmp_path))
    repo = make_repo(
        storage, executor, tmp_path, asset, resources={"airbyte": make_resource(fake)}
    )

    assert repo.materialize().success
    assert [call.path for call in other_fake.calls] == [TOKEN_PATH, CONNECTION_PATH]
    assert [call.path for call in fake.calls] == [
        TOKEN_PATH,
        CONNECTION_PATH,
        f"{API}/jobs",
        JOB_PATH,
        JOB_PATH,
        STATS_PATH,
    ]
    assert materializations(storage, "pg_users") == [
        stream_metadata("public.users", 7, 2048)
    ]


def test_run_config_overrides_the_resource(
    fake, other_fake, storage, executor, tmp_path
):
    """A run's config rebuilds the registered resource for that run: here it
    syncs with another Airbyte, with the same credentials."""
    airbyte = make_resource(fake)
    asset = connection_asset(airbyte, io_handler=pickle_handler(tmp_path))
    repo = make_repo(storage, executor, tmp_path, asset, resources={"airbyte": airbyte})
    urls = {
        "api_url": other_fake.url + API,
        "configuration_api_url": other_fake.url + CONFIG_API,
    }

    result = repo.materialize(config={"resources": {"airbyte": urls}})

    assert result.success
    assert [call.path for call in fake.calls] == [TOKEN_PATH, CONNECTION_PATH]
    assert other_fake.calls == [
        ("POST", TOKEN_PATH, None, TOKEN_BODY),
        ("GET", CONNECTION_PATH, "Bearer token-1", None),
        (
            "POST",
            f"{API}/jobs",
            "Bearer token-1",
            {"connectionId": CONNECTION_ID, "jobType": "sync"},
        ),
        ("GET", JOB_PATH, "Bearer token-1", None),
        ("GET", JOB_PATH, "Bearer token-1", None),
        ("POST", STATS_PATH, "Bearer token-1", {"id": JOB_ID}),
    ]
    assert materializations(storage, "pg_users") == [
        stream_metadata("public.users", 7, 2048)
    ]


def test_resource_key_picks_the_resource(fake, other_fake, storage, executor, tmp_path):
    """A run uses the resource registered under ``resource_key``, not the one
    under ``airbyte``."""
    asset = connection_asset(
        make_resource(fake), io_handler=pickle_handler(tmp_path), resource_key="cloud"
    )
    repo = make_repo(
        storage,
        executor,
        tmp_path,
        asset,
        resources={"airbyte": make_resource(other_fake), "cloud": make_resource(fake)},
    )

    assert repo.materialize().success
    assert other_fake.calls == []
    assert materializations(storage, "pg_users") == [
        stream_metadata("public.users", 7, 2048)
    ]


def test_asset_without_its_resource_fails_to_load(fake):
    repo = rs.CodeRepository(assets=[connection_asset(make_resource(fake))])

    with pytest.raises(
        GraphValidationError, match="^Asset 'pg_events': unresolved input 'airbyte'$"
    ):
        repo.validate()


def test_failed_job_fails_every_output(fake, storage, executor, tmp_path):
    fake.statuses = ["running", "failed"]
    airbyte = make_resource(fake)
    asset = connection_asset(airbyte, io_handler=pickle_handler(tmp_path))
    repo = make_repo(storage, executor, tmp_path, asset, resources={"airbyte": airbyte})

    result = repo.materialize(raise_on_error=False)

    assert not result.success
    error = (
        f"AirbyteError: Airbyte job {JOB_ID} of connection {CONNECTION_ID} "
        "ended with status 'failed'"
    )
    for name in OUTPUTS:
        assert step_errors(storage, name) == [error]
        assert materializations(storage, name) == []
    assert ("POST", STATS_PATH) not in [(c.method, c.path) for c in fake.calls]


def test_poll_timeout_cancels_the_job(fake, storage, executor, tmp_path):
    fake.statuses = ["running"]
    airbyte = make_resource(fake, poll_timeout=0.05)
    asset = connection_asset(airbyte, io_handler=pickle_handler(tmp_path))
    repo = make_repo(storage, executor, tmp_path, asset, resources={"airbyte": airbyte})

    result = repo.materialize(raise_on_error=False)

    assert not result.success
    error = f"AirbyteError: Airbyte job {JOB_ID} did not finish within 0.05 s"
    for name in OUTPUTS:
        assert step_errors(storage, name) == [error]
    assert without_auth(fake.calls)[-1] == ("DELETE", JOB_PATH, None)


def test_subset_run_syncs_the_connection_once(fake, storage, tmp_path):
    airbyte = make_resource(fake)
    repo = make_repo(
        storage,
        "in_process",
        tmp_path,
        connection_asset(airbyte),
        resources={"airbyte": airbyte},
    )

    repo.materialize(selection=["pg_users"])

    assert materializations(storage, "pg_users") == [
        stream_metadata("public.users", 7, 2048)
    ]
    assert materializations(storage, "pg_orders") == []
    assert materializations(storage, "pg_events") == []
    assert [(c.method, c.path) for c in fake.calls].count(("POST", f"{API}/jobs")) == 1


def test_missing_counts_still_record_materializations(fake, storage, tmp_path, caplog):
    fake.overrides[("POST", STATS_PATH)] = [(403, {"message": "forbidden"})]
    airbyte = make_resource(fake)
    repo = make_repo(
        storage,
        "in_process",
        tmp_path,
        connection_asset(airbyte),
        resources={"airbyte": airbyte},
    )

    repo.materialize()

    assert materializations(storage, "pg_users") == [stream_metadata("public.users")]
    assert materializations(storage, "pg_events") == [stream_metadata("events")]
    assert (
        f"No per-stream counts for Airbyte job {JOB_ID}: POST {fake.url}{STATS_PATH}: "
        'HTTP 403 {"message": "forbidden"}'
    ) in caplog.messages


def test_stream_removed_after_load_fails_its_output(fake, storage, tmp_path):
    airbyte = make_resource(fake)
    asset = connection_asset(airbyte)
    fake.connection = connection_json(STREAMS[:2])
    repo = make_repo(
        storage, "in_process", tmp_path, asset, resources={"airbyte": airbyte}
    )

    result = repo.materialize(raise_on_error=False)

    assert not result.success
    assert materializations(storage, "pg_users") == [
        stream_metadata("public.users", 7, 2048)
    ]
    assert materializations(storage, "pg_orders") == [
        stream_metadata("public.orders", 3, 1024)
    ]
    assert step_errors(storage, "pg_events") == [
        f"AirbyteError: Airbyte connection {CONNECTION_ID} no longer syncs the "
        "streams of pg_events. Reload the code location."
    ]


def test_stream_added_after_load_is_logged(fake, storage, tmp_path, caplog):
    airbyte = make_resource(fake)
    asset = connection_asset(airbyte)
    fake.connection = connection_json(
        [*STREAMS, {"name": "refunds", "namespace": "public"}]
    )
    repo = make_repo(
        storage, "in_process", tmp_path, asset, resources={"airbyte": airbyte}
    )

    result = repo.materialize()

    warning = (
        f"Airbyte connection {CONNECTION_ID} also syncs public.refunds, which have "
        "no outputs. Reload the code location to add them."
    )
    assert warning in caplog.messages
    assert warning in run_log(storage, result.run_id)
    assert [len(materializations(storage, name)) for name in OUTPUTS] == [1, 1, 1]


def test_downstream_asset_depends_on_an_airbyte_output(fake, storage, tmp_path):
    @rs.Asset(deps=[rs.AssetDef.dep("pg_users")])
    def active_users() -> int:
        return 1

    airbyte = make_resource(fake)
    repo = make_repo(
        storage,
        "in_process",
        tmp_path,
        connection_asset(airbyte),
        active_users,
        resources={"airbyte": airbyte},
    )

    repo.materialize()

    assert repo.load_node("active_users") == 1
    assert materializations(storage, "pg_users") == [
        stream_metadata("public.users", 7, 2048)
    ]
