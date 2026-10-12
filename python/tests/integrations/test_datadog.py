"""Datadog integration: the resource, its client, its hooks, and its run-failure sensor.

A loopback UDP socket stands in for the Agent's DogStatsD server and a local
HTTP server for the Datadog API, so nothing reaches Datadog.
"""

import importlib
import json
import re
import socket
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from pydantic import SecretStr

pytest.importorskip("datadog")

from datadog.dogstatsd.base import DogStatsd  # noqa: E402

import rivers as rs  # noqa: E402
from rivers._core import AutomationDaemon  # noqa: E402
from rivers.integrations.datadog import DatadogClient, DatadogResource  # noqa: E402

from _polling import wait_for_ticks, wait_until  # noqa: E402

KEY = "test-api-key"
UI = "https://rivers.test/locations/prod/etl"
SENSOR = "datadog_run_failure_sensor"


class DogStatsdServer:
    def __init__(self) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.settimeout(5)
        self.port = self.sock.getsockname()[1]

    def receive(self, count: int) -> list[str]:
        packets = []
        while len(packets) < count:
            packet = self.sock.recv(65535).decode().rstrip("\n")
            # The client's own telemetry, sent at most every 10 s.
            if not packet.startswith("datadog.dogstatsd.client."):
                packets.append(packet)
        return packets


class DatadogAPI:
    def __init__(self) -> None:
        self.requests: list[dict] = []
        self.status = 202
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                body = self.rfile.read(int(self.headers["Content-Length"]))
                fake.requests.append(
                    {
                        "path": self.path,
                        "headers": dict(self.headers),
                        "body": json.loads(body),
                    }
                )
                ok = fake.status < 400
                reply = json.dumps(
                    {"status": "ok"} if ok else {"errors": ["Forbidden"]}
                )
                self.send_response(fake.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(reply)))
                self.end_headers()
                self.wfile.write(reply.encode())

            def log_message(self, format, *args) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch):
    for name in (
        "DATADOG_API_KEY",
        "DATADOG_APP_KEY",
        "DATADOG_API_HOST",
        "DATADOG_STATSD_HOST",
        "DATADOG_STATSD_PORT",
        "DATADOG_STATSD_SOCKET_PATH",
        "DATADOG_CONSTANT_TAGS",
        "DATADOG_TAGS",
        "DD_AGENT_HOST",
        "DD_DOGSTATSD_PORT",
        "DD_DOGSTATSD_URL",
        "DD_ENTITY_ID",
        "DD_ENV",
        "DD_SERVICE",
        "DD_VERSION",
        "DD_EXTERNAL_ENV",
        "RIVERS_CODE_LOCATION_NAME",
    ):
        monkeypatch.delenv(name, raising=False)
    # No container id from the cgroup in the packets, and no proxy for 127.0.0.1.
    monkeypatch.setenv("DD_ORIGIN_DETECTION_ENABLED", "false")
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")


@pytest.fixture
def statsd():
    server = DogStatsdServer()
    yield server
    server.sock.close()


@pytest.fixture
def api():
    fake = DatadogAPI()
    yield fake
    fake.server.shutdown()
    fake.server.server_close()


def _posted_event(api: DatadogAPI) -> dict:
    """The one event the fake API received, after checking its route and auth."""
    [request] = api.requests
    headers = request["headers"]
    assert request["path"] == "/api/v1/events"
    assert (headers["DD-API-KEY"], headers["Content-Type"]) == (KEY, "application/json")
    assert "DD-APPLICATION-KEY" not in headers
    return request["body"]


def test_resource_defaults_and_env(monkeypatch):
    assert DatadogResource().model_dump() == {
        "api_key": None,
        "app_key": None,
        "api_host": "https://api.datadoghq.com",
        "statsd_host": None,
        "statsd_port": None,
        "statsd_socket_path": None,
        "constant_tags": [],
    }

    monkeypatch.setenv("DATADOG_API_KEY", "key-from-env")
    monkeypatch.setenv("DATADOG_APP_KEY", "app-from-env")
    monkeypatch.setenv("DATADOG_API_HOST", "https://api.datadoghq.eu")
    monkeypatch.setenv("DATADOG_STATSD_HOST", "agent")
    monkeypatch.setenv("DATADOG_STATSD_PORT", "8126")
    monkeypatch.setenv("DATADOG_STATSD_SOCKET_PATH", "/var/run/datadog/dsd.socket")
    monkeypatch.setenv("DATADOG_CONSTANT_TAGS", '["env:prod"]')
    datadog = DatadogResource()
    assert datadog.model_dump() == {
        "api_key": SecretStr("key-from-env"),
        "app_key": SecretStr("app-from-env"),
        "api_host": "https://api.datadoghq.eu",
        "statsd_host": "agent",
        "statsd_port": 8126,
        "statsd_socket_path": "/var/run/datadog/dsd.socket",
        "constant_tags": ["env:prod"],
    }
    assert "key-from-env" not in datadog.model_dump_json() + repr(datadog)


def test_missing_sdk_names_the_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "datadog", None)
    monkeypatch.delitem(sys.modules, "rivers.integrations.datadog")
    with pytest.raises(ImportError, match=r"pip install rivers\[datadog\]"):
        importlib.import_module("rivers.integrations.datadog")


def test_client_sends_to_dogstatsd(statsd):
    datadog = DatadogResource(
        statsd_host="127.0.0.1", statsd_port=statsd.port, constant_tags=["env:test"]
    )
    client = datadog.get_client()
    assert isinstance(client, DogStatsd)
    assert datadog.get_client() is client

    @client.timed("run_fn")
    def run_fn() -> None:
        pass

    client.gauge("users.online", 1001, tags=["protocol:http"])
    client.increment("page.views")
    client.decrement("page.views")
    client.histogram("album.photo.count", 26, tags=["gender:female"])
    client.distribution("album.photo.count", 26, tags=["color:blue"])
    client.set("visitors.uniques", 999, tags=["browser:ie"])
    client.timing("query.response.time", 1234)
    client.event("Man down!", "This server needs assistance.", alert_type="error")
    client.service_check("svc.check_name", client.WARNING, message="degraded")
    run_fn()
    client.flush()
    client.wait_for_pending()

    *packets, timed = statsd.receive(10)
    assert packets == [
        "users.online:1001|g|#protocol:http,env:test",
        "page.views:1|c|#env:test",
        "page.views:-1|c|#env:test",
        "album.photo.count:26|h|#gender:female,env:test",
        "album.photo.count:26|d|#color:blue,env:test",
        "visitors.uniques:999|s|#browser:ie,env:test",
        "query.response.time:1234|ms|#env:test",
        "_e{9,29}:Man down!|This server needs assistance.|t:error|#env:test",
        "_sc|svc.check_name|1|#env:test|m:degraded",
    ]
    assert re.fullmatch(r"run_fn:[0-9.e-]+\|ms\|#env:test", timed)


def test_client_finds_the_agent_from_env(statsd, monkeypatch):
    monkeypatch.setenv("DD_AGENT_HOST", "127.0.0.1")
    monkeypatch.setenv("DD_DOGSTATSD_PORT", str(statsd.port))
    # A tag of this test's own, so no client cached by another test matches.
    client = DatadogResource(constant_tags=[f"port:{statsd.port}"]).get_client()

    client.increment("rows")

    assert statsd.receive(1) == [f"rows:1|c|#port:{statsd.port}"]


@pytest.mark.skipif(sys.platform == "win32", reason="no Unix datagram sockets")
def test_client_sends_to_a_unix_socket(tmp_path, monkeypatch):
    # Relative: a macOS tmp_path can pass the 104-byte socket path limit.
    monkeypatch.chdir(tmp_path)
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as server:
        server.bind("dsd.socket")
        server.settimeout(5)
        client = DatadogResource(statsd_socket_path="dsd.socket").get_client()

        client.increment("rows", tags=["asset:orders"])

        assert server.recv(65535).decode().rstrip("\n") == "rows:1|c|#asset:orders"


def test_client_api_uses_the_resource_keys(api):
    datadog = DatadogResource(api_key=KEY, app_key="test-app-key", api_host=api.url)

    datadog.get_client().api.Event.create(
        title="Deploy", text="v2 is live", tags=["env:test"], host="worker-1"
    )

    [request] = api.requests
    headers = request["headers"]
    assert request["path"] == "/api/v1/events"
    assert (headers["DD-API-KEY"], headers["DD-APPLICATION-KEY"]) == (
        KEY,
        "test-app-key",
    )
    assert request["body"] == {
        "title": "Deploy",
        "text": "v2 is live",
        "tags": ["env:test"],
        "host": "worker-1",
    }


def test_events_need_an_api_key():
    datadog = DatadogResource()
    for build in (
        datadog.failure_hook,
        datadog.success_hook,
        datadog.run_failure_sensor,
    ):
        with pytest.raises(ValueError, match="set DATADOG_API_KEY"):
            build()
    assert isinstance(datadog.get_client(), DatadogClient)


def test_failure_hook_posts_an_error_event(storage, executor_env, api):
    executor, io_factory = executor_env
    datadog = DatadogResource(api_key=KEY, api_host=api.url, constant_tags=["env:test"])

    @rs.Asset(
        name="boom",
        io_handler=io_factory(),
        hooks=[datadog.failure_hook(tags=["team:data"])],
    )
    def boom() -> int:
        raise ValueError("boom went the asset")

    # A second step: the parallel executor runs a lone step in-process.
    @rs.Asset(name="other", io_handler=io_factory())
    def other() -> int:
        return 2

    repo = rs.CodeRepository(assets=[boom, other], default_executor=executor)
    repo.resolve(storage=storage)
    run_id = repo.materialize(raise_on_error=False).run_id

    assert _posted_event(api) == {
        "title": "Asset boom failed",
        "text": (
            f"%%% \nAsset `boom` failed in run `{run_id}`:\n"
            "```\nValueError: boom went the asset\n```\n %%%"
        ),
        "alert_type": "error",
        "aggregation_key": run_id,
        "tags": ["asset:boom", f"run_id:{run_id}", "team:data", "env:test"],
    }


def test_success_hook_posts_a_success_event(storage, executor_env, api):
    executor, io_factory = executor_env
    datadog = DatadogResource(api_key=KEY, api_host=api.url)

    @rs.Asset(name="fine", io_handler=io_factory(), hooks=[datadog.success_hook()])
    def fine() -> int:
        return 1

    @rs.Asset(name="other", io_handler=io_factory())
    def other() -> int:
        return 2

    repo = rs.CodeRepository(assets=[fine, other], default_executor=executor)
    repo.resolve(storage=storage)
    run_id = repo.materialize().run_id

    assert _posted_event(api) == {
        "title": "Asset fine succeeded",
        "text": f"%%% \nAsset `fine` succeeded in run `{run_id}`.\n %%%",
        "alert_type": "success",
        "aggregation_key": run_id,
        "tags": ["asset:fine", f"run_id:{run_id}"],
    }


def test_hook_text_fn_run_link_and_code_location(storage, api, monkeypatch):
    monkeypatch.setenv("RIVERS_CODE_LOCATION_NAME", "etl")
    datadog = DatadogResource(api_key=KEY, api_host=api.url + "/")
    hook = datadog.success_hook(
        text_fn=lambda context: f"**{context.asset_name}** is fresh",
        ui_location_url=UI + "/",
        name="announce",
    )
    assert hook.name == "announce"

    @rs.Asset(name="fine", hooks=[hook])
    def fine() -> int:
        return 1

    repo = rs.CodeRepository(assets=[fine])
    repo.resolve(storage=storage)
    run_id = repo.materialize().run_id

    assert _posted_event(api) == {
        "title": "Asset fine succeeded",
        "text": f"%%% \n**fine** is fresh\n\n[View run]({UI}/runs/{run_id})\n %%%",
        "alert_type": "success",
        "aggregation_key": run_id,
        "tags": ["asset:fine", f"run_id:{run_id}", "code_location:etl"],
    }


def test_rejected_hook_event_does_not_fail_the_step(storage, api):
    api.status = 403
    datadog = DatadogResource(api_key=KEY, api_host=api.url)

    @rs.Asset(name="fine", hooks=[datadog.success_hook()])
    def fine() -> int:
        return 1

    repo = rs.CodeRepository(assets=[fine])
    repo.resolve(storage=storage)
    result = repo.materialize()

    assert (result.success, result.failed_assets) == (True, [])
    assert _posted_event(api)["title"] == "Asset fine succeeded"


def test_run_failure_sensor_definition():
    datadog = DatadogResource(api_key=KEY)
    sensor = datadog.run_failure_sensor(
        name="etl_alerts",
        minimum_interval="10s",
        monitored_jobs=["nightly"],
        default_status=rs.SensorStatus.Running,
    )
    assert sensor.name == "etl_alerts"
    assert sensor.monitored_status == rs.RunStatus.Failure
    assert sensor.monitored_jobs == ["nightly"]
    assert sensor.minimum_interval == "10s"
    assert sensor.default_status == rs.SensorStatus.Running
    assert datadog.run_failure_sensor().default_status == rs.SensorStatus.Stopped


def _run_sensor(repo, storage, fail, done):
    daemon = AutomationDaemon(repo=repo, storage=storage)
    daemon.start()
    try:
        assert wait_for_ticks(storage, SENSOR)
        result = fail()
        assert wait_until(done, timeout=15)
        # Two more ticks: the run must not be reported again.
        seen = len(storage.get_ticks(SENSOR, limit=100))
        wait_for_ticks(storage, SENSOR, min_count=seen + 2, timeout=15)
        return result
    finally:
        daemon.stop()


def test_run_failure_sensor_posts_one_event_per_failed_run(storage, api):
    datadog = DatadogResource(api_key=KEY, api_host=api.url, constant_tags=["env:test"])
    pd = rs.PartitionsDefinition.static_(["2024-01-01", "2024-01-02"])

    @rs.Asset(name="boom", partitions_def=pd)
    def boom() -> int:
        raise ValueError("boom went the asset")

    repo = rs.CodeRepository(
        assets=[boom],
        sensors=[
            datadog.run_failure_sensor(
                ui_location_url=UI,
                minimum_interval="1s",
                default_status=rs.SensorStatus.Running,
            )
        ],
    )
    repo.resolve(storage=storage)
    run_id = _run_sensor(
        repo,
        storage,
        lambda: repo.materialize(
            partition_key=rs.PartitionKey.single("2024-01-01"), raise_on_error=False
        ),
        lambda: api.requests,
    ).run_id

    assert _posted_event(api) == {
        "title": f"Run {run_id} failed",
        "text": (
            f"%%% \nRun `{run_id}` failed.\n\n"
            "`boom[2024-01-01]`:\n```\nValueError: boom went the asset\n```\n\n"
            f"[View run]({UI}/runs/{run_id})\n %%%"
        ),
        "alert_type": "error",
        "aggregation_key": run_id,
        "tags": [f"run_id:{run_id}", "partition:2024-01-01", "asset:boom", "env:test"],
    }


def test_run_failure_sensor_job_tags_and_text_fn(storage, api):
    datadog = DatadogResource(api_key=KEY, api_host=api.url)

    @rs.Asset(name="boom")
    def boom() -> int:
        raise ValueError("boom")

    job = rs.Job(name="nightly", assets=[boom])
    repo = rs.CodeRepository(
        assets=[boom],
        jobs=[job],
        sensors=[
            datadog.run_failure_sensor(
                tags=["team:data"],
                text_fn=lambda context: f"{context.run.job_name} needs a look",
                monitored_jobs=["nightly"],
                minimum_interval="1s",
                default_status=rs.SensorStatus.Running,
            )
        ],
    )
    repo.resolve(storage=storage)
    run_id = _run_sensor(
        repo,
        storage,
        lambda: repo.get_job("nightly").execute(raise_on_error=False),
        lambda: api.requests,
    ).run_id

    assert _posted_event(api) == {
        "title": f"Run {run_id} failed (job nightly)",
        "text": "%%% \nnightly needs a look\n %%%",
        "alert_type": "error",
        "aggregation_key": run_id,
        "tags": [f"run_id:{run_id}", "job:nightly", "asset:boom", "team:data"],
    }


def test_rejected_event_fails_the_sensor_tick(storage, api):
    api.status = 403
    datadog = DatadogResource(api_key=KEY, api_host=api.url)

    @rs.Asset(name="boom")
    def boom() -> int:
        raise ValueError("boom")

    repo = rs.CodeRepository(
        assets=[boom],
        sensors=[
            datadog.run_failure_sensor(
                minimum_interval="1s", default_status=rs.SensorStatus.Running
            )
        ],
    )
    repo.resolve(storage=storage)

    def failed_ticks():
        return [t for t in storage.get_ticks(SENSOR, limit=100) if t.status == "Failed"]

    run_id = _run_sensor(
        repo, storage, lambda: repo.materialize(raise_on_error=False), failed_ticks
    ).run_id

    [tick] = failed_ticks()
    assert tick.error == (
        f"run {run_id}: HTTPError: 403 Client Error: Forbidden for url: "
        f"{api.url}/api/v1/events"
    )
    assert len(api.requests) == 1
