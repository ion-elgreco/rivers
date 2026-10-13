"""Databricks integration: the resource, job runs, and job assets.

``FakeDatabricks`` serves the Jobs API endpoints on 127.0.0.1, so the real SDK
sends each request.
"""

import asyncio
import importlib
import json
import logging
import os
import re
import sys
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, NamedTuple
from urllib.parse import parse_qs, urlsplit

import pytest

pytest.importorskip("databricks.sdk")

from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.service import jobs  # noqa: E402

import rivers as rs  # noqa: E402
from rivers.integrations import databricks as databricks_module  # noqa: E402
from rivers.integrations.databricks import (  # noqa: E402
    DatabricksResource,
    DatabricksRun,
    DatabricksRunError,
)

TOKEN = "dapi-test"
START = 1_700_000_000_000  # epoch ms
END = START + 12_500

QUEUED = {"life_cycle_state": "QUEUED", "state_message": ""}
RUNNING = {"life_cycle_state": "RUNNING", "state_message": ""}
SUCCESS = {
    "life_cycle_state": "TERMINATED",
    "result_state": "SUCCESS",
    "state_message": "",
}
FAILED = {"life_cycle_state": "TERMINATED", "result_state": "FAILED"}
NOTEBOOK = {"notebook_task": {"notebook_path": "/Workspace/etl/sales"}}


@dataclass
class Run:
    """How the fake reports one run.

    ``runs/get`` returns ``states`` in order and then repeats the last one.
    With ``waits_for``, the run stays at its first state until that run has
    started.
    """

    run_id: int
    job_id: int
    states: list[dict]
    tasks: list[dict] = field(default_factory=list)
    outputs: dict[int, dict] = field(default_factory=dict)  # task run id -> get-output
    waits_for: int | None = None


class Request(NamedTuple):
    endpoint: str  # the path after ``/api/2.x/jobs/``
    data: dict  # JSON body, or query for a GET
    authorization: str | None


class FakeDatabricks:
    """The Jobs API on 127.0.0.1.

    ``run-now`` starts the next run added for its job id, ``runs/submit`` the
    next run added for its run name. The last run added for a key repeats.
    """

    def __init__(self) -> None:
        self.requests: list[Request] = []
        self.cancel_fails = False
        self._added: dict[int | str, list[Run]] = {}
        self._started: dict[int, Run] = {}
        self._outputs: dict[int, dict] = {}
        self._polls: dict[int, int] = {}
        self._canceled: set[int] = set()
        self._lock = threading.Lock()
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._server.fake = self  # type: ignore[attr-defined]
        self.host = f"http://127.0.0.1:{self._server.server_port}"
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.05},
            daemon=True,
        )
        self._thread.start()

    def add(self, key: int | str, *runs: Run) -> None:
        self._added[key] = list(runs)
        for run in runs:
            self._outputs.update(run.outputs)

    def bodies(self, endpoint: str) -> list[dict]:
        return [r.data for r in self.requests if r.endpoint == endpoint]

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()

    def respond(
        self, path: str, data: dict, authorization: str | None
    ) -> tuple[int, dict]:
        if path == "/.well-known/databricks-config":
            return 200, {}

        endpoint = path.split("/jobs/", 1)[1]
        with self._lock:
            self.requests.append(Request(endpoint, data, authorization))
            if endpoint in ("run-now", "runs/submit"):
                runs = self._added[data.get("job_id", data.get("run_name"))]
                run = runs.pop(0) if len(runs) > 1 else runs[0]
                self._started[run.run_id] = run
                return 200, {"run_id": run.run_id}

            run_id = int(data["run_id"])
            if endpoint == "runs/cancel":
                if self.cancel_fails:
                    return 400, {
                        "error_code": "INVALID_STATE",
                        "message": f"Run {run_id} cannot be canceled",
                    }
                self._canceled.add(run_id)
                return 200, {}
            if endpoint == "runs/get-output":
                if run_id not in self._outputs:
                    return 400, {
                        "error_code": "INVALID_PARAMETER_VALUE",
                        "message": f"Run {run_id} has no output",
                    }
                return 200, self._outputs[run_id]
            return 200, self._get(self._started[run_id])

    def _get(self, run: Run) -> dict:
        if run.waits_for is not None and run.waits_for not in self._started:
            state = run.states[0]
        else:
            poll = self._polls[run.run_id] = self._polls.get(run.run_id, 0) + 1
            state = run.states[min(poll, len(run.states)) - 1]
        if run.run_id in self._canceled:
            state = {"life_cycle_state": "TERMINATED", "result_state": "CANCELED"}
        ended = state["life_cycle_state"] in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")
        return {
            "run_id": run.run_id,
            "job_id": run.job_id,
            "run_page_url": f"{self.host}/jobs/{run.job_id}/runs/{run.run_id}",
            "state": state,
            "start_time": START,
            "end_time": END if ended else 0,
            "tasks": [
                {
                    "run_page_url": f"{self.host}/jobs/{run.job_id}/runs/{task['run_id']}",
                    "start_time": START + 1_000,
                    "end_time": START + 11_000,
                    **task,
                }
                for task in run.tasks
            ],
        }


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        url = urlsplit(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            data = json.loads(self.rfile.read(length))
        else:
            data = {key: values[0] for key, values in parse_qs(url.query).items()}
        status, payload = self.server.fake.respond(  # type: ignore[attr-defined]
            url.path, data, self.headers.get("Authorization")
        )

        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_POST = do_GET

    def log_message(self, format: str, *args: Any) -> None:
        pass


@pytest.fixture(autouse=True)
def _no_databricks_env(monkeypatch):
    for name in list(os.environ):
        if name.startswith(("DATABRICKS_", "ARM_")):
            monkeypatch.delenv(name)


@pytest.fixture
def fake():
    server = FakeDatabricks()
    yield server
    server.close()


@pytest.fixture(params=["in_process", "parallel"])
def executor(request) -> str:
    return request.param


def make_resource(fake: FakeDatabricks, **fields: Any) -> DatabricksResource:
    return DatabricksResource(
        **{"host": fake.host, "token": TOKEN, "poll_interval": 0, **fields}
    )


@pytest.fixture(params=["sync", "async"])
def call(request) -> Callable[..., DatabricksRun]:
    """Calls ``run_job`` or ``submit_run``, or its async version."""

    def call(resource: DatabricksResource, method: str, *args, **kwargs):
        if request.param == "sync":
            return getattr(resource, method)(*args, **kwargs)
        return asyncio.run(getattr(resource, f"async_{method}")(*args, **kwargs))

    return call


def make_repo(storage, executor: str, *assets: rs.Asset) -> rs.CodeRepository:
    repo = rs.CodeRepository(
        assets=list(assets),
        default_executor=rs.Executor.in_process()
        if executor == "in_process"
        else rs.Executor.parallel(max_workers=2),
    )
    repo.resolve(storage=storage)
    return repo


def task(key: str, run_id: int, state: dict, **spec: Any) -> dict:
    return {"task_key": key, "run_id": run_id, "state": state, **spec}


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


def run_metadata(
    fake: FakeDatabricks, run_id: int, job_id: int, **outputs: str
) -> dict:
    """The stored metadata of a job asset without ``tasks``."""
    metadata = {
        "databricks/run_id": {"Int": {"value": run_id}},
        "databricks/job_id": {"Int": {"value": job_id}},
        "databricks/run_url": {
            "Url": {"value": f"{fake.host}/jobs/{job_id}/runs/{run_id}"}
        },
        "databricks/duration": {"Duration": {"value": 12.5}},
    }
    for task_key, output in outputs.items():
        metadata[f"databricks/output/{task_key}"] = {"Text": {"value": output}}
    return metadata


def task_metadata(
    fake: FakeDatabricks, task_key: str, task_run_id: int, output: str | None = None
) -> dict:
    """The stored metadata of one output of the job asset for job 9, run 90."""
    metadata = {
        "databricks/run_id": {"Int": {"value": 90}},
        "databricks/job_id": {"Int": {"value": 9}},
        "databricks/task_key": {"Text": {"value": task_key}},
        "databricks/run_url": {
            "Url": {"value": f"{fake.host}/jobs/9/runs/{task_run_id}"}
        },
        "databricks/duration": {"Duration": {"value": 10.0}},
    }
    if output is not None:
        metadata["databricks/output"] = {"Text": {"value": output}}
    return metadata


def messages(caplog) -> list[str]:
    return [
        r.getMessage() for r in caplog.records if r.name == databricks_module.__name__
    ]


# ── Resource ─────────────────────────────────────────────────────────


def test_fields_from_env(monkeypatch, fake):
    monkeypatch.setenv("DATABRICKS_HOST", fake.host)
    monkeypatch.setenv("DATABRICKS_TOKEN", TOKEN)
    monkeypatch.setenv("DATABRICKS_POLL_TIMEOUT", "3600")

    resource = DatabricksResource()
    client = resource.get_client()

    assert (resource.host, resource.poll_timeout) == (fake.host, 3600)
    assert resource.token is not None and resource.token.get_secret_value() == TOKEN
    assert isinstance(client, WorkspaceClient)
    assert (client.config.host, client.config.token) == (fake.host, TOKEN)
    assert client.config.auth_type == "pat"


def test_get_client_passes_each_field_to_the_sdk(monkeypatch):
    calls: list[dict] = []
    monkeypatch.setattr(
        databricks_module, "WorkspaceClient", lambda **kw: calls.append(kw)
    )
    fields = {
        "host": "https://adb-1.azuredatabricks.net",
        "token": "dapi",
        "client_id": "sp-id",
        "client_secret": "sp-secret",
        "azure_client_id": "app-id",
        "azure_client_secret": "app-secret",
        "azure_tenant_id": "tenant",
    }

    DatabricksResource(**fields, poll_timeout=60).get_client()
    DatabricksResource().get_client()

    # Fields left unset reach the SDK as None, so it reads its own configuration.
    assert calls == [fields, dict.fromkeys(fields)]


def test_repr_hides_secrets():
    resource = DatabricksResource(
        token="dapi-secret",
        client_secret="oauth-secret",
        azure_client_secret="azure-secret",
    )

    assert "dapi-secret" not in repr(resource)
    assert "oauth-secret" not in repr(resource)
    assert "azure-secret" not in repr(resource)


def test_launch_dialog_gets_no_secret(grpc_stubs, storage):
    grpc = pytest.importorskip("grpc")

    @rs.Asset
    def noop() -> int:
        return 1

    repo = rs.CodeRepository(
        assets=[noop],
        resources={
            "databricks": DatabricksResource(
                host="https://dbc.example.com",
                token="dapi-secret",
                client_id="sp-id",
                client_secret="sp-secret",
            )
        },
    )
    repo.resolve(storage=storage)
    port = repo._start_grpc_server("127.0.0.1", 0)
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    try:
        pb2, pb2_grpc = grpc_stubs
        stub = pb2_grpc.CodeLocationServiceStub(channel)
        [resource] = stub.GetAssetsInfo(pb2.GetAssetsInfoRequest()).resources
    finally:
        channel.close()
        repo._stop_grpc_server()

    properties = json.loads(resource.config_schema)["properties"]
    assert "dapi-secret" not in resource.config_schema
    assert "sp-secret" not in resource.config_schema
    assert properties["host"]["default"] == "https://dbc.example.com"
    assert properties["client_id"]["default"] == "sp-id"


def test_missing_sdk_names_the_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "databricks.sdk", None)
    monkeypatch.delitem(sys.modules, "rivers.integrations.databricks")
    with pytest.raises(ImportError, match=r"pip install rivers\[databricks\]"):
        importlib.import_module("rivers.integrations.databricks")


# ── run_job() and submit_run() ───────────────────────────────────────


def test_run_job_polls_until_the_run_ends(fake, caplog, call):
    fake.add(7, Run(70, job_id=7, states=[QUEUED, RUNNING, RUNNING, SUCCESS]))
    caplog.set_level(logging.INFO, logger=databricks_module.__name__)

    run = call(make_resource(fake), "run_job", 7)

    url = f"{fake.host}/jobs/7/runs/70"
    assert run == DatabricksRun(run_id=70, job_id=7, run_page_url=url, duration=12.5)
    assert fake.bodies("run-now") == [{"job_id": 7}]
    assert fake.bodies("runs/get") == [{"run_id": "70"}] * 4
    assert messages(caplog) == [
        f"Databricks run 70 is QUEUED: {url}",
        f"Databricks run 70 is RUNNING: {url}",
        "Databricks run 70 ended with SUCCESS",
    ]


def test_run_job_sends_parameters_and_tasks(fake, call):
    fake.add(7, Run(70, job_id=7, states=[SUCCESS]))

    call(
        make_resource(fake),
        "run_job",
        7,
        job_parameters={"day": "2024-01-01"},
        only=["ingest"],
    )

    assert fake.bodies("run-now") == [
        {"job_id": 7, "job_parameters": {"day": "2024-01-01"}, "only": ["ingest"]}
    ]


@pytest.mark.parametrize("cancel", [True, False])
def test_poll_timeout_raises(fake, cancel, call):
    fake.add(7, Run(70, job_id=7, states=[RUNNING]))
    databricks = make_resource(
        fake, poll_interval=0.01, poll_timeout=0.05, cancel_on_interrupt=cancel
    )

    with pytest.raises(
        TimeoutError, match=r"^Databricks run 70 did not end within 0.05 s$"
    ):
        call(databricks, "run_job", 7)

    assert fake.bodies("runs/cancel") == ([{"run_id": 70}] if cancel else [])


class _InterruptWhenRunning(logging.Logger):
    def info(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        if "is RUNNING" in msg % args:
            raise KeyboardInterrupt


def test_interrupt_cancels_the_run(fake, call):
    fake.add("adhoc", Run(70, job_id=7, states=[RUNNING]))

    with pytest.raises(KeyboardInterrupt):
        call(
            make_resource(fake),
            "submit_run",
            [{"task_key": "main", **NOTEBOOK}],
            run_name="adhoc",
            log=_InterruptWhenRunning("databricks"),
        )

    assert fake.bodies("runs/cancel") == [{"run_id": 70}]


def test_cancelling_the_async_wait_cancels_the_run(fake):
    fake.add(7, Run(70, job_id=7, states=[RUNNING]))

    async def cancel_while_running() -> None:
        wait = asyncio.create_task(make_resource(fake).async_run_job(7))
        while not fake.bodies("runs/get"):
            await asyncio.sleep(0.01)
        wait.cancel()
        with pytest.raises(asyncio.CancelledError):
            await wait

    asyncio.run(cancel_while_running())

    assert fake.bodies("runs/cancel") == [{"run_id": 70}]


def test_failed_cancel_keeps_the_original_error(fake, caplog, call):
    fake.add(7, Run(70, job_id=7, states=[RUNNING]))
    fake.cancel_fails = True

    with pytest.raises(TimeoutError):
        call(make_resource(fake, poll_interval=0.01, poll_timeout=0.05), "run_job", 7)

    assert fake.bodies("runs/cancel") == [{"run_id": 70}]
    assert messages(caplog)[-1] == (
        "Could not cancel Databricks run 70: Run 70 cannot be canceled"
    )


def test_failure_message_has_each_failed_task(fake, call):
    fake.add(
        7,
        Run(
            70,
            job_id=7,
            states=[{**FAILED, "state_message": "2 tasks failed"}],
            tasks=[
                task("ingest", 71, FAILED, **NOTEBOOK),
                task(
                    "report",
                    72,
                    {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "UPSTREAM_FAILED",
                    },
                ),
                task(
                    "audit",
                    73,
                    {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "TIMEDOUT",
                        "state_message": "Task audit timed out",
                    },
                ),
                task("load", 74, FAILED),
            ],
            outputs={
                71: {
                    "error": "ZeroDivisionError: division by zero",
                    "error_trace": "Traceback (most recent call last):\n  1 / 0",
                },
                74: {},
            },
        ),
    )

    with pytest.raises(DatabricksRunError) as raised:
        call(make_resource(fake), "run_job", 7)

    assert str(raised.value) == (
        "Databricks run 70 ended with FAILED: 2 tasks failed\n"
        f"{fake.host}/jobs/7/runs/70\n"
        "Task ingest FAILED: ZeroDivisionError: division by zero\n"
        "Traceback (most recent call last):\n"
        "  1 / 0\n"
        # No output for 73: the fake answers 400, so the state message is used.
        "Task audit TIMEDOUT: Task audit timed out\n"
        "Task load FAILED"
    )
    assert fake.bodies("runs/get-output") == [
        {"run_id": "71"},
        {"run_id": "73"},
        {"run_id": "74"},
    ]


@pytest.mark.parametrize(
    ("state", "message"),
    [
        pytest.param(
            {
                "life_cycle_state": "INTERNAL_ERROR",
                "state_message": "Could not reach the driver",
            },
            "Databricks run 70 ended with INTERNAL_ERROR: Could not reach the driver",
            id="internal_error",
        ),
        pytest.param(
            {
                "life_cycle_state": "SKIPPED",
                "result_state": "MAXIMUM_CONCURRENT_RUNS_REACHED",
                "state_message": "",
            },
            "Databricks run 70 ended with MAXIMUM_CONCURRENT_RUNS_REACHED",
            id="skipped",
        ),
        pytest.param(
            {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS_WITH_FAILURES",
                "state_message": "",
            },
            "Databricks run 70 ended with SUCCESS_WITH_FAILURES",
            id="success_with_failures",
        ),
    ],
)
def test_runs_that_end_without_success_raise(fake, state, message, call):
    fake.add(7, Run(70, job_id=7, states=[state]))

    with pytest.raises(DatabricksRunError) as raised:
        call(make_resource(fake), "run_job", 7)

    assert str(raised.value) == f"{message}\n{fake.host}/jobs/7/runs/70"
    assert fake.bodies("runs/cancel") == []


def test_outputs_of_successful_notebook_tasks(fake, call):
    fake.add(
        7,
        Run(
            70,
            job_id=7,
            states=[SUCCESS],
            tasks=[
                task("totals", 71, SUCCESS, **NOTEBOOK),
                task("no_exit", 72, SUCCESS, **NOTEBOOK),
                task("script", 73, SUCCESS, spark_python_task={"python_file": "s.py"}),
                task(
                    "skipped",
                    74,
                    {"life_cycle_state": "TERMINATED", "result_state": "EXCLUDED"},
                    **NOTEBOOK,
                ),
            ],
            outputs={
                71: {"notebook_output": {"result": "42", "truncated": False}},
                72: {"notebook_output": {}},
            },
        ),
    )

    run = call(make_resource(fake), "run_job", 7)

    assert run.outputs == {"totals": "42"}
    assert fake.bodies("runs/get-output") == [{"run_id": "71"}, {"run_id": "72"}]


def test_submit_run_takes_sdk_tasks_and_dicts(fake, call):
    fake.add("nightly", Run(80, job_id=8, states=[SUCCESS]))

    run = call(
        make_resource(fake),
        "submit_run",
        [
            jobs.SubmitTask(
                task_key="extract",
                notebook_task=jobs.NotebookTask(
                    notebook_path="/Workspace/etl/extract",
                    base_parameters={"day": "1"},
                ),
                existing_cluster_id="0101-abc",
            ),
            {
                "task_key": "load",
                "depends_on": [{"task_key": "extract"}],
                "spark_python_task": {
                    "python_file": "load.py",
                    "parameters": ["--day", "1"],
                },
                "existing_cluster_id": "0101-abc",
            },
        ],
        run_name="nightly",
        timeout_seconds=3600,
    )

    assert run.run_id == 80
    assert fake.bodies("runs/submit") == [
        {
            "run_name": "nightly",
            "timeout_seconds": 3600,
            "tasks": [
                {
                    "task_key": "extract",
                    "notebook_task": {
                        "notebook_path": "/Workspace/etl/extract",
                        "base_parameters": {"day": "1"},
                    },
                    "existing_cluster_id": "0101-abc",
                },
                {
                    "task_key": "load",
                    "depends_on": [{"task_key": "extract"}],
                    "spark_python_task": {
                        "python_file": "load.py",
                        "parameters": ["--day", "1"],
                    },
                    "existing_cluster_id": "0101-abc",
                },
            ],
        }
    ]


# ── job_asset() without tasks ────────────────────────────────────────


def test_job_asset_runs_the_job(fake, storage, executor):
    fake.add(
        7,
        Run(
            70,
            job_id=7,
            states=[RUNNING, SUCCESS],
            tasks=[task("ingest", 71, SUCCESS, **NOTEBOOK)],
            outputs={71: {"notebook_output": {"result": '{"rows": 3}'}}},
        ),
    )
    asset = make_resource(fake).job_asset(
        7, name="daily_sales", job_parameters={"day": "2024-01-01"}
    )
    repo = make_repo(storage, executor, asset)

    assert repo.materialize().success

    assert (asset.name, asset.kinds, asset.is_async) == (
        "daily_sales",
        ["databricks"],
        True,
    )
    assert fake.bodies("run-now") == [
        {"job_id": 7, "job_parameters": {"day": "2024-01-01"}}
    ]
    assert {r.authorization for r in fake.requests} == {f"Bearer {TOKEN}"}
    assert materializations(storage, "daily_sales") == [
        run_metadata(fake, 70, 7, ingest='{"rows": 3}')
    ]


def test_job_parameters_from_the_partition(fake, storage, executor):
    fake.add(7, Run(70, job_id=7, states=[SUCCESS]))
    fake.add(8, Run(80, job_id=8, states=[SUCCESS]))
    databricks = make_resource(fake)
    days = rs.PartitionsDefinition.static_(["2024-01-01", "2024-01-02"])
    assets = [
        databricks.job_asset(
            job_id,
            name=f"job_{job_id}",
            partitions_def=days,
            job_parameters=lambda context: {"day": context.partition_key},
        )
        for job_id in (7, 8)
    ]
    repo = make_repo(storage, executor, *assets)

    assert repo.materialize(partition_key=rs.PartitionKey.single("2024-01-02")).success

    assert sorted(fake.bodies("run-now"), key=lambda body: body["job_id"]) == [
        {"job_id": 7, "job_parameters": {"day": "2024-01-02"}},
        {"job_id": 8, "job_parameters": {"day": "2024-01-02"}},
    ]


def test_failed_job_fails_the_asset(fake, storage, executor):
    fake.add(
        7,
        Run(
            70,
            job_id=7,
            states=[{**FAILED, "state_message": "Task ingest failed"}],
            tasks=[task("ingest", 71, FAILED, **NOTEBOOK)],
            outputs={71: {"error": "ZeroDivisionError: division by zero"}},
        ),
    )
    fake.add(8, Run(80, job_id=8, states=[SUCCESS]))
    databricks = make_resource(fake)
    repo = make_repo(
        storage,
        executor,
        databricks.job_asset(7, name="orders"),
        databricks.job_asset(8, name="customers"),
    )

    assert not repo.materialize(raise_on_error=False).success

    assert step_errors(storage, "orders") == [
        "DatabricksRunError: Databricks run 70 ended with FAILED: Task ingest failed\n"
        f"{fake.host}/jobs/7/runs/70\n"
        "Task ingest FAILED: ZeroDivisionError: division by zero"
    ]
    assert materializations(storage, "orders") == []
    assert materializations(storage, "customers") == [run_metadata(fake, 80, 8)]
    assert fake.bodies("runs/cancel") == []


def test_poll_timeout_fails_the_asset_with_reason_timeout(fake, storage):
    fake.add(7, Run(70, job_id=7, states=[RUNNING]))
    databricks = make_resource(fake, poll_interval=0.01, poll_timeout=0.05)
    repo = make_repo(storage, "in_process", databricks.job_asset(7, name="orders"))

    assert not repo.materialize(raise_on_error=False).success

    [failure] = [
        dict(e.metadata)
        for e in storage.get_events_for_asset("orders")
        if e.event_type == "StepFailure"
    ]
    assert failure["error"] == (
        "TimeoutError: Databricks run 70 did not end within 0.05 s"
    )
    assert failure["rivers/failure_reason"] == "timeout"
    assert fake.bodies("runs/cancel") == [{"run_id": 70}]


def test_job_asset_retries(fake, storage, executor):
    fake.add(
        7,
        Run(70, job_id=7, states=[FAILED]),
        Run(71, job_id=7, states=[SUCCESS]),
    )
    asset = make_resource(fake).job_asset(
        7,
        name="orders",
        retry=rs.RetryPolicy(max_retries=1, retry_on=[DatabricksRunError]),
    )
    repo = make_repo(storage, executor, asset)

    assert repo.materialize().success

    assert fake.bodies("run-now") == [{"job_id": 7}] * 2
    assert materializations(storage, "orders") == [run_metadata(fake, 71, 7)]


@pytest.mark.parametrize("with_tasks", [False, True], ids=["job", "tasks"])
def test_job_assets_wait_at_the_same_time(fake, storage, executor, with_tasks):
    """Each job's run ends only once the other job's run has started, so the
    two assets succeed only when they wait at the same time."""
    for job_id, other_run in ((7, 80), (8, 70)):
        main = task("main", job_id * 10 + 1, SUCCESS)
        fake.add(
            job_id,
            Run(
                job_id * 10,
                job_id=job_id,
                states=[RUNNING, SUCCESS],
                tasks=[main],
                waits_for=other_run,
            ),
        )
    databricks = make_resource(fake, poll_interval=0.01, poll_timeout=5)
    repo = make_repo(
        storage,
        executor,
        *(
            databricks.job_asset(
                job_id,
                name=f"job_{job_id}",
                tasks={"main": rs.AssetDef(f"main_{job_id}")} if with_tasks else None,
            )
            for job_id in (7, 8)
        ),
    )

    assert repo.materialize().success


# ── job_asset() with tasks ───────────────────────────────────────────

SALES_TASKS = [
    task("orders", 91, SUCCESS, **NOTEBOOK),
    task("refunds", 92, SUCCESS, spark_python_task={"python_file": "refunds.py"}),
]


def test_job_asset_has_an_output_per_task():
    databricks = DatabricksResource()
    unnamed = rs.AssetDef(group="finance")

    listed = databricks.job_asset(9, name="sales", tasks=["orders", "refunds"])
    mapped = databricks.job_asset(
        9, name="sales", tasks={"orders": rs.AssetDef("raw/orders"), "refunds": unnamed}
    )

    assert listed.name == "sales"
    assert [(d.name, d.kinds) for d in listed.output_defs] == [
        ("orders", ["databricks"]),
        ("refunds", ["databricks"]),
    ]
    assert [(d.name, d.group) for d in mapped.output_defs] == [
        ("raw/orders", None),
        ("refunds", "finance"),
    ]
    assert unnamed.name is None


@pytest.mark.parametrize(
    ("tasks", "message"),
    [
        ([], "Databricks job asset 'sales' has no tasks"),
        (
            {"a": rs.AssetDef("x"), "b": rs.AssetDef("x")},
            "Databricks tasks 'a' and 'b' both have output name 'x'",
        ),
    ],
)
def test_bad_tasks_raise(tasks, message):
    with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
        DatabricksResource().job_asset(9, name="sales", tasks=tasks)


def test_each_task_records_its_output(fake, storage, executor):
    fake.add(
        9,
        Run(
            90,
            job_id=9,
            states=[RUNNING, SUCCESS],
            tasks=SALES_TASKS,
            outputs={91: {"notebook_output": {"result": "1200"}}},
        ),
    )
    asset = make_resource(fake).job_asset(
        9,
        name="sales",
        tasks=["orders", "refunds"],
        job_parameters={"day": "2024-01-01"},
    )
    repo = make_repo(storage, executor, asset)

    assert repo.materialize().success

    assert fake.bodies("run-now") == [
        {"job_id": 9, "job_parameters": {"day": "2024-01-01"}}
    ]
    assert materializations(storage, "orders") == [
        task_metadata(fake, "orders", 91, output="1200")
    ]
    assert materializations(storage, "refunds") == [task_metadata(fake, "refunds", 92)]


def test_subset_runs_only_the_selected_tasks(fake, storage):
    excluded = {"life_cycle_state": "TERMINATED", "result_state": "EXCLUDED"}
    fake.add(
        9,
        Run(
            90,
            job_id=9,
            states=[SUCCESS],
            tasks=[task("orders", 91, excluded, **NOTEBOOK), SALES_TASKS[1]],
        ),
    )
    asset = make_resource(fake).job_asset(9, name="sales", tasks=["orders", "refunds"])
    repo = make_repo(storage, "in_process", asset)

    assert repo.materialize(selection=["refunds"]).success

    assert fake.bodies("run-now") == [{"job_id": 9, "only": ["refunds"]}]
    assert materializations(storage, "orders") == []
    assert materializations(storage, "refunds") == [task_metadata(fake, "refunds", 92)]


def test_failed_task_fails_only_its_output(fake, storage, executor):
    fake.add(
        9,
        Run(
            90,
            job_id=9,
            states=[{**FAILED, "state_message": "Task refunds failed"}],
            tasks=[SALES_TASKS[0], task("refunds", 92, FAILED)],
            outputs={
                91: {"notebook_output": {}},
                92: {"error": "ValueError: negative refund"},
            },
        ),
    )
    asset = make_resource(fake).job_asset(9, name="sales", tasks=["orders", "refunds"])
    repo = make_repo(storage, executor, asset)

    assert not repo.materialize(raise_on_error=False).success

    assert materializations(storage, "orders") == [task_metadata(fake, "orders", 91)]
    assert materializations(storage, "refunds") == []
    assert step_errors(storage, "refunds") == [
        "DatabricksRunError: Databricks run 90 ended with FAILED: Task refunds failed\n"
        f"{fake.host}/jobs/9/runs/90\n"
        "Task refunds FAILED: ValueError: negative refund"
    ]


def test_task_missing_from_the_run_fails_its_output(fake, storage):
    fake.add(
        9,
        Run(
            90,
            job_id=9,
            states=[SUCCESS],
            tasks=[SALES_TASKS[0]],
            outputs={91: {"notebook_output": {}}},
        ),
    )
    asset = make_resource(fake).job_asset(9, name="sales", tasks=["orders", "refunds"])
    repo = make_repo(storage, "in_process", asset)

    assert not repo.materialize(raise_on_error=False).success

    assert materializations(storage, "orders") == [task_metadata(fake, "orders", 91)]
    assert step_errors(storage, "refunds") == [
        "DatabricksRunError: Databricks run 90 ended with SUCCESS\n"
        f"{fake.host}/jobs/9/runs/90\n"
        "Task refunds is not in the run"
    ]


def test_failed_task_without_an_output_is_logged(fake, storage, caplog):
    fake.add(
        9,
        Run(
            90,
            job_id=9,
            states=[FAILED],
            tasks=[*SALES_TASKS, task("cleanup", 93, FAILED)],
            outputs={
                91: {"notebook_output": {}},
                93: {"error": "OSError: disk full"},
            },
        ),
    )
    asset = make_resource(fake).job_asset(9, name="sales", tasks=["orders", "refunds"])
    repo = make_repo(storage, "in_process", asset)

    assert repo.materialize().success

    assert (
        "Databricks run 90 ended with FAILED\n"
        f"{fake.host}/jobs/9/runs/90\n"
        "Task cleanup FAILED: OSError: disk full"
    ) in caplog.messages
    assert materializations(storage, "refunds") == [task_metadata(fake, "refunds", 92)]
