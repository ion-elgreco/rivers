"""OTLP export from the deployed stack.

The rivers release sets `otel.endpoint` and `otel.headers.existingSecret`
(see `dev/k3d/helmfile.yaml.gotmpl`). The operator must forward both to every
code-location, run, and step pod, and the code-location daemon's sensor
evaluation must land in the rivers-dev collector, which only accepts the
bearer token from the `otel-headers` Secret.
"""

import re
import time

import grpc
import pytest
from kr8s.objects import Deployment, Job, Pod, Service

from .conftest import KUBECTL_CONTEXT, cluster_gate, kube_api
from .test_k8s_integration import (
    CODE_LOCATION_NAME,
    NAMESPACE,
    TERMINAL_PHASES,
    GrpcChannel,
    _cluster_reachable,
    _delete_runs_and_workers,
    _dump_debug_info,
    _wait_for_executor_pod,
    _wait_for_phase,
    _wait_for_run_cr,
)

COLLECTOR = "otel-collector"
ENDPOINT = f"http://{COLLECTOR}.{NAMESPACE}.svc:4317"
HEADERS_SECRET_REF = {"name": "otel-headers", "key": "headers"}
TOKEN = "rivers-k3d-token"
EXPORT_METHOD = "/opentelemetry.proto.collector.trace.v1.TraceService/Export"

pytestmark = [
    cluster_gate(
        _cluster_reachable(),
        f"k3d cluster '{KUBECTL_CONTEXT}' not reachable or namespace '{NAMESPACE}' missing",
    ),
    pytest.mark.timeout(600),
]


def _assert_chart_otel_env(container: dict, what: str) -> None:
    env = {e["name"]: e for e in container.get("env", [])}
    endpoint = env.get("OTEL_EXPORTER_OTLP_ENDPOINT", {}).get("value")
    assert endpoint == ENDPOINT, f"{what}: endpoint {endpoint!r}"
    headers = (
        env.get("OTEL_EXPORTER_OTLP_HEADERS", {})
        .get("valueFrom", {})
        .get("secretKeyRef")
    )
    assert headers == HEADERS_SECRET_REF, f"{what}: headers {headers!r}"


def _wait_for_step_jobs(timeout: int = 180) -> list[Job]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        jobs = list(
            Job.list(
                namespace=NAMESPACE,
                label_selector="rivers.io/component=step-worker",
                api=kube_api(),
            )
        )
        if jobs:
            return jobs
        time.sleep(2)
    return []


def test_code_location_pod_carries_the_chart_otel_settings():
    dep = Deployment.get(CODE_LOCATION_NAME, namespace=NAMESPACE, api=kube_api())
    container = dep.raw["spec"]["template"]["spec"]["containers"][0]
    _assert_chart_otel_env(container, "code-location")


class TestRunAndStepPods:
    def setup_method(self):
        _delete_runs_and_workers()

    def teardown_method(self):
        _delete_runs_and_workers()

    def test_run_and_step_pods_carry_the_chart_otel_settings(self, grpc_stubs):
        with GrpcChannel(grpc_stubs) as ch:
            resp = ch.stub.ExecuteJob(ch.pb2.ExecuteJobRequest(job_name="k8s_step_job"))
            assert resp.run_id

        run_name = _wait_for_run_cr(timeout=30)
        assert run_name, "No Run CR appeared after ExecuteJob call"
        exec_pod = _wait_for_executor_pod(run_name, timeout=120)
        assert exec_pod, (
            f"No executor pod for '{run_name}'\n{_dump_debug_info(run_name)}"
        )
        pod = Pod.get(exec_pod, namespace=NAMESPACE, api=kube_api())
        _assert_chart_otel_env(pod.raw["spec"]["containers"][0], "run pod")

        jobs = _wait_for_step_jobs()
        assert jobs, f"No step Job for '{run_name}'\n{_dump_debug_info(run_name)}"
        for job in jobs:
            container = job.raw["spec"]["template"]["spec"]["containers"][0]
            _assert_chart_otel_env(container, f"step job {job.name}")

        phase = _wait_for_phase(run_name, TERMINAL_PHASES)
        assert phase == "Succeeded", f"{phase}\n{_dump_debug_info(run_name)}"


def test_collector_rejects_exports_without_the_bearer_token():
    svc = Service.get(COLLECTOR, namespace=NAMESPACE, api=kube_api())
    with svc.portforward(remote_port=4317, local_port="auto") as local_port:
        with grpc.insecure_channel(f"127.0.0.1:{local_port}") as channel:
            grpc.channel_ready_future(channel).result(timeout=10)
            export = channel.unary_unary(EXPORT_METHOD)

            with pytest.raises(grpc.RpcError) as err:
                export(b"", timeout=10)  # an empty ExportTraceServiceRequest
            assert err.value.code() == grpc.StatusCode.UNAUTHENTICATED

            # With the token the same call succeeds.
            export(b"", metadata=(("authorization", f"Bearer {TOKEN}"),), timeout=10)


def _collector_logs() -> str:
    pods = [
        pod
        for pod in Pod.list(
            namespace=NAMESPACE,
            label_selector=f"app.kubernetes.io/name={COLLECTOR}",
            api=kube_api(),
        )
        if pod.raw["status"].get("phase") == "Running"
        and not pod.raw["metadata"].get("deletionTimestamp")
    ]
    return "\n".join(line for pod in pods for line in pod.logs())


def _probe_spans(logs: str) -> list[tuple[str, str]]:
    """(resource header, span block) for each `eval` span of `otel_probe`.

    The debug exporter prints each batch as a `ResourceSpans #N` header with
    the resource attributes, followed by one `Span #N` block per span.
    """
    found = []
    for batch in re.split(r"\bResourceSpans #\d+", logs)[1:]:
        header, *spans = re.split(r"\bSpan #\d+", batch)
        found += [
            (header, span)
            for span in spans
            if re.search(r"^\s*Name\s*:\s*eval\s*$", span, re.M)
            and "-> name: Str(otel_probe)" in span
        ]
    return found


def test_collector_receives_the_probe_sensor_span():
    """The daemon in the code-location pod evaluates `otel_probe` every 30s; the
    collector logs the `eval` span it exports with the token from the Secret."""
    logs = ""
    deadline = time.monotonic() + 150
    while time.monotonic() < deadline:
        logs = _collector_logs()
        if spans := _probe_spans(logs):
            break
        time.sleep(5)
    else:
        pytest.fail(f"no eval span for otel_probe in collector logs:\n{logs[-4000:]}")

    header, span = spans[-1]
    assert "service.name: Str(rivers)" in header, header
    assert "automation_type: Str(Sensor)" in span, span
