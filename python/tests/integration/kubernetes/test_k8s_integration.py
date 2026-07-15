"""Integration tests for the Kubernetes execution stack.

Exercises the full flow:
  test → (port-forward) → gRPC ExecuteJob/Materialize on code-location pod
       → daemon enqueues run → K8sRunBackend creates Run CR
       → operator reconciles → executor pod → step Jobs → SurrealDB outcome

Requires a running k3d cluster with the rivers Helm chart deployed.
Run with:
    just k8s-up
    pytest python/tests/integration/test_k8s_integration.py -v

Skipped automatically when the test cluster cannot be reached.

The kubectl context is pinned to the k3d test cluster
(`RIVERS_K8S_TEST_CONTEXT`, default `k3d-rivers-test`) so the suite can
never accidentally hit whatever cluster the user's kubeconfig happens
to point at.
"""

import base64
import os
import time

import grpc
import httpx
import pytest
from kr8s import NotFoundError
from kr8s.objects import APIObject, Deployment, Pod, Secret, Service

from .conftest import KUBECTL_CONTEXT, kube_api

NAMESPACE = "rivers"
# Operator-managed CodeLocation CR applied by `dev/k3d/setup.sh`; the
# operator names the backing Service `<cr-name>-grpc` and the Deployment
# `<cr-name>` (see `rivers-operator/src/codelocation/resources.rs`).
CODE_LOCATION_NAME = os.environ.get(
    "RIVERS_K8S_TEST_CODE_LOCATION", "k8s-test-pipeline"
)
GRPC_SERVICE_NAME = f"{CODE_LOCATION_NAME}-grpc"
GRPC_PORT = 3001


class Run(APIObject):
    """rivers.io/v1alpha1 Run CRD."""

    version = "rivers.io/v1alpha1"
    endpoint = "runs"
    kind = "Run"
    plural = "runs"
    singular = "run"
    namespaced = True


def _cluster_reachable() -> bool:
    try:
        api = kube_api()
        # Trivial probe: list namespaces in the target context.
        list(api.get("namespaces", NAMESPACE))
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _cluster_reachable(),
    reason=f"k3d cluster '{KUBECTL_CONTEXT}' not reachable or namespace '{NAMESPACE}' missing",
)


class GrpcChannel:
    """Context manager: kr8s port-forward to the code-location service + gRPC stub."""

    def __init__(self, grpc_stubs):
        self._pb2, self._pb2_grpc = grpc_stubs
        svc = Service.get(GRPC_SERVICE_NAME, namespace=NAMESPACE, api=kube_api())
        self._pf_ctx = svc.portforward(remote_port=GRPC_PORT, local_port="auto")
        self._channel: grpc.Channel | None = None

    def __enter__(self) -> "GrpcChannel":
        local_port = self._pf_ctx.__enter__()
        self._channel = grpc.insecure_channel(f"127.0.0.1:{local_port}")
        grpc.channel_ready_future(self._channel).result(timeout=10)
        self.stub = self._pb2_grpc.CodeLocationServiceStub(self._channel)
        return self

    def __exit__(self, *exc):
        if self._channel:
            self._channel.close()
        self._pf_ctx.__exit__(*exc)

    @property
    def pb2(self):
        return self._pb2


def _list_runs() -> list[Run]:
    return list(Run.list(namespace=NAMESPACE, api=kube_api()))


def _get_run(name: str) -> Run | None:
    try:
        return Run.get(name, namespace=NAMESPACE, api=kube_api())
    except NotFoundError:
        return None


def _get_pod(name: str) -> Pod | None:
    try:
        return Pod.get(name, namespace=NAMESPACE, api=kube_api())
    except NotFoundError:
        return None


def _wait_for_run_cr(timeout: int = 30) -> str | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        runs = _list_runs()
        if runs:
            return runs[0].name
        time.sleep(2)
    return None


def _run_status(name: str) -> dict:
    run = _get_run(name)
    if run is None:
        return {}
    return run.raw.get("status", {}) or {}


def _wait_for_phase(name: str, terminal: set[str], timeout: int = 120) -> str:
    phase = "Unknown"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        phase = _run_status(name).get("phase") or "Pending"
        if phase in terminal:
            return phase
        time.sleep(3)
    return phase


def _delete_runs_and_workers() -> None:
    api = kube_api()
    for run in _list_runs():
        try:
            run.delete()
        except NotFoundError:
            pass
    for pod in Pod.list(
        namespace=NAMESPACE,
        label_selector="rivers.io/component=run-coordinator",
        api=api,
    ):
        try:
            pod.delete()
        except NotFoundError:
            pass
    from kr8s.objects import Job

    for job in Job.list(
        namespace=NAMESPACE,
        label_selector="rivers.io/component=step-worker",
        api=api,
    ):
        try:
            job.delete()
        except NotFoundError:
            pass


def _deployment_logs(name: str, tail: int = 40) -> str:
    try:
        dep = Deployment.get(name, namespace=NAMESPACE, api=kube_api())
        return "\n".join(dep.logs(tail_lines=tail))
    except Exception:
        return ""


def _pod_logs(pod_name: str, tail: int = 40) -> str:
    pod = _get_pod(pod_name)
    if pod is None:
        return ""
    try:
        return "\n".join(pod.logs(tail_lines=tail))
    except Exception:
        return ""


def _dump_debug_info(run_name: str) -> str:
    lines = []
    operator_logs = _deployment_logs("rivers-operator")
    if operator_logs:
        lines.append(f"--- Operator logs ---\n{operator_logs}")

    code_logs = _deployment_logs(CODE_LOCATION_NAME)
    if code_logs:
        lines.append(f"--- Code location logs ---\n{code_logs}")

    if run_name:
        exec_pod = _run_status(run_name).get("executorPod", "")
        if exec_pod:
            exec_logs = _pod_logs(exec_pod)
            if exec_logs:
                lines.append(f"--- Executor logs ---\n{exec_logs}")
    return "\n".join(lines)


def _query_run_events(
    run_id: str, fields: str = "event_type, asset_key"
) -> list[dict]:
    """Query SurrealDB via its HTTP /sql endpoint over a kr8s port-forward.

    kr8s exec doesn't support stdin on the v4 channel protocol that k3s
    speaks, and `surreal sql` only reads queries from stdin — so we go
    through the HTTP API instead.
    """
    pods = list(
        Pod.list(
            namespace=NAMESPACE,
            label_selector="app.kubernetes.io/name=surrealdb",
            api=kube_api(),
        )
    )
    if not pods:
        return []
    pod = pods[0]
    secret = Secret.get(
        name="rivers-surrealdb-auth", namespace=NAMESPACE, api=kube_api()
    )
    user = base64.b64decode(secret.raw["data"]["username"]).decode()
    pwd = base64.b64decode(secret.raw["data"]["password"]).decode()
    query = f"SELECT {fields} FROM events WHERE run_id = '{run_id}';"
    with pod.portforward(remote_port=8000, local_port="auto") as local_port:
        # SurrealDB v3 root-user basic auth doesn't apply to DB-scoped queries;
        # we have to sign in to (ns=rivers, db=main) first and use the JWT.
        si = httpx.post(
            f"http://127.0.0.1:{local_port}/signin",
            json={"ns": "rivers", "db": "main", "user": user, "pass": pwd},
            timeout=10,
        )
        if si.status_code != 200:
            return []
        token = si.json().get("token", "")
        r = httpx.post(
            f"http://127.0.0.1:{local_port}/sql",
            content=query,
            headers={
                "Surreal-NS": "rivers",
                "Surreal-DB": "main",
                "Accept": "application/json",
                "Authorization": f"Bearer {token}",
            },
            timeout=10,
        )
    if r.status_code != 200:
        return []
    try:
        data = r.json()
    except ValueError:
        return []
    if (
        isinstance(data, list)
        and data
        and isinstance(data[0], dict)
        and "result" in data[0]
    ):
        return data[0]["result"] or []
    return []


def _wait_for_executor_pod(run_name: str, timeout: int = 60) -> str:
    deadline = time.monotonic() + timeout
    exec_pod = ""
    while time.monotonic() < deadline:
        exec_pod = _run_status(run_name).get("executorPod", "")
        if exec_pod:
            return exec_pod
        time.sleep(1)
    return exec_pod


TERMINAL_PHASES = {"Succeeded", "Failed", "TimedOut", "Cancelled"}


class TestK8sGrpcFlow:
    """Test the full gRPC → daemon → K8sRunBackend → operator → executor flow."""

    def setup_method(self):
        _delete_runs_and_workers()

    def teardown_method(self):
        _delete_runs_and_workers()

    def test_ping(self, grpc_stubs):
        """Code location gRPC server responds to Ping."""
        with GrpcChannel(grpc_stubs) as ch:
            resp = ch.stub.Ping(ch.pb2.PingRequest())
            assert resp.status == "ok"

    def test_inprocess_job(self, grpc_stubs):
        """InProcess executor job: gRPC → daemon → Run CR → executor pod runs all steps in-process."""
        with GrpcChannel(grpc_stubs) as ch:
            resp = ch.stub.ExecuteJob(
                ch.pb2.ExecuteJobRequest(job_name="k8s_inprocess_job")
            )
            assert resp.run_id

            run_name = _wait_for_run_cr(timeout=30)
            assert run_name, "No Run CR appeared after ExecuteJob call"

            phase = _wait_for_phase(run_name, TERMINAL_PHASES)
            if phase != "Succeeded":
                pytest.fail(
                    f"Run '{run_name}' ended with phase '{phase}'\n{_dump_debug_info(run_name)}"
                )

            status = _run_status(run_name)
            assert status.get("completedSteps") == status.get("totalSteps")
            assert int(status.get("totalSteps", 0)) == 3

    def test_k8s_step_job(self, grpc_stubs):
        """K8s step executor job: each step runs as a separate K8s Job with S3 IO."""
        with GrpcChannel(grpc_stubs) as ch:
            resp = ch.stub.ExecuteJob(ch.pb2.ExecuteJobRequest(job_name="k8s_step_job"))
            assert resp.run_id

            run_name = _wait_for_run_cr(timeout=30)
            assert run_name, "No Run CR appeared after ExecuteJob call"

            phase = _wait_for_phase(run_name, TERMINAL_PHASES, timeout=180)
            if phase != "Succeeded":
                pytest.fail(
                    f"Run '{run_name}' ended with phase '{phase}'\n{_dump_debug_info(run_name)}"
                )

            status = _run_status(run_name)
            assert status.get("completedSteps") == status.get("totalSteps")
            assert int(status.get("totalSteps", 0)) == 3

            # Verify no duplicate events — step pods emit their own events,
            # the K8s executor coordinator must not re-emit them.
            events = _query_run_events(resp.run_id)
            for step in ["s3_source", "s3_transform", "s3_report"]:
                starts = [
                    e
                    for e in events
                    if e["asset_key"] == step and e["event_type"] == "StepStart"
                ]
                successes = [
                    e
                    for e in events
                    if e["asset_key"] == step and e["event_type"] == "StepSuccess"
                ]
                assert len(starts) == 1, (
                    f"Expected 1 StepStart for '{step}', got {len(starts)}"
                )
                assert len(successes) == 1, (
                    f"Expected 1 StepSuccess for '{step}', got {len(successes)}"
                )

    def test_graph_asset_inner_inprocess(self, grpc_stubs):
        """Graph asset with `rivers/node/executor=in_process` collapses its
        internal tasks into the orchestrator pod; they don't get scheduled
        as separate K8s Jobs.

        This is the "outer pod / inner in-process" pattern from
        `docs/guides/graph-assets.md`. The graph asset itself is composition-
        only (`executor/dispatch/classify.rs:93` short-circuits to event
        emission with no executor dispatch), so the only steps that *could*
        become K8s Jobs are the internal tasks. Assertions:

          (a) zero `created step Job` log lines from the executor —
              internal tasks ran in-process (otherwise we'd see 2, one per
              inner task dispatched to the kubernetes executor),
          (b) all three nodes (graph asset + both internal tasks) emit
              Materialization events — the inner tasks actually executed and
              persisted through S3, not silently skipped,
          (c) the run reaches Succeeded with `3/3 assets materialized`.

        Compare to `test_k8s_step_job` which exercises the same shape but
        without `rivers/node/executor` — that one creates a Job per asset.
        """
        with GrpcChannel(grpc_stubs) as ch:
            resp = ch.stub.ExecuteJob(
                ch.pb2.ExecuteJobRequest(job_name="k8s_graph_job")
            )
            assert resp.run_id
            run_id = resp.run_id

            run_name = _wait_for_run_cr(timeout=30)
            assert run_name, "No Run CR appeared after ExecuteJob call"

            exec_pod = _wait_for_executor_pod(run_name)
            assert exec_pod, "Executor pod never appeared"

            phase = _wait_for_phase(run_name, TERMINAL_PHASES, timeout=180)
            if phase != "Succeeded":
                pytest.fail(
                    f"Run '{run_name}' ended with phase '{phase}'\n{_dump_debug_info(run_name)}"
                )

            # (a) Executor logged zero `created step Job` lines — every
            # internal task ran in-process inside the orchestrator. If the
            # `rivers/node/executor=in_process` metadata weren't being
            # honored, we'd see 2 lines (one per inner task dispatched to
            # the kubernetes executor).
            exec_logs = _pod_logs(exec_pod, tail=400)
            create_lines = [
                line for line in exec_logs.splitlines() if "created step Job" in line
            ]
            assert len(create_lines) == 0, (
                f"Expected zero `created step Job` log lines (inner tasks "
                f"should run in-process), got {len(create_lines)}:\n"
                + "\n".join(create_lines)
                + f"\n{_dump_debug_info(run_name)}"
            )

            # (b) Materialization events fired for all three nodes — the graph
            # asset (via classify's composition-only short-circuit) and both
            # inner tasks (via in-process execution + S3 persistence). Inner
            # tasks carry namespaced names like `graph_pipeline/graph_inner_load`
            # since that's how the planner stamps composition-bound tasks.
            # The composition flow (`graph_inner_transform(graph_inner_load())`)
            # also forces a round-trip through the upstream's S3 object — if
            # persistence were broken the run would have failed earlier.
            events = _query_run_events(run_id)
            expected = [
                "graph_pipeline",
                "graph_pipeline/graph_inner_load",
                "graph_pipeline/graph_inner_transform",
            ]
            for asset in expected:
                mats = [
                    e
                    for e in events
                    if e["asset_key"] == asset and e["event_type"] == "Materialization"
                ]
                assert len(mats) == 1, (
                    f"Expected one Materialization event for '{asset}', got {len(mats)}"
                )

            # (c) The Run CR's status counters match.
            status = _run_status(run_name)
            assert status.get("completedSteps") == status.get("totalSteps")
            assert int(status.get("totalSteps", 0)) == 3

    def test_materialize_creates_run(self, grpc_stubs):
        """Materialize call goes through daemon queue and creates a Run CR."""
        with GrpcChannel(grpc_stubs) as ch:
            resp = ch.stub.Materialize(
                ch.pb2.MaterializeRequest(
                    selection=["source_data", "transform_data", "final_report"],
                )
            )
            assert resp.status == "queued", f"Expected queued, got: {resp.status}"
            assert resp.run_id

            run_name = _wait_for_run_cr(timeout=30)
            assert run_name, "No Run CR appeared after Materialize call"

            phase = _wait_for_phase(run_name, TERMINAL_PHASES)
            if phase != "Succeeded":
                pytest.fail(
                    f"Run '{run_name}' ended with phase '{phase}'\n{_dump_debug_info(run_name)}"
                )

    def test_operator_creates_executor_pod(self, grpc_stubs):
        """Operator creates an executor pod labelled with the run ID."""
        with GrpcChannel(grpc_stubs) as ch:
            resp = ch.stub.ExecuteJob(
                ch.pb2.ExecuteJobRequest(job_name="k8s_inprocess_job")
            )
            assert resp.run_id

            run_name = _wait_for_run_cr(timeout=30)
            assert run_name

            exec_pod_name = _wait_for_executor_pod(run_name)
            assert exec_pod_name, "Operator never created an executor pod"

            exec_pod = _get_pod(exec_pod_name)
            assert exec_pod is not None
            labels = exec_pod.raw["metadata"].get("labels", {})
            assert labels.get("rivers.io/run-id") == resp.run_id

            _wait_for_phase(run_name, TERMINAL_PHASES)

    def test_executor_resume_after_crash(self, grpc_stubs):
        """Operator restarts executor pod with --resume after crash, run still succeeds.

        Uses the in-process job because killing the executor kills in-flight
        steps (unlike step Jobs which are independent K8s resources).
        Kills the executor immediately after it appears to guarantee it hasn't
        finished yet.
        """
        with GrpcChannel(grpc_stubs) as ch:
            resp = ch.stub.ExecuteJob(
                ch.pb2.ExecuteJobRequest(job_name="k8s_inprocess_job")
            )
            assert resp.run_id

            run_name = _wait_for_run_cr(timeout=30)
            assert run_name, "No Run CR appeared after ExecuteJob call"

            exec_pod_name = _wait_for_executor_pod(run_name)
            assert exec_pod_name, "Executor pod never appeared"

            exec_pod = _get_pod(exec_pod_name)
            assert exec_pod is not None
            exec_pod.delete(grace_period=0, force=True)

            # Wait for a NEW executor pod (operator restart)
            deadline = time.monotonic() + 60
            new_exec_pod = ""
            while time.monotonic() < deadline:
                new_exec_pod = _run_status(run_name).get("executorPod", "")
                if new_exec_pod and new_exec_pod != exec_pod_name:
                    break
                time.sleep(2)

            assert new_exec_pod and new_exec_pod != exec_pod_name, (
                f"Operator did not create a new executor pod after crash "
                f"(still: {exec_pod_name})"
            )

            new_pod = _get_pod(new_exec_pod)
            assert new_pod is not None
            args = new_pod.raw["spec"]["containers"][0].get("args", [])
            assert "--resume" in args, (
                f"Restarted executor pod missing --resume flag: {args}"
            )

            phase = _wait_for_phase(run_name, TERMINAL_PHASES, timeout=180)
            if phase != "Succeeded":
                pytest.fail(
                    f"Run '{run_name}' ended with phase '{phase}' after resume\n"
                    f"{_dump_debug_info(run_name)}"
                )

    def test_cancel_run_via_grpc(self, grpc_stubs):
        """CancelRun gRPC call terminates a K8s step job run.

        Full flow: ExecuteJob → wait for Running → CancelRun gRPC →
        operator kills step Jobs → executor detects cancellation → Cancelled.
        """
        with GrpcChannel(grpc_stubs) as ch:
            resp = ch.stub.ExecuteJob(ch.pb2.ExecuteJobRequest(job_name="k8s_step_job"))
            assert resp.run_id
            run_id = resp.run_id

            run_name = _wait_for_run_cr(timeout=30)
            assert run_name, "No Run CR appeared after ExecuteJob call"

            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                status = _run_status(run_name)
                if status.get("executorPod") and status.get("phase") == "Running":
                    break
                time.sleep(1)

            cancel_resp = ch.stub.CancelRun(ch.pb2.CancelRunRequest(run_id=run_id))
            assert cancel_resp.success

            phase = _wait_for_phase(run_name, TERMINAL_PHASES, timeout=60)
            assert phase == "Cancelled", (
                f"Expected Cancelled but got '{phase}'\n{_dump_debug_info(run_name)}"
            )

    def test_failing_asset_marks_run_failed(self, grpc_stubs):
        """Asset that raises an exception drives the Run to Failed (executor outcome=Failure)."""
        with GrpcChannel(grpc_stubs) as ch:
            resp = ch.stub.ExecuteJob(
                ch.pb2.ExecuteJobRequest(job_name="k8s_failing_job")
            )
            assert resp.run_id

            run_name = _wait_for_run_cr(timeout=30)
            assert run_name, "No Run CR appeared after ExecuteJob call"

            phase = _wait_for_phase(run_name, TERMINAL_PHASES, timeout=120)
            if phase != "Failed":
                pytest.fail(
                    f"Expected Failed but got '{phase}'\n{_dump_debug_info(run_name)}"
                )

    def test_run_deletion_cleans_up_pods(self, grpc_stubs):
        """Deleting a Run CR while running triggers finalizer to clean up the executor pod."""
        with GrpcChannel(grpc_stubs) as ch:
            resp = ch.stub.ExecuteJob(ch.pb2.ExecuteJobRequest(job_name="k8s_slow_job"))
            assert resp.run_id

            run_name = _wait_for_run_cr(timeout=30)
            assert run_name, "No Run CR appeared after ExecuteJob call"

            exec_pod_name = _wait_for_executor_pod(run_name)
            assert exec_pod_name, "Executor pod never appeared"

            run = _get_run(run_name)
            assert run is not None
            run.delete()

            # Run CR should disappear once finalizer runs
            deadline = time.monotonic() + 60
            cr_gone = False
            while time.monotonic() < deadline:
                if _get_run(run_name) is None:
                    cr_gone = True
                    break
                time.sleep(2)
            assert cr_gone, (
                f"Run CR '{run_name}' still present after delete (finalizer stuck?)"
            )

            # Executor pod should be gone too (may briefly be in Terminating state)
            deadline = time.monotonic() + 60
            pod_gone = False
            while time.monotonic() < deadline:
                if _get_pod(exec_pod_name) is None:
                    pod_gone = True
                    break
                time.sleep(2)
            assert pod_gone, (
                f"Executor pod '{exec_pod_name}' still present after Run CR deletion"
            )

    def test_timeout_sets_timedout_phase(self, grpc_stubs):
        """Patching a Run CR with timeoutSeconds drives it to TimedOut once elapsed."""
        with GrpcChannel(grpc_stubs) as ch:
            resp = ch.stub.ExecuteJob(ch.pb2.ExecuteJobRequest(job_name="k8s_slow_job"))
            assert resp.run_id

            run_name = _wait_for_run_cr(timeout=30)
            assert run_name, "No Run CR appeared after ExecuteJob call"

            run = _get_run(run_name)
            assert run is not None
            run.patch({"spec": {"timeoutSeconds": 10}}, type="merge")

            phase = _wait_for_phase(run_name, TERMINAL_PHASES, timeout=120)
            if phase != "TimedOut":
                pytest.fail(
                    f"Expected TimedOut but got '{phase}'\n{_dump_debug_info(run_name)}"
                )

    def test_cancel_force_kills_after_grace_period(self, grpc_stubs):
        """Cancelling → Cancelled via grace-period expiry: pod is force-killed.

        Uses the in-process slow job whose single asset sleeps 120s — the
        executor only checks the cancel signal between assets, so it won't
        exit voluntarily during the grace period and the operator hits the
        force-kill branch in cancel.rs.
        """
        with GrpcChannel(grpc_stubs) as ch:
            resp = ch.stub.ExecuteJob(ch.pb2.ExecuteJobRequest(job_name="k8s_slow_job"))
            assert resp.run_id
            run_id = resp.run_id

            run_name = _wait_for_run_cr(timeout=30)
            assert run_name, "No Run CR appeared after ExecuteJob call"

            run = _get_run(run_name)
            assert run is not None
            run.patch({"spec": {"cancelGracePeriodSeconds": 5}}, type="merge")

            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                status = _run_status(run_name)
                if status.get("executorPod") and status.get("phase") == "Running":
                    break
                time.sleep(1)

            cancel_resp = ch.stub.CancelRun(ch.pb2.CancelRunRequest(run_id=run_id))
            assert cancel_resp.success

            phase = _wait_for_phase(run_name, TERMINAL_PHASES, timeout=60)
            assert phase == "Cancelled", (
                f"Expected Cancelled but got '{phase}'\n{_dump_debug_info(run_name)}"
            )

            message = _run_status(run_name).get("message", "")
            assert "force-killed" in message, (
                f"Expected force-kill message, got: {message!r}"
            )
