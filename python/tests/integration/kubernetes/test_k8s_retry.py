"""Integration tests for declarative retries on the Kubernetes executor.

Exercises per-attempt step Jobs, OOM classification from pod status, compute
escalation, and the Job-status poll fallback (an OOM-killed pod writes no
event; before the fallback the step poll hung until run timeout).

Requires the k3d test cluster with the retry jobs from
`dev/k3d/k8s_test_pipeline` deployed:
    just k8s-up
    pytest python/tests/integration/kubernetes/test_k8s_retry.py -v

Skipped automatically when the test cluster cannot be reached.
"""

import time

import pytest
from kr8s.objects import Job as K8sJob

from .conftest import KUBECTL_CONTEXT, kube_api
from .test_k8s_integration import (
    NAMESPACE,
    TERMINAL_PHASES,
    GrpcChannel,
    _cluster_reachable,
    _delete_runs_and_workers,
    _dump_debug_info,
    _query_run_events,
    _wait_for_phase,
    _wait_for_run_cr,
)

pytestmark = [
    pytest.mark.skipif(
        not _cluster_reachable(),
        reason=f"k3d cluster '{KUBECTL_CONTEXT}' not reachable or namespace '{NAMESPACE}' missing",
    ),
    # Retry ladders run several pods to completion; the repo-wide 60s
    # pytest-timeout (thread method: kills the whole session) is far too tight.
    pytest.mark.timeout(600),
]


def _events_with_metadata(run_id: str) -> list[dict]:
    return _query_run_events(run_id, fields="event_type, asset_key, metadata")


def _metadata_dict(event: dict) -> dict:
    return {k: v for k, v in (event.get("metadata") or [])}


def _wait_for_events(run_id: str, predicate, timeout: int = 60) -> list[dict]:
    """Storage writes trail the Run CR phase; poll until `predicate` holds."""
    deadline = time.monotonic() + timeout
    events: list[dict] = []
    while time.monotonic() < deadline:
        events = _events_with_metadata(run_id)
        if predicate(events):
            return events
        time.sleep(3)
    return events


def _step_worker_job_names(step_label: str) -> list[str]:
    jobs = K8sJob.list(
        namespace=NAMESPACE,
        label_selector=f"rivers.io/component=step-worker,rivers.io/step={step_label}",
        api=kube_api(),
    )
    return [j.name for j in jobs]


class TestK8sRetry:
    def setup_method(self):
        _delete_runs_and_workers()

    def teardown_method(self):
        _delete_runs_and_workers()

    def test_retry_budget_exhausted_emits_attempt_ladder(self, grpc_stubs):
        """An always-failing step is attempted 1 + max_retries times, each
        attempt as its own step Job, before the run fails."""
        with GrpcChannel(grpc_stubs) as ch:
            resp = ch.stub.ExecuteJob(
                ch.pb2.ExecuteJobRequest(job_name="k8s_retry_exhausted_job")
            )
            assert resp.run_id

            run_name = _wait_for_run_cr(timeout=30)
            assert run_name, "No Run CR appeared after ExecuteJob call"

            phase = _wait_for_phase(run_name, TERMINAL_PHASES, timeout=300)
            if phase != "Failed":
                pytest.fail(
                    f"Run '{run_name}' ended with phase '{phase}' (expected Failed)\n"
                    f"{_dump_debug_info(run_name)}"
                )

            events = _wait_for_events(
                resp.run_id,
                lambda evs: sum(e["event_type"] == "StepFailure" for e in evs) >= 3,
            )
            failures = [e for e in events if e["event_type"] == "StepFailure"]
            retries = [e for e in events if e["event_type"] == "StepRetry"]
            assert len(failures) == 3, f"expected 3 attempts, events: {events}"
            assert len(retries) == 2
            assert sorted(_metadata_dict(r)["rivers/attempt"] for r in retries) == ["1", "2"]
            assert not any(e["event_type"] == "StepSuccess" for e in events)

            # Attempts 2 and 3 ran as distinct -rN step Jobs.
            names = _step_worker_job_names("retry_always_fails")
            assert any(n.endswith("-r2") for n in names), names
            assert any(n.endswith("-r3") for n in names), names

    def test_oom_escalation_retries_and_succeeds(self, grpc_stubs):
        """An OOM-killed step is classified from pod status (it never wrote an
        event), retried with doubled memory, and succeeds."""
        with GrpcChannel(grpc_stubs) as ch:
            resp = ch.stub.ExecuteJob(
                ch.pb2.ExecuteJobRequest(job_name="k8s_oom_escalation_job")
            )
            assert resp.run_id

            run_name = _wait_for_run_cr(timeout=30)
            assert run_name, "No Run CR appeared after ExecuteJob call"

            phase = _wait_for_phase(run_name, TERMINAL_PHASES, timeout=300)
            if phase != "Succeeded":
                pytest.fail(
                    f"Run '{run_name}' ended with phase '{phase}' (expected Succeeded)\n"
                    f"{_dump_debug_info(run_name)}"
                )

            events = _wait_for_events(
                resp.run_id,
                lambda evs: any(e["event_type"] == "StepSuccess" for e in evs),
            )
            retries = [e for e in events if e["event_type"] == "StepRetry"]
            assert retries, f"expected a StepRetry, events: {events}"
            meta = _metadata_dict(retries[0])
            assert meta["rivers/failure_reason"] == "out_of_memory"
            assert '"memory":"512Mi"' in meta["rivers/next_compute"]
            assert any(e["event_type"] == "StepSuccess" for e in events)

            names = _step_worker_job_names("oom_hungry")
            assert any(n.endswith("-r2") for n in names), names

    def test_exception_allowlist_listed_type_retries(self, grpc_stubs):
        """The step pod stamps the exception MRO on its StepFailure event and
        the orchestrator matches exception-type allow-lists against it."""
        with GrpcChannel(grpc_stubs) as ch:
            resp = ch.stub.ExecuteJob(
                ch.pb2.ExecuteJobRequest(job_name="k8s_exc_listed_job")
            )
            assert resp.run_id

            run_name = _wait_for_run_cr(timeout=30)
            assert run_name, "No Run CR appeared after ExecuteJob call"

            phase = _wait_for_phase(run_name, TERMINAL_PHASES, timeout=300)
            if phase != "Failed":
                pytest.fail(
                    f"Run '{run_name}' ended with phase '{phase}' (expected Failed)\n"
                    f"{_dump_debug_info(run_name)}"
                )

            events = _wait_for_events(
                resp.run_id,
                lambda evs: sum(e["event_type"] == "StepFailure" for e in evs) >= 2,
            )
            failures = [e for e in events if e["event_type"] == "StepFailure"]
            retries = [e for e in events if e["event_type"] == "StepRetry"]
            assert len(failures) == 2, f"expected 2 attempts, events: {events}"
            assert len(retries) == 1
            meta = _metadata_dict(failures[0])
            assert meta["rivers/failure_reason"] == "error"
            assert "builtins.ValueError" in meta["rivers/exc_type"]

    def test_exception_allowlist_unlisted_type_fails_fast(self, grpc_stubs):
        """An exception type absent from the allow-list is not retried, even
        though the failure crossed the pod boundary as an event."""
        with GrpcChannel(grpc_stubs) as ch:
            resp = ch.stub.ExecuteJob(
                ch.pb2.ExecuteJobRequest(job_name="k8s_exc_unlisted_job")
            )
            assert resp.run_id

            run_name = _wait_for_run_cr(timeout=30)
            assert run_name, "No Run CR appeared after ExecuteJob call"

            phase = _wait_for_phase(run_name, TERMINAL_PHASES, timeout=300)
            if phase != "Failed":
                pytest.fail(
                    f"Run '{run_name}' ended with phase '{phase}' (expected Failed)\n"
                    f"{_dump_debug_info(run_name)}"
                )

            events = _wait_for_events(
                resp.run_id,
                lambda evs: any(e["event_type"] == "StepFailure" for e in evs),
            )
            failures = [e for e in events if e["event_type"] == "StepFailure"]
            assert len(failures) == 1, f"expected a single attempt, events: {events}"
            assert not any(e["event_type"] == "StepRetry" for e in events)

    def test_oom_without_policy_fails_fast(self, grpc_stubs):
        """Poll-hang regression: with no retry policy an OOM-killed pod leaves
        no event, and the run must still fail promptly via the Job-status
        fallback instead of hanging until the run timeout."""
        with GrpcChannel(grpc_stubs) as ch:
            resp = ch.stub.ExecuteJob(
                ch.pb2.ExecuteJobRequest(job_name="k8s_oom_no_retry_job")
            )
            assert resp.run_id

            run_name = _wait_for_run_cr(timeout=30)
            assert run_name, "No Run CR appeared after ExecuteJob call"

            phase = _wait_for_phase(run_name, TERMINAL_PHASES, timeout=180)
            if phase != "Failed":
                pytest.fail(
                    f"Run '{run_name}' ended with phase '{phase}' (expected Failed "
                    f"within the window — the pre-fallback behavior was a hang)\n"
                    f"{_dump_debug_info(run_name)}"
                )

            events = _events_with_metadata(resp.run_id)
            assert not any(e["event_type"] == "StepRetry" for e in events)
            assert not any(e["event_type"] == "StepSuccess" for e in events)
