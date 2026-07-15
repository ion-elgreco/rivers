"""Integration test for per-asset compute on the Kubernetes executor.

Requires the k3d test cluster with `dev/k3d/k8s_test_pipeline` deployed
(`just k8s-up`). Skipped when the cluster is unreachable.
"""

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
    _wait_for_phase,
    _wait_for_run_cr,
)

pytestmark = [
    pytest.mark.skipif(
        not _cluster_reachable(),
        reason=f"k3d cluster '{KUBECTL_CONTEXT}' not reachable or namespace '{NAMESPACE}' missing",
    ),
    # Runs real pods to completion; exempt from the repo-wide 60s timeout.
    pytest.mark.timeout(600),
]


class TestK8sCompute:
    def setup_method(self):
        _delete_runs_and_workers()

    def teardown_method(self):
        _delete_runs_and_workers()

    def test_per_asset_compute_sets_pod_resources(self, grpc_stubs):
        """The step Job's container carries the asset's Compute, with unset
        axes untouched by the run-wide worker defaults."""
        with GrpcChannel(grpc_stubs) as ch:
            resp = ch.stub.ExecuteJob(ch.pb2.ExecuteJobRequest(job_name="k8s_compute_job"))
            assert resp.run_id

            run_name = _wait_for_run_cr(timeout=30)
            assert run_name, "No Run CR appeared after ExecuteJob call"

            phase = _wait_for_phase(run_name, TERMINAL_PHASES, timeout=180)
            if phase != "Succeeded":
                pytest.fail(
                    f"Run '{run_name}' ended with phase '{phase}'\n{_dump_debug_info(run_name)}"
                )

            jobs = list(
                K8sJob.list(
                    namespace=NAMESPACE,
                    label_selector="rivers.io/component=step-worker,rivers.io/step=sized_step",
                    api=kube_api(),
                )
            )
            assert jobs, "step Job for 'sized_step' not found"
            container = jobs[0].raw["spec"]["template"]["spec"]["containers"][0]
            requests = container["resources"]["requests"]
            limits = container["resources"]["limits"]
            assert requests["memory"] == "384Mi"
            assert requests["cpu"] == "300m"
            assert limits["memory"] == "384Mi"
            assert limits["cpu"] == "300m"
