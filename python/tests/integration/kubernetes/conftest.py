"""K8s-specific fixtures for integration tests.

The session-scoped `grpc_stubs` fixture (compiled `rivers.proto` stubs) is
defined in the root `python/tests/conftest.py` and inherited here. This
file only adds k8s-context-specific helpers that the wider suite doesn't
need.
"""

import os

import kr8s
import pytest

KUBECTL_CONTEXT = os.environ.get("RIVERS_K8S_TEST_CONTEXT", "k3d-rivers-test")

# In CI the cluster is a hard prerequisite: a missing or broken deploy must
# fail the job loudly, never skip to green. Locally (unset) the suite still
# skips when no cluster is up, so plain `pytest` stays runnable without k3d.
CLUSTER_REQUIRED = os.environ.get("RIVERS_K8S_REQUIRE_CLUSTER") == "1"


def kube_api() -> kr8s.Api:
    """kr8s client pinned to the test k3d context — never the user's
    accidentally-set kubeconfig default."""
    return kr8s.api(context=KUBECTL_CONTEXT)


def cluster_gate(ready: bool, reason: str) -> pytest.MarkDecorator:
    """Module ``pytestmark`` factory for the k8s integration suites.

    When the cluster is reachable this is a no-op mark. When it isn't:

      * locally — skip the module, so ``pytest`` works without a k3d cluster;
      * under CI (``RIVERS_K8S_REQUIRE_CLUSTER=1``) — hard-fail collection, so a
        broken deploy can never slip through as a silent green skip.
    """
    if not ready and CLUSTER_REQUIRED:
        pytest.fail(
            f"RIVERS_K8S_REQUIRE_CLUSTER=1 but cluster is not ready: {reason}",
            pytrace=False,
        )
    return pytest.mark.skipif(not ready, reason=reason)
