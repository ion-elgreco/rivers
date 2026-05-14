"""K8s-specific fixtures for integration tests.

The session-scoped `grpc_stubs` fixture (compiled `rivers.proto` stubs) is
defined in the root `python/tests/conftest.py` and inherited here. This
file only adds k8s-context-specific helpers that the wider suite doesn't
need.
"""

import os

import kr8s

KUBECTL_CONTEXT = os.environ.get("RIVERS_K8S_TEST_CONTEXT", "k3d-rivers-test")


def kube_api() -> kr8s.Api:
    """kr8s client pinned to the test k3d context — never the user's
    accidentally-set kubeconfig default."""
    return kr8s.api(context=KUBECTL_CONTEXT)
