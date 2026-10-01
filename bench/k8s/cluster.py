"""The k3d cluster the Kubernetes benchmark runs on, and how images reach it."""

from __future__ import annotations

import subprocess
from collections.abc import Callable

CLUSTER = "orch-bench"
CONTEXT = f"k3d-{CLUSTER}"

# The cluster brings up its own registry, because Docker Desktop's containerd
# image store produces manifests `k3d image import` rejects. Every other name
# below is derived from these two so they cannot drift apart.
REGISTRY_NAME = f"{CLUSTER}-registry"
REGISTRY_PORT = 5222
REGISTRY_CREATE = f"{REGISTRY_NAME}:0.0.0.0:{REGISTRY_PORT}"
REGISTRY_HOST = f"localhost:{REGISTRY_PORT}"
REGISTRY_IN_CLUSTER = f"{REGISTRY_NAME}:5000"

Runner = Callable[..., subprocess.CompletedProcess]


def _tool(binary: str, context_flag: str, default_timeout: int) -> Runner:
    """Build a runner that points one CLI at the benchmark cluster."""

    def run(
        *args: str,
        check: bool = True,
        timeout: int = default_timeout,
        input: str | None = None,
    ) -> subprocess.CompletedProcess:
        return subprocess.run(
            [binary, context_flag, CONTEXT, *args],
            input=input,
            capture_output=True,
            text=True,
            check=check,
            timeout=timeout,
        )

    return run


kubectl = _tool("kubectl", "--context", 120)
helm = _tool("helm", "--kube-context", 900)


def cluster_exists() -> bool:
    proc = subprocess.run(
        ["k3d", "cluster", "get", CLUSTER], capture_output=True, text=True
    )
    return proc.returncode == 0


def create_cluster() -> None:
    """Create the benchmark cluster with its own registry."""
    if cluster_exists():
        print("cluster already exists", flush=True)
        return
    print(f"creating cluster {CLUSTER}", flush=True)
    subprocess.run(
        [
            "k3d",
            "cluster",
            "create",
            CLUSTER,
            "--agents",
            "1",
            "--registry-create",
            REGISTRY_CREATE,
            "--wait",
        ],
        check=True,
        timeout=900,
    )


def push_images(images: list[str]) -> None:
    """Push locally-built images into the cluster registry."""
    for image in images:
        have = subprocess.run(
            ["docker", "image", "inspect", image], capture_output=True
        )
        if have.returncode != 0:
            raise SystemExit(f"missing local image {image} — build it first")
        ref = f"{REGISTRY_HOST}/{image}"
        print(f"  pushing {image}", flush=True)
        subprocess.run(["docker", "tag", image, ref], check=True)
        subprocess.run(["docker", "push", ref], check=True, timeout=1800)


def reset_namespace(namespace: str) -> None:
    """Delete the namespace and wait for it to go, so each run starts clean."""
    kubectl(
        "delete",
        "namespace",
        namespace,
        "--ignore-not-found",
        "--wait=true",
        check=False,
        timeout=600,
    )
