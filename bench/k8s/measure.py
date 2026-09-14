"""What the Kubernetes benchmark records: readiness, pods, images and memory."""

from __future__ import annotations

import json
import subprocess
import time
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path

from bench.k8s.cluster import REGISTRY_HOST, REGISTRY_IN_CLUSTER, kubectl
from bench.paths import write_json


@dataclass
class Phase:
    """One timed phase of a deployment."""

    name: str
    seconds: float
    status: str = "ok"
    detail: str = ""


@dataclass
class Measurement:
    """Everything recorded for one orchestrator."""

    orchestrator: str
    namespace: str
    phases: list[Phase] = field(default_factory=list)
    pods: list[dict] = field(default_factory=list)
    images: list[str] = field(default_factory=list)
    image_bytes: int = 0
    memory: dict = field(default_factory=dict)
    attempt: int = 1
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "orchestrator": self.orchestrator,
            "namespace": self.namespace,
            "attempt": self.attempt,
            "phases": [vars(p) for p in self.phases],
            "pod_count": len(self.pods),
            "container_count": sum(p["containers"] for p in self.pods),
            "pods": self.pods,
            "images": self.images,
            "image_mb": self.image_bytes / 1024**2,
            "memory": self.memory,
            "notes": self.notes,
        }


# --- readiness ---------------------------------------------------------------


def get_items(namespace: str, kind: str) -> list[dict] | None:
    """Fetch one kind of object as JSON, or None when the query failed."""
    proc = kubectl("-n", namespace, "get", kind, "-o", "json", check=False)
    if proc.returncode != 0:
        return None
    return json.loads(proc.stdout or '{"items": []}').get("items", [])


def workloads_ready(namespace: str) -> tuple[bool, str]:
    """True when every Deployment and StatefulSet in ``namespace`` is fully ready."""
    pending = []
    seen = 0
    for kind in ("deployments", "statefulsets"):
        items = get_items(namespace, kind)
        if items is None:
            return False, f"{kind}: query failed"
        seen += len(items)
        for item in items:
            name = item["metadata"]["name"]
            want = item["spec"].get("replicas", 1)
            have = item.get("status", {}).get("readyReplicas", 0)
            if have < want:
                pending.append(f"{name} {have}/{want}")
    if seen == 0:
        # Nothing scheduled yet — the chart's objects have not landed. Counting
        # across both kinds matters: a namespace can have Deployments and no
        # StatefulSets, or the reverse.
        return False, "no workloads yet"
    return (not pending), ", ".join(pending)


def workload_exists(namespace: str, name: str) -> bool:
    """True once a Deployment with this name has been created."""
    proc = kubectl("-n", namespace, "get", "deployment", name, check=False, timeout=60)
    return proc.returncode == 0


def wait_ready(
    namespace: str, timeout: float, poll: float = 1.0, require: str | None = None
) -> Phase:
    """Block until every workload in ``namespace`` is ready, or time out.

    ``require`` names a Deployment that must exist before the check counts.
    Without it, a check run straight after `helm install` can pass against the
    workloads that were already there, before the new one is even created.
    """
    start = time.monotonic()
    last = ""
    while time.monotonic() - start < timeout:
        if require is not None and not workload_exists(namespace, require):
            last = f"{require} not created yet"
            time.sleep(poll)
            continue
        ready, last = workloads_ready(namespace)
        if ready:
            return Phase("ready", time.monotonic() - start)
        time.sleep(poll)
    return Phase("ready", time.monotonic() - start, "timeout", last)


# --- footprint ---------------------------------------------------------------


def collect_pods(namespace: str) -> list[dict]:
    """Snapshot the pods running in ``namespace``."""
    out = []
    for item in get_items(namespace, "pods") or []:
        spec = item["spec"]
        containers = spec.get("containers", [])
        out.append(
            {
                "name": item["metadata"]["name"],
                "phase": item.get("status", {}).get("phase"),
                "containers": len(containers),
                "images": [c["image"] for c in containers],
                "init_images": [c["image"] for c in spec.get("initContainers", [])],
            }
        )
    return out


def _image_candidates(image: str) -> list[str]:
    """Names the same image may go by locally.

    Pods reference images through the cluster registry or a fully qualified
    docker.io path, while the local daemon knows them under the name they were
    tagged or pulled with.
    """
    names = [image]
    if image.startswith(f"{REGISTRY_IN_CLUSTER}/"):
        bare = image[len(REGISTRY_IN_CLUSTER) + 1 :]
        names += [f"{REGISTRY_HOST}/{bare}", bare]
    if image.startswith("docker.io/"):
        bare = image[len("docker.io/") :]
        names.append(bare)
        if bare.startswith("library/"):
            names.append(bare[len("library/") :])
    return names


@cache
def image_size_bytes(image: str) -> int:
    """Local size of an image, or 0 when no local copy can be found.

    Cached: the same images are inventoried after every install of the same
    orchestrator, and an image's size does not change during a run.
    """
    for name in _image_candidates(image):
        proc = subprocess.run(
            ["docker", "image", "inspect", name, "--format", "{{.Size}}"],
            capture_output=True,
            text=True,
        )
        if proc.returncode == 0:
            try:
                return int(proc.stdout.strip())
            except ValueError:
                continue
    return 0


def summarize_images(pods: list[dict]) -> tuple[list[str], int]:
    """Distinct images across the pods, and their combined local size."""
    images = sorted({img for pod in pods for img in pod["images"] + pod["init_images"]})
    total = sum(image_size_bytes(img) for img in images)
    return images, total


# What `kubectl top` reports memory in, as a multiple of one megabyte.
MB_PER_UNIT = {"Ki": 1 / 1024, "Mi": 1.0, "Gi": 1024.0}


def collect_memory(namespace: str, settle: float = 60.0) -> dict:
    """Resident memory of the namespace once it has settled.

    metrics-server needs a scrape cycle or two before it reports, so this waits
    before asking. The figure is what the control plane costs at idle.
    """
    time.sleep(settle)
    proc = kubectl(
        "top", "pods", "-n", namespace, "--no-headers", check=False, timeout=120
    )
    if proc.returncode != 0:
        return {"status": "unavailable", "detail": (proc.stderr or "")[-200:]}
    per_pod = {}
    for line in proc.stdout.splitlines():
        fields = line.split()
        if len(fields) < 3:
            continue
        name, memory = fields[0], fields[2]
        scale = MB_PER_UNIT.get(memory[-2:])
        if scale is not None:
            per_pod[name] = float(memory[:-2]) * scale
    return {"status": "ok", "total_mb": sum(per_pod.values()), "per_pod_mb": per_pod}


def save(measurements: list[Measurement], path: Path) -> None:
    """Write every measurement recorded so far."""
    write_json([m.to_dict() for m in measurements], path)
