"""Install each orchestrator's control plane and time it.

One deploy function per orchestrator, because the three stacks are not the same
shape. rivers reconciles a CodeLocation resource, Dagster ships its code
location with the chart, and Prefect needs a separate worker release before it
can run anything. Everything around those functions — namespace, releases,
images — is the same kind of fact for all three, so it lives in one table.

Each deployer records what it deployed in `notes`, so the published table can
say so.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from collections.abc import Callable

from bench.k8s.cluster import REGISTRY_IN_CLUSTER, helm, kubectl, reset_namespace
from bench.k8s.measure import (
    Measurement,
    Phase,
    collect_memory,
    collect_pods,
    summarize_images,
    wait_ready,
)
from bench.paths import K8S, ROOT

CHART_TIMEOUT = 600
READY_TIMEOUT = 600

# The versions the in-cluster measurement runs. They must match
# `bench/envs/pyproject.toml`, or the local and Kubernetes numbers come from
# two different frameworks. `images/` builds these tags.
DAGSTER_VERSION = "1.13.22"
PREFECT_VERSION = "3.8.5"

# Every code location holds this many no-op assets, whichever orchestrator is
# serving it, so "code location ready" measures the same job three times.
WORK_POOL = "bench"


def _timed_install(label: str, args: list[str]) -> Phase:
    """Run one `helm install` and time it. Does not wait for pods."""
    start = time.monotonic()
    proc = helm(*args, check=False, timeout=CHART_TIMEOUT)
    seconds = time.monotonic() - start
    if proc.returncode != 0:
        return Phase(label, seconds, "error", (proc.stderr or proc.stdout)[-600:])
    return Phase(label, seconds)


def _ready_phase(
    m: Measurement,
    namespace: str,
    start: float,
    label: str = "control_plane_ready",
    require: str | None = None,
) -> Phase:
    """Wait for the workloads to come up, unless an install already failed."""
    failed = next((p for p in m.phases if p.status != "ok"), None)
    if failed is not None:
        return Phase(label, 0.0, "error", f"{failed.name}: {failed.detail[:200]}")
    ready = wait_ready(namespace, READY_TIMEOUT, require=require)
    return Phase(label, time.monotonic() - start, ready.status, ready.detail)


def _finish(m: Measurement, namespace: str, footprint: bool) -> None:
    """Record the footprint once the deployment settled.

    Warm-up installs exist only to cache images and their measurement is
    discarded, so they skip the inventory and the minute-long memory settle.
    """
    if not footprint:
        m.memory = {"status": "skipped"}
        return
    m.pods = collect_pods(namespace)
    m.images, m.image_bytes = summarize_images(m.pods)
    m.memory = collect_memory(namespace)


# --- rivers ------------------------------------------------------------------

CODE_LOCATION_CR = """apiVersion: rivers.io/v1alpha1
kind: CodeLocation
metadata:
  name: bench
  namespace: {namespace}
spec:
  image: {registry}/rivers-bench-code-location
  tag: release
  module: bench_pipeline
  replicas: 1
  env:
    - name: BENCH_N_ASSETS
      value: "{assets}"
"""


def wait_code_location(namespace: str, name: str, timeout: float) -> Phase:
    """Wait for a rivers CodeLocation to report Ready."""
    start = time.monotonic()
    phase = ""
    while time.monotonic() - start < timeout:
        proc = kubectl(
            "-n",
            namespace,
            "get",
            "codelocation",
            name,
            "-o",
            "jsonpath={.status.phase}",
            check=False,
        )
        phase = (proc.stdout or "").strip()
        if phase == "Ready":
            return Phase("code_location_ready", time.monotonic() - start)
        time.sleep(1.0)
    return Phase(
        "code_location_ready",
        time.monotonic() - start,
        "timeout",
        f"phase={phase or 'none'}",
    )


def deploy_rivers(namespace: str, assets: int, footprint: bool = True) -> Measurement:
    """Install the rivers control plane, then a code location, timing both."""
    m = Measurement("rivers", namespace)
    # The chart depends on the upstream SurrealDB subchart; fetch it before the
    # clock starts so the measurement is install time, not download time.
    helm(
        "dependency",
        "build",
        str(ROOT / "deploy/helm/rivers"),
        check=False,
        timeout=300,
    )
    reset_namespace(namespace)

    start = time.monotonic()
    m.phases.append(
        _timed_install(
            "install",
            [
                "install",
                "rivers-crds",
                str(ROOT / "deploy/helm/rivers-crds"),
                "-n",
                namespace,
                "--create-namespace",
                "--set",
                f"global.namespace={namespace}",
            ],
        )
    )
    m.phases.append(
        _timed_install(
            "install-main",
            [
                "install",
                "rivers",
                str(ROOT / "deploy/helm/rivers"),
                "-n",
                namespace,
                # The chart places its objects in `global.namespace`, not the
                # release namespace, so both must be set.
                "--set",
                f"global.namespace={namespace}",
                "--set",
                f"operator.image={REGISTRY_IN_CLUSTER}/rivers-operator:release",
                "--set",
                "operator.allowInsecureRegistry=true",
                "--set",
                f"ui.image={REGISTRY_IN_CLUSTER}/rivers-ui:release",
            ],
        )
    )
    m.phases.append(_ready_phase(m, namespace, start))

    if m.phases[-1].status == "ok":
        manifest = CODE_LOCATION_CR.format(
            namespace=namespace, registry=REGISTRY_IN_CLUSTER, assets=assets
        )
        applied = kubectl("apply", "-f", "-", input=manifest, check=False)
        if applied.returncode != 0:
            m.phases.append(
                Phase("code_location_ready", 0.0, "error", applied.stderr[-300:])
            )
        else:
            m.phases.append(wait_code_location(namespace, "bench", READY_TIMEOUT))

    _finish(m, namespace, footprint)
    m.notes.append(
        "Control plane: operator, UI, SurrealDB, plus a SurrealDB user bootstrap job."
    )
    m.notes.append("Code location is a CodeLocation resource the operator reconciles.")
    m.notes.append(f"Code location holds {assets} assets.")
    return m


# --- Dagster -----------------------------------------------------------------


def deploy_dagster(namespace: str, assets: int, footprint: bool = True) -> Measurement:
    """Install the Dagster control plane and its user-code deployment."""
    m = Measurement("dagster", namespace)
    reset_namespace(namespace)

    user_deployment = "dagster-user-deployments.deployments[0]"
    start = time.monotonic()
    m.phases.append(
        _timed_install(
            "install",
            [
                "install",
                "dagster",
                "dagster/dagster",
                "--version",
                DAGSTER_VERSION,
                "-n",
                namespace,
                "--create-namespace",
                "-f",
                str(K8S / "values" / "dagster.yaml"),
                "--set",
                f"dagsterWebserver.image.repository={REGISTRY_IN_CLUSTER}/dagster-bench",
                "--set",
                f"dagsterDaemon.image.repository={REGISTRY_IN_CLUSTER}/dagster-bench",
                "--set",
                f"{user_deployment}.image.repository="
                f"{REGISTRY_IN_CLUSTER}/dagster-bench-usercode",
                # The chart's schema types these values as strings, so a bare
                # `--set` of a number fails validation.
                "--set-string",
                f"dagsterWebserver.image.tag={DAGSTER_VERSION}",
                "--set-string",
                f"dagsterDaemon.image.tag={DAGSTER_VERSION}",
                "--set-string",
                f"{user_deployment}.image.tag={DAGSTER_VERSION}",
                "--set-string",
                f"{user_deployment}.env.BENCH_N_ASSETS={assets}",
            ],
        )
    )
    m.phases.append(_ready_phase(m, namespace, start))

    _finish(m, namespace, footprint)
    m.notes.append(
        "Control plane: webserver, daemon, PostgreSQL, plus one user-code gRPC server."
    )
    m.notes.append(
        "The code location installs with the chart, so it is inside control_plane_ready."
    )
    m.notes.append("Images rebuilt natively for arm64 — Dagster publishes amd64 only.")
    m.notes.append(f"Code location holds {assets} assets.")
    return m


# --- Prefect -----------------------------------------------------------------


def register_flows(namespace: str, assets: int) -> tuple[str, str]:
    """Register ``assets`` deployments against the in-cluster Prefect server.

    Prefect has no code location object, so a flow becomes servable through a
    client call rather than a cluster resource. Running that call in-cluster is
    the closest counterpart to the other two loading a code location.
    """
    api = f"http://prefect-server.{namespace}.svc.cluster.local:4200/api"
    proc = kubectl(
        "run",
        "bench-register",
        "-n",
        namespace,
        "--image",
        f"{REGISTRY_IN_CLUSTER}/prefect-bench-flows:{PREFECT_VERSION}",
        "--image-pull-policy=IfNotPresent",
        "--restart=Never",
        "--attach",
        "--rm",
        "--quiet",
        "--env",
        f"PREFECT_API_URL={api}",
        "--env",
        f"BENCH_N_ASSETS={assets}",
        "--env",
        f"BENCH_WORK_POOL={WORK_POOL}",
        "--command",
        "--",
        "python",
        "-m",
        "bench_flows",
        check=False,
        timeout=READY_TIMEOUT,
    )
    if proc.returncode != 0:
        return "error", (proc.stderr or proc.stdout)[-300:]
    return "ok", ""


def deploy_prefect(namespace: str, assets: int, footprint: bool = True) -> Measurement:
    """Install the Prefect server control plane, a worker, then the flows."""
    m = Measurement("prefect", namespace)
    reset_namespace(namespace)

    start = time.monotonic()
    m.phases.append(
        _timed_install(
            "install",
            [
                "install",
                "prefect",
                "prefect/prefect-server",
                "-n",
                namespace,
                "--create-namespace",
                "-f",
                str(K8S / "values" / "prefect.yaml"),
                "--set-string",
                f"server.image.prefectTag={PREFECT_VERSION}-python3.11",
            ],
        )
    )
    m.phases.append(_ready_phase(m, namespace, start))

    if m.phases[-1].status == "ok":
        worker_start = time.monotonic()
        m.phases.append(
            _timed_install(
                "install-worker",
                [
                    "install",
                    "prefect-worker",
                    "prefect/prefect-worker",
                    "-n",
                    namespace,
                    "--set",
                    f"worker.config.workPool={WORK_POOL}",
                    "--set",
                    "worker.apiConfig=selfHostedServer",
                    "--set",
                    "worker.selfHostedServerApiConfig.apiUrl="
                    f"http://prefect-server.{namespace}.svc.cluster.local:4200/api",
                    "--set",
                    "worker.image.pullPolicy=IfNotPresent",
                    # The worker needs the `-kubernetes` image; the plain
                    # one has no Kubernetes worker and crash-loops.
                    "--set-string",
                    f"worker.image.prefectTag={PREFECT_VERSION}-python3.11-kubernetes",
                ],
            )
        )
        # Without `require`, this check can pass against the server workloads
        # that were already ready, before the worker Deployment even exists.
        ready = wait_ready(namespace, READY_TIMEOUT, require="prefect-worker")
        status, detail = ready.status, ready.detail
        if status == "ok":
            # A worker with nothing registered cannot accept work, so the phase
            # only ends once the flows are servable.
            status, detail = register_flows(namespace, assets)
        m.phases.append(
            Phase(
                "code_location_ready",
                time.monotonic() - worker_start,
                status,
                detail,
            )
        )

    _finish(m, namespace, footprint)
    m.notes.append("Control plane: Prefect server plus PostgreSQL.")
    m.notes.append(
        "A worker is installed separately; the server alone cannot run a flow."
    )
    m.notes.append(
        "Prefect has no code location object; flows reach workers through deployments, "
        "so `code location ready` covers the worker plus registering them."
    )
    m.notes.append(f"Code location holds {assets} flows.")
    return m


# --- the table ---------------------------------------------------------------


@dataclass(frozen=True)
class Orchestrator:
    """Everything the Kubernetes benchmark needs to know about one stack."""

    name: str
    releases: list[str]
    images: list[str]
    deploy: Callable[..., Measurement]

    @property
    def namespace(self) -> str:
        return f"{self.name}-bench"


# Images built locally rather than pulled. Docker Desktop's containerd image
# store produces manifests `k3d image import` rejects, so these go through the
# cluster's own registry instead.
ORCHESTRATORS = {
    o.name: o
    for o in (
        Orchestrator(
            name="rivers",
            releases=["rivers", "rivers-crds"],
            images=[
                "rivers-operator:release",
                "rivers-ui:release",
                "rivers-bench-code-location:release",
            ],
            deploy=deploy_rivers,
        ),
        Orchestrator(
            name="dagster",
            releases=["dagster"],
            images=[
                f"dagster-bench:{DAGSTER_VERSION}",
                f"dagster-bench-usercode:{DAGSTER_VERSION}",
            ],
            deploy=deploy_dagster,
        ),
        Orchestrator(
            name="prefect",
            releases=["prefect", "prefect-worker"],
            images=[f"prefect-bench-flows:{PREFECT_VERSION}"],
            deploy=deploy_prefect,
        ),
    )
}


def cleanup(orchestrator: Orchestrator) -> None:
    """Remove every release and the namespace, so the next install starts cold."""
    for release in orchestrator.releases:
        helm(
            "uninstall",
            release,
            "-n",
            orchestrator.namespace,
            "--ignore-not-found",
            check=False,
            timeout=600,
        )
    reset_namespace(orchestrator.namespace)
