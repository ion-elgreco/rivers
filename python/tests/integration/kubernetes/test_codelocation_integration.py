"""Integration tests for the K8s CodeLocation CRD.

Covers four areas:
  1. CodeLocation reconciliation lifecycle
  2. CodeLocationRegistry gRPC service
  3. Run CR admission webhook
  4. Multi-CodeLocation behaviour (same namespace) and namespace-scoped
     operator semantics

Each test owns its own CodeLocation/Run fixtures — nothing here depends on
the static `rivers-code-location` daemon, so these tests are independent
of `test_k8s_integration.py`.

Cluster prerequisites (set up by `dev/k3d/setup.sh`):
  * k3d cluster `${RIVERS_K8S_TEST_CONTEXT:-k3d-rivers-test}` with the local
    `k3d-rivers-registry` registry holding `rivers-code-location:latest`
  * `rivers` namespace + chart installed with cert-manager and
    `--set operator.webhook.cert.issuerRef.name=selfsigned`
  * Baseline `CodeLocation/k8s-test-pipeline` Ready in `rivers` namespace

Test CodeLocations reuse the baseline's already-resolved `image@digest`
(via `spec.digest` escape hatch) so they pull from the local registry
without the operator needing HTTPS access to it.
"""

import base64
import os
import time

import grpc
import pytest
from kr8s import NotFoundError
from kr8s.objects import APIObject, Deployment, Namespace, Secret, Service

from .conftest import KUBECTL_CONTEXT, kube_api

NAMESPACE = "rivers"
REGISTRY_SERVICE_NAME = "rivers-operator-registry"
REGISTRY_PORT = 50052
REGISTRY_TOKEN_SECRET = "rivers-registry-token"

BASELINE_CODE_LOCATION = os.environ.get(
    "RIVERS_K8S_TEST_CODE_LOCATION", "k8s-test-pipeline"
)


class CodeLocation(APIObject):
    """rivers.io/v1alpha1 CodeLocation CRD."""

    version = "rivers.io/v1alpha1"
    endpoint = "codelocations"
    kind = "CodeLocation"
    plural = "codelocations"
    singular = "codelocation"
    namespaced = True


class Run(APIObject):
    """rivers.io/v1alpha1 Run CRD."""

    version = "rivers.io/v1alpha1"
    endpoint = "runs"
    kind = "Run"
    plural = "runs"
    singular = "run"
    namespaced = True


def _baseline_resolved_image() -> str | None:
    """`<image>@<digest>` of the baseline CodeLocation, or None if unset."""
    try:
        cl = CodeLocation.get(
            BASELINE_CODE_LOCATION, namespace=NAMESPACE, api=kube_api()
        )
        return (cl.raw.get("status") or {}).get("resolvedImage")
    except Exception:
        return None


def _cluster_ready() -> bool:
    """Cluster reachable, registry Service present, baseline CodeLocation Ready."""
    try:
        api = kube_api()
        list(api.get("namespaces", NAMESPACE))
        list(CodeLocation.list(namespace=NAMESPACE, api=api))
        Service.get(REGISTRY_SERVICE_NAME, namespace=NAMESPACE, api=api)
        cl = CodeLocation.get(BASELINE_CODE_LOCATION, namespace=NAMESPACE, api=api)
        return (cl.raw.get("status") or {}).get("phase") == "Ready"
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _cluster_ready(),
    reason=(
        f"k3d cluster '{KUBECTL_CONTEXT}' / namespace '{NAMESPACE}' / "
        f"registry Service '{REGISTRY_SERVICE_NAME}' / baseline CodeLocation "
        f"'{BASELINE_CODE_LOCATION}' (Ready) not all present — "
        "run `just k8s-up` first"
    ),
)


# ---------------------------------------------------------------------------
# CodeLocation fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def baseline_image_and_digest() -> tuple[str, str]:
    """Image + digest of the baseline CodeLocation. New test CRs reuse these
    so the local registry has the manifest k8s will pull."""
    resolved = _baseline_resolved_image()
    assert resolved and "@" in resolved, (
        f"Baseline CodeLocation '{BASELINE_CODE_LOCATION}' has no resolvedImage; "
        "cluster setup is incomplete"
    )
    image, digest = resolved.split("@", 1)
    return image, digest


def _make_codelocation(
    name: str,
    image_and_digest: tuple[str, str],
    *,
    namespace: str = NAMESPACE,
    module: str = "k8s_test_pipeline.pipeline",
) -> CodeLocation:
    image, digest = image_and_digest
    body = {
        "apiVersion": "rivers.io/v1alpha1",
        "kind": "CodeLocation",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "image": image,
            "tag": "latest",
            "digest": digest,
            "module": module,
            "replicas": 1,
        },
    }
    return CodeLocation(body, api=kube_api())


def _wait_for_codelocation_phase(
    name: str,
    terminal: set[str],
    *,
    namespace: str = NAMESPACE,
    timeout: int = 180,
) -> str:
    """Wait until phase ∈ terminal. 180s default leaves room for image pull."""
    deadline = time.monotonic() + timeout
    phase = "Unknown"
    while time.monotonic() < deadline:
        try:
            cl = CodeLocation.get(name, namespace=namespace, api=kube_api())
        except NotFoundError:
            time.sleep(1)
            continue
        phase = (cl.raw.get("status") or {}).get("phase") or "Pending"
        if phase in terminal:
            return phase
        time.sleep(2)
    return phase


def _delete_codelocations_except_baseline(*, namespace: str = NAMESPACE) -> None:
    """Delete all CodeLocations except the shared baseline, then wait for the
    operator-managed Deployments to be GC'd. Tests that create CRs back-to-back
    can otherwise race the previous test's pod for node resources, causing
    spurious `Deploying`-stuck CRs."""
    deleted_uids: set[str] = set()
    try:
        for cl in CodeLocation.list(namespace=namespace, api=kube_api()):
            if cl.name == BASELINE_CODE_LOCATION and namespace == NAMESPACE:
                continue
            deleted_uids.add(cl.raw["metadata"]["uid"])
            try:
                cl.delete()
            except NotFoundError:
                pass
    except Exception:
        pass

    if not deleted_uids:
        return
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            deps = list(Deployment.list(namespace=namespace, api=kube_api()))
        except Exception:
            return
        leftovers = [
            d
            for d in deps
            if any(
                ref.get("uid") in deleted_uids
                for ref in (d.raw["metadata"].get("ownerReferences") or [])
            )
        ]
        if not leftovers:
            return
        time.sleep(2)


def _delete_runs(*, namespace: str = NAMESPACE) -> None:
    try:
        for run in Run.list(namespace=namespace, api=kube_api()):
            try:
                run.delete()
            except NotFoundError:
                pass
    except Exception:
        pass


def _build_run_body(
    code_location_name: str,
    *,
    namespace: str = NAMESPACE,
    name_prefix: str = "webhook-",
    image: str | None = None,
) -> dict:
    spec: dict = {
        "codeLocationRef": {"name": code_location_name},
        "target": "*",
    }
    if image is not None:
        spec["image"] = image
    return {
        "apiVersion": "rivers.io/v1alpha1",
        "kind": "Run",
        "metadata": {"generateName": name_prefix, "namespace": namespace},
        "spec": spec,
    }


# ---------------------------------------------------------------------------
# Registry gRPC plumbing
# ---------------------------------------------------------------------------


def _registry_token() -> str:
    """Read the bearer token the chart wrote to the rivers-registry-token Secret.

    Helm's `randAlphaNum 48 | b64enc` already base64-encodes the random
    string before storing it in the Secret. The apiserver then base64-encodes
    Secret data over the wire. So we decode twice: apiserver decode →
    b64-encoded token → underlying token bytes.
    """
    sec = Secret.get(REGISTRY_TOKEN_SECRET, namespace=NAMESPACE, api=kube_api())
    encoded = sec.raw["data"]["token"]
    inner = base64.b64decode(encoded)
    try:
        return base64.b64decode(inner).decode("ascii")
    except Exception:
        return inner.decode("ascii")


class RegistryChannel:
    """Context manager: kr8s port-forward to the registry Service + auth metadata."""

    def __init__(self, grpc_stubs, *, token: str | None = None):
        self._pb2, self._pb2_grpc = grpc_stubs
        self._token = token if token is not None else _registry_token()
        svc = Service.get(REGISTRY_SERVICE_NAME, namespace=NAMESPACE, api=kube_api())
        self._pf_ctx = svc.portforward(remote_port=REGISTRY_PORT, local_port="auto")
        self._channel: grpc.Channel | None = None

    def __enter__(self) -> "RegistryChannel":
        local_port = self._pf_ctx.__enter__()
        self._channel = grpc.insecure_channel(f"127.0.0.1:{local_port}")
        grpc.channel_ready_future(self._channel).result(timeout=10)
        self.stub = self._pb2_grpc.CodeLocationRegistryServiceStub(self._channel)
        return self

    def __exit__(self, *exc):
        if self._channel:
            self._channel.close()
        self._pf_ctx.__exit__(*exc)

    @property
    def pb2(self):
        return self._pb2

    def auth(self) -> list[tuple[str, str]]:
        return [("authorization", f"Bearer {self._token}")]


# ---------------------------------------------------------------------------
# CodeLocation reconciliation lifecycle
# ---------------------------------------------------------------------------


class TestCodeLocationLifecycle:
    """Operator reconciles CodeLocation CRs into Deployment + Service + status."""

    def setup_method(self):
        _delete_runs()
        _delete_codelocations_except_baseline()

    def teardown_method(self):
        _delete_runs()
        _delete_codelocations_except_baseline()

    def test_codelocation_reaches_ready_with_resolved_image(
        self, baseline_image_and_digest
    ):
        """Pinned digest short-circuits registry probe; CR reaches Ready when the
        operator-managed Deployment becomes Available."""
        _, expected_digest = baseline_image_and_digest
        cl = _make_codelocation("life-ready", baseline_image_and_digest)
        cl.create()

        phase = _wait_for_codelocation_phase("life-ready", {"Ready", "Failed"})
        assert phase == "Ready", f"Expected Ready, got {phase}"

        cl.refresh()
        resolved = (cl.raw.get("status") or {}).get("resolvedImage", "")
        assert resolved.endswith(f"@{expected_digest}"), (
            f"Expected resolvedImage to end with @{expected_digest}, got {resolved!r}"
        )

    def test_codelocation_creates_owned_deployment_and_service(
        self, baseline_image_and_digest
    ):
        """Reconciler creates a Deployment + Service with owner refs back to the CR."""
        cl = _make_codelocation("life-owned", baseline_image_and_digest)
        cl.create()
        _wait_for_codelocation_phase("life-owned", {"Ready", "Failed"})

        cl.refresh()
        cr_uid = cl.raw["metadata"]["uid"]

        deps = list(Deployment.list(namespace=NAMESPACE, api=kube_api()))
        owned_dep = next(
            (
                d
                for d in deps
                if any(
                    ref.get("uid") == cr_uid
                    for ref in (d.raw["metadata"].get("ownerReferences") or [])
                )
            ),
            None,
        )
        assert owned_dep is not None, (
            "No Deployment in the namespace has an ownerReference back to the CodeLocation"
        )

        services = list(Service.list(namespace=NAMESPACE, api=kube_api()))
        owned_svc = next(
            (
                s
                for s in services
                if any(
                    ref.get("uid") == cr_uid
                    for ref in (s.raw["metadata"].get("ownerReferences") or [])
                )
            ),
            None,
        )
        assert owned_svc is not None, (
            "No Service in the namespace has an ownerReference back to the CodeLocation"
        )

    def test_codelocation_deletion_garbage_collects_owned_resources(
        self, baseline_image_and_digest
    ):
        """Deleting the CR triggers k8s GC of the owned Deployment + Service."""
        cl = _make_codelocation("life-gc", baseline_image_and_digest)
        cl.create()
        _wait_for_codelocation_phase("life-gc", {"Ready", "Failed"})

        cl.refresh()
        cr_uid = cl.raw["metadata"]["uid"]
        cl.delete()

        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            deps = list(Deployment.list(namespace=NAMESPACE, api=kube_api()))
            still_owned = [
                d
                for d in deps
                if any(
                    ref.get("uid") == cr_uid
                    for ref in (d.raw["metadata"].get("ownerReferences") or [])
                )
            ]
            if not still_owned:
                return
            time.sleep(2)
        pytest.fail("Deployment owned by deleted CodeLocation was not GC'd")


# ---------------------------------------------------------------------------
# CodeLocationRegistry gRPC
# ---------------------------------------------------------------------------


class TestRegistryGrpc:
    """Operator-hosted CodeLocationRegistry service (gRPC, bearer-auth)."""

    def setup_method(self):
        _delete_codelocations_except_baseline()

    def teardown_method(self):
        _delete_codelocations_except_baseline()

    def test_registry_list_returns_applied_codelocation(
        self, grpc_stubs, baseline_image_and_digest
    ):
        """List RPC sees a freshly-applied CR after the watcher syncs."""
        cl = _make_codelocation("reg-list", baseline_image_and_digest)
        cl.create()
        _wait_for_codelocation_phase("reg-list", {"Ready", "Failed"})

        with RegistryChannel(grpc_stubs) as ch:
            deadline = time.monotonic() + 15
            entries: list = []
            while time.monotonic() < deadline:
                resp = ch.stub.List(
                    ch.pb2.ListCodeLocationsRequest(namespace=NAMESPACE),
                    metadata=ch.auth(),
                )
                entries = [e for e in resp.entries if e.name == "reg-list"]
                if entries:
                    break
                time.sleep(1)

        assert entries, "Registry List did not surface the new CodeLocation"
        entry = entries[0]
        assert entry.namespace == NAMESPACE
        assert entry.module == "k8s_test_pipeline.pipeline"
        _, expected_digest = baseline_image_and_digest
        assert expected_digest in entry.image, (
            f"Registry entry.image should carry the resolved digest, got {entry.image!r}"
        )

    def test_registry_watch_emits_synced_marker(
        self, grpc_stubs, baseline_image_and_digest
    ):
        """Watch streams snapshot ADDED events then a SYNCED marker."""
        _make_codelocation("reg-watch", baseline_image_and_digest).create()
        _wait_for_codelocation_phase("reg-watch", {"Ready", "Failed"})

        with RegistryChannel(grpc_stubs) as ch:
            req = ch.pb2.WatchCodeLocationsRequest(namespace=NAMESPACE)
            stream = ch.stub.Watch(req, metadata=ch.auth(), timeout=15)

            saw_added_for_target = False
            saw_synced = False
            for event in stream:
                if event.type == ch.pb2.CodeLocationEvent.TYPE_ADDED:
                    if event.entry.name == "reg-watch":
                        saw_added_for_target = True
                elif event.type == ch.pb2.CodeLocationEvent.TYPE_SYNCED:
                    saw_synced = True
                    break

            assert saw_added_for_target, (
                "Snapshot stream missing TYPE_ADDED for reg-watch"
            )
            assert saw_synced, "Stream never emitted TYPE_SYNCED"

    def test_registry_rejects_missing_bearer_token(self, grpc_stubs):
        """No / malformed auth metadata → Unauthenticated."""
        with RegistryChannel(grpc_stubs, token="") as ch:
            with pytest.raises(grpc.RpcError) as exc_info:
                ch.stub.List(
                    ch.pb2.ListCodeLocationsRequest(namespace=NAMESPACE),
                    metadata=[("authorization", "Bearer")],
                )
            assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED


# ---------------------------------------------------------------------------
# Run admission webhook
# ---------------------------------------------------------------------------


class TestAdmissionWebhook:
    """Operator-hosted MutatingWebhookConfiguration stamps + guards Run CRs."""

    def setup_method(self):
        _delete_runs()
        _delete_codelocations_except_baseline()

    def teardown_method(self):
        _delete_runs()
        _delete_codelocations_except_baseline()

    def _ready_codelocation(
        self,
        name: str,
        image_and_digest: tuple[str, str],
        *,
        module: str = "k8s_test_pipeline.pipeline",
    ) -> CodeLocation:
        cl = _make_codelocation(name, image_and_digest, module=module)
        cl.create()
        phase = _wait_for_codelocation_phase(name, {"Ready", "Failed"})
        assert phase == "Ready", (
            f"CodeLocation {name!r} required Ready for webhook test, got {phase}"
        )
        return cl

    def test_webhook_stamps_image_and_module_on_create(self, baseline_image_and_digest):
        """A Run with only `codeLocationRef` lands with `image`/`module` populated."""
        image, digest = baseline_image_and_digest
        self._ready_codelocation("wh-stamp", baseline_image_and_digest)

        run = Run(_build_run_body("wh-stamp"), api=kube_api())
        run.create()
        run.refresh()

        assert run.raw["spec"]["image"] == f"{image}@{digest}"
        assert run.raw["spec"]["module"] == "k8s_test_pipeline.pipeline"

    def test_webhook_rejects_unknown_codelocation(self):
        """codeLocationRef.name pointing at a non-existent CR → admission denial."""
        run = Run(_build_run_body("does-not-exist"), api=kube_api())
        with pytest.raises(Exception) as exc_info:
            run.create()
        msg = str(exc_info.value).lower()
        assert "codelocation" in msg or "does-not-exist" in msg, (
            f"Unexpected error message: {exc_info.value!r}"
        )

    def test_webhook_rejects_image_change_on_update(self, baseline_image_and_digest):
        """`spec.image` is immutable post-stamping."""
        _, digest = baseline_image_and_digest
        self._ready_codelocation("wh-imm-img", baseline_image_and_digest)

        run = Run(_build_run_body("wh-imm-img"), api=kube_api())
        run.create()

        with pytest.raises(Exception) as exc_info:
            run.patch(
                {"spec": {"image": f"evil/replacement@{digest}"}},
                type="merge",
            )
        msg = str(exc_info.value).lower()
        assert "image" in msg or "immutable" in msg, (
            f"Expected immutability error, got: {exc_info.value!r}"
        )

    def test_webhook_rejects_codelocationref_change_on_update(
        self, baseline_image_and_digest
    ):
        """`codeLocationRef` is immutable post-create.

        `check_update` is purely structural (compares new vs old) and runs
        BEFORE any CodeLocation lookup, so the patched-to name doesn't need
        to point at a real CR to exercise this path — saves us from waiting
        for a second baseline to reach Ready.
        """
        self._ready_codelocation("wh-ref-a", baseline_image_and_digest)

        run = Run(_build_run_body("wh-ref-a"), api=kube_api())
        run.create()

        with pytest.raises(Exception) as exc_info:
            run.patch(
                {"spec": {"codeLocationRef": {"name": "wh-ref-other"}}},
                type="merge",
            )
        msg = str(exc_info.value).lower()
        assert "codelocationref" in msg or "immutable" in msg, (
            f"Expected immutability error, got: {exc_info.value!r}"
        )

    def test_webhook_passes_through_pinned_digest(self, baseline_image_and_digest):
        """User-supplied `image: foo@sha256:...` bypasses CodeLocation lookup."""
        _, digest = baseline_image_and_digest
        # The webhook accepts pre-pinned digests without needing a backing CR.
        pinned = f"my.registry/img@{digest}"
        run = Run(_build_run_body("wh-passthrough", image=pinned), api=kube_api())
        run.create()
        run.refresh()
        assert run.raw["spec"]["image"] == pinned

    def test_webhook_rejects_tag_only_image(self, baseline_image_and_digest):
        """`image: foo:latest` (no `@sha256:`) is rejected, even with valid CR ref."""
        self._ready_codelocation("wh-tag", baseline_image_and_digest)

        run = Run(
            _build_run_body("wh-tag", image="ghcr.io/rivers/t:latest"),
            api=kube_api(),
        )
        with pytest.raises(Exception) as exc_info:
            run.create()
        msg = str(exc_info.value).lower()
        assert "digest" in msg or "tag" in msg or "sha256" in msg, (
            f"Expected digest-format error, got: {exc_info.value!r}"
        )


# ---------------------------------------------------------------------------
# Multi-CodeLocation behaviour
# ---------------------------------------------------------------------------


class TestMultiCodeLocation:
    """Two CodeLocations side-by-side; namespace-scoped operator visibility."""

    def setup_method(self):
        _delete_runs()
        _delete_codelocations_except_baseline()

    def teardown_method(self):
        _delete_runs()
        _delete_codelocations_except_baseline()

    def test_two_codelocations_same_namespace_both_ready_and_listed(
        self, grpc_stubs, baseline_image_and_digest
    ):
        """Apply A + B in the same namespace; both reach Ready, registry returns both."""
        _make_codelocation("multi-a", baseline_image_and_digest).create()
        _make_codelocation("multi-b", baseline_image_and_digest).create()

        assert _wait_for_codelocation_phase("multi-a", {"Ready", "Failed"}) == "Ready"
        assert _wait_for_codelocation_phase("multi-b", {"Ready", "Failed"}) == "Ready"

        with RegistryChannel(grpc_stubs) as ch:
            deadline = time.monotonic() + 15
            names: set[str] = set()
            while time.monotonic() < deadline:
                resp = ch.stub.List(
                    ch.pb2.ListCodeLocationsRequest(namespace=NAMESPACE),
                    metadata=ch.auth(),
                )
                names = {e.name for e in resp.entries}
                if {"multi-a", "multi-b"}.issubset(names):
                    break
                time.sleep(1)
            assert {"multi-a", "multi-b"}.issubset(names), (
                f"Registry missing one of multi-a/multi-b: {names}"
            )

    def test_runs_stamped_per_codelocation_ref(self, baseline_image_and_digest):
        """Each Run is stamped from the CodeLocation its `codeLocationRef.name` resolves to.

        Both CRs share the same image+module (the only working pipeline in the
        test image), so the assertion just verifies that each Run's stamping
        matches its referenced CR's resolved values — the per-CR isolation
        proof is in the registry sees-both test plus the unknown-codelocation
        rejection test.
        """
        image, digest = baseline_image_and_digest
        _make_codelocation("stamp-a", baseline_image_and_digest).create()
        _make_codelocation("stamp-b", baseline_image_and_digest).create()
        assert _wait_for_codelocation_phase("stamp-a", {"Ready", "Failed"}) == "Ready"
        assert _wait_for_codelocation_phase("stamp-b", {"Ready", "Failed"}) == "Ready"

        run_a = Run(_build_run_body("stamp-a", name_prefix="multi-a-"), api=kube_api())
        run_a.create()
        run_a.refresh()

        run_b = Run(_build_run_body("stamp-b", name_prefix="multi-b-"), api=kube_api())
        run_b.create()
        run_b.refresh()

        expected_image = f"{image}@{digest}"
        expected_module = "k8s_test_pipeline.pipeline"
        assert run_a.raw["spec"]["image"] == expected_image
        assert run_a.raw["spec"]["module"] == expected_module
        assert run_b.raw["spec"]["image"] == expected_image
        assert run_b.raw["spec"]["module"] == expected_module

    def test_codelocation_in_other_namespace_invisible_to_operator(
        self, baseline_image_and_digest
    ):
        """The operator is namespace-scoped: a CodeLocation in another namespace
        is not resolvable by Runs in the operator's namespace.

        We don't expect that *foreign* CodeLocation to ever reach Ready (no
        operator runs there) — what we're checking is that referencing it from
        a Run in `rivers` is rejected by the webhook with a not-found error.
        """
        other_ns = "rivers-other-cl"
        api = kube_api()

        # Best-effort namespace creation; tolerate AlreadyExists.
        try:
            Namespace(
                {
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": other_ns},
                },
                api=api,
            ).create()
        except Exception:
            pass

        try:
            _make_codelocation(
                "foreign", baseline_image_and_digest, namespace=other_ns
            ).create()

            run = Run(
                _build_run_body("foreign", name_prefix="foreign-ref-"),
                api=kube_api(),
            )
            with pytest.raises(Exception) as exc_info:
                run.create()
            msg = str(exc_info.value).lower()
            assert "codelocation" in msg or "foreign" in msg or "not found" in msg, (
                f"Expected not-found error, got: {exc_info.value!r}"
            )
        finally:
            _delete_codelocations_except_baseline(namespace=other_ns)
            try:
                Namespace.get(other_ns, api=api).delete()
            except Exception:
                pass
