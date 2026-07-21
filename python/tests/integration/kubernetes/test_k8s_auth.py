"""Live UI-auth tests against the deployed rivers-ui.

Each test patches ``RIVERS_AUTH_*`` env onto the running ``rivers-ui``
Deployment (restored to mode ``none`` at module teardown) and drives the
HTTP surface through a port-forward:

  * forward mode — trusted-proxy gating, identity headers, allowlists;
  * oidc mode — the full authorization-code flow (login, callback, session
    cookie, logout) against an in-cluster FerrisKey (admin-API bootstrap,
    JSON ``login-actions/authenticate``) with a ``john.doe@example.com``
    user.

Port-forwarded traffic reaches the pod from 127.0.0.1, which is what the
forward-mode trust checks key on. The OIDC redirect URI must be registered
at the IdP ahead of time, so the UI port-forward uses a fixed local port.

Requires `just k8s-up` (with the UI image — skipped under
RIVERS_K8S_SKIP_UI deploys).
"""

import base64
import os
import subprocess
import time
from contextlib import contextmanager

import httpx
import pytest
from kr8s.objects import Deployment, Secret, Service

from .conftest import KUBECTL_CONTEXT, cluster_gate, kube_api
from .test_k8s_integration import NAMESPACE, _cluster_reachable

UI_DEPLOYMENT = "rivers-ui"
UI_SERVICE = "rivers-ui"
UI_PORT = 3000
# Fixed so the IdP can pre-register http://127.0.0.1:18300/auth/callback.
UI_LOCAL_PORT = 18300


def _ui_deployed() -> bool:
    try:
        Deployment.get(UI_DEPLOYMENT, namespace=NAMESPACE, api=kube_api())
        return True
    except Exception:
        return False


_reachable = _cluster_reachable()
pytestmark = [
    cluster_gate(
        _reachable,
        f"k3d cluster '{KUBECTL_CONTEXT}' not reachable or namespace '{NAMESPACE}' missing",
    ),
    # UI-less deploys (RIVERS_K8S_SKIP_UI=1) are legitimate even in CI —
    # skip rather than fail.
    pytest.mark.skipif(
        _reachable and not _ui_deployed(),
        reason="rivers-ui deployment not present (UI-less deploy)",
    ),
    # Each test rolls the UI deployment (and one installs a Helm chart) —
    # far beyond the repo-wide 60s per-test budget.
    pytest.mark.timeout(900),
]


def _retry_api(fn, attempts: int = 5, delay: float = 3.0):
    """The k3d API server can 504 briefly under image-pull / install load."""
    last: Exception | None = None
    for _ in range(attempts):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 — transport + status errors alike
            last = e
            time.sleep(delay)
    raise last


def _ui_pods() -> list:
    return list(
        kube_api().get(
            "pods",
            namespace=NAMESPACE,
            label_selector="app.kubernetes.io/name=rivers-ui",
        )
    )


def _wait_rollout(timeout: float = 240.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        d = _retry_api(
            lambda: Deployment.get(UI_DEPLOYMENT, namespace=NAMESPACE, api=kube_api())
        )
        want = d.spec.get("replicas", 1)
        st = d.status
        pods = _retry_api(_ui_pods)
        settled = len(pods) == want and all(
            not p.raw["metadata"].get("deletionTimestamp") for p in pods
        )
        if (
            st.get("observedGeneration", 0) >= d.metadata.get("generation", 0)
            and st.get("updatedReplicas", 0) == want
            and st.get("readyReplicas", 0) == want
            and st.get("replicas", 0) == want
            and settled
        ):
            return
        time.sleep(2)
    pytest.fail("rivers-ui rollout did not complete in time")


def _apply_auth_env(auth_env: dict[str, str]) -> None:
    """Replace the ui container's RIVERS_AUTH_* env with `auth_env`, keeping
    everything else, then wait for the rollout."""
    deploy = _retry_api(
        lambda: Deployment.get(UI_DEPLOYMENT, namespace=NAMESPACE, api=kube_api())
    )
    containers = deploy.raw["spec"]["template"]["spec"]["containers"]
    idx = next(i for i, c in enumerate(containers) if c["name"] == "ui")
    env = [
        e
        for e in containers[idx].get("env", [])
        if not e["name"].startswith("RIVERS_AUTH_")
    ]
    env += [{"name": k, "value": v} for k, v in sorted(auth_env.items())]
    _retry_api(
        lambda: deploy.patch(
            [
                {
                    "op": "replace",
                    "path": f"/spec/template/spec/containers/{idx}/env",
                    "value": env,
                }
            ],
            type="json",
        )
    )
    _wait_rollout()


@pytest.fixture(scope="module")
def ui_auth_env():
    """Configure UI auth env for a test; module teardown restores mode none."""
    yield _apply_auth_env
    _retry_api(lambda: _apply_auth_env({}), attempts=3)


def _wait_http_ready(client: httpx.Client, timeout: float = 60.0) -> None:
    # /healthz is public in every auth mode; also absorbs the port-forward /
    # endpoint-swap races right after a rollout.
    deadline = time.time() + timeout
    last: Exception | None = None
    while time.time() < deadline:
        try:
            if client.get("/healthz").status_code == 200:
                return
        except httpx.HTTPError as e:
            last = e
        time.sleep(1)
    pytest.fail(f"UI did not become ready through the port-forward: {last}")


@contextmanager
def ui_client():
    pods = [
        p
        for p in _retry_api(_ui_pods)
        if not p.raw["metadata"].get("deletionTimestamp")
    ]
    assert pods, "no rivers-ui pods found"
    pod = max(pods, key=lambda p: p.raw["metadata"]["creationTimestamp"])
    with pod.portforward(remote_port=UI_PORT, local_port=UI_LOCAL_PORT) as port:
        with httpx.Client(
            base_url=f"http://127.0.0.1:{port}",
            follow_redirects=False,
            timeout=10.0,
        ) as client:
            _wait_http_ready(client)
            yield client


HTML = {"accept": "text/html"}


def get_page(ui: httpx.Client, headers: dict) -> httpx.Response:
    """GET `/` as a document; on a populated cluster the root route 302s to
    the first Ready code location — follow that one hop (302 only, so the
    auth middleware's 303-to-login redirects stay visible to assertions)."""
    r = ui.get("/", headers=headers)
    if r.status_code == 302:
        r = ui.get(r.headers["location"], headers=headers)
    return r


AS_JDOE = {
    "Remote-User": "jdoe",
    "Remote-Email": "john.doe@example.com",
    "Remote-Name": "John Doe",
}


def test_forward_auth_gates_and_passes_identity(ui_auth_env):
    ui_auth_env(
        {
            "RIVERS_AUTH_MODE": "forward",
            "RIVERS_AUTH_FORWARD_TRUSTED_PROXIES": "127.0.0.1/32",
            "RIVERS_AUTH_ALLOWED_GROUPS": "data-eng",
        }
    )
    with ui_client() as ui:
        # Trusted peer without identity headers: the proxy is misconfigured.
        assert ui.get("/", headers=HTML).status_code == 401

        # Identity + admitted group → SSR page renders, chip shows the name.
        ok = get_page(ui, HTML | AS_JDOE | {"Remote-Groups": "data-eng, other"})
        assert ok.status_code == 200
        assert "John Doe" in ok.text

        # Authenticated but not on the allowlist → 403.
        denied = ui.get("/", headers=HTML | AS_JDOE | {"Remote-Groups": "other"})
        assert denied.status_code == 403

        # Health stays public.
        assert ui.get("/healthz").status_code == 200


def test_forward_auth_untrusted_peer_rejected(ui_auth_env):
    # TEST-NET CIDR: the port-forward peer (127.0.0.1) is never trusted, so
    # identity headers must be ignored outright.
    ui_auth_env(
        {
            "RIVERS_AUTH_MODE": "forward",
            "RIVERS_AUTH_FORWARD_TRUSTED_PROXIES": "192.0.2.0/24",
        }
    )
    with ui_client() as ui:
        assert ui.get("/", headers=HTML | AS_JDOE).status_code == 403
        assert ui.get("/healthz").status_code == 200


# ── oidc against in-cluster FerrisKey ───────────────────────────────────
#
# Deployed via the official chart with its embedded PostgreSQL
# (`postgresql.enabled` is the chart default); the login SPA is scaled to
# zero — the test authenticates against the JSON endpoint directly.

FERRISKEY_CHART = "oci://ghcr.io/ferriskey/charts/ferriskey"
FERRISKEY_CHART_VERSION = os.environ.get("RIVERS_FERRISKEY_CHART_VERSION", "0.7.1")
# The chart serves the API under `api.rootPath` (default /api).
FERRISKEY_HOST = "http://ferriskey-api.rivers.svc.cluster.local:3333"
FERRISKEY_ISSUER = f"{FERRISKEY_HOST}/api/realms/master"
# FerrisKey enforces a 12+ char complexity policy on reset-password.
FK_USER_PASSWORD = "JohnDoe-Passw0rd!"


def _helm(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["helm", "--kube-context", KUBECTL_CONTEXT, "-n", NAMESPACE, *args],
        capture_output=True,
        text=True,
    )


def _ferriskey_bootstrap(base: str, admin_password: str) -> str:
    """Provision the `rivers` client + john.doe through the admin API and
    return the server-minted client secret."""
    with httpx.Client(base_url=base, timeout=15.0) as c:
        tok = c.post(
            "/realms/master/protocol/openid-connect/token",
            data={
                "grant_type": "password",
                "client_id": "admin-cli",
                "username": "admin",
                "password": admin_password,
            },
        ).json()["access_token"]
        c.headers["authorization"] = f"Bearer {tok}"

        client = c.post(
            "/realms/master/clients",
            json={
                "client_id": "rivers",
                "name": "rivers",
                "enabled": True,
                "protocol": "openid-connect",
                "public_client": False,
                "service_account_enabled": False,
                "client_type": "confidential",
            },
        ).json()
        secret = client["secret"]
        # Redirect URIs are a sub-resource and start disabled.
        redirect = c.post(
            f"/realms/master/clients/{client['id']}/redirects",
            json={"value": f"http://127.0.0.1:{UI_LOCAL_PORT}/auth/callback"},
        ).json()
        r = c.put(
            f"/realms/master/clients/{client['id']}/redirects/{redirect['id']}",
            json={
                "enabled": True,
                "value": f"http://127.0.0.1:{UI_LOCAL_PORT}/auth/callback",
            },
        )
        assert r.status_code == 200, r.text

        user = c.post(
            "/realms/master/users",
            json={
                "username": "john.doe",
                "email": "john.doe@example.com",
                "firstname": "John",
                "lastname": "Doe",
                "email_verified": True,
            },
        ).json()["data"]
        r = c.put(
            f"/realms/master/users/{user['id']}/reset-password",
            json={
                "credential_type": "password",
                "value": FK_USER_PASSWORD,
                "temporary": False,
            },
        )
        assert r.status_code == 200, r.text
        return secret


@pytest.fixture()
def ferriskey():
    # No --wait: the chart's migrations Job is a `post-install` hook, and
    # --wait deadlocks on it (the api can't become ready against an
    # unmigrated database, and the hook only fires after --wait succeeds).
    install = _helm(
        "install",
        "ferriskey",
        FERRISKEY_CHART,
        "--version",
        FERRISKEY_CHART_VERSION,
        "--set",
        "postgresql.persistence.enabled=false",
        "--set",
        "webapp.replicas=0",
    )
    try:
        if install.returncode != 0:
            pytest.fail(f"helm install ferriskey failed: {install.stderr}")
        deadline = time.time() + 480
        while time.time() < deadline:
            ready = _retry_api(
                lambda: Deployment.get(
                    "ferriskey-api", namespace=NAMESPACE, api=kube_api()
                ).status.get("readyReplicas", 0)
            )
            if ready == 1:
                break
            time.sleep(5)
        else:
            pytest.fail("ferriskey-api never became ready")
        admin_password = base64.b64decode(
            Secret.get("ferriskey-api-admin", namespace=NAMESPACE, api=kube_api()).raw[
                "data"
            ]["password"]
        ).decode()
        svc = Service.get("ferriskey-api", namespace=NAMESPACE, api=kube_api())
        with svc.portforward(remote_port=3333, local_port="auto") as port:
            base = f"http://127.0.0.1:{port}/api"
            with httpx.Client(base_url=base, timeout=10.0) as c:
                deadline = time.time() + 120
                while time.time() < deadline:
                    try:
                        if (
                            c.get(
                                "/realms/master/.well-known/openid-configuration"
                            ).status_code
                            == 200
                        ):
                            break
                    except httpx.HTTPError:
                        pass
                    time.sleep(1)
                else:
                    pytest.fail("ferriskey discovery endpoint never came up")
            yield _ferriskey_bootstrap(base, admin_password)
    finally:
        _helm("uninstall", "ferriskey", "--wait")


def test_oidc_full_login_flow_against_ferriskey(ui_auth_env, ferriskey):
    ui_auth_env(
        {
            "RIVERS_AUTH_MODE": "oidc",
            "RIVERS_AUTH_OIDC_ISSUER": FERRISKEY_ISSUER,
            "RIVERS_AUTH_OIDC_CLIENT_ID": "rivers",
            "RIVERS_AUTH_OIDC_CLIENT_SECRET": ferriskey,
            "RIVERS_AUTH_PUBLIC_URL": f"http://127.0.0.1:{UI_LOCAL_PORT}",
        }
    )
    fk_svc = Service.get("ferriskey-api", namespace=NAMESPACE, api=kube_api())
    with fk_svc.portforward(remote_port=3333, local_port="auto") as fk_port:

        def to_local(url: str) -> str:
            return url.replace(FERRISKEY_HOST, f"http://127.0.0.1:{fk_port}")

        with (
            ui_client() as ui,
            httpx.Client(follow_redirects=False, timeout=10.0) as browser,
        ):
            r = ui.get("/", headers=HTML)
            assert r.status_code == 303
            assert r.headers["location"].startswith("/auth/login")

            r = ui.get("/auth/login?rd=/")
            assert r.status_code == 303
            authorize = r.headers["location"]
            assert authorize.startswith(FERRISKEY_ISSUER)
            assert "code_challenge=" in authorize

            # The authorize hit binds the auth session to browser cookies and
            # redirects toward the (undeployed) login SPA; authenticate
            # directly against the JSON endpoint instead.
            r = browser.get(to_local(authorize))
            assert r.status_code in (302, 303), r.text
            r = browser.post(
                to_local(
                    f"{FERRISKEY_ISSUER}/login-actions/authenticate?client_id=rivers"
                ),
                json={"username": "john.doe", "password": FK_USER_PASSWORD},
            )
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["status"] == "Success", body
            callback = body["url"]
            assert callback.startswith(
                f"http://127.0.0.1:{UI_LOCAL_PORT}/auth/callback"
            )

            r = ui.get(callback.removeprefix(f"http://127.0.0.1:{UI_LOCAL_PORT}"))
            assert r.status_code == 303, r.text
            assert r.headers["location"] == "/"

            page = get_page(ui, HTML)
            assert page.status_code == 200
            # FerrisKey issues no `name` claim for local users; the chip
            # falls back to the email snapshot.
            assert "john.doe@example.com" in page.text

            assert ui.get("/auth/logout").status_code == 303
            r = ui.get("/", headers=HTML)
            assert r.status_code == 303
            assert r.headers["location"].startswith("/auth/login")
