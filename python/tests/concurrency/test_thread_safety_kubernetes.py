"""Thread-safety of the Kubernetes executor's first connection.

The executor builds one kube client per process and reads its CodeLocation's
env from the API server once. A thread that waits for that answer while
attached to the interpreter blocks every thread that needs the interpreter,
the server's too. Each scenario runs in a child process with a fake API server
in that process, so a deadlock fails its test instead of the whole test run.
"""

import gc
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import obstore.store
import pytest
from _threads import run_in_child, run_threads

import rivers as rs

N_RUNS = 4
CODE_LOCATION_PATH = "/apis/rivers.io/v1alpha1/namespaces/ns/codelocations/cl"
JOBS_PATH = "/apis/batch/v1/namespaces/ns/jobs"
KUBECONFIG = """\
apiVersion: v1
kind: Config
clusters:
- name: fake
  cluster:
    server: http://127.0.0.1:{port}
contexts:
- name: fake
  context:
    cluster: fake
    user: fake
current-context: fake
users:
- name: fake
  user: {{}}
"""


class FakeKubeApi(BaseHTTPRequestHandler):
    """Serves the CodeLocation after a pause of every thread; rejects every Job."""

    requests = []

    def do_GET(self):
        gc.collect()
        time.sleep(0.2)
        path = self.path.split("?")[0]
        self.requests.append(["GET", path])
        if path != CODE_LOCATION_PATH:
            self.reply(404, {"kind": "Status", "reason": "NotFound", "code": 404})
            return
        self.reply(
            200,
            {
                "apiVersion": "rivers.io/v1alpha1",
                "kind": "CodeLocation",
                "metadata": {"name": "cl", "namespace": "ns"},
                "spec": {
                    "image": "img",
                    "env": [{"name": "FROM_CODE_LOCATION", "value": "1"}],
                },
            },
        )

    def do_POST(self):
        job = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        env = {
            e["name"]: e.get("value")
            for c in job["spec"]["template"]["spec"]["containers"]
            for e in c.get("env", [])
        }
        self.requests.append(
            ["POST", self.path.split("?")[0], env.get("FROM_CODE_LOCATION")]
        )
        self.reply(422, {"kind": "Status", "reason": "Invalid", "code": 422})

    def reply(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


def use_kubeconfig(path):
    for name in ("KUBERNETES_SERVICE_HOST", "HTTPS_PROXY", "https_proxy"):
        os.environ.pop(name, None)
    os.environ.update(
        KUBECONFIG=str(path), RIVERS_CODE_LOCATION_NAME="cl", RIVERS_NAMESPACE="ns"
    )


def use_fake_api():
    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeKubeApi)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    kubeconfig = Path("kubeconfig").resolve()
    kubeconfig.write_text(KUBECONFIG.format(port=server.server_port))
    use_kubeconfig(kubeconfig)


def kubernetes_repo():
    @rs.Asset(io_handler=rs.PickleIOHandler(store=obstore.store.MemoryStore()))
    def a() -> int:
        return 1

    return rs.CodeRepository(
        assets=[a],
        default_executor=rs.Executor.kubernetes("img:latest", namespace="ns"),
    )


def first_runs_in_threads():
    """Child process of ``test_first_runs_in_threads_share_one_connection``."""
    use_fake_api()
    repo = kubernetes_repo()
    repo.resolve()
    _, errors = run_threads(lambda i: repo.materialize(["a"]), n=N_RUNS)
    print(
        json.dumps(
            {
                "errors": errors,
                "requests": sorted(FakeKubeApi.requests),
                "statuses": [r.status for r in repo.storage.get_runs()],
            }
        )
    )


def test_first_runs_in_threads_share_one_connection(tmp_path):
    """Runs that start together read the CodeLocation once, while the API
    server pauses every thread, and give each step Job its env."""
    proc = run_in_child(
        "concurrency.test_thread_safety_kubernetes:first_runs_in_threads",
        cwd=tmp_path,
    )

    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout.splitlines()[-1]) == {
        "errors": ["ExecutionError: K8s step pod failed for 'a'"] * N_RUNS,
        "requests": [["GET", CODE_LOCATION_PATH]] + [["POST", JOBS_PATH, "1"]] * N_RUNS,
        "statuses": ["Failure"] * N_RUNS,
    }


def run_that_cannot_connect(missing):
    """Child process of ``test_run_that_cannot_connect_raises``."""
    if missing == "kubeconfig":
        use_kubeconfig(Path("missing-kubeconfig").resolve())
    else:
        use_fake_api()
        if missing == "code_location_name":
            del os.environ["RIVERS_CODE_LOCATION_NAME"]
        else:
            os.environ["RIVERS_CODE_LOCATION_NAME"] = "missing"
    repo = kubernetes_repo()
    error = None
    try:
        repo.materialize(["a"])
    except Exception as e:
        error = f"{type(e).__name__}: {e}"
    print(
        json.dumps(
            {"error": error, "statuses": [r.status for r in repo.storage.get_runs()]}
        )
    )


CONNECT_ERRORS = {
    "kubeconfig": (
        "failed to construct in-cluster kube client: ",
        "missing-kubeconfig",
    ),
    "code_location_name": (
        "RIVERS_CODE_LOCATION_NAME is required for KubernetesBackend; ",
        "",
    ),
    "code_location": (
        "failed to fetch CodeLocation 'missing' in namespace 'ns': ",
        "NotFound",
    ),
}


@pytest.mark.parametrize("missing", CONNECT_ERRORS)
def test_run_that_cannot_connect_raises(tmp_path, missing):
    """A run that cannot read its CodeLocation raises an ordinary exception
    and ends as Failure, instead of a panic that leaves the run Started."""
    error, cause = CONNECT_ERRORS[missing]
    proc = run_in_child(
        "concurrency.test_thread_safety_kubernetes:run_that_cannot_connect",
        missing,
        cwd=tmp_path,
    )

    assert proc.returncode == 0, proc.stderr
    result = json.loads(proc.stdout.splitlines()[-1])
    assert result["error"].startswith(f"ExecutionError: {error}")
    assert cause in result["error"]
    assert result["statuses"] == ["Failure"]
