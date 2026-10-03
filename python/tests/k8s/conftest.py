"""Harness for running ``deploy/docker/rivers-workspace-sync`` on the host.

The script runs the way the init container runs it, but against a git remote
in ``tmp_path`` (a ``file://`` URL) and a fake ``uv`` that records its
arguments. Each run gets a new, empty termination log, as kubelet gives each
container start one. Hosts without ``flock``, ``timeout`` or GNU ``stat``
(macOS) get stand-ins on ``PATH``; Linux CI runs the real tools. macOS runs
the script with bash 3.2, so it must not use bash-4 syntax.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

SYNC_SCRIPT = (
    Path(__file__).resolve().parents[3] / "deploy" / "docker" / "rivers-workspace-sync"
)
_GIT_IDENTITY = {
    "GIT_AUTHOR_NAME": "rivers",
    "GIT_AUTHOR_EMAIL": "rivers@example.com",
    "GIT_COMMITTER_NAME": "rivers",
    "GIT_COMMITTER_EMAIL": "rivers@example.com",
}

# Appends its argv as one JSON line to $FAKE_UV_LOG, then sleeps
# $FAKE_UV_SLEEP seconds (default 0). When its subcommand is listed in
# $FAKE_UV_FAIL (comma-separated), it prints `error: uv <subcommand> failed`
# to stderr and exits 1. `sync` and `venv` create the venv entrypoint, as the
# real installs do.
_FAKE_UV = """\
import json, os, sys, time
from pathlib import Path

args = sys.argv[1:]
with open(os.environ["FAKE_UV_LOG"], "a") as log:
    log.write(json.dumps(args) + "\\n")
time.sleep(float(os.environ.get("FAKE_UV_SLEEP", "0")))
if args[0] in os.environ.get("FAKE_UV_FAIL", "").split(","):
    sys.stderr.write(f"error: uv {args[0]} failed\\n")
    sys.exit(1)
venv = {"sync": os.environ.get("UV_PROJECT_ENVIRONMENT"), "venv": args[-1]}.get(args[0])
if venv:
    (Path(venv) / "bin").mkdir(parents=True, exist_ok=True)
    (Path(venv) / "bin" / "rivers").touch()
"""

# `flock [-n] FD`. The lock belongs to the open file, which the calling shell
# keeps open, so it outlives this process as flock(1)'s does.
_FLOCK = """\
import fcntl, sys

*opts, fd = sys.argv[1:]
try:
    fcntl.flock(int(fd), fcntl.LOCK_EX | (fcntl.LOCK_NB if "-n" in opts else 0))
except BlockingIOError:
    sys.exit(1)
"""

# `timeout DURATION COMMAND...`, as GNU timeout: past DURATION seconds it
# sends SIGTERM to the command and everything the command started, then
# exits 124.
_TIMEOUT = """\
import os, signal, subprocess, sys

duration, *command = sys.argv[1:]
child = subprocess.Popen(command, start_new_session=True)
try:
    code = child.wait(timeout=float(duration))
except subprocess.TimeoutExpired:
    os.killpg(child.pid, signal.SIGTERM)
    child.wait()
    sys.exit(124)
sys.exit(code if code >= 0 else 128 - code)
"""

# GNU `stat -c %Y PATH`: modification time in epoch seconds.
_STAT = """\
import os, sys

assert sys.argv[1:3] == ["-c", "%Y"], sys.argv
print(int(os.stat(sys.argv[3]).st_mtime))
"""


def _missing_tools() -> dict[str, str]:
    """Stand-ins for the tools this host lacks, by command name."""
    shims = {}
    if shutil.which("flock") is None:
        shims["flock"] = _FLOCK
    if shutil.which("timeout") is None:
        shims["timeout"] = _TIMEOUT
    gnu_stat = subprocess.run(["stat", "-c", "%Y", "/"], capture_output=True)
    if gnu_stat.returncode != 0:
        shims["stat"] = _STAT
    return shims


def _write_python_tool(path: Path, body: str) -> None:
    path.write_text(f"#!{sys.executable}\n{body}")
    path.chmod(0o755)


class WorkspaceSync:
    """A git remote, a workspace volume and a fake ``uv`` for the sync script.

    The remote's commits go on its branch ``main``. ``volume`` stands for the
    workspace volume: the PVC root in shared mode, the pod's emptyDir in
    fallback mode. ``tree`` is the subPath the pod mounts at ``/workspace``.

    Args:
        tmp_path: Directory that holds everything the script touches.
    """

    def __init__(self, tmp_path: Path) -> None:
        self.remote = tmp_path / "remote"
        self.volume = tmp_path / "volume"
        self.tree = self.volume / "tree"
        self.creds = tmp_path / "creds"
        self._unmounted_root = tmp_path / "unmounted"
        self._uv_log = tmp_path / "uv.log"
        self._termination_log = tmp_path / "termination-log"
        bin_dir = tmp_path / "bin"
        home = tmp_path / "home"
        for path in (self.remote, self.volume, self.creds, bin_dir, home):
            path.mkdir()
        self.env = {
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "HOME": str(home),
            "GIT_CONFIG_NOSYSTEM": "1",
            "FAKE_UV_LOG": str(self._uv_log),
        }
        _write_python_tool(bin_dir / "uv", _FAKE_UV)
        for name, body in _missing_tools().items():
            _write_python_tool(bin_dir / name, body)
        self._git("init", "-q", "-b", "main")
        self._git("config", "uploadpack.allowAnySHA1InWant", "true")

    def commit(self, files: dict[str, str]) -> str:
        """Commit files to the remote.

        Args:
            files: Content by path, relative to the repository root.

        Returns:
            The SHA of the new commit.
        """
        for name, content in files.items():
            path = self.remote / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
        self._git("add", "-A")
        self._git("commit", "-q", "-m", "commit")
        return self._git("rev-parse", "HEAD").strip()

    def tag(self, name: str) -> None:
        """Tag the last commit with an annotated tag.

        Args:
            name: The tag's name, without ``refs/tags/``.
        """
        self._git("tag", "-a", name, "-m", name)

    def refuse_fetch_by_commit(self) -> None:
        """Make the remote refuse a fetch by SHA of a commit no branch points at.

        Over protocol v2, a remote serves any commit it has, so the script's
        git speaks v0, and the remote does not allow any SHA in a want. The
        commit of an annotated tag is then served only by the tag's name.
        """
        self._git("config", "uploadpack.allowAnySHA1InWant", "false")
        self.env.update(
            {
                "GIT_CONFIG_COUNT": "1",
                "GIT_CONFIG_KEY_0": "protocol.version",
                "GIT_CONFIG_VALUE_0": "0",
            }
        )

    def run(
        self, commit: str, *, shared: bool, **env: str
    ) -> subprocess.CompletedProcess[str]:
        """Run the script as one pod's init container.

        Args:
            commit: The pinned commit to build (``RIVERS_GIT_COMMIT``).
            shared: Shared-PVC mode, where the code-location pod mounts the
                volume root at ``/workspaces``. Fallback pods do not.
            **env: Extra or overriding environment variables.

        Returns:
            The finished process; ``stderr`` holds the script's log.
        """
        self._termination_log.write_text("")
        return subprocess.run(
            ["bash", str(SYNC_SCRIPT)],
            env={
                **self.env,
                "RIVERS_GIT_URL": self.remote.as_uri(),
                "RIVERS_GIT_COMMIT": commit,
                "RIVERS_WORKSPACE_DIR": str(self.tree),
                "RIVERS_WORKSPACES_ROOT": str(
                    self.volume if shared else self._unmounted_root
                ),
                "RIVERS_GIT_CREDS_DIR": str(self.creds),
                "RIVERS_TERMINATION_LOG": str(self._termination_log),
                **env,
            },
            capture_output=True,
            text=True,
            timeout=60,
        )

    def termination_message(self) -> str:
        """Return what the last run wrote to its termination log.

        With ``terminationMessagePolicy: FallbackToLogsOnError``, kubelet
        reports this message for the init container, or, when it is empty
        and the container failed, the tail of the container's log.
        """
        return self._termination_log.read_text()

    def uv_calls(self) -> list[list[str]]:
        """Return the arguments of every ``uv`` call so far, oldest first."""
        if not self._uv_log.exists():
            return []
        return [json.loads(line) for line in self._uv_log.read_text().splitlines()]

    def _git(self, *args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(self.remote), *args],
            env={**self.env, **_GIT_IDENTITY},
            check=True,
            capture_output=True,
            text=True,
        ).stdout


@pytest.fixture
def workspace_sync(tmp_path: Path) -> WorkspaceSync:
    """A fresh remote and volume for one test."""
    if sys.platform == "win32":
        pytest.skip("rivers-workspace-sync is a bash script for Linux pods")
    return WorkspaceSync(tmp_path)
