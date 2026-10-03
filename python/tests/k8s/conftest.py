"""Harness for running ``deploy/docker/rivers-workspace-sync`` on the host.

The script runs the way the init container runs it, but against a git remote
in ``tmp_path`` (a ``file://`` URL), a fake ``uv`` that records its
arguments, and a ``python3`` that records its arguments before it runs. Each
``WorkspaceSync.run`` gets a new, empty termination log, as kubelet gives each
container start one. Hosts without ``flock``, ``timeout`` or GNU ``stat``
(macOS) get stand-ins on ``PATH``; Linux CI runs the real tools. macOS runs
the script with bash 3.2, so it must not use bash-4 syntax.

A held step waits until the test releases it (``WorkspaceSync.release``), or
until the test ends.
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import IO

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
# $FAKE_UV_HOLD (comma-separated), it is held: it waits for the release of
# the subcommand. When its subcommand is listed in $FAKE_UV_FAIL, it prints
# `error: uv <subcommand> failed` to stderr and exits 1. `sync` and `venv`
# create the venv, with its entrypoint, as the real installs do. `sync` and
# `pip` install each directory in $FAKE_UV_EDITABLE (separated by
# os.pathsep) as editable, as uv does: a .pth file in the venv's
# site-packages holds the directory, without a trailing newline.
_FAKE_UV = """\
import json, os, sys, time
from pathlib import Path

args = sys.argv[1:]
with open(os.environ["FAKE_UV_LOG"], "a") as log:
    log.write(json.dumps(args) + "\\n")
time.sleep(float(os.environ.get("FAKE_UV_SLEEP", "0")))
if args[0] in os.environ.get("FAKE_UV_HOLD", "").split(","):
    released = Path(os.environ["FAKE_RELEASED"])
    while released.is_dir() and not (released / args[0]).exists():
        time.sleep(0.02)
if args[0] in os.environ.get("FAKE_UV_FAIL", "").split(","):
    sys.stderr.write(f"error: uv {args[0]} failed\\n")
    sys.exit(1)
venv = {
    "sync": os.environ.get("UV_PROJECT_ENVIRONMENT"),
    "venv": args[-1],
    "pip": os.environ.get("VIRTUAL_ENV"),
}.get(args[0])
if venv:
    python = f"python{sys.version_info[0]}.{sys.version_info[1]}"
    site = Path(venv, "lib", python, "site-packages")
    site.mkdir(parents=True, exist_ok=True)
    if args[0] != "pip":
        (Path(venv) / "bin").mkdir(exist_ok=True)
        (Path(venv) / "bin" / "rivers").touch()
        (site / "_virtualenv.pth").write_text("import _virtualenv\\n")
    if args[0] != "venv":
        editable = os.environ.get("FAKE_UV_EDITABLE", "").split(os.pathsep)
        for number, path in enumerate(filter(None, editable)):
            (site / f"_editable_{number}.pth").write_text(path)
"""

# Appends its argv as one JSON line to $FAKE_PYTHON3_LOG, then runs as the
# interpreter of the tests, so that the script's `python3 -m compileall`
# compiles.
_PYTHON3 = """\
import json, os, sys

with open(os.environ["FAKE_PYTHON3_LOG"], "a") as log:
    log.write(json.dumps(sys.argv[1:]) + "\\n")
os.execv(sys.executable, [sys.executable, *sys.argv[1:]])
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


def _json_lines(path: Path) -> list[list[str]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines()]


def _flock(file: IO[str], *, wait: bool) -> bool:
    """Take an exclusive ``flock(2)`` lock on ``file``; return whether it was taken."""
    import fcntl  # POSIX only; the fixture skips on Windows.

    try:
        fcntl.flock(file, fcntl.LOCK_EX | (0 if wait else fcntl.LOCK_NB))
    except BlockingIOError:
        return False
    return True


class StartedPod:
    """One run of the script that the test does not wait for.

    Args:
        process: The script, which leads its own process group.
        log: The file that gets the script's log (its stderr).
    """

    def __init__(self, process: subprocess.Popen[bytes], log: Path) -> None:
        self.process = process
        self._log = log

    def log(self) -> str:
        """Return what the script has logged so far."""
        return self._log.read_text()

    def wait_for(self, text: str) -> None:
        """Wait until the script logs ``text``.

        Args:
            text: Text that a line of the log contains.

        Raises:
            AssertionError: The script exited, or 20 seconds went by, before
                it logged ``text``.
        """
        deadline = time.monotonic() + 20
        while text not in self.log():
            exited = self.process.poll() is not None
            if (exited and text not in self.log()) or time.monotonic() > deadline:
                raise AssertionError(f"the script did not log {text!r}:\n{self.log()}")
            time.sleep(0.02)

    def exit_code_within(self, seconds: float) -> int | None:
        """Wait for the script to exit.

        Args:
            seconds: How long to wait.

        Returns:
            The exit code, or ``None`` if the script still runs after ``seconds``.
        """
        try:
            return self.process.wait(timeout=seconds)
        except subprocess.TimeoutExpired:
            return None

    def stop(self) -> None:
        """Wait up to 10 seconds for the script to end, then kill its process group."""
        if self.exit_code_within(10) is None:
            os.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait()


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
        self._tmp_path = tmp_path
        self._unmounted_root = tmp_path / "unmounted"
        self._uv_log = tmp_path / "uv.log"
        self._python3_log = tmp_path / "python3.log"
        self._termination_log = tmp_path / "termination-log"
        self._released = tmp_path / "released"
        self._started: list[StartedPod] = []
        bin_dir = tmp_path / "bin"
        home = tmp_path / "home"
        for path in (self.remote, self.volume, self.creds, bin_dir, home):
            path.mkdir()
        self._released.mkdir()
        self.env = {
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "HOME": str(home),
            "GIT_CONFIG_NOSYSTEM": "1",
            "FAKE_UV_LOG": str(self._uv_log),
            "FAKE_PYTHON3_LOG": str(self._python3_log),
            "FAKE_RELEASED": str(self._released),
        }
        _write_python_tool(bin_dir / "uv", _FAKE_UV)
        _write_python_tool(bin_dir / "python3", _PYTHON3)
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
            env=self._pod_env(commit, shared, env),
            capture_output=True,
            text=True,
            timeout=60,
        )

    def start(self, commit: str, *, shared: bool, **env: str) -> StartedPod:
        """Start the script as one pod's init container, and do not wait for it.

        Args:
            commit: The pinned commit to build (``RIVERS_GIT_COMMIT``).
            shared: Shared-PVC mode, as in :meth:`run`.
            **env: Extra or overriding environment variables.

        Returns:
            The running script. At the end of the test, every held step is
            released, and a script that does not end then is killed.
        """
        log = self._tmp_path / f"pod-{len(self._started)}.log"
        with log.open("w") as stderr:
            process = subprocess.Popen(
                ["bash", str(SYNC_SCRIPT)],
                env=self._pod_env(commit, shared, env),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=stderr,
                start_new_session=True,
            )
        pod = StartedPod(process, log)
        self._started.append(pod)
        return pod

    def release(self, step: str) -> None:
        """Let a held step go on.

        Args:
            step: The ``uv`` subcommand that ``FAKE_UV_HOLD`` holds, or the
                name of a tool that the test holds.
        """
        (self._released / step).touch()

    @contextlib.contextmanager
    def holding_build_lock(self) -> Iterator[None]:
        """Hold the tree's build lock, as a pod that builds the tree does."""
        self.tree.mkdir(parents=True, exist_ok=True)
        with (self.tree / ".lock").open("w") as lock:
            _flock(lock, wait=True)
            yield

    def build_lock_is_free(self) -> bool:
        """Return whether a pod waiting for the tree could take its build lock now."""
        with (self.tree / ".lock").open("a") as lock:
            return _flock(lock, wait=False)

    def stop(self) -> None:
        """Release every held step, then let each started script end."""
        shutil.rmtree(self._released)
        for pod in self._started:
            pod.stop()

    def termination_message(self) -> str:
        """Return what the last run wrote to its termination log.

        With ``terminationMessagePolicy: FallbackToLogsOnError``, kubelet
        reports this message for the init container, or, when it is empty
        and the container failed, the tail of the container's log.
        """
        return self._termination_log.read_text()

    def uv_calls(self) -> list[list[str]]:
        """Return the arguments of every ``uv`` call so far, oldest first."""
        return _json_lines(self._uv_log)

    def python3_calls(self) -> list[list[str]]:
        """Return the arguments of every ``python3`` call so far, oldest first."""
        return _json_lines(self._python3_log)

    def _pod_env(
        self, commit: str, shared: bool, env: dict[str, str]
    ) -> dict[str, str]:
        return {
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
        }

    def _git(self, *args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(self.remote), *args],
            env={**self.env, **_GIT_IDENTITY},
            check=True,
            capture_output=True,
            text=True,
        ).stdout


@pytest.fixture
def workspace_sync(tmp_path: Path) -> Iterator[WorkspaceSync]:
    """A fresh remote and volume for one test."""
    if sys.platform == "win32":
        pytest.skip("rivers-workspace-sync is a bash script for Linux pods")
    sync = WorkspaceSync(tmp_path)
    yield sync
    sync.stop()
