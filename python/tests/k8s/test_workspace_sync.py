"""``rivers-workspace-sync``, run against a local git remote and a fake ``uv``.

Shared mode keeps one uv cache on the PVC. After each successful build the
script prunes it to the wheels built from source (``uv cache prune --ci``).
Fallback pods install without a cache, so there is nothing to prune. Past
the recency and age floors, the tree prune keeps a tree only when its whole
name is in the keep-set, or when it is the pod's own tree
(``RIVERS_WORKSPACE_KEY``). A floor that is not a whole number skips the tree
prune. The prune renames a tree to ``.deleting-<key>-…`` before it deletes
it, so a prune that is killed mid-delete leaves no partial tree with
``.ready`` under the key. The next prune deletes what was left.

Deps mode ``auto`` reads the project directory (``RIVERS_GIT_PATH``): its
``uv.lock``, else its ``requirements.txt``. A member of a uv workspace has no
``uv.lock`` of its own, so ``auto`` uses the lock at the workspace root.

The url's scheme picks the git Secret's keys, as in the operator: ``ssh://``
urls use ``identity`` and ``known_hosts``, other urls ``username`` and
``password``. A key that the url needs and the pod cannot read stops the
build before the fetch.

The script fetches the commit by its SHA, so a branch that moves on does not
change the tree. When that fetch fails, it fetches the branch or tag
(``RIVERS_GIT_REF``) and verifies the commit. A pinned commit has no ref.

A failed build gives kubelet the most specific termination message there is:
the error the script found, a failed git fetch with the last lines of git's
error, or the time budget the build ran past. When ``uv`` fails, the
termination log stays empty, so kubelet reports the tail of the log, where
uv's error is (``FallbackToLogsOnError``).

Run and step pods mount a shared tree read-only, so Python cannot write
bytecode into it. The shared build compiles, with 4 workers, what these pods
import from the checkout: the project directory, and the directories that
the venv's ``.pth`` files add from the checkout (editable installs, such as
the uv workspace members that the project uses). In fallback mode, each pod
writes to its own tree, and Python writes the bytecode of what it imports,
so the build compiles nothing from the checkout.

Once the tree is ready, a pod releases the tree's build lock before it
deletes old trees or prunes the uv cache, so the pods that wait for the
same tree start without waiting for these.
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import signal
import sys
import time
from pathlib import Path
from typing import NamedTuple

import pytest

UV_PROJECT = {"uv.lock": "version = 1\n"}
UV_WORKSPACE = {
    "pyproject.toml": '[tool.uv.workspace]\nmembers = ["analytics", "libs/*"]\n',
    "uv.lock": "version = 1\n",
}
MEMBER = '[project]\nname = "analytics"\nversion = "0.1.0"\n'
PRUNE_CACHE = ["cache", "prune", "--ci"]


def _install(workspace_sync, path: str = "") -> list[str]:
    return ["sync", "--locked", "--project", str(workspace_sync.tree / "src" / path)]


def _install_requirements(workspace_sync, path: str) -> list[list[str]]:
    requirements = workspace_sync.tree / "src" / path / "requirements.txt"
    return [
        ["venv", "-q", str(workspace_sync.tree / "venv")],
        ["pip", "install", "-r", str(requirements)],
    ]


def test_shared_build_prunes_the_uv_cache(workspace_sync):
    commit = workspace_sync.commit(UV_PROJECT)

    built = workspace_sync.run(commit, shared=True)

    assert built.returncode == 0, built.stderr
    assert workspace_sync.uv_calls() == [_install(workspace_sync), PRUNE_CACHE]

    # The next pod start finds the tree ready: nothing installed, nothing pruned.
    reused = workspace_sync.run(commit, shared=True)

    assert reused.returncode == 0, reused.stderr
    assert workspace_sync.uv_calls() == [_install(workspace_sync), PRUNE_CACHE]


def test_fallback_build_does_not_prune_a_uv_cache(workspace_sync):
    commit = workspace_sync.commit(UV_PROJECT)

    built = workspace_sync.run(commit, shared=False)

    assert built.returncode == 0, built.stderr
    assert (workspace_sync.tree / ".ready").exists()
    assert workspace_sync.uv_calls() == [_install(workspace_sync)]


def test_failed_build_does_not_prune_the_uv_cache(workspace_sync):
    commit = workspace_sync.commit(UV_PROJECT)

    failed = workspace_sync.run(commit, shared=True, FAKE_UV_FAIL="sync")

    assert failed.returncode == 1
    assert "workspace build failed" in failed.stderr
    assert not (workspace_sync.tree / ".ready").exists()
    assert workspace_sync.uv_calls() == [_install(workspace_sync)]


def test_failed_uv_cache_prune_keeps_the_built_tree(workspace_sync):
    commit = workspace_sync.commit(UV_PROJECT)

    built = workspace_sync.run(commit, shared=True, FAKE_UV_FAIL="cache")

    assert built.returncode == 0, built.stderr
    assert "uv cache prune failed (non-fatal)" in built.stderr
    assert (workspace_sync.tree / ".ready").exists()
    assert workspace_sync.uv_calls() == [_install(workspace_sync), PRUNE_CACHE]


@pytest.mark.parametrize(
    ("env", "error"),
    [
        pytest.param(
            {"RIVERS_GIT_PATH": "analytics"},
            "RIVERS_GIT_PATH 'analytics' does not exist in the repository",
            id="missing-path",
        ),
        pytest.param(
            {"RIVERS_DEPS_MODE": "poetry"},
            "unknown RIVERS_DEPS_MODE 'poetry' (auto|uvSync|requirements|none)",
            id="unknown-deps-mode",
        ),
    ],
)
def test_the_error_the_build_finds_is_the_termination_message(
    workspace_sync, env, error
):
    commit = workspace_sync.commit(UV_PROJECT)

    failed = workspace_sync.run(commit, shared=False, **env)

    assert failed.returncode == 1
    assert f"workspace-sync: ERROR: {error}\n" in failed.stderr
    assert workspace_sync.termination_message() == f"{error}\n"
    assert not (workspace_sync.tree / ".ready").exists()


@pytest.mark.parametrize("shared", [True, False], ids=["shared", "fallback"])
@pytest.mark.parametrize(
    ("files", "failing"),
    [
        pytest.param(UV_PROJECT, "sync", id="uv-sync"),
        pytest.param({"requirements.txt": "pandas\n"}, "pip", id="uv-pip-install"),
    ],
)
def test_a_failed_install_leaves_the_log_tail_to_kubelet(
    workspace_sync, files, failing, shared
):
    commit = workspace_sync.commit(files)

    failed = workspace_sync.run(commit, shared=shared, FAKE_UV_FAIL=failing)

    assert failed.returncode == 1
    assert workspace_sync.termination_message() == ""
    assert failed.stderr.endswith(
        f"error: uv {failing} failed\n"
        "workspace-sync: ERROR: workspace build failed (exit 1)\n"
    )
    assert not (workspace_sync.tree / ".ready").exists()


MISSING_COMMIT = "f" * 40


def test_a_pinned_commit_the_remote_lacks_fails_with_the_git_error(workspace_sync):
    workspace_sync.commit(UV_PROJECT)

    failed = workspace_sync.run(MISSING_COMMIT, shared=False)

    message = workspace_sync.termination_message()
    assert failed.returncode == 1
    assert message.startswith(f"git fetch of commit {MISSING_COMMIT} failed: "), message
    assert f"upload-pack: not our ref {MISSING_COMMIT}\n" in message
    assert f"workspace-sync: ERROR: {message}" in failed.stderr
    assert not (workspace_sync.tree / ".ready").exists()


def test_a_url_without_a_repository_fails_with_the_git_error(workspace_sync, tmp_path):
    commit = workspace_sync.commit(UV_PROJECT)

    failed = workspace_sync.run(
        commit, shared=False, RIVERS_GIT_URL=(tmp_path / "missing").as_uri()
    )

    message = workspace_sync.termination_message()
    assert failed.returncode == 1
    assert message.startswith(f"git fetch of commit {commit} failed: "), message
    assert "does not appear to be a git repository\n" in message
    assert f"workspace-sync: ERROR: {message}" in failed.stderr


def test_when_the_ref_fails_too_its_error_is_the_termination_message(workspace_sync):
    workspace_sync.commit(UV_PROJECT)

    # The remote lacks the commit, and the branch is gone.
    failed = workspace_sync.run(
        MISSING_COMMIT, shared=False, RIVERS_GIT_REF="refs/heads/gone"
    )

    error = (
        "git fetch of refs/heads/gone failed: "
        "fatal: couldn't find remote ref refs/heads/gone"
    )
    assert failed.returncode == 1
    assert workspace_sync.termination_message() == f"{error}\n"
    # Before it fetches the ref, the log says why the fetch by commit failed.
    log = failed.stderr
    by_commit = log.index(
        f"workspace-sync: git fetch of commit {MISSING_COMMIT} failed: "
    )
    by_ref = log.index(
        "workspace-sync: fetching refs/heads/gone instead, then verifying the commit\n"
    )
    assert f"upload-pack: not our ref {MISSING_COMMIT}\n" in log[by_commit:by_ref]
    assert log[by_ref:].endswith(
        f"workspace-sync: ERROR: {error}\n"
        "workspace-sync: ERROR: workspace build failed (exit 1)\n"
    )


def test_a_commit_the_server_refuses_by_sha_is_fetched_by_its_ref(workspace_sync):
    tagged = workspace_sync.commit({**UV_PROJECT, "version.txt": "1\n"})
    workspace_sync.tag("v1")
    workspace_sync.commit({"version.txt": "2\n"})
    workspace_sync.refuse_fetch_by_commit()

    built = workspace_sync.run(tagged, shared=False, RIVERS_GIT_REF="refs/tags/v1")

    assert built.returncode == 0, built.stderr
    assert (
        f"workspace-sync: git fetch of commit {tagged} failed: "
        f"error: Server does not allow request for unadvertised object {tagged}\n"
        "workspace-sync: fetching refs/tags/v1 instead, then verifying the commit\n"
    ) in built.stderr
    assert (workspace_sync.tree / "src" / "version.txt").read_text() == "1\n"
    assert (workspace_sync.tree / ".ready").exists()


@pytest.mark.parametrize(
    "env",
    [
        pytest.param({}, id="pinned-commit"),
        pytest.param({"RIVERS_GIT_REF": "refs/heads/main"}, id="branch"),
    ],
)
def test_the_commit_is_fetched_by_its_sha_after_the_branch_moves_on(
    workspace_sync, env
):
    pinned = workspace_sync.commit({**UV_PROJECT, "version.txt": "1\n"})
    workspace_sync.commit({"version.txt": "2\n"})

    built = workspace_sync.run(pinned, shared=False, **env)

    assert built.returncode == 0, built.stderr
    assert (workspace_sync.tree / "src" / "version.txt").read_text() == "1\n"
    assert (workspace_sync.tree / ".ready").exists()
    assert workspace_sync.uv_calls() == [_install(workspace_sync)]


def test_a_build_past_its_time_budget_says_so_in_the_termination_message(
    workspace_sync,
):
    commit = workspace_sync.commit(UV_PROJECT)

    timed_out = workspace_sync.run(
        commit,
        shared=False,
        RIVERS_DEPS_TIMEOUT_SECONDS="1",
        FAKE_UV_SLEEP="30",
    )

    error = (
        "workspace build did not finish within 1s "
        "(spec.git.dependencies.timeoutSeconds)"
    )
    assert timed_out.returncode == 1
    assert workspace_sync.termination_message() == f"{error}\n"
    assert f"workspace-sync: ERROR: {error}\n" in timed_out.stderr
    assert not (workspace_sync.tree / ".ready").exists()


def test_prune_keeps_only_the_trees_whose_whole_key_is_kept(workspace_sync):
    # The same commit and runtime image, other dependency settings.
    kept = "9f3c1ab8d2e4-1a2b3c4d-37c771fd"
    stale = "9f3c1ab8d2e4-1a2b3c4d-03a30844"
    for name in (kept, stale, "cache"):
        (workspace_sync.volume / name).mkdir()
    commit = workspace_sync.commit(UV_PROJECT)

    built = workspace_sync.run(
        commit,
        shared=True,
        RIVERS_WORKSPACE_KEEP=f"tree,{kept}",
        RIVERS_WORKSPACE_KEEP_REVISIONS="0",
        RIVERS_WORKSPACE_MIN_AGE_SECONDS="0",
    )

    assert built.returncode == 0, built.stderr
    assert f"prune: removing {stale}" in built.stderr
    left = sorted(
        p.name for p in workspace_sync.volume.iterdir() if not p.name.startswith(".")
    )
    assert left == [kept, "cache", "tree"]


DAY = 24 * 3600
OLD_TREES = {"old-1": 2 * DAY, "old-2": 3 * DAY, "old-3": 4 * DAY}


def _sibling_trees(workspace_sync, ages: dict[str, int]) -> None:
    """Built trees on the volume, last modified ``age`` seconds ago."""
    now = time.time()
    for name, age in ages.items():
        tree = workspace_sync.volume / name
        (tree / "venv" / "bin").mkdir(parents=True)
        (tree / "venv" / "bin" / "rivers").touch()
        (tree / ".ready").touch()
        os.utime(tree, (now - age, now - age))


def _volume(workspace_sync) -> list[str]:
    """Every entry at the volume root, dot entries included."""
    return sorted(p.name for p in workspace_sync.volume.iterdir())


@pytest.mark.parametrize(
    ("keep_revisions", "min_age"),
    [
        pytest.param("0", "1d", id="min-age-1d"),
        pytest.param("0", "1h30m", id="min-age-1h30m"),
        pytest.param("0", "abc", id="min-age-abc"),
        pytest.param("three", "3600", id="keep-revisions-three"),
        pytest.param("2.5", "3600", id="keep-revisions-2.5"),
    ],
)
def test_prune_deletes_nothing_when_it_cannot_read_a_floor(
    workspace_sync, keep_revisions, min_age
):
    _sibling_trees(workspace_sync, OLD_TREES)
    commit = workspace_sync.commit(UV_PROJECT)

    built = workspace_sync.run(
        commit,
        shared=True,
        RIVERS_WORKSPACE_KEEP="tree",
        RIVERS_WORKSPACE_KEEP_REVISIONS=keep_revisions,
        RIVERS_WORKSPACE_MIN_AGE_SECONDS=min_age,
    )

    assert built.returncode == 0, built.stderr
    assert _volume(workspace_sync) == ["old-1", "old-2", "old-3", "tree"]
    assert (
        f"workspace-sync: prune: skipped — RIVERS_WORKSPACE_KEEP_REVISIONS="
        f"'{keep_revisions}' and RIVERS_WORKSPACE_MIN_AGE_SECONDS='{min_age}' "
        "must both be whole numbers\n"
    ) in built.stderr


@pytest.mark.parametrize(
    ("keep_revisions", "left"),
    [
        pytest.param("1", ["tree", "young"], id="age-floor-keeps-young"),
        pytest.param("3", ["old-1", "tree", "young"], id="recency-floor-keeps-old-1"),
    ],
)
def test_prune_removes_the_old_trees_past_both_floors(
    workspace_sync, keep_revisions, left
):
    _sibling_trees(workspace_sync, {"young": 60, **OLD_TREES})
    commit = workspace_sync.commit(UV_PROJECT)

    built = workspace_sync.run(
        commit,
        shared=True,
        RIVERS_WORKSPACE_KEEP="tree",
        RIVERS_WORKSPACE_KEEP_REVISIONS=keep_revisions,
        RIVERS_WORKSPACE_MIN_AGE_SECONDS="3600",
    )

    assert built.returncode == 0, built.stderr
    assert _volume(workspace_sync) == [".prune.lock", *left]
    ready = [
        name for name in left if (workspace_sync.volume / name / ".ready").exists()
    ]
    assert ready == left


@pytest.mark.parametrize(
    ("built", "min_age"),
    [
        pytest.param(True, "3600", id="ready-tree-past-both-floors"),
        pytest.param(False, "0", id="new-tree-without-an-age-floor"),
    ],
)
def test_prune_never_removes_the_tree_of_its_own_pod(workspace_sync, built, min_age):
    # A rollback in a pass where the operator could not refresh the keep-set:
    # it names only the tree that was in use before.
    trees = {"in-use": DAY, **OLD_TREES}
    if built:
        trees["tree"] = 5 * DAY
    _sibling_trees(workspace_sync, trees)
    commit = workspace_sync.commit(UV_PROJECT)

    started = workspace_sync.run(
        commit,
        shared=True,
        RIVERS_WORKSPACE_KEY="tree",
        RIVERS_WORKSPACE_KEEP="in-use",
        RIVERS_WORKSPACE_KEEP_REVISIONS="0",
        RIVERS_WORKSPACE_MIN_AGE_SECONDS=min_age,
    )

    assert started.returncode == 0, started.stderr
    assert _volume(workspace_sync) == [".prune.lock", "in-use", "tree"]
    assert (workspace_sync.tree / ".ready").exists()
    assert (workspace_sync.tree / "venv" / "bin" / "rivers").exists()


EVICTED = "4b1d2c3e4f5a-1a2b3c4d-37c771fd"
# The code location serves `tree`: every other tree older than an hour goes.
SERVING_TREE = {
    "RIVERS_WORKSPACE_KEEP": "tree",
    "RIVERS_WORKSPACE_KEEP_REVISIONS": "0",
    "RIVERS_WORKSPACE_MIN_AGE_SECONDS": "3600",
}

# `rm` stopped by SIGKILL: it deletes the `venv` directories under its
# targets, then kills the script that runs it, as the kubelet ends an init
# container. Calls whose targets hold no `venv` run the real `rm`.
_KILLED_RM = """\
import os, shutil, signal, sys
from pathlib import Path

targets = [Path(arg) for arg in sys.argv[1:] if not arg.startswith("-")]
venvs = [venv for target in targets if target.is_dir() for venv in target.rglob("venv")]
if not venvs:
    os.execv(REAL_RM, ["rm", *sys.argv[1:]])
for venv in venvs:
    shutil.rmtree(venv, ignore_errors=True)
os.kill(os.getppid(), signal.SIGKILL)
sys.exit(137)
"""


def _kill_the_prune_while_it_deletes(workspace_sync, tmp_path, commit: str) -> None:
    """Start the code-location pod; its prune is killed while it deletes EVICTED."""
    bin_dir = tmp_path / "killed-rm"
    bin_dir.mkdir()
    rm = bin_dir / "rm"
    rm.write_text(f"#!{sys.executable}\nREAL_RM = {shutil.which('rm')!r}\n{_KILLED_RM}")
    rm.chmod(0o755)

    killed = workspace_sync.run(
        commit,
        shared=True,
        PATH=f"{bin_dir}{os.pathsep}{workspace_sync.env['PATH']}",
        **SERVING_TREE,
    )

    assert killed.returncode == -signal.SIGKILL, killed.stderr


def test_a_tree_whose_prune_was_killed_is_built_again(workspace_sync, tmp_path):
    _sibling_trees(workspace_sync, {EVICTED: 2 * DAY})
    commit = workspace_sync.commit(UV_PROJECT)
    _kill_the_prune_while_it_deletes(workspace_sync, tmp_path, commit)

    # The code location is pinned back to the commit of the evicted tree.
    evicted = workspace_sync.volume / EVICTED
    rebuilt = workspace_sync.run(
        commit,
        shared=True,
        RIVERS_WORKSPACE_DIR=str(evicted),
        RIVERS_WORKSPACE_KEEP=EVICTED,
    )

    assert rebuilt.returncode == 0, rebuilt.stderr
    assert workspace_sync.uv_calls() == [
        _install(workspace_sync),
        ["sync", "--locked", "--project", str(evicted / "src")],
        PRUNE_CACHE,
    ]
    assert (evicted / "venv" / "bin" / "rivers").exists()


def test_the_next_prune_deletes_the_tree_a_killed_prune_left(workspace_sync, tmp_path):
    _sibling_trees(workspace_sync, {EVICTED: 2 * DAY})
    commit = workspace_sync.commit(UV_PROJECT)
    _kill_the_prune_while_it_deletes(workspace_sync, tmp_path, commit)

    entries = _volume(workspace_sync)
    assert entries[1:] == [".prune.lock", "tree"], entries
    assert entries[0].startswith(f".deleting-{EVICTED}-"), entries
    aside = workspace_sync.volume / entries[0]
    assert (aside / ".ready").exists()
    assert not (aside / "venv").exists()

    pruned = workspace_sync.run(commit, shared=True, **SERVING_TREE)

    assert pruned.returncode == 0, pruned.stderr
    assert _volume(workspace_sync) == [".prune.lock", "tree"]


def test_a_pod_waiting_for_the_tree_starts_while_the_builder_prunes_the_uv_cache(
    workspace_sync,
):
    commit = workspace_sync.commit(UV_PROJECT)
    builder = workspace_sync.start(commit, shared=True, FAKE_UV_HOLD="sync,cache")
    builder.wait_for("deps mode auto -> uvSync")
    waiter = workspace_sync.start(commit, shared=True)
    waiter.wait_for("waiting for the build lock")

    workspace_sync.release("sync")

    assert waiter.exit_code_within(10) == 0, waiter.log()
    assert "tree became ready while waiting — skipping build" in waiter.log()
    assert builder.process.poll() is None, builder.log()
    workspace_sync.release("cache")
    assert builder.exit_code_within(10) == 0, builder.log()
    assert workspace_sync.uv_calls() == [_install(workspace_sync), PRUNE_CACHE]


# `rm` that, before it deletes a tree that the prune set aside
# (`.deleting-…`), waits for the test to release "rm".
_HELD_RM = """\
import os, sys, time
from pathlib import Path

if any(Path(arg).name.startswith(".deleting-") for arg in sys.argv[1:]):
    released = Path(os.environ["FAKE_RELEASED"])
    while released.is_dir() and not (released / "rm").exists():
        time.sleep(0.02)
os.execv(REAL_RM, ["rm", *sys.argv[1:]])
"""


def _holding_tree_deletes(workspace_sync, tmp_path) -> str:
    """Return a ``PATH`` on which the prune's deletes wait for the release of "rm"."""
    bin_dir = tmp_path / "held-rm"
    bin_dir.mkdir()
    rm = bin_dir / "rm"
    rm.write_text(f"#!{sys.executable}\nREAL_RM = {shutil.which('rm')!r}\n{_HELD_RM}")
    rm.chmod(0o755)
    return f"{bin_dir}{os.pathsep}{workspace_sync.env['PATH']}"


# A second pod cannot show this: it could take the prune lock first, and
# then the pod under test would not delete.
@pytest.mark.parametrize(
    "built", [True, False], ids=["pod-that-built-the-tree", "pod-that-waited"]
)
def test_the_build_lock_is_free_while_a_pod_deletes_old_trees(
    workspace_sync, tmp_path, built
):
    _sibling_trees(workspace_sync, {EVICTED: 2 * DAY})
    commit = workspace_sync.commit(UV_PROJECT)
    env = {"PATH": _holding_tree_deletes(workspace_sync, tmp_path), **SERVING_TREE}
    if built:
        pod = workspace_sync.start(commit, shared=True, **env)
    else:
        # Another pod builds the tree.
        with workspace_sync.holding_build_lock():
            pod = workspace_sync.start(commit, shared=True, **env)
            pod.wait_for("waiting for the build lock")
            (workspace_sync.tree / ".ready").touch()
    pod.wait_for(f"prune: removing {EVICTED}")

    assert workspace_sync.build_lock_is_free()

    workspace_sync.release("rm")
    assert pod.exit_code_within(10) == 0, pod.log()
    assert _volume(workspace_sync) == [".prune.lock", "tree"]
    assert workspace_sync.uv_calls() == (
        [_install(workspace_sync), PRUNE_CACHE] if built else []
    )


@pytest.mark.parametrize("shared", [True, False], ids=["shared", "fallback"])
@pytest.mark.parametrize("member", ["analytics", "libs/analytics"])
def test_auto_syncs_a_uv_workspace_member_with_the_root_lock(
    workspace_sync, member, shared
):
    commit = workspace_sync.commit({**UV_WORKSPACE, f"{member}/pyproject.toml": MEMBER})

    built = workspace_sync.run(commit, shared=shared, RIVERS_GIT_PATH=member)

    assert built.returncode == 0, built.stderr
    assert "deps mode auto -> uvSync" in built.stderr
    install = _install(workspace_sync, member)
    assert workspace_sync.uv_calls() == (
        [install, PRUNE_CACHE] if shared else [install]
    )


def test_auto_syncs_a_project_with_its_own_uv_lock(workspace_sync):
    commit = workspace_sync.commit(
        {"analytics/pyproject.toml": MEMBER, "analytics/uv.lock": "version = 1\n"}
    )

    built = workspace_sync.run(commit, shared=False, RIVERS_GIT_PATH="analytics")

    assert built.returncode == 0, built.stderr
    assert "deps mode auto -> uvSync" in built.stderr
    assert workspace_sync.uv_calls() == [_install(workspace_sync, "analytics")]


def test_auto_installs_the_project_requirements_txt(workspace_sync):
    commit = workspace_sync.commit({"analytics/requirements.txt": "pandas\n"})

    built = workspace_sync.run(commit, shared=False, RIVERS_GIT_PATH="analytics")

    assert built.returncode == 0, built.stderr
    assert "deps mode auto -> requirements" in built.stderr
    assert workspace_sync.uv_calls() == _install_requirements(
        workspace_sync, "analytics"
    )


def test_auto_prefers_the_project_requirements_txt_to_the_workspace_lock(
    workspace_sync,
):
    commit = workspace_sync.commit(
        {
            **UV_WORKSPACE,
            "analytics/pyproject.toml": MEMBER,
            "analytics/requirements.txt": "pandas\n",
        }
    )

    built = workspace_sync.run(commit, shared=False, RIVERS_GIT_PATH="analytics")

    assert built.returncode == 0, built.stderr
    assert "deps mode auto -> requirements" in built.stderr
    assert workspace_sync.uv_calls() == _install_requirements(
        workspace_sync, "analytics"
    )


@pytest.mark.parametrize(
    "files",
    [
        pytest.param({"analytics/pyproject.toml": MEMBER}, id="no-lock"),
        pytest.param(
            {
                "pyproject.toml": '[project]\nname = "root"\nversion = "0.1.0"\n',
                "uv.lock": "version = 1\n",
                "analytics/pyproject.toml": MEMBER,
            },
            id="lock-of-a-project-that-is-not-a-workspace",
        ),
        pytest.param(
            {**UV_WORKSPACE, "analytics/pipeline.py": ""},
            id="directory-without-pyproject",
        ),
    ],
)
def test_auto_installs_nothing_without_a_lock_for_the_project(workspace_sync, files):
    commit = workspace_sync.commit(files)

    built = workspace_sync.run(commit, shared=False, RIVERS_GIT_PATH="analytics")

    assert built.returncode == 0, built.stderr
    assert "deps mode auto -> none" in built.stderr
    assert workspace_sync.uv_calls() == []


def test_auto_does_not_look_for_a_lock_above_the_repository(workspace_sync):
    commit = workspace_sync.commit({"analytics/pyproject.toml": MEMBER})
    workspace_sync.tree.mkdir()
    for name, content in UV_WORKSPACE.items():
        (workspace_sync.tree / name).write_text(content)

    built = workspace_sync.run(commit, shared=False, RIVERS_GIT_PATH="analytics")

    assert built.returncode == 0, built.stderr
    assert "deps mode auto -> none" in built.stderr
    assert workspace_sync.uv_calls() == []


SHARED_LIB = '[project]\nname = "shared"\nversion = "0.1.0"\n'
MODULE = "X = 1\n"


class Layout(NamedTuple):
    """A repository, and the directories of its checkout that a shared build compiles.

    Attributes:
        files: Content by path, relative to the repository root.
        path: The project directory (``RIVERS_GIT_PATH``); empty for a
            project at the repository root.
        editable: The directories that ``uv`` installs as editable.
        compiled: The directories that a shared build compiles, in order.
    """

    files: dict[str, str]
    path: str
    editable: list[str]
    compiled: list[str]


LAYOUTS = [
    pytest.param(
        Layout(
            files={**UV_PROJECT, "pipelines/assets.py": MODULE},
            path="",
            editable=[""],
            compiled=[""],
        ),
        id="project-at-the-root",
    ),
    pytest.param(
        Layout(
            files={
                "analytics/requirements.txt": "pandas\n",
                "analytics/pipelines/assets.py": MODULE,
                "billing/jobs.py": MODULE,
            },
            path="analytics",
            editable=[],
            compiled=["analytics"],
        ),
        id="project-in-a-monorepo",
    ),
    pytest.param(
        Layout(
            files={
                **UV_WORKSPACE,
                "analytics/pyproject.toml": MEMBER,
                "analytics/src/analytics/assets.py": MODULE,
                "libs/shared/pyproject.toml": SHARED_LIB,
                "libs/shared/src/shared/io.py": MODULE,
                "libs/unused/src/unused/io.py": MODULE,
            },
            path="analytics",
            editable=["analytics/src", "libs/shared/src"],
            compiled=["analytics", "libs/shared/src"],
        ),
        id="uv-workspace-member",
    ),
    pytest.param(
        # A package at the repository root, in flat layout, puts the whole
        # checkout on sys.path.
        Layout(
            files={
                "pyproject.toml": (
                    '[project]\nname = "root"\nversion = "0.1.0"\n\n'
                    '[tool.uv.workspace]\nmembers = ["analytics"]\n'
                ),
                "uv.lock": "version = 1\n",
                "root/io.py": MODULE,
                "analytics/pyproject.toml": MEMBER,
                "analytics/pipelines/assets.py": MODULE,
            },
            path="analytics",
            editable=[""],
            compiled=["analytics", ""],
        ),
        id="uv-workspace-root-package",
    ),
    pytest.param(
        Layout(
            files={
                "analytics/requirements.txt": "-e ../libs/shared\n",
                "analytics/pipelines/assets.py": MODULE,
                "libs/shared/pyproject.toml": SHARED_LIB,
                "libs/shared/src/shared/io.py": MODULE,
                "libs/unused/src/unused/io.py": MODULE,
            },
            path="analytics",
            editable=["libs/shared/src"],
            compiled=["analytics", "libs/shared/src"],
        ),
        id="editable-requirement",
    ),
]


def _build(workspace_sync, tmp_path, layout: Layout, *, shared: bool):
    """Build the layout's tree; uv also installs ``tmp_path/elsewhere`` editable."""
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / "io.py").write_text(MODULE)
    src = workspace_sync.tree / "src"
    editable = [str(src / name) for name in layout.editable] + [str(elsewhere)]
    env = {"RIVERS_GIT_PATH": layout.path} if layout.path else {}
    commit = workspace_sync.commit(layout.files)
    return workspace_sync.run(
        commit, shared=shared, FAKE_UV_EDITABLE=os.pathsep.join(editable), **env
    )


def _bytecode(path: Path) -> Path:
    return Path(importlib.util.cache_from_source(str(path)))


@pytest.mark.parametrize("layout", LAYOUTS)
def test_a_shared_tree_has_the_bytecode_of_what_runs_import_from_the_checkout(
    workspace_sync, tmp_path, layout
):
    built = _build(workspace_sync, tmp_path, layout, shared=True)

    assert built.returncode == 0, built.stderr
    src = workspace_sync.tree / "src"
    compiled = [src / name for name in layout.compiled]
    assert workspace_sync.python3_calls() == [
        ["-m", "compileall", "-q", "-j", "4", *map(str, compiled)]
    ]
    modules = [src / name for name in layout.files if name.endswith(".py")]
    assert {module: _bytecode(module).exists() for module in modules} == {
        module: any(module.is_relative_to(directory) for directory in compiled)
        for module in modules
    }
    assert not _bytecode(tmp_path / "elsewhere" / "io.py").exists()


@pytest.mark.parametrize("layout", LAYOUTS)
def test_a_fallback_tree_gets_no_bytecode_from_the_build(
    workspace_sync, tmp_path, layout
):
    built = _build(workspace_sync, tmp_path, layout, shared=False)

    assert built.returncode == 0, built.stderr
    assert workspace_sync.python3_calls() == []
    assert list((workspace_sync.tree / "src").rglob("__pycache__")) == []


SSH_URL = "ssh://git@127.0.0.1:1/acme/pipelines.git"


def _git_secret(workspace_sync, *keys: str) -> None:
    for key in keys:
        (workspace_sync.creds / key).write_text(f"{key}\n")


def test_a_non_ssh_url_ignores_the_ssh_keys_of_the_git_secret(workspace_sync):
    commit = workspace_sync.commit(UV_PROJECT)
    _git_secret(workspace_sync, "identity")

    built = workspace_sync.run(commit, shared=False)

    assert built.returncode == 0, built.stderr
    assert (workspace_sync.tree / ".ready").exists()
    assert workspace_sync.uv_calls() == [_install(workspace_sync)]


@pytest.mark.parametrize(
    ("keys", "error"),
    [
        pytest.param(
            ["username", "password"],
            "git Secret has no 'identity' — ssh:// urls need 'identity' and "
            "'known_hosts'",
            id="no-identity",
        ),
        pytest.param(
            ["identity", "username", "password"],
            "git Secret has 'identity' but no 'known_hosts' — refusing SSH without "
            "host-key pinning",
            id="no-known-hosts",
        ),
    ],
)
def test_an_ssh_url_needs_identity_and_known_hosts(workspace_sync, keys, error):
    commit = workspace_sync.commit(UV_PROJECT)
    _git_secret(workspace_sync, *keys)

    failed = workspace_sync.run(commit, shared=False, RIVERS_GIT_URL=SSH_URL)

    assert failed.returncode == 1
    assert f"workspace-sync: ERROR: {error}\n" in failed.stderr
    assert not (workspace_sync.tree / "src").exists()


@pytest.mark.parametrize(
    ("env", "keys", "unreadable"),
    [
        pytest.param({}, ["username", "password"], "password", id="password"),
        pytest.param({}, ["username", "password"], "username", id="username"),
        pytest.param(
            {"RIVERS_GIT_URL": SSH_URL},
            ["identity", "known_hosts"],
            "identity",
            id="identity",
        ),
        pytest.param(
            {"RIVERS_GIT_URL": SSH_URL},
            ["identity", "known_hosts"],
            "known_hosts",
            id="known_hosts",
        ),
    ],
)
def test_a_git_secret_key_the_pod_cannot_read_stops_the_build(
    workspace_sync, env, keys, unreadable
):
    if os.geteuid() == 0:
        pytest.skip("root reads files whatever their mode")
    commit = workspace_sync.commit(UV_PROJECT)
    _git_secret(workspace_sync, *keys)
    path = workspace_sync.creds / unreadable
    path.chmod(0)

    failed = workspace_sync.run(commit, shared=False, **env)

    error = f"cannot read the git Secret's '{unreadable}' ({path}): permission denied"
    assert failed.returncode == 1
    assert workspace_sync.termination_message() == f"{error}\n"
    assert f"workspace-sync: ERROR: {error}\n" in failed.stderr
    assert not (workspace_sync.tree / "src").exists()


def test_an_ssh_url_fetches_with_the_identity_and_known_hosts(workspace_sync, tmp_path):
    commit = workspace_sync.commit(UV_PROJECT)
    _git_secret(workspace_sync, "identity", "known_hosts", "username", "password")
    ssh_bin = tmp_path / "ssh-bin"
    ssh_bin.mkdir()
    (ssh_bin / "ssh").write_text(
        '#!/bin/sh\nprintf "%s\\n" "$@" >"$FAKE_SSH_LOG"\nexit 1\n'
    )
    (ssh_bin / "ssh").chmod(0o755)
    ssh_log = tmp_path / "ssh.log"

    workspace_sync.run(
        commit,
        shared=False,
        RIVERS_GIT_URL=SSH_URL,
        PATH=f"{ssh_bin}{os.pathsep}{workspace_sync.env['PATH']}",
        FAKE_SSH_LOG=str(ssh_log),
    )

    creds = workspace_sync.creds
    assert ssh_log.read_text().splitlines()[:8] == [
        "-i",
        f"{creds}/identity",
        "-o",
        f"UserKnownHostsFile={creds}/known_hosts",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        "IdentitiesOnly=yes",
    ]
