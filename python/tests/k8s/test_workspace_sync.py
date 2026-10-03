"""``rivers-workspace-sync``, run against a local git remote and a fake ``uv``.

Shared mode keeps one uv cache on the PVC. After each successful build the
script prunes it to the wheels built from source (``uv cache prune --ci``).
Fallback pods install without a cache, so there is nothing to prune.

Deps mode ``auto`` reads the project directory (``RIVERS_GIT_PATH``): its
``uv.lock``, else its ``requirements.txt``. A member of a uv workspace has no
``uv.lock`` of its own, so ``auto`` uses the lock at the workspace root.
"""

from __future__ import annotations

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
