"""``rivers-workspace-sync``, run against a local git remote and a fake ``uv``.

Shared mode keeps one uv cache on the PVC. After each successful build the
script prunes it to the wheels built from source (``uv cache prune --ci``).
Fallback pods install without a cache, so there is nothing to prune.
"""

from __future__ import annotations

UV_PROJECT = {"uv.lock": "version = 1\n"}
PRUNE_CACHE = ["cache", "prune", "--ci"]


def _install(workspace_sync) -> list[str]:
    return ["sync", "--locked", "--project", str(workspace_sync.tree / "src")]


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
