"""Shared fixtures for the CLI test suite."""

import os

import pytest
from rivers._cli.config import _find_toml


@pytest.fixture(autouse=True)
def clear_rivers_env(monkeypatch):
    """Remove any ``RIVERS_*`` environment variables before each test.

    Ambient ``RIVERS_*`` vars set in the developer's shell or CI environment
    would silently override config values and cause spurious failures.  This
    fixture runs automatically for every test in the suite so individual tests
    never need to clean up after themselves.
    """
    for key in [k for k in os.environ if k.startswith("RIVERS_")]:
        monkeypatch.delenv(key, raising=False)


@pytest.fixture(autouse=True)
def resolved_tmp_path(tmp_path, monkeypatch):
    """Yield a fully resolved ``tmp_path`` and chdir into it.

    On macOS, ``tmp_path`` is typically under ``/var/folders/...`` which is a
    symlink to ``/private/var/folders/...``. ``Path.cwd()`` returns the
    resolved path, so ``_find_toml``'s upward walk would never match a
    ``tmp_path`` that still contains the unresolved symlink. Resolving once
    here keeps every test consistent without per-test boilerplate.
    """
    resolved = tmp_path.resolve()
    monkeypatch.chdir(resolved)

    # Initialize an empty pyproject.toml to ignore warning:
    # UserWarning: Config key `pyproject_toml_table_header` is set
    # in model_config but will be ignored because no
    # PyprojectTomlConfigSettingsSource source is configured.
    (resolved / "pyproject.toml").write_text("[tool]\n")

    original_find_toml = _find_toml
    monkeypatch.setattr(
        "rivers._cli.config._find_toml",
        lambda filename: original_find_toml(filename, start_path=resolved),
    )

    return resolved
