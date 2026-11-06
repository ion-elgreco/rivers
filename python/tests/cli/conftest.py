"""Shared fixtures for the CLI test suite."""

import os

import pytest


@pytest.fixture(autouse=True)
def clear_rivers_env(monkeypatch):
    """Remove any ``RIVERS_*`` environment variables around each test.

    Ambient ``RIVERS_*`` vars set in the developer's shell or CI environment
    would silently override config values and cause spurious failures.  This
    fixture runs automatically for every test in the suite so individual tests
    never need to clean up after themselves.

    The sweep must also run *after* the test: commands write ``RIVERS_*`` vars
    in place (``execute`` switches the process into cloud mode), monkeypatch
    only restores keys it saw before, and a leaked ``RIVERS_DEPLOYMENT=cloud``
    makes every later ``Storage`` construction in the process panic.
    """
    for key in [k for k in os.environ if k.startswith("RIVERS_")]:
        monkeypatch.delenv(key, raising=False)
    yield
    for key in [k for k in os.environ if k.startswith("RIVERS_")]:
        os.environ.pop(key, None)


@pytest.fixture(autouse=True)
def resolved_tmp_path(tmp_path, monkeypatch):
    """Yield a fully resolved ``tmp_path`` and chdir into it."""
    resolved = tmp_path.resolve()
    monkeypatch.chdir(resolved)

    # Initialize an empty pyproject.toml to ignore warning:
    # UserWarning: Config key `pyproject_toml_table_header` is set
    # in model_config but will be ignored because no
    # PyprojectTomlConfigSettingsSource source is configured.
    (resolved / "pyproject.toml").write_text("[tool]\n")

    return resolved


@pytest.fixture(autouse=True)
def user_config_dir(tmp_path_factory, monkeypatch):
    """Point the user-level config directory away from the developer's ``~/.config``."""
    path = tmp_path_factory.mktemp("xdg")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(path))
    return path
