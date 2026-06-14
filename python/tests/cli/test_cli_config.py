"""CLI tests for configuration strategies."""

from pathlib import Path

from rivers._cli.config import (
    DaemonConfig,
    ModuleConfig,
    RiversConfig,
    ServerConfig,
    StorageConfig,
    SyntheticConfig,
    _find_toml,
)


def _make_rivers_toml(tmp_path: Path, **sections) -> Path:
    """Write a minimal rivers.toml under 'tmp_path' and return its path."""
    lines = []
    for section, values in sections.items():
        lines.append(f"[{section}]")
        for k, v in values.items():
            lines.append(f"{k} = {_toml_value(v)}")
        lines.append("")
    (tmp_path / "rivers.toml").write_text("\n".join(lines))
    return tmp_path / "rivers.toml"


def _make_pyproject_toml(tmp_path: Path, **sections) -> Path:
    """Write a minimal pyproject.toml under `tmp_path' and return its path."""
    lines = []
    for section, values in sections.items():
        lines.append(f"[tool.rivers.{section}]")
        for k, v in values.items():
            lines.append(f"{k} = {_toml_value(v)}")
        lines.append("")
    (tmp_path / "pyproject.toml").write_text("\n".join(lines))

    return tmp_path / "pyproject.toml"


def _toml_value(v):
    if isinstance(v, bool):
        return str(v).lower()
    if isinstance(v, str):
        return f'"{v}"'
    return str(v)


# ---------------------------------------------------------------------------
# _find_toml
# ---------------------------------------------------------------------------


class TestFindToml:
    def test_finds_file_in_start_directory(self, tmp_path):
        (tmp_path / "rivers.toml").touch()
        assert (
            _find_toml("rivers.toml", start_path=tmp_path) == tmp_path / "rivers.toml"
        )

    def test_finds_file_in_parent_directory(self, tmp_path):
        child_path = tmp_path / "a" / "b"
        child_path.mkdir(parents=True)
        (tmp_path / "rivers.toml").touch()
        assert (
            _find_toml("rivers.toml", start_path=child_path) == tmp_path / "rivers.toml"
        )

    def test_returns_none_when_not_found(self, tmp_path):
        assert _find_toml("rivers.toml", start_path=tmp_path) is None

    def test_prefers_closer_directory(self, tmp_path):
        child_path = tmp_path / "a"
        child_path.mkdir()
        (tmp_path / "rivers.toml").touch()
        (child_path / "rivers.toml").touch()
        assert (
            _find_toml("rivers.toml", start_path=child_path)
            == child_path / "rivers.toml"
        )


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------


class TestDefaultConfigValues:
    def test_default_config_values(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)  # ensure no stray TOML files are picked up
        cfg = RiversConfig()
        assert cfg.module == ModuleConfig()
        assert cfg.module.path is None
        assert cfg.module.repo_var == "repo"
        assert cfg.storage == StorageConfig()
        assert cfg.storage.path == ".rivers/storage/"
        assert cfg.storage.endpoint is None
        assert cfg.server == ServerConfig()
        assert cfg.server.host == "127.0.0.1"
        assert cfg.server.port == 3000
        assert cfg.server.grpc_port == 3001
        assert cfg.daemon == DaemonConfig()
        assert cfg.daemon.no_daemon is False
        assert cfg.synthetic == SyntheticConfig()
        assert cfg.synthetic.size is None


# ---------------------------------------------------------------------------
# Init (direct kwargs) – highest priority
# ---------------------------------------------------------------------------


class TestInitSettings:
    """Direct constructor arguments must win over every other source."""

    def test_init_override_defaults(self, tmp_path, monkeypatch):
        """A value supplied at construction time overrides the field default."""
        monkeypatch.chdir(tmp_path)
        cfg = RiversConfig(server=ServerConfig(port=9999))
        assert cfg.server.port == 9999

    def test_init_overrides_env(self, tmp_path, monkeypatch):
        """Constructur kwargs beat a matching ``RIVERS_*`` environment variable."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("RIVERS_SERVER_PORT", "8888")
        cfg = RiversConfig(server=ServerConfig(port=9999))
        assert cfg.server.port == 9999

    def test_init_overrides_rivers_toml(self, tmp_path, monkeypatch):
        """Constructor kwargs beat a value defined in ``rivers.toml``."""
        monkeypatch.chdir(tmp_path)
        _make_rivers_toml(tmp_path, server={"port": 7777})
        cfg = RiversConfig(server=ServerConfig(port=9999))
        assert cfg.server.port == 9999

    def test_init_overrides_pyproject_toml(self, tmp_path, monkeypatch):
        """Constructor kwargs beat a value defined in ``pyproject.toml``."""
        monkeypatch.chdir(tmp_path)
        _make_pyproject_toml(tmp_path, server={"port": 6666})
        cfg = RiversConfig(server=ServerConfig(port=9999))
        assert cfg.server.port == 9999


# ---------------------------------------------------------------------------
# Environment variables
# ---------------------------------------------------------------------------


class TestEnvSettings:
    """``RIVERS_<SECTION>_<FIELD>`` env vars are applied after init kwargs."""

    def test_nested_env_var_sets_server_port(self, tmp_path, monkeypatch):
        """``RIVERS_SERVER_PORT`` is mapped to ``cfg.server.port``."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("RIVERS_SERVER_PORT", "4242")
        cfg = RiversConfig()
        assert cfg.server.port == 4242

    def test_nested_env_var_sets_module_path(self, tmp_path, monkeypatch):
        """``RIVERS_MODULE_PATH`` is mapped to ``cfg.module.path``."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("RIVERS_MODULE_PATH", "my.module")
        cfg = RiversConfig()
        assert cfg.module.path == "my.module"

    def test_nested_env_var_sets_storage_endpoint(self, tmp_path, monkeypatch):
        """``RIVERS_STORAGE_ENDPOINT`` is mapped to ``cfg.storage.endpoint``."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("RIVERS_STORAGE_ENDPOINT", "http://localhost:8000")
        cfg = RiversConfig()
        assert cfg.storage.endpoint == "http://localhost:8000"

    def test_top_level_collision_env_var_is_ignored(self, tmp_path, monkeypatch):
        """Bare top-level names like ``RIVERS_MODULE`` are filtered out."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("RIVERS_MODULE", "should_be_ignored")
        cfg = RiversConfig()
        assert isinstance(cfg.module, ModuleConfig)

    def test_env_overrides_rivers_toml(self, tmp_path, monkeypatch):
        """An env var takes priority over the same field in ``rivers.toml``."""
        monkeypatch.chdir(tmp_path)
        _make_rivers_toml(tmp_path, server={"port": 5555})
        monkeypatch.setenv("RIVERS_SERVER_PORT", "6666")
        cfg = RiversConfig()
        assert cfg.server.port == 6666

    def test_env_overrides_pyproject_toml(self, tmp_path, monkeypatch):
        """An env var takes priority over the same field in ``pyproject.toml``."""
        monkeypatch.chdir(tmp_path)
        _make_pyproject_toml(tmp_path, server={"port": 4444})
        monkeypatch.setenv("RIVERS_SERVER_PORT", "6666")
        cfg = RiversConfig()
        assert cfg.server.port == 6666

    def test_unrelated_env_vars_are_ignored(self, tmp_path, monkeypatch):
        """Env vars without the ``RIVERS_`` prefix have no effect on config."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("UNRELATED_VAR", "something")
        cfg = RiversConfig()
        assert cfg.server.port == 3000


# ---------------------------------------------------------------------------
# rivers.toml
# ---------------------------------------------------------------------------


class TestRiversToml:
    """``rivers.toml`` values are applied when no higher-priority source sets a field.

    The file is discovered by walking upward from ``cwd``, so tests use
    ``monkeypatch.chdir`` to control which directory is treated as the project root.
    """

    def test_reads_module_config(self, resolved_tmp_path):
        """Module path and repo_var are loaded from ``[module]``."""
        _make_rivers_toml(
            resolved_tmp_path, module={"path": "demo.repo", "repo_var": "my_repo"}
        )
        cfg = RiversConfig()
        assert cfg.module.path == "demo.repo"
        assert cfg.module.repo_var == "my_repo"

    def test_reads_server_config(self, resolved_tmp_path):
        """Host, port, and grpc_port are loaded from ``[server]``."""
        _make_rivers_toml(
            resolved_tmp_path,
            server={"host": "0.0.0.0", "port": 8080, "grpc_port": 8081},
        )
        cfg = RiversConfig()
        assert cfg.server.host == "0.0.0.0"
        assert cfg.server.port == 8080
        assert cfg.server.grpc_port == 8081

    def test_reads_storage_config(self, resolved_tmp_path):
        """Storage path and remote endpoint are loaded from ``[storage]``."""
        _make_rivers_toml(
            resolved_tmp_path,
            storage={"path": "/data/storage", "endpoint": "http://db:8000"},
        )
        cfg = RiversConfig()
        assert cfg.storage.path == "/data/storage"
        assert cfg.storage.endpoint == "http://db:8000"

    def test_reads_daemon_no_daemon(self, resolved_tmp_path):
        """The ``no_daemon`` boolean flag is loaded from ``[daemon]``."""
        _make_rivers_toml(resolved_tmp_path, daemon={"no_daemon": True})
        cfg = RiversConfig()
        assert cfg.daemon.no_daemon is True

    def test_toml_in_parent_directory_is_discovered(self, tmp_path, monkeypatch):
        """A ``rivers.toml`` in an ancestor directory is found via upward walk."""
        subdir = tmp_path / "project" / "src"
        subdir.mkdir(parents=True)
        _make_rivers_toml(tmp_path, server={"port": 7070})
        monkeypatch.chdir(subdir)
        cfg = RiversConfig()
        assert cfg.server.port == 7070


# ---------------------------------------------------------------------------
# pyproject.toml
# ---------------------------------------------------------------------------


class TestPyprojectToml:
    """``pyproject.toml`` values are read from the ``[tool.rivers.*]`` tables.

    This source has the lowest priority and is only used when neither init
    kwargs, env vars, nor ``rivers.toml`` supply a value for a given field.
    """

    def test_reads_module_config(self, resolved_tmp_path):
        """Module path is loaded from ``[module]``."""
        _make_pyproject_toml(resolved_tmp_path, module={"path": "pkg.repo"})
        cfg = RiversConfig()
        assert cfg.module.path == "pkg.repo"

    def test_reads_server_port(self, resolved_tmp_path):
        """Server port is loaded from ``[tool.rivers.server]``."""
        _make_pyproject_toml(resolved_tmp_path, server={"port": 5050})
        cfg = RiversConfig()
        assert cfg.server.port == 5050

    def test_pyproject_in_parent_directory_is_discovered(self, tmp_path, monkeypatch):
        """A ``pyproject.toml`` in an ancestor directory is found via upward walk."""
        subdir = tmp_path / "nested"
        subdir.mkdir()
        _make_pyproject_toml(tmp_path, server={"port": 4040})
        monkeypatch.chdir(subdir)
        cfg = RiversConfig()
        assert cfg.server.port == 4040


# ---------------------------------------------------------------------------
# Source priority: rivers.toml beats pyproject.toml
# ---------------------------------------------------------------------------


class TestSourcePriority:
    """Verify the full source-priority chain end-to-end.

    Expected order (highest → lowest):

    1. Init kwargs
    2. ``RIVERS_*`` environment variables
    3. ``rivers.toml``
    4. ``pyproject.toml``
    """

    def test_rivers_toml_takes_priority_over_pyproject(self, tmp_path, monkeypatch):
        """When both TOML files exist, ``rivers.toml`` wins for the same field."""
        monkeypatch.chdir(tmp_path)
        _make_rivers_toml(tmp_path, server={"port": 1111})
        _make_pyproject_toml(tmp_path, server={"port": 2222})
        cfg = RiversConfig()
        assert cfg.server.port == 1111

    def test_partial_override_merges_correctly(self, tmp_path, monkeypatch):
        """Fields not set in ``rivers.toml`` fall through to ``pyproject.toml``.

        Pydantic-settings applies sources field-by-field, so a field absent
        from the higher-priority source should still be filled in by the next
        source in the chain rather than left at its default.
        """
        monkeypatch.chdir(tmp_path)
        _make_rivers_toml(tmp_path, server={"port": 1111})
        _make_pyproject_toml(tmp_path, server={"grpc_port": 9090})
        cfg = RiversConfig()
        assert cfg.server.port == 1111
        assert cfg.server.grpc_port == 9090
