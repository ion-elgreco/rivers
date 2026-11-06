"""CLI tests for configuration strategies."""

from pathlib import Path

import pytest
from rivers.cli.config import (
    RIVERS_TOML_NAMES,
    DaemonConfig,
    ModuleConfig,
    RiversConfig,
    ServerConfig,
    StorageConfig,
    SyntheticConfig,
    _find_toml,
)


def _make_rivers_toml(tmp_path: Path, name: str = "rivers.toml", **sections) -> Path:
    """Write a minimal rivers.toml at 'tmp_path / name' and return its path."""
    lines = []
    for section, values in sections.items():
        lines.append(f"[{section}]")
        for k, v in values.items():
            lines.append(f"{k} = {_toml_value(v)}")
        lines.append("")
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines))
    return path


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

    @pytest.mark.parametrize("name", RIVERS_TOML_NAMES)
    def test_finds_each_rivers_toml_location(self, tmp_path, name):
        _make_rivers_toml(tmp_path, name)
        assert _find_toml(*RIVERS_TOML_NAMES, start_path=tmp_path) == tmp_path / name

    def test_names_are_tried_in_order_within_a_directory(self, tmp_path):
        for name in RIVERS_TOML_NAMES:
            _make_rivers_toml(tmp_path, name)
        found = _find_toml(*RIVERS_TOML_NAMES, start_path=tmp_path)
        assert found == tmp_path / "rivers.toml"

        (tmp_path / "rivers.toml").unlink()
        found = _find_toml(*RIVERS_TOML_NAMES, start_path=tmp_path)
        assert found == tmp_path / ".config" / "rivers.toml"

    def test_closer_config_dir_beats_parent_rivers_toml(self, tmp_path):
        child_path = tmp_path / "a"
        _make_rivers_toml(tmp_path)
        _make_rivers_toml(child_path, "config/rivers.toml")
        found = _find_toml(*RIVERS_TOML_NAMES, start_path=child_path)
        assert found == child_path / "config" / "rivers.toml"

    def test_skips_directory_with_the_file_name(self, tmp_path):
        (tmp_path / "rivers.toml").mkdir()
        assert _find_toml("rivers.toml", start_path=tmp_path) is None

    def test_defaults_to_current_working_directory(self, tmp_path, monkeypatch):
        child_path = tmp_path / "a"
        _make_rivers_toml(child_path)
        monkeypatch.chdir(child_path)
        assert _find_toml("rivers.toml") == child_path / "rivers.toml"


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

    @pytest.mark.parametrize("name", [".config/rivers.toml", "config/rivers.toml"])
    def test_config_directory_in_monorepo_root_is_discovered(
        self, tmp_path, monkeypatch, name
    ):
        """A monorepo root's ``.config/`` or ``config/`` file reaches each project."""
        project = tmp_path / "projects" / "a"
        project.mkdir(parents=True)
        _make_rivers_toml(tmp_path, name, server={"port": 7171})
        monkeypatch.chdir(project)
        cfg = RiversConfig()
        assert cfg.server.port == 7171


# ---------------------------------------------------------------------------
# User-level rivers.toml
# ---------------------------------------------------------------------------


class TestUserConfig:
    """``$XDG_CONFIG_HOME/rivers/rivers.toml`` (default ``~/.config``) supplies
    defaults below every project-level source."""

    def test_reads_user_config(self, user_config_dir):
        _make_rivers_toml(user_config_dir, "rivers/rivers.toml", server={"port": 6060})
        cfg = RiversConfig()
        assert cfg.server.port == 6060

    def test_defaults_to_home_config(self, tmp_path_factory, monkeypatch):
        home = tmp_path_factory.mktemp("home")
        monkeypatch.delenv("XDG_CONFIG_HOME")
        monkeypatch.setenv("HOME", str(home))
        monkeypatch.setenv("USERPROFILE", str(home))
        _make_rivers_toml(home, ".config/rivers/rivers.toml", server={"port": 6161})
        cfg = RiversConfig()
        assert cfg.server.port == 6161

    @pytest.mark.parametrize("project_file", ["rivers.toml", "pyproject.toml"])
    def test_project_config_beats_user_config(
        self, resolved_tmp_path, user_config_dir, project_file
    ):
        """Project files win per key; keys they leave out fall through."""
        _make_rivers_toml(
            user_config_dir,
            "rivers/rivers.toml",
            server={"port": 6060, "grpc_port": 6061},
        )
        if project_file == "rivers.toml":
            _make_rivers_toml(resolved_tmp_path, server={"port": 1111})
        else:
            _make_pyproject_toml(resolved_tmp_path, server={"port": 1111})
        cfg = RiversConfig()
        assert cfg.server.port == 1111
        assert cfg.server.grpc_port == 6061

    def test_env_beats_user_config(self, user_config_dir, monkeypatch):
        _make_rivers_toml(user_config_dir, "rivers/rivers.toml", server={"port": 6060})
        monkeypatch.setenv("RIVERS_SERVER_PORT", "6666")
        cfg = RiversConfig()
        assert cfg.server.port == 6666


# ---------------------------------------------------------------------------
# Explicit rivers.toml path (--config / RIVERS_CONFIG)
# ---------------------------------------------------------------------------


class TestExplicitConfig:
    """An explicit path replaces the discovered ``rivers.toml``; it is not
    merged with it. ``pyproject.toml`` and the user-level file still apply."""

    @pytest.fixture
    def explicit(self, tmp_path_factory):
        return _make_rivers_toml(
            tmp_path_factory.mktemp("explicit"), "custom.toml", server={"port": 3333}
        )

    @pytest.fixture(params=["from_cli", "env"])
    def load(self, request, monkeypatch):
        def load(path: Path) -> RiversConfig:
            if request.param == "from_cli":
                return RiversConfig.from_cli(config=path)
            monkeypatch.setenv("RIVERS_CONFIG", str(path))
            return RiversConfig()

        return load

    def test_replaces_discovered_rivers_toml(self, resolved_tmp_path, explicit, load):
        _make_rivers_toml(resolved_tmp_path, server={"port": 1111, "grpc_port": 2222})
        cfg = load(explicit)
        assert cfg.server.port == 3333
        assert cfg.server.grpc_port == 3001

    def test_pyproject_and_user_config_still_apply(
        self, resolved_tmp_path, user_config_dir, explicit, load
    ):
        _make_pyproject_toml(resolved_tmp_path, server={"grpc_port": 4444})
        _make_rivers_toml(user_config_dir, "rivers/rivers.toml", server={"host": "h"})
        cfg = load(explicit)
        assert cfg.server.port == 3333
        assert cfg.server.grpc_port == 4444
        assert cfg.server.host == "h"

    def test_env_var_beats_explicit_file(self, explicit, load, monkeypatch):
        monkeypatch.setenv("RIVERS_SERVER_PORT", "6666")
        assert load(explicit).server.port == 6666

    def test_missing_file_raises(self, resolved_tmp_path, load):
        with pytest.raises(FileNotFoundError, match="missing.toml"):
            load(resolved_tmp_path / "missing.toml")

    def test_from_cli_beats_env_var(self, resolved_tmp_path, explicit, monkeypatch):
        monkeypatch.setenv("RIVERS_CONFIG", str(resolved_tmp_path / "missing.toml"))
        assert RiversConfig.from_cli(config=explicit).server.port == 3333


# ---------------------------------------------------------------------------
# pyproject.toml
# ---------------------------------------------------------------------------


class TestPyprojectToml:
    """``pyproject.toml`` values are read from the ``[tool.rivers.*]`` tables.

    This source is only used when neither init kwargs, env vars, nor
    ``rivers.toml`` supply a value for a given field.
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
    3. ``rivers.toml`` (``--config``/``RIVERS_CONFIG``, else discovered)
    4. ``pyproject.toml``
    5. User-level ``rivers.toml``
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


class TestFromCli:
    """``RiversConfig.from_cli`` — the flat CLI args → nested config adapter.

    This is the seam ``rivers dev`` uses; every flat flag must land on its
    nested group, and unset flags (``None``) must leave lower-precedence
    sources (env/TOML) in charge.
    """

    def test_all_flags_map_to_nested_groups(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        cfg = RiversConfig.from_cli(
            module="a.b.c",
            repo_var="my_repo",
            host="0.0.0.0",
            port=4000,
            grpc_port=4001,
            storage_path="/tmp/store",
            surreal_endpoint="ws://db:8000",
            no_daemon=True,
            synthetic="1k",
        )
        assert cfg.module.path == "a.b.c"
        assert cfg.module.repo_var == "my_repo"
        assert cfg.server.host == "0.0.0.0"
        assert cfg.server.port == 4000
        assert cfg.server.grpc_port == 4001
        assert cfg.storage.path == "/tmp/store"
        assert cfg.storage.endpoint == "ws://db:8000"
        assert cfg.daemon.no_daemon is True
        assert cfg.synthetic.size == "1k"

    def test_omitted_flags_fall_back_to_defaults(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        cfg = RiversConfig.from_cli(module="a.b.c")
        assert cfg.module.path == "a.b.c"
        assert cfg.module.repo_var == "repo"
        assert cfg.server.host == "127.0.0.1"
        assert cfg.server.port == 3000
        assert cfg.storage.path == ".rivers/storage/"
        assert cfg.daemon.no_daemon is False
        assert cfg.synthetic.size is None

    def test_no_flags_matches_plain_construction(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        assert RiversConfig.from_cli() == RiversConfig()

    def test_flag_overrides_toml_but_sibling_keys_survive(self, tmp_path, monkeypatch):
        """A flag replaces only its own key: siblings in the same TOML group
        and untouched groups deep-merge through."""
        monkeypatch.chdir(tmp_path)
        _make_rivers_toml(
            tmp_path,
            module={"path": "toml.mod", "repo_var": "toml_repo"},
            server={"port": 5555},
        )
        cfg = RiversConfig.from_cli(module="cli.mod")
        assert cfg.module.path == "cli.mod"
        assert cfg.module.repo_var == "toml_repo"
        assert cfg.server.port == 5555

    def test_unset_bool_flag_leaves_toml_value(self, tmp_path, monkeypatch):
        """``no_daemon=None`` (flag not passed) must not clobber a TOML
        ``no_daemon = true``; an explicit ``False`` must."""
        monkeypatch.chdir(tmp_path)
        _make_rivers_toml(tmp_path, daemon={"no_daemon": True})
        assert RiversConfig.from_cli().daemon.no_daemon is True
        assert RiversConfig.from_cli(no_daemon=False).daemon.no_daemon is False
