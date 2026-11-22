import os
from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path
from typing import ClassVar

import typer
from pydantic import BaseModel, Field
from pydantic_settings import (
    BaseSettings,
    EnvSettingsSource,
    PydanticBaseSettingsSource,
    PyprojectTomlConfigSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

from rivers._core.storage import Storage


class ModuleConfig(BaseModel):
    path: str | None = None
    repo_var: str = "repo"


class StorageConfig(BaseModel):
    path: str = ".rivers/storage/"
    endpoint: str | None = None
    url: str | None = None


class ServerConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 3000
    grpc_port: int = 3001


class DaemonConfig(BaseModel):
    no_daemon: bool = False


class SyntheticConfig(BaseModel):
    size: str | None = None


# Parameter name of a command -> (table, key) of the value it takes.
# ``[server]``, ``[daemon]`` and ``[synthetic]`` are for ``rivers dev`` only: a
# project's file also sits in the image that ``rivers serve`` runs from.
SHARED_PARAMS = {
    "module": ("module", "path"),
    "repo_var": ("module", "repo_var"),
    "storage_path": ("storage", "path"),
    "surreal_endpoint": ("storage", "endpoint"),
    "storage_url": ("storage", "url"),
}


class RiversEnvSource(EnvSettingsSource):
    def _load_env_vars(self):
        env = super()._load_env_vars()
        collisions = {
            f"{self.env_prefix}{name}".lower()
            for name in self.settings_cls.model_fields
        }
        return {k: v for k, v in env.items() if k.lower() not in collisions}


class RiversConfig(BaseSettings):
    module: ModuleConfig = Field(default_factory=ModuleConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    server: ServerConfig = Field(default_factory=ServerConfig)
    daemon: DaemonConfig = Field(default_factory=DaemonConfig)
    synthetic: SyntheticConfig = Field(default_factory=SyntheticConfig)

    config_file: ClassVar[Path | None] = None

    model_config = SettingsConfigDict(
        env_prefix="RIVERS_",
        env_nested_delimiter="_",
        env_nested_max_split=1,
        pyproject_toml_table_header=("tool", "rivers"),
        extra="ignore",
    )

    @classmethod
    def from_cli(
        cls,
        config: Path | None = None,
        module: str | None = None,
        repo_var: str | None = None,
        host: str | None = None,
        port: int | None = None,
        grpc_port: int | None = None,
        no_daemon: bool | None = None,
        synthetic: str | None = None,
    ) -> "RiversConfig":
        """Build a config from flat CLI arguments.

        Maps each provided flag onto its nested group as a partial dict so
        pydantic-settings deep-merges it over the env/TOML sources. ``None``
        means "flag not passed" and leaves the lower-precedence sources in
        charge of that key. The storage settings are not merged here:
        `decide_storage` picks the store.

        Args:
            config: Path to a ``rivers.toml`` that replaces the discovered one.

        Returns:
            RiversConfig: the merged configuration.
        """
        settings_cls = cls
        if config is not None:
            settings_cls = type(cls.__name__, (cls,), {"config_file": config})

        groups: dict[str, dict] = {
            "module": {"path": module, "repo_var": repo_var},
            "server": {"host": host, "port": port, "grpc_port": grpc_port},
            "daemon": {"no_daemon": no_daemon},
            "synthetic": {"size": synthetic},
        }
        provided = {
            name: {k: v for k, v in fields.items() if v is not None}
            for name, fields in groups.items()
        }
        return settings_cls(
            **{name: fields for name, fields in provided.items() if fields}  # type: ignore
        )

    def default_map(self, command) -> dict:
        """Build the click ``default_map`` for a command tree.

        Holds only the values that a source set, so a command keeps its own
        default for everything else.

        Args:
            command: The root click command.

        Returns:
            dict: per command, the configured value of each parameter it takes.
        """
        configured = self.model_dump(exclude_unset=True)
        values = {
            param: configured[table][key]
            for param, (table, key) in SHARED_PARAMS.items()
            if key in configured.get(table, {})
        }

        def build(cmd) -> dict:
            subcommands = getattr(cmd, "commands", None)
            if subcommands is not None:
                return {name: build(sub) for name, sub in subcommands.items()}
            return {p.name: values[p.name] for p in cmd.params if p.name in values}

        return build(command)

    def setting_names(self) -> dict[str, tuple["Source", str]]:
        """Where each value of `default_map` comes from.

        Returns:
            dict: per parameter, its source (the environment or a config
            file) and the name of the setting there.
        """
        configured = self.model_dump(exclude_unset=True)
        from_env = RiversEnvSource(type(self))()
        names = {}
        for param, (table, key) in SHARED_PARAMS.items():
            if key not in configured.get(table, {}):
                continue
            if key in from_env.get(table, {}):
                names[param] = (Source.ENVIRONMENT, f"RIVERS_{table}_{key}".upper())
            else:
                names[param] = (Source.CONFIG_FILE, f"[{table}] {key}")
        return names

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        sources: list[PydanticBaseSettingsSource] = [
            init_settings,
            RiversEnvSource(settings_cls),
        ]

        # Using _find_toml() to recursively lookup the TOML configuration files
        # within the current code location. This makes it possible to run operations
        # such as `rivers dev` from any project directory.
        explicit = cls.config_file or os.environ.get("RIVERS_CONFIG")
        if explicit:
            rivers_toml = Path(explicit)
            if not rivers_toml.is_file():
                raise FileNotFoundError(f"rivers config file not found: {rivers_toml}")
        else:
            rivers_toml = _find_toml(*RIVERS_TOML_NAMES)
        pyproject_toml = _find_toml("pyproject.toml")
        user_toml = _user_config_dir() / "rivers" / "rivers.toml"

        if rivers_toml:
            sources.append(
                TomlConfigSettingsSource(
                    settings_cls,
                    toml_file=rivers_toml,
                )
            )
        if pyproject_toml:
            sources.append(
                PyprojectTomlConfigSettingsSource(
                    settings_cls,
                    toml_file=pyproject_toml,
                )
            )
        if user_toml.is_file():
            sources.append(TomlConfigSettingsSource(settings_cls, toml_file=user_toml))

        return tuple(sources)


RIVERS_TOML_NAMES = ("rivers.toml", ".config/rivers.toml", "config/rivers.toml")

# The key of `setting_names` in the `meta` of the command's context.
SETTING_NAMES = "rivers.setting_names"
SURREAL_SCHEMES = ("ws", "wss", "http", "https")


class Source(IntEnum):
    """Where a setting comes from; a lower value wins."""

    COMMAND_LINE = 0
    ENVIRONMENT = 1
    CONFIG_FILE = 2
    DEFAULT = 3


@dataclass(frozen=True)
class StorageChoice:
    """The store a command opens. At most one field is set; none: the
    command's own default store."""

    url: str | None = None
    endpoint: str | None = None
    path: str | None = None
    memory: bool = False

    @property
    def in_memory(self) -> bool:
        return self.memory or _scheme(self.url) in ("mem", "memory")

    @property
    def kept(self) -> bool:
        """Whether the store outlives the command."""
        return not self.in_memory and bool(self.url or self.endpoint or self.path)

    @property
    def server(self) -> bool:
        """Whether the store is a server, which a deployed code location may read."""
        if self.endpoint is not None:
            return True
        return (
            self.url is not None
            and not self.in_memory
            and _scheme(self.url) != "surrealkv"
        )


@dataclass(frozen=True)
class _Setting:
    source: Source
    rank: int
    field: str
    value: str | bool
    name: str

    @property
    def location(self) -> str:
        """The store it names, in one form for each store."""
        if self.field == "memory" or (
            self.field == "url" and _scheme(str(self.value)) in ("mem", "memory")
        ):
            return "mem://"
        if self.field == "path":
            return f"surrealkv://{self.value}"
        return str(self.value)


def decide_storage(
    ctx: typer.Context,
    *,
    url: str | None = None,
    endpoint: str | None = None,
    path: str | None = None,
    memory: bool = False,
) -> StorageChoice:
    """Pick the store of the current command from its storage settings.

    The source decides first: the command line, then the environment, then a
    config file, then the default of the command. Among settings from the
    same source, the URL wins over the endpoint, and the endpoint over the
    path and ``--memory``.

    Args:
        ctx: The context of the command.
        url: The value of ``--storage-url``.
        endpoint: The value of ``--surreal-endpoint``.
        path: The value of ``--storage-path``.
        memory: The value of ``--memory``.

    Returns:
        StorageChoice: the store to open.

    Raises:
        UsageError: Two settings on the command line name different stores,
            or the endpoint is not a SurrealDB server.
    """
    candidates = [
        ("url", "storage_url", url),
        ("endpoint", "surreal_endpoint", endpoint),
        ("path", "storage_path", path),
        ("memory", "memory", memory),
    ]
    settings = []
    for rank, (field, param, value) in enumerate(candidates):
        if value:
            source, name = _source_and_name(ctx, param)
            settings.append(_Setting(source, rank, field, value, name))
    for setting in settings:
        if (
            setting.field == "endpoint"
            and _scheme(str(setting.value)) not in SURREAL_SCHEMES
        ):
            ctx.fail(
                f"{setting.name} takes a SurrealDB server (ws://, wss://, http:// or "
                f"https://), not {Storage.redact_url(str(setting.value))}; pass any "
                "other storage URL with --storage-url"
            )

    on_command_line = [s for s in settings if s.source is Source.COMMAND_LINE]
    for other in on_command_line[1:]:
        if other.location != on_command_line[0].location:
            ctx.fail(
                f"{on_command_line[0].name} and {other.name} name different stores; "
                "pass one of them"
            )

    if not settings:
        return StorageChoice()
    chosen = min(settings, key=lambda s: (s.source, s.rank))
    return StorageChoice(**{chosen.field: chosen.value})


def _source_and_name(ctx: typer.Context, param: str) -> tuple[Source, str]:
    option = next(p for p in ctx.command.params if p.name == param)
    source = ctx.get_parameter_source(param)
    match source.name if source else None:
        case "COMMANDLINE":
            return Source.COMMAND_LINE, option.opts[0]
        case "ENVIRONMENT":
            return Source.ENVIRONMENT, str(option.envvar)
        case "DEFAULT_MAP":
            names = ctx.meta.get(SETTING_NAMES, {})
            return names.get(param, (Source.CONFIG_FILE, option.opts[0]))
        case _:
            return Source.DEFAULT, option.opts[0]


def _scheme(url: str | None) -> str | None:
    if url is None or "://" not in url:
        return None
    return url.split("://", 1)[0].lower()


def _find_toml(*filenames: str, start_path: Path | None = None) -> Path | None:
    """Recursively lookup TOML-file from current terminal path.

    In each directory, ``filenames`` are tried in order; the closest directory wins.
    """
    start_path = start_path or Path.cwd()
    for directory in [start_path, *start_path.parents]:
        for filename in filenames:
            path = directory / filename
            if path.is_file():
                return path
    return None


def _user_config_dir() -> Path:
    return Path(os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config")
