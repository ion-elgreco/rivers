from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import (
    BaseSettings,
    EnvSettingsSource,
    PydanticBaseSettingsSource,
    PyprojectTomlConfigSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)


class ModuleConfig(BaseModel):
    path: str | None = None
    repo_var: str = "repo"


class StorageConfig(BaseModel):
    path: str = ".rivers/storage/"
    endpoint: str | None = None


class ServerConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 3000
    grpc_port: int = 3001


class DaemonConfig(BaseModel):
    no_daemon: bool = False


class SyntheticConfig(BaseModel):
    size: str | None = None


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

    model_config = SettingsConfigDict(
        env_prefix="RIVERS_",
        env_nested_delimiter="_",
        env_nested_max_split=1,
        pyproject_toml_table_header=("tool", "rivers"),
        extra="ignore",
    )

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
        rivers_toml = _find_toml("rivers.toml")
        pyproject_toml = _find_toml("pyproject.toml")

        print(f"rivers_toml={rivers_toml}")
        print(f"pyproject_toml={pyproject_toml}")

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

        return tuple(sources)


def _find_toml(filename: str, start_path: Path = Path.cwd()) -> Path | None:
    """Recursively lookup TOML-file from current terminal path."""
    for directory in [start_path, *start_path.parents]:
        path = directory / filename
        if path.exists():
            return path
    return None
