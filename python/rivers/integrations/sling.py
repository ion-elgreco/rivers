"""Sling integration: run a sling replication as a multi-asset with one output per stream.

Install with ``pip install rivers[sling]``. The ``sling`` package downloads the
sling binary on first use; set ``SLING_BINARY`` to the path of a binary to use
that one instead.

Example::

    import rivers as rs
    from rivers.integrations.sling import (
        SlingConnection,
        SlingReplication,
        SlingResource,
    )

    replication = SlingReplication("replication.yaml")

    @replication.assets(name="raw")
    def raw(context: rs.AssetExecutionContext, sling: SlingResource):
        yield from sling.replicate(context, replication)

    repo = rs.CodeRepository(
        assets=[raw],
        resources={
            "sling": SlingResource(
                connections=[
                    SlingConnection(name="PG", url="${PG_URL}"),
                    SlingConnection(name="WAREHOUSE", type="duckdb", instance="warehouse.duckdb"),
                ]
            )
        },
    )
"""

import hashlib
import json
import logging
import os
import re
import subprocess
import tempfile
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from importlib.util import find_spec
from pathlib import Path
from time import monotonic
from typing import Any

from pydantic import BaseModel, ConfigDict, SecretStr, model_validator
from pydantic_settings import SettingsConfigDict

from rivers._core import Materialization, MetadataValue
from rivers._core.assets import Asset, AssetDef, AssetExecutionContext, MultiAsset
from rivers.resource import Resource

# `import sling` downloads the binary, so it waits for the first replication.
if find_spec("sling") is None or find_spec("yaml") is None:
    raise ImportError(
        "rivers.integrations.sling needs sling: pip install rivers[sling]"
    )

__all__ = [
    "SlingConnection",
    "SlingError",
    "SlingReplication",
    "SlingResource",
    "SlingStream",
]

_ENV_REFERENCE = re.compile(r"\$\{(\w+)\}")
_STREAM_START = re.compile(
    r"^(?:\[\d+ / \d+\] running stream |Sling Replication \| [^|]* -> [^|]* \| )(.+)$"
)
_ROWS = re.compile(
    r"^(?:inserted|wrote) (\d+) rows(?: \[[^\]]*\])?(?: (?:into|to) (.+?))?(?: in \d+ secs.*)?$"
)
_LEVELS = {
    "trace": logging.DEBUG,
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warn": logging.WARNING,
    "error": logging.ERROR,
    "fatal": logging.ERROR,
    "panic": logging.ERROR,
}


class SlingError(RuntimeError):
    """Sling exited with an error. The message holds the errors sling reported."""


class SlingConnection(BaseModel):
    """A connection that sling reads from the environment variable ``name``.

    Give the connection's properties as keyword arguments (``host``, ``user``,
    ``database``, ``instance``, ...; see https://docs.slingdata.io/connections),
    or a ``url``. Give secret properties as ``SecretStr``, like ``url`` and
    ``password``. A string can also reference environment variables as
    ``"${VAR}"``, read when the replication runs. The values of secrets and of
    references are masked in the logs.

    Args:
        name: The name the replication's ``source`` or ``target`` uses.
        type: The connection type, e.g. ``"postgres"`` or ``"file"``. Sling
            reads it from ``url`` when not given.
        url: A connection URL, e.g. ``"postgresql://etl:${PG_PASSWORD}@db:5432/app"``.
        password: The password.

    Raises:
        ValueError: If neither ``type`` nor ``url`` is given.
    """

    model_config = ConfigDict(extra="allow")

    name: str
    type: str | None = None
    url: SecretStr | None = None
    password: SecretStr | None = None

    @model_validator(mode="after")
    def _check_type_or_url(self) -> "SlingConnection":
        if self.type is None and self.url is None:
            raise ValueError(f"sling connection {self.name!r} needs a type or a url")
        return self


class SlingStream(BaseModel):
    """One stream of a replication, as passed to ``asset_def_fn``.

    Args:
        name: The stream's key under ``streams``.
        config: The stream's own config (without the replication's ``defaults``).
    """

    model_config = ConfigDict(frozen=True)

    name: str
    config: dict[str, Any] = {}

    @property
    def asset_name(self) -> str:
        """The default asset name.

        The stream's ``object`` when it has no runtime variables, else its
        name, lowercased, without quotes, and with every run of characters
        other than letters, digits, ``_`` and ``.`` replaced by ``_``:
        ``public.users`` stays ``public.users``, ``file://data/orders.csv``
        becomes ``file_data_orders.csv``.
        """
        target = self.config.get("object")
        name = target if isinstance(target, str) and "{" not in target else self.name
        name = name.replace('"', "").replace("`", "")
        return re.sub(r"[^A-Za-z0-9_.]+", "_", name).lower()

    def output_def(self, **fields: Any) -> AssetDef:
        """The stream's default output, with ``fields`` set or replaced.

        Defaults: ``name`` is :attr:`asset_name`, ``kinds`` is ``"sling"``, and
        ``metadata`` is ``{"sling/stream": <stream name>}``. Other keyword
        arguments go to :class:`~rivers.AssetDef`, e.g.
        ``deps=[rs.AssetDef.dep("postgres_users")]`` or ``group="raw"``.

        Returns:
            The output definition.
        """
        defaults: dict[str, Any] = {
            "name": self.asset_name,
            "kinds": "sling",
            "metadata": {"sling/stream": self.name},
        }
        return AssetDef(**(defaults | fields))


class SlingReplication:
    """A sling replication and the rivers output of each of its streams.

    Each enabled stream becomes one output. ``asset_def_fn`` builds the output;
    when it sets no ``code_version``, the output gets a hash of the stream's
    config and the replication's other keys, so a config change marks the
    asset as changed.

    Args:
        config: A replication file (YAML or JSON), or its contents as a dict.
            See https://docs.slingdata.io/concepts/replication.
        asset_def_fn: Builds a stream's output definition. Defaults to
            :meth:`SlingStream.output_def`, which declares no deps: return
            ``stream.output_def(deps=[...])`` to link a stream to the asset
            that holds its source data.

    Attributes:
        config: The replication.
        streams: Each output's stream, by output name, in replication order.
        output_defs: The output definitions, for ``Asset.from_multi``.

    Raises:
        ValueError: If the replication has no enabled streams, an output has
            no name, or two streams get the same output name.
    """

    def __init__(
        self,
        config: str | os.PathLike[str] | Mapping[str, Any],
        asset_def_fn: Callable[[SlingStream], AssetDef] | None = None,
    ) -> None:
        if isinstance(config, Mapping):
            self.config: dict[str, Any] = dict(config)
        else:
            import yaml

            self.config = yaml.safe_load(Path(config).read_text(encoding="utf-8"))

        make_def = asset_def_fn or SlingStream.output_def
        shared = {key: value for key, value in self.config.items() if key != "streams"}
        self.streams: dict[str, SlingStream] = {}
        self.output_defs: list[AssetDef] = []
        for name, stream_config in (self.config.get("streams") or {}).items():
            stream = SlingStream(name=name, config=stream_config or {})
            if stream.config.get("disabled"):
                continue

            output = make_def(stream)
            if not output.name:
                raise ValueError(f"the output of sling stream {name!r} has no name")
            if output.name in self.streams:
                raise ValueError(
                    f"sling streams {self.streams[output.name].name!r} and {name!r} "
                    f"both map to the output {output.name!r}; "
                    "pass an asset_def_fn that names them apart"
                )
            if output.code_version is None:
                output.code_version = _digest([shared, stream.config])

            self.streams[output.name] = stream
            self.output_defs.append(output)

        if not self.streams:
            raise ValueError("the sling replication has no enabled streams")

    def assets(self, **kwargs: Any) -> Callable[[Callable[..., Any]], MultiAsset]:
        """Decorator: a multi-asset with one output per stream.

        The function runs the replication, normally with
        ``yield from sling.replicate(context, replication)``.

        Args:
            **kwargs: Passed to :meth:`~rivers.Asset.from_multi`, e.g. ``name``,
                ``partitions_def``, ``hooks``, or ``retry``.

        Returns:
            A decorator that builds the :class:`~rivers.MultiAsset`.
        """
        return Asset.from_multi(None, output_defs=self.output_defs, **kwargs)

    def _for_outputs(self, outputs: list[str]) -> dict[str, Any]:
        """The replication with every stream not in ``outputs`` disabled.

        Disabled, not removed, so sling still leaves them out of a wildcard stream.
        """
        selected = {self.streams[output].name for output in outputs}
        streams = {
            name: config if name in selected else {**(config or {}), "disabled": True}
            for name, config in self.config["streams"].items()
        }
        return self.config | {"streams": streams}


class SlingResource(Resource):
    """Runs sling replications with the connections it holds.

    Each connection is passed to sling in an environment variable named after
    it. Connections that sling finds on its own (environment variables,
    ``~/.sling/env.yaml``) work too.

    Args:
        connections: The connections the replications use. Read from the
            ``SLING_CONNECTIONS`` environment variable (a JSON list) when not given.
    """

    model_config = SettingsConfigDict(env_prefix="SLING_")

    connections: list[SlingConnection] = []

    def replicate(
        self, context: AssetExecutionContext, replication: SlingReplication
    ) -> Iterator[Materialization]:
        """Run the streams of ``context.output_selection``, one materialization per stream.

        The other streams are disabled for the run. Sling's log goes to
        ``context.log``. A stream is materialized as soon as sling reports it
        succeeded, with ``sling/stream``, ``sling/object``,
        ``sling/rows_written``, and ``sling/duration`` metadata when sling's
        log shows them. A stream sling's log does not name (such as a wildcard
        stream) is materialized when the whole replication succeeds.

        Args:
            context: The multi-asset's context.
            replication: The replication the multi-asset was built from.

        Yields:
            A :class:`~rivers.Materialization` per selected stream.

        Raises:
            SlingError: If sling exits with an error. Streams that succeeded
                before it keep their materialization.
            ValueError: If a connection references an environment variable
                that is not set.
        """
        outputs = {
            replication.streams[output].name: output
            for output in context.output_selection
        }
        env, secrets = self._environment()
        materialized: set[str] = set()
        errors: list[str] = []

        output: str | None = None
        metadata: dict[str, Any] = {}
        started = monotonic()
        with _sling_run(
            replication._for_outputs(context.output_selection), env
        ) as process:
            for line in process.stdout or ():
                level, message = _parse(line)
                if not message:
                    continue

                shown = _mask(message, secrets)
                context.log.log(level, "%s", shown)
                if level >= logging.ERROR:
                    errors.append(shown)

                if start := _STREAM_START.match(message):
                    output = outputs.get(start[1])
                    metadata = {"sling/stream": start[1]}
                    started = monotonic()
                elif rows := _ROWS.match(message):
                    metadata["sling/rows_written"] = int(rows[1])
                    if rows[2]:
                        metadata["sling/object"] = rows[2].replace('"', "")
                elif message.startswith("execution succeeded") and output:
                    if output not in materialized:
                        materialized.add(output)
                        metadata["sling/duration"] = MetadataValue.duration(
                            round(monotonic() - started, 3)
                        )
                        yield Materialization(output_name=output, metadata=metadata)
                    output = None
                elif message.startswith("execution failed"):
                    output = None

            code = process.wait()

        if code:
            raise SlingError("\n".join([f"sling exited with code {code}", *errors]))

        for output in context.output_selection:
            if output not in materialized:
                yield Materialization(
                    output_name=output,
                    metadata={"sling/stream": replication.streams[output].name},
                )

    def _environment(self) -> tuple[dict[str, str], list[str]]:
        """Sling's environment, with each connection as JSON in its own variable.

        Returns:
            The environment and the secrets in it: ``SecretStr`` values and
            the values read for ``${VAR}`` references.
        """
        env = dict(os.environ)
        secrets: list[str] = []
        for connection in self.connections:
            properties = connection.model_dump(exclude={"name"}, exclude_none=True)
            try:
                env[connection.name] = json.dumps(_expand(properties, secrets))
            except KeyError as e:
                raise ValueError(
                    f"sling connection {connection.name!r}: environment variable "
                    f"{e.args[0]} is not set"
                ) from None

        env["SLING_LOGGING"] = "JSON"
        return env, secrets


@contextmanager
def _sling_run(
    config: dict[str, Any], env: dict[str, str]
) -> Iterator[subprocess.Popen[str]]:
    """``sling run`` on ``config``, its stderr merged into stdout. Killed if the caller stops early."""
    binary = os.environ.get("SLING_BINARY")
    if not binary:
        # `import sling` also imports pandas, pyarrow and polars when they are installed.
        import sling

        binary = sling.SLING_BIN

    with tempfile.TemporaryDirectory(prefix="rivers-sling-") as tmp:
        path = Path(tmp, "replication.json")
        path.write_text(json.dumps(config, default=str), encoding="utf-8")
        with subprocess.Popen(
            [str(binary), "run", "-r", str(path)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        ) as process:
            try:
                yield process
            finally:
                process.kill()


def _expand(value: Any, secrets: list[str]) -> Any:
    """``value`` with each ``SecretStr`` revealed and each ``${VAR}`` replaced by the variable.

    Both values go to ``secrets``.

    Raises:
        KeyError: If a variable is not set.
    """
    if isinstance(value, SecretStr):
        secrets.append(_expand(value.get_secret_value(), secrets))
        return secrets[-1]
    if isinstance(value, str):

        def read(match: re.Match[str]) -> str:
            secrets.append(os.environ[match[1]])
            return secrets[-1]

        return _ENV_REFERENCE.sub(read, value)
    if isinstance(value, dict):
        return {key: _expand(item, secrets) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand(item, secrets) for item in value]
    return value


def _parse(line: str) -> tuple[int, str]:
    """A sling output line as ``(level, message)``.

    With ``SLING_LOGGING=JSON`` sling logs JSON records to stdout; the rest,
    from stderr, is errors.
    """
    try:
        record = json.loads(line)
    except ValueError:
        record = None

    if isinstance(record, dict):
        level = _LEVELS.get(record.get("lvl", ""), logging.INFO)
        return level, str(record.get("msg", "")).rstrip()
    return logging.ERROR, line.rstrip()


def _mask(message: str, secrets: list[str]) -> str:
    for secret in filter(None, secrets):
        message = message.replace(secret, "***")
    return message


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, default=str).encode()
    ).hexdigest()[:16]
