"""Airbyte integration: run Airbyte syncs as rivers assets.

The integration calls the Airbyte API with the Python standard library, so it
needs no extra.

Example::

    import rivers as rs
    from rivers.integrations.airbyte import AirbyteResource

    airbyte = AirbyteResource()  # AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET

    # One API call when the code location loads.
    postgres = airbyte.get_connection("9924bcd0-99be-453d-ba47-c2c9766f7da5")

    repo = rs.CodeRepository(assets=[airbyte.connection_asset(postgres)])
"""

import base64
import json
import logging
import re
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from http.client import HTTPException
from typing import Any, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    PrivateAttr,
    SecretStr,
    ValidationError,
    model_validator,
)
from pydantic.alias_generators import to_camel
from pydantic_settings import SettingsConfigDict

from rivers._core import Materialization, MetadataValue
from rivers._core.assets import Asset, AssetDef, AssetExecutionContext, MultiAsset
from rivers.resource import Resource

__all__ = [
    "AirbyteConnection",
    "AirbyteError",
    "AirbyteJob",
    "AirbyteResource",
    "AirbyteStream",
]

_log = logging.getLogger(__name__)

# Job statuses that can still change.
_RUNNING = frozenset({"pending", "queued", "running", "incomplete"})

# Seconds before expiry to fetch a new token. Dagster fetches a new
# 3-minute token after 150 s.
_TOKEN_MARGIN = 30.0


class AirbyteError(Exception):
    """An Airbyte API request failed, or a sync job did not succeed."""


class _ApiModel(BaseModel):
    """Reads the API's camelCase keys and the snake_case field names."""

    model_config = ConfigDict(alias_generator=to_camel, validate_by_name=True)


class AirbyteStream(_ApiModel):
    """An enabled stream of an Airbyte connection.

    Args:
        name: Stream name in the source, e.g. ``"users"``.
        namespace: Namespace in the source, e.g. a Postgres schema.
        sync_mode: Airbyte sync mode, e.g. ``"incremental_append"``.
    """

    name: str
    namespace: str | None = None
    sync_mode: str | None = None


class AirbyteConnection(_ApiModel):
    """An Airbyte connection and its enabled streams.

    Args:
        connection_id: Connection id, from the connection's URL in the Airbyte UI.
        name: Connection name.
        prefix: Prefix the connection adds to each table name in the destination.
        streams: Enabled streams.
    """

    connection_id: str
    name: str
    prefix: str | None = None
    streams: list[AirbyteStream] = []


class AirbyteJob(_ApiModel):
    """An Airbyte job.

    Args:
        job_id: Job id.
        status: ``"pending"``, ``"queued"``, ``"running"``, ``"incomplete"``,
            ``"succeeded"``, ``"failed"``, or ``"cancelled"``.
        rows_synced: Records the job committed, over all streams.
        bytes_synced: Bytes the job committed, over all streams.
    """

    job_id: int
    status: str
    rows_synced: int | None = None
    bytes_synced: int | None = None


class _StreamStats(_ApiModel):
    stream_name: str
    stream_namespace: str | None = None
    records_committed: int
    bytes_committed: int


class _JobStats(_ApiModel):
    stream_aggregated_stats: list[_StreamStats] = []


class _Token(BaseModel):
    access_token: str
    expires_in: float


class AirbyteResource(Resource):
    """Calls the Airbyte API of Airbyte Cloud or of a self-managed Airbyte.

    Authenticates with an application's client id and secret, with a username
    and password, or not at all. An access token is fetched with the client
    credentials and fetched again 30 s before it expires.

    Requests that fail with a connection error, HTTP 429, or HTTP 5xx are
    retried; other HTTP errors raise :class:`AirbyteError` at once.

    Fields are read from ``AIRBYTE_*`` environment variables when not given.

    Args:
        api_url: Base URL of the Airbyte API. For self-managed Airbyte,
            ``"<host>/api/public/v1"``.
        configuration_api_url: Base URL of Airbyte's Configuration API, which
            has the per-stream counts of a job. For self-managed Airbyte,
            ``"<host>/api/v1"``.
        client_id: Client id of an Airbyte application.
        client_secret: Client secret of an Airbyte application.
        username: Username, for self-managed Airbyte with basic authentication.
        password: Password, for self-managed Airbyte with basic authentication.
        request_timeout: Seconds to wait for each API response.
        request_max_retries: Retries of a failed request.
        request_retry_delay: Seconds between retries.
        poll_interval: Seconds between status checks of a running job.
        poll_timeout: Seconds to wait for a job to finish. ``None`` waits until
            it finishes.
        cancel_on_interrupt: Cancel the job when the wait stops before the job
            finishes: after ``poll_timeout``, on an error, or on an interrupt
            such as Ctrl-C.

    Example::

        airbyte = AirbyteResource(
            api_url="http://localhost:8000/api/public/v1",
            configuration_api_url="http://localhost:8000/api/v1",
            client_id="...",
            client_secret="...",
        )
    """

    model_config = SettingsConfigDict(env_prefix="AIRBYTE_")

    api_url: str = "https://api.airbyte.com/v1"
    configuration_api_url: str = "https://cloud.airbyte.com/api/v1"
    client_id: str | None = None
    client_secret: SecretStr | None = None
    username: str | None = None
    password: SecretStr | None = None
    request_timeout: float = 15
    request_max_retries: int = 3
    request_retry_delay: float = 0.25
    poll_interval: float = 10
    poll_timeout: float | None = None
    cancel_on_interrupt: bool = True

    _token: str = PrivateAttr(default="")
    _token_expiry: float = PrivateAttr(default=0.0)

    @model_validator(mode="after")
    def _check_credentials(self) -> Self:
        if (self.client_id is None) != (self.client_secret is None):
            raise ValueError("set both client_id and client_secret, or neither")
        if (self.username is None) != (self.password is None):
            raise ValueError("set both username and password, or neither")
        if self.client_id is not None and self.username is not None:
            raise ValueError(
                "set client_id and client_secret, or username and password, not both"
            )
        return self

    def get_connection(self, connection_id: str) -> AirbyteConnection:
        """Fetch a connection and its enabled streams.

        Raises:
            AirbyteError: The request failed.
        """
        data = self._request("GET", _join(self.api_url, f"connections/{connection_id}"))
        streams = (data.get("configurations") or {}).get("streams") or []
        return AirbyteConnection.model_validate({**data, "streams": streams})

    def sync(
        self, connection_id: str, *, log: logging.Logger | None = None
    ) -> AirbyteJob:
        """Start a sync job of a connection and wait until it finishes.

        Checks the job every ``poll_interval`` seconds. When the wait stops
        before the job finishes, the job is cancelled if ``cancel_on_interrupt``
        is set.

        Args:
            connection_id: The connection to sync.
            log: Logger for the job's progress, e.g. ``context.log``.

        Returns:
            The succeeded job.

        Raises:
            AirbyteError: A request failed, the job did not succeed, or
                ``poll_timeout`` passed.
        """
        log = log or _log
        job = self._job(
            "POST", "jobs", {"connectionId": connection_id, "jobType": "sync"}
        )
        log.info("Airbyte job %d started for connection %s", job.job_id, connection_id)

        started = time.monotonic()
        try:
            while job.status in _RUNNING:
                if (
                    self.poll_timeout is not None
                    and time.monotonic() - started > self.poll_timeout
                ):
                    raise AirbyteError(
                        f"Airbyte job {job.job_id} did not finish "
                        f"within {self.poll_timeout} s"
                    )
                time.sleep(self.poll_interval)
                previous = job.status
                job = self._job("GET", f"jobs/{job.job_id}")
                if job.status != previous:
                    log.info("Airbyte job %d: %s", job.job_id, job.status)
        except BaseException:
            if self.cancel_on_interrupt and job.status in _RUNNING:
                self._cancel(job.job_id, log)
            raise

        if job.status != "succeeded":
            raise AirbyteError(
                f"Airbyte job {job.job_id} of connection {connection_id} "
                f"ended with status {job.status!r}"
            )
        log.info(
            "Airbyte job %d committed %s records, %s bytes",
            job.job_id,
            job.rows_synced,
            job.bytes_synced,
        )
        return job

    def connection_asset(
        self,
        connection: AirbyteConnection,
        *,
        name: str | None = None,
        asset_def_fn: Callable[[AirbyteConnection, AirbyteStream], AssetDef]
        | None = None,
        **asset_options: Any,
    ) -> MultiAsset:
        """A multi-asset that syncs a connection, with one output per enabled stream.

        A run starts one sync job for the whole connection and waits for it
        (see :meth:`sync`). Then each selected output records a
        materialization with the job id and the stream's committed records
        and bytes. A selected output whose stream the connection no longer
        syncs fails with :class:`AirbyteError`.

        Args:
            connection: From :meth:`get_connection`, or built by hand.
            name: Asset name. Defaults to ``"airbyte_"`` and the connection
                name in snake case.
            asset_def_fn: Builds the output of a stream. The default names it
                like the destination table: the connection's prefix and the
                stream name.
            **asset_options: Passed to ``Asset.from_multi``, e.g. ``group``,
                ``hooks``, ``automation_condition``, or ``retry``. ``kinds``
                defaults to ``["airbyte"]``.

        Returns:
            A ``MultiAsset`` to pass to ``CodeRepository(assets=[...])``.

        Raises:
            ValueError: The connection has no enabled streams, or two streams
                have the same output name.
        """
        make_def = asset_def_fn or _table_asset_def
        streams: dict[str | None, AirbyteStream] = {}
        output_defs = []
        for stream in connection.streams:
            output_def = make_def(connection, stream)
            if output_def.name in streams:
                raise ValueError(
                    f"Airbyte streams {_label(streams[output_def.name])!r} and "
                    f"{_label(stream)!r} both have output name {output_def.name!r}; "
                    "pass an asset_def_fn that names them apart"
                )
            streams[output_def.name] = stream
            output_defs.append(output_def)

        if not output_defs:
            raise ValueError(
                f"Airbyte connection {connection.name!r} has no enabled streams"
            )

        def sync_streams(context: AssetExecutionContext) -> Iterator[Materialization]:
            yield from self._materialize(connection.connection_id, streams, context)

        asset_options.setdefault("kinds", ["airbyte"])
        return Asset.from_multi(
            sync_streams,
            output_defs=output_defs,
            name=name or _asset_name(connection.name),
            **asset_options,
        )

    def _materialize(
        self,
        connection_id: str,
        streams: dict[str | None, AirbyteStream],
        context: AssetExecutionContext,
    ) -> Iterator[Materialization]:
        enabled = self._enabled_streams(connection_id, streams, context.log)

        job = self.sync(connection_id, log=context.log)
        stats = self._stream_stats(job.job_id, context.log)

        gone = []
        for output_name in context.output_selection:
            stream = streams[output_name]
            if _key(stream) not in enabled:
                gone.append(output_name)
                continue

            metadata: dict[str, Any] = {
                "airbyte/connection_id": connection_id,
                "airbyte/job_id": job.job_id,
                "airbyte/stream": _label(stream),
            }
            counts = stats.get(_key(stream))
            if counts is not None:
                metadata["airbyte/records_committed"] = counts.records_committed
                metadata["airbyte/bytes_committed"] = MetadataValue.bytes(
                    counts.bytes_committed
                )
            yield Materialization(output_name=output_name, metadata=metadata)

        if gone:
            raise AirbyteError(
                f"Airbyte connection {connection_id} no longer syncs the streams "
                f"of {', '.join(gone)}. Reload the code location."
            )

    def _enabled_streams(
        self,
        connection_id: str,
        streams: dict[str | None, AirbyteStream],
        log: logging.Logger,
    ) -> set[tuple[str | None, str]]:
        """Keys of the enabled streams. Warns about streams with no outputs."""
        current = self.get_connection(connection_id).streams
        outputs = {_key(s) for s in streams.values()}
        new = [_label(s) for s in current if _key(s) not in outputs]
        if new:
            log.warning(
                "Airbyte connection %s also syncs %s, which have no outputs. "
                "Reload the code location to add them.",
                connection_id,
                ", ".join(new),
            )

        return {_key(s) for s in current}

    def _stream_stats(
        self, job_id: int, log: logging.Logger
    ) -> dict[tuple[str | None, str], _StreamStats]:
        """Per-stream counts. The Airbyte API has only job totals."""
        try:
            data = self._request(
                "POST",
                _join(self.configuration_api_url, "jobs/get_without_logs"),
                {"id": job_id},
            )
            stats = _JobStats.model_validate(data.get("job") or {})
        except (AirbyteError, ValidationError) as e:
            log.warning("No per-stream counts for Airbyte job %d: %s", job_id, e)
            return {}

        return {
            (s.stream_namespace, s.stream_name): s
            for s in stats.stream_aggregated_stats
        }

    def _job(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> AirbyteJob:
        return AirbyteJob.model_validate(
            self._request(method, _join(self.api_url, path), body)
        )

    def _cancel(self, job_id: int, log: logging.Logger) -> None:
        try:
            self._request("DELETE", _join(self.api_url, f"jobs/{job_id}"))
        except AirbyteError as e:
            log.warning("Could not cancel Airbyte job %d: %s", job_id, e)
        else:
            log.info("Cancelled Airbyte job %d", job_id)

    def _request(
        self,
        method: str,
        url: str,
        body: dict[str, Any] | None = None,
        *,
        auth: bool = True,
    ) -> Any:
        data = None if body is None else json.dumps(body).encode()
        retries = 0
        while True:
            headers = {"Accept": "application/json", "User-Agent": "rivers"}
            if data is not None:
                headers["Content-Type"] = "application/json"
            if auth:
                headers.update(self._auth_headers())

            request = urllib.request.Request(
                url, data=data, headers=headers, method=method
            )
            try:
                with urllib.request.urlopen(
                    request, timeout=self.request_timeout
                ) as response:
                    payload = response.read()
                return _parse_json(method, url, payload)
            except urllib.error.HTTPError as e:
                message = (
                    f"{method} {url}: HTTP {e.code} {e.read().decode(errors='replace')}"
                )
                if e.code < 500 and e.code != 429:
                    raise AirbyteError(message) from None
            except (OSError, HTTPException) as e:
                message = f"{method} {url}: {e}"

            if retries == self.request_max_retries:
                raise AirbyteError(message)
            retries += 1
            time.sleep(self.request_retry_delay)

    def _auth_headers(self) -> dict[str, str]:
        if self.client_id is not None and self.client_secret is not None:
            # Wall-clock time: the parallel executor pickles the token into
            # worker processes.
            if time.time() >= self._token_expiry:
                token = _Token.model_validate(
                    self._request(
                        "POST",
                        _join(self.api_url, "applications/token"),
                        {
                            "client_id": self.client_id,
                            "client_secret": self.client_secret.get_secret_value(),
                            "grant-type": "client_credentials",
                        },
                        auth=False,
                    )
                )
                self._token = token.access_token
                self._token_expiry = time.time() + token.expires_in - _TOKEN_MARGIN
            return {"Authorization": f"Bearer {self._token}"}

        if self.username is not None and self.password is not None:
            credentials = f"{self.username}:{self.password.get_secret_value()}"
            basic = base64.b64encode(credentials.encode()).decode()
            return {"Authorization": f"Basic {basic}"}

        return {}


def _join(base: str, path: str) -> str:
    return f"{base.rstrip('/')}/{path}"


def _parse_json(method: str, url: str, payload: bytes) -> Any:
    if not payload:
        return {}
    try:
        return json.loads(payload)
    except ValueError:
        raise AirbyteError(
            f"{method} {url}: the response is not JSON; check the URL: {payload[:200]!r}"
        ) from None


def _key(stream: AirbyteStream) -> tuple[str | None, str]:
    return stream.namespace, stream.name


def _label(stream: AirbyteStream) -> str:
    return f"{stream.namespace}.{stream.name}" if stream.namespace else stream.name


def _table_asset_def(connection: AirbyteConnection, stream: AirbyteStream) -> AssetDef:
    return AssetDef(f"{connection.prefix or ''}{stream.name}")


def _asset_name(connection_name: str) -> str:
    return "airbyte_" + re.sub(r"[^A-Za-z0-9_]+", "_", connection_name).lower()
