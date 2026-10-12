"""Datadog integration: a client resource with per-asset hooks and a run-failure sensor.

Install with ``pip install rivers[datadog]``.

Example::

    import rivers as rs
    from rivers.integrations.datadog import DatadogResource

    datadog = DatadogResource()  # keys from DATADOG_API_KEY and DATADOG_APP_KEY

    @rs.Asset(hooks=[datadog.failure_hook(tags=["team:data"])])
    def orders() -> int: ...

    repo = rs.CodeRepository(
        assets=[orders],
        sensors=[datadog.run_failure_sensor(default_status=rs.SensorStatus.Running)],
    )
"""

import functools
import os
from collections.abc import Callable
from typing import Any

from pydantic import SecretStr
from pydantic_settings import SettingsConfigDict

from rivers._core.hooks import Hook, HookContext
from rivers._core.sensor import RunStatusSensorContext, Sensor, SensorStatus
from rivers.resource import Resource

try:
    import requests
    from datadog import api, initialize
    from datadog.dogstatsd.base import DogStatsd
except ImportError as e:
    raise ImportError(
        "rivers.integrations.datadog needs datadog: pip install rivers[datadog]"
    ) from e

__all__ = ["DatadogClient", "DatadogResource"]


class DatadogClient(DogStatsd):
    """A ``datadog.DogStatsd`` client, with datadogpy's HTTP API client as ``api``."""

    api = api


class DatadogResource(Resource):
    """A Datadog client, and the hooks and sensor that post events through it.

    Register it as a resource to send metrics and events from assets and
    sensors, or use its methods to build alerts. Hooks get no resources, so
    each method posts through this instance.

    Metrics go to the DogStatsD server of a Datadog Agent. The hooks and the
    sensor post events to the Datadog API with ``api_key``, with a 10 s
    timeout, so they need no Agent.

    Fields are read from ``DATADOG_*`` environment variables when not given.

    Args:
        api_key: API key. The hooks and the sensor need it.
        app_key: Application key, for ``api`` calls that read data.
        api_host: The API endpoint of your Datadog site, e.g.
            ``"https://api.datadoghq.eu"``.
        statsd_host: DogStatsD host. Defaults to ``DD_AGENT_HOST``, else ``localhost``.
        statsd_port: DogStatsD port. Defaults to ``DD_DOGSTATSD_PORT``, else ``8125``.
        statsd_socket_path: DogStatsD Unix socket, e.g.
            ``"/var/run/datadog/dsd.socket"``. Used instead of the host and port.
        constant_tags: Tags added to every metric and event, e.g. ``["env:prod"]``.
    """

    model_config = SettingsConfigDict(env_prefix="DATADOG_")

    api_key: SecretStr | None = None
    app_key: SecretStr | None = None
    api_host: str = "https://api.datadoghq.com"
    statsd_host: str | None = None
    statsd_port: int | None = None
    statsd_socket_path: str | None = None
    constant_tags: list[str] = []

    def get_client(self) -> DatadogClient:
        """Return the DogStatsD client for this resource, with the HTTP API as ``api``.

        Resources with the same DogStatsD fields share one client per process.
        ``api`` is datadogpy's process-wide API client: this call points it at
        this resource's ``api_key``, ``app_key`` and ``api_host``.
        """
        initialize(
            api_key=_reveal(self.api_key),
            app_key=_reveal(self.app_key),
            api_host=self.api_host,
            hostname_from_config=False,
        )
        return _statsd_client(
            self.statsd_host,
            self.statsd_port,
            self.statsd_socket_path,
            tuple(self.constant_tags),
        )

    def failure_hook(
        self,
        *,
        tags: list[str] | None = None,
        text_fn: Callable[[HookContext], str] | None = None,
        ui_location_url: str | None = None,
        name: str = "datadog_failure_hook",
    ) -> Hook:
        """A hook that posts an ``error`` event when an asset step fails.

        The event is tagged ``asset:<asset name>`` and ``run_id:<run id>``.

        Args:
            tags: More tags for the event, e.g. ``["team:finance"]``.
            text_fn: Builds the event text (Markdown) from the hook context.
                The default names the asset and run and quotes the error.
            ui_location_url: The code location's UI URL, e.g.
                ``"https://rivers.example.com/locations/prod/etl"``. When set,
                the event links to the run.
            name: Hook name.

        Returns:
            A ``Hook.failure`` to pass to ``@Asset(hooks=[...])``.

        Raises:
            ValueError: ``api_key`` is not set.
        """
        return self._hook(
            Hook.failure,
            "failed",
            "error",
            text_fn or _asset_failure_text,
            tags,
            ui_location_url,
            name,
        )

    def success_hook(
        self,
        *,
        tags: list[str] | None = None,
        text_fn: Callable[[HookContext], str] | None = None,
        ui_location_url: str | None = None,
        name: str = "datadog_success_hook",
    ) -> Hook:
        """A hook that posts a ``success`` event when an asset step succeeds.

        The event is tagged ``asset:<asset name>`` and ``run_id:<run id>``.

        Args:
            tags: More tags for the event, e.g. ``["team:finance"]``.
            text_fn: Builds the event text (Markdown) from the hook context.
                The default names the asset and run.
            ui_location_url: The code location's UI URL. When set, the event
                links to the run.
            name: Hook name.

        Returns:
            A ``Hook.success`` to pass to ``@Asset(hooks=[...])``.

        Raises:
            ValueError: ``api_key`` is not set.
        """
        return self._hook(
            Hook.success,
            "succeeded",
            "success",
            text_fn or _asset_success_text,
            tags,
            ui_location_url,
            name,
        )

    def run_failure_sensor(
        self,
        *,
        tags: list[str] | None = None,
        text_fn: Callable[[RunStatusSensorContext], str] | None = None,
        ui_location_url: str | None = None,
        name: str = "datadog_run_failure_sensor",
        minimum_interval: str | None = None,
        monitored_jobs: list[str] | None = None,
        default_status: SensorStatus = SensorStatus.Stopped,
    ) -> Sensor:
        """A run-status sensor that posts one ``error`` event per failed run.

        The event is tagged ``run_id:<run id>``, ``job:<job name>`` and
        ``partition:<partition key>`` when the run has them, and
        ``asset:<asset name>`` for each failed step. The daemon evaluates the
        sensor only with ``default_status=SensorStatus.Running``.

        Args:
            tags: More tags for the event, e.g. ``["team:finance"]``.
            text_fn: Builds the event text (Markdown). The default names the
                run and job and lists each failed step with its error, or the
                launch error.
            ui_location_url: The code location's UI URL. When set, the event
                links to the run.
            name: Sensor name. Two Datadog sensors in one repository need
                distinct names.
            minimum_interval: Minimum interval between ticks (default ``"30s"``).
            monitored_jobs: Only report runs of these jobs. ``None`` reports
                every failed run of the code location.
            default_status: Whether the daemon evaluates the sensor.

        Returns:
            A ``Sensor`` to pass to ``CodeRepository(sensors=[...])``.

        Raises:
            ValueError: ``api_key`` is not set.
        """
        self._require_api_key()
        make_text = text_fn or _run_failure_text

        def post(context: RunStatusSensorContext) -> None:
            run = context.run
            job = f" (job {run.job_name})" if run.job_name else ""
            self._post_event(
                f"Run {run.run_id} failed{job}",
                make_text(context),
                "error",
                run.run_id,
                [*_run_tags(context), *(tags or [])],
                ui_location_url,
            )

        return Sensor.run_failure(
            name=name,
            minimum_interval=minimum_interval,
            monitored_jobs=monitored_jobs,
            default_status=default_status,
        )(post)

    def _hook(
        self,
        make_hook: Callable[..., Hook],
        outcome: str,
        alert_type: str,
        text_fn: Callable[[HookContext], str],
        tags: list[str] | None,
        ui_location_url: str | None,
        name: str,
    ) -> Hook:
        self._require_api_key()

        def post(context: HookContext) -> None:
            asset, run_id = context.asset_name, context.run_id
            self._post_event(
                f"Asset {asset} {outcome}",
                text_fn(context),
                alert_type,
                run_id,
                [f"asset:{asset}", f"run_id:{run_id}", *(tags or [])],
                ui_location_url,
            )

        return make_hook(name=name)(post)

    def _require_api_key(self) -> str:
        api_key = _reveal(self.api_key)
        if not api_key:
            raise ValueError(
                "Datadog events need an API key: set DATADOG_API_KEY or pass "
                "api_key to DatadogResource"
            )
        return api_key

    def _post_event(
        self,
        title: str,
        text: str,
        alert_type: str,
        run_id: str,
        tags: list[str],
        ui_location_url: str | None,
    ) -> None:
        if ui_location_url:
            text = f"{text}\n\n[View run]({ui_location_url.rstrip('/')}/runs/{run_id})"

        # Not datadogpy's api client: it keeps its keys in module globals and
        # returns HTTP errors instead of raising them.
        response = requests.post(
            f"{self.api_host.rstrip('/')}/api/v1/events",
            headers={"DD-API-KEY": self._require_api_key()},
            json={
                "title": title,
                "text": f"%%% \n{text}\n %%%",
                "alert_type": alert_type,
                "aggregation_key": run_id,
                "tags": [*tags, *_code_location_tags(), *self.constant_tags],
            },
            timeout=10,
        )
        response.raise_for_status()


@functools.cache
def _statsd_client(
    host: str | None,
    port: int | None,
    socket_path: str | None,
    constant_tags: tuple[str, ...],
) -> DatadogClient:
    # datadogpy before 0.53 takes no None for host and port.
    connection: dict[str, Any] = {
        "host": host,
        "port": port,
        "socket_path": socket_path,
    }
    return DatadogClient(
        constant_tags=list(constant_tags),
        **{key: value for key, value in connection.items() if value is not None},
    )


def _reveal(secret: SecretStr | None) -> str | None:
    return secret.get_secret_value() if secret is not None else None


def _code_location_tags() -> list[str]:
    # The operator sets RIVERS_CODE_LOCATION_NAME on code location and run pods.
    name = os.environ.get("RIVERS_CODE_LOCATION_NAME")
    return [f"code_location:{name}"] if name else []


def _run_tags(context: RunStatusSensorContext) -> list[str]:
    run = context.run
    tags = [f"run_id:{run.run_id}"]
    if run.job_name:
        tags.append(f"job:{run.job_name}")
    if run.partition_key is not None:
        tags.append(f"partition:{run.partition_key}")
    tags.extend(dict.fromkeys(f"asset:{f.asset_name}" for f in context.step_failures))
    return tags


def _asset_failure_text(context: HookContext) -> str:
    return (
        f"Asset `{context.asset_name}` failed in run `{context.run_id}`:\n"
        f"```\n{context.error}\n```"
    )


def _asset_success_text(context: HookContext) -> str:
    return f"Asset `{context.asset_name}` succeeded in run `{context.run_id}`."


def _run_failure_text(context: RunStatusSensorContext) -> str:
    run = context.run
    job = f" (job `{run.job_name}`)" if run.job_name else ""
    parts = [f"Run `{run.run_id}` failed{job}."]
    if context.launch_error:
        parts.append(f"Launch error:\n```\n{context.launch_error}\n```")
    for failure in context.step_failures:
        step = failure.asset_name
        if failure.partition_key is not None:
            step = f"{step}[{failure.partition_key}]"
        parts.append(f"`{step}`:\n```\n{failure.error}\n```")
    return "\n\n".join(parts)
