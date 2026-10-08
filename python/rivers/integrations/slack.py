"""Slack integration: a client resource with per-asset hooks and a run-failure sensor.

Install with ``pip install rivers[slack]``.

Example::

    import rivers as rs
    from rivers.integrations.slack import SlackResource

    slack = SlackResource()  # token from SLACK_TOKEN

    @rs.Asset(hooks=[slack.failure_hook("#data-alerts")])
    def orders() -> int: ...

    repo = rs.CodeRepository(
        assets=[orders],
        sensors=[slack.run_failure_sensor("#data-alerts", default_status=rs.SensorStatus.Running)],
    )
"""

from collections.abc import Callable
from typing import Any

from pydantic_settings import SettingsConfigDict

from rivers._core.hooks import Hook, HookContext
from rivers._core.sensor import RunStatusSensorContext, Sensor, SensorStatus
from rivers.resource import Resource

try:
    from slack_sdk import WebClient
except ImportError as e:
    raise ImportError(
        "rivers.integrations.slack needs slack-sdk: pip install rivers[slack]"
    ) from e

__all__ = ["SlackResource"]


class SlackResource(Resource):
    """A Slack client, and the hooks and sensor that post through it.

    Register it as a resource to call Slack from assets and sensors, or use
    its methods to build alerts. Hooks get no resources, so each method posts
    through this instance.

    Args:
        token: Bot token (``xoxb-...``). Read from ``SLACK_TOKEN`` when not given.
    """

    model_config = SettingsConfigDict(env_prefix="SLACK_")

    token: str

    def get_client(self) -> WebClient:
        """Return a ``slack_sdk.WebClient`` authenticated with ``token``."""
        return WebClient(token=self.token)

    def failure_hook(
        self,
        channel: str,
        *,
        text_fn: Callable[[HookContext], str] | None = None,
        ui_location_url: str | None = None,
        name: str = "slack_failure_hook",
    ) -> Hook:
        """A hook that posts to ``channel`` when an asset step fails.

        Args:
            channel: Channel name or id, e.g. ``"#data-alerts"``.
            text_fn: Builds the message from the hook context. The default
                names the asset and run and quotes the error.
            ui_location_url: The code location's UI URL, e.g.
                ``"https://rivers.example.com/locations/prod/etl"``. When set,
                the message links to the run.
            name: Hook name.

        Returns:
            A ``Hook.failure`` to pass to ``@Asset(hooks=[...])``.
        """
        return self._hook(
            Hook.failure, text_fn or _asset_failure_text, channel, ui_location_url, name
        )

    def success_hook(
        self,
        channel: str,
        *,
        text_fn: Callable[[HookContext], str] | None = None,
        ui_location_url: str | None = None,
        name: str = "slack_success_hook",
    ) -> Hook:
        """A hook that posts to ``channel`` when an asset step succeeds.

        Args:
            channel: Channel name or id, e.g. ``"#data-alerts"``.
            text_fn: Builds the message from the hook context. The default
                names the asset and run.
            ui_location_url: The code location's UI URL. When set, the message
                links to the run.
            name: Hook name.

        Returns:
            A ``Hook.success`` to pass to ``@Asset(hooks=[...])``.
        """
        return self._hook(
            Hook.success, text_fn or _asset_success_text, channel, ui_location_url, name
        )

    def run_failure_sensor(
        self,
        channel: str,
        *,
        text_fn: Callable[[RunStatusSensorContext], str] | None = None,
        blocks_fn: Callable[[RunStatusSensorContext], list[dict[str, Any]]]
        | None = None,
        ui_location_url: str | None = None,
        name: str = "slack_run_failure_sensor",
        minimum_interval: str | None = None,
        monitored_jobs: list[str] | None = None,
        default_status: SensorStatus = SensorStatus.Stopped,
    ) -> Sensor:
        """A run-status sensor that posts to ``channel`` once per failed run.

        The daemon evaluates the sensor only with
        ``default_status=SensorStatus.Running``.

        Args:
            channel: Channel name or id, e.g. ``"#data-alerts"``.
            text_fn: Builds the message text. The default names the run and job
                and lists each failed step with its error, or the launch error.
            blocks_fn: Builds Slack Block Kit blocks; ``text`` is then the
                notification fallback.
            ui_location_url: The code location's UI URL. When set, the message
                links to the run.
            name: Sensor name. Two Slack sensors in one repository need distinct names.
            minimum_interval: Minimum interval between ticks (default ``"30s"``).
            monitored_jobs: Only report runs of these jobs. ``None`` reports
                every failed run of the code location.
            default_status: Whether the daemon evaluates the sensor.

        Returns:
            A ``Sensor`` to pass to ``CodeRepository(sensors=[...])``.
        """
        make_text = text_fn or _run_failure_text

        def post(context: RunStatusSensorContext) -> None:
            blocks = blocks_fn(context) if blocks_fn else None
            self._post(
                channel, make_text(context), ui_location_url, context.run.run_id, blocks
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
        text_fn: Callable[[HookContext], str],
        channel: str,
        ui_location_url: str | None,
        name: str,
    ) -> Hook:
        def post(context: HookContext) -> None:
            self._post(channel, text_fn(context), ui_location_url, context.run_id)

        return make_hook(name=name)(post)

    def _post(
        self,
        channel: str,
        text: str,
        ui_location_url: str | None,
        run_id: str,
        blocks: list[dict[str, Any]] | None = None,
    ) -> None:
        if ui_location_url:
            text = f"{text}\n<{ui_location_url.rstrip('/')}/runs/{run_id}|View run>"
        self.get_client().chat_postMessage(channel=channel, text=text, blocks=blocks)


def _asset_failure_text(context: HookContext) -> str:
    return (
        f"Asset `{context.asset_name}` failed in run `{context.run_id}`:\n"
        f"```{context.error}```"
    )


def _asset_success_text(context: HookContext) -> str:
    return f"Asset `{context.asset_name}` succeeded in run `{context.run_id}`."


def _run_failure_text(context: RunStatusSensorContext) -> str:
    run = context.run
    job = f" (job `{run.job_name}`)" if run.job_name else ""
    lines = [f"Run `{run.run_id}` failed{job}."]
    if context.launch_error:
        lines.append(f"Launch error: {context.launch_error}")
    for failure in context.step_failures:
        step = failure.asset_name
        if failure.partition_key is not None:
            step = f"{step}[{failure.partition_key}]"
        lines.append(f"`{step}`: {failure.error}")
    return "\n".join(lines)
