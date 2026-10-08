# Slack

`rivers.integrations.slack` posts to Slack when assets or runs fail. `SlackResource` holds the bot token. Its methods build the hooks and the sensor that post through it.

## Setup

```bash
pip install rivers[slack]
```

Create a Slack app with a bot token that has the `chat:write` scope, and invite the bot to the channel. `SlackResource` reads the token from `SLACK_TOKEN`:

```python
from rivers.integrations.slack import SlackResource

slack = SlackResource()                  # token from SLACK_TOKEN
slack = SlackResource(token="xoxb-...")  # or explicit
```

## Alert once per failed run

`run_failure_sensor` returns a [run-status sensor](../api-reference/sensors.md#run-status-sensors) that posts one message per failed run of the code location:

```python
import rivers as rs

repo = rs.CodeRepository(
    assets=[orders, revenue],
    sensors=[
        slack.run_failure_sensor(
            "#data-alerts",
            ui_location_url="https://rivers.example.com/locations/prod/etl",
            default_status=rs.SensorStatus.Running,
        )
    ],
)
```

The daemon evaluates the sensor only with `default_status=rs.SensorStatus.Running`.

The default message names the run and its job, and lists each failed step with its error. For a run that failed to launch, it shows the launch error instead:

```text
Run `3f2a…` failed (job `nightly`).
`revenue[2024-01-01]`: ValueError: negative total
View run
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `channel` | required | Channel name or id, e.g. `"#data-alerts"`. |
| `text_fn` | built-in | `(RunStatusSensorContext) -> str`. Builds the message text. |
| `blocks_fn` | `None` | `(RunStatusSensorContext) -> list[dict]`. Builds [Block Kit](https://api.slack.com/block-kit) blocks; the text is then the notification fallback. |
| `ui_location_url` | `None` | The code location's UI URL. When set, the message links to the run at `{ui_location_url}/runs/{run_id}`. |
| `name` | `"slack_run_failure_sensor"` | Sensor name. Two Slack sensors in one repository need distinct names. |
| `minimum_interval` | `"30s"` | Minimum interval between ticks. |
| `monitored_jobs` | `None` | Only report runs of these jobs. `None` reports every failed run. |
| `default_status` | `Stopped` | Whether the daemon evaluates the sensor. |

Each tick posts for at most 5 runs; later failures go out on the next ticks. See [how runs are picked](../api-reference/sensors.md#how-runs-are-picked).

## Alert per asset

`failure_hook` and `success_hook` return [hooks](../api-reference/hooks.md) that post when an asset step fails or succeeds:

```python
@rs.Asset(hooks=[slack.failure_hook("#data-alerts")])
def orders() -> int: ...

@rs.Asset(hooks=[slack.success_hook("#data-releases", text_fn=lambda c: f"`{c.asset_name}` is fresh")])
def revenue() -> int: ...
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `channel` | required | Channel name or id. |
| `text_fn` | built-in | `(HookContext) -> str`. The default names the asset and run, and quotes the error on failure. |
| `ui_location_url` | `None` | The code location's UI URL. When set, the message links to the run. |
| `name` | `"slack_failure_hook"` / `"slack_success_hook"` | Hook name. |

A hook fires once per failed step, so a run with three failed assets sends three messages. Use `run_failure_sensor` for one message per run. A Slack error in a hook is logged and does not fail the step.

## Post from your own code

Register `SlackResource` as a resource to post from assets or sensors:

```python
@rs.Asset
def report(slack: SlackResource) -> None:
    slack.get_client().chat_postMessage(channel="#reports", text="Report ready")

repo = rs.CodeRepository(assets=[report], resources={"slack": SlackResource()})
```

`get_client()` returns a [`slack_sdk.WebClient`](https://docs.slack.dev/tools/python-slack-sdk/web).
