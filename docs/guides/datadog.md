# Datadog

`rivers.integrations.datadog` sends events and metrics to [Datadog](https://www.datadoghq.com). `DatadogResource` holds the keys and the address of the Agent. Its methods build the hooks and the sensor that post an event when an asset or a run fails, and `get_client()` returns a client for your own metrics.

## Setup

```bash
pip install rivers[datadog]
```

The hooks and the sensor post events to the Datadog API with an [API key](https://docs.datadoghq.com/account_management/api-app-keys/). They need no Agent. Metrics go to the [DogStatsD](https://docs.datadoghq.com/extend/dogstatsd/) server of a Datadog Agent; rivers needs no key for them. `DatadogResource` reads the fields that you do not give from `DATADOG_*` environment variables, for example `DATADOG_API_KEY` or `DATADOG_API_HOST`:

```python
from rivers.integrations.datadog import DatadogResource

datadog = DatadogResource()  # keys from DATADOG_API_KEY and DATADOG_APP_KEY
datadog = DatadogResource(api_key="...", api_host="https://api.datadoghq.eu")  # or explicit
```

| Field | Default | Description |
|-------|---------|-------------|
| `api_key` | `None` | API key. The hooks and the sensor need it. |
| `app_key` | `None` | Application key, for `client.api` calls that read data. |
| `api_host` | `"https://api.datadoghq.com"` | The API endpoint of your [Datadog site](https://docs.datadoghq.com/getting_started/site/), e.g. `"https://api.datadoghq.eu"` or `"https://api.us5.datadoghq.com"`. |
| `statsd_host` | `DD_AGENT_HOST`, else `"localhost"` | DogStatsD host. |
| `statsd_port` | `DD_DOGSTATSD_PORT`, else `8125` | DogStatsD port. |
| `statsd_socket_path` | `None` | DogStatsD Unix socket, e.g. `"/var/run/datadog/dsd.socket"`. Used instead of the host and port. |
| `constant_tags` | `[]` | Tags on every metric and event, e.g. `["env:prod"]`. In `DATADOG_CONSTANT_TAGS`, a JSON list. |

On Kubernetes, set these variables in `CodeLocation.spec.env`: `DATADOG_API_KEY` from a Secret, and `DD_AGENT_HOST` from a `status.hostIP` `fieldRef` when an Agent runs on each node. Run and step pods get the same variables.

## Alert once per failed run

`run_failure_sensor` returns a [run-status sensor](../api-reference/sensors.md#run-status-sensors) that posts one event per failed run of the code location:

```python
import rivers as rs

repo = rs.CodeRepository(
    assets=[orders, revenue],
    sensors=[
        datadog.run_failure_sensor(
            tags=["team:data"],
            ui_location_url="https://rivers.example.com/locations/prod/etl",
            default_status=rs.SensorStatus.Running,
        )
    ],
)
```

The daemon evaluates the sensor only with `default_status=rs.SensorStatus.Running`.

The event has the alert type `error` and the title `Run <run id> failed`, followed by `(job <job>)` when the run belongs to a job. Its text lists each failed step with its error, or the launch error:

````text
Run `3f2a…` failed (job `nightly`).

`revenue[2024-01-01]`:
```
ValueError: negative total
```

View run
````

The event is tagged:

- `run_id:<run id>`
- `job:<job>` and `partition:<partition key>`, when the run has them
- `asset:<asset>` for each failed step
- `code_location:<name>` when `RIVERS_CODE_LOCATION_NAME` is set. The operator sets it to the name of the `CodeLocation` on code location and run pods.
- the sensor's `tags`, then `constant_tags`

The run id is also the event's aggregation key, so the events of one run group together. To get notified, create an [event monitor](https://docs.datadoghq.com/monitors/types/event/) that matches these tags, for example `asset:revenue`.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `tags` | `None` | More tags for the event, e.g. `["team:data"]`. |
| `text_fn` | built-in | `(RunStatusSensorContext) -> str`. Builds the event text, in Markdown. |
| `ui_location_url` | `None` | The code location's UI URL. When set, the event links to the run at `{ui_location_url}/runs/{run_id}`. |
| `name` | `"datadog_run_failure_sensor"` | Sensor name. Two Datadog sensors in one repository need distinct names. |
| `minimum_interval` | `"30s"` | Minimum interval between ticks. |
| `monitored_jobs` | `None` | Only report runs of these jobs. `None` reports every failed run. |
| `default_status` | `Stopped` | Whether the daemon evaluates the sensor. |

Each tick posts for at most 5 runs; later failures go out on the next ticks. When Datadog rejects an event or cannot be reached within 10 s, the tick fails with the error, and the sensor does not try that run again. See [how runs are picked](../api-reference/sensors.md#how-runs-are-picked).

## Alert per asset

`failure_hook` and `success_hook` return [hooks](../api-reference/hooks.md) that post an event when an asset step fails or succeeds:

```python
@rs.Asset(hooks=[datadog.failure_hook(tags=["team:finance"])])
def orders() -> int: ...

@rs.Asset(hooks=[datadog.success_hook(text_fn=lambda c: f"`{c.asset_name}` is fresh")])
def revenue() -> int: ...
```

The event has the title `Asset <asset> failed` and the alert type `error`, or `Asset <asset> succeeded` and `success`. The default text names the asset and the run, and quotes the error on failure. The event is tagged `asset:<asset>` and `run_id:<run id>`, then `code_location:<name>` as above, the hook's `tags`, and `constant_tags`. The hook context has no partition key, so the event has no partition tag; the run-failure sensor's event has one.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `tags` | `None` | More tags for the event. |
| `text_fn` | built-in | `(HookContext) -> str`. Builds the event text, in Markdown. |
| `ui_location_url` | `None` | The code location's UI URL. When set, the event links to the run. |
| `name` | `"datadog_failure_hook"` / `"datadog_success_hook"` | Hook name. |

A hook fires once per step, so a run with three failed assets posts three events. Use `run_failure_sensor` for one event per run. A Datadog error in a hook is logged and does not fail the step.

Without an API key, `failure_hook`, `success_hook`, and `run_failure_sensor` raise `ValueError` when you build them, not when the first event is due.

## Send metrics from your own code

Register `DatadogResource` as a resource to send metrics or events from assets and sensors:

```python
import pyarrow as pa

@rs.Asset
def orders(datadog: DatadogResource) -> pa.Table:
    table = load_orders()
    datadog.get_client().gauge("orders.rows", table.num_rows, tags=["asset:orders"])
    return table

repo = rs.CodeRepository(assets=[orders], resources={"datadog": DatadogResource()})
```

`get_client()` returns a `DatadogClient`, a [`datadog.DogStatsd`](https://datadogpy.readthedocs.io/en/latest/#datadog.dogstatsd.base.DogStatsd) client with `constant_tags` on every metric. Resources with the same DogStatsD fields share one client per process. Its methods send through the Agent: `gauge`, `increment`, `decrement`, `histogram`, `distribution`, `set`, `timing`, `timed`, `event`, and `service_check`.

Each distinct set of tags on a metric is a separate [custom metric](https://docs.datadoghq.com/metrics/custom_metrics/) in Datadog. Do not tag metrics with run ids or partition keys.

`client.api` is datadogpy's [HTTP API client](https://datadogpy.readthedocs.io/en/latest/#datadog-api), for example `client.api.Event.create(...)` or `client.api.Monitor.get_all()`. datadogpy keeps its keys for the whole process: `get_client()` sets them to the resource's `api_key`, `app_key`, and `api_host`. It returns a rejected request as a dict with an `errors` key; it does not raise.

## Traces through OpenTelemetry

rivers itself exports traces over OTLP/gRPC when `OTEL_EXPORTER_OTLP_ENDPOINT` is set (see [Observability](../api-reference/environment-variables.md#observability)). This needs no `rivers[datadog]`. The Datadog Agent accepts OTLP from version 7.32: enable its [OTLP receiver](https://docs.datadoghq.com/opentelemetry/setup/otlp_ingest_in_the_agent/), for example with `DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_GRPC_ENDPOINT=0.0.0.0:4317`, and point `OTEL_EXPORTER_OTLP_ENDPOINT` at it. The spans then show in Datadog APM under the service `rivers`, or the name in `OTEL_SERVICE_NAME`. On Kubernetes, [OpenTelemetry export](../installation/kubernetes.md#opentelemetry-export) shows how to point the pods at an Agent on the same node.

The two work together. Traces show what rivers did and how long it took; this integration tells Datadog when an asset or a run fails, and carries the metrics that your assets send.
