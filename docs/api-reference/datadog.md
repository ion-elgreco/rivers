# Datadog

## `DatadogResource`

A Datadog client, and the hooks and sensor that post events through it. Fields are read from `DATADOG_*` environment variables when not given. See the [Datadog guide](../guides/datadog.md) for setup and examples.

```python
from rivers.integrations.datadog import DatadogResource

datadog = DatadogResource(api_key="...", constant_tags=["env:prod"])
```

**Constructor:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `api_key` | `SecretStr \| None` | `None` | API key. The hooks and the sensor need it. |
| `app_key` | `SecretStr \| None` | `None` | Application key, for `client.api` calls that read data. |
| `api_host` | `str` | `"https://api.datadoghq.com"` | The API endpoint of your Datadog site, e.g. `"https://api.datadoghq.eu"`. |
| `statsd_host` | `str \| None` | `None` | DogStatsD host. `None` reads `DD_AGENT_HOST`, else uses `"localhost"`. |
| `statsd_port` | `int \| None` | `None` | DogStatsD port. `None` reads `DD_DOGSTATSD_PORT`, else uses `8125`. |
| `statsd_socket_path` | `str \| None` | `None` | DogStatsD Unix socket. Used instead of the host and port. |
| `constant_tags` | `list[str]` | `[]` | Tags on every metric and event. In `DATADOG_CONSTANT_TAGS`, a JSON list. |

### Methods

#### `get_client()`

Returns the [`DatadogClient`](#datadogclient) for the DogStatsD fields. Resources with the same DogStatsD fields share one client per process. Also points datadogpy's process-wide API client at `api_key`, `app_key`, and `api_host`.

```python
client = datadog.get_client()
client.increment("orders.loaded", tags=["source:s3"])
```

#### `failure_hook(*, tags=None, text_fn=None, ui_location_url=None, name="datadog_failure_hook")`

Returns a `Hook.failure` that posts an `error` event to the Datadog API when an asset step fails. The title is `Asset <asset> failed`, and the default text quotes the error.

#### `success_hook(*, tags=None, text_fn=None, ui_location_url=None, name="datadog_success_hook")`

Returns a `Hook.success` that posts a `success` event when an asset step succeeds. The title is `Asset <asset> succeeded`.

The hook events are tagged `asset:<asset>` and `run_id:<run id>`.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tags` | `list[str] \| None` | `None` | More tags for the event. |
| `text_fn` | `Callable[[HookContext], str] \| None` | `None` | Builds the event text, in Markdown. |
| `ui_location_url` | `str \| None` | `None` | The code location's UI URL. When set, the event links to `{ui_location_url}/runs/{run_id}`. |
| `name` | `str` | see signature | Hook name. |

#### `run_failure_sensor(*, tags=None, text_fn=None, ui_location_url=None, name="datadog_run_failure_sensor", minimum_interval=None, monitored_jobs=None, default_status=SensorStatus.Stopped)`

Returns a [run-status sensor](sensors.md#run-status-sensors) that posts one `error` event per failed run. The title is `Run <run id> failed`, with `(job <job>)` for a job's run. The event is tagged `run_id:<run id>`, `job:<job>` and `partition:<partition key>` when the run has them, and `asset:<asset>` for each failed step.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tags` | `list[str] \| None` | `None` | More tags for the event. |
| `text_fn` | `Callable[[RunStatusSensorContext], str] \| None` | `None` | Builds the event text, in Markdown. The default lists each failed step with its error, or the launch error. |
| `ui_location_url` | `str \| None` | `None` | The code location's UI URL. When set, the event links to the run. |
| `name` | `str` | `"datadog_run_failure_sensor"` | Sensor name. |
| `minimum_interval` | `str \| None` | `None` | Minimum interval between ticks (default `"30s"`). |
| `monitored_jobs` | `list[str] \| None` | `None` | Only report runs of these jobs. |
| `default_status` | `SensorStatus` | `Stopped` | Whether the daemon evaluates the sensor. |

The hooks and the sensor raise `ValueError` when `api_key` is not set. Every event also gets `code_location:<name>` when `RIVERS_CODE_LOCATION_NAME` is set, then `tags`, then `constant_tags`, and the run id as its aggregation key. Each post has a 10 s timeout and raises `requests.HTTPError` when Datadog rejects the event: a hook logs the error, and the sensor's tick fails.

---

## `DatadogClient`

A [`datadog.DogStatsd`](https://datadogpy.readthedocs.io/en/latest/#datadog.dogstatsd.base.DogStatsd) client, returned by `DatadogResource.get_client()`.

| Member | Description |
|--------|-------------|
| `gauge`, `increment`, `decrement`, `histogram`, `distribution`, `set`, `timing`, `timed` | Send a metric to DogStatsD. |
| `event`, `service_check` | Send an event or a service check through the Agent. |
| `flush`, `wait_for_pending` | Send buffered metrics; wait for the background sender. |
| `OK`, `WARNING`, `CRITICAL`, `UNKNOWN` | Service check statuses. |
| `api` | datadogpy's [HTTP API client](https://datadogpy.readthedocs.io/en/latest/#datadog-api), e.g. `api.Event.create(...)`. Its keys are process-wide; it returns a rejected request as a dict with an `errors` key. |
