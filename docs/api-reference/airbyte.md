# Airbyte

## `AirbyteResource`

Calls the Airbyte API of Airbyte Cloud or of a self-managed Airbyte. Fields are read from `AIRBYTE_*` environment variables when not given.

```python
from rivers.integrations.airbyte import AirbyteResource

airbyte = AirbyteResource(client_id="...", client_secret="...")
```

**Constructor:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `api_url` | `str` | `"https://api.airbyte.com/v1"` | Base URL of the Airbyte API. Self-managed: `<host>/api/public/v1`. |
| `configuration_api_url` | `str` | `"https://cloud.airbyte.com/api/v1"` | Base URL of Airbyte's Configuration API, which has the per-stream counts of a job. Self-managed: `<host>/api/v1`. |
| `client_id` | `str \| None` | `None` | Client id of an Airbyte application. |
| `client_secret` | `SecretStr \| None` | `None` | Client secret of an Airbyte application. A string given here is stored as a `SecretStr`. |
| `username` | `str \| None` | `None` | Username, for basic authentication. |
| `password` | `SecretStr \| None` | `None` | Password, for basic authentication. A string given here is stored as a `SecretStr`. |
| `request_timeout` | `float` | `15` | Seconds to wait for each API response. |
| `request_max_retries` | `int` | `3` | Retries of a request that fails with a connection error, HTTP 429, or HTTP 5xx. |
| `request_retry_delay` | `float` | `0.25` | Seconds between retries. |
| `poll_interval` | `float` | `10` | Seconds between status checks of a running job. |
| `poll_timeout` | `float \| None` | `None` | Seconds to wait for a job to finish. `None` waits until it finishes. |
| `cancel_on_interrupt` | `bool` | `True` | Cancel the job when the wait stops before the job finishes: after `poll_timeout`, on an error, or on an interrupt such as Ctrl-C. |

Set `client_id` and `client_secret` together, or `username` and `password` together, or none of them. Other combinations raise a `ValidationError`.

With client credentials, the resource gets an access token from `POST {api_url}/applications/token`, and gets a new one 30 s before the token expires (`expires_in` in the response).

### Methods

#### `get_connection(connection_id)`

Returns the [`AirbyteConnection`](#airbyteconnection) with its enabled streams, from `GET {api_url}/connections/{connection_id}`. Raises `AirbyteError` when the request fails.

#### `connection_asset(connection, *, name=None, asset_def_fn=None, **asset_options)`

Returns a `MultiAsset` that syncs `connection`, with one output per enabled stream. The asset's function is async, so its step waits for the job without holding a worker of the executor.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `connection` | `AirbyteConnection` | required | From `get_connection()`, or built by hand. |
| `name` | `str \| None` | `None` | Asset name. The default is `airbyte_` and the connection name in snake case. |
| `asset_def_fn` | `(AirbyteConnection, AirbyteStream) -> AssetDef` | `AirbyteConnection.output_def` | Makes the output of a stream. The default names it like the destination table: the connection's `prefix` and the stream name. |
| `**asset_options` | | | Passed to [`Asset.from_multi()`](assets.md#assetfrom_multi), for example `group`, `hooks`, `automation_condition`, or `retry`. `kinds` defaults to `["airbyte"]`. |

Raises `ValueError` when the connection has no enabled streams, or when two streams get the same output name.

A run:

1. Reads the connection again with `get_connection()`.
2. Calls `async_sync()`.
3. Gets the per-stream counts from `POST {configuration_api_url}/jobs/get_without_logs`. When this request fails, the run log has a warning and the materializations have no counts.
4. Records a materialization for each selected output, with the metadata below.

A selected output whose stream the connection no longer syncs fails with `AirbyteError`, after the other outputs are recorded.

Each output records a [`Materialization`](assets.md#materialization) and stores no value, so a downstream asset declares the dependency with `AssetDef.dep()`.

**Materialization metadata:**

| Key | Type | Description |
|-----|------|-------------|
| `airbyte/connection_id` | `str` | The connection id. |
| `airbyte/job_id` | `int` | The Airbyte job id. |
| `airbyte/stream` | `str` | `namespace.name`, or the stream name when the stream has no namespace. |
| `airbyte/records_committed` | `int` | Records the job committed for this stream. Missing when Airbyte has no counts for the stream. |
| `airbyte/bytes_committed` | `MetadataValue.bytes` | Bytes the job committed for this stream. Missing when Airbyte has no counts for the stream. |

#### `sync(connection_id, *, log=None)`

Starts a sync job with `POST {api_url}/jobs`, checks it with `GET {api_url}/jobs/{job_id}` every `poll_interval` seconds, and returns the succeeded [`AirbyteJob`](#airbytejob). `log` is a `logging.Logger` for the job's progress, for example `context.log`.

When the wait stops before the job finishes and `cancel_on_interrupt` is set, the job is cancelled with `DELETE {api_url}/jobs/{job_id}`. A failed cancel is logged and does not replace the original error.

Raises `AirbyteError` when a request fails, when the job ends with a status other than `succeeded`, or when `poll_timeout` passes.

#### `async_sync(connection_id, *, log=None)`

`sync` for async code. It takes the same arguments, returns and raises the same, and waits without blocking the event loop. A cancellation of the task, for example on Ctrl-C, stops the wait, so the job is cancelled as above.

```python
@rs.Asset(kinds="airbyte")
async def postgres_sync(context: rs.AssetExecutionContext) -> rs.Materialization:
    job = await airbyte.async_sync("9924bcd0-99be-453d-ba47-c2c9766f7da5", log=context.log)
    return rs.Materialization(metadata={"airbyte/job_id": job.job_id})
```

---

## `AirbyteConnection`

An Airbyte connection and its enabled streams. A pydantic model: save it with `model_dump_json()` and read it with `AirbyteConnection.model_validate_json()`.

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `connection_id` | `str` | required | The connection id, from the connection's URL in the Airbyte UI. |
| `name` | `str` | required | The connection name. |
| `prefix` | `str \| None` | `None` | Prefix the connection adds to each table name in the destination. |
| `streams` | `list[AirbyteStream]` | `[]` | The enabled streams. |

### `output_def(stream, **fields)`

Returns the default output of `stream`, one of the connection's streams, with `fields` set or replaced. The keyword arguments go to [`AssetDef`](assets.md).

| Field | Default |
|-------|---------|
| `name` | `prefix` and the stream name, the name of the destination table, e.g. `pg_users` |

The default output sets no `kinds`, so it gets the kinds of `connection_asset()`, `["airbyte"]` by default.

```python
airbyte.connection_asset(
    connection,
    asset_def_fn=lambda connection, stream: connection.output_def(stream, kinds=["airbyte", "snowflake"]),
)
```

## `AirbyteStream`

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | required | The stream name in the source. |
| `namespace` | `str \| None` | `None` | The namespace in the source, for example a Postgres schema. |
| `sync_mode` | `str \| None` | `None` | The sync mode, for example `"incremental_append"`. |

## `AirbyteJob`

| Attribute | Type | Description |
|-----------|------|-------------|
| `job_id` | `int` | The job id. |
| `status` | `str` | `pending`, `queued`, `running`, `incomplete`, `succeeded`, `failed`, or `cancelled`. |
| `rows_synced` | `int \| None` | Records the job committed, over all streams. |
| `bytes_synced` | `int \| None` | Bytes the job committed, over all streams. |

## `AirbyteError`

Raised when an API request fails, or when a job does not succeed. Use it in a retry policy: `rs.RetryPolicy(max_retries=2, retry_on=[AirbyteError])`.
