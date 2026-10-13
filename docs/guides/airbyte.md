# Airbyte

`rivers.integrations.airbyte` runs [Airbyte](https://airbyte.com) syncs as rivers assets. `AirbyteResource` calls the Airbyte API. Its `connection_asset` method makes a multi-asset for an Airbyte connection, with one output per enabled stream. A run starts a sync job, waits for it, and records a materialization for each stream.

## Setup

The integration needs no extra: it calls the Airbyte API with the Python standard library.

Create an application in Airbyte (**Settings → Applications**) to get a client id and a client secret. `AirbyteResource` reads them from `AIRBYTE_CLIENT_ID` and `AIRBYTE_CLIENT_SECRET`, and connects to Airbyte Cloud:

```python
from rivers.integrations.airbyte import AirbyteResource

airbyte = AirbyteResource()  # Airbyte Cloud, credentials from AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET
```

For a self-managed Airbyte, set the URLs of its two APIs:

```python
airbyte = AirbyteResource(
    api_url="http://localhost:8000/api/public/v1",
    configuration_api_url="http://localhost:8000/api/v1",
)
```

With basic authentication, set `username` and `password` instead of the client credentials. Without authentication, set neither.

Fields that you do not give are read from `AIRBYTE_*` environment variables, for example `AIRBYTE_API_URL`.

| Field | Default | Description |
|-------|---------|-------------|
| `api_url` | `"https://api.airbyte.com/v1"` | The Airbyte API. Self-managed: `<host>/api/public/v1`. |
| `configuration_api_url` | `"https://cloud.airbyte.com/api/v1"` | Airbyte's Configuration API, for the per-stream counts. Self-managed: `<host>/api/v1`. |
| `client_id`, `client_secret` | `None` | Application credentials. The resource gets an access token with them, and gets a new one 30 s before it expires. |
| `username`, `password` | `None` | Basic authentication, for a self-managed Airbyte. |
| `request_timeout` | `15` | Seconds to wait for each API response. |
| `request_max_retries` | `3` | Retries of a request that fails with a connection error, HTTP 429, or HTTP 5xx. Other HTTP errors fail at once. |
| `request_retry_delay` | `0.25` | Seconds between retries. |
| `poll_interval` | `10` | Seconds between status checks of a running job. |
| `poll_timeout` | `None` | Seconds to wait for a job. `None` waits until the job finishes. |
| `cancel_on_interrupt` | `True` | Cancel the job when rivers stops waiting before the job finishes. See [Timeouts and cancel](#timeouts-and-cancel). |

## Assets for a connection

```python
import rivers as rs

postgres = airbyte.get_connection("9924bcd0-99be-453d-ba47-c2c9766f7da5")

repo = rs.CodeRepository(assets=[airbyte.connection_asset(postgres)])
```

`get_connection` reads the connection and its enabled streams from the Airbyte API. `connection_asset` makes a multi-asset named `airbyte_` and the connection name in snake case, for example `airbyte_postgres_to_snowflake`. Each output has the name of its destination table: the connection's table prefix and the stream name, for example `pg_users`. The outputs have the kind `airbyte`.

`get_connection` runs each time a process loads the code location, for example each Kubernetes step pod. To load the code location without the Airbyte API, for example in tests, save the connection as JSON and read it back:

```python
from pathlib import Path
from rivers.integrations.airbyte import AirbyteConnection

Path("airbyte/postgres.json").write_text(postgres.model_dump_json())  # once

postgres = AirbyteConnection.model_validate_json(Path("airbyte/postgres.json").read_text())
```

When the connection's streams change in Airbyte, load the code location again to update the outputs.

## What a run does

1. Reads the connection again, to find streams that were removed or added after the code location loaded.
2. Starts one sync job for the whole connection.
3. Checks the job every `poll_interval` seconds until it finishes.
4. Records a materialization for each selected output.

Airbyte syncs all the streams of a connection. A run that selects some outputs syncs all the streams too, and records materializations only for the selected outputs.

When the job fails or is cancelled in Airbyte, all the outputs fail with `AirbyteError`. When a selected output's stream is no longer enabled, that output fails and the others succeed. A stream that was enabled after the code location loaded is synced, and the run log names it.

When a sync of the connection is already running, Airbyte does not start another, and the run fails with `AirbyteError` (HTTP 409).

Each materialization has this metadata:

| Key | Type | Description |
|-----|------|-------------|
| `airbyte/connection_id` | `str` | The connection id. |
| `airbyte/job_id` | `int` | The Airbyte job id. |
| `airbyte/stream` | `str` | The stream: `namespace.name`, or the name when the stream has no namespace. |
| `airbyte/records_committed` | `int` | Records the job committed for this stream. |
| `airbyte/bytes_committed` | `MetadataValue.bytes` | Bytes the job committed for this stream. |

The counts come from the Configuration API, because the Airbyte API reports only totals for the whole job. When Airbyte has no counts for a stream, its materialization has no count keys. When the Configuration API request fails, no materialization has counts, and the run log has a warning.

## Timeouts and cancel

Set `poll_timeout` to stop waiting for long jobs:

```python
airbyte = AirbyteResource(poll_timeout=3600)
```

When rivers stops waiting before the job finishes, it cancels the job in Airbyte. This occurs after `poll_timeout`, when an API request fails, and on an interrupt such as Ctrl-C. Set `cancel_on_interrupt=False` to let the job continue in Airbyte. A process that is killed, for example a deleted Kubernetes pod, cannot cancel the job.

To try a failed sync again, give the asset a [retry policy](../concepts/retries.md):

```python
from rivers.integrations.airbyte import AirbyteError

airbyte.connection_asset(postgres, retry=rs.RetryPolicy(max_retries=2, retry_on=[AirbyteError]))
```

## Change the outputs

`asset_def_fn` makes the `rs.AssetDef` of each stream. It takes the `AirbyteConnection` and one of its `AirbyteStream`s. `connection.output_def(stream, ...)` returns the default output with the fields that you give. The other keyword arguments of `connection_asset` go to [`rs.Asset.from_multi`](../api-reference/assets.md#assetfrom_multi), for example `group`, `hooks`, `automation_condition`, or `retry`:

```python
postgres_sync = airbyte.connection_asset(
    postgres,
    name="postgres_sync",
    asset_def_fn=lambda connection, stream: connection.output_def(
        stream, kinds=["airbyte", "snowflake"]
    ),
    group="raw",
    automation_condition=rs.AutomationCondition.on_cron("0 2 * * *"),
)
```

The default output sets only the name, so it gets `group` and the kind `airbyte` from `connection_asset`. A field that `asset_def_fn` sets replaces them: above, the outputs have the kinds `airbyte` and `snowflake`.

Two streams with the same name in different namespaces need an `asset_def_fn` that gives them different names, for example with `stream.namespace`.

## Downstream assets

Airbyte writes the tables, and each run records a [`Materialization`](../api-reference/assets.md#materialization), so rivers stores no values for these assets. Declare the dependency with `rs.AssetDef.dep`, and read the table from the destination:

```python
@rs.Asset(deps=[rs.AssetDef.dep("pg_users")])
def active_users() -> None:
    ...  # query pg_users in the destination
```

## Start a sync in your own code

`sync` starts a sync job, waits for it, and returns the `AirbyteJob`. It raises `AirbyteError` when the job does not succeed:

```python
job = airbyte.sync("9924bcd0-99be-453d-ba47-c2c9766f7da5", log=context.log)
job.rows_synced  # records of all streams
```

See the [API reference](../api-reference/airbyte.md) for all the classes.
