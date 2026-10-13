# Sling

`rivers.integrations.sling` (`pip install rivers[sling]`). See the [guide](../guides/sling.md).

## `SlingReplication`

A sling replication and the rivers output of each of its enabled streams.

```python
from rivers.integrations.sling import SlingReplication

replication = SlingReplication("replication.yaml", asset_def_fn=None)
```

**Constructor:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config` | `str \| PathLike \| Mapping` | required | A [replication](https://docs.slingdata.io/concepts/replication) file (YAML or JSON), or the replication as a dict. |
| `asset_def_fn` | `(SlingStream) -> AssetDef` | `SlingStream.output_def` | Builds the output of each stream. When it sets no `code_version`, the output gets a hash of the stream's config and of the replication's other keys. |

Raises `ValueError` when the replication has no enabled streams, when an output has no name, or when two streams get the same output name.

**Attributes:**

| Attribute | Type | Description |
|-----------|------|-------------|
| `config` | `dict` | The replication. |
| `streams` | `dict[str, SlingStream]` | The stream of each output, by output name, in replication order. |
| `output_defs` | `list[AssetDef]` | The outputs, for `Asset.from_multi`. |

### `assets(**kwargs)`

Decorator. Builds a [`MultiAsset`](assets.md) with one output per stream. The keyword arguments go to `Asset.from_multi`, e.g. `name`, `group`, `partitions_def`, `hooks`, or `retry`.

```python
@replication.assets(name="raw")
def raw(context: rs.AssetExecutionContext, sling: SlingResource):
    yield from sling.replicate(context, replication)
```

---

## `SlingStream`

One stream, as `asset_def_fn` gets it.

| Field | Type | Description |
|-------|------|-------------|
| `name` | `str` | The stream's key under `streams`. |
| `config` | `dict` | The stream's own config, without the replication's `defaults`. |

### `asset_name`

The default asset name: the stream's `object` when it has no runtime variables (`{...}`), else the stream name. Lowercased, without quotes, and with each run of characters other than letters, digits, `_`, and `.` replaced by `_`.

### `output_def(**fields)`

Returns the default output with `fields` set or replaced. The keyword arguments go to [`AssetDef`](assets.md).

| Field | Default |
|-------|---------|
| `name` | `asset_name` |
| `kinds` | `["sling"]` |
| `metadata` | `{"sling/stream": name}` |

```python
SlingReplication(
    "replication.yaml",
    asset_def_fn=lambda stream: stream.output_def(group="raw", deps=[rs.AssetDef.dep("source_users")]),
)
```

---

## `SlingResource`

Runs replications with the connections it holds. Each connection goes to sling as JSON in an environment variable named after the connection. Connections that sling finds by itself (environment variables, `~/.sling/env.yaml`) work too.

```python
from rivers.integrations.sling import SlingConnection, SlingResource

sling = SlingResource(connections=[SlingConnection(name="PG", url="${PG_URL}")])
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connections` | `list[SlingConnection]` | `[]` | The connections. Read from the `SLING_CONNECTIONS` environment variable (a JSON list) when not given. |

### `replicate(context, replication)`

Runs the streams of `context.output_selection` and yields a `Materialization` for each. The other streams are disabled for the run. Sling's log goes to `context.log`.

| Parameter | Type | Description |
|-----------|------|-------------|
| `context` | `AssetExecutionContext` | The context of the multi-asset. |
| `replication` | `SlingReplication` | The replication that the multi-asset was built from. |

A stream is materialized as soon as sling reports that it succeeded. A stream that sling's log does not name, such as a wildcard stream, is materialized when the whole replication succeeds, with `sling/stream` only.

Raises `SlingError` when sling exits with an error; the streams that succeeded before keep their materialization. Raises `ValueError` when a connection references an environment variable that is not set.

**Output metadata:**

| Key | Type | Description |
|-----|------|-------------|
| `sling/stream` | `str` | The stream. |
| `sling/object` | `str` | The table or file that sling wrote. |
| `sling/rows_written` | `int` | Rows that sling wrote for the stream in this run. |
| `sling/duration_s` | `float` | Seconds the stream took. |

The binary comes from the `sling` package, which downloads it on first use. Set `SLING_BINARY` to use another.

---

## `SlingConnection`

A connection, by the name that the replication's `source` and `target` use. Other keyword arguments are the connection's [properties](https://docs.slingdata.io/connections/database-connections), e.g. `host`, `user`, `database`, or `instance`.

```python
SlingConnection(name="PG", type="postgres", host="db", user="etl", password="${PG_PASSWORD}")
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | required | The connection name. |
| `type` | `str \| None` | `None` | The connection type, e.g. `"postgres"` or `"file"`. Sling reads it from `url` when not given. |
| `url` | `SecretStr \| None` | `None` | A connection URL. |
| `password` | `SecretStr \| None` | `None` | The password. |

Give other secret properties as `SecretStr` too. A string can reference environment variables as `"${VAR}"`; they are read when the replication runs. The values of `SecretStr` properties and of references are masked as `***` in the step log and in `SlingError`. A connection needs `type` or `url`; without both it raises a validation error.

---

## `SlingError`

`RuntimeError` subclass. Sling exited with an error; the message holds the exit code and the errors that sling reported.
