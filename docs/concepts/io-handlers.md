# IO Handlers

IO handlers control how asset outputs are persisted and loaded. Every asset can have its own handler, or share one across assets.

## Default behavior

When no `io_handler` is specified, rivers automatically assigns a shared `InMemoryIOHandler` to all assets and tasks during repository resolution. This means values flow between nodes via the IO handler interface — no special-casing.

To check which handler an asset will actually use without running it, call `repo.io_handler_for_output(asset_name)` — useful for debugging custom-handler configurations.

The shared `InMemoryIOHandler` stores values in a Python dictionary within the process. This is fast (zero serialization overhead) but does not persist data across runs.

To persist outputs, assign a `PickleIOHandler` or `DeltaIOHandler` explicitly.

!!! note "Subprocess / Kubernetes executors"
    `InMemoryIOHandler` cannot survive a process boundary, since subprocesses (and K8s step pods) cannot access the parent's memory. If you use `Executor.parallel()` or `Executor.kubernetes()`, assign a persistent handler (e.g. `PickleIOHandler` or `DeltaIOHandler`) to every asset that may run out-of-process.

## Opting an asset out of the IO handler

For terminal side-effecting nodes — assets that push to an API, emit a message, write directly to an external table, or otherwise manage their own persistence — the IO handler framework has nothing meaningful to round-trip. In these cases, return a [`Materialization`](../api-reference/assets.md#materialization) instead of an `Output(value)`:

```python
import requests
import rivers as rs

@rs.Asset
def push_to_api(rows: list[dict]) -> rs.Materialization:
    response = requests.post(API_URL, json=rows)
    return rs.Materialization(
        metadata={"status_code": rs.MetadataValue.int(response.status_code)},
        data_version=response.headers["ETag"],
    )
```

The framework still records a `Materialization` event (with metadata, data version, tags, and provenance), but **never invokes the IO handler**. The asset is effectively terminal — downstream consumers cannot `load_input` it because no value was written.

This works uniformly across `Executor.in_process()`, `Executor.parallel()`, and `Executor.kubernetes()` — the discriminator lives at the return type, so every executor takes the same code path. See the [`Materialization`](../api-reference/assets.md#materialization) API reference for full details.

## How it works

When a job executes:

1. The asset function runs and returns a value
2. `handle_output(context, obj)` is called to persist the result
3. When a downstream asset needs the data, `load_input(context)` loads it

## Built-in handlers

### InMemoryIOHandler

Stores outputs in a Python dictionary. No persistence across runs. This is the default handler assigned to all assets and tasks that don't have an explicit handler:

```python
import rivers as rs

# Explicit assignment (equivalent to the default behavior):
io = rs.InMemoryIOHandler()

@rs.Asset(io_handler=io)
def my_asset():
    return [1, 2, 3]
```

### PickleIOHandler

Serializes outputs as pickle files to any `obstore`-compatible backend:

```python
from obstore.store import LocalStore, S3Store

# Local filesystem
io = rs.PickleIOHandler(store=LocalStore(prefix="/data/assets"))

# S3
io = rs.PickleIOHandler(
    store=S3Store(bucket="my-bucket", config={"region": "us-east-1"}),
    prefix="assets/v1",
)
```

### DeltaIOHandler

Persists data as Delta Lake tables. Requires `pip install rivers[delta]` and at least one of `rivers[pyarrow]` or `rivers[polars]`:

```python
from rivers.io_handlers.delta import DeltaIOHandler

io = DeltaIOHandler(table_uri="/data/delta")
```

See the [Delta Lake guide](../guides/delta-lake.md) for full details.

## Writing a custom handler

Subclass `BaseIOHandler` (which extends `pydantic_settings.BaseSettings`). Configuration fields can be resolved from environment variables, `.env` files, or explicit kwargs:

```python
from rivers import BaseIOHandler, InputContext, OutputContext

class JsonIOHandler(BaseIOHandler):
    base_path: str

    def handle_output(self, context: OutputContext, obj: object) -> None:
        import json
        path = f"{self.base_path}/{context.asset_name}.json"
        with open(path, "w") as f:
            json.dump(obj, f)
        context.add_output_metadata({"path": path})

    def load_input(self, context: InputContext) -> object:
        import json
        path = f"{self.base_path}/{context.asset_name}.json"
        with open(path) as f:
            return json.load(f)
```

### Register data version during handle_output

IO handlers can register a data version while materialing, by using `output_context.register_data_version()`. This takes precedence over the auto created UUID version.

All IO handlers **must** inherit from `BaseIOHandler`. Duck-typing (objects that merely have `handle_output`/`load_input` methods) is not supported.

### Context objects

**`OutputContext`** provides:

- `asset_name` — name of the asset being written
- `asset_metadata` — static metadata from the asset definition
- `partition` — `PartitionContext` if the asset is partitioned
- `type_hint` — the return type annotation, if any
- `output_metadata` — `dict[str, MetadataValue]` accumulated via `add_output_metadata()`
- `add_output_metadata(metadata)` — attach runtime metadata
- `register_data_version(version)` — record a content-addressable data-version string for this output

**`InputContext`** provides:

- `asset_name` — name of the upstream asset being loaded
- `downstream_asset` — name of the asset requesting the load
- `asset_metadata` — static metadata from the upstream asset
- `partition` — `PartitionContext` if partitioned
- `type_hint` — the type annotation on the downstream parameter
