# IO Handlers

## `BaseIOHandler`

Abstract base class for IO handlers. Extends `pydantic_settings.BaseSettings`, so handler configuration can be resolved from environment variables, `.env` files, or explicit kwargs.

All IO handlers **must** inherit from `BaseIOHandler`. Duck-typing is not supported.

```python
from rivers import BaseIOHandler

class MyHandler(BaseIOHandler):
    def handle_output(self, context: OutputContext, obj: Any) -> None:
        ...

    def load_input(self, context: InputContext) -> Any:
        ...
```

**Abstract methods:**

| Method | Signature |
|--------|-----------|
| `handle_output` | `(context: OutputContext, obj: Any) -> None` |
| `load_input` | `(context: InputContext) -> Any` |

---

## `OutputContext`

Passed to `handle_output()` with information about the asset being written.

**Attributes:**

| Attribute | Type | Description |
|-----------|------|-------------|
| `asset_name` | `str` | Name of the asset. |
| `asset_metadata` | `dict[str, str] \| None` | Static metadata from asset definition. |
| `partition` | `PartitionContext \| None` | Partition info, if partitioned. |
| `type_hint` | `type \| None` | Return type annotation. |
| `output_metadata` | `dict[str, MetadataValue] \| None` | Metadata attached during execution. |

**Methods:**

```python
def add_output_metadata(
    self,
    metadata: dict[str, str | int | float | bool | None | MetadataValue],
) -> None
```

Attach runtime metadata to the output. Values are automatically converted to `MetadataValue` instances.

```python
def register_data_version(self, version: str) -> None
```

Record a content-addressable data-version string for this output. Used by automation conditions like `data_version_changed()`.

---

## `InputContext`

Passed to `load_input()` with information about the asset being loaded.

**Attributes:**

| Attribute | Type | Description |
|-----------|------|-------------|
| `asset_name` | `str` | Name of the upstream asset. |
| `downstream_asset` | `str` | Name of the asset requesting the load. |
| `asset_metadata` | `dict[str, str] \| None` | Static metadata from the upstream asset. |
| `partition` | `PartitionContext \| None` | Partition info, if partitioned. |
| `type_hint` | `type \| None` | Type annotation on the downstream parameter. |

---

## `InMemoryIOHandler`

Stores outputs in a Python dictionary. No persistence across runs. **This is the default IO handler** — assigned automatically to all assets and tasks that don't have an explicit handler.

```python
io = rs.InMemoryIOHandler()
```

No constructor parameters. Cannot survive a process boundary — switch to `PickleIOHandler` or `DeltaIOHandler` if you use `Executor.parallel()` or `Executor.kubernetes()`.

**Output metadata:** `{"storage": "memory", "size_bytes": <int>}`

---

## `PickleIOHandler`

Persists outputs as pickle files via any `obstore`-compatible backend.

```python
from obstore.store import LocalStore

io = rs.PickleIOHandler(
    store=LocalStore(prefix="/data/assets"),
    prefix="v1",
)
```

**Constructor:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `store` | `ObjectStore` | required | Any obstore backend (Local, S3, GCS, Azure, Memory). |
| `prefix` | `str` | `""` | Path prefix for all keys. |

**Output metadata:** `{"path": <str>, "serializer": "pickle", "size_bytes": <int>, "write_duration_s": <float>}`

---

## `rivers.testing`

Test-only `Storage` factories that wrap each instance in a dedicated tokio runtime, so dropping the `Storage` synchronously drains its router task — releasing the RocksDB file lock (for embedded backends) and tearing down in-memory state before control returns.

Use these in pytest fixtures; production code should keep using `Storage.memory()` / `Storage.embedded()` / `Storage.connect()`, which share the global IO runtime.

```python
import pytest
from rivers.testing import memory_storage, embedded_storage

@pytest.fixture
def storage():
    return memory_storage()

@pytest.fixture
def embedded(tmp_path):
    return embedded_storage(str(tmp_path / "db"))
```

| Function | Returns |
|----------|---------|
| `memory_storage()` | `Storage` — in-memory with sync-shutdown on drop. |
| `embedded_storage(path)` | `Storage` — embedded RocksDB at `path` with sync-shutdown on drop. |
