# Exceptions

Exceptions raised by the rivers runtime. All are re-exported under `rivers.exceptions` — catch them from there rather than reaching into `_core`.

```python
import rivers as rs
from rivers.exceptions import GraphValidationError, ConfigurationError

try:
    repo = rs.CodeRepository(assets=[bad_asset])
except GraphValidationError as exc:
    print("Graph could not be validated:", exc)
```

Every class extends Python's built-in `Exception`.

| Class | Raised when |
|-------|-------------|
| `AssetDefinitionError` | An `@Asset` definition is invalid (bad signature, conflicting flags, …). |
| `AssetNotFoundError` | A referenced asset key does not exist in the repository. |
| `AssetOutputValidationError` | An asset output fails validation (type, schema, metadata). |
| `ConfigurationError` | User configuration (resources, run config) cannot be resolved. |
| `ExecutionError` | An asset/task body raised during execution. |
| `GraphValidationError` | The asset graph fails validation (cycles, dangling deps). |
| `InvalidMetadataError` | A metadata value cannot be encoded as a `MetadataValue`. |
| `NodeNotFoundError` | A referenced node (asset/task) is missing from the graph. |
| `PartitionDefinitionError` | A `PartitionsDefinition` is constructed with invalid arguments. |
| `PartitionValidationError` | A `PartitionKey` is not valid for the asset's partitions def. |
| `ResultDefinitionError` | An asset's `Output` / `Observation` is malformed. |
| `ScheduleDefinitionError` | A `Schedule` is constructed with invalid arguments. |
| `SensorDefinitionError` | A `Sensor` is constructed with invalid arguments. |
| `StorageError` | A storage backend failure (connection, transaction, serialization). |
| `TaskDefinitionError` | A `Task` / `BashTask` definition is invalid. |

When `materialize(raise_on_error=True)` (the default) is used, an `ExecutionError` is raised on the first failed step. With `raise_on_error=False`, failures are collected into `RunResult.failed_assets` instead.
