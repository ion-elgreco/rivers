"""Exception types raised by the Rust core, surfaced via ``rivers.exceptions``."""

class AssetDefinitionError(Exception):
    """Raised when an ``@Asset`` definition is invalid (bad signature, conflicting flags, ...)."""

class AssetNotFoundError(Exception):
    """Raised when a referenced asset key does not exist in the repository."""

class AssetOutputValidationError(Exception):
    """Raised when an asset output fails validation (type, schema, metadata)."""

class ConfigurationError(Exception):
    """Raised when user configuration (resources, run config) cannot be resolved."""

class ExecutionError(Exception):
    """Raised when an asset/task body fails during execution."""

class GraphValidationError(Exception):
    """Raised when the asset graph fails validation (cycles, dangling deps)."""

class InvalidMetadataError(Exception):
    """Raised when a metadata value cannot be encoded as a ``MetadataValue``."""

class NodeNotFoundError(Exception):
    """Raised when a referenced node (asset/task) is missing from the graph."""

class PartitionDefinitionError(Exception):
    """Raised when a ``PartitionsDefinition`` is constructed with invalid arguments."""

class PartitionValidationError(Exception):
    """Raised when a ``PartitionKey`` is not valid for the asset's partitions def."""

class ResultDefinitionError(Exception):
    """Raised when an asset's ``Output`` / ``Observation`` is malformed."""

class ScheduleDefinitionError(Exception):
    """Raised when a ``Schedule`` is constructed with invalid arguments."""

class SensorDefinitionError(Exception):
    """Raised when a ``Sensor`` is constructed with invalid arguments."""

class StorageError(Exception):
    """Raised on storage backend failures (connection, transaction, serialization)."""

class SchemaMigrationNeededError(StorageError):
    """Raised when opening a database whose schema is behind this rivers build.

    A subclass of :class:`StorageError`. ``rivers db migrate`` brings the
    database forward; ``rivers dev`` catches this to offer the migration.
    """

class TaskDefinitionError(Exception):
    """Raised when a ``Task`` / ``BashTask`` definition is invalid."""
