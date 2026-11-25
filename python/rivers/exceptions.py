"""Public exception types raised by the rivers runtime.

These are thin re-exports of the exception classes defined in the Rust ``_core``
extension. Catch ``rivers.exceptions.X`` rather than reaching into ``_core``.
"""

from rivers._core.exceptions import (
    AssetDefinitionError,
    AssetMigrationPendingError,
    AssetNotFoundError,
    AssetOutputValidationError,
    AssetVersionMismatchError,
    ConfigurationError,
    ExecutionError,
    GraphValidationError,
    InvalidMetadataError,
    NodeNotFoundError,
    PartitionDefinitionError,
    PartitionValidationError,
    ResultDefinitionError,
    ScheduleDefinitionError,
    MigrationBlockedError,
    SchemaMigrationNeededError,
    SensorDefinitionError,
    StorageError,
    TaskDefinitionError,
)

__all__ = [
    "AssetDefinitionError",
    "AssetMigrationPendingError",
    "AssetNotFoundError",
    "AssetOutputValidationError",
    "AssetVersionMismatchError",
    "ConfigurationError",
    "ExecutionError",
    "GraphValidationError",
    "InvalidMetadataError",
    "NodeNotFoundError",
    "PartitionDefinitionError",
    "PartitionValidationError",
    "ResultDefinitionError",
    "ScheduleDefinitionError",
    "MigrationBlockedError",
    "SchemaMigrationNeededError",
    "SensorDefinitionError",
    "StorageError",
    "TaskDefinitionError",
]
