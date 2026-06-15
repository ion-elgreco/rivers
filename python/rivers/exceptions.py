"""Public exception types raised by the rivers runtime.

These are thin re-exports of the exception classes defined in the Rust ``_core``
extension. Catch ``rivers.exceptions.X`` rather than reaching into ``_core``.
"""

from rivers._core.exceptions import (
    AssetDefinitionError,
    AssetNotFoundError,
    AssetOutputValidationError,
    ConfigurationError,
    ExecutionError,
    GraphValidationError,
    InvalidMetadataError,
    NodeNotFoundError,
    PartitionDefinitionError,
    PartitionValidationError,
    ResultDefinitionError,
    ScheduleDefinitionError,
    SchemaMigrationNeededError,
    SensorDefinitionError,
    StorageError,
    TaskDefinitionError,
)

__all__ = [
    "AssetDefinitionError",
    "AssetNotFoundError",
    "AssetOutputValidationError",
    "ConfigurationError",
    "ExecutionError",
    "GraphValidationError",
    "InvalidMetadataError",
    "NodeNotFoundError",
    "PartitionDefinitionError",
    "PartitionValidationError",
    "ResultDefinitionError",
    "ScheduleDefinitionError",
    "SchemaMigrationNeededError",
    "SensorDefinitionError",
    "StorageError",
    "TaskDefinitionError",
]
