"""rivers — Rust-powered asset orchestration with a Python API.

Re-exports the public surface from the Rust ``_core`` extension and the pure-Python
helpers (resources, IO handlers, exceptions). Everything listed in :data:`__all__`
is meant to be imported from the top-level ``rivers`` namespace; submodules are
implementation detail.

Example::

    import rivers as rs

    @rs.Asset
    def my_data() -> int:
        return 42

    repo = rs.CodeRepository(assets=[my_data])
    repo.materialize()
"""

import atexit

from rivers import exceptions
from rivers._core import (
    DynamicOutput,
    InputContext,
    InvokedNodeOutput,
    Job,
    Materialization,
    MetadataValue,
    Observation,
    Output,
    OutputContext,
    RunBackendConfig,
    RunQueueConfig,
    Schema,
    TagConcurrencyLimit,
)
from rivers._core.assets import (
    Asset,
    AssetDef,
    AssetExecutionContext,
    DepDef,
    ExternalAsset,
    GraphAsset,
    MultiAsset,
    SelfDependency,
    SingleAsset,
)
from rivers._core.automation import AutomationCondition
from rivers._core.executor import Executor
from rivers._core.hooks import Hook, HookContext
from rivers._core.partitions import (
    BackfillStrategy,
    PartitionContext,
    PartitionKey,
    PartitionKeyRange,
    PartitionMapping,
    PartitionsDefinition,
)
from rivers._core.repo import (
    BackfillResult,
    BackfillStatus,
    CodeRepository,
    RunHandle,
    RunResult,
)
from rivers._core.schedule import (
    BackfillRequest,
    EvalMode,
    RunRequest,
    Schedule,
    ScheduleEvaluationContext,
    ScheduleStatus,
    ScheduleTickResult,
    SkipReason,
)
from rivers._core.sensor import (
    Sensor,
    SensorEvaluationContext,
    SensorResult,
    SensorStatus,
    SensorTickResult,
)
from rivers._core.storage import (
    AssetRecord,
    LaunchedBy,
    RunRecord,
    StaleCause,
    Storage,
    StorageType,
    StoredEvent,
    StoredTick,
)
from rivers._core.tasks import BashTask, Task, TaskExecutionContext
from rivers.io_handlers import BaseIOHandler, InMemoryIOHandler, PickleIOHandler
from rivers.resource import Resource
from rivers._core import drain_in_flight as _drain_in_flight

# Join in-flight materialization / backfill / run worker threads before the
# interpreter finalizes. it's a no-op once the pools are already drained.
atexit.register(_drain_in_flight)

__all__ = [
    "exceptions",
    "AssetDef",
    "Asset",
    "AssetExecutionContext",
    "DepDef",
    "BashTask",
    "ExternalAsset",
    "SingleAsset",
    "MultiAsset",
    "GraphAsset",
    "BackfillResult",
    "BackfillStatus",
    "BackfillStrategy",
    "CodeRepository",
    "RunHandle",
    "RunResult",
    "Output",
    "Observation",
    "Materialization",
    "DynamicOutput",
    "Resource",
    "Task",
    "InvokedNodeOutput",
    "Job",
    "Executor",
    "Hook",
    "HookContext",
    "OutputContext",
    "InputContext",
    "MetadataValue",
    "Schema",
    "PartitionKey",
    "PartitionKeyRange",
    "PartitionsDefinition",
    "PartitionContext",
    "PartitionMapping",
    "AssetRecord",
    "BaseIOHandler",
    "InMemoryIOHandler",
    "PickleIOHandler",
    "LaunchedBy",
    "RunRecord",
    "RunBackendConfig",
    "RunQueueConfig",
    "SelfDependency",
    "TagConcurrencyLimit",
    "TaskExecutionContext",
    "Storage",
    "StorageType",
    "StoredEvent",
    "StaleCause",
    "StoredTick",
    "BackfillRequest",
    "RunRequest",
    "Schedule",
    "ScheduleEvaluationContext",
    "ScheduleStatus",
    "ScheduleTickResult",
    "SkipReason",
    "Sensor",
    "SensorEvaluationContext",
    "SensorResult",
    "SensorStatus",
    "SensorTickResult",
    "AutomationCondition",
    "EvalMode",
]

try:
    from rivers.io_handlers.delta import DeltaIOHandler

    __all__ = [*__all__, "DeltaIOHandler"]
except ImportError:
    pass
